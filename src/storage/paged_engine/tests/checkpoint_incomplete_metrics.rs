use super::*;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use bson::{doc, Bson};

use crate::error::{CheckpointIncompleteReason, Error, PoolExhaustedReason, Result};
use crate::keys::encode_key;
use crate::mvcc::metrics::{
    checkpoint_frontier_blocked_snapshot, reset_checkpoint_frontier_blocked,
};
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::reconcile::driver::{
    checkpoint_incomplete_error, checkpoint_reason_for_plan_blocker, CheckpointPlanBlocker,
};
use crate::storage::reconcile::driver::{DirtyReason, TreeIdent, TreeKind};
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "phase7.us010";
const LARGE_INLINE_BYTES: usize = crate::storage::page::PAGE_SIZE_LEAF as usize;

fn us010_metrics_guard() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().expect("US-010 metrics lock poisoned")
}

fn buffered_engine() -> (PagedEngine, Arc<MockIo>) {
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let header = FileHeader::new_now();
    let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
    let engine = PagedEngine::new_buffered(handle, 0, 0).expect("create buffered engine");
    (engine, io)
}

fn primary_ident(engine: &PagedEngine) -> TreeIdent {
    let epoch = engine.shared.load_published();
    let ns_snap = epoch.catalog.get_by_name(NS).expect("namespace snapshot");
    TreeIdent {
        collection_id: ns_snap.id,
        kind: TreeKind::Primary,
    }
}

fn primary_leaf_for_id(engine: &PagedEngine, id: &Bson) -> Result<(Vec<u8>, u32)> {
    let key = encode_key(id);
    let epoch = engine.shared.load_published();
    let ns_snap = epoch.catalog.get_by_name(NS).expect("namespace snapshot");
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&engine.shared.handle)),
        ns_snap.data_root_page,
        ns_snap.data_root_level,
    );
    let leaf = tree.find_leaf(&key)?;
    Ok((key, leaf))
}

fn committed_inline(start_ts: Ts, payload: Vec<u8>) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts: Ts::MAX,
        txn_id: 710,
        state: VersionState::Committed,
        data: VersionData::Inline(payload),
        is_tombstone: false,
    }
}

fn install_oversized_checkpoint_visible_leaf(engine: &PagedEngine) -> Result<(TreeIdent, u32)> {
    engine.create_namespace(NS)?;
    engine.insert(NS, doc! { "_id": 1, "value": "before-checkpoint" })?;
    let ident = primary_ident(engine);
    let (key, leaf) = primary_leaf_for_id(engine, &Bson::Int32(1))?;
    let checkpoint_ts = engine.shared.load_published().visible_ts;
    engine.shared.handle.pool().put_chain(
        leaf,
        key,
        Arc::new(VecDeque::from([committed_inline(
            checkpoint_ts,
            vec![0xA5; LARGE_INLINE_BYTES],
        )])),
    )?;
    engine.shared.dirty_leaves.clear();
    engine
        .shared
        .mark_leaf_dirty(ident.clone(), leaf, DirtyReason::PrimaryWrite);
    Ok((ident, leaf))
}

#[test]
fn test_checkpoint_incomplete_reason_mapping_for_visible_notinstallable() -> Result<()> {
    let _guard = us010_metrics_guard();
    reset_checkpoint_frontier_blocked();
    let (engine, _io) = buffered_engine();
    let (_ident, leaf) = install_oversized_checkpoint_visible_leaf(&engine)?;

    let err = engine
        .checkpoint()
        .expect_err("visible over-budget leaf must block before mutation");

    assert!(matches!(
        err,
        Error::CheckpointIncomplete {
            first_blocking_page,
            reason: CheckpointIncompleteReason::VisibleWinnerExceedsPageBudget
        } if first_blocking_page == leaf
    ));
    assert_eq!(
        checkpoint_reason_for_plan_blocker(CheckpointPlanBlocker::FrameCoWRefused),
        CheckpointIncompleteReason::FrameCoWRefused
    );
    assert_eq!(
        checkpoint_reason_for_plan_blocker(CheckpointPlanBlocker::OverflowSpillNotWired),
        CheckpointIncompleteReason::OverflowSpillNotWired
    );
    assert_eq!(
        checkpoint_reason_for_plan_blocker(CheckpointPlanBlocker::TombstonePredecessorPressure),
        CheckpointIncompleteReason::TombstonePredecessorPressure
    );
    assert_eq!(
        checkpoint_reason_for_plan_blocker(CheckpointPlanBlocker::HistoryDuplicateConflict),
        CheckpointIncompleteReason::HistoryDuplicateConflict
    );
    assert_eq!(
        checkpoint_reason_for_plan_blocker(CheckpointPlanBlocker::HistoryDuplicateCapExceeded),
        CheckpointIncompleteReason::HistoryDuplicateCapExceeded
    );
    assert_eq!(
        checkpoint_reason_for_plan_blocker(CheckpointPlanBlocker::ReachabilityRepairRequired),
        CheckpointIncompleteReason::ReachabilityRepairRequired
    );
    Ok(())
}

#[test]
fn test_checkpoint_incomplete_pool_pressure_not_raw_pool_exhausted() -> Result<()> {
    let _guard = us010_metrics_guard();
    reset_checkpoint_frontier_blocked();
    let io = Arc::new(MockIo::default());
    let pool = BufferPool::new(PageSize::Large32k.bytes(), Box::new(ArcIo(Arc::clone(&io))));
    let _pinned = pool.pin(1, PageSize::Large32k)?;

    let live_err = match pool.pin(2, PageSize::Large32k) {
        Err(err) => err,
        Ok(_) => panic!("live pool pressure must fail"),
    };
    assert!(matches!(
        live_err,
        Error::PoolExhausted {
            reason: PoolExhaustedReason::AllFramesPinned
        }
    ));

    let checkpoint_err = checkpoint_incomplete_error(
        9,
        CheckpointIncompleteReason::PoolExhausted(PoolExhaustedReason::AllFramesPinned),
    );
    assert!(matches!(
        checkpoint_err,
        Error::CheckpointIncomplete {
            first_blocking_page: 9,
            reason: CheckpointIncompleteReason::PoolExhausted(PoolExhaustedReason::AllFramesPinned)
        }
    ));
    reset_checkpoint_frontier_blocked();
    Ok(())
}

#[test]
fn test_checkpoint_frontier_blocked_total_increments_once() -> Result<()> {
    let _guard = us010_metrics_guard();
    reset_checkpoint_frontier_blocked();
    let (engine, _io) = buffered_engine();
    install_oversized_checkpoint_visible_leaf(&engine)?;

    assert_eq!(checkpoint_frontier_blocked_snapshot(), 0);
    let err = engine
        .checkpoint()
        .expect_err("checkpoint-visible blocker increments frontier metric");
    assert!(matches!(err, Error::CheckpointIncomplete { .. }));
    assert_eq!(checkpoint_frontier_blocked_snapshot(), 1);
    Ok(())
}

#[test]
fn test_checkpoint_incomplete_operator_detail_lists_retry_actions() {
    let err = Error::CheckpointIncomplete {
        first_blocking_page: 42,
        reason: CheckpointIncompleteReason::PoolExhausted(PoolExhaustedReason::DeltaBearingFrames),
    };
    let detail = err.to_string();

    assert!(detail.contains("first_blocking_page=42"));
    assert!(detail.contains("pool exhausted: delta-bearing frames"));
    assert!(detail.contains("close or expire long readers or pins"));
    assert!(detail.contains("enable overflow spill if blocking"));
    assert!(detail.contains("raise pool or cap limits"));
    assert!(detail.contains("retry checkpoint"));
}
