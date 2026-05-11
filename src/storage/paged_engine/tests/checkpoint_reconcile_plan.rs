use super::*;

use std::collections::VecDeque;
use std::sync::Arc;

use bson::{doc, Bson};

use crate::error::{EngineFatalReason, Error, Result};
use crate::keys::encode_key;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool, LatchMode};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::reconcile::driver::{DirtyReason, TreeIdent, TreeKind};
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "phase7.us003";
const LARGE_INLINE_BYTES: usize = crate::storage::page::PAGE_SIZE_LEAF as usize;

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
        txn_id: 703,
        state: VersionState::Committed,
        data: VersionData::Inline(payload),
        is_tombstone: false,
    }
}

fn mark_primary_dirty(engine: &PagedEngine, ident: TreeIdent, leaf: u32) {
    engine
        .shared
        .mark_leaf_dirty(ident, leaf, DirtyReason::PrimaryWrite);
}

fn assert_dirty_leaf(engine: &PagedEngine, ident: &TreeIdent, leaf: u32) {
    let dirty = engine
        .shared
        .dirty_leaves
        .get(ident)
        .expect("dirty tree entry");
    assert!(dirty.contains_key(&leaf), "expected dirty leaf {leaf}");
}

#[test]
fn test_checkpoint_reconcile_plan_detects_visible_notinstallable_before_mutation() -> Result<()> {
    let (engine, io) = buffered_engine();
    engine.create_namespace(NS)?;
    engine.insert(NS, doc! { "_id": 1, "value": "before-checkpoint" })?;
    let ident = primary_ident(&engine);
    let (key, leaf) = primary_leaf_for_id(&engine, &Bson::Int32(1))?;
    let checkpoint_ts = engine.shared.load_published().visible_ts;
    let before_written_pages = io
        .pages
        .lock()
        .map_err(|_| Error::Internal("mock io pages mutex poisoned".into()))?
        .len();

    engine.shared.handle.pool().with_chain_under_latch(
        leaf,
        &key,
        LatchMode::Exclusive,
        |slot| {
            *slot = Some(Arc::new(VecDeque::from([committed_inline(
                checkpoint_ts,
                vec![0xA5; LARGE_INLINE_BYTES],
            )])));
        },
    )?;
    engine.shared.dirty_leaves.clear();
    mark_primary_dirty(&engine, ident.clone(), leaf);

    let err = engine
        .checkpoint()
        .expect_err("visible non-installable leaf blocks checkpoint planning");
    assert!(matches!(
        err,
        Error::CheckpointIncomplete {
            first_blocking_page,
            ..
        } if first_blocking_page == leaf
    ));
    assert_dirty_leaf(&engine, &ident, leaf);
    assert_eq!(
        io.pages
            .lock()
            .map_err(|_| Error::Internal("mock io pages mutex poisoned".into()))?
            .len(),
        before_written_pages,
        "planning failure must not flush checkpoint bytes"
    );

    let (future_engine, _io) = buffered_engine();
    future_engine.create_namespace(NS)?;
    future_engine.insert(NS, doc! { "_id": 2, "value": "future-only" })?;
    let future_ident = primary_ident(&future_engine);
    let (future_key, future_leaf) = primary_leaf_for_id(&future_engine, &Bson::Int32(2))?;
    let future_checkpoint_ts = future_engine.shared.load_published().visible_ts;
    let future_ts = future_checkpoint_ts
        .successor()
        .expect("test timestamp can advance");
    let future_chain = Arc::new(VecDeque::from([committed_inline(
        future_ts,
        bson::to_vec(&doc! { "_id": 2, "value": "future-only" })
            .map_err(Error::BsonSerialization)?,
    )]));
    future_engine.shared.handle.pool().with_chain_under_latch(
        future_leaf,
        &future_key,
        LatchMode::Exclusive,
        |slot| {
            *slot = Some(future_chain);
        },
    )?;
    future_engine.shared.dirty_leaves.clear();
    mark_primary_dirty(&future_engine, future_ident.clone(), future_leaf);

    future_engine.checkpoint()?;

    assert_dirty_leaf(&future_engine, &future_ident, future_leaf);
    future_engine.shared.handle.pool().with_chain_under_latch(
        future_leaf,
        &future_key,
        LatchMode::Exclusive,
        |slot| -> Result<()> {
            let chain = slot
                .as_ref()
                .ok_or_else(|| Error::Internal("future-only residue missing".into()))?;
            assert_eq!(chain.len(), 1);
            // Slot left as-is — inspection only.
            Ok(())
        },
    )??;
    Ok(())
}

#[test]
fn test_checkpoint_incomplete_after_reconcile_mutation_poisons_engine() -> Result<()> {
    let (engine, _io) = buffered_engine();
    engine.create_namespace(NS)?;
    engine.insert(NS, doc! { "_id": 10, "value": "installable" })?;

    engine.us026_arm_post_register_failpoint(Us026PostRegisterFailpoint::Flush);

    let err = engine
        .checkpoint()
        .expect_err("post-reconcile checkpoint failure poisons the engine");
    assert!(matches!(
        err,
        Error::EngineFatal {
            reason: EngineFatalReason::CheckpointPostMutationFailure
        }
    ));

    let rejected = engine
        .insert(NS, doc! { "_id": 11, "value": "after-poison" })
        .expect_err("poisoned engine rejects later writes");
    assert!(matches!(
        rejected,
        Error::EngineFatal {
            reason: EngineFatalReason::CheckpointPostMutationFailure
        }
    ));
    Ok(())
}
