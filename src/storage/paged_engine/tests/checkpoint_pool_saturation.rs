//! R2 repro: checkpoint must not poison the engine under pool saturation.
//!
//! The fail-closed eviction guard refuses to evict any frame carrying a live
//! committed delta head, so a workload that touches more leaves than the
//! 32 KiB partition has frames saturates the pool: live CRUD fails with
//! `PoolExhausted { DeltaBearingFrames }`, whose operator guidance is "wait
//! for checkpoint relief". But checkpoint's MATERIALIZATION stage
//! (`materialize_*_deltas_for_checkpoint`: catalog reads plus the
//! `visible_delta_entries` -> `read_leaf` walk) pins pages again; a
//! non-resident page needs an eviction that the saturated pool cannot
//! perform, the stage fails with `PoolExhausted` AFTER the plan stage, and
//! `poison_checkpoint_post_mutation` used to escalate that recoverable
//! pressure error to `EngineFatal { CheckpointPostMutationFailure }` —
//! permanently poisoning the only relief path the error message points at.
//!
//! Fixed: (1) pre-mutation checkpoint failures (everything before the
//! structural batch commit, including the materialization stage) surface as
//! the recoverable `CheckpointIncomplete { PoolExhausted }` taxonomy instead
//! of poisoning; (2) checkpoint runs a resident-only reconcile relief pass
//! over the plan's mutation-ready pages before materialization, folding
//! committed delta heads in place so the stage has evictable frames again.

use std::sync::Arc;

use bson::doc;

use super::*;
use crate::error::Result;
use crate::storage::btree::BTreePageStore;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

const BIG_NS: &str = "test.r2big";
const SAT_NS_COUNT: usize = 24;

/// 512 KiB total budget: the 32 KiB partition gets 3/4 of it, i.e. 12
/// frames — small enough that a handful of single-leaf namespaces with
/// live committed delta heads saturates every frame.
const TINY_POOL_BYTES: usize = 512 * 1024;

fn tiny_pool_engine() -> PagedEngine {
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        TINY_POOL_BYTES,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        TINY_POOL_BYTES,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let header = FileHeader::new_now();
    let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
    PagedEngine::new_buffered(handle, 0, 0).expect("create buffered engine")
}

fn sat_ns(i: usize) -> String {
    format!("test.r2sat{i}")
}

fn desktop_pool_engine() -> PagedEngine {
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
    PagedEngine::new_buffered(handle, 0, 0).expect("create buffered engine")
}

/// F0: a recoverable checkpoint failure must not leak the deferred-free
/// pages the structural batch drained out of the page-lifetime queue.
///
/// `StructuralPageBatch::new` eagerly drains the allocator's deferred-free
/// queue; `abort()` is the only requeue path and there is no `Drop` impl.
/// Pre-fix, `stage_checkpoint_pre_mutation` created the batch FIRST and
/// every `?` early-return in the spill/relief window (journal sync, LSN
/// stamps, the reconcile passes — exactly the `PoolExhausted` saturation
/// regime) dropped the batch silently: the checkpoint surfaced as
/// recoverable, the engine kept serving, and the drained pages were on no
/// free list, in no queue, and held no refcount until reopen.
#[test]
fn recoverable_checkpoint_failure_does_not_leak_drained_deferred_free_pages() -> Result<()> {
    let engine = desktop_pool_engine();
    engine.create_namespace("test.f0leak")?;
    engine.insert("test.f0leak", doc! { "_id": 1i64, "v": 1i64 })?;

    // A real (allocated, otherwise unreferenced) page whose overflow
    // refcount hit zero: enqueued for deferred free and fence-eligible for
    // the next structural-batch drain.
    let page = {
        let mut store = BufferPoolPageStore::new(Arc::clone(&engine.shared.handle));
        store.alloc_leaf()?
    };
    let allocator = engine.shared.handle.allocator();
    allocator.enqueue_overflow_deferred_free(page);
    allocator.advance_page_lifetime_checkpoint_fence();
    assert_eq!(allocator.page_lifetime_queue().depth(), 1);

    super::snapshot_ops::checkpoint_stage_failpoint::arm_spill_relief_window_failure();
    let err = engine
        .checkpoint()
        .expect_err("armed spill/relief-window failpoint must fail the checkpoint");
    assert!(
        !matches!(err, Error::EngineFatal { .. }),
        "spill/relief-window failure must stay recoverable, got {err:?}"
    );

    assert_eq!(
        engine
            .shared
            .handle
            .allocator()
            .page_lifetime_queue()
            .depth(),
        1,
        "recoverable checkpoint failure leaked the drained deferred-free page"
    );
    Ok(())
}

/// Make every evictable 32 KiB frame delta-bearing: insert one committed
/// row per (pre-created, checkpointed-clean) namespace until the pool fails
/// closed. Each insert faults the namespace's clean root leaf back in —
/// evicting another clean frame (catalog and BIG_NS leaves included) — then
/// parks a live committed delta head on it, making the frame unevictable.
/// Returns how many inserts committed before exhaustion.
fn saturate_pool_with_delta_bearing_frames(engine: &PagedEngine) -> Result<usize> {
    for i in 0..SAT_NS_COUNT {
        match engine.insert(&sat_ns(i), doc! { "_id": 2i64, "v": i as i64 }) {
            Ok(_inserted_id) => {}
            Err(Error::PoolExhausted { .. }) => return Ok(i),
            Err(other) => return Err(other),
        }
    }
    Err(Error::Internal(
        "tiny pool failed to saturate within the pre-created namespaces".into(),
    ))
}

#[test]
fn checkpoint_under_pool_saturation_does_not_poison_engine() -> Result<()> {
    let engine = tiny_pool_engine();

    // 1. Build a multi-leaf tree in BIG_NS plus the single-leaf saturation
    //    namespaces, then checkpoint so every page is durable, delta-free,
    //    and therefore evictable.
    engine.create_namespace(BIG_NS)?;
    let pad = "x".repeat(8 * 1024);
    for id in 0..12i64 {
        engine.insert(BIG_NS, doc! { "_id": id, "pad": pad.clone() })?;
    }
    // Materialize BIG_NS first so the tree splits into several clean leaves
    // before the saturation namespaces start competing for frames.
    engine.checkpoint()?;
    for i in 0..SAT_NS_COUNT {
        let ns = sat_ns(i);
        engine.create_namespace(&ns)?;
        engine.insert(&ns, doc! { "_id": 1i64, "v": i as i64 })?;
        if i % 6 == 5 {
            // Fold the seeded delta heads while frames remain: the tiny
            // pool cannot host all SAT_NS_COUNT delta-bearing roots at once.
            engine.checkpoint()?;
        }
    }
    engine.checkpoint()?;

    // 2. Re-dirty BIG_NS's RIGHTMOST leaf so the checkpoint walk must
    //    re-read the (about to be evicted) leftmost leaves before reaching
    //    it.
    engine.insert(BIG_NS, doc! { "_id": 1_000_000i64, "pad": pad.clone() })?;

    // 3. Saturate: every frame ends up carrying a live committed delta head
    //    (clean catalog and BIG_NS leaves get evicted to make room), then
    //    the fail-closed guard reports exhaustion.
    let committed = saturate_pool_with_delta_bearing_frames(&engine)?;
    assert!(
        committed > 0,
        "at least one delta-bearing insert must commit before exhaustion"
    );

    // 4. Checkpoint under saturation. This is the relief path the
    //    PoolExhausted operator guidance points at: it must NOT poison the
    //    engine. Either it succeeds outright (relief pass) or it returns a
    //    recoverable error and a retry makes progress.
    match engine.checkpoint() {
        Ok(()) => {}
        Err(Error::EngineFatal { reason }) => {
            panic!("checkpoint under pool saturation poisoned the engine: {reason:?}")
        }
        Err(recoverable) => {
            // Recoverable failure: the retry after the relief pass must
            // make progress instead of wedging permanently.
            engine.checkpoint().map_err(|err| match err {
                Error::EngineFatal { reason } => {
                    panic!(
                        "checkpoint retry after recoverable {recoverable:?} poisoned: {reason:?}"
                    )
                }
                other => other,
            })?;
        }
    }

    // 5. The engine must accept new work afterwards (poison refuses all
    //    operations) — and the relief pass must have freed delta-bearing
    //    frames, so this insert also proves eviction works again.
    engine.insert(BIG_NS, doc! { "_id": 2_000_000i64, "pad": pad })?;

    // 6. Previously committed data survived the saturated checkpoint.
    assert!(
        engine.find_one(BIG_NS, &doc! { "_id": 0i64 })?.is_some(),
        "row in the leftmost leaf must survive the saturated checkpoint"
    );
    assert!(
        engine
            .find_one(BIG_NS, &doc! { "_id": 1_000_000i64 })?
            .is_some(),
        "row committed just before saturation must survive"
    );
    assert!(
        engine
            .find_one(&sat_ns(0), &doc! { "_id": 2i64 })?
            .is_some(),
        "delta-bearing row committed during saturation must survive"
    );
    Ok(())
}
