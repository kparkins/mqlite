#![allow(clippy::panic, clippy::unwrap_used)]

use super::*;

use std::collections::VecDeque;
use std::sync::Arc;

use crate::mvcc::read_view::ReadView;
use crate::mvcc::registry::ReadViewRegistry;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::allocator::AllocatorHandle;
use crate::storage::buffer_pool::partition::Partition;
use crate::storage::header::FileHeader;
use crate::storage::test_support::ZeroIo;

const DELTA_PAGE: u32 = 101;
const SPARE_PAGE: u32 = 102;
const PRESSURE_PAGE: u32 = 103;
const DELTA_KEY: &[u8] = b"delta-only";
const BLOCKED_REASON: &str = "delta-bearing frame; Phase 4 reconcile not yet available";

fn ts(physical_ms: u64) -> Ts {
    Ts {
        physical_ms,
        logical: 0,
    }
}

fn entry(state: VersionState, stop_ts: Ts, tombstone: bool) -> VersionEntry {
    VersionEntry {
        start_ts: ts(10),
        stop_ts,
        txn_id: 1,
        state,
        data: VersionData::Inline(Vec::from(&b"value"[..])),
        is_tombstone: tombstone,
    }
}

fn committed_head() -> VersionEntry {
    entry(VersionState::Committed, Ts::MAX, false)
}

fn committed_tombstone_head() -> VersionEntry {
    entry(VersionState::Committed, Ts::MAX, true)
}

fn pending_head() -> VersionEntry {
    entry(VersionState::Pending { txn_id: 1 }, Ts::MAX, false)
}

fn expired_entry() -> VersionEntry {
    entry(VersionState::Committed, ts(20), false)
}

fn aborted_residue_head() -> VersionEntry {
    // Aborted first-write residue: the abort flip sets `state = Aborted`
    // without touching `stop_ts`, and there is no predecessor to restore,
    // leaving exactly `[Aborted, stop_ts = Ts::MAX]` (BUG-11 shape).
    entry(VersionState::Aborted, Ts::MAX, false)
}

fn install_chain(pool: &BufferPool, page: u32, entry: VersionEntry) {
    pool.with_chain_under_latch(page, DELTA_KEY, LatchMode::Exclusive, |slot| {
        *slot = Some(Arc::new(VecDeque::from([entry])));
    })
    .unwrap();
}

fn two_frame_pool() -> (BufferPool, Arc<ReadViewRegistry>, AllocatorHandle) {
    let pool = BufferPool::new(PageSize::Large32k.bytes() * 3, Box::new(ZeroIo));
    let registry = ReadViewRegistry::new();
    let allocator = AllocatorHandle::new(FileHeader::new(0, 0, 0));
    (pool, registry, allocator)
}

fn one_frame_pool() -> (BufferPool, Arc<ReadViewRegistry>, AllocatorHandle) {
    let pool = BufferPool::new(PageSize::Large32k.bytes(), Box::new(ZeroIo));
    let registry = ReadViewRegistry::new();
    let allocator = AllocatorHandle::new(FileHeader::new(0, 0, 0));
    (pool, registry, allocator)
}

fn load_and_unpin(pool: &BufferPool, page: u32) {
    drop(pool.pin(page, PageSize::Large32k).unwrap());
}

fn assert_cached(pool: &BufferPool, page: u32) {
    let guard = pool.inner_32k.lock().unwrap();
    assert!(guard.is_cached(page), "page {page} should stay cached");
}

fn assert_not_cached(pool: &BufferPool, page: u32) {
    let guard = pool.inner_32k.lock().unwrap();
    assert!(!guard.is_cached(page), "page {page} should be evicted");
}

#[test]
fn test_pin_page_reconciling_does_not_lose_delta_only_key() {
    let (pool, registry, allocator) = two_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);
    load_and_unpin(&pool, SPARE_PAGE);
    install_chain(&pool, DELTA_PAGE, committed_head());

    let snapshot = pool.snapshot_chains(DELTA_PAGE, None).unwrap().unwrap();

    drop(
        pool.pin_with_reconcile(PRESSURE_PAGE, PageSize::Large32k, &registry, &allocator)
            .unwrap(),
    );

    assert_eq!(snapshot.chain_len(DELTA_KEY), 1);
    assert_cached(&pool, DELTA_PAGE);
    assert!(!pool.chains_empty(DELTA_PAGE).unwrap());
}

#[test]
fn test_evict_blocked_when_frame_has_live_committed_head() {
    let mut partition = Partition::new(1, PageSize::Large32k.bytes());
    let io = ZeroIo;

    partition
        .pin_page_reconciling(DELTA_PAGE, Ts::MAX, &io, PageSize::Large32k, u64::MAX)
        .unwrap();
    partition.unpin_page(DELTA_PAGE, false, None).unwrap();
    let frame = partition.frames[0].as_mut().unwrap();
    frame.deltas.insert(
        DELTA_KEY.to_vec(),
        Arc::new(VecDeque::from([committed_head()])),
    );

    let err = partition
        .pin_page_reconciling(PRESSURE_PAGE, Ts::MAX, &io, PageSize::Large32k, u64::MAX)
        .unwrap_err();

    match err {
        Error::BufferPoolEvictionBlocked { page, reason } => {
            assert_eq!(page, DELTA_PAGE);
            assert_eq!(reason, BLOCKED_REASON);
        }
        other => panic!("expected BufferPoolEvictionBlocked, got {other}"),
    }
}

#[test]
fn test_evict_blocked_for_tombstone_head_until_ort_passes() {
    let (pool, registry, allocator) = one_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);
    install_chain(&pool, DELTA_PAGE, committed_tombstone_head());

    let result = pool.pin_with_reconcile(PRESSURE_PAGE, PageSize::Large32k, &registry, &allocator);
    assert!(result.is_err(), "live tombstone head must block eviction");
    assert_cached(&pool, DELTA_PAGE);

    pool.with_chain_under_latch(DELTA_PAGE, DELTA_KEY, LatchMode::Exclusive, |slot| {
        *slot = Some(Arc::new(VecDeque::from([expired_entry()])));
    })
    .unwrap();

    drop(
        pool.pin_with_reconcile(PRESSURE_PAGE, PageSize::Large32k, &registry, &allocator)
            .unwrap(),
    );
    assert_not_cached(&pool, DELTA_PAGE);
}

#[test]
fn test_evict_allowed_for_frame_with_only_expired_chains() {
    let (pool, registry, allocator) = one_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);
    install_chain(&pool, DELTA_PAGE, expired_entry());

    drop(
        pool.pin_with_reconcile(PRESSURE_PAGE, PageSize::Large32k, &registry, &allocator)
            .unwrap(),
    );

    assert_not_cached(&pool, DELTA_PAGE);
}

#[test]
fn test_committed_delta_survives_cache_pressure_round_trip() {
    let (pool, registry, allocator) = two_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);
    load_and_unpin(&pool, SPARE_PAGE);
    install_chain(&pool, DELTA_PAGE, committed_head());

    drop(
        pool.pin_with_reconcile(PRESSURE_PAGE, PageSize::Large32k, &registry, &allocator)
            .unwrap(),
    );

    assert_cached(&pool, DELTA_PAGE);
    assert_not_cached(&pool, SPARE_PAGE);

    let view = ReadView::new_frontier_pinned_for_tests(ts(10), 1);
    let snapshot = pool.snapshot_chains(DELTA_PAGE, None).unwrap().unwrap();
    let visible = snapshot.visible_at(DELTA_KEY, &view);

    assert!(
        visible.is_some(),
        "live committed delta must survive pressure"
    );
}

#[test]
fn reconcile_refuses_to_destroy_retained_above_horizon_version() {
    // R8: a Committed entry with `ort < stop_ts < Ts::MAX` is superseded
    // but still required by a live reader whose read_ts sits below its
    // stop_ts. `has_live_delta_entry` does not block on it (stop_ts !=
    // MAX) and `reconcile_frame_at` deliberately RETAINS it — so the
    // miss path must refuse the victim instead of destroying the
    // just-retained version (silent snapshot-isolation violation).
    // Self-relieving: once the reader drops and ort advances past
    // stop_ts, the prune drops the entry and eviction proceeds.
    let (pool, registry, allocator) = one_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);

    // Live reader at ts(10) pins the horizon: ort = ts(10) < stop_ts =
    // ts(20) < Ts::MAX, so the entry below is above-horizon.
    let view = ReadView::open_frontier_pinned_for_tests(Arc::clone(&registry), ts(10), 7);
    install_chain(&pool, DELTA_PAGE, expired_entry());

    let result = pool.pin_with_reconcile(PRESSURE_PAGE, PageSize::Large32k, &registry, &allocator);
    match result {
        Err(Error::PoolExhausted {
            reason: PoolExhaustedReason::DeltaBearingFrames,
        }) => {}
        Ok(_) => panic!(
            "reconcile miss path must refuse a frame whose pruned chains \
             still hold a version required above the horizon"
        ),
        Err(other) => panic!("expected PoolExhausted(DeltaBearingFrames), got {other:?}"),
    }
    assert_cached(&pool, DELTA_PAGE);

    let snapshot = pool.snapshot_chains(DELTA_PAGE, None).unwrap().unwrap();
    assert_eq!(
        snapshot.chain_len(DELTA_KEY),
        1,
        "the superseded version still needed by the ts(10) reader must survive"
    );
    drop(view);
}

#[test]
fn unflushable_delta_bearing_frame_attributes_delta_bearing_exhaustion() {
    // R-attrib: a frame blocked by BOTH unflushable dirty bytes and
    // resident deltas must attribute the pool exhaustion to its deltas —
    // a checkpoint (reconcile + flush) relieves it; resizing the pool
    // does not. Requiring `can_flush_at` before the DeltaBearingFrames
    // attribution misreports this frame as AllFramesPinned.
    let mut partition = Partition::new(1, PageSize::Large32k.bytes());
    let io = ZeroIo;

    partition
        .pin_page(DELTA_PAGE, &io, PageSize::Large32k, u64::MAX)
        .unwrap();
    // Dirty unpin marks the frame Unflushable: `can_flush_at` is false
    // at every durable LSN until a covering commit end-LSN is stamped.
    partition
        .unpin_page(
            DELTA_PAGE,
            true,
            Some(vec![0u8; PageSize::Large32k.bytes()]),
        )
        .unwrap();
    let frame = partition.frames[0].as_mut().unwrap();
    frame.deltas.insert(
        DELTA_KEY.to_vec(),
        Arc::new(VecDeque::from([committed_head()])),
    );

    let err = partition
        .pin_page(PRESSURE_PAGE, &io, PageSize::Large32k, u64::MAX)
        .unwrap_err();
    match err {
        Error::PoolExhausted {
            reason: PoolExhaustedReason::DeltaBearingFrames,
        } => {}
        other => panic!(
            "delta-bearing unflushable frame must report \
             PoolExhausted(DeltaBearingFrames), got {other:?}"
        ),
    }
}

#[test]
fn plain_miss_evicts_clean_frame_under_partial_delta_saturation() {
    // R-progress: with one delta-bearing frame and one clean frame, a
    // plain-path miss must still make progress by evicting the clean
    // frame — the hard skip on delta-bearing frames must not starve the
    // sweep of its clean victims.
    let (pool, _registry, _allocator) = two_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);
    load_and_unpin(&pool, SPARE_PAGE);
    install_chain(&pool, DELTA_PAGE, committed_head());

    drop(
        pool.pin(PRESSURE_PAGE, PageSize::Large32k)
            .expect("miss must succeed by evicting the clean frame"),
    );

    assert_cached(&pool, DELTA_PAGE);
    assert_not_cached(&pool, SPARE_PAGE);
    assert!(
        !pool.chains_empty(DELTA_PAGE).unwrap(),
        "live committed chain must survive the plain-path miss"
    );
}

#[test]
fn plain_miss_evicts_frame_with_only_aborted_residue() {
    // R-progress (plain-path variant of BUG-11): a frame whose only
    // chain entry is aborted first-write residue is dead to every
    // reader, so the horizon-free plain miss path must reclaim it.
    let (pool, _registry, _allocator) = one_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);
    install_chain(&pool, DELTA_PAGE, aborted_residue_head());

    let result = pool.pin(PRESSURE_PAGE, PageSize::Large32k);
    assert!(
        result.is_ok(),
        "all-aborted-residue frame must be evictable on the plain path; got {:?}",
        result.err()
    );
    drop(result);
    assert_not_cached(&pool, DELTA_PAGE);
}

#[test]
fn pending_heads_block_reconcile_eviction() {
    // BUG-2: during the commit envelope's install→flip window a frame's
    // only live head is the txn's Pending entry. Evicting it would make
    // the post-durable flip a silent no-op and lose the committed write,
    // so pending-only frames must block eviction like committed heads.
    let (pool, registry, allocator) = one_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);
    install_chain(&pool, DELTA_PAGE, pending_head());

    let result = pool.pin_with_reconcile(PRESSURE_PAGE, PageSize::Large32k, &registry, &allocator);
    match result {
        Err(Error::PoolExhausted {
            reason: PoolExhaustedReason::DeltaBearingFrames,
        }) => {}
        Ok(_) => panic!("pending-only frame must not be evictable"),
        Err(other) => panic!("expected PoolExhausted(DeltaBearingFrames), got {other:?}"),
    }
    assert_cached(&pool, DELTA_PAGE);
}
