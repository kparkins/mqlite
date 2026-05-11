#![allow(clippy::panic, clippy::unwrap_used)]

use super::*;

use std::collections::VecDeque;
use std::sync::Arc;

use crate::mvcc::read_view::{ReadView, ReadViewRegistry};
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::allocator::AllocatorHandle;
use crate::storage::buffer_pool::partition::Partition;
use crate::storage::header::FileHeader;

const DELTA_PAGE: u32 = 101;
const SPARE_PAGE: u32 = 102;
const PRESSURE_PAGE: u32 = 103;
const DELTA_KEY: &[u8] = b"delta-only";
const BLOCKED_REASON: &str = "delta-bearing frame; Phase 4 reconcile not yet available";

struct ZeroIo;

impl PageSource for ZeroIo {
    fn read_page(&self, _page_number: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
        assert_eq!(buf.len(), size.bytes());
        buf.fill(0);
        Ok(())
    }

    fn write_page(&self, _page_number: u32, _size: PageSize, _buf: &[u8]) -> Result<()> {
        Ok(())
    }
}

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

    let view = ReadView::new(ts(10), 1);
    let snapshot = pool.snapshot_chains(DELTA_PAGE, None).unwrap().unwrap();
    let visible = snapshot.visible_at(DELTA_KEY, &view);

    assert!(
        visible.is_some(),
        "live committed delta must survive pressure"
    );
}

#[test]
fn pending_heads_do_not_block_reconcile_eviction() {
    let (pool, registry, allocator) = one_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);
    install_chain(&pool, DELTA_PAGE, pending_head());

    drop(
        pool.pin_with_reconcile(PRESSURE_PAGE, PageSize::Large32k, &registry, &allocator)
            .unwrap(),
    );

    assert_not_cached(&pool, DELTA_PAGE);
}
