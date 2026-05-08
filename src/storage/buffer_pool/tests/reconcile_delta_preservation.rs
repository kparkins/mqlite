use super::*;

use std::collections::VecDeque;
use std::sync::Arc;

use crate::mvcc::read_view::ReadViewRegistry;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::allocator::AllocatorHandle;
use crate::storage::header::FileHeader;
use crate::storage::page::PAGE_TYPE_LEAF;

struct ZeroIo;

impl PageSource for ZeroIo {
    fn read_page(&self, _page_number: u32, _size: PageSize, buf: &mut [u8]) -> Result<()> {
        buf.fill(0);
        buf[0] = PAGE_TYPE_LEAF;
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

fn resident_pool(page: u32) -> (BufferPool, AllocatorHandle) {
    let pool = BufferPool::new(PageSize::Large32k.bytes() * 4, Box::new(ZeroIo));
    drop(pool.pin(page, PageSize::Large32k).unwrap());
    let allocator = AllocatorHandle::new(FileHeader::new(0, 0, 0));
    (pool, allocator)
}

fn inline_entry(start_ts: Ts, stop_ts: Ts, payload: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts,
        txn_id: start_ts.physical_ms,
        state: VersionState::Committed,
        data: VersionData::Inline(payload.to_vec()),
        is_tombstone: false,
    }
}

fn tombstone_entry(start_ts: Ts, stop_ts: Ts) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts,
        txn_id: start_ts.physical_ms,
        state: VersionState::Committed,
        data: VersionData::Inline(Vec::new()),
        is_tombstone: true,
    }
}

fn install_chain(pool: &BufferPool, page: u32, key: &[u8], chain: VecDeque<VersionEntry>) {
    pool.put_chain(page, key.to_vec(), Arc::new(chain)).unwrap();
}

#[test]
fn test_reconcile_preserves_delta_only_live_head() {
    let page = 19;
    let (pool, allocator) = resident_pool(page);
    let registry = ReadViewRegistry::new();

    let mut chain = VecDeque::new();
    chain.push_back(inline_entry(ts(100), Ts::MAX, b"delta-head"));
    install_chain(&pool, page, b"delta-only", chain);

    let dropped = pool.reconcile(page, &registry, &allocator).unwrap();

    assert_eq!(dropped, 0);
    assert!(!pool.chains_empty(page).unwrap());
}

#[test]
fn test_reconcile_collapses_base_backed_head() {
    let page = 20;
    let (pool, allocator) = resident_pool(page);
    let registry = ReadViewRegistry::new();

    let mut chain = VecDeque::new();
    chain.push_back(inline_entry(ts(100), Ts::MAX, b"base-backed-head"));
    chain.push_back(inline_entry(ts(10), ts(20), b"expired"));
    install_chain(&pool, page, b"base-backed", chain);

    let dropped = pool.reconcile(page, &registry, &allocator).unwrap();

    assert_eq!(dropped, 1);
    assert!(!pool.chains_empty(page).unwrap());
}

#[test]
fn test_reconcile_prunes_expired_delta_only_tombstone() {
    let page = 21;
    let (pool, allocator) = resident_pool(page);
    let registry = ReadViewRegistry::new();

    let mut chain = VecDeque::new();
    chain.push_back(tombstone_entry(ts(50), ts(60)));
    install_chain(&pool, page, b"expired-tombstone", chain);

    let dropped = pool.reconcile(page, &registry, &allocator).unwrap();

    assert_eq!(dropped, 1);
    assert!(pool.chains_empty(page).unwrap());
}
