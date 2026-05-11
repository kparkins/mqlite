#![allow(clippy::panic, clippy::unwrap_used)]

use super::*;

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};

use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::page::{PAGE_TYPE_LEAF, PAGE_TYPE_OVERFLOW};
use crate::storage::reconcile::driver::{TreeIdent, TreeKind};

const COLLECTION_ID: i64 = 42;
const PAGE_ID: u32 = 55;
const OLD_KEY: &[u8] = b"old-key";
const RETAINED_KEY: &[u8] = b"retained-key";
const PAYLOAD_OFFSET: usize = 64;

#[derive(Default)]
struct MockIo {
    pages: StdMutex<BTreeMap<u32, Vec<u8>>>,
}

impl MockIo {
    fn seed(&self, page: u32, data: Vec<u8>) {
        self.pages.lock().unwrap().insert(page, data);
    }
}

impl PageSource for MockIo {
    fn read_page(&self, page: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
        assert_eq!(buf.len(), size.bytes());
        let pages = self.pages.lock().unwrap();
        if let Some(data) = pages.get(&page) {
            buf.copy_from_slice(data);
        } else {
            buf.fill(0);
            buf[0] = PAGE_TYPE_LEAF;
        }
        Ok(())
    }

    fn write_page(&self, page: u32, _size: PageSize, buf: &[u8]) -> Result<()> {
        self.pages.lock().unwrap().insert(page, buf.to_vec());
        Ok(())
    }
}

struct ArcIo(Arc<MockIo>);

impl PageSource for ArcIo {
    fn read_page(&self, page: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
        self.0.read_page(page, size, buf)
    }

    fn write_page(&self, page: u32, size: PageSize, buf: &[u8]) -> Result<()> {
        self.0.write_page(page, size, buf)
    }
}

fn primary_ident() -> TreeIdent {
    TreeIdent {
        collection_id: COLLECTION_ID,
        kind: TreeKind::Primary,
    }
}

fn pool_with_leaf(page: u32) -> BufferPool {
    let io = Arc::new(MockIo::default());
    io.seed(page, leaf_page(0x11));
    let pool = BufferPool::new(PageSize::Large32k.bytes(), Box::new(ArcIo(io)));
    drop(pool.pin(page, PageSize::Large32k).unwrap());
    pool
}

fn leaf_page(marker: u8) -> Vec<u8> {
    let mut data = vec![0u8; PageSize::Large32k.bytes()];
    data[0] = PAGE_TYPE_LEAF;
    data[PAYLOAD_OFFSET] = marker;
    data
}

fn non_leaf_page() -> Vec<u8> {
    let mut data = vec![0u8; PageSize::Large32k.bytes()];
    data[0] = PAGE_TYPE_OVERFLOW;
    data
}

fn version(payload: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: Ts {
            physical_ms: 10,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id: 7,
        state: VersionState::Committed,
        data: VersionData::Inline(payload.to_vec()),
        is_tombstone: false,
    }
}

fn chain(payload: &[u8]) -> Arc<VecDeque<VersionEntry>> {
    Arc::new(VecDeque::from([version(payload)]))
}

#[test]
fn replace_leaf_and_chains_swaps_base_and_retained_chains() {
    let pool = pool_with_leaf(PAGE_ID);
    pool.with_chain_under_latch(PAGE_ID, OLD_KEY, LatchMode::Exclusive, |slot| {
        *slot = Some(chain(b"old"));
    })
    .unwrap();

    let mut retained = BTreeMap::new();
    retained.insert(RETAINED_KEY.to_vec(), chain(b"retained"));

    let mut guard = pool
        .pin_leaf_for_reconcile(primary_ident(), PAGE_ID)
        .unwrap();
    pool.replace_leaf_and_chains(&mut guard, leaf_page(0xA5), retained)
        .unwrap();
    drop(guard);

    let page = pool.pin(PAGE_ID, PageSize::Large32k).unwrap();
    assert_eq!(page.data()[PAYLOAD_OFFSET], 0xA5);
    drop(page);

    let snapshot = pool.snapshot_chains(PAGE_ID, None).unwrap().unwrap();
    assert_eq!(snapshot.chain_len(OLD_KEY), 0);
    assert_eq!(snapshot.chain_len(RETAINED_KEY), 1);

    let partition = pool.inner_32k.lock().unwrap();
    assert_eq!(partition.pin_count(PAGE_ID), Some(0));
    assert_eq!(partition.is_dirty(PAGE_ID), Some(true));
}

#[test]
fn concurrent_plain_pin_keeps_snapshot_while_latched_replacement_commits() {
    let pool = pool_with_leaf(PAGE_ID);
    let reader_pin = pool.pin(PAGE_ID, PageSize::Large32k).unwrap();
    let mut guard = pool
        .pin_leaf_for_reconcile(primary_ident(), PAGE_ID)
        .unwrap();

    pool.replace_leaf_and_chains(&mut guard, leaf_page(0xF0), BTreeMap::new())
        .unwrap();
    assert_eq!(reader_pin.data()[PAYLOAD_OFFSET], 0x11);
    assert_eq!(pool.inner_32k.lock().unwrap().pin_count(PAGE_ID), Some(2));

    drop(guard);
    assert_eq!(pool.inner_32k.lock().unwrap().pin_count(PAGE_ID), Some(1));
    drop(reader_pin);
    assert_eq!(pool.inner_32k.lock().unwrap().pin_count(PAGE_ID), Some(0));

    let page = pool.pin(PAGE_ID, PageSize::Large32k).unwrap();
    assert_eq!(page.data()[PAYLOAD_OFFSET], 0xF0);
}

#[test]
fn not_leaf_drops_guard_without_replacing_frame() {
    let pool = pool_with_leaf(PAGE_ID);
    let mut guard = pool
        .pin_leaf_for_reconcile(primary_ident(), PAGE_ID)
        .unwrap();

    let error = pool
        .replace_leaf_and_chains(&mut guard, non_leaf_page(), BTreeMap::new())
        .unwrap_err();

    assert!(matches!(error, ReplaceLeafError::NotLeaf));
    drop(guard);
    assert_eq!(pool.inner_32k.lock().unwrap().pin_count(PAGE_ID), Some(0));

    let page = pool.pin(PAGE_ID, PageSize::Large32k).unwrap();
    assert_eq!(page.data()[PAYLOAD_OFFSET], 0x11);
}

#[test]
fn pin_leaf_for_reconcile_reports_non_resident_pages() {
    let pool = pool_with_leaf(PAGE_ID);

    let error = match pool.pin_leaf_for_reconcile(primary_ident(), PAGE_ID + 1) {
        Ok(_) => panic!("expected NotResident"),
        Err(error) => error,
    };

    assert!(matches!(error, ReplaceLeafError::NotResident));
}
