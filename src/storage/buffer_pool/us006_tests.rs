#![allow(clippy::panic, clippy::unwrap_used)]

use super::*;

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};

use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::page::PAGE_TYPE_LEAF;
use crate::storage::reconcile::plan::{TreeIdent, TreeKind};

const COLLECTION_ID: i64 = 42;
const PAGE_ID: u32 = 65;
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
fn typed_constructor_holds_pin_until_guard_drop() {
    let pool = pool_with_leaf(PAGE_ID);

    let guard = pool
        .pin_leaf_for_reconcile(primary_ident(), PAGE_ID)
        .unwrap();

    assert_eq!(guard.page_id(), PAGE_ID);
    assert_eq!(pool.inner_32k.lock().unwrap().pin_count(PAGE_ID), Some(1));

    drop(guard);

    assert_eq!(pool.inner_32k.lock().unwrap().pin_count(PAGE_ID), Some(0));
}

#[test]
fn replace_leaf_accepts_typed_guard_and_releases_pin_on_success() {
    let pool = pool_with_leaf(PAGE_ID);
    let mut retained = BTreeMap::new();
    retained.insert(RETAINED_KEY.to_vec(), chain(b"retained"));

    let mut guard = pool
        .pin_leaf_for_reconcile(primary_ident(), PAGE_ID)
        .unwrap();

    pool.replace_leaf_and_chains(&mut guard, leaf_page(0xA5), retained)
        .unwrap();
    drop(guard);

    assert_eq!(pool.inner_32k.lock().unwrap().pin_count(PAGE_ID), Some(0));

    let page = pool.pin(PAGE_ID, PageSize::Large32k).unwrap();
    assert_eq!(page.data()[PAYLOAD_OFFSET], 0xA5);
}

#[test]
fn not_resident_is_reported_before_a_guard_exists() {
    let pool = pool_with_leaf(PAGE_ID);

    let error = match pool.pin_leaf_for_reconcile(primary_ident(), PAGE_ID + 1) {
        Ok(_) => panic!("expected NotResident"),
        Err(error) => error,
    };

    assert!(matches!(error, ReplaceLeafError::NotResident));
    assert_eq!(pool.inner_32k.lock().unwrap().pin_count(PAGE_ID), Some(0));
}
