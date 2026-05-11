use std::collections::VecDeque;
use std::sync::Arc;

use super::*;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::btree::{BTreePageStore, LeafPageImage};
use crate::storage::buffer_pool::{default_sizes, BufferPool, LatchMode, PageSize};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

fn make_store() -> BufferPoolPageStore {
    let io = MockIo::new();
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
    BufferPoolPageStore::new(handle)
}

fn version(payload: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: Ts {
            physical_ms: 35,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id: 35,
        state: VersionState::Committed,
        data: VersionData::Inline(payload.to_vec()),
        is_tombstone: false,
    }
}

fn chain(payload: &[u8]) -> Arc<VecDeque<VersionEntry>> {
    Arc::new(VecDeque::from([version(payload)]))
}

#[test]
fn buffer_pool_read_leaf_returns_shared_frame_snapshot() {
    let mut store = make_store();
    let page = store.alloc_leaf().expect("allocate leaf page");
    let mut data = [0u8; LEAF_SIZE];
    data[0] = 0x35;
    data[LEAF_SIZE - 1] = 0x53;
    store
        .write_leaf_structural(page, &data)
        .expect("write leaf page");

    let (first, _) = store.read_leaf(page).expect("first shared read");
    let (second, _) = store.read_leaf(page).expect("second shared read");

    let (LeafPageImage::Shared(first_arc), LeafPageImage::Shared(second_arc)) = (&first, &second)
    else {
        panic!("buffer-pool leaf reads must use shared page images");
    };
    assert!(Arc::ptr_eq(first_arc, second_arc));
    assert_eq!(first[0], 0x35);
    assert_eq!(second[LEAF_SIZE - 1], 0x53);
}

#[test]
fn guarded_buffer_pool_read_leaf_returns_shared_frame_snapshot() {
    let mut store = make_store();
    let page = store.alloc_leaf().expect("allocate leaf page");
    let mut data = [0u8; LEAF_SIZE];
    data[0] = 0x17;
    store
        .write_leaf_structural(page, &data)
        .expect("write leaf page");

    let guard = store
        .pin_shared_for_read(page, PageSize::Large32k)
        .expect("pin page for shared read");
    let (image, _) = store
        .read_leaf_guarded(page, &guard)
        .expect("guarded leaf read");

    assert!(matches!(image, LeafPageImage::Shared(_)));
    assert_eq!(image[0], 0x17);
}

#[test]
fn point_leaf_read_snapshots_only_requested_chain() {
    let mut store = make_store();
    let page = store.alloc_leaf().expect("allocate leaf page");
    let data = [0u8; LEAF_SIZE];
    store
        .write_leaf_structural(page, &data)
        .expect("write leaf page");
    store
        .with_chain_under_latch(page, b"target", LatchMode::Exclusive, |slot| {
            *slot = Some(chain(b"target"));
        })
        .expect("install target chain");
    store
        .with_chain_under_latch(page, b"other", LatchMode::Exclusive, |slot| {
            *slot = Some(chain(b"other"));
        })
        .expect("install other chain");

    let (_, full_snapshot) = store.read_leaf(page).expect("full leaf read");
    assert_eq!(full_snapshot.expect("full snapshot").key_count(), 2);

    let guard = store
        .pin_shared_for_read(page, PageSize::Large32k)
        .expect("pin page for point leaf read");
    let (_, point_snapshot) = store
        .read_leaf_for_key_guarded(page, &guard, b"target")
        .expect("point leaf read");
    let point_snapshot = point_snapshot.expect("point snapshot");
    assert_eq!(point_snapshot.key_count(), 1);
    assert_eq!(point_snapshot.chain_len(b"target"), 1);
    assert_eq!(point_snapshot.chain_len(b"other"), 0);
}
