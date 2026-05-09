use std::sync::Arc;

use crate::storage::btree::{BTree, BTreePageStore};
use crate::storage::btree_store::{BufferPoolPageStore, INTERNAL_SIZE, LEAF_SIZE};
use crate::storage::buffer_pool::default_sizes;
use crate::storage::buffer_pool::BufferPool;
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

// -----------------------------------------------------------------------
// alloc_internal / alloc_leaf
// -----------------------------------------------------------------------

#[test]
fn alloc_internal_returns_first_free_page() {
    let mut store = make_store();
    let pn = store.alloc_internal().unwrap();
    assert_eq!(pn, 1, "first internal page must be 1");
}

#[test]
fn alloc_leaf_returns_first_free_page() {
    let mut store = make_store();
    let pn = store.alloc_leaf().unwrap();
    assert_eq!(pn, 1, "first leaf page must be 1");
}

#[test]
fn sequential_allocs_return_consecutive_pages() {
    let mut store = make_store();
    let a = store.alloc_internal().unwrap();
    let b = store.alloc_leaf().unwrap();
    let c = store.alloc_internal().unwrap();

    assert_eq!(a, 1);
    assert_eq!(b, 2);
    assert_eq!(c, 3);
}

// -----------------------------------------------------------------------
// write / read roundtrip
// -----------------------------------------------------------------------

#[test]
fn write_and_read_internal_roundtrip() {
    let mut store = make_store();
    let pn = store.alloc_internal().unwrap();

    let mut data = [0u8; INTERNAL_SIZE];
    data[0] = 0xAA;
    data[4090] = 0xBB;
    store.write_internal(pn, &data).unwrap();

    let read_back = store.read_internal(pn).unwrap();
    assert_eq!(read_back[0], 0xAA);
    assert_eq!(read_back[4090], 0xBB);
}

#[test]
fn write_and_read_leaf_roundtrip() {
    let mut store = make_store();
    let pn = store.alloc_leaf().unwrap();

    let mut data = [0u8; LEAF_SIZE];
    data[0] = 0xCC;
    data[32760] = 0xDD;
    store.write_leaf_structural(pn, &data).unwrap();

    let (read_back, _) = store.read_leaf(pn).unwrap();
    assert_eq!(read_back[0], 0xCC);
    assert_eq!(read_back[32760], 0xDD);
}

// -----------------------------------------------------------------------
// free / realloc
// -----------------------------------------------------------------------

#[test]
fn free_internal_recycles_on_next_alloc() {
    let mut store = make_store();
    let pn = store.alloc_internal().unwrap();
    store.free_internal(pn).unwrap();
    let recycled = store.alloc_internal().unwrap();
    assert_eq!(recycled, pn, "freed internal page must be recycled");
}

#[test]
fn free_leaf_recycles_on_next_alloc() {
    let mut store = make_store();
    let pn = store.alloc_leaf().unwrap();
    store.free_leaf(pn).unwrap();
    let recycled = store.alloc_leaf().unwrap();
    assert_eq!(recycled, pn, "freed leaf page must be recycled");
}

// -----------------------------------------------------------------------
// B+ tree smoke test through BufferPoolPageStore
// -----------------------------------------------------------------------

#[test]
fn btree_insert_and_get_via_pool_store() {
    let store = make_store();
    let mut tree = BTree::create(store).unwrap();

    let key = b"hello";
    let val = b"world!";

    tree.insert(key, val).unwrap();

    let result = tree.get(key).unwrap();
    assert_eq!(result.as_deref(), Some(val.as_ref()));
}

#[test]
fn btree_insert_multiple_keys_and_get_all() {
    let store = make_store();
    let mut tree = BTree::create(store).unwrap();

    for i in 0u8..50 {
        let key = [i];
        let val = [i, i + 1];
        tree.insert(&key, &val).unwrap();
    }

    for i in 0u8..50 {
        let key = [i];
        let expected = [i, i + 1];
        let result = tree.get(&key).unwrap();
        assert_eq!(
            result.as_deref(),
            Some(expected.as_ref()),
            "key {i} not found"
        );
    }
}
