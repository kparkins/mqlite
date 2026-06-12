//! R1 regression guards: frees issued inside a structural batch must not
//! destroy pages the durable base image still references.
//!
//! `AllocatorHandle::free_*` destroys the page in place (first 4 bytes become
//! the free-list link, the rest is zeroed, the frame is dirtied). The staged
//! leaf that DE-references the freed page only lands at
//! `commit_lsn_fenced`, so a straight-through free inside the batch:
//!
//! 1. corrupts the base image if the batch ABORTS (the zeroed page is
//!    flushable while the durable leaf still points at it),
//! 2. opens a WAL-before-data crash window even on the success path, and
//! 3. lets the allocator hand the page out again mid-batch.
//!
//! The fix defers frees of pages NOT allocated within the batch to
//! `AllocatorLifetimeBatch::commit_lsn_fenced` and drops them on abort.
//! Pages allocated within the batch stay immediately freeable — they are
//! invisible outside the batch.
//!
//! Also guards R1b: `AllocatorLifetimeBatch::abort` must re-enqueue the
//! lifetime-queue pages it drained at batch creation even when freeing a
//! batch-allocated page fails partway through.

use super::*;
use crate::storage::btree::{BTree, OVERFLOW_THRESHOLD};
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::test_support::{ArcIo, MockIo};

/// Spans two overflow pages so the chain walk exercises the link traversal.
const OLD_VALUE_LEN: usize = OVERFLOW_THRESHOLD * 2;
const NEW_VALUE_LEN: usize = OVERFLOW_THRESHOLD * 2;

fn make_handle() -> Arc<BufferPoolHandle> {
    let io = MockIo::new();
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(default_sizes::IOT, Box::new(ArcIo(io))));
    Arc::new(BufferPoolHandle::new(
        pool,
        history_pool,
        FileHeader::new_now(),
    ))
}

/// Build a single-leaf base tree holding `k` -> `old_value` (overflow), and
/// return `(root_page, root_level, old_overflow_chain_pages)`.
fn build_base_tree(handle: &Arc<BufferPoolHandle>, old_value: &[u8]) -> (u32, u8, Vec<u32>) {
    let mut tree = BTree::create(BufferPoolPageStore::new(Arc::clone(handle))).unwrap();
    tree.insert(b"k", old_value).unwrap();
    let root_page = tree.root_page;
    let root_level = tree.root_level;
    let chain: Vec<u32> = tree
        .collect_pages_by_size()
        .unwrap()
        .into_iter()
        .filter(|(page, size)| *size == PageSize::Large32k && *page != root_page)
        .map(|(page, _)| page)
        .collect();
    assert!(
        chain.len() >= 2,
        "test setup must produce a multi-page overflow chain, got {chain:?}"
    );
    let base = BufferPoolPageStore::new(Arc::clone(handle));
    for page in &chain {
        let (image, _) = base.read_leaf(*page).unwrap();
        assert_eq!(
            image.as_slice()[0],
            PAGE_TYPE_OVERFLOW,
            "test setup: page {page} is not an overflow page"
        );
    }
    (root_page, root_level, chain)
}

fn snapshot_pages(handle: &Arc<BufferPoolHandle>, pages: &[u32]) -> Vec<Vec<u8>> {
    let base = BufferPoolPageStore::new(Arc::clone(handle));
    pages
        .iter()
        .map(|page| {
            let (image, _) = base.read_leaf(*page).unwrap();
            image.as_slice().to_vec()
        })
        .collect()
}

/// R1 core repro: a fold-style `replace_existing` through the batch store
/// frees the OLD overflow chain. Aborting the batch must leave those pages
/// intact (readable, not zeroed or link-stamped, off the free list) because
/// the durable base leaf still references them.
#[test]
fn structural_batch_abort_preserves_preexisting_overflow_chain_freed_in_batch() {
    let handle = make_handle();
    let old_value = vec![0xA5u8; OLD_VALUE_LEN];
    let (root_page, root_level, old_chain) = build_base_tree(&handle, &old_value);
    let old_bytes = snapshot_pages(&handle, &old_chain);

    let mut batch = StructuralPageBatch::new(&handle);
    {
        let store = batch.store(BufferPoolPageStore::new(Arc::clone(&handle)));
        let mut tree = BTree::open(store, root_page, root_level);
        let new_value = vec![0x5Au8; NEW_VALUE_LEN];
        assert!(tree.replace_existing(b"k", &new_value).unwrap());
    }
    batch.abort(&handle).unwrap();

    // The old chain pages must be byte-identical to their pre-batch state:
    // a straight-through free stamps the free-list link over the overflow
    // header and zeroes the payload.
    let base = BufferPoolPageStore::new(Arc::clone(&handle));
    for (page, before) in old_chain.iter().zip(&old_bytes) {
        let (image, _) = base.read_leaf(*page).unwrap();
        assert_eq!(
            image.as_slice()[0],
            PAGE_TYPE_OVERFLOW,
            "aborted structural batch destroyed old overflow page {page}"
        );
        assert_eq!(
            image.as_slice(),
            before.as_slice(),
            "aborted structural batch mutated old overflow page {page}"
        );
    }

    // `k` must still resolve through the old base image.
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&handle)),
        root_page,
        root_level,
    );
    assert_eq!(
        tree.get(b"k").unwrap().as_deref(),
        Some(old_value.as_slice()),
        "old key must resolve through the untouched base image after abort"
    );

    // None of the old chain pages may be on the free list: drain it through
    // the allocator and check every recycled page number.
    let free_count = handle
        .allocator()
        .with_header(|header| header.free_page_count_32k)
        .unwrap();
    let mut store = BufferPoolPageStore::new(Arc::clone(&handle));
    for _ in 0..free_count {
        let recycled = store.alloc_leaf().unwrap();
        assert!(
            !old_chain.contains(&recycled),
            "old overflow page {recycled} was on the free list after abort"
        );
    }
}

/// Success path: commit must still apply the deferred frees so the old
/// chain is reclaimed exactly once (no leak), after the staged leaf bytes
/// that dereference it have landed.
#[test]
fn structural_batch_commit_frees_preexisting_overflow_chain_freed_in_batch() {
    let handle = make_handle();
    let old_value = vec![0xA5u8; OLD_VALUE_LEN];
    let (root_page, root_level, old_chain) = build_base_tree(&handle, &old_value);
    let new_value = vec![0x5Au8; NEW_VALUE_LEN];

    let mut batch = StructuralPageBatch::new(&handle);
    {
        let store = batch.store(BufferPoolPageStore::new(Arc::clone(&handle)));
        let mut tree = BTree::open(store, root_page, root_level);
        assert!(tree.replace_existing(b"k", &new_value).unwrap());
    }

    // The old chain must stay intact while the batch is still open.
    let pre_commit_free = handle
        .allocator()
        .with_header(|header| header.free_page_count_32k)
        .unwrap();
    assert_eq!(
        pre_commit_free, 0,
        "in-batch free of a pre-existing page must not hit the live allocator"
    );

    let mut base = BufferPoolPageStore::new(Arc::clone(&handle));
    batch.commit_lsn_fenced(&mut base, &handle, 1).unwrap();

    // The new value is visible through the committed leaf.
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&handle)),
        root_page,
        root_level,
    );
    assert_eq!(
        tree.get(b"k").unwrap().as_deref(),
        Some(new_value.as_slice())
    );

    // The old chain pages are the only frees: reclaimed exactly once.
    let free_count = handle
        .allocator()
        .with_header(|header| header.free_page_count_32k)
        .unwrap();
    assert_eq!(
        free_count as usize,
        old_chain.len(),
        "commit must free exactly the replaced overflow chain"
    );
    let mut store = BufferPoolPageStore::new(Arc::clone(&handle));
    let mut recycled: Vec<u32> = (0..free_count)
        .map(|_| store.alloc_leaf().unwrap())
        .collect();
    recycled.sort_unstable();
    let mut expected = old_chain;
    expected.sort_unstable();
    assert_eq!(recycled, expected);
}

/// 4 KiB twin of the deferral contract: freeing a pre-existing internal page
/// through the batch store must not touch the live allocator until commit.
#[test]
fn structural_batch_defers_preexisting_internal_free_until_commit() {
    let handle = make_handle();
    let page = {
        let mut store = BufferPoolPageStore::new(Arc::clone(&handle));
        store.alloc_internal().unwrap()
    };

    let mut batch = StructuralPageBatch::new(&handle);
    {
        let mut store = batch.store(BufferPoolPageStore::new(Arc::clone(&handle)));
        store.free_internal(page).unwrap();
    }
    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.free_page_count_4k, 0);
    assert_eq!(header.free_list_head_4k, 0);

    let mut base = BufferPoolPageStore::new(Arc::clone(&handle));
    batch.commit_lsn_fenced(&mut base, &handle, 1).unwrap();

    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.free_page_count_4k, 1);
    assert_eq!(header.free_list_head_4k, page);
}

/// Abort drops the deferred free entirely: the pre-existing internal page
/// stays allocated because the durable base image still owns it.
#[test]
fn structural_batch_abort_drops_deferred_preexisting_internal_free() {
    let handle = make_handle();
    let page = {
        let mut store = BufferPoolPageStore::new(Arc::clone(&handle));
        store.alloc_internal().unwrap()
    };

    let mut batch = StructuralPageBatch::new(&handle);
    {
        let mut store = batch.store(BufferPoolPageStore::new(Arc::clone(&handle)));
        store.free_internal(page).unwrap();
    }
    batch.abort(&handle).unwrap();

    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.free_page_count_4k, 0);
    assert_eq!(header.free_list_head_4k, 0);
}

/// R1b: abort must re-enqueue the lifetime-queue pages drained at batch
/// creation even when freeing a batch-allocated page errors. Page 0 is the
/// header page, so `free_page(0, _)` always fails — pre-fix the `?` inside
/// the new-allocs loop skipped the re-enqueue and the deferred page leaked
/// out of the lifetime queue forever.
#[test]
fn lifetime_abort_requeues_deferred_pages_despite_new_alloc_free_error() {
    let handle = make_handle();
    handle.allocator().enqueue_overflow_deferred_free(7);
    handle.allocator().advance_page_lifetime_checkpoint_fence();

    let mut lifetime = AllocatorLifetimeBatch::new(&handle);
    assert_eq!(handle.allocator().page_lifetime_queue().depth(), 0);
    lifetime.record_new_alloc(0, PageSize::Small4k);

    assert!(
        lifetime.abort(&handle).is_err(),
        "abort must still surface the free error"
    );
    assert_eq!(
        handle.allocator().page_lifetime_queue().depth(),
        1,
        "deferred lifetime page must be re-enqueued even on abort error"
    );
}
