use super::*;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::test_support::{ArcIo, MockIo};

fn handle_with_header(header: FileHeader) -> BufferPoolHandle {
    let io = MockIo::new();
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(default_sizes::IOT, Box::new(ArcIo(io))));
    BufferPoolHandle::new(pool, history_pool, header)
}

fn base_header() -> FileHeader {
    let mut header = FileHeader::new(1, 2, 3);
    header.total_page_count = 10;
    header.catalog_root_page = 1;
    header.catalog_root_backup = 1;
    header.catalog_root_level = 0;
    header.next_namespace_id = 10;
    header.next_index_id = 20;
    header
}

#[test]
fn header_owner_abort_preserves_later_header_update() {
    let handle = handle_with_header(base_header());
    let mut batch = StructuralPageBatch::new(&handle);

    batch
        .update_header(&handle, |header| {
            header.catalog_root_page = 2;
            header.catalog_root_backup = 2;
            header.catalog_root_level = 1;
            header.next_namespace_id = 11;
            header.next_index_id = 21;
        })
        .unwrap();

    handle
        .allocator()
        .update_header(|header| {
            header.catalog_root_page = 3;
            header.catalog_root_backup = 3;
            header.catalog_root_level = 2;
            header.next_namespace_id = 100;
            header.next_index_id = 200;
            header.total_page_count = 99;
        })
        .unwrap();

    batch.abort(&handle).unwrap();

    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.catalog_root_page, 3);
    assert_eq!(header.catalog_root_backup, 3);
    assert_eq!(header.catalog_root_level, 2);
    assert_eq!(header.next_namespace_id, 100);
    assert_eq!(header.next_index_id, 200);
    assert_eq!(header.total_page_count, 99);
}

#[test]
fn header_owner_abort_restores_catalog_root_without_regressing_ids() {
    let handle = handle_with_header(base_header());
    let mut batch = StructuralPageBatch::new(&handle);

    batch
        .update_header(&handle, |header| {
            header.catalog_root_page = 2;
            header.catalog_root_backup = 2;
            header.catalog_root_level = 1;
            header.next_namespace_id = 11;
            header.next_index_id = 21;
        })
        .unwrap();

    batch.abort(&handle).unwrap();

    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.catalog_root_page, 1);
    assert_eq!(header.catalog_root_backup, 1);
    assert_eq!(header.catalog_root_level, 0);
    assert_eq!(header.next_namespace_id, 11);
    assert_eq!(header.next_index_id, 21);
}

#[test]
fn header_owner_abort_returns_new_allocations_to_free_list() {
    let handle = Arc::new(handle_with_header(base_header()));
    let mut batch = StructuralPageBatch::new(&handle);
    let page = {
        let mut store = batch.store(BufferPoolPageStore::new(Arc::clone(&handle)));
        store.alloc_leaf().unwrap()
    };

    batch
        .update_header(&handle, |header| {
            header.catalog_root_page = 2;
            header.catalog_root_backup = 2;
            header.catalog_root_level = 1;
        })
        .unwrap();

    batch.abort(&handle).unwrap();

    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.catalog_root_page, 1);
    assert_eq!(header.free_list_head_32k, page);
    assert_eq!(header.free_page_count_32k, 1);
}

#[test]
fn structural_batch_abort_does_not_double_free_alloc_then_free_page() {
    let handle = Arc::new(handle_with_header(base_header()));
    let mut batch = StructuralPageBatch::new(&handle);
    let page = {
        let mut store = batch.store(BufferPoolPageStore::new(Arc::clone(&handle)));
        let page = store.alloc_leaf().unwrap();
        store.free_leaf(page).unwrap();
        page
    };

    batch.abort(&handle).unwrap();

    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.free_list_head_32k, page);
    assert_eq!(header.free_page_count_32k, 1);
}

#[test]
fn structural_batch_commit_frees_deferred_lifetime_page() {
    let handle = Arc::new(handle_with_header(base_header()));
    handle.allocator().enqueue_overflow_deferred_free(7);
    handle.allocator().advance_page_lifetime_checkpoint_fence();

    let batch = StructuralPageBatch::new(&handle);
    assert_eq!(handle.allocator().page_lifetime_queue().depth(), 0);

    let mut base = BufferPoolPageStore::new(Arc::clone(&handle));
    batch.commit_lsn_fenced(&mut base, &handle, 1).unwrap();

    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.free_list_head_32k, 7);
    assert_eq!(header.free_page_count_32k, 1);
}

#[test]
fn structural_batch_abort_requeues_deferred_lifetime_page_once() {
    let handle = Arc::new(handle_with_header(base_header()));
    handle.allocator().enqueue_overflow_deferred_free(7);
    handle.allocator().advance_page_lifetime_checkpoint_fence();

    let batch = StructuralPageBatch::new(&handle);
    assert_eq!(handle.allocator().page_lifetime_queue().depth(), 0);

    batch.abort(&handle).unwrap();

    assert_eq!(handle.allocator().page_lifetime_queue().depth(), 1);
    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.free_list_head_32k, 0);
    assert_eq!(header.free_page_count_32k, 0);
}

/// F28: a lifetime-abort error must not skip the catalog-root header
/// rollback. Pre-fix, `StructuralPageBatch::abort` ran
/// `self.lifetime.abort(handle)?;` before `self.header.abort(handle)`, so a
/// failing batch-allocated free (post-R1b those errors are surfaced) left
/// `header.catalog_root_page` pointing at the batch-staged root that abort
/// just tried to tear down — and every caller `let _ = abort(..)`s the error.
#[test]
fn structural_batch_abort_rolls_back_header_despite_lifetime_abort_error() {
    let handle = handle_with_header(base_header());
    let mut batch = StructuralPageBatch::new(&handle);
    batch
        .update_header(&handle, |header| {
            header.catalog_root_page = 9;
            header.catalog_root_backup = 9;
            header.catalog_root_level = 4;
        })
        .unwrap();
    // Page 0 is the live header page, so `free_page(0, _)` always fails:
    // this models the lifetime owner erroring mid-abort (same injection as
    // the R1b guard in `structural_batch_abort_free_safety.rs`).
    batch.lifetime.record_new_alloc(0, PageSize::Small4k);

    assert!(
        batch.abort(&handle).is_err(),
        "abort must still surface the lifetime free error"
    );

    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(
        header.catalog_root_page, 1,
        "catalog root must be rolled back even when the lifetime abort errors"
    );
    assert_eq!(header.catalog_root_backup, 1);
    assert_eq!(header.catalog_root_level, 0);
}

#[test]
fn header_owner_captures_live_allocator_header_as_rollback_baseline() {
    let handle = handle_with_header(base_header());
    handle
        .allocator()
        .update_header(|header| {
            header.catalog_root_page = 4;
            header.catalog_root_backup = 4;
            header.catalog_root_level = 3;
        })
        .unwrap();

    let mut batch = StructuralPageBatch::new(&handle);
    batch
        .update_header(&handle, |header| {
            header.catalog_root_page = 5;
            header.catalog_root_backup = 5;
            header.catalog_root_level = 4;
        })
        .unwrap();

    batch.abort(&handle).unwrap();

    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.catalog_root_page, 4);
    assert_eq!(header.catalog_root_backup, 4);
    assert_eq!(header.catalog_root_level, 3);
}
