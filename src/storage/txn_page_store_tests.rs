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
fn rollback_header_change_preserves_later_header_update() {
    let handle = handle_with_header(base_header());
    let mut overlay = TxnOverlay::new();

    handle
        .allocator()
        .update_header(|header| {
            let before = header.clone();
            header.catalog_root_page = 2;
            header.catalog_root_backup = 2;
            header.catalog_root_level = 1;
            header.next_namespace_id = 11;
            header.next_index_id = 21;
            let after = header.clone();
            overlay.capture_header_change_once(&before, &after);
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

    overlay.rollback(&handle).unwrap();

    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.catalog_root_page, 3);
    assert_eq!(header.catalog_root_backup, 3);
    assert_eq!(header.catalog_root_level, 2);
    assert_eq!(header.next_namespace_id, 100);
    assert_eq!(header.next_index_id, 200);
    assert_eq!(header.total_page_count, 99);
}

#[test]
fn rollback_header_change_restores_catalog_root_without_regressing_ids() {
    let handle = handle_with_header(base_header());
    let mut overlay = TxnOverlay::new();

    handle
        .allocator()
        .update_header(|header| {
            let before = header.clone();
            header.catalog_root_page = 2;
            header.catalog_root_backup = 2;
            header.catalog_root_level = 1;
            header.next_namespace_id = 11;
            header.next_index_id = 21;
            let after = header.clone();
            overlay.capture_header_change_once(&before, &after);
        })
        .unwrap();

    overlay.rollback(&handle).unwrap();

    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.catalog_root_page, 1);
    assert_eq!(header.catalog_root_backup, 1);
    assert_eq!(header.catalog_root_level, 0);
    assert_eq!(header.next_namespace_id, 11);
    assert_eq!(header.next_index_id, 21);
}

#[test]
fn rollback_with_header_change_returns_new_allocations_to_free_list() {
    let handle = handle_with_header(base_header());
    let mut overlay = TxnOverlay::new();
    overlay.push_reservation(PageReservation {
        page: 2,
        size: PageSize::Large32k,
        origin: PageOrigin::NewAlloc,
    });

    handle
        .allocator()
        .update_header(|header| {
            let before = header.clone();
            header.catalog_root_page = 2;
            header.catalog_root_backup = 2;
            header.catalog_root_level = 1;
            let after = header.clone();
            overlay.capture_header_change_once(&before, &after);
        })
        .unwrap();

    overlay.rollback(&handle).unwrap();

    let header = handle.allocator().with_header(Clone::clone).unwrap();
    assert_eq!(header.catalog_root_page, 1);
    assert_eq!(header.free_list_head_32k, 2);
    assert_eq!(header.free_page_count_32k, 1);
}
