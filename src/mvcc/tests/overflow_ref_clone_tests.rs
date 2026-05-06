//! US-013 `OverflowRef::try_clone` regression tests.

use super::*;

use crate::storage::header::FileHeader;

const FIRST_PAGE: u32 = 77;
const TOTAL_LENGTH: u64 = 4096;

fn fresh_allocator() -> AllocatorHandle {
    AllocatorHandle::new(FileHeader::new(0, 0, 0))
}

#[test]
fn test_overflow_ref_try_clone_refcount() {
    let alloc = fresh_allocator();
    let live = OverflowRef::new_owned(FIRST_PAGE, TOTAL_LENGTH, alloc.clone()).unwrap();

    let history_side = live
        .try_clone()
        .expect("live overflow ref must be cloneable for history transfer");

    assert_eq!(alloc.overflow_refcount(FIRST_PAGE), 2);
    assert_eq!(history_side.first_page(), FIRST_PAGE);
    assert_eq!(history_side.total_length(), TOTAL_LENGTH);

    drop(live);
    assert_eq!(
        alloc.overflow_refcount(FIRST_PAGE),
        1,
        "history-side ref must keep the overflow chain live"
    );
    assert_eq!(alloc.page_lifetime_queue().depth(), 0);

    drop(history_side);
    assert_eq!(alloc.overflow_refcount(FIRST_PAGE), 0);
    assert_eq!(alloc.page_lifetime_queue().depth(), 1);
}

#[test]
fn try_clone_returns_none_for_dropped_record_without_resurrecting() {
    let alloc = fresh_allocator();
    let stale = OverflowRef::new_owned(FIRST_PAGE, TOTAL_LENGTH, alloc.clone()).unwrap();
    alloc.set_overflow_refcount_for_test(FIRST_PAGE, 0);

    assert!(
        stale.try_clone().is_none(),
        "try_clone must not resurrect a zero-refcount overflow record"
    );
    assert_eq!(alloc.overflow_refcount(FIRST_PAGE), 0);
    assert_eq!(alloc.page_lifetime_queue().depth(), 0);

    std::mem::forget(stale);
}
