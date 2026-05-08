use super::*;
use crate::storage::header::FileHeader;

fn fresh_allocator() -> AllocatorHandle {
    AllocatorHandle::new(FileHeader::new(0, 0, 0))
}

#[test]
fn overflow_ref_new_bumps_refcount_to_one() {
    let alloc = fresh_allocator();
    let r = OverflowRef::new_owned(42, 100, alloc.clone()).unwrap();
    assert_eq!(r.first_page(), 42);
    assert_eq!(r.total_length(), 100);
    assert_eq!(alloc.overflow_refcount(42), 1);
}

#[test]
fn overflow_ref_clone_bumps_refcount() {
    let alloc = fresh_allocator();
    let r = OverflowRef::new_owned(42, 100, alloc.clone()).unwrap();
    assert_eq!(alloc.overflow_refcount(42), 1);

    #[allow(
        clippy::redundant_clone,
        reason = "test asserts Clone bumps the overflow refcount"
    )]
    let r2 = r.clone();
    assert_eq!(alloc.overflow_refcount(42), 2);
    assert_eq!(r2.first_page(), 42);
}

#[test]
fn overflow_ref_drop_decrefs_and_enqueues_on_zero() {
    let alloc = fresh_allocator();
    let r = OverflowRef::new_owned(42, 100, alloc.clone()).unwrap();
    drop(r);
    assert_eq!(alloc.overflow_refcount(42), 0);
    assert_eq!(
        alloc.page_lifetime_queue().depth(),
        1,
        "refcount 0 drop must enqueue for deferred free"
    );
}

#[test]
fn overflow_ref_drop_does_not_enqueue_when_others_live() {
    let alloc = fresh_allocator();
    let r = OverflowRef::new_owned(42, 100, alloc.clone()).unwrap();
    let r2 = r.clone();
    assert_eq!(alloc.overflow_refcount(42), 2);

    drop(r);
    assert_eq!(alloc.overflow_refcount(42), 1);
    assert_eq!(
        alloc.page_lifetime_queue().depth(),
        0,
        "must not enqueue while a live OverflowRef remains"
    );

    drop(r2);
    assert_eq!(alloc.overflow_refcount(42), 0);
    assert_eq!(alloc.page_lifetime_queue().depth(), 1);
}

#[test]
fn version_data_clone_preserves_refcount_invariant() {
    let alloc = fresh_allocator();
    let r = OverflowRef::new_owned(7, 32, alloc.clone()).unwrap();
    let vd = VersionData::Overflow(r);
    assert_eq!(alloc.overflow_refcount(7), 1);

    let vd2 = vd.clone();
    assert_eq!(alloc.overflow_refcount(7), 2);

    drop(vd);
    assert_eq!(alloc.overflow_refcount(7), 1);
    drop(vd2);
    assert_eq!(alloc.overflow_refcount(7), 0);
}

#[test]
fn version_entry_clone_works() {
    let alloc = fresh_allocator();
    let r = OverflowRef::new_owned(100, 1024, alloc.clone()).unwrap();
    let entry = VersionEntry {
        start_ts: Ts {
            physical_ms: 10,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id: 1,
        state: VersionState::Committed,
        data: VersionData::Overflow(r),
        is_tombstone: false,
    };
    assert_eq!(alloc.overflow_refcount(100), 1);

    #[allow(
        clippy::redundant_clone,
        reason = "test asserts VersionEntry::clone bumps the overflow refcount"
    )]
    let clone = entry.clone();
    assert_eq!(alloc.overflow_refcount(100), 2);
    assert_eq!(clone.txn_id, 1);
}
