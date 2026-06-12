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

/// Pinning test for the canonical [`VersionEntry::is_live_head`]
/// predicate that the live-head call sites
/// (`partition::has_live_delta_entry`, `chains::chain_live_head_bytes`,
/// `LatchedPinnedPage::live_head`,
/// `LatchedPinnedPage::has_live_delta_key_in_range`, and the
/// `paged_engine::pending_install` head lookups) were consolidated onto.
///
/// It enumerates every `VersionState × stop_ts` combination and asserts
/// `is_live_head` returns the expected verdict, then asserts the exact
/// boolean expression the four sites previously open-coded
/// (`stop_ts == Ts::MAX && !Aborted`) agrees with the helper for every
/// case. If any wrapper ever drifts from the canonical definition, this
/// test (plus the `running_sum` / `eviction_bug_suspects` regressions)
/// will catch the divergence.
#[test]
fn is_live_head_matches_open_coded_predicate_across_all_states() {
    fn entry(state: VersionState, stop_ts: Ts) -> VersionEntry {
        VersionEntry {
            start_ts: Ts {
                physical_ms: 1,
                logical: 0,
            },
            stop_ts,
            txn_id: 7,
            state,
            data: VersionData::Inline(vec![1, 2, 3]),
            is_tombstone: false,
        }
    }

    let superseded = Ts {
        physical_ms: 100,
        logical: 0,
    };
    let states = [
        VersionState::Pending { txn_id: 7 },
        VersionState::Committed,
        VersionState::Aborted,
    ];
    let stops = [Ts::MAX, superseded];

    for state in states {
        for stop_ts in stops {
            let e = entry(state, stop_ts);
            // The canonical helper.
            let got = e.is_live_head();
            // The exact boolean the four call sites used before
            // consolidation — the pinning oracle.
            let open_coded = e.stop_ts == Ts::MAX && !matches!(e.state, VersionState::Aborted);
            assert_eq!(
                got,
                open_coded,
                "is_live_head disagrees with open-coded predicate for \
                 state={:?} stop_ts==MAX? {}",
                e.state,
                e.stop_ts == Ts::MAX
            );

            // Spell out the expected verdict per combination so the
            // pinning intent is explicit, not just self-referential.
            let expected = match (state, stop_ts == Ts::MAX) {
                // Live head iff head-positioned and not aborted.
                (VersionState::Pending { .. }, true) => true,
                (VersionState::Committed, true) => true,
                (VersionState::Aborted, true) => false,
                // Any superseded entry (stop_ts != MAX) is never a live head.
                (_, false) => false,
            };
            assert_eq!(
                got,
                expected,
                "is_live_head verdict wrong for state={:?} stop_ts==MAX? {}",
                e.state,
                e.stop_ts == Ts::MAX
            );
        }
    }
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
