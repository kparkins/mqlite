//! Segment semantics of [`PageLifetimeQueue`] (F8 / F39).
//!
//! `RetiredTree*` entries live in their own segment: hot drains
//! (`take_eligible`) never scan or release them and never evaluate the
//! reader floor; only the checkpoint drain (`take_eligible_retired`)
//! releases them, gated on the fence AND the reader low-water.

use super::*;

fn ts(ms: u64) -> Ts {
    Ts {
        physical_ms: ms,
        logical: 0,
    }
}

#[test]
fn take_eligible_never_yields_retired_entries() {
    let q = PageLifetimeQueue::new();
    q.push_retired_tree(PageLifetimeKind::RetiredTree4k, 7, 1, ts(10));
    q.push_retired_tree(PageLifetimeKind::RetiredTree32k, 8, 1, ts(10));
    q.push_overflow_deferred_free(9, 1);
    assert_eq!(q.depth(), 3);

    let drained: Vec<u32> = q
        .take_eligible(u64::MAX)
        .into_iter()
        .map(PageLifetimeEntry::page)
        .collect();
    assert_eq!(drained, vec![9], "hot drain must release overflow entries only");
    assert_eq!(q.depth(), 2, "retired entries must stay queued for the checkpoint drain");
}

#[test]
fn take_eligible_retired_gates_on_fence_and_reader_floor() {
    let q = PageLifetimeQueue::new();
    q.push_retired_tree(PageLifetimeKind::RetiredTree32k, 5, 1, ts(10));

    // Fence has not passed the enqueue fence.
    let drained = q.take_eligible_retired(1, || Some(Ts::MAX));
    assert!(drained.is_empty(), "fence gate must hold");

    // Reader floor predates the drop's reader fence.
    let drained = q.take_eligible_retired(2, || Some(ts(9)));
    assert!(drained.is_empty(), "reader low-water gate must hold");

    // No reader-floor provider: conservatively keep the entry queued.
    let drained = q.take_eligible_retired(2, || None);
    assert!(drained.is_empty(), "missing provider must keep retired entries queued");
    assert_eq!(q.depth(), 1);

    // Both gates pass: released.
    let drained: Vec<u32> = q
        .take_eligible_retired(2, || Some(ts(10)))
        .into_iter()
        .map(PageLifetimeEntry::page)
        .collect();
    assert_eq!(drained, vec![5]);
    assert_eq!(q.depth(), 0);
}

#[test]
fn take_eligible_retired_ignores_overflow_entries() {
    let q = PageLifetimeQueue::new();
    q.push_overflow_deferred_free(3, 1);
    let drained = q.take_eligible_retired(u64::MAX, || Some(Ts::MAX));
    assert!(drained.is_empty());
    assert_eq!(q.depth(), 1, "overflow entry must stay for the hot drain");
}

#[test]
fn push_entry_routes_requeued_retired_entries_back_to_their_segment() {
    let q = PageLifetimeQueue::new();
    q.push_retired_tree(PageLifetimeKind::RetiredTree4k, 4, 1, ts(1));
    let entries = q.take_eligible_retired(2, || Some(Ts::MAX));
    assert_eq!(entries.len(), 1);

    q.push_entry(entries[0]);
    let hot = q.take_eligible(u64::MAX);
    assert!(
        hot.is_empty(),
        "a requeued retired entry must not surface on the hot drain"
    );
    let retired = q.take_eligible_retired(2, || Some(Ts::MAX));
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].page(), 4);
}

#[test]
fn reader_floor_not_evaluated_when_no_retired_entry_is_fence_eligible() {
    let q = PageLifetimeQueue::new();
    q.push_retired_tree(PageLifetimeKind::RetiredTree32k, 6, 5, ts(1));
    let drained = q.take_eligible_retired(5, || -> Option<Ts> {
        panic!("reader floor must not be evaluated when no entry is fence-eligible")
    });
    assert!(drained.is_empty());
    assert_eq!(q.depth(), 1);
}
