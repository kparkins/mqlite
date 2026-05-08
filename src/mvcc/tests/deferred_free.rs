use super::*;

#[test]
fn new_queue_is_empty() {
    let q = PageLifetimeQueue::new();
    assert_eq!(q.depth(), 0);
    assert!(q.take_eligible(1).is_empty());
}

#[test]
fn push_and_take_eligible_round_trip() {
    let q = PageLifetimeQueue::new();
    q.push_overflow_deferred_free(10, 1);
    q.push_overflow_deferred_free(20, 1);
    q.push_overflow_deferred_free(30, 1);
    assert_eq!(q.depth(), 3);

    assert!(q.take_eligible(1).is_empty());
    assert_eq!(q.depth(), 3);

    let drained: Vec<u32> = q
        .take_eligible(2)
        .into_iter()
        .map(PageLifetimeEntry::page)
        .collect();
    assert_eq!(drained, vec![10, 20, 30]);
    assert_eq!(q.depth(), 0);
}

#[test]
fn push_many_appends() {
    let q = PageLifetimeQueue::new();
    q.push_overflow_deferred_free(1, 1);
    q.push_overflow_deferred_free(2, 1);
    q.push_overflow_deferred_free(3, 1);
    q.push_overflow_deferred_free(4, 1);
    let drained: Vec<u32> = q
        .take_eligible(2)
        .into_iter()
        .map(PageLifetimeEntry::page)
        .collect();
    assert_eq!(drained, vec![1, 2, 3, 4]);
}

#[test]
fn take_eligible_preserves_later_entries() {
    let q = PageLifetimeQueue::new();
    q.push_overflow_deferred_free(42, 1);
    q.push_overflow_deferred_free(99, 2);
    let drained: Vec<u32> = q
        .take_eligible(2)
        .into_iter()
        .map(PageLifetimeEntry::page)
        .collect();
    assert_eq!(drained, vec![42]);
    assert_eq!(q.depth(), 1);

    let drained: Vec<u32> = q
        .take_eligible(3)
        .into_iter()
        .map(PageLifetimeEntry::page)
        .collect();
    assert_eq!(drained, vec![99]);
}
