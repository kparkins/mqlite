use super::*;

#[test]
fn new_index_is_empty() {
    let idx = JournalIndex::new();
    assert_eq!(idx.occupied_count(), 0);
    assert!(idx.lookup(42).is_none());
}

#[test]
fn insert_and_lookup() {
    let mut idx = JournalIndex::new();
    let hot = idx.insert(10, 1024);
    assert!(!hot);
    assert_eq!(idx.lookup(10), Some(1024));
    assert!(idx.lookup(99).is_none());
}

#[test]
fn update_existing_entry() {
    let mut idx = JournalIndex::new();
    idx.insert(5, 100);
    idx.insert(5, 200);
    assert_eq!(idx.lookup(5), Some(200));
    assert_eq!(idx.occupied_count(), 1);
}

#[test]
fn clear_index_resets_everything() {
    let mut idx = JournalIndex::new();
    idx.insert(1, 10);
    idx.insert(2, 20);
    idx.insert(3, 30);
    idx.clear_index();
    assert_eq!(idx.occupied_count(), 0);
    assert!(idx.lookup(1).is_none());
    assert!(idx.lookup(2).is_none());
}

#[test]
fn multiple_inserts_all_recoverable() {
    let mut idx = JournalIndex::new();
    for i in 0..100u32 {
        idx.insert(i, (i as u64) * 4096);
    }
    for i in 0..100u32 {
        assert_eq!(idx.lookup(i), Some((i as u64) * 4096));
    }
}

#[test]
fn iter_entries_covers_all_inserted() {
    let mut idx = JournalIndex::new();
    for i in 0..10u32 {
        idx.insert(i, (i as u64) * 1000);
    }
    let mut found: Vec<u32> = idx.iter_entries().map(|(pn, _)| pn).collect();
    found.sort();
    let expected: Vec<u32> = (0..10).collect();
    assert_eq!(found, expected);
}

#[test]
fn hot_threshold_constant() {
    assert_eq!(JOURNAL_INDEX_HOT_THRESHOLD, 3072);
}

#[test]
fn insert_signals_when_hot_threshold_reached() {
    let mut idx = JournalIndex::new();
    for i in 0..(JOURNAL_INDEX_HOT_THRESHOLD as u32 - 1) {
        assert!(!idx.insert(i, i as u64));
    }
    assert!(
        idx.insert(JOURNAL_INDEX_HOT_THRESHOLD as u32 - 1, 0),
        "insert that crosses the threshold must signal hot"
    );
}
