//! In-memory journal index.
//!
//! Maps `page_number -> latest journal frame byte offset` so reads through
//! [`crate::journal::JournalLayeredSource`] can locate the freshest journaled
//! version of a page in O(1) without scanning the journal.
//!
//! ## Design
//!
//! The index is **volatile**. It lives only in memory and is rebuilt from the
//! journal on every open via [`crate::journal::JournalManager::open_or_create`].
//! There is no companion on-disk file. This matches the WiredTiger model used
//! by MongoDB: the journal alone is the durable record, and the in-memory
//! index exists purely as a lookup accelerator.
//!
//! ## Hot-threshold trigger
//!
//! When the index reaches [`JOURNAL_INDEX_HOT_THRESHOLD`] live entries,
//! [`JournalIndex::insert`] returns `true` to signal that an emergency
//! checkpoint should drain the journal before it grows further. The threshold
//! value is preserved verbatim from the previous hash-table-based design so
//! checkpoint cadence is unchanged.
//!
//! Recovery, rollback, and checkpoint correctness depend on the index
//! reflecting only durable, committed frames — see the call sites in
//! [`crate::journal::JournalManager`] for the maintenance contract.

use std::collections::HashMap;

/// Journal index hot-threshold: when the index holds at least this many live
/// entries, [`JournalIndex::insert`] signals that the journal should be
/// drained by an emergency checkpoint.
pub(crate) const JOURNAL_INDEX_HOT_THRESHOLD: usize = 3072;

// ---------------------------------------------------------------------------
// JournalIndex
// ---------------------------------------------------------------------------

/// In-memory `page_number -> journal frame offset` index.
///
/// The index is rebuilt by [`crate::journal::JournalManager`] on open and
/// maintained as frames are appended, committed, rolled back, or checkpointed.
pub(crate) struct JournalIndex {
    map: HashMap<u32, u64>,
}

impl JournalIndex {
    /// Create a new, empty index.
    pub(crate) fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Look up `page_number` in the index.
    ///
    /// Returns `Some(journal_frame_offset)` when the page has a frame in the
    /// journal, or `None` if the page should be served from the main file.
    pub(crate) fn lookup(&self, page_number: u32) -> Option<u64> {
        self.map.get(&page_number).copied()
    }

    /// Insert or update the journal frame offset for `page_number`.
    ///
    /// Returns `true` when the index has crossed
    /// [`JOURNAL_INDEX_HOT_THRESHOLD`] and an emergency checkpoint should be
    /// triggered.
    pub(crate) fn insert(&mut self, page_number: u32, journal_offset: u64) -> bool {
        self.map.insert(page_number, journal_offset);
        self.map.len() >= JOURNAL_INDEX_HOT_THRESHOLD
    }

    /// Remove `page_number` from the index. No-op if absent.
    pub(crate) fn remove(&mut self, page_number: u32) {
        self.map.remove(&page_number);
    }

    /// Drop every entry. Called after a full checkpoint or before rebuilding
    /// the index from a journal scan.
    pub(crate) fn clear_index(&mut self) {
        self.map.clear();
    }

    /// Number of live entries.
    pub(crate) fn occupied_count(&self) -> usize {
        self.map.len()
    }

    /// Iterate over `(page_number, journal_frame_offset)` pairs in unspecified order.
    pub(crate) fn iter_entries(&self) -> impl Iterator<Item = (u32, u64)> + '_ {
        self.map.iter().map(|(&pn, &off)| (pn, off))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
    fn remove_entry() {
        let mut idx = JournalIndex::new();
        idx.insert(7, 512);
        idx.remove(7);
        assert!(idx.lookup(7).is_none());
        assert_eq!(idx.occupied_count(), 0);
    }

    #[test]
    fn remove_nonexistent_is_noop() {
        let mut idx = JournalIndex::new();
        idx.remove(999);
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
}
