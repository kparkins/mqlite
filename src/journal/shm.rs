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
//! checkpoint should drain the journal before it grows further.
//!
//! Recovery, rollback, and checkpoint correctness depend on the index being
//! rebuilt or cleared with the journal state — see the call sites in
//! [`crate::journal::JournalManager`] for the maintenance contract.

use std::collections::HashMap;

/// Journal index hot-threshold: when the index holds at least this many live
/// entries, [`JournalIndex::insert`] returns `true` to signal that an
/// emergency checkpoint should drain the journal.
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
#[path = "tests/shm.rs"]
mod tests;
