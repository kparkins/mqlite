//! US-039 counters for append/sync ownership and Phase 8 benches.
//!
//! The production code under test lives in `src/journal/mod.rs` and
//! `src/storage/handle.rs`. This module keeps the intrusive counter state
//! separate from those owners. Counters stay disabled until reset by tests or
//! the Phase 8 benchmark.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static ENABLED: AtomicBool = AtomicBool::new(false);
static HANDLE_FLUSHES: AtomicU64 = AtomicU64::new(0);
static HANDLE_JOURNAL_SYNCS: AtomicU64 = AtomicU64::new(0);
static JOURNAL_SYNC_OS_BOUNDARIES: AtomicU64 = AtomicU64::new(0);
static MAIN_FILE_SYNCS: AtomicU64 = AtomicU64::new(0);
static JOURNAL_TRUNCATES: AtomicU64 = AtomicU64::new(0);
/// Global 1-based event ticket shared by the ordered observations below;
/// `0` in a `FIRST_*_SEQ` slot means "never recorded since reset".
static EVENT_SEQ: AtomicU64 = AtomicU64::new(0);
static FIRST_MAIN_FILE_SYNC_SEQ: AtomicU64 = AtomicU64::new(0);
static FIRST_JOURNAL_TRUNCATE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Snapshot of US-039 append/sync ownership counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[doc(hidden)]
pub struct Us039AppendSyncObservations {
    /// Calls to `BufferPoolHandle::flush`.
    pub handle_flushes: u64,
    /// Calls to `BufferPoolHandle::journal_sync`.
    pub handle_journal_syncs: u64,
    /// Successful OS journal sync boundaries in `JournalManager::sync_journal`.
    pub journal_sync_os_boundaries: u64,
    /// Successful main-file sync boundaries in `BufferPoolHandle`.
    pub main_file_syncs: u64,
    /// Journal-truncate boundaries recorded at the FIRST destructive write
    /// of recovery's `journal_truncatable` branch (the journal-header
    /// rewrite that precedes `set_len`).
    pub journal_truncates: u64,
    /// Global event ordinal (1-based) of the first main-file sync recorded
    /// since reset, or `None` if none was recorded.
    pub first_main_file_sync_seq: Option<u64>,
    /// Global event ordinal (1-based) of the first journal truncate recorded
    /// since reset, or `None` if none was recorded.
    pub first_journal_truncate_seq: Option<u64>,
}

/// Reset all US-039 append/sync counters.
pub(crate) fn reset() {
    ENABLED.store(true, Ordering::Release);
    HANDLE_FLUSHES.store(0, Ordering::Release);
    HANDLE_JOURNAL_SYNCS.store(0, Ordering::Release);
    JOURNAL_SYNC_OS_BOUNDARIES.store(0, Ordering::Release);
    MAIN_FILE_SYNCS.store(0, Ordering::Release);
    JOURNAL_TRUNCATES.store(0, Ordering::Release);
    EVENT_SEQ.store(0, Ordering::Release);
    FIRST_MAIN_FILE_SYNC_SEQ.store(0, Ordering::Release);
    FIRST_JOURNAL_TRUNCATE_SEQ.store(0, Ordering::Release);
}

/// Return the current US-039 append/sync counter snapshot.
pub(crate) fn snapshot() -> Us039AppendSyncObservations {
    Us039AppendSyncObservations {
        handle_flushes: HANDLE_FLUSHES.load(Ordering::Acquire),
        handle_journal_syncs: HANDLE_JOURNAL_SYNCS.load(Ordering::Acquire),
        journal_sync_os_boundaries: JOURNAL_SYNC_OS_BOUNDARIES.load(Ordering::Acquire),
        main_file_syncs: MAIN_FILE_SYNCS.load(Ordering::Acquire),
        journal_truncates: JOURNAL_TRUNCATES.load(Ordering::Acquire),
        first_main_file_sync_seq: load_first_seq(&FIRST_MAIN_FILE_SYNC_SEQ),
        first_journal_truncate_seq: load_first_seq(&FIRST_JOURNAL_TRUNCATE_SEQ),
    }
}

/// Record a `BufferPoolHandle::flush` call.
pub(crate) fn record_handle_flush() {
    record_if_enabled(&HANDLE_FLUSHES);
}

/// Record a `BufferPoolHandle::journal_sync` call.
pub(crate) fn record_handle_journal_sync() {
    record_if_enabled(&HANDLE_JOURNAL_SYNCS);
}

/// Record a successful `JournalManager::sync_journal` OS sync boundary.
pub(crate) fn record_journal_sync_os_boundary() {
    record_if_enabled(&JOURNAL_SYNC_OS_BOUNDARIES);
}

/// Record a successful main-file sync boundary.
pub(crate) fn record_main_file_sync() {
    record_if_enabled(&MAIN_FILE_SYNCS);
    record_first_seq_if_enabled(&FIRST_MAIN_FILE_SYNC_SEQ);
}

/// Record a journal-truncate boundary (recovery's `journal_truncatable`
/// branch, recorded at its FIRST destructive write — the journal-header
/// rewrite that precedes `set_len`).
pub(crate) fn record_journal_truncate() {
    record_if_enabled(&JOURNAL_TRUNCATES);
    record_first_seq_if_enabled(&FIRST_JOURNAL_TRUNCATE_SEQ);
}

fn record_if_enabled(counter: &AtomicU64) {
    if ENABLED.load(Ordering::Acquire) {
        counter.fetch_add(1, Ordering::AcqRel);
    }
}

/// Stamp `slot` with the next global event ordinal if it has not been
/// stamped since the last reset (first occurrence wins).
fn record_first_seq_if_enabled(slot: &AtomicU64) {
    if ENABLED.load(Ordering::Acquire) {
        let ticket = EVENT_SEQ.fetch_add(1, Ordering::AcqRel) + 1;
        let _ = slot.compare_exchange(0, ticket, Ordering::AcqRel, Ordering::Acquire);
    }
}

/// Read a `FIRST_*_SEQ` slot, mapping the `0` sentinel to `None`.
fn load_first_seq(slot: &AtomicU64) -> Option<u64> {
    match slot.load(Ordering::Acquire) {
        0 => None,
        seq => Some(seq),
    }
}
