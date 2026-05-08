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
}

/// Reset all US-039 append/sync counters.
pub(crate) fn reset() {
    ENABLED.store(true, Ordering::Release);
    HANDLE_FLUSHES.store(0, Ordering::Release);
    HANDLE_JOURNAL_SYNCS.store(0, Ordering::Release);
    JOURNAL_SYNC_OS_BOUNDARIES.store(0, Ordering::Release);
    MAIN_FILE_SYNCS.store(0, Ordering::Release);
}

/// Return the current US-039 append/sync counter snapshot.
pub(crate) fn snapshot() -> Us039AppendSyncObservations {
    Us039AppendSyncObservations {
        handle_flushes: HANDLE_FLUSHES.load(Ordering::Acquire),
        handle_journal_syncs: HANDLE_JOURNAL_SYNCS.load(Ordering::Acquire),
        journal_sync_os_boundaries: JOURNAL_SYNC_OS_BOUNDARIES.load(Ordering::Acquire),
        main_file_syncs: MAIN_FILE_SYNCS.load(Ordering::Acquire),
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
}

fn record_if_enabled(counter: &AtomicU64) {
    if ENABLED.load(Ordering::Acquire) {
        counter.fetch_add(1, Ordering::AcqRel);
    }
}
