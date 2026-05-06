//! US-039 test-only counters for append/sync ownership.
//!
//! The production code under test lives in `src/journal/mod.rs`,
//! `src/storage/handle.rs`, and `src/mvcc/transaction.rs`. This module keeps
//! the intrusive counter state separate from those owners.

use std::sync::atomic::{AtomicU64, Ordering};

static APPEND_LOGICAL_TXN_FLUSHES: AtomicU64 = AtomicU64::new(0);
static APPEND_CHAIN_COMMIT_FLUSHES: AtomicU64 = AtomicU64::new(0);
static COMMIT_TXN_FRAME_FLUSHES: AtomicU64 = AtomicU64::new(0);
static COMMIT_CHAIN_COMMIT_SYNCS: AtomicU64 = AtomicU64::new(0);
static HANDLE_FLUSHES: AtomicU64 = AtomicU64::new(0);
static HANDLE_JOURNAL_SYNCS: AtomicU64 = AtomicU64::new(0);
static JOURNAL_SYNC_OS_BOUNDARIES: AtomicU64 = AtomicU64::new(0);

/// Snapshot of US-039 append/sync ownership counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[doc(hidden)]
pub struct Us039AppendSyncObservations {
    /// Flushes attempted from `JournalManager::append_logical_txn`.
    pub append_logical_txn_flushes: u64,
    /// Flushes attempted from `JournalManager::append_chain_commit`.
    pub append_chain_commit_flushes: u64,
    /// Flushes attempted from `JournalManager::commit`'s commit-frame append.
    pub commit_txn_frame_flushes: u64,
    /// Syncs attempted directly from `WriteTxn::commit_chain_commit`.
    pub commit_chain_commit_syncs: u64,
    /// Calls to `BufferPoolHandle::flush`.
    pub handle_flushes: u64,
    /// Calls to `BufferPoolHandle::journal_sync`.
    pub handle_journal_syncs: u64,
    /// Successful OS journal sync boundaries in `JournalManager::sync_journal`.
    pub journal_sync_os_boundaries: u64,
}

/// Reset all US-039 append/sync counters.
pub(crate) fn reset() {
    APPEND_LOGICAL_TXN_FLUSHES.store(0, Ordering::Release);
    APPEND_CHAIN_COMMIT_FLUSHES.store(0, Ordering::Release);
    COMMIT_TXN_FRAME_FLUSHES.store(0, Ordering::Release);
    COMMIT_CHAIN_COMMIT_SYNCS.store(0, Ordering::Release);
    HANDLE_FLUSHES.store(0, Ordering::Release);
    HANDLE_JOURNAL_SYNCS.store(0, Ordering::Release);
    JOURNAL_SYNC_OS_BOUNDARIES.store(0, Ordering::Release);
}

/// Return the current US-039 append/sync counter snapshot.
pub(crate) fn snapshot() -> Us039AppendSyncObservations {
    Us039AppendSyncObservations {
        append_logical_txn_flushes: APPEND_LOGICAL_TXN_FLUSHES.load(Ordering::Acquire),
        append_chain_commit_flushes: APPEND_CHAIN_COMMIT_FLUSHES.load(Ordering::Acquire),
        commit_txn_frame_flushes: COMMIT_TXN_FRAME_FLUSHES.load(Ordering::Acquire),
        commit_chain_commit_syncs: COMMIT_CHAIN_COMMIT_SYNCS.load(Ordering::Acquire),
        handle_flushes: HANDLE_FLUSHES.load(Ordering::Acquire),
        handle_journal_syncs: HANDLE_JOURNAL_SYNCS.load(Ordering::Acquire),
        journal_sync_os_boundaries: JOURNAL_SYNC_OS_BOUNDARIES.load(Ordering::Acquire),
    }
}

/// Record a flush from `JournalManager::append_logical_txn`.
pub(crate) fn record_append_logical_txn_flush() {
    APPEND_LOGICAL_TXN_FLUSHES.fetch_add(1, Ordering::AcqRel);
}

/// Record a flush from `JournalManager::append_chain_commit`.
pub(crate) fn record_append_chain_commit_flush() {
    APPEND_CHAIN_COMMIT_FLUSHES.fetch_add(1, Ordering::AcqRel);
}

/// Record a commit-frame append flush from `JournalManager::commit`.
pub(crate) fn record_commit_txn_frame_flush() {
    COMMIT_TXN_FRAME_FLUSHES.fetch_add(1, Ordering::AcqRel);
}

/// Record a sync from `WriteTxn::commit_chain_commit`.
pub(crate) fn record_commit_chain_commit_sync() {
    COMMIT_CHAIN_COMMIT_SYNCS.fetch_add(1, Ordering::AcqRel);
}

/// Record a `BufferPoolHandle::flush` call.
pub(crate) fn record_handle_flush() {
    HANDLE_FLUSHES.fetch_add(1, Ordering::AcqRel);
}

/// Record a `BufferPoolHandle::journal_sync` call.
pub(crate) fn record_handle_journal_sync() {
    HANDLE_JOURNAL_SYNCS.fetch_add(1, Ordering::AcqRel);
}

/// Record a successful `JournalManager::sync_journal` OS sync boundary.
pub(crate) fn record_journal_sync_os_boundary() {
    JOURNAL_SYNC_OS_BOUNDARIES.fetch_add(1, Ordering::AcqRel);
}
