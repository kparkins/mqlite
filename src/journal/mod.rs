//! Journal — durability, recovery, checkpoint.
//!
//! This is a private internal module. The public API is exposed through
//! [`Database`](crate::Database) (checkpoint, close, durability configuration).
//!
//! ## Overview
//!
//! The journal implements crash-safe durability using a two-file model:
//!
//! | File | Purpose |
//! |------|---------|
//! `db.mqlite` | Main database file (pages after last checkpoint) |
//! `db.mqlite-journal` | Append-only log of modified pages |
//!
//! Lookup acceleration is provided by a **volatile in-memory** journal index
//! ([`shm::JournalIndex`]) — a `page_number -> latest journal frame offset`
//! map rebuilt from a journal scan on every open. There is no on-disk
//! sidecar for the index. This matches the WiredTiger/MongoDB model: the
//! journal is the only durable artifact, the index is a pure cache.
//!
//! Durability is provided by appending commit records to the journal, with
//! explicit sync ownership at higher-level durability boundaries. Recovery
//! scans can replay any committed batch and discard any trailing uncommitted
//! frames.
//!
//! On clean close, [`JournalManager::close_and_cleanup`] checkpoints all
//! journal pages into the main file and deletes the journal, leaving only
//! `db.mqlite`.

// Crate convention: `expect("N bytes")` on infallible array slices is used
// throughout the journal module to keep the code readable and is acknowledged
// as a non-issue by the team. The clippy lint is allowed at the module
// boundary so denylist-mode CI does not trip on the pre-existing pattern.
#![allow(clippy::expect_used)]

#[path = "tests/append_sync_observations.rs"]
pub(crate) mod append_sync_observations;
#[allow(dead_code)]
pub(crate) mod log_file;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/logical_replay_fixtures.rs"]
pub(crate) mod logical_replay_fixtures;
mod recovery;
#[allow(dead_code)]
pub(crate) mod shm;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex as ParkingMutex};

use crate::error::{EngineFatalReason, Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::storage::header::FileHeader;
use crate::storage::page::PAGE_SIZE_LEAF;
#[cfg(any(test, feature = "test-hooks"))]
use crate::storage::paged_engine::group_commit_observations;

use self::shm::JournalIndex;

pub(crate) use self::recovery::ParsedLogicalFrames;

#[cfg(any(test, feature = "test-hooks"))]
use self::log_file::LogicalTxnFrame;
use self::log_file::{
    JournalHeader, JournalOffset, JournalPageSize, PageId, PositionedLogFile, JOURNAL_HEADER_SIZE,
};

// ---------------------------------------------------------------------------
// JournalManager
// ---------------------------------------------------------------------------

/// Durable checkpoint-boundary append token.
///
/// The token is produced only by
/// [`JournalManager::append_checkpoint_commit_boundary`] and consumed by the
/// allocator staged-header commit path.
#[must_use = "BoundaryAppended must be consumed by commit_staged_header_after_boundary"]
#[derive(Debug)]
pub(crate) struct BoundaryAppended {
    journal_offset: JournalOffset,
    db_page_count: u32,
    checkpoint_ts: Ts,
    _private: (),
}

impl BoundaryAppended {
    /// Database page count covered by the durable boundary.
    pub(crate) fn db_page_count(&self) -> u32 {
        self.db_page_count
    }

    /// Journal byte offset where the boundary starts.
    pub(crate) fn journal_offset(&self) -> JournalOffset {
        self.journal_offset
    }
}

// ---------------------------------------------------------------------------
// Phase 8 LogManager
// ---------------------------------------------------------------------------

/// Byte-LSN range reserved for exactly one Phase 8 log record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LogSlot {
    start_lsn: u64,
    end_lsn: u64,
    bytes_len: usize,
}

impl LogSlot {
    /// Inclusive byte offset where this record must be written.
    pub(crate) fn start_lsn(&self) -> u64 {
        self.start_lsn
    }

    /// Exclusive byte offset just past this record.
    pub(crate) fn end_lsn(&self) -> u64 {
        self.end_lsn
    }

    /// Exact byte length reserved for this record.
    pub(crate) fn bytes_len(&self) -> usize {
        self.bytes_len
    }
}

/// Result of marking a reserved slot written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LogWriteReceipt {
    end_lsn: u64,
    ready_lsn: u64,
}

impl LogWriteReceipt {
    /// Exclusive end LSN of the slot just marked written.
    pub(crate) fn end_lsn(&self) -> u64 {
        self.end_lsn
    }

    /// Contiguous ready frontier after this mark operation.
    pub(crate) fn ready_lsn(&self) -> u64 {
        self.ready_lsn
    }
}

/// Reserved Phase 8 log record plus the manager slot that owns its byte range.
pub(crate) struct ReservedLogRecord {
    log_manager: Option<Arc<LogManager>>,
    slot: Option<LogSlot>,
    record: log_file::FinalizedLogRecord,
}

impl ReservedLogRecord {
    fn journaled(
        log_manager: Arc<LogManager>,
        slot: LogSlot,
        record: log_file::FinalizedLogRecord,
    ) -> Self {
        Self {
            log_manager: Some(log_manager),
            slot: Some(slot),
            record,
        }
    }

    pub(crate) fn journalless(record: log_file::FinalizedLogRecord) -> Self {
        Self {
            log_manager: None,
            slot: None,
            record,
        }
    }

    /// Inclusive byte-LSN where the reserved record begins.
    pub(crate) fn start_lsn(&self) -> u64 {
        self.record.start_lsn()
    }

    /// Exclusive end LSN of this complete record.
    pub(crate) fn end_lsn(&self) -> u64 {
        self.record.end_lsn()
    }

    /// Returns `true` when this record owns a real log-manager slot.
    pub(crate) fn is_journaled(&self) -> bool {
        self.log_manager.is_some()
    }

    /// Poison this reserved record's slot after a post-reservation failure.
    pub(crate) fn poison_slot(&self, error: Error) -> Error {
        match (&self.log_manager, &self.slot) {
            (Some(log_manager), Some(slot)) => log_manager.poison_slot(slot, error),
            _ => error,
        }
    }

    /// Write the finalized bytes and mark the record written.
    pub(crate) fn write_and_mark(&self) -> Result<u64> {
        match (&self.log_manager, &self.slot) {
            (Some(log_manager), Some(slot)) => {
                log_manager.write_reserved(slot, self.record.bytes())?;
                let receipt = log_manager.mark_written(slot)?;
                Ok(receipt.end_lsn())
            }
            _ => Ok(self.record.end_lsn()),
        }
    }
}

#[derive(Clone, Debug)]
struct LogPoison {
    reason: EngineFatalReason,
    _source: String,
}

#[derive(Debug)]
enum LogSlotState {
    Reserved { end_lsn: u64 },
    Writing { end_lsn: u64 },
    WriteComplete { end_lsn: u64 },
    Written { end_lsn: u64 },
    Poisoned { end_lsn: u64 },
}

impl LogSlotState {
    fn end_lsn(&self) -> u64 {
        match self {
            Self::Reserved { end_lsn }
            | Self::Writing { end_lsn }
            | Self::WriteComplete { end_lsn }
            | Self::Written { end_lsn }
            | Self::Poisoned { end_lsn } => *end_lsn,
        }
    }
}

#[derive(Debug, Default)]
struct LogSlotMap {
    slots: BTreeMap<u64, LogSlotState>,
    poisoned: Option<LogPoison>,
}

const LSN_GROUP_COMMIT_WAIT_POLL_MS: u64 = 1;
const LSN_GROUP_COMMIT_MAX_WAIT_MS: u64 = 2;
#[cfg(any(test, feature = "test-hooks"))]
const LSN_GROUP_COMMIT_TEST_HOOK_WAIT_MS: u64 = 5_000;

/// Phase 8 byte-LSN reservation and positioned-write manager.
///
/// `LogManager` owns byte-range reservation independently of commit metadata:
/// [`reserve`](Self::reserve) depends only on a record length, while callers
/// finalize and write their already-built record bytes into the returned slot.
pub(crate) struct LogManager {
    next_lsn: AtomicU64,
    ready_lsn: AtomicU64,
    durable_lsn: AtomicU64,
    slots: ParkingMutex<LogSlotMap>,
    sync_cv: Condvar,
    sync_in_progress: AtomicBool,
    file: PositionedLogFile,
    #[cfg(any(test, feature = "test-hooks"))]
    probe_id: u64,
}

impl LogManager {
    /// Create a log manager over `file`, seeded at `initial_lsn`.
    pub(crate) fn new(file: File, initial_lsn: u64) -> Self {
        Self::from_positioned_file(PositionedLogFile::new(file), initial_lsn)
    }

    /// Create a log manager from an explicit positioned I/O adapter.
    pub(crate) fn from_positioned_file(file: PositionedLogFile, initial_lsn: u64) -> Self {
        Self {
            next_lsn: AtomicU64::new(initial_lsn),
            ready_lsn: AtomicU64::new(initial_lsn),
            durable_lsn: AtomicU64::new(initial_lsn),
            slots: ParkingMutex::new(LogSlotMap::default()),
            sync_cv: Condvar::new(),
            sync_in_progress: AtomicBool::new(false),
            file,
            #[cfg(any(test, feature = "test-hooks"))]
            probe_id: group_commit_observations::next_probe_id(),
        }
    }

    /// Reserve a disjoint byte-LSN range for a record of `bytes_len` bytes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if `bytes_len` is zero or the LSN range
    /// would overflow. Returns [`Error::EngineFatal`] after the manager is
    /// poisoned.
    pub(crate) fn reserve(&self, bytes_len: usize) -> Result<LogSlot> {
        if bytes_len == 0 {
            return Err(Error::Internal(
                "log reservation length must be non-zero".into(),
            ));
        }
        let bytes_len_u64 = u64::try_from(bytes_len)
            .map_err(|_| Error::Internal("log reservation length overflows u64".into()))?;

        let mut state = self.slots.lock();
        Self::check_poisoned_locked(&state)?;

        let start_lsn = self.next_lsn.load(Ordering::Acquire);
        let end_lsn = start_lsn
            .checked_add(bytes_len_u64)
            .ok_or_else(|| Error::Internal("log reservation LSN overflow".into()))?;
        if state
            .slots
            .insert(start_lsn, LogSlotState::Reserved { end_lsn })
            .is_some()
        {
            return Err(Error::Internal(format!(
                "duplicate log slot reservation at LSN {start_lsn}"
            )));
        }
        self.next_lsn.store(end_lsn, Ordering::Release);

        Ok(LogSlot {
            start_lsn,
            end_lsn,
            bytes_len,
        })
    }

    /// Write `bytes` into `slot` using absolute-offset file I/O.
    ///
    /// The slot is not made ready by this call. Call
    /// [`mark_written`](Self::mark_written) after this returns successfully.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EngineFatal`] and poisons the manager if the write
    /// fails after reservation or the reserved slot is no longer in the
    /// expected state. Returns [`Error::Internal`] for an unknown slot.
    pub(crate) fn write_reserved(&self, slot: &LogSlot, bytes: &[u8]) -> Result<()> {
        if bytes.len() != slot.bytes_len {
            let error = Error::Internal(format!(
                "log slot length mismatch: reserved {} bytes, got {}",
                slot.bytes_len,
                bytes.len()
            ));
            return Err(self.poison_slot(slot, error));
        }

        {
            let mut state = self.slots.lock();
            Self::check_poisoned_locked(&state)?;
            let slot_state = Self::slot_state_mut(&mut state, slot)?;
            match slot_state {
                LogSlotState::Reserved { end_lsn } if *end_lsn == slot.end_lsn => {
                    *slot_state = LogSlotState::Writing {
                        end_lsn: slot.end_lsn,
                    };
                }
                _ => return Err(self.poison_bad_slot_state_locked(&mut state, slot)),
            }
        }

        if let Err(error) = self.file.write_all_at(slot.start_lsn, bytes) {
            return Err(self.poison_slot(slot, Error::Io(error)));
        }

        let mut state = self.slots.lock();
        Self::check_poisoned_locked(&state)?;
        let slot_state = Self::slot_state_mut(&mut state, slot)?;
        match slot_state {
            LogSlotState::Writing { end_lsn } if *end_lsn == slot.end_lsn => {
                *slot_state = LogSlotState::WriteComplete {
                    end_lsn: slot.end_lsn,
                };
                Ok(())
            }
            LogSlotState::Poisoned { .. } => Err(Self::fatal_error(
                &EngineFatalReason::PostReservationLogWriteFailure,
            )),
            _ => Err(self.poison_bad_slot_state_locked(&mut state, slot)),
        }
    }

    /// Mark a fully written slot ready and advance the contiguous ready LSN.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EngineFatal`] if the slot was not successfully written
    /// after reservation or after any slot poisons the manager. Returns
    /// [`Error::Internal`] for an unknown slot.
    pub(crate) fn mark_written(&self, slot: &LogSlot) -> Result<LogWriteReceipt> {
        let mut state = self.slots.lock();
        Self::check_poisoned_locked(&state)?;

        let slot_state = Self::slot_state_mut(&mut state, slot)?;
        match slot_state {
            LogSlotState::WriteComplete { end_lsn } if *end_lsn == slot.end_lsn => {
                *slot_state = LogSlotState::Written {
                    end_lsn: slot.end_lsn,
                };
            }
            _ => return Err(self.poison_bad_slot_state_locked(&mut state, slot)),
        }

        let ready_before = self.ready_lsn.load(Ordering::Acquire);
        let mut ready = ready_before;
        while matches!(state.slots.get(&ready), Some(LogSlotState::Written { .. })) {
            let end_lsn = state
                .slots
                .remove(&ready)
                .expect("ready slot exists")
                .end_lsn();
            ready = end_lsn;
        }
        if ready != ready_before {
            self.ready_lsn.store(ready, Ordering::Release);
        }
        drop(state);
        self.sync_cv.notify_all();

        Ok(LogWriteReceipt {
            end_lsn: slot.end_lsn,
            ready_lsn: ready,
        })
    }

    /// Poison a reserved slot and wake every waiter.
    ///
    /// # Errors
    ///
    /// This method returns the [`Error::EngineFatal`] value callers should
    /// propagate. It does not itself return a `Result`.
    pub(crate) fn poison_slot(&self, slot: &LogSlot, error: Error) -> Error {
        self.poison(
            EngineFatalReason::PostReservationLogWriteFailure,
            error.to_string(),
            Some(slot),
        )
    }

    /// Wait until the contiguous ready LSN covers `end_lsn`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EngineFatal`] if any slot has poisoned the manager.
    pub(crate) fn wait_ready(&self, end_lsn: u64) -> Result<()> {
        let mut state = self.slots.lock();
        loop {
            Self::check_poisoned_locked(&state)?;
            if self.ready_lsn.load(Ordering::Acquire) >= end_lsn {
                return Ok(());
            }
            self.sync_cv.wait(&mut state);
        }
    }

    /// Sync the log through `target_lsn` once it is ready.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EngineFatal`] if a write or sync failure poisons the
    /// manager, or the underlying I/O error wrapped by that fatal state.
    pub(crate) fn ensure_sync(&self, target_lsn: u64) -> Result<()> {
        self.wait_ready(target_lsn)?;
        loop {
            if self.durable_lsn() >= target_lsn {
                return Ok(());
            }
            self.check_poisoned()?;

            if self
                .sync_in_progress
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                #[cfg(any(test, feature = "test-hooks"))]
                let leader_guard = group_commit_observations::leader_entered();

                let sync_target = match self.close_sync_target_after_wait() {
                    Ok(target) => target,
                    Err(error) => {
                        #[cfg(any(test, feature = "test-hooks"))]
                        drop(leader_guard);
                        self.sync_in_progress.store(false, Ordering::Release);
                        self.sync_cv.notify_all();
                        return Err(error);
                    }
                };
                #[cfg(any(test, feature = "test-hooks"))]
                let result = if group_commit_observations::take_fail_next_fsync() {
                    Err(Error::Internal(
                        "US-017 injected group-commit fsync failure".into(),
                    ))
                } else {
                    self.file.sync_data().map_err(Error::Io)
                };
                #[cfg(not(any(test, feature = "test-hooks")))]
                let result = self.file.sync_data().map_err(Error::Io);
                match result {
                    Ok(()) => {
                        let durable_target = sync_target.min(self.ready_lsn());
                        self.durable_lsn.store(durable_target, Ordering::Release);
                        self::append_sync_observations::record_handle_journal_sync();
                        self::append_sync_observations::record_journal_sync_os_boundary();
                        #[cfg(any(test, feature = "test-hooks"))]
                        group_commit_observations::record_fsync_success(durable_target);
                        self.sync_in_progress.store(false, Ordering::Release);
                        #[cfg(any(test, feature = "test-hooks"))]
                        drop(leader_guard);
                        self.sync_cv.notify_all();
                    }
                    Err(error) => {
                        #[cfg(any(test, feature = "test-hooks"))]
                        {
                            group_commit_observations::record_fsync_failure(sync_target);
                            drop(leader_guard);
                        }
                        return Err(self.poison(
                            EngineFatalReason::PostDurablePublishFailure,
                            error.to_string(),
                            None,
                        ));
                    }
                }
                continue;
            }

            let mut state = self.slots.lock();
            while self.durable_lsn() < target_lsn && self.sync_in_progress.load(Ordering::Acquire) {
                Self::check_poisoned_locked(&state)?;
                self.sync_cv.wait(&mut state);
            }
        }
    }

    fn close_sync_target_after_wait(&self) -> Result<u64> {
        let production_deadline =
            Instant::now() + Duration::from_millis(LSN_GROUP_COMMIT_MAX_WAIT_MS);
        #[cfg(any(test, feature = "test-hooks"))]
        let test_deadline =
            Instant::now() + Duration::from_millis(LSN_GROUP_COMMIT_TEST_HOOK_WAIT_MS);
        let mut state = self.slots.lock();

        loop {
            Self::check_poisoned_locked(&state)?;

            #[cfg(any(test, feature = "test-hooks"))]
            if let Some(expected) = group_commit_observations::expected_cohort_size() {
                if group_commit_observations::active_waiters() >= expected {
                    group_commit_observations::clear_expected_cohort_size();
                    break;
                }
                if Instant::now() < test_deadline {
                    self.sync_cv.wait_for(
                        &mut state,
                        Duration::from_millis(LSN_GROUP_COMMIT_WAIT_POLL_MS),
                    );
                    continue;
                }
            }

            if Instant::now() >= production_deadline {
                break;
            }
            self.sync_cv.wait_for(
                &mut state,
                Duration::from_millis(LSN_GROUP_COMMIT_WAIT_POLL_MS),
            );
        }

        let sync_target = self.ready_lsn();
        drop(state);
        #[cfg(any(test, feature = "test-hooks"))]
        group_commit_observations::pause_after_close_if_installed(self.probe_id, sync_target);
        Ok(sync_target)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn probe_id(&self) -> u64 {
        self.probe_id
    }

    /// Wait until `end_lsn` is durable.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EngineFatal`] if a write or sync failure poisons the
    /// manager.
    pub(crate) fn wait_durable(&self, end_lsn: u64) -> Result<()> {
        #[cfg(any(test, feature = "test-hooks"))]
        let _waiter_guard = group_commit_observations::waiter_entered();
        self.ensure_sync(end_lsn)
    }

    /// Reset all LSN frontiers after rollback, recovery truncation, or
    /// checkpoint log truncation.
    pub(crate) fn reset_to(&self, lsn: u64) {
        let mut state = self.slots.lock();
        state.slots.clear();
        state.poisoned = None;
        self.next_lsn.store(lsn, Ordering::Release);
        self.ready_lsn.store(lsn, Ordering::Release);
        self.durable_lsn.store(lsn, Ordering::Release);
        drop(state);
        self.sync_cv.notify_all();
    }

    /// Return the contiguous fully written byte frontier.
    pub(crate) fn ready_lsn(&self) -> u64 {
        self.ready_lsn.load(Ordering::Acquire)
    }

    /// Return the next byte-LSN reservation frontier.
    pub(crate) fn next_lsn(&self) -> u64 {
        self.next_lsn.load(Ordering::Acquire)
    }

    /// Return the synced byte frontier.
    pub(crate) fn durable_lsn(&self) -> u64 {
        self.durable_lsn.load(Ordering::Acquire)
    }

    fn check_poisoned(&self) -> Result<()> {
        let state = self.slots.lock();
        Self::check_poisoned_locked(&state)
    }

    fn check_poisoned_locked(state: &LogSlotMap) -> Result<()> {
        if let Some(poison) = &state.poisoned {
            return Err(Self::fatal_error(&poison.reason));
        }
        Ok(())
    }

    fn fatal_error(reason: &EngineFatalReason) -> Error {
        Error::EngineFatal {
            reason: reason.clone(),
        }
    }

    fn poison(&self, reason: EngineFatalReason, source: String, slot: Option<&LogSlot>) -> Error {
        let mut state = self.slots.lock();
        let error = self.poison_locked(&mut state, reason, source, slot);
        drop(state);
        self.sync_cv.notify_all();
        error
    }

    fn poison_bad_slot_state_locked(&self, state: &mut LogSlotMap, slot: &LogSlot) -> Error {
        self.poison_locked(
            state,
            EngineFatalReason::PostReservationLogWriteFailure,
            Self::bad_slot_state(slot).to_string(),
            Some(slot),
        )
    }

    fn poison_locked(
        &self,
        state: &mut LogSlotMap,
        reason: EngineFatalReason,
        source: String,
        slot: Option<&LogSlot>,
    ) -> Error {
        if state.poisoned.is_none() {
            state.poisoned = Some(LogPoison {
                reason: reason.clone(),
                _source: source,
            });
        }
        if let Some(slot) = slot {
            state.slots.insert(
                slot.start_lsn,
                LogSlotState::Poisoned {
                    end_lsn: slot.end_lsn,
                },
            );
        }
        self.sync_in_progress.store(false, Ordering::Release);
        self.sync_cv.notify_all();
        Self::fatal_error(&reason)
    }

    fn slot_state_mut<'a>(
        state: &'a mut LogSlotMap,
        slot: &LogSlot,
    ) -> Result<&'a mut LogSlotState> {
        let Some(slot_state) = state.slots.get_mut(&slot.start_lsn) else {
            return Err(Self::bad_slot_state(slot));
        };
        if slot_state.end_lsn() != slot.end_lsn {
            return Err(Self::bad_slot_state(slot));
        }
        Ok(slot_state)
    }

    fn bad_slot_state(slot: &LogSlot) -> Error {
        Error::Internal(format!(
            "log slot [{}, {}) is not in the expected state",
            slot.start_lsn, slot.end_lsn
        ))
    }
}

/// Monotonic identity for a checkpoint-owned journal batch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CheckpointBatchId(u64);

impl CheckpointBatchId {
    /// Wire-format identifier carried by Phase 8 `CheckpointPageFrame` and
    /// `CheckpointBoundary` records.
    pub(crate) fn as_u64(self) -> u64 {
        self.0
    }
}

/// Pool that produced a checkpoint journal frame.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum CheckpointPoolKind {
    /// Main data/catalog buffer pool.
    Main,
    /// Dedicated history-store buffer pool.
    History,
}

/// Non-clone cursor proving the clean start of one checkpoint batch.
#[derive(Debug)]
pub(crate) struct CheckpointBatchCursor {
    expected_pending_start: JournalOffset,
    clean_start_offset: JournalOffset,
    batch_id: CheckpointBatchId,
    _private: (),
}

impl CheckpointBatchCursor {
    /// Batch id assigned by [`JournalManager::begin_checkpoint_batch`].
    pub(crate) fn batch_id(&self) -> CheckpointBatchId {
        self.batch_id
    }

    /// Offset where checkpoint-owned pending frames must begin.
    pub(crate) fn expected_pending_start(&self) -> JournalOffset {
        self.expected_pending_start
    }
}

/// Checkpoint-owned dirty pages selected for step-8 journal flushing.
#[derive(Debug)]
pub(crate) struct CheckpointFlushSet {
    batch_id: CheckpointBatchId,
    main_pages: BTreeSet<PageId>,
    history_pages: BTreeSet<PageId>,
    excluded_future_dirty_pages: BTreeSet<PageId>,
    _private: (),
}

impl CheckpointFlushSet {
    /// Build a flush set after validating page ownership is unambiguous.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if a page is owned by both pools.
    pub(crate) fn new(
        batch_id: CheckpointBatchId,
        main_pages: BTreeSet<PageId>,
        history_pages: BTreeSet<PageId>,
        excluded_future_dirty_pages: BTreeSet<PageId>,
    ) -> Result<Self> {
        if let Some(page) = main_pages.intersection(&history_pages).next() {
            return Err(Error::Internal(format!(
                "checkpoint flush set page {} is owned by both pools",
                page.0
            )));
        }
        Ok(Self {
            batch_id,
            main_pages,
            history_pages,
            excluded_future_dirty_pages,
            _private: (),
        })
    }

    /// Batch id that all flushed frames must carry.
    pub(crate) fn batch_id(&self) -> CheckpointBatchId {
        self.batch_id
    }

    /// Main-pool pages covered by this checkpoint batch.
    pub(crate) fn main_pages(&self) -> &BTreeSet<PageId> {
        &self.main_pages
    }

    /// History-pool pages covered by this checkpoint batch.
    pub(crate) fn history_pages(&self) -> &BTreeSet<PageId> {
        &self.history_pages
    }

    /// Dirty pages intentionally left out because they are above the frontier.
    pub(crate) fn excluded_future_dirty_pages(&self) -> &BTreeSet<PageId> {
        &self.excluded_future_dirty_pages
    }
}

#[derive(Clone, Copy, Debug)]
struct CheckpointFrameTag {
    batch_id: CheckpointBatchId,
    pool: CheckpointPoolKind,
    page_id: PageId,
}

/// Manages the journal and its in-memory page-offset index for one database.
///
/// Created via [`JournalManager::open_or_create`].  On clean shutdown call
/// [`JournalManager::close_and_cleanup`]; on crash, the next `open_or_create`
/// automatically runs recovery.
pub(crate) struct JournalManager {
    /// Path to the `.mqlite-journal` file.
    pub(super) journal_path: PathBuf,
    /// Open handle to the journal file (positioned at the write cursor).
    pub(super) journal_file: File,
    /// In-memory `page_number -> journal frame offset` index, rebuilt from
    /// a journal scan on open and maintained in-place. Not persisted.
    pub(super) index: JournalIndex,
    /// Salt 1 from the main file header (stored in every journal frame).
    pub(super) salt1: u32,
    /// Salt 2 from the main file header.
    pub(super) salt2: u32,
    /// Checkpoint sequence counter from the journal file header.
    pub(super) checkpoint_seq: u32,
    /// Byte offset of the next frame to write (append cursor).
    pub(super) write_cursor: u64,
    /// Phase 8 byte-LSN reservation manager for ordinary commit-log appends.
    log_manager: Arc<LogManager>,
    /// Total database page count as of the last committed journal frame.
    /// Carried forward across commits; `None` if no commit has occurred yet
    /// in this journal.
    pub(super) last_committed_db_page_count: Option<u32>,
    /// Highest `commit_ts` observed on any durable `ChainCommit` frame
    /// during recovery (`recover_existing`). `None` when the journal was
    /// freshly created or carried no ChainCommit frames. The MVCC backend
    /// reads this via [`recovered_max_commit_ts`](Self::recovered_max_commit_ts)
    /// to floor [`TimestampOracle`] so every post-recovery commit is strictly
    /// greater than any durable commit from the previous lifetime.
    pub(super) recovered_max_commit_ts: Option<Ts>,
    /// Highest non-control Phase 8 `publish_seq` accepted during recovery.
    /// The MVCC backend uses this to start the live publish sequencer above
    /// every durable pre-crash publish slot.
    pub(super) recovered_max_publish_seq: Option<u64>,
    /// Phase 2 §5.1 Pass 1 hand-off: logical frames collected during
    /// `recover_existing`, consumed exactly once by
    /// [`SharedState::new`](crate::storage::paged_engine::state::SharedState::new)
    /// via [`take_parsed_logical_frames`](Self::take_parsed_logical_frames)
    /// for Pass 2 validation (§5.2).
    pub(crate) parsed_logical_frames: ParsedLogicalFrames,
    /// Start offset for an uncommitted legacy page-frame range.
    pub(super) legacy_pending_start_offset: Option<JournalOffset>,
    /// End offset of the most recent committed legacy page-frame range.
    pub(super) last_legacy_commit_end_offset: JournalOffset,
    /// Open checkpoint batch id/start, if step-8 flushing is active.
    pub(super) checkpoint_batch_active: Option<(CheckpointBatchId, JournalOffset)>,
    /// Next in-process checkpoint batch id.
    pub(super) next_checkpoint_batch_id: u64,
    /// In-memory tags for checkpoint-owned pending page frames.
    checkpoint_frame_tags: BTreeMap<JournalOffset, CheckpointFrameTag>,
}

impl JournalManager {
    // -----------------------------------------------------------------------
    // Open / recovery
    // -----------------------------------------------------------------------

    /// Open or create the journal for the database at `db_path`.
    ///
    /// If a journal file already exists, recovery is called to
    /// replay any committed frames into the main file before returning.
    ///
    /// `main_header` is the file header of the main database file.  Its salt
    /// fields are used to detect stale journal files.
    ///
    /// `main_file` is an open handle to the main database file, needed during
    /// recovery to write checkpointed pages.
    pub(crate) fn open_or_create(
        db_path: &Path,
        main_header: &FileHeader,
        main_file: &mut File,
    ) -> Result<Self> {
        let journal_path = journal_path_for(db_path);
        let salt1 = main_header.wal_salt1;
        let salt2 = main_header.wal_salt2;

        // Does a journal file already exist?
        if journal_path.exists() {
            // Try to recover it.
            let recovered = Self::recover_existing(&journal_path, main_header, main_file)?;
            if let Some(mgr) = recovered {
                return Ok(mgr);
            }
            // If recover_existing returned None, the journal was stale/corrupt and
            // has been deleted.  Fall through to create a fresh journal.
        }

        // Create a new journal file.
        let mut journal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&journal_path)
            .map_err(Error::Io)?;

        let header = JournalHeader::new(salt1, salt2);
        journal_file
            .write_all(&header.to_bytes())
            .map_err(Error::Io)?;
        journal_file.flush().map_err(Error::Io)?;
        let log_manager_file = journal_file.try_clone().map_err(Error::Io)?;

        Ok(Self {
            journal_path,
            journal_file,
            index: JournalIndex::new(),
            salt1,
            salt2,
            checkpoint_seq: 0,
            write_cursor: JOURNAL_HEADER_SIZE as u64,
            log_manager: Arc::new(LogManager::new(
                log_manager_file,
                JOURNAL_HEADER_SIZE as u64,
            )),
            last_committed_db_page_count: None,
            recovered_max_commit_ts: None,
            recovered_max_publish_seq: None,
            parsed_logical_frames: ParsedLogicalFrames::default(),
            legacy_pending_start_offset: None,
            last_legacy_commit_end_offset: JOURNAL_HEADER_SIZE as u64,
            checkpoint_batch_active: None,
            next_checkpoint_batch_id: 1,
            checkpoint_frame_tags: BTreeMap::new(),
        })
    }

    // -----------------------------------------------------------------------
    // Writing (appending frames)
    // -----------------------------------------------------------------------

    /// Return the batch id that the next checkpoint batch will receive.
    pub(crate) fn next_checkpoint_batch_id(&self) -> CheckpointBatchId {
        CheckpointBatchId(self.next_checkpoint_batch_id)
    }

    /// Consume the next checkpoint batch id, advancing the in-memory counter.
    ///
    /// The returned id is the value that will be persisted into the next
    /// `CheckpointBoundary` record. Recovery seeds this counter from the
    /// maximum boundary `batch_id` observed during scan, so post-restart
    /// batches do not collide with persisted ids.
    pub(crate) fn consume_checkpoint_batch_id(&mut self) -> u64 {
        let id = self.next_checkpoint_batch_id;
        self.next_checkpoint_batch_id = self.next_checkpoint_batch_id.saturating_add(1);
        id
    }

    /// Open a checkpoint-owned pending range at the current clean cursor.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if an ordinary legacy page-frame range is
    /// pending or another checkpoint batch is already open.
    pub(crate) fn begin_checkpoint_batch(&mut self) -> Result<CheckpointBatchCursor> {
        if self.checkpoint_batch_active.is_some() {
            return Err(Error::Internal("checkpoint batch already active".into()));
        }
        let batch_id = CheckpointBatchId(self.next_checkpoint_batch_id);
        self.next_checkpoint_batch_id = self.next_checkpoint_batch_id.saturating_add(1);
        let clean_start_offset = self.log_manager.next_lsn();
        self.checkpoint_batch_active = Some((batch_id, clean_start_offset));
        Ok(CheckpointBatchCursor {
            expected_pending_start: clean_start_offset,
            clean_start_offset,
            batch_id,
            _private: (),
        })
    }

    /// Abort an open checkpoint batch before any frame append has happened.
    pub(crate) fn abort_empty_checkpoint_batch(&mut self, cursor: &CheckpointBatchCursor) {
        if self.log_manager.next_lsn() != cursor.clean_start_offset {
            return;
        }
        if self.checkpoint_batch_active == Some((cursor.batch_id, cursor.clean_start_offset)) {
            self.checkpoint_batch_active = None;
        }
    }

    /// Append a checkpoint-owned page frame tagged with `batch_id`.
    ///
    /// Reserves through [`LogManager`] so concurrent CRUD reservations cannot
    /// interleave bytes with the per-page record. Returns the exclusive end
    /// LSN of the written record so callers may stamp the dirty buffer-pool
    /// frame with that LSN to arm the eviction pin invariant
    /// (`src/storage/buffer_pool/partition.rs`).
    pub(crate) fn append_checkpoint_page_frame(
        &self,
        batch_id: CheckpointBatchId,
        pool: CheckpointPoolKind,
        page_number: u32,
        page_size: JournalPageSize,
        page_data: &[u8],
    ) -> Result<u64> {
        debug_assert_eq!(page_data.len(), page_size.bytes());
        let Some((active_batch, _start)) = self.checkpoint_batch_active else {
            return Err(Error::Internal(
                "cannot append checkpoint frame without active batch".into(),
            ));
        };
        if active_batch != batch_id {
            return Err(Error::Internal(format!(
                "checkpoint frame batch {:?} does not match active batch {:?}",
                batch_id, active_batch
            )));
        }
        let pool_kind = match pool {
            CheckpointPoolKind::Main => log_file::CheckpointPagePool::Main,
            CheckpointPoolKind::History => log_file::CheckpointPagePool::History,
        };
        let payload = log_file::CheckpointPageFramePayload {
            batch_id: batch_id.as_u64(),
            pool: pool_kind,
            page_number,
            page_size,
            data: page_data.to_vec(),
        }
        .encode()?;
        let draft = log_file::LogRecordDraft::checkpoint_page_frame(Ts::default(), payload);
        let reserved = Self::reserve_log_record_on(&self.log_manager, draft)?;
        let end_lsn = reserved.end_lsn();
        reserved.write_and_mark()?;
        Ok(end_lsn)
    }

    /// Test-only `&mut self` wrapper preserving the historical signature.
    /// Returns the inclusive byte LSN where the per-page record starts so
    /// callers that previously located a 24-byte legacy frame at that offset
    /// keep computing offsets that fall inside the new Phase 8 record's
    /// header bytes.
    pub(crate) fn append_checkpoint_frame(
        &mut self,
        batch_id: CheckpointBatchId,
        pool: CheckpointPoolKind,
        page_number: u32,
        page_size: JournalPageSize,
        page_data: &[u8],
    ) -> Result<u64> {
        let start_lsn = self.log_manager.next_lsn();
        let _end_lsn =
            self.append_checkpoint_page_frame(batch_id, pool, page_number, page_size, page_data)?;
        Ok(start_lsn)
    }

    /// Append an MVCC `ChainCommit` frame to the journal.
    ///
    /// Emits one `ChainCommitFrame` carrying `commit_ts`, `refcount_deltas`,
    /// and zero or more `page_writes`. The frame reserves a byte-LSN slot and
    /// writes at that absolute offset; the compatibility cursor follows the
    /// ready LSN after the write. Durability belongs to the caller's explicit
    /// sync boundary, not to this append path.
    ///
    /// The in-memory index is NOT updated — `ChainCommit` frames carry no
    /// single page number (every `page_writes` entry has its own). Recovery
    /// scans `ChainCommit` frames linearly.
    /// Append a page-0 checkpoint commit boundary to the journal.
    ///
    /// Returns the [`BoundaryAppended`] token for the durable page-0 frame.
    /// The staged header bytes are encoded before any allocator header state is
    /// mutated; durability belongs to the journal sync boundary here.
    pub(crate) fn append_checkpoint_commit_boundary(
        &mut self,
        staged_header: &FileHeader,
        checkpoint_batch: CheckpointBatchCursor,
    ) -> Result<BoundaryAppended> {
        let Some((batch_id, expected_start)) = self.checkpoint_batch_active else {
            return Err(Error::Internal(
                "checkpoint boundary requires an active checkpoint batch".into(),
            ));
        };
        if batch_id != checkpoint_batch.batch_id
            || expected_start != checkpoint_batch.expected_pending_start
        {
            return Err(Error::Internal(
                "checkpoint boundary cursor does not match active checkpoint batch".into(),
            ));
        }
        self.log_manager.check_poisoned()?;

        let db_page_count = staged_header.total_page_count;
        let checkpoint_ts = staged_header.last_checkpoint_ts;
        let payload = log_file::CheckpointBoundaryPayload {
            checkpoint_applied_lsn: staged_header.checkpoint_applied_lsn,
            batch_id: batch_id.as_u64(),
            header: staged_header.clone(),
        }
        .encode()?;
        let draft = log_file::LogRecordDraft::checkpoint_boundary(0, checkpoint_ts, payload);
        let reserved = Self::reserve_log_record_on(&self.log_manager, draft)?;
        let frame_offset = reserved.start_lsn();
        reserved.write_and_mark()?;
        self.last_committed_db_page_count = Some(db_page_count);
        self.checkpoint_batch_active = None;
        Ok(BoundaryAppended {
            journal_offset: frame_offset,
            db_page_count,
            checkpoint_ts,
            _private: (),
        })
    }

    // -----------------------------------------------------------------------
    // Durability
    // -----------------------------------------------------------------------

    /// fsync the journal file, making all committed-but-unsynced frames durable.
    ///
    /// The Phase 8 log manager waits for the ready prefix, calls
    /// `sync_data()` (fdatasync), and advances `durable_lsn` through the synced
    /// frontier. Main-file contents are NOT touched — this is the FullSync hot
    /// path, not a checkpoint.
    pub(crate) fn sync_journal(&self) -> Result<()> {
        self.log_manager.ensure_sync(self.log_manager.next_lsn())
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Return the current journal write cursor (byte offset past the last frame).
    ///
    /// Delegates to the unified [`LogManager`] reservation frontier so callers
    /// observe the same value the next reservation will hand out.
    pub(crate) fn write_cursor(&self) -> u64 {
        self.log_manager.next_lsn()
    }

    /// Convenience alias for [`Self::write_cursor`] used by callers that prefer
    /// the LSN naming when reading the next reservation frontier.
    pub(crate) fn next_lsn(&self) -> u64 {
        self.log_manager.next_lsn()
    }

    /// Return the shared log manager for ready/durable LSN waits that must
    /// not hold the journal append mutex while waiting or syncing.
    pub(crate) fn log_manager(&self) -> Arc<LogManager> {
        Arc::clone(&self.log_manager)
    }

    /// Reserve and finalize one Phase 8 log record.
    ///
    /// `&self` so concurrent CRUD reservations need no outer mutex —
    /// [`LogManager::reserve`] owns the LSN-allocation atomic and slot map.
    pub(crate) fn reserve_log_record(
        &self,
        draft: log_file::LogRecordDraft,
    ) -> Result<ReservedLogRecord> {
        Self::reserve_log_record_on(&self.log_manager, draft)
    }

    /// Same as [`reserve_log_record`](Self::reserve_log_record) but driven by
    /// a caller-held [`Arc<LogManager>`] so callers do not need to acquire the
    /// outer journal mutex to reserve a slot.
    pub(crate) fn reserve_log_record_on(
        log_manager: &Arc<LogManager>,
        draft: log_file::LogRecordDraft,
    ) -> Result<ReservedLogRecord> {
        let bytes_len = draft.encoded_len()?;
        let slot = log_manager.reserve(bytes_len)?;
        let record = match draft.finalize(slot.start_lsn()) {
            Ok(record) => record,
            Err(error) => return Err(log_manager.poison_slot(&slot, error)),
        };
        if record.end_lsn() != slot.end_lsn() {
            let error = Error::Internal(format!(
                "finalized log record [{}, {}) did not match reserved slot [{}, {})",
                record.start_lsn(),
                record.end_lsn(),
                slot.start_lsn(),
                slot.end_lsn()
            ));
            return Err(log_manager.poison_slot(&slot, error));
        }
        Ok(ReservedLogRecord::journaled(
            Arc::clone(log_manager),
            slot,
            record,
        ))
    }

    /// Return the journal's database-lifetime salt values `(salt1, salt2)`
    /// for callers that need to stamp legacy logical-frame probes or Phase 8
    /// payloads with the database salts.
    pub(crate) fn salts(&self) -> (u32, u32) {
        (self.salt1, self.salt2)
    }

    /// Return a reference to the in-memory journal index (for inspection in tests).
    pub(crate) fn index(&self) -> &JournalIndex {
        &self.index
    }

    /// Highest `ChainCommit::commit_ts` observed during recovery, or `None`
    /// when the journal was freshly created or carried no ChainCommit
    /// frames. The MVCC backend uses this to floor the HLC oracle at
    /// `max.successor()` so that every post-recovery `commit()` is
    /// strictly greater than any durable commit from the previous lifetime.
    pub(crate) fn recovered_max_commit_ts(&self) -> Option<Ts> {
        self.recovered_max_commit_ts
    }

    /// Highest non-control Phase 8 `publish_seq` observed during recovery.
    pub(crate) fn recovered_max_publish_seq(&self) -> Option<u64> {
        self.recovered_max_publish_seq
    }

    /// Take the `ParsedLogicalFrames` collected during Pass 1 recovery
    /// (§5.3). Leaves `Default::default()` in its place so the second call
    /// returns an empty struct. Consumed exactly once by Pass 2 in
    /// [`SharedState::new`](crate::storage::paged_engine::state::SharedState::new).
    pub(crate) fn take_parsed_logical_frames(&mut self) -> ParsedLogicalFrames {
        std::mem::take(&mut self.parsed_logical_frames)
    }

    /// Returns `true` if journal recovery wrote at least one committed page
    /// batch to the main database file during `open_or_create`.
    ///
    /// Used by `Client::open_with_options` to decide whether to re-read page 0
    /// after recovery (the catalog_root_page in the pre-recovery header may be
    /// stale if recovery updated page 0).
    pub(crate) fn did_recover_pages(&self) -> bool {
        self.last_committed_db_page_count.is_some()
    }

    // -----------------------------------------------------------------------
    // Rollback
    // -----------------------------------------------------------------------

    /// Truncate the journal back to `cursor` bytes and reset every LSN
    /// frontier in the unified [`LogManager`].
    ///
    /// `cursor` must be a byte offset previously obtained from
    /// [`write_cursor`](Self::write_cursor) at the start of a transaction.
    /// All log records written since that mark are dropped; this is the
    /// rollback primitive used by [`crate::storage::paged_engine::PagedEngine`]
    /// when a mutator returns an error. The unified record stream is
    /// self-describing via `total_len`, so recovery scans the surviving
    /// records on the next open — no in-memory index to rebuild here.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn truncate_to(&mut self, cursor: u64) -> Result<()> {
        let next_lsn = self.log_manager.next_lsn();
        if cursor < JOURNAL_HEADER_SIZE as u64 || cursor > next_lsn {
            return Err(Error::Internal(format!(
                "journal truncate_to: cursor {cursor} out of range \
                 [{JOURNAL_HEADER_SIZE}, {next_lsn}]"
            )));
        }
        self.log_manager.check_poisoned()?;

        self.journal_file.set_len(cursor).map_err(Error::Io)?;
        self.journal_file.flush().map_err(Error::Io)?;
        self.log_manager.reset_to(cursor);
        self.last_committed_db_page_count = None;
        self.checkpoint_batch_active = None;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Truncate the journal file to just its 32-byte header and reposition the
    /// write cursor.
    fn truncate_journal(&mut self) -> Result<()> {
        self.log_manager.check_poisoned()?;
        self.journal_file
            .seek(SeekFrom::Start(0))
            .map_err(Error::Io)?;

        // Re-write header with incremented checkpoint sequence.
        self.checkpoint_seq = self.checkpoint_seq.wrapping_add(1);
        let mut header = JournalHeader::new(self.salt1, self.salt2);
        header.checkpoint_seq = self.checkpoint_seq;
        self.journal_file
            .write_all(&header.to_bytes())
            .map_err(Error::Io)?;
        self.journal_file
            .set_len(JOURNAL_HEADER_SIZE as u64)
            .map_err(Error::Io)?;
        self.journal_file.flush().map_err(Error::Io)?;

        self.write_cursor = JOURNAL_HEADER_SIZE as u64;
        self.log_manager.reset_to(self.write_cursor);
        self.legacy_pending_start_offset = None;
        self.last_legacy_commit_end_offset = self.write_cursor;
        self.checkpoint_batch_active = None;
        self.checkpoint_frame_tags.clear();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

/// Derive the journal path from the main database path.
pub(crate) fn journal_path_for(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-journal");
    PathBuf::from(p)
}

// ---------------------------------------------------------------------------
// Internal I/O helper
// ---------------------------------------------------------------------------

/// Write `page_data` for `page_number` into the main database file.
///
/// The byte offset into the main file is computed from the page number and
/// its size.  Page 0 (the file header) is always 4 KB.  All other pages
/// occupy their natural size (`page_size_bytes`).
///
/// **Assumption**: the main file uses contiguous page layout where page N
/// starts at `N * page_size_bytes` (with page 0 always being 4 KB).  In the
/// dual-page-size model, page numbers are allocated by the allocator which
/// tracks size separately.  For journal replay, we rely on the `page_size_bytes`
/// recorded in the journal frame rather than deriving it from the page number.
pub(crate) fn write_page_to_main(
    main_file: &mut File,
    page_number: u32,
    _page_size_bytes: usize,
    page_data: &[u8],
) -> Result<()> {
    // The main file uses a uniform 32 KB slot for every page regardless of its
    // actual size (4 KB internal nodes or 32 KB leaf/overflow pages).  Using
    // `page_size_bytes` as the stride would write 4 KB pages at wrong offsets.
    let offset = page_number as u64 * PAGE_SIZE_LEAF as u64;
    main_file.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;
    main_file.write_all(page_data).map_err(Error::Io)?;
    Ok(())
}

#[cfg(test)]
#[path = "tests/journal_manager.rs"]
mod tests_extracted;

#[cfg(test)]
#[path = "tests/header_format.rs"]
mod header_format;

#[cfg(test)]
#[path = "tests/checkpoint_boundary_recovery.rs"]
mod checkpoint_boundary_recovery;
