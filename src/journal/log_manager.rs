//! Phase 8 byte-LSN log manager — reservation, positioned write, and the
//! leader-elected group-commit fsync.
//!
//! This module is the timing-sensitive heart of mqlite's MWMR (multi-writer,
//! multi-namespace) commit hot path. Read this doc before touching anything
//! here: the slot state machine, the contiguous ready frontier, and the
//! wait/notify protocol are correctness- *and* throughput-critical.
//!
//! ## Byte-LSN == file offset
//!
//! There is no separate logical sequence number. A record's LSN *is* the
//! absolute byte offset where it lives in the journal file. [`LogManager`]
//! hands out a disjoint `[start_lsn, end_lsn)` byte range per record via
//! [`reserve`](LogManager::reserve) (a single atomic bump of `next_lsn`), and
//! the record bytes are written at exactly that offset with positioned I/O
//! ([`PositionedLogFile::write_all_at`]). Because reservation only needs a
//! length, many writers reserve concurrently without serializing behind a
//! journal append mutex — that is what makes the multi-writer path scale.
//!
//! ## The slot state machine
//!
//! Each reserved byte range owns one [`LogSlotState`] in the `slots` map and
//! advances through a fixed lifecycle:
//!
//! ```text
//! Reserved → Writing → WriteComplete → Written
//!     │         │            │
//!     └─────────┴────────────┴──────────────────→ Poisoned (on any failure)
//! ```
//!
//! - **Reserved**: byte range allocated, no bytes on disk yet.
//! - **Writing**: a `write_reserved` call is in flight for this slot. The
//!   transition `Reserved → Writing` happens under the slots mutex *before*
//!   the positioned write, so a concurrent failure can never observe a
//!   half-written `Reserved` slot as ready.
//! - **WriteComplete**: positioned bytes are durable to the page cache; the
//!   `Writing → WriteComplete` transition is re-checked under the mutex after
//!   the write so a poisoning peer is observed.
//! - **Written**: [`mark_written`](LogManager::mark_written) has accepted the
//!   slot into the contiguous frontier (see below).
//! - **Poisoned**: any post-reservation failure poisons the *manager* and the
//!   slot. A reserved-but-never-written gap must never be skipped by the ready
//!   frontier, so a poisoned hole stops frontier advancement permanently.
//!
//! ## The contiguous ready frontier
//!
//! `ready_lsn` is the high-water byte offset such that *every* byte below it
//! belongs to a fully `Written` record with no gaps. `mark_written` walks the
//! slot map forward from `ready_lsn`, consuming consecutive `Written` slots
//! and advancing `ready_lsn` to each slot's `end_lsn`. A slot that is written
//! out of LSN order parks in the map until the earlier slots fill in — so the
//! frontier only ever exposes a crash-consistent prefix. Recovery replays the
//! contiguous prefix and discards any trailing gap, so this invariant is the
//! durability contract: a reserved-but-unwritten hole below a written record
//! must never let that later record become ready.
//!
//! ## Leader-elected fdatasync (group commit)
//!
//! Many writers may need their bytes durable at once. Rather than each
//! writer issuing its own `fdatasync`, exactly one becomes the *sync leader*
//! via a `compare_exchange` on `sync_in_progress`; the rest park on `sync_cv`.
//! The leader briefly waits to gather a cohort (bounded by
//! [`LSN_GROUP_COMMIT_MAX_WAIT_MS`]), snapshots the ready frontier as its sync
//! target, issues a single `sync_data()`, publishes `durable_lsn`, then wakes
//! every waiter. One syscall makes a whole batch of commits durable — the
//! throughput win of group commit — while still guaranteeing each caller's
//! `end_lsn` is durable before [`ensure_sync`](LogManager::ensure_sync)
//! returns. A sync failure poisons the manager so no caller mistakes a failed
//! batch for durable.

#![allow(clippy::expect_used)]

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex as ParkingMutex};

use crate::error::{EngineFatalReason, Error, Result};
#[cfg(any(test, feature = "test-hooks"))]
use crate::storage::paged_engine::group_commit_observations;

use super::append_sync_observations;
use super::wire::record::FinalizedLogRecord;

// ---------------------------------------------------------------------------
// Positioned log I/O
// ---------------------------------------------------------------------------

/// Positioned write and sync operations used by the Phase 8 log manager.
pub(crate) trait PositionedLogIo: Send + Sync {
    /// Write some bytes from `data` at absolute byte offset `offset`.
    ///
    /// Returning a short byte count is allowed; callers are responsible for
    /// retrying until the full buffer is written or an error is returned.
    ///
    /// # Errors
    ///
    /// Returns any OS or test-injected write error.
    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<usize>;

    /// Sync log file data to stable storage.
    ///
    /// # Errors
    ///
    /// Returns any OS or test-injected sync error.
    fn sync_data(&self) -> io::Result<()>;
}

impl PositionedLogIo for File {
    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<usize> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            FileExt::write_at(self, data, offset)
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            self.seek_write(data, offset)
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = (offset, data);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "positioned log writes are unsupported on this platform",
            ))
        }
    }

    fn sync_data(&self) -> io::Result<()> {
        File::sync_data(self)
    }
}

/// File wrapper that writes log bytes at explicit offsets.
pub(crate) struct PositionedLogFile {
    io: Box<dyn PositionedLogIo>,
}

impl PositionedLogFile {
    /// Create a positioned log writer from a file handle.
    pub(crate) fn new(file: File) -> Self {
        Self { io: Box::new(file) }
    }

    /// Create a positioned log writer from a test or adapter implementation.
    pub(crate) fn from_io(io: Box<dyn PositionedLogIo>) -> Self {
        Self { io }
    }

    /// Write all bytes from `data` at absolute byte offset `offset`.
    ///
    /// Short positioned writes are retried. A zero-length progress report is
    /// converted to [`io::ErrorKind::WriteZero`] so callers never mark a
    /// partially written slot complete.
    ///
    /// # Errors
    ///
    /// Returns any write error from the underlying positioned writer or
    /// [`io::ErrorKind::WriteZero`] if the writer made no progress.
    pub(crate) fn write_all_at(&self, mut offset: u64, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            let written = self.io.write_at(offset, data)?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "positioned log write made no progress",
                ));
            }
            offset = offset.checked_add(written as u64).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "positioned log offset overflow",
                )
            })?;
            data = &data[written..];
        }
        Ok(())
    }

    /// Sync log file data to stable storage.
    ///
    /// # Errors
    ///
    /// Returns any sync error from the underlying writer.
    pub(crate) fn sync_data(&self) -> io::Result<()> {
        self.io.sync_data()
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
///
/// A journaled record owns a `Reserved` entry in the manager's slot map. That
/// entry MUST be resolved exactly once — either `write_and_mark` (→ `Written`)
/// or `poison_slot` (→ `Poisoned`) — before the value is dropped. A
/// `Reserved` hole that is never resolved permanently stops the contiguous
/// ready frontier (`mark_written`'s forward walk only consumes `Written`
/// slots), wedging `wait_ready` / `ensure_sync` for that LSN and every LSN
/// above it forever. Production commit paths hand-route this correctly, but
/// the `Drop` backstop below converts any forgotten resolution into a manager
/// poison instead of a silent durability wedge.
pub(crate) struct ReservedLogRecord {
    log_manager: Option<Arc<LogManager>>,
    slot: Option<LogSlot>,
    record: FinalizedLogRecord,
    /// Set once the owned slot has been resolved (written or poisoned), so the
    /// `Drop` backstop does not double-poison an already-finalized slot.
    resolved: std::cell::Cell<bool>,
}

impl ReservedLogRecord {
    pub(super) fn journaled(
        log_manager: Arc<LogManager>,
        slot: LogSlot,
        record: FinalizedLogRecord,
    ) -> Self {
        Self {
            log_manager: Some(log_manager),
            slot: Some(slot),
            record,
            resolved: std::cell::Cell::new(false),
        }
    }

    pub(crate) fn journalless(record: FinalizedLogRecord) -> Self {
        Self {
            log_manager: None,
            slot: None,
            record,
            // A journal-less record owns no slot, so there is nothing for the
            // Drop backstop to resolve.
            resolved: std::cell::Cell::new(true),
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
            (Some(log_manager), Some(slot)) => {
                self.resolved.set(true);
                log_manager.poison_slot(slot, error)
            }
            _ => error,
        }
    }

    /// Write the finalized bytes and mark the record written.
    pub(crate) fn write_and_mark(&self) -> Result<u64> {
        match (&self.log_manager, &self.slot) {
            (Some(log_manager), Some(slot)) => {
                // The slot transitions away from `Reserved` inside
                // `write_reserved`/`mark_written`; mark it resolved up front so
                // the Drop backstop never re-poisons a slot whose write failed
                // and already poisoned the manager via the calls below.
                self.resolved.set(true);
                log_manager.write_reserved(slot, self.record.bytes())?;
                let receipt = log_manager.mark_written(slot)?;
                Ok(receipt.end_lsn())
            }
            _ => Ok(self.record.end_lsn()),
        }
    }
}

impl Drop for ReservedLogRecord {
    /// Mechanical durability backstop. A journaled record that reaches `Drop`
    /// without `write_and_mark` or `poison_slot` would otherwise leave its slot
    /// permanently `Reserved`, stopping the contiguous ready frontier and
    /// wedging `ensure_sync` for that LSN and every LSN above it forever (no
    /// error, no recovery). Poisoning the slot here converts that silent wedge
    /// into an observable `EngineFatal` so the abandoned reservation surfaces.
    ///
    /// LOCK ORDER WARNING: the poison path acquires the manager's `slots`
    /// mutex (non-reentrant). Never let an unresolved `ReservedLogRecord`
    /// drop while holding that lock — e.g. from inside a `LogManager` method
    /// or a closure run under `slots` — or the backstop self-deadlocks
    /// instead of poisoning.
    fn drop(&mut self) {
        if self.resolved.get() {
            return;
        }
        if let (Some(log_manager), Some(slot)) = (&self.log_manager, &self.slot) {
            let _ = log_manager.poison_slot(
                slot,
                Error::Internal(
                    "ReservedLogRecord dropped without write_and_mark or poison_slot".into(),
                ),
            );
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
                        append_sync_observations::record_handle_journal_sync();
                        append_sync_observations::record_journal_sync_os_boundary();
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
    pub(crate) fn probe_id(&self) -> u64 {
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

    pub(crate) fn check_poisoned(&self) -> Result<()> {
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
