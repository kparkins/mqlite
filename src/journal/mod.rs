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
//! Every journal write is reserved through [`LogManager`] as a Phase 8
//! `LogRecord`; the journal is the only durable artifact and recovery scans
//! replay any committed batch and discard trailing uncommitted records.
//! Durability is provided by explicit sync ownership at higher-level
//! durability boundaries.

// Crate convention: `expect("N bytes")` on infallible array slices is used
// throughout the journal module to keep the code readable and is acknowledged
// as a non-issue by the team. The clippy lint is allowed at the module
// boundary so denylist-mode CI does not trip on the pre-existing pattern.
#![allow(clippy::expect_used)]

#[path = "tests/append_sync_observations.rs"]
pub(crate) mod append_sync_observations;
pub(crate) mod checkpoint_batch;
pub(crate) mod log_manager;
pub(crate) mod metrics;
pub(crate) mod wire;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/logical_replay_fixtures.rs"]
pub(crate) mod logical_replay_fixtures;
mod recovery;

use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::storage::header::FileHeader;
use crate::storage::page::PAGE_SIZE_LEAF;

pub(crate) use self::recovery::ParsedLogicalFrames;

// R4 split: the byte-LSN log manager (reserve/write/group-commit) moved to
// `log_manager.rs`; the checkpoint-batch handshake types moved to
// `checkpoint_batch.rs`. They are re-exported here so every historical
// `crate::journal::{LogManager, LogSlot, BoundaryAppended, ...}` path resolves
// unchanged.
// `CheckpointBatchId` stays live in every config: the `checkpoint_batch_active`
// field below holds one and the production checkpoint path consumes batch ids.
pub(crate) use self::checkpoint_batch::CheckpointBatchId;
// QUARANTINED dormant US-005 producer re-exports — see docs/staged-work/us-005-incremental-checkpoint.md
#[allow(unused_imports)]
#[cfg(any(
    test,
    feature = "test-hooks",
    feature = "us005-incremental-checkpoint"
))]
pub(crate) use self::checkpoint_batch::{BoundaryAppended, CheckpointBatchCursor};
#[allow(unused_imports)]
#[cfg(any(test, feature = "us005-incremental-checkpoint"))]
pub(crate) use self::checkpoint_batch::CheckpointFlushSet;
#[allow(unused_imports)]
pub(crate) use self::log_manager::{
    LogManager, LogSlot, LogWriteReceipt, PositionedLogFile, PositionedLogIo, ReservedLogRecord,
};

// R4 item 7: `CheckpointPoolKind` and `wire::CheckpointPagePool` were
// byte-identical enums (`{ Main, History }`, identical derives, identical
// serialized bytes Main=0/History=1). They are unified to the single wire
// enum, surfaced under the historical journal-facing name. The journal→wire
// conversion `match` is deleted; the one type flows straight through.
//
// QUARANTINED dormant US-005 producer imports — see docs/staged-work/us-005-incremental-checkpoint.md
// `CheckpointPoolKind` and `JournalPageSize` are consumed only by the gated
// per-page checkpoint producers below.
#[cfg(any(
    test,
    feature = "test-hooks",
    feature = "us005-incremental-checkpoint"
))]
pub(crate) use self::wire::CheckpointPagePool as CheckpointPoolKind;
#[cfg(any(
    test,
    feature = "test-hooks",
    feature = "us005-incremental-checkpoint"
))]
use self::wire::JournalPageSize;

use self::wire::{JournalHeader, JournalOffset, JOURNAL_HEADER_SIZE};

// ---------------------------------------------------------------------------
// JournalManager
// ---------------------------------------------------------------------------

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
    /// Salt 1 from the main file header (stored in every journal frame).
    pub(super) salt1: u32,
    /// Salt 2 from the main file header.
    pub(super) salt2: u32,
    /// Checkpoint sequence counter from the journal file header.
    pub(super) checkpoint_seq: u32,
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
    /// Open checkpoint batch id/start, if step-8 flushing is active.
    pub(super) checkpoint_batch_active: Option<(CheckpointBatchId, JournalOffset)>,
    /// Next in-process checkpoint batch id.
    pub(super) next_checkpoint_batch_id: u64,
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
            salt1,
            salt2,
            checkpoint_seq: 0,
            log_manager: Arc::new(LogManager::new(
                log_manager_file,
                JOURNAL_HEADER_SIZE as u64,
            )),
            last_committed_db_page_count: None,
            recovered_max_commit_ts: None,
            recovered_max_publish_seq: None,
            parsed_logical_frames: ParsedLogicalFrames::default(),
            checkpoint_batch_active: None,
            next_checkpoint_batch_id: 1,
        })
    }

    // -----------------------------------------------------------------------
    // Writing (appending frames)
    // -----------------------------------------------------------------------

    /// Return the batch id that the next checkpoint batch will receive.
    // QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
    #[cfg(any(test, feature = "us005-incremental-checkpoint"))]
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
    // QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
    #[cfg(any(test, feature = "us005-incremental-checkpoint"))]
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
    // QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
    #[cfg(any(test, feature = "us005-incremental-checkpoint"))]
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
    // QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
    // Widened to include `test-hooks` because the `test-hooks`-gated test
    // fixtures `append_checkpoint_frame` / `append_checkpoint_commit_boundary`
    // call it; the callee must exist wherever its callers compile.
    #[cfg(any(
        test,
        feature = "test-hooks",
        feature = "us005-incremental-checkpoint"
    ))]
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
        let payload = wire::CheckpointPageFramePayload {
            batch_id: batch_id.as_u64(),
            pool,
            page_number,
            page_size,
            data: page_data.to_vec(),
        }
        .encode()?;
        let draft = wire::LogRecordDraft::checkpoint_page_frame(Ts::default(), payload);
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
    // QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
    #[cfg(any(
        test,
        feature = "test-hooks",
        feature = "us005-incremental-checkpoint"
    ))]
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
    // QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
    #[cfg(any(
        test,
        feature = "test-hooks",
        feature = "us005-incremental-checkpoint"
    ))]
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
        let payload = wire::CheckpointBoundaryPayload {
            checkpoint_applied_lsn: staged_header.checkpoint_applied_lsn,
            batch_id: batch_id.as_u64(),
            header: staged_header.clone(),
        }
        .encode()?;
        let draft = wire::LogRecordDraft::checkpoint_boundary(0, checkpoint_ts, payload);
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
        draft: wire::LogRecordDraft,
    ) -> Result<ReservedLogRecord> {
        Self::reserve_log_record_on(&self.log_manager, draft)
    }

    /// Same as [`reserve_log_record`](Self::reserve_log_record) but driven by
    /// a caller-held [`Arc<LogManager>`] so callers do not need to acquire the
    /// outer journal mutex to reserve a slot.
    pub(crate) fn reserve_log_record_on(
        log_manager: &Arc<LogManager>,
        draft: wire::LogRecordDraft,
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
/// The main file uses a **uniform 32 KB slot for every page**, so a page's
/// byte offset is always `page_number * PAGE_SIZE_LEAF` regardless of whether
/// it is a 4 KB internal node or a 32 KB leaf/overflow page. The
/// `page_size_bytes` argument is therefore intentionally unused for offset
/// math: a page number is the only thing that determines where the page lives
/// on disk, and using the logical page size as the stride would compute a
/// wrong offset for every 4 KB page and silently overwrite a neighbour. The
/// fixed-stride layout trades file density for the property that any allocated
/// page number maps to exactly one slot, which keeps allocation, eviction,
/// and journal replay from ever having to reconcile two different offset
/// formulas for the same page.
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

#[cfg(test)]
#[path = "tests/bug_recovery_inner_payload_corruption.rs"]
mod bug_recovery_inner_payload_corruption;

#[cfg(test)]
#[path = "tests/bug_recovery_main_file_durability.rs"]
mod bug_recovery_main_file_durability;

#[cfg(test)]
#[path = "tests/bug_recovery_chain_payload_corruption.rs"]
mod bug_recovery_chain_payload_corruption;

#[cfg(test)]
#[path = "tests/bug_recovery_outer_semantic_corruption.rs"]
mod bug_recovery_outer_semantic_corruption;

#[cfg(test)]
#[path = "tests/bugsuspect_journal_reserved_drop_wedge.rs"]
mod bugsuspect_journal_reserved_drop_wedge;
