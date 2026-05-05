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

#[allow(dead_code)]
pub(crate) mod log_file;
mod recovery;
#[allow(dead_code)]
pub(crate) mod shm;
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) mod us018_test_probe;
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) mod us039_test_probe;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::storage::buffer_pool::{PageSize, PageSource};
use crate::storage::header::FileHeader;
use crate::storage::page::PAGE_SIZE_LEAF;

use self::shm::JournalIndex;

pub(crate) use self::recovery::ParsedLogicalFrames;

use self::log_file::{
    try_skip_chain_commit, try_skip_logical_txn, CheckpointBatchPageRecord, JournalFrameHeader,
    JournalHeader, JournalOffset, JournalPageSize, LogicalTxnFrame, Page0BoundaryRecord, PageId,
    JOURNAL_FRAME_HEADER_SIZE, JOURNAL_HEADER_SIZE,
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
    #[allow(dead_code)]
    pub(crate) fn journal_offset(&self) -> JournalOffset {
        self.journal_offset
    }

    /// Checkpoint timestamp covered by the boundary.
    #[allow(dead_code)]
    pub(crate) fn checkpoint_ts(&self) -> Ts {
        self.checkpoint_ts
    }
}

/// Monotonic identity for a checkpoint-owned journal batch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CheckpointBatchId(u64);

impl CheckpointBatchId {
    /// Return the numeric batch identity.
    #[allow(dead_code)]
    pub(crate) fn get(self) -> u64 {
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
    #[allow(dead_code)]
    pub(crate) fn expected_pending_start(&self) -> JournalOffset {
        self.expected_pending_start
    }

    /// Clean journal cursor observed before the batch opened.
    #[allow(dead_code)]
    pub(crate) fn clean_start_offset(&self) -> JournalOffset {
        self.clean_start_offset
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

        Ok(Self {
            journal_path,
            journal_file,
            index: JournalIndex::new(),
            salt1,
            salt2,
            checkpoint_seq: 0,
            write_cursor: JOURNAL_HEADER_SIZE as u64,
            last_committed_db_page_count: None,
            recovered_max_commit_ts: None,
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

    /// Append a non-commit journal frame for `page_number`.
    ///
    /// Call this for each modified page within a transaction, then call
    /// [`commit`](Self::commit) when the transaction is complete.
    ///
    /// `page_data` must be exactly `page_size.bytes()` bytes.
    pub(crate) fn append_non_commit(
        &mut self,
        page_number: u32,
        page_size: JournalPageSize,
        page_data: &[u8],
    ) -> Result<u64> {
        debug_assert_eq!(page_data.len(), page_size.bytes());
        self.append_frame(page_number, 0, page_size, page_data, None)
    }

    /// Return the batch id that the next checkpoint batch will receive.
    pub(crate) fn next_checkpoint_batch_id(&self) -> CheckpointBatchId {
        CheckpointBatchId(self.next_checkpoint_batch_id)
    }

    /// Open a checkpoint-owned pending range at the current clean cursor.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if an ordinary legacy page-frame range is
    /// pending or another checkpoint batch is already open.
    pub(crate) fn begin_checkpoint_batch(&mut self) -> Result<CheckpointBatchCursor> {
        if let Some(start) = self.legacy_pending_start_offset {
            return Err(Error::Internal(format!(
                "cannot begin checkpoint batch with legacy pending range at {start}"
            )));
        }
        if self.checkpoint_batch_active.is_some() {
            return Err(Error::Internal("checkpoint batch already active".into()));
        }
        let batch_id = CheckpointBatchId(self.next_checkpoint_batch_id);
        self.next_checkpoint_batch_id = self.next_checkpoint_batch_id.saturating_add(1);
        let clean_start_offset = self.write_cursor;
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
        if self.write_cursor != cursor.clean_start_offset {
            return;
        }
        if self.checkpoint_batch_active == Some((cursor.batch_id, cursor.clean_start_offset)) {
            self.checkpoint_batch_active = None;
            self.checkpoint_frame_tags.clear();
        }
    }

    /// Append a checkpoint-owned page frame tagged with `batch_id`.
    pub(crate) fn append_checkpoint_frame(
        &mut self,
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
        self.append_frame(
            page_number,
            0,
            page_size,
            page_data,
            Some(CheckpointFrameTag {
                batch_id,
                pool,
                page_id: PageId(page_number),
            }),
        )
    }

    /// Append an MVCC `ChainCommit` frame to the journal.
    ///
    /// Emits one `ChainCommitFrame` carrying `commit_ts`, `refcount_deltas`,
    /// and zero or more `page_writes`. The frame is written at the current
    /// `write_cursor`; the cursor advances past the encoded frame. Durability
    /// belongs to the caller's explicit sync boundary, not to this append path.
    ///
    /// The in-memory index is NOT updated — `ChainCommit` frames carry no
    /// single page number (every `page_writes` entry has its own). Recovery
    /// scans `ChainCommit` frames linearly.
    pub(crate) fn append_chain_commit(
        &mut self,
        commit_ts: crate::mvcc::timestamp::Ts,
        refcount_deltas: Vec<(u32, i32)>,
        page_writes: Vec<crate::journal::log_file::ChainPageWrite>,
    ) -> Result<u64> {
        if self.checkpoint_batch_active.is_some() {
            return Err(Error::Internal(
                "chain commit cannot be appended inside checkpoint batch".into(),
            ));
        }
        let frame = crate::journal::log_file::ChainCommitFrame {
            salt1: self.salt1,
            salt2: self.salt2,
            commit_ts,
            refcount_deltas,
            page_writes,
        };
        let bytes = frame.encode()?;
        let frame_offset = self.write_cursor;
        self.journal_file
            .seek(SeekFrom::Start(frame_offset))
            .map_err(Error::Io)?;
        self.journal_file.write_all(&bytes).map_err(Error::Io)?;
        self.write_cursor += bytes.len() as u64;
        Ok(frame_offset)
    }

    /// Append a `LogicalTxnFrame` to the journal (§6.4). Returns the byte
    /// offset at which the frame was written.
    ///
    /// Encodes before any file I/O, so an oversize frame returns
    /// [`Error::JournalFrameTooLarge`] without touching the journal.
    ///
    /// The in-memory [`JournalIndex`] is not updated: logical frames carry
    /// no `page_number` and recovery scans them linearly (§5).
    pub(crate) fn append_logical_txn(&mut self, frame: LogicalTxnFrame) -> Result<u64> {
        if self.checkpoint_batch_active.is_some() {
            return Err(Error::Internal(
                "logical transaction cannot be appended inside checkpoint batch".into(),
            ));
        }
        let bytes = frame.encode()?;
        let frame_offset = self.write_cursor;
        self.journal_file
            .seek(SeekFrom::Start(frame_offset))
            .map_err(Error::Io)?;
        self.journal_file.write_all(&bytes).map_err(Error::Io)?;
        self.write_cursor += bytes.len() as u64;
        crate::mvcc::metrics::record_logical_txn_append_bytes(bytes.len() as u64);
        // §7 / US-024 AC#3 — duration timing is sampled OUTSIDE the
        // journal critical section by `LogicalTxnAppendPercentileRefresh`
        // in `src/storage/paged_engine.rs::run_write_existing`. The
        // RAII guard captures `Instant::now()` BEFORE acquiring the
        // journal_mutex and samples elapsed AFTER releasing it
        // (LIFO drop order). No `Instant::now()` inside the journal
        // critical section.
        Ok(frame_offset)
    }

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
        self.validate_active_checkpoint_batch_before_boundary(&checkpoint_batch)?;
        let record = Page0BoundaryRecord::new(self.salt1, self.salt2, staged_header.clone());
        let frame_offset = self.write_cursor;
        self.journal_file
            .seek(SeekFrom::Start(frame_offset))
            .map_err(Error::Io)?;
        record.write(&mut self.journal_file).map_err(Error::Io)?;
        self.journal_file.flush().map_err(Error::Io)?;
        self.write_cursor += (JOURNAL_FRAME_HEADER_SIZE + JournalPageSize::Small4k.bytes()) as u64;
        self.last_committed_db_page_count = Some(staged_header.total_page_count);
        self.checkpoint_batch_active = None;
        self.checkpoint_frame_tags.clear();
        Ok(BoundaryAppended {
            journal_offset: frame_offset,
            db_page_count: record.db_page_count(),
            checkpoint_ts: record.checkpoint_ts(),
            _private: (),
        })
    }

    /// Append a commit journal frame, completing the current transaction.
    ///
    /// `db_page_count` is the total number of database pages after this commit
    /// (stored in the commit frame so recovery can update the main file header).
    ///
    /// After this call, the in-memory index is updated.  Returns `true`
    /// if an emergency checkpoint should be triggered (the journal index
    /// has reached the hot-threshold).
    pub(crate) fn commit(
        &mut self,
        page_number: u32,
        page_size: JournalPageSize,
        page_data: &[u8],
        db_page_count: u32,
    ) -> Result<bool> {
        debug_assert!(
            db_page_count > 0,
            "commit frame must have non-zero page count"
        );
        let offset = self.append_frame(page_number, db_page_count, page_size, page_data, None)?;
        self.last_committed_db_page_count = Some(db_page_count);
        self.legacy_pending_start_offset = None;
        self.last_legacy_commit_end_offset = self.write_cursor;

        // Update the in-memory index with the commit frame's page.
        let emergency = self.index.insert(page_number, offset);

        Ok(emergency)
    }

    /// Low-level frame append.  Returns the byte offset of the written frame.
    fn append_frame(
        &mut self,
        page_number: u32,
        db_page_count: u32,
        page_size: JournalPageSize,
        page_data: &[u8],
        checkpoint_tag: Option<CheckpointFrameTag>,
    ) -> Result<u64> {
        if db_page_count == 0 && checkpoint_tag.is_none() && self.checkpoint_batch_active.is_some()
        {
            return Err(Error::Internal(
                "ordinary page frame cannot be appended inside checkpoint batch".into(),
            ));
        }
        let frame_offset = self.write_cursor;
        self.journal_file
            .seek(SeekFrom::Start(frame_offset))
            .map_err(Error::Io)?;

        let frame_size = if checkpoint_tag.is_some() {
            let record = CheckpointBatchPageRecord {
                page_number,
                salt1: self.salt1,
                salt2: self.salt2,
                page_size,
            };
            record
                .write(&mut self.journal_file, page_data)
                .map_err(Error::Io)?;
            record.total_size()
        } else {
            let frame_hdr = JournalFrameHeader {
                page_number,
                db_page_count,
                salt1: self.salt1,
                salt2: self.salt2,
                page_size,
            };
            frame_hdr
                .write(&mut self.journal_file, page_data)
                .map_err(Error::Io)?;
            frame_hdr.total_size()
        };

        self.write_cursor += frame_size as u64;

        if let Some(tag) = checkpoint_tag {
            self.checkpoint_frame_tags.insert(frame_offset, tag);
            return Ok(frame_offset);
        }

        // Update the in-memory index for non-commit frames too so reads
        // through `JournalLayeredSource` see in-progress writes within the
        // same process. Only the journal file is durable; the index lives
        // in memory and is rebuilt on open.
        if db_page_count == 0 {
            if self.legacy_pending_start_offset.is_none() {
                self.legacy_pending_start_offset = Some(frame_offset);
            }
            self.index.insert(page_number, frame_offset);
        }

        Ok(frame_offset)
    }

    // -----------------------------------------------------------------------
    // Reading
    // -----------------------------------------------------------------------

    /// Look up `page_number` in the journal.
    ///
    /// Returns the page data if found, or `None` if the page should be read
    /// from the main file.
    ///
    /// Uses the in-memory journal index for O(1) lookup.
    pub(crate) fn read_page(&mut self, page_number: u32) -> Result<Option<Vec<u8>>> {
        let frame_offset = match self.index.lookup(page_number) {
            Some(off) => off,
            None => return Ok(None),
        };

        // Read the frame header at the recorded offset.
        self.journal_file
            .seek(SeekFrom::Start(frame_offset))
            .map_err(Error::Io)?;

        let mut header_buf = [0u8; JOURNAL_FRAME_HEADER_SIZE];
        self.journal_file
            .read_exact(&mut header_buf)
            .map_err(Error::Io)?;

        let page_size_u32 = u32::from_le_bytes(header_buf[16..20].try_into().expect("4 bytes"));
        let page_size = JournalPageSize::from_u32(page_size_u32)?;

        let mut page_data = vec![0u8; page_size.bytes()];
        self.journal_file
            .read_exact(&mut page_data)
            .map_err(Error::Io)?;

        Ok(Some(page_data))
    }

    /// Fallback: linear journal scan to find `page_number`.
    ///
    /// O(journal frames) per lookup.  Acceptable with aggressive checkpointing.
    pub(crate) fn read_page_linear(&mut self, page_number: u32) -> Result<Option<Vec<u8>>> {
        self.journal_file
            .seek(SeekFrom::Start(JOURNAL_HEADER_SIZE as u64))
            .map_err(Error::Io)?;

        let mut latest: Option<Vec<u8>> = None;
        let mut cursor = JOURNAL_HEADER_SIZE as u64;

        loop {
            self.journal_file
                .seek(SeekFrom::Start(cursor))
                .map_err(Error::Io)?;

            // Skip ChainCommit frames — they carry no page_number and
            // are invisible to the per-page linear scan.
            if let Some((n, _commit_ts, _offset)) =
                try_skip_chain_commit(&mut self.journal_file, self.salt1, self.salt2)?
            {
                cursor += n;
                continue;
            }

            // Phase 2 §6.5 / US-018: LogicalTxnFrame frames also carry
            // no `page_number` for the per-page linear scan — skip past
            // them so subsequent legacy page frames are still visited.
            if let Some((n, _frame)) =
                try_skip_logical_txn(&mut self.journal_file, self.salt1, self.salt2)?
            {
                cursor += n;
                continue;
            }

            let Some(frame_hdr) =
                JournalFrameHeader::read(&mut self.journal_file, self.salt1, self.salt2)?
            else {
                break;
            };

            let page_size_bytes = frame_hdr.page_size.bytes();
            // Read the page data
            let data_offset = cursor + JOURNAL_FRAME_HEADER_SIZE as u64;
            self.journal_file
                .seek(SeekFrom::Start(data_offset))
                .map_err(Error::Io)?;
            let mut page_data = vec![0u8; page_size_bytes];
            self.journal_file
                .read_exact(&mut page_data)
                .map_err(Error::Io)?;

            if frame_hdr.page_number == page_number {
                // Found a frame for this page — keep it (later frames overwrite earlier)
                latest = Some(page_data);
            }

            cursor = data_offset + page_size_bytes as u64;
        }

        Ok(latest)
    }

    // -----------------------------------------------------------------------
    // Checkpoint
    // -----------------------------------------------------------------------

    /// Checkpoint all committed journal frames into the main file.
    ///
    /// After a successful checkpoint:
    /// 1. The journal file is truncated to just the header.
    /// 2. The in-memory journal index is cleared.
    /// 3. The checkpoint sequence counter is incremented.
    ///
    /// `main_file` must be open for read/write.
    pub(crate) fn checkpoint(
        &mut self,
        main_file: &mut File,
        main_header: &mut FileHeader,
    ) -> Result<()> {
        #[cfg(feature = "tracing")]
        let _chk_start = std::time::Instant::now();
        #[cfg(feature = "tracing")]
        let _journal_size_before = self.write_cursor;

        // Collect all entries from the in-memory index.
        let entries: Vec<(u32, u64)> = self.index.iter_entries().collect();

        // For each indexed page, read the journal frame and write to main file.
        for (page_number, frame_offset) in &entries {
            // Read page_size from the frame header.
            let header_offset = *frame_offset;
            self.journal_file
                .seek(SeekFrom::Start(header_offset))
                .map_err(Error::Io)?;
            let mut hbuf = [0u8; JOURNAL_FRAME_HEADER_SIZE];
            self.journal_file.read_exact(&mut hbuf).map_err(Error::Io)?;
            let page_size_u32 = u32::from_le_bytes(hbuf[16..20].try_into().expect("4 bytes"));
            let page_size_bytes = JournalPageSize::from_u32(page_size_u32)?.bytes();

            let mut page_data = vec![0u8; page_size_bytes];
            self.journal_file
                .read_exact(&mut page_data)
                .map_err(Error::Io)?;

            write_page_to_main(main_file, *page_number, page_size_bytes, &page_data)?;
        }

        // Update main file header with latest committed page count.
        if let Some(db_page_count) = self.last_committed_db_page_count {
            main_header.total_page_count = db_page_count;
        }
        main_file.flush().map_err(Error::Io)?;

        // Reset journal to empty (truncate to just the header).
        self.truncate_journal()?;

        // Clear the in-memory index.
        self.index.clear_index();

        #[cfg(feature = "tracing")]
        {
            let duration_ms = _chk_start.elapsed().as_millis() as u64;
            tracing::info!(
                target: "mqlite",
                pages_copied = entries.len() as u64,
                duration_ms,
                journal_size_before = _journal_size_before,
                "mqlite::checkpoint"
            );
        }

        Ok(())
    }

    /// Copy a durable checkpoint batch into the main file after its page-0
    /// boundary has been appended.
    ///
    /// This is the Phase 7 post-boundary copy primitive. It copies journaled
    /// page frames through the first durable page-0 checkpoint boundary,
    /// fdatasyncs the main file, then truncates the journal. It does not
    /// mutate allocator header state; the boundary page-0 image is already the
    /// durable header authority.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] when the journal does not contain a durable
    /// page-0 boundary or when the boundary page count differs from
    /// `expected_total_page_count`.
    pub(crate) fn emergency_checkpoint_after_boundary(
        &mut self,
        main_file: &mut File,
        expected_total_page_count: u32,
    ) -> Result<()> {
        let boundary_page_count = self
            .checkpoint_boundary_page_count()?
            .ok_or_else(|| Error::Internal("checkpoint boundary not found in journal".into()))?;
        if boundary_page_count != expected_total_page_count {
            return Err(Error::Internal(format!(
                "boundary page count {boundary_page_count} does not match expected \
                 {expected_total_page_count}",
            )));
        }

        self.copy_pages_through_checkpoint_boundary(main_file, expected_total_page_count)?;
        main_file.sync_data().map_err(Error::Io)?;
        self.truncate_journal()?;
        self.index.clear_index();
        Ok(())
    }

    /// Checkpoint all journal frames, then delete the journal file.
    ///
    /// Called on clean database close.  After this returns, only the main
    /// `.mqlite` file remains.
    pub(crate) fn close_and_cleanup(
        mut self,
        main_file: &mut File,
        main_header: &mut FileHeader,
    ) -> Result<()> {
        // Checkpoint everything to main file.
        self.checkpoint(main_file, main_header)?;

        // Delete journal file.
        drop(self.journal_file);
        let _ = std::fs::remove_file(&self.journal_path);

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Durability
    // -----------------------------------------------------------------------

    /// fsync the journal file, making all committed-but-unsynced frames durable.
    ///
    /// Calls `sync_data()` (fdatasync) which is sufficient to guarantee that
    /// journal frame data survives a process crash. Main-file contents are NOT
    /// touched — this is the FullSync hot path, not a checkpoint.
    pub(crate) fn sync_journal(&self) -> Result<()> {
        self.journal_file.sync_data().map_err(Error::Io)?;
        #[cfg(any(test, feature = "test-hooks"))]
        self::us039_test_probe::record_journal_sync_os_boundary();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Return the current journal write cursor (byte offset past the last frame).
    pub(crate) fn write_cursor(&self) -> u64 {
        self.write_cursor
    }

    /// Return the journal's database-lifetime salt values `(salt1, salt2)`
    /// for callers (e.g. `WriteTxn::emit_logical_txn_frame`) that need to
    /// stamp a [`LogicalTxnFrame`] before handing it off to
    /// [`append_logical_txn`](Self::append_logical_txn).
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

    /// Truncate the journal file back to `cursor` bytes and rebuild the
    /// in-memory index so it reflects only the surviving frames.
    ///
    /// `cursor` must be a byte offset previously obtained from
    /// [`write_cursor`](Self::write_cursor) at the start of a transaction.
    /// All frames written since that mark are dropped; this is the rollback
    /// primitive used by [`crate::storage::paged_engine::PagedEngine`] when a
    /// mutator returns an error.
    ///
    /// The index is rebuilt by a linear scan over the surviving frame
    /// range — O(surviving frames) — which is correct regardless of whether
    /// the dropped frames were commit or non-commit frames.
    pub(crate) fn truncate_to(&mut self, cursor: u64) -> Result<()> {
        if cursor < JOURNAL_HEADER_SIZE as u64 || cursor > self.write_cursor {
            return Err(Error::Internal(format!(
                "journal truncate_to: cursor {cursor} out of range \
                 [{JOURNAL_HEADER_SIZE}, {}]",
                self.write_cursor
            )));
        }

        self.journal_file.set_len(cursor).map_err(Error::Io)?;
        self.journal_file.flush().map_err(Error::Io)?;
        self.write_cursor = cursor;

        self.index.clear_index();
        self.journal_file
            .seek(SeekFrom::Start(JOURNAL_HEADER_SIZE as u64))
            .map_err(Error::Io)?;
        let mut scan = JOURNAL_HEADER_SIZE as u64;
        let mut latest_commit_pages: Option<u32> = None;
        while scan < cursor {
            self.journal_file
                .seek(SeekFrom::Start(scan))
                .map_err(Error::Io)?;

            // ChainCommit frames are part of the durable log but carry no
            // `page_number` for the in-memory index. Skip past them so
            // `JournalFrameHeader::read` below sees only legacy frames.
            if let Some((n, _commit_ts, _offset)) =
                try_skip_chain_commit(&mut self.journal_file, self.salt1, self.salt2)?
            {
                scan += n;
                continue;
            }

            // Phase 2 §6.5 / US-018: LogicalTxnFrame carries no page_number
            // for the legacy per-page index, but truncate_to must skip past
            // it so the scan continues into any following legacy page
            // frames. Without this skip, a rollback (rollback_txn →
            // truncate_to) after a successful CRUD commit would rebuild
            // the index halted at the logical frame and lose every
            // legacy page frame written after it.
            if let Some((n, _frame)) =
                try_skip_logical_txn(&mut self.journal_file, self.salt1, self.salt2)?
            {
                scan += n;
                continue;
            }

            let Some(frame_hdr) =
                JournalFrameHeader::read(&mut self.journal_file, self.salt1, self.salt2)?
            else {
                break;
            };
            self.index.insert(frame_hdr.page_number, scan);
            if frame_hdr.db_page_count > 0 {
                latest_commit_pages = Some(frame_hdr.db_page_count);
            }
            scan += (JOURNAL_FRAME_HEADER_SIZE + frame_hdr.page_size.bytes()) as u64;
        }
        self.last_committed_db_page_count = latest_commit_pages;
        self.legacy_pending_start_offset = None;
        self.last_legacy_commit_end_offset = self.write_cursor;
        self.checkpoint_batch_active = None;
        self.checkpoint_frame_tags.clear();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn checkpoint_boundary_page_count(&mut self) -> Result<Option<u32>> {
        self.journal_file
            .seek(SeekFrom::Start(JOURNAL_HEADER_SIZE as u64))
            .map_err(Error::Io)?;
        let mut cursor = JOURNAL_HEADER_SIZE as u64;

        loop {
            self.journal_file
                .seek(SeekFrom::Start(cursor))
                .map_err(Error::Io)?;
            if let Some((n, _commit_ts, _offset)) =
                try_skip_chain_commit(&mut self.journal_file, self.salt1, self.salt2)?
            {
                cursor += n;
                continue;
            }
            if let Some((n, _frame)) =
                try_skip_logical_txn(&mut self.journal_file, self.salt1, self.salt2)?
            {
                cursor += n;
                continue;
            }

            let Some(frame_hdr) =
                JournalFrameHeader::read(&mut self.journal_file, self.salt1, self.salt2)?
            else {
                return Ok(None);
            };
            let page_size_bytes = frame_hdr.page_size.bytes();
            let data_offset = cursor + JOURNAL_FRAME_HEADER_SIZE as u64;
            self.journal_file
                .seek(SeekFrom::Start(data_offset))
                .map_err(Error::Io)?;
            let mut page_data = vec![0u8; page_size_bytes];
            self.journal_file
                .read_exact(&mut page_data)
                .map_err(Error::Io)?;

            if let Some(boundary) = Page0BoundaryRecord::from_page_frame_parts(
                frame_hdr.page_number,
                frame_hdr.db_page_count,
                frame_hdr.salt1,
                frame_hdr.salt2,
                frame_hdr.page_size,
                &page_data,
            )? {
                return Ok(Some(boundary.db_page_count()));
            }

            cursor = data_offset + page_size_bytes as u64;
        }
    }

    fn copy_pages_through_checkpoint_boundary(
        &mut self,
        main_file: &mut File,
        expected_total_page_count: u32,
    ) -> Result<()> {
        self.journal_file
            .seek(SeekFrom::Start(JOURNAL_HEADER_SIZE as u64))
            .map_err(Error::Io)?;
        let mut cursor = JOURNAL_HEADER_SIZE as u64;

        loop {
            self.journal_file
                .seek(SeekFrom::Start(cursor))
                .map_err(Error::Io)?;
            if let Some((n, _commit_ts, _offset)) =
                try_skip_chain_commit(&mut self.journal_file, self.salt1, self.salt2)?
            {
                cursor += n;
                continue;
            }
            if let Some((n, _frame)) =
                try_skip_logical_txn(&mut self.journal_file, self.salt1, self.salt2)?
            {
                cursor += n;
                continue;
            }

            let Some(frame_hdr) =
                JournalFrameHeader::read(&mut self.journal_file, self.salt1, self.salt2)?
            else {
                return Err(Error::Internal(
                    "checkpoint boundary not found before journal end".into(),
                ));
            };
            let page_size_bytes = frame_hdr.page_size.bytes();
            let data_offset = cursor + JOURNAL_FRAME_HEADER_SIZE as u64;
            self.journal_file
                .seek(SeekFrom::Start(data_offset))
                .map_err(Error::Io)?;
            let mut page_data = vec![0u8; page_size_bytes];
            self.journal_file
                .read_exact(&mut page_data)
                .map_err(Error::Io)?;

            let boundary = Page0BoundaryRecord::from_page_frame_parts(
                frame_hdr.page_number,
                frame_hdr.db_page_count,
                frame_hdr.salt1,
                frame_hdr.salt2,
                frame_hdr.page_size,
                &page_data,
            )?;
            if let Some(boundary) = boundary.as_ref() {
                if boundary.db_page_count() != expected_total_page_count {
                    return Err(Error::Internal(format!(
                        "boundary page count {} does not match expected \
                         {expected_total_page_count}",
                        boundary.db_page_count(),
                    )));
                }
            }

            write_page_to_main(
                main_file,
                frame_hdr.page_number,
                page_size_bytes,
                &page_data,
            )?;
            if boundary.is_some() {
                self.last_committed_db_page_count = Some(expected_total_page_count);
                return Ok(());
            }

            cursor = data_offset + page_size_bytes as u64;
        }
    }

    fn validate_active_checkpoint_batch_before_boundary(
        &mut self,
        cursor: &CheckpointBatchCursor,
    ) -> Result<()> {
        let Some((batch_id, expected_start)) = self.checkpoint_batch_active else {
            return Err(Error::Internal(
                "checkpoint boundary requires an active checkpoint batch".into(),
            ));
        };
        if batch_id != cursor.batch_id || expected_start != cursor.expected_pending_start {
            return Err(Error::Internal(
                "checkpoint boundary cursor does not match active checkpoint batch".into(),
            ));
        }
        if self.legacy_pending_start_offset.is_some() {
            return Err(Error::Internal(
                "checkpoint boundary cannot cover legacy pending frames".into(),
            ));
        }
        let mut scan = expected_start;
        let mut previous: Option<(CheckpointPoolKind, PageId)> = None;
        while scan < self.write_cursor {
            let Some(tag) = self.checkpoint_frame_tags.get(&scan).copied() else {
                return Err(Error::Internal(format!(
                    "checkpoint pending frame at offset {scan} is not tagged"
                )));
            };
            if tag.batch_id != batch_id {
                return Err(Error::Internal(format!(
                    "checkpoint frame at offset {scan} has wrong batch id"
                )));
            }
            if let Some(prev) = previous {
                let current = (tag.pool, tag.page_id);
                if current <= prev {
                    return Err(Error::Internal(
                        "checkpoint frames must be tagged in ascending pool/page order".into(),
                    ));
                }
            }
            self.journal_file
                .seek(SeekFrom::Start(scan))
                .map_err(Error::Io)?;
            let Some(frame_hdr) =
                CheckpointBatchPageRecord::read(&mut self.journal_file, self.salt1, self.salt2)?
            else {
                return Err(Error::Internal(format!(
                    "checkpoint pending range at offset {scan} is not a checkpoint page frame"
                )));
            };
            if tag.page_id != PageId(frame_hdr.page_number) {
                return Err(Error::Internal(format!(
                    "checkpoint tag page {} does not match frame page {}",
                    tag.page_id.0, frame_hdr.page_number
                )));
            }
            previous = Some((tag.pool, tag.page_id));
            scan += frame_hdr.total_size() as u64;
        }
        Ok(())
    }

    /// Truncate the journal file to just its 32-byte header and reposition the
    /// write cursor.
    fn truncate_journal(&mut self) -> Result<()> {
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
        self.legacy_pending_start_offset = None;
        self.last_legacy_commit_end_offset = self.write_cursor;
        self.checkpoint_batch_active = None;
        self.checkpoint_frame_tags.clear();
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// JournalLayeredSource — PageSource that composes a file source with a journal
// ---------------------------------------------------------------------------

/// Maps the storage-layer [`PageSize`] to the journal's own page-size enum.
fn page_size_to_journal(size: PageSize) -> JournalPageSize {
    match size {
        PageSize::Small4k => JournalPageSize::Small4k,
        PageSize::Large32k => JournalPageSize::Large32k,
    }
}

/// A [`PageSource`] that consults the journal before falling back to an
/// underlying file source.
///
/// * **Reads** — the journal is checked first via [`JournalManager::read_page`]; on
///   miss, the inner `PageSource` (typically [`crate::storage::file_io::FilePageSource`])
///   services the read.
/// * **Writes** — each `write_page` appends a non-commit journal frame via
///   [`JournalManager::append_non_commit`]; the main database file is not touched
///   until checkpoint time.
///
/// The journal state is shared via `Arc<Mutex<JournalManager>>`; contention is
/// managed at a higher level by the engine's `RwLock<BpBackend>`.
pub(crate) struct JournalLayeredSource {
    inner: Arc<dyn PageSource>,
    journal: Arc<Mutex<JournalManager>>,
}

impl JournalLayeredSource {
    pub(crate) fn new(inner: Arc<dyn PageSource>, journal: Arc<Mutex<JournalManager>>) -> Self {
        Self { inner, journal }
    }
}

impl PageSource for JournalLayeredSource {
    fn read_page(&self, page_number: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
        let mut guard = self
            .journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        if let Some(bytes) = guard.read_page(page_number)? {
            debug_assert_eq!(
                bytes.len(),
                size.bytes(),
                "journal frame size does not match requested PageSize"
            );
            buf.copy_from_slice(&bytes);
            return Ok(());
        }
        drop(guard);
        self.inner.read_page(page_number, size, buf)
    }

    fn write_page(&self, page_number: u32, size: PageSize, buf: &[u8]) -> Result<()> {
        let mut guard = self
            .journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        guard.append_non_commit(page_number, page_size_to_journal(size), buf)?;
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
#[path = "mod_tests.rs"]
mod tests_extracted;

#[cfg(test)]
#[path = "phase7_us006_tests.rs"]
mod phase7_us006_tests;

#[cfg(test)]
#[path = "phase7_us011_tests.rs"]
mod phase7_us011_tests;
