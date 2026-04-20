//! Journal — durability, recovery, checkpoint.
//!
//! This is a private internal module. The public API is exposed through
//! [`Database`](crate::database::Database) (checkpoint, close, durability configuration).
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
//! Durability is provided by `flush()`-ing the journal file after every
//! commit frame and every `ChainCommit` frame, so the next open's recovery
//! scan can replay any committed batch and discard any trailing
//! uncommitted frames.
//!
//! On clean close, [`JournalManager::close_and_cleanup`] checkpoints all
//! journal pages into the main file and deletes the journal, leaving only
//! `db.mqlite`.

#[allow(dead_code)]
pub(crate) mod shm;
#[allow(dead_code)]
pub(crate) mod log_file;

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::storage::buffer_pool::{PageSize, PageSource};
use crate::storage::header::FileHeader;
use crate::storage::page::PAGE_SIZE_LEAF;

use self::shm::JournalIndex;

use self::log_file::{
    try_skip_chain_commit, JournalFrameHeader, JournalHeader, JournalPageSize,
    JOURNAL_FRAME_HEADER_SIZE, JOURNAL_HEADER_SIZE,
};

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
    journal_path: PathBuf,
    /// Open handle to the journal file (positioned at the write cursor).
    journal_file: File,
    /// In-memory `page_number -> journal frame offset` index, rebuilt from
    /// a journal scan on open and maintained in-place. Not persisted.
    index: JournalIndex,
    /// Salt 1 from the main file header (stored in every journal frame).
    salt1: u32,
    /// Salt 2 from the main file header.
    salt2: u32,
    /// Checkpoint sequence counter from the journal file header.
    checkpoint_seq: u32,
    /// Byte offset of the next frame to write (append cursor).
    write_cursor: u64,
    /// Total database page count as of the last committed journal frame.
    /// Carried forward across commits; `None` if no commit has occurred yet
    /// in this journal.
    last_committed_db_page_count: Option<u32>,
    /// Highest `commit_ts` observed on any durable `ChainCommit` frame
    /// during recovery (`recover_existing`). `None` when the journal was
    /// freshly created or carried no ChainCommit frames. The MVCC backend
    /// reads this via [`recovered_max_commit_ts`](Self::recovered_max_commit_ts)
    /// to floor [`TimestampOracle`] (plan T7 "journal-tail scan").
    recovered_max_commit_ts: Option<Ts>,
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
            let recovered =
                Self::recover_existing(&journal_path, salt1, salt2, main_file)?;
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
        })
    }

    /// Replay an existing journal into the main file.
    ///
    /// Returns `None` if the journal is stale (salt mismatch) and was deleted.
    /// Returns `Some(JournalManager)` if recovery succeeded (including the empty
    /// case where the journal had no committed frames).
    fn recover_existing(
        journal_path: &Path,
        salt1: u32,
        salt2: u32,
        main_file: &mut File,
    ) -> Result<Option<JournalManager>> {
        let mut journal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(journal_path)
            .map_err(Error::Io)?;

        // Read and validate the journal header.
        let mut header_buf = [0u8; JOURNAL_HEADER_SIZE];
        match journal_file.read_exact(&mut header_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Empty or truncated journal — delete and recreate.
                drop(journal_file);
                let _ = std::fs::remove_file(journal_path);
                return Ok(None);
            }
            Err(e) => return Err(Error::Io(e)),
        }

        let journal_header = match JournalHeader::from_bytes(&header_buf) {
            Ok(h) => h,
            Err(_) => {
                // Corrupt journal header — treat as stale and delete.
                drop(journal_file);
                let _ = std::fs::remove_file(journal_path);
                return Ok(None);
            }
        };

        // Salt mismatch → stale journal from a different database open session.
        if journal_header.salt1 != salt1 || journal_header.salt2 != salt2 {
            drop(journal_file);
            let _ = std::fs::remove_file(journal_path);
            return Ok(None);
        }

        let checkpoint_seq = journal_header.checkpoint_seq;

        #[cfg(feature = "tracing")]
        let _rec_start = std::time::Instant::now();
        #[cfg(feature = "tracing")]
        let mut _frames_replayed: u64 = 0;

        // Scan frames: collect committed batches.
        // A "batch" is a sequence of non-commit frames followed by one commit frame.
        // On recovery we apply each committed batch to the main file.
        let mut index = JournalIndex::new();
        let mut pending: Vec<(u32, JournalPageSize, Vec<u8>, u64)> =
            Vec::new(); // (page_num, size, data, offset)
        let mut write_cursor = JOURNAL_HEADER_SIZE as u64;
        let mut last_committed_db_page_count: Option<u32> = None;
        // MVCC T7 — journal-tail HLC oracle recovery. Fold every ChainCommit
        // frame's `commit_ts` into a running max. The backend reads the max
        // via `recovered_max_commit_ts` after `open_or_create` returns and
        // floors `TimestampOracle` at `max.successor()`.
        let mut max_commit_ts: Option<Ts> = None;

        loop {
            let frame_offset = write_cursor;

            // MVCC T5'/T6: peek for a `ChainCommit` frame first. These carry
            // no legacy `JournalFrameHeader` and would crash the scan if
            // parsed as one. `try_skip_chain_commit` advances the reader
            // past a valid ChainCommit and returns its length; otherwise
            // it restores position and returns None for legacy fall-through.
            journal_file
                .seek(SeekFrom::Start(frame_offset))
                .map_err(Error::Io)?;
            if let Some((n, commit_ts)) =
                try_skip_chain_commit(&mut journal_file, salt1, salt2)?
            {
                // ChainCommit frame replay is a no-op for the page-replay
                // loop (it carries no single page_number). Version-chain
                // state is rebuilt on demand; the only recovery-critical
                // datum is `commit_ts`, which folds into `max_commit_ts`
                // so the HLC oracle lifts above every durable commit.
                write_cursor += n;
                max_commit_ts = Some(match max_commit_ts {
                    Some(prev) if prev >= commit_ts => prev,
                    _ => commit_ts,
                });
                continue;
            }

            let frame_opt =
                JournalFrameHeader::read(&mut journal_file, salt1, salt2)?;
            let frame_hdr = match frame_opt {
                None => break, // Bad checksum or EOF — stop here
                Some(h) => h,
            };

            // NOTE: JournalFrameHeader::read above already consumed the page data bytes
            // from the reader (to validate checksum), but didn't return them.
            // We need to re-read from the known offset.
            let page_size_bytes = frame_hdr.page_size.bytes();
            let data_offset = frame_offset + JOURNAL_FRAME_HEADER_SIZE as u64;

            journal_file
                .seek(SeekFrom::Start(data_offset))
                .map_err(Error::Io)?;
            let mut page_data = vec![0u8; page_size_bytes];
            journal_file.read_exact(&mut page_data).map_err(Error::Io)?;

            let next_frame_offset = data_offset + page_size_bytes as u64;
            write_cursor = next_frame_offset;

            if frame_hdr.db_page_count == 0 {
                // Non-commit frame — add to pending.
                pending.push((
                    frame_hdr.page_number,
                    frame_hdr.page_size,
                    page_data,
                    frame_offset,
                ));
            } else {
                // Commit frame — apply all pending + this frame.
                pending.push((
                    frame_hdr.page_number,
                    frame_hdr.page_size,
                    page_data,
                    frame_offset,
                ));

                let db_page_count = frame_hdr.db_page_count;
                for (pn, ps, data, off) in &pending {
                    // Write page to main file.
                    write_page_to_main(main_file, *pn, ps.bytes(), data)?;
                    // Update in-memory index.
                    index.insert(*pn, *off);
                }
                last_committed_db_page_count = Some(db_page_count);
                #[cfg(feature = "tracing")]
                {
                    _frames_replayed += pending.len() as u64;
                }
                pending.clear();

                // Seek back to after this commit frame for next iteration.
                journal_file
                    .seek(SeekFrom::Start(write_cursor))
                    .map_err(Error::Io)?;
            }
        }

        // Discard pending non-committed frames (already past write_cursor point).
        // write_cursor now points to the start of the first bad/missing frame.

        // Flush main file after replaying all committed frames.
        if last_committed_db_page_count.is_some() {
            main_file.flush().map_err(Error::Io)?;
        }

        // The in-memory index was rebuilt during the scan above; nothing
        // to persist (the journal itself is the only durable artifact).

        // Reposition journal file at write cursor for new appends.
        journal_file
            .seek(SeekFrom::Start(write_cursor))
            .map_err(Error::Io)?;

        #[cfg(feature = "tracing")]
        {
            let duration_ms = _rec_start.elapsed().as_millis() as u64;
            tracing::warn!(
                target: "mqlite",
                frames_replayed = _frames_replayed,
                duration_ms,
                "mqlite::journal_recovery"
            );
        }

        Ok(Some(JournalManager {
            journal_path: journal_path.to_path_buf(),
            journal_file,
            index,
            salt1,
            salt2,
            checkpoint_seq,
            write_cursor,
            last_committed_db_page_count,
            recovered_max_commit_ts: max_commit_ts,
        }))
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
        self.append_frame(page_number, 0, page_size, page_data)
    }

    /// Append an MVCC `ChainCommit` frame to the journal (Format Lock §A.2).
    ///
    /// Emits one `ChainCommitFrame` carrying `commit_ts`, `refcount_deltas`,
    /// and zero or more `page_writes`. The frame is written at the current
    /// `write_cursor`; the cursor advances past the encoded frame. An
    /// `fsync`-equivalent `flush()` is called before the cursor advances so
    /// the frame is durable before any later frames can overwrite its tail.
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
        self.journal_file.flush().map_err(Error::Io)?;
        self.write_cursor += bytes.len() as u64;
        Ok(frame_offset)
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
        let offset = self.append_frame(page_number, db_page_count, page_size, page_data)?;
        self.last_committed_db_page_count = Some(db_page_count);

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
    ) -> Result<u64> {
        let frame_offset = self.write_cursor;
        self.journal_file
            .seek(SeekFrom::Start(frame_offset))
            .map_err(Error::Io)?;

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
        self.journal_file.flush().map_err(Error::Io)?;

        self.write_cursor += (JOURNAL_FRAME_HEADER_SIZE + page_size.bytes()) as u64;

        // Update the in-memory index for non-commit frames too so reads
        // through `JournalLayeredSource` see in-progress writes within the
        // same process. Only the journal file is durable; the index lives
        // in memory and is rebuilt on open.
        if db_page_count == 0 {
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
    /// O(journal frames) per lookup.  Acceptable for Phase 1 with aggressive
    /// checkpointing.  See scale.md §journal design fallback.
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

            // MVCC T5'/T6: skip ChainCommit frames — they carry no
            // page_number and are invisible to the per-page linear scan.
            if let Some((n, _commit_ts)) =
                try_skip_chain_commit(&mut self.journal_file, self.salt1, self.salt2)?
            {
                cursor += n;
                continue;
            }

            let frame_opt =
                JournalFrameHeader::read(&mut self.journal_file, self.salt1, self.salt2)?;
            let frame_hdr = match frame_opt {
                None => break,
                Some(h) => h,
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
            self.journal_file
                .read_exact(&mut hbuf)
                .map_err(Error::Io)?;
            let page_size_u32 =
                u32::from_le_bytes(hbuf[16..20].try_into().expect("4 bytes"));
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
        self.journal_file.sync_data().map_err(Error::Io)
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Return the current journal write cursor (byte offset past the last frame).
    pub(crate) fn write_cursor(&self) -> u64 {
        self.write_cursor
    }

    /// Return a reference to the in-memory journal index (for inspection in tests).
    pub(crate) fn index(&self) -> &JournalIndex {
        &self.index
    }

    /// Highest `ChainCommit::commit_ts` observed during recovery, or `None`
    /// when the journal was freshly created or carried no ChainCommit
    /// frames. The MVCC backend uses this to floor the HLC oracle at
    /// `max.successor()` so that every post-recovery `commit()` is
    /// strictly greater than any durable commit from the previous
    /// lifetime (plan T7 — "journal-tail scan, HLC-aware").
    pub(crate) fn recovered_max_commit_ts(&self) -> Option<Ts> {
        self.recovered_max_commit_ts
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

            // MVCC T5'/T6: ChainCommit frames are part of the durable log
            // but carry no `page_number` for the in-memory index. Skip past
            // them so `JournalFrameHeader::read` below sees only legacy frames.
            if let Some((n, _commit_ts)) =
                try_skip_chain_commit(&mut self.journal_file, self.salt1, self.salt2)?
            {
                scan += n;
                continue;
            }

            let frame_opt =
                JournalFrameHeader::read(&mut self.journal_file, self.salt1, self.salt2)?;
            let frame_hdr = match frame_opt {
                None => break,
                Some(h) => h,
            };
            self.index.insert(frame_hdr.page_number, scan);
            if frame_hdr.db_page_count > 0 {
                latest_commit_pages = Some(frame_hdr.db_page_count);
            }
            scan += (JOURNAL_FRAME_HEADER_SIZE + frame_hdr.page_size.bytes()) as u64;
        }
        self.last_committed_db_page_count = latest_commit_pages;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Truncate the journal file to just its 32-byte header and reposition the
    /// write cursor.
    fn truncate_journal(&mut self) -> Result<()> {
        self.journal_file
            .seek(SeekFrom::Start(0))
            .map_err(Error::Io)?;

        // Re-write header with incremented checkpoint sequence.
        self.checkpoint_seq = self.checkpoint_seq.wrapping_add(1);
        let header = JournalHeader {
            magic: self::log_file::JOURNAL_MAGIC,
            format_version: self::log_file::JOURNAL_FORMAT_VERSION,
            page_size_internal: crate::storage::page::PAGE_SIZE_INTERNAL,
            page_size_leaf: crate::storage::page::PAGE_SIZE_LEAF,
            salt1: self.salt1,
            salt2: self.salt2,
            checkpoint_seq: self.checkpoint_seq,
        };
        self.journal_file
            .write_all(&header.to_bytes())
            .map_err(Error::Io)?;
        self.journal_file
            .set_len(JOURNAL_HEADER_SIZE as u64)
            .map_err(Error::Io)?;
        self.journal_file.flush().map_err(Error::Io)?;

        self.write_cursor = JOURNAL_HEADER_SIZE as u64;
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
