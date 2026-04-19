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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::header::FileHeader;
    use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};
    use std::io::Read;
    use tempfile::TempDir;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_db_file() -> (TempDir, PathBuf, File) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.mqlite");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_path)
            .unwrap();
        (dir, db_path, file)
    }

    fn make_header() -> FileHeader {
        FileHeader::new(1_700_000_000_000, 0xDEAD_BEEF, 0xCAFE_BABE)
    }

    fn make_page_4k(fill: u8) -> Vec<u8> {
        vec![fill; PAGE_SIZE_INTERNAL as usize]
    }

    fn make_page_32k(fill: u8) -> Vec<u8> {
        vec![fill; PAGE_SIZE_LEAF as usize]
    }

    // -----------------------------------------------------------------------
    // Path helpers
    // -----------------------------------------------------------------------

    #[test]
    fn journal_path_derivation() {
        let db = Path::new("/tmp/foo.mqlite");
        assert_eq!(
            journal_path_for(db),
            PathBuf::from("/tmp/foo.mqlite-journal")
        );
    }

    // -----------------------------------------------------------------------
    // Open / create
    // -----------------------------------------------------------------------

    #[test]
    fn open_creates_journal_file() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let jp = journal_path_for(&db_path);
        assert!(jp.exists(), "journal file must be created");

        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    /// Regression: no `.mqlite-shm` sidecar must ever be created. The journal
    /// index is in-memory only.
    #[test]
    fn no_shm_file_created_in_any_phase() {
        let (dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let mut header = make_header();
        let shm_sidecar = {
            let mut p = db_path.as_os_str().to_owned();
            p.push("-shm");
            PathBuf::from(p)
        };

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert!(!shm_sidecar.exists(), "no -shm after open");

        mgr.append_non_commit(1, JournalPageSize::Small4k, &make_page_4k(0x01))
            .unwrap();
        assert!(!shm_sidecar.exists(), "no -shm after append");

        mgr.commit(2, JournalPageSize::Small4k, &make_page_4k(0x02), 5)
            .unwrap();
        assert!(!shm_sidecar.exists(), "no -shm after commit");

        mgr.checkpoint(&mut main_file, &mut header).unwrap();
        assert!(!shm_sidecar.exists(), "no -shm after checkpoint");

        mgr.close_and_cleanup(&mut main_file, &mut header).unwrap();
        assert!(!shm_sidecar.exists(), "no -shm after clean close");

        drop(main_file);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // Append and read back
    // -----------------------------------------------------------------------

    #[test]
    fn append_and_read_4k() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_data = make_page_4k(0xAB);
        mgr.append_non_commit(3, JournalPageSize::Small4k, &page_data)
            .unwrap();

        let result = mgr.read_page(3).unwrap();
        assert_eq!(result, Some(page_data));
        assert!(mgr.read_page(99).unwrap().is_none());
    }

    #[test]
    fn append_and_read_32k() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_data = make_page_32k(0xCC);
        mgr.append_non_commit(10, JournalPageSize::Large32k, &page_data)
            .unwrap();

        let result = mgr.read_page(10).unwrap();
        assert_eq!(result, Some(page_data));
    }

    #[test]
    fn latest_write_wins() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_v1 = make_page_4k(0x01);
        let page_v2 = make_page_4k(0x02);
        mgr.append_non_commit(5, JournalPageSize::Small4k, &page_v1)
            .unwrap();
        mgr.append_non_commit(5, JournalPageSize::Small4k, &page_v2)
            .unwrap();

        // Index lookup returns offset of latest (second) frame.
        let result = mgr.read_page(5).unwrap().unwrap();
        assert_eq!(result[0], 0x02);
    }

    // -----------------------------------------------------------------------
    // Commit
    // -----------------------------------------------------------------------

    #[test]
    fn commit_frame_marks_transaction_boundary() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_a = make_page_4k(0xAA);
        let page_b = make_page_4k(0xBB);
        mgr.append_non_commit(1, JournalPageSize::Small4k, &page_a)
            .unwrap();
        let emergency = mgr
            .commit(2, JournalPageSize::Small4k, &page_b, 10)
            .unwrap();
        assert!(!emergency);
        assert_eq!(mgr.last_committed_db_page_count, Some(10));
    }

    // -----------------------------------------------------------------------
    // Checkpoint
    // -----------------------------------------------------------------------

    #[test]
    fn checkpoint_writes_pages_to_main_file() {
        let (_dir, db_path, mut main_file) = make_db_file();
        // Pre-allocate main file large enough
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();

        let mut header = make_header();
        let mut mgr =
            JournalManager::open_or_create(&db_path, &mut header, &mut main_file).unwrap();

        let page_data = make_page_4k(0x42);
        mgr.append_non_commit(2, JournalPageSize::Small4k, &page_data)
            .unwrap();
        mgr.commit(2, JournalPageSize::Small4k, &page_data, 5)
            .unwrap();

        mgr.checkpoint(&mut main_file, &mut header).unwrap();

        // Verify: page 2 in main file at the uniform 32 KB slot offset.
        let offset = 2u64 * PAGE_SIZE_LEAF as u64;
        main_file.seek(SeekFrom::Start(offset)).unwrap();
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0x42);

        // Journal should be reset.
        assert_eq!(mgr.write_cursor, JOURNAL_HEADER_SIZE as u64);
        assert_eq!(mgr.index.occupied_count(), 0);
    }

    #[test]
    fn checkpoint_increments_sequence() {
        let (_dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let mut header = make_header();
        let mut mgr =
            JournalManager::open_or_create(&db_path, &mut header, &mut main_file).unwrap();
        assert_eq!(mgr.checkpoint_seq, 0);

        let page_data = make_page_4k(0x01);
        mgr.commit(1, JournalPageSize::Small4k, &page_data, 2)
            .unwrap();
        mgr.checkpoint(&mut main_file, &mut header).unwrap();

        assert_eq!(mgr.checkpoint_seq, 1);
    }

    // -----------------------------------------------------------------------
    // Recovery — crash simulation
    // -----------------------------------------------------------------------

    #[test]
    fn recovery_replays_committed_frames() {
        let (_dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let header = make_header();

        // Write two frames and commit.
        {
            let mut mgr =
                JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

            let page_a = make_page_4k(0xAA);
            let page_b = make_page_4k(0xBB);
            mgr.append_non_commit(1, JournalPageSize::Small4k, &page_a)
                .unwrap();
            mgr.commit(2, JournalPageSize::Small4k, &page_b, 5).unwrap();
            // Simulate crash: don't call close_and_cleanup.
            // Journal file left on disk.
        }

        // Reopen — recovery runs automatically.
        let mut main_file2 = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let _mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file2).unwrap();

        // Both pages should have been replayed into main file at 32 KB slots.
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file2
            .seek(SeekFrom::Start(1 * PAGE_SIZE_LEAF as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0xAA, "page 1 should be replayed");

        main_file2
            .seek(SeekFrom::Start(2 * PAGE_SIZE_LEAF as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0xBB, "page 2 should be replayed");
    }

    #[test]
    fn recovery_discards_uncommitted_frames() {
        let (_dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let header = make_header();

        // Write one committed frame, then one uncommitted (simulated crash mid-tx).
        {
            let mut mgr =
                JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

            let page_committed = make_page_4k(0xCC);
            let page_uncommitted = make_page_4k(0xDD);
            mgr.commit(1, JournalPageSize::Small4k, &page_committed, 3)
                .unwrap();
            // Append non-commit frame — transaction never completed.
            mgr.append_non_commit(2, JournalPageSize::Small4k, &page_uncommitted)
                .unwrap();
            // Crash: no commit for page 2.
        }

        let mut main_file2 = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file2).unwrap();

        // Page 1 (committed) should be in main file at the 32 KB slot offset.
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file2
            .seek(SeekFrom::Start(1 * PAGE_SIZE_LEAF as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0xCC, "committed page must be present");

        // Page 2 (uncommitted) — index should NOT have it after recovery.
        assert!(
            mgr2.index().lookup(2).is_none(),
            "uncommitted page must not be in journal index after recovery"
        );
    }

    #[test]
    fn stale_journal_is_deleted_on_open() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        // Create journal with original salts.
        {
            let _mgr =
                JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        }
        assert!(journal_path_for(&db_path).exists());

        // Reopen with different salts (simulates a different database open).
        let different_header =
            FileHeader::new(1_700_000_000_001, 0x1111_1111, 0x2222_2222);
        let mgr2 = JournalManager::open_or_create(
            &db_path,
            &different_header,
            &mut main_file,
        )
        .unwrap();
        // A fresh journal should have been created with the new salts.
        assert_eq!(mgr2.salt1, 0x1111_1111);
        assert_eq!(mgr2.salt2, 0x2222_2222);
    }

    // -----------------------------------------------------------------------
    // Clean close
    // -----------------------------------------------------------------------

    #[test]
    fn close_and_cleanup_removes_journal() {
        let (dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let mut header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &mut header, &mut main_file).unwrap();
        let page_data = make_page_4k(0xFF);
        mgr.commit(1, JournalPageSize::Small4k, &page_data, 2)
            .unwrap();

        let jp = journal_path_for(&db_path);

        mgr.close_and_cleanup(&mut main_file, &mut header).unwrap();

        assert!(!jp.exists(), "journal must be deleted after clean close");
        let _ = dir;
    }

    // -----------------------------------------------------------------------
    // Linear scan fallback
    // -----------------------------------------------------------------------

    #[test]
    fn linear_scan_finds_committed_pages() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_data = make_page_4k(0x77);
        mgr.append_non_commit(7, JournalPageSize::Small4k, &page_data)
            .unwrap();

        let result = mgr.read_page_linear(7).unwrap();
        assert_eq!(result, Some(page_data));
        assert!(mgr.read_page_linear(999).unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // Rollback (truncate_to)
    // -----------------------------------------------------------------------

    #[test]
    fn truncate_to_drops_frames_written_after_mark() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_non_commit(1, JournalPageSize::Small4k, &make_page_4k(0x11))
            .unwrap();
        let mark = mgr.write_cursor();
        mgr.append_non_commit(2, JournalPageSize::Small4k, &make_page_4k(0x22))
            .unwrap();
        mgr.append_non_commit(3, JournalPageSize::Small4k, &make_page_4k(0x33))
            .unwrap();

        mgr.truncate_to(mark).unwrap();

        assert_eq!(mgr.write_cursor(), mark);
        assert_eq!(
            mgr.read_page(1).unwrap(),
            Some(make_page_4k(0x11)),
            "frame before mark must survive"
        );
        assert!(
            mgr.read_page(2).unwrap().is_none(),
            "frame after mark must be dropped"
        );
        assert!(mgr.read_page(3).unwrap().is_none());
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn truncate_to_preserves_prior_commit_state() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_non_commit(1, JournalPageSize::Small4k, &make_page_4k(0x11))
            .unwrap();
        mgr.commit(2, JournalPageSize::Small4k, &make_page_4k(0x22), 50)
            .unwrap();
        let mark = mgr.write_cursor();
        mgr.append_non_commit(3, JournalPageSize::Small4k, &make_page_4k(0x33))
            .unwrap();

        mgr.truncate_to(mark).unwrap();

        assert_eq!(mgr.last_committed_db_page_count, Some(50));
        assert_eq!(mgr.read_page(1).unwrap(), Some(make_page_4k(0x11)));
        assert_eq!(mgr.read_page(2).unwrap(), Some(make_page_4k(0x22)));
        assert!(mgr.read_page(3).unwrap().is_none());
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn truncate_to_full_drops_all_non_header_frames() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_non_commit(1, JournalPageSize::Small4k, &make_page_4k(0xAA))
            .unwrap();
        mgr.append_non_commit(2, JournalPageSize::Small4k, &make_page_4k(0xBB))
            .unwrap();

        mgr.truncate_to(JOURNAL_HEADER_SIZE as u64).unwrap();

        assert_eq!(mgr.write_cursor(), JOURNAL_HEADER_SIZE as u64);
        assert!(mgr.read_page(1).unwrap().is_none());
        assert!(mgr.read_page(2).unwrap().is_none());
        assert_eq!(mgr.last_committed_db_page_count, None);
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // JournalLayeredSource
    // -----------------------------------------------------------------------

    struct StubFileSource {
        pages: Mutex<std::collections::HashMap<u32, Vec<u8>>>,
    }

    impl StubFileSource {
        fn new() -> Self {
            Self {
                pages: Mutex::new(std::collections::HashMap::new()),
            }
        }
    }

    impl PageSource for StubFileSource {
        fn read_page(&self, n: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
            let pages = self.pages.lock().unwrap();
            if let Some(v) = pages.get(&n) {
                buf.copy_from_slice(v);
            } else {
                buf.fill(0);
                let _ = size;
            }
            Ok(())
        }
        fn write_page(&self, n: u32, size: PageSize, buf: &[u8]) -> Result<()> {
            debug_assert_eq!(buf.len(), size.bytes());
            self.pages.lock().unwrap().insert(n, buf.to_vec());
            Ok(())
        }
    }

    #[test]
    fn layered_source_read_hits_journal_first() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let journal = Arc::new(Mutex::new(mgr));

        let file_src: Arc<dyn PageSource> = Arc::new(StubFileSource::new());
        let file_only = make_page_32k(0xFA);
        file_src
            .write_page(5, PageSize::Large32k, &file_only)
            .unwrap();

        journal
            .lock()
            .unwrap()
            .append_non_commit(5, JournalPageSize::Large32k, &make_page_32k(0xB1))
            .unwrap();

        let layered = JournalLayeredSource::new(Arc::clone(&file_src), Arc::clone(&journal));
        let mut buf = vec![0u8; PageSize::Large32k.bytes()];
        layered.read_page(5, PageSize::Large32k, &mut buf).unwrap();
        assert_eq!(buf, make_page_32k(0xB1), "journal version must win over file");
        drop(journal);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn layered_source_read_falls_back_to_file() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let journal = Arc::new(Mutex::new(mgr));

        let file_src: Arc<dyn PageSource> = Arc::new(StubFileSource::new());
        file_src
            .write_page(9, PageSize::Small4k, &make_page_4k(0xCC))
            .unwrap();

        let layered = JournalLayeredSource::new(Arc::clone(&file_src), Arc::clone(&journal));
        let mut buf = vec![0u8; PageSize::Small4k.bytes()];
        layered.read_page(9, PageSize::Small4k, &mut buf).unwrap();
        assert_eq!(buf, make_page_4k(0xCC));
        drop(journal);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn layered_source_write_appends_to_journal_not_file() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let journal = Arc::new(Mutex::new(mgr));

        let file_src = Arc::new(StubFileSource::new());
        let layered = JournalLayeredSource::new(
            Arc::clone(&file_src) as Arc<dyn PageSource>,
            Arc::clone(&journal),
        );

        let payload = make_page_4k(0x5A);
        layered.write_page(13, PageSize::Small4k, &payload).unwrap();

        let journal_bytes = journal.lock().unwrap().read_page(13).unwrap();
        assert_eq!(journal_bytes, Some(payload.clone()));

        let pages = file_src.pages.lock().unwrap();
        assert!(
            !pages.contains_key(&13),
            "write_page must not touch the backing file source"
        );
        drop(pages);
        drop(journal);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn truncate_to_rejects_out_of_range_cursor() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let cur = mgr.write_cursor();

        assert!(mgr.truncate_to(cur + 1).is_err());
        assert!(mgr.truncate_to(0).is_err());
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // T7 — HLC oracle recovery: ChainCommit frames fold into
    // `recovered_max_commit_ts` across reopen.
    // -----------------------------------------------------------------------

    #[test]
    fn recovered_max_commit_ts_none_on_fresh_journal() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert_eq!(mgr.recovered_max_commit_ts(), None);
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn recovered_max_commit_ts_folds_across_reopen() {
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        // Lifetime 1 — append three ChainCommit frames with non-monotonic ts;
        // `open_or_create` in the second lifetime must return the max.
        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_chain_commit(Ts { physical_ms: 50, logical: 0 }, vec![], vec![])
            .unwrap();
        mgr.append_chain_commit(Ts { physical_ms: 150, logical: 0 }, vec![], vec![])
            .unwrap();
        mgr.append_chain_commit(Ts { physical_ms: 100, logical: 7 }, vec![], vec![])
            .unwrap();
        drop(mgr);

        let mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert_eq!(
            mgr2.recovered_max_commit_ts(),
            Some(Ts { physical_ms: 150, logical: 0 }),
            "recovery must fold max(commit_ts) across ChainCommit frames"
        );
        drop(mgr2);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn recovered_max_commit_ts_compares_logical_component() {
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_chain_commit(Ts { physical_ms: 200, logical: 3 }, vec![], vec![])
            .unwrap();
        mgr.append_chain_commit(Ts { physical_ms: 200, logical: 9 }, vec![], vec![])
            .unwrap();
        mgr.append_chain_commit(Ts { physical_ms: 200, logical: 1 }, vec![], vec![])
            .unwrap();
        drop(mgr);

        let mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert_eq!(
            mgr2.recovered_max_commit_ts(),
            Some(Ts { physical_ms: 200, logical: 9 }),
            "tie-breaking on logical component required for HLC recovery"
        );
        drop(mgr2);
        drop(main_file);
        drop(dir);
    }
}

// ---------------------------------------------------------------------------
// Crash-recovery tests (Unix only — uses fork/SIGKILL)
// ---------------------------------------------------------------------------

#[cfg(all(test, unix))]
mod crash_recovery_tests {
    //! Crash Recovery Testing — 500 cycles, 10 scenarios.
    //!
    //! Implements Jepsen-style crash injection against the mqlite journal layer.
    //! For each cycle the test:
    //!
    //!   1. Sets up a fresh database directory with pre-committed "epoch-1" data
    //!      in the journal (5 pages, fill byte derived from the cycle seed).
    //!   2. `fork()`s a child process that opens the journal (triggering recovery of
    //!      epoch-1) and then runs a scenario-specific "operation" — writing some
    //!      frames to the journal, or directly to the main file during a simulated
    //!      checkpoint.
    //!   3. The parent SIGKILLs the child at the scenario's injection point.
    //!   4. The parent re-opens the journal (triggering recovery again).
    //!   5. The parent validates all five correctness conditions:
    //!
    //!      (a) Database opens without error after crash.
    //!      (b) Journal replay does not fail (covered by (a) succeeding).
    //!      (c) Committed data is present in the main file.
    //!      (d) Uncommitted data does not appear (no phantom pages in the journal index).
    //!      (e) Index pages are absent when the index build was uncommitted.

    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;

    use crate::error::{Error, Result};
    use crate::storage::header::FileHeader;
    use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};
    use crate::journal::log_file::JournalPageSize;
    use crate::journal::{write_page_to_main, JournalManager};

    const CYCLES_PER_SCENARIO: u32 = 50;
    const EPOCH1_START: u32 = 1;
    const EPOCH1_END: u32 = 6;
    const EPOCH2_START: u32 = 6;
    const EPOCH2_END: u32 = 21;
    const INDEX_START: u32 = 100;
    const INDEX_END: u32 = 110;
    const CHECKPOINT_PAGES: u32 = 20;
    const SALT1: u32 = 0xDEAD_BEEF;
    const SALT2: u32 = 0xCAFE_BABE;

    fn epoch1_fill(seed: u32) -> u8 { ((seed % 200) + 1) as u8 }
    fn epoch2_fill(seed: u32) -> u8 { (((seed + 100) % 200) + 1) as u8 }
    fn uncommitted_fill(seed: u32) -> u8 { (((seed + 50) % 200) + 1) as u8 }
    const CHECKPOINT_GARBAGE_FILL: u8 = 0xDE;

    #[derive(Debug, Clone, Copy)]
    enum Scenario {
        InsertAtFrame0,
        InsertAtFrame10,
        InsertAtFrame100,
        InsertAtFinalFrame,
        CheckpointAt25Pct,
        CheckpointAt50Pct,
        CheckpointAt75Pct,
        IndexBuildAtStart,
        IndexBuildMidway,
        IndexBuildAtEnd,
    }

    const ALL_SCENARIOS: [Scenario; 10] = [
        Scenario::InsertAtFrame0,
        Scenario::InsertAtFrame10,
        Scenario::InsertAtFrame100,
        Scenario::InsertAtFinalFrame,
        Scenario::CheckpointAt25Pct,
        Scenario::CheckpointAt50Pct,
        Scenario::CheckpointAt75Pct,
        Scenario::IndexBuildAtStart,
        Scenario::IndexBuildMidway,
        Scenario::IndexBuildAtEnd,
    ];

    fn setup_epoch1(db_path: &Path, seed: u32) -> Result<()> {
        let mut main_file = OpenOptions::new()
            .read(true).write(true).create(true).open(db_path)
            .map_err(Error::Io)?;
        main_file.set_len(200 * PAGE_SIZE_LEAF as u64).map_err(Error::Io)?;
        let header = FileHeader::new(1_700_000_000_000, SALT1, SALT2);
        main_file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
        main_file.write_all(&header.to_bytes()).map_err(Error::Io)?;
        main_file.flush().map_err(Error::Io)?;
        let mut journal = JournalManager::open_or_create(db_path, &header, &mut main_file)?;
        let page_data = vec![epoch1_fill(seed); PAGE_SIZE_INTERNAL as usize];
        for page_no in EPOCH1_START..(EPOCH1_END - 1) {
            journal.append_non_commit(page_no, JournalPageSize::Small4k, &page_data)?;
        }
        journal.commit(EPOCH1_END - 1, JournalPageSize::Small4k, &page_data, EPOCH1_END - 1)?;
        drop(journal);
        drop(main_file);
        Ok(())
    }

    unsafe fn child_run_scenario(db_path: &Path, scenario: Scenario, seed: u32, write_fd: libc::c_int) {
        macro_rules! step {
            () => {{
                let b: u8 = 1;
                libc::write(write_fd, &b as *const u8 as *const libc::c_void, 1);
            }};
        }
        let mut main_file = match OpenOptions::new().read(true).write(true).open(db_path) {
            Ok(f) => f,
            Err(_) => libc::_exit(2),
        };
        let header = FileHeader::new(1_700_000_000_000, SALT1, SALT2);
        let mut journal = match JournalManager::open_or_create(db_path, &header, &mut main_file) {
            Ok(w) => w,
            Err(_) => libc::_exit(3),
        };
        let uc_fill = uncommitted_fill(seed);
        let e2_fill = epoch2_fill(seed);
        match scenario {
            Scenario::InsertAtFrame0 => {
                step!();
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::InsertAtFrame10 => {
                let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
                for i in 0u32..10 {
                    let _ = journal.append_non_commit(EPOCH2_START + i, JournalPageSize::Small4k, &page_data);
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::InsertAtFrame100 => {
                let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
                let span = EPOCH2_END - EPOCH2_START;
                for i in 0u32..100 {
                    let page_no = EPOCH2_START + (i % span);
                    let _ = journal.append_non_commit(page_no, JournalPageSize::Small4k, &page_data);
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::InsertAtFinalFrame => {
                let page_data = vec![e2_fill; PAGE_SIZE_INTERNAL as usize];
                for i in 0u32..5 {
                    let _ = journal.append_non_commit(EPOCH2_START + i, JournalPageSize::Small4k, &page_data);
                }
                let _ = journal.commit(EPOCH2_START + 5, JournalPageSize::Small4k, &page_data, EPOCH2_START + 5);
                step!();
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::CheckpointAt25Pct | Scenario::CheckpointAt50Pct | Scenario::CheckpointAt75Pct => {
                let epoch2_data = vec![e2_fill; PAGE_SIZE_INTERNAL as usize];
                let e2_span = EPOCH2_END - EPOCH2_START;
                for i in 0..(e2_span - 1) {
                    let _ = journal.append_non_commit(EPOCH2_START + i, JournalPageSize::Small4k, &epoch2_data);
                }
                let _ = journal.commit(EPOCH2_START + e2_span - 1, JournalPageSize::Small4k, &epoch2_data, EPOCH2_START + e2_span - 1);
                let garbage = vec![CHECKPOINT_GARBAGE_FILL; PAGE_SIZE_INTERNAL as usize];
                for page_no in 1..=CHECKPOINT_PAGES {
                    let _ = write_page_to_main(&mut main_file, page_no, PAGE_SIZE_INTERNAL as usize, &garbage);
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::IndexBuildAtStart => {
                step!();
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::IndexBuildMidway => {
                let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
                for i in 0u32..5 {
                    let _ = journal.append_non_commit(INDEX_START + i, JournalPageSize::Small4k, &page_data);
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::IndexBuildAtEnd => {
                let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
                for i in 0u32..(INDEX_END - INDEX_START) {
                    let _ = journal.append_non_commit(INDEX_START + i, JournalPageSize::Small4k, &page_data);
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
        }
        libc::_exit(0);
    }

    fn read_main_page(file: &mut std::fs::File, page_no: u32) -> Result<Vec<u8>> {
        let offset = page_no as u64 * PAGE_SIZE_LEAF as u64;
        file.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        file.read_exact(&mut buf).map_err(Error::Io)?;
        Ok(buf)
    }

    fn validate(journal: &JournalManager, main_file: &mut std::fs::File, scenario: Scenario, seed: u32) -> Result<()> {
        let e1_fill = epoch1_fill(seed);
        let e2_fill = epoch2_fill(seed);

        for page_no in EPOCH1_START..EPOCH1_END {
            let page = read_main_page(main_file, page_no)?;
            if page[0] != e1_fill {
                return Err(Error::Internal(format!(
                    "condition (c) FAIL: epoch-1 page {} fill={:#04x} want={:#04x} [scenario {:?} seed {}]",
                    page_no, page[0], e1_fill, scenario, seed
                )));
            }
        }

        if matches!(scenario, Scenario::InsertAtFinalFrame) {
            for page_no in EPOCH2_START..(EPOCH2_START + 6) {
                let page = read_main_page(main_file, page_no)?;
                if page[0] != e2_fill {
                    return Err(Error::Internal(format!(
                        "condition (c) FAIL: InsertAtFinalFrame page {} fill={:#04x} want={:#04x} [seed {}]",
                        page_no, page[0], e2_fill, seed
                    )));
                }
            }
        }

        if matches!(scenario, Scenario::CheckpointAt25Pct | Scenario::CheckpointAt50Pct | Scenario::CheckpointAt75Pct) {
            for page_no in EPOCH2_START..EPOCH2_END {
                let page = read_main_page(main_file, page_no)?;
                if page[0] != e2_fill {
                    return Err(Error::Internal(format!(
                        "condition (c) FAIL: checkpoint page {} fill={:#04x} want={:#04x} [scenario {:?} seed {}]",
                        page_no, page[0], e2_fill, scenario, seed
                    )));
                }
            }
            for page_no in 1..=CHECKPOINT_PAGES {
                let page = read_main_page(main_file, page_no)?;
                if page[0] == CHECKPOINT_GARBAGE_FILL {
                    return Err(Error::Internal(format!(
                        "condition (d) FAIL: checkpoint garbage fill {:#04x} found at page {} after journal recovery [scenario {:?} seed {}]",
                        CHECKPOINT_GARBAGE_FILL, page_no, scenario, seed
                    )));
                }
            }
        }

        if matches!(scenario, Scenario::InsertAtFrame10) {
            for i in 0u32..10 {
                let page_no = EPOCH2_START + i;
                if journal.index().lookup(page_no).is_some() {
                    return Err(Error::Internal(format!(
                        "condition (d) FAIL: uncommitted page {} in journal index after recovery [InsertAtFrame10 seed {}]",
                        page_no, seed
                    )));
                }
            }
        }

        if matches!(scenario, Scenario::InsertAtFrame100) {
            for page_no in EPOCH2_START..EPOCH2_END {
                if journal.index().lookup(page_no).is_some() {
                    return Err(Error::Internal(format!(
                        "condition (d) FAIL: uncommitted page {} in journal index after recovery [InsertAtFrame100 seed {}]",
                        page_no, seed
                    )));
                }
            }
        }

        if matches!(scenario, Scenario::IndexBuildAtStart | Scenario::IndexBuildMidway | Scenario::IndexBuildAtEnd) {
            for page_no in INDEX_START..INDEX_END {
                if journal.index().lookup(page_no).is_some() {
                    return Err(Error::Internal(format!(
                        "condition (e) FAIL: uncommitted index page {} in journal index after recovery [scenario {:?} seed {}]",
                        page_no, scenario, seed
                    )));
                }
            }
        }

        Ok(())
    }

    fn run_cycle(scenario: Scenario, seed: u32) -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::Io)?;
        let db_path = dir.path().join("crash.mqlite");
        setup_epoch1(&db_path, seed)?;

        let kill_after: u32 = match scenario {
            Scenario::InsertAtFrame0 => 1,
            Scenario::InsertAtFrame10 => 10,
            Scenario::InsertAtFrame100 => 100,
            Scenario::InsertAtFinalFrame => 1,
            Scenario::CheckpointAt25Pct => (CHECKPOINT_PAGES / 4).max(1),
            Scenario::CheckpointAt50Pct => CHECKPOINT_PAGES / 2,
            Scenario::CheckpointAt75Pct => (CHECKPOINT_PAGES * 3) / 4,
            Scenario::IndexBuildAtStart => 1,
            Scenario::IndexBuildMidway => 5,
            Scenario::IndexBuildAtEnd => INDEX_END - INDEX_START,
        };

        let mut pipe_fds = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(pipe_fds.as_mut_ptr()) }, 0, "pipe() failed");
        let (read_fd, write_fd) = (pipe_fds[0], pipe_fds[1]);

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork() failed");

        if pid == 0 {
            unsafe { libc::close(read_fd) };
            unsafe { child_run_scenario(&db_path, scenario, seed, write_fd) };
            unsafe { libc::_exit(1) };
        }

        unsafe { libc::close(write_fd) };
        let mut buf = 0u8;
        for signal_idx in 0..kill_after {
            let n = unsafe { libc::read(read_fd, &mut buf as *mut u8 as *mut libc::c_void, 1) };
            if n != 1 {
                unsafe { libc::kill(pid, libc::SIGKILL) };
                unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };
                unsafe { libc::close(read_fd) };
                return Err(Error::Internal(format!(
                    "child exited early: got {signal_idx}/{kill_after} signals [scenario {:?} seed {seed}]",
                    scenario
                )));
            }
        }
        unsafe { libc::close(read_fd) };
        unsafe { libc::kill(pid, libc::SIGKILL) };
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };

        let mut main_file = OpenOptions::new().read(true).write(true).open(&db_path)
            .map_err(|e| Error::Internal(format!(
                "condition (a) FAIL: cannot reopen main file after crash [scenario {:?} seed {seed}]: {e}",
                scenario
            )))?;
        let header = FileHeader::new(1_700_000_000_000, SALT1, SALT2);
        let journal = JournalManager::open_or_create(&db_path, &header, &mut main_file)
            .map_err(|e| Error::Internal(format!(
                "condition (a)+(b) FAIL: JournalManager::open_or_create failed after crash [scenario {:?} seed {seed}]: {e}",
                scenario
            )))?;
        validate(&journal, &mut main_file, scenario, seed)?;
        Ok(())
    }

    #[test]
    fn crash_recovery_500_cycles() {
        let mut failures: Vec<String> = Vec::new();
        let mut total: u32 = 0;
        for scenario in &ALL_SCENARIOS {
            for cycle in 0..CYCLES_PER_SCENARIO {
                total += 1;
                let seed = cycle;
                if let Err(e) = run_cycle(*scenario, seed) {
                    failures.push(format!(
                        "  [cycle {total}/500 | scenario {:?} | seed {seed}] {e}",
                        scenario
                    ));
                }
            }
        }
        if !failures.is_empty() {
            panic!(
                "CRASH RECOVERY FAILURES — {}/{} cycles failed:\n{}\n\
                 Hint: re-run with `RUST_BACKTRACE=1 cargo test crash_recovery` to reproduce.",
                failures.len(), total, failures.join("\n")
            );
        }
    }
}
