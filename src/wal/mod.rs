//! Write-ahead log (WAL) — durability, recovery, checkpoint.
//!
//! This is a private internal module. The public API is exposed through
//! [`Database`](crate::database::Database) (checkpoint, close, durability configuration).
//!
//! ## Overview
//!
//! The WAL implements crash-safe durability using a three-file model:
//!
//! | File | Purpose |
//! |------|---------|
//! `db.mqlite` | Main database file (pages after last checkpoint) |
//! `db.mqlite-wal` | Append-only log of modified pages |
//! `db.mqlite-shm` | WAL index hash table + reader/writer coordination |
//!
//! On clean close, [`WalManager::close_and_cleanup`] checkpoints all WAL
//! pages into the main file and deletes the WAL and SHM files, leaving only
//! `db.mqlite`.
//!
//! ## Phase 1 Implementation
//!
//! - hq-dz9: WAL and SHM implementation
//!
//! ## Fallback: Linear WAL Scan
//!
//! Per scale.md §Decision Trigger: if the SHM hash-table-based WAL index
//! proves unworkable (correctness or platform issues), fall back to a linear
//! WAL scan for page lookups.  The linear scan is O(WAL frames) per page read
//! but requires no SHM file.  It is acceptable for Phase 1 if WAL is
//! aggressively checkpointed (every ~100 pages).  The [`WalManager::read_page_linear`]
//! method implements this fallback.

// Phase 1: WAL is implemented but not yet wired into the write/read paths.
// Allow dead_code until the database handle integrates WAL in a later phase.
#[allow(dead_code)]
pub(crate) mod shm;
#[allow(dead_code)]
pub(crate) mod wal_file;

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::storage::header::FileHeader;

use self::shm::ShmIndex;
use self::wal_file::{
    WalFrameHeader, WalHeader, WalPageSize, WAL_FRAME_HEADER_SIZE, WAL_HEADER_SIZE,
};

// ---------------------------------------------------------------------------
// WalManager
// ---------------------------------------------------------------------------

/// Manages the write-ahead log and shared memory WAL index for one database.
///
/// Created via [`WalManager::open_or_create`].  On clean shutdown call
/// [`WalManager::close_and_cleanup`]; on crash, the next `open_or_create`
/// automatically runs recovery.
pub(crate) struct WalManager {
    /// Path to the `.mqlite-wal` file.
    wal_path: PathBuf,
    /// Path to the `.mqlite-shm` file.
    shm_path: PathBuf,
    /// Open handle to the WAL file (positioned at the write cursor).
    wal_file: File,
    /// In-memory WAL index (SHM).
    shm: ShmIndex,
    /// Salt 1 from the main file header (stored in every WAL frame).
    salt1: u32,
    /// Salt 2 from the main file header.
    salt2: u32,
    /// Checkpoint sequence counter from the WAL file header.
    checkpoint_seq: u32,
    /// Byte offset of the next frame to write (append cursor).
    write_cursor: u64,
    /// Total database page count as of the last committed WAL frame.
    /// Carried forward across commits; `None` if no commit has occurred yet
    /// in this WAL.
    last_committed_db_page_count: Option<u32>,
}

impl WalManager {
    // -----------------------------------------------------------------------
    // Open / recovery
    // -----------------------------------------------------------------------

    /// Open or create the WAL for the database at `db_path`.
    ///
    /// If a WAL file already exists, [`recover`](Self::recover) is called to
    /// replay any committed frames into the main file before returning.
    ///
    /// `main_header` is the file header of the main database file.  Its salt
    /// fields are used to detect stale WAL files.
    ///
    /// `main_file` is an open handle to the main database file, needed during
    /// recovery to write checkpointed pages.
    pub(crate) fn open_or_create(
        db_path: &Path,
        main_header: &FileHeader,
        main_file: &mut File,
    ) -> Result<Self> {
        let wal_path = wal_path_for(db_path);
        let shm_path = shm_path_for(db_path);
        let salt1 = main_header.wal_salt1;
        let salt2 = main_header.wal_salt2;

        // Does a WAL file already exist?
        if wal_path.exists() {
            // Try to recover it.
            let recovered = Self::recover_existing(&wal_path, &shm_path, salt1, salt2, main_file)?;
            if let Some(mgr) = recovered {
                return Ok(mgr);
            }
            // If recover_existing returned None, the WAL was stale/corrupt and
            // has been deleted.  Fall through to create a fresh WAL.
        }

        // Create a new WAL file.
        let mut wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&wal_path)
            .map_err(Error::Io)?;

        let header = WalHeader::new(salt1, salt2);
        wal_file.write_all(&header.to_bytes()).map_err(Error::Io)?;
        wal_file.flush().map_err(Error::Io)?;

        Ok(Self {
            wal_path,
            shm_path,
            wal_file,
            shm: ShmIndex::new(),
            salt1,
            salt2,
            checkpoint_seq: 0,
            write_cursor: WAL_HEADER_SIZE as u64,
            last_committed_db_page_count: None,
        })
    }

    /// Replay an existing WAL into the main file.
    ///
    /// Returns `None` if the WAL is stale (salt mismatch) and was deleted.
    /// Returns `Some(WalManager)` if recovery succeeded (including the empty
    /// case where the WAL had no committed frames).
    fn recover_existing(
        wal_path: &Path,
        shm_path: &Path,
        salt1: u32,
        salt2: u32,
        main_file: &mut File,
    ) -> Result<Option<WalManager>> {
        let mut wal_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(wal_path)
            .map_err(Error::Io)?;

        // Read and validate the WAL header.
        let mut header_buf = [0u8; WAL_HEADER_SIZE];
        match wal_file.read_exact(&mut header_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // Empty or truncated WAL — delete and recreate.
                drop(wal_file);
                let _ = std::fs::remove_file(wal_path);
                let _ = std::fs::remove_file(shm_path);
                return Ok(None);
            }
            Err(e) => return Err(Error::Io(e)),
        }

        let wal_header = match WalHeader::from_bytes(&header_buf) {
            Ok(h) => h,
            Err(_) => {
                // Corrupt WAL header — treat as stale and delete.
                drop(wal_file);
                let _ = std::fs::remove_file(wal_path);
                let _ = std::fs::remove_file(shm_path);
                return Ok(None);
            }
        };

        // Salt mismatch → stale WAL from a different database open session.
        if wal_header.salt1 != salt1 || wal_header.salt2 != salt2 {
            drop(wal_file);
            let _ = std::fs::remove_file(wal_path);
            let _ = std::fs::remove_file(shm_path);
            return Ok(None);
        }

        let checkpoint_seq = wal_header.checkpoint_seq;

        #[cfg(feature = "tracing")]
        let _rec_start = std::time::Instant::now();
        #[cfg(feature = "tracing")]
        let mut _frames_replayed: u64 = 0;

        // Scan frames: collect committed batches.
        // A "batch" is a sequence of non-commit frames followed by one commit frame.
        // On recovery we apply each committed batch to the main file.
        let mut shm = ShmIndex::new();
        let mut pending: Vec<(u32, WalPageSize, Vec<u8>, u64)> = Vec::new(); // (page_num, size, data, offset)
        let mut write_cursor = WAL_HEADER_SIZE as u64;
        let mut last_committed_db_page_count: Option<u32> = None;

        loop {
            let frame_offset = write_cursor;
            let frame_opt = WalFrameHeader::read(&mut wal_file, salt1, salt2)?;
            let frame_hdr = match frame_opt {
                None => break, // Bad checksum or EOF — stop here
                Some(h) => h,
            };

            // Read the page data (already consumed by WalFrameHeader::read, but
            // we need to re-read it; actually read inside read() already - we need
            // to store it differently).
            // NOTE: WalFrameHeader::read above already consumed the page data bytes
            // from the reader (to validate checksum), but didn't return them.
            // We need to re-read. Let's re-position and re-read.
            //
            // Actually, WalFrameHeader::read reads header+data together for checksum.
            // We need the data separately.  Let me re-read from the known offset.
            let page_size_bytes = frame_hdr.page_size.bytes();
            let data_offset = frame_offset + WAL_FRAME_HEADER_SIZE as u64;

            wal_file
                .seek(SeekFrom::Start(data_offset))
                .map_err(Error::Io)?;
            let mut page_data = vec![0u8; page_size_bytes];
            wal_file.read_exact(&mut page_data).map_err(Error::Io)?;

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
                    // Update SHM index.
                    shm.insert(*pn, *off);
                }
                last_committed_db_page_count = Some(db_page_count);
                #[cfg(feature = "tracing")]
                {
                    _frames_replayed += pending.len() as u64;
                }
                pending.clear();

                // Seek back to after this commit frame for next iteration.
                wal_file
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

        // Rebuild SHM — we built it during scan above.
        // Persist SHM to disk.
        shm.save(shm_path)?;

        // Reposition WAL file at write cursor for new appends.
        wal_file
            .seek(SeekFrom::Start(write_cursor))
            .map_err(Error::Io)?;

        #[cfg(feature = "tracing")]
        {
            let duration_ms = _rec_start.elapsed().as_millis() as u64;
            tracing::warn!(
                target: "mqlite",
                frames_replayed = _frames_replayed,
                duration_ms,
                "mqlite::wal_recovery"
            );
        }

        Ok(Some(WalManager {
            wal_path: wal_path.to_path_buf(),
            shm_path: shm_path.to_path_buf(),
            wal_file,
            shm,
            salt1,
            salt2,
            checkpoint_seq,
            write_cursor,
            last_committed_db_page_count,
        }))
    }

    // -----------------------------------------------------------------------
    // Writing (appending frames)
    // -----------------------------------------------------------------------

    /// Append a non-commit WAL frame for `page_number`.
    ///
    /// Call this for each modified page within a transaction, then call
    /// [`commit`](Self::commit) when the transaction is complete.
    ///
    /// `page_data` must be exactly `page_size.bytes()` bytes.
    pub(crate) fn append_non_commit(
        &mut self,
        page_number: u32,
        page_size: WalPageSize,
        page_data: &[u8],
    ) -> Result<u64> {
        debug_assert_eq!(page_data.len(), page_size.bytes());
        self.append_frame(page_number, 0, page_size, page_data)
    }

    /// Append a commit WAL frame, completing the current transaction.
    ///
    /// `db_page_count` is the total number of database pages after this commit
    /// (stored in the commit frame so recovery can update the main file header).
    ///
    /// After this call, the SHM index is updated and flushed.  Returns `true`
    /// if an emergency checkpoint should be triggered (WAL index is 75% full).
    pub(crate) fn commit(
        &mut self,
        page_number: u32,
        page_size: WalPageSize,
        page_data: &[u8],
        db_page_count: u32,
    ) -> Result<bool> {
        debug_assert!(
            db_page_count > 0,
            "commit frame must have non-zero page count"
        );
        let offset = self.append_frame(page_number, db_page_count, page_size, page_data)?;
        self.last_committed_db_page_count = Some(db_page_count);

        // Update SHM index with the commit frame's page.
        let emergency = self.shm.insert(page_number, offset);
        self.shm.save(&self.shm_path)?;

        Ok(emergency)
    }

    /// Low-level frame append.  Returns the byte offset of the written frame.
    fn append_frame(
        &mut self,
        page_number: u32,
        db_page_count: u32,
        page_size: WalPageSize,
        page_data: &[u8],
    ) -> Result<u64> {
        let frame_offset = self.write_cursor;
        self.wal_file
            .seek(SeekFrom::Start(frame_offset))
            .map_err(Error::Io)?;

        let frame_hdr = WalFrameHeader {
            page_number,
            db_page_count,
            salt1: self.salt1,
            salt2: self.salt2,
            page_size,
        };
        frame_hdr
            .write(&mut self.wal_file, page_data)
            .map_err(Error::Io)?;
        self.wal_file.flush().map_err(Error::Io)?;

        self.write_cursor += (WAL_FRAME_HEADER_SIZE + page_size.bytes()) as u64;

        // Update SHM index for non-commit frames too (so readers can see
        // in-progress writes within the same process — single-process Phase 1).
        // For multi-process, this would only happen after commit.
        if db_page_count == 0 {
            self.shm.insert(page_number, frame_offset);
        }

        Ok(frame_offset)
    }

    // -----------------------------------------------------------------------
    // Reading
    // -----------------------------------------------------------------------

    /// Look up `page_number` in the WAL.
    ///
    /// Returns the page data if found, or `None` if the page should be read
    /// from the main file.
    ///
    /// Uses the SHM hash table for O(1) lookup.
    pub(crate) fn read_page(&mut self, page_number: u32) -> Result<Option<Vec<u8>>> {
        let frame_offset = match self.shm.lookup(page_number) {
            Some(off) => off,
            None => return Ok(None),
        };

        // Read the frame header at the recorded offset.
        self.wal_file
            .seek(SeekFrom::Start(frame_offset))
            .map_err(Error::Io)?;

        let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE];
        self.wal_file
            .read_exact(&mut header_buf)
            .map_err(Error::Io)?;

        let page_size_u32 = u32::from_le_bytes(header_buf[16..20].try_into().expect("4 bytes"));
        let page_size = WalPageSize::from_u32(page_size_u32)?;

        let mut page_data = vec![0u8; page_size.bytes()];
        self.wal_file
            .read_exact(&mut page_data)
            .map_err(Error::Io)?;

        Ok(Some(page_data))
    }

    /// Fallback: linear WAL scan to find `page_number`.
    ///
    /// O(WAL frames) per lookup.  Acceptable for Phase 1 with aggressive
    /// checkpointing.  See scale.md §WAL design fallback.
    pub(crate) fn read_page_linear(&mut self, page_number: u32) -> Result<Option<Vec<u8>>> {
        self.wal_file
            .seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))
            .map_err(Error::Io)?;

        let mut latest: Option<Vec<u8>> = None;
        let mut cursor = WAL_HEADER_SIZE as u64;

        loop {
            self.wal_file
                .seek(SeekFrom::Start(cursor))
                .map_err(Error::Io)?;

            let frame_opt = WalFrameHeader::read(&mut self.wal_file, self.salt1, self.salt2)?;
            let frame_hdr = match frame_opt {
                None => break,
                Some(h) => h,
            };

            let page_size_bytes = frame_hdr.page_size.bytes();
            // Read the page data
            let data_offset = cursor + WAL_FRAME_HEADER_SIZE as u64;
            self.wal_file
                .seek(SeekFrom::Start(data_offset))
                .map_err(Error::Io)?;
            let mut page_data = vec![0u8; page_size_bytes];
            self.wal_file
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

    /// Checkpoint all committed WAL frames into the main file.
    ///
    /// After a successful checkpoint:
    /// 1. The WAL file is truncated to just the header.
    /// 2. The SHM index is cleared.
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
        let _wal_size_before = self.write_cursor;

        // Collect all entries from the SHM index.
        let entries: Vec<(u32, u64)> = self.shm.iter_entries().collect();

        // For each indexed page, read the WAL frame and write to main file.
        for (page_number, frame_offset) in &entries {
            self.wal_file
                .seek(SeekFrom::Start(
                    *frame_offset + WAL_FRAME_HEADER_SIZE as u64,
                ))
                .map_err(Error::Io)?;

            // Read page_size from the frame header.
            let header_offset = *frame_offset;
            self.wal_file
                .seek(SeekFrom::Start(header_offset))
                .map_err(Error::Io)?;
            let mut hbuf = [0u8; WAL_FRAME_HEADER_SIZE];
            self.wal_file.read_exact(&mut hbuf).map_err(Error::Io)?;
            let page_size_u32 = u32::from_le_bytes(hbuf[16..20].try_into().expect("4 bytes"));
            let page_size_bytes = WalPageSize::from_u32(page_size_u32)?.bytes();

            let mut page_data = vec![0u8; page_size_bytes];
            self.wal_file
                .read_exact(&mut page_data)
                .map_err(Error::Io)?;

            write_page_to_main(main_file, *page_number, page_size_bytes, &page_data)?;
        }

        // Update main file header with latest committed page count.
        if let Some(db_page_count) = self.last_committed_db_page_count {
            main_header.total_page_count = db_page_count;
        }
        main_file.flush().map_err(Error::Io)?;

        // Reset WAL to empty (truncate to just the header).
        self.truncate_wal()?;

        // Clear SHM index.
        self.shm.clear_index();
        self.shm.save(&self.shm_path)?;

        #[cfg(feature = "tracing")]
        {
            let duration_ms = _chk_start.elapsed().as_millis() as u64;
            tracing::info!(
                target: "mqlite",
                pages_copied = entries.len() as u64,
                duration_ms,
                wal_size_before = _wal_size_before,
                "mqlite::checkpoint"
            );
        }

        Ok(())
    }

    /// Checkpoint all WAL frames, then delete the WAL and SHM files.
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

        // Delete WAL file.
        drop(self.wal_file);
        let _ = std::fs::remove_file(&self.wal_path);

        // Delete SHM file.
        let _ = std::fs::remove_file(&self.shm_path);

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Return the current WAL write cursor (byte offset past the last frame).
    pub(crate) fn write_cursor(&self) -> u64 {
        self.write_cursor
    }

    /// Return a reference to the SHM index (for inspection in tests).
    pub(crate) fn shm(&self) -> &ShmIndex {
        &self.shm
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Truncate the WAL file to just its 32-byte header and reposition the
    /// write cursor.
    fn truncate_wal(&mut self) -> Result<()> {
        self.wal_file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;

        // Re-write header with incremented checkpoint sequence.
        self.checkpoint_seq = self.checkpoint_seq.wrapping_add(1);
        let header = WalHeader {
            magic: self::wal_file::WAL_MAGIC,
            format_version: self::wal_file::WAL_FORMAT_VERSION,
            page_size_internal: crate::storage::page::PAGE_SIZE_INTERNAL,
            page_size_leaf: crate::storage::page::PAGE_SIZE_LEAF,
            salt1: self.salt1,
            salt2: self.salt2,
            checkpoint_seq: self.checkpoint_seq,
        };
        self.wal_file
            .write_all(&header.to_bytes())
            .map_err(Error::Io)?;
        self.wal_file
            .set_len(WAL_HEADER_SIZE as u64)
            .map_err(Error::Io)?;
        self.wal_file.flush().map_err(Error::Io)?;

        self.write_cursor = WAL_HEADER_SIZE as u64;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Path helpers
// ---------------------------------------------------------------------------

/// Derive the WAL path from the main database path.
pub(crate) fn wal_path_for(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-wal");
    PathBuf::from(p)
}

/// Derive the SHM path from the main database path.
pub(crate) fn shm_path_for(db_path: &Path) -> PathBuf {
    let mut p = db_path.as_os_str().to_owned();
    p.push("-shm");
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
/// tracks size separately.  For WAL replay, we rely on the `page_size_bytes`
/// recorded in the WAL frame rather than deriving it from the page number.
pub(crate) fn write_page_to_main(
    main_file: &mut File,
    page_number: u32,
    page_size_bytes: usize,
    page_data: &[u8],
) -> Result<()> {
    let offset = page_number as u64 * page_size_bytes as u64;
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
    use tempfile::NamedTempFile;
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
    fn wal_path_derivation() {
        let db = Path::new("/tmp/foo.mqlite");
        assert_eq!(wal_path_for(db), PathBuf::from("/tmp/foo.mqlite-wal"));
        assert_eq!(shm_path_for(db), PathBuf::from("/tmp/foo.mqlite-shm"));
    }

    // -----------------------------------------------------------------------
    // Open / create
    // -----------------------------------------------------------------------

    #[test]
    fn open_creates_wal_and_shm_paths() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mgr = WalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let wal_path = wal_path_for(&db_path);
        assert!(wal_path.exists(), "WAL file must be created");
        // SHM is persisted only after first commit, so we don't check it here.

        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // Append and read back
    // -----------------------------------------------------------------------

    #[test]
    fn append_and_read_4k() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr = WalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_data = make_page_4k(0xAB);
        mgr.append_non_commit(3, WalPageSize::Small4k, &page_data)
            .unwrap();

        let result = mgr.read_page(3).unwrap();
        assert_eq!(result, Some(page_data));
        assert!(mgr.read_page(99).unwrap().is_none());
    }

    #[test]
    fn append_and_read_32k() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr = WalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_data = make_page_32k(0xCC);
        mgr.append_non_commit(10, WalPageSize::Large32k, &page_data)
            .unwrap();

        let result = mgr.read_page(10).unwrap();
        assert_eq!(result, Some(page_data));
    }

    #[test]
    fn latest_write_wins() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr = WalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_v1 = make_page_4k(0x01);
        let page_v2 = make_page_4k(0x02);
        mgr.append_non_commit(5, WalPageSize::Small4k, &page_v1)
            .unwrap();
        mgr.append_non_commit(5, WalPageSize::Small4k, &page_v2)
            .unwrap();

        // SHM lookup returns offset of latest (second) frame.
        let result = mgr.read_page(5).unwrap().unwrap();
        assert_eq!(result[0], 0x02);
    }

    // -----------------------------------------------------------------------
    // Commit
    // -----------------------------------------------------------------------

    #[test]
    fn commit_frame_marks_transaction_boundary() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr = WalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_a = make_page_4k(0xAA);
        let page_b = make_page_4k(0xBB);
        mgr.append_non_commit(1, WalPageSize::Small4k, &page_a)
            .unwrap();
        let emergency = mgr.commit(2, WalPageSize::Small4k, &page_b, 10).unwrap();
        assert!(!emergency);
        assert_eq!(mgr.last_committed_db_page_count, Some(10));
    }

    // -----------------------------------------------------------------------
    // Checkpoint
    // -----------------------------------------------------------------------

    #[test]
    fn checkpoint_writes_pages_to_main_file() {
        let (dir, db_path, mut main_file) = make_db_file();
        // Pre-allocate main file large enough
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();

        let mut header = make_header();
        let mut mgr = WalManager::open_or_create(&db_path, &mut header, &mut main_file).unwrap();

        let page_data = make_page_4k(0x42);
        mgr.append_non_commit(2, WalPageSize::Small4k, &page_data)
            .unwrap();
        mgr.commit(2, WalPageSize::Small4k, &page_data, 5).unwrap();

        mgr.checkpoint(&mut main_file, &mut header).unwrap();

        // Verify: page 2 in main file should now contain the data.
        let offset = 2u64 * PAGE_SIZE_INTERNAL as u64;
        main_file.seek(SeekFrom::Start(offset)).unwrap();
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0x42);

        // WAL should be reset.
        assert_eq!(mgr.write_cursor, WAL_HEADER_SIZE as u64);
        assert_eq!(mgr.shm.occupied_count(), 0);
    }

    #[test]
    fn checkpoint_increments_sequence() {
        let (dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let mut header = make_header();
        let mut mgr = WalManager::open_or_create(&db_path, &mut header, &mut main_file).unwrap();
        assert_eq!(mgr.checkpoint_seq, 0);

        let page_data = make_page_4k(0x01);
        mgr.commit(1, WalPageSize::Small4k, &page_data, 2).unwrap();
        mgr.checkpoint(&mut main_file, &mut header).unwrap();

        assert_eq!(mgr.checkpoint_seq, 1);
    }

    // -----------------------------------------------------------------------
    // Recovery — crash simulation
    // -----------------------------------------------------------------------

    #[test]
    fn recovery_replays_committed_frames() {
        let (dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let header = make_header();

        // Write two frames and commit.
        {
            let mut mgr = WalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

            let page_a = make_page_4k(0xAA);
            let page_b = make_page_4k(0xBB);
            mgr.append_non_commit(1, WalPageSize::Small4k, &page_a)
                .unwrap();
            mgr.commit(2, WalPageSize::Small4k, &page_b, 5).unwrap();
            // Simulate crash: don't call close_and_cleanup.
            // WAL file left on disk.
        }

        // Reopen — recovery runs automatically.
        let mut main_file2 = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mgr2 = WalManager::open_or_create(&db_path, &header, &mut main_file2).unwrap();

        // Both pages should have been replayed into main file.
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file2
            .seek(SeekFrom::Start(1 * PAGE_SIZE_INTERNAL as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0xAA, "page 1 should be replayed");

        main_file2
            .seek(SeekFrom::Start(2 * PAGE_SIZE_INTERNAL as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0xBB, "page 2 should be replayed");
    }

    #[test]
    fn recovery_discards_uncommitted_frames() {
        let (dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let header = make_header();

        // Write one committed frame, then one uncommitted (simulated crash mid-tx).
        {
            let mut mgr = WalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

            let page_committed = make_page_4k(0xCC);
            let page_uncommitted = make_page_4k(0xDD);
            mgr.commit(1, WalPageSize::Small4k, &page_committed, 3)
                .unwrap();
            // Append non-commit frame — transaction never completed.
            mgr.append_non_commit(2, WalPageSize::Small4k, &page_uncommitted)
                .unwrap();
            // Crash: no commit for page 2.
        }

        let mut main_file2 = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mgr2 = WalManager::open_or_create(&db_path, &header, &mut main_file2).unwrap();

        // Page 1 (committed) should be in main file.
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file2
            .seek(SeekFrom::Start(1 * PAGE_SIZE_INTERNAL as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0xCC, "committed page must be present");

        // Page 2 (uncommitted) — SHM should NOT have it after recovery.
        assert!(
            mgr2.shm().lookup(2).is_none(),
            "uncommitted page must not be in SHM after recovery"
        );
    }

    #[test]
    fn stale_wal_is_deleted_on_open() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        // Create WAL with original salts.
        {
            let mgr = WalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        }
        assert!(wal_path_for(&db_path).exists());

        // Reopen with different salts (simulates a different database open).
        let mut different_header = FileHeader::new(1_700_000_000_001, 0x1111_1111, 0x2222_2222);
        let mgr2 = WalManager::open_or_create(&db_path, &different_header, &mut main_file).unwrap();
        // A fresh WAL should have been created with the new salts.
        assert_eq!(mgr2.salt1, 0x1111_1111);
        assert_eq!(mgr2.salt2, 0x2222_2222);
    }

    // -----------------------------------------------------------------------
    // Clean close
    // -----------------------------------------------------------------------

    #[test]
    fn close_and_cleanup_removes_wal_and_shm() {
        let (dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let mut header = make_header();

        let mut mgr = WalManager::open_or_create(&db_path, &mut header, &mut main_file).unwrap();
        let page_data = make_page_4k(0xFF);
        mgr.commit(1, WalPageSize::Small4k, &page_data, 2).unwrap();

        let wal_path = wal_path_for(&db_path);
        let shm_path = shm_path_for(&db_path);

        mgr.close_and_cleanup(&mut main_file, &mut header).unwrap();

        assert!(!wal_path.exists(), "WAL must be deleted after clean close");
        assert!(!shm_path.exists(), "SHM must be deleted after clean close");
    }

    // -----------------------------------------------------------------------
    // Linear scan fallback
    // -----------------------------------------------------------------------

    #[test]
    fn linear_scan_finds_committed_pages() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr = WalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_data = make_page_4k(0x77);
        mgr.append_non_commit(7, WalPageSize::Small4k, &page_data)
            .unwrap();

        let result = mgr.read_page_linear(7).unwrap();
        assert_eq!(result, Some(page_data));
        assert!(mgr.read_page_linear(999).unwrap().is_none());
    }
}
