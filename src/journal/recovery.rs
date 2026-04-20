//! Journal recovery — replay an existing journal into the main file.
//!
//! This submodule owns the `recover_existing` entry point called from
//! [`JournalManager::open_or_create`].

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;

use super::log_file::{
    try_skip_chain_commit, JournalFrameHeader, JournalHeader, JournalPageSize,
    JOURNAL_FRAME_HEADER_SIZE, JOURNAL_HEADER_SIZE,
};
use super::shm::JournalIndex;
use super::{write_page_to_main, JournalManager};

impl JournalManager {
    /// Replay an existing journal into the main file.
    ///
    /// Returns `None` if the journal is stale (salt mismatch) and was deleted.
    /// Returns `Some(JournalManager)` if recovery succeeded (including the empty
    /// case where the journal had no committed frames).
    pub(super) fn recover_existing(
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
        // Fold every ChainCommit frame's `commit_ts` into a running max.
        // The backend reads the max via `recovered_max_commit_ts` after
        // `open_or_create` returns and floors `TimestampOracle` at
        // `max.successor()`.
        let mut max_commit_ts: Option<Ts> = None;

        loop {
            let frame_offset = write_cursor;

            // Peek for a `ChainCommit` frame first. These carry no legacy
            // `JournalFrameHeader` and would crash the scan if parsed as one.
            // `try_skip_chain_commit` advances the reader past a valid
            // ChainCommit and returns its length; otherwise it restores
            // position and returns None for legacy fall-through.
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
                max_commit_ts = Some(max_commit_ts.map_or(commit_ts, |prev| prev.max(commit_ts)));
                continue;
            }

            // Bad checksum or EOF — stop here
            let Some(frame_hdr) = JournalFrameHeader::read(&mut journal_file, salt1, salt2)?
            else {
                break;
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
}
