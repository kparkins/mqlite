//! Journal recovery — replay an existing journal into the main file.
//!
//! This submodule owns the `recover_existing` entry point called from
//! [`JournalManager::open_or_create`].

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::storage::header::FileHeader;

use super::log_file::{
    read_chain_commit_at_cursor, JournalHeader, LogicalTxnFrame, JOURNAL_FORMAT_VERSION,
    JOURNAL_HEADER_SIZE, JOURNAL_MAGIC, RETIRED_PRE_RELEASE_JOURNAL_FORMAT_VERSIONS,
};
use super::shm::JournalIndex;
use super::CheckpointRecoveryFrameKind;
use super::JournalManager;

// ---------------------------------------------------------------------------
// ParsedLogicalFrames (Phase 2 §5.3) — Pass 1 → Pass 2 hand-off
// ---------------------------------------------------------------------------

/// Collection of `LogicalTxnFrame`s parsed during Pass 1 recovery, handed
/// off to Pass 2 post-open validation (§5.2) via
/// [`JournalManager::take_parsed_logical_frames`].
///
/// `frames` stores `(byte_offset, frame)` in journal order. `seen_commit_ts`
/// is the dedup set used by Pass 1 to enforce §3.8(a)/(d) first-wins
/// semantics — any duplicate `commit_ts` is dropped with a tracing warning.
/// Pass 2 (US-014) post-processes this further (ChainCommit-only HLC floor,
/// orphan-logical sweep).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct RecoveryCaseCCandidate {
    /// Commit timestamp carried by the orphan `ChainCommit`.
    pub(crate) commit_ts: Ts,
    /// Starting byte offset of the orphan `ChainCommit` in the journal.
    pub(crate) chain_commit_offset: u64,
}

#[derive(Debug, Default)]
#[allow(dead_code)]
pub(crate) struct ParsedLogicalFrames {
    /// Parsed frames with their byte offset in the journal, in scan order.
    pub(crate) frames: Vec<(u64, LogicalTxnFrame)>,
    /// Commit-ts values already seen — first-wins dedup set.
    pub(crate) seen_commit_ts: HashSet<Ts>,
    /// Phase 7 case-c candidates: durable `ChainCommit` frames that have
    /// no matching `LogicalTxnFrame` after Pass 1 matching and frontier cull.
    pub(crate) case_c_candidates: Vec<RecoveryCaseCCandidate>,
}

// Compile-time guard: ParsedLogicalFrames must be Send so it can cross the
// Pass 1 / Pass 2 hand-off (SharedState::new runs on the caller's thread,
// but the crate invariant is that all recovery artifacts are Send).
#[allow(dead_code)]
fn _assert_parsed_logical_frames_send() {
    fn assert_send<T: Send>() {}
    assert_send::<ParsedLogicalFrames>();
}

impl JournalManager {
    /// Replay an existing journal into the main file.
    ///
    /// Returns `None` if the journal is stale (salt mismatch) and was deleted.
    /// Returns `Some(JournalManager)` if recovery succeeded (including the empty
    /// case where the journal had no committed frames).
    pub(super) fn recover_existing(
        journal_path: &Path,
        main_header: &FileHeader,
        main_file: &mut File,
    ) -> Result<Option<JournalManager>> {
        let salt1 = main_header.wal_salt1;
        let salt2 = main_header.wal_salt2;
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
            Err(err) => {
                let magic: [u8; 4] = header_buf[0..4].try_into().expect("4 bytes");
                let format_version =
                    u32::from_le_bytes(header_buf[4..8].try_into().expect("4 bytes"));
                if magic == JOURNAL_MAGIC
                    && RETIRED_PRE_RELEASE_JOURNAL_FORMAT_VERSIONS.contains(&format_version)
                {
                    drop(journal_file);
                    let _ = std::fs::remove_file(journal_path);
                    return Ok(None);
                }
                if magic == JOURNAL_MAGIC && format_version != JOURNAL_FORMAT_VERSION {
                    return Err(err);
                }
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

        // Scan the active logical/checkpoint journal. Phase 6 does not parse
        // retired generic page-frame batches during recovery.
        let index = JournalIndex::new();
        let mut pending_checkpoint_start_offset: Option<u64> = None;
        let mut write_cursor = JOURNAL_HEADER_SIZE as u64;
        let mut last_committed_db_page_count: Option<u32> = None;
        // Fold every ChainCommit frame's `commit_ts` into a running max.
        // The backend reads the max via `recovered_max_commit_ts` after
        // `open_or_create` returns and floors `TimestampOracle` at
        // `max.successor()`.
        let mut max_commit_ts: Option<Ts> = None;
        // Phase 2 §5.1 / §3.8 — Pass 1 collects logical frames for Pass 2
        // validation (§5.2). Dedup by commit_ts via first-wins HashSet
        // per §3.8(a)/(d). Pass 1 does NOT touch the catalog; all catalog
        // resolution happens in Pass 2.
        let mut parsed_logical = ParsedLogicalFrames::default();
        // Phase 2 §3.10 / §3.8(b) — the set of commit_ts values observed
        // on durable ChainCommit frames. Used AFTER the scan for the
        // orphan-logical sweep: any logical frame whose commit_ts is not
        // in this set has no matching ChainCommit and is dropped (case (b)).
        let mut seen_chain_commit_ts: HashSet<Ts> = HashSet::new();
        let mut chain_commit_candidates: Vec<RecoveryCaseCCandidate> = Vec::new();
        // Phase 7 recovered page-0 frontier. The main-file header is the
        // baseline; valid page-0 checkpoint boundaries in the journal can
        // advance the recovered image. Logical frames at or below the final
        // frontier are already reconciled to the main file.
        let mut highest_checkpoint_ts: Option<Ts> = (main_header.last_checkpoint_ts
            != Ts::default())
        .then_some(main_header.last_checkpoint_ts);
        let mut durable_checkpoint_page_count: Option<u32> = None;

        loop {
            let frame_offset = write_cursor;

            journal_file
                .seek(SeekFrom::Start(frame_offset))
                .map_err(Error::Io)?;
            if let Some((n, commit_ts, chain_commit_offset)) =
                read_chain_commit_at_cursor(&mut journal_file, salt1, salt2)?
            {
                // ChainCommit frame replay is a no-op for the page-copy loop
                // because it carries no single page number. Version-chain
                // state is rebuilt on demand; the only recovery-critical
                // datum is `commit_ts`, which folds into `max_commit_ts`
                // so the HLC oracle lifts above every durable commit.
                write_cursor += n;
                max_commit_ts = Some(max_commit_ts.map_or(commit_ts, |prev| prev.max(commit_ts)));
                // §3.10 / §3.8(b) — record the commit_ts for the
                // orphan-logical sweep run after the main scan.
                seen_chain_commit_ts.insert(commit_ts);
                chain_commit_candidates.push(RecoveryCaseCCandidate {
                    commit_ts,
                    chain_commit_offset,
                });
                crate::mvcc::metrics::record_recovery_chain_commit_frame();
                continue;
            }

            // Phase 2 §5.1: parse `LogicalTxnFrame` frames and collect them
            // into `parsed_logical` for Pass 2 validation (§5.2). The
            // logical frame is parsed and validated here (US-006 scanner
            // semantics) but never mutates durable state — Phase 2 is a
            // correctness bridge, not a replayer (§3.3). commit_ts is NOT
            // folded into `max_commit_ts` — §3.10 reserves HLC floor
            // advancement to ChainCommit frames only.
            //
            // The disposition helper distinguishes `Torn` (structural
            // signature matches but body truncated or CRC fails) from
            // `NotLogical` (try the next helper). Torn ticks the §7
            // `logical_txn_torn_frames_total` counter and halts the scan
            // since later bytes are downstream of an unfinished write.
            match crate::journal::log_file::try_skip_logical_txn_disposition(
                &mut journal_file,
                salt1,
                salt2,
            )? {
                crate::journal::log_file::LogicalScan::Valid(n, frame) => {
                    if parsed_logical.seen_commit_ts.insert(frame.commit_ts) {
                        parsed_logical.frames.push((frame_offset, frame));
                    } else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            target: "mqlite",
                            commit_ts = ?frame.commit_ts,
                            offset = frame_offset,
                            "duplicate LogicalTxnFrame commit_ts — first-wins dedup (§3.8)"
                        );
                    }
                    write_cursor += n;
                    continue;
                }
                crate::journal::log_file::LogicalScan::Torn => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        target: "mqlite",
                        offset = frame_offset,
                        "torn LogicalTxnFrame — scan halts here (§3.10/§7)"
                    );
                    crate::mvcc::metrics::record_logical_txn_torn_frame();
                    break;
                }
                crate::journal::log_file::LogicalScan::NotLogical => {}
            }

            if let Some(frame) =
                JournalManager::try_checkpoint_recovery_frame(&mut journal_file, salt1, salt2)?
            {
                match frame.kind {
                    CheckpointRecoveryFrameKind::BatchPage => {
                        pending_checkpoint_start_offset.get_or_insert(frame_offset);
                    }
                    CheckpointRecoveryFrameKind::Boundary {
                        checkpoint_ts,
                        db_page_count,
                    } => {
                        if let Some(prev) = highest_checkpoint_ts {
                            if checkpoint_ts < prev {
                                return Err(Error::CorruptDatabase {
                                    path: journal_path.to_path_buf(),
                                    detail: format!(
                                        "page-0 checkpoint boundary last_checkpoint_ts regressed: \
                                         prev={prev:?}, new={checkpoint_ts:?}"
                                    ),
                                    recoverable: false,
                                });
                            }
                        }
                        highest_checkpoint_ts = Some(
                            highest_checkpoint_ts
                                .map_or(checkpoint_ts, |prev| prev.max(checkpoint_ts)),
                        );
                        durable_checkpoint_page_count = Some(db_page_count);
                        last_committed_db_page_count = Some(db_page_count);
                        pending_checkpoint_start_offset = None;
                        crate::mvcc::metrics::record_recovery_page0_boundary_frame();
                    }
                }
                write_cursor = frame.next_cursor;
                continue;
            }

            break;
        }

        // Discard an incomplete checkpoint batch. Its page records are not
        // durable authority without a following page-0 boundary.
        if let Some(start) = pending_checkpoint_start_offset {
            write_cursor = start;
        }

        // §3.8(b) — orphan-logical sweep: any logical frame whose commit_ts
        // has no matching durable ChainCommit is dropped. Phase 2 tolerates
        // orphan frames (case (b) — logical-without-chain-commit is a
        // crash between S5 and S7); Phase 4 promotes this to a hard
        // error. §3.10 explicitly forbids logical-only commit_ts from
        // advancing the HLC floor, so the sweep also guarantees that no
        // orphan leaks into Pass 2 and thus no subsequent path could
        // accidentally observe a logical-only commit_ts.
        let before_sweep = parsed_logical.frames.len();
        // Snapshot logical-frame commit_ts set BEFORE the retain so we can
        // also detect unmatched ChainCommits (case (c) tolerance, §3.7
        // envelope violation; Phase 4 §8.13.3 promotes to hard error).
        let logical_ts_pre_sweep: HashSet<Ts> = parsed_logical
            .frames
            .iter()
            .map(|(_, frame)| frame.commit_ts)
            .collect();
        parsed_logical
            .frames
            .retain(|(_, frame)| seen_chain_commit_ts.contains(&frame.commit_ts));
        let dropped = before_sweep - parsed_logical.frames.len();
        for _ in 0..dropped {
            crate::mvcc::metrics::record_logical_txn_pass1_orphan_logical_dropped();
            // §7 / US-024 — sum every kind of recovery-discarded
            // frame into a single counter alongside the per-reason
            // counters above.
            crate::mvcc::metrics::record_logical_txn_recovery_discarded_frame();
        }
        if dropped > 0 {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: "mqlite",
                dropped,
                "orphan logical frames swept by Pass 1 (§3.8(b) logical-without-ChainCommit)"
            );
            // Rebuild the dedup set so it reflects only the surviving
            // frames — this keeps `seen_commit_ts` consistent with
            // `frames` for any Pass 2 consumer that iterates both.
            parsed_logical.seen_commit_ts.clear();
            for (_, frame) in &parsed_logical.frames {
                parsed_logical.seen_commit_ts.insert(frame.commit_ts);
            }
        }

        parsed_logical.case_c_candidates = chain_commit_candidates
            .into_iter()
            .filter(|candidate| !logical_ts_pre_sweep.contains(&candidate.commit_ts))
            .collect();

        let unmatched_chain_commits = parsed_logical.case_c_candidates.len();
        for _ in 0..unmatched_chain_commits {
            crate::mvcc::metrics::record_logical_txn_pass1_unmatched_chain_commit();
        }
        #[cfg(feature = "tracing")]
        if unmatched_chain_commits > 0 {
            tracing::warn!(
                target: "mqlite",
                unmatched = unmatched_chain_commits,
                "ChainCommit frames without matching LogicalTxnFrame (§3.7 envelope violation; \
                 Phase 7 validates post-frontier candidates as hard errors)"
            );
        }

        // Phase 7 page-0 boundary cull. After the orphan sweep
        // (above) and the HLC-floor ChainCommit fold (below), any logical
        // frame whose `commit_ts <= highest_checkpoint_ts` has already
        // been reconciled to the main file by a prior checkpoint and must
        // NOT be handed to Pass 2 for validation. Case-c candidates at or
        // below the same frontier are also safe because the corresponding
        // ChainCommit is checkpoint-covered. Runs unconditionally — if
        // `highest_checkpoint_ts` is `None`, the retain is a no-op. Also
        // rebuilds the `seen_commit_ts` dedup set so the two views stay
        // consistent.
        if let Some(hi) = highest_checkpoint_ts {
            let before_boundary = parsed_logical.frames.len();
            parsed_logical
                .frames
                .retain(|(_, frame)| frame.commit_ts > hi);
            let boundary_dropped = before_boundary - parsed_logical.frames.len();
            if boundary_dropped > 0 {
                parsed_logical.seen_commit_ts.clear();
                for (_, frame) in &parsed_logical.frames {
                    parsed_logical.seen_commit_ts.insert(frame.commit_ts);
                }
                #[cfg(feature = "tracing")]
                tracing::debug!(
                    target: "mqlite",
                    dropped = boundary_dropped,
                    last_checkpoint_ts = ?hi,
                    "pre-boundary logical frames culled by Pass 1"
                );
                for _ in 0..boundary_dropped {
                    crate::mvcc::metrics::record_logical_txn_pass1_pre_boundary_dropped();
                    crate::mvcc::metrics::record_logical_txn_recovery_discarded_frame();
                }
            }
            parsed_logical
                .case_c_candidates
                .retain(|candidate| candidate.commit_ts > hi);
        }

        // §7 / US-024 — `parsed_logical_frames_len` is a per-open
        // gauge: it reflects the size of the Pass 1 → Pass 2 hand-off
        // for THIS open. Set after all post-walk culls so the value
        // matches what Pass 2 will receive.
        crate::mvcc::metrics::set_parsed_logical_frames_len(parsed_logical.frames.len() as u64);

        // Flush main file after replaying all committed frames.
        if last_committed_db_page_count.is_some() {
            main_file.flush().map_err(Error::Io)?;
        }

        // The in-memory index was rebuilt during the scan above; nothing
        // to persist (the journal itself is the only durable artifact).

        // Reposition journal file at write cursor for new appends.
        let journal_len = journal_file.metadata().map_err(Error::Io)?.len();
        if write_cursor < journal_len {
            journal_file.set_len(write_cursor).map_err(Error::Io)?;
            journal_file.sync_data().map_err(Error::Io)?;
        }
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

        let mut manager = JournalManager {
            journal_path: journal_path.to_path_buf(),
            journal_file,
            index,
            salt1,
            salt2,
            checkpoint_seq,
            write_cursor,
            last_committed_db_page_count,
            recovered_max_commit_ts: max_commit_ts,
            parsed_logical_frames: parsed_logical,
            legacy_pending_start_offset: None,
            last_legacy_commit_end_offset: write_cursor,
            checkpoint_batch_active: None,
            next_checkpoint_batch_id: 1,
            checkpoint_frame_tags: std::collections::BTreeMap::new(),
        };
        if let Some(expected_total_page_count) = durable_checkpoint_page_count {
            manager.emergency_checkpoint_after_boundary(main_file, expected_total_page_count)?;
        }

        Ok(Some(manager))
    }
}
