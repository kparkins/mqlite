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

use super::log_file::{
    try_skip_chain_commit, try_skip_checkpoint_commit_boundary, BoundaryScan, JournalFrameHeader,
    JournalHeader, JournalPageSize, LogicalTxnFrame, JOURNAL_FRAME_HEADER_SIZE,
    JOURNAL_HEADER_SIZE,
};
use super::shm::JournalIndex;
use super::{write_page_to_main, JournalManager};

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
#[derive(Debug, Default)]
#[allow(dead_code)]
pub(crate) struct ParsedLogicalFrames {
    /// Parsed frames with their byte offset in the journal, in scan order.
    pub(crate) frames: Vec<(u64, LogicalTxnFrame)>,
    /// Commit-ts values already seen — first-wins dedup set.
    pub(crate) seen_commit_ts: HashSet<Ts>,
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
        let mut pending: Vec<(u32, JournalPageSize, Vec<u8>, u64)> = Vec::new(); // (page_num, size, data, offset)
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
        // Phase 2 §3.11 — highest `covers_commit_ts_hi` observed across all
        // VALID CheckpointCommitBoundaryFrames. After the main walk plus the
        // HLC-floor/orphan-logical sweep, logical frames with
        // `commit_ts <= covers_commit_ts_hi` are removed (they are already
        // fully reconciled to the main file). Torn/truncated boundary frames
        // are treated as absent per §3.11 point 4: the skip helper returns
        // `None` for them, and recovery resumes from the previous valid
        // boundary's `covers_commit_ts_hi` because only valid boundaries
        // update this running max.
        let mut highest_covers_commit_ts_hi: Option<Ts> = None;

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
            if let Some((n, commit_ts)) = try_skip_chain_commit(&mut journal_file, salt1, salt2)? {
                // ChainCommit frame replay is a no-op for the page-replay
                // loop (it carries no single page_number). Version-chain
                // state is rebuilt on demand; the only recovery-critical
                // datum is `commit_ts`, which folds into `max_commit_ts`
                // so the HLC oracle lifts above every durable commit.
                write_cursor += n;
                max_commit_ts = Some(max_commit_ts.map_or(commit_ts, |prev| prev.max(commit_ts)));
                // §3.10 / §3.8(b) — record the commit_ts for the
                // orphan-logical sweep run after the main scan.
                seen_chain_commit_ts.insert(commit_ts);
                crate::mvcc::metrics::record_recovery_chain_commit_frame();
                continue;
            }

            // Phase 2 §3.11: dispatch on `BoundaryScan` tri-state.
            // - `Valid` → fold `covers_commit_ts_hi` into the running max
            //   and assert monotonicity (release-active hard error on
            //   regression — Phase 2 cannot tolerate a backward boundary
            //   because it would silently un-cull frames that a prior
            //   checkpoint had already reconciled).
            // - `Torn` → §3.11 point 4: scan MUST halt at this offset.
            //   Boundary bytes overlap with legacy-frame bytes (kind
            //   0x04 maps to legacy `page_number=4`); falling through
            //   would risk a hard `CorruptDatabase` from the legacy
            //   `page_size` parse. `highest_covers_commit_ts_hi` keeps
            //   its prior value, so recovery resumes from the previous
            //   valid boundary's cutoff.
            // - `NotBoundary` → fall through to logical / legacy dispatch.
            match try_skip_checkpoint_commit_boundary(&mut journal_file, salt1, salt2)? {
                BoundaryScan::Valid(n, frame) => {
                    if let Some(prev) = highest_covers_commit_ts_hi {
                        if frame.covers_commit_ts_hi < prev {
                            return Err(Error::CorruptDatabase {
                                path: journal_path.to_path_buf(),
                                detail: format!(
                                    "CheckpointCommitBoundaryFrame covers_commit_ts_hi \
                                     monotonicity violated (§3.11): prev={prev:?}, \
                                     new={:?}",
                                    frame.covers_commit_ts_hi
                                ),
                                recoverable: false,
                            });
                        }
                    }
                    highest_covers_commit_ts_hi = Some(
                        highest_covers_commit_ts_hi.map_or(frame.covers_commit_ts_hi, |prev| {
                            prev.max(frame.covers_commit_ts_hi)
                        }),
                    );
                    write_cursor += n;
                    crate::mvcc::metrics::record_recovery_checkpoint_boundary_frame();
                    continue;
                }
                BoundaryScan::Torn => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        target: "mqlite",
                        offset = frame_offset,
                        "torn CheckpointCommitBoundaryFrame — scan halts here, \
                         resuming from previous valid boundary (§3.11 point 4)"
                    );
                    crate::mvcc::metrics::record_recovery_torn_checkpoint_boundary();
                    break;
                }
                BoundaryScan::NotBoundary => {}
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

            // Bad checksum or EOF — stop here
            let Some(frame_hdr) = JournalFrameHeader::read(&mut journal_file, salt1, salt2)? else {
                break;
            };
            crate::mvcc::metrics::record_recovery_legacy_page_frame();

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

        // Phase 2 §3.11 — checkpoint-boundary cull. After the orphan sweep
        // (above) and the HLC-floor ChainCommit fold (below), any logical
        // frame whose `commit_ts <= highest_covers_commit_ts_hi` has already
        // been reconciled to the main file by a prior checkpoint and must
        // NOT be handed to Pass 2 for validation. Pass 2 is for
        // not-yet-checkpointed frames only (§3.11). Runs unconditionally —
        // if `highest_covers_commit_ts_hi` is `None` (no valid boundary
        // frame observed), the retain is a no-op. Also rebuilds the
        // `seen_commit_ts` dedup set so the two views stay consistent.
        if let Some(hi) = highest_covers_commit_ts_hi {
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
                    covers_commit_ts_hi = ?hi,
                    "pre-boundary logical frames culled by Pass 1 (§3.11)"
                );
                for _ in 0..boundary_dropped {
                    crate::mvcc::metrics::record_logical_txn_pass1_pre_boundary_dropped();
                    crate::mvcc::metrics::record_logical_txn_recovery_discarded_frame();
                }
            }
        }

        // Case (c) tolerance, §3.7 envelope violation: every ChainCommit
        // SHOULD have had a paired LogicalTxnFrame at the same commit_ts.
        // Phase 2 only logs + ticks a counter; Phase 4 §8.13.3 promotes
        // to a hard error (covered by `test_phase4_case_c_is_hard_error`).
        let unmatched_chain_commits: usize = seen_chain_commit_ts
            .iter()
            .filter(|ts| !logical_ts_pre_sweep.contains(ts))
            .count();
        for _ in 0..unmatched_chain_commits {
            crate::mvcc::metrics::record_logical_txn_pass1_unmatched_chain_commit();
        }
        #[cfg(feature = "tracing")]
        if unmatched_chain_commits > 0 {
            tracing::warn!(
                target: "mqlite",
                unmatched = unmatched_chain_commits,
                "ChainCommit frames without matching LogicalTxnFrame (§3.7 envelope violation; \
                 Phase 2 tolerance, Phase 4 §8.13.3 promotes to hard error)"
            );
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
            parsed_logical_frames: parsed_logical,
        }))
    }
}
