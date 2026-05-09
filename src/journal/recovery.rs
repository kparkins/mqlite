//! Journal recovery — replay an existing journal into the main file.
//!
//! This submodule owns the `recover_existing` entry point called from
//! [`JournalManager::open_or_create`].

use std::collections::{BTreeSet, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::storage::header::FileHeader;

use super::log_file::{
    read_chain_commit_at_cursor, CatalogCommitPayload, ChainCommitFrame, CheckpointBoundaryPayload,
    DecodeCtx, JournalHeader, LogRecord, LogRecordKind, LogRecordPayload, LogicalTxnFrame,
    JOURNAL_FORMAT_VERSION, JOURNAL_HEADER_SIZE, JOURNAL_MAGIC, LOG_RECORD_HEADER_LEN,
    LOG_RECORD_MAGIC, LOG_RECORD_TOTAL_LEN_OFFSET, MAX_LOG_RECORD_BYTES,
    RETIRED_PRE_RELEASE_JOURNAL_FORMAT_VERSIONS,
};
use super::shm::JournalIndex;
use super::write_page_to_main;
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

impl ParsedLogicalFrames {
    fn rebuild_seen_commit_ts(&mut self) {
        self.seen_commit_ts.clear();
        for (_, frame) in &self.frames {
            self.seen_commit_ts.insert(frame.commit_ts);
        }
    }
}

const _: () = {
    fn assert_send<T: Send>() {}
    let _ = assert_send::<ParsedLogicalFrames>;
};

#[derive(Debug)]
struct RecoveredLogRecord {
    record: LogRecord,
    logical_frame: Option<LogicalTxnFrame>,
}

#[derive(Debug, Default)]
struct LogRecordRecoveryScan {
    valid_end_lsn: u64,
    parsed_logical: ParsedLogicalFrames,
    recovered_max_commit_ts: Option<Ts>,
    recovered_max_publish_seq: Option<u64>,
    applied_catalog_commit: bool,
}

impl JournalManager {
    fn recover_log_record_journal(
        journal_path: &Path,
        mut journal_file: File,
        main_file: &mut File,
        main_header: &FileHeader,
        salt1: u32,
        salt2: u32,
        checkpoint_seq: u32,
    ) -> Result<Option<JournalManager>> {
        let scan = Self::scan_log_records(
            journal_path,
            &mut journal_file,
            main_file,
            main_header,
            salt1,
            salt2,
        )?;
        if scan.applied_catalog_commit {
            main_file.flush().map_err(Error::Io)?;
        }
        Self::truncate_tail_to_valid_end_lsn(&mut journal_file, scan.valid_end_lsn)?;
        let log_manager_file = journal_file.try_clone().map_err(Error::Io)?;

        Ok(Some(JournalManager {
            journal_path: journal_path.to_path_buf(),
            journal_file,
            index: JournalIndex::new(),
            salt1,
            salt2,
            checkpoint_seq,
            write_cursor: scan.valid_end_lsn,
            log_manager: Arc::new(super::LogManager::new(log_manager_file, scan.valid_end_lsn)),
            last_committed_db_page_count: None,
            recovered_max_commit_ts: scan.recovered_max_commit_ts,
            recovered_max_publish_seq: scan.recovered_max_publish_seq,
            parsed_logical_frames: scan.parsed_logical,
            legacy_pending_start_offset: None,
            last_legacy_commit_end_offset: scan.valid_end_lsn,
            checkpoint_batch_active: None,
            next_checkpoint_batch_id: 1,
            checkpoint_frame_tags: std::collections::BTreeMap::new(),
        }))
    }

    fn log_records_present(journal_file: &mut File, journal_len: u64) -> Result<bool> {
        let first_record_lsn = JOURNAL_HEADER_SIZE as u64;
        if journal_len <= first_record_lsn {
            return Ok(true);
        }

        if journal_len - first_record_lsn < 4 {
            return Ok(true);
        }

        let mut magic = [0u8; 4];
        journal_file
            .seek(SeekFrom::Start(first_record_lsn))
            .map_err(Error::Io)?;
        journal_file.read_exact(&mut magic).map_err(Error::Io)?;
        Ok(u32::from_le_bytes(magic) == LOG_RECORD_MAGIC)
    }

    fn scan_log_records(
        journal_path: &Path,
        journal_file: &mut File,
        main_file: &mut File,
        main_header: &FileHeader,
        salt1: u32,
        salt2: u32,
    ) -> Result<LogRecordRecoveryScan> {
        let journal_len = journal_file.metadata().map_err(Error::Io)?.len();
        let mut cursor = JOURNAL_HEADER_SIZE as u64;
        let mut accepted = Vec::new();

        while cursor < journal_len {
            let Some(record) =
                Self::read_log_record_at(journal_file, cursor, journal_len, salt1, salt2)?
            else {
                break;
            };
            cursor = record.record.end_lsn;
            accepted.push(record);
        }

        let checkpoint_applied_lsn = main_header.checkpoint_applied_lsn;

        let mut apply_set: Vec<_> = accepted
            .iter()
            .filter(|record| record.record.end_lsn > checkpoint_applied_lsn)
            .filter(|record| record.record.kind != LogRecordKind::CheckpointBoundary)
            .collect();
        apply_set.sort_by_key(|record| record.record.publish_seq);

        let mut seen_publish_seq = BTreeSet::new();
        let mut parsed_logical = ParsedLogicalFrames::default();
        let mut recovered_max_commit_ts = (main_header.last_checkpoint_ts != Ts::default())
            .then_some(main_header.last_checkpoint_ts);
        let mut recovered_max_publish_seq = None;
        let mut applied_catalog_commit = false;
        let mut recovered_header = main_header.clone();

        for record in apply_set {
            if !seen_publish_seq.insert(record.record.publish_seq) {
                return Err(Self::log_record_recovery_corruption(
                    journal_path,
                    format!(
                        "duplicate Phase 8 LogRecord publish_seq {}",
                        record.record.publish_seq
                    ),
                ));
            }

            recovered_max_commit_ts = Some(
                recovered_max_commit_ts.map_or(record.record.commit_ts, |prev: Ts| {
                    prev.max(record.record.commit_ts)
                }),
            );
            recovered_max_publish_seq = Some(
                recovered_max_publish_seq.map_or(record.record.publish_seq, |prev: u64| {
                    prev.max(record.record.publish_seq)
                }),
            );

            match &record.record.payload {
                LogRecordPayload::CrudCommit { .. } => {
                    if let Some(frame) = &record.logical_frame {
                        if parsed_logical.seen_commit_ts.insert(frame.commit_ts) {
                            parsed_logical
                                .frames
                                .push((record.record.start_lsn, frame.clone()));
                        }
                    }
                }
                LogRecordPayload::CatalogCommit(payload) => {
                    let catalog = CatalogCommitPayload::decode(payload).map_err(|error| {
                        Self::log_record_recovery_corruption(
                            journal_path,
                            format!("invalid Phase 8 CatalogCommit payload: {error}"),
                        )
                    })?;
                    for page in &catalog.pages {
                        write_page_to_main(
                            main_file,
                            page.page_number,
                            page.page_size.bytes(),
                            &page.data,
                        )?;
                    }
                    let mut header = catalog.header.clone();
                    header.last_checkpoint_ts = header
                        .last_checkpoint_ts
                        .max(recovered_header.last_checkpoint_ts);
                    header.checkpoint_applied_lsn = header
                        .checkpoint_applied_lsn
                        .max(recovered_header.checkpoint_applied_lsn);
                    let header = header.to_bytes();
                    write_page_to_main(main_file, 0, header.len(), &header)?;
                    recovered_header = FileHeader::from_bytes(&header)?;
                    applied_catalog_commit = true;
                }
                LogRecordPayload::CheckpointBoundary(_) => {}
            }
        }

        Ok(LogRecordRecoveryScan {
            valid_end_lsn: cursor,
            parsed_logical,
            recovered_max_commit_ts,
            recovered_max_publish_seq,
            applied_catalog_commit,
        })
    }

    fn read_log_record_at(
        journal_file: &mut File,
        cursor: u64,
        journal_len: u64,
        salt1: u32,
        salt2: u32,
    ) -> Result<Option<RecoveredLogRecord>> {
        if journal_len.saturating_sub(cursor) < LOG_RECORD_HEADER_LEN as u64 {
            return Ok(None);
        }

        journal_file
            .seek(SeekFrom::Start(cursor))
            .map_err(Error::Io)?;
        let mut header = [0u8; LOG_RECORD_HEADER_LEN];
        journal_file.read_exact(&mut header).map_err(Error::Io)?;

        let total_len = u32::from_le_bytes(
            header[LOG_RECORD_TOTAL_LEN_OFFSET..LOG_RECORD_TOTAL_LEN_OFFSET + 4]
                .try_into()
                .expect("4 bytes"),
        ) as usize;
        if !(LOG_RECORD_HEADER_LEN..=MAX_LOG_RECORD_BYTES).contains(&total_len) {
            return Ok(None);
        }

        let Some(record_end_lsn) = cursor.checked_add(total_len as u64) else {
            return Ok(None);
        };
        if record_end_lsn > journal_len {
            return Ok(None);
        }

        let mut bytes = vec![0u8; total_len];
        bytes[..LOG_RECORD_HEADER_LEN].copy_from_slice(&header);
        journal_file
            .read_exact(&mut bytes[LOG_RECORD_HEADER_LEN..])
            .map_err(Error::Io)?;

        let Ok(record) = LogRecord::decode(&bytes) else {
            return Ok(None);
        };
        if record.start_lsn != cursor {
            return Ok(None);
        }

        Ok(Self::decode_log_record_recovery_payload(
            record, salt1, salt2,
        ))
    }

    fn decode_log_record_recovery_payload(
        record: LogRecord,
        salt1: u32,
        salt2: u32,
    ) -> Option<RecoveredLogRecord> {
        match &record.payload {
            LogRecordPayload::CrudCommit {
                logical_payload,
                chain_payload,
            } => {
                let logical_frame = LogicalTxnFrame::decode(
                    logical_payload,
                    salt1,
                    salt2,
                    DecodeCtx::MidStream { follower: true },
                )
                .ok()
                .flatten()?;
                let chain_frame = ChainCommitFrame::decode(chain_payload, salt1, salt2)
                    .ok()
                    .flatten()?;
                if logical_frame.commit_ts != record.commit_ts
                    || chain_frame.commit_ts != record.commit_ts
                {
                    return None;
                }
                Some(RecoveredLogRecord {
                    record,
                    logical_frame: Some(logical_frame),
                })
            }
            LogRecordPayload::CatalogCommit(_) => Some(RecoveredLogRecord {
                record,
                logical_frame: None,
            }),
            LogRecordPayload::CheckpointBoundary(payload) => {
                let checkpoint = CheckpointBoundaryPayload::decode(payload).ok()?;
                let checkpoint_applied_lsn = checkpoint.checkpoint_applied_lsn;
                if checkpoint_applied_lsn < JOURNAL_HEADER_SIZE as u64
                    || checkpoint_applied_lsn > record.end_lsn
                {
                    return None;
                }
                Some(RecoveredLogRecord {
                    record,
                    logical_frame: None,
                })
            }
        }
    }

    fn truncate_tail_to_valid_end_lsn(journal_file: &mut File, valid_end_lsn: u64) -> Result<()> {
        let journal_len = journal_file.metadata().map_err(Error::Io)?.len();
        if valid_end_lsn < journal_len {
            journal_file.set_len(valid_end_lsn).map_err(Error::Io)?;
            journal_file.sync_data().map_err(Error::Io)?;
        }
        journal_file
            .seek(SeekFrom::Start(valid_end_lsn))
            .map_err(Error::Io)?;
        Ok(())
    }

    fn log_record_recovery_corruption(path: &Path, detail: impl Into<String>) -> Error {
        Error::CorruptDatabase {
            path: path.to_path_buf(),
            detail: detail.into(),
            recoverable: false,
        }
    }

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

        let journal_len = journal_file.metadata().map_err(Error::Io)?.len();
        if Self::log_records_present(&mut journal_file, journal_len)? {
            return Self::recover_log_record_journal(
                journal_path,
                journal_file,
                main_file,
                main_header,
                salt1,
                salt2,
                checkpoint_seq,
            );
        }

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
            parsed_logical.rebuild_seen_commit_ts();
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
                parsed_logical.rebuild_seen_commit_ts();
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
        let log_manager_file = journal_file.try_clone().map_err(Error::Io)?;

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
            log_manager: Arc::new(super::LogManager::new(log_manager_file, write_cursor)),
            last_committed_db_page_count,
            recovered_max_commit_ts: max_commit_ts,
            recovered_max_publish_seq: None,
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

#[cfg(test)]
#[path = "tests/recovery.rs"]
mod recovery_tests;
