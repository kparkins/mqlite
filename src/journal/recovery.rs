//! Journal recovery — replay an existing journal into the main file.
//!
//! This submodule owns the `recover_existing` entry point called from
//! [`JournalManager::open_or_create`].

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::storage::header::FileHeader;

use super::wire::{
    CatalogCommitPayload, ChainCommitFrame, CheckpointBoundaryPayload, CheckpointPageFramePayload,
    DecodeCtx, JournalHeader, LogRecord, LogRecordKind, LogRecordPayload, LogicalTxnFrame,
    JOURNAL_FORMAT_VERSION, JOURNAL_HEADER_SIZE, JOURNAL_MAGIC, LOG_RECORD_HEADER_LEN,
    LOG_RECORD_MAGIC, LOG_RECORD_TOTAL_LEN_OFFSET, MAX_LOG_RECORD_BYTES,
    RETIRED_PRE_RELEASE_JOURNAL_FORMAT_VERSIONS,
};
use super::wire::record::LogRecordDecodeError;
use super::write_page_to_main;
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
/// A later pass post-processes this further (ChainCommit-only HLC floor,
/// orphan-logical sweep).
///
/// Recovery "case C" (a durable `ChainCommit` with no matching
/// `LogicalTxnFrame`) is **structurally impossible** in the Phase 8 wire
/// format and therefore not represented here: there is no standalone
/// `ChainCommit` log record. A CRUD commit is a single `CrudCommit`
/// `LogRecord` whose payload carries the logical frame and the chain frame
/// together (see `decode_log_record_recovery_payload` and
/// `LogRecordPayload::CrudCommit { logical_payload, chain_payload }`), so a
/// chain commit can never be recovered without its paired logical frame.
/// The former unpaired-chain-commit rejection was provably inert and has
/// been removed.
#[derive(Debug, Default)]
pub(crate) struct ParsedLogicalFrames {
    /// Parsed frames with their byte offset in the journal, in scan order.
    pub(crate) frames: Vec<(u64, LogicalTxnFrame)>,
    /// Commit-ts values already seen — first-wins dedup set.
    pub(crate) seen_commit_ts: HashSet<Ts>,
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

/// `true` when `payload` carries the inner-frame salt words and they differ
/// from this database lifetime's salts.
///
/// Both inner CrudCommit frame layouts — `LogicalTxnFrame` (§4.1) and
/// `ChainCommitFrame` — store `salt1`/`salt2` at bytes 8..16, which is what
/// lets recovery classify the §4.6 salt-mismatch row at this layer without
/// changing the decoder return types: the decoders flatten that row into the
/// same `Ok(None)` as their content-error rows. A payload too short to carry
/// the salt words cannot be a stale frame and falls through to the
/// outer-valid ⇒ corrupt path.
fn inner_frame_salts_differ(payload: &[u8], expected_salt1: u32, expected_salt2: u32) -> bool {
    if payload.len() < 16 {
        return false;
    }
    let salt1 = u32::from_le_bytes(payload[8..12].try_into().expect("4 bytes"));
    let salt2 = u32::from_le_bytes(payload[12..16].try_into().expect("4 bytes"));
    salt1 != expected_salt1 || salt2 != expected_salt2
}

/// Attach the real journal path to an inner-decode `CorruptDatabase` error
/// (the §4.6 dispose helper fills in `PathBuf::new()`) and normalize its
/// `recoverable` disposition to match
/// [`JournalManager::log_record_recovery_corruption`] (`false`).
///
/// The §4.6 per-row `recoverable` flags carried by the wire decoders (most
/// content rows `true`, unknown `format_version` `false`,
/// `invalid_log_record` rows `true`) are deliberately discarded here: every
/// error funneled through this helper means a dual-CRC-valid record's
/// content failed validation mid-stream, so the journal cannot be safely
/// replayed past it and no later record can be trusted — the same
/// non-recoverable disposition every other recovery-layer corruption error
/// carries. Pinned by
/// `bug4_inner_decode_corruption_error_carries_journal_path_and_normalized_disposition`.
fn with_journal_recovery_context(journal_path: &Path, error: Error) -> Error {
    match error {
        Error::CorruptDatabase { detail, .. } => {
            JournalManager::log_record_recovery_corruption(journal_path, detail)
        }
        other => other,
    }
}

#[derive(Debug, Default)]
struct LogRecordRecoveryScan {
    valid_end_lsn: u64,
    parsed_logical: ParsedLogicalFrames,
    recovered_max_commit_ts: Option<Ts>,
    recovered_max_publish_seq: Option<u64>,
    applied_catalog_commit: bool,
    /// Highest `batch_id` observed across replayed `CheckpointBoundary`
    /// records, used to seed `next_checkpoint_batch_id` so post-restart
    /// batches do not collide with persisted ids.
    max_boundary_batch_id: Option<u64>,
    /// `total_page_count` carried by the latest replayed boundary header.
    /// Surfaces through [`JournalManager::did_recover_pages`] so the engine
    /// knows recovery materialised checkpoint pages into the main file.
    recovered_db_page_count: Option<u32>,
    /// `true` when every record encountered by the scan was a
    /// checkpoint-related kind (boundary, per-page, catalog) and at least one
    /// boundary was applied. The journal can be truncated to its header
    /// because no CRUD record remains pending in the journal.
    journal_truncatable: bool,
}

/// Output of [`JournalManager::collect_checkpoint_boundaries`] — the
/// `CheckpointBoundary` records decoded up front so later passes can drain
/// page frames, seed batch ids, and refresh the main-file header.
///
/// WiredTiger semantics: a `CheckpointBoundary` record is the *checkpoint
/// completion fence*. A batch's per-page frames only count as durably
/// checkpointed once their boundary is present, so the set of boundary
/// `batch_id`s is exactly the set of batches whose pages may be replayed.
#[derive(Debug, Default)]
struct CheckpointBoundaryIndex {
    /// Highest `batch_id` across all boundaries — seeds
    /// `next_checkpoint_batch_id` past every persisted id.
    max_boundary_batch_id: Option<u64>,
    /// `start_lsn` → `batch_id` for every boundary record, used by the drain
    /// pass to match a boundary's accumulated page frames.
    boundary_batch_ids: BTreeMap<u64, u64>,
    /// Highest `last_checkpoint_ts` observed across boundaries — folded into
    /// the recovered HLC floor.
    highest_boundary_ts: Option<Ts>,
    /// Header image of the latest boundary that lands above the main-file
    /// checkpoint frontier, persisted by the finalize pass.
    latest_boundary_header: Option<FileHeader>,
}

/// Output of [`JournalManager::partition_checkpoint_page_frames`] — page
/// frames split into batches that reached their boundary (replayable) and the
/// torn-checkpoint disposition for batches that did not.
///
/// WiredTiger semantics: a per-page frame whose batch never reached its commit
/// boundary is an *orphan page frame* — the visible remnant of a torn
/// checkpoint. It is excluded from replay (only batches with a boundary drain
/// into the main file). It is NOT, however, a torn tail: it is a fully-written
/// record, and checkpoint frames share one LSN-ordered append stream with CRUD
/// commits (`reserve_log_record_on`, `&self`), so a transaction committed
/// concurrently with an in-progress checkpoint can land at a HIGHER LSN than
/// the orphan. The orphan must therefore not pull the truncation floor below a
/// later committed record that recovery replays.
#[derive(Debug, Default)]
struct CheckpointPageFramePartition {
    /// Page frames whose batch reached its boundary, grouped by `batch_id` in
    /// LSN order, ready for the drain pass to flush into the main file.
    pending_pages_by_batch: BTreeMap<u64, Vec<CheckpointPageFramePayload>>,
    /// Lowest `start_lsn` of an orphan page frame (torn checkpoint), or `None`
    /// when every page frame above the frontier belongs to a known batch.
    orphan_truncate_lsn: Option<u64>,
}

/// Mutable accumulator threaded through the apply / drain / finalize passes of
/// [`JournalManager::scan_log_records`]. Each pass advances exactly the fields
/// it owns; the orchestrator reads the finished state into a
/// [`LogRecordRecoveryScan`].
#[derive(Debug)]
struct ScanState {
    /// Parsed logical frames handed off to Pass 2 post-open validation.
    parsed_logical: ParsedLogicalFrames,
    /// Highest commit timestamp seen across replayed records and boundaries.
    recovered_max_commit_ts: Option<Ts>,
    /// Highest `publish_seq` seen across replayed CRUD/Catalog records.
    recovered_max_publish_seq: Option<u64>,
    /// `true` once any record materialised bytes into the main file.
    applied_catalog_commit: bool,
    /// Working copy of the main-file header refreshed as records replay.
    recovered_header: FileHeader,
    /// Highest `end_lsn` of any record recovery keeps (replays or drains).
    /// Pins the truncation floor so an orphan page frame can never truncate a
    /// later committed record off the journal (orphan-truncate vs replay
    /// divergence).
    max_kept_record_end_lsn: u64,
}

impl ScanState {
    fn new(main_header: &FileHeader, highest_boundary_ts: Option<Ts>) -> Self {
        let mut recovered_max_commit_ts = (main_header.last_checkpoint_ts != Ts::default())
            .then_some(main_header.last_checkpoint_ts);
        if let Some(boundary_ts) = highest_boundary_ts {
            recovered_max_commit_ts =
                Some(recovered_max_commit_ts.map_or(boundary_ts, |prev| prev.max(boundary_ts)));
        }
        Self {
            parsed_logical: ParsedLogicalFrames::default(),
            recovered_max_commit_ts,
            recovered_max_publish_seq: None,
            applied_catalog_commit: false,
            recovered_header: main_header.clone(),
            max_kept_record_end_lsn: JOURNAL_HEADER_SIZE as u64,
        }
    }

    /// Record that `end_lsn` belongs to a kept (replayed or drained) record so
    /// the orphan-frame truncation floor never cuts below it.
    fn note_kept_record_end(&mut self, end_lsn: u64) {
        self.max_kept_record_end_lsn = self.max_kept_record_end_lsn.max(end_lsn);
    }
}

impl JournalManager {
    fn recover_log_record_journal(
        journal_path: &Path,
        mut journal_file: File,
        main_file: &mut File,
        main_header: &FileHeader,
        salt1: u32,
        salt2: u32,
        mut checkpoint_seq: u32,
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

        // Reset the journal to its bare header when every replayed record was
        // already materialised into the main file. Performs inline the same
        // header-rewrite + set_len that the now-removed legacy
        // `truncate_journal()` helper did once it had finished copying the
        // boundary's pages.
        let resume_lsn = if scan.journal_truncatable {
            // The header rewrite + set_len below durably destroys the
            // journal's only copy of the replayed pages, so the main file
            // must be durably synced FIRST (`flush()` above is not an fsync).
            // Otherwise a power cut after open() returns loses pages that
            // were durable before recovery ran. This is a sync-ownership
            // boundary; recorded like BufferPoolHandle::sync_main_file.
            main_file.sync_data().map_err(Error::Io)?;
            #[cfg(any(test, feature = "test-hooks"))]
            crate::journal::append_sync_observations::record_main_file_sync();
            checkpoint_seq = checkpoint_seq.wrapping_add(1);
            let mut header = super::wire::JournalHeader::new(salt1, salt2);
            header.checkpoint_seq = checkpoint_seq;
            // The journal-header rewrite below is the FIRST destructive
            // write of this branch (it bumps checkpoint_seq, retiring every
            // record that follows), so the truncate observation is recorded
            // here — before any byte of the journal's only copy of the
            // replayed pages is destroyed — and ordered against
            // record_main_file_sync above so tests can pin
            // sync-BEFORE-first-destructive-write, not just sync presence
            // (R-bug8-order).
            #[cfg(any(test, feature = "test-hooks"))]
            crate::journal::append_sync_observations::record_journal_truncate();
            journal_file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
            journal_file
                .write_all(&header.to_bytes())
                .map_err(Error::Io)?;
            journal_file
                .set_len(JOURNAL_HEADER_SIZE as u64)
                .map_err(Error::Io)?;
            journal_file.sync_data().map_err(Error::Io)?;
            journal_file
                .seek(SeekFrom::Start(JOURNAL_HEADER_SIZE as u64))
                .map_err(Error::Io)?;
            JOURNAL_HEADER_SIZE as u64
        } else {
            Self::truncate_tail_to_valid_end_lsn(&mut journal_file, scan.valid_end_lsn)?;
            scan.valid_end_lsn
        };
        let log_manager_file = journal_file.try_clone().map_err(Error::Io)?;

        let next_checkpoint_batch_id = scan
            .max_boundary_batch_id
            .map_or(1, |max| max.saturating_add(1));
        Ok(Some(JournalManager {
            journal_path: journal_path.to_path_buf(),
            journal_file,
            salt1,
            salt2,
            checkpoint_seq,
            log_manager: Arc::new(super::LogManager::new(log_manager_file, resume_lsn)),
            last_committed_db_page_count: scan.recovered_db_page_count,
            recovered_max_commit_ts: scan.recovered_max_commit_ts,
            recovered_max_publish_seq: scan.recovered_max_publish_seq,
            parsed_logical_frames: scan.parsed_logical,
            checkpoint_batch_active: None,
            next_checkpoint_batch_id,
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
        let (accepted, cursor) =
            Self::read_accepted_records(journal_path, journal_file, journal_len, salt1, salt2)?;

        let checkpoint_applied_lsn = main_header.checkpoint_applied_lsn;
        let boundaries =
            Self::collect_checkpoint_boundaries(journal_path, &accepted, checkpoint_applied_lsn)?;
        let mut partition = Self::partition_checkpoint_page_frames(
            journal_path,
            &accepted,
            &boundaries,
            checkpoint_applied_lsn,
        )?;

        let mut state = ScanState::new(main_header, boundaries.highest_boundary_ts);
        Self::apply_commit_records(
            journal_path,
            main_file,
            &accepted,
            checkpoint_applied_lsn,
            &mut state,
        )?;
        Self::drain_checkpoint_batches(
            main_file,
            &accepted,
            &boundaries,
            checkpoint_applied_lsn,
            &mut partition,
            &mut state,
        )?;

        let journal_truncatable = Self::finalize_recovered_header(
            main_file,
            &accepted,
            &boundaries,
            &mut state,
        )?;
        let recovered_db_page_count = boundaries
            .latest_boundary_header
            .as_ref()
            .map(|header| header.total_page_count);

        // An orphan page frame (torn checkpoint) discards its own tail, but it
        // must never truncate below a committed record recovery replayed —
        // checkpoint frames and CRUD share one LSN stream, so a concurrently
        // committed transaction can sit ABOVE the orphan
        // (orphan-truncate vs replay divergence).
        let valid_end_lsn = partition.orphan_truncate_lsn.map_or(cursor, |orphan| {
            orphan.min(cursor).max(state.max_kept_record_end_lsn)
        });
        Ok(LogRecordRecoveryScan {
            valid_end_lsn,
            parsed_logical: state.parsed_logical,
            recovered_max_commit_ts: state.recovered_max_commit_ts,
            recovered_max_publish_seq: state.recovered_max_publish_seq,
            applied_catalog_commit: state.applied_catalog_commit,
            max_boundary_batch_id: boundaries.max_boundary_batch_id,
            recovered_db_page_count,
            journal_truncatable,
        })
    }

    /// Pass 1 — read the contiguous prefix of dual-CRC-valid log records.
    ///
    /// Stops at the first torn-tail row ([`Self::read_log_record_at`] returns
    /// `Ok(None)`); a post-CRC semantic corruption row instead propagates an
    /// error. Returns the accepted records and the byte cursor just past the
    /// last accepted record — the physical end of the trustworthy prefix.
    fn read_accepted_records(
        journal_path: &Path,
        journal_file: &mut File,
        journal_len: u64,
        salt1: u32,
        salt2: u32,
    ) -> Result<(Vec<RecoveredLogRecord>, u64)> {
        let mut cursor = JOURNAL_HEADER_SIZE as u64;
        let mut accepted = Vec::new();

        while cursor < journal_len {
            let Some(record) = Self::read_log_record_at(
                journal_path,
                journal_file,
                cursor,
                journal_len,
                salt1,
                salt2,
            )?
            else {
                break;
            };
            cursor = record.record.end_lsn;
            accepted.push(record);
        }

        Ok((accepted, cursor))
    }

    /// Pass 2 — decode every `CheckpointBoundary` record up front.
    ///
    /// A boundary record is the WiredTiger checkpoint completion fence: it
    /// seeds `next_checkpoint_batch_id` past every persisted id, identifies
    /// the batches whose page frames may drain, enforces `last_checkpoint_ts`
    /// monotonicity across boundaries, and tracks the latest boundary header
    /// (above the main-file frontier) to refresh the recovered header.
    fn collect_checkpoint_boundaries(
        journal_path: &Path,
        accepted: &[RecoveredLogRecord],
        checkpoint_applied_lsn: u64,
    ) -> Result<CheckpointBoundaryIndex> {
        let mut index = CheckpointBoundaryIndex::default();
        for record in accepted {
            if let LogRecordPayload::CheckpointBoundary(payload) = &record.record.payload {
                let boundary = CheckpointBoundaryPayload::decode(payload).map_err(|error| {
                    Self::log_record_recovery_corruption(
                        journal_path,
                        format!("invalid CheckpointBoundary payload: {error}"),
                    )
                })?;
                index.max_boundary_batch_id = Some(
                    index
                        .max_boundary_batch_id
                        .map_or(boundary.batch_id, |prev| prev.max(boundary.batch_id)),
                );
                index
                    .boundary_batch_ids
                    .insert(record.record.start_lsn, boundary.batch_id);
                let ts = boundary.header.last_checkpoint_ts;
                if let Some(prev) = index.highest_boundary_ts {
                    if ts < prev {
                        return Err(Self::log_record_recovery_corruption(
                            journal_path,
                            format!(
                                "page-0 checkpoint boundary last_checkpoint_ts regressed: \
                                 prev={prev:?}, new={ts:?}"
                            ),
                        ));
                    }
                }
                index.highest_boundary_ts =
                    Some(index.highest_boundary_ts.map_or(ts, |prev| prev.max(ts)));
                // The latest boundary above the main-file frontier carries the
                // header image the finalize pass persists; later boundaries in
                // scan order overwrite earlier ones.
                if record.record.end_lsn > checkpoint_applied_lsn {
                    index.latest_boundary_header = Some(boundary.header);
                }
            }
        }
        Ok(index)
    }

    /// Pass 3 — partition `CheckpointPageFrame` records by batch.
    ///
    /// Frames above the main-file frontier whose `batch_id` has a boundary
    /// (`boundary_batch_ids`) accumulate per batch, in LSN order, for the drain
    /// pass. Frames whose batch never reached its boundary are orphan page
    /// frames — the remnant of a torn checkpoint — excluded from replay and
    /// tracked by their lowest `start_lsn` for the truncation decision.
    fn partition_checkpoint_page_frames(
        journal_path: &Path,
        accepted: &[RecoveredLogRecord],
        boundaries: &CheckpointBoundaryIndex,
        checkpoint_applied_lsn: u64,
    ) -> Result<CheckpointPageFramePartition> {
        let mut partition = CheckpointPageFramePartition::default();
        let known_batch_ids: BTreeSet<u64> =
            boundaries.boundary_batch_ids.values().copied().collect();
        for record in accepted {
            if record.record.end_lsn <= checkpoint_applied_lsn {
                continue;
            }
            if let LogRecordPayload::CheckpointPageFrame(payload) = &record.record.payload {
                let frame = CheckpointPageFramePayload::decode(payload).map_err(|error| {
                    Self::log_record_recovery_corruption(
                        journal_path,
                        format!("invalid CheckpointPageFrame payload: {error}"),
                    )
                })?;
                if !known_batch_ids.contains(&frame.batch_id) {
                    partition.orphan_truncate_lsn = Some(
                        partition
                            .orphan_truncate_lsn
                            .map_or(record.record.start_lsn, |prev| {
                                prev.min(record.record.start_lsn)
                            }),
                    );
                    continue;
                }
                partition
                    .pending_pages_by_batch
                    .entry(frame.batch_id)
                    .or_default()
                    .push(frame);
            }
        }
        Ok(partition)
    }

    /// Pass 4 — replay committed CRUD and Catalog records in `publish_seq`
    /// order.
    ///
    /// Only records above the main-file frontier that are neither boundaries
    /// nor page frames are applied. CRUD records contribute their logical frame
    /// to `parsed_logical` (first-wins on `commit_ts`); Catalog records write
    /// their pages + header image into the main file. A duplicate `publish_seq`
    /// is an integrity violation. Every applied record's `end_lsn` is recorded
    /// as a kept-record high-water mark so a later orphan-frame truncation
    /// cannot destroy it.
    fn apply_commit_records(
        journal_path: &Path,
        main_file: &mut File,
        accepted: &[RecoveredLogRecord],
        checkpoint_applied_lsn: u64,
        state: &mut ScanState,
    ) -> Result<()> {
        let mut apply_set: Vec<_> = accepted
            .iter()
            .filter(|record| record.record.end_lsn > checkpoint_applied_lsn)
            .filter(|record| {
                record.record.kind != LogRecordKind::CheckpointBoundary
                    && record.record.kind != LogRecordKind::CheckpointPageFrame
            })
            .collect();
        apply_set.sort_by_key(|record| record.record.publish_seq);

        let mut seen_publish_seq = BTreeSet::new();
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

            state.recovered_max_commit_ts = Some(
                state
                    .recovered_max_commit_ts
                    .map_or(record.record.commit_ts, |prev: Ts| {
                        prev.max(record.record.commit_ts)
                    }),
            );
            state.recovered_max_publish_seq = Some(
                state
                    .recovered_max_publish_seq
                    .map_or(record.record.publish_seq, |prev: u64| {
                        prev.max(record.record.publish_seq)
                    }),
            );
            state.note_kept_record_end(record.record.end_lsn);

            match &record.record.payload {
                LogRecordPayload::CrudCommit { .. } => {
                    if let Some(frame) = &record.logical_frame {
                        if state.parsed_logical.seen_commit_ts.insert(frame.commit_ts) {
                            state
                                .parsed_logical
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
                        .max(state.recovered_header.last_checkpoint_ts);
                    header.checkpoint_applied_lsn = header
                        .checkpoint_applied_lsn
                        .max(state.recovered_header.checkpoint_applied_lsn);
                    let header = header.to_bytes();
                    write_page_to_main(main_file, 0, header.len(), &header)?;
                    state.recovered_header = FileHeader::from_bytes(&header)?;
                    state.applied_catalog_commit = true;
                }
                LogRecordPayload::CheckpointBoundary(_)
                | LogRecordPayload::CheckpointPageFrame(_) => {
                    // Filtered out of apply_set above; never reached here.
                }
            }
        }
        Ok(())
    }

    /// Pass 5 — drain each batch's accumulated page frames into the main file
    /// when its `CheckpointBoundary` is replayed (above the frontier).
    ///
    /// The boundary is the fence that proves the whole batch is complete, so
    /// the pages flush only once it is observed, in strict LSN order. The
    /// boundary's `end_lsn` is recorded as a kept-record high-water mark so an
    /// orphan frame below it cannot truncate a drained batch off the journal.
    fn drain_checkpoint_batches(
        main_file: &mut File,
        accepted: &[RecoveredLogRecord],
        boundaries: &CheckpointBoundaryIndex,
        checkpoint_applied_lsn: u64,
        partition: &mut CheckpointPageFramePartition,
        state: &mut ScanState,
    ) -> Result<()> {
        for record in accepted
            .iter()
            .filter(|record| record.record.end_lsn > checkpoint_applied_lsn)
            .filter(|record| record.record.kind == LogRecordKind::CheckpointBoundary)
        {
            if let Some(batch_id) = boundaries.boundary_batch_ids.get(&record.record.start_lsn) {
                state.note_kept_record_end(record.record.end_lsn);
                if let Some(pages) = partition.pending_pages_by_batch.remove(batch_id) {
                    for page in pages {
                        write_page_to_main(
                            main_file,
                            page.page_number,
                            page.page_size.bytes(),
                            &page.data,
                        )?;
                        state.applied_catalog_commit = true;
                    }
                }
            }
        }
        Ok(())
    }

    /// Pass 6 — decide journal truncatability and persist the latest boundary
    /// header into the main file.
    ///
    /// The journal can be truncated to its bare header iff a boundary advanced
    /// the checkpoint frontier and EVERY accepted record was checkpoint-related
    /// (boundary / per-page / catalog): the presence of any `CrudCommit` means
    /// MVCC version-chain state is still pending in the journal, blocking
    /// truncation until the next checkpoint. The latest boundary header (above
    /// the frontier) is written to page 0 so the next open observes the
    /// advanced frontier; when the journal will be reset, the persisted
    /// `checkpoint_applied_lsn` filter is rewound to `JOURNAL_HEADER_SIZE` so
    /// post-open appends are not dropped by the next recovery scan. Returns
    /// whether the journal is truncatable.
    fn finalize_recovered_header(
        main_file: &mut File,
        accepted: &[RecoveredLogRecord],
        boundaries: &CheckpointBoundaryIndex,
        state: &mut ScanState,
    ) -> Result<bool> {
        // Select the latest boundary header that lands above the frontier — the
        // image the finalize step persists. `collect_checkpoint_boundaries`
        // already validated boundary monotonicity, so the last such record in
        // scan order is the most advanced.
        let latest_boundary_header = boundaries.latest_boundary_header.clone();

        let journal_truncatable = latest_boundary_header.is_some()
            && accepted.iter().all(|record| {
                matches!(
                    record.record.kind,
                    LogRecordKind::CheckpointBoundary
                        | LogRecordKind::CheckpointPageFrame
                        | LogRecordKind::CatalogCommit
                )
            });

        if let Some(header) = latest_boundary_header {
            let mut header = header;
            header.last_checkpoint_ts = header
                .last_checkpoint_ts
                .max(state.recovered_header.last_checkpoint_ts);
            if journal_truncatable {
                header.checkpoint_applied_lsn = JOURNAL_HEADER_SIZE as u64;
            } else {
                header.checkpoint_applied_lsn = header
                    .checkpoint_applied_lsn
                    .max(state.recovered_header.checkpoint_applied_lsn);
            }
            let bytes = header.to_bytes();
            write_page_to_main(main_file, 0, bytes.len(), &bytes)?;
            state.recovered_header = FileHeader::from_bytes(&bytes)?;
            state.applied_catalog_commit = true;
        }

        Ok(journal_truncatable)
    }

    fn read_log_record_at(
        journal_path: &Path,
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

        // F24: only rows at or before the dual-CRC32C gates may stop the
        // scan as a torn tail. Semantic rows that fire AFTER both CRCs
        // passed — publish_seq kind rules, the CrudCommit split-header
        // consistency check — are content corruption inside a fully-written
        // record (the outer-layer sibling of the §4.6 inner-frame split
        // below); flattening them into `Ok(None)` would physically truncate
        // every later committed record off the journal (the BUG-4 failure
        // class).
        let record = match LogRecord::decode_classified(&bytes) {
            Ok(record) => record,
            Err(LogRecordDecodeError::TornEligible(_)) => return Ok(None),
            Err(LogRecordDecodeError::PostCrcCorrupt(error)) => {
                return Err(with_journal_recovery_context(journal_path, error));
            }
        };
        if record.start_lsn != cursor {
            return Ok(None);
        }

        Self::decode_log_record_recovery_payload(journal_path, record, salt1, salt2)
    }

    fn decode_log_record_recovery_payload(
        journal_path: &Path,
        record: LogRecord,
        salt1: u32,
        salt2: u32,
    ) -> Result<Option<RecoveredLogRecord>> {
        match &record.payload {
            LogRecordPayload::CrudCommit {
                logical_payload,
                chain_payload,
            } => {
                // The outer LogRecord passed both CRC32C checks, so the inner
                // payload bytes are exactly what the writer committed. Per
                // §4.6 the ONLY inner failure row that can occur here without
                // implying corruption is a salt mismatch — a stale,
                // fully-written record from a previous database lifetime —
                // which stops the scan like a torn tail. EVERY other inner
                // failure (CRC mismatch, count/body inconsistency, invalid
                // page-size marker, trailing bytes, mid-payload truncation)
                // is impossible inside a length+dual-CRC-verified outer
                // record and must surface as Err(CorruptDatabase) —
                // flattening it into a torn-tail Ok(None) would physically
                // truncate every later committed record off the journal (the
                // original BUG-4 failure mode). `ChainCommitFrame::decode`
                // reports all of its failure rows as Ok(None), so the
                // salt-mismatch row is classified here, before decoding, from
                // the salt words both frame layouts carry at bytes 8..16.
                if inner_frame_salts_differ(logical_payload, salt1, salt2)
                    || inner_frame_salts_differ(chain_payload, salt1, salt2)
                {
                    return Ok(None);
                }
                let logical_frame = LogicalTxnFrame::decode(
                    logical_payload,
                    salt1,
                    salt2,
                    DecodeCtx::MidStream { follower: true },
                )
                .map_err(|error| with_journal_recovery_context(journal_path, error))?
                .ok_or_else(|| {
                    Self::log_record_recovery_corruption(
                        journal_path,
                        "Phase 8 CrudCommit logical frame failed to decode inside a \
                         dual-CRC-valid outer record",
                    )
                })?;
                let chain_frame = ChainCommitFrame::decode(chain_payload, salt1, salt2)
                    .map_err(|error| with_journal_recovery_context(journal_path, error))?
                    .ok_or_else(|| {
                        Self::log_record_recovery_corruption(
                            journal_path,
                            "Phase 8 CrudCommit chain frame failed to decode inside a \
                             dual-CRC-valid outer record",
                        )
                    })?;
                if logical_frame.commit_ts != record.commit_ts
                    || chain_frame.commit_ts != record.commit_ts
                {
                    // The outer record is CRC-valid, so an inner/outer
                    // commit_ts disagreement is an integrity violation, not a
                    // torn tail.
                    return Err(Self::log_record_recovery_corruption(
                        journal_path,
                        format!(
                            "Phase 8 CrudCommit inner-frame commit_ts mismatch: \
                             outer={:?}, logical={:?}, chain={:?}",
                            record.commit_ts, logical_frame.commit_ts, chain_frame.commit_ts
                        ),
                    ));
                }
                Ok(Some(RecoveredLogRecord {
                    record,
                    logical_frame: Some(logical_frame),
                }))
            }
            LogRecordPayload::CatalogCommit(_) => Ok(Some(RecoveredLogRecord {
                record,
                logical_frame: None,
            })),
            LogRecordPayload::CheckpointBoundary(payload) => {
                // Same outer-valid ⇒ corrupt reasoning as the CrudCommit
                // inner frames above: the boundary payload carries no
                // frame-level salts, so no stale-lifetime row exists here and
                // every decode failure is detected corruption, never a torn
                // tail.
                let checkpoint = CheckpointBoundaryPayload::decode(payload).map_err(|error| {
                    Self::log_record_recovery_corruption(
                        journal_path,
                        format!(
                            "invalid CheckpointBoundary payload inside a dual-CRC-valid \
                             record: {error}"
                        ),
                    )
                })?;
                if checkpoint.checkpoint_applied_lsn > record.end_lsn {
                    // The writer derives the boundary's frontier from records
                    // it already materialised, so a frontier beyond the
                    // boundary's own end is an integrity violation.
                    return Err(Self::log_record_recovery_corruption(
                        journal_path,
                        format!(
                            "CheckpointBoundary checkpoint_applied_lsn {} beyond its own \
                             record end_lsn {} inside a dual-CRC-valid record",
                            checkpoint.checkpoint_applied_lsn, record.end_lsn
                        ),
                    ));
                }
                Ok(Some(RecoveredLogRecord {
                    record,
                    logical_frame: None,
                }))
            }
            LogRecordPayload::CheckpointPageFrame(payload) => {
                CheckpointPageFramePayload::decode(payload).map_err(|error| {
                    Self::log_record_recovery_corruption(
                        journal_path,
                        format!(
                            "invalid CheckpointPageFrame payload inside a dual-CRC-valid \
                             record: {error}"
                        ),
                    )
                })?;
                Ok(Some(RecoveredLogRecord {
                    record,
                    logical_frame: None,
                }))
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

        // Pre-release format change: the legacy 24-byte page-frame walker
        // and intermixed checkpoint/logical scan have been removed. Any
        // journal whose first record byte is not `LOG_RECORD_MAGIC` is
        // unreadable; treat it as stale, delete, and let the caller create
        // a fresh journal.
        drop(journal_file);
        let _ = std::fs::remove_file(journal_path);
        Ok(None)
    }
}

#[cfg(test)]
#[path = "tests/recovery.rs"]
mod recovery_tests;
