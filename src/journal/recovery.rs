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

use super::log_file::{
    CatalogCommitPayload, ChainCommitFrame, CheckpointBoundaryPayload, CheckpointPageFramePayload,
    DecodeCtx, JournalHeader, LogRecord, LogRecordKind, LogRecordPayload, LogicalTxnFrame,
    JOURNAL_FORMAT_VERSION, JOURNAL_HEADER_SIZE, JOURNAL_MAGIC, LOG_RECORD_HEADER_LEN,
    LOG_RECORD_MAGIC, LOG_RECORD_TOTAL_LEN_OFFSET, MAX_LOG_RECORD_BYTES,
    RETIRED_PRE_RELEASE_JOURNAL_FORMAT_VERSIONS,
};
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
        // already materialised into the main file. Mirrors the legacy
        // `truncate_journal()` step that emergency_checkpoint_after_boundary
        // used to perform once it had finished copying the boundary's pages.
        let resume_lsn = if scan.journal_truncatable {
            checkpoint_seq = checkpoint_seq.wrapping_add(1);
            let mut header = super::log_file::JournalHeader::new(salt1, salt2);
            header.checkpoint_seq = checkpoint_seq;
            journal_file
                .seek(SeekFrom::Start(0))
                .map_err(Error::Io)?;
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
            write_cursor: resume_lsn,
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

        // Decode every boundary record up front so we can:
        //   1. Seed `next_checkpoint_batch_id` past every persisted batch id.
        //   2. Drain matching `CheckpointPageFrame` records by batch_id.
        //   3. Enforce `last_checkpoint_ts` monotonicity across replayed
        //      boundaries (mirrors the legacy regression check).
        //   4. Track the highest replayed boundary header so it can refresh
        //      the main-file header image and drive `recovered_max_commit_ts`.
        let mut max_boundary_batch_id: Option<u64> = None;
        let mut boundary_batch_ids: BTreeMap<u64, u64> = BTreeMap::new();
        let mut highest_boundary_ts: Option<Ts> = None;
        let mut latest_boundary_header: Option<FileHeader> = None;
        for record in &accepted {
            if let LogRecordPayload::CheckpointBoundary(payload) = &record.record.payload {
                let boundary = CheckpointBoundaryPayload::decode(payload).map_err(|error| {
                    Self::log_record_recovery_corruption(
                        journal_path,
                        format!("invalid CheckpointBoundary payload: {error}"),
                    )
                })?;
                max_boundary_batch_id = Some(
                    max_boundary_batch_id
                        .map_or(boundary.batch_id, |prev| prev.max(boundary.batch_id)),
                );
                boundary_batch_ids.insert(record.record.start_lsn, boundary.batch_id);
                let ts = boundary.header.last_checkpoint_ts;
                if let Some(prev) = highest_boundary_ts {
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
                highest_boundary_ts = Some(highest_boundary_ts.map_or(ts, |prev| prev.max(ts)));
                if record.record.end_lsn > checkpoint_applied_lsn {
                    latest_boundary_header = Some(boundary.header);
                }
            }
        }

        // Group CheckpointPageFrame records by batch_id, in LSN order. Frames
        // whose batch never reached its commit boundary are "orphans" — the
        // legacy recovery path discarded them by truncating past their first
        // byte; we mirror that by tracking the lowest orphan LSN and trimming
        // `valid_end_lsn` to it before returning the scan.
        let mut pending_pages_by_batch: BTreeMap<u64, Vec<CheckpointPageFramePayload>> =
            BTreeMap::new();
        let mut orphan_truncate_lsn: Option<u64> = None;
        let known_batch_ids: BTreeSet<u64> = boundary_batch_ids.values().copied().collect();
        for record in &accepted {
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
                    orphan_truncate_lsn = Some(
                        orphan_truncate_lsn
                            .map_or(record.record.start_lsn, |prev| prev.min(record.record.start_lsn)),
                    );
                    continue;
                }
                pending_pages_by_batch
                    .entry(frame.batch_id)
                    .or_default()
                    .push(frame);
            }
        }

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
        let mut parsed_logical = ParsedLogicalFrames::default();
        let mut recovered_max_commit_ts = (main_header.last_checkpoint_ts != Ts::default())
            .then_some(main_header.last_checkpoint_ts);
        if let Some(boundary_ts) = highest_boundary_ts {
            recovered_max_commit_ts = Some(
                recovered_max_commit_ts.map_or(boundary_ts, |prev| prev.max(boundary_ts)),
            );
        }
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
                LogRecordPayload::CheckpointBoundary(_)
                | LogRecordPayload::CheckpointPageFrame(_) => {
                    // Filtered out of apply_set above; never reached here.
                }
            }
        }

        // Drain accumulated CheckpointPageFrame records when their matching
        // CheckpointBoundary is replayed (boundary records replay above
        // checkpoint_applied_lsn). Pages flush in strict LSN order.
        for record in accepted
            .iter()
            .filter(|record| record.record.end_lsn > checkpoint_applied_lsn)
            .filter(|record| record.record.kind == LogRecordKind::CheckpointBoundary)
        {
            if let Some(batch_id) = boundary_batch_ids.get(&record.record.start_lsn) {
                if let Some(pages) = pending_pages_by_batch.remove(batch_id) {
                    for page in pages {
                        write_page_to_main(
                            main_file,
                            page.page_number,
                            page.page_size.bytes(),
                            &page.data,
                        )?;
                        applied_catalog_commit = true;
                    }
                }
            }
        }

        // The journal can be truncated to its header iff every record was
        // already materialised into the main file: boundaries + per-page
        // records replay above and CatalogCommit records replay through the
        // apply_set loop. The presence of a CrudCommit means there is MVCC
        // version-chain state in the journal that has not yet been
        // checkpointed, so truncation must wait for the next normal
        // checkpoint pass.
        let recovered_db_page_count = latest_boundary_header
            .as_ref()
            .map(|header| header.total_page_count);
        let journal_truncatable = recovered_db_page_count.is_some()
            && accepted.iter().all(|record| {
                matches!(
                    record.record.kind,
                    LogRecordKind::CheckpointBoundary
                        | LogRecordKind::CheckpointPageFrame
                        | LogRecordKind::CatalogCommit
                )
            });

        // Persist the latest replayed boundary header so the next open observes
        // the advanced checkpoint frontier in the main-file header (mirrors the
        // page-0 image the legacy boundary record used to write directly).
        // When the journal will be truncated to its header, reset the
        // `checkpoint_applied_lsn` filter to `JOURNAL_HEADER_SIZE` so records
        // appended after this open are not silently dropped on the next
        // recovery scan by the `end_lsn > checkpoint_applied_lsn` filter.
        if let Some(header) = latest_boundary_header {
            let mut header = header;
            header.last_checkpoint_ts = header
                .last_checkpoint_ts
                .max(recovered_header.last_checkpoint_ts);
            if journal_truncatable {
                header.checkpoint_applied_lsn = JOURNAL_HEADER_SIZE as u64;
            } else {
                header.checkpoint_applied_lsn = header
                    .checkpoint_applied_lsn
                    .max(recovered_header.checkpoint_applied_lsn);
            }
            let bytes = header.to_bytes();
            write_page_to_main(main_file, 0, bytes.len(), &bytes)?;
            recovered_header = FileHeader::from_bytes(&bytes)?;
            applied_catalog_commit = true;
        }
        let _ = recovered_header;

        let valid_end_lsn = orphan_truncate_lsn.map_or(cursor, |orphan| orphan.min(cursor));
        Ok(LogRecordRecoveryScan {
            valid_end_lsn,
            parsed_logical,
            recovered_max_commit_ts,
            recovered_max_publish_seq,
            applied_catalog_commit,
            max_boundary_batch_id,
            recovered_db_page_count,
            journal_truncatable,
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
                if checkpoint.checkpoint_applied_lsn > record.end_lsn {
                    return None;
                }
                Some(RecoveredLogRecord {
                    record,
                    logical_frame: None,
                })
            }
            LogRecordPayload::CheckpointPageFrame(payload) => {
                CheckpointPageFramePayload::decode(payload).ok()?;
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
