//! F24 regression tests (outer-layer sibling of
//! `bug_recovery_chain_payload_corruption.rs`): `read_log_record_at`
//! (`src/journal/recovery.rs`) used to flatten EVERY `LogRecord::decode`
//! failure into a torn-tail `Ok(None)`, and
//! `truncate_tail_to_valid_end_lsn` then physically `set_len()`ed the journal
//! at the corrupt record — durably destroying every LATER
//! committed-and-fsynced record.
//!
//! `LogRecord::decode` (`src/journal/wire/record.rs`) validates the header
//! CRC32C and payload CRC32C before its semantic rows, so any failure row
//! that fires AFTER both CRC gates passed — the publish_seq kind rules and
//! the CrudCommit split-header consistency check (`chain_end !=
//! payload.len()`) — is content corruption inside a fully-written,
//! dual-CRC-valid record: the exact class the R4 fix surfaced as
//! `Err(CorruptDatabase)` one layer down. Rows at or before the CRC gates
//! (bad magic, bad lengths, either CRC mismatch) remain indistinguishable
//! from a torn tail and must KEEP their `Ok(None)` disposition, pinned by
//! the torn-tail guard test below.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::Error;
use crate::journal::wire::{
    ChainCommitFrame, FinalizedLogRecord, JournalHeader, LogRecordDraft, LogicalTxnFrame,
    JOURNAL_HEADER_SIZE, LOGICAL_TXN_FORMAT_VERSION, LOG_RECORD_HEADER_CRC32C_OFFSET,
    LOG_RECORD_HEADER_LEN, LOG_RECORD_PAYLOAD_CRC32C_OFFSET, LOG_RECORD_PUBLISH_SEQ_OFFSET,
};
use crate::journal::{journal_path_for, JournalManager};
use crate::mvcc::timestamp::Ts;
use crate::storage::header::FileHeader;

const SALT1: u32 = 0xF24C_E001;
const SALT2: u32 = 0xF24D_E002;

struct DbFixture {
    _dir: tempfile::TempDir,
    db_path: PathBuf,
    header: FileHeader,
    main_file: std::fs::File,
}

fn make_db_file() -> DbFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("f24-outer.mqlite");
    let header = FileHeader::new(456, SALT1, SALT2);
    let mut main_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&db_path)
        .expect("create db");
    main_file
        .write_all(&header.to_bytes())
        .expect("write header");
    DbFixture {
        _dir: dir,
        db_path,
        header,
        main_file,
    }
}

fn write_journal(db_path: &Path, chunks: &[&[u8]]) {
    let journal_path = journal_path_for(db_path);
    let mut journal = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(journal_path)
        .expect("create journal");
    journal
        .write_all(&JournalHeader::new(SALT1, SALT2).to_bytes())
        .expect("write journal header");
    for chunk in chunks {
        journal.write_all(chunk).expect("write journal chunk");
    }
    journal.sync_all().expect("sync journal");
}

fn ts(physical_ms: u64, logical: u32) -> Ts {
    Ts {
        physical_ms,
        logical,
    }
}

fn crud_record(start_lsn: u64, txn_id: u64, publish_seq: u64, commit_ts: Ts) -> FinalizedLogRecord {
    let logical = LogicalTxnFrame {
        salt1: SALT1,
        salt2: SALT2,
        commit_ts,
        diagnostic_txn_id: txn_id,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![],
    }
    .encode()
    .expect("encode logical");
    let chain = ChainCommitFrame {
        salt1: SALT1,
        salt2: SALT2,
        commit_ts,
        refcount_deltas: vec![],
        page_writes: vec![],
    }
    .encode()
    .expect("encode chain");
    LogRecordDraft::crud(txn_id, publish_seq, commit_ts, logical, chain)
        .finalize(start_lsn)
        .expect("finalize crud")
}

/// Recompute both OUTER CRC32C fields after a mutation so the outer
/// `LogRecord` still passes dual-CRC validation.
fn recompute_outer_crcs(bytes: &mut [u8]) {
    let payload_crc = crc32c::crc32c(&bytes[LOG_RECORD_HEADER_LEN..]);
    bytes[LOG_RECORD_PAYLOAD_CRC32C_OFFSET..LOG_RECORD_PAYLOAD_CRC32C_OFFSET + 4]
        .copy_from_slice(&payload_crc.to_le_bytes());
    bytes[LOG_RECORD_HEADER_CRC32C_OFFSET..LOG_RECORD_HEADER_CRC32C_OFFSET + 4]
        .copy_from_slice(&0u32.to_le_bytes());
    let header_crc = crc32c::crc32c(&bytes[..LOG_RECORD_HEADER_LEN]);
    bytes[LOG_RECORD_HEADER_CRC32C_OFFSET..LOG_RECORD_HEADER_CRC32C_OFFSET + 4]
        .copy_from_slice(&header_crc.to_le_bytes());
}

/// Harness sanity gate (not the bug): prove the mutated record is still
/// dual-CRC-valid — i.e. it is a fully-written record, not a torn tail —
/// by recomputing both CRC32C values over the mutated bytes and comparing
/// them to the stored fields.
fn assert_outer_crcs_valid(bytes: &[u8]) {
    let mut header_for_crc = [0u8; LOG_RECORD_HEADER_LEN];
    header_for_crc.copy_from_slice(&bytes[..LOG_RECORD_HEADER_LEN]);
    header_for_crc[LOG_RECORD_HEADER_CRC32C_OFFSET..LOG_RECORD_HEADER_CRC32C_OFFSET + 4]
        .copy_from_slice(&0u32.to_le_bytes());
    let stored_header_crc = u32::from_le_bytes(
        bytes[LOG_RECORD_HEADER_CRC32C_OFFSET..LOG_RECORD_HEADER_CRC32C_OFFSET + 4]
            .try_into()
            .expect("4 bytes"),
    );
    assert_eq!(
        stored_header_crc,
        crc32c::crc32c(&header_for_crc),
        "harness: mutated record must still pass the header CRC32C gate"
    );
    let stored_payload_crc = u32::from_le_bytes(
        bytes[LOG_RECORD_PAYLOAD_CRC32C_OFFSET..LOG_RECORD_PAYLOAD_CRC32C_OFFSET + 4]
            .try_into()
            .expect("4 bytes"),
    );
    assert_eq!(
        stored_payload_crc,
        crc32c::crc32c(&bytes[LOG_RECORD_HEADER_LEN..]),
        "harness: mutated record must still pass the payload CRC32C gate"
    );
}

/// Make the CrudCommit split header inconsistent: bump the `chain_len` word
/// (payload bytes 4..8) so `chain_end != payload.len()` (the record.rs
/// `LogRecordPayload::decode` row), then recompute both OUTER CRCs so the
/// record stays dual-CRC-valid.
fn corrupt_split_header(record_bytes: &[u8]) -> Vec<u8> {
    let mut bytes = record_bytes.to_vec();
    let chain_len_off = LOG_RECORD_HEADER_LEN + 4;
    let chain_len = u32::from_le_bytes(
        bytes[chain_len_off..chain_len_off + 4]
            .try_into()
            .expect("4 bytes"),
    );
    bytes[chain_len_off..chain_len_off + 4].copy_from_slice(&(chain_len + 1).to_le_bytes());
    recompute_outer_crcs(&mut bytes);
    assert_outer_crcs_valid(&bytes);
    bytes
}

/// Zero the `publish_seq` header field on a CrudCommit record (reserved for
/// CheckpointBoundary — the record.rs post-CRC kind-rule row) and recompute
/// the OUTER CRCs so the record stays dual-CRC-valid.
fn corrupt_publish_seq_to_zero(record_bytes: &[u8]) -> Vec<u8> {
    let mut bytes = record_bytes.to_vec();
    bytes[LOG_RECORD_PUBLISH_SEQ_OFFSET..LOG_RECORD_PUBLISH_SEQ_OFFSET + 8]
        .copy_from_slice(&0u64.to_le_bytes());
    recompute_outer_crcs(&mut bytes);
    assert_outer_crcs_valid(&bytes);
    bytes
}

fn assert_recovery_fails_corrupt(corrupted_a: &[u8], record_b: &FinalizedLogRecord) {
    let mut fixture = make_db_file();
    write_journal(&fixture.db_path, &[corrupted_a, record_b.bytes()]);

    let result = JournalManager::open_or_create(
        &fixture.db_path,
        &fixture.header,
        &mut fixture.main_file,
    );

    match result {
        Err(Error::CorruptDatabase {
            path, recoverable, ..
        }) => {
            assert_eq!(
                path,
                journal_path_for(&fixture.db_path),
                "outer semantic corruption must carry the real journal path"
            );
            assert!(
                !recoverable,
                "outer semantic corruption must normalize `recoverable` to match \
                 log_record_recovery_corruption (false)"
            );
        }
        Ok(manager) => {
            let journal_len = std::fs::metadata(journal_path_for(&fixture.db_path))
                .expect("journal metadata")
                .len();
            panic!(
                "BUG(F24): recovery silently succeeded over a semantically corrupt \
                 record whose outer LogRecord passed both CRC32C checks; \
                 write_cursor={} journal_len={} (committed record B at [{}, {}) \
                 was durably destroyed)",
                manager.write_cursor(),
                journal_len,
                record_b.start_lsn(),
                record_b.end_lsn(),
            );
        }
        Err(other) => panic!("expected CorruptDatabase, got unexpected error: {other:?}"),
    }
}

/// F24 invariant: a CrudCommit whose OUTER header+payload CRC32Cs both
/// validate but whose split header is inconsistent (`chain_end !=
/// payload.len()`) is detected corruption and must fail recovery with
/// `Err(CorruptDatabase)`, not silently truncate as a torn tail.
#[test]
fn f24_split_header_corruption_inside_dual_crc_valid_record_fails_recovery_as_corrupt() {
    let record_a = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let record_b = crud_record(record_a.end_lsn(), 2, 2, ts(20, 0));
    let corrupted_a = corrupt_split_header(record_a.bytes());
    assert_recovery_fails_corrupt(&corrupted_a, &record_b);
}

/// F24 blast radius: whatever the recovery outcome for the corrupt record
/// itself, recovery must never physically truncate LATER committed-and-synced
/// records off the journal when the damaged record's OUTER CRCs validate.
#[test]
fn f24_split_header_corruption_must_not_destroy_later_committed_records() {
    let mut fixture = make_db_file();
    let record_a = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let record_b = crud_record(record_a.end_lsn(), 2, 2, ts(20, 0));
    let corrupted_a = corrupt_split_header(record_a.bytes());

    write_journal(&fixture.db_path, &[&corrupted_a, record_b.bytes()]);

    // Outcome (Ok vs Err) is pinned by the sibling test; here we only pin
    // that record B's committed bytes physically survive the recovery pass.
    let _ = JournalManager::open_or_create(
        &fixture.db_path,
        &fixture.header,
        &mut fixture.main_file,
    );

    let journal_len = std::fs::metadata(journal_path_for(&fixture.db_path))
        .expect("journal metadata")
        .len();
    assert!(
        journal_len >= record_b.end_lsn(),
        "BUG(F24): recovery truncated the journal to {journal_len} bytes, physically \
         destroying committed-and-synced record B at [{}, {}); a post-CRC semantic \
         decode failure on a dual-CRC-valid outer record is corruption, not a torn tail",
        record_b.start_lsn(),
        record_b.end_lsn(),
    );
}

/// F24 sibling row: the publish_seq kind rules are also checked AFTER both
/// outer CRC gates, so a CrudCommit carrying the reserved publish_seq 0
/// inside a dual-CRC-valid record is corruption, not a torn tail.
#[test]
fn f24_publish_seq_zero_crud_inside_dual_crc_valid_record_fails_recovery_as_corrupt() {
    let record_a = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let record_b = crud_record(record_a.end_lsn(), 2, 2, ts(20, 0));
    let corrupted_a = corrupt_publish_seq_to_zero(record_a.bytes());
    assert_recovery_fails_corrupt(&corrupted_a, &record_b);
}

/// Torn-tail guard: a payload CRC32C mismatch (outer CRCs NOT recomputed)
/// fires AT the CRC gate, so it stays indistinguishable from a torn tail —
/// the scan must stop at the damaged record and recovery must succeed,
/// truncating from the torn record onward (the pre-F24 disposition for
/// CRC/magic/length rows must NOT change).
#[test]
fn f24_payload_crc_mismatch_remains_torn_tail() {
    let mut fixture = make_db_file();
    let record_a = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let record_b = crud_record(record_a.end_lsn(), 2, 2, ts(20, 0));
    let mut torn_a = record_a.bytes().to_vec();
    // Flip one payload byte WITHOUT recomputing the outer CRCs: the payload
    // CRC32C gate fails, exactly as if the tail were torn mid-payload.
    torn_a[LOG_RECORD_HEADER_LEN + 9] ^= 0xFF;

    write_journal(&fixture.db_path, &[&torn_a, record_b.bytes()]);

    let manager = JournalManager::open_or_create(
        &fixture.db_path,
        &fixture.header,
        &mut fixture.main_file,
    )
    .expect("a CRC-gate failure is a torn tail; recovery must succeed");
    assert_eq!(
        manager.write_cursor(),
        record_a.start_lsn(),
        "scan must stop at the torn record's start"
    );
}
