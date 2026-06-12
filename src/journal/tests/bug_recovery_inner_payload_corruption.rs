//! BUG-4 regression tests: a CRUD record whose OUTER `LogRecord` passes both
//! CRC32C checks (`src/journal/wire/record.rs`) but whose INNER
//! `LogicalTxnFrame` payload is corrupt must surface `Err(CorruptDatabase)`
//! per the §4.6 MidStream disposition table (`src/journal/wire/logical.rs`).
//! It must NOT be treated as a torn tail: the dual outer CRCs prove the
//! record was fully written, so tail truncation cannot explain the damage.
//!
//! `decode_log_record_recovery_payload` (`src/journal/recovery.rs`) used to
//! flatten the MidStream `Err(CorruptDatabase)` to `None` via
//! `.ok().flatten()?`; `read_log_record_at` then reported `Ok(None)`, the
//! scan loop stopped as if the journal tail were torn, and
//! `truncate_tail_to_valid_end_lsn` physically `set_len()`ed the journal
//! before the corrupt record — durably destroying every LATER
//! committed-and-fsynced record. The decode error now propagates and
//! recovery fails without touching the journal bytes.

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
    ChainCommitFrame, DecodeCtx, FinalizedLogRecord, JournalHeader, LogRecord, LogRecordDraft,
    LogRecordPayload, LogicalTxnFrame, JOURNAL_HEADER_SIZE, LOGICAL_TXN_FORMAT_VERSION,
    LOG_RECORD_HEADER_CRC32C_OFFSET, LOG_RECORD_HEADER_LEN, LOG_RECORD_PAYLOAD_CRC32C_OFFSET,
};
use crate::journal::{journal_path_for, JournalManager};
use crate::mvcc::timestamp::Ts;
use crate::storage::header::FileHeader;

const SALT1: u32 = 0xA11C_E001;
const SALT2: u32 = 0xB22D_E002;

struct DbFixture {
    _dir: tempfile::TempDir,
    db_path: PathBuf,
    header: FileHeader,
    main_file: std::fs::File,
}

fn make_db_file() -> DbFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("bug4.mqlite");
    let header = FileHeader::new(123, SALT1, SALT2);
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

/// Flip one byte INSIDE the inner `LogicalTxnFrame` payload of a finalized
/// CrudCommit record and recompute both OUTER CRC32C fields so the outer
/// `LogRecord` still validates. The inner frame's trailing CRC32C is left
/// untouched, so the inner decode hits the §4.6 MidStream content-error row
/// ("LogicalTxnFrame CRC32C mismatch" — `Err(CorruptDatabase)`).
fn corrupt_inner_logical_payload(record_bytes: &[u8]) -> Vec<u8> {
    let mut bytes = record_bytes.to_vec();
    // CrudCommit payload layout: [logical_len u32][chain_len u32][logical][chain],
    // so the inner logical frame starts 8 bytes past the 72-byte outer header.
    // Frame offset 28..36 is `diagnostic_txn_id`: flipping it breaks the inner
    // frame CRC while leaving frame_kind/total_frame_bytes/salts intact, so the
    // inner decode reaches the CRC gate (a content error, not a tail-like row).
    let target = LOG_RECORD_HEADER_LEN + 8 + 28;
    bytes[target] ^= 0xFF;

    // Recompute the outer payload CRC over the (now corrupt) payload bytes.
    let payload_crc = crc32c::crc32c(&bytes[LOG_RECORD_HEADER_LEN..]);
    bytes[LOG_RECORD_PAYLOAD_CRC32C_OFFSET..LOG_RECORD_PAYLOAD_CRC32C_OFFSET + 4]
        .copy_from_slice(&payload_crc.to_le_bytes());

    // Recompute the outer header CRC (covers bytes 0..72 with the header-CRC
    // field zeroed, including the payload-CRC field rewritten above).
    bytes[LOG_RECORD_HEADER_CRC32C_OFFSET..LOG_RECORD_HEADER_CRC32C_OFFSET + 4]
        .copy_from_slice(&0u32.to_le_bytes());
    let header_crc = crc32c::crc32c(&bytes[..LOG_RECORD_HEADER_LEN]);
    bytes[LOG_RECORD_HEADER_CRC32C_OFFSET..LOG_RECORD_HEADER_CRC32C_OFFSET + 4]
        .copy_from_slice(&header_crc.to_le_bytes());
    bytes
}

/// Harness sanity gate (not the bug): proves the corruption is inner-only —
/// the outer record still passes dual-CRC validation while the inner logical
/// frame decode surfaces the §4.6 MidStream `Err(CorruptDatabase)` row.
fn assert_outer_valid_inner_corrupt(corrupted: &[u8], start_lsn: u64) {
    let record = LogRecord::decode(corrupted)
        .expect("harness: outer LogRecord must pass both CRC32C checks after inner-only corruption");
    assert_eq!(record.start_lsn, start_lsn, "harness: outer record intact");
    let LogRecordPayload::CrudCommit {
        logical_payload, ..
    } = &record.payload
    else {
        panic!("harness: corrupted record must still decode as CrudCommit");
    };
    let inner = LogicalTxnFrame::decode(
        logical_payload,
        SALT1,
        SALT2,
        DecodeCtx::MidStream { follower: true },
    );
    assert!(
        matches!(inner, Err(Error::CorruptDatabase { .. })),
        "harness: inner logical decode must hit the §4.6 MidStream \
         content-error row (CRC mismatch), got {inner:?}"
    );
}

/// BUG-4 invariant: mid-stream corruption confined to a record's INNER
/// payload — while the OUTER record passes both CRC32C checks — is detected
/// corruption per §4.6 and must fail recovery with `Err(CorruptDatabase)`,
/// not silently succeed as a torn-tail truncation.
#[test]
fn bug4_inner_payload_corruption_of_outer_valid_record_fails_recovery_as_corrupt() {
    let mut fixture = make_db_file();
    let record_a = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let record_b = crud_record(record_a.end_lsn(), 2, 2, ts(20, 0));
    let corrupted_a = corrupt_inner_logical_payload(record_a.bytes());
    assert_outer_valid_inner_corrupt(&corrupted_a, record_a.start_lsn());

    write_journal(&fixture.db_path, &[&corrupted_a, record_b.bytes()]);

    let result = JournalManager::open_or_create(
        &fixture.db_path,
        &fixture.header,
        &mut fixture.main_file,
    );

    match result {
        Err(Error::CorruptDatabase { .. }) => {}
        Ok(manager) => {
            let journal_len = std::fs::metadata(journal_path_for(&fixture.db_path))
                .expect("journal metadata")
                .len();
            panic!(
                "BUG(BUG-4): recovery silently succeeded over an inner-payload-corrupt \
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

/// BUG-4 blast radius: whatever the recovery outcome for the corrupt record
/// itself, recovery must never physically truncate LATER committed-and-synced
/// records off the journal when the damaged record's OUTER CRCs validate.
/// Today the scan misclassifies the inner corruption as a torn tail and
/// `set_len()`s the journal back to the bare header, durably destroying
/// record B.
#[test]
fn bug4_inner_payload_corruption_must_not_destroy_later_committed_records() {
    let mut fixture = make_db_file();
    let record_a = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let record_b = crud_record(record_a.end_lsn(), 2, 2, ts(20, 0));
    let corrupted_a = corrupt_inner_logical_payload(record_a.bytes());
    assert_outer_valid_inner_corrupt(&corrupted_a, record_a.start_lsn());

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
        "BUG(BUG-4): recovery truncated the journal to {journal_len} bytes, physically \
         destroying committed-and-synced record B at [{}, {}); an inner-payload decode \
         failure on an outer-CRC-valid record is corruption, not a torn tail",
        record_b.start_lsn(),
        record_b.end_lsn(),
    );
}
