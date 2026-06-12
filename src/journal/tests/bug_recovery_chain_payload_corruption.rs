//! BUG-4 chain-half regression tests (critic finding R4): a CRUD record whose
//! OUTER `LogRecord` passes both CRC32C checks but whose INNER
//! `ChainCommitFrame` payload is corrupt must surface `Err(CorruptDatabase)`,
//! exactly like the logical half fixed by
//! `bug_recovery_inner_payload_corruption.rs`. `ChainCommitFrame::decode`
//! (`src/journal/wire/payloads.rs`) flattens ALL of its failure rows — CRC
//! mismatch, count/body inconsistency, invalid page-size marker, trailing
//! bytes — into `Ok(None)`; `decode_log_record_recovery_payload`
//! (`src/journal/recovery.rs`) used to map that to a torn tail, and
//! `truncate_tail_to_valid_end_lsn` physically `set_len()`ed the journal
//! before the corrupt record — durably destroying every LATER
//! committed-and-fsynced record. The dual outer CRCs prove the record was
//! fully written, so tail truncation cannot explain the damage.
//!
//! Per §4.6 the ONLY inner failure row that may legitimately occur inside an
//! outer-CRC-valid record is a salt mismatch (a stale, fully-written record
//! from a previous database lifetime); that row must KEEP its tail-like
//! `Ok(None)` disposition, pinned by the stale-salt guard test below.

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

/// Salts from "a previous database lifetime" for the stale-salt guard test.
const STALE_SALT1: u32 = 0x0DD5_A171;
const STALE_SALT2: u32 = 0x0DD5_A172;

struct DbFixture {
    _dir: tempfile::TempDir,
    db_path: PathBuf,
    header: FileHeader,
    main_file: std::fs::File,
}

fn make_db_file() -> DbFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("bug4-chain.mqlite");
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

fn crud_record_with_frame_salts(
    start_lsn: u64,
    txn_id: u64,
    publish_seq: u64,
    commit_ts: Ts,
    frame_salt1: u32,
    frame_salt2: u32,
) -> FinalizedLogRecord {
    let logical = LogicalTxnFrame {
        salt1: frame_salt1,
        salt2: frame_salt2,
        commit_ts,
        diagnostic_txn_id: txn_id,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![],
    }
    .encode()
    .expect("encode logical");
    let chain = ChainCommitFrame {
        salt1: frame_salt1,
        salt2: frame_salt2,
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

fn crud_record(start_lsn: u64, txn_id: u64, publish_seq: u64, commit_ts: Ts) -> FinalizedLogRecord {
    crud_record_with_frame_salts(start_lsn, txn_id, publish_seq, commit_ts, SALT1, SALT2)
}

/// Recompute both OUTER CRC32C fields after an inner-payload mutation so the
/// outer `LogRecord` still passes dual-CRC validation.
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

/// Flip one byte INSIDE the inner `ChainCommitFrame` payload of a finalized
/// CrudCommit record and recompute both OUTER CRC32C fields so the outer
/// `LogRecord` still validates. The chain frame's trailing CRC32C is left
/// untouched, so `ChainCommitFrame::decode` hits its CRC-mismatch row
/// (`payloads.rs`) — which it silently reports as `Ok(None)`.
fn corrupt_inner_chain_payload(record_bytes: &[u8]) -> Vec<u8> {
    let mut bytes = record_bytes.to_vec();
    // CrudCommit payload layout: [logical_len u32][chain_len u32][logical][chain].
    let logical_len = u32::from_le_bytes(
        bytes[LOG_RECORD_HEADER_LEN..LOG_RECORD_HEADER_LEN + 4]
            .try_into()
            .expect("4 bytes"),
    ) as usize;
    let chain_start = LOG_RECORD_HEADER_LEN + 8 + logical_len;
    // Chain-frame offset 16..28 is `commit_ts`: flipping it breaks the chain
    // frame's trailing CRC32C while leaving frame_kind/total_frame_bytes/salts
    // (bytes 0..16) intact, so the decode reaches the CRC gate — a content
    // error inside a dual-CRC-valid outer record, not a tail-like row.
    bytes[chain_start + 16] ^= 0xFF;
    recompute_outer_crcs(&mut bytes);
    bytes
}

/// Flip one byte INSIDE the inner `LogicalTxnFrame` payload (mirrors the
/// BUG-4 logical-half corruption) so the §4.6 MidStream dispose row fires —
/// used to pin the journal-path / `recoverable` normalization of that error.
fn corrupt_inner_logical_payload(record_bytes: &[u8]) -> Vec<u8> {
    let mut bytes = record_bytes.to_vec();
    // Logical frame offset 28..36 is `diagnostic_txn_id`: flipping it breaks
    // the inner frame CRC while leaving frame_kind/total/salts intact.
    let target = LOG_RECORD_HEADER_LEN + 8 + 28;
    bytes[target] ^= 0xFF;
    recompute_outer_crcs(&mut bytes);
    bytes
}

/// Harness sanity gate (not the bug): proves the corruption is confined to
/// the CHAIN half — the outer record still passes dual-CRC validation, the
/// logical frame still decodes, and `ChainCommitFrame::decode` reports the
/// damage as the silent `Ok(None)` row.
fn assert_outer_valid_chain_silently_corrupt(corrupted: &[u8], start_lsn: u64) {
    let record = LogRecord::decode(corrupted)
        .expect("harness: outer LogRecord must pass both CRC32C checks after chain-only corruption");
    assert_eq!(record.start_lsn, start_lsn, "harness: outer record intact");
    let LogRecordPayload::CrudCommit {
        logical_payload,
        chain_payload,
    } = &record.payload
    else {
        panic!("harness: corrupted record must still decode as CrudCommit");
    };
    let logical = LogicalTxnFrame::decode(
        logical_payload,
        SALT1,
        SALT2,
        DecodeCtx::MidStream { follower: true },
    )
    .expect("harness: logical half must be untouched");
    assert!(
        logical.is_some(),
        "harness: logical half must still decode — corruption is chain-only"
    );
    let chain = ChainCommitFrame::decode(chain_payload, SALT1, SALT2);
    assert!(
        matches!(chain, Ok(None)),
        "harness: chain decode must hit the silent CRC-mismatch Ok(None) row, got {chain:?}"
    );
}

/// R4 invariant (chain half of BUG-4): mid-stream corruption confined to a
/// record's INNER chain payload — while the OUTER record passes both CRC32C
/// checks — is detected corruption and must fail recovery with
/// `Err(CorruptDatabase)`, not silently succeed as a torn-tail truncation.
#[test]
fn bug4_chain_payload_corruption_of_outer_valid_record_fails_recovery_as_corrupt() {
    let mut fixture = make_db_file();
    let record_a = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let record_b = crud_record(record_a.end_lsn(), 2, 2, ts(20, 0));
    let corrupted_a = corrupt_inner_chain_payload(record_a.bytes());
    assert_outer_valid_chain_silently_corrupt(&corrupted_a, record_a.start_lsn());

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
                "BUG(R4): recovery silently succeeded over a chain-payload-corrupt \
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

/// R4 blast radius: whatever the recovery outcome for the corrupt record
/// itself, recovery must never physically truncate LATER committed-and-synced
/// records off the journal when the damaged record's OUTER CRCs validate.
#[test]
fn bug4_chain_payload_corruption_must_not_destroy_later_committed_records() {
    let mut fixture = make_db_file();
    let record_a = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let record_b = crud_record(record_a.end_lsn(), 2, 2, ts(20, 0));
    let corrupted_a = corrupt_inner_chain_payload(record_a.bytes());
    assert_outer_valid_chain_silently_corrupt(&corrupted_a, record_a.start_lsn());

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
        "BUG(R4): recovery truncated the journal to {journal_len} bytes, physically \
         destroying committed-and-synced record B at [{}, {}); an inner chain-payload \
         decode failure on an outer-CRC-valid record is corruption, not a torn tail",
        record_b.start_lsn(),
        record_b.end_lsn(),
    );
}

/// R4 error-context normalization: the inner-decode `CorruptDatabase` error
/// surfaced through recovery must carry the REAL journal path (the §4.6
/// dispose helper fills in `PathBuf::new()`) and the same `recoverable`
/// disposition as every other `log_record_recovery_corruption` error
/// (`recoverable: false`).
#[test]
fn bug4_inner_decode_corruption_error_carries_journal_path_and_normalized_disposition() {
    let mut fixture = make_db_file();
    let record_a = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let corrupted_a = corrupt_inner_logical_payload(record_a.bytes());

    let result = {
        write_journal(&fixture.db_path, &[&corrupted_a]);
        JournalManager::open_or_create(&fixture.db_path, &fixture.header, &mut fixture.main_file)
    };

    match result {
        Err(Error::CorruptDatabase {
            path, recoverable, ..
        }) => {
            assert_eq!(
                path,
                journal_path_for(&fixture.db_path),
                "inner-decode corruption must carry the real journal path, \
                 not the dispose helper's PathBuf::new()"
            );
            assert!(
                !recoverable,
                "inner-decode corruption must normalize `recoverable` to match \
                 log_record_recovery_corruption (false)"
            );
        }
        Err(other) => panic!("expected CorruptDatabase, got unexpected error: {other:?}"),
        Ok(_) => panic!("expected CorruptDatabase, recovery silently succeeded"),
    }
}

/// §4.6 stale-salt guard: a fully-written CrudCommit whose INNER frames carry
/// salts from a previous database lifetime is the ONE inner failure row that
/// is NOT corruption — it must keep its tail-like `Ok(None)` disposition
/// (scan stops, recovery succeeds) rather than fail as `CorruptDatabase`.
#[test]
fn bug4_stale_salt_inner_frames_remain_tail_like_not_corrupt() {
    let mut fixture = make_db_file();
    let stale = crud_record_with_frame_salts(
        JOURNAL_HEADER_SIZE as u64,
        1,
        1,
        ts(10, 0),
        STALE_SALT1,
        STALE_SALT2,
    );

    write_journal(&fixture.db_path, &[stale.bytes()]);

    let manager = JournalManager::open_or_create(
        &fixture.db_path,
        &fixture.header,
        &mut fixture.main_file,
    )
    .expect("stale-salted inner frames are a tail-like row, not corruption");
    assert_eq!(
        manager.write_cursor(),
        JOURNAL_HEADER_SIZE as u64,
        "scan must stop at the stale record's start, like a torn tail"
    );
}
