//! Journal recovery regression tests.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::journal::log_file::{
    CatalogCommitKind, CatalogCommitPayload, ChainCommitFrame, CheckpointBoundaryPayload,
    FinalizedLogRecord, LogRecordDraft, LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION,
    LOG_RECORD_HEADER_CRC32C_OFFSET, LOG_RECORD_HEADER_LEN, LOG_RECORD_KIND_OFFSET,
};
use crate::journal::{journal_path_for, JournalManager};
use crate::mvcc::timestamp::Ts;
use crate::storage::header::FileHeader;

use super::*;

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
    let db_path = dir.path().join("phase8.mqlite");
    let header = FileHeader::new(123, SALT1, SALT2);
    make_db_file_with_header(dir, db_path, header)
}

fn make_db_file_with_checkpoint_applied_lsn(checkpoint_applied_lsn: u64) -> DbFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("phase8.mqlite");
    let mut header = FileHeader::new(123, SALT1, SALT2);
    header.checkpoint_applied_lsn = checkpoint_applied_lsn;
    make_db_file_with_header(dir, db_path, header)
}

fn make_db_file_with_header(
    dir: tempfile::TempDir,
    db_path: PathBuf,
    header: FileHeader,
) -> DbFixture {
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

fn logical_frame(commit_ts: Ts, txn_id: u64) -> LogicalTxnFrame {
    LogicalTxnFrame {
        salt1: SALT1,
        salt2: SALT2,
        commit_ts,
        diagnostic_txn_id: txn_id,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![],
    }
}

fn crud_record(start_lsn: u64, txn_id: u64, publish_seq: u64, commit_ts: Ts) -> FinalizedLogRecord {
    let logical = logical_frame(commit_ts, txn_id)
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

fn catalog_record(
    start_lsn: u64,
    txn_id: u64,
    publish_seq: u64,
    commit_ts: Ts,
) -> FinalizedLogRecord {
    LogRecordDraft::catalog(txn_id, publish_seq, commit_ts, b"catalog".to_vec())
        .finalize(start_lsn)
        .expect("finalize catalog")
}

fn typed_catalog_record(
    start_lsn: u64,
    txn_id: u64,
    publish_seq: u64,
    commit_ts: Ts,
    header: FileHeader,
) -> FinalizedLogRecord {
    let payload = CatalogCommitPayload {
        kind: CatalogCommitKind::NamespaceCreate,
        catalog_generation_before: 1,
        catalog_generation_after: 2,
        header,
        pages: vec![],
    }
    .encode()
    .expect("encode catalog payload");
    LogRecordDraft::catalog(txn_id, publish_seq, commit_ts, payload)
        .finalize(start_lsn)
        .expect("finalize catalog")
}

fn checkpoint_record(
    start_lsn: u64,
    txn_id: u64,
    commit_ts: Ts,
    checkpoint_applied_lsn: u64,
) -> FinalizedLogRecord {
    let mut header = FileHeader::new(123, SALT1, SALT2);
    header.checkpoint_applied_lsn = checkpoint_applied_lsn;
    let payload = CheckpointBoundaryPayload {
        checkpoint_applied_lsn,
        header,
    }
    .encode()
    .expect("encode checkpoint boundary");
    LogRecordDraft::checkpoint_boundary(txn_id, commit_ts, payload)
        .finalize(start_lsn)
        .expect("finalize checkpoint")
}

fn recover(mut fixture: DbFixture) -> JournalManager {
    JournalManager::open_or_create(&fixture.db_path, &fixture.header, &mut fixture.main_file)
        .expect("recover phase8 journal")
}

fn read_main_header(path: &Path) -> FileHeader {
    let mut file = OpenOptions::new().read(true).open(path).expect("open db");
    let mut bytes = [0u8; crate::storage::header::HEADER_PAGE_SIZE];
    std::io::Read::read_exact(&mut file, &mut bytes).expect("read header");
    FileHeader::from_bytes(&bytes).expect("parse header")
}

#[test]
fn log_record_recovery_applies_crud_records_by_publish_seq_not_lsn() {
    let fixture = make_db_file();
    let first_lsn = JOURNAL_HEADER_SIZE as u64;
    let later_publish = crud_record(first_lsn, 20, 2, ts(20, 0));
    let earlier_publish = crud_record(later_publish.end_lsn(), 10, 1, ts(10, 0));
    let valid_end = earlier_publish.end_lsn();

    write_journal(
        &fixture.db_path,
        &[later_publish.bytes(), earlier_publish.bytes()],
    );
    let mut manager = recover(fixture);
    let parsed = manager.take_parsed_logical_frames();

    assert_eq!(manager.write_cursor(), valid_end);
    assert_eq!(manager.log_manager.ready_lsn(), valid_end);
    assert_eq!(manager.log_manager.durable_lsn(), valid_end);
    assert_eq!(manager.recovered_max_commit_ts(), Some(ts(20, 0)));
    assert_eq!(manager.recovered_max_publish_seq(), Some(2));
    assert_eq!(parsed.frames.len(), 2);
    assert_eq!(parsed.frames[0].0, earlier_publish.start_lsn());
    assert_eq!(parsed.frames[1].0, later_publish.start_lsn());

    let slot = manager.log_manager.reserve(1).expect("reserve next lsn");
    assert_eq!(slot.start_lsn(), valid_end);
}

#[test]
fn log_record_recovery_truncates_torn_tail_before_later_bytes() {
    let fixture = make_db_file();
    let first = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let torn = crud_record(first.end_lsn(), 2, 2, ts(20, 0));
    let post_torn = crud_record(torn.end_lsn(), 3, 3, ts(30, 0));
    let torn_prefix = &torn.bytes()[..torn.bytes().len() - 3];

    write_journal(
        &fixture.db_path,
        &[first.bytes(), torn_prefix, post_torn.bytes()],
    );
    let mut manager = recover(fixture);
    let parsed = manager.take_parsed_logical_frames();

    assert_eq!(manager.write_cursor(), first.end_lsn());
    assert_eq!(manager.recovered_max_commit_ts(), Some(ts(10, 0)));
    assert_eq!(manager.recovered_max_publish_seq(), Some(1));
    assert_eq!(parsed.frames.len(), 1);
}

#[test]
fn log_record_recovery_truncates_bad_crc_tail() {
    let fixture = make_db_file();
    let first = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let bad = crud_record(first.end_lsn(), 2, 2, ts(20, 0));
    let mut bad_bytes = bad.bytes().to_vec();
    let last = bad_bytes.len() - 1;
    bad_bytes[last] ^= 0x55;

    write_journal(&fixture.db_path, &[first.bytes(), &bad_bytes]);
    let manager = recover(fixture);

    assert_eq!(manager.write_cursor(), first.end_lsn());
    assert_eq!(manager.recovered_max_commit_ts(), Some(ts(10, 0)));
}

#[test]
fn log_record_recovery_truncates_unknown_kind_at_previous_valid_lsn() {
    let fixture = make_db_file();
    let first = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let unknown = catalog_record(first.end_lsn(), 2, 2, ts(20, 0));
    let mut unknown_bytes = unknown.bytes().to_vec();
    unknown_bytes[LOG_RECORD_KIND_OFFSET..LOG_RECORD_KIND_OFFSET + 2]
        .copy_from_slice(&99u16.to_le_bytes());
    unknown_bytes[LOG_RECORD_HEADER_CRC32C_OFFSET..LOG_RECORD_HEADER_CRC32C_OFFSET + 4]
        .copy_from_slice(&0u32.to_le_bytes());
    let header_crc = crc32c::crc32c(&unknown_bytes[..LOG_RECORD_HEADER_LEN]);
    unknown_bytes[LOG_RECORD_HEADER_CRC32C_OFFSET..LOG_RECORD_HEADER_CRC32C_OFFSET + 4]
        .copy_from_slice(&header_crc.to_le_bytes());

    write_journal(&fixture.db_path, &[first.bytes(), &unknown_bytes]);
    let manager = recover(fixture);

    assert_eq!(manager.write_cursor(), first.end_lsn());
    assert_eq!(manager.recovered_max_commit_ts(), Some(ts(10, 0)));
}

#[test]
fn log_record_recovery_truncates_valid_prefix_plus_garbage() {
    let fixture = make_db_file();
    let first = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));

    write_journal(&fixture.db_path, &[first.bytes(), b"garbage tail"]);
    let manager = recover(fixture);

    assert_eq!(manager.write_cursor(), first.end_lsn());
    assert_eq!(manager.recovered_max_commit_ts(), Some(ts(10, 0)));
}

#[test]
fn log_record_recovery_skips_checkpoint_applied_records_and_control_floors() {
    let skipped = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 1, ts(10, 0));
    let fixture = make_db_file_with_checkpoint_applied_lsn(skipped.end_lsn());
    let boundary = checkpoint_record(skipped.end_lsn(), 2, ts(999, 0), skipped.end_lsn());
    let applied = crud_record(boundary.end_lsn(), 3, 3, ts(30, 0));

    write_journal(
        &fixture.db_path,
        &[skipped.bytes(), boundary.bytes(), applied.bytes()],
    );
    let mut manager = recover(fixture);
    let parsed = manager.take_parsed_logical_frames();

    assert_eq!(manager.write_cursor(), applied.end_lsn());
    assert_eq!(manager.recovered_max_commit_ts(), Some(ts(30, 0)));
    assert_eq!(manager.recovered_max_publish_seq(), Some(3));
    assert_eq!(
        parsed.frames,
        vec![(applied.start_lsn(), logical_frame(ts(30, 0), 3))]
    );
}

#[test]
fn log_record_recovery_seeds_hlc_from_main_checkpoint_ts_without_apply_set() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("phase8.mqlite");
    let mut header = FileHeader::new(123, SALT1, SALT2);
    header.last_checkpoint_ts = ts(900, 7);
    header.checkpoint_applied_lsn = JOURNAL_HEADER_SIZE as u64;
    let fixture = make_db_file_with_header(dir, db_path, header.clone());
    let boundary = checkpoint_record(
        JOURNAL_HEADER_SIZE as u64,
        2,
        ts(999, 0),
        header.checkpoint_applied_lsn,
    );

    write_journal(&fixture.db_path, &[boundary.bytes()]);
    let manager = recover(fixture);

    assert_eq!(
        manager.recovered_max_commit_ts(),
        Some(header.last_checkpoint_ts)
    );
    assert_eq!(manager.recovered_max_publish_seq(), None);
}

#[test]
fn catalog_recovery_preserves_main_checkpoint_frontier() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("phase8.mqlite");
    let mut header = FileHeader::new(123, SALT1, SALT2);
    header.last_checkpoint_ts = ts(700, 3);
    header.checkpoint_applied_lsn = JOURNAL_HEADER_SIZE as u64;
    let mut fixture = make_db_file_with_header(dir, db_path, header.clone());
    let stale_catalog_header = FileHeader::new(123, SALT1, SALT2);
    let catalog = typed_catalog_record(
        JOURNAL_HEADER_SIZE as u64,
        10,
        1,
        ts(710, 0),
        stale_catalog_header,
    );

    write_journal(&fixture.db_path, &[catalog.bytes()]);
    let manager =
        JournalManager::open_or_create(&fixture.db_path, &fixture.header, &mut fixture.main_file)
            .expect("recover phase8 journal");
    let recovered = read_main_header(&fixture.db_path);

    assert_eq!(recovered.last_checkpoint_ts, header.last_checkpoint_ts);
    assert_eq!(
        recovered.checkpoint_applied_lsn,
        header.checkpoint_applied_lsn
    );
    assert_eq!(manager.recovered_max_commit_ts(), Some(ts(710, 0)));
}

#[test]
fn log_record_recovery_rejects_duplicate_publish_seq_after_prefix_validation() {
    let mut fixture = make_db_file();
    let first = crud_record(JOURNAL_HEADER_SIZE as u64, 1, 7, ts(10, 0));
    let duplicate = crud_record(first.end_lsn(), 2, 7, ts(20, 0));

    write_journal(&fixture.db_path, &[first.bytes(), duplicate.bytes()]);
    let err = match JournalManager::open_or_create(
        &fixture.db_path,
        &fixture.header,
        &mut fixture.main_file,
    ) {
        Ok(_) => panic!("duplicate publish_seq must reject recovery"),
        Err(err) => err,
    };

    match err {
        Error::CorruptDatabase { detail, .. } => {
            assert!(detail.contains("duplicate Phase 8 LogRecord publish_seq 7"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
