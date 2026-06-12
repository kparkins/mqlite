//! Journal recovery regression tests.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::journal::wire::{
    CatalogCommitKind, CatalogCommitPayload, ChainCommitFrame, CheckpointBoundaryPayload,
    CheckpointPageFramePayload, CheckpointPagePool, FinalizedLogRecord, JournalPageSize,
    LogRecordDraft, LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION, LOG_RECORD_HEADER_CRC32C_OFFSET,
    LOG_RECORD_HEADER_LEN, LOG_RECORD_KIND_OFFSET,
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

fn checkpoint_page_frame_record(
    start_lsn: u64,
    batch_id: u64,
    page_number: u32,
    fill: u8,
) -> FinalizedLogRecord {
    let page_size = JournalPageSize::Small4k;
    let payload = CheckpointPageFramePayload {
        batch_id,
        pool: CheckpointPagePool::Main,
        page_number,
        page_size,
        data: vec![fill; page_size.bytes()],
    }
    .encode()
    .expect("encode checkpoint page frame");
    LogRecordDraft::checkpoint_page_frame(Ts::default(), payload)
        .finalize(start_lsn)
        .expect("finalize checkpoint page frame")
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
        batch_id: 0,
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

/// Suspect-3 regression (orphan-truncate vs replay divergence): a checkpoint
/// page frame whose commit boundary never arrived (an "orphan" — a torn
/// checkpoint) is correctly excluded from replay, but it must NOT drive the
/// journal truncation floor below a later committed CrudCommit. Checkpoint
/// page frames and CRUD records share one LSN-ordered append stream
/// (`reserve_log_record_on`, `&self`, no outer mutex), so a transaction
/// committed-and-fsynced concurrently with an in-progress checkpoint lands at
/// a HIGHER LSN than that checkpoint's page frames. If the checkpoint then
/// crashes before its boundary, recovery sees `[orphan frame][committed
/// CrudCommit]`.
///
/// The divergence bug: the orphan's `start_lsn` was used as `valid_end_lsn`,
/// physically truncating the journal back below the committed CrudCommit —
/// even though that same CrudCommit was simultaneously replayed into recovered
/// state (`parsed_logical_frames`, `recovered_max_commit_ts`). Recovered state
/// and the durable journal then disagree: the next restart finds the record
/// gone. WiredTiger semantics: an orphan page frame is a torn checkpoint
/// excluded from replay, but it is a fully-written record that must not corrupt
/// the disposition of later committed records. The journal must be kept at
/// least through every record recovery replayed.
#[test]
fn orphan_checkpoint_frame_must_not_truncate_later_committed_crud() {
    let mut fixture = make_db_file();
    let orphan = checkpoint_page_frame_record(JOURNAL_HEADER_SIZE as u64, 42, 5, 0xAB);
    let crud = crud_record(orphan.end_lsn(), 7, 1, ts(50, 0));
    let valid_end = crud.end_lsn();

    write_journal(&fixture.db_path, &[orphan.bytes(), crud.bytes()]);
    let journal_path = journal_path_for(&fixture.db_path);
    // Keep `fixture` (and its tempdir) alive so the journal file is still on
    // disk after recovery — the consuming `recover()` helper would drop it.
    let mut manager =
        JournalManager::open_or_create(&fixture.db_path, &fixture.header, &mut fixture.main_file)
            .expect("recover orphan-frame journal");
    let parsed = manager.take_parsed_logical_frames();

    // The CrudCommit was replayed into recovered state.
    assert_eq!(
        parsed.frames,
        vec![(crud.start_lsn(), logical_frame(ts(50, 0), 7))],
        "the committed CrudCommit above the orphan must be replayed"
    );
    assert_eq!(manager.recovered_max_commit_ts(), Some(ts(50, 0)));

    // ...so its bytes must survive in the journal. The orphan frame (a torn
    // checkpoint with no boundary) is excluded from replay but must not drag
    // the truncation floor below the committed record it precedes.
    assert_eq!(
        manager.write_cursor(),
        valid_end,
        "orphan-frame truncation must not destroy the later committed CrudCommit \
         that recovery replayed into state (orphan-truncate vs replay divergence)"
    );
    let journal_len = std::fs::metadata(&journal_path)
        .expect("journal metadata")
        .len();
    // Exact equality: nothing exists above the CrudCommit, so recovery must
    // take the no-truncation path (a `>=` here could mask a partial
    // truncation if the fixture ever shrinks).
    assert_eq!(
        journal_len,
        crud.end_lsn(),
        "recovery truncated the journal to {journal_len} bytes, physically destroying \
         the committed CrudCommit at [{}, {}) that it replayed into recovered state",
        crud.start_lsn(),
        crud.end_lsn(),
    );

    // Idempotence across restarts: the orphan frame survives below the kept
    // record, so the NEXT recovery re-encounters it, re-classifies it as an
    // orphan, and must reach the identical end state without further
    // truncation or replay divergence.
    drop(manager);
    let mut manager2 =
        JournalManager::open_or_create(&fixture.db_path, &fixture.header, &mut fixture.main_file)
            .expect("second recovery over surviving orphan bytes");
    let parsed2 = manager2.take_parsed_logical_frames();
    assert_eq!(
        parsed2.frames,
        vec![(crud.start_lsn(), logical_frame(ts(50, 0), 7))],
        "second restart must replay the same CrudCommit (surviving orphan must be harmless)"
    );
    assert_eq!(manager2.recovered_max_commit_ts(), Some(ts(50, 0)));
    assert_eq!(
        manager2.write_cursor(),
        valid_end,
        "second restart must be a truncation no-op (byte-stable journal)"
    );
    assert_eq!(
        std::fs::metadata(&journal_path)
            .expect("journal metadata after second recovery")
            .len(),
        crud.end_lsn(),
        "journal length must be byte-stable across repeated recoveries"
    );
}

/// Multi-orphan interleaving: `[orphan B1][committed CRUD][orphan B2]`.
/// The truncation floor is the MINIMUM orphan start (B1), but the kept-record
/// clamp must hold it at the CRUD's end: the tail orphan B2 is discarded
/// (classic torn-tail behavior), while B1 — pinned beneath committed data in
/// the shared LSN stream — survives harmlessly and the CRUD is preserved.
#[test]
fn orphan_frames_straddling_committed_crud_truncate_only_the_tail() {
    let mut fixture = make_db_file();
    let orphan_below = checkpoint_page_frame_record(JOURNAL_HEADER_SIZE as u64, 42, 5, 0xAB);
    let crud = crud_record(orphan_below.end_lsn(), 7, 1, ts(50, 0));
    let orphan_above = checkpoint_page_frame_record(crud.end_lsn(), 43, 3, 0xCD);

    write_journal(
        &fixture.db_path,
        &[orphan_below.bytes(), crud.bytes(), orphan_above.bytes()],
    );
    let journal_path = journal_path_for(&fixture.db_path);
    let mut manager =
        JournalManager::open_or_create(&fixture.db_path, &fixture.header, &mut fixture.main_file)
            .expect("recover straddled-orphan journal");
    let parsed = manager.take_parsed_logical_frames();

    assert_eq!(
        parsed.frames,
        vec![(crud.start_lsn(), logical_frame(ts(50, 0), 7))],
        "the committed CrudCommit between the orphans must be replayed"
    );
    assert_eq!(
        manager.write_cursor(),
        crud.end_lsn(),
        "truncation must cut exactly at the kept CrudCommit's end: the tail \
         orphan is discarded, the below-record orphan must not drag the floor lower"
    );
    assert_eq!(
        std::fs::metadata(&journal_path)
            .expect("journal metadata")
            .len(),
        crud.end_lsn(),
        "tail orphan bytes must be physically truncated; kept bytes preserved"
    );
}
