use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::client::Client;
use crate::error::Error;
use crate::journal::log_file::{LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION};
use crate::journal::{journal_path_for, JournalManager};
use crate::mvcc::Ts;
use crate::options::OpenOptions as DbOpenOptions;
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};

const FRONTIER_TS: Ts = Ts {
    physical_ms: 10_000,
    logical: 0,
};
const PRE_FRONTIER_TS: Ts = Ts {
    physical_ms: 9_999,
    logical: 9,
};
const CLEAN_TS: Ts = Ts {
    physical_ms: 11_000,
    logical: 0,
};
const ORPHAN_TS: Ts = Ts {
    physical_ms: 12_000,
    logical: 0,
};
const MIN_SYNTHETIC_COMMIT_TS_OFFSET_MS: u64 = 1;

fn open_main_file(db_path: &Path) -> File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .open(db_path)
        .expect("open main file")
}

fn read_main_header(db_path: &Path) -> FileHeader {
    let mut file = open_main_file(db_path);
    let mut bytes = [0u8; HEADER_PAGE_SIZE];
    file.seek(SeekFrom::Start(0)).expect("seek header");
    file.read_exact(&mut bytes).expect("read header");
    FileHeader::from_bytes(&bytes).expect("decode header")
}

fn write_main_header(db_path: &Path, header: &FileHeader) {
    let mut file = open_main_file(db_path);
    file.seek(SeekFrom::Start(0)).expect("seek header");
    file.write_all(&header.to_bytes()).expect("write header");
    file.sync_data().expect("sync header");
}

fn seed_database(db_path: &Path) {
    let client =
        Client::open_with_options(db_path, DbOpenOptions::new()).expect("open setup client");
    client.close().expect("checkpoint setup database");

    let mut recovered_header = read_main_header(db_path);
    recovered_header.last_checkpoint_ts = FRONTIER_TS;
    write_main_header(db_path, &recovered_header);

    match fs::remove_file(journal_path_for(db_path)) {
        Ok(()) => {}
        Err(error) if error.kind() == ErrorKind::NotFound => {}
        Err(error) => panic!("remove phase8 seed journal: {error}"),
    }
}

fn synthetic_uncheckpointed_ts(header: &FileHeader, requested: Ts) -> Ts {
    if requested > header.last_checkpoint_ts {
        return requested;
    }
    Ts {
        physical_ms: header
            .last_checkpoint_ts
            .physical_ms
            .saturating_add(requested.physical_ms.max(MIN_SYNTHETIC_COMMIT_TS_OFFSET_MS)),
        logical: requested.logical,
    }
}

fn append_orphan_chain_commit_at(db_path: &Path, commit_ts: Ts) -> (u64, Ts) {
    let mut main_file = open_main_file(db_path);
    let header = read_main_header(db_path);
    let mut journal =
        JournalManager::open_or_create(db_path, &header, &mut main_file).expect("open journal");
    let offset = journal
        .append_chain_commit(commit_ts, vec![], vec![])
        .expect("append orphan chain commit");
    journal.sync_journal().expect("sync journal");
    (offset, commit_ts)
}

fn append_orphan_chain_commit(db_path: &Path, commit_ts: Ts) -> (u64, Ts) {
    let header = read_main_header(db_path);
    append_orphan_chain_commit_at(db_path, synthetic_uncheckpointed_ts(&header, commit_ts))
}

fn append_clean_logical_then_orphan(db_path: &Path) -> (u64, Ts) {
    let mut main_file = open_main_file(db_path);
    let header = read_main_header(db_path);
    let mut journal =
        JournalManager::open_or_create(db_path, &header, &mut main_file).expect("open journal");
    let (salt1, salt2) = journal.salts();
    let clean_ts = synthetic_uncheckpointed_ts(&header, CLEAN_TS);
    let orphan_ts = synthetic_uncheckpointed_ts(&header, ORPHAN_TS);
    let frame = LogicalTxnFrame {
        salt1,
        salt2,
        commit_ts: clean_ts,
        diagnostic_txn_id: 1,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![],
    };

    journal.append_logical_txn(frame).expect("append logical");
    journal
        .append_chain_commit(clean_ts, vec![], vec![])
        .expect("append matching chain commit");
    let orphan_offset = journal
        .append_chain_commit(orphan_ts, vec![], vec![])
        .expect("append orphan chain commit");
    journal.sync_journal().expect("sync journal");
    (orphan_offset, orphan_ts)
}

fn expect_recovery_error(db_path: &Path) -> String {
    match Client::open_with_options(db_path, DbOpenOptions::new()) {
        Ok(_) => panic!("open must fail with Error::Recovery"),
        Err(Error::Recovery { detail }) => detail,
        Err(other) => panic!("expected Error::Recovery, got {other:?}"),
    }
}

#[test]
fn test_unpaired_chain_commit_is_hard_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("phase7-us009-hard-error.mqlite");
    seed_database(&db_path);

    let (offset, orphan_ts) = append_orphan_chain_commit(&db_path, ORPHAN_TS);
    let mut main_file = open_main_file(&db_path);
    let header = read_main_header(&db_path);
    let mut journal =
        JournalManager::open_or_create(&db_path, &header, &mut main_file).expect("parse journal");
    let parsed = journal.take_parsed_logical_frames();
    assert_eq!(parsed.case_c_candidates.len(), 1);
    assert_eq!(parsed.case_c_candidates[0].commit_ts, orphan_ts);
    assert_eq!(parsed.case_c_candidates[0].chain_commit_offset, offset);
    drop(journal);
    drop(main_file);

    let detail = expect_recovery_error(&db_path);
    assert!(
        detail.contains("ChainCommit without matching LogicalTxnFrame"),
        "expected case-c recovery detail, got {detail}"
    );
}

#[test]
fn test_unpaired_chain_commit_does_not_trip_for_pre_frontier_chain_commits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("phase7-us009-pre-frontier.mqlite");
    seed_database(&db_path);
    append_orphan_chain_commit_at(&db_path, PRE_FRONTIER_TS);

    let client = Client::open_with_options(&db_path, DbOpenOptions::new())
        .expect("pre-frontier case-c candidate must be checkpoint-covered");
    drop(client);
}

#[test]
fn test_unpaired_chain_commit_error_detail_includes_chain_commit_offset() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("phase7-us009-offset-detail.mqlite");
    seed_database(&db_path);

    let (offset, _) = append_orphan_chain_commit(&db_path, ORPHAN_TS);
    let detail = expect_recovery_error(&db_path);
    let expected_prefix = format!("chain_commit_offset={offset}:");
    assert!(
        detail.starts_with(&expected_prefix),
        "expected detail prefix {expected_prefix:?}, got {detail:?}"
    );
}

#[test]
fn test_unpaired_chain_commit_refuses_open_when_only_orphan_chain_commit_differs() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("phase7-us009-only-orphan-differs.mqlite");
    seed_database(&db_path);

    let (orphan_offset, _) = append_clean_logical_then_orphan(&db_path);
    let main_before = fs::read(&db_path).expect("read main before failed open");
    let journal_path = journal_path_for(&db_path);
    let journal_before = fs::read(&journal_path).expect("read journal before failed open");

    let detail = expect_recovery_error(&db_path);
    assert!(
        detail.starts_with(&format!("chain_commit_offset={orphan_offset}:")),
        "open must fail on the orphan ChainCommit, got {detail:?}"
    );
    assert_eq!(
        fs::read(&db_path).expect("read main after failed open"),
        main_before,
        "case-c validation must run before logical replay mutates the main file"
    );
    assert_eq!(
        fs::read(&journal_path).expect("read journal after failed open"),
        journal_before,
        "case-c validation must not repair or truncate the journal"
    );
}

#[test]
fn test_unpaired_chain_commit_read_only_open_refuses_orphan_chain_commit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("phase7-us012-read-only-case-c.mqlite");
    seed_database(&db_path);

    let (orphan_offset, _) = append_orphan_chain_commit(&db_path, ORPHAN_TS);
    let detail = match Client::open_with_options(&db_path, DbOpenOptions::new().read_only(true)) {
        Ok(_) => panic!("read-only open must fail with Error::Recovery"),
        Err(Error::Recovery { detail }) => detail,
        Err(other) => panic!("expected Error::Recovery, got {other:?}"),
    };

    assert!(
        detail.starts_with(&format!("chain_commit_offset={orphan_offset}:")),
        "read-only open must fail on the orphan ChainCommit, got {detail:?}"
    );
}
