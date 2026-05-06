#![allow(non_snake_case)]

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use bson::Bson;
use serial_test::serial;

use crate::client::Client;
use crate::journal::log_file::{
    LogicalOp, LogicalOpKind, LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION,
};
use crate::journal::JournalManager;
use crate::keys::encode_key;
use crate::mvcc::metrics::{
    logical_txn_pass2_unresolved_ops_snapshot, reset_logical_txn_pass2_unresolved_ops,
};
use crate::mvcc::Ts;
use crate::options::OpenOptions as DbOpenOptions;
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};

const ABSENT_NS_ID: i64 = 99_999;
const FRONTIER_TS: Ts = Ts {
    physical_ms: 10_000,
    logical: 0,
};
const PRE_FRONTIER_TS: Ts = Ts {
    physical_ms: 9_999,
    logical: 9,
};

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
}

fn append_durable_logical_insert(db_path: &Path, ns_id: i64, commit_ts: Ts) {
    let mut main_file = open_main_file(db_path);
    let header = read_main_header(db_path);
    let mut journal =
        JournalManager::open_or_create(db_path, &header, &mut main_file).expect("open journal");
    let (salt1, salt2) = journal.salts();
    let frame = LogicalTxnFrame {
        salt1,
        salt2,
        commit_ts,
        diagnostic_txn_id: 42,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![LogicalOp {
            op_ordinal: 0,
            kind: LogicalOpKind::PrimaryInsert {
                ns_id,
                key: encode_key(&Bson::Int32(1)),
                value: b"stale".to_vec(),
                overflow: None,
            },
        }],
    };

    journal.append_logical_txn(frame).expect("append logical");
    journal
        .append_chain_commit(commit_ts, vec![], vec![])
        .expect("append chain commit");
    journal.sync_journal().expect("sync journal");
}

#[test]
#[serial(logical_txn_pass2_metrics)]
fn F_test_recovered_page0_last_checkpoint_ts_is_logical_replay_frontier() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("phase7-us008-frontier.mqlite");
    seed_database(&db_path);

    let mut recovered_header = read_main_header(&db_path);
    recovered_header.last_checkpoint_ts = FRONTIER_TS;
    write_main_header(&db_path, &recovered_header);
    append_durable_logical_insert(&db_path, ABSENT_NS_ID, PRE_FRONTIER_TS);

    reset_logical_txn_pass2_unresolved_ops();
    let before = logical_txn_pass2_unresolved_ops_snapshot();
    let client = Client::open_with_options(&db_path, DbOpenOptions::new())
        .expect("open with recovered frontier");
    assert_eq!(
        logical_txn_pass2_unresolved_ops_snapshot(),
        before,
        "logical frames at or below recovered last_checkpoint_ts must be \
         discarded before Pass 2"
    );
    assert_eq!(read_main_header(&db_path).last_checkpoint_ts, FRONTIER_TS);

    drop(client);
}

#[test]
fn test_recovery_replay_does_not_call_history_spill() {
    let recovery_apply = include_str!("../recovery_apply.rs");
    assert!(
        !recovery_apply.contains("spill_primary(")
            && !recovery_apply.contains("spill_sec_index(")
            && !recovery_apply.contains("commit_spill_txn"),
        "logical recovery replay must install committed resident deltas \
         directly, not route through history-spill APIs"
    );
}

#[test]
fn test_fresh_db_creates_history_root_after_recovered_header_construction() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("phase7-us008-fresh.mqlite");
    let client =
        Client::open_with_options(&db_path, DbOpenOptions::new()).expect("open fresh database");

    assert_eq!(
        client.__recovery_open_published_store_count(),
        1,
        "fresh open must publish exactly once after recovered-header construction"
    );
    client.close().expect("checkpoint fresh database");

    let header = read_main_header(&db_path);
    assert_ne!(
        header.history_store_root_page, 0,
        "fresh open must create and persist the history root through the \
         recovered-header construction path"
    );
    assert_eq!(header.history_store_root_level, 0);
}
