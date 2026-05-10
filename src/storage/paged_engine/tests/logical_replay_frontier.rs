use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::client::Client;
use crate::options::OpenOptions as DbOpenOptions;
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};

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
