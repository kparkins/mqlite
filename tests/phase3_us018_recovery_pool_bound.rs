#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]
#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

#[path = "crash_harness.rs"]
mod crash_harness;

use std::path::PathBuf;

use bson::Document;
use mqlite::{Client, DurabilityMode, Error, IndexModel, OpenOptions};

const SECONDARY_INDEX_COUNT: usize = 3;
const TARGET_LEAF_FRAME_COUNT: usize = SECONDARY_INDEX_COUNT + 1;
const LEAF_FRAME_BYTES: usize = 32 * 1024;

fn field_name(index: usize) -> String {
    format!("f{index}")
}

fn single_field_index(field: &str) -> IndexModel {
    let mut keys = Document::new();
    keys.insert(field, 1i32);
    IndexModel::builder().keys(keys).build()
}

fn indexed_doc() -> Document {
    let mut doc = Document::new();
    doc.insert("_id", 1i32);
    for index in 0..SECONDARY_INDEX_COUNT {
        doc.insert(field_name(index), index as i32);
    }
    doc
}

fn prepare_replay_fixture() -> (tempfile::TempDir, PathBuf, Vec<u8>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("us018.mqlite");

    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open");
    let db = client.database("db");
    db.create_collection("c").expect("create collection");
    let coll = db.collection::<Document>("c");

    for index in 0..SECONDARY_INDEX_COUNT {
        coll.create_index(single_field_index(&field_name(index)))
            .expect("create secondary index");
    }
    client.checkpoint().expect("checkpoint baseline catalog");

    coll.insert_one(&indexed_doc())
        .expect("insert logical replay fixture");
    std::mem::forget(client);

    let chain_commits = crash_harness::scan_chain_commits(&path).expect("scan chain commits");
    let (last_chain_offset, _last_commit_ts) = chain_commits
        .last()
        .copied()
        .expect("insert must append a ChainCommit");
    let legacy_frames =
        crash_harness::scan_legacy_commit_frames(&path).expect("scan legacy commit frames");
    let cut = legacy_frames
        .iter()
        .find_map(|(start, _end)| (*start > last_chain_offset).then_some(*start))
        .expect("insert ChainCommit must be followed by a legacy commit frame");
    crash_harness::truncate_journal_to_offset(&path, cut).expect("truncate after ChainCommit");

    let main_file_before = std::fs::read(&path).expect("read main file before failed reopen");
    (dir, path, main_file_before)
}

#[test]
fn test_recovery_of_large_delta_set_fits_in_configured_pool() {
    let (_ok_dir, ok_path, _ok_main_before) = prepare_replay_fixture();
    let sufficient_pool = TARGET_LEAF_FRAME_COUNT * LEAF_FRAME_BYTES * 2;
    let reopened = Client::open_with_options(
        &ok_path,
        OpenOptions::new().buffer_pool_size(sufficient_pool),
    )
    .expect("reopen with sufficient pool");
    let doc = reopened
        .database("db")
        .collection::<Document>("c")
        .find_one(bson::doc! { "_id": 1i32 })
        .expect("find recovered doc")
        .expect("recovered doc should be visible");
    assert_eq!(doc.get_i32("_id").ok(), Some(1));

    let (_err_dir, err_path, err_main_before) = prepare_replay_fixture();
    let insufficient_pool = TARGET_LEAF_FRAME_COUNT * LEAF_FRAME_BYTES - 1;
    let err = match Client::open_with_options(
        &err_path,
        OpenOptions::new().buffer_pool_size(insufficient_pool),
    ) {
        Ok(_) => panic!("reopen with insufficient pool must fail"),
        Err(err) => err,
    };
    assert!(matches!(err, Error::RecoveryPoolExhausted));
    assert!(
        err.to_string()
            .contains("increase max_pool_bytes or perform a forced reconcile on the previous open"),
        "operator guidance must be present in Display output: {err}"
    );
    let err_main_after = std::fs::read(&err_path).expect("read main file after failed reopen");
    assert_eq!(
        err_main_after, err_main_before,
        "failed reopen must leave the main file byte-identical"
    );
}
