#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

#[path = "crash_harness.rs"]
mod crash_harness;

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, OpenOptions};

#[test]
fn test_durable_logical_frame_exists_before_resident_install_crash_on_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("us012.mqlite");

    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open");
    let db = client.database("db");
    db.create_collection("c").expect("create collection");
    client.checkpoint().expect("checkpoint baseline catalog");

    db.collection::<Document>("c")
        .insert_one(&doc! { "_id": 42i32, "phase": "logical-first" })
        .expect("insert durable logical transaction");
    std::mem::forget(client);

    let chain_commits = crash_harness::scan_chain_commits(&path).expect("scan chain commits");
    let (last_chain_offset, last_commit_ts) = chain_commits
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

    let (reopened, recovery) = crash_harness::reopen_inspect(&path).expect("reopen");
    assert_eq!(
        recovery.recovered_max_commit_ts,
        Some(last_commit_ts),
        "recovery must observe the durable ChainCommit left by the cut"
    );

    let doc = reopened
        .database("db")
        .collection::<Document>("c")
        .find_one(doc! { "_id": 42i32 })
        .expect("find after reopen")
        .expect("logical frame + ChainCommit must recover the committed doc");
    assert_eq!(doc.get_i32("_id").ok(), Some(42));
}
