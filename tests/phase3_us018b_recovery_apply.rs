#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

#[path = "crash_harness.rs"]
mod crash_harness;

use std::path::PathBuf;

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, OpenOptions};

fn prepare_replay_fixture() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("us018b.mqlite");

    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open");
    let db = client.database("db");
    db.create_collection("c").expect("create collection");
    client.checkpoint().expect("checkpoint baseline catalog");

    db.collection::<Document>("c")
        .insert_one(&doc! { "_id": 42i32, "phase": "replay" })
        .expect("insert replay fixture");
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

    (dir, path)
}

#[test]
fn test_reopen_installs_published_epoch_once_after_replay() {
    let (_fresh_dir, fresh_path) = {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("empty.mqlite");
        (dir, path)
    };
    let fresh = Client::open_with_options(&fresh_path, OpenOptions::new()).expect("fresh open");
    assert_eq!(
        fresh.__recovery_open_published_store_count(),
        1,
        "empty-journal open must perform exactly one post-open epoch store"
    );
    assert_eq!(
        fresh.__published_sequencer_frontier(),
        (0, 0),
        "empty journal publishes the Ts::MIN sequencer frontier"
    );

    let (_dir, path) = prepare_replay_fixture();
    let (reopened, recovery) = crash_harness::reopen_inspect(&path).expect("reopen");
    assert_eq!(
        reopened.__recovery_open_published_store_count(),
        1,
        "replay open must perform exactly one post-open epoch store"
    );
    assert_eq!(
        reopened.__published_sequencer_frontier(),
        recovery
            .recovered_max_commit_ts
            .expect("recovery must see ChainCommit"),
        "sequencer frontier must match recovered max commit ts"
    );

    let doc = reopened
        .database("db")
        .collection::<Document>("c")
        .find_one(doc! { "_id": 42i32 })
        .expect("find after replay")
        .expect("replayed doc should be visible");
    assert_eq!(doc.get_i32("_id").ok(), Some(42));
}

#[test]
fn test_reopen_published_epoch_is_coherent_after_replay() {
    let (_dir, path) = prepare_replay_fixture();
    let (reopened, recovery) = crash_harness::reopen_inspect(&path).expect("reopen");
    let recovered = recovery
        .recovered_max_commit_ts
        .expect("recovery must see ChainCommit");

    assert_eq!(reopened.__published_visible_ts(), recovered);
    assert_eq!(reopened.__published_sequencer_frontier(), recovered);
    assert_eq!(
        reopened.__published_catalog_gen(),
        1,
        "catalog generation is process-local and resets to 1 on open"
    );
}
