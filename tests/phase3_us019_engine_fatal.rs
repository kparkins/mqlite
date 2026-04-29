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

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, Error, OpenOptions};

fn fullsync_options() -> OpenOptions {
    OpenOptions::new().durability(DurabilityMode::FullSync)
}

fn assert_engine_fatal<T>(result: mqlite::Result<T>) {
    assert!(
        matches!(result, Err(Error::EngineFatal)),
        "expected EngineFatal"
    );
}

#[test]
fn test_post_durable_install_failure_poisons_engine() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("us019-fatal.mqlite");

    let client = Client::open_with_options(&path, fullsync_options()).expect("open");
    let db = client.database("db");
    db.create_collection("c").expect("create collection");
    client.checkpoint().expect("checkpoint baseline catalog");

    client.__us019_set_primary_install_failures(1);
    db.collection::<Document>("c")
        .insert_one(&doc! { "_id": 1i32, "phase": "retry" })
        .expect("single injected S9 failure should retry and commit");
    assert_eq!(
        client.__us019_primary_install_attempts(),
        2,
        "S9 failure must be retried exactly once"
    );

    client.__us019_set_primary_install_failures(2);
    assert_engine_fatal(
        db.collection::<Document>("c")
            .insert_one(&doc! { "_id": 2i32, "phase": "fatal" }),
    );
    assert_eq!(
        client.__us019_primary_install_attempts(),
        2,
        "S9 repeated failure must stop after the retry"
    );

    let journal_len_after_fatal = std::fs::metadata(crash_harness::journal_path(&path))
        .expect("journal metadata after fatal")
        .len();
    assert_engine_fatal(
        db.collection::<Document>("c")
            .find_one(doc! { "_id": 1i32 }),
    );
    assert_engine_fatal(
        db.collection::<Document>("c")
            .insert_one(&doc! { "_id": 3i32, "phase": "poisoned" }),
    );
    let journal_len_after_poisoned_ops = std::fs::metadata(crash_harness::journal_path(&path))
        .expect("journal metadata after poisoned ops")
        .len();
    assert_eq!(
        journal_len_after_poisoned_ops, journal_len_after_fatal,
        "poisoned read/write attempts must not append durable state"
    );
    assert_eq!(
        client.__us019_primary_install_attempts(),
        2,
        "poisoned write must return before another resident install attempt"
    );

    std::mem::forget(client);
    let reopened = Client::open_with_options(&path, fullsync_options()).expect("reopen");
    let doc = reopened
        .database("db")
        .collection::<Document>("c")
        .find_one(doc! { "_id": 2i32 })
        .expect("find recovered doc")
        .expect("logical replay must recover the post-S7 committed doc");
    assert_eq!(doc.get_i32("_id").ok(), Some(2));
}

#[test]
fn test_class_c_chain_commit_durable_legacy_absent_recovers_crud() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("us019-class-c.mqlite");

    let client = Client::open_with_options(&path, fullsync_options()).expect("open");
    let db = client.database("db");
    db.create_collection("c").expect("create collection");
    client.checkpoint().expect("checkpoint baseline catalog");

    db.collection::<Document>("c")
        .insert_one(&doc! { "_id": 49i32, "phase": "class-c" })
        .expect("insert class-C fixture");
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
    let legacy_after_cut =
        crash_harness::scan_legacy_commit_frames(&path).expect("scan post-cut legacy frames");
    assert!(
        legacy_after_cut
            .iter()
            .all(|(start, _end)| *start <= last_chain_offset),
        "target transaction's legacy commit frame must be absent before reopen"
    );

    let (reopened, recovery) = crash_harness::reopen_inspect(&path).expect("reopen");
    assert_eq!(
        recovery.recovered_max_commit_ts,
        Some(last_commit_ts),
        "recovery must observe the durable ChainCommit"
    );
    let doc = reopened
        .database("db")
        .collection::<Document>("c")
        .find_one(doc! { "_id": 49i32 })
        .expect("find after class-C recovery")
        .expect("class-C CRUD transaction must recover from logical replay");
    assert_eq!(doc.get_i32("_id").ok(), Some(49));
}
