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

fn assert_internal<T>(result: mqlite::Result<T>) {
    assert!(
        matches!(result, Err(Error::Internal(_))),
        "expected pre-durable internal failure"
    );
}

#[test]
fn test_primary_install_failure_aborts_pre_durable_without_poison() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("us019-fatal.mqlite");

    let client = Client::open_with_options(&path, fullsync_options()).expect("open");
    let db = client.database("db");
    db.create_collection("c").expect("create collection");
    client.checkpoint().expect("checkpoint baseline catalog");

    db.collection::<Document>("c")
        .insert_one(&doc! { "_id": 1i32, "phase": "baseline" })
        .expect("baseline insert");

    let journal_len_before_failure = std::fs::metadata(crash_harness::journal_path(&path))
        .expect("journal metadata before repeated failure")
        .len();
    client.__us019_set_primary_install_failures(1);
    assert_internal(
        db.collection::<Document>("c")
            .insert_one(&doc! { "_id": 2i32, "phase": "aborted" }),
    );
    assert_eq!(
        client.__us019_primary_install_attempts(),
        1,
        "S9 failure must not be retried inside the same commit envelope"
    );
    let journal_len_after_failure = std::fs::metadata(crash_harness::journal_path(&path))
        .expect("journal metadata after repeated failure")
        .len();
    assert_eq!(
        journal_len_after_failure, journal_len_before_failure,
        "pre-durable install failure must not append durable state"
    );
    assert!(db
        .collection::<Document>("c")
        .find_one(doc! { "_id": 1i32 })
        .expect("engine remains readable")
        .is_some());
    assert!(db
        .collection::<Document>("c")
        .find_one(doc! { "_id": 2i32 })
        .expect("aborted write remains readable")
        .is_none());
    db.collection::<Document>("c")
        .insert_one(&doc! { "_id": 3i32, "phase": "not-poisoned" })
        .expect("engine must remain writable after pre-durable abort");
    assert_eq!(
        client.__us019_primary_install_attempts(),
        2,
        "successful follow-up write should use one normal primary install attempt"
    );

    std::mem::forget(client);
    let reopened = Client::open_with_options(&path, fullsync_options()).expect("reopen");
    let db = reopened.database("db");
    let doc = db
        .collection::<Document>("c")
        .find_one(doc! { "_id": 1i32 })
        .expect("find recovered committed doc")
        .expect("first committed doc must recover");
    assert_eq!(doc.get_i32("_id").ok(), Some(1));
    let aborted = db
        .collection::<Document>("c")
        .find_one(doc! { "_id": 2i32 })
        .expect("find aborted doc after recovery");
    assert!(
        aborted.is_none(),
        "pre-durable aborted doc must not recover from the journal"
    );
}
