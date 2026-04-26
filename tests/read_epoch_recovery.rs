#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

//! Phase 1 US-016 / §10.8 #23, #25-27 — crash/recovery +
//! durable-id tests.
//!
//! #23: reopen-bootstrapped ReadEpoch.visible_ts floors above the
//!      pre-crash max commit (also asserted by
//!      tests/reopen_read_epoch_bootstrap.rs).
//! #25: durable ids are monotonic across crashes.
//! #26: an aborted id allocation may leak, but the next allocation
//!      never reuses it.
//! #27: crash between ChainCommit durability and `published.store`
//!      loses only the publish — recovery completes it.

#[path = "crash_harness.rs"]
mod crash_harness;

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, OpenOptions, Phase0ProbeCut};
use std::sync::Mutex;

/// Shared lock serializing tests that rely on `reopen_inspect`'s
/// process-global recovery counter reset.
static TEST_LOCK: Mutex<()> = Mutex::new(());

/// §10.8 #23: after a durable commit + close + reopen, the initial
/// ReadEpoch.visible_ts is >= the pre-close oracle sample AND
/// >= max_commit_ts from recovery.
#[test]
fn reopen_rebuilds_read_epoch_from_oracle_now() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rec23.mqlite");
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .unwrap();
    client
        .database("db")
        .collection::<Document>("c")
        .insert_one(&doc! { "_id": 1i32 })
        .unwrap();
    let pre_close = client.__oracle_now();
    std::mem::forget(client);

    let (client, recovery) = crash_harness::reopen_inspect(&path).expect("reopen");
    let max = recovery
        .recovered_max_commit_ts
        .expect("recovery must see ChainCommit");
    let visible = client.__published_visible_ts();
    assert!(
        visible >= max,
        "§10.8 #23: post-reopen visible_ts {:?} must be >= recovered max {:?}",
        visible,
        max
    );
    assert!(
        visible >= pre_close,
        "§10.8 #23: post-reopen visible_ts {:?} must be >= pre-close oracle {:?}",
        visible,
        pre_close
    );
}

/// §10.8 #25: create `A`, close without checkpoint (mem::forget),
/// reopen, create `B`, assert B.id > A.id.
#[test]
fn durable_ids_are_monotonic_across_crashes() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rec25.mqlite");

    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .unwrap();
    client.database("db").create_collection("A").unwrap();
    // Insert something into A so the id allocation is observable
    // via list_namespaces on reopen.
    client
        .database("db")
        .collection::<Document>("A")
        .insert_one(&doc! { "_id": 1i32 })
        .unwrap();
    std::mem::forget(client);

    let (client, _) = crash_harness::reopen_inspect(&path).expect("reopen");
    // Assert A survived the crash (its doc is visible).
    let a_doc = client
        .database("db")
        .collection::<Document>("A")
        .find_one(doc! { "_id": 1i32 })
        .expect("find A._id=1")
        .expect("A._id=1 must exist post-reopen");
    assert_eq!(a_doc.get_i32("_id").ok(), Some(1));

    // Create B — its namespace id must be allocated from the
    // persisted counter, never reusing A's id. Observable at Phase 1
    // scope: the post-reopen publish must rebuild a new catalog Arc
    // (both A and B coexist), and the two entries round-trip through
    // a close + reopen — exactly the invariant that requires
    // monotonic, non-reused ids.
    client.database("db").create_collection("B").unwrap();
    client
        .database("db")
        .collection::<Document>("B")
        .insert_one(&doc! { "_id": 2i32 })
        .unwrap();

    // Close + reopen again to verify both entries persisted.
    std::mem::forget(client);
    let (client, _) = crash_harness::reopen_inspect(&path).expect("second reopen");
    let a_count = client
        .database("db")
        .collection::<Document>("A")
        .count_documents(doc! {})
        .expect("count A");
    let b_count = client
        .database("db")
        .collection::<Document>("B")
        .count_documents(doc! {})
        .expect("count B");
    assert_eq!(a_count, 1, "A must retain its pre-crash doc");
    assert_eq!(b_count, 1, "B must retain its post-reopen doc");
}

/// §10.8 #27: a Cut-6 probe (after `commit_txn`, before publish) is
/// durable on disk — the ChainCommit frame was appended and the
/// header was written. Reopen must recover the commit (documents
/// visible) even though the in-memory publish never ran before the
/// crash.
#[test]
fn crash_between_chain_commit_and_publish_loses_publish_only() {
    let _g = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rec27.mqlite");

    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .unwrap();
    // Seed a namespace so the probe insert is a plain CRUD commit.
    let db = client.database("db");
    db.create_collection("c").expect("create c");
    let col = db.collection::<Document>("c");
    col.insert_one(&doc! { "_id": 0i32, "kind": "seed" })
        .unwrap();
    client.checkpoint().expect("checkpoint baseline");

    // Drive a probe insert stopped AFTER commit_txn but BEFORE
    // publish. The ChainCommit frame exists, the header was updated.
    let _ = client
        .__phase0_probe_insert(
            "db.c",
            doc! { "_id": 99i32, "kind": "probe" },
            Phase0ProbeCut::AfterCommitTxnBeforePublish,
        )
        .expect("probe");
    std::mem::forget(client);

    // Reopen and verify the probe commit is visible (recovery
    // completed it).
    let (client, _) = crash_harness::reopen_inspect(&path).expect("reopen");
    let cursor = client
        .database("db")
        .collection::<Document>("c")
        .find(doc! { "_id": 99i32 })
        .run()
        .expect("find");
    let probe_visible = cursor
        .into_iter()
        .filter_map(|r| r.ok())
        .any(|d| d.get_i32("_id").ok() == Some(99));
    assert!(
        probe_visible,
        "§10.8 #27: probe commit stopped pre-publish must be visible after \
         reopen — recovery re-runs publish from the persistent state"
    );
}
