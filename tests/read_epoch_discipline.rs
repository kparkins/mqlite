#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

//! Phase 1 US-015 / §10.8 #15-20, #24 — single-load + ordering +
//! strict-monotonicity tests.
//!
//! Single-load discipline is also covered at the unit-test level in
//! `src/storage/paged_engine/tests.rs` (those tests can reach the
//! pub(crate) `ReadOpScope` directly). This file adds the integration-
//! test-level coverage asked for by §10.8 #17, #19, #20 via
//! observables that survive the public API boundary.

//! NOTE: The §10.8 #19 deterministic rendezvous test
//! (`test_publish_happens_after_commit_txn`) lives as a UNIT test in
//! `src/storage/paged_engine/tests.rs`, because the publish-pause
//! hook it uses is `#[cfg(test)]`-gated (§11 #10 guardrail — no new
//! `Arc` / `Mutex` in production builds).

use bson::{doc, Document};
use mqlite::mvcc::metrics::{read_epoch_publish_count_snapshot, reset_read_epoch_publish_count};
use mqlite::Client;
use std::sync::Mutex;

static COUNTER_SERIAL: Mutex<()> = Mutex::new(());

fn new_client(name: &str) -> (Client, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(format!("{}.mqlite", name));
    let client = Client::open(&path).unwrap();
    (client, dir)
}

/// §10.8 #17: an update operation plans against the catalog AND
/// installs the new version using observations from ONE ReadEpoch
/// load. The public API goes through `run_write` (write path), which
/// consumes the catalog via `metadata.read()` — it does NOT
/// re-load the published epoch during the write. Observable: a
/// root-neutral update consumes exactly one CRUD commit publish.
#[test]
fn update_publishes_exactly_once_per_commit() {
    let _g = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _d) = new_client("disc_update");
    let col = client.database("db").collection::<Document>("c");
    col.insert_one(&doc! { "_id": 0i32, "v": 0 }).unwrap();

    reset_read_epoch_publish_count();
    col.update_one(doc! { "_id": 0i32 }, doc! { "$set": { "v": 1 } })
        .run()
        .unwrap();
    assert_eq!(
        read_epoch_publish_count_snapshot(),
        1,
        "§10.8 #17: an update commit runs publish_commit exactly once"
    );
}

/// §10.8 #20: interleaved DDL and CRUD on distinct namespaces —
/// every successive visible_ts must be >= previous (commit_seq + the
/// strict-monotonic oracle.commit() path guarantee this).
#[test]
fn visible_ts_monotonic_under_interleaved_ddl_and_crud() {
    let _g = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _d) = new_client("disc_mix");

    // Interleave 6 commits across DDL (create_collection) and CRUD
    // (insert into a seeded namespace). All must produce strictly
    // greater visible_ts.
    client.database("db").create_collection("primed").unwrap();
    let mut prev = client.__published_visible_ts();
    let col = client.database("db").collection::<Document>("primed");
    for i in 0..3 {
        // DDL
        client
            .database("db")
            .create_collection(&format!("x{}", i))
            .unwrap();
        let after_ddl = client.__published_visible_ts();
        assert!(
            after_ddl > prev,
            "§10.8 #20: DDL {} must advance visible_ts; prev={:?} after={:?}",
            i,
            prev,
            after_ddl
        );
        prev = after_ddl;

        // CRUD (root-neutral insert, reuses Arc but still advances ts)
        col.insert_one(&doc! { "_id": i }).unwrap();
        let after_crud = client.__published_visible_ts();
        assert!(
            after_crud > prev,
            "§10.8 #20: CRUD {} must advance visible_ts; prev={:?} after={:?}",
            i,
            prev,
            after_crud
        );
        prev = after_crud;
    }
}

/// §10.8 #24: two back-to-back metadata-only DDL commits produce
/// strictly greater visible_ts. Guards against the oracle.now()
/// regression — if `oracle.now()` were used at publish sites, two
/// sub-ms DDLs could land equal visible_ts.
#[test]
fn two_metadata_only_ddls_produce_strict_inequality_even_in_same_ms() {
    let _g = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let (client, _d) = new_client("disc_ms");
    client.database("db").create_collection("a").unwrap();
    let t1 = client.__published_visible_ts();
    client.database("db").create_collection("b").unwrap();
    let t2 = client.__published_visible_ts();
    assert!(
        t2 > t1,
        "§10.8 #24: two metadata-only DDLs must produce strict > visible_ts; \
         t1={:?} t2={:?}",
        t1,
        t2
    );
}

// §10.8 #19 rendezvous test is a UNIT test in
// src/storage/paged_engine/tests.rs — see the module-level note above.
