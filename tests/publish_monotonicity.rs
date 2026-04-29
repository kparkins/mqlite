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

//! Phase 1 US-007 — strict visible_ts monotonicity at every publish
//! site (§6.3).
//!
//! The rule: every `publish_commit` call must supply a `visible_ts`
//! that is strictly greater than the previous `PublishedEpoch.visible_ts`.
//! For txns with primary writes, the allocated `commit_ts` meets that
//! (commit_seq + oracle.commit() are serialized). For metadata-only
//! DDL / bootstrap commits the caller MUST use `oracle.commit()` — not
//! `oracle.now()`, which only peeks at the HLC and can return equal
//! Ts across two sub-ms calls.
//!
//! These tests drive back-to-back metadata-only publishes and assert
//! that every successive `visible_ts` is strictly greater, even when
//! both publishes land inside the same wall-clock millisecond.

use bson::{doc, Document};
use mqlite::Client;

/// Sample `visible_ts` via a post-publish observation: insert one doc
/// and read back its HLC witness via the `__oracle_now` accessor. The
/// oracle is floored above every prior commit, so the witness is a
/// conservative upper bound on the last published visible_ts and is
/// strictly monotonic across successive commits.
///
/// Simpler: each DDL publish advances `oracle.commit()` by at least
/// `(0, +1)`. Two back-to-back DDL publishes therefore produce
/// `visible_ts` pairs (t1, t2) with t2 > t1 strictly.
fn oracle_now(client: &Client) -> (u64, u32) {
    client.__oracle_now()
}

#[test]
fn two_metadata_only_ddl_commits_produce_strictly_monotonic_visible_ts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pm.mqlite");
    let client = Client::open(&path).unwrap();

    // Prime so the oracle has advanced past (0, 0).
    client
        .database("db")
        .collection::<Document>("prime")
        .insert_one(&doc! { "_id": 0i32 })
        .unwrap();
    let t_prime = oracle_now(&client);

    // Two back-to-back metadata-only DDL commits. `oracle.commit()`
    // advances the HLC on each call; if the publish path used
    // `oracle.now()` and both calls landed in the same millisecond,
    // the published visible_ts would be equal on both and the
    // debug_assert in publish_commit would fire (or a reader could
    // observe two distinct catalogs at the same visible_ts).
    client.database("db").create_collection("a").unwrap();
    let t1 = oracle_now(&client);
    client.database("db").create_collection("b").unwrap();
    let t2 = oracle_now(&client);

    // Strict monotonicity across each pair.
    assert!(
        t_prime < t1,
        "first DDL publish must advance oracle past prime: prime={:?} t1={:?}",
        t_prime,
        t1
    );
    assert!(
        t1 < t2,
        "second DDL publish must be strictly > first: t1={:?} t2={:?}",
        t1,
        t2
    );
}

#[test]
fn two_drop_index_ddls_produce_strictly_monotonic_visible_ts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pm_di.mqlite");
    let client = Client::open(&path).unwrap();
    let col = client.database("db").collection::<Document>("c");
    col.insert_one(&doc! { "_id": 0i32, "x": 1, "y": 1 })
        .unwrap();
    let idx_x = col
        .create_index(mqlite::IndexModel::builder().keys(doc! { "x": 1 }).build())
        .unwrap();
    let idx_y = col
        .create_index(mqlite::IndexModel::builder().keys(doc! { "y": 1 }).build())
        .unwrap();

    let t0 = oracle_now(&client);
    col.drop_index(&idx_x).unwrap();
    let t1 = oracle_now(&client);
    col.drop_index(&idx_y).unwrap();
    let t2 = oracle_now(&client);

    assert!(
        t0 < t1,
        "first drop_index must advance oracle: {:?} < {:?}",
        t0,
        t1
    );
    assert!(
        t1 < t2,
        "second drop_index must advance oracle: {:?} < {:?}",
        t1,
        t2
    );
}

#[test]
fn bootstrap_then_ddl_are_strictly_monotonic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pm_boot.mqlite");
    let client = Client::open(&path).unwrap();

    // First observable: an insert into a fresh namespace triggers
    // `bootstrap_namespace`, which is a metadata-only publish under
    // Phase 1 §6.3's `oracle.commit()` rule. The subsequent commit
    // publish uses the primary-write `commit_ts`.
    client
        .database("db")
        .collection::<Document>("c")
        .insert_one(&doc! { "_id": 1i32 })
        .unwrap();
    let t1 = oracle_now(&client);
    client.database("db").create_collection("d").unwrap();
    let t2 = oracle_now(&client);

    assert!(
        t1 < t2,
        "DDL after bootstrap must strictly advance visible_ts: t1={:?} t2={:?}",
        t1,
        t2
    );
}
