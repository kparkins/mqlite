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

//! Phase 1 US-010 / §10.6 — open-time `PublishedEpoch` bootstrap.
//!
//! The rule: at `SharedState::new` the engine constructs the initial
//! `PublishedEpoch` by building the PublishedCatalog directly and pairing
//! it with `oracle.now()` (post-floor). On reopen the oracle has
//! already been floored above `max_commit_ts.successor()` by journal
//! recovery, so the post-reopen `PublishedEpoch.visible_ts` must be
//! `>= max_commit_ts.successor()`. On a fresh database with no prior
//! commits the initial `visible_ts` is `Ts { 0, 0 }`.

use bson::{doc, Document};
use mqlite::Client;

mod crash_harness;

/// §10.6: on reopen the initial PublishedEpoch.visible_ts is floored above
/// the pre-close max commit ts.
#[test]
fn reopen_initial_read_epoch_visible_ts_is_floored_above_max_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reopen_boot.mqlite");

    // One durable commit, then abandon the client without running
    // Drop so the durable commit record survives in the journal.
    let client = Client::open(&path).unwrap();
    client
        .database("db")
        .collection::<Document>("c")
        .insert_one(&doc! { "_id": 1i32 })
        .unwrap();
    let pre_close_oracle = client.__oracle_now();
    std::mem::forget(client);

    // Reopen and inspect via the crash harness (which asserts the
    // journal was actually present). `recovered_max_commit_ts` is the
    // post-floor witness.
    let (client, recovery) = crash_harness::reopen_inspect(&path).expect("reopen");
    let max = recovery
        .recovered_max_commit_ts
        .expect("recovery must find at least one ChainCommit frame");

    let visible = client.__published_visible_ts();

    // visible_ts should be >= oracle.commit() floor, which is at least
    // max_commit_ts.successor() per §10.6.
    assert!(
        visible >= max,
        "post-reopen visible_ts {:?} must be >= max_commit_ts {:?}",
        visible,
        max
    );
    // Also sanity-check we didn't lose state: visible must be at least
    // as large as what we observed pre-close.
    assert!(
        visible >= pre_close_oracle,
        "post-reopen visible_ts {:?} must be >= pre-close oracle {:?}",
        visible,
        pre_close_oracle
    );
}

/// §10.6: on a fresh database with zero prior commits, the initial
/// `PublishedEpoch.visible_ts` is `Ts { physical_ms: 0, logical: 0 }` and
/// the very first write's commit advances strictly above that.
#[test]
fn fresh_db_initial_visible_ts_is_zero_and_first_commit_strictly_greater() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fresh_boot.mqlite");
    let client = Client::open(&path).unwrap();

    let initial = client.__published_visible_ts();
    assert_eq!(
        initial,
        (0, 0),
        "fresh DB must bootstrap PublishedEpoch.visible_ts = Ts {{ 0, 0 }}; saw {:?}",
        initial
    );

    // First write triggers bootstrap_namespace + CRUD commit — both are
    // publishes and both must advance visible_ts strictly.
    client
        .database("db")
        .collection::<Document>("c")
        .insert_one(&doc! { "_id": 1i32 })
        .unwrap();
    let after = client.__published_visible_ts();
    assert!(
        after > initial,
        "first commit's visible_ts {:?} must be strictly greater than fresh-DB Ts(0,0)",
        after
    );
}
