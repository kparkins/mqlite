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

//! US-005 — Contract 3.2 direct-assertion test for the two-stage write
//! envelope.
//!
//! Contract 3.2 predicate (see docs/STORAGE-CONTRACTS-FROZEN.md §3.2):
//!
//!   CRUD writers serialize through the namespace lane and then the global
//!   journal envelope in that order; publication happens only after staged secondary
//!   work, primary install, overlay commit, journal flush, ChainCommit, and
//!   commit_txn complete.
//!
//! # Approach
//!
//! Use the hidden Phase 0 integration probe to execute one insert through the
//! normal storage envelope. The probe samples a reader on the currently
//! published snapshot after `commit_txn` but before `rebuild_and_publish_locked`,
//! then completes publication and samples again. The test then asserts:
//!
//! 1. the pre-publish reader does not see the inserted document;
//! 2. the post-publish reader sees it;
//! 3. the publish timestamp is exactly the ChainCommit commit timestamp.

#[path = "crash_harness.rs"]
mod crash_harness;

use bson::doc;
use bson::Document;
use mqlite::{Client, DurabilityMode, OpenOptions, Phase0ProbeCut};

/// Directly asserts Contract 3.2's staged-envelope-then-publish predicate.
///
/// Phase 0 anchor: docs/STORAGE-CONTRACTS-FROZEN.md §3.2 and the commit
/// envelope at src/storage/paged_engine.rs:320-449.
#[test]
fn publication_follows_full_staged_envelope() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("envelope.mqlite");

    // -----------------------------------------------------------------------
    // Step 1: one committed CRUD write.  FullSync guarantees the ChainCommit
    // frame is durable before insert_one returns.
    // -----------------------------------------------------------------------
    let client = Client::open_with_options(
        &db_path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open");

    let db = client.database("envdb");
    db.create_collection("env_col").expect("create collection");
    let col = db.collection::<Document>("env_col");

    let report = client
        .__phase0_probe_insert(
            "envdb.env_col",
            doc! { "_id": 1i32, "v": "staged" },
            Phase0ProbeCut::CompleteWithPrePublishProbe,
        )
        .expect("phase0 probe insert");

    // -----------------------------------------------------------------------
    // Predicate (a): the committed document is invisible before publish.
    // -----------------------------------------------------------------------
    assert_eq!(
        report.pre_publish_visible,
        Some(false),
        "predicate (a) failed: reader on the old published snapshot saw the \
         document before rebuild_and_publish_locked ran"
    );

    // -----------------------------------------------------------------------
    // Predicate (b): the document is visible after publish.
    // -----------------------------------------------------------------------
    assert_eq!(
        report.post_publish_visible,
        Some(true),
        "predicate (b) failed: reader on the new published snapshot did not \
         see the document after rebuild_and_publish_locked ran"
    );

    // -----------------------------------------------------------------------
    // Predicate (c): publish_ts exactly matches ChainCommit commit_ts.
    // -----------------------------------------------------------------------
    assert_eq!(
        report.publish_ts, report.commit_ts,
        "predicate (c) failed: publish_ts must equal the ChainCommit commit_ts \
         for the write-envelope publication step"
    );

    let frames = crash_harness::scan_chain_commits(&db_path).expect("scan_chain_commits");
    assert_eq!(
        frames.len(),
        1,
        "predicate (c) failed: expected exactly one ChainCommit frame for \
         one committed CRUD write, got {} frames: {:?}",
        frames.len(),
        frames
    );
    assert_eq!(
        Some(frames[0].1),
        report.commit_ts,
        "predicate (c) failed: ChainCommit frame timestamp must match the \
         probed commit_ts"
    );

    // -----------------------------------------------------------------------
    // Sanity: the public read path sees the published payload.
    // -----------------------------------------------------------------------
    let found = col
        .find_one(doc! { "_id": 1i32 })
        .expect("find_one")
        .expect("document must be visible after publication");
    assert_eq!(
        found.get_str("v").ok(),
        Some("staged"),
        "predicate (c) failed: post-publish reader does not see the staged \
         write — publication ran but the durable payload is wrong or missing"
    );
}
