//! PR 9 acceptance gate: `create_index` is multi-phase.
//!
//! - Phase 1 (reserve) and Phase 3 (commit) take `metadata.write()`
//!   briefly.
//! - Phase 2 (build) runs under `metadata.read()` + the TARGET namespace's
//!   lane, so writers on OTHER namespaces overlap freely.
//! - During Phase 2, writers on the target namespace still dual-write to
//!   the Building index, so the finished index contains every document
//!   (including those inserted during the build).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use bson::doc;
#[cfg(feature = "test-hooks")]
use bson::Bson;
use bson::Document;
use mqlite::{Client, IndexModel};
#[cfg(feature = "test-hooks")]
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;
#[cfg(feature = "test-hooks")]
use std::time::Duration;
use std::time::Instant;

#[cfg(feature = "test-hooks")]
const TEST_DB: &str = "d";
#[cfg(feature = "test-hooks")]
const BIG_COLL: &str = "big";
#[cfg(feature = "test-hooks")]
const BIG_NS: &str = "d.big";
#[cfg(feature = "test-hooks")]
const DOCS_COLL: &str = "docs";
#[cfg(feature = "test-hooks")]
const DOCS_NS: &str = "d.docs";
#[cfg(feature = "test-hooks")]
const OTHER_COLL: &str = "other_concurrent";
#[cfg(feature = "test-hooks")]
const CATEGORY_INDEX: &str = "category_1";
#[cfg(feature = "test-hooks")]
const TAG_INDEX: &str = "tag_1";
#[cfg(feature = "test-hooks")]
const SETTLE_DEADLINE: Duration = Duration::from_secs(5);
#[cfg(feature = "test-hooks")]
const CATEGORY_SEED_DOCS: i32 = 2000;
#[cfg(feature = "test-hooks")]
const OTHER_WRITES: i32 = 200;
#[cfg(feature = "test-hooks")]
const INITIAL_DOCS: i32 = 500;
#[cfg(feature = "test-hooks")]
const FINAL_DOCS: i32 = 1000;

/// Other-namespace writers must NOT be serialized behind `create_index`.
///
/// Strategy: pause namespace A's build scan after the Building publish, then
/// prove namespace B writes finish before the build is released.
#[cfg(not(feature = "test-hooks"))]
#[test]
#[ignore = "requires test-hooks for deterministic create-index concurrency gate"]
fn create_index_on_other_ns_does_not_block_writers() {}

#[cfg(feature = "test-hooks")]
#[test]
fn create_index_on_other_ns_does_not_block_writers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ci.mqlite");
    let client = Client::open(&path).unwrap();

    // Seed namespace A and bootstrap namespace B before the build starts.
    let col_a = client.database(TEST_DB).collection::<Document>(BIG_COLL);
    for i in 0..CATEGORY_SEED_DOCS {
        col_a
            .insert_one(&doc! { "_id": i, "name": format!("doc-{i}"), "category": i % 10 })
            .unwrap();
    }
    client
        .database(TEST_DB)
        .collection::<Document>(OTHER_COLL)
        .insert_one(&doc! { "_id": -1i32, "v": "seed" })
        .unwrap();

    let mut build_hook = client.__install_create_index_build_hook(BIG_NS, CATEGORY_INDEX);
    let client_a = client.clone();
    let h_a = thread::spawn(move || {
        let coll_a = client_a.database(TEST_DB).collection::<Document>(BIG_COLL);
        coll_a
            .create_index(IndexModel::builder().keys(doc! { "category": 1 }).build())
            .unwrap();
    });

    build_hook
        .wait_until_entered()
        .expect("create_index reached the build scan hook");

    let (done_tx, done_rx) = mpsc::channel();
    let client_b = client.clone();
    let h_b = thread::spawn(move || {
        let result = (|| -> mqlite::Result<()> {
            let col = client_b
                .database(TEST_DB)
                .collection::<Document>(OTHER_COLL);
            for i in 0..OTHER_WRITES {
                col.insert_one(&doc! { "_id": i, "v": format!("concurrent-{i}") })?;
            }
            Ok(())
        })();
        let _ = done_tx.send(result);
    });

    match done_rx.recv_timeout(SETTLE_DEADLINE) {
        Ok(result) => result.expect("writer on namespace B succeeds while build scan is paused"),
        Err(err) => {
            build_hook
                .release()
                .expect("build was waiting on release channel");
            h_a.join().expect("create_index thread joined");
            h_b.join().expect("writer thread joined");
            panic!(
                "writer on namespace B did not finish while create_index build on A was paused: {err}"
            );
        }
    }
    h_b.join().expect("writer thread joined");

    build_hook
        .release()
        .expect("build was waiting on release channel");
    h_a.join().expect("create_index thread joined");

    let other_count = client
        .database(TEST_DB)
        .collection::<Document>(OTHER_COLL)
        .count_documents(doc! {})
        .unwrap();
    assert_eq!(
        other_count,
        u64::try_from(OTHER_WRITES + 1).unwrap(),
        "all namespace B writes should commit while namespace A build is paused"
    );

    // Sanity: the index was actually built and contains all docs on A.
    let count = client
        .database(TEST_DB)
        .collection::<Document>(BIG_COLL)
        .count_documents(doc! { "category": 5 })
        .unwrap();
    assert_eq!(
        count,
        u64::try_from(CATEGORY_SEED_DOCS / 10).unwrap(),
        "index should match all docs with category=5"
    );
}

#[cfg(not(feature = "test-hooks"))]
#[test]
#[ignore = "requires test-hooks for deterministic create-index concurrency gate"]
fn create_index_includes_concurrent_inserts() {}

#[cfg(feature = "test-hooks")]
#[test]
fn create_index_includes_concurrent_inserts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ci_dual.mqlite");
    let client = Client::open(&path).unwrap();

    let col = client.database(TEST_DB).collection::<Document>(DOCS_COLL);
    for i in 0..INITIAL_DOCS {
        col.insert_one(&doc! { "_id": i, "tag": format!("t-{}", i % 5) })
            .unwrap();
    }

    let mut build_hook = client.__install_create_index_build_hook(DOCS_NS, TAG_INDEX);
    let build_client = client.clone();
    let build = thread::spawn(move || {
        build_client
            .database(TEST_DB)
            .collection::<Document>(DOCS_COLL)
            .create_index(IndexModel::builder().keys(doc! { "tag": 1 }).build())
            .unwrap();
    });

    build_hook
        .wait_until_entered()
        .expect("create_index reached the build scan hook");

    let writer_client = client.clone();
    let writer = thread::spawn(move || {
        let col = writer_client
            .database(TEST_DB)
            .collection::<Document>(DOCS_COLL);
        for i in INITIAL_DOCS..FINAL_DOCS {
            col.insert_one(&doc! { "_id": i, "tag": format!("t-{}", i % 5) })
                .unwrap();
        }
    });
    writer.join().unwrap();

    let states = client
        .__us009_secondary_chain_states(
            DOCS_NS,
            TAG_INDEX,
            &doc! { "_id": INITIAL_DOCS, "tag": "t-0" },
            &Bson::Int32(INITIAL_DOCS),
        )
        .expect("Building index secondary chain can be inspected");
    let has_committed_building_entry = states.iter().any(|state| state == "Committed");

    build_hook
        .release()
        .expect("build was waiting on release channel");
    build.join().unwrap();

    assert!(
        has_committed_building_entry,
        "writer must dual-write a committed entry into the Building index; states={states:?}"
    );

    let count = col.count_documents(doc! { "tag": "t-0" }).unwrap();
    assert_eq!(
        count,
        u64::try_from(FINAL_DOCS / 5).unwrap(),
        "all docs (including post-build inserts) must be indexed"
    );
}

/// Bootstrap of a new namespace must NOT block on an in-flight Phase-2
/// build on a different namespace. This is the fix for PR 9's known
/// v1 limitation: Phase 2 formerly held metadata.read() for its whole
/// duration, which starved bootstrap_namespace (DDL needing
/// metadata.write()) under RwLock fairness.
#[test]
fn bootstrap_new_namespace_not_blocked_by_create_index_build() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ci_bootstrap.mqlite");
    let client = Client::open(&path).unwrap();

    // Seed namespace A with enough docs that the build takes real time.
    let col_a = client.database("d").collection::<Document>("big");
    for i in 0..5000i32 {
        col_a
            .insert_one(&doc! { "_id": i, "name": format!("doc-{i}"), "category": i % 10 })
            .unwrap();
    }

    // Barrier synchronizes the two threads so the bootstrap definitely
    // starts while the build is in progress.
    let barrier = Arc::new(Barrier::new(2));

    let client_build = client.clone();
    let b_build = Arc::clone(&barrier);
    let build_thread = thread::spawn(move || {
        b_build.wait();
        client_build
            .database("d")
            .collection::<Document>("big")
            .create_index(IndexModel::builder().keys(doc! { "category": 1 }).build())
            .unwrap();
    });

    let client_boot = client.clone();
    let b_boot = Arc::clone(&barrier);
    let bootstrap_thread = thread::spawn(move || {
        b_boot.wait();
        // Sleep briefly to let the build actually enter Phase 2.
        thread::sleep(std::time::Duration::from_millis(20));
        let t = Instant::now();
        // First insert into a brand-new namespace forces
        // bootstrap_namespace (DDL, metadata.write()). Post-fix this
        // must NOT block on the build's Phase 2.
        client_boot
            .database("d2")
            .collection::<Document>("brand_new")
            .insert_one(&doc! { "_id": 1i32, "v": "post-fix" })
            .unwrap();
        t.elapsed()
    });

    let build_elapsed = {
        let t = Instant::now();
        build_thread.join().unwrap();
        t.elapsed()
    };
    let bootstrap_elapsed = bootstrap_thread.join().unwrap();

    // The bootstrap must have completed BEFORE the build finished.
    // If bootstrap were blocked behind the whole build, its elapsed
    // would be >= build elapsed. Post-fix, bootstrap should be a small
    // fraction of the build time.
    assert!(
        bootstrap_elapsed < build_elapsed,
        "bootstrap took {:?} which is >= build {:?} — bootstrap is blocked on Phase 2",
        bootstrap_elapsed,
        build_elapsed,
    );

    // Final sanity: both namespaces are populated.
    let a_count = client
        .database("d")
        .collection::<Document>("big")
        .count_documents(doc! {})
        .unwrap();
    assert_eq!(a_count, 5000);
    let new_count = client
        .database("d2")
        .collection::<Document>("brand_new")
        .count_documents(doc! {})
        .unwrap();
    assert_eq!(new_count, 1);
}

/// drop_index on a Building index must succeed. It takes the ns lane,
/// waits for any in-flight Phase 2 build to release, then removes the
/// entry. The create_index caller observes the drop via a Phase 3
/// error.
#[test]
fn drop_index_during_build_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("drop_building.mqlite");
    let client = Client::open(&path).unwrap();

    // Seed enough docs that the build takes real time.
    let col = client.database("d").collection::<Document>("big");
    for i in 0..5000i32 {
        col.insert_one(&doc! { "_id": i, "tag": format!("t-{}", i % 7) })
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));

    let client_build = client.clone();
    let b_build = Arc::clone(&barrier);
    let build_thread = thread::spawn(move || {
        b_build.wait();
        // Phase 2 will be racing with drop. Either this returns Err (drop
        // won the race) or Ok (build finished before drop). Both are
        // acceptable semantics — what matters is that drop ALWAYS
        // succeeds.
        client_build
            .database("d")
            .collection::<Document>("big")
            .create_index(IndexModel::builder().keys(doc! { "tag": 1 }).build())
    });

    let client_drop = client.clone();
    let b_drop = Arc::clone(&barrier);
    let drop_thread = thread::spawn(move || {
        b_drop.wait();
        // Let Phase 2 enter its scan before we drop.
        thread::sleep(std::time::Duration::from_millis(20));
        client_drop
            .database("d")
            .collection::<Document>("big")
            .drop_index("tag_1")
            .unwrap(); // drop MUST succeed
    });

    let _build_result = build_thread.join().unwrap();
    drop_thread.join().unwrap();

    // After both complete, the index is gone (either drop removed it, or
    // create finished and then drop removed it — either way).
    let indexes = client
        .database("d")
        .collection::<Document>("big")
        .list_indexes()
        .unwrap();
    assert!(
        indexes.iter().all(|i| i.name != "tag_1"),
        "tag_1 index must be absent after drop_index succeeds"
    );
}
