//! PR 9 acceptance gate: `create_index` is multi-phase.
//!
//! - Phase 1 (reserve) and Phase 3 (commit) take `metadata.write()`
//!   briefly.
//! - Phase 2 (build) runs under `metadata.read()` + the TARGET namespace's
//!   lane, so writers on OTHER namespaces overlap freely.
//! - During Phase 2, writers on the target namespace still dual-write to
//!   the Building index, so the finished index contains every document
//!   (including those inserted during the build).

use bson::doc;
use bson::Document;
use mqlite::{Client, IndexModel};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

/// Other-namespace writers must NOT be serialized behind `create_index`.
///
/// Strategy: seed namespace A with enough docs that the index build takes
/// measurable wall-clock time, then time N inserts on namespace B both
/// serially (baseline) and concurrently with a `create_index` on A.
/// If lanes work, the concurrent time is close to the baseline —
/// definitely less than 2.5x.
#[test]
fn create_index_on_other_ns_does_not_block_writers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ci.mqlite");
    let client = Client::open(&path).unwrap();

    // Seed namespace A with a large-ish collection so the build takes
    // measurable time.
    let col_a = client.database("d").collection::<Document>("big");
    for i in 0..2000i32 {
        col_a
            .insert_one(&doc! { "_id": i, "name": format!("doc-{i}"), "category": i % 10 })
            .unwrap();
    }

    // Note: no pre-create needed. Post-fix, Phase 2 of create_index
    // holds only the target namespace's lane during the long scan —
    // it does NOT hold metadata.read(). `bootstrap_namespace` (DDL)
    // for a new namespace takes metadata.write() and is free to run
    // concurrently with an in-flight create_index build on another
    // namespace.

    // Baseline: insert 200 docs into namespace B serially (no concurrent
    // build), measure duration.
    let col_b_baseline = client.database("d").collection::<Document>("other_baseline");
    let t0 = Instant::now();
    for i in 0..200i32 {
        col_b_baseline
            .insert_one(&doc! { "_id": i, "v": format!("baseline-{i}") })
            .unwrap();
    }
    let baseline = t0.elapsed();

    // Concurrent: spawn create_index on A, while inserting 200 docs into
    // a distinct namespace B.
    let barrier = Arc::new(Barrier::new(2));
    let client_b = client.clone();
    let bb = Arc::clone(&barrier);
    let h_b = thread::spawn(move || {
        bb.wait();
        let col = client_b
            .database("d")
            .collection::<Document>("other_concurrent");
        let t = Instant::now();
        for i in 0..200i32 {
            col.insert_one(&doc! { "_id": i, "v": format!("concurrent-{i}") })
                .unwrap();
        }
        t.elapsed()
    });

    let client_a = client.clone();
    let ba = Arc::clone(&barrier);
    let h_a = thread::spawn(move || {
        ba.wait();
        let coll_a = client_a.database("d").collection::<Document>("big");
        coll_a
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "category": 1 })
                    .build()
                    .unwrap(),
            )
            .unwrap();
    });

    let elapsed_b = h_b.join().unwrap();
    h_a.join().unwrap();

    assert!(
        elapsed_b.as_micros() < (baseline.as_micros() as f64 * 2.5) as u128,
        "writer on namespace B was blocked by create_index on A: \
         baseline={:?} concurrent_b={:?}",
        baseline,
        elapsed_b,
    );

    // Sanity: the index was actually built and contains all docs on A.
    let count = client
        .database("d")
        .collection::<Document>("big")
        .count_documents(doc! { "category": 5 })
        .unwrap();
    assert_eq!(count, 200, "index should match all docs with category=5");
}

/// Docs inserted DURING the build must still appear in the finished
/// index — dual-writes from concurrent writers on the same ns must hit
/// the Building index.
#[test]
fn create_index_includes_concurrent_inserts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ci_dual.mqlite");
    let client = Client::open(&path).unwrap();

    // Seed with 500 docs.
    let col = client.database("d").collection::<Document>("docs");
    for i in 0..500i32 {
        col.insert_one(&doc! { "_id": i, "tag": format!("t-{}", i % 5) })
            .unwrap();
    }

    // Concurrent writer: inserts 500 more docs on the SAME namespace.
    let writer_client = client.clone();
    let writer = thread::spawn(move || {
        let col = writer_client.database("d").collection::<Document>("docs");
        for i in 500..1000i32 {
            col.insert_one(&doc! { "_id": i, "tag": format!("t-{}", i % 5) })
                .unwrap();
        }
    });

    // Build the index. The build's namespace-lane phase serializes with
    // the concurrent writer (same ns), so the writer waits for Phase 2
    // to finish. Inserts that happen between Phase 1 (reserve) and
    // Phase 2 (build), and between Phase 2 and Phase 3 (commit), must
    // also be in the index — `maintain_secondary_on_insert` dual-writes
    // to the Building entry.
    let coll = client.database("d").collection::<Document>("docs");
    coll.create_index(
        IndexModel::builder()
            .keys(doc! { "tag": 1 })
            .build()
            .unwrap(),
    )
    .unwrap();

    writer.join().unwrap();

    // All 1000 docs must be observable via the index.
    let count = coll.count_documents(doc! { "tag": "t-0" }).unwrap();
    assert_eq!(
        count, 200,
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
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "category": 1 })
                    .build()
                    .unwrap(),
            )
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
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "tag": 1 })
                    .build()
                    .unwrap(),
            )
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
    let tag_indexes: Vec<_> = indexes.iter().filter(|i| i.name == "tag_1").collect();
    assert!(
        tag_indexes.is_empty(),
        "tag_1 index must be absent after drop_index succeeds"
    );
}
