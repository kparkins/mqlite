//! Edge-case tests for mqlite MWMR v1 — create_index 3-phase (PR 9) and
//! follow-up fixes.
//!
//! These tests probe corner cases not covered by the PR-9 acceptance gates:
//! idempotency, concurrent index creation on different fields, drop races,
//! empty / non-existent collections, multikey, unique-violation at build time,
//! dual-write during build, building-index invisibility, drop_collection races,
//! and reserved-name protection.

use bson::doc;
use bson::Document;
use mqlite::{Client, IndexModel, IndexOptions};
use std::sync::{Arc, Barrier};
use std::thread;

// ---------------------------------------------------------------------------
// TC1: Idempotency — create_index with same name twice
// ---------------------------------------------------------------------------

/// Calling create_index twice with the identical key pattern must not create
/// a duplicate entry. Both calls return Ok with the same index name, and
/// list_indexes reports exactly one index (plus the _id_ index).
#[test]
fn tc01_create_index_same_name_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc01.mqlite");
    let client = Client::open(&path).unwrap();

    let col = client.database("d").collection::<Document>("col");
    // Seed a few docs.
    for i in 0..10i32 {
        col.insert_one(&doc! { "_id": i, "x": i }).unwrap();
    }

    let name1 = col
        .create_index(
            IndexModel::builder()
                .keys(doc! { "x": 1 })
                .options(IndexOptions::new().name("x_idx"))
                .build()
                .unwrap(),
        )
        .unwrap();

    let name2 = col
        .create_index(
            IndexModel::builder()
                .keys(doc! { "x": 1 })
                .options(IndexOptions::new().name("x_idx"))
                .build()
                .unwrap(),
        )
        .unwrap();

    assert_eq!(name1, name2, "both calls must return the same index name");

    let indexes = col.list_indexes().unwrap();
    let x_indexes: Vec<_> = indexes.iter().filter(|i| i.name == "x_idx").collect();
    assert_eq!(
        x_indexes.len(),
        1,
        "list_indexes must show exactly one x_idx (got {})",
        x_indexes.len()
    );
}

// ---------------------------------------------------------------------------
// TC2: Idempotency — same keys, different explicit names => two indexes
// ---------------------------------------------------------------------------

/// Two create_index calls with identical key patterns but different explicit
/// names should each succeed and produce separate index entries.
#[test]
fn tc02_same_keys_different_names_two_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc02.mqlite");
    let client = Client::open(&path).unwrap();

    let col = client.database("d").collection::<Document>("col");
    for i in 0..20i32 {
        col.insert_one(&doc! { "_id": i, "y": i % 5 }).unwrap();
    }

    let n1 = col
        .create_index(
            IndexModel::builder()
                .keys(doc! { "y": 1 })
                .options(IndexOptions::new().name("y_alpha"))
                .build()
                .unwrap(),
        )
        .unwrap();

    let n2 = col
        .create_index(
            IndexModel::builder()
                .keys(doc! { "y": 1 })
                .options(IndexOptions::new().name("y_beta"))
                .build()
                .unwrap(),
        )
        .unwrap();

    assert_eq!(n1, "y_alpha");
    assert_eq!(n2, "y_beta");

    let indexes = col.list_indexes().unwrap();
    let has_alpha = indexes.iter().any(|i| i.name == "y_alpha");
    let has_beta = indexes.iter().any(|i| i.name == "y_beta");
    assert!(has_alpha, "y_alpha index missing from list_indexes");
    assert!(has_beta, "y_beta index missing from list_indexes");

    // A find that benefits from either index must still return correct results.
    let count = col.count_documents(doc! { "y": 3 }).unwrap();
    assert_eq!(count, 4, "expected 4 docs with y=3");
}

// ---------------------------------------------------------------------------
// TC3: Concurrent create_index on DIFFERENT indexes of same namespace
// ---------------------------------------------------------------------------

/// Two threads concurrently creating indexes on different fields of the same
/// collection must both complete successfully. After both finish, list_indexes
/// shows both new indexes, and docs inserted before the build are indexed by both.
#[test]
fn tc03_concurrent_create_index_different_fields() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc03.mqlite");
    let client = Client::open(&path).unwrap();

    let col = client.database("d").collection::<Document>("col");
    // Seed docs with two fields.
    for i in 0..500i32 {
        col.insert_one(&doc! { "_id": i, "field_a": i % 10, "field_b": i % 20 })
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));

    let c1 = client.clone();
    let b1 = Arc::clone(&barrier);
    let h1 = thread::spawn(move || {
        b1.wait();
        c1.database("d")
            .collection::<Document>("col")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "field_a": 1 })
                    .build()
                    .unwrap(),
            )
            .unwrap()
    });

    let c2 = client.clone();
    let b2 = Arc::clone(&barrier);
    let h2 = thread::spawn(move || {
        b2.wait();
        c2.database("d")
            .collection::<Document>("col")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "field_b": 1 })
                    .build()
                    .unwrap(),
            )
            .unwrap()
    });

    let name_a = h1.join().unwrap();
    let name_b = h2.join().unwrap();

    let indexes = col.list_indexes().unwrap();
    let has_a = indexes.iter().any(|i| i.name == name_a);
    let has_b = indexes.iter().any(|i| i.name == name_b);
    assert!(has_a, "{} missing from list_indexes after concurrent build", name_a);
    assert!(has_b, "{} missing from list_indexes after concurrent build", name_b);

    // Both indexes must cover pre-seeded docs.
    let cnt_a = col.count_documents(doc! { "field_a": 5 }).unwrap();
    assert_eq!(cnt_a, 50, "field_a index: wrong count for field_a=5");

    let cnt_b = col.count_documents(doc! { "field_b": 7 }).unwrap();
    assert_eq!(cnt_b, 25, "field_b index: wrong count for field_b=7");
}

// ---------------------------------------------------------------------------
// TC4: drop_index on Ready index — sanity baseline
// ---------------------------------------------------------------------------

/// Create an index, verify it appears in list_indexes (state Ready), drop it,
/// verify it is absent.
#[test]
fn tc04_drop_ready_index() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc04.mqlite");
    let client = Client::open(&path).unwrap();

    let col = client.database("d").collection::<Document>("col");
    for i in 0..50i32 {
        col.insert_one(&doc! { "_id": i, "val": i }).unwrap();
    }

    let idx_name = col
        .create_index(
            IndexModel::builder()
                .keys(doc! { "val": 1 })
                .build()
                .unwrap(),
        )
        .unwrap();

    // Verify present.
    let before = col.list_indexes().unwrap();
    assert!(
        before.iter().any(|i| i.name == idx_name),
        "index {} should be present after creation",
        idx_name
    );

    // Drop.
    col.drop_index(&idx_name).unwrap();

    // Verify absent.
    let after = col.list_indexes().unwrap();
    assert!(
        !after.iter().any(|i| i.name == idx_name),
        "index {} should be absent after drop",
        idx_name
    );
}

// ---------------------------------------------------------------------------
// TC5: drop_index during build — drop wins race
// ---------------------------------------------------------------------------

/// Seed 5000 docs. Start create_index in background, wait 30ms, then call
/// drop_index. drop_index must succeed. After both complete the index is absent.
#[test]
fn tc05_drop_wins_race_during_build() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc05.mqlite");
    let client = Client::open(&path).unwrap();

    let col = client.database("d").collection::<Document>("col");
    for i in 0..5000i32 {
        col.insert_one(&doc! { "_id": i, "tag": format!("t-{}", i % 7) })
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));

    let c_build = client.clone();
    let b_build = Arc::clone(&barrier);
    let build_handle = thread::spawn(move || {
        b_build.wait();
        c_build
            .database("d")
            .collection::<Document>("col")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "tag": 1 })
                    .build()
                    .unwrap(),
            )
        // Either Ok (build finished before drop) or Err (drop won). Both allowed.
    });

    let c_drop = client.clone();
    let b_drop = Arc::clone(&barrier);
    let drop_handle = thread::spawn(move || {
        b_drop.wait();
        // Give Phase 2 a chance to enter its scan.
        thread::sleep(std::time::Duration::from_millis(30));
        c_drop
            .database("d")
            .collection::<Document>("col")
            .drop_index("tag_1")
            .unwrap(); // drop MUST succeed
    });

    let _build_result = build_handle.join().unwrap();
    drop_handle.join().unwrap();

    // After both complete the index must be gone.
    let indexes = col.list_indexes().unwrap();
    let remaining: Vec<_> = indexes.iter().filter(|i| i.name == "tag_1").collect();
    assert!(
        remaining.is_empty(),
        "tag_1 index must be absent after drop_index (remaining: {:?})",
        remaining
    );
}

// ---------------------------------------------------------------------------
// TC6: create_index on empty collection
// ---------------------------------------------------------------------------

/// create_index on a collection with zero documents must succeed immediately.
/// A doc inserted after index creation must be findable via the index.
#[test]
fn tc06_create_index_empty_collection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc06.mqlite");
    let client = Client::open(&path).unwrap();

    // Bootstrap collection (no docs).
    let col = client.database("d").collection::<Document>("empty");
    // Force namespace creation with one insert then delete... or just create_index
    // directly — the spec says create_index on an empty collection should succeed.
    let idx_name = col
        .create_index(
            IndexModel::builder()
                .keys(doc! { "email": 1 })
                .build()
                .unwrap(),
        )
        .unwrap();

    let indexes = col.list_indexes().unwrap();
    assert!(
        indexes.iter().any(|i| i.name == idx_name),
        "index must appear in list_indexes after create on empty collection"
    );

    // Insert a doc — must be indexed.
    col.insert_one(&doc! { "_id": 1i32, "email": "alice@example.com" })
        .unwrap();

    let count = col
        .count_documents(doc! { "email": "alice@example.com" })
        .unwrap();
    assert_eq!(count, 1, "post-insert doc must be findable via the index");
}

// ---------------------------------------------------------------------------
// TC7: create_index on non-existent collection (bootstrap)
// ---------------------------------------------------------------------------

/// Calling create_index on a namespace that has never had a doc inserted must
/// bootstrap the collection and create the index. After the call both the
/// collection (empty) and the index exist.
#[test]
fn tc07_create_index_bootstraps_collection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc07.mqlite");
    let client = Client::open(&path).unwrap();

    // Deliberately do NOT insert any doc before create_index.
    let col = client
        .database("d")
        .collection::<Document>("brand_new");

    let idx_name = col
        .create_index(
            IndexModel::builder()
                .keys(doc! { "score": 1 })
                .build()
                .unwrap(),
        )
        .unwrap();

    // Collection now exists (even if empty).
    let count = col.count_documents(doc! {}).unwrap();
    assert_eq!(count, 0, "bootstrapped collection should be empty");

    // Index exists.
    let indexes = col.list_indexes().unwrap();
    assert!(
        indexes.iter().any(|i| i.name == idx_name),
        "index {} must be present in the bootstrapped collection",
        idx_name
    );

    // Insert a doc and query via the index.
    col.insert_one(&doc! { "_id": 1i32, "score": 42i32 }).unwrap();
    let cnt = col.count_documents(doc! { "score": 42i32 }).unwrap();
    assert_eq!(cnt, 1, "inserted doc must be findable via the index");
}

// ---------------------------------------------------------------------------
// TC8: Multikey index detection
// ---------------------------------------------------------------------------

/// Insert a doc with an array field. After building an index on that field,
/// querying with an element value must return the doc. Multikey functionality
/// is verified by query result (the internal `multikey` flag is not public).
#[test]
fn tc08_multikey_index_query() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc08.mqlite");
    let client = Client::open(&path).unwrap();

    let col = client.database("d").collection::<Document>("mk");
    // Insert docs; one has an array field.
    col.insert_one(&doc! { "_id": 1i32, "arr": [1i32, 2i32, 3i32] }).unwrap();
    col.insert_one(&doc! { "_id": 2i32, "arr": [4i32, 5i32] }).unwrap();
    col.insert_one(&doc! { "_id": 3i32, "arr": 7i32 }).unwrap(); // scalar

    let idx_name = col
        .create_index(
            IndexModel::builder()
                .keys(doc! { "arr": 1 })
                .build()
                .unwrap(),
        )
        .unwrap();

    // Index must be present in list_indexes.
    let indexes = col.list_indexes().unwrap();
    assert!(
        indexes.iter().any(|i| i.name == idx_name),
        "multikey index must appear in list_indexes"
    );

    // Querying by an array element must return the correct doc.
    let count = col.count_documents(doc! { "arr": 1i32 }).unwrap();
    assert_eq!(count, 1, "doc with arr=[1,2,3] must match query {{arr: 1}}");

    let count2 = col.count_documents(doc! { "arr": 4i32 }).unwrap();
    assert_eq!(count2, 1, "doc with arr=[4,5] must match query {{arr: 4}}");

    let count3 = col.count_documents(doc! { "arr": 7i32 }).unwrap();
    assert_eq!(count3, 1, "scalar doc must match query {{arr: 7}}");
}

// ---------------------------------------------------------------------------
// TC9: Unique index violation at build time
// ---------------------------------------------------------------------------

/// Insert two docs with the same value for `email`. Create a unique index on
/// `email`. The build must either fail or be unusable, and no orphan Building
/// entry must remain in the catalog.
#[test]
fn tc09_unique_index_build_time_violation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc09.mqlite");
    let client = Client::open(&path).unwrap();

    let col = client.database("d").collection::<Document>("users");
    col.insert_one(&doc! { "_id": 1i32, "email": "dup@example.com" }).unwrap();
    col.insert_one(&doc! { "_id": 2i32, "email": "dup@example.com" }).unwrap();

    let result = col.create_index(
        IndexModel::builder()
            .keys(doc! { "email": 1 })
            .options(IndexOptions::new().unique(true).name("email_idx"))
            .build()
            .unwrap(),
    );

    // create_index must fail with a DuplicateKey error (or similar) since the
    // data has a unique violation.
    assert!(
        result.is_err(),
        "create_index with unique violation must return Err, got Ok({:?})",
        result.ok()
    );

    // No orphan Building entry: list_indexes must not show a partial email_idx.
    let indexes = col.list_indexes().unwrap();
    let building: Vec<_> = indexes.iter().filter(|i| i.name == "email_idx").collect();
    assert!(
        building.is_empty(),
        "orphan Building entry for email_idx must not remain after failed build: {:?}",
        building
    );
}

// ---------------------------------------------------------------------------
// TC10: Dual-write during build — all docs indexed
// ---------------------------------------------------------------------------

/// Seed 500 docs (tag in ["a","b"]). Concurrently: (a) create_index on `tag`,
/// (b) insert 500 more docs (tag in ["c","d"]). After both complete, verify
/// counts per tag and total = 1000; every doc must be in the final index.
#[test]
fn tc10_dual_write_during_build_all_docs_indexed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc10.mqlite");
    let client = Client::open(&path).unwrap();

    let col = client.database("d").collection::<Document>("docs");

    // Seed 500 docs: 250 "a", 250 "b".
    for i in 0..500i32 {
        let tag = if i < 250 { "a" } else { "b" };
        col.insert_one(&doc! { "_id": i, "tag": tag }).unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));

    // Thread A: build index on `tag`.
    let c_idx = client.clone();
    let b_idx = Arc::clone(&barrier);
    let idx_thread = thread::spawn(move || {
        b_idx.wait();
        c_idx
            .database("d")
            .collection::<Document>("docs")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "tag": 1 })
                    .build()
                    .unwrap(),
            )
            .unwrap()
    });

    // Thread B: insert 500 more docs (tag "c" or "d").
    let c_ins = client.clone();
    let b_ins = Arc::clone(&barrier);
    let ins_thread = thread::spawn(move || {
        b_ins.wait();
        let col2 = c_ins.database("d").collection::<Document>("docs");
        for i in 500..1000i32 {
            let tag = if i < 750 { "c" } else { "d" };
            col2.insert_one(&doc! { "_id": i, "tag": tag }).unwrap();
        }
    });

    let _idx_name = idx_thread.join().unwrap();
    ins_thread.join().unwrap();

    // Total doc count must be 1000.
    let total = col.count_documents(doc! {}).unwrap();
    assert_eq!(total, 1000, "total doc count must be 1000");

    // Count per tag.
    for (tag, expected) in [("a", 250u64), ("b", 250), ("c", 250), ("d", 250)] {
        let cnt = col.count_documents(doc! { "tag": tag }).unwrap();
        assert_eq!(
            cnt, expected,
            "tag={} expected {} docs, got {}",
            tag, expected, cnt
        );
    }
}

// ---------------------------------------------------------------------------
// TC11: Building index invisible to queries (collscan fallback)
// ---------------------------------------------------------------------------

/// While a create_index build is in progress on a large collection, concurrent
/// queries must still succeed (fall back to collection scan) and return correct
/// results. The query must not fail because the index is in Building state.
#[test]
fn tc11_building_index_invisible_to_queries() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc11.mqlite");
    let client = Client::open(&path).unwrap();

    let col = client.database("d").collection::<Document>("bigcol");

    // Seed 5000 docs.
    for i in 0..5000i32 {
        col.insert_one(&doc! { "_id": i, "status": if i < 1000 { "active" } else { "inactive" } })
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));

    // Thread A: start the index build.
    let c_build = client.clone();
    let b_build = Arc::clone(&barrier);
    let build_thread = thread::spawn(move || {
        b_build.wait();
        c_build
            .database("d")
            .collection::<Document>("bigcol")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "status": 1 })
                    .build()
                    .unwrap(),
            )
            .unwrap();
    });

    // Thread B: while build is in progress, run queries.
    let c_query = client.clone();
    let b_query = Arc::clone(&barrier);
    let query_thread = thread::spawn(move || {
        b_query.wait();
        // Brief sleep so the build enters Phase 2 scan before we query.
        thread::sleep(std::time::Duration::from_millis(10));
        let col_q = c_query.database("d").collection::<Document>("bigcol");
        // Must succeed and return correct count (falls back to collscan).
        let cnt = col_q
            .count_documents(doc! { "status": "active" })
            .unwrap();
        cnt
    });

    let query_count = query_thread.join().unwrap();
    build_thread.join().unwrap();

    assert_eq!(
        query_count, 1000,
        "collscan fallback during build must return correct count (expected 1000, got {})",
        query_count
    );

    // After build completes, query via the ready index also correct.
    let final_count = col.count_documents(doc! { "status": "active" }).unwrap();
    assert_eq!(final_count, 1000, "post-build query must return correct count");
}

// ---------------------------------------------------------------------------
// TC12: create_index failure — concurrent drop_collection
// ---------------------------------------------------------------------------

/// Start create_index on a large collection. In a separate thread, immediately
/// drop_collection. create_index may Ok or Err (race-dependent); no panics.
/// After drop_collection, the collection is gone (count = 0).
#[test]
fn tc12_create_index_vs_drop_collection_no_panic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc12.mqlite");
    let client = Client::open(&path).unwrap();

    let col = client.database("d").collection::<Document>("victim");
    for i in 0..5000i32 {
        col.insert_one(&doc! { "_id": i, "score": i }).unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));

    // Thread A: create_index (may succeed or fail — race with drop).
    let c_idx = client.clone();
    let b_idx = Arc::clone(&barrier);
    let idx_thread = thread::spawn(move || {
        b_idx.wait();
        // Ignore result — either Ok or Err is acceptable under the race.
        let _ = c_idx
            .database("d")
            .collection::<Document>("victim")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "score": 1 })
                    .build()
                    .unwrap(),
            );
    });

    // Thread B: drop the collection.
    let b_drop = Arc::clone(&barrier);
    let client_drop = client.clone();
    let drop_thread = thread::spawn(move || {
        b_drop.wait();
        client_drop
            .database("d")
            .drop_collection("victim")
            .unwrap();
    });

    // Both threads must complete without panicking.
    idx_thread.join().unwrap();
    drop_thread.join().unwrap();

    // After drop, collection is gone or empty.
    let count = client
        .database("d")
        .collection::<Document>("victim")
        .count_documents(doc! {})
        .unwrap_or(0);
    assert_eq!(count, 0, "dropped collection must be empty/absent");
}

// ---------------------------------------------------------------------------
// TC13: Reserved index names — drop_index("_id_") must be rejected
// ---------------------------------------------------------------------------

/// Attempting to drop the reserved `_id_` index must return an
/// `InvalidWireMessage` error (per paged_engine.rs drop_index guard).
#[test]
fn tc13_drop_reserved_id_index_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc13.mqlite");
    let client = Client::open(&path).unwrap();

    let col = client.database("d").collection::<Document>("col");
    col.insert_one(&doc! { "_id": 1i32, "x": 1i32 }).unwrap();

    let result = col.drop_index("_id_");
    assert!(
        result.is_err(),
        "drop_index('_id_') must return Err, got Ok"
    );

    // The error must be the InvalidWireMessage variant.
    match result.unwrap_err() {
        mqlite::Error::InvalidWireMessage { detail } => {
            assert!(
                detail.contains("_id_"),
                "error detail should mention '_id_', got: {detail}"
            );
        }
        other => panic!(
            "expected Error::InvalidWireMessage, got: {:?}",
            other
        ),
    }
}
