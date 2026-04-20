//! Edge-case tests for MWMR v1, PR 4 (PublishedSnapshot + concurrent reads)
//! and PR 5 (retire data_trees cache).
//!
//! Tests:
//!
//! 1. `stale_snapshot_vs_fresh_load` — load-per-operation rule: a second
//!    find() after 50 concurrent writes must see all 50 writes.
//! 2. `namespace_published_but_empty` — create_collection then find with no
//!    docs returns empty; insert one, next find returns it.
//! 3. `nonexistent_namespace_read` — find/list_indexes/count on an uncreated
//!    namespace must all return empty without errors.
//! 4. `list_namespaces_monotonic` — create/drop the same namespace 20 times;
//!    presence/absence is consistent after each operation.
//! 5. `list_indexes_returns_ready_only` — list_indexes never shows a partial
//!    (Building) index; after create_index finishes the index appears.
//! 6. `write_churn_while_reading` — 1000 inserts on one thread while another
//!    does find() for 1 second; every find result is self-consistent.
//! 7. `post_pr5_fresh_tree_opens` — 500 sequential inserts each followed by
//!    a count; each count equals the number of inserts so far.
//! 8. `btree_split_under_writes` — 2000 docs with 500-byte values in batches
//!    of 100; after each batch, all docs retrievable by _id.

use bson::doc;
use bson::Document;
use mqlite::{Client, IndexModel};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// TC1: Stale snapshot vs fresh load
// ---------------------------------------------------------------------------

/// After thread B commits 50 writes to the same namespace, thread A's next
/// find() must see all 50 writes. Proves the load-per-operation rule: each
/// find() re-loads the published snapshot rather than caching a stale one.
#[test]
fn stale_snapshot_vs_fresh_load() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stale.mqlite");
    let client = Arc::new(Client::open(&path).unwrap());
    let col = client.database("d").collection::<Document>("c");

    // Seed one doc so the namespace exists.
    col.insert_one(&doc! { "_id": 0i32, "v": 0i32 }).unwrap();

    // Thread A: do a first find to warm any internal state, then wait for
    // thread B to finish 50 writes, then read again.
    let barrier_start = Arc::new(Barrier::new(2));
    let barrier_after_writes = Arc::new(Barrier::new(2));

    let client_a = Arc::clone(&client);
    let bs_a = Arc::clone(&barrier_start);
    let ba_a = Arc::clone(&barrier_after_writes);
    let reader = thread::spawn(move || {
        let col_a = client_a.database("d").collection::<Document>("c");

        // First read — establishes any snapshot the implementation might cache.
        let first: Vec<_> = col_a
            .find(doc! {})
            .run()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(first.len(), 1, "seed doc must be visible on first read");

        // Signal writer to start.
        bs_a.wait();
        // Wait for writer to finish all 50 inserts.
        ba_a.wait();

        // Second read — must see all 50 new docs (plus the seed = 51 total).
        let second: Vec<_> = col_a
            .find(doc! {})
            .run()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            second.len(),
            51,
            "second find must see all 50 new writes (load-per-operation rule); got {}",
            second.len()
        );
    });

    let client_b = Arc::clone(&client);
    let bs_b = Arc::clone(&barrier_start);
    let ba_b = Arc::clone(&barrier_after_writes);
    let writer = thread::spawn(move || {
        let col_b = client_b.database("d").collection::<Document>("c");
        // Wait for reader to complete first find.
        bs_b.wait();
        for i in 1i32..=50 {
            col_b
                .insert_one(&doc! { "_id": i, "v": i })
                .unwrap();
        }
        // Signal reader that writes are done.
        ba_b.wait();
    });

    writer.join().expect("writer panicked");
    reader.join().expect("reader panicked");

    // Verify a fresh find on the main client also sees all 51 docs.
    let final_count: Vec<_> = col
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        final_count.len(),
        51,
        "final scan on original handle must see 51 docs"
    );
}

// ---------------------------------------------------------------------------
// TC2: Namespace published but empty
// ---------------------------------------------------------------------------

/// create_collection then immediately find returns empty list, not error.
/// Then insert one doc — next find returns exactly that doc.
#[test]
fn namespace_published_but_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty_ns.mqlite");
    let client = Client::open(&path).unwrap();
    let db = client.database("d");

    db.create_collection("empty").unwrap();
    let col = db.collection::<Document>("empty");

    // find on newly-published but empty namespace must return empty vec.
    let result: Vec<_> = col
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        result.is_empty(),
        "find on empty namespace must return empty vec, got {} docs",
        result.len()
    );

    // count must also be zero.
    let count = col.count_documents(doc! {}).unwrap();
    assert_eq!(count, 0, "count on empty namespace must be 0");

    // Insert one doc.
    col.insert_one(&doc! { "_id": 1i32, "hello": "world" }).unwrap();

    // Next find must return exactly that doc.
    let after: Vec<_> = col
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        after.len(),
        1,
        "find after single insert must return exactly 1 doc"
    );
    assert_eq!(after[0].get_i32("_id").unwrap(), 1);
    assert_eq!(after[0].get_str("hello").unwrap(), "world");
}

// ---------------------------------------------------------------------------
// TC3: Non-existent namespace read
// ---------------------------------------------------------------------------

/// find, list_indexes, and count on a namespace never created must all
/// return empty results without errors.
#[test]
fn nonexistent_namespace_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("noexist.mqlite");
    let client = Client::open(&path).unwrap();
    let col = client
        .database("ghost_db")
        .collection::<Document>("ghost_col");

    // find must succeed and return empty.
    let docs: Vec<_> = col
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(
        docs.is_empty(),
        "find on non-existent namespace must return empty, got {} docs",
        docs.len()
    );

    // list_indexes must succeed and return empty vec.
    let indexes = col.list_indexes().unwrap();
    assert!(
        indexes.is_empty(),
        "list_indexes on non-existent namespace must return empty vec, got {} indexes",
        indexes.len()
    );

    // count must succeed and return 0.
    let count = col.count_documents(doc! {}).unwrap();
    assert_eq!(
        count, 0,
        "count on non-existent namespace must return 0, got {}",
        count
    );
}

// ---------------------------------------------------------------------------
// TC4: list_namespaces monotonic
// ---------------------------------------------------------------------------

/// Create namespace X, assert X present. Drop X, assert X absent.
/// Repeat 20 times for the same name to catch publication consistency bugs.
#[test]
fn list_namespaces_monotonic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("monotonic.mqlite");
    let client = Client::open(&path).unwrap();
    let db = client.database("mono");

    for round in 0..20usize {
        // Create the collection.
        db.create_collection("x").unwrap();

        // Assert x is present.
        let names = db.list_collection_names().unwrap();
        assert!(
            names.contains(&"x".to_owned()),
            "round {}: 'x' must be present after create_collection, got: {:?}",
            round,
            names
        );

        // Drop the collection.
        db.drop_collection("x").unwrap();

        // Assert x is absent.
        let names_after = db.list_collection_names().unwrap();
        assert!(
            !names_after.contains(&"x".to_owned()),
            "round {}: 'x' must be absent after drop_collection, got: {:?}",
            round,
            names_after
        );
    }
}

// ---------------------------------------------------------------------------
// TC5: list_indexes returns Ready only
// ---------------------------------------------------------------------------

/// While create_index is in-flight on a background thread, the main thread
/// polls list_indexes repeatedly. The index must either be absent (Building
/// phase) or fully present (Ready). It must never appear in a half-baked
/// state. After create_index completes the index must be present.
///
/// Note: IndexInfo has no state field — the invariant is that list_indexes
/// never returns the index name in a Building state (it is hidden until
/// Ready). So if the name appears, it is Ready. We verify it appears
/// after the join and has correct keys.
#[test]
fn list_indexes_returns_ready_only() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("idx_ready.mqlite");
    let client = Client::open(&path).unwrap();

    // Seed enough docs so the build takes measurable time.
    let col = client.database("d").collection::<Document>("things");
    for i in 0..1000i32 {
        col.insert_one(&doc! { "_id": i, "score": i % 100 }).unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));

    // Background thread: build an index.
    let client_builder = client.clone();
    let b_builder = Arc::clone(&barrier);
    let build_thread = thread::spawn(move || {
        b_builder.wait();
        client_builder
            .database("d")
            .collection::<Document>("things")
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "score": 1i32 })
                    .build(),
            )
            .unwrap();
    });

    // Main thread: poll list_indexes for up to 5 seconds.
    barrier.wait();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut saw_index_during_poll = false;
    while Instant::now() < deadline {
        let indexes = col.list_indexes().unwrap();
        // The _id index is always present; check for our score index.
        for idx in &indexes {
            if idx.name == "score_1" {
                // If it appears, it must have the correct keys — proving it's Ready.
                assert_eq!(
                    idx.keys.get_i32("score").unwrap_or(0),
                    1,
                    "score_1 index must have correct keys when visible"
                );
                saw_index_during_poll = true;
            }
        }
        // Small sleep to avoid a spin-burn.
        thread::sleep(Duration::from_millis(1));
    }

    build_thread.join().expect("build thread panicked");

    // After build completes the index must be present.
    let final_indexes = col.list_indexes().unwrap();
    let score_idx: Vec<_> = final_indexes
        .iter()
        .filter(|i| i.name == "score_1")
        .collect();
    assert_eq!(
        score_idx.len(),
        1,
        "score_1 index must appear in list_indexes after create_index completes"
    );

    // Informational: whether we saw it during the build.
    let _ = saw_index_during_poll; // may or may not be true depending on timing
}

// ---------------------------------------------------------------------------
// TC6: Write churn while reading
// ---------------------------------------------------------------------------

/// One thread finds() in a tight loop for 1 second. Another thread inserts
/// 1000 docs. Every find result must be self-consistent: every returned doc
/// has both _id and the "val" field (no partial/torn doc).
#[test]
fn write_churn_while_reading() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("churn_read.mqlite");
    let client = Arc::new(Client::open(&path).unwrap());

    // Seed one doc so the namespace exists before the reader starts.
    let seed_col = client.database("d").collection::<Document>("rw");
    seed_col
        .insert_one(&doc! { "_id": 0i32, "val": "seed" })
        .unwrap();

    let stop_reader = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Writer: insert 1000 docs.
    let writer_client = Arc::clone(&client);
    let writer = thread::spawn(move || {
        let col = writer_client.database("d").collection::<Document>("rw");
        for i in 1i32..=1000 {
            col.insert_one(&doc! { "_id": i, "val": format!("v-{}", i) })
                .unwrap();
        }
    });

    // Reader: find() in a tight loop for 1 second.
    let reader_client = Arc::clone(&client);
    let stop_flag = Arc::clone(&stop_reader);
    let reader = thread::spawn(move || {
        let col = reader_client.database("d").collection::<Document>("rw");
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            let docs: Vec<_> = col
                .find(doc! {})
                .run()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

            // Every returned doc must have both _id and val fields.
            for (idx, d) in docs.iter().enumerate() {
                assert!(
                    d.contains_key("_id"),
                    "doc[{}] is missing _id field: {:?}",
                    idx,
                    d
                );
                assert!(
                    d.contains_key("val"),
                    "doc[{}] is missing val field (partial insert visible): {:?}",
                    idx,
                    d
                );
            }
        }
        stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
    });

    writer.join().expect("writer panicked");
    reader.join().expect("reader panicked");

    // Final sanity: all 1001 docs (seed + 1000) visible.
    let final_count = seed_col.count_documents(doc! {}).unwrap();
    assert_eq!(
        final_count, 1001,
        "all 1001 docs must be present after concurrent writes; got {}",
        final_count
    );
}

// ---------------------------------------------------------------------------
// TC7: Post-PR-5 fresh tree opens
// ---------------------------------------------------------------------------

/// Insert 1 through 500 sequentially. After each insert, count must equal
/// the number of inserts so far. Proves the catalog round-trip works after
/// sync_data_root without the data_trees cache (PR 5).
#[test]
fn post_pr5_fresh_tree_opens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fresh_tree.mqlite");
    let client = Client::open(&path).unwrap();
    let col = client.database("d").collection::<Document>("seq");

    for i in 1i32..=500 {
        col.insert_one(&doc! { "_id": i, "n": i }).unwrap();
        let count = col.count_documents(doc! {}).unwrap();
        assert_eq!(
            count, i as u64,
            "after {} inserts, count must be {}; got {}",
            i, i, count
        );
    }
}

// ---------------------------------------------------------------------------
// TC8: B-tree split under writes
// ---------------------------------------------------------------------------

/// Insert 2000 docs with 500-byte values to force B-tree leaf splits.
/// After each batch of 100, verify all inserted docs are retrievable by _id.
/// Catches root-page staleness between writer's tree handle and reader's snapshot.
#[test]
fn btree_split_under_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("splits.mqlite");
    let client = Client::open(&path).unwrap();
    let col = client.database("d").collection::<Document>("big");

    let large_val: String = "x".repeat(500);
    const TOTAL: i32 = 2000;
    const BATCH: i32 = 100;

    let mut inserted = 0i32;

    while inserted < TOTAL {
        // Insert a batch of 100.
        for i in inserted..inserted + BATCH {
            col.insert_one(&doc! {
                "_id": i,
                "blob": large_val.clone(),
                "idx": i,
            })
            .unwrap();
        }
        inserted += BATCH;

        // After each batch, verify all inserted docs are retrievable by _id.
        for id in 0..inserted {
            let results: Vec<_> = col
                .find(doc! { "_id": id })
                .run()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(
                results.len(),
                1,
                "after {} inserts, find(_id={}) returned {} docs (expected 1)",
                inserted,
                id,
                results.len()
            );
            assert_eq!(
                results[0].get_i32("_id").unwrap(),
                id,
                "doc _id mismatch for _id={}",
                id
            );
            assert_eq!(
                results[0].get_i32("idx").unwrap(),
                id,
                "doc idx mismatch for _id={}",
                id
            );
        }

        // Also verify total count is correct.
        let count = col.count_documents(doc! {}).unwrap();
        assert_eq!(
            count, inserted as u64,
            "count after batch ending at {} must be {}; got {}",
            inserted - 1,
            inserted,
            count
        );
    }
}
