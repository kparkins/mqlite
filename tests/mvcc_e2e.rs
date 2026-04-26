//! MVCC end-to-end test suite.
//!
//! * `drop_collection_barrier` — open N ReadViews, issue `drop_collection`,
//!   verify every registered view is force-expired, new reads return empty,
//!   no deadlock.
//! * `drop_same_session_resurrection_guard` — drop_namespace blocks implicit
//!   same-session re-bootstrap (Contract 3.6); explicit create_namespace clears
//!   the guard and subsequent inserts succeed.
//! * `reader_stable_across_other_ns_commits` — ReadView `read_ts` pin keeps
//!   a reader's snapshot stable while an unrelated namespace commits.
//! * `overflow_pages_stable_under_mixed_load` — 4W/4R/ReadView-churn for a
//!   short soak; `mvcc.overflow.pages_in_use` stays bounded (no leak).

#![cfg(feature = "test-hooks")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bson::Document;
use mqlite::mvcc::{ReadView, Ts};
use mqlite::{doc, Client};

// ---------------------------------------------------------------------------
// 5. drop_collection barrier — the sole NON-ignored test
// ---------------------------------------------------------------------------

#[test]
fn drop_collection_barrier() {
    // Scenario: open a database, seed a collection, register several
    // ReadViews against the engine's ReadViewRegistry, then call
    // `drop_collection`. The barrier protocol (plan §T9) guarantees:
    //
    // 1. Every registered ReadView is force-expired (poisoned flipped
    //    true).
    // 2. `pin_ops_in_flight` drains to zero before `free_subtree` runs.
    // 3. Subsequent reads on the dropped collection return no rows and
    //    the namespace is absent from `list_collection_names`.
    //
    // Because ReadViews are collection-agnostic in v1, this test opens
    // them with no association to `dropped` — the barrier still expires
    // them globally.

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("drop_barrier.mqlite");

    let client = Client::open(&db_path).expect("open client");
    let db = client.database("drops");
    let col = db.collection::<Document>("victims");

    // Seed the collection so the drop has real B-tree pages to free.
    for i in 0..64 {
        col.insert_one(&doc! { "_id": i, "payload": format!("row-{i}") })
            .expect("seed insert");
    }
    assert_eq!(col.count_documents(doc! {}).unwrap(), 64);

    let registry = client
        .__read_view_registry()
        .expect("buffer-pool backed client has a ReadViewRegistry");

    // Open N=5 ReadViews directly against the registry.
    let mut views: Vec<Arc<ReadView>> = Vec::with_capacity(5);
    for i in 0..5u64 {
        views.push(ReadView::open(
            Arc::clone(&registry),
            Ts {
                physical_ms: 1_000 + i,
                logical: 0,
            },
            1_000 + i,
        ));
    }
    assert_eq!(registry.len(), 5, "registry must track all 5 views");
    for v in &views {
        assert!(!v.is_poisoned(), "no view starts poisoned");
    }

    // Fire the drop. This acquires the writer mutex, calls
    // `force_expire_all`, waits for `pin_ops_in_flight == 0` per view,
    // then runs `free_subtree`. The call must return `Ok(())`.
    db.drop_collection("victims").expect("drop_collection");

    // Every pre-opened ReadView must be poisoned.
    for (i, v) in views.iter().enumerate() {
        assert!(
            v.is_poisoned(),
            "view {i} must be poisoned after drop_collection barrier",
        );
        assert!(
            matches!(v.check_active(), Err(mqlite::Error::ReadViewExpired)),
            "poisoned view must surface Error::ReadViewExpired",
        );
    }

    // New reads on the dropped collection see no rows, and the
    // collection is absent from the catalog.
    assert_eq!(
        col.count_documents(doc! {}).unwrap(),
        0,
        "dropped collection must report 0 docs",
    );
    let remaining = db.list_collection_names().expect("list");
    assert!(
        !remaining.iter().any(|n| n == "victims"),
        "dropped collection must not appear in list_collection_names",
    );

    // Dropping the views releases the registry horizon.
    drop(views);
    assert!(registry.is_empty(), "all views must unregister on drop",);
}

// ---------------------------------------------------------------------------
// Contract 3.6 — same-session resurrection guard (direct assertion)
// ---------------------------------------------------------------------------

/// Directly asserts the `dropped_namespaces` resurrection-guard from Contract 3.6.
///
/// Sequence:
/// 1. create_namespace "ns" implicitly (via first insert).
/// 2. Prove insert works.
/// 3. drop_namespace "ns" via `db.drop_collection`.
/// 4. Attempt an implicit use (insert without explicit create_namespace).
/// 5. Assert the attempt fails with `Error::CollectionNotFound` — the variant
///    returned by `bootstrap_namespace` at src/storage/paged_engine.rs:191-193
///    when `dropped_namespaces` contains the name.
/// 6. Call create_namespace explicitly (via `db.create_collection`).
/// 7. Insert into the recreated namespace — assert it succeeds.
///
/// Contract ref: docs/STORAGE-CONTRACTS-FROZEN.md §3.6.
/// Guard source:  src/storage/paged_engine.rs:183-194, :566-628.
#[test]
fn drop_same_session_resurrection_guard() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("resurrection_guard.mqlite");

    let client = Client::open(&db_path).expect("open client");
    let db = client.database("testdb");
    let col = db.collection::<Document>("ns");

    // Step 1-2: implicit create via first insert; prove it works.
    col.insert_one(&doc! { "_id": 1, "v": "before-drop" })
        .expect("initial insert must succeed");
    assert_eq!(
        col.count_documents(doc! {}).expect("count"),
        1,
        "collection must contain 1 document before drop",
    );

    // Step 3: drop the namespace.
    db.drop_collection("ns")
        .expect("drop_collection must succeed");

    // Step 4-5: implicit re-use must be blocked by the resurrection guard.
    // The guard returns Error::CollectionNotFound (not a string-matched error).
    let err = col
        .insert_one(&doc! { "_id": 2, "v": "post-drop-implicit" })
        .expect_err("insert after drop without explicit create must fail");
    assert!(
        matches!(err, mqlite::Error::CollectionNotFound { .. }),
        "resurrection guard must return Error::CollectionNotFound, got: {err:?}",
    );

    // Step 6: explicit create_namespace clears the guard.
    db.create_collection("ns")
        .expect("explicit create_collection must succeed");

    // Step 7: insert into the recreated namespace must succeed.
    col.insert_one(&doc! { "_id": 3, "v": "after-explicit-create" })
        .expect("insert after explicit create must succeed");
    assert_eq!(
        col.count_documents(doc! {}).expect("count after recreate"),
        1,
        "recreated collection must contain exactly 1 document",
    );
}

// ---------------------------------------------------------------------------
// PR-2 snapshot-isolation regression guard
// ---------------------------------------------------------------------------

/// Verify that a reader's snapshot of `db_a.docs` remains stable while an
/// unrelated namespace (`db_b.other`) accumulates 100 inserts concurrently.
///
/// Today the engine-global mutex serialises reads, so the test passes
/// trivially. After PR 4 removes that mutex from the read path, correctness
/// depends entirely on ReadView `read_ts` pinning. This test will catch any
/// regression where that pin is not honoured.
#[test]
fn reader_stable_across_other_ns_commits() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("reader_stable.mqlite");

    // ---- set up the two independent namespaces -------------------------
    let client_a = Client::open(&db_path).expect("open client_a");
    let col_a = client_a.database("db_a").collection::<Document>("docs");

    // Seed db_a.docs with 10 documents.
    for i in 0..10i32 {
        col_a
            .insert_one(&doc! { "_id": i, "val": format!("item-{i}") })
            .expect("seed insert");
    }

    // Capture the baseline snapshot.
    let baseline: Vec<Document> = col_a
        .find(doc! {})
        .run()
        .expect("baseline find")
        .collect::<Result<_, _>>()
        .expect("baseline collect");
    assert_eq!(baseline.len(), 10, "baseline must contain exactly 10 docs");

    // ---- background writer into db_b.other ----------------------------
    // Clone `client_a` so both threads share the SAME engine — that's what
    // makes this test meaningful post-PR-4: the writer's commits publish
    // snapshots on the engine the reader is loading from.
    let writer_client = client_a.clone();
    let writer_handle: thread::JoinHandle<()> = thread::spawn(move || {
        let col_b = writer_client
            .database("db_b")
            .collection::<Document>("other");
        for j in 0..100i32 {
            col_b
                .insert_one(&doc! { "_id": j, "noise": format!("noise-{j}") })
                .expect("background insert");
        }
    });

    // ---- main thread: 20 reads must each equal the baseline ------------
    for round in 0..20usize {
        let result: Vec<Document> = col_a
            .find(doc! {})
            .run()
            .expect("mid-write find")
            .collect::<Result<_, _>>()
            .expect("mid-write collect");
        assert_eq!(
            result.len(),
            baseline.len(),
            "round {round}: db_a.docs count must equal baseline ({})",
            baseline.len(),
        );
        assert_eq!(
            result, baseline,
            "round {round}: db_a.docs contents must equal baseline byte-for-byte",
        );
    }

    // ---- join the writer -----------------------------------------------
    writer_handle.join().expect("writer thread panicked");

    // ---- one final read on db_a.docs must still equal baseline ---------
    let final_result: Vec<Document> = col_a
        .find(doc! {})
        .run()
        .expect("final find")
        .collect::<Result<_, _>>()
        .expect("final collect");
    assert_eq!(
        final_result, baseline,
        "after writer joined: db_a.docs must still equal baseline",
    );

    // ---- sanity: db_b.other must contain exactly 100 documents ---------
    // Use `client_a` directly — it shares an engine with the writer thread,
    // so it sees the committed state without a reopen.
    let col_b_check = client_a.database("db_b").collection::<Document>("other");
    let b_count = col_b_check
        .count_documents(doc! {})
        .expect("count db_b.other");
    assert_eq!(
        b_count, 100,
        "db_b.other must contain exactly 100 docs (background writes must have happened)",
    );
}

// ---------------------------------------------------------------------------
// overflow-page stability under mixed load — leak guard
// ---------------------------------------------------------------------------

#[test]
fn overflow_pages_stable_under_mixed_load() {
    // 4 writers + 4 readers + a ReadView-churn thread. Short soak; the
    // invariant is that `mvcc.overflow.pages_in_use` does not drift
    // beyond a small bound between start and end.
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("soak.mqlite");

    let client = Client::open(&db_path).expect("open");
    {
        let col = client.database("s").collection::<Document>("rows");
        let payload = "x".repeat(512);
        for i in 0..500i32 {
            col.insert_one(&doc! { "_id": i, "payload": &payload })
                .unwrap();
        }
    }

    let start_pages = mqlite::mvcc::overflow_pages_in_use_snapshot();

    let stop = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + Duration::from_secs(3);

    let mut handles = Vec::new();

    // Writers.
    for t in 0..4i32 {
        let s = Arc::clone(&stop);
        let c = client.clone();
        handles.push(thread::spawn(move || {
            let col = c.database("s").collection::<Document>("rows");
            let mut i = t * 10_000;
            let payload = "y".repeat(512);
            while !s.load(Ordering::Relaxed) {
                let _ = col
                    .update_one(
                        doc! { "_id": i % 500 },
                        doc! { "$set": { "payload": &payload } },
                    )
                    .run();
                i += 1;
            }
        }));
    }
    // Readers.
    for _ in 0..4 {
        let s = Arc::clone(&stop);
        let c = client.clone();
        handles.push(thread::spawn(move || {
            let col = c.database("s").collection::<Document>("rows");
            while !s.load(Ordering::Relaxed) {
                if let Ok(cur) = col.find(doc! {}).run() {
                    let _ = cur.count();
                }
            }
        }));
    }
    // ReadView churn.
    let s_churn = Arc::clone(&stop);
    let churn_client = client.clone();
    handles.push(thread::spawn(move || {
        let reg = churn_client
            .__read_view_registry()
            .expect("registry present");
        let mut txn = 100_000u64;
        while !s_churn.load(Ordering::Relaxed) {
            let _v = ReadView::open(
                Arc::clone(&reg),
                Ts {
                    physical_ms: txn,
                    logical: 0,
                },
                txn,
            );
            txn += 1;
            thread::sleep(Duration::from_millis(100));
        }
    }));

    while Instant::now() < deadline {
        thread::sleep(Duration::from_millis(250));
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    let end_pages = mqlite::mvcc::overflow_pages_in_use_snapshot();
    let drift = if start_pages == 0 {
        end_pages as f64
    } else {
        (end_pages as f64 - start_pages as f64).abs() / (start_pages as f64).max(1.0)
    };
    assert!(
        drift < 0.10 || end_pages <= start_pages + 8,
        "overflow.pages_in_use drift > 10%: start={start_pages} end={end_pages} drift={drift:.3}",
    );
}
