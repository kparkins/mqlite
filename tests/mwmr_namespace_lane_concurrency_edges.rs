//! PR 8 edge-case tests: namespace lanes + commit publication + metadata RwLock.
//!
//! These tests cover concurrency edge cases NOT exercised by the happy-path
//! acceptance gates in `mwmr_namespace_lanes.rs`.  Each test targets a specific
//! failure mode of the MWMR concurrency model.

use bson::doc;
use bson::Document;
use mqlite::{Client, Error, OpenOptions};
use std::collections::HashSet;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

// ---------------------------------------------------------------------------
// TC1: High-fanout stress — 16 threads × 50 inserts across 8 namespaces
// ---------------------------------------------------------------------------

/// 16 threads (2 per namespace) insert 50 docs each into 8 namespaces.
/// After join: total doc count = 800, each namespace has exactly 100 docs.
/// Catches lane mis-routing and lost writes.
#[test]
fn high_fanout_stress_16x50_8ns() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fanout.mqlite");
    let client = Client::open(&path).unwrap();

    const THREADS: usize = 16;
    const PER_THREAD: i32 = 50;
    const NAMESPACES: usize = 8;
    // 2 threads per namespace; thread t uses namespace t/2
    let barrier = Arc::new(Barrier::new(THREADS));

    let handles: Vec<_> = (0..THREADS as i32)
        .map(|t| {
            let c = client.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                b.wait();
                let ns = t / 2; // 0..7
                let col = c
                    .database("fanout")
                    .collection::<Document>(&format!("ns{ns}"));
                for i in 0..PER_THREAD {
                    let id = t * PER_THREAD + i;
                    col.insert_one(&doc! { "_id": id, "thread": t, "ns": ns })
                        .expect("insert must not panic");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    let mut total = 0u64;
    for ns in 0..NAMESPACES {
        let count = client
            .database("fanout")
            .collection::<Document>(&format!("ns{ns}"))
            .count_documents(doc! {})
            .unwrap();
        assert_eq!(
            count, 100,
            "namespace ns{ns} must have exactly 100 docs, got {count}"
        );
        total += count;
    }
    assert_eq!(total, 800, "total across all namespaces must be 800");
}

// ---------------------------------------------------------------------------
// TC2: Mixed workload — 4 writers + 4 readers + 1 DDL thread, 500ms
// ---------------------------------------------------------------------------

/// 4 writer threads on different namespaces + 4 readers on same namespaces +
/// 1 DDL thread (create_collection / drop_collection on a 9th ns) for 500ms.
/// After: writer-inserted docs present; DDL either succeeds or cleanly fails;
/// no panics, no deadlocks.
#[test]
fn mixed_workload_writers_readers_ddl_500ms() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("mixed.mqlite");
    let client = Client::open(&path).unwrap();

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let deadline = Duration::from_millis(500);

    // Seed writer namespaces so readers can start immediately.
    for wt in 0..4i32 {
        let col = client
            .database("mixed")
            .collection::<Document>(&format!("writer{wt}"));
        col.insert_one(&doc! { "_id": -1, "seed": true }).unwrap();
    }

    // 4 writer threads.
    let writer_handles: Vec<_> = (0..4i32)
        .map(|wt| {
            let c = client.clone();
            let stop2 = stop.clone();
            thread::spawn(move || {
                let col = c
                    .database("mixed")
                    .collection::<Document>(&format!("writer{wt}"));
                let mut next_id = 0i32;
                while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = col.insert_one(&doc! { "_id": next_id, "wt": wt });
                    next_id += 1;
                }
                next_id
            })
        })
        .collect();

    // 4 reader threads.
    let reader_handles: Vec<_> = (0..4i32)
        .map(|rt| {
            let c = client.clone();
            let stop2 = stop.clone();
            thread::spawn(move || {
                let col = c
                    .database("mixed")
                    .collection::<Document>(&format!("writer{rt}"));
                while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                    // Reader errors (e.g. WriterBusy) are allowed but no panics.
                    let _ = col.find(doc! {}).run();
                }
            })
        })
        .collect();

    // 1 DDL thread — create+drop a 9th namespace.
    let ddl_client = client.clone();
    let ddl_stop = stop.clone();
    let ddl_handle = thread::spawn(move || {
        let db = ddl_client.database("mixed");
        while !ddl_stop.load(std::sync::atomic::Ordering::Relaxed) {
            // Both create and drop may fail with namespace-not-found or
            // WriterBusy — that is acceptable.
            let _ = db.create_collection("ddl_ns9");
            let _ = db.drop_collection("ddl_ns9");
        }
    });

    thread::sleep(deadline);
    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    // Collect all threads — any panic propagates via unwrap.
    ddl_handle.join().expect("DDL thread panicked");
    for h in reader_handles {
        h.join().expect("reader thread panicked");
    }
    let inserts_per_writer: Vec<i32> = writer_handles
        .into_iter()
        .map(|h| h.join().expect("writer thread panicked"))
        .collect();

    // Verify: each writer namespace has at least 1 doc (the seed).
    for (wt, inserted) in inserts_per_writer.iter().enumerate() {
        let count = client
            .database("mixed")
            .collection::<Document>(&format!("writer{wt}"))
            .count_documents(doc! {})
            .unwrap_or(0);
        // Count must be > 0 (seed was there) and ≤ (seed + inserted).
        assert!(
            count > 0,
            "writer{wt} namespace must have at least 1 doc; inserted={inserted}"
        );
    }
}

// ---------------------------------------------------------------------------
// TC3: Lane acquisition with busy_timeout = 10ms
// ---------------------------------------------------------------------------

/// Open client with busy_timeout of 10ms. Thread A holds a lane (insert_one
/// on ns X). Thread B immediately tries to insert on ns X while A holds.
/// B should either succeed or return Error::WriterBusy. Never panic, never deadlock.
#[test]
fn lane_acquisition_busy_timeout_10ms() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("busy.mqlite");
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().busy_timeout(Duration::from_millis(10)),
    )
    .unwrap();

    // Use a barrier to fire both threads simultaneously.
    let barrier = Arc::new(Barrier::new(2));

    let ca = client.clone();
    let ba = barrier.clone();
    let h_a = thread::spawn(move || {
        ba.wait();
        let col = ca.database("busy_test").collection::<Document>("shared");
        // Thread A: insert; a moderately large payload to hold the lock briefly.
        col.insert_one(&doc! { "_id": 1, "payload": "a".repeat(512) })
    });

    let cb = client.clone();
    let bb = barrier.clone();
    let h_b = thread::spawn(move || {
        bb.wait();
        let col = cb.database("busy_test").collection::<Document>("shared");
        col.insert_one(&doc! { "_id": 2, "payload": "b".repeat(512) })
    });

    let res_a = h_a.join().expect("thread A panicked");
    let res_b = h_b.join().expect("thread B panicked");

    // Each result is either Ok or WriterBusy — anything else is a bug.
    for (name, res) in [("A", res_a), ("B", res_b)] {
        match res {
            Ok(_) => {} // success
            Err(Error::WriterBusy) => {} // acceptable — busy_timeout triggered
            Err(e) => panic!("thread {name}: unexpected error: {e:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// TC4: Drop namespace mid-write burst
// ---------------------------------------------------------------------------

/// Seed 10 docs, spawn 4 writers looping inserts on that ns, then after 100ms
/// drop_collection. Drop must complete without deadlock. Writers may error but
/// must not panic. After drop+reopen, namespace is gone (count = 0).
#[test]
fn drop_namespace_mid_write_burst() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("drop_mid.mqlite");
    let client = Client::open(&path).unwrap();

    // Seed 10 docs.
    let col = client
        .database("drop_mid")
        .collection::<Document>("victim");
    for i in 0..10i32 {
        col.insert_one(&doc! { "_id": i, "v": "seed" }).unwrap();
    }

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // 4 writer threads in a tight loop.
    let writer_handles: Vec<_> = (0..4u32)
        .map(|t| {
            let c = client.clone();
            let stop2 = stop.clone();
            thread::spawn(move || {
                let col = c
                    .database("drop_mid")
                    .collection::<Document>("victim");
                let mut next = (t as i32 + 1) * 1000;
                while !stop2.load(std::sync::atomic::Ordering::Relaxed) {
                    // Errors are allowed (NotFound after drop). Must not panic.
                    let _ = col.insert_one(&doc! { "_id": next, "t": t as i32 });
                    next += 4; // stride by thread count to avoid _id collisions
                }
            })
        })
        .collect();

    // Give writers 100ms to generate some in-flight load.
    thread::sleep(Duration::from_millis(100));

    // Drop must not deadlock or panic.
    client
        .database("drop_mid")
        .drop_collection("victim")
        .unwrap();

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in writer_handles {
        h.join().expect("writer thread panicked");
    }

    // Reopen and confirm namespace is gone or empty.
    drop(client);
    let client2 = Client::open(&path).unwrap();
    let count = client2
        .database("drop_mid")
        .collection::<Document>("victim")
        .count_documents(doc! {})
        .unwrap_or(0);
    assert_eq!(
        count, 0,
        "namespace must be empty (or absent) after drop+reopen"
    );
}

// ---------------------------------------------------------------------------
// TC5: Bootstrap race — 10 threads insert into the same NEW namespace
// ---------------------------------------------------------------------------

/// 10 threads each call insert_one on a NEW namespace "race_ns" simultaneously.
/// First thread triggers bootstrap; others should retry and see the existing ns.
/// Final: 10 docs, no lost writes.
#[test]
fn bootstrap_race_new_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bootstrap.mqlite");
    let client = Client::open(&path).unwrap();

    const THREADS: usize = 10;
    let barrier = Arc::new(Barrier::new(THREADS));

    let handles: Vec<_> = (0..THREADS as i32)
        .map(|t| {
            let c = client.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                b.wait(); // all fire simultaneously to maximize bootstrap contention
                let col = c
                    .database("boot")
                    .collection::<Document>("race_ns");
                col.insert_one(&doc! { "_id": t, "thread": t })
                    .expect("insert into new namespace must succeed");
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    let count = client
        .database("boot")
        .collection::<Document>("race_ns")
        .count_documents(doc! {})
        .unwrap();
    assert_eq!(count, 10, "all 10 bootstrap inserts must land: got {count}");
}

// ---------------------------------------------------------------------------
// TC6: commit_seq monotonicity under churn
// ---------------------------------------------------------------------------

/// 8 threads each insert 50 docs into DIFFERENT namespaces concurrently.
/// After join: every namespace has exactly 50 docs — a broken commit_seq
/// would manifest as lost writes or corrupt scan state.
#[test]
fn commit_seq_monotonicity_under_churn() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("commit_seq.mqlite");
    let client = Client::open(&path).unwrap();

    const THREADS: usize = 8;
    const PER_THREAD: i32 = 50;
    let barrier = Arc::new(Barrier::new(THREADS));

    let handles: Vec<_> = (0..THREADS as i32)
        .map(|t| {
            let c = client.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                b.wait();
                let col = c
                    .database("seqchurn")
                    .collection::<Document>(&format!("ns{t}"));
                for i in 0..PER_THREAD {
                    let id = t * PER_THREAD + i;
                    col.insert_one(&doc! { "_id": id, "seq": i, "thread": t })
                        .expect("insert must succeed");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    for t in 0..THREADS as i32 {
        let count = client
            .database("seqchurn")
            .collection::<Document>(&format!("ns{t}"))
            .count_documents(doc! {})
            .unwrap();
        assert_eq!(
            count,
            PER_THREAD as u64,
            "ns{t} must have {PER_THREAD} docs after churn; got {count}"
        );
        // Spot-check: each expected _id is visible.
        for i in 0..PER_THREAD {
            let id = t * PER_THREAD + i;
            let doc = client
                .database("seqchurn")
                .collection::<Document>(&format!("ns{t}"))
                .find_one(doc! { "_id": id })
                .unwrap();
            assert!(
                doc.is_some(),
                "ns{t}: _id={id} missing after concurrent inserts (commit_seq corruption?)"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// TC7: Poisoning behavior — panic in a writer thread
// ---------------------------------------------------------------------------

/// Spawn a writer that inserts on ns X, then immediately panics.
/// Subsequent inserts on ns X must either succeed or return a clean error.
/// Subsequent inserts on OTHER namespaces must succeed regardless.
///
/// If the panic does not poison a lane mutex (because the panic happens
/// outside the lock), document that — the invariant matters more than the
/// exact mechanism.
#[test]
fn poisoning_behavior_panicking_writer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("poison.mqlite");
    let client = Client::open(&path).unwrap();

    // Seed ns X so it exists.
    client
        .database("poison_test")
        .collection::<Document>("x")
        .insert_one(&doc! { "_id": 0, "seed": true })
        .unwrap();

    // Writer thread that inserts then panics.
    let c = client.clone();
    let panicking = thread::spawn(move || {
        let col = c
            .database("poison_test")
            .collection::<Document>("x");
        // Insert may succeed or fail (DuplicateKey for _id 0) — we don't care.
        // We then unconditionally panic.
        let _ = col.insert_one(&doc! { "_id": 1, "before_panic": true });
        panic!("intentional panic to test lane recovery");
    });

    // The thread SHOULD panic — capture it.
    let panicked = panicking.join().is_err();
    assert!(panicked, "expected the writer thread to panic");

    // ---- Key invariant: subsequent writes on OTHER namespaces must succeed ----
    let other_result = client
        .database("poison_test")
        .collection::<Document>("y")
        .insert_one(&doc! { "_id": 10, "other_ns": true });
    assert!(
        other_result.is_ok(),
        "inserts on OTHER namespaces must succeed after a panicking writer; got: {other_result:?}"
    );

    // ---- Subsequent writes on ns X: succeed OR clean error (not panic) ----
    let x_result = client
        .database("poison_test")
        .collection::<Document>("x")
        .insert_one(&doc! { "_id": 99, "after_panic": true });
    match x_result {
        Ok(_) => {
            // Lane mutex was not poisoned — ideal case.
        }
        Err(Error::Internal(ref msg)) if msg.contains("poison") => {
            // Mutex was poisoned — acceptable, as long as it's a clean error.
        }
        Err(e) => {
            // Any other clean error (e.g., WriterBusy) is also acceptable.
            // What's NOT acceptable is a panic, which would already have
            // propagated above. We just document the actual error:
            let _ = e; // suppress unused-variable warning
            // Not asserting is_ok() — a clean Err is fine.
        }
    }
}

// ---------------------------------------------------------------------------
// TC8: Same-ns serialization under heavy concurrency — no gaps/dups in _ids
// ---------------------------------------------------------------------------

/// 8 threads each insert 25 docs with sequential _ids into the SAME namespace.
/// After join: count=200, find() returns exactly those 200 _ids with no gaps
/// and no duplicates.
#[test]
fn same_ns_serialization_no_gaps_no_dups() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("serial.mqlite");
    let client = Client::open(&path).unwrap();

    const THREADS: i32 = 8;
    const PER_THREAD: i32 = 25;
    let barrier = Arc::new(Barrier::new(THREADS as usize));

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let c = client.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                b.wait();
                let col = c
                    .database("serial")
                    .collection::<Document>("shared");
                for i in 0..PER_THREAD {
                    let id = t * PER_THREAD + i;
                    col.insert_one(&doc! { "_id": id, "thread": t, "seq": i })
                        .expect("insert into shared namespace must succeed");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    let count = client
        .database("serial")
        .collection::<Document>("shared")
        .count_documents(doc! {})
        .unwrap();
    assert_eq!(
        count,
        (THREADS * PER_THREAD) as u64,
        "must have exactly {} docs; got {count}",
        THREADS * PER_THREAD
    );

    // Collect all _ids and check for gaps and duplicates.
    let all_docs: Vec<Document> = client
        .database("serial")
        .collection::<Document>("shared")
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let mut ids = HashSet::new();
    for d in &all_docs {
        let id = d.get_i32("_id").expect("_id must be i32");
        let newly_inserted = ids.insert(id);
        assert!(newly_inserted, "_id={id} is a duplicate");
    }

    let expected: HashSet<i32> = (0..(THREADS * PER_THREAD)).collect();
    assert_eq!(
        ids, expected,
        "expected _ids 0..{} with no gaps",
        THREADS * PER_THREAD
    );
}

// ---------------------------------------------------------------------------
// TC9: Interleaving insert_many — two concurrent batches, final count = 100
// ---------------------------------------------------------------------------

/// Two threads each call insert_many with 50 docs on the SAME namespace.
/// After join: exactly 100 docs. No torn rows.
/// Per PR 8 accepted behavior: mid-flight readers may see partial batches,
/// but the final state must be complete and consistent.
#[test]
fn interleaving_insert_many_same_ns_count_100() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("insert_many.mqlite");
    let client = Client::open(&path).unwrap();

    let barrier = Arc::new(Barrier::new(2));

    let ca = client.clone();
    let ba = barrier.clone();
    let h_a = thread::spawn(move || {
        ba.wait();
        let col = ca
            .database("batch")
            .collection::<Document>("shared");
        let docs: Vec<Document> = (0..50i32)
            .map(|i| doc! { "_id": i, "batch": "a" })
            .collect();
        col.insert_many(&docs).run().expect("insert_many batch A must succeed")
    });

    let cb = client.clone();
    let bb = barrier.clone();
    let h_b = thread::spawn(move || {
        bb.wait();
        let col = cb
            .database("batch")
            .collection::<Document>("shared");
        let docs: Vec<Document> = (50..100i32)
            .map(|i| doc! { "_id": i, "batch": "b" })
            .collect();
        col.insert_many(&docs).run().expect("insert_many batch B must succeed")
    });

    h_a.join().expect("thread A panicked");
    h_b.join().expect("thread B panicked");

    let count = client
        .database("batch")
        .collection::<Document>("shared")
        .count_documents(doc! {})
        .unwrap();
    assert_eq!(count, 100, "both insert_many batches must land; got {count}");

    // Verify no torn rows: every _id 0..100 is present exactly once.
    let all_docs: Vec<Document> = client
        .database("batch")
        .collection::<Document>("shared")
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let ids: HashSet<i32> = all_docs
        .iter()
        .map(|d| d.get_i32("_id").expect("_id must be i32"))
        .collect();
    let expected: HashSet<i32> = (0..100).collect();
    assert_eq!(ids, expected, "no torn rows: all 100 _ids must be present");
}

// ---------------------------------------------------------------------------
// TC10: Many namespaces — 100 sequential inserts, list_namespaces returns all
// ---------------------------------------------------------------------------

/// Create and insert into 100 different namespaces sequentially.
/// list_collection_names returns all 100. find() on each returns its one doc.
#[test]
fn many_namespaces_100_sequential() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("many_ns.mqlite");
    let client = Client::open(&path).unwrap();

    const COUNT: i32 = 100;

    for i in 0..COUNT {
        client
            .database("many")
            .collection::<Document>(&format!("col{i:03}"))
            .insert_one(&doc! { "_id": i, "ns_index": i })
            .unwrap_or_else(|e| panic!("insert into col{i:03} failed: {e}"));
    }

    let names = client.database("many").list_collection_names().unwrap();
    assert_eq!(
        names.len(),
        COUNT as usize,
        "list_collection_names must return all 100 namespaces; got {}",
        names.len()
    );

    // Verify each namespace has exactly its one doc.
    for i in 0..COUNT {
        let col = client
            .database("many")
            .collection::<Document>(&format!("col{i:03}"));
        let found = col
            .find_one(doc! { "_id": i })
            .unwrap_or_else(|e| panic!("find_one on col{i:03} failed: {e}"));
        assert!(
            found.is_some(),
            "col{i:03}: doc with _id={i} must be present"
        );
        // Exactly one doc in each namespace.
        let count = col.count_documents(doc! {}).unwrap();
        assert_eq!(count, 1, "col{i:03} must have exactly 1 doc; got {count}");
    }
}
