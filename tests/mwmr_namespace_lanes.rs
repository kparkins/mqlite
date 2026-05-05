//! PR 8 acceptance gates: namespace lanes + commit publication.
//!
//! These tests exercise the in-process MWMR contract:
//! - Writers on DIFFERENT namespaces overlap at deterministic body-entry hooks.
//! - Writers on the SAME namespace serialize (don't corrupt each other).
//! - drop_namespace waits for in-flight writers and recovers cleanly.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use bson::doc;
use bson::Document;
use mqlite::Client;
#[cfg(feature = "test-hooks")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "test-hooks")]
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::thread;

/// Two writers on DIFFERENT namespaces enter their write bodies concurrently.
#[cfg(feature = "test-hooks")]
#[test]
fn different_namespace_writers_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lanes.mqlite");
    let client = Client::open(&path).unwrap();

    const N: i32 = 1;

    let db = client.database("bench");
    db.create_collection("a").unwrap();
    db.create_collection("b").unwrap();

    let mut hook_a = client.__install_write_body_entry_hook("bench.a");
    let mut hook_b = client.__install_write_body_entry_hook("bench.b");
    let ca = client.clone();
    let cb = client.clone();
    let h_a = thread::spawn(move || {
        let col = ca.database("bench").collection::<Document>("a");
        for i in 0..N {
            col.insert_one(&doc! { "_id": i, "v": format!("a-{i}") })
                .unwrap();
        }
    });

    hook_a.wait_until_entered().unwrap();

    let h_b = thread::spawn(move || {
        let col = cb.database("bench").collection::<Document>("b");
        for i in 0..N {
            col.insert_one(&doc! { "_id": i, "v": format!("b-{i}") })
                .unwrap();
        }
    });

    hook_b.wait_until_entered().unwrap();
    hook_a.release().unwrap();
    h_a.join().unwrap();
    hook_b.release().unwrap();
    h_b.join().unwrap();

    // Sanity: both namespaces have N docs.
    let count_a = client
        .database("bench")
        .collection::<Document>("a")
        .count_documents(doc! {})
        .unwrap();
    let count_b = client
        .database("bench")
        .collection::<Document>("b")
        .count_documents(doc! {})
        .unwrap();
    assert_eq!(count_a, N as u64);
    assert_eq!(count_b, N as u64);
}

/// Same-namespace writers cannot both enter their write bodies at once.
#[cfg(feature = "test-hooks")]
#[test]
fn same_namespace_writers_serialize_with_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("same_ns_barrier.mqlite");
    let client = Client::open(&path).unwrap();

    let ns = "bench.shared";
    client
        .database("bench")
        .create_collection("shared")
        .unwrap();

    let first_released = Arc::new(AtomicBool::new(false));
    let mut hook_a = client.__install_write_body_entry_hook(ns);
    let mut hook_b =
        client.__install_write_body_entry_hook_observing(ns, Arc::clone(&first_released));

    let ca = client.clone();
    let h_a = thread::spawn(move || {
        ca.database("bench")
            .collection::<Document>("shared")
            .insert_one(&doc! { "_id": 1, "v": "a" })
            .unwrap();
    });

    hook_a.wait_until_entered().unwrap();

    let (started_tx, started_rx) = mpsc::channel();
    let cb = client.clone();
    let h_b = thread::spawn(move || {
        started_tx.send(()).unwrap();
        cb.database("bench")
            .collection::<Document>("shared")
            .insert_one(&doc! { "_id": 2, "v": "b" })
            .unwrap();
    });

    started_rx.recv().unwrap();
    hook_b.assert_not_entered().unwrap();
    first_released.store(true, Ordering::Release);
    hook_a.release().unwrap();
    h_a.join().unwrap();

    let event_b = hook_b.wait_until_entered().unwrap();
    assert_eq!(
        event_b.observed_flag(),
        Some(true),
        "same-namespace writer entered before the first writer was released",
    );
    hook_b.release().unwrap();
    h_b.join().unwrap();

    let count = client
        .database("bench")
        .collection::<Document>("shared")
        .count_documents(doc! {})
        .unwrap();
    assert_eq!(count, 2);
}

/// Same-namespace writers must serialize (no torn writes, full count).
#[test]
fn same_namespace_writers_serialize() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("same_ns.mqlite");
    let client = Client::open(&path).unwrap();

    const THREADS: i32 = 4;
    const PER_THREAD: i32 = 100;

    let barrier = Arc::new(Barrier::new(THREADS as usize));
    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let c = client.clone();
            let b = barrier.clone();
            thread::spawn(move || {
                b.wait();
                let col = c.database("d").collection::<Document>("shared");
                for i in 0..PER_THREAD {
                    let id = t * PER_THREAD + i;
                    col.insert_one(&doc! { "_id": id, "thread": t, "i": i })
                        .unwrap();
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    let count = client
        .database("d")
        .collection::<Document>("shared")
        .count_documents(doc! {})
        .unwrap();
    assert_eq!(
        count,
        (THREADS * PER_THREAD) as u64,
        "all writes must land — same-namespace lane serialization broken",
    );
}

/// drop_namespace called concurrently with writes on that namespace must
/// wait the writers out and complete cleanly. Phase 5 §10.1.1 retired the
/// legacy `dropped_namespaces` resurrection guard, so the post-F5 contract
/// is: after `drop_namespace` returns and reopen, durable id monotonicity
/// guarantees pre-drop documents are gone. Any post-drop writer that races
/// past the drop bootstraps a fresh incarnation of the same name with a
/// strictly greater `CollectionEntry.id`, so leaked rows (if any) carry
/// only post-drop data and never pre-drop seed data.
#[test]
fn drop_namespace_waits_for_writers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("drop_ns.mqlite");
    let client = Client::open(&path).unwrap();

    // Seed.
    let col = client
        .database("victim_db")
        .collection::<Document>("victim_coll");
    for i in 0..50 {
        col.insert_one(&doc! { "_id": i, "v": "seed" }).unwrap();
    }

    // Writer thread: insert in a tight loop until told to stop.
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_stop = stop.clone();
    let writer_client = client.clone();
    let writer = thread::spawn(move || {
        let col = writer_client
            .database("victim_db")
            .collection::<Document>("victim_coll");
        let mut next = 1000;
        while !writer_stop.load(std::sync::atomic::Ordering::Relaxed) {
            // Insert may legitimately fail with NotFound after drop — that's OK.
            let _ = col.insert_one(&doc! { "_id": next, "v": "live" });
            next += 1;
        }
    });

    // Let the writer get going.
    thread::sleep(std::time::Duration::from_millis(50));

    // Issue drop. Should not error, should not deadlock.
    client
        .database("victim_db")
        .drop_collection("victim_coll")
        .unwrap();

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();

    // Reopen: pre-drop seed documents must be gone — durable id monotonicity
    // (Phase 1 §10.7, Phase 5 §10.1.1 F5 retirement) isolates incarnations.
    drop(client);
    let client2 = Client::open(&path).unwrap();
    let col2 = client2
        .database("victim_db")
        .collection::<Document>("victim_coll");
    let seed_count = col2
        .count_documents(doc! { "v": "seed" })
        .expect("count by seed marker");
    assert_eq!(
        seed_count, 0,
        "pre-drop seed documents must not survive drop+reopen",
    );
}
