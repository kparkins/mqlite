//! PR 10 edge-case tests — FullSync journal fsync-only + dropped_namespaces
//! guard + page-0 re-read on recovery.
//!
//! All tests use `tempfile::tempdir()` so that each run starts with a clean
//! file.  None of these tests modify `src/`.
//!
//! # Notes on "drop without checkpoint"
//!
//! The doc comment on `Client::drop` states: "Checkpoints when this is the
//! last handle."  In practice, dropping the last `Client` handle DOES run
//! `checkpoint()`.  These tests therefore verify the complete
//! open → write → (implicit checkpoint on drop) → reopen cycle, not a raw
//! OS-kill crash scenario.  They remain valid regression gates for the
//! durability contract.
//!
//! # DurabilityMode variants
//!
//! - `DurabilityMode::FullSync`             — fdatasync after every commit
//! - `DurabilityMode::Interval(Duration)`   — flush every N ms (default 100 ms)
//! - `DurabilityMode::None`                 — no explicit flush

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use bson::doc;
use bson::Document;
use mqlite::{Client, DurabilityMode, Error, OpenOptions};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn fullsync_opts() -> OpenOptions {
    OpenOptions::new().durability(DurabilityMode::FullSync)
}

fn interval_opts() -> OpenOptions {
    OpenOptions::new().durability(DurabilityMode::Interval(Duration::from_millis(100)))
}

fn nosync_opts() -> OpenOptions {
    OpenOptions::new().durability(DurabilityMode::None)
}

// ---------------------------------------------------------------------------
// TC1 — FullSync survival without explicit checkpoint (N=100, content check)
// ---------------------------------------------------------------------------

#[test]
fn tc1_fullsync_100_docs_content_integrity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc1.mqlite");

    // Write phase
    {
        let client = Client::open_with_options(&path, fullsync_opts()).unwrap();
        let col = client.database("tc1db").collection::<Document>("col");
        for i in 0..100i32 {
            col.insert_one(&doc! {
                "_id": i,
                "name": format!("item-{i}"),
                "value": i * 10,
            })
            .unwrap();
        }
        // Intentional implicit drop — checkpoint runs on last handle.
    }

    // Recovery phase
    let client = Client::open(&path).unwrap();
    let col = client.database("tc1db").collection::<Document>("col");

    let count = col.count_documents(doc! {}).unwrap();
    assert_eq!(count, 100, "all 100 FullSync docs must survive drop+reopen");

    // Content integrity: every doc must have the expected _id, name, and value.
    for i in 0..100i32 {
        let doc = col
            .find_one(doc! { "_id": i })
            .unwrap()
            .unwrap_or_else(|| panic!("doc _id={i} missing after reopen"));
        assert_eq!(
            doc.get_str("name").unwrap(),
            format!("item-{i}"),
            "name mismatch for _id={i}"
        );
        assert_eq!(
            doc.get_i32("value").unwrap(),
            i * 10,
            "value mismatch for _id={i}"
        );
    }
}

// ---------------------------------------------------------------------------
// TC2 — Mixed durability + recovery: FullSync batch then Interval batch
// ---------------------------------------------------------------------------

#[test]
fn tc2_mixed_durability_fullsync_then_interval() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc2.mqlite");

    // Phase 1: FullSync — insert 50 docs
    {
        let client = Client::open_with_options(&path, fullsync_opts()).unwrap();
        let col = client.database("tc2db").collection::<Document>("items");
        for i in 0..50i32 {
            col.insert_one(&doc! { "_id": i, "batch": "fullsync" })
                .unwrap();
        }
        // Drop → implicit checkpoint.
    }

    // Phase 2: Interval (NormalSync equivalent) — insert another 50 docs
    {
        let client = Client::open_with_options(&path, interval_opts()).unwrap();
        let col = client.database("tc2db").collection::<Document>("items");
        for i in 50..100i32 {
            col.insert_one(&doc! { "_id": i, "batch": "interval" })
                .unwrap();
        }
        // Drop → implicit checkpoint.
    }

    // Recovery phase
    let client = Client::open(&path).unwrap();
    let col = client.database("tc2db").collection::<Document>("items");

    // FullSync docs MUST all survive.
    let fullsync_count = col
        .count_documents(doc! { "batch": "fullsync" })
        .unwrap();
    assert_eq!(
        fullsync_count, 50,
        "all 50 FullSync docs must survive; got {fullsync_count}"
    );

    // Interval docs: count is in [0, 50] — we do not require a specific value.
    let interval_count = col
        .count_documents(doc! { "batch": "interval" })
        .unwrap();
    assert!(
        interval_count <= 50,
        "interval batch count must not exceed 50; got {interval_count}"
    );
    // (lower bound of 0 is implied by u64)

    // Total must be at least the guaranteed FullSync batch.
    let total = col.count_documents(doc! {}).unwrap();
    assert!(
        total >= 50,
        "total count must be >= 50 (FullSync floor); got {total}"
    );
}

// ---------------------------------------------------------------------------
// TC3 — drop_namespace persists across drop+reopen
// ---------------------------------------------------------------------------

#[test]
fn tc3_drop_namespace_persists_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc3.mqlite");

    // Seed phase: insert 20 docs into "victims".
    {
        let client = Client::open_with_options(&path, fullsync_opts()).unwrap();
        let col = client
            .database("tc3db")
            .collection::<Document>("victims");
        for i in 0..20i32 {
            col.insert_one(&doc! { "_id": i, "data": format!("row-{i}") })
                .unwrap();
        }
        assert_eq!(col.count_documents(doc! {}).unwrap(), 20);

        // Drop the collection.
        client
            .database("tc3db")
            .drop_collection("victims")
            .expect("drop_collection must succeed");

        // Verify it's gone within the same session.
        assert_eq!(
            col.count_documents(doc! {}).unwrap(),
            0,
            "collection must be empty immediately after drop"
        );
        // Drop → implicit checkpoint.
    }

    // Recovery phase
    let client = Client::open(&path).unwrap();
    let db = client.database("tc3db");

    // Collection must be absent from the catalog.
    let names = db.list_collection_names().expect("list_collection_names");
    assert!(
        !names.iter().any(|n| n == "victims"),
        "dropped collection 'victims' must not appear after reopen; found: {names:?}"
    );

    // Count must be 0.
    let col = db.collection::<Document>("victims");
    assert_eq!(
        col.count_documents(doc! {}).unwrap(),
        0,
        "dropped collection must have 0 docs after reopen"
    );
}

// ---------------------------------------------------------------------------
// TC4 — Create-after-drop same name (5 cycles)
// ---------------------------------------------------------------------------

#[test]
fn tc4_create_after_drop_same_name_five_cycles() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc4.mqlite");

    for cycle in 0..5u32 {
        let client = Client::open_with_options(&path, fullsync_opts()).unwrap();
        let db = client.database("tc4db");

        // Drop any pre-existing collection from the previous cycle.
        // On cycle 0 the collection doesn't exist yet; ignore CollectionNotFound.
        match db.drop_collection("x") {
            Ok(()) => {}
            Err(Error::CollectionNotFound { .. }) => {}
            Err(e) => panic!("unexpected error dropping 'x' on cycle {cycle}: {e:?}"),
        }

        // Explicitly re-create the collection after the drop.
        // `create_collection` clears the dropped_namespaces guard so that
        // subsequent inserts are not blocked by the same-session protection.
        db.create_collection("x").expect("create_collection after drop");

        // Obtain a fresh handle AFTER the drop+create.
        let col = db.collection::<Document>("x");

        // Fresh insert.
        col.insert_one(&doc! { "cycle": cycle as i32 }).unwrap();

        let count = col.count_documents(doc! {}).unwrap();
        assert_eq!(
            count, 1,
            "cycle {cycle}: after drop+insert, count must be 1; got {count}"
        );

        // Verify no docs from previous cycles leaked.
        let old = col
            .count_documents(doc! { "cycle": { "$lt": cycle as i32 } })
            .unwrap();
        assert_eq!(
            old, 0,
            "cycle {cycle}: no docs from previous cycles must be present"
        );
        // Drop → implicit checkpoint.
    }
}

// ---------------------------------------------------------------------------
// TC5 — Page-0 correctness after journal recovery (catalog root integrity)
// ---------------------------------------------------------------------------

#[test]
fn tc5_page0_correctness_after_journal_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc5.mqlite");

    let index_name = "seq_idx";

    // Write phase: FullSync, create collection + index.
    {
        let client = Client::open_with_options(&path, fullsync_opts()).unwrap();
        let db = client.database("tc5db");
        let col = db.collection::<Document>("catalog_ns");

        for i in 0..30i32 {
            col.insert_one(&doc! { "_id": i, "seq": i }).unwrap();
        }

        // Create an index so catalog page 0 must reference it after replay.
        let model = mqlite::IndexModel::builder()
            .keys(doc! { "seq": 1i32 })
            .options(
                mqlite::IndexOptions::new()
                    .name(index_name.to_string()),
            )
            .build()
            .unwrap();
        col.create_index(model).expect("create seq index");

        // Drop → implicit checkpoint.
    }

    // Recovery phase
    let client = Client::open(&path).unwrap();
    let db = client.database("tc5db");

    // list_collection_names must include our collection (catalog root intact).
    let names = db.list_collection_names().expect("list_collection_names");
    assert!(
        names.iter().any(|n| n == "catalog_ns"),
        "catalog_ns must appear in list_collection_names after recovery; got: {names:?}"
    );

    // Indexes must be intact.
    let col = db.collection::<Document>("catalog_ns");
    let indexes = col.list_indexes().expect("list_indexes");
    let found = indexes.iter().find(|i| i.name == index_name);
    assert!(
        found.is_some(),
        "index '{index_name}' must survive journal recovery; found: {:?}",
        indexes.iter().map(|i| &i.name).collect::<Vec<_>>()
    );

    // Docs must be present.
    assert_eq!(
        col.count_documents(doc! {}).unwrap(),
        30,
        "30 docs must survive journal recovery"
    );
}

// ---------------------------------------------------------------------------
// TC6 — Concurrent FullSync writes, 8 threads × 25 docs → 200 total
// ---------------------------------------------------------------------------

#[test]
fn tc6_concurrent_fullsync_8_threads_200_docs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc6.mqlite");

    const THREADS: usize = 8;
    const DOCS_PER_THREAD: i32 = 25;

    let client = Arc::new(Client::open_with_options(&path, fullsync_opts()).unwrap());
    let barrier = Arc::new(Barrier::new(THREADS));

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let c = Arc::clone(&client);
            let b = Arc::clone(&barrier);
            thread::spawn(move || {
                b.wait(); // all threads start together
                let ns_name = format!("ns{t}");
                let col = c.database("tc6db").collection::<Document>(&ns_name);
                for i in 0..DOCS_PER_THREAD {
                    col.insert_one(&doc! {
                        "_id": i,
                        "thread": t as i32,
                        "seq": i,
                    })
                    .expect("insert failed");
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("thread panicked");
    }

    // Drop client → implicit checkpoint.
    drop(client);

    // Recovery phase
    let client = Client::open(&path).unwrap();
    let mut total: u64 = 0;
    for t in 0..THREADS {
        let ns_name = format!("ns{t}");
        let col = client.database("tc6db").collection::<Document>(&ns_name);
        let count = col.count_documents(doc! {}).unwrap();
        assert_eq!(
            count, DOCS_PER_THREAD as u64,
            "thread {t} namespace must have {DOCS_PER_THREAD} docs after reopen; got {count}"
        );
        total += count;
    }
    assert_eq!(
        total, 200,
        "all 200 docs across 8 namespaces must survive reopen"
    );
}

// ---------------------------------------------------------------------------
// TC7 — FullSync + drop_namespace + reopen: A gone, B intact
// ---------------------------------------------------------------------------

#[test]
fn tc7_fullsync_drop_a_keep_b() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc7.mqlite");

    // Write phase
    {
        let client = Client::open_with_options(&path, fullsync_opts()).unwrap();
        let db = client.database("tc7db");

        let col_a = db.collection::<Document>("ns_a");
        let col_b = db.collection::<Document>("ns_b");

        for i in 0..50i32 {
            col_a.insert_one(&doc! { "_id": i, "src": "a" }).unwrap();
            col_b.insert_one(&doc! { "_id": i, "src": "b" }).unwrap();
        }

        // Drop A only.
        db.drop_collection("ns_a").expect("drop ns_a");

        // Verify within session
        assert_eq!(
            col_a.count_documents(doc! {}).unwrap(),
            0,
            "ns_a must be 0 immediately after drop"
        );
        assert_eq!(
            col_b.count_documents(doc! {}).unwrap(),
            50,
            "ns_b must still have 50 docs"
        );
        // Drop → implicit checkpoint.
    }

    // Recovery phase
    let client = Client::open(&path).unwrap();
    let db = client.database("tc7db");

    // ns_a must be gone.
    let col_a = db.collection::<Document>("ns_a");
    assert_eq!(
        col_a.count_documents(doc! {}).unwrap(),
        0,
        "ns_a must have 0 docs after reopen"
    );
    let names = db.list_collection_names().expect("list");
    assert!(
        !names.iter().any(|n| n == "ns_a"),
        "ns_a must not appear in list_collection_names; got: {names:?}"
    );

    // ns_b must have all 50.
    let col_b = db.collection::<Document>("ns_b");
    assert_eq!(
        col_b.count_documents(doc! {}).unwrap(),
        50,
        "ns_b must have all 50 docs after reopen"
    );

    // Spot-check a B doc.
    let doc = col_b
        .find_one(doc! { "_id": 25 })
        .unwrap()
        .expect("doc _id=25 in ns_b must exist");
    assert_eq!(doc.get_str("src").unwrap(), "b");
}

// ---------------------------------------------------------------------------
// TC8 — FullSync drop latency: client drop must complete in < 5 s
// ---------------------------------------------------------------------------

#[test]
fn tc8_fullsync_drop_latency() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc8.mqlite");

    let client = Client::open_with_options(&path, fullsync_opts()).unwrap();
    let col = client.database("tc8db").collection::<Document>("items");

    for i in 0..500i32 {
        col.insert_one(&doc! { "_id": i, "v": i }).unwrap();
    }

    let t = Instant::now();
    drop(client); // checkpoint + close
    let elapsed = t.elapsed();

    assert!(
        elapsed < Duration::from_secs(5),
        "Client::drop must complete within 5 s; took {:?}",
        elapsed
    );
}

// ---------------------------------------------------------------------------
// TC9 — Emergency checkpoint (journal fill) must not deadlock on drop
// ---------------------------------------------------------------------------

#[test]
fn tc9_journal_fill_no_deadlock_on_drop() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc9.mqlite");

    // Use a small journal_max_size to trigger emergency checkpoints quickly.
    let opts = OpenOptions::new()
        .durability(DurabilityMode::FullSync)
        .journal_max_size(256 * 1024) // 256 KB — triggers checkpoints frequently
        .journal_auto_checkpoint(50);  // checkpoint every 50 pages

    let client = Client::open_with_options(&path, opts).unwrap();
    let col = client.database("tc9db").collection::<Document>("stress");

    // Insert enough data to exercise the emergency checkpoint path multiple times.
    for i in 0..500i32 {
        col.insert_one(&doc! {
            "_id": i,
            "payload": "x".repeat(200),
        })
        .unwrap();
    }

    // Drop must complete without deadlock.
    let t = Instant::now();
    drop(client);
    let elapsed = t.elapsed();

    assert!(
        elapsed < Duration::from_secs(10),
        "Client::drop must not deadlock or hang; took {:?}",
        elapsed
    );
}

// ---------------------------------------------------------------------------
// TC10 — Sync-mode roundtrip: write with None, reopen with FullSync
// ---------------------------------------------------------------------------

#[test]
fn tc10_sync_mode_roundtrip_nosync_then_fullsync() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tc10.mqlite");

    // Write with NoSync (DurabilityMode::None)
    {
        let client = Client::open_with_options(&path, nosync_opts()).unwrap();
        let col = client.database("tc10db").collection::<Document>("items");
        for i in 0..50i32 {
            col.insert_one(&doc! { "_id": i, "mode": "nosync" })
                .unwrap();
        }
        // Explicit close to guarantee flush (since NoSync doesn't fdatasync).
        client.close().expect("close must succeed");
    }

    // Reopen with FullSync — data must still be readable.
    {
        let client = Client::open_with_options(&path, fullsync_opts()).unwrap();
        let col = client.database("tc10db").collection::<Document>("items");

        let count = col.count_documents(doc! {}).unwrap();
        assert_eq!(
            count, 50,
            "50 docs written with NoSync must be readable after reopen with FullSync; got {count}"
        );

        // Spot-check content integrity.
        let doc = col
            .find_one(doc! { "_id": 25 })
            .unwrap()
            .expect("doc _id=25 must be present");
        assert_eq!(
            doc.get_str("mode").unwrap(),
            "nosync",
            "mode field must match"
        );

        // Insert a new doc under FullSync to verify the engine is not corrupted.
        col.insert_one(&doc! { "_id": 50i32, "mode": "fullsync" })
            .unwrap();

        let count2 = col.count_documents(doc! {}).unwrap();
        assert_eq!(
            count2, 51,
            "after adding 1 FullSync doc, total must be 51; got {count2}"
        );
    }
}
