#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

//! Phase-0 observability counter tests (US-001).
//!
//! Each test validates that the named counter in `mqlite::mvcc::metrics`
//! moves exactly as specified by the PRD acceptance criteria. Tests reset
//! the counter first, perform the action under test, then assert the delta.
//!
//! The lane-wait vs commit_seq-wait split is exercised by two dedicated
//! concurrent-writer tests.
//!
//! All tests acquire `COUNTER_SERIAL` before touching process-global
//! counters so concurrent rust-test parallelism cannot race on the
//! reset / record / snapshot sequences.

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, OpenOptions};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use mqlite::mvcc::metrics::{
    commit_seq_wait_ns_snapshot, crud_commits_root_changing_snapshot,
    crud_commits_root_neutral_snapshot, emergency_checkpoint_triggers_snapshot,
    lane_wait_ns_snapshot, published_snapshot_rebuilds_snapshot,
    recovery_chain_commit_frames_snapshot, recovery_legacy_page_frames_snapshot,
    reset_commit_seq_wait_ns, reset_crud_commits_root_changing, reset_crud_commits_root_neutral,
    reset_emergency_checkpoint_triggers, reset_lane_wait_ns, reset_published_snapshot_rebuilds,
    reset_recovery_chain_commit_frames, reset_recovery_legacy_page_frames,
};

#[path = "crash_harness.rs"]
mod crash_harness;

/// Process-global test serialization lock.
///
/// The Phase-0 counters are process-global atomics. Rust's default test
/// runner spawns test fns in parallel threads within a single process, so
/// two tests that both reset+record+snapshot the same counter will race
/// on their own reset calls. Every test in this file locks this mutex for
/// its entire duration.
static COUNTER_SERIAL: Mutex<()> = Mutex::new(());

// ---------------------------------------------------------------------------
// Counter 1 — published_snapshot_rebuilds_total
// ---------------------------------------------------------------------------
//
// Phase 1 US-006 changed this counter's semantics: it now ticks only
// when `dirty.published_catalog_dirty == true` at `publish_commit` —
// i.e. when a fresh `Arc<PublishedCatalog>` was actually built. Under
// Phase 0 it ticked once per CRUD commit; under Phase 1 root-neutral
// CRUD reuses the existing Arc and does NOT tick. To keep the counter
// covered by a dedicated test, we exercise N DDL commits
// (`create_collection`) — every DDL publish sets both flags so the
// rebuild counter reliably rises by N. The pure "tick-per-CRUD-commit"
// assertion is no longer correct and would contradict §4.1 / §10.3.

#[test]
fn published_snapshot_rebuilds_ticks_once_per_rebuild_publish() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("psr.mqlite");
    let client = Client::open(&path).unwrap();

    reset_published_snapshot_rebuilds();
    let before = published_snapshot_rebuilds_snapshot();

    // Every create_collection is a DDL publish that sets both dirty
    // flags → one rebuild tick per call.
    const N: i32 = 10;
    for i in 0..N {
        client
            .database("psrdb")
            .create_collection(&format!("c{}", i))
            .unwrap();
    }

    let after = published_snapshot_rebuilds_snapshot();
    let delta = after - before;
    assert_eq!(
        delta, N as u64,
        "published_snapshot_rebuilds_total must rise by exactly N={} after N \
         DDL publishes that set published_catalog_dirty; saw delta={}",
        N, delta
    );
}

// ---------------------------------------------------------------------------
// Counters 2a/2b — root-neutral vs root-changing CRUD commits
// ---------------------------------------------------------------------------

#[test]
fn root_neutral_crud_commit_ticks_neutral_not_changing() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rn.mqlite");
    let client = Client::open(&path).unwrap();
    let col = client.database("rndb").collection::<Document>("items");

    // Prime the namespace so subsequent inserts do not bootstrap. Bootstrap
    // goes through run_ddl, which does NOT tick the CRUD split counters.
    col.insert_one(&doc! { "_id": 0i32, "v": 0 }).unwrap();

    reset_crud_commits_root_neutral();
    reset_crud_commits_root_changing();

    // A small insert into an already-bootstrapped tree should not split
    // the root, so sync_catalog_root_overlay is never called and the
    // commit is root-neutral.
    col.insert_one(&doc! { "_id": 1i32, "v": 1 }).unwrap();

    let neutral = crud_commits_root_neutral_snapshot();
    let changing = crud_commits_root_changing_snapshot();
    assert_eq!(
        neutral, 1,
        "small insert must tick root-neutral exactly once; neutral={}, changing={}",
        neutral, changing
    );
    assert_eq!(
        changing, 0,
        "small insert must NOT tick root-changing; neutral={}, changing={}",
        neutral, changing
    );
}

#[test]
fn root_changing_crud_commit_ticks_changing_counter() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rc.mqlite");
    let client = Client::open(&path).unwrap();
    let col = client.database("rcdb").collection::<Document>("items");

    // Prime the namespace.
    col.insert_one(&doc! { "_id": 0i32, "v": 0 }).unwrap();

    reset_crud_commits_root_neutral();
    reset_crud_commits_root_changing();

    // Large payloads maximise fan-out pressure so the tree root eventually
    // splits and sync_catalog_root_overlay fires on at least one commit.
    for i in 100..400i32 {
        col.insert_one(&doc! {
            "_id": i,
            "payload": "x".repeat(1024),
        })
        .unwrap();
    }
    let changing = crud_commits_root_changing_snapshot();
    assert!(
        changing > 0,
        "at least one large-insert CRUD commit must persist an updated \
         tree-root (root-changing > 0); saw changing={}",
        changing
    );
}

// ---------------------------------------------------------------------------
// Counter 3a — lane_wait_ns_total (same-namespace contention)
// ---------------------------------------------------------------------------

#[test]
fn lane_wait_ns_total_rises_with_same_namespace_writers() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("lane_wait.mqlite");
    let client = Client::open(&path).unwrap();

    // Pre-create the namespace so the writers do not race on bootstrap.
    let col = client.database("lwdb").collection::<Document>("sharedns");
    col.insert_one(&doc! { "_id": -1i32, "v": 0 }).unwrap();

    reset_lane_wait_ns();
    reset_commit_seq_wait_ns();

    const N: i32 = 200;
    let barrier = Arc::new(Barrier::new(2));
    let c1 = client.clone();
    let b1 = Arc::clone(&barrier);
    let t1 = thread::spawn(move || {
        let col = c1.database("lwdb").collection::<Document>("sharedns");
        b1.wait();
        for i in 0..N {
            col.insert_one(&doc! { "_id": i, "payload": "x".repeat(256) })
                .unwrap();
        }
    });
    let c2 = client.clone();
    let b2 = Arc::clone(&barrier);
    let t2 = thread::spawn(move || {
        let col = c2.database("lwdb").collection::<Document>("sharedns");
        b2.wait();
        for i in N..(2 * N) {
            col.insert_one(&doc! { "_id": i, "payload": "x".repeat(256) })
                .unwrap();
        }
    });
    t1.join().unwrap();
    t2.join().unwrap();

    let lane_wait = lane_wait_ns_snapshot();
    assert!(
        lane_wait > 0,
        "two writers on the SAME namespace must record nonzero lane_wait_ns_total; \
         saw lane_wait_ns_total={}",
        lane_wait
    );
}

// ---------------------------------------------------------------------------
// Counter 3b — commit_seq_wait_ns_total (different-namespace contention)
// ---------------------------------------------------------------------------

#[test]
fn commit_seq_wait_ns_total_rises_with_different_namespace_writers() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("csq_wait.mqlite");
    let client = Client::open(&path).unwrap();

    // Pre-create both namespaces so the writers do not race on bootstrap
    // (which itself contends on commit_seq and would pollute the lane/seq
    // split).
    let col_a = client.database("csqdb").collection::<Document>("ns_a");
    col_a.insert_one(&doc! { "_id": -1i32, "v": 0 }).unwrap();
    let col_b = client.database("csqdb").collection::<Document>("ns_b");
    col_b.insert_one(&doc! { "_id": -1i32, "v": 0 }).unwrap();

    reset_lane_wait_ns();
    reset_commit_seq_wait_ns();

    const N: i32 = 200;
    let barrier = Arc::new(Barrier::new(2));
    let c1 = client.clone();
    let b1 = Arc::clone(&barrier);
    let t1 = thread::spawn(move || {
        let col = c1.database("csqdb").collection::<Document>("ns_a");
        b1.wait();
        for i in 0..N {
            col.insert_one(&doc! { "_id": i, "payload": "x".repeat(256) })
                .unwrap();
        }
    });
    let c2 = client.clone();
    let b2 = Arc::clone(&barrier);
    let t2 = thread::spawn(move || {
        let col = c2.database("csqdb").collection::<Document>("ns_b");
        b2.wait();
        for i in 0..N {
            col.insert_one(&doc! { "_id": i, "payload": "x".repeat(256) })
                .unwrap();
        }
    });
    t1.join().unwrap();
    t2.join().unwrap();

    let lane_wait = lane_wait_ns_snapshot();
    let commit_seq_wait = commit_seq_wait_ns_snapshot();
    assert!(
        commit_seq_wait > 0,
        "two writers on DIFFERENT namespaces must record nonzero \
         commit_seq_wait_ns_total; saw commit_seq_wait_ns_total={}",
        commit_seq_wait
    );
    // Lanes are disjoint, so lane contention should be near-zero relative
    // to commit_seq contention. Allow 10% headroom for noise.
    assert!(
        lane_wait < commit_seq_wait,
        "disjoint-namespace writers should record lane_wait < commit_seq_wait; \
         saw lane_wait={}, commit_seq_wait={}",
        lane_wait,
        commit_seq_wait
    );
}

// ---------------------------------------------------------------------------
// Counters 4a/4b — recovery_legacy_page_frames_total and
// recovery_chain_commit_frames_total
// ---------------------------------------------------------------------------

#[test]
fn recovery_frame_counters_rise_on_reopen_after_workload() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("recov.mqlite");

    // Build up a non-empty journal via a known workload, then drop the
    // client WITHOUT calling close() so the journal survives (close would
    // checkpoint everything into the main file).
    {
        let client = Client::open_with_options(
            &path,
            OpenOptions::new().durability(DurabilityMode::FullSync),
        )
        .unwrap();
        let col = client.database("rcdb").collection::<Document>("items");
        for i in 0..20i32 {
            col.insert_one(&doc! { "_id": i, "v": i }).unwrap();
        }
        drop(client);
    }

    // Scan the journal BEFORE reopen to compute the authoritative
    // expected frame counts. Reopen runs the recovery loop and may
    // checkpoint frames into the main file (shrinking the journal), so
    // the scan must happen first.
    let (expected_legacy, expected_chain) =
        crash_harness::scan_all_frame_counts(&path).expect("scan_all_frame_counts");

    // Reset the recovery counters, then reopen — the recovery loop runs
    // during open_or_create before any new work. The counters observed
    // immediately after open reflect what recovery saw.
    reset_recovery_legacy_page_frames();
    reset_recovery_chain_commit_frames();

    let reopened = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .unwrap();
    let legacy = recovery_legacy_page_frames_snapshot();
    let chain = recovery_chain_commit_frames_snapshot();

    // US-001 acceptance: the two recovery counters must equal EXACTLY
    // the journal's actual frame counts from the workload (not merely
    // `> 0`). Any divergence indicates the recovery loop silently
    // skipped or double-counted frames.
    assert_eq!(
        legacy, expected_legacy,
        "recovery_legacy_page_frames_total must equal the journal's \
         legacy-frame count ({}); saw {}",
        expected_legacy, legacy
    );
    assert_eq!(
        chain, expected_chain,
        "recovery_chain_commit_frames_total must equal the journal's \
         ChainCommit-frame count ({}); saw {}",
        expected_chain, chain
    );
    drop(reopened);
}

// ---------------------------------------------------------------------------
// Counter 5 — emergency_checkpoint_triggers_total
// ---------------------------------------------------------------------------

#[test]
fn emergency_checkpoint_triggers_rise_on_journal_fill_stress() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ec.mqlite");

    // The emergency path fires when the journal index reaches
    // JOURNAL_INDEX_HOT_THRESHOLD (= 3072) distinct page numbers. A single
    // insert typically touches only 2-3 distinct pages (leaf + header +
    // occasionally catalog), and the leaf is reused across inserts once
    // the tree stabilises, so we need inserts that allocate fresh pages
    // each time. Large payloads allocated into fresh leaves + high insert
    // counts drive the index above 3072 live entries.
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .unwrap();
    let col = client.database("ecdb").collection::<Document>("stress");

    // Seed one write outside the measurement window so the namespace is
    // bootstrapped before we snapshot the counter.
    col.insert_one(&doc! { "_id": -1i32, "payload": "x".repeat(200) })
        .unwrap();

    reset_emergency_checkpoint_triggers();
    let before = emergency_checkpoint_triggers_snapshot();

    // Each doc carries a ~32 KiB payload. Overflow-page chains plus the
    // per-commit header and leaf-page mutations push the journal index
    // past JOURNAL_INDEX_HOT_THRESHOLD (= 3072 distinct pages) before the
    // loop finishes. 4000 inserts empirically crosses the threshold
    // while keeping the test under ~20 s on a modern laptop.
    const N: i32 = 4000;
    for i in 0..N {
        col.insert_one(&doc! {
            "_id": i,
            "payload": "x".repeat(32 * 1024),
        })
        .unwrap();
    }

    thread::sleep(Duration::from_millis(10));

    let after = emergency_checkpoint_triggers_snapshot();
    assert!(
        after > before,
        "emergency_checkpoint_triggers_total must rise during the \
         journal-fill stress workload; saw before={}, after={}",
        before,
        after
    );
    drop(client);
}
