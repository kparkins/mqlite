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

//! Phase-0 observability counter tests (US-001).
//!
//! Each test validates that the named counter in `mqlite::mvcc::metrics`
//! moves exactly as specified by the PRD acceptance criteria. Tests reset
//! the counter first, perform the action under test, then assert the delta.
//!
//! Lane wait is exercised by a dedicated concurrent-writer test.
//!
//! All tests acquire `COUNTER_SERIAL` before touching process-global
//! counters so concurrent rust-test parallelism cannot race on the
//! reset / record / snapshot sequences.

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, OpenOptions};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use mqlite::mvcc::metrics::{
    crud_commits_root_changing_snapshot, crud_commits_root_neutral_snapshot,
    emergency_checkpoint_triggers_snapshot, lane_wait_ns_snapshot,
    published_snapshot_rebuilds_snapshot, recovery_chain_commit_frames_snapshot,
    recovery_legacy_page_frames_snapshot, reset_crud_commits_root_changing,
    reset_crud_commits_root_neutral, reset_emergency_checkpoint_triggers, reset_lane_wait_ns,
    reset_published_snapshot_rebuilds, reset_recovery_chain_commit_frames,
    reset_recovery_legacy_page_frames,
};

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
    let ns_id = client
        .__us036_namespace_id("lwdb.sharedns")
        .unwrap()
        .expect("namespace id");

    reset_lane_wait_ns();
    let first_ticket = client.__us036_admit_writer(ns_id, 1_000).unwrap();
    let c1 = client;
    let waiting = thread::spawn(move || {
        c1.__us036_admit_writer(ns_id, 1_000)
            .expect("second writer admitted after first releases")
    });
    thread::sleep(Duration::from_millis(20));
    first_ticket.drop_ticket();
    waiting.join().unwrap().drop_ticket();

    let lane_wait = lane_wait_ns_snapshot();
    assert!(
        lane_wait > 0,
        "two writers on the SAME namespace must record nonzero lane_wait_ns_total; \
         saw lane_wait_ns_total={}",
        lane_wait
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
fn emergency_checkpoint_trigger_stays_zero_for_logical_workload() {
    let _guard = COUNTER_SERIAL.lock().unwrap_or_else(|p| p.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ec.mqlite");

    // Phase 6 ordinary CRUD writes logical journal records. The retired
    // page-frame journal-fill trigger must stay inactive for this workload.
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

    const N: i32 = 200;
    for i in 0..N {
        col.insert_one(&doc! {
            "_id": i,
            "payload": "x".repeat(32 * 1024),
        })
        .unwrap();
    }

    thread::sleep(Duration::from_millis(10));

    let after = emergency_checkpoint_triggers_snapshot();
    assert_eq!(
        after, before,
        "emergency_checkpoint_triggers_total must stay unchanged for Phase 6 \
         logical CRUD workload; saw before={}, after={}",
        before, after
    );
    drop(client);
}
