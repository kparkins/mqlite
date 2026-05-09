//! Phase 5 §10.19.0 C-2 / US-036 — engine-fatal poison surface.
//!
//! Locks down the post-durable poison contract:
//!
//!  * Setting the poison reason fails-closed every public read, CRUD
//!    write, DDL, and checkpoint entry point with `Error::EngineFatal`
//!    without touching durable state.
//!  * Repeated poison calls preserve the first reason for diagnosis.
//!  * Reopen recovery is the only clearing path: a fresh `SharedState`
//!    + fresh `PublishSequencer` mean the new live engine is
//!      unpoisoned and the prior durable commit is reinstalled by
//!      logical redo.
//!  * A successor blocked behind a poisoned predecessor wakes via the
//!    sequencer poison hook and returns `Error::EngineFatal` instead
//!    of running its publish closure or marking its durable slot
//!    `Aborted`.
//!  * `NsDdlBarrierGuard::close_and_drain` completes after poison
//!    once the in-flight CRUD writer drops its `NsWriteTicket` on the
//!    `EngineFatal` return path.

#![cfg(feature = "test-hooks")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test target uses assertion-style panics and setup unwraps"
)]

mod crash_harness;

use std::env;
use std::fs;
use std::path::Path;
use std::process::{Command, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use bson::{doc, Document};
use mqlite::error::EngineFatalReason;
use mqlite::{
    __us018_append_logical_replay_frames, arm_legacy_commit_failpoint, Client, DurabilityMode,
    Error, LegacyCommitFailpoint, OpenOptions, Us018LogicalReplayFrame,
};
use serial_test::serial;

// This target exercises process-wide test hooks: commit failpoints,
// group-commit probes, journal sync counters, and engine poison state.
// Keep the integration tests serialized while preserving each test's
// internal writer-thread concurrency.

const NS: &str = "p5crash.docs";
const SHORT_SLEEP: Duration = Duration::from_millis(50);
const SETTLE_DEADLINE: Duration = Duration::from_secs(2);
const GROUP_COMMIT_WRITERS: usize = 4;
const GROUP_COMMIT_CHURN_WRITERS: usize = 100;
const US018_CHILD_ENV: &str = "US018_P5_CRASH_CHILD";
const US018_DBPATH_ENV: &str = "US018_P5_CRASH_DBPATH";
const US018_CHILD_ENV_VALUE: &str = "1";
const US018_CRASH_ID: i32 = 18_001;
const US018_MISSING_PUBLISH_ID: i32 = 18_002;
const US018_REPLAY_COMMITTED_ID: i32 = 18_003;
const US018_SEQUENCER_BOOTSTRAP_ID: i32 = 18_004;
const US018_SEQUENCER_LIVE_ID: i32 = 18_005;
const US018_DUPLICATE_FIRST_ID: i32 = 18_006;
const US018_DUPLICATE_SECOND_ID: i32 = 18_007;
const US018_GAP_FIRST_ID: i32 = 18_008;
const US018_GAP_SECOND_ID: i32 = 18_009;
const US018_GAP_LIVE_ID: i32 = 18_010;
const US018_PARTIAL_GOOD_ID: i32 = 18_011;
const US018_PARTIAL_BAD_ID: i32 = 18_012;
const US018_TS_BASE: u64 = 9_018_000_000_000;
const US018_TS_COMMITTED: u64 = US018_TS_BASE + 1;
const US018_TS_SEQUENCER: u64 = US018_TS_BASE + 2;
const US018_TS_DUPLICATE: u64 = US018_TS_BASE + 3;
const US018_TS_GAP_FIRST: u64 = US018_TS_BASE + 4;
const US018_TS_GAP_SECOND: u64 = US018_TS_BASE + 6;
const US018_TS_PARTIAL_GOOD: u64 = US018_TS_BASE + 7;
const US018_TS_PARTIAL_BAD: u64 = US018_TS_BASE + 8;

fn open_with_collection() -> (tempfile::TempDir, Client) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us036.mqlite");
    let client = Client::open(&path).unwrap();
    client
        .database("p5crash")
        .create_collection("docs")
        .unwrap();
    (dir, client)
}

fn open_with_collection_options(
    file_name: &str,
    opts: OpenOptions,
) -> (tempfile::TempDir, std::path::PathBuf, Client) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(file_name);
    let client = Client::open_with_options(&path, opts).unwrap();
    let db = client.database("p5crash");
    db.create_collection("docs").unwrap();
    db.create_collection("other").unwrap();
    (dir, path, client)
}

fn assert_doc_value(client: &Client, coll: &str, id: i32, expected: Option<&str>) {
    let got = client
        .database("p5crash")
        .collection::<Document>(coll)
        .find_one(doc! { "_id": id })
        .unwrap();
    match expected {
        Some(value) => {
            let got = got.expect("expected document to exist");
            assert_eq!(got.get_str("v").unwrap(), value);
        }
        None => assert!(got.is_none(), "document {id} in {coll} must be absent"),
    }
}

fn assert_engine_fatal<T: std::fmt::Debug>(
    res: Result<T, Error>,
    expected: &EngineFatalReason,
    label: &str,
) {
    match res {
        Err(Error::EngineFatal { reason }) => {
            assert_eq!(
                &reason, expected,
                "{label}: expected reason {expected:?}, got {reason:?}"
            );
        }
        other => panic!("{label} must return Error::EngineFatal, got {other:?}"),
    }
}

fn spawn_fullsync_insert(
    client: &Client,
    id: i32,
    start: Arc<Barrier>,
    done: Option<Arc<AtomicBool>>,
) -> thread::JoinHandle<Result<(), Error>> {
    let client = client.clone();
    thread::spawn(move || {
        start.wait();
        let result = client
            .database("p5crash")
            .collection::<Document>("docs")
            .insert_one(&doc! { "_id": id, "v": format!("fullsync-{id}") })
            .map(|_| ());
        if let Some(done) = done {
            done.store(true, Ordering::Release);
        }
        result
    })
}

fn assert_group_commit_docs_present(client: &Client, count: usize) {
    for id in 0..count {
        assert_doc_value(client, "docs", id as i32, Some(&format!("fullsync-{id}")));
    }
}

fn prepare_us018_replay_db(path: &Path) -> i64 {
    let client = Client::open_with_options(path, OpenOptions::new()).unwrap();
    let db = client.database("p5crash");
    db.create_collection("docs").unwrap();
    let ns_id = client
        .__us036_namespace_id(NS)
        .unwrap()
        .expect("p5crash.docs namespace id exists");
    client.close().unwrap();
    ns_id
}

fn us018_replay_frame(
    ns_id: i64,
    id: i32,
    commit_ts_physical_ms: u64,
    value: &str,
) -> Us018LogicalReplayFrame {
    Us018LogicalReplayFrame {
        ns_id,
        id,
        value: value.to_owned(),
        commit_ts_physical_ms,
        commit_ts_logical: 0,
        op_ordinal: 0,
        use_bad_overflow: false,
    }
}

fn assert_ts_pair(client: &Client, expected: (u64, u32)) {
    assert_eq!(
        client.__published_visible_ts(),
        expected,
        "recovered PublishedEpoch.visible_ts must match recovered max"
    );
    assert_eq!(
        client.__published_sequencer_frontier(),
        expected,
        "recovered sequencer frontier must match recovered max"
    );
}

fn run_us018_crash_child_if_requested() -> bool {
    if env::var(US018_CHILD_ENV).ok().as_deref() != Some(US018_CHILD_ENV_VALUE) {
        return false;
    }

    let db_path = env::var(US018_DBPATH_ENV).expect("US-018 child database path env");
    let _guard = arm_legacy_commit_failpoint(LegacyCommitFailpoint::AfterLegacyCommitBeforePublish)
        .expect("arm US-018 crash failpoint");
    let client =
        Client::open_with_options(&db_path, crash_harness::fullsync_options()).expect("child open");
    client
        .database("p5crash")
        .collection::<Document>("docs")
        .insert_one(&doc! { "_id": US018_CRASH_ID, "v": "crash-before-publish" })
        .expect("child insert should reach the armed failpoint");
    panic!("armed US-018 failpoint did not abort the child process");
}

fn spawn_us018_crash_child(path: &Path, test_name: &str) -> ExitStatus {
    let mut child = Command::new(env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(US018_CHILD_ENV, US018_CHILD_ENV_VALUE)
        .env(US018_DBPATH_ENV, path)
        .spawn()
        .expect("spawn US-018 failpoint child");
    child.wait().expect("wait for US-018 failpoint child")
}

fn assert_child_aborted(status: ExitStatus) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        assert_eq!(
            status.signal(),
            Some(libc::SIGABRT),
            "child must terminate through SIGABRT"
        );
    }
    #[cfg(not(unix))]
    assert!(!status.success(), "child must terminate unsuccessfully");
}

/// US-018 / §10.29 — if the process dies after the durable commit envelope
/// but before the sequencer publishes, reopen recovery must install the
/// logical record as committed and seed the recovered frontier.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_crash_before_sequencer_publish_preserves_durability() {
    if run_us018_crash_child_if_requested() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us018_crash_before_publish.mqlite");
    let client = Client::open_with_options(&path, crash_harness::fullsync_options()).unwrap();
    client
        .database("p5crash")
        .create_collection("docs")
        .unwrap();
    client.close().unwrap();

    let status = spawn_us018_crash_child(
        &path,
        "test_crash_before_sequencer_publish_preserves_durability",
    );
    assert_child_aborted(status);

    let reopened = Client::open_with_options(&path, crash_harness::fullsync_options()).unwrap();
    assert_doc_value(
        &reopened,
        "docs",
        US018_CRASH_ID,
        Some("crash-before-publish"),
    );
    let recovered = reopened
        .__recovered_max_commit_ts()
        .expect("crashed durable commit contributes recovered max");
    assert_ts_pair(&reopened, recovered);
}

/// US-018 / §10.29 — a failed replay cannot return an engine with a partial
/// frontier. The open fails before publishing the end-of-open epoch and leaves
/// the durable files unchanged for the next recovery attempt.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_crash_mid_out_of_order_install_does_not_expose_partial_frontier() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us018_partial_replay.mqlite");
    let ns_id = prepare_us018_replay_db(&path);
    let bad = Us018LogicalReplayFrame {
        use_bad_overflow: true,
        ..us018_replay_frame(
            ns_id,
            US018_PARTIAL_BAD_ID,
            US018_TS_PARTIAL_BAD,
            "partial-bad",
        )
    };
    __us018_append_logical_replay_frames(
        &path,
        &[
            us018_replay_frame(
                ns_id,
                US018_PARTIAL_GOOD_ID,
                US018_TS_PARTIAL_GOOD,
                "partial-good",
            ),
            bad,
        ],
    )
    .unwrap();

    let main_before = fs::read(&path).unwrap();
    let journal_path = crash_harness::journal_path(&path);
    let journal_before = fs::read(&journal_path).unwrap();
    let err = match Client::open_with_options(&path, OpenOptions::new()) {
        Ok(_) => panic!("bad overflow replay must fail open before exposing a client"),
        Err(err) => err,
    };
    assert!(
        format!("{err:?}").contains("overflow payloads"),
        "expected overflow replay error, got {err:?}"
    );
    assert_eq!(fs::read(&path).unwrap(), main_before);
    assert_eq!(fs::read(&journal_path).unwrap(), journal_before);
}

/// US-018 / §10.29 — a durable commit whose live publish never completed is
/// tolerated by reopen recovery and becomes visible from logical redo.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_recovery_tolerates_missing_publish_for_durable_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us018_missing_publish.mqlite");
    let client = Client::open_with_options(&path, crash_harness::fullsync_options()).unwrap();
    client
        .database("p5crash")
        .create_collection("docs")
        .unwrap();
    client.__us009_reset_flip_publish_order();
    client.__us009_fail_after_committed_flip_once();

    assert_engine_fatal(
        client
            .database("p5crash")
            .collection::<Document>("docs")
            .insert_one(&doc! { "_id": US018_MISSING_PUBLISH_ID, "v": "missing-publish" }),
        &EngineFatalReason::PostDurablePendingFlipFailure,
        "post-durable pre-publish write",
    );
    drop(client);

    let reopened = Client::open_with_options(&path, crash_harness::fullsync_options()).unwrap();
    assert_doc_value(
        &reopened,
        "docs",
        US018_MISSING_PUBLISH_ID,
        Some("missing-publish"),
    );
    let recovered = reopened
        .__recovered_max_commit_ts()
        .expect("missing-publish commit contributes recovered max");
    assert_ts_pair(&reopened, recovered);
}

/// US-018 / Phase 2 §3.8(a) — when duplicate logical frames carry the same
/// `commit_ts`, the recovery parser keeps the first frame and discards the
/// later duplicate before logical redo runs.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_recovery_duplicate_commit_ts_discards_later_frame() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us018_duplicate_commit_ts.mqlite");
    let ns_id = prepare_us018_replay_db(&path);
    __us018_append_logical_replay_frames(
        &path,
        &[
            us018_replay_frame(
                ns_id,
                US018_DUPLICATE_FIRST_ID,
                US018_TS_DUPLICATE,
                "duplicate-first",
            ),
            Us018LogicalReplayFrame {
                ..us018_replay_frame(
                    ns_id,
                    US018_DUPLICATE_SECOND_ID,
                    US018_TS_DUPLICATE,
                    "duplicate-second",
                )
            },
        ],
    )
    .unwrap();

    let reopened = Client::open_with_options(&path, OpenOptions::new()).unwrap();
    assert_doc_value(
        &reopened,
        "docs",
        US018_DUPLICATE_FIRST_ID,
        Some("duplicate-first"),
    );
    assert_doc_value(&reopened, "docs", US018_DUPLICATE_SECOND_ID, None);
}

/// US-018 / §10.29 — recovery accepts commit timestamp gaps caused by aborted
/// writers and seeds visibility from the highest durable ChainCommit.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_recovery_tolerates_hlc_gap_from_abort() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us018_hlc_gap.mqlite");
    let ns_id = prepare_us018_replay_db(&path);
    __us018_append_logical_replay_frames(
        &path,
        &[
            us018_replay_frame(ns_id, US018_GAP_FIRST_ID, US018_TS_GAP_FIRST, "gap-first"),
            Us018LogicalReplayFrame {
                ..us018_replay_frame(
                    ns_id,
                    US018_GAP_SECOND_ID,
                    US018_TS_GAP_SECOND,
                    "gap-second",
                )
            },
        ],
    )
    .unwrap();

    let reopened = Client::open_with_options(&path, OpenOptions::new()).unwrap();
    assert_doc_value(&reopened, "docs", US018_GAP_FIRST_ID, Some("gap-first"));
    assert_doc_value(&reopened, "docs", US018_GAP_SECOND_ID, Some("gap-second"));
    let expected = (US018_TS_GAP_SECOND, 0);
    assert_eq!(reopened.__recovered_max_commit_ts(), Some(expected));
    assert_ts_pair(&reopened, expected);

    reopened
        .database("p5crash")
        .collection::<Document>("docs")
        .insert_one(&doc! { "_id": US018_GAP_LIVE_ID, "v": "gap-live" })
        .unwrap();
    assert!(reopened.__published_sequencer_frontier() > expected);
}

/// US-018 / §10.29 — the reopen path constructs a fresh dense publish window
/// from recovered HLC visibility, so the first post-recovery live write can
/// publish immediately instead of waiting behind nonexistent recovered slots.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_recovery_initializes_sequencer_from_recovered_max() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us018_sequencer_recovered_max.mqlite");
    let ns_id = prepare_us018_replay_db(&path);
    __us018_append_logical_replay_frames(
        &path,
        &[us018_replay_frame(
            ns_id,
            US018_SEQUENCER_BOOTSTRAP_ID,
            US018_TS_SEQUENCER,
            "sequencer-bootstrap",
        )],
    )
    .unwrap();

    let reopened = Client::open_with_options(&path, OpenOptions::new()).unwrap();
    let expected = (US018_TS_SEQUENCER, 0);
    assert_eq!(reopened.__recovered_max_commit_ts(), Some(expected));
    assert_ts_pair(&reopened, expected);
    assert_doc_value(
        &reopened,
        "docs",
        US018_SEQUENCER_BOOTSTRAP_ID,
        Some("sequencer-bootstrap"),
    );

    reopened
        .database("p5crash")
        .collection::<Document>("docs")
        .insert_one(&doc! { "_id": US018_SEQUENCER_LIVE_ID, "v": "first-live" })
        .unwrap();
    assert_doc_value(
        &reopened,
        "docs",
        US018_SEQUENCER_LIVE_ID,
        Some("first-live"),
    );
    assert!(reopened.__published_sequencer_frontier() > expected);
}

/// US-018 / §10.29 rule 4 — logical redo installs final `Committed` entries
/// directly, bypassing live Pending-slot publication.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_replay_installs_committed_bypassing_sequencer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us018_replay_committed.mqlite");
    let ns_id = prepare_us018_replay_db(&path);
    __us018_append_logical_replay_frames(
        &path,
        &[us018_replay_frame(
            ns_id,
            US018_REPLAY_COMMITTED_ID,
            US018_TS_COMMITTED,
            "replay-committed",
        )],
    )
    .unwrap();

    let reopened = Client::open_with_options(&path, OpenOptions::new()).unwrap();
    assert_doc_value(
        &reopened,
        "docs",
        US018_REPLAY_COMMITTED_ID,
        Some("replay-committed"),
    );
    let states = reopened
        .__us009_primary_chain_states(NS, &bson::Bson::Int32(US018_REPLAY_COMMITTED_ID))
        .unwrap();
    assert_eq!(states.first().map(String::as_str), Some("Committed"));
    assert!(
        !states.iter().any(|state| state == "Pending"),
        "replay must not install Pending entries"
    );
    assert_eq!(reopened.__recovery_open_published_store_count(), 1);
    assert_ts_pair(&reopened, (US018_TS_COMMITTED, 0));
}

/// AC #4 + #7 — once the engine is poisoned, every public read, CRUD
/// write, DDL, and checkpoint entry point fails-closed with
/// `Error::EngineFatal` carrying the recorded reason.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_engine_fatal_poison_blocks_reads_writes_and_ddl() {
    let (_dir, client) = open_with_collection();

    // Prime the namespace with a document so reads have something to
    // miss after poisoning. Done BEFORE poison so the poison check
    // does not block this setup write.
    client
        .database("p5crash")
        .collection::<Document>("docs")
        .insert_one(&doc! { "_id": 1i32, "v": "before-poison" })
        .unwrap();

    let reason = EngineFatalReason::PostDurablePublishFailure;
    client.__us036_poison_engine(reason.clone());
    assert_eq!(client.__us036_poisoned_reason(), Some(reason.clone()));

    let db = client.database("p5crash");
    let coll = db.collection::<Document>("docs");

    // Reads.
    assert_engine_fatal(coll.find_one(doc! { "_id": 1i32 }), &reason, "find_one");
    assert_engine_fatal(coll.count_documents(doc! {}), &reason, "count_documents");
    assert_engine_fatal(coll.list_indexes(), &reason, "list_indexes");

    // CRUD writes.
    assert_engine_fatal(
        coll.insert_one(&doc! { "_id": 2i32, "v": "after-poison" }),
        &reason,
        "insert_one",
    );
    assert_engine_fatal(
        coll.update_one(doc! { "_id": 1i32 }, doc! { "$set": { "v": "x" } })
            .run(),
        &reason,
        "update_one",
    );
    assert_engine_fatal(coll.delete_one(doc! { "_id": 1i32 }), &reason, "delete_one");

    // DDL.
    assert_engine_fatal(
        db.create_collection("after_poison"),
        &reason,
        "create_collection",
    );
    assert_engine_fatal(db.drop_collection("docs"), &reason, "drop_collection");

    // Test-hook entry points (AC #7): `__us036_namespace_id` resolves
    // a durable id from the published catalog. After poison the hook
    // must fail-closed before reading engine state.
    assert_engine_fatal(
        client.__us036_namespace_id(NS),
        &reason,
        "__us036_namespace_id",
    );
    // `__us036_admit_writer` and `__us036_close_and_drain` route
    // through the lane primitive but the public surface on `Client`
    // currently does NOT depend on poison; AC #7 requires the
    // probe-style read hooks to fail-closed, which we verify above.

    // Checkpoint / lifecycle.
    assert_engine_fatal(client.close(), &reason, "client.close");
}

/// AC #3 — `poison_after_durable_commit` records the reason exactly
/// once. Later calls notify waiters but do not overwrite the stored
/// reason; the first reason wins for diagnosis.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_engine_fatal_preserves_first_reason() {
    let (_dir, client) = open_with_collection();

    let first = EngineFatalReason::PostDurablePublishFailure;
    let second = EngineFatalReason::PostDurablePendingFlipFailure;
    let third = EngineFatalReason::PostDurableDdlPublishFailure;

    client.__us036_poison_engine(first.clone());
    client.__us036_poison_engine(second);
    client.__us036_poison_engine(third);

    assert_eq!(
        client.__us036_poisoned_reason(),
        Some(first.clone()),
        "first poison reason must be preserved across later poison calls"
    );

    // Public surface still reports the first reason.
    let db = client.database("p5crash");
    let coll = db.collection::<Document>("docs");
    assert_engine_fatal(coll.find_one(doc! { "_id": 1i32 }), &first, "find_one");
}

/// AC #8 — reopen recovery is the only clearing path. Logical redo
/// reinstalls durable commits as `VersionState::Committed` and the
/// new engine starts with a fresh unpoisoned `SharedState` + a fresh
/// `PublishSequencer::new()`.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_reopen_clears_engine_fatal_after_logical_redo() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us036_reopen.mqlite");

    {
        let client = Client::open(&path).unwrap();
        client
            .database("p5crash")
            .create_collection("docs")
            .unwrap();
        client
            .database("p5crash")
            .collection::<Document>("docs")
            .insert_one(&doc! { "_id": 7i32, "v": "pre-poison" })
            .unwrap();

        // Poison after the durable insert. The doc is already in the
        // journal so logical redo will reinstall it on reopen.
        client.__us036_poison_engine(EngineFatalReason::PostDurablePublishFailure);
        assert!(client.__us036_poisoned_reason().is_some());

        // Drop the client. Drop's silent checkpoint will be poisoned;
        // the journal still carries the durable insert so reopen redo
        // restores it.
        drop(client);
    }

    // Reopen — fresh SharedState + fresh PublishSequencer.
    let client = Client::open(&path).unwrap();
    assert_eq!(
        client.__us036_poisoned_reason(),
        None,
        "reopen recovery must construct an unpoisoned SharedState"
    );

    // Logical redo reinstalled the pre-poison commit; reads succeed.
    let coll = client.database("p5crash").collection::<Document>("docs");
    let got = coll.find_one(doc! { "_id": 7i32 }).unwrap();
    let got = got.expect("logical redo restored the pre-poison insert");
    assert_eq!(got.get_str("v").unwrap(), "pre-poison");

    // CRUD on the reopened engine works again.
    coll.insert_one(&doc! { "_id": 8i32, "v": "post-reopen" })
        .unwrap();
    assert_eq!(coll.count_documents(doc! {}).unwrap(), 2);
}

/// AC #5 — a successor that has already registered its publish slot
/// and is blocked in `wait_until_predecessors_complete` wakes via
/// the sequencer poison hook and returns `Error::EngineFatal` instead
/// of running its publish closure or marking its slot `Aborted`.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_successor_before_poison_wakes_and_returns_engine_fatal() {
    let (_dir, client) = open_with_collection();

    // Predecessor stays Pending. Successor waits on its predecessor.
    let slot1 = client.__us036_register_publish_slot().unwrap();
    let slot2 = client.__us036_register_publish_slot().unwrap();
    assert!(
        slot2.seq() > slot1.seq(),
        "successor slot must sort after predecessor"
    );

    let waiting = Arc::new(AtomicBool::new(false));
    let waiting_writer = Arc::clone(&waiting);
    let worker = thread::spawn(move || {
        waiting_writer.store(true, Ordering::Release);
        slot2.wait_for_predecessor_or_poison()
    });

    // Spin until the worker has entered the wait. Bounded so a buggy
    // implementation cannot hang the suite.
    let deadline = Instant::now() + SETTLE_DEADLINE;
    while !waiting.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "worker did not enter wait");
        thread::sleep(Duration::from_millis(1));
    }
    // Allow the worker to reach the cvar wait inside the sequencer
    // mutex. The poison hook below must wake it via notify_all.
    thread::sleep(SHORT_SLEEP);

    let reason = EngineFatalReason::PostDurablePublishFailure;
    client.__us036_poison_engine(reason.clone());

    let res = worker.join().expect("worker thread joined");
    assert_engine_fatal(res, &reason, "blocked successor wait");
}

/// AC #6 — an in-flight CRUD writer that receives `Error::EngineFatal`
/// from the sequencer drops its `NsWriteTicket` before returning, so
/// `NsDdlBarrierGuard::close_and_drain` completes after poison
/// instead of waiting forever on a poisoned successor.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_ddl_drain_completes_after_poisoned_successor_drops_ticket() {
    let (_dir, client) = open_with_collection();

    let ns_id = client
        .__us036_namespace_id(NS)
        .expect("engine not poisoned before drain test setup")
        .expect("durable ns_id resolved from published catalog");

    // Admit a writer ticket on this lane. The drain below cannot
    // complete until this ticket drops.
    let ticket = client.__us036_admit_writer(ns_id, 5_000).unwrap();

    // Predecessor stays Pending. Successor waits on it.
    let _slot1 = client.__us036_register_publish_slot().unwrap();
    let slot2 = client.__us036_register_publish_slot().unwrap();

    // Worker owns both the ticket and the publish slot. On
    // `EngineFatal` it returns; the ticket drops on scope exit and
    // the lane's `releases` counter advances.
    let waiting = Arc::new(AtomicBool::new(false));
    let waiting_writer = Arc::clone(&waiting);
    let worker = thread::spawn(move || -> Result<(), Error> {
        let _ticket = ticket;
        waiting_writer.store(true, Ordering::Release);
        slot2.wait_for_predecessor_or_poison()
    });

    let deadline = Instant::now() + SETTLE_DEADLINE;
    while !waiting.load(Ordering::Acquire) {
        assert!(Instant::now() < deadline, "writer did not enter wait");
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(SHORT_SLEEP);

    // Drain in another thread. It sets `closed=true` then blocks on
    // `admits != releases` until the writer drops its ticket.
    let drain_client = client.clone();
    let drain = thread::spawn(move || drain_client.__us036_close_and_drain(ns_id, 5_000));

    // Give the drain thread time to enter its wait loop.
    thread::sleep(SHORT_SLEEP);

    // Poison the engine. The blocked writer wakes with EngineFatal,
    // returns, and drops its ticket — drain unblocks.
    let reason = EngineFatalReason::PostDurablePublishFailure;
    client.__us036_poison_engine(reason.clone());

    let res = worker.join().expect("worker thread joined");
    assert_engine_fatal(res, &reason, "writer blocked on poisoned predecessor");

    drain
        .join()
        .expect("drain thread joined")
        .expect("close_and_drain must complete after poisoned writer drops its ticket");
}

/// US-017 / US-039 — append-only journal paths must not perform their own
/// flush/sync work before the FullSync group-commit leader runs.
#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_group_commit_fsync_count_1_for_4_writers() {
    let opts = OpenOptions::new().durability(DurabilityMode::FullSync);
    let (_dir, _path, client) = open_with_collection_options("us017_fullsync.mqlite", opts);

    client.__us017_reset_group_commit_probe();
    client.__us017_expect_group_commit_cohort_size(GROUP_COMMIT_WRITERS as u64);
    client.__us039_reset_append_sync_observations();

    let start = Arc::new(Barrier::new(GROUP_COMMIT_WRITERS + 1));
    let handles = (0..GROUP_COMMIT_WRITERS)
        .map(|id| spawn_fullsync_insert(&client, id as i32, Arc::clone(&start), None))
        .collect::<Vec<_>>();
    start.wait();
    for handle in handles {
        handle
            .join()
            .expect("writer thread joined")
            .expect("writer commit succeeds");
    }

    let obs = client.__us039_append_sync_observations();
    assert_eq!(
        obs.handle_flushes, 0,
        "the FullSync LSN boundary must not flush dirty main-file pages"
    );
    assert_eq!(
        obs.handle_journal_syncs, 1,
        "the FullSync boundary owner should issue exactly one handle sync"
    );
    assert_eq!(
        obs.journal_sync_os_boundaries, 1,
        "the FullSync boundary owner should reach exactly one OS fsync boundary"
    );
    assert_group_commit_docs_present(&client, GROUP_COMMIT_WRITERS);
}

#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_group_commit_leader_fsync_failure_poisons_and_wakes_followers() {
    let opts = OpenOptions::new().durability(DurabilityMode::FullSync);
    let (_dir, path, client) = open_with_collection_options("us017_fullsync_failure.mqlite", opts);

    client.__us017_reset_group_commit_probe();
    client.__us017_expect_group_commit_cohort_size(GROUP_COMMIT_WRITERS as u64);
    client.__us017_fail_next_group_commit_fsync();

    let start = Arc::new(Barrier::new(GROUP_COMMIT_WRITERS + 1));
    let handles = (0..GROUP_COMMIT_WRITERS)
        .map(|id| spawn_fullsync_insert(&client, id as i32, Arc::clone(&start), None))
        .collect::<Vec<_>>();
    start.wait();

    let reason = EngineFatalReason::PostDurablePublishFailure;
    for handle in handles {
        assert_engine_fatal(
            handle.join().expect("writer thread joined"),
            &reason,
            "group-commit writer",
        );
    }

    let group = client.__us017_group_commit_observations();
    assert!(
        !group.leader_elected,
        "failed leader must clear the election flag"
    );
    assert!(
        group.failed_high_water > 0,
        "failed LSN high-water must record the closed sync target"
    );
    assert_eq!(group.fsync_failures, 1);

    assert_engine_fatal(
        client
            .database("p5crash")
            .collection::<Document>("docs")
            .find_one(doc! { "_id": 0i32 }),
        &reason,
        "post-poison read",
    );

    drop(client);
    let reopened = Client::open(&path).expect("reopen after poisoned cohort");
    let _ = reopened
        .database("p5crash")
        .collection::<Document>("docs")
        .count_documents(doc! {})
        .expect("recovery handles any durable records from failed cohort");
}

#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_group_commit_late_arrival_forms_next_cohort() {
    let opts = OpenOptions::new().durability(DurabilityMode::FullSync);
    let (_dir, _path, client) = open_with_collection_options("us017_late_arrival.mqlite", opts);

    client.__us017_reset_group_commit_probe();
    client.__us017_expect_group_commit_cohort_size(GROUP_COMMIT_WRITERS as u64);
    client.__us039_reset_append_sync_observations();
    let mut pause = client.__us017_pause_next_group_commit_after_close();

    let start = Arc::new(Barrier::new(GROUP_COMMIT_WRITERS + 1));
    let handles = (0..GROUP_COMMIT_WRITERS)
        .map(|id| spawn_fullsync_insert(&client, id as i32, Arc::clone(&start), None))
        .collect::<Vec<_>>();
    start.wait();
    pause
        .wait_until_paused_timeout(SETTLE_DEADLINE)
        .expect("leader paused after closing first cohort");

    let late_done = Arc::new(AtomicBool::new(false));
    let late_start = Arc::new(Barrier::new(2));
    let late = spawn_fullsync_insert(
        &client,
        GROUP_COMMIT_WRITERS as i32,
        Arc::clone(&late_start),
        Some(Arc::clone(&late_done)),
    );
    late_start.wait();
    thread::sleep(SHORT_SLEEP);
    assert!(
        !late_done.load(Ordering::Acquire),
        "late writer must not be released by the closed first cohort"
    );

    pause.release().unwrap();
    for handle in handles {
        handle
            .join()
            .expect("writer thread joined")
            .expect("first cohort writer succeeds");
    }
    late.join()
        .expect("late writer joined")
        .expect("late writer succeeds");

    let obs = client.__us039_append_sync_observations();
    assert_eq!(
        obs.journal_sync_os_boundaries, 2,
        "late arrival must require a second fsync"
    );
    let group = client.__us017_group_commit_observations();
    assert!(
        group.last_fsync_seq >= (GROUP_COMMIT_WRITERS + 1) as u64,
        "second LSN sync must cover the late writer"
    );
}

#[test]
#[serial(mwmr_crash_recovery_hooks)]
fn test_group_commit_leader_election_safe_under_churn() {
    let opts = OpenOptions::new().durability(DurabilityMode::FullSync);
    let (_dir, _path, client) = open_with_collection_options("us017_churn.mqlite", opts);

    client.__us017_reset_group_commit_probe();
    client.__us039_reset_append_sync_observations();

    let start = Arc::new(Barrier::new(GROUP_COMMIT_CHURN_WRITERS + 1));
    let handles = (0..GROUP_COMMIT_CHURN_WRITERS)
        .map(|id| spawn_fullsync_insert(&client, id as i32, Arc::clone(&start), None))
        .collect::<Vec<_>>();
    start.wait();
    for handle in handles {
        handle
            .join()
            .expect("writer thread joined")
            .expect("writer commit succeeds under churn");
    }

    let group = client.__us017_group_commit_observations();
    assert!(
        group.leader_entries > 0,
        "at least one leader must be elected"
    );
    assert!(
        group.max_active_leaders <= 1,
        "CAS must prevent simultaneous leaders"
    );
    assert!(
        group.last_fsync_seq >= GROUP_COMMIT_CHURN_WRITERS as u64,
        "every writer LSN must be covered by fsync"
    );
    assert_eq!(
        client
            .database("p5crash")
            .collection::<Document>("docs")
            .count_documents(doc! {})
            .unwrap(),
        GROUP_COMMIT_CHURN_WRITERS as u64
    );
}
