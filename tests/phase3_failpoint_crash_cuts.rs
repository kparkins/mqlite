#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

//! US-021b -- deterministic Phase 3 crash-cut tests.
//!
//! Each test maps one Section 5.4.2 residue class to a Section 10.16
//! failpoint:
//! - class A before logical append;
//! - class A after logical append but before the explicit logical fsync;
//! - class B after logical fsync but before ChainCommit;
//! - class C after ChainCommit but before legacy effects;
//! - class D after legacy effects but before publish;
//! - class D/E boundary during publish before the PublishedEpoch store.
//!
//! The child branch aborts the process at the armed failpoint. The parent
//! inspects the journal as left by the child and then verifies reopen
//! visibility. This file intentionally does not edit the journal after the
//! child exits.

#[path = "crash_harness.rs"]
mod crash_harness;

use std::env;
use std::path::Path;
use std::process::{Command, ExitStatus};

use bson::{doc, Document};
use mqlite::{
    arm_phase3_commit_failpoint, Client, DurabilityMode, OpenOptions, Phase3CommitFailpoint,
};

const CHILD_ENV: &str = "PHASE3_FAILPOINT_CHILD";
const DBPATH_ENV: &str = "PHASE3_FAILPOINT_DBPATH";
const FAILPOINT_ENV: &str = "PHASE3_FAILPOINT";
const CHILD_ENV_VALUE: &str = "1";
const DB_NAME: &str = "db";
const COLL_NAME: &str = "c";
const SEED_ID: i32 = 1;
const TARGET_ID: i32 = 2;
const LARGE_PAD_BYTES: usize = 24 * 1024;
const EXPECTED_NEW_FRAME: usize = 1;

#[derive(Clone, Copy)]
enum ExpectedVisibility {
    Missing,
    Present,
}

#[derive(Clone, Copy)]
enum LogicalResidue {
    None,
    One,
}

#[derive(Clone, Copy)]
enum ChainResidue {
    None,
    One,
}

#[derive(Clone, Copy)]
enum LegacyResidue {
    Unchanged,
    Increased,
}

#[derive(Clone, Copy)]
struct CrashCase {
    test_name: &'static str,
    failpoint_name: &'static str,
    logical: LogicalResidue,
    chain: ChainResidue,
    legacy: LegacyResidue,
    visibility: ExpectedVisibility,
}

#[derive(Clone, Copy, Default)]
struct JournalResidue {
    logical_frames: usize,
    chain_commits: usize,
    legacy_commits: usize,
    last_chain_ts: Option<(u64, u32)>,
}

#[test]
fn test_class_a_pre_logical_crash_recovers_uncommitted() {
    if run_child_if_requested() {
        return;
    }
    run_parent_case(CrashCase {
        test_name: "test_class_a_pre_logical_crash_recovers_uncommitted",
        failpoint_name: "before_logical_txn_append",
        logical: LogicalResidue::None,
        chain: ChainResidue::None,
        legacy: LegacyResidue::Unchanged,
        visibility: ExpectedVisibility::Missing,
    });
}

#[test]
fn test_class_a_logical_in_memory_crash_recovers_uncommitted() {
    if run_child_if_requested() {
        return;
    }
    run_parent_case(CrashCase {
        test_name: "test_class_a_logical_in_memory_crash_recovers_uncommitted",
        failpoint_name: "after_logical_txn_append_before_fsync",
        logical: LogicalResidue::One,
        chain: ChainResidue::None,
        legacy: LegacyResidue::Unchanged,
        visibility: ExpectedVisibility::Missing,
    });
}

#[test]
fn test_class_b_logical_durable_no_chaincommit_recovers_orphan() {
    if run_child_if_requested() {
        return;
    }
    run_parent_case(CrashCase {
        test_name: "test_class_b_logical_durable_no_chaincommit_recovers_orphan",
        failpoint_name: "after_logical_txn_fsync_before_chain_commit",
        logical: LogicalResidue::One,
        chain: ChainResidue::None,
        legacy: LegacyResidue::Unchanged,
        visibility: ExpectedVisibility::Missing,
    });
}

#[test]
fn test_class_c_chain_commit_durable_legacy_absent_recovers_crud() {
    if run_child_if_requested() {
        return;
    }
    run_parent_case(CrashCase {
        test_name: "test_class_c_chain_commit_durable_legacy_absent_recovers_crud",
        failpoint_name: "after_chain_commit_before_legacy_commit",
        logical: LogicalResidue::One,
        chain: ChainResidue::One,
        legacy: LegacyResidue::Unchanged,
        visibility: ExpectedVisibility::Present,
    });
}

#[test]
fn test_class_d_legacy_committed_publish_absent_recovers_committed() {
    if run_child_if_requested() {
        return;
    }
    run_parent_case(CrashCase {
        test_name: "test_class_d_legacy_committed_publish_absent_recovers_committed",
        failpoint_name: "after_legacy_commit_before_publish",
        logical: LogicalResidue::One,
        chain: ChainResidue::One,
        legacy: LegacyResidue::Increased,
        visibility: ExpectedVisibility::Present,
    });
}

#[test]
fn test_class_e_publish_in_progress_recovers_committed() {
    if run_child_if_requested() {
        return;
    }
    run_parent_case(CrashCase {
        test_name: "test_class_e_publish_in_progress_recovers_committed",
        failpoint_name: "during_publish_before_store",
        logical: LogicalResidue::One,
        chain: ChainResidue::One,
        legacy: LegacyResidue::Increased,
        visibility: ExpectedVisibility::Present,
    });
}

fn run_child_if_requested() -> bool {
    if env::var(CHILD_ENV).ok().as_deref() != Some(CHILD_ENV_VALUE) {
        return false;
    }

    let db_path = env::var(DBPATH_ENV).expect("child database path env");
    let failpoint_name = env::var(FAILPOINT_ENV).expect("child failpoint env");
    let failpoint = Phase3CommitFailpoint::from_name(&failpoint_name)
        .expect("child failpoint name must be recognized");
    let _guard = arm_phase3_commit_failpoint(failpoint).expect("arm failpoint");

    let client = Client::open_with_options(&db_path, fullsync_options()).expect("child open");
    client
        .database(DB_NAME)
        .collection::<Document>(COLL_NAME)
        .insert_one(&large_doc(TARGET_ID))
        .expect("child target insert should reach the armed failpoint");
    panic!("armed Phase 3 failpoint did not abort the child process");
}

fn run_parent_case(case: CrashCase) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("phase3-failpoint.mqlite");
    let baseline = prepare_database(&path);

    let status = spawn_child(&path, case);
    assert_child_aborted(status);

    let residue = inspect_residue(&path);
    assert_logical_residue(baseline, residue, case.logical);
    assert_chain_residue(baseline, residue, case.chain);
    assert_legacy_residue(baseline, residue, case.legacy);

    let (client, recovery) = crash_harness::reopen_inspect(&path).expect("reopen after abort");
    match case.chain {
        ChainResidue::None => assert_eq!(
            recovery.recovered_max_commit_ts, baseline.last_chain_ts,
            "classes A/B must not advance the recovered HLC floor"
        ),
        ChainResidue::One => assert_eq!(
            recovery.recovered_max_commit_ts, residue.last_chain_ts,
            "classes C/D/E must recover the target ChainCommit"
        ),
    }
    assert_target_visibility(&client, case.visibility);
}

fn prepare_database(path: &Path) -> JournalResidue {
    let client = Client::open_with_options(path, fullsync_options()).expect("parent open");
    let db = client.database(DB_NAME);
    db.create_collection(COLL_NAME).expect("create collection");
    db.collection::<Document>(COLL_NAME)
        .insert_one(&large_doc(SEED_ID))
        .expect("seed insert");
    client.close().expect("close baseline database");
    inspect_residue(path)
}

fn spawn_child(path: &Path, case: CrashCase) -> ExitStatus {
    let mut child = Command::new(env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg(case.test_name)
        .arg("--nocapture")
        .env(CHILD_ENV, CHILD_ENV_VALUE)
        .env(FAILPOINT_ENV, case.failpoint_name)
        .env(DBPATH_ENV, path)
        .spawn()
        .expect("spawn failpoint child");
    child.wait().expect("wait for failpoint child")
}

fn inspect_residue(path: &Path) -> JournalResidue {
    if !crash_harness::journal_path(path).exists() {
        return JournalResidue::default();
    }
    let logical_frames =
        crash_harness::scan_logical_txn_first_op_id(path).expect("scan logical frames");
    let chain_commits = crash_harness::scan_chain_commits(path).expect("scan chain commits");
    let legacy_commits =
        crash_harness::scan_legacy_commit_frames(path).expect("scan legacy commits");
    JournalResidue {
        logical_frames: logical_frames.len(),
        chain_commits: chain_commits.len(),
        legacy_commits: legacy_commits.len(),
        last_chain_ts: chain_commits.last().map(|(_, ts)| *ts),
    }
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

fn assert_logical_residue(
    baseline: JournalResidue,
    residue: JournalResidue,
    expected: LogicalResidue,
) {
    let delta = residue
        .logical_frames
        .saturating_sub(baseline.logical_frames);
    match expected {
        LogicalResidue::None => assert_eq!(delta, 0, "logical frame residue mismatch"),
        LogicalResidue::One => assert_eq!(
            delta, EXPECTED_NEW_FRAME,
            "target logical frame residue mismatch"
        ),
    }
}

fn assert_chain_residue(baseline: JournalResidue, residue: JournalResidue, expected: ChainResidue) {
    let delta = residue.chain_commits.saturating_sub(baseline.chain_commits);
    match expected {
        ChainResidue::None => assert_eq!(delta, 0, "ChainCommit residue mismatch"),
        ChainResidue::One => assert_eq!(
            delta, EXPECTED_NEW_FRAME,
            "target ChainCommit residue mismatch"
        ),
    }
}

fn assert_legacy_residue(
    baseline: JournalResidue,
    residue: JournalResidue,
    expected: LegacyResidue,
) {
    match expected {
        LegacyResidue::Unchanged => assert_eq!(
            residue.legacy_commits, baseline.legacy_commits,
            "target legacy commit residue mismatch"
        ),
        LegacyResidue::Increased => assert!(
            residue.legacy_commits > baseline.legacy_commits,
            "target legacy commit frame must be present before reopen"
        ),
    }
}

fn assert_target_visibility(client: &Client, expected: ExpectedVisibility) {
    let found = client
        .database(DB_NAME)
        .collection::<Document>(COLL_NAME)
        .find_one(doc! { "_id": TARGET_ID })
        .expect("find target after reopen");
    match expected {
        ExpectedVisibility::Missing => assert!(
            found.is_none(),
            "target document must not be visible after reopen"
        ),
        ExpectedVisibility::Present => {
            let doc = found.expect("target document must be visible after reopen");
            assert_eq!(doc.get_i32("_id").ok(), Some(TARGET_ID));
        }
    }
}

fn fullsync_options() -> OpenOptions {
    OpenOptions::new().durability(DurabilityMode::FullSync)
}

fn large_doc(id: i32) -> Document {
    doc! {
        "_id": id,
        "pad": "x".repeat(LARGE_PAD_BYTES),
    }
}
