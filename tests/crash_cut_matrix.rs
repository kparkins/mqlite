#![doc = "Integration test requiring the test-hooks feature."]
#![cfg(feature = "test-hooks")]

//! US-007 — Crash-cut matrix for the current two-stage commit envelope.
//!
//! Covers all 6 cuts listed in
//! docs/STORAGE-UPGRADE-PHASE-00-BASELINE-HARDENING.md §4.2 with one
//! invariant-class predicate and one HEAD-ORDERING-class predicate per cut.
//! The workload uses the hidden Phase 0 probe to stop inside the real write
//! envelope at each named cut point rather than approximating cuts via a
//! shared journal-tail truncation.

#[path = "crash_harness.rs"]
mod crash_harness;

use std::collections::BTreeSet;
use std::fs;
use std::sync::Mutex;

use bson::doc;
use bson::Document;
use mqlite::{Client, DurabilityMode, OpenOptions, Phase0ProbeCut, Phase0ProbeReport};

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn with_test_lock<R>(f: impl FnOnce() -> R) -> R {
    let _g = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    f()
}

#[derive(Debug, Clone, Copy)]
struct CrashCut {
    cut_id: u8,
    source_range: &'static str,
    probe_cut: Phase0ProbeCut,
    chain_commit_expected: bool,
    commit_txn_expected: bool,
}

const CUTS: [CrashCut; 6] = [
    CrashCut {
        cut_id: 1,
        source_range: "src/storage/paged_engine.rs:437-443",
        probe_cut: Phase0ProbeCut::AfterAllocateCommitTs,
        chain_commit_expected: false,
        commit_txn_expected: false,
    },
    CrashCut {
        cut_id: 2,
        source_range: "src/storage/paged_engine.rs:445-455",
        probe_cut: Phase0ProbeCut::AfterInstallPendingPrimary,
        chain_commit_expected: false,
        commit_txn_expected: false,
    },
    CrashCut {
        cut_id: 3,
        source_range: "src/storage/paged_engine.rs:463-464",
        probe_cut: Phase0ProbeCut::AfterOverlayCommit,
        chain_commit_expected: false,
        commit_txn_expected: false,
    },
    CrashCut {
        cut_id: 4,
        source_range: "src/storage/paged_engine.rs:469-469",
        probe_cut: Phase0ProbeCut::AfterFlushBeforeChainCommit,
        chain_commit_expected: false,
        commit_txn_expected: false,
    },
    CrashCut {
        cut_id: 5,
        source_range: "src/storage/paged_engine.rs:470-471",
        probe_cut: Phase0ProbeCut::AfterChainCommitBeforeCommitTxn,
        chain_commit_expected: true,
        commit_txn_expected: false,
    },
    CrashCut {
        cut_id: 6,
        source_range: "src/storage/paged_engine.rs:491-502",
        probe_cut: Phase0ProbeCut::AfterCommitTxnBeforePublish,
        chain_commit_expected: true,
        commit_txn_expected: true,
    },
];

const DB_NAME: &str = "ccmdb";
const COL_NAME: &str = "crud";
const NS_NAME: &str = "ccmdb.crud";
const PRECEDING_COMMITS: usize = 2;
const BASELINE_LEGACY_COMMITS: usize = 0;
const CUT_COMMIT_ID: i32 = 99;

fn preceding_ids() -> Vec<i32> {
    (0..PRECEDING_COMMITS as i32).collect()
}

fn build_workload(cut: &CrashCut) -> (tempfile::TempDir, std::path::PathBuf, Phase0ProbeReport) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("ccm.mqlite");

    let (report, pre_probe_journal_len) = {
        let client = Client::open_with_options(
            &db_path,
            OpenOptions::new().durability(DurabilityMode::FullSync),
        )
        .expect("open");
        let db = client.database(DB_NAME);
        db.create_collection(COL_NAME).expect("create collection");
        let col = db.collection::<Document>(COL_NAME);
        for id in preceding_ids() {
            col.insert_one(&doc! { "_id": id, "kind": "preceding" })
                .expect("insert preceding");
        }
        client.checkpoint().expect("checkpoint baseline commits");
        let pre_probe_journal_len =
            fs::metadata(crash_harness::journal_path(&db_path)).map_or(32, |m| m.len());
        let report = client
            .__phase0_probe_insert(
                NS_NAME,
                doc! { "_id": CUT_COMMIT_ID, "kind": "cut" },
                cut.probe_cut,
            )
            .expect("phase0 probe insert");
        std::mem::forget(client);
        (report, pre_probe_journal_len)
    };

    if cut.probe_cut == Phase0ProbeCut::AfterOverlayCommit {
        crash_harness::truncate_journal_to_offset(&db_path, pre_probe_journal_len)
            .expect("truncate unflushed cut-3 journal tail");
    }

    (dir, db_path, report)
}

fn visible_ids_after_reopen(client: &Client) -> BTreeSet<i32> {
    let col = client.database(DB_NAME).collection::<Document>(COL_NAME);
    let cursor = col.find(doc! {}).run().expect("find all");
    let mut out = BTreeSet::new();
    for item in cursor {
        let d = item.expect("doc");
        out.insert(d.get_i32("_id").expect("_id i32"));
    }
    out
}

fn assert_preceding_visible(cut: CrashCut, ids: &BTreeSet<i32>) {
    for id in preceding_ids() {
        assert!(
            ids.contains(&id),
            "cut {} invariant failed: preceding committed write _id={} \
             must remain durable",
            cut.cut_id,
            id
        );
    }
}

fn assert_cut_absent(cut: CrashCut, ids: &BTreeSet<i32>) {
    assert!(
        !ids.contains(&CUT_COMMIT_ID),
        "cut {} invariant failed: cut commit _id={} must not be visible",
        cut.cut_id,
        CUT_COMMIT_ID
    );
}

fn assert_cut_visible(cut: CrashCut, ids: &BTreeSet<i32>) {
    assert!(
        ids.contains(&CUT_COMMIT_ID),
        "cut {} invariant failed: commit_txn completed before the cut, so \
         cut commit _id={} must be visible after reopen",
        cut.cut_id,
        CUT_COMMIT_ID
    );
}

fn assert_frame_presence(cut: CrashCut, db_path: &std::path::Path, report: &Phase0ProbeReport) {
    let cut_ts = report.commit_ts.expect("probe must allocate commit_ts");
    let chain_frames = crash_harness::scan_chain_commits(db_path).expect("scan ChainCommit");
    let chain_present = chain_frames.iter().any(|(_, ts)| *ts == cut_ts);
    assert_eq!(
        chain_present, cut.chain_commit_expected,
        "cut {} HEAD-ORDERING failed: ChainCommit presence for cut_ts {:?} \
         was {}, expected {}",
        cut.cut_id, cut_ts, chain_present, cut.chain_commit_expected
    );

    let legacy_commit_frames =
        crash_harness::scan_legacy_commit_frames(db_path).expect("scan legacy commit frames");
    let expected_legacy_commits = BASELINE_LEGACY_COMMITS + usize::from(cut.commit_txn_expected);
    assert_eq!(
        legacy_commit_frames.len(),
        expected_legacy_commits,
        "cut {} HEAD-ORDERING failed: legacy commit-frame count mismatch",
        cut.cut_id
    );
}

fn assert_recovered_floor(cut: CrashCut, db_path: &std::path::Path, report: &Phase0ProbeReport) {
    let cut_ts = report.commit_ts.expect("probe must allocate commit_ts");
    let (_client, recovery) = crash_harness::reopen_inspect(db_path).expect("reopen inspect");
    match cut.chain_commit_expected {
        true => assert!(
            recovery.recovered_max_commit_ts >= Some(cut_ts),
            "cut {} HEAD-ORDERING failed: recovered_max_commit_ts {:?} must \
             include the cut ChainCommit ts {:?}",
            cut.cut_id,
            recovery.recovered_max_commit_ts,
            cut_ts
        ),
        false => assert!(
            recovery.recovered_max_commit_ts < Some(cut_ts),
            "cut {} HEAD-ORDERING failed: recovered_max_commit_ts {:?} must \
             exclude the uncommitted cut ts {:?}",
            cut.cut_id,
            recovery.recovered_max_commit_ts,
            cut_ts
        ),
    }
}

fn run_invariant(cut: CrashCut) {
    with_test_lock(|| {
        let (_dir, db_path, _report) = build_workload(&cut);
        let (client, _recovery) = crash_harness::reopen_inspect(&db_path).expect("reopen");
        let ids = visible_ids_after_reopen(&client);
        assert_preceding_visible(cut, &ids);
        if cut.commit_txn_expected {
            assert_cut_visible(cut, &ids);
        } else {
            assert_cut_absent(cut, &ids);
        }
    });
}

fn run_head_ordering(cut: CrashCut) {
    with_test_lock(|| {
        let (_dir, db_path, report) = build_workload(&cut);
        assert_frame_presence(cut, &db_path, &report);
        assert_recovered_floor(cut, &db_path, &report);
        let _ = cut.source_range;
    });
}

#[test]
fn cut1_invariant_reopen_preserves_committed_writes_no_damage() {
    run_invariant(CUTS[0]);
}

#[test]
fn cut1_head_ordering_no_chain_commit_for_allocated_only() {
    run_head_ordering(CUTS[0]);
}

#[test]
fn cut2_invariant_rollback_leaves_no_durable_damage() {
    run_invariant(CUTS[1]);
}

#[test]
fn cut2_head_ordering_no_chain_commit_and_pre_commit_main_file() {
    run_head_ordering(CUTS[1]);
}

#[test]
fn cut3_invariant_rollback_leaves_no_durable_damage() {
    run_invariant(CUTS[2]);
}

#[test]
fn cut3_head_ordering_journal_tail_discarded_on_reopen() {
    run_head_ordering(CUTS[2]);
}

#[test]
fn cut4_invariant_rollback_leaves_no_durable_damage() {
    run_invariant(CUTS[3]);
}

#[test]
fn cut4_head_ordering_no_chain_commit_frame_and_hlc_excludes_cut_ts() {
    run_head_ordering(CUTS[3]);
}

#[test]
fn cut5_invariant_rollback_leaves_no_durable_damage() {
    run_invariant(CUTS[4]);
}

#[test]
fn cut5_head_ordering_chain_commit_present_hlc_includes_cut_ts() {
    run_head_ordering(CUTS[4]);
}

#[test]
fn cut6_invariant_reopen_preserves_committed_writes() {
    run_invariant(CUTS[5]);
}

#[test]
fn cut6_head_ordering_legacy_commit_present_and_publish_runs_on_reopen() {
    with_test_lock(|| {
        let cut = CUTS[5];
        let (_dir, db_path, report) = build_workload(&cut);
        assert_frame_presence(cut, &db_path, &report);

        let (client, recovery) = crash_harness::reopen_inspect(&db_path).expect("reopen");
        let cut_ts = report.commit_ts.expect("probe must allocate commit_ts");
        assert!(
            recovery.recovered_max_commit_ts >= Some(cut_ts),
            "cut {} HEAD-ORDERING failed: recovered_max_commit_ts {:?} must \
             include cut ts {:?}",
            cut.cut_id,
            recovery.recovered_max_commit_ts,
            cut_ts
        );
        let ids = visible_ids_after_reopen(&client);
        assert_cut_visible(cut, &ids);

        mqlite::mvcc::metrics::reset_published_snapshot_rebuilds();
        // Phase 1 US-006: only publications that REBUILD the catalog Arc
        // tick this counter (root-neutral CRUD reuses the Arc). Drive a
        // DDL so the post-reopen publish path is guaranteed to rebuild.
        client
            .database(DB_NAME)
            .create_collection("post_reopen_ddl_probe")
            .expect("post-reopen DDL");
        let rebuilds = mqlite::mvcc::metrics::published_snapshot_rebuilds_snapshot();
        assert!(
            rebuilds >= 1,
            "cut {} HEAD-ORDERING failed: a post-reopen DDL must cause \
             rebuild_and_publish_locked to run at least once, observed {}",
            cut.cut_id,
            rebuilds
        );
        let _ = cut.source_range;
    });
}
