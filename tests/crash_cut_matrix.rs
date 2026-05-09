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

//! Crash-cut matrix for the ordered Phase 8 write envelope.
//!
//! Each active test is an invariant-class predicate: preceding committed
//! writes remain visible, and the cut write is visible after reopen only once
//! the authoritative log record exists.

mod crash_harness;

use std::collections::BTreeSet;
use std::fs;
use std::sync::Mutex;

use bson::doc;
use bson::Document;
use mqlite::{Client, DurabilityMode, OpenOptions, WriteEnvelopeProbeCut};

static TEST_LOCK: Mutex<()> = Mutex::new(());

fn with_test_lock<R>(f: impl FnOnce() -> R) -> R {
    let _g = TEST_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    f()
}

#[derive(Debug, Clone, Copy)]
struct CrashCut {
    cut_id: &'static str,
    source_range: &'static str,
    probe_cut: WriteEnvelopeProbeCut,
    cut_commit_visible_expected: bool,
    truncate_unflushed_journal_tail: bool,
}

const REBASELINE_CUTS: [CrashCut; 6] = [
    CrashCut {
        cut_id: "stage",
        source_range: "staged body before commit timestamp",
        probe_cut: WriteEnvelopeProbeCut::AfterStageBeforeCommitTs,
        cut_commit_visible_expected: false,
        truncate_unflushed_journal_tail: false,
    },
    CrashCut {
        cut_id: "timestamp",
        source_range: "commit timestamp before logical frame",
        probe_cut: WriteEnvelopeProbeCut::AfterCommitTsBeforeLogicalFrame,
        cut_commit_visible_expected: false,
        truncate_unflushed_journal_tail: false,
    },
    CrashCut {
        cut_id: "logical",
        source_range: "logical frame before log reservation",
        probe_cut: WriteEnvelopeProbeCut::AfterLogicalFrameBeforeReservation,
        cut_commit_visible_expected: false,
        truncate_unflushed_journal_tail: false,
    },
    CrashCut {
        cut_id: "pending",
        source_range: "pending install before log reservation",
        probe_cut: WriteEnvelopeProbeCut::AfterPendingInstallBeforeReservation,
        cut_commit_visible_expected: false,
        truncate_unflushed_journal_tail: false,
    },
    CrashCut {
        cut_id: "written",
        source_range: "log record write before durability wait",
        probe_cut: WriteEnvelopeProbeCut::AfterLogRecordWriteBeforeDurabilityWait,
        cut_commit_visible_expected: true,
        truncate_unflushed_journal_tail: false,
    },
    CrashCut {
        cut_id: "durable",
        source_range: "durable log record before publish",
        probe_cut: WriteEnvelopeProbeCut::AfterDurabilityWaitBeforePublish,
        cut_commit_visible_expected: true,
        truncate_unflushed_journal_tail: false,
    },
];

const DB_NAME: &str = "ccmdb";
const COL_NAME: &str = "crud";
const NS_NAME: &str = "ccmdb.crud";
const PRECEDING_COMMITS: usize = 2;
const CUT_COMMIT_ID: i32 = 99;

fn preceding_ids() -> Vec<i32> {
    (0..PRECEDING_COMMITS as i32).collect()
}

fn build_workload(cut: &CrashCut) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("ccm.mqlite");

    let pre_probe_journal_len = {
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
        client
            .__crash_cut_probe_insert(
                NS_NAME,
                doc! { "_id": CUT_COMMIT_ID, "kind": "cut" },
                cut.probe_cut,
            )
            .expect("crash-cut probe insert");
        std::mem::forget(client);
        pre_probe_journal_len
    };

    if cut.truncate_unflushed_journal_tail {
        crash_harness::truncate_journal_to_offset(&db_path, pre_probe_journal_len)
            .expect("truncate unflushed journal tail");
    }

    (dir, db_path)
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
            "cut {} ({}) invariant failed: preceding committed write _id={} \
             must remain durable",
            cut.cut_id,
            cut.source_range,
            id
        );
    }
}

fn assert_cut_absent(cut: CrashCut, ids: &BTreeSet<i32>) {
    assert!(
        !ids.contains(&CUT_COMMIT_ID),
        "cut {} ({}) invariant failed: cut commit _id={} must not be visible",
        cut.cut_id,
        cut.source_range,
        CUT_COMMIT_ID
    );
}

fn assert_cut_visible(cut: CrashCut, ids: &BTreeSet<i32>) {
    assert!(
        ids.contains(&CUT_COMMIT_ID),
        "cut {} ({}) invariant failed: durable log-record evidence \
         must make cut commit _id={} visible after reopen",
        cut.cut_id,
        cut.source_range,
        CUT_COMMIT_ID
    );
}

fn run_invariant(cut: CrashCut) {
    with_test_lock(|| {
        let (_dir, db_path) = build_workload(&cut);
        let (client, _recovery) = crash_harness::reopen_inspect(&db_path).expect("reopen");
        let ids = visible_ids_after_reopen(&client);
        assert_preceding_visible(cut, &ids);
        if cut.cut_commit_visible_expected {
            assert_cut_visible(cut, &ids);
        } else {
            assert_cut_absent(cut, &ids);
        }
    });
}

#[test]
fn cut_stage_invariant_staged_body_without_commit_ts_reopens_without_cut_write() {
    run_invariant(REBASELINE_CUTS[0]);
}

#[test]
fn cut_timestamp_invariant_allocated_ts_without_logical_frame_reopens_without_cut_write() {
    run_invariant(REBASELINE_CUTS[1]);
}

#[test]
fn cut_logical_invariant_logical_frame_without_log_record_reopens_without_cut_write() {
    run_invariant(REBASELINE_CUTS[2]);
}

#[test]
fn cut_pending_invariant_pending_install_without_log_record_reopens_without_cut_write() {
    run_invariant(REBASELINE_CUTS[3]);
}

#[test]
fn cut_written_invariant_log_record_write_replays_commit() {
    run_invariant(REBASELINE_CUTS[4]);
}

#[test]
fn cut_durable_invariant_pre_publish_durable_commit_replays() {
    run_invariant(REBASELINE_CUTS[5]);
}
