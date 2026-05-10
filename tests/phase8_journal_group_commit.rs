#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test targets use assertion-style panics"
)]
#![doc = "Integration test target for Phase 8 journal group commit stories."]
#![cfg(feature = "test-hooks")]

mod crash_harness;

use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::{env, fs, path::Path, path::PathBuf};

use bson::{doc, Bson, Document};
use mqlite::error::{EngineFatalReason, WriteConflictReason};
use mqlite::{
    arm_checkpoint_boundary_failpoint, CheckpointBoundaryFailpoint, Client, DurabilityMode, Error,
    IndexModel, JournalCatalogCommitKind, JournalLogRecordKind, JournalLogRecordSummary,
    OpenOptions, Us026PostRegisterFailpoint,
};
use serial_test::serial;

use crash_harness::journal_path;

const JOURNAL_GROUP_COMMIT_NS: &str = "phase8.docs";
const HEADER_PAGE_SIZE: usize = 4096;
const LAST_CHECKPOINT_TS_OFFSET: usize = 24;
const CHECKPOINT_APPLIED_LSN_OFFSET: usize = 102;
const CHECKPOINT_CRASH_CHILD_ENV: &str = "MQLITE_PHASE8_CHECKPOINT_CRASH_CHILD";
const CHECKPOINT_CRASH_DB_ENV: &str = "MQLITE_PHASE8_CHECKPOINT_CRASH_DB";
const JOURNAL_GROUP_COMMIT_SETTLE_DEADLINE: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HeaderState {
    last_checkpoint_ts: (u64, u32),
    checkpoint_applied_lsn: u64,
}

fn open_journal_group_commit_client(
    name: &str,
    durability: DurabilityMode,
) -> (tempfile::TempDir, Client) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(name);
    let client = Client::open_with_options(&path, OpenOptions::new().durability(durability))
        .expect("open phase8 test database");
    (dir, client)
}

fn open_fullsync(path: &Path) -> Client {
    Client::open_with_options(
        path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open fullsync phase8 test database")
}

fn journal_log_records(client: &Client) -> Vec<JournalLogRecordSummary> {
    client
        .__journal_log_records()
        .expect("decode phase8 log records")
}

fn catalog_kinds(records: &[JournalLogRecordSummary]) -> Vec<JournalCatalogCommitKind> {
    records
        .iter()
        .filter_map(|record| record.catalog_kind)
        .collect()
}

fn assert_catalog_kinds_in_order(
    records: &[JournalLogRecordSummary],
    expected: &[JournalCatalogCommitKind],
) {
    let actual = catalog_kinds(records);
    let mut cursor = 0;
    for kind in expected {
        let Some(next) = actual[cursor..].iter().position(|actual| actual == kind) else {
            panic!("missing catalog kind {kind:?} in {actual:?}");
        };
        cursor += next + 1;
    }
}

fn assert_doc_value(client: &Client, id: i32, expected: Option<&str>) {
    let got = client
        .database("phase8")
        .collection::<Document>("docs")
        .find_one(doc! { "_id": id })
        .expect("read phase8 document");
    match expected {
        Some(value) => {
            let got = got.expect("expected document to exist");
            assert_eq!(got.get_str("mode").unwrap(), value);
        }
        None => assert!(got.is_none(), "document {id} must be absent"),
    }
}

fn crud_log_records(client: &Client) -> Vec<JournalLogRecordSummary> {
    journal_log_records(client)
        .into_iter()
        .filter(|record| record.kind == JournalLogRecordKind::CrudCommit)
        .collect()
}

fn read_header_state(path: &Path) -> HeaderState {
    let bytes = fs::read(path).expect("read main file");
    assert!(bytes.len() >= HEADER_PAGE_SIZE);
    let physical_ms = u64::from_le_bytes(
        bytes[LAST_CHECKPOINT_TS_OFFSET..LAST_CHECKPOINT_TS_OFFSET + 8]
            .try_into()
            .expect("last checkpoint physical bytes"),
    );
    let logical = u32::from_le_bytes(
        bytes[LAST_CHECKPOINT_TS_OFFSET + 8..LAST_CHECKPOINT_TS_OFFSET + 12]
            .try_into()
            .expect("last checkpoint logical bytes"),
    );
    let checkpoint_applied_lsn = u64::from_le_bytes(
        bytes[CHECKPOINT_APPLIED_LSN_OFFSET..CHECKPOINT_APPLIED_LSN_OFFSET + 8]
            .try_into()
            .expect("checkpoint lsn bytes"),
    );
    HeaderState {
        last_checkpoint_ts: (physical_ms, logical),
        checkpoint_applied_lsn,
    }
}

fn ts_gt(left: (u64, u32), right: (u64, u32)) -> bool {
    left.0 > right.0 || (left.0 == right.0 && left.1 > right.1)
}

fn run_checkpoint_crash_child_if_requested() -> bool {
    if env::var(CHECKPOINT_CRASH_CHILD_ENV).ok().as_deref() != Some("1") {
        return false;
    }
    let path = PathBuf::from(env::var(CHECKPOINT_CRASH_DB_ENV).expect("checkpoint crash db env"));
    let client = open_fullsync(&path);
    let collection = client.database("phase8").collection::<Document>("docs");
    collection
        .insert_one(&doc! { "_id": 1i32, "phase": "before-crash-cut" })
        .expect("child insert before checkpoint crash cut");
    let _guard = arm_checkpoint_boundary_failpoint(
        CheckpointBoundaryFailpoint::AfterMaterializationFlushBeforeBoundary,
    )
    .expect("arm checkpoint crash failpoint");
    client
        .checkpoint()
        .expect("child checkpoint should reach armed crash failpoint");
    panic!("armed Phase 8 checkpoint failpoint did not abort the child process");
}

#[test]
fn journal_group_commit_target_is_wired() {
    assert_eq!("phase8_journal_group_commit", module_path!());
}

fn source_between<'a>(source: &'a str, start: &str, end: &str) -> &'a str {
    source
        .split(start)
        .nth(1)
        .expect("start marker exists")
        .split(end)
        .next()
        .expect("end marker exists")
}

#[test]
fn commit_append_hot_path_uses_log_manager_reservations() {
    let source = include_str!("../src/journal/mod.rs");
    let chain_body = source_between(
        source,
        "fn append_chain_commit_record",
        "/// Append a `LogicalTxnFrame`",
    );
    let logical_body = source_between(
        source,
        "pub(crate) fn append_logical_txn",
        "/// Append a page-0 checkpoint commit boundary",
    );

    for body in [chain_body, logical_body] {
        assert!(body.contains("self.log_manager.reserve(bytes.len())"));
        assert!(body.contains("self.log_manager.write_reserved(&slot, &bytes)"));
        assert!(body.contains("self.log_manager.mark_written(&slot)"));
        assert!(!body.contains("frame_offset = self.write_cursor"));
        assert!(!body.contains(".seek("));
        assert!(!body.contains(".write_all("));
    }
}

#[test]
fn sync_journal_uses_log_manager_durable_frontier() {
    let source = include_str!("../src/journal/mod.rs");
    let sync_body = source_between(
        source,
        "pub(crate) fn sync_journal",
        "// -----------------------------------------------------------------------",
    );

    assert!(sync_body.contains("self.log_manager.ensure_sync(self.log_manager.next_lsn())"));
    assert!(!sync_body.contains("self.journal_file.sync_data()"));
}

#[test]
fn fullsync_commit_waits_on_end_lsn_not_ticket_manager() {
    let engine = include_str!("../src/storage/paged_engine.rs");
    let state = include_str!("../src/storage/paged_engine/state.rs");

    assert!(!engine.contains("mod group_commit;"));
    assert!(!engine.contains("fullsync_group_commit"));
    assert!(!engine.contains("shared.group_commit"));
    assert!(!state.contains("GroupCommitManager"));
    assert!(engine.contains("let commit_end_lsn = reserved.end_lsn()"));
    assert!(engine.contains("self.wait_for_commit_durability(commit_end_lsn)?"));
    assert!(engine.contains("DurabilityMode::FullSync"));
    assert!(engine.contains("self.shared.handle.wait_journal_durable(end_lsn)"));
    assert!(engine.contains("DurabilityMode::Interval(_) | DurabilityMode::None"));
    assert!(engine.contains("self.shared.handle.wait_journal_ready(end_lsn)"));
}

#[test]
fn interval_and_none_use_ready_frontier_without_commit_sync_wait() {
    let engine = include_str!("../src/storage/paged_engine.rs");

    assert!(engine.contains("fn maybe_sync_interval_after_publish"));
    assert!(engine.contains("DurabilityMode::Interval(interval)"));
    assert!(engine.contains("sync_journal_ready_prefix()"));
    assert!(engine.contains("DurabilityMode::None"));
    assert!(!engine.contains("!matches!(self.durability_mode, DurabilityMode::FullSync)"));
}

#[test]
fn dirty_pages_have_explicit_lsn_state() {
    let partition = include_str!("../src/storage/buffer_pool/partition.rs");

    assert!(partition.contains("enum PageDirtyLsn"));
    assert!(partition.contains("Clean"));
    assert!(partition.contains("Unflushable"));
    assert!(partition.contains("Dirty {"));
    assert!(partition.contains("last_lsn: u64"));
    assert!(partition.contains("AtomicU64"));
    assert!(partition.contains("Ordering::Release"));
    assert!(partition.contains("Ordering::Acquire"));
    assert!(!partition.contains("pub(super) dirty: bool"));
}

#[test]
fn dirty_eviction_skips_unflushable_or_undurable_pages() {
    let partition = include_str!("../src/storage/buffer_pool/partition.rs");

    assert!(partition.contains("fn find_victim(&mut self, durable_lsn: u64)"));
    assert!(partition.contains("!frame.can_flush_at(durable_lsn)"));
    assert!(partition.contains("continue;"));
    assert!(partition.contains("PageDirtyLsn::Unflushable"));
    assert!(partition.contains("last_lsn <= durable_lsn"));
    assert!(partition.contains("fn evict_frame("));
    assert!(partition.contains("frame.flushable_last_lsn(durable_lsn)"));
}

#[test]
fn main_file_flush_is_lsn_fenced() {
    let handle = include_str!("../src/storage/handle.rs");
    let buffer_pool = include_str!("../src/storage/buffer_pool/mod.rs");

    assert!(handle.contains("let durable_lsn = self.journal_durable_lsn()?"));
    assert!(handle.contains("self.pool.flush_lsn_fenced(durable_lsn)?"));
    assert!(handle.contains("self.history_pool.flush_lsn_fenced(durable_lsn)?"));
    assert!(handle.contains("checkpoint_applied_lsn"));
    assert!(handle.contains("checkpoint_dirty_frame_snapshots("));
    assert!(buffer_pool.contains("pub(crate) fn flush_lsn_fenced(&self, durable_lsn: u64)"));
    assert!(buffer_pool.contains("flush_all_lsn_fenced"));
    assert!(buffer_pool.contains("dirty_frame_snapshots_lsn_fenced"));
}

#[test]
fn checkpoint_fsyncs_main_file_before_boundary_record() {
    let snapshot_ops = include_str!("../src/storage/paged_engine/snapshot_ops.rs");
    let checkpoint_body = source_between(
        snapshot_ops,
        "fn checkpoint_after_reconcile_plan",
        "fn poison_checkpoint_post_mutation",
    );
    let first_flush_pos = checkpoint_body
        .find("engine.shared.handle.flush()?")
        .expect("checkpoint materialization flush exists");
    let early_return_pos = checkpoint_body[first_flush_pos..]
        .find("return Ok(());")
        .map(|offset| first_flush_pos + offset)
        .expect("checkpoint logical-tail early return exists");
    let header_advance_pos = checkpoint_body
        .find("header.checkpoint_applied_lsn = checkpoint_applied_lsn")
        .expect("checkpoint advances applied LSN in header");
    let second_flush_pos = checkpoint_body[header_advance_pos..]
        .find("engine.shared.handle.flush()?")
        .map(|offset| header_advance_pos + offset)
        .expect("checkpoint flushes advanced header");
    let sync_pos = checkpoint_body
        .find("sync_main_file()?")
        .expect("checkpoint syncs main file");
    let boundary_pos = checkpoint_body
        .find("LogRecordDraft::checkpoint_boundary")
        .expect("checkpoint writes boundary record");

    assert!(
        first_flush_pos < early_return_pos && early_return_pos < header_advance_pos,
        "checkpoint_applied_lsn must not advance before the logical-tail early return"
    );
    assert!(
        header_advance_pos < second_flush_pos
            && second_flush_pos < sync_pos
            && sync_pos < boundary_pos,
        "main file/header must be flushed and synced before CheckpointBoundary"
    );
}

#[test]
fn commit_stamps_dirty_pages_with_commit_end_lsn_before_waiting() {
    let engine = include_str!("../src/storage/paged_engine.rs");
    let run_write_commit_envelope = source_between(
        engine,
        "fn run_write_commit_envelope",
        "fn register_ordinary_crud_slot",
    );
    let stamp = "stamp_dirty_pages_lsn(&pending_pages, commit_end_lsn)";
    let wait = "self.wait_for_commit_durability(commit_end_lsn)?";
    let stamp_pos = run_write_commit_envelope
        .find(stamp)
        .expect("commit stamps touched pages");
    let wait_pos = run_write_commit_envelope
        .find(wait)
        .expect("commit waits for durability after stamping");

    assert!(
        stamp_pos < wait_pos,
        "dirty page LSN stamping must precede durability waiting"
    );
    assert!(run_write_commit_envelope.contains("LogRecordDraft::crud("));
    assert!(run_write_commit_envelope.contains("self.shared.handle.reserve_log_record(draft)"));
    assert!(run_write_commit_envelope.contains("reserved.write_and_mark()"));
    assert!(
        !run_write_commit_envelope.contains("journal_ready_lsn()"),
        "ordinary commits must stamp pages with their own ChainCommit end LSN"
    );
    assert!(
        !run_write_commit_envelope.contains("self.lock_journal_mutex()"),
        "ordinary CRUD must not enter the production journal mutex"
    );
    assert!(
        !run_write_commit_envelope.contains("self.shared.handle.flush()"),
        "ordinary CRUD must not use commit-time handle.flush"
    );
    assert!(
        run_write_commit_envelope.contains("§4.6 deviation: US-006 installs Pending pages as")
            && run_write_commit_envelope.contains("Unflushable before reservation"),
        "the live install/reserve/stamp ordering deviation must be documented"
    );
}

#[test]
fn pre_durable_cleanup_does_not_drop_global_dirty_pages() {
    let engine = include_str!("../src/storage/paged_engine.rs");
    let cleanup = source_between(
        engine,
        "fn cleanup_registered_pre_durable_failure",
        "fn write_visibility_after_capture",
    );

    assert!(cleanup.contains("flip_pending_to_aborted_for(&self.shared, txn_id)"));
    assert!(cleanup.contains("self.shared.publish_sequencer.mark_aborted(slot)"));
    assert!(
        !cleanup.contains("rollback_txn"),
        "pre-reservation cleanup must not call rollback_txn/drop_all_dirty"
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn none_commit_waits_ready_without_syncing() {
    let (_dir, client) =
        open_journal_group_commit_client("phase8_none.mqlite", DurabilityMode::None);
    client.__us017_reset_group_commit_probe();
    client.__us039_reset_append_sync_observations();

    let collection = client.database("phase8").collection::<Document>("docs");
    collection
        .insert_one(&doc! { "_id": 1i32, "mode": "none" })
        .expect("None durability commit publishes after ready log write");

    let obs = client.__us039_append_sync_observations();
    assert_eq!(obs.handle_journal_syncs, 0);
    assert_eq!(obs.journal_sync_os_boundaries, 0);
    assert_eq!(
        collection
            .count_documents(doc! {})
            .expect("count visible doc"),
        1
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn interval_sync_failure_poisons_after_visible_interval_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_interval_failure.mqlite");
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::Interval(Duration::ZERO)),
    )
    .expect("open phase8 interval failure database");
    let collection = client.database("phase8").collection::<Document>("docs");

    client.__us017_reset_group_commit_probe();
    collection
        .insert_one(&doc! { "_id": 1i32, "mode": "interval-baseline" })
        .expect("baseline interval write succeeds");
    let baseline_group = client.__us017_group_commit_observations();
    assert_eq!(baseline_group.fsync_failures, 0);
    assert!(
        baseline_group.last_fsync_seq > 0,
        "baseline interval write must complete a periodic sync"
    );

    client.__us017_reset_group_commit_probe();
    client.__us039_reset_append_sync_observations();
    client.__us017_fail_next_group_commit_fsync();

    let err = collection
        .insert_one(&doc! { "_id": 2i32, "mode": "interval-failed-sync" })
        .expect_err("interval sync failure poisons the live engine");
    assert!(matches!(
        err,
        Error::EngineFatal {
            reason: EngineFatalReason::PostDurablePublishFailure
        }
    ));

    let group = client.__us017_group_commit_observations();
    assert_eq!(group.fsync_failures, 1);
    assert!(group.failed_high_water > 0);
    assert!(matches!(
        collection.find_one(doc! { "_id": 1i32 }),
        Err(Error::EngineFatal {
            reason: EngineFatalReason::PostDurablePublishFailure
        })
    ));

    drop(collection);
    drop(client);

    let reopened = Client::open(&path).expect("reopen after interval sync failure poison");
    let recovered = reopened.database("phase8").collection::<Document>("docs");
    let baseline = recovered
        .find_one(doc! { "_id": 1i32 })
        .expect("read recovered baseline interval write")
        .expect("baseline interval write survives reopen recovery");
    assert_eq!(
        baseline.get_str("mode").unwrap(),
        "interval-baseline",
        "previously synced interval write must recover intact"
    );

    let failed_sync_doc = recovered
        .find_one(doc! { "_id": 2i32 })
        .expect("reopen recovery decides failed-sync interval write");
    if let Some(doc) = &failed_sync_doc {
        assert_eq!(
            doc.get_str("mode").unwrap(),
            "interval-failed-sync",
            "surviving failed-sync write must recover intact"
        );
    }
    assert_eq!(
        recovered
            .count_documents(doc! {})
            .expect("count recovered interval writes"),
        1 + u64::from(failed_sync_doc.is_some()),
        "recovered count must match the durable prefix selected by recovery"
    );
}

#[test]
fn recovery_scans_contiguous_log_records_and_truncates_once() {
    let source = include_str!("../src/journal/recovery.rs");

    assert!(source.contains("fn scan_log_records("));
    assert!(source.contains("fn read_log_record_at("));
    assert!(source.contains("fn truncate_tail_to_valid_end_lsn("));
    assert!(source.contains("LogRecord::decode(&bytes)"));
    assert!(source.contains("record.start_lsn != cursor"));
    assert!(source.contains("sort_by_key(|record| record.record.publish_seq)"));
    assert!(source.contains("duplicate Phase 8 LogRecord publish_seq"));
    assert!(source.contains("LogRecordKind::CheckpointBoundary"));
}

#[test]
fn reopen_seeds_live_publish_sequencer_above_recovered_floor() {
    let state = include_str!("../src/storage/paged_engine/state.rs");
    let sequencer = include_str!("../src/storage/paged_engine/publish_sequencer.rs");

    assert!(state.contains("let recovered_max_publish_seq = handle.recovered_max_publish_seq()?"));
    assert!(state.contains("seq.checked_add(1)"));
    assert!(state.contains("PublishSequencer::new_with_recovery_state"));
    assert!(sequencer.contains("pub(crate) fn new_with_recovery_state"));
    assert!(sequencer.contains("last_published: next_seq.saturating_sub(1)"));
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn same_key_conflict_after_peer_commit_does_not_advance_lsn() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_same_key_conflict.mqlite");
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open phase8 same-key database");
    client
        .database("phase8")
        .create_collection("docs")
        .expect("create docs collection");

    let mut hook = client.__install_write_body_entry_hook(JOURNAL_GROUP_COMMIT_NS);
    let stalled = client.clone();
    let stalled_writer = thread::spawn(move || {
        stalled
            .database("phase8")
            .collection::<Document>("docs")
            .insert_one(&doc! { "_id": 1i32, "mark": "stalled" })
    });
    hook.wait_until_entered()
        .expect("stalled writer entered body");

    let collection = client.database("phase8").collection::<Document>("docs");
    collection
        .insert_one(&doc! { "_id": 1i32, "mark": "winner" })
        .expect("peer writer commits first");
    let after_winner = client
        .__journal_lsn_snapshot()
        .expect("snapshot after winning commit");

    hook.release().expect("release stalled writer");
    let stalled_result = stalled_writer
        .join()
        .expect("stalled writer thread must not panic");
    assert!(matches!(
        stalled_result,
        Err(Error::WriteConflict {
            reason: WriteConflictReason::SameKeyConflict { .. },
        })
    ));
    let after_conflict = client
        .__journal_lsn_snapshot()
        .expect("snapshot after same-key conflict");
    assert_eq!(
        after_conflict, after_winner,
        "same-key loser must not reserve or write a complete log record"
    );

    drop(collection);
    drop(client);

    let reopened = Client::open(&path).expect("reopen after same-key conflict");
    let recovered = reopened.database("phase8").collection::<Document>("docs");
    let doc = recovered
        .find_one(doc! { "_id": 1i32 })
        .expect("read recovered winner")
        .expect("winner survives recovery");
    assert_eq!(
        doc.get_str("mark").unwrap(),
        "winner",
        "recovery must not publish the same-key loser"
    );
    assert_eq!(
        recovered
            .count_documents(doc! {})
            .expect("count recovered docs"),
        1
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn pre_reservation_failure_writes_no_complete_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_pre_reservation_failure.mqlite");
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open phase8 pre-reservation database");
    client
        .database("phase8")
        .create_collection("docs")
        .expect("create docs collection");
    let collection = client.database("phase8").collection::<Document>("docs");
    let before = client
        .__journal_lsn_snapshot()
        .expect("snapshot before pre-reservation failure");

    client.__us026_arm_post_register_failpoint(Us026PostRegisterFailpoint::BeforeLogReservation);
    let err = collection
        .insert_one(&doc! { "_id": 7i32, "mode": "pre-reservation" })
        .expect_err("pre-reservation failpoint aborts the write");
    assert!(matches!(err, Error::Internal(message) if message.contains("US-026 injected")));

    let after = client
        .__journal_lsn_snapshot()
        .expect("snapshot after pre-reservation failure");
    assert_eq!(
        after, before,
        "pre-reservation abort must not advance next/ready/durable LSN"
    );
    let states = client
        .__us009_primary_chain_states(JOURNAL_GROUP_COMMIT_NS, &Bson::Int32(7))
        .expect("inspect aborted pending chain");
    assert_eq!(
        states.first().map(String::as_str),
        Some("Aborted"),
        "pre-reservation cleanup must abort the installed Pending head"
    );

    drop(collection);
    drop(client);

    let reopened = Client::open(&path).expect("reopen after pre-reservation failure");
    let recovered = reopened.database("phase8").collection::<Document>("docs");
    assert!(
        recovered
            .find_one(doc! { "_id": 7i32 })
            .expect("read after pre-reservation recovery")
            .is_none(),
        "pre-reservation failure must leave no recoverable committed record"
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn pre_reservation_cleanup_preserves_other_writer_pending_pages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_cleanup_preserves_peer.mqlite");
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open phase8 peer-pending database");
    client
        .database("phase8")
        .create_collection("docs")
        .expect("create docs collection");

    let mut before_reservation = client.__install_before_log_reservation_hook();
    let peer = client.clone();
    let peer_writer = thread::spawn(move || {
        peer.database("phase8")
            .collection::<Document>("docs")
            .insert_one(&doc! { "_id": 20i32, "writer": "peer" })
    });
    before_reservation
        .wait_until_entered()
        .expect("peer writer reached before-reservation hook");

    client.__us026_arm_post_register_failpoint(Us026PostRegisterFailpoint::BeforeLogReservation);
    let collection = client.database("phase8").collection::<Document>("docs");
    let cleanup_err = collection
        .insert_one(&doc! { "_id": 21i32, "writer": "cleanup" })
        .expect_err("pre-reservation cleanup writer aborts");
    assert!(matches!(cleanup_err, Error::Internal(message) if message.contains("US-026 injected")));

    before_reservation
        .release()
        .expect("release peer writer after cleanup");
    peer_writer
        .join()
        .expect("peer writer thread must not panic")
        .expect("peer writer must commit after another writer cleanup");

    assert!(
        collection
            .find_one(doc! { "_id": 20i32 })
            .expect("read peer after cleanup")
            .is_some(),
        "pre-reservation cleanup must not drop another writer's Pending pages"
    );
    assert!(
        collection
            .find_one(doc! { "_id": 21i32 })
            .expect("read cleanup loser")
            .is_none(),
        "cleanup loser must remain aborted"
    );

    drop(collection);
    drop(client);

    let reopened = Client::open(&path).expect("reopen after peer-pending cleanup");
    let recovered = reopened.database("phase8").collection::<Document>("docs");
    assert!(
        recovered
            .find_one(doc! { "_id": 20i32 })
            .expect("read recovered peer")
            .is_some(),
        "peer writer must recover intact after overlapping cleanup"
    );
    assert!(
        recovered
            .find_one(doc! { "_id": 21i32 })
            .expect("read recovered cleanup loser")
            .is_none(),
        "cleanup loser must leave no recovered record"
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn namespace_create_drop_recovers_catalog_commits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_namespace_recovery.mqlite");
    let client = open_fullsync(&path);

    client
        .database("phase8")
        .create_collection("ddl")
        .expect("create namespace through Client");
    let create_records = journal_log_records(&client);
    let create = create_records
        .iter()
        .find(|record| record.catalog_kind == Some(JournalCatalogCommitKind::NamespaceCreate))
        .expect("namespace create writes CatalogCommit");
    assert_eq!(create.kind, JournalLogRecordKind::CatalogCommit);
    assert!(create.publish_seq > 0);
    assert!(
        create.catalog_generation_after > create.catalog_generation_before,
        "namespace create must publish a new catalog generation"
    );
    std::mem::forget(client);

    let reopened = open_fullsync(&path);
    assert!(
        reopened
            .database("phase8")
            .list_collection_names()
            .expect("list recovered namespaces")
            .contains(&"ddl".to_owned()),
        "CatalogCommit recovery must make the created namespace visible"
    );

    reopened
        .database("phase8")
        .drop_collection("ddl")
        .expect("drop namespace through Client");
    let drop_records = journal_log_records(&reopened);
    let drop_record = drop_records
        .iter()
        .find(|record| record.catalog_kind == Some(JournalCatalogCommitKind::NamespaceDrop))
        .expect("namespace drop writes CatalogCommit");
    assert_eq!(drop_record.kind, JournalLogRecordKind::CatalogCommit);
    assert!(drop_record.publish_seq > create.publish_seq);
    std::mem::forget(reopened);

    let recovered = open_fullsync(&path);
    assert!(
        !recovered
            .database("phase8")
            .list_collection_names()
            .expect("list after recovered drop")
            .contains(&"ddl".to_owned()),
        "NamespaceDrop CatalogCommit must recover as an absent namespace"
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn create_and_drop_index_recovers_typed_catalog_records() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_index_recovery.mqlite");
    let client = open_fullsync(&path);
    let collection = client.database("phase8").collection::<Document>("docs");
    for id in 0..80i32 {
        collection
            .insert_one(&doc! {
                "_id": id,
                "tag": format!("tag-{}", id % 4),
                "payload": format!("payload-{id:03}"),
            })
            .expect("seed index document");
    }

    let baseline = journal_log_records(&client).len();
    let index_name = collection
        .create_index(IndexModel::builder().keys(doc! { "tag": 1 }).build())
        .expect("create index through Client");
    assert_eq!(index_name, "tag_1");
    let create_records = journal_log_records(&client);
    let create_delta = &create_records[baseline..];
    assert_catalog_kinds_in_order(
        create_delta,
        &[
            JournalCatalogCommitKind::IndexReserve,
            JournalCatalogCommitKind::IndexBuild,
            JournalCatalogCommitKind::IndexBuildCommit,
        ],
    );
    assert!(
        create_delta
            .iter()
            .filter(|record| record.kind == JournalLogRecordKind::CatalogCommit)
            .all(|record| record.publish_seq > 0),
        "index catalog records must use nonzero publish sequences"
    );
    drop(collection);
    std::mem::forget(client);

    let reopened = open_fullsync(&path);
    let recovered = reopened.database("phase8").collection::<Document>("docs");
    let cursor = recovered
        .find(doc! { "tag": "tag-3" })
        .run()
        .expect("query recovered index");
    let explain = cursor.explain().expect("explain recovered index query");
    let docs = cursor
        .collect::<mqlite::Result<Vec<_>>>()
        .expect("collect recovered index results");
    assert_eq!(explain.index_used.as_deref(), Some("tag_1"));
    assert_eq!(docs.len(), 20);

    let before_drop = journal_log_records(&reopened).len();
    recovered
        .drop_index("tag_1")
        .expect("drop index through Client");
    let drop_records = journal_log_records(&reopened);
    assert_catalog_kinds_in_order(
        &drop_records[before_drop..],
        &[JournalCatalogCommitKind::IndexDrop],
    );
    drop(recovered);
    std::mem::forget(reopened);

    let after_drop = open_fullsync(&path);
    let indexes = after_drop
        .database("phase8")
        .collection::<Document>("docs")
        .list_indexes()
        .expect("list indexes after recovered drop");
    assert!(
        indexes.iter().all(|index| index.name != "tag_1"),
        "IndexDrop CatalogCommit must recover as an absent index"
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn failed_create_index_cleans_up_building_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_index_cleanup.mqlite");
    let client = open_fullsync(&path);
    let collection = client.database("phase8").collection::<Document>("docs");
    for id in 0..32i32 {
        collection
            .insert_one(&doc! { "_id": id, "tag": format!("tag-{}", id % 2) })
            .expect("seed cleanup document");
    }

    let baseline = journal_log_records(&client).len();
    let mut hook =
        client.__install_create_index_build_failure_hook(JOURNAL_GROUP_COMMIT_NS, "tag_1");
    let worker_client = client.clone();
    let worker = thread::spawn(move || {
        worker_client
            .database("phase8")
            .collection::<Document>("docs")
            .create_index(IndexModel::builder().keys(doc! { "tag": 1 }).build())
    });
    hook.wait_until_entered()
        .expect("create_index reaches build hook after reserve");
    hook.release().expect("release failing build hook");
    let err = worker
        .join()
        .expect("create_index worker must not panic")
        .expect_err("injected build failure must surface");
    assert!(matches!(err, Error::Internal(message) if message.contains("US-038 injected")));

    let records = journal_log_records(&client);
    let delta = &records[baseline..];
    assert_catalog_kinds_in_order(
        delta,
        &[
            JournalCatalogCommitKind::IndexReserve,
            JournalCatalogCommitKind::IndexCleanup,
        ],
    );
    assert!(
        !catalog_kinds(delta).contains(&JournalCatalogCommitKind::IndexBuildCommit),
        "failed create_index must not publish a Ready transition"
    );
    assert!(
        collection
            .list_indexes()
            .expect("list indexes after cleanup")
            .iter()
            .all(|index| index.name != "tag_1"),
        "cleanup must remove the Building reservation before reopen"
    );
    drop(collection);
    std::mem::forget(client);

    let recovered = open_fullsync(&path);
    let indexes = recovered
        .database("phase8")
        .collection::<Document>("docs")
        .list_indexes()
        .expect("list indexes after cleanup recovery");
    assert!(
        indexes.iter().all(|index| index.name != "tag_1"),
        "IndexCleanup CatalogCommit must recover as an absent index"
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn checkpoint_boundary_skips_prefix_and_replays_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_checkpoint_skip.mqlite");
    let client = open_fullsync(&path);
    let collection = client.database("phase8").collection::<Document>("docs");
    collection
        .insert_one(&doc! { "_id": 1i32, "phase": "before-checkpoint" })
        .expect("insert pre-checkpoint doc");

    client.__us039_reset_append_sync_observations();
    client.checkpoint().expect("checkpoint through Client");
    let sync_obs = client.__us039_append_sync_observations();
    assert!(
        sync_obs.main_file_syncs >= 1,
        "checkpoint must fsync the main file before its CheckpointBoundary is durable"
    );
    let checkpoint_header = read_header_state(&path);
    assert!(checkpoint_header.checkpoint_applied_lsn > 0);
    let boundary = journal_log_records(&client)
        .into_iter()
        .find(|record| record.kind == JournalLogRecordKind::CheckpointBoundary)
        .expect("checkpoint writes CheckpointBoundary");
    assert_eq!(boundary.publish_seq, 0);
    assert_eq!(
        boundary.checkpoint_applied_lsn,
        Some(checkpoint_header.checkpoint_applied_lsn)
    );
    assert!(
        boundary.end_lsn > checkpoint_header.checkpoint_applied_lsn,
        "CheckpointBoundary must sit after the materialized main-file frontier"
    );
    collection
        .insert_one(&doc! { "_id": 2i32, "phase": "after-checkpoint" })
        .expect("insert post-checkpoint doc");
    let post_checkpoint_records = journal_log_records(&client);
    assert!(
        post_checkpoint_records
            .iter()
            .any(|record| record.end_lsn <= checkpoint_header.checkpoint_applied_lsn),
        "test setup must include records at or below checkpoint_applied_lsn"
    );
    let post_checkpoint_publish_seq = post_checkpoint_records
        .iter()
        .filter(|record| record.end_lsn > checkpoint_header.checkpoint_applied_lsn)
        .filter(|record| record.kind != JournalLogRecordKind::CheckpointBoundary)
        .map(|record| record.publish_seq)
        .max()
        .expect("post-checkpoint commit exists");
    drop(collection);
    std::mem::forget(client);

    let reopened = open_fullsync(&path);
    let recovered = reopened.database("phase8").collection::<Document>("docs");
    assert_eq!(
        recovered
            .count_documents(doc! {})
            .expect("count recovered checkpoint straddle docs"),
        2,
        "recovery must keep checkpointed state and replay only the tail above the frontier"
    );
    assert!(recovered
        .find_one(doc! { "_id": 1i32 })
        .expect("read checkpointed doc")
        .is_some());
    assert!(recovered
        .find_one(doc! { "_id": 2i32 })
        .expect("read replayed tail doc")
        .is_some());
    let recovered_floor = reopened
        .__recovered_max_commit_ts()
        .expect("recovery folds checkpoint timestamp or tail commit timestamp");
    assert!(
        !ts_gt(checkpoint_header.last_checkpoint_ts, recovered_floor),
        "checkpoint timestamp must contribute to the recovered HLC floor"
    );

    let before_new_commit = journal_log_records(&reopened).len();
    recovered
        .insert_one(&doc! { "_id": 3i32, "phase": "after-reopen" })
        .expect("insert after checkpoint recovery");
    let records = journal_log_records(&reopened);
    let new_publish_seq = records[before_new_commit..]
        .iter()
        .find(|record| record.kind == JournalLogRecordKind::CrudCommit)
        .expect("post-reopen insert writes CrudCommit")
        .publish_seq;
    assert!(
        new_publish_seq > post_checkpoint_publish_seq,
        "CheckpointBoundary publish_seq=0 must not seed the publish-sequence floor"
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn checkpoint_crash_before_boundary_does_not_advance_header() {
    if run_checkpoint_crash_child_if_requested() {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_checkpoint_crash_cut.mqlite");
    let status = Command::new(env::current_exe().expect("current test binary"))
        .arg("checkpoint_crash_before_boundary_does_not_advance_header")
        .arg("--exact")
        .env(CHECKPOINT_CRASH_CHILD_ENV, "1")
        .env(CHECKPOINT_CRASH_DB_ENV, &path)
        .status()
        .expect("spawn checkpoint crash-cut child");
    assert!(
        !status.success(),
        "checkpoint crash-cut child must abort at the armed failpoint"
    );

    let crashed_header = read_header_state(&path);
    assert_eq!(
        crashed_header.checkpoint_applied_lsn, 0,
        "checkpoint_applied_lsn must not advance before CheckpointBoundary is written"
    );

    let reopened = open_fullsync(&path);
    let recovered = reopened.database("phase8").collection::<Document>("docs");
    assert_eq!(
        recovered
            .count_documents(doc! {})
            .expect("count recovered crash-cut doc"),
        1,
        "recovery must replay the pre-checkpoint commit after the crash cut"
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn reopen_after_checkpoint_seeds_hlc_without_tail_commits() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_checkpoint_hlc.mqlite");
    let client = open_fullsync(&path);
    let collection = client.database("phase8").collection::<Document>("docs");
    collection
        .insert_one(&doc! { "_id": 1i32, "phase": "checkpointed" })
        .expect("insert before checkpoint");
    client
        .checkpoint()
        .expect("checkpoint without later commits");
    let checkpoint_header = read_header_state(&path);
    assert_ne!(checkpoint_header.last_checkpoint_ts, (0, 0));
    drop(collection);
    std::mem::forget(client);

    let reopened = open_fullsync(&path);
    assert_eq!(
        reopened.__recovered_max_commit_ts(),
        Some(checkpoint_header.last_checkpoint_ts),
        "clean checkpoint reopen must seed HLC from last_checkpoint_ts"
    );
    let before_insert = journal_log_records(&reopened).len();
    reopened
        .database("phase8")
        .collection::<Document>("docs")
        .insert_one(&doc! { "_id": 2i32, "phase": "after-hlc-reopen" })
        .expect("insert after HLC recovery");
    let records = journal_log_records(&reopened);
    let new_commit_ts = records[before_insert..]
        .iter()
        .find(|record| record.kind == JournalLogRecordKind::CrudCommit)
        .expect("post-reopen insert writes CrudCommit")
        .commit_ts;
    assert!(
        ts_gt(new_commit_ts, checkpoint_header.last_checkpoint_ts),
        "post-reopen commit_ts must be above last_checkpoint_ts"
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn post_reservation_dirty_stamp_failure_poisons_gap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_post_reservation_failure.mqlite");
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open phase8 post-reservation database");
    client
        .database("phase8")
        .create_collection("docs")
        .expect("create docs collection");
    let collection = client.database("phase8").collection::<Document>("docs");
    let before = client
        .__journal_lsn_snapshot()
        .expect("snapshot before post-reservation failure");

    client.__fail_next_dirty_lsn_stamp();
    let err = collection
        .insert_one(&doc! { "_id": 9i32, "mode": "post-reservation" })
        .expect_err("post-reservation failure poisons the engine");
    assert!(matches!(
        err,
        Error::EngineFatal {
            reason: EngineFatalReason::PostReservationLogWriteFailure,
        }
    ));
    assert_eq!(
        client.__us036_poisoned_reason(),
        Some(EngineFatalReason::PostReservationLogWriteFailure)
    );

    let after = client
        .__journal_lsn_snapshot()
        .expect("snapshot after post-reservation failure");
    assert!(
        after.0 > before.0,
        "failure must happen after reserving a byte-LSN range"
    );
    assert_eq!(
        after.1, before.1,
        "ready_lsn must not advance across the poisoned reserved range"
    );
    assert_eq!(
        after.2, before.2,
        "durable_lsn must not advance across the poisoned reserved range"
    );
    assert!(matches!(
        collection.find_one(doc! { "_id": 9i32 }),
        Err(Error::EngineFatal {
            reason: EngineFatalReason::PostReservationLogWriteFailure,
        })
    ));
    assert!(matches!(
        collection.insert_one(&doc! { "_id": 10i32 }),
        Err(Error::EngineFatal {
            reason: EngineFatalReason::PostReservationLogWriteFailure,
        })
    ));

    drop(collection);
    drop(client);

    let reopened = Client::open(&path).expect("reopen after post-reservation poison");
    let recovered = reopened.database("phase8").collection::<Document>("docs");
    assert!(
        recovered
            .find_one(doc! { "_id": 9i32 })
            .expect("read after post-reservation recovery")
            .is_none(),
        "unwritten reserved slot must not recover as a committed record"
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn crash_cut_after_dirty_install_before_write_ignores_reserved_gap() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_after_dirty_before_write.mqlite");
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open phase8 after-dirty-before-write database");
    client
        .database("phase8")
        .create_collection("docs")
        .expect("create docs collection");
    let collection = client.database("phase8").collection::<Document>("docs");
    let before = client
        .__journal_lsn_snapshot()
        .expect("snapshot before after-dirty-before-write failure");

    client.__fail_next_after_dirty_lsn_stamp();
    let err = collection
        .insert_one(&doc! { "_id": 11i32, "mode": "after-dirty-before-write" })
        .expect_err("after-dirty-before-write cut poisons the engine");
    assert!(matches!(
        err,
        Error::EngineFatal {
            reason: EngineFatalReason::PostReservationLogWriteFailure,
        }
    ));

    let after = client
        .__journal_lsn_snapshot()
        .expect("snapshot after after-dirty-before-write failure");
    assert!(
        after.0 > before.0,
        "reserved LSN range must be visible at this crash cut"
    );
    assert_eq!(
        after.1, before.1,
        "ready_lsn must not skip an unwritten reserved range"
    );
    assert_eq!(
        after.2, before.2,
        "durable_lsn must not advance across an unwritten reserved range"
    );

    drop(collection);
    drop(client);
    let reopened = Client::open(&path).expect("reopen after dirty-before-write poison");
    assert_doc_value(&reopened, 11, None);
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn crash_cut_after_sync_before_pending_flip_recovers_durable_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_after_sync_before_flip.mqlite");
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open phase8 after-sync-before-flip database");
    client
        .database("phase8")
        .create_collection("docs")
        .expect("create docs collection");
    let collection = client.database("phase8").collection::<Document>("docs");

    client.__fail_next_after_durable_before_flip();
    let err = collection
        .insert_one(&doc! { "_id": 12i32, "mode": "after-sync-before-flip" })
        .expect_err("after-sync-before-flip cut poisons the engine");
    assert!(matches!(
        err,
        Error::EngineFatal {
            reason: EngineFatalReason::PostDurablePendingFlipFailure,
        }
    ));

    let (_, ready_lsn, durable_lsn) = client
        .__journal_lsn_snapshot()
        .expect("snapshot after after-sync-before-flip cut");
    assert_eq!(
        ready_lsn, durable_lsn,
        "FullSync cut after sync must leave the written record durable"
    );
    drop(collection);
    drop(client);

    let reopened = Client::open(&path).expect("reopen after sync-before-flip poison");
    assert_doc_value(&reopened, 12, Some("after-sync-before-flip"));
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn crash_cut_after_pending_flip_before_publish_recovers_durable_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_after_flip_before_publish.mqlite");
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open phase8 after-flip-before-publish database");
    client
        .database("phase8")
        .create_collection("docs")
        .expect("create docs collection");
    let collection = client.database("phase8").collection::<Document>("docs");

    client.__us009_fail_after_committed_flip_once();
    let err = collection
        .insert_one(&doc! { "_id": 13i32, "mode": "after-flip-before-publish" })
        .expect_err("after-flip-before-publish cut poisons the engine");
    assert!(matches!(
        err,
        Error::EngineFatal {
            reason: EngineFatalReason::PostDurablePendingFlipFailure,
        }
    ));
    drop(collection);
    drop(client);

    let reopened = Client::open(&path).expect("reopen after flip-before-publish poison");
    assert_doc_value(&reopened, 13, Some("after-flip-before-publish"));
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn crash_cut_mid_record_ignores_torn_tail_and_keeps_valid_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_mid_record_tail.mqlite");
    let client = open_fullsync(&path);
    let collection = client.database("phase8").collection::<Document>("docs");
    collection
        .insert_one(&doc! { "_id": 14i32, "mode": "valid-prefix" })
        .expect("insert valid prefix record");
    collection
        .insert_one(&doc! { "_id": 15i32, "mode": "torn-tail" })
        .expect("insert tail record to tear");

    let crud = crud_log_records(&client);
    let tail = crud.last().expect("tail CRUD record exists");
    let cut_lsn = tail.start_lsn + ((tail.end_lsn - tail.start_lsn) / 2);
    fs::OpenOptions::new()
        .write(true)
        .open(journal_path(&path))
        .expect("open journal for mid-record truncation")
        .set_len(cut_lsn)
        .expect("truncate journal in the middle of tail record");
    drop(collection);
    std::mem::forget(client);

    let reopened = open_fullsync(&path);
    assert_doc_value(&reopened, 14, Some("valid-prefix"));
    assert_doc_value(&reopened, 15, None);
    let recovered_records = crud_log_records(&reopened);
    assert!(
        recovered_records
            .iter()
            .all(|record| record.end_lsn <= cut_lsn),
        "recovery must truncate the torn tail to the valid committed prefix"
    );
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn crash_cut_after_write_before_sync_uses_valid_prefix_if_tail_lost() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_after_write_before_sync.mqlite");
    let client = open_fullsync(&path);
    let collection = client.database("phase8").collection::<Document>("docs");
    collection
        .insert_one(&doc! { "_id": 16i32, "mode": "valid-prefix" })
        .expect("insert valid prefix record");
    collection
        .insert_one(&doc! { "_id": 17i32, "mode": "lost-unsynced-tail" })
        .expect("insert tail record to remove");

    let crud = crud_log_records(&client);
    let tail = crud.last().expect("tail CRUD record exists");
    fs::OpenOptions::new()
        .write(true)
        .open(journal_path(&path))
        .expect("open journal for tail-loss truncation")
        .set_len(tail.start_lsn)
        .expect("truncate journal before unsynced tail record");
    drop(collection);
    std::mem::forget(client);

    let reopened = open_fullsync(&path);
    assert_doc_value(&reopened, 16, Some("valid-prefix"));
    assert_doc_value(&reopened, 17, None);
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn crash_cut_post_publish_covers_in_process_and_close_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_post_publish.mqlite");
    let client = open_fullsync(&path);
    let collection = client.database("phase8").collection::<Document>("docs");

    collection
        .insert_one(&doc! { "_id": 18i32, "mode": "post-publish" })
        .expect("post-publish insert succeeds");
    assert_doc_value(&client, 18, Some("post-publish"));
    drop(collection);
    client
        .close()
        .expect("close checkpoints post-publish commit");

    let reopened = open_fullsync(&path);
    assert_doc_value(&reopened, 18, Some("post-publish"));
}

#[test]
#[serial(phase8_journal_group_commit_hooks)]
fn earlier_publish_slot_stalls_before_reservation_later_lsn_writes_first() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("phase8_out_of_order_lsn.mqlite");
    let client = Client::open_with_options(
        &path,
        OpenOptions::new().durability(DurabilityMode::FullSync),
    )
    .expect("open phase8 out-of-order LSN database");
    client
        .database("phase8")
        .create_collection("docs")
        .expect("create docs collection");

    let mut early_hook = client.__install_before_log_reservation_hook();
    let early_client = client.clone();
    let early = thread::spawn(move || {
        early_client
            .database("phase8")
            .collection::<Document>("docs")
            .insert_one(&doc! { "_id": 19i32, "mode": "early-publish-seq" })
    });
    early_hook
        .wait_until_entered()
        .expect("early writer reached before-reservation hook");

    let late_done = Arc::new(AtomicBool::new(false));
    let late_client = client.clone();
    let late_done_writer = Arc::clone(&late_done);
    let late = thread::spawn(move || {
        let result = late_client
            .database("phase8")
            .collection::<Document>("docs")
            .insert_one(&doc! { "_id": 20i32, "mode": "later-lsn-first" });
        late_done_writer.store(true, Ordering::Release);
        result
    });

    let deadline = Instant::now() + JOURNAL_GROUP_COMMIT_SETTLE_DEADLINE;
    let mut observed_late_record = None;
    while Instant::now() < deadline {
        let crud = crud_log_records(&client);
        if let Some(record) = crud.last().cloned() {
            observed_late_record = Some(record);
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let late_record = observed_late_record.expect("late writer must write first LSN record");
    assert!(
        !late_done.load(Ordering::Acquire),
        "late writer must wait for the earlier publish slot before returning"
    );

    early_hook.release().expect("release early writer");
    early
        .join()
        .expect("early writer thread must not panic")
        .expect("early writer eventually commits");
    late.join()
        .expect("late writer thread must not panic")
        .expect("late writer eventually commits");

    let crud = crud_log_records(&client);
    let early_record = crud
        .iter()
        .find(|record| record.start_lsn > late_record.start_lsn)
        .expect("early writer reserves a later LSN after release");
    assert!(
        early_record.publish_seq < late_record.publish_seq,
        "persisted publish_seq order must differ from LSN reservation order"
    );
    std::mem::forget(client);

    let reopened = open_fullsync(&path);
    assert_doc_value(&reopened, 19, Some("early-publish-seq"));
    assert_doc_value(&reopened, 20, Some("later-lsn-first"));
}
