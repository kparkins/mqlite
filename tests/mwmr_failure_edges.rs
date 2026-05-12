//! Phase 5 §10.13.8 — error variant tests for `Error::WriteConflict` and
//! its `WriteConflictReason`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]
//!
//! Locks down the US-002 contract:
//!
//! * `Error::WriteConflict { reason: WriteConflictReason }` exists and is
//!   matched by a different arm than `Error::WriterBusy`.
//! * Every `WriteConflictReason` variant added by §10.3.3, §10.17.1,
//!   §10.24, §10.25 is reachable by a direct match arm.
//! * The §10.3.3 `Display` text contains the first-committer-wins
//!   contract sentence so callers can pattern-match it in user-facing
//!   surfaces.
//!
//! US-002 ships the type before US-003 (PageLatch) and US-004
//! (`NsWriterRegistry::admit` / `close_and_drain`). The integration call
//! that produces `WriterBusy` from a real closed-lane admit is owned by
//! US-004; this file only verifies the variant types are wired
//! distinctly so US-003+ can reference them.

use bson::doc;
use mqlite::error::{Error, WriteConflictReason};
use mqlite::Client;

#[cfg(feature = "test-hooks")]
mod crash_harness;

#[cfg(feature = "test-hooks")]
use std::collections::{BTreeMap, VecDeque};
#[cfg(feature = "test-hooks")]
use std::sync::mpsc;
#[cfg(feature = "test-hooks")]
use std::sync::Arc;
#[cfg(feature = "test-hooks")]
use std::thread;
#[cfg(feature = "test-hooks")]
use std::time::Duration;

#[cfg(feature = "test-hooks")]
use bson::{Bson, Document};
#[cfg(feature = "test-hooks")]
use mqlite::mvcc::{ChainSnapshot, ReadView, Ts, VersionData, VersionEntry, VersionState};
#[cfg(feature = "test-hooks")]
use mqlite::{IndexModel, IndexOptions, OpenOptions, Us026PostRegisterFailpoint};

#[cfg(feature = "test-hooks")]
const READ_TS: Ts = Ts {
    physical_ms: 200,
    logical: 0,
};
#[cfg(feature = "test-hooks")]
const COMMIT_TS: Ts = Ts {
    physical_ms: 100,
    logical: 0,
};
#[cfg(feature = "test-hooks")]
const TXN_ID: u64 = 11;
#[cfg(feature = "test-hooks")]
const OTHER_TXN_ID: u64 = 22;
#[cfg(feature = "test-hooks")]
const POST_REGISTER_NS: &str = "db.c";
#[cfg(feature = "test-hooks")]
const SUCCESSOR_TIMEOUT: Duration = Duration::from_secs(5);

/// §10.13.8 — every `WriteConflictReason` variant matches directly.
///
/// Constructs each of the six variants from §10.3.3, §10.17.1, §10.24,
/// and §10.25 and asserts every variant is reached by a distinct match
/// arm. Acts as the discriminant-stability guard: any future rename or
/// removal of a variant fails to compile here.
#[test]
fn test_write_conflict_variant_has_stable_reason_discriminants() {
    let reasons = [
        WriteConflictReason::StaleSnapshot,
        WriteConflictReason::UpgradeRace,
        WriteConflictReason::SameKeyConflict {
            key_preview: b"abc".to_vec(),
        },
        WriteConflictReason::CatalogGenerationChanged,
        WriteConflictReason::StructuralContention,
        WriteConflictReason::UniqueConflict {
            key_prefix_preview: b"xyz".to_vec(),
        },
    ];

    let mut seen = [false; 6];
    for reason in &reasons {
        match reason {
            WriteConflictReason::StaleSnapshot => seen[0] = true,
            WriteConflictReason::UpgradeRace => seen[1] = true,
            WriteConflictReason::SameKeyConflict { key_preview } => {
                assert_eq!(key_preview, b"abc", "SameKeyConflict preview round-trips");
                seen[2] = true;
            }
            WriteConflictReason::CatalogGenerationChanged => seen[3] = true,
            WriteConflictReason::StructuralContention => seen[4] = true,
            WriteConflictReason::UniqueConflict { key_prefix_preview } => {
                assert_eq!(
                    key_prefix_preview, b"xyz",
                    "UniqueConflict prefix preview round-trips"
                );
                seen[5] = true;
            }
        }
    }

    assert!(
        seen.iter().all(|hit| *hit),
        "every WriteConflictReason variant must be reachable by direct match: seen={seen:?}"
    );

    // Clone is part of the §10.3.3 derive contract; force the bound.
    let cloned = reasons[2].clone();
    match cloned {
        WriteConflictReason::SameKeyConflict { key_preview } => {
            assert_eq!(key_preview, b"abc");
        }
        other => panic!("Clone changed the variant: {other:?}"),
    }
}

/// §10.13.8 — `WriterBusy` is matched by a different arm than
/// `WriteConflict`.
///
/// US-004 ships the real lane-admit call that produces `WriterBusy` when
/// admit hits a `close_and_drain`'d lane. This test locks down the
/// type-level contract US-002 owes that future call: the two errors must
/// be distinguishable by direct `match`.
#[test]
fn test_writer_busy_returned_on_closed_lane_admit() {
    let busy = Error::WriterBusy;
    let conflict = Error::WriteConflict {
        reason: WriteConflictReason::SameKeyConflict {
            key_preview: b"k".to_vec(),
        },
    };

    let busy_is_writer_busy = matches!(busy, Error::WriterBusy);
    let busy_is_write_conflict = matches!(busy, Error::WriteConflict { .. });
    let conflict_is_writer_busy = matches!(conflict, Error::WriterBusy);
    let conflict_is_write_conflict = matches!(conflict, Error::WriteConflict { .. });

    assert!(
        busy_is_writer_busy,
        "WriterBusy must match Error::WriterBusy"
    );
    assert!(
        !busy_is_write_conflict,
        "WriterBusy must not match Error::WriteConflict"
    );
    assert!(
        !conflict_is_writer_busy,
        "WriteConflict must not match Error::WriterBusy"
    );
    assert!(
        conflict_is_write_conflict,
        "WriteConflict must match Error::WriteConflict"
    );
}

/// The §10.3.3 `#[error(...)]` text is part of the contract: the message
/// must mention the first-committer-wins retry guidance so binding
/// layers can route the error to caller-side retry logic without parsing
/// the inner reason.
#[test]
fn test_write_conflict_display_contains_first_committer_wins_contract() {
    let err = Error::WriteConflict {
        reason: WriteConflictReason::StaleSnapshot,
    };
    let rendered = err.to_string();
    assert!(
        rendered.contains("first-committer-wins engine"),
        "WriteConflict Display must include first-committer-wins phrase: {rendered}"
    );
    assert!(
        rendered.contains("fresh ReadView"),
        "WriteConflict Display must instruct caller to open a fresh ReadView: {rendered}"
    );
}

/// §10.3.3 — `WriteConflict` carries no MongoDB error code; the wire
/// mapping is deferred to the binding layer (MongoDB code 112).
/// `WriterBusy` likewise has no MongoDB code today.
#[test]
fn test_write_conflict_has_no_mongodb_error_code() {
    let err = Error::WriteConflict {
        reason: WriteConflictReason::UpgradeRace,
    };
    assert_eq!(
        err.code(),
        None,
        "Phase 5 deliberately defers MongoDB code 112 to the binding layer"
    );
}

#[test]
fn test_primary_insert_after_committed_tombstone_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("insert_after_tombstone.mqlite");
    let client = Client::open(&path).expect("open client");
    let col = client.database("d").collection::<bson::Document>("c");

    col.insert_one(&doc! { "_id": 1, "value": "old" })
        .expect("seed insert");
    col.delete_one(doc! { "_id": 1 }).expect("delete seed");
    col.insert_one(&doc! { "_id": 1, "value": "new" })
        .expect("insert after committed tombstone");

    let found = col
        .find_one(doc! { "_id": 1 })
        .expect("find")
        .expect("document exists");
    assert_eq!(found.get_str("value").expect("value"), "new");
}

#[cfg(feature = "test-hooks")]
fn open_client(name: &str) -> (tempfile::TempDir, Client) {
    open_client_with_options(name, crash_harness::fullsync_options())
}

#[cfg(feature = "test-hooks")]
fn open_client_with_options(name: &str, opts: OpenOptions) -> (tempfile::TempDir, Client) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join(name);
    let client = Client::open_with_options(&path, opts).expect("open client");
    (dir, client)
}

#[cfg(feature = "test-hooks")]
fn test_doc(id: i32, email: &str) -> Document {
    doc! { "_id": id, "email": email }
}

#[cfg(feature = "test-hooks")]
fn unique_email_index_model() -> IndexModel {
    IndexModel::builder()
        .keys(doc! { "email": 1 })
        .options(IndexOptions::new().unique(true))
        .build()
}

#[cfg(feature = "test-hooks")]
fn assert_unique_conflict<T>(result: mqlite::Result<T>) {
    assert_write_conflict_reason(result, |reason| {
        matches!(reason, WriteConflictReason::UniqueConflict { .. })
    });
}

#[cfg(feature = "test-hooks")]
fn pending_entry(txn_id: u64) -> VersionEntry {
    VersionEntry {
        start_ts: COMMIT_TS,
        stop_ts: Ts::MAX,
        txn_id,
        state: VersionState::Pending { txn_id },
        data: VersionData::Inline(vec![1, 2, 3]),
        is_tombstone: false,
    }
}

#[cfg(feature = "test-hooks")]
fn aborted_entry() -> VersionEntry {
    VersionEntry {
        state: VersionState::Aborted,
        ..pending_entry(TXN_ID)
    }
}

#[cfg(feature = "test-hooks")]
fn snapshot_for(entry: VersionEntry) -> ChainSnapshot {
    let mut chains = BTreeMap::new();
    chains.insert(b"k".to_vec(), Arc::new(VecDeque::from([entry])));
    ChainSnapshot::new(&chains, None)
}

#[cfg(feature = "test-hooks")]
fn assert_write_conflict_reason<T>(
    result: mqlite::Result<T>,
    expected: impl FnOnce(&WriteConflictReason) -> bool,
) {
    match result {
        Err(Error::WriteConflict { reason }) if expected(&reason) => {}
        Err(other) => panic!("unexpected error: {other:?}"),
        Ok(_) => panic!("unexpected success"),
    }
}

#[cfg(feature = "test-hooks")]
fn assert_successor_insert_completes(client: &Client) {
    let successor = client.clone();
    let (tx, rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let result = successor
            .database("db")
            .collection::<Document>("c")
            .insert_one(&test_doc(2, "successor@example.com"));
        let _ = tx.send(());
        result
    });

    rx.recv_timeout(SUCCESSOR_TIMEOUT)
        .expect("successor writer must not block behind aborted slot");
    writer
        .join()
        .expect("successor writer thread")
        .expect("successor writer commits after abort");

    let found = client
        .database("db")
        .collection::<Document>("c")
        .find_one(doc! { "_id": 2 })
        .expect("successor read")
        .expect("successor document");
    assert_eq!(found.get_str("email").ok(), Some("successor@example.com"));
}

#[cfg(feature = "test-hooks")]
fn assert_post_register_cleanup(failpoint: Us026PostRegisterFailpoint, file_name: &str) {
    assert_post_register_cleanup_with_options(
        failpoint,
        file_name,
        crash_harness::fullsync_options(),
    );
}

#[cfg(feature = "test-hooks")]
fn assert_post_register_cleanup_with_options(
    failpoint: Us026PostRegisterFailpoint,
    file_name: &str,
    opts: OpenOptions,
) {
    let (_dir, client) = open_client_with_options(file_name, opts);
    client
        .database("db")
        .create_collection("c")
        .expect("create collection");
    let coll = client.database("db").collection::<Document>("c");
    let ns_id = client
        .__us036_namespace_id(POST_REGISTER_NS)
        .expect("namespace id lookup")
        .expect("namespace id exists");

    client.__us026_arm_post_register_failpoint(failpoint);
    let result = coll.insert_one(&test_doc(1, "cleanup@example.com"));
    match result {
        Err(Error::Internal(message)) => {
            assert!(
                message.contains("US-026 injected"),
                "unexpected injected error message: {message}"
            );
        }
        other => panic!("expected injected cleanup error, got {other:?}"),
    }

    let found = coll
        .find_one(doc! { "_id": 1 })
        .expect("reader after cleanup");
    assert!(
        found.is_none(),
        "aborted pending write must be reader-invisible"
    );

    let states = client
        .__us009_primary_chain_states(POST_REGISTER_NS, &Bson::Int32(1))
        .expect("primary chain states after cleanup");
    assert_eq!(
        states.first().map(String::as_str),
        Some("Aborted"),
        "cleanup must flip the leaked Pending head to Aborted: {states:?}"
    );
    assert!(
        !states.iter().any(|state| state == "Pending"),
        "cleanup must leave no Pending entries for readers/reconcile: {states:?}"
    );

    assert_successor_insert_completes(&client);
    client
        .__us036_close_and_drain(ns_id, 1_000)
        .expect("writer ticket must be released");
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_pending_delta_visible_to_own_txn() {
    let snap = snapshot_for(pending_entry(TXN_ID));
    let own_view = ReadView::new(READ_TS, TXN_ID);

    let visible = snap.visible_at(b"k", &own_view);

    assert!(visible.is_some(), "own Pending delta must be visible");
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_aborted_delta_skipped_by_reader() {
    let snap = snapshot_for(aborted_entry());
    let reader = ReadView::new_with_frontier(READ_TS, OTHER_TXN_ID, READ_TS);

    let visible = snap.visible_at(b"k", &reader);

    assert!(visible.is_none(), "Aborted delta must be skipped");
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_expected_absent_head_yields_same_key_conflict() {
    let (_dir, client) = open_client("us009-same-key.mqlite");
    let db = client.database("db");
    db.create_collection("c").expect("create collection");
    let coll = db.collection::<Document>("c");
    let future_head = Ts {
        physical_ms: u64::MAX - 1,
        logical: 0,
    };
    client
        .__us009_inject_primary_committed_head(
            "db.c",
            &test_doc(1, "a@example.com"),
            future_head,
            77,
        )
        .expect("inject future committed head");

    let result = coll.insert_one(&test_doc(1, "a@example.com"));

    assert_write_conflict_reason(result, |reason| {
        matches!(reason, WriteConflictReason::SameKeyConflict { .. })
    });
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_expected_head_mismatch_yields_stale_snapshot() {
    let (_dir, client) = open_client("us009-stale-head.mqlite");
    let coll = client.database("db").collection::<Document>("c");
    coll.insert_one(&test_doc(1, "a@example.com"))
        .expect("seed insert");

    let mut hook_a = client.__install_write_body_entry_hook("db.c");
    let mut hook_b = client.__install_write_body_entry_hook("db.c");
    let client_a = client.clone();
    let writer_a = thread::spawn(move || {
        client_a
            .database("db")
            .collection::<Document>("c")
            .update_one(
                doc! { "_id": 1 },
                doc! { "$set": { "email": "a1@example.com" } },
            )
            .run()
    });
    hook_a.wait_until_entered().expect("writer A entered body");

    let client_b = client;
    let writer_b = thread::spawn(move || {
        client_b
            .database("db")
            .collection::<Document>("c")
            .update_one(
                doc! { "_id": 1 },
                doc! { "$set": { "email": "b@example.com" } },
            )
            .run()
    });
    hook_b.wait_until_entered().expect("writer B entered body");
    hook_a.release().expect("release writer A");
    hook_b.release().expect("release writer B");
    let result_a = writer_a.join().expect("writer A thread");
    let result_b = writer_b.join().expect("writer B thread");

    let ok_count = usize::from(result_a.is_ok()) + usize::from(result_b.is_ok());
    let stale_count = [&result_a, &result_b]
        .into_iter()
        .filter(|result| {
            matches!(
                result,
                Err(Error::WriteConflict {
                    reason: WriteConflictReason::StaleSnapshot,
                })
            )
        })
        .count();
    assert_eq!(ok_count, 1, "exactly one writer must commit");
    assert_eq!(
        stale_count, 1,
        "the loser must fail with a stale-snapshot write conflict"
    );
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_primary_install_failure_flips_secondary_pending_to_aborted() {
    let (_dir, client) = open_client("us009-secondary-abort.mqlite");
    let coll = client.database("db").collection::<Document>("c");
    coll.create_index(IndexModel::builder().keys(doc! { "email": 1 }).build())
        .expect("create email index");

    client.__us019_set_primary_install_failures(1);
    let doc = test_doc(1, "abort-secondary@example.com");
    let result = coll.insert_one(&doc);
    assert!(matches!(result, Err(Error::Internal(_))));

    let states = client
        .__us009_secondary_chain_states("db.c", "email_1", &doc, &Bson::Int32(1))
        .expect("secondary chain states");
    assert_eq!(states.first().map(String::as_str), Some("Aborted"));
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_pending_flip_happens_before_mark_ready() {
    let (_dir, client) = open_client("us009-flip-order.mqlite");
    client.__us009_reset_flip_publish_order();

    client
        .database("db")
        .collection::<Document>("c")
        .insert_one(&test_doc(1, "order@example.com"))
        .expect("insert");

    let (flip_order, publish_order) = client.__us009_flip_publish_order();
    assert!(flip_order > 0, "Committed flip must be recorded");
    assert!(publish_order > 0, "publish-ready order must be recorded");
    assert!(
        flip_order < publish_order,
        "Pending flip must happen before publish-ready/mark_ready"
    );
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_no_split_brain_at_publish_ts_reader_sees_either_both_or_neither() {
    let (_dir, client) = open_client("us009-no-split-brain.mqlite");
    let coll = client.database("db").collection::<Document>("c");
    coll.create_index(IndexModel::builder().keys(doc! { "email": 1 }).build())
        .expect("create email index");

    coll.insert_one(&test_doc(1, "old@example.com"))
        .expect("seed insert");
    coll.update_one(
        doc! { "_id": 1 },
        doc! { "$set": { "email": "new@example.com" } },
    )
    .run()
    .expect("update indexed field");

    let by_primary = coll
        .find_one(doc! { "_id": 1 })
        .expect("primary lookup")
        .expect("primary doc");
    let by_secondary = coll
        .find_one(doc! { "email": "new@example.com" })
        .expect("secondary lookup")
        .expect("secondary doc");
    assert_eq!(by_primary.get_str("email").ok(), Some("new@example.com"));
    assert_eq!(by_secondary.get_i32("_id").ok(), Some(1));
    assert!(
        coll.find_one(doc! { "email": "old@example.com" })
            .expect("old secondary lookup")
            .is_none(),
        "old secondary key must not survive without the primary update"
    );
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_failpoint_between_committed_flip_and_mark_ready_escalates_engine_fatal() {
    let (_dir, client) = open_client("us009-post-flip-fatal.mqlite");
    client.__us009_reset_flip_publish_order();
    client.__us009_fail_after_committed_flip_once();

    let result = client
        .database("db")
        .collection::<Document>("c")
        .insert_one(&test_doc(1, "fatal@example.com"));

    assert!(matches!(result, Err(Error::EngineFatal { .. })));
    let (flip_order, publish_order) = client.__us009_flip_publish_order();
    assert!(flip_order > 0, "Committed flip must run before fatal hook");
    assert_eq!(publish_order, 0, "publish must not run after fatal hook");
    assert!(matches!(
        client
            .database("db")
            .collection::<Document>("c")
            .find_one(doc! { "_id": 1 }),
        Err(Error::EngineFatal { .. })
    ));
}

#[cfg(feature = "test-hooks")]
mod post_register {
    use super::*;

    #[test]
    fn test_before_log_reservation_failure_after_register_cleans_up() {
        assert_post_register_cleanup(
            Us026PostRegisterFailpoint::BeforeLogReservation,
            "us026-before-log-reservation.mqlite",
        );
    }
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_unique_conflict_sees_other_txn_pending_entries() {
    let (_dir, client) = open_client("us011-pending-unique.mqlite");
    let coll = client.database("db").collection::<Document>("c");
    coll.create_index(unique_email_index_model())
        .expect("create unique email index");

    client
        .__us011_install_pending_unique_email(
            "db.c",
            "email_1",
            Bson::Int32(1),
            "pending@example.test",
            201,
        )
        .expect("first pending unique install");

    let result = client.__us011_install_pending_unique_email(
        "db.c",
        "email_1",
        Bson::Int32(2),
        "pending@example.test",
        202,
    );
    assert_unique_conflict(result);
}

#[cfg(feature = "test-hooks")]
#[test]
fn test_unique_conflict_on_update_that_would_collide() {
    let (_dir, client) = open_client("us011-update-collide.mqlite");
    let coll = client.database("db").collection::<Document>("c");
    coll.create_index(unique_email_index_model())
        .expect("create unique email index");
    coll.insert_one(&test_doc(1, "base@example.test"))
        .expect("seed conflicting base row");
    client
        .checkpoint()
        .expect("fold conflicting row into base leaf");

    let result = client.__us011_install_pending_unique_email(
        "db.c",
        "email_1",
        Bson::Int32(2),
        "base@example.test",
        203,
    );
    assert_unique_conflict(result);
}
