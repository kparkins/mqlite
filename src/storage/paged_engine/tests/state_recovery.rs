//! Phase 2 US-015 — unit tests for Pass 2 post-open validation.
//!
//! Lives in a separate file (see `state.rs` tail `#[path]` wiring) so
//! the intrusive test scaffolding does not co-mingle with the
//! production `SharedState::new` path. Exercises
//! `validate_parsed_logical_frames_against_catalog` against a minimal
//! in-memory `Catalog<MemPageStore>` built directly from catalog
//! constructors — no engine, no buffer pool, no journal replay.

use bson::doc;

use super::{validate_frame_ordinals_dense, validate_parsed_logical_frames_against_catalog};
use crate::index::IndexModel;
use crate::journal::log_file::{
    LogicalOp, LogicalOpKind, LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION,
};
use crate::journal::ParsedLogicalFrames;
use crate::mvcc::metrics::{
    logical_txn_pass2_resolved_ops_snapshot, logical_txn_pass2_unresolved_ops_snapshot,
    reset_logical_txn_pass2_resolved_ops, reset_logical_txn_pass2_unresolved_ops,
};
use crate::mvcc::timestamp::Ts;
use crate::storage::btree::MemPageStore;
use crate::storage::catalog::Catalog;
use std::path::Path;

const LIVE_NS_ID: i64 = 1;
const ABSENT_NS_ID: i64 = 999;
const RESOLVED_COMMIT_TS: Ts = Ts {
    physical_ms: 1_000,
    logical: 0,
};
const UNRESOLVED_COMMIT_TS: Ts = Ts {
    physical_ms: 2_000,
    logical: 0,
};
const MIN_SYNTHETIC_COMMIT_TS_OFFSET_MS: u64 = 1;

/// Build a fresh in-memory catalog with one collection (id=1) and one
/// index (id=10) attached to that collection.
fn catalog_with_ns1_and_idx10() -> Catalog<MemPageStore> {
    let mut cat = Catalog::create(MemPageStore::new()).expect("create catalog");
    cat.create_collection("c1", 1, bson::Document::new(), 0)
        .expect("create collection ns=1");
    let model = IndexModel::builder().keys(doc! { "x": 1i32 }).build();
    cat.create_index("c1", 10, &model, "by_x")
        .expect("create index id=10");
    cat
}

fn frame_primary_insert(commit_ts: Ts, ns_id: i64) -> LogicalTxnFrame {
    LogicalTxnFrame {
        salt1: 0,
        salt2: 0,
        commit_ts,
        diagnostic_txn_id: 1,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![LogicalOp {
            op_ordinal: 0,
            kind: LogicalOpKind::PrimaryInsert {
                ns_id,
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                overflow: None,
            },
        }],
    }
}

fn frame_sec_insert(commit_ts: Ts, index_id: i64) -> LogicalTxnFrame {
    LogicalTxnFrame {
        salt1: 0,
        salt2: 0,
        commit_ts,
        diagnostic_txn_id: 2,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![LogicalOp {
            op_ordinal: 0,
            kind: LogicalOpKind::SecondaryInsert {
                index_id,
                key: b"k".to_vec(),
                id_bytes: b"pkv".to_vec(),
            },
        }],
    }
}

fn setup_checkpointed_collection(db_path: &Path) {
    use crate::client::Client;
    use crate::options::OpenOptions as DbOpts;

    let client = Client::open_with_options(db_path, DbOpts::new()).expect("open setup client");
    client
        .database("d")
        .create_collection("c")
        .expect("create live collection");
    client.close().expect("checkpoint setup catalog");
}

fn synthetic_uncheckpointed_ts(header: &crate::storage::header::FileHeader, requested: Ts) -> Ts {
    if requested > header.last_checkpoint_ts {
        return requested;
    }
    Ts {
        physical_ms: header
            .last_checkpoint_ts
            .physical_ms
            .saturating_add(requested.physical_ms.max(MIN_SYNTHETIC_COMMIT_TS_OFFSET_MS)),
        logical: requested.logical,
    }
}

fn append_durable_logical_insert(db_path: &Path, ns_id: i64, commit_ts: Ts) {
    use crate::journal::log_file::{ChainCommitFrame, LogRecordDraft};
    use crate::journal::JournalManager;
    use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom};

    let mut main_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(db_path)
        .expect("open main file");
    let header = {
        let mut buf = [0u8; HEADER_PAGE_SIZE];
        main_file.seek(SeekFrom::Start(0)).expect("seek header");
        main_file.read_exact(&mut buf).expect("read header");
        FileHeader::from_bytes(&buf).expect("decode header")
    };
    let mgr = JournalManager::open_or_create(db_path, &header, &mut main_file)
        .expect("open journal manager");
    let (salt1, salt2) = mgr.salts();
    let commit_ts = synthetic_uncheckpointed_ts(&header, commit_ts);
    let publish_seq = mgr.recovered_max_publish_seq().unwrap_or(0) + 1;
    let mut frame = frame_primary_insert(commit_ts, ns_id);
    frame.salt1 = salt1;
    frame.salt2 = salt2;
    let logical = frame.encode().expect("encode logical");
    let chain = ChainCommitFrame {
        salt1,
        salt2,
        commit_ts,
        refcount_deltas: vec![],
        page_writes: vec![],
    }
    .encode()
    .expect("encode chain");
    let record = mgr
        .reserve_log_record(LogRecordDraft::crud(
            ns_id as u64,
            publish_seq,
            commit_ts,
            logical,
            chain,
        ))
        .expect("reserve phase8 crud");
    record.write_and_mark().expect("write phase8 crud");
    mgr.sync_journal().expect("sync journal");
}

/// Pass 2 increments the resolved counter once per op that matches a
/// live catalog entry. Primary ns_id=1 present → resolved; secondary
/// index_id=10 present → resolved.
#[test]
#[serial_test::serial(logical_txn_pass2_metrics)]
fn pass2_ticks_resolved_counter_on_matching_ids() {
    reset_logical_txn_pass2_resolved_ops();
    reset_logical_txn_pass2_unresolved_ops();
    let base = logical_txn_pass2_resolved_ops_snapshot();

    let cat = catalog_with_ns1_and_idx10();
    let parsed = ParsedLogicalFrames {
        frames: vec![
            (
                100,
                frame_primary_insert(
                    Ts {
                        physical_ms: 1,
                        logical: 0,
                    },
                    1,
                ),
            ),
            (
                200,
                frame_sec_insert(
                    Ts {
                        physical_ms: 2,
                        logical: 0,
                    },
                    10,
                ),
            ),
        ],
        ..Default::default()
    };

    validate_parsed_logical_frames_against_catalog(&cat, &parsed).expect("pass2 ok");

    assert_eq!(
        logical_txn_pass2_resolved_ops_snapshot(),
        base + 2,
        "two ops, both resolvable → resolved counter should tick by 2"
    );
    assert_eq!(
        logical_txn_pass2_unresolved_ops_snapshot(),
        0,
        "no unresolved ops expected"
    );
}

/// Phase 2 tolerance: unresolved ns_id / index_id must NOT fail open.
/// The unresolved counter ticks, Pass 2 returns Ok(()), and the caller
/// (SharedState::new) can proceed with publishing the initial epoch.
#[test]
#[serial_test::serial(logical_txn_pass2_metrics)]
fn pass2_logs_unresolved_ids_without_failing_open() {
    reset_logical_txn_pass2_resolved_ops();
    reset_logical_txn_pass2_unresolved_ops();

    let cat = catalog_with_ns1_and_idx10();
    let parsed = ParsedLogicalFrames {
        frames: vec![
            // ns_id=99 absent → unresolved.
            (
                100,
                frame_primary_insert(
                    Ts {
                        physical_ms: 1,
                        logical: 0,
                    },
                    99,
                ),
            ),
            // index_id=77 absent → unresolved.
            (
                200,
                frame_sec_insert(
                    Ts {
                        physical_ms: 2,
                        logical: 0,
                    },
                    77,
                ),
            ),
        ],
        ..Default::default()
    };

    let res = validate_parsed_logical_frames_against_catalog(&cat, &parsed);
    assert!(
        res.is_ok(),
        "Pass 2 must log-and-proceed on unresolved ids in Phase 2, got {res:?}"
    );

    assert_eq!(
        logical_txn_pass2_unresolved_ops_snapshot(),
        2,
        "two ops, both unresolvable → unresolved counter should tick by 2"
    );
    assert_eq!(
        logical_txn_pass2_resolved_ops_snapshot(),
        0,
        "no resolved ops expected"
    );
}

/// Duplicate op_ordinal in a frame is a Phase 2 invariant violation —
/// Pass 1 should already have rejected it via the decoder, but the
/// defensive re-check in `validate_frame_ordinals_dense` must surface
/// the corruption as `Err`.
#[test]
fn pass2_rejects_duplicate_op_ordinal() {
    let frame = LogicalTxnFrame {
        salt1: 0,
        salt2: 0,
        commit_ts: Ts {
            physical_ms: 5,
            logical: 0,
        },
        diagnostic_txn_id: 3,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![
            LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::PrimaryDelete {
                    ns_id: 1,
                    key: b"a".to_vec(),
                },
            },
            LogicalOp {
                op_ordinal: 0, // duplicate
                kind: LogicalOpKind::PrimaryDelete {
                    ns_id: 1,
                    key: b"b".to_vec(),
                },
            },
        ],
    };
    let err = validate_frame_ordinals_dense(&frame).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("duplicate op_ordinal"),
        "expected duplicate-ordinal error, got: {msg}"
    );
}

/// Out-of-range op_ordinal (>= ops.len()) is also a corruption error.
#[test]
fn pass2_rejects_out_of_range_op_ordinal() {
    let frame = LogicalTxnFrame {
        salt1: 0,
        salt2: 0,
        commit_ts: Ts {
            physical_ms: 6,
            logical: 0,
        },
        diagnostic_txn_id: 4,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![LogicalOp {
            op_ordinal: 5, // out of range for a 1-op frame
            kind: LogicalOpKind::PrimaryDelete {
                ns_id: 1,
                key: b"a".to_vec(),
            },
        }],
    };
    let err = validate_frame_ordinals_dense(&frame).unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("out of range"),
        "expected out-of-range error, got: {msg}"
    );
}

/// US-015 AC#5 integration: drive the full engine open path
/// (`Client::open` → `PagedEngine::open` → `MetadataState::new` → Pass 2)
/// and assert Pass 2 resolves the live frame's ns_id and increments the
/// resolved counter. The test checkpoints a real catalog, appends a durable
/// uncheckpointed LogicalTxnFrame + ChainCommit pair, then reopens through
/// Client::open. That triggers recovery, catalog rebuild, and Pass 2 —
/// proving Pass 2 runs inside `SharedState::new` (not just inside the
/// unit-tested helper).
///
/// The unresolved-id Phase 4 promotion path is covered by
/// `pass2_logs_unresolved_ids_without_failing_open` above (synthetic
/// `ParsedLogicalFrames` against an empty catalog) plus the
/// `test_phase4_case_c_is_hard_error` placeholder. Phase 2's Phase 4
/// gate (`#[ignore = "Phase 4 exit criterion §8.13.3"]`) covers the
/// promotion site for both case (c) ChainCommit-without-logical and
/// Pass-2 unresolved-id; both promote together when Phase 4 lands.
#[test]
#[serial_test::serial(logical_txn_pass2_metrics)]
fn pass2_through_engine_resolves_real_ns_id_after_reopen() {
    use crate::client::Client;
    use crate::options::OpenOptions as DbOpts;

    reset_logical_txn_pass2_resolved_ops();
    reset_logical_txn_pass2_unresolved_ops();

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("p2_engine.mqlite");

    // Step 1: checkpoint a real catalog, then append an uncheckpointed
    // LogicalTxnFrame + ChainCommit pair to the journal. This models the
    // post-crash Pass 1 hand-off deterministically without relying on a
    // process-leaking test writer.
    setup_checkpointed_collection(&db_path);
    append_durable_logical_insert(&db_path, LIVE_NS_ID, RESOLVED_COMMIT_TS);

    // Step 2: reopen — Pass 2 runs inside `SharedState::new`. The logical
    // frame's ns_id (=1, the first allocated namespace id) MUST resolve
    // against the recovered catalog and increment the resolved counter.
    let resolved_before = logical_txn_pass2_resolved_ops_snapshot();
    let unresolved_before = logical_txn_pass2_unresolved_ops_snapshot();

    let client2 = Client::open_with_options(&db_path, DbOpts::default())
        .expect("open client #2 (recovery + Pass 2)");

    let resolved_after = logical_txn_pass2_resolved_ops_snapshot();
    let unresolved_after = logical_txn_pass2_unresolved_ops_snapshot();

    assert!(
        resolved_after > resolved_before,
        "Pass 2 should resolve ≥1 op for the recovered logical frame; \
         resolved before={resolved_before}, after={resolved_after}"
    );
    assert_eq!(
        unresolved_after, unresolved_before,
        "Resolved-only path: unresolved counter must not tick"
    );

    drop(client2);
    drop(dir);
}

/// US-015 AC#5 integration (unresolved branch through `SharedState::new`).
///
/// This is the variant codex round 3 approved as the correct AC#5 proof:
/// inject a durable logical frame that references an `ns_id` absent from
/// the reopened catalog via the pub(crate) `JournalManager::append_*`
/// API, reopen through `Client::open_with_options`, and assert the
/// engine opens cleanly AND `logical_txn_pass2_unresolved_ops_total`
/// increments.
#[test]
#[serial_test::serial(logical_txn_pass2_metrics)]
fn pass2_through_engine_ticks_unresolved_on_recovered_absent_ns_id() {
    use crate::client::Client;
    use crate::options::OpenOptions as DbOpts;

    reset_logical_txn_pass2_resolved_ops();
    reset_logical_txn_pass2_unresolved_ops();

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("p2_unresolved.mqlite");

    // Step 1: establish a live catalog, then append one resolvable and one
    // unresolvable logical commit. Both frames are durable and uncheckpointed,
    // so Pass 1 must hand them to Pass 2 on the next open.
    setup_checkpointed_collection(&db_path);
    append_durable_logical_insert(&db_path, LIVE_NS_ID, RESOLVED_COMMIT_TS);
    append_durable_logical_insert(&db_path, ABSENT_NS_ID, UNRESOLVED_COMMIT_TS);

    // Step 2: reopen through the full Client path. Recovery sees the
    // original resolved frame (ns_id=1) AND the injected unresolved
    // frame (ns_id=999). Pass 2 runs inside `SharedState::new` and must
    // NOT fail open.
    let resolved_before = logical_txn_pass2_resolved_ops_snapshot();
    let unresolved_before = logical_txn_pass2_unresolved_ops_snapshot();

    let client2 = Client::open_with_options(&db_path, DbOpts::default())
        .expect("open client #2 must succeed despite unresolved ns_id");

    let resolved_after = logical_txn_pass2_resolved_ops_snapshot();
    let unresolved_after = logical_txn_pass2_unresolved_ops_snapshot();

    assert!(
        unresolved_after > unresolved_before,
        "Pass 2 MUST tick logical_txn_pass2_unresolved_ops_total for \
         the injected ns_id=999 frame; before={unresolved_before}, after={unresolved_after}"
    );
    assert!(
        resolved_after > resolved_before,
        "Pass 2 should also resolve the original ns_id=1 frame; \
         resolved before={resolved_before}, after={resolved_after}"
    );

    drop(client2);
    drop(dir);
}
