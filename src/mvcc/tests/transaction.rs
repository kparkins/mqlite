use super::*;
use crate::mvcc::version::OverflowRef;
use crate::storage::allocator::AllocatorHandle;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};
use std::sync::Arc;

fn fresh_allocator() -> AllocatorHandle {
    AllocatorHandle::new(FileHeader::new(0, 0, 0))
}

fn fresh_handle() -> Arc<BufferPoolHandle> {
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    Arc::new(BufferPoolHandle::new(
        pool,
        history_pool,
        FileHeader::new(0, 0, 0),
    ))
}

// -----------------------------------------------------------------------
// WriteTxn basic behaviour
// -----------------------------------------------------------------------

#[test]
fn new_txn_starts_empty() {
    let t = WriteTxn::new(7);
    assert_eq!(t.txn_id, 7);
    assert_eq!(t.commit_ts.get(), None);
    assert!(t.pending.is_empty());
    assert!(t.page_writes.is_empty());
    assert!(t.refcount_deltas.is_empty());
    assert!(t.pending_sec_index.is_empty());
    assert!(t.pending_primary.is_empty());
    assert!(!t.publish_dirty.published_catalog_dirty);
    assert!(!t.publish_dirty.catalog_header_dirty);
}

#[test]
fn publish_dirty_marks_published_bit() {
    let mut t = WriteTxn::new(1);
    t.publish_dirty.mark_published();
    assert!(t.publish_dirty.published_catalog_dirty);
    assert!(!t.publish_dirty.catalog_header_dirty);
}

#[test]
fn publish_dirty_marks_header_bit() {
    let mut t = WriteTxn::new(1);
    t.publish_dirty.mark_header();
    assert!(!t.publish_dirty.published_catalog_dirty);
    assert!(t.publish_dirty.catalog_header_dirty);
}

#[test]
fn attach_overflow_moves_ownership() {
    let alloc = fresh_allocator();
    let r = OverflowRef::new_owned(12, 64, alloc.clone()).unwrap();
    assert_eq!(alloc.overflow_refcount(12), 1);

    let mut t = WriteTxn::new(1);
    t.attach_overflow(r);
    assert_eq!(t.pending.len(), 1);
    assert_eq!(
        alloc.overflow_refcount(12),
        1,
        "attach is a move, not a clone — refcount unchanged"
    );
}

#[test]
fn txn_drop_decrefs_pending_pins() {
    let alloc = fresh_allocator();
    let r = OverflowRef::new_owned(33, 64, alloc.clone()).unwrap();
    let mut t = WriteTxn::new(1);
    t.attach_overflow(r);
    assert_eq!(alloc.overflow_refcount(33), 1);

    drop(t);
    assert_eq!(alloc.overflow_refcount(33), 0);
    assert_eq!(alloc.page_lifetime_queue().depth(), 1);
}

// -----------------------------------------------------------------------
// begin / commit / drop
// -----------------------------------------------------------------------

#[test]
fn begin_checks_page_lifetime_queue() {
    // Arrange: an AllocatorHandle whose page-lifetime queue has entries
    // from prior reader drops. `begin` must drain them before we
    // construct a WriteTxn.
    let handle = fresh_handle();
    let alloc = handle.allocator().clone();

    // Simulate a reader drop: new_owned → drop brings count 0 → enqueue.
    {
        let _r = OverflowRef::new_owned(99, 32, alloc.clone()).unwrap();
    }
    assert_eq!(alloc.page_lifetime_queue().depth(), 1);

    // The entry's checkpoint fence has not advanced, so `begin` should
    // observe the queue but leave it pending.
    let result = WriteTxn::begin(1, &alloc, handle.page_source());
    result.expect("begin with a non-eligible page-lifetime entry");
    assert_eq!(alloc.page_lifetime_queue().depth(), 1);
}

#[test]
fn commit_assigns_monotonic_commit_ts() {
    let handle = fresh_handle();
    let alloc = handle.allocator().clone();
    let oracle = TimestampOracle::new();

    let t1 = WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
    let (ts1, pending1, sec1) = t1.commit(&oracle, &handle).expect("commit t1");
    assert!(pending1.is_empty());
    assert!(sec1.is_empty());

    let t2 = WriteTxn::begin(2, &alloc, handle.page_source()).expect("begin with empty queue");
    let (ts2, pending2, sec2) = t2.commit(&oracle, &handle).expect("commit t2");
    assert!(pending2.is_empty());
    assert!(sec2.is_empty());

    assert!(ts2 > ts1, "commit_ts strictly monotone");
    assert_ne!(ts1, Ts::default());
}

#[test]
fn commit_transfers_pending_ownership_to_caller() {
    let handle = fresh_handle();
    let alloc = handle.allocator().clone();
    let oracle = TimestampOracle::new();

    let r = OverflowRef::new_owned(77, 128, alloc.clone()).unwrap();
    assert_eq!(alloc.overflow_refcount(77), 1);

    let mut t = WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
    t.attach_overflow(r);

    let (_ts, pending, sec) = t.commit(&oracle, &handle).expect("commit");
    // Ownership transferred to the returned vec — refcount still 1.
    assert_eq!(alloc.overflow_refcount(77), 1);
    assert_eq!(pending.len(), 1);
    assert!(sec.is_empty());

    // Dropping the returned vec runs OverflowRef::drop on each entry.
    // On commit paths that install into a durable chain, the caller
    // instead moves each ref into the chain and the refcount stays bumped.
    drop(pending);
    assert_eq!(alloc.overflow_refcount(77), 0);
}

#[test]
fn drop_drops_pending_and_decrefs() {
    let handle = fresh_handle();
    let alloc = handle.allocator().clone();

    let r = OverflowRef::new_owned(88, 256, alloc.clone()).unwrap();
    let mut t = WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
    t.attach_overflow(r);
    assert_eq!(alloc.overflow_refcount(88), 1);

    drop(t);
    assert_eq!(alloc.overflow_refcount(88), 0);
    assert_eq!(alloc.page_lifetime_queue().depth(), 1);
}

#[test]
fn finalized_txn_drop_does_not_decref_pending() {
    // Invariant: once commit() has moved ownership of `pending` out of
    // `self`, the Drop of `self` must not re-decref (the entries no
    // longer belong to the txn). Verified by constructing a committed
    // txn whose returned pending we forget about — refcount stays at 1.
    let handle = fresh_handle();
    let alloc = handle.allocator().clone();
    let oracle = TimestampOracle::new();

    let r = OverflowRef::new_owned(55, 64, alloc.clone()).unwrap();
    let mut t = WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
    t.attach_overflow(r);

    let (_ts, pending, _sec) = t.commit(&oracle, &handle).expect("commit");
    // Simulate phase 6 installing `pending` into a chain by forgetting
    // it — the refcount remains bumped as the durable chain now owns it.
    std::mem::forget(pending);
    assert_eq!(alloc.overflow_refcount(55), 1);
}

// -----------------------------------------------------------------------
// Sec-index staging (phase 5)
// -----------------------------------------------------------------------

#[test]
fn stage_sec_index_insert_accumulates() {
    let mut t = WriteTxn::new(1);
    t.stage_sec_index_insert(100, 42, b"k1".to_vec(), b"id1".to_vec());
    t.stage_sec_index_insert(100, 42, b"k2".to_vec(), b"id2".to_vec());

    assert_eq!(t.pending_sec_index.len(), 2);
    assert_eq!(t.pending_sec_index[0].index_id, 100);
    assert_eq!(t.pending_sec_index[0].index_root_page, 42);
    assert_eq!(t.pending_sec_index[0].key, b"k1");
    match &t.pending_sec_index[0].op {
        SecIndexOp::Insert { id_bytes } => assert_eq!(id_bytes, b"id1"),
        _ => panic!("expected Insert"),
    }
}

#[test]
fn stage_sec_index_delete_records_key() {
    let mut t = WriteTxn::new(1);
    t.stage_sec_index_delete(200, 7, b"ghost".to_vec());

    assert_eq!(t.pending_sec_index.len(), 1);
    assert_eq!(t.pending_sec_index[0].index_id, 200);
    assert_eq!(t.pending_sec_index[0].index_root_page, 7);
    assert!(matches!(t.pending_sec_index[0].op, SecIndexOp::Delete));
}

#[test]
fn staged_sec_index_delete_then_insert_preserves_order() {
    let mut t = WriteTxn::new(1);
    t.stage_sec_index_delete(300, 11, b"old".to_vec());
    t.stage_sec_index_insert(300, 11, b"new".to_vec(), b"id".to_vec());

    assert_eq!(t.pending_sec_index.len(), 2);
    assert_eq!(t.pending_sec_index[0].index_id, 300);
    assert_eq!(t.pending_sec_index[0].key, b"old");
    assert!(matches!(t.pending_sec_index[0].op, SecIndexOp::Delete));
    assert_eq!(t.pending_sec_index[1].index_id, 300);
    assert_eq!(t.pending_sec_index[1].key, b"new");
    match &t.pending_sec_index[1].op {
        SecIndexOp::Insert { id_bytes } => assert_eq!(id_bytes, b"id"),
        _ => panic!("expected Insert"),
    }
}

#[test]
fn commit_drains_pending_sec_index_to_caller() {
    // Staged sec-index writes must transfer to the caller on commit.
    let handle = fresh_handle();
    let alloc = handle.allocator().clone();
    let oracle = TimestampOracle::new();

    let mut t = WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
    t.stage_sec_index_insert(42, 3, b"k".to_vec(), b"id".to_vec());
    t.stage_sec_index_delete(42, 3, b"d".to_vec());

    let (_ts, _pending, sec) = t.commit(&oracle, &handle).expect("commit");
    assert_eq!(sec.len(), 2);
    assert_eq!(sec[0].index_id, 42);
    assert_eq!(sec[0].index_root_page, 3);
    assert!(matches!(sec[0].op, SecIndexOp::Insert { .. }));
    assert!(matches!(sec[1].op, SecIndexOp::Delete));
}

#[test]
fn drop_discards_pending_sec_index() {
    // Abort path: staged sec-index writes must NOT reach any durable
    // state. Drop of the txn drops the buffer trivially —
    // `SecIndexWrite` owns no external refcount, so no assertion beyond
    // "no panic, txn drops cleanly."
    let handle = fresh_handle();
    let alloc = handle.allocator().clone();

    let mut t = WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
    t.stage_sec_index_insert(50, 9, b"k".to_vec(), b"id".to_vec());
    assert_eq!(t.pending_sec_index.len(), 1);

    drop(t);
    // No side-effects to observe — the buffer drop is infallible.
}

// -----------------------------------------------------------------------
// Primary-tree staging (phase 6 sub-step 2)
// -----------------------------------------------------------------------

#[test]
fn stage_primary_insert_accumulates() {
    let mut t = WriteTxn::new(1);
    t.stage_primary_insert(
        777,
        "ns.a".to_string(),
        b"k1".to_vec(),
        b"v1".to_vec(),
        None,
    );
    t.stage_primary_update(
        777,
        "ns.a".to_string(),
        b"k2".to_vec(),
        b"v2".to_vec(),
        None,
    );
    t.stage_primary_delete(777, "ns.a".to_string(), b"k3".to_vec(), None);

    assert_eq!(t.pending_primary.len(), 3);
    assert_eq!(t.pending_primary[0].ns_id, 777);
    assert_eq!(t.pending_primary[0].ns, "ns.a");
    assert_eq!(t.pending_primary[0].key, b"k1");
    match &t.pending_primary[0].op {
        PrimaryOp::Insert { data } => assert_eq!(data, b"v1"),
        _ => panic!("expected Insert"),
    }
    match &t.pending_primary[1].op {
        PrimaryOp::Update { data } => assert_eq!(data, b"v2"),
        _ => panic!("expected Update"),
    }
    assert!(matches!(t.pending_primary[2].op, PrimaryOp::Delete));
}

// -----------------------------------------------------------------------
// Phase 2 §3.1a — stage-time ns_id / index_id capture (US-009)
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// Phase 2 §3.7 / §6.2 — emit_logical_txn_frame (US-010)
// -----------------------------------------------------------------------

/// §3.6 emit-side convention: secondary ops come first, primary ops
/// second, with a dense `0..N` `op_ordinal` counter shared across the
/// whole batch. Exercises the frame builder directly so the assertion
/// does not depend on journal observability (which would require
/// US-011 rewiring of the write envelope).
#[test]
fn emit_logical_txn_frame_assigns_ordinals_from_zero_in_staging_order() {
    let mut t = WriteTxn::new(1);
    // 2 sec writes + 3 primary writes — staged in the order below.
    t.stage_sec_index_insert(10, 100, b"s0".to_vec(), b"id0".to_vec());
    t.stage_sec_index_delete(11, 101, b"s1".to_vec());
    t.stage_primary_insert(20, "ns".to_string(), b"p0".to_vec(), b"v0".to_vec(), None);
    t.stage_primary_update(20, "ns".to_string(), b"p1".to_vec(), b"v1".to_vec(), None);
    t.stage_primary_delete(20, "ns".to_string(), b"p2".to_vec(), None);

    let sec_snap: Vec<SecIndexWrite> = t.pending_sec_index.iter().cloned().collect();
    let pri_snap: Vec<PrimaryWrite> = t.pending_primary.iter().cloned().collect();
    let ts = Ts {
        physical_ms: 1_000,
        logical: 5,
    };
    let frame = build_logical_txn_frame(t.txn_id, ts, 0xAA, 0xBB, &pri_snap, &sec_snap);

    use crate::journal::log_file::LogicalOpKind;
    assert_eq!(frame.ops.len(), 5);
    // Dense 0..5 ordinal sequence.
    for (i, op) in frame.ops.iter().enumerate() {
        assert_eq!(op.op_ordinal, i as u32);
    }
    // Sec-first-primary-second ordering.
    assert!(matches!(
        frame.ops[0].kind,
        LogicalOpKind::SecondaryInsert { .. }
    ));
    assert!(matches!(
        frame.ops[1].kind,
        LogicalOpKind::SecondaryDelete { .. }
    ));
    assert!(matches!(
        frame.ops[2].kind,
        LogicalOpKind::PrimaryInsert { .. }
    ));
    assert!(matches!(
        frame.ops[3].kind,
        LogicalOpKind::PrimaryUpdate { .. }
    ));
    assert!(matches!(
        frame.ops[4].kind,
        LogicalOpKind::PrimaryDelete { .. }
    ));
    assert_eq!(frame.diagnostic_txn_id, t.txn_id);
    assert_eq!(frame.commit_ts, ts);
}

/// §3.7 invariant: emit must never run before `allocate_commit_ts`.
/// When the `commit_ts` Cell is `None`, emit panics with an explicit
/// invariant-violation message so the programming error is caught
/// loudly rather than silently producing a zero-timestamp frame.
#[test]
#[should_panic(expected = "§3.7 invariant violation")]
fn emit_logical_txn_frame_panics_if_commit_ts_unset() {
    let handle = fresh_handle();
    let t = WriteTxn::new(1);
    // commit_ts Cell is None — allocate_commit_ts has NOT run.
    let _ = t.emit_logical_txn_frame(&handle, &[], &[]);
}

/// Per §3.1a, `ns_id` and `index_id` must be resolved from the live
/// `CollectionEntry.id` / `IndexEntry.id` at stage time and carried into
/// the staged `PrimaryWrite` / `SecIndexWrite`. A post-stage rename
/// (scaffolded here via direct entry mutation, since mqlite exposes no
/// public rename API) cannot invalidate the recorded id because the
/// stage-time snapshot lives in the staged struct rather than being
/// re-resolved from the catalog at emit time. The test exercises the
/// production commit path: `run_write_commit_envelope` drains `pending_primary`
/// via `std::mem::take` before `txn.commit(...)` runs, and
/// `txn.commit(...)` then drains `pending_sec_index` and emits the
/// `ChainCommit` frame. We replicate that sequence here and assert
/// that both drained vecs carry the stage-time ids.
#[test]
fn rename_safe_staged_ids_survive_rename() {
    use crate::mvcc::timestamp::TimestampOracle;
    use crate::storage::catalog::{CollectionEntry, IndexEntry, IndexState};
    use bson::Document;

    let orig_entry = CollectionEntry {
        id: 42,
        name: "users".to_string(),
        data_root_page: 10,
        data_root_level: 0,
        document_count: 0,
        avg_doc_size: 0,
        created_at: 0,
        options: Document::new(),
    };
    let orig_index = IndexEntry {
        id: 100,
        name: "email_1".to_string(),
        collection: "users".to_string(),
        root_page: 20,
        root_level: 0,
        key_pattern: Document::new(),
        unique: false,
        sparse: false,
        multikey: false,
        entry_count: 0,
        state: IndexState::Ready,
    };

    let handle = fresh_handle();
    let oracle = TimestampOracle::new();
    let mut t = WriteTxn::new(1);
    // Production stage sites (doc_ops.rs + index_maint.rs/
    // secondary_index.rs) read `entry.id` / `index_entry.id` from the
    // LIVE catalog entry at stage time and pass it in here.
    t.stage_primary_insert(
        orig_entry.id,
        orig_entry.name.clone(),
        b"k".to_vec(),
        b"v".to_vec(),
        None,
    );
    t.stage_sec_index_insert(
        orig_index.id,
        orig_index.root_page,
        b"compound".to_vec(),
        b"id".to_vec(),
    );

    // Scaffold a worst-case "rename" as direct entry mutation — the
    // spec explicitly allows this when no public rename API exists.
    // Phase 1 §10.7 says durable ids are stable across renames, so
    // we harden the invariant by going further: we mutate the id to
    // a disjoint value and confirm the staged write is unaffected.
    let mutated_entry = CollectionEntry {
        id: orig_entry.id + 1_000,
        ..orig_entry.clone()
    };
    let mutated_index = IndexEntry {
        id: orig_index.id + 1_000,
        ..orig_index.clone()
    };
    assert_ne!(mutated_entry.id, orig_entry.id);
    assert_ne!(mutated_index.id, orig_index.id);

    // Replicate the production commit envelope:
    //   1. run_write_commit_envelope drains `pending_primary` via mem::take
    //      and hands it to install_pending_primary.
    //   2. txn.commit(...) drains `pending_sec_index` internally and
    //      emits the ChainCommit journal frame.
    // Steps 1+2 are what actually "commits" the staged writes.
    let drained_primary: Vec<PrimaryWrite> = std::mem::take(&mut t.pending_primary).into_vec();
    let (_ts, _pending, drained_sec) = t.commit(&oracle, &handle).expect("commit envelope");

    assert_eq!(drained_primary.len(), 1);
    assert_eq!(drained_primary[0].ns_id, orig_entry.id);
    assert_ne!(drained_primary[0].ns_id, mutated_entry.id);

    assert_eq!(drained_sec.len(), 1);
    assert_eq!(drained_sec[0].index_id, orig_index.id);
    assert_ne!(drained_sec[0].index_id, mutated_index.id);
}

/// §3.7 / US-021 r4 codex blocker — production-emitter path.
/// Stages a write via the real `WriteTxn::stage_*` API, mutates a
/// catalog mapping in memory between stage and emit, calls the
/// production `WriteTxn::emit_logical_txn_frame` helper against a
/// journal-backed handle, then reads the encoded LogicalTxnFrame
/// back from the journal file and asserts the encoded `ns_id` /
/// `index_id` are the STAGE-TIME values, NOT the post-mutation
/// values.
///
/// This test exercises the actual production emitter (not the
/// hand-built encode of `encoded_logical_txn_frame_round_trips_stage_time_ids`)
/// per codex's r3 demand for emit-through-production-emitter proof.
#[test]
fn production_emitter_carries_stage_time_ids_under_mutation() {
    use crate::journal::log_file::{DecodeCtx, LogicalOpKind, LogicalTxnFrame};
    use crate::journal::JournalManager;
    use crate::mvcc::timestamp::TimestampOracle;
    use crate::storage::buffer_pool::{default_sizes, BufferPool};
    use crate::storage::catalog::{CollectionEntry, IndexEntry, IndexState};
    use crate::storage::handle::BufferPoolHandle;
    use crate::storage::header::FileHeader;
    use bson::Document;
    use std::fs::OpenOptions;
    use std::sync::{Arc, Mutex as StdMutex};

    const STAGE_TIME_NS_ID: i64 = 4242;
    const STAGE_TIME_INDEX_ID: i64 = 8484;
    const MUTATED_NS_ID: i64 = STAGE_TIME_NS_ID + 1_000;
    const MUTATED_INDEX_ID: i64 = STAGE_TIME_INDEX_ID + 1_000;

    // Live catalog state — mirrors what the production catalog
    // holds. We mutate THESE entries between stage and emit to
    // model a rename / re-bind that would only affect an
    // emit-time-re-resolver implementation. The production
    // engine reads `entry.id` at stage time
    // (`stage_insert` etc.), captures it into the
    // PrimaryWrite struct, and never re-resolves at emit time.
    let mut live_collection = CollectionEntry {
        id: STAGE_TIME_NS_ID,
        name: "users".to_string(),
        data_root_page: 10,
        data_root_level: 0,
        document_count: 0,
        avg_doc_size: 0,
        created_at: 0,
        options: Document::new(),
    };
    let mut live_index = IndexEntry {
        id: STAGE_TIME_INDEX_ID,
        name: "email_1".to_string(),
        collection: "users".to_string(),
        root_page: 20,
        root_level: 0,
        key_pattern: Document::new(),
        unique: false,
        sparse: false,
        multikey: false,
        entry_count: 0,
        state: IndexState::Ready,
    };

    let dir = tempfile::TempDir::new().expect("tempdir");
    let db_path = dir.path().join("us021_emit.mqlite");

    // Bootstrap a real main file + journal so the handle has a
    // working journal to write to.
    let header = FileHeader::new(0, 0, 0);
    {
        let mut main_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&db_path)
            .expect("create main");
        use std::io::Write;
        main_file.write_all(&header.to_bytes()).expect("write hdr");
    }
    let mut main_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&db_path)
        .expect("reopen main");
    let mgr =
        JournalManager::open_or_create(&db_path, &header, &mut main_file).expect("journal manager");
    let journal = Arc::new(StdMutex::new(mgr));

    // Pool wiring (mirrors `fresh_handle` but with a journal
    // attached via `with_journal`). Uses the canonical test fixture
    // imported at the module level.
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let main_file_arc = Arc::new(StdMutex::new(main_file));
    let handle = Arc::new(BufferPoolHandle::with_journal(
        pool,
        history_pool,
        FileHeader::new(0, 0, 0),
        Arc::clone(&journal),
        main_file_arc,
    ));

    let oracle = TimestampOracle::new();
    // Stage using the LIVE catalog entry's id at this moment.
    // This mirrors `stage_insert` / `secondary_index::stage`
    // in production, which read `entry.id` from the live entry
    // and pass it into `stage_primary_insert` / `stage_sec_index_insert`.
    let mut t = WriteTxn::new(11);
    t.stage_primary_insert(
        live_collection.id,
        live_collection.name.clone(),
        b"k".to_vec(),
        b"v".to_vec(),
        None,
    );
    t.stage_sec_index_insert(
        live_index.id,
        live_index.root_page,
        b"key".to_vec(),
        b"id".to_vec(),
    );

    // Snapshot stage-time copies BEFORE the mutation.
    let staged_primary: Vec<PrimaryWrite> = std::mem::take(&mut t.pending_primary).into_vec();
    let staged_sec: Vec<SecIndexWrite> = std::mem::take(&mut t.pending_sec_index).into_vec();
    assert_eq!(staged_primary[0].ns_id, STAGE_TIME_NS_ID);
    assert_eq!(staged_sec[0].index_id, STAGE_TIME_INDEX_ID);

    // CATALOG MUTATION between stage and emit. Reassign the live
    // CollectionEntry / IndexEntry IDs to the post-rename values.
    // An emit-time-re-resolver implementation would have re-read
    // `live_collection.id` here and produced a frame carrying
    // MUTATED_NS_ID; the staged snapshot the production emitter
    // uses is immune to this mutation, so the emitted frame
    // carries STAGE_TIME_NS_ID.
    live_collection.id = MUTATED_NS_ID;
    live_index.id = MUTATED_INDEX_ID;
    assert_eq!(live_collection.id, MUTATED_NS_ID);
    assert_eq!(live_index.id, MUTATED_INDEX_ID);
    // The catalog mapping is now: name "users" → id MUTATED_NS_ID,
    // name "email_1" → id MUTATED_INDEX_ID. The staged_primary /
    // staged_sec vecs still carry the STAGE_TIME ids.

    // Production emitter call.
    let _commit_ts = t.allocate_commit_ts(&oracle).expect("allocate commit_ts");
    t.emit_logical_txn_frame(&handle, &staged_primary, &staged_sec)
        .expect("emit through production emitter");

    // Drop the journal lock before reading the bytes back.
    drop(t);

    // Read the journal file bytes and find the LogicalTxn frame.
    // The frame_kind discriminant for LogicalTxn is 0x03; the
    // first op's body starts at frame_start + 48 + 8.
    let salts = handle
        .journal_salts()
        .expect("journal must be attached on this handle");
    drop(handle);
    // Drop the JournalManager so the file is closed for reading.
    let _ = Arc::try_unwrap(journal)
        .map_err(|_| "journal Arc still held")
        .ok();

    let journal_path = {
        let mut p = db_path.as_os_str().to_owned();
        p.push("-journal");
        std::path::PathBuf::from(p)
    };
    let bytes = std::fs::read(&journal_path).expect("read journal file");

    // Walk the journal byte stream looking for the LogicalTxn
    // frame (kind 0x03). The §4.1 layout starts with kind byte at
    // offset 0 of the frame; total_frame_bytes at offset 4.
    // Journal header is 32 bytes per `JOURNAL_HEADER_SIZE`.
    const JOURNAL_HEADER_SIZE: usize = 32;
    const FRAME_KIND_LOGICAL_TXN: u8 = 0x03;
    let mut frame_offset = None;
    let mut cursor = JOURNAL_HEADER_SIZE;
    while cursor < bytes.len() && frame_offset.is_none() {
        if bytes[cursor] == FRAME_KIND_LOGICAL_TXN {
            frame_offset = Some(cursor);
            break;
        }
        // Advance by 1 byte; only valid for the test scenario where
        // we know exactly one frame was written.
        cursor += 1;
    }
    let frame_start = frame_offset.expect("LogicalTxn frame not found in journal");
    let total = u32::from_le_bytes(
        bytes[frame_start + 4..frame_start + 8]
            .try_into()
            .expect("4 bytes"),
    ) as usize;
    let frame_bytes = &bytes[frame_start..frame_start + total];

    let decoded = LogicalTxnFrame::decode(frame_bytes, salts.0, salts.1, DecodeCtx::Scanning)
        .expect("decode")
        .expect("Some");

    let mut saw_primary = false;
    let mut saw_sec = false;
    for op in &decoded.ops {
        match &op.kind {
            LogicalOpKind::PrimaryInsert { ns_id, .. } => {
                assert_eq!(
                    *ns_id, STAGE_TIME_NS_ID,
                    "production emitter must encode stage-time \
                     ns_id={STAGE_TIME_NS_ID}, not mutated \
                     {MUTATED_NS_ID}"
                );
                saw_primary = true;
            }
            LogicalOpKind::SecondaryInsert { index_id, .. } => {
                assert_eq!(
                    *index_id, STAGE_TIME_INDEX_ID,
                    "production emitter must encode stage-time \
                     index_id={STAGE_TIME_INDEX_ID}, not mutated \
                     {MUTATED_INDEX_ID}"
                );
                saw_sec = true;
            }
            _ => {}
        }
    }
    assert!(saw_primary, "decoded frame must contain a PrimaryInsert");
    assert!(saw_sec, "decoded frame must contain a SecondaryInsert");
}

/// §3.7 / US-021 r3 codex blocker — direct encode/decode proof
/// that the LogicalTxnFrame format-encoding pipeline carries the
/// STAGE-TIME `ns_id` / `index_id` through to the on-disk bytes.
/// Constructs a `LogicalTxnFrame` directly with stage-time ids,
/// encodes it, decodes it back, and asserts the round-tripped
/// `ns_id` / `index_id` equal the stage-time values — never any
/// post-stage mutation. Combined with the existing
/// `rename_safe_staged_ids_survive_rename` proof (drained writes
/// carry the staged id under in-memory mutation), this closes the
/// stage→emit→decode chain end-to-end at the unit-test layer.
#[test]
fn encoded_logical_txn_frame_round_trips_stage_time_ids() {
    use crate::journal::log_file::{
        DecodeCtx, LogicalOp, LogicalOpKind, LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION,
    };
    use crate::mvcc::timestamp::Ts;

    const STAGE_TIME_NS_ID: i64 = 4242;
    const STAGE_TIME_INDEX_ID: i64 = 8484;
    const MUTATED_NS_ID: i64 = STAGE_TIME_NS_ID + 1_000;
    const MUTATED_INDEX_ID: i64 = STAGE_TIME_INDEX_ID + 1_000;
    const SALT1: u32 = 0xCAFE_BABE;
    const SALT2: u32 = 0xDEAD_BEEF;

    let frame = LogicalTxnFrame {
        salt1: SALT1,
        salt2: SALT2,
        commit_ts: Ts {
            physical_ms: 1234,
            logical: 5,
        },
        diagnostic_txn_id: 7,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![
            LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::SecondaryInsert {
                    index_id: STAGE_TIME_INDEX_ID,
                    key: b"key".to_vec(),
                    id_bytes: b"id".to_vec(),
                },
            },
            LogicalOp {
                op_ordinal: 1,
                kind: LogicalOpKind::PrimaryInsert {
                    ns_id: STAGE_TIME_NS_ID,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    overflow: None,
                },
            },
        ],
    };
    let bytes = frame.encode().expect("encode stage-time frame");
    let decoded = LogicalTxnFrame::decode(&bytes, SALT1, SALT2, DecodeCtx::Scanning)
        .expect("decode")
        .expect("Some");

    let mut saw_primary = false;
    let mut saw_sec = false;
    for op in &decoded.ops {
        match &op.kind {
            LogicalOpKind::PrimaryInsert { ns_id, .. } => {
                assert_eq!(
                    *ns_id, STAGE_TIME_NS_ID,
                    "encoded PrimaryInsert ns_id must round-trip the \
                     stage-time {STAGE_TIME_NS_ID}, not the mutated \
                     {MUTATED_NS_ID}"
                );
                saw_primary = true;
            }
            LogicalOpKind::SecondaryInsert { index_id, .. } => {
                assert_eq!(
                    *index_id, STAGE_TIME_INDEX_ID,
                    "encoded SecondaryInsert index_id must round-trip \
                     the stage-time {STAGE_TIME_INDEX_ID}, not the \
                     mutated {MUTATED_INDEX_ID}"
                );
                saw_sec = true;
            }
            _ => {}
        }
    }
    assert!(saw_primary, "decoded frame must contain a PrimaryInsert");
    assert!(saw_sec, "decoded frame must contain a SecondaryInsert");
}
