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

// -----------------------------------------------------------------------
// Sec-index staging (phase 5)
// -----------------------------------------------------------------------

#[test]
fn stage_sec_index_insert_accumulates() {
    let mut t = WriteTxn::new(1);
    t.stage_sec_index_insert(100, 42, 0, None, b"k1".to_vec(), b"id1".to_vec());
    t.stage_sec_index_insert(100, 42, 0, None, b"k2".to_vec(), b"id2".to_vec());

    assert_eq!(t.pending_sec_index.len(), 2);
    assert_eq!(t.pending_sec_index[0].index_id, 100);
    assert_eq!(t.pending_sec_index[0].index_root_page, 42);
    assert_eq!(t.pending_sec_index[0].index_root_level, 0);
    assert_eq!(t.pending_sec_index[0].key, b"k1");
    match &t.pending_sec_index[0].op {
        SecIndexOp::Insert { id_bytes } => assert_eq!(id_bytes, b"id1"),
        _ => panic!("expected Insert"),
    }
}

#[test]
fn stage_sec_index_delete_records_key() {
    let mut t = WriteTxn::new(1);
    t.stage_sec_index_delete(200, 7, 0, b"ghost".to_vec());

    assert_eq!(t.pending_sec_index.len(), 1);
    assert_eq!(t.pending_sec_index[0].index_id, 200);
    assert_eq!(t.pending_sec_index[0].index_root_page, 7);
    assert_eq!(t.pending_sec_index[0].index_root_level, 0);
    assert!(matches!(t.pending_sec_index[0].op, SecIndexOp::Delete));
}

#[test]
fn staged_sec_index_delete_then_insert_preserves_order() {
    let mut t = WriteTxn::new(1);
    t.stage_sec_index_delete(300, 11, 0, b"old".to_vec());
    t.stage_sec_index_insert(300, 11, 0, None, b"new".to_vec(), b"id".to_vec());

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
fn drop_discards_pending_sec_index() {
    // Abort path: staged sec-index writes must NOT reach any durable
    // state. Drop of the txn drops the buffer trivially —
    // `SecIndexWrite` owns no external refcount, so no assertion beyond
    // "no panic, txn drops cleanly."
    let handle = fresh_handle();
    let alloc = handle.allocator().clone();

    let mut t = WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
    t.stage_sec_index_insert(50, 9, 0, None, b"k".to_vec(), b"id".to_vec());
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
        PrimaryTarget::new(777, "ns.a".to_string(), 9, 0),
        b"k1".to_vec(),
        b"v1".to_vec(),
        None,
    );
    t.stage_primary_update(
        PrimaryTarget::new(777, "ns.a".to_string(), 9, 0),
        b"k2".to_vec(),
        b"v2".to_vec(),
        None,
    );
    t.stage_primary_delete(777, "ns.a".to_string(), 9, 0, b"k3".to_vec(), None);

    assert_eq!(t.pending_primary.len(), 3);
    assert_eq!(t.pending_primary[0].ns_id, 777);
    assert_eq!(t.pending_primary[0].ns, "ns.a");
    assert_eq!(t.pending_primary[0].root_page, 9);
    assert_eq!(t.pending_primary[0].root_level, 0);
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
    use crate::journal::wire::{
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
