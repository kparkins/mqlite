//! Tests extracted from `history_store.rs` to keep the source file under the
//! 800-line guideline. See [`super`] for the production code.

use super::*;
use crate::storage::btree::MemPageStore;

fn ts(ms: u64, logical: u32) -> Ts {
    Ts {
        physical_ms: ms,
        logical,
    }
}

fn inline_entry(start: Ts, stop: Ts, txn: u64, payload: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: start,
        stop_ts: stop,
        txn_id: txn,
        data: VersionData::Inline(payload.to_vec()),
        is_tombstone: false,
    }
}

fn tombstone(start: Ts, stop: Ts, txn: u64) -> VersionEntry {
    VersionEntry {
        start_ts: start,
        stop_ts: stop,
        txn_id: txn,
        data: VersionData::Inline(Vec::new()),
        is_tombstone: true,
    }
}

// -----------------------------------------------------------------------
// Key schema
// -----------------------------------------------------------------------

#[test]
fn key_schema_encode_decode_roundtrip() {
    let key =
        encode_history_key(7, KIND_PRIMARY, b"abc", Ts { physical_ms: 100, logical: 5 });
    // (ns=7 BE) || (kind=0) || b"abc" || (ts BE 12B)
    // 4 + 1 + 3 + 12 = 20
    assert_eq!(key.len(), 20);
    assert_eq!(&key[0..4], &[0, 0, 0, 7]);
    assert_eq!(key[4], KIND_PRIMARY);
    assert_eq!(&key[5..8], b"abc");
    let ts_buf: [u8; 12] = key[8..20].try_into().unwrap();
    assert_eq!(Ts::from_be_bytes(ts_buf), Ts { physical_ms: 100, logical: 5 });

    let (ns, kind, key_bytes, start_ts) = decode_history_key(&key).unwrap();
    assert_eq!(ns, 7);
    assert_eq!(kind, KIND_PRIMARY);
    assert_eq!(key_bytes, b"abc");
    assert_eq!(start_ts, Ts { physical_ms: 100, logical: 5 });
}

#[test]
fn key_schema_primary_vs_sec_index_do_not_alias() {
    // Same (ns, bytes, start_ts), different kind_tag — must not collide.
    let pri = encode_history_key(1, KIND_PRIMARY, b"K", ts(50, 0));
    let sec = encode_history_key(1, KIND_SEC_INDEX_BASE, b"K", ts(50, 0));
    assert_ne!(pri, sec);
    // Primary sorts before sec-index (kind_tag 0x00 < 0x01).
    assert!(pri < sec);
}

#[test]
fn key_schema_lexicographic_sort_matches_chronological() {
    // Same (ns, kind, key_bytes), different start_ts.
    let early = encode_history_key(9, KIND_PRIMARY, b"X", ts(10, 0));
    let mid = encode_history_key(9, KIND_PRIMARY, b"X", ts(10, 7));
    let late = encode_history_key(9, KIND_PRIMARY, b"X", ts(11, 0));
    assert!(early < mid);
    assert!(mid < late);
}

#[test]
fn key_schema_ns_id_big_endian_prefix_groups_by_namespace() {
    let ns1_first = encode_history_key(1, KIND_PRIMARY, b"zzz", ts(100, 0));
    let ns2_first = encode_history_key(2, KIND_PRIMARY, b"aaa", ts(0, 0));
    // ns1_first must sort before ns2_first even though b"zzz" > b"aaa" —
    // the ns prefix dominates.
    assert!(ns1_first < ns2_first);
}

#[test]
fn key_schema_decode_rejects_truncated_buffer() {
    assert!(decode_history_key(&[0, 0, 0, 1, KIND_PRIMARY]).is_none());
}

// -----------------------------------------------------------------------
// VersionEntry value roundtrip
// -----------------------------------------------------------------------

#[test]
fn version_entry_inline_roundtrip() {
    let entry = inline_entry(ts(1, 0), ts(2, 0), 42, b"hello");
    let bytes = encode_version_entry_value(&entry);
    let decoded = decode_version_entry_value(&bytes, None).unwrap();
    assert_eq!(decoded.start_ts, entry.start_ts);
    assert_eq!(decoded.stop_ts, entry.stop_ts);
    assert_eq!(decoded.txn_id, entry.txn_id);
    assert!(!decoded.is_tombstone);
    match decoded.data {
        VersionData::Inline(b) => assert_eq!(b, b"hello".to_vec()),
        _ => panic!("expected Inline"),
    }
}

#[test]
fn version_entry_tombstone_roundtrip() {
    let entry = tombstone(ts(10, 0), ts(20, 0), 7);
    let bytes = encode_version_entry_value(&entry);
    let decoded = decode_version_entry_value(&bytes, None).unwrap();
    assert!(decoded.is_tombstone);
}

#[test]
fn version_entry_truncated_buffer_errors() {
    let err = decode_version_entry_value(&[0u8; 10], None);
    assert!(err.is_err());
}

// -----------------------------------------------------------------------
// Cold-read probe (acceptance: "ReadView missing in-memory chain, then
// history store probed via descending range-scan")
// -----------------------------------------------------------------------

#[test]
fn cold_read_probe_returns_newest_version_below_read_ts() {
    let mut hs = HistoryStore::create(MemPageStore::new()).unwrap();
    // Three versions of doc "d" at ts 5, 10, 50 — all in ns=3.
    hs.insert(3, KIND_PRIMARY, b"d", &inline_entry(ts(5, 0), ts(10, 0), 1, b"v5"))
        .unwrap();
    hs.insert(3, KIND_PRIMARY, b"d", &inline_entry(ts(10, 0), ts(50, 0), 2, b"v10"))
        .unwrap();
    hs.insert(3, KIND_PRIMARY, b"d", &inline_entry(ts(50, 0), ts(100, 0), 3, b"v50"))
        .unwrap();
    // Noise in another namespace — must not leak into the ns=3 probe.
    hs.insert(4, KIND_PRIMARY, b"d", &inline_entry(ts(5, 0), ts(100, 0), 9, b"other"))
        .unwrap();

    // read_ts = 30 → should return v10.
    let got = hs.probe_primary(3, b"d", ts(30, 0)).unwrap().unwrap();
    match got.data {
        VersionData::Inline(bytes) => assert_eq!(bytes, b"v10".to_vec()),
        _ => panic!("expected Inline"),
    }

    // read_ts = 4 → below earliest version, returns None.
    assert!(hs.probe_primary(3, b"d", ts(4, 0)).unwrap().is_none());

    // read_ts = 200 → above all, returns v50 (newest).
    let latest = hs.probe_primary(3, b"d", ts(200, 0)).unwrap().unwrap();
    match latest.data {
        VersionData::Inline(bytes) => assert_eq!(bytes, b"v50".to_vec()),
        _ => panic!("expected Inline"),
    }
}

#[test]
fn cold_read_probe_respects_namespace_and_kind_boundaries() {
    let mut hs = HistoryStore::create(MemPageStore::new()).unwrap();
    // Same key bytes, same ns, different kind_tag → must not cross.
    hs.insert(1, KIND_PRIMARY, b"K", &inline_entry(ts(10, 0), ts(20, 0), 1, b"primary"))
        .unwrap();
    hs.insert(
        1,
        KIND_SEC_INDEX_BASE,
        b"K",
        &inline_entry(ts(10, 0), ts(20, 0), 2, b"sec"),
    )
    .unwrap();

    let pri = hs.probe_primary(1, b"K", ts(100, 0)).unwrap().unwrap();
    match pri.data {
        VersionData::Inline(b) => assert_eq!(b, b"primary".to_vec()),
        _ => panic!(),
    }

    let sec = hs
        .probe_sec_index(1, b"K", KIND_SEC_INDEX_BASE, ts(100, 0))
        .unwrap()
        .unwrap();
    match sec.data {
        VersionData::Inline(b) => assert_eq!(b, b"sec".to_vec()),
        _ => panic!(),
    }
}

#[test]
fn sec_index_tombstone_hides_candidate_and_ticks_metric() {
    let mut hs = HistoryStore::create(MemPageStore::new()).unwrap();
    // A sec-index tombstone at ts=50; newest entry `<= read_ts`.
    hs.insert(
        1,
        KIND_SEC_INDEX_BASE,
        b"K",
        &inline_entry(ts(10, 0), ts(50, 0), 1, b"real"),
    )
    .unwrap();
    hs.insert(1, KIND_SEC_INDEX_BASE, b"K", &tombstone(ts(50, 0), Ts::MAX, 2))
        .unwrap();

    crate::mvcc::metrics::reset_secondary_index_tombstone_hits();
    let got = hs
        .probe_sec_index(1, b"K", KIND_SEC_INDEX_BASE, ts(100, 0))
        .unwrap();
    assert!(got.is_none(), "tombstone must hide the candidate");
    assert!(
        crate::mvcc::metrics::secondary_index_tombstone_hits_snapshot() >= 1,
        "tombstone_hits counter must tick on probe"
    );
}

// -----------------------------------------------------------------------
// Non-recursion criterion: the history store runs on its own BTreePageStore,
// never pinning any page from a foreign store. Demonstrated by giving
// the main-data store and the history store two independent
// MemPageStores and verifying the history store's ops don't mutate the
// main store.
// -----------------------------------------------------------------------

#[test]
fn history_store_isolated_from_main_data_store() {
    let main_store = MemPageStore::new();
    let hist_store = MemPageStore::new();

    let main_tree = BTree::create(main_store).unwrap();
    let main_root_before = main_tree.root_page;

    let mut hs = HistoryStore::create(hist_store).unwrap();
    hs.insert(1, KIND_PRIMARY, b"K", &inline_entry(ts(10, 0), Ts::MAX, 1, b"v"))
        .unwrap();
    // A full probe round-trip would also traverse the history store only.
    let _ = hs.probe_primary(1, b"K", ts(100, 0)).unwrap();

    // Main tree untouched — root never moved, no leaves allocated,
    // no journal/frame I/O on the main store is possible by type
    // construction because `HistoryStore` only holds `hist_store`.
    assert_eq!(main_tree.root_page, main_root_before);
}

// -----------------------------------------------------------------------
// GC pass
// -----------------------------------------------------------------------

/// Given 10k entries with `stop_ts` spread across [1, 10_000], and
/// `ort = Ts{3000, 0}`: exactly 3000 entries are deleted (inline variant;
/// no overflow required to prove the delete-count invariant).
#[test]
fn gc_pass_deletes_exactly_the_expired_entries() {
    let _gc_lock = crate::mvcc::metrics::GC_PASSES_TEST_LOCK.lock().unwrap();
    let mut hs = HistoryStore::create(MemPageStore::new()).unwrap();
    // 10_000 entries keyed by (ns=1, KIND_PRIMARY, i-big-endian)
    // with distinct start_ts per entry. stop_ts == start_ts + 1 so
    // `stop_ts <= ort == 3000` iff start_ts < 3000.
    for i in 0..10_000u64 {
        let key = (i as u32).to_be_bytes();
        hs.insert(
            1,
            KIND_PRIMARY,
            &key,
            &inline_entry(
                ts(i, 0),
                ts(i + 1, 0),
                i,
                format!("v{i}").as_bytes(),
            ),
        )
        .unwrap();
    }

    crate::mvcc::metrics::reset_history_store_gc_passes();
    let result = hs.gc_pass(ts(3000, 0)).unwrap();
    // stop_ts <= 3000 is entries with start_ts < 3000 → i in 0..2999 (2999 entries),
    // plus i == 2999 where stop_ts = 3000 (exactly equal, also expired).
    // So i in 0..=2999 → 3000 entries.
    assert_eq!(result.entries_deleted, 3000);
    assert_eq!(result.pages_freed, 0, "no overflow entries → no pages freed");
    assert_eq!(
        crate::mvcc::metrics::history_store_gc_passes_snapshot(),
        1,
        "gc_passes counter must tick exactly once"
    );

    // Post-GC: a probe at read_ts = 5000 for a non-GC'd key must still
    // resolve; a probe for a GC'd key must return None.
    let live_key = (5000u32).to_be_bytes();
    let got = hs.probe_primary(1, &live_key, ts(10_000, 0)).unwrap();
    assert!(got.is_some(), "non-expired entry must still be reachable");

    let gc_key = (0u32).to_be_bytes();
    let gone = hs.probe_primary(1, &gc_key, ts(10_000, 0)).unwrap();
    assert!(gone.is_none(), "GC'd entry must be absent");
}

/// Plan T8 acceptance bullet 2: GC respects active-reader horizon. An
/// entry with `stop_ts > ort` must never be deleted even if the entry
/// looks stale by oracle time.
#[test]
fn gc_pass_respects_active_readview_horizon() {
    let _gc_lock = crate::mvcc::metrics::GC_PASSES_TEST_LOCK.lock().unwrap();
    let mut hs = HistoryStore::create(MemPageStore::new()).unwrap();
    // Reader's `ort = Ts{100,0}`. Two entries:
    //   A: stop_ts = 50 (expired — visible to no live reader)
    //   B: stop_ts = 150 (STILL visible at ts 100 — must NOT be deleted)
    //   C: stop_ts = Ts::MAX (live head — must NEVER be deleted)
    hs.insert(
        1,
        KIND_PRIMARY,
        b"A",
        &inline_entry(ts(10, 0), ts(50, 0), 1, b"a"),
    )
    .unwrap();
    hs.insert(
        1,
        KIND_PRIMARY,
        b"B",
        &inline_entry(ts(90, 0), ts(150, 0), 2, b"b"),
    )
    .unwrap();
    hs.insert(
        1,
        KIND_PRIMARY,
        b"C",
        &inline_entry(ts(100, 0), Ts::MAX, 3, b"c"),
    )
    .unwrap();

    let result = hs.gc_pass(ts(100, 0)).unwrap();
    assert_eq!(result.entries_deleted, 1, "only A expires at ort=100");

    assert!(
        hs.probe_primary(1, b"A", ts(200, 0)).unwrap().is_none(),
        "A should be GC'd"
    );
    assert!(
        hs.probe_primary(1, b"B", ts(200, 0)).unwrap().is_some(),
        "B has stop_ts=150 > ort=100; must be retained"
    );
    assert!(
        hs.probe_primary(1, b"C", ts(200, 0)).unwrap().is_some(),
        "C has stop_ts=Ts::MAX (live head); must be retained"
    );
}

/// Plan T8 acceptance bullet 1: overflow-bearing entries get their
/// refcount decremented by RAII on GC. At refcount 0 the page is
/// enqueued on the allocator's deferred-free queue and counted in
/// `pages_freed`.
#[test]
fn gc_pass_overflow_entries_decref_via_raii_and_enqueue_deferred_free() {
    let _gc_lock = crate::mvcc::metrics::GC_PASSES_TEST_LOCK.lock().unwrap();
    use crate::storage::allocator::AllocatorHandle;
    use crate::storage::header::FileHeader;
    use std::sync::Arc;

    let alloc = Arc::new(AllocatorHandle::new(FileHeader::new(0, 0, 0)));
    // Seed a logical +1 refcount on first_page=777 — simulates the
    // refcount owned by the history entry. This matches what a real
    // reconciliation-path insert would have done.
    alloc.set_overflow_refcount_for_test(777, 1);

    let mut hs = HistoryStore::create(MemPageStore::new())
        .unwrap()
        .with_overflow_allocator(Arc::clone(&alloc));
    let overflow_entry = VersionEntry {
        start_ts: ts(10, 0),
        stop_ts: ts(20, 0), // expired at ort=100
        txn_id: 42,
        data: VersionData::Overflow(crate::mvcc::version::OverflowRef::from_existing_refcount(
            777,
            2048,
            (*alloc).clone(),
        )),
        is_tombstone: false,
    };
    // Insert serializes the bytes; re-seed the refcount post-insert to
    // match the "caller leaks its OverflowRef" production semantics.
    hs.insert(1, KIND_PRIMARY, b"K", &overflow_entry).unwrap();
    std::mem::forget(overflow_entry);
    assert_eq!(alloc.overflow_refcount(777), 1);

    let before_depth = alloc.deferred_free_queue().depth();
    let result = hs.gc_pass(ts(100, 0)).unwrap();
    assert_eq!(result.entries_deleted, 1);
    assert_eq!(
        result.pages_freed, 1,
        "refcount hit 0 → one page counted as freed"
    );
    assert_eq!(
        alloc.overflow_refcount(777),
        0,
        "RAII decref must bring refcount to 0"
    );
    assert_eq!(
        alloc.deferred_free_queue().depth(),
        before_depth + 1,
        "refcount 0 drop must enqueue first_page for deferred free"
    );
}

/// GC counter ticks exactly once per `gc_pass` invocation, even when
/// the scan found nothing to delete.
#[test]
fn gc_pass_noop_still_ticks_counter() {
    let _gc_lock = crate::mvcc::metrics::GC_PASSES_TEST_LOCK.lock().unwrap();
    let mut hs = HistoryStore::create(MemPageStore::new()).unwrap();
    hs.insert(
        1,
        KIND_PRIMARY,
        b"K",
        &inline_entry(ts(10, 0), Ts::MAX, 1, b"live"),
    )
    .unwrap();

    crate::mvcc::metrics::reset_history_store_gc_passes();
    let result = hs.gc_pass(ts(1000, 0)).unwrap();
    assert_eq!(result.entries_deleted, 0);
    assert_eq!(
        crate::mvcc::metrics::history_store_gc_passes_snapshot(),
        1,
        "gc_passes counter ticks on every call (even no-op)"
    );
}
