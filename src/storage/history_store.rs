//! History store — kind-tagged B-tree of aged MVCC version entries.
//!
//! Lives on a **dedicated buffer-pool partition** (plan §T7: "Dedicated
//! buffer pool partition is NON-NEGOTIABLE — prevents recursive eviction").
//! Reconciliation evicting a main-data leaf page can install an aged
//! `VersionEntry` here without re-entering the main-data partition mutex,
//! because the history store's B-tree pins pages in its own
//! [`BufferPool`](super::buffer_pool::BufferPool). The lock-order document
//! at the top of `src/mvcc/read_view.rs` pins this at position **1**
//! (outermost).
//!
//! ## Key schema (v1 — Format Lock)
//!
//! ```text
//! key = (ns_id: u32 BE)(kind_tag: u8)(key_bytes: bytes)(start_ts: Ts BE 12B)
//! ```
//!
//! * `ns_id` — collection/namespace identifier. Big-endian so lexicographic
//!   sort matches numeric sort for prefix scans.
//! * `kind_tag` — [`KIND_PRIMARY`] (`0x00`) for primary-document versions;
//!   [`KIND_SEC_INDEX_BASE`] (`0x01`)..=`0xFE` for secondary-index versions.
//!   `0xFF` is reserved.
//! * `key_bytes` — for primary: document id; for sec-index: compound key.
//! * `start_ts` — [`Ts::to_be_bytes`] so chronological order equals
//!   lexicographic order. A descending range scan from
//!   `(ns, kind, key, read_ts)` finds the newest version `<= read_ts`
//!   as the first hit.
//!
//! ## Probe semantics
//!
//! * [`HistoryStore::probe_primary`] — cold-read fallthrough when the
//!   main-data leaf's in-memory chain has no entry visible at `read_ts`.
//! * [`HistoryStore::probe_sec_index`] — cold-read fallthrough for a
//!   secondary-index reader; tombstone hits short-circuit to `None`
//!   and tick `secondary_index_tombstone_hits_total` (plan §T6 / §T7).
//!
//! ## Value layout
//!
//! The B-tree value for one history entry carries a self-contained
//! serialization of [`VersionEntry`]:
//!
//! ```text
//! value = start_ts(12 LE)
//!       | stop_ts(12 LE)
//!       | txn_id(8 LE)
//!       | is_tombstone(1 B)
//!       | data_kind(1 B)   // 0 = Inline, 1 = Overflow
//!       | payload…
//! ```
//!
//! Inline payload: `len: u32 LE` || bytes.
//! Overflow payload: `first_page: u32 LE` || `total_length: u64 LE`.
//! Overflow rehydration needs an allocator handle and is deferred to the
//! caller (plan §T9 wires the real refcounting on probe).

use std::cell::Cell;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::{OverflowRef, VersionData, VersionEntry};
use crate::storage::allocator::AllocatorHandle;
use crate::storage::btree::{BTree, BTreePageStore};

// ---------------------------------------------------------------------------
// Thread-local non-recursion sentinel (plan §T7)
// ---------------------------------------------------------------------------
//
// Architect non-blocking request: catch "history-store reconcile pins
// main-data page" at runtime. The invariant is enforced structurally by
// giving `HistoryStore` its own dedicated [`BufferPool`] partition, but a
// runtime sentinel guards against future wiring mistakes (e.g. someone
// accidentally routing a history-store probe through the main pool's
// `BufferPoolPageSource`). Every public `HistoryStore` entry point
// increments the depth; the main pool's `fetch_page` `debug_assert!`s the
// depth is zero.
thread_local! {
    static HISTORY_STORE_DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// Returns the depth of nested `HistoryStore` calls on the current thread.
/// Non-zero ⇒ the caller is somewhere inside a `HistoryStore` entry point.
pub(crate) fn history_store_depth() -> usize {
    HISTORY_STORE_DEPTH.with(|c| c.get())
}

/// RAII depth-counter guard. Increments on `new`, decrements on drop.
struct HistoryStoreGuard;

impl HistoryStoreGuard {
    fn enter() -> Self {
        HISTORY_STORE_DEPTH.with(|c| c.set(c.get() + 1));
        Self
    }
}

impl Drop for HistoryStoreGuard {
    fn drop(&mut self) {
        HISTORY_STORE_DEPTH.with(|c| c.set(c.get().saturating_sub(1)));
    }
}

// ---------------------------------------------------------------------------
// Kind tags
// ---------------------------------------------------------------------------

/// Kind-tag for a primary-document version entry.
pub(crate) const KIND_PRIMARY: u8 = 0x00;

/// Kind-tag base for secondary-index version entries. Add the 1-based
/// sec-index ordinal (1..=0xFE) to get the concrete tag.
#[allow(dead_code)]
pub(crate) const KIND_SEC_INDEX_BASE: u8 = 0x01;

/// Reserved upper bound; kinds above this are not valid.
#[allow(dead_code)]
pub(crate) const KIND_RESERVED: u8 = 0xFF;

// ---------------------------------------------------------------------------
// Key encoding / decoding
// ---------------------------------------------------------------------------

/// Encode a history-store key per the v1 schema.
///
/// Layout: `(ns_id BE 4)(kind_tag 1)(key_bytes)(start_ts BE 12)`.
pub(crate) fn encode_history_key(
    ns_id: u32,
    kind_tag: u8,
    key_bytes: &[u8],
    start_ts: Ts,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 1 + key_bytes.len() + 12);
    out.extend_from_slice(&ns_id.to_be_bytes());
    out.push(kind_tag);
    out.extend_from_slice(key_bytes);
    out.extend_from_slice(&start_ts.to_be_bytes());
    out
}

/// Inverse of [`encode_history_key`]. Returns `None` when `bytes` is too
/// short to carry a valid header/footer.
#[allow(dead_code)]
pub(crate) fn decode_history_key(bytes: &[u8]) -> Option<(u32, u8, &[u8], Ts)> {
    if bytes.len() < 4 + 1 + 12 {
        return None;
    }
    let ns_id = u32::from_be_bytes(bytes[0..4].try_into().ok()?);
    let kind_tag = bytes[4];
    let body_end = bytes.len() - 12;
    let key_bytes = &bytes[5..body_end];
    let mut ts_buf = [0u8; 12];
    ts_buf.copy_from_slice(&bytes[body_end..]);
    let start_ts = Ts::from_be_bytes(ts_buf);
    Some((ns_id, kind_tag, key_bytes, start_ts))
}

/// Build the inclusive upper bound of a descending probe: the largest key
/// that shares `(ns, kind, key_bytes)` with the probe target and has
/// `start_ts <= read_ts`. The v1 schema means this is simply the encoding
/// of `(ns, kind, key_bytes, read_ts)`.
fn probe_upper_bound(ns_id: u32, kind_tag: u8, key_bytes: &[u8], read_ts: Ts) -> Vec<u8> {
    encode_history_key(ns_id, kind_tag, key_bytes, read_ts)
}

/// Build the prefix that every entry for `(ns, kind, key_bytes)` shares.
/// Used to confirm a scanned key still belongs to the probe target before
/// decoding it as a version entry.
fn probe_prefix(ns_id: u32, kind_tag: u8, key_bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + 1 + key_bytes.len());
    out.extend_from_slice(&ns_id.to_be_bytes());
    out.push(kind_tag);
    out.extend_from_slice(key_bytes);
    out
}

// ---------------------------------------------------------------------------
// VersionEntry value serialization
// ---------------------------------------------------------------------------

const DATA_KIND_INLINE: u8 = 0;
const DATA_KIND_OVERFLOW: u8 = 1;

/// Serialize a `VersionEntry` to the history-store value layout.
#[cfg(test)]
pub(crate) fn encode_version_entry_value(entry: &VersionEntry) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + 12 + 8 + 1 + 1 + 16);
    out.extend_from_slice(&entry.start_ts.to_le_bytes());
    out.extend_from_slice(&entry.stop_ts.to_le_bytes());
    out.extend_from_slice(&entry.txn_id.to_le_bytes());
    out.push(u8::from(entry.is_tombstone));
    match &entry.data {
        VersionData::Inline(bytes) => {
            out.push(DATA_KIND_INLINE);
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        VersionData::Overflow(oref) => {
            out.push(DATA_KIND_OVERFLOW);
            out.extend_from_slice(&oref.first_page().to_le_bytes());
            out.extend_from_slice(&oref.total_length().to_le_bytes());
        }
    }
    out
}

/// Deserialize a `VersionEntry` from the history-store value layout.
///
/// Overflow entries require an `allocator` so `OverflowRef::new_owned` can
/// bump the refcount. Passing `None` rehydrates overflow payloads as an
/// error — callers on a pure probe path that only need metadata (or that
/// reject overflow entries in tests) can opt out of the allocator bump.
pub(crate) fn decode_version_entry_value(
    bytes: &[u8],
    allocator: Option<&AllocatorHandle>,
) -> Result<VersionEntry> {
    if bytes.len() < 12 + 12 + 8 + 1 + 1 {
        return Err(Error::Internal(
            "history_store: value buffer truncated before fixed header".into(),
        ));
    }
    let start_ts = Ts::from_le_bytes(bytes[0..12].try_into().expect("12 bytes"));
    let stop_ts = Ts::from_le_bytes(bytes[12..24].try_into().expect("12 bytes"));
    let txn_id = u64::from_le_bytes(bytes[24..32].try_into().expect("8 bytes"));
    let is_tombstone = bytes[32] != 0;
    let data_kind = bytes[33];
    let data = match data_kind {
        DATA_KIND_INLINE => {
            if bytes.len() < 34 + 4 {
                return Err(Error::Internal(
                    "history_store: inline value missing length prefix".into(),
                ));
            }
            let len = u32::from_le_bytes(bytes[34..38].try_into().expect("4 bytes")) as usize;
            let start = 38usize;
            let end = start.checked_add(len).ok_or_else(|| {
                Error::Internal("history_store: inline length overflow".into())
            })?;
            if bytes.len() < end {
                return Err(Error::Internal(
                    "history_store: inline payload truncated".into(),
                ));
            }
            VersionData::Inline(bytes[start..end].to_vec())
        }
        DATA_KIND_OVERFLOW => {
            if bytes.len() < 34 + 4 + 8 {
                return Err(Error::Internal(
                    "history_store: overflow value truncated".into(),
                ));
            }
            let first_page =
                u32::from_le_bytes(bytes[34..38].try_into().expect("4 bytes"));
            let total_length =
                u64::from_le_bytes(bytes[38..46].try_into().expect("8 bytes"));
            let alloc = allocator.ok_or_else(|| {
                Error::Internal(
                    "history_store: overflow entry requires allocator handle".into(),
                )
            })?;
            VersionData::Overflow(OverflowRef::new_owned(
                first_page,
                total_length,
                alloc.clone(),
            )?)
        }
        _ => {
            return Err(Error::Internal(format!(
                "history_store: unknown data_kind {data_kind}"
            )));
        }
    };
    Ok(VersionEntry {
        start_ts,
        stop_ts,
        txn_id,
        data,
        is_tombstone,
    })
}

// ---------------------------------------------------------------------------
// HistoryStore
// ---------------------------------------------------------------------------

/// B-tree-backed history store.
///
/// Generic over [`BTreePageStore`] so the same type can run on the
/// production dedicated-pool adapter and on an in-memory store for tests.
/// The caller supplies the store — `BufferPool` partition isolation is
/// achieved by giving the caller's store its own
/// [`BufferPool`](super::buffer_pool::BufferPool) (lock position 1).
pub(crate) struct HistoryStore<S: BTreePageStore> {
    tree: BTree<S>,
    /// Optional allocator for rehydrating `VersionData::Overflow` probes.
    /// `None` on tests that never store overflow entries.
    overflow_allocator: Option<Arc<AllocatorHandle>>,
}

impl<S: BTreePageStore> HistoryStore<S> {
    /// Create a new history store over a freshly-built B-tree.
    pub(crate) fn create(store: S) -> Result<Self> {
        Ok(Self {
            tree: BTree::create(store)?,
            overflow_allocator: None,
        })
    }

    /// Open an existing history store at `root_page` / `root_level`.
    #[allow(dead_code)]
    pub(crate) fn open(store: S, root_page: u32, root_level: u8) -> Self {
        Self {
            tree: BTree::open(store, root_page, root_level),
            overflow_allocator: None,
        }
    }

    /// Attach an allocator handle for rehydrating overflow entries on probe.
    #[allow(dead_code)]
    pub(crate) fn with_overflow_allocator(mut self, allocator: Arc<AllocatorHandle>) -> Self {
        self.overflow_allocator = Some(allocator);
        self
    }

    /// Insert a version entry at `(ns, kind, key_bytes, entry.start_ts)`.
    #[cfg(test)]
    pub(crate) fn insert(
        &mut self,
        ns_id: u32,
        kind_tag: u8,
        key_bytes: &[u8],
        entry: &VersionEntry,
    ) -> Result<()> {
        let _guard = HistoryStoreGuard::enter();
        let key = encode_history_key(ns_id, kind_tag, key_bytes, entry.start_ts);
        let value = encode_version_entry_value(entry);
        self.tree.insert(&key, &value)
    }

    /// Probe for the newest entry with `start_ts <= read_ts` at
    /// `(ns, KIND_PRIMARY, doc_id)`.
    ///
    /// Returns `None` when no such entry exists.
    pub(crate) fn probe_primary(
        &self,
        ns_id: u32,
        doc_id: &[u8],
        read_ts: Ts,
    ) -> Result<Option<VersionEntry>> {
        self.probe(ns_id, KIND_PRIMARY, doc_id, read_ts, false)
    }

    /// Probe for the newest sec-index version with `start_ts <= read_ts`
    /// at `(ns, KIND_SEC_INDEX_BASE + ordinal, sec_key)`. A live tombstone
    /// hit causes the probe to return `None` and tick
    /// `secondary_index_tombstone_hits_total`.
    #[allow(dead_code)]
    pub(crate) fn probe_sec_index(
        &self,
        ns_id: u32,
        sec_key: &[u8],
        kind_tag: u8,
        read_ts: Ts,
    ) -> Result<Option<VersionEntry>> {
        self.probe(ns_id, kind_tag, sec_key, read_ts, true)
    }

    /// Inner shared probe. `skip_tombstones` toggles the sec-index
    /// "tombstone wins → hide" rule.
    fn probe(
        &self,
        ns_id: u32,
        kind_tag: u8,
        key_bytes: &[u8],
        read_ts: Ts,
        skip_tombstones: bool,
    ) -> Result<Option<VersionEntry>> {
        let _guard = HistoryStoreGuard::enter();
        let upper = probe_upper_bound(ns_id, kind_tag, key_bytes, read_ts);
        let prefix = probe_prefix(ns_id, kind_tag, key_bytes);

        // Range-scan ascending over the full prefix, truncated at `upper`.
        // Descending scans are not exposed on `BTree`; the scan is bounded
        // both above and below by the prefix so the candidate set is
        // small (one entry per start_ts for this key). The newest visible
        // entry is the last in the ascending list.
        let rows = self.tree.range_scan(Some(&prefix), Some(&upper))?;

        // `range_scan` with `end_key = upper` is inclusive (see btree.rs:
        // `if cell.key.as_slice() > ek { break }`). Any row whose key
        // starts with `prefix` and whose encoded start_ts <= read_ts is
        // a valid candidate.
        //
        // Walk the results in reverse so the newest entry wins. We still
        // pay an O(n) scan for `n` retained versions of this key — the
        // history store is expected to keep few per key because
        // reconciliation prunes aggressively.
        for (key, cell_value) in rows.into_iter().rev() {
            if !key.starts_with(&prefix) {
                continue;
            }
            let value_bytes = cell_value_bytes(cell_value)?;
            let entry =
                decode_version_entry_value(&value_bytes, self.overflow_allocator.as_deref())?;
            if entry.start_ts > read_ts {
                // Defensive — should not happen because `upper` clips the
                // scan, but the comparison is cheap and makes the rule
                // explicit.
                continue;
            }
            if skip_tombstones && entry.is_tombstone {
                crate::mvcc::metrics::record_secondary_index_tombstone_hit();
                return Ok(None);
            }
            return Ok(Some(entry));
        }
        Ok(None)
    }
}

/// Result of a [`HistoryStore::gc_pass`] sweep.
///
/// `entries_deleted` counts history-store B-tree cells removed. `pages_freed`
/// counts overflow-chain `first_page`s whose refcount dropped to zero as a
/// direct consequence of the sweep (thereby enqueued for deferred free by
/// the [`OverflowRef`] RAII `Drop`). Actual page reclamation runs on the
/// writer path via `AllocatorHandle::drain_free_queue`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GcResult {
    pub entries_deleted: u64,
    pub pages_freed: u64,
}

impl<S: BTreePageStore> HistoryStore<S> {
    /// Sweep history-store entries whose `stop_ts <= ort` (oldest required
    /// timestamp). Plan §T8: "`gc_pass(ort: Ts) -> GcResult { entries_deleted,
    /// pages_freed }`. Called from checkpoint hook in paged_engine."
    ///
    /// Each expired entry is deleted from the B-tree. For entries with
    /// `VersionData::Overflow`, the history entry's logical +1 refcount on
    /// `first_page` is transferred to an ephemeral [`OverflowRef`] via
    /// [`OverflowRef::from_existing_refcount`] (no bump) which then
    /// `Drop`s — decrementing the refcount and enqueueing the page for
    /// deferred free when the count reaches 0. All decrement accounting
    /// goes through RAII; this method never calls
    /// `AllocatorHandle::decref_overflow` directly.
    ///
    /// Ticks `mvcc.history_store.gc_passes_total` on every invocation. Does
    /// not tick `mvcc.reconcile.entries_dropped_total` — that counter
    /// measures reconciliation drops, not GC-pass deletes.
    ///
    /// Never deletes entries with `stop_ts == Ts::MAX` (live heads) nor
    /// entries with `stop_ts > ort` (still visible to some live reader).
    pub(crate) fn gc_pass(&mut self, ort: Ts) -> Result<GcResult> {
        let _guard = HistoryStoreGuard::enter();

        // Phase 1: scan, identifying victims. Full-tree range_scan is
        // acceptable in v1 — the history store is expected to be sparse
        // relative to main data, and GC runs at checkpoint cadence.
        let rows = self.tree.range_scan(None, None)?;
        type Victim = (Vec<u8>, Option<(u32, u64)>);
        let mut victims: Vec<Victim> = Vec::new();
        for (key, cell_value) in rows {
            let value_bytes = cell_value_bytes(cell_value)?;
            if value_bytes.len() < 34 {
                continue;
            }
            let stop_ts =
                Ts::from_le_bytes(value_bytes[12..24].try_into().expect("12 bytes"));
            if stop_ts == Ts::MAX || stop_ts > ort {
                continue;
            }
            let data_kind = value_bytes[33];
            let overflow = if data_kind == DATA_KIND_OVERFLOW {
                if value_bytes.len() < 46 {
                    continue;
                }
                let first_page =
                    u32::from_le_bytes(value_bytes[34..38].try_into().expect("4 bytes"));
                let total_length =
                    u64::from_le_bytes(value_bytes[38..46].try_into().expect("8 bytes"));
                Some((first_page, total_length))
            } else {
                None
            };
            victims.push((key, overflow));
        }

        // Phase 2: delete each victim and, for overflow entries, transfer
        // the logical +1 refcount into an ephemeral OverflowRef and drop it.
        let mut result = GcResult::default();
        for (key, overflow) in victims {
            if !self.tree.delete(&key)? {
                continue;
            }
            result.entries_deleted += 1;
            if let Some((first_page, total_length)) = overflow {
                if let Some(alloc) = self.overflow_allocator.as_deref() {
                    {
                        let _oref = OverflowRef::from_existing_refcount(
                            first_page,
                            total_length,
                            alloc.clone(),
                        );
                        // `_oref` drops at the end of this scope; Drop runs
                        // `decref_overflow` and enqueues for deferred free
                        // on refcount 0.
                    }
                    if alloc.overflow_refcount(first_page) == 0 {
                        result.pages_freed += 1;
                    }
                }
            }
        }

        crate::mvcc::metrics::record_history_store_gc_pass();
        Ok(result)
    }
}

/// Resolve a `CellValue` from `BTree::range_scan` into the raw bytes that
/// were stored at insert time.
fn cell_value_bytes(
    value: crate::storage::btree::CellValue,
) -> Result<Vec<u8>> {
    use crate::storage::btree::CellValue;
    match value {
        CellValue::Inline(bytes) => Ok(bytes),
        CellValue::Overflow { .. } => Err(Error::Internal(
            "history_store: value spilled to overflow — not supported in v1".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
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
    // GC pass — plan §T8 acceptance bullets
    // -----------------------------------------------------------------------

    /// Given 10k entries with `stop_ts` spread across [1, 10_000], and
    /// `ort = Ts{3000, 0}`: exactly 3000 entries are deleted. Plan T8
    /// acceptance bullet 1 (inline variant; no overflow required to prove
    /// the delete-count invariant).
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
        // refcount "owned by the history entry" convention (plan §T7 bullet
        // 909). This matches what a real reconciliation-path insert would
        // have done.
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
}
