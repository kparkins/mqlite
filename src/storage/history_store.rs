//! History store — kind-tagged B-tree of aged MVCC version entries.
//!
//! Lives on a **dedicated buffer-pool partition** to prevent recursive
//! eviction: reconciliation evicting a main-data leaf page can install an
//! aged `VersionEntry` here without re-entering the main-data partition
//! mutex, because the history store's B-tree pins pages in its own
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
//!   and tick `secondary_index_tombstone_hits_total`.
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
//! caller.

use std::cell::Cell;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::{OverflowRef, VersionData, VersionEntry};
use crate::storage::allocator::AllocatorHandle;
use crate::storage::btree::{BTree, BTreePageStore};

// ---------------------------------------------------------------------------
// Thread-local non-recursion sentinel
// ---------------------------------------------------------------------------
//
// Guards against wiring mistakes where a history-store probe is accidentally
// routed through the main pool's `BufferPoolPageSource`. The invariant is
// enforced structurally by giving `HistoryStore` its own dedicated
// [`BufferPool`] partition, but a runtime sentinel catches future mistakes
// at runtime. Every public `HistoryStore` entry point increments the depth;
// the main pool's `fetch_page` `debug_assert!`s the depth is zero.
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

fn ts_from_le_slice(bytes: &[u8]) -> Ts {
    let mut out = [0u8; 12];
    out.copy_from_slice(bytes);
    Ts::from_le_bytes(out)
}

fn u32_from_le_slice(bytes: &[u8]) -> u32 {
    let mut out = [0u8; 4];
    out.copy_from_slice(bytes);
    u32::from_le_bytes(out)
}

fn u64_from_le_slice(bytes: &[u8]) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(bytes);
    u64::from_le_bytes(out)
}

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
    let start_ts = ts_from_le_slice(&bytes[0..12]);
    let stop_ts = ts_from_le_slice(&bytes[12..24]);
    let txn_id = u64_from_le_slice(&bytes[24..32]);
    let is_tombstone = bytes[32] != 0;
    let data_kind = bytes[33];
    let data = match data_kind {
        DATA_KIND_INLINE => {
            if bytes.len() < 34 + 4 {
                return Err(Error::Internal(
                    "history_store: inline value missing length prefix".into(),
                ));
            }
            let len = u32_from_le_slice(&bytes[34..38]) as usize;
            let start = 38usize;
            let end = start
                .checked_add(len)
                .ok_or_else(|| Error::Internal("history_store: inline length overflow".into()))?;
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
            let first_page = u32_from_le_slice(&bytes[34..38]);
            let total_length = u64_from_le_slice(&bytes[38..46]);
            let alloc = allocator.ok_or_else(|| {
                Error::Internal("history_store: overflow entry requires allocator handle".into())
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
    ///
    /// Phase 1 uses [`HistoryStore::create_empty_root`] at open time;
    /// this raw constructor stays as part of the API surface for
    /// Phase 4 (§8.11) history repopulation.
    #[allow(dead_code)]
    pub(crate) fn create(store: S) -> Result<Self> {
        Ok(Self {
            tree: BTree::create(store)?,
            overflow_allocator: None,
        })
    }

    /// Phase 1 §10.7 — allocate a fresh empty root page in `store` and
    /// return the persisted page id. Used by open-time bootstrap when the
    /// file header's `history_store_root_page` is `0` (fresh DB). The
    /// caller writes the returned page id into `FileHeader::history_store_root_page`
    /// atomically with the rest of open-time initialization, so reopen
    /// can call [`HistoryStore::open`] with a valid page id.
    pub(crate) fn create_empty_root(store: S) -> Result<(Self, u32)> {
        let tree = BTree::create(store)?;
        let root_page = tree.root_page;
        Ok((
            Self {
                tree,
                overflow_allocator: None,
            },
            root_page,
        ))
    }

    /// Open an existing history store at `root_page` / `root_level`.
    ///
    /// Phase 1 uses [`HistoryStore::create_empty_root`] at open time;
    /// Phase 4 (§8.11) wires this constructor for cross-lifetime
    /// history repopulation.
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
    /// timestamp). Called from the checkpoint hook in `paged_engine`.
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

        // Scan, identifying victims. Full-tree range_scan is acceptable
        // because the history store is sparse relative to main data, and
        // GC runs at checkpoint cadence.
        let rows = self.tree.range_scan(None, None)?;
        type Victim = (Vec<u8>, Option<(u32, u64)>);
        let mut victims: Vec<Victim> = Vec::with_capacity(rows.len());
        for (key, cell_value) in rows {
            let value_bytes = cell_value_bytes(cell_value)?;
            if value_bytes.len() < 34 {
                continue;
            }
            let stop_ts = ts_from_le_slice(&value_bytes[12..24]);
            if stop_ts == Ts::MAX || stop_ts > ort {
                continue;
            }
            let data_kind = value_bytes[33];
            let overflow = if data_kind == DATA_KIND_OVERFLOW {
                if value_bytes.len() < 46 {
                    continue;
                }
                let first_page = u32_from_le_slice(&value_bytes[34..38]);
                let total_length = u64_from_le_slice(&value_bytes[38..46]);
                Some((first_page, total_length))
            } else {
                None
            };
            victims.push((key, overflow));
        }

        // Delete each victim and, for overflow entries, transfer the
        // logical +1 refcount into an ephemeral OverflowRef and drop it.
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
fn cell_value_bytes(value: crate::storage::btree::CellValue) -> Result<Vec<u8>> {
    use crate::storage::btree::CellValue;
    match value {
        CellValue::Inline(bytes) => Ok(bytes),
        CellValue::Overflow { .. } => Err(Error::Internal(
            "history_store: value spilled to overflow — not supported in v1".into(),
        )),
    }
}

#[cfg(test)]
#[path = "history_store_tests.rs"]
mod tests;
