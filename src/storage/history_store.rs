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
//! ## Key schema (Phase 4 — Format Lock)
//!
//! ```text
//! key = collection_id(i64 BE)
//!     | tree_kind(u8)
//!     | index_id(i64 BE)
//!     | key_len(u32 BE)
//!     | key_bytes
//!     | start_ts(Ts BE 12B)
//!     | counter(u32 BE)
//! ```
//!
//! * `collection_id` — durable collection identifier. Big-endian so
//!   lexicographic sort matches numeric sort for prefix scans.
//! * `tree_kind` — [`HISTORY_TREE_KIND_PRIMARY`] (`0x00`) for primary data
//!   trees or [`HISTORY_TREE_KIND_SECONDARY`] (`0x01`) for secondary indexes.
//! * `index_id` — durable secondary index id, or `0` for primary data trees.
//! * `key_len` — length delimiter for `key_bytes`, preventing prefix aliasing.
//! * `key_bytes` — for primary: document id; for sec-index: compound key.
//! * `start_ts` — [`Ts::to_be_bytes`] so chronological order equals
//!   lexicographic order.
//! * `counter` — stable duplicate disambiguator for spills with the same
//!   `(TreeIdent, key, start_ts)`.
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
//! Overflow rehydration requires a caller-supplied allocator handle.

use std::cell::Cell;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::{OverflowRef, VersionData, VersionEntry};
// Re-exported for the `#[path]`-included test modules that build `VersionEntry`
// values via `use super::*` (the body no longer constructs `VersionState`
// directly — that moved into `codec::decode_version_entry_value`).
#[cfg(test)]
use crate::mvcc::version::VersionState;
use crate::storage::allocator::AllocatorHandle;
use crate::storage::btree::{BTree, BTreePageStore};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::reconcile::driver::{TreeIdent, TreeKind};

mod codec;

// Re-export the codec surface at the historical `history_store::` paths so
// existing callers and the `#[path]`-included test modules (`use super::*`)
// resolve every key/value codec symbol unchanged after the extraction.
pub(crate) use codec::{
    decode_version_entry_value, encode_history_key, encode_version_entry_value,
};
// `decode_history_key` and the tree-kind tags are only read by the
// `#[path]`-included test modules via `use super::*`; gate the re-exports so
// non-test builds do not warn on an unused import.
#[cfg(test)]
pub(crate) use codec::{decode_history_key, HISTORY_TREE_KIND_PRIMARY, HISTORY_TREE_KIND_SECONDARY};
use codec::{
    decode_overflow_payload, decode_value_header, probe_prefix, DATA_KIND_OVERFLOW,
};

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

/// In-memory batch of history-store writes for one folded-leaf install.
///
/// Reconciliation stages old committed versions here first, then commits the
/// batch to the history-store B-tree before removing those versions from the
/// main leaf. This makes the history-before-leaf ordering explicit at the
/// call boundary.
#[derive(Debug, Default)]
pub(crate) struct HistorySpillTxn {
    staged: Vec<StagedHistorySpill>,
}

impl HistorySpillTxn {
    /// Create an empty history spill transaction.
    pub(crate) fn new() -> Self {
        Self { staged: Vec::new() }
    }

    /// Return the number of staged history writes.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.staged.len()
    }

    /// Return true when the transaction has no staged writes.
    pub(crate) fn is_empty(&self) -> bool {
        self.staged.is_empty()
    }

    fn stage(
        &mut self,
        ident: TreeIdent,
        key_bytes: &[u8],
        entry: &VersionEntry,
        counter: u32,
    ) -> Result<()> {
        let entry = entry.try_clone().ok_or_else(|| {
            Error::Internal("history_store: overflow ref dropped before history spill".into())
        })?;
        self.staged.push(StagedHistorySpill {
            ident,
            key_bytes: key_bytes.to_vec(),
            entry,
            counter,
        });
        Ok(())
    }
}

#[derive(Debug)]
struct StagedHistorySpill {
    ident: TreeIdent,
    key_bytes: Vec<u8>,
    entry: VersionEntry,
    counter: u32,
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
    #[cfg(test)]
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
    pub(crate) fn open(store: S, root_page: u32, root_level: u8) -> Self {
        Self {
            tree: BTree::open(store, root_page, root_level),
            overflow_allocator: None,
        }
    }

    /// Attach an allocator handle for rehydrating overflow entries on probe.
    pub(crate) fn with_overflow_allocator(mut self, allocator: Arc<AllocatorHandle>) -> Self {
        self.overflow_allocator = Some(allocator);
        self
    }

    /// Stage a primary-tree version entry in a history spill transaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if `ident` does not identify a primary
    /// tree.
    pub(crate) fn spill_primary(
        txn: &mut HistorySpillTxn,
        ident: TreeIdent,
        key_bytes: &[u8],
        entry: &VersionEntry,
        counter: u32,
    ) -> Result<()> {
        if !matches!(&ident.kind, TreeKind::Primary) {
            return Err(Error::Internal(
                "history_store: primary spill requires primary tree identity".into(),
            ));
        }
        txn.stage(ident, key_bytes, entry, counter)
    }

    /// Stage a secondary-index version entry in a history spill transaction.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if `ident` does not identify a secondary
    /// index tree.
    pub(crate) fn spill_sec_index(
        txn: &mut HistorySpillTxn,
        ident: TreeIdent,
        key_bytes: &[u8],
        entry: &VersionEntry,
        counter: u32,
    ) -> Result<()> {
        if !matches!(&ident.kind, TreeKind::Secondary { .. }) {
            return Err(Error::Internal(
                "history_store: secondary spill requires secondary tree identity".into(),
            ));
        }
        txn.stage(ident, key_bytes, entry, counter)
    }

    /// Commit a staged history spill batch into the history-store B-tree.
    ///
    /// # Errors
    ///
    /// Returns any B-tree insert or duplicate-key error encountered while
    /// applying the staged writes.
    pub(crate) fn commit_spill_txn(&mut self, txn: HistorySpillTxn) -> Result<()> {
        if txn.is_empty() {
            return Ok(());
        }
        let _guard = HistoryStoreGuard::enter();
        for write in txn.staged {
            let StagedHistorySpill {
                ident,
                key_bytes,
                entry,
                counter,
            } = write;
            let inserted = self.apply_spill(&ident, &key_bytes, &entry, counter)?;
            if inserted {
                forget_history_record_overflow_ref(entry);
            }
        }
        Ok(())
    }

    fn apply_spill(
        &mut self,
        ident: &TreeIdent,
        key_bytes: &[u8],
        entry: &VersionEntry,
        counter: u32,
    ) -> Result<bool> {
        let key = encode_history_key(ident, key_bytes, entry.start_ts, counter);
        let value = encode_version_entry_value(entry);
        if let Some(existing) = self.tree.get(&key)? {
            if existing == value {
                return Ok(false);
            }
            return Err(Error::DuplicateKey {
                detail: "history_store: duplicate spill key has different value bytes".into(),
            });
        }
        self.tree.insert(&key, &value)?;
        Ok(true)
    }

    /// Probe for the newest entry with `start_ts <= read_ts` at
    /// `(collection_id, primary tree, doc_id)`.
    ///
    /// Returns `None` when no such entry exists.
    pub(crate) fn probe_primary(
        &self,
        collection_id: i64,
        doc_id: &[u8],
        read_ts: Ts,
    ) -> Result<Option<VersionEntry>> {
        let ident = TreeIdent {
            collection_id,
            kind: TreeKind::Primary,
        };
        self.probe_visible_entry(&ident, doc_id, read_ts, false)
    }

    /// Probe for the newest sec-index version with `start_ts <= read_ts`
    /// at `(collection_id, secondary index_id, sec_key)`. A live tombstone hit
    /// causes the probe to return `None` and tick
    /// `secondary_index_tombstone_hits_total`.
    pub(crate) fn probe_sec_index(
        &self,
        collection_id: i64,
        index_id: i64,
        sec_key: &[u8],
        read_ts: Ts,
    ) -> Result<Option<VersionEntry>> {
        let ident = TreeIdent {
            collection_id,
            kind: TreeKind::Secondary { index_id },
        };
        self.probe_visible_entry(&ident, sec_key, read_ts, true)
    }

    /// Inner shared probe. `skip_tombstones` toggles the sec-index
    /// "tombstone wins → hide" rule.
    fn probe_visible_entry(
        &self,
        ident: &TreeIdent,
        key_bytes: &[u8],
        read_ts: Ts,
        skip_tombstones: bool,
    ) -> Result<Option<VersionEntry>> {
        let _guard = HistoryStoreGuard::enter();
        let prefix = probe_prefix(ident, key_bytes);
        let mut upper = prefix.clone();
        upper.extend_from_slice(&read_ts.to_be_bytes());
        upper.extend_from_slice(&u32::MAX.to_be_bytes());

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
            if entry.start_ts > read_ts || read_ts >= entry.stop_ts {
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

impl HistoryStore<BufferPoolPageStore> {
    /// Commit staged history writes durably before the caller installs the
    /// matching folded leaf.
    ///
    /// The history B-tree root is persisted to the file header after the batch
    /// is applied because inserting the staged versions can split the history
    /// root. The subsequent handle flush stamps the spill's `Unflushable`
    /// history frames at the current journal-durable LSN and writes the
    /// history pool to the backing store before the header write is issued,
    /// so when this returns the aged versions reach the backing store ahead
    /// of both the header write that references them and the folded main leaf
    /// the caller installs next (history-before-leaf WAL ordering). That
    /// ordering is OS-page-cache write order only — no fsync runs between the
    /// history pass and the header write; the checkpoint boundary-record
    /// fsync is the durable barrier.
    ///
    /// # Errors
    ///
    /// Returns any B-tree, header-update, or flush error encountered while
    /// durably applying the staged batch.
    pub(crate) fn commit_spill_txn_durable(&mut self, txn: HistorySpillTxn) -> Result<()> {
        if txn.is_empty() {
            return Ok(());
        }
        let handle = Arc::clone(self.tree.store.handle());
        self.commit_spill_txn(txn)?;
        let root_page = self.tree.root_page;
        let root_level = self.tree.root_level;
        handle.allocator().update_header(|h| {
            h.history_store_root_page = root_page;
            h.history_store_root_level = root_level;
        })?;
        handle.flush()
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
            // Reuse the codec's fixed-header decode rather than reparsing raw
            // offsets here — keeps GC's `stop_ts`/`data_kind` view byte-for-byte
            // identical to `decode_version_entry_value`. A short buffer that the
            // full decoder would reject is skipped (matches the prior
            // `len < 34` / `len < 46` `continue` guards).
            let Ok(header) = decode_value_header(&value_bytes) else {
                continue;
            };
            if header.stop_ts == Ts::MAX || header.stop_ts > ort {
                continue;
            }
            let overflow = if header.data_kind == DATA_KIND_OVERFLOW {
                match decode_overflow_payload(&value_bytes) {
                    Ok(payload) => Some(payload),
                    Err(_) => continue,
                }
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
                    drop(OverflowRef::from_existing_refcount(
                        first_page,
                        total_length,
                        alloc.clone(),
                    ));
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

/// Leak the producer's `OverflowRef` after a history record is durably
/// inserted, so the chain's "persisted history record owns +1 refcount"
/// charge stays attached to the persisted cell rather than the transient
/// in-flight handle.
///
/// The full persisted-history refcount lifecycle (spill bump → persisted +1 →
/// GC decref via `OverflowRef::Drop`) now lives with the refcount machinery in
/// `crate::storage::allocator::overflow`; see that module's
/// "Persisted-history refcount lifecycle" note.
fn forget_history_record_overflow_ref(entry: VersionEntry) {
    if let VersionData::Overflow(oref) = entry.data {
        std::mem::forget(oref);
    }
}

#[cfg(test)]
#[path = "tests/history_store_behavior.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/history_store_key_schema.rs"]
mod history_store_key_schema;

#[cfg(test)]
#[path = "tests/history_store_transaction_stage.rs"]
mod history_store_transaction_stage;

#[cfg(test)]
#[path = "tests/history_store_visibility_probe.rs"]
mod history_store_visibility_probe;

#[cfg(test)]
#[path = "tests/history_store_overflow_transfer.rs"]
mod history_store_overflow_transfer;

#[cfg(test)]
#[path = "tests/history_store_spill_durability.rs"]
mod history_store_spill_durability;
