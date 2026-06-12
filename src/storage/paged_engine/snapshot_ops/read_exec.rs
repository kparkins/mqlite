//! Snapshot-based read helpers — mutex-free read path.
//!
//! WiredTiger semantics: every function here serves reads from an
//! already-published `NamespaceSnapshot`/`PublishedEpoch` without taking the
//! engine metadata lock, mirroring WiredTiger's lock-free cursor reads over a
//! stable snapshot. The only synchronization is the per-read-view registry
//! handshake in [`open_snapshot_read_view`].

use std::cell::Cell;
use std::ops::Bound;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::keys::{encode_compound_key, encode_key, COMPOUND_SEP};
use crate::mvcc::read_view::ReadView;
use crate::mvcc::registry::ReadViewRegistry;
use crate::mvcc::timestamp::Ts;
use crate::options::FindOptions;
use crate::query::eval_filter;
use crate::query::planner::{
    select_plan, IndexCondition, IndexMeta, PrimaryKeyCondition, ScanPlan,
};
use crate::storage::btree::{BTree, BTreePageStore, CellValue, HistoryProbe};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::IndexState;
use crate::storage::history_store::HistoryStore;
use crate::storage::root_snapshot::{NamespaceSnapshot, PublishedEpoch, PublishedIndex};

use super::super::btree_ops::btree_collscan;
use super::super::doc_helpers::{apply_projection_to_doc, compare_docs};
use super::super::index_maint::{index_bounds_free, index_entry_id_free};
use super::super::state::SharedState;

type SnapshotPairs = Vec<(Vec<u8>, Document)>;
type PlannedSnapshotPairs = (ScanPlan, SnapshotPairs);

struct SnapshotIndexScan<'a> {
    ready_indexes: &'a [&'a PublishedIndex],
    filter: &'a Document,
    view: &'a ReadView,
    index_name: &'a str,
    primary_field: &'a str,
    condition: &'a IndexCondition,
    match_limit: Option<usize>,
}

/// RAII guard for the conservative registry pin taken BEFORE the epoch load
/// (ITEM 1, option a). If any early-return path runs between the pin and the
/// point where the constructed `ReadView` takes ownership of the slot's
/// unregistration (its `Drop`), this guard removes the orphaned `txn_id`
/// slot so a momentary pin does not leak into a permanent `Ts::default()`
/// floor that wedges reclamation forever.
struct ConservativePinGuard<'a> {
    registry: &'a ReadViewRegistry,
    txn_id: u64,
    armed: bool,
}

impl ConservativePinGuard<'_> {
    /// Disarm once the `ReadView` owns the slot — its `Drop` now unregisters.
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for ConservativePinGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.registry.unregister(self.txn_id);
        }
    }
}

/// Open a registry-tracked snapshot `ReadView`, taking the conservative
/// registry pin BEFORE loading the published epoch (ITEM 1, option a).
///
/// Ordering invariant — the conservative pin precedes the epoch load. The
/// CRUD-reconcile / eviction prune (`partition::reconcile_frame_at`) drops
/// resident superseded committed versions whose
/// `stop_ts <= ReadViewRegistry::oldest_required_ts()` WITHOUT spilling them
/// to the history store, so the floor must be a true lower bound on every
/// live reader's `read_ts`. The earlier "conservative-then-refine" shape
/// still loaded the epoch in the caller and registered AFTER, leaving the
/// dangerous window (caller loads epoch → register) open: a prune in that
/// window computes a floor above this reader's `read_ts` and drops a version
/// it still needs (F36's `catalog_generation` recheck does not catch it —
/// a CRUD commit does not bump `catalog_generation`).
///
/// This function closes the window by structure rather than by a recheck:
///
/// 1. Allocate `txn_id` and REGISTER it at the conservative floor
///    `Ts::default()` (the lowest possible `read_ts`) with a placeholder
///    `Weak::new()` — pinning the entire reclaim horizon — guarded so any
///    early error unregisters the slot.
/// 2. Load the published epoch HERE (coherent (epoch, frontier) pair,
///    §10.19 C-1 / US-037). Because the pin already happened, no prune can
///    run between the pin and this load that drops any version visible at
///    `ts >= Ts::default()` — i.e. any version this reader could need.
/// 3. Compute `read_ts` from the loaded epoch, build the view, publish the
///    real `Weak` back-pointer, and refine the pinned slot up to `read_ts`.
/// 4. F36 catalog-generation recheck (see [`open_snapshot_read_view_for_epoch`]).
///
/// Hot-path cost is unchanged from the prior conservative-then-refine shape:
/// the pin only MOVED earlier. The open still performs exactly two registry
/// ops (the conservative `register` + the `attach_view_and_refine`), one
/// epoch load for the view, and one extra `ArcSwap` load + `u64` compare for
/// the F36 generation check.
///
/// # Errors
///
/// Returns [`Error::ReadViewExpired`] when a DDL published a new catalog
/// generation between this function's coherent epoch load and the
/// post-`attach_view_and_refine` generation re-read (F36).
pub(in crate::storage::paged_engine) fn open_snapshot_read_view(
    shared: &SharedState,
) -> Result<Arc<ReadView>> {
    let registry = shared.handle.read_view_registry();
    let txn_id = shared.txn_counter.fetch_add(1, Ordering::Relaxed);

    // Step 1: conservative pin BEFORE the load. From this insert onward the
    // reader is a member of every `oldest_required_ts()` snapshot at the
    // lowest floor, so no concurrent reconcile/eviction prune can drop a
    // resident superseded version visible at any `ts >= Ts::default()`.
    registry.register(txn_id, Ts::default(), std::sync::Weak::new());
    let pin_guard = ConservativePinGuard {
        registry,
        txn_id,
        armed: true,
    };

    // Step 2: load the published epoch. The cfg-gated rendezvous lets the
    // prune-race test pause a reader exactly here — pinned, not yet loaded —
    // and prove the reclaim floor is already `Ts::default()`.
    #[cfg(any(test, feature = "test-hooks"))]
    super::super::hidden_accessors::read_view_pin_before_epoch_load_if_installed(shared);
    let epoch = shared.load_published_coherent();

    // Steps 3-4: build the view over the pinned slot, refine, F36 recheck.
    open_snapshot_read_view_with_pin(shared, epoch, txn_id, pin_guard)
}

/// Open a registry-tracked snapshot `ReadView` over an already-loaded,
/// caller-supplied published epoch.
///
/// This is the variant for callers that have ALREADY chosen a specific epoch
/// — notably the F36 / drop-namespace stale-epoch regression tests that
/// deliberately open a view against a captured pre-drop epoch to exercise the
/// `catalog_generation` recheck. Unlike [`open_snapshot_read_view`], the
/// caller owns the load-to-register window for the supplied epoch; this
/// function still takes the conservative pin FIRST (before touching the
/// caller's epoch) so the prune dimension is closed for the registration
/// itself, then applies the F36 recheck against the caller's captured
/// generation.
///
/// F36 — post-registration revalidation closes the caller's
/// load-to-register window against the DDL (`catalog_generation`) dimension:
/// after registering, ONE extra published-epoch load compares
/// `catalog_generation` against the captured epoch:
///
/// - Equal ⇒ safe. Any drop whose retired drain could have missed this
///   registration must have published BEFORE the post-registration load
///   (drop publish → retire enqueue → queue-mutex → drain → registry-mutex
///   floor read ordered before our `register` ⇒ publish happens-before our
///   load), so its bumped `catalog_generation` would be visible — equality
///   proves every published drop's epoch is ≤ the captured epoch, whose
///   catalog therefore cannot route to any retired tree.
/// - Mismatch ⇒ a DDL published inside the window; a retired drain may
///   already have run without seeing this reader. Fail the open cleanly
///   with `ReadViewExpired` (dropping the view unregisters it); the caller
///   retries on a freshly loaded epoch.
///
/// # Errors
///
/// Returns [`Error::ReadViewExpired`] when a DDL published a new catalog
/// generation between the caller's epoch capture and the view's
/// registration.
#[cfg(test)]
pub(in crate::storage::paged_engine) fn open_snapshot_read_view_for_epoch(
    shared: &SharedState,
    epoch: Arc<PublishedEpoch>,
) -> Result<Arc<ReadView>> {
    let registry = shared.handle.read_view_registry();
    let txn_id = shared.txn_counter.fetch_add(1, Ordering::Relaxed);

    // §10.19 C-1 / US-037: the caller already loaded `epoch`, so only spin
    // on the frontier side until it catches up.
    while shared
        .publish_sequencer
        .published_frontier
        .load(Ordering::Acquire)
        < epoch.visible_ts
    {
        std::hint::spin_loop();
    }

    // Conservative pin BEFORE building the view (same prune-dimension fix).
    registry.register(txn_id, Ts::default(), std::sync::Weak::new());
    let pin_guard = ConservativePinGuard {
        registry,
        txn_id,
        armed: true,
    };
    open_snapshot_read_view_with_pin(shared, epoch, txn_id, pin_guard)
}

/// Shared tail of the two open paths: build the view over `epoch` and the
/// already-pinned `txn_id` slot, hand slot ownership to the view, then run
/// the F36 catalog-generation recheck. `pin_guard` is disarmed once the view
/// owns the slot; on the F36-mismatch early return the view's `Drop`
/// unregisters the slot (the guard is already disarmed).
fn open_snapshot_read_view_with_pin(
    shared: &SharedState,
    epoch: Arc<PublishedEpoch>,
    txn_id: u64,
    pin_guard: ConservativePinGuard<'_>,
) -> Result<Arc<ReadView>> {
    let captured_generation = epoch.catalog_generation;
    let view = ReadView::open_for_epoch_conservative_then_refine(
        Arc::clone(shared.handle.read_view_registry()),
        epoch,
        txn_id,
        Arc::clone(&shared.publish_sequencer),
    );
    // The view now owns the slot's unregistration via its Drop.
    pin_guard.disarm();

    // F36: post-registration revalidation. On mismatch, returning Err drops
    // `view`, whose Drop unregisters the slot.
    if shared.published.load().catalog_generation != captured_generation {
        return Err(Error::ReadViewExpired);
    }
    Ok(view)
}

pub(in crate::storage::paged_engine) fn primary_history_probe<'a>(
    shared: &'a SharedState,
    collection_id: i64,
) -> PrimaryHistoryProbe<'a, BufferPoolPageStore> {
    PrimaryHistoryProbe {
        store: &shared.history_store,
        collection_id,
    }
}

pub(in crate::storage::paged_engine) fn fetch_primary_pair(
    tree: &BTree<BufferPoolPageStore>,
    key: Vec<u8>,
    filter: &Document,
    view: &ReadView,
    history: Option<&dyn HistoryProbe>,
) -> Result<Option<(Vec<u8>, Document)>> {
    let Some(bson_bytes) = tree.get_mvcc(&key, view, history)? else {
        return Ok(None);
    };
    let doc: Document = bson::from_slice(&bson_bytes).map_err(Error::BsonDeserialization)?;
    if eval_filter(&doc, filter)? {
        Ok(Some((key, doc)))
    } else {
        Ok(None)
    }
}

fn execute_primary_key_lookup_from_snap(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    view: &ReadView,
    condition: &PrimaryKeyCondition,
    match_limit: Option<usize>,
) -> Result<SnapshotPairs> {
    let store = BufferPoolPageStore::new(Arc::clone(&shared.handle));
    let tree = BTree::open(store, ns_snap.data_root_page, ns_snap.data_root_level);
    let probe = primary_history_probe(shared, ns_snap.id);

    match condition {
        PrimaryKeyCondition::Eq(id) => {
            let key = encode_key(id);
            Ok(fetch_primary_pair(&tree, key, filter, view, Some(&probe))?
                .into_iter()
                .collect())
        }
        PrimaryKeyCondition::In(vals) => {
            let mut keys: Vec<Vec<u8>> = vals.iter().map(encode_key).collect();
            keys.sort();
            keys.dedup();
            let mut matched = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some(pair) = fetch_primary_pair(&tree, key, filter, view, Some(&probe))? {
                    matched.push(pair);
                    if match_limit.is_some_and(|limit| matched.len() >= limit) {
                        break;
                    }
                }
            }
            Ok(matched)
        }
    }
}

/// Bind the primary-key probe path of a [`HistoryStore`] to a fixed
/// `(collection_id, primary tree)` so the BTree layer sees a key-only probe.
pub(in crate::storage::paged_engine) struct PrimaryHistoryProbe<'a, S: BTreePageStore> {
    store: &'a std::sync::Mutex<HistoryStore<S>>,
    collection_id: i64,
}

impl<S: BTreePageStore> crate::storage::btree::HistoryProbe for PrimaryHistoryProbe<'_, S> {
    fn probe_visible_version(
        &self,
        key: &[u8],
        read_ts: crate::mvcc::timestamp::Ts,
    ) -> Result<Option<crate::mvcc::version::VersionEntry>> {
        let guard = self
            .store
            .lock()
            .map_err(|_| Error::Internal("history_store mutex poisoned".into()))?;
        guard.probe_primary(self.collection_id, key, read_ts)
    }
}

/// Apply sort/skip/limit/projection to a list of matched documents.
pub(in crate::storage::paged_engine) fn apply_find_opts(
    mut docs: Vec<Document>,
    opts: &FindOptions,
) -> Vec<Document> {
    if let Some(s) = &opts.sort {
        docs.sort_by(|a, b| compare_docs(a, b, s));
    }
    if let Some(skip) = opts.skip {
        let n = skip as usize;
        if n >= docs.len() {
            docs.clear();
        } else {
            docs.drain(..n);
        }
    }
    if let Some(limit) = opts.limit {
        if limit > 0 {
            docs.truncate(limit as usize);
        }
    }
    if let Some(proj) = &opts.projection {
        for d in docs.iter_mut() {
            *d = apply_projection_to_doc(std::mem::take(d), proj);
        }
    }
    docs
}

/// Index scan using a published `NamespaceSnapshot` instead of the catalog.
fn execute_index_scan_from_snap(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    scan: SnapshotIndexScan<'_>,
) -> Result<Vec<(Vec<u8>, Document)>> {
    let idx_snap = scan
        .ready_indexes
        .iter()
        .find(|i| i.name == scan.index_name)
        .ok_or_else(|| Error::Internal(format!("index '{}' not in snapshot", scan.index_name)))?;

    let ascending = idx_snap
        .key_pattern
        .get(scan.primary_field)
        .map(|v| !matches!(v, Bson::Int32(-1) | Bson::Int64(-1)))
        .unwrap_or(true);

    let handle = Arc::clone(&shared.handle);
    let data_store = BufferPoolPageStore::new(Arc::clone(&handle));
    let data_tree = BTree::open(data_store, ns_snap.data_root_page, ns_snap.data_root_level);
    let probe = primary_history_probe(shared, ns_snap.id);
    let mut docs = Vec::new();
    let limit_reached = Cell::new(false);
    let mut fetch_index_entry = |entry_bytes: Vec<u8>| -> Result<bool> {
        let id_bson =
            index_entry_id_free(&handle, CellValue::Inline(entry_bytes))?.ok_or_else(|| {
                Error::Internal("index entry has empty or corrupt _id payload".into())
            })?;
        let data_key = encode_key(&id_bson);
        if let Some(pair) =
            fetch_primary_pair(&data_tree, data_key, scan.filter, scan.view, Some(&probe))?
        {
            docs.push(pair);
            if scan.match_limit.is_some_and(|limit| docs.len() >= limit) {
                limit_reached.set(true);
                return Ok(false);
            }
        }
        Ok(true)
    };

    if let IndexCondition::In(vals) = scan.condition {
        for v in vals {
            let mut p = encode_compound_key(&[(v, ascending)]);
            p.push(COMPOUND_SEP);
            let mut p_next = p.clone();
            if let Some(last) = p_next.last_mut() {
                *last += 1;
            }
            let idx_store = BufferPoolPageStore::new(Arc::clone(&handle));
            let idx_tree = BTree::open(idx_store, idx_snap.root_page, idx_snap.root_level);
            idx_tree.try_for_each_range_scan_mvcc_bounded(
                Bound::Included(&p),
                Bound::Excluded(&p_next),
                scan.view,
                None,
                |_, entry_bytes| fetch_index_entry(entry_bytes),
            )?;
            if limit_reached.get() {
                break;
            }
        }
    } else {
        let (start, end) = index_bounds_free(scan.condition, ascending);
        let idx_store = BufferPoolPageStore::new(Arc::clone(&handle));
        let idx_tree = BTree::open(idx_store, idx_snap.root_page, idx_snap.root_level);
        let start_bound = start.as_deref().map_or(Bound::Unbounded, Bound::Included);
        let end_bound = end.as_deref().map_or(Bound::Unbounded, Bound::Included);
        idx_tree.try_for_each_range_scan_mvcc_bounded(
            start_bound,
            end_bound,
            scan.view,
            None,
            |_, entry_bytes| fetch_index_entry(entry_bytes),
        )?;
    }
    Ok(docs)
}

fn execute_collscan_from_snap(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    view: &ReadView,
    match_limit: Option<usize>,
) -> Result<SnapshotPairs> {
    let store = BufferPoolPageStore::new(Arc::clone(&shared.handle));
    let tree = BTree::open(store, ns_snap.data_root_page, ns_snap.data_root_level);
    let probe = primary_history_probe(shared, ns_snap.id);
    btree_collscan(&tree, filter, view, Some(&probe), match_limit)
}

pub(in crate::storage::paged_engine) fn plan_and_collect_snapshot_pairs(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    view: &ReadView,
    allow_secondary_indexes: bool,
) -> Result<PlannedSnapshotPairs> {
    plan_and_collect_snapshot_pairs_limited(
        shared,
        ns_snap,
        filter,
        view,
        allow_secondary_indexes,
        None,
    )
}

/// Like [`plan_and_collect_snapshot_pairs`] but stops collecting after
/// `match_limit` matches. On the collscan path this avoids decoding
/// documents that cannot contribute to the result.
///
/// `ns_snap` MUST be routed from `view.published_epoch()` — the view's
/// pinned epoch is the single epoch this snapshot read uses, and that epoch
/// was loaded only AFTER the view took its conservative registry pin (ITEM
/// 1). Routing from any other (separately-loaded) epoch would reopen the
/// load-to-register window this restructure closes.
pub(in crate::storage::paged_engine) fn plan_and_collect_snapshot_pairs_limited(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    view: &ReadView,
    allow_secondary_indexes: bool,
    match_limit: Option<usize>,
) -> Result<PlannedSnapshotPairs> {
    let ready_indexes: Vec<&PublishedIndex> = if allow_secondary_indexes {
        ns_snap
            .indexes
            .iter()
            .filter(|i| matches!(i.state, IndexState::Ready))
            .collect()
    } else {
        Vec::new()
    };
    let index_metas: Vec<IndexMeta<'_>> = ready_indexes
        .iter()
        .map(|i| IndexMeta {
            name: &i.name,
            keys: &i.key_pattern,
        })
        .collect();

    let plan = select_plan(filter, &index_metas);
    let pairs = execute_plan_from_snap(
        &plan,
        shared,
        ns_snap,
        &ready_indexes,
        filter,
        view,
        match_limit,
    )?;
    Ok((plan, pairs))
}

fn execute_plan_from_snap(
    plan: &ScanPlan,
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    ready_indexes: &[&PublishedIndex],
    filter: &Document,
    view: &ReadView,
    match_limit: Option<usize>,
) -> Result<SnapshotPairs> {
    match plan {
        ScanPlan::PrimaryKeyLookup { condition } => execute_primary_key_lookup_from_snap(
            shared,
            ns_snap,
            filter,
            view,
            condition,
            match_limit,
        ),
        ScanPlan::IndexScan {
            index_name,
            primary_field,
            condition,
        } => execute_index_scan_from_snap(
            shared,
            ns_snap,
            SnapshotIndexScan {
                ready_indexes,
                filter,
                view,
                index_name,
                primary_field,
                condition,
                match_limit,
            },
        ),
        ScanPlan::CollScan => {
            execute_collscan_from_snap(shared, ns_snap, filter, view, match_limit)
        }
    }
}
