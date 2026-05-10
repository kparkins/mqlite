//! Snapshot-based read helpers — mutex-free read path.

use std::ops::Bound;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bson::{Bson, Document};

use crate::error::{EngineFatalReason, Error, Result};
use crate::journal::log_file::{CheckpointBoundaryPayload, LogRecordDraft};
use crate::keys::{encode_compound_key, encode_key, COMPOUND_SEP};
use crate::mvcc::read_view::ReadView;
use crate::options::FindOptions;
use crate::query::eval_filter;
use crate::query::planner::{
    select_plan, IndexCondition, IndexMeta, PrimaryKeyCondition, ScanPlan,
};
use crate::storage::btree::{BTree, BTreePageStore, CellValue, HistoryProbe};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::PageSize;
use crate::storage::catalog::IndexState;
use crate::storage::history_store::HistoryStore;
use crate::storage::reconcile::driver::{
    build_checkpoint_reconcile_plan, reconcile_tree_dirty_set, CheckpointReconcilePlan,
};
use crate::storage::root_snapshot::{NamespaceSnapshot, PublishedEpoch, PublishedIndex};
use crate::storage::structural_page_batch::StructuralPageBatch;

use super::btree_ops::{btree_collscan, btree_collscan_limited};
use super::doc_helpers::{apply_projection_to_doc, compare_docs};
use super::index_maint::{
    index_bounds_free, index_entry_id_free, materialize_primary_deltas_for_checkpoint,
    materialize_ready_secondary_deltas_for_checkpoint,
};
use super::publish::rebuild_and_publish;
use super::publish::PublishDirty;
use super::state::SharedState;

type SnapshotPairs = Vec<(Vec<u8>, Document)>;
type PlannedSnapshotPairs = (ScanPlan, SnapshotPairs);

pub(super) fn open_snapshot_read_view(
    shared: &SharedState,
    epoch: Arc<PublishedEpoch>,
) -> Arc<ReadView> {
    // §10.19 C-1 / US-037: retry the (epoch, frontier) publish pair
    // before constructing the view. The caller already loaded `epoch`,
    // so we only spin on the frontier side until it catches up.
    while shared
        .publish_sequencer
        .published_frontier
        .load(Ordering::Acquire)
        < epoch.visible_ts
    {
        std::hint::spin_loop();
    }
    let txn_id = shared.txn_counter.fetch_add(1, Ordering::Relaxed);
    ReadView::open_for_epoch(
        Arc::clone(shared.handle.read_view_registry()),
        epoch,
        txn_id,
        Arc::clone(&shared.publish_sequencer),
    )
}

pub(super) fn primary_history_probe<'a>(
    shared: &'a SharedState,
    collection_id: i64,
) -> PrimaryHistoryProbe<'a, BufferPoolPageStore> {
    PrimaryHistoryProbe {
        store: &shared.history_store,
        collection_id,
    }
}

pub(super) fn fetch_primary_pair(
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
    epoch: Arc<PublishedEpoch>,
    condition: &PrimaryKeyCondition,
) -> Result<SnapshotPairs> {
    let store = BufferPoolPageStore::new(Arc::clone(&shared.handle));
    let tree = BTree::open(store, ns_snap.data_root_page, ns_snap.data_root_level);
    let view = open_snapshot_read_view(shared, epoch);
    let probe = primary_history_probe(shared, ns_snap.id);

    match condition {
        PrimaryKeyCondition::Eq(id) => {
            let key = encode_key(id);
            Ok(fetch_primary_pair(&tree, key, filter, &view, Some(&probe))?
                .into_iter()
                .collect())
        }
        PrimaryKeyCondition::In(vals) => {
            let mut keys: Vec<Vec<u8>> = vals.iter().map(encode_key).collect();
            keys.sort();
            keys.dedup();
            let mut matched = Vec::with_capacity(keys.len());
            for key in keys {
                if let Some(pair) = fetch_primary_pair(&tree, key, filter, &view, Some(&probe))? {
                    matched.push(pair);
                }
            }
            Ok(matched)
        }
    }
}

/// Bind the primary-key probe path of a [`HistoryStore`] to a fixed
/// `(collection_id, primary tree)` so the BTree layer sees a key-only probe.
pub(super) struct PrimaryHistoryProbe<'a, S: BTreePageStore> {
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
pub(super) fn apply_find_opts(mut docs: Vec<Document>, opts: &FindOptions) -> Vec<Document> {
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
    ready_indexes: &[&PublishedIndex],
    filter: &Document,
    view: &ReadView,
    index_name: &str,
    primary_field: &str,
    condition: &IndexCondition,
) -> Result<Vec<(Vec<u8>, Document)>> {
    let idx_snap = ready_indexes
        .iter()
        .find(|i| i.name == index_name)
        .ok_or_else(|| Error::Internal(format!("index '{}' not in snapshot", index_name)))?;

    let ascending = idx_snap
        .key_pattern
        .get(primary_field)
        .map(|v| !matches!(v, Bson::Int32(-1) | Bson::Int64(-1)))
        .unwrap_or(true);

    let handle = Arc::clone(&shared.handle);
    let id_bsons: Vec<Bson> = if let IndexCondition::In(vals) = condition {
        let mut results = Vec::with_capacity(vals.len());
        for v in vals {
            let mut p = encode_compound_key(&[(v, ascending)]);
            p.push(COMPOUND_SEP);
            let mut p_next = p.clone();
            if let Some(last) = p_next.last_mut() {
                *last += 1;
            }
            let idx_store = BufferPoolPageStore::new(Arc::clone(&handle));
            let idx_tree = BTree::open(idx_store, idx_snap.root_page, idx_snap.root_level);
            for (_, cv) in idx_tree.range_scan_mvcc_bounded(
                Bound::Included(&p),
                Bound::Excluded(&p_next),
                view,
                None,
            )? {
                let id = index_entry_id_free(&handle, CellValue::Inline(cv))?;
                if !matches!(id, Bson::Null) {
                    results.push(id);
                }
            }
        }
        results
    } else {
        let (start, end) = index_bounds_free(condition, ascending);
        let idx_store = BufferPoolPageStore::new(Arc::clone(&handle));
        let idx_tree = BTree::open(idx_store, idx_snap.root_page, idx_snap.root_level);
        let start_bound = start.as_deref().map_or(Bound::Unbounded, Bound::Included);
        let end_bound = end.as_deref().map_or(Bound::Unbounded, Bound::Included);
        idx_tree
            .range_scan_mvcc_bounded(start_bound, end_bound, view, None)?
            .into_iter()
            .filter_map(|(_, cv)| {
                index_entry_id_free(&handle, CellValue::Inline(cv))
                    .ok()
                    .filter(|id| !matches!(id, Bson::Null))
            })
            .collect()
    };

    // Fetch matching docs from the data tree using the same MVCC-aware point
    // lookup path as direct primary-key plans.
    let mut docs = Vec::new();
    if !id_bsons.is_empty() {
        let data_store = BufferPoolPageStore::new(Arc::clone(&handle));
        let data_tree = BTree::open(data_store, ns_snap.data_root_page, ns_snap.data_root_level);
        let probe = primary_history_probe(shared, ns_snap.id);
        for id_bson in id_bsons {
            let data_key = encode_key(&id_bson);
            if let Some(pair) =
                fetch_primary_pair(&data_tree, data_key, filter, view, Some(&probe))?
            {
                docs.push(pair);
            }
        }
    }
    Ok(docs)
}

fn execute_collscan_from_snap(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    epoch: Arc<PublishedEpoch>,
) -> Result<SnapshotPairs> {
    let store = BufferPoolPageStore::new(Arc::clone(&shared.handle));
    let tree = BTree::open(store, ns_snap.data_root_page, ns_snap.data_root_level);
    let view = open_snapshot_read_view(shared, epoch);
    let probe = primary_history_probe(shared, ns_snap.id);
    btree_collscan(&tree, filter, &view, Some(&probe))
}

pub(super) fn plan_and_collect_snapshot_pairs(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    epoch: Arc<PublishedEpoch>,
    allow_secondary_indexes: bool,
) -> Result<PlannedSnapshotPairs> {
    plan_and_collect_snapshot_pairs_limited(
        shared,
        ns_snap,
        filter,
        epoch,
        allow_secondary_indexes,
        None,
    )
}

/// Like [`plan_and_collect_snapshot_pairs`] but stops collecting after
/// `match_limit` matches. On the collscan path this avoids decoding
/// documents that cannot contribute to the result.
pub(super) fn plan_and_collect_snapshot_pairs_limited(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    epoch: Arc<PublishedEpoch>,
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
    let pairs = match (&plan, match_limit) {
        (ScanPlan::CollScan, Some(limit)) => {
            execute_collscan_from_snap_limited(shared, ns_snap, filter, epoch, limit)?
        }
        _ => execute_plan_from_snap(&plan, shared, ns_snap, &ready_indexes, filter, epoch)?,
    };
    Ok((plan, pairs))
}

fn execute_collscan_from_snap_limited(
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    epoch: Arc<PublishedEpoch>,
    match_limit: usize,
) -> Result<SnapshotPairs> {
    let store = BufferPoolPageStore::new(Arc::clone(&shared.handle));
    let tree = BTree::open(store, ns_snap.data_root_page, ns_snap.data_root_level);
    let view = open_snapshot_read_view(shared, epoch);
    let probe = primary_history_probe(shared, ns_snap.id);
    btree_collscan_limited(&tree, filter, &view, Some(&probe), Some(match_limit))
}

fn execute_plan_from_snap(
    plan: &ScanPlan,
    shared: &SharedState,
    ns_snap: &NamespaceSnapshot,
    ready_indexes: &[&PublishedIndex],
    filter: &Document,
    epoch: Arc<PublishedEpoch>,
) -> Result<SnapshotPairs> {
    match plan {
        ScanPlan::PrimaryKeyLookup { condition } => {
            execute_primary_key_lookup_from_snap(shared, ns_snap, filter, epoch, condition)
        }
        ScanPlan::IndexScan {
            index_name,
            primary_field,
            condition,
        } => {
            let view = open_snapshot_read_view(shared, epoch);
            execute_index_scan_from_snap(
                shared,
                ns_snap,
                ready_indexes,
                filter,
                &view,
                index_name,
                primary_field,
                condition,
            )
        }
        ScanPlan::CollScan => execute_collscan_from_snap(shared, ns_snap, filter, epoch),
    }
}

// ---------------------------------------------------------------------------
// Engine-level snapshot/lifecycle free functions
// ---------------------------------------------------------------------------

pub(super) fn checkpoint(engine: &super::PagedEngine) -> crate::error::Result<()> {
    let _checkpoint_admission = engine
        .shared
        .checkpoint_admission
        .close_and_drain_all(engine.busy_timeout)?;
    let _md_w = engine
        .metadata
        .write()
        .map_err(|_| crate::error::Error::Internal("metadata RwLock poisoned".into()))?;
    let md = &engine.metadata_state;
    let checkpoint_ts = engine.shared.published.load_full().visible_ts;
    let ort = engine
        .shared
        .handle
        .read_view_registry()
        .oldest_required_ts();

    let checkpoint_plan = build_checkpoint_reconcile_plan(engine, checkpoint_ts, ort)?;

    if let Err(err) =
        checkpoint_after_reconcile_plan(engine, md, checkpoint_ts, ort, checkpoint_plan)
    {
        return Err(poison_checkpoint_post_mutation(engine, err));
    }
    Ok(())
}

fn checkpoint_after_reconcile_plan(
    engine: &super::PagedEngine,
    md: &super::state::MetadataState,
    checkpoint_ts: crate::mvcc::Ts,
    ort: crate::mvcc::Ts,
    checkpoint_plan: CheckpointReconcilePlan,
) -> Result<()> {
    let mut batch = StructuralPageBatch::new(&engine.shared.handle);
    engine
        .shared
        .handle
        .sync_journal_ready_prefix()
        .map_err(|error| engine.poison_after_log_manager_failure(error))?;
    let initial_checkpoint_applied_lsn = engine.shared.handle.current_journal_durable_lsn()?;
    engine
        .shared
        .handle
        .set_main_file_flush_lsn(initial_checkpoint_applied_lsn);
    engine
        .shared
        .handle
        .stamp_unflushable_dirty_pages_lsn(initial_checkpoint_applied_lsn)?;
    let materialize_result = (|| -> Result<_> {
        let (primary_catalog_dirty, mut materialized_trees, primary_requires_logical_tail) =
            materialize_primary_deltas_for_checkpoint(&engine.shared, md, &mut batch)?;
        let (
            secondary_catalog_dirty,
            secondary_materialized_trees,
            secondary_requires_logical_tail,
        ) = materialize_ready_secondary_deltas_for_checkpoint(&engine.shared, md, &mut batch)?;
        materialized_trees.extend(secondary_materialized_trees);
        Ok((
            primary_catalog_dirty || secondary_catalog_dirty,
            materialized_trees,
            primary_requires_logical_tail || secondary_requires_logical_tail,
        ))
    })();
    let (published_catalog_dirty, materialized_trees, mut requires_logical_tail) =
        match materialize_result {
            Ok(result) => result,
            Err(err) => {
                let _ = batch.abort(&engine.shared.handle);
                return Err(err);
            }
        };
    let mut base_store = engine.shared.new_btree_store();
    batch.commit_lsn_fenced(
        &mut base_store,
        &engine.shared.handle,
        initial_checkpoint_applied_lsn,
    )?;
    for tree in checkpoint_plan.trees() {
        if !materialized_trees.contains(tree.ident()) {
            continue;
        }
        for page_id in tree.mutation_ready_pages() {
            engine
                .shared
                .handle
                .pool()
                .clear_chains_on_page(*page_id, PageSize::Large32k)?;
        }
        engine
            .shared
            .clear_dirty_pages(tree.ident(), tree.mutation_ready_pages());
        if !tree.excluded_future_dirty_pages().is_empty() {
            requires_logical_tail = true;
        }
    }
    if published_catalog_dirty {
        let mut dirty = PublishDirty::default();
        dirty.mark_published();
        dirty.mark_header();
        let publish_ts = engine.shared.oracle.commit()?;
        // Phase 5 §10.17.1 / US-006: checkpoint materializes Ready
        // secondary deltas under `metadata.write()` and is a DDL-style
        // publish. Reserve the next catalog generation BEFORE publish so
        // readers observe the same ordered identity advance contract as
        // create/drop_index.
        let reserved_gen = engine
            .shared
            .next_catalog_gen
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        rebuild_and_publish(&engine.shared, md, publish_ts, dirty, Some(reserved_gen))?;
    }

    let (root_page, root_level) = {
        let cat = md.catalog_lock();
        (cat.root_page(), cat.root_level())
    };
    engine.shared.handle.allocator().update_header(|h| {
        h.catalog_root_page = root_page;
        h.catalog_root_level = root_level;
        h.catalog_root_backup = root_page;
    })?;

    for tree in checkpoint_plan.trees() {
        if materialized_trees.contains(tree.ident()) {
            continue;
        }
        let stats = reconcile_tree_dirty_set(
            engine,
            md,
            tree.ident().clone(),
            tree.mutation_ready_pages(),
            checkpoint_ts,
            ort,
            materialized_trees.contains(tree.ident()),
        )?;
        if stats.not_installable > 0 {
            // The pre-mutation plan rejected hard blockers up front. A page
            // that becomes non-installable here keeps the logical journal tail
            // as the durable recovery source for this checkpoint round.
            requires_logical_tail = true;
            continue;
        }
        debug_assert!(
            !tree.excluded_future_dirty_pages().is_empty()
                || !tree.mutation_ready_pages().is_empty()
        );
    }

    {
        let mut hs =
            engine
                .shared
                .history_store
                .lock()
                .map_err(|_| crate::error::Error::StatePoisoned {
                    component: "history_store",
                })?;
        hs.gc_pass(ort)?;
    }
    let lag_ms = if ort == crate::mvcc::timestamp::Ts::MAX {
        0
    } else {
        engine
            .shared
            .oracle
            .now()
            .physical_ms
            .saturating_sub(ort.physical_ms)
    };
    crate::mvcc::metrics::set_oldest_required_ts_lag_ms(lag_ms);
    crate::mvcc::metrics::set_overflow_pages_in_use(
        engine.shared.handle.allocator().overflow_pages_in_use() as u64,
    );
    crate::mvcc::metrics::set_deferred_free_queue_depth(
        engine
            .shared
            .handle
            .allocator()
            .page_lifetime_queue()
            .depth() as u64,
    );
    let mut checkpoint_applied_lsn = None;
    if !requires_logical_tail {
        engine
            .shared
            .handle
            .sync_journal_ready_prefix()
            .map_err(|error| engine.poison_after_log_manager_failure(error))?;
        let candidate_lsn = engine.shared.handle.current_journal_durable_lsn()?;
        engine.shared.handle.set_main_file_flush_lsn(candidate_lsn);
        engine
            .shared
            .handle
            .stamp_unflushable_dirty_pages_lsn(candidate_lsn)?;
        checkpoint_applied_lsn = Some(candidate_lsn);
    }
    #[cfg(any(test, feature = "test-hooks"))]
    super::hidden_accessors::us026_fail_if_armed(
        &engine.shared,
        super::hidden_accessors::Us026PostRegisterFailpoint::Flush,
    )?;
    engine.shared.handle.flush()?;
    if engine.shared.handle.has_dirty_pages()? {
        requires_logical_tail = true;
    }
    if requires_logical_tail {
        engine
            .shared
            .handle
            .sync_journal_ready_prefix()
            .map_err(|error| engine.poison_after_log_manager_failure(error))?;
        return Ok(());
    }
    #[cfg(any(test, feature = "test-hooks"))]
    super::hidden_accessors::checkpoint_boundary_abort_if_armed(
        super::hidden_accessors::CheckpointBoundaryFailpoint::AfterMaterializationFlushBeforeBoundary,
    );
    let checkpoint_applied_lsn = checkpoint_applied_lsn.ok_or_else(|| {
        Error::Internal("checkpoint boundary requested without an applied LSN".into())
    })?;
    engine.shared.handle.allocator().update_header(|header| {
        header.last_checkpoint_ts = header.last_checkpoint_ts.max(checkpoint_ts);
        header.checkpoint_applied_lsn = checkpoint_applied_lsn;
    })?;
    engine.shared.handle.flush()?;
    engine.shared.handle.sync_main_file()?;
    let header = engine.shared.handle.allocator().with_header(Clone::clone)?;
    let payload = CheckpointBoundaryPayload {
        checkpoint_applied_lsn: header.checkpoint_applied_lsn,
        header,
    }
    .encode()?;
    let draft = LogRecordDraft::checkpoint_boundary(0, checkpoint_ts, payload);
    let reserved = engine.shared.handle.reserve_log_record(draft)?;
    let boundary_end_lsn = reserved.end_lsn();
    let written_end_lsn = reserved
        .write_and_mark()
        .map_err(|error| engine.poison_after_log_manager_failure(error))?;
    debug_assert_eq!(written_end_lsn, boundary_end_lsn);
    engine
        .shared
        .handle
        .wait_journal_durable(boundary_end_lsn)
        .map_err(|error| engine.poison_after_log_manager_failure(error))?;
    engine.shared.handle.advance_page_lifetime_checkpoint()?;
    Ok(())
}

fn poison_checkpoint_post_mutation(engine: &super::PagedEngine, err: Error) -> Error {
    if matches!(err, Error::EngineFatal { .. }) {
        return err;
    }
    let reason = EngineFatalReason::CheckpointPostMutationFailure;
    engine.shared.poison_engine(reason.clone());
    Error::EngineFatal { reason }
}

#[allow(
    dead_code,
    reason = "FullSync CRUD now syncs through group commit before publish; \
              this helper backs the explicit journal_sync inherent method"
)]
pub(super) fn journal_sync(engine: &super::PagedEngine) -> crate::error::Result<()> {
    engine
        .shared
        .handle
        .sync_journal_ready_prefix()
        .map_err(|error| engine.poison_after_log_manager_failure(error))?;
    let durable_lsn = engine.shared.handle.current_journal_durable_lsn()?;
    engine.shared.handle.set_main_file_flush_lsn(durable_lsn);
    engine
        .shared
        .handle
        .stamp_unflushable_dirty_pages_lsn(durable_lsn)
}

pub(super) fn snapshot_bytes(
    _engine: &super::PagedEngine,
) -> crate::error::Result<Option<Vec<u8>>> {
    Ok(None)
}
