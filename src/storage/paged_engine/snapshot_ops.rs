//! Snapshot-based read helpers (PR 4 — mutex-free read path).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::key_encoding::{encode_compound_key, encode_key, COMPOUND_SEP};
use crate::mvcc::read_view::ReadView;
use crate::options::FindOptions;
use crate::query::eval_filter;
use crate::query::planner::{
    select_plan, IndexCondition, IndexMeta, PrimaryKeyCondition, ScanPlan,
};
use crate::storage::btree::{BTree, BTreePageStore, HistoryProbe};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::IndexState;
use crate::storage::history_store::HistoryStore;
use crate::storage::root_snapshot::{NamespaceSnapshot, PublishedIndex};

use super::btree_ops::btree_collscan;
use super::doc_helpers::{apply_projection_to_doc, sort_docs};
use super::index_maint::{index_bounds_free, index_entry_id_free};
use super::state::SharedState;

pub(super) fn open_snapshot_read_view(
    shared: &SharedState,
    publish_ts: crate::mvcc::timestamp::Ts,
) -> Arc<ReadView> {
    let txn_id = shared.txn_counter.fetch_add(1, Ordering::Relaxed);
    ReadView::open(
        Arc::clone(shared.handle.read_view_registry()),
        publish_ts,
        txn_id,
    )
}

pub(super) fn primary_history_probe<'a>(
    shared: &'a SharedState,
    ns: &str,
) -> PrimaryHistoryProbe<'a, BufferPoolPageStore> {
    PrimaryHistoryProbe {
        store: &shared.history_store,
        ns_id: ns_id_for(ns),
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

pub(super) fn execute_primary_key_lookup_from_snap(
    shared: &SharedState,
    ns: &str,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    publish_ts: crate::mvcc::timestamp::Ts,
    condition: &PrimaryKeyCondition,
) -> Result<Vec<(Vec<u8>, Document)>> {
    let store = BufferPoolPageStore::new(Arc::clone(&shared.handle));
    let tree = BTree::open(store, ns_snap.data_root_page, ns_snap.data_root_level);
    let view = open_snapshot_read_view(shared, publish_ts);
    let probe = primary_history_probe(shared, ns);

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

/// Derive a stable `ns_id: u32` from a collection / namespace name.
///
/// FNV-1a 32-bit. Used purely as a key-space partitioning hint for the
/// history store; collisions just mean two collections share a key
/// prefix in the history B-tree, which is harmless because the
/// remaining key material (kind-tag + user key) already disambiguates.
pub(super) fn ns_id_for(ns: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in ns.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// Bind the primary-key probe path of a [`HistoryStore`] to a fixed
/// `(ns_id, KIND_PRIMARY)` so the BTree layer sees a key-only probe.
pub(super) struct PrimaryHistoryProbe<'a, S: BTreePageStore> {
    store: &'a std::sync::Mutex<HistoryStore<S>>,
    ns_id: u32,
}

impl<S: BTreePageStore> crate::storage::btree::HistoryProbe for PrimaryHistoryProbe<'_, S> {
    fn probe(
        &self,
        key: &[u8],
        read_ts: crate::mvcc::timestamp::Ts,
    ) -> Result<Option<crate::mvcc::version::VersionEntry>> {
        let guard = self.store.lock().map_err(|_| {
            Error::Internal("history_store mutex poisoned".into())
        })?;
        guard.probe_primary(self.ns_id, key, read_ts)
    }
}

/// Apply sort/skip/limit/projection to a list of matched documents.
pub(super) fn apply_find_opts(mut docs: Vec<Document>, opts: &FindOptions) -> Vec<Document> {
    if let Some(s) = &opts.sort {
        sort_docs(&mut docs, s);
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

/// Index scan using a [`PublishedSnapshot`] instead of the catalog.
pub(super) fn execute_index_scan_from_snap(
    shared: &SharedState,
    ns: &str,
    ns_snap: &crate::storage::root_snapshot::NamespaceSnapshot,
    ready_indexes: &[&PublishedIndex],
    filter: &Document,
    publish_ts: crate::mvcc::timestamp::Ts,
    index_name: &str,
    primary_field: &str,
    condition: &IndexCondition,
) -> Result<Vec<(Vec<u8>, Document)>> {
    let idx_snap = ready_indexes
        .iter()
        .find(|i| i.name == index_name)
        .ok_or_else(|| Error::Internal(format!("index '{}' not in snapshot", index_name)))?;

    let ascending = idx_snap.key_pattern
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
            *p_next.last_mut().expect("compound key always contains at least COMPOUND_SEP") += 1;
            let idx_store = BufferPoolPageStore::new(Arc::clone(&handle));
            let idx_tree = BTree::open(idx_store, idx_snap.root_page, idx_snap.root_level);
            for (_, cv) in idx_tree.range_scan(Some(&p), Some(&p_next))? {
                let id = index_entry_id_free(&handle, cv)?;
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
        idx_tree
            .range_scan(start.as_deref(), end.as_deref())?
            .into_iter()
            .filter_map(|(_, cv)| {
                index_entry_id_free(&handle, cv)
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
        let view = open_snapshot_read_view(shared, publish_ts);
        let probe = primary_history_probe(shared, ns);
        for id_bson in id_bsons {
            let data_key = encode_key(&id_bson);
            if let Some(pair) =
                fetch_primary_pair(&data_tree, data_key, filter, &view, Some(&probe))?
            {
                docs.push(pair);
            }
        }
    }
    Ok(docs)
}

pub(super) fn execute_collscan_from_snap(
    shared: &SharedState,
    ns: &str,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    publish_ts: crate::mvcc::timestamp::Ts,
) -> Result<Vec<(Vec<u8>, Document)>> {
    let store = BufferPoolPageStore::new(Arc::clone(&shared.handle));
    let tree = BTree::open(store, ns_snap.data_root_page, ns_snap.data_root_level);
    let view = open_snapshot_read_view(shared, publish_ts);
    let probe = primary_history_probe(shared, ns);
    btree_collscan(&tree, filter, &view, Some(&probe))
}

pub(super) fn execute_snapshot_pairs_from_snap(
    shared: &SharedState,
    ns: &str,
    ns_snap: &NamespaceSnapshot,
    filter: &Document,
    publish_ts: crate::mvcc::timestamp::Ts,
    allow_secondary_indexes: bool,
) -> Result<Vec<(Vec<u8>, Document)>> {
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

    match select_plan(filter, &index_metas) {
        ScanPlan::PrimaryKeyLookup { condition } => execute_primary_key_lookup_from_snap(
            shared,
            ns,
            ns_snap,
            filter,
            publish_ts,
            &condition,
        ),
        ScanPlan::IndexScan {
            index_name,
            primary_field,
            condition,
        } => execute_index_scan_from_snap(
            shared,
            ns,
            ns_snap,
            &ready_indexes,
            filter,
            publish_ts,
            &index_name,
            &primary_field,
            &condition,
        ),
        ScanPlan::CollScan => execute_collscan_from_snap(shared, ns, ns_snap, filter, publish_ts),
    }
}

// ---------------------------------------------------------------------------
// Engine-level snapshot/lifecycle free functions
// ---------------------------------------------------------------------------

pub(super) fn checkpoint(engine: &super::PagedEngine) -> crate::error::Result<()> {
    let md = engine.metadata.write().map_err(|_| {
        crate::error::Error::Internal("metadata RwLock poisoned".into())
    })?;

    let (root_page, root_level) = {
        let cat = md.catalog.lock().expect("catalog poisoned");
        (cat.root_page(), cat.root_level())
    };
    engine.shared.handle.allocator().update_header(|h| {
        h.catalog_root_page = root_page;
        h.catalog_root_level = root_level;
        h.catalog_root_backup = root_page;
    })?;

    let ort = engine.shared.handle.read_view_registry().oldest_required_ts();
    {
        let mut hs = engine.shared.history_store.lock().map_err(|_| crate::error::Error::StatePoisoned { component: "history_store" })?;
        hs.gc_pass(ort)?;
    }
    let lag_ms = if ort == crate::mvcc::timestamp::Ts::MAX {
        0
    } else {
        engine.shared
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
        engine.shared.handle.allocator().deferred_free_queue().depth() as u64,
    );
    engine.shared.handle.flush()
}

pub(super) fn close(engine: &super::PagedEngine) -> crate::error::Result<()> {
    checkpoint(engine)
}

pub(super) fn journal_sync(engine: &super::PagedEngine) -> crate::error::Result<()> {
    engine.shared.handle.journal_sync()
}

pub(super) fn snapshot_bytes(_engine: &super::PagedEngine) -> crate::error::Result<Option<Vec<u8>>> {
    Ok(None)
}
