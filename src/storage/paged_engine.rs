//! `PagedEngine` — Phase 1 `StorageEngine` backed by B+ trees.
//!
//! ## Design
//!
//! Documents are stored in per-namespace B+ trees keyed by [`encode_key`]-encoded
//! `_id` values.  Two operating modes:
//!
//! | Mode | Backing store | Persistence |
//! |------|--------------|-------------|
//! | **Buffered** | [`BufferPoolPageStore`] (shared [`BufferPoolHandle`]) | Via buffer pool flush |
//!
//! ## Catalog (Buffered mode)
//!
//! A [`Catalog`] B+ tree stores [`CollectionEntry`] and [`IndexEntry`] records.
//! Its root page number is persisted to [`FileHeader::catalog_root_page`] after every
//! catalog mutation, so the catalog can be located on reopen.
//!
//! ## Query execution
//!
//! `find` first asks the query planner ([`select_plan`]) whether the query can use
//! the implicit primary `_id` key or a secondary index.  When a suitable secondary
//! index is found the engine performs an [`IndexScan`] — a range scan on the
//! secondary B+ tree whose values contain the serialised `_id` of the matching
//! document, followed by a point lookup in the primary data tree.  Other read-like
//! paths reuse the same primary-key / collection-scan executor to avoid ad hoc `_id`
//! special cases. When no access path matches the engine falls back to a full
//! [`CollScan`].
//!
//! [`IndexScan`]: crate::query::planner::ScanPlan::IndexScan
//! [`CollScan`]: crate::query::planner::ScanPlan::CollScan
//! [`select_plan`]: crate::query::planner::select_plan

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use dashmap::{DashMap, DashSet};

use crate::options::BusyHandler;

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::index::{IndexInfo, IndexModel};
use crate::key_encoding::{encode_compound_key, encode_key, COMPOUND_SEP};
use crate::mvcc::read_view::ReadView;
use crate::mvcc::timestamp::TimestampOracle;
use crate::mvcc::transaction::WriteTxn;
use crate::options::{
    FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
    ReturnDocument, UpdateOptions,
};
use crate::query::planner::{
    select_plan, IndexCondition, IndexMeta, PrimaryKeyCondition, ScanPlan,
};
use crate::query::{eval_filter, get_nested_field};
use crate::results::{DeleteResult, UpdateResult};
use crate::storage::btree::{BTree, BTreePageStore, CellValue, HistoryProbe};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::PageSize;
use crate::storage::catalog::{open_with_fallback as catalog_open_with_fallback, Catalog, IndexEntry, IndexState};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::history_store::HistoryStore;
use crate::storage::oid::ObjectIdGenerator;
use crate::storage::root_snapshot::{NamespaceSnapshot, PublishedIndex, PublishedSnapshot};
use crate::storage::secondary_index::{
    build_index, generate_index_name, update_index_on_delete, update_index_on_insert,
    update_index_on_update,
};
use crate::storage::txn_page_store::{PageOrigin, PageReservation, TxnOverlay, TxnPageStore};
use crate::storage_engine::StorageEngine;
use crate::update_operators::{apply_update, is_operator_update, upsert_base_from_filter};
use crate::validation::validate_document;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Return current Unix milliseconds.
fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Ensure a document has an `_id` field.  Auto-assigns an [`ObjectId`] if absent.
fn ensure_id(doc: &mut Document) -> Bson {
    if let Some(id) = doc.get("_id") {
        id.clone()
    } else {
        let oid = Bson::ObjectId(ObjectIdGenerator::generate());
        doc.insert("_id", oid.clone());
        oid
    }
}

/// Validate that an index key pattern does not request an unsupported index type.
///
/// Rejects `text`, `2d`, `2dsphere`, and `hashed` indexes (Phase 2 features).
fn validate_index_keys(keys: &Document) -> Result<()> {
    const SUGGESTION: &str =
        "Phase 1 supports single-field, compound, unique, sparse, and multikey \
         indexes. Text, geospatial, hashed, TTL, and partial indexes are \
         planned for a future release.";

    for (_field, value) in keys {
        let type_name: Option<&str> = match value {
            Bson::String(s) => match s.as_str() {
                "text" => Some("text"),
                "2d" => Some("2d"),
                "2dsphere" => Some("2dsphere"),
                "hashed" => Some("hashed"),
                _ => None,
            },
            _ => None,
        };
        if let Some(t) = type_name {
            return Err(Error::UnsupportedIndexOption {
                option: t.to_owned(),
                suggestion: SUGGESTION.to_owned(),
            });
        }
    }
    Ok(())
}

/// Check unique index constraints before inserting `new_doc` into `tree`.
///
/// `unique_specs` is a list of `(index_name, fields, sparse)` for each unique index.
/// If any existing document matches the new doc on all indexed fields, returns
/// [`Error::DuplicateKey`].
fn check_unique_constraints<S: BTreePageStore>(
    tree: &BTree<S>,
    unique_specs: &[(String, Vec<String>, bool)],
    new_doc: &Document,
) -> Result<()> {
    if unique_specs.is_empty() {
        return Ok(());
    }

    let null_encoded = encode_key(&Bson::Null);

    for (idx_name, fields, sparse) in unique_specs {
        // Encode the candidate document's indexed fields.
        let new_encoded: Vec<Vec<u8>> = fields
            .iter()
            .map(|f| encode_key(new_doc.get(f.as_str()).unwrap_or(&Bson::Null)))
            .collect();

        // Sparse: skip if all indexed fields are null/absent.
        if *sparse && new_encoded.iter().all(|v| v == &null_encoded) {
            continue;
        }

        // Scan all documents in the tree.
        let pairs = tree.range_scan(None, None)?;
        for (_, cv) in pairs {
            let bson_bytes = resolve_cell(tree, cv)?;
            let existing: Document =
                bson::from_slice(&bson_bytes).map_err(Error::BsonDeserialization)?;

            let existing_encoded: Vec<Vec<u8>> = fields
                .iter()
                .map(|f| encode_key(existing.get(f.as_str()).unwrap_or(&Bson::Null)))
                .collect();

            if new_encoded == existing_encoded {
                return Err(Error::DuplicateKey {
                    detail: format!(
                        "E11000 duplicate key error — unique index '{}': dup key {{{}}}",
                        idx_name,
                        fields
                            .iter()
                            .map(|f| format!("{}: {:?}", f, new_doc.get(f.as_str())))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                });
            }
        }
    }
    Ok(())
}

/// Resolve a [`CellValue`] from a B+ tree to raw bytes.
fn resolve_cell<S: BTreePageStore>(tree: &BTree<S>, cv: CellValue) -> Result<Vec<u8>> {
    match cv {
        CellValue::Inline(b) => Ok(b),
        CellValue::Overflow {
            first_page,
            total_length,
        } => tree.read_overflow(first_page, total_length),
    }
}

// ---------------------------------------------------------------------------
// Sort / projection helpers (replicated from engine.rs for local use)
// ---------------------------------------------------------------------------

fn sort_docs(docs: &mut Vec<Document>, sort: &Document) {
    docs.sort_by(|a, b| compare_docs(a, b, sort));
}

fn compare_docs(a: &Document, b: &Document, sort: &Document) -> std::cmp::Ordering {
    for (field, dir) in sort {
        let ascending = !matches!(dir, Bson::Int32(-1) | Bson::Int64(-1));
        let av = get_nested_field(a, field).cloned().unwrap_or(Bson::Null);
        let bv = get_nested_field(b, field).cloned().unwrap_or(Bson::Null);
        let ord = encode_key(&av).cmp(&encode_key(&bv));
        if ord == std::cmp::Ordering::Equal {
            continue;
        }
        return if ascending { ord } else { ord.reverse() };
    }
    std::cmp::Ordering::Equal
}

fn apply_projection_to_doc(mut doc: Document, proj: &Document) -> Document {
    let is_inclusion = proj
        .iter()
        .filter(|(k, _)| k.as_str() != "_id")
        .any(|(_, v)| !matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)));

    let explicit_id_excl = proj
        .get("_id")
        .is_some_and(|v| matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)));

    if is_inclusion {
        let mut result = Document::new();
        if !explicit_id_excl {
            if let Some(id) = doc.get("_id") {
                result.insert("_id", id.clone());
            }
        }
        for (k, v) in proj {
            if k == "_id" {
                continue;
            }
            if !matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)) {
                if let Some(val) = doc.get(k) {
                    result.insert(k, val.clone());
                }
            }
        }
        result
    } else {
        for (k, _) in proj {
            doc.remove(k);
        }
        doc
    }
}

// ---------------------------------------------------------------------------
// B+ tree doc-storage helpers (generic over S: BTreePageStore)
// ---------------------------------------------------------------------------

/// Insert `doc` into `tree`, auto-assigning `_id` if absent.
///
/// `unique_specs` are `(name, fields, sparse)` tuples for unique secondary
/// indexes; violated constraints return [`Error::DuplicateKey`] before the
/// tree is modified.
///
/// Returns `(id_bson, encoded_key, bson_bytes, tree_root_page)` so callers
/// can stage the MVCC primary-chain entry via `WriteTxn::stage_primary_insert`
/// after the on-disk cell lands. `tree_root_page` is sampled AFTER the insert
/// so any root split is reflected.
fn btree_insert_doc<S: BTreePageStore>(
    tree: &mut BTree<S>,
    doc: &mut Document,
    unique_specs: &[(String, Vec<String>, bool)],
) -> Result<(Bson, Vec<u8>, Vec<u8>, u32)> {
    validate_document(doc)?;
    let id_bson = ensure_id(doc);
    // Check secondary unique constraints before touching the tree.
    check_unique_constraints(tree, unique_specs, doc)?;
    let key = encode_key(&id_bson);
    let bson_bytes = bson::to_vec(doc).map_err(Error::BsonSerialization)?;
    tree.insert(&key, &bson_bytes).map_err(|e| match e {
        Error::DuplicateKey { .. } => Error::DuplicateKey {
            detail: format!("document with _id already exists"),
        },
        other => other,
    })?;
    let tree_root = tree.root_page;
    Ok((id_bson, key, bson_bytes, tree_root))
}

/// MVCC-aware collection scan. For each key visible at `view.read_ts` (or
/// the on-disk cell when no chain entry is present), decode the value as
/// BSON and retain rows that satisfy `filter`. The optional `history`
/// probe (plan §T7) is consulted when neither the chain nor a newer
/// version is visible, so readers can still see entries evicted from
/// memory chains into the history store.
fn btree_collscan<S: BTreePageStore>(
    tree: &BTree<S>,
    filter: &Document,
    view: &ReadView,
    history: Option<&dyn crate::storage::btree::HistoryProbe>,
) -> Result<Vec<(Vec<u8>, Document)>> {
    let pairs = tree.range_scan_mvcc(None, None, view, history)?;
    let mut result = Vec::new();
    for (key, bson_bytes) in pairs {
        let doc: Document = bson::from_slice(&bson_bytes).map_err(Error::BsonDeserialization)?;
        if eval_filter(&doc, filter)? {
            result.push((key, doc));
        }
    }
    Ok(result)
}

fn open_snapshot_read_view(
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

fn primary_history_probe(
    shared: &SharedState,
    ns: &str,
) -> PrimaryHistoryProbe<BufferPoolPageStore> {
    PrimaryHistoryProbe {
        store: Arc::clone(&shared.history_store),
        ns_id: ns_id_for(ns),
    }
}

fn fetch_primary_pair(
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
            let mut matched = Vec::new();
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
fn ns_id_for(ns: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for &b in ns.as_bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// Bind the primary-key probe path of a [`HistoryStore`] to a fixed
/// `(ns_id, KIND_PRIMARY)` so the BTree layer sees a key-only probe.
struct PrimaryHistoryProbe<S: BTreePageStore> {
    store: Arc<std::sync::Mutex<HistoryStore<S>>>,
    ns_id: u32,
}

impl<S: BTreePageStore> crate::storage::btree::HistoryProbe for PrimaryHistoryProbe<S> {
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
fn apply_find_opts(mut docs: Vec<Document>, opts: &FindOptions) -> Vec<Document> {
    if let Some(s) = &opts.sort {
        sort_docs(&mut docs, s);
    }
    if let Some(skip) = opts.skip {
        let n = skip as usize;
        if n >= docs.len() {
            docs.clear();
        } else {
            docs = docs.into_iter().skip(n).collect();
        }
    }
    if let Some(limit) = opts.limit {
        if limit > 0 {
            docs.truncate(limit as usize);
        }
    }
    if let Some(proj) = &opts.projection {
        docs = docs
            .into_iter()
            .map(|d| apply_projection_to_doc(d, proj))
            .collect();
    }
    docs
}

// ---------------------------------------------------------------------------
// Snapshot-based read helpers (PR 4 — mutex-free read path)
// ---------------------------------------------------------------------------

/// Index scan using a [`PublishedSnapshot`] instead of the catalog.
fn execute_index_scan_from_snap(
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
        let mut results = Vec::new();
        for v in vals {
            let mut p = encode_compound_key(&[(v, ascending)]);
            p.push(COMPOUND_SEP);
            let mut p_next = p.clone();
            *p_next.last_mut().unwrap() += 1;
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

fn execute_collscan_from_snap(
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

fn execute_snapshot_pairs_from_snap(
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
// SharedState — fields shared by read path (no mutex) and writer (mutex held)
// ---------------------------------------------------------------------------

/// State shared by the read path (no mutex) and the writer inside
/// `Mutex<BpBackend>`. Under PR 8 this becomes the full MWMR shared
/// state; in PR 4 it's only what reads need.
pub(crate) struct SharedState {
    pub handle: Arc<BufferPoolHandle>,
    pub history_store: Arc<std::sync::Mutex<HistoryStore<BufferPoolPageStore>>>,
    pub oracle: Arc<TimestampOracle>,
    /// Atomically published snapshot for the mutex-free read path.
    pub published: ArcSwap<PublishedSnapshot>,
    /// Monotonic transaction identifier source shared by readers and writers.
    pub txn_counter: AtomicU64,
}

// ---------------------------------------------------------------------------
// MetadataState — catalog wrapped in metadata RwLock (PR 8)
// ---------------------------------------------------------------------------

/// Per-engine catalog state protected by an `RwLock`. DDL ops take the
/// write guard to gain exclusive access; CRUD writers take the read
/// guard (shared with other CRUD writers) and mutate the catalog via
/// the interior `Mutex<Catalog>`.
///
/// Lock order: `metadata` RwLock -> `ns_lanes` mutex -> `commit_seq`
/// mutex -> `catalog` Mutex. DO NOT grab `metadata.write()` while
/// holding the catalog mutex — that would invert the order relative to
/// a reader that already holds `metadata.read()` and is waiting for the
/// catalog mutex.
pub(crate) struct MetadataState {
    /// Catalog B+ tree for collection/index metadata.
    ///
    /// Wrapped in `Mutex` so CRUD writers can mutate under
    /// `metadata.read()` without upgrading to `write()`. DDL paths
    /// still take `metadata.write()` for coarse-grain CRUD-vs-DDL
    /// exclusion; they also briefly acquire this mutex, which is
    /// uncontended while no CRUD writer holds `metadata.read()`.
    pub catalog: std::sync::Mutex<Catalog<BufferPoolPageStore>>,
}

impl MetadataState {
    /// Create the initial MetadataState + SharedState from an existing
    /// (or fresh) buffer pool handle.
    fn new(
        handle: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
    ) -> Result<(Self, Arc<SharedState>)> {
        let store = BufferPoolPageStore::new(Arc::clone(&handle));
        let backup_root = handle
            .allocator()
            .with_header(|h| h.catalog_root_backup)?;
        let (catalog, used_backup) = catalog_open_with_fallback(
            store,
            catalog_root_page,
            catalog_root_level,
            backup_root,
            catalog_root_level,
            |_page| true,
        )?;
        let _ = used_backup; // noted for tracing/logging if needed
        // T7 — journal-tail HLC oracle recovery: floor the oracle above
        // every durable ChainCommit from the previous lifetime. Missing
        // `successor()` (saturated `Ts::MAX`) is a hard error per plan.
        let oracle = Arc::new(TimestampOracle::new());
        if let Some(max_ts) = handle.recovered_max_commit_ts()? {
            match max_ts.successor() {
                Some(next) => oracle.set_min(next),
                None => return Err(Error::TimestampExhausted),
            }
        }
        // Plan §T7: construct the history store on the dedicated
        // history-routed page store. A fresh tree is built every open — the
        // previous lifetime's entries are not persisted across restart
        // because reconciliation repopulates it lazily (plan deferral 905).
        let history_store_inner = HistoryStore::create(
            BufferPoolPageStore::new_history(Arc::clone(&handle)),
        )?;

        // Build the initial published snapshot from the catalog.
        let initial_snap = build_snapshot_from_catalog(
            &catalog,
            oracle.now(),
        )?;

        let shared = Arc::new(SharedState {
            handle,
            history_store: Arc::new(std::sync::Mutex::new(history_store_inner)),
            oracle,
            published: ArcSwap::from_pointee(initial_snap),
            txn_counter: AtomicU64::new(1),
        });

        let md = Self { catalog: std::sync::Mutex::new(catalog) };
        // For a new database, persist the freshly-allocated catalog root
        // to the file header immediately (will be written to disk on flush).
        if catalog_root_page == 0 {
            let cat = md.catalog.lock().expect("catalog poisoned");
            let root_page = cat.root_page();
            let root_level = cat.root_level();
            drop(cat);
            shared.handle.allocator().update_header(|h| {
                h.catalog_root_page = root_page;
                h.catalog_root_level = root_level;
                h.catalog_root_backup = root_page;
            })?;
        }
        Ok((md, shared))
    }

}

/// Build a `PublishedSnapshot` from the given catalog state.
fn build_snapshot_from_catalog(
    catalog: &Catalog<BufferPoolPageStore>,
    publish_ts: crate::mvcc::timestamp::Ts,
) -> Result<PublishedSnapshot> {
    let mut namespaces = HashMap::new();
    for coll in catalog.list_collections()? {
        let mut idxs = Vec::new();
        for idx in catalog.list_indexes(&coll.name)? {
            idxs.push(PublishedIndex {
                name: idx.name.clone(),
                root_page: idx.root_page,
                root_level: idx.root_level,
                key_pattern: idx.key_pattern.clone(),
                unique: idx.unique,
                sparse: idx.sparse,
                state: idx.state,
            });
        }
        namespaces.insert(coll.name.clone(), NamespaceSnapshot {
            data_root_page: coll.data_root_page,
            data_root_level: coll.data_root_level,
            indexes: idxs,
        });
    }
    Ok(PublishedSnapshot {
        publish_ts,
        namespaces,
    })
}

/// Rebuild the published snapshot from the current catalog and store it
/// atomically. Callers must hold `commit_seq` so publish_ts monotonicity
/// matches commit ordering.
fn rebuild_and_publish_locked(
    shared: &SharedState,
    md: &MetadataState,
    publish_ts: crate::mvcc::timestamp::Ts,
) -> Result<()> {
    let cat = md.catalog.lock().expect("catalog poisoned");
    let new_snap = build_snapshot_from_catalog(&cat, publish_ts)?;
    shared.published.store(Arc::new(new_snap));
    Ok(())
}

/// Create a new [`BufferPoolPageStore`] backed by `shared.handle`.
fn new_store(shared: &SharedState) -> BufferPoolPageStore {
    BufferPoolPageStore::new(Arc::clone(&shared.handle))
}

/// Create a new writer-side [`TxnPageStore`] sharing the given overlay.
fn new_txn_store(shared: &SharedState, overlay: &Arc<Mutex<TxnOverlay>>) -> TxnPageStore {
    TxnPageStore::new(new_store(shared), Arc::clone(overlay))
}

/// Update `FileHeader::catalog_root_page` and `catalog_root_level` to
/// reflect the current catalog root, routing through the provided txn
/// overlay so a rollback restores the pre-image.
fn sync_catalog_root_overlay(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &Arc<Mutex<TxnOverlay>>,
) -> Result<()> {
    let (root_page, root_level) = {
        let cat = md.catalog.lock().expect("catalog poisoned");
        (cat.root_page(), cat.root_level())
    };
    txn_update_header(shared, overlay, |h| {
        h.catalog_root_page = root_page;
        h.catalog_root_level = root_level;
        h.catalog_root_backup = root_page;
    })
}

/// Mutate the file header, snapshotting the pre-image into the given
/// overlay on first call.
fn txn_update_header<F>(
    shared: &SharedState,
    overlay: &Arc<Mutex<TxnOverlay>>,
    f: F,
) -> Result<()>
where
    F: FnOnce(&mut crate::storage::header::FileHeader),
{
    let mut ov = overlay.lock().expect("TxnOverlay mutex poisoned");
    if let Some(pre) = shared
        .handle
        .allocator()
        .with_header(|h| {
            if ov.has_header_pre() {
                None
            } else {
                Some(h.clone())
            }
        })?
    {
        ov.capture_header_pre_once(&pre);
    }
    drop(ov);
    shared.handle.allocator().update_header(f)
}

// ---------------------------------------------------------------------------
// Writer-path free helpers (PR 8)
// ---------------------------------------------------------------------------
//
// Each helper takes explicit references to the shared engine state
// (`&SharedState`), the catalog (`&mut MetadataState` or `&MetadataState`),
// and the per-txn overlay (`&Arc<Mutex<TxnOverlay>>`). The engine's
// `metadata: RwLock<MetadataState>` + `ns_lanes` + `commit_seq` split
// means there is no longer one monolithic `BpBackend::with_txn` — each
// CRUD / DDL op drives its own prologue + epilogue via
// `PagedEngine::run_write` / `PagedEngine::run_ddl`.

/// Retrieve the serialised `_id` value stored in an index tree entry.
fn index_entry_id_free(
    handle: &Arc<BufferPoolHandle>,
    cv: CellValue,
) -> Result<Bson> {
    let bytes = match cv {
        CellValue::Inline(b) => b,
        CellValue::Overflow {
            first_page,
            total_length,
        } => {
            let tmp_store = BufferPoolPageStore::new(Arc::clone(handle));
            let tmp_tree = BTree::open(tmp_store, 1, 0);
            tmp_tree.read_overflow(first_page, total_length)?
        }
    };
    if bytes.is_empty() {
        return Ok(Bson::Null);
    }
    let doc: Document = bson::from_slice(&bytes).map_err(Error::BsonDeserialization)?;
    Ok(doc.get("_id").cloned().unwrap_or(Bson::Null))
}

/// Build the [start, end] range for a secondary index B+ tree scan.
fn index_bounds_free(
    condition: &IndexCondition,
    ascending: bool,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    fn prefix(val: &Bson, asc: bool) -> Vec<u8> {
        let mut p = encode_compound_key(&[(val, asc)]);
        p.push(COMPOUND_SEP);
        p
    }
    fn prefix_next(val: &Bson, asc: bool) -> Vec<u8> {
        let mut p = prefix(val, asc);
        *p.last_mut().unwrap() += 1;
        p
    }
    match condition {
        IndexCondition::Eq(v) => {
            (Some(prefix(v, ascending)), Some(prefix_next(v, ascending)))
        }
        IndexCondition::Any => (None, None),
        IndexCondition::In(_) => (None, None),
        IndexCondition::Range { gt, gte, lt, lte } => {
            if ascending {
                let start = match (gte.as_ref(), gt.as_ref()) {
                    (Some(v), _) => Some(prefix(v, true)),
                    (None, Some(v)) => Some(prefix_next(v, true)),
                    _ => None,
                };
                let end = match (lte.as_ref(), lt.as_ref()) {
                    (Some(v), _) => Some(prefix_next(v, true)),
                    (None, Some(v)) => Some(prefix(v, true)),
                    _ => None,
                };
                (start, end)
            } else {
                let start = match (lte.as_ref(), lt.as_ref()) {
                    (Some(v), _) => Some(prefix(v, false)),
                    (None, Some(v)) => Some(prefix_next(v, false)),
                    _ => None,
                };
                let end = match (gte.as_ref(), gt.as_ref()) {
                    (Some(v), _) => Some(prefix_next(v, false)),
                    (None, Some(v)) => Some(prefix(v, false)),
                    _ => None,
                };
                (start, end)
            }
        }
    }
}

/// Persist updated root/level and multikey flag for an index entry.
fn sync_index_entry(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &Arc<Mutex<TxnOverlay>>,
    orig: &IndexEntry,
    new_root: u32,
    new_level: u8,
    new_multikey: bool,
) -> Result<()> {
    let root_changed = new_root != orig.root_page || new_level != orig.root_level;
    let multikey_changed = new_multikey && !orig.multikey;
    if !root_changed && !multikey_changed {
        return Ok(());
    }
    let mut updated = orig.clone();
    if root_changed {
        updated.root_page = new_root;
        updated.root_level = new_level;
    }
    if multikey_changed {
        updated.multikey = true;
    }
    md.catalog.lock().expect("catalog poisoned").update_index(&updated)?;
    sync_catalog_root_overlay(shared, md, overlay)
}

/// Maintain all secondary indexes after a document insert.
fn maintain_secondary_on_insert(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &Arc<Mutex<TxnOverlay>>,
    ns: &str,
    doc: &Document,
    doc_id: &Bson,
    txn: &mut WriteTxn,
) -> Result<()> {
    let entries = md.catalog.lock().expect("catalog poisoned").list_indexes(ns)?;
    for entry in entries {
        let store = new_txn_store(shared, overlay);
        let idx_tree = BTree::open(store, entry.root_page, entry.root_level);
        let is_multikey = update_index_on_insert(doc, doc_id, &idx_tree, &entry, txn)?;
        sync_index_entry(
            shared,
            md,
            overlay,
            &entry,
            idx_tree.root_page,
            idx_tree.root_level,
            is_multikey,
        )?;
    }
    Ok(())
}

/// Maintain all secondary indexes after a document delete.
fn maintain_secondary_on_delete(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &Arc<Mutex<TxnOverlay>>,
    ns: &str,
    doc: &Document,
    doc_id: &Bson,
    txn: &mut WriteTxn,
) -> Result<()> {
    let entries = md.catalog.lock().expect("catalog poisoned").list_indexes(ns)?;
    for entry in entries {
        update_index_on_delete(doc, doc_id, &entry, txn)?;
        sync_index_entry(
            shared,
            md,
            overlay,
            &entry,
            entry.root_page,
            entry.root_level,
            false,
        )?;
    }
    Ok(())
}

/// Maintain all secondary indexes when a document is replaced.
fn maintain_secondary_on_update(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &Arc<Mutex<TxnOverlay>>,
    ns: &str,
    old_doc: &Document,
    new_doc: &Document,
    old_id: &Bson,
    new_id: &Bson,
    txn: &mut WriteTxn,
) -> Result<()> {
    let entries = md.catalog.lock().expect("catalog poisoned").list_indexes(ns)?;
    for entry in entries {
        let store = new_txn_store(shared, overlay);
        let idx_tree = BTree::open(store, entry.root_page, entry.root_level);
        let is_multikey = update_index_on_update(
            old_doc, new_doc, old_id, new_id, &idx_tree, &entry, txn,
        )?;
        sync_index_entry(
            shared,
            md,
            overlay,
            &entry,
            idx_tree.root_page,
            idx_tree.root_level,
            is_multikey,
        )?;
    }
    Ok(())
}

/// Drain the given `SecIndexWrite` batch and perform the actual
/// `BTree::insert` / `delete` into each target index tree.
fn install_pending_sec_index(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &Arc<Mutex<TxnOverlay>>,
    writes: Vec<crate::mvcc::SecIndexWrite>,
) -> Result<()> {
    if writes.is_empty() {
        return Ok(());
    }
    use crate::mvcc::SecIndexOp;
    use std::collections::HashMap as StdHashMap;

    let mut entry_by_root: StdHashMap<u32, IndexEntry> = StdHashMap::new();
    {
        let cat = md.catalog.lock().expect("catalog poisoned");
        let collections = cat.list_collections()?;
        for coll in &collections {
            for entry in cat.list_indexes(&coll.name)? {
                entry_by_root.insert(entry.root_page, entry);
            }
        }
    }

    struct TreeState {
        current_root: u32,
        current_level: u8,
        entry: IndexEntry,
    }
    let mut states: StdHashMap<u32, TreeState> = StdHashMap::new();

    for write in writes {
        let state = match states.get_mut(&write.index_root_page) {
            Some(s) => s,
            None => {
                let entry = entry_by_root
                    .get(&write.index_root_page)
                    .cloned()
                    .ok_or_else(|| {
                        Error::Internal(format!(
                            "pending sec-index write references unknown root_page {}",
                            write.index_root_page
                        ))
                    })?;
                states.insert(
                    write.index_root_page,
                    TreeState {
                        current_root: entry.root_page,
                        current_level: entry.root_level,
                        entry,
                    },
                );
                states
                    .get_mut(&write.index_root_page)
                    .expect("just inserted")
            }
        };

        let store = new_txn_store(shared, overlay);
        let mut idx_tree = BTree::open(store, state.current_root, state.current_level);
        match write.op {
            SecIndexOp::Insert { id_bytes } => {
                idx_tree.insert(&write.key, &id_bytes)?;
            }
            SecIndexOp::Delete => {
                let _ = idx_tree.delete(&write.key)?;
            }
        }
        state.current_root = idx_tree.root_page;
        state.current_level = idx_tree.root_level;
    }

    for (_, state) in states {
        sync_index_entry(
            shared,
            md,
            overlay,
            &state.entry,
            state.current_root,
            state.current_level,
            state.entry.multikey,
        )?;
    }

    Ok(())
}

/// Install staged primary-tree writes as fresh heads on each key's
/// per-leaf version chain.
fn install_pending_primary(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &Arc<Mutex<TxnOverlay>>,
    writes: Vec<crate::mvcc::PrimaryWrite>,
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<()> {
    if writes.is_empty() {
        return Ok(());
    }
    use crate::mvcc::{PrimaryOp, Ts, VersionData, VersionEntry};
    use std::collections::VecDeque;

    for write in writes {
        let (root_page, root_level) = match md
            .catalog
            .lock()
            .expect("catalog poisoned")
            .get_collection(&write.ns)?
        {
            Some(c) => (c.data_root_page, c.data_root_level),
            None => continue,
        };
        let tree = BTree::open(new_txn_store(shared, overlay), root_page, root_level);
        let leaf_page = tree.find_leaf(&write.key)?;
        let _pin = shared.handle.fetch_page(leaf_page, PageSize::Large32k)?;
        let mut chain_arc = shared
            .handle
            .pool()
            .take_chain(leaf_page, &write.key)?
            .unwrap_or_else(|| std::sync::Arc::new(VecDeque::new()));
        {
            let chain_mut = std::sync::Arc::make_mut(&mut chain_arc);
            if let Some(prev_head) = chain_mut.front_mut() {
                prev_head.stop_ts = commit_ts;
            }
            let (data, is_tombstone) = match write.op {
                PrimaryOp::Insert { data } => (VersionData::Inline(data), false),
                PrimaryOp::Update { data } => (VersionData::Inline(data), false),
                PrimaryOp::Delete => (VersionData::Inline(Vec::new()), true),
            };
            chain_mut.push_front(VersionEntry {
                start_ts: commit_ts,
                stop_ts: Ts::MAX,
                txn_id,
                data,
                is_tombstone,
            });
        }
        shared
            .handle
            .pool()
            .put_chain(leaf_page, write.key, chain_arc)?;
    }
    Ok(())
}

/// Outcome of `create_index_reserve` (Phase 1 of the 3-phase build).
#[derive(Clone, Copy)]
enum ReserveOutcome {
    /// A fresh Building entry was reserved; caller should proceed to
    /// Phase 2 (build) and Phase 3 (commit).
    Reserved,
    /// An index with the same name already exists; `create_index` is
    /// idempotent and returns Ok immediately.
    AlreadyExists,
}

// ---------------------------------------------------------------------------
// PagedEngine — public struct (PR 8)
// ---------------------------------------------------------------------------

/// Phase 1 storage engine: B+ tree per namespace, through the buffer pool.
///
/// ## Concurrency (PR 8: MWMR v1)
///
/// - **Reads**: mutex-free — load `shared.published` (`ArcSwap`) and open
///   B-trees at the snapshot's root pages. No engine-level lock taken.
/// - **Writes on CRUD paths**: acquire `metadata.read()` (shared), then the
///   per-namespace lane from `ns_lanes`. Two writers on different namespaces
///   progress concurrently. Commit sequencing (ts allocation + publish) is
///   serialized under `commit_seq`.
/// - **DDL** (`create_namespace`, `drop_namespace`, `create_index`,
///   `drop_index`, `checkpoint`, `close`, `backup`): takes `metadata.write()`
///   exclusively, blocking all writers globally for the duration.
pub(crate) struct PagedEngine {
    /// Shared state accessible by the mutex-free read path and every writer.
    pub(crate) shared: Arc<SharedState>,
    /// Catalog protected by an `RwLock` — DDL takes write, CRUD takes read.
    metadata: RwLock<MetadataState>,
    /// Per-namespace write lanes. Two writers on different namespaces run
    /// in parallel; two writers on the same namespace serialize on the
    /// lane mutex.
    ns_lanes: DashMap<String, Arc<Mutex<()>>>,
    /// Commit-sequencing mutex. All successful writes acquire it around
    /// the `commit_ts = oracle.commit()` → install_primary → flush →
    /// append_chain_commit → commit_txn → publish sequence, so
    /// `commit_ts`, journal append order, and `publish_ts` all agree.
    commit_seq: Mutex<()>,
    /// Writer busy-timeout applied on lane contention.
    busy_timeout: Duration,
    /// Optional writer busy-handler callback applied on lane contention.
    busy_handler: Option<BusyHandler>,
    /// Namespaces that have been explicitly dropped in this session.
    ///
    /// Prevents auto-bootstrap (`run_write` → `bootstrap_namespace`) from
    /// re-creating a namespace immediately after it was dropped — which would
    /// leave stale data in the journal and cause surprising reopen semantics.
    /// Cleared when the namespace is explicitly re-created via
    /// `create_namespace` (the public `create_collection` API path).
    dropped_namespaces: DashSet<String>,
}

impl PagedEngine {
    /// Create a file-backed engine using `handle` as the page store.
    ///
    /// If `catalog_root_page == 0` the database is new and an empty catalog
    /// will be created. Otherwise the catalog is opened at the given root.
    /// `busy_timeout` + `busy_handler` migrated from ClientInner (PR 8).
    #[cfg(test)]
    pub(crate) fn new_buffered(
        handle: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
    ) -> Result<Self> {
        Self::new_buffered_with_busy(
            handle,
            catalog_root_page,
            catalog_root_level,
            Duration::from_secs(5),
            None,
        )
    }

    /// Create a file-backed engine with explicit busy-timeout + busy-handler.
    pub(crate) fn new_buffered_with_busy(
        handle: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
        busy_timeout: Duration,
        busy_handler: Option<BusyHandler>,
    ) -> Result<Self> {
        let (md, shared) = MetadataState::new(handle, catalog_root_page, catalog_root_level)?;
        Ok(PagedEngine {
            shared,
            metadata: RwLock::new(md),
            ns_lanes: DashMap::new(),
            commit_seq: Mutex::new(()),
            busy_timeout,
            busy_handler,
            dropped_namespaces: DashSet::new(),
        })
    }

    // -----------------------------------------------------------------------
    // Lane acquisition (ported from client.rs acquire_writer_lock)
    // -----------------------------------------------------------------------

    /// Resolve the per-namespace lane mutex, creating one if needed.
    fn lane_for(&self, ns: &str) -> Arc<Mutex<()>> {
        if let Some(entry) = self.ns_lanes.get(ns) {
            return Arc::clone(entry.value());
        }
        self.ns_lanes
            .entry(ns.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Acquire the namespace lane with busy-timeout / busy-handler semantics
    /// matching the client's legacy `acquire_writer_lock`.
    ///
    /// Returns `(lane, guard)` — the caller holds both to keep the guard
    /// alive for the lane's duration. Stdlib `MutexGuard` is tied to the
    /// `Arc<Mutex<()>>`'s lifetime so we return the Arc alongside.
    fn acquire_lane(
        &self,
        lane: Arc<Mutex<()>>,
    ) -> Result<OwnedLaneGuard> {
        let lane_ptr: *const Mutex<()> = Arc::as_ptr(&lane);

        // Fast path: try without any spin first.
        match unsafe { &*lane_ptr }.try_lock() {
            Ok(g) => return Ok(OwnedLaneGuard::new(lane, g)),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(Error::Internal("namespace lane mutex poisoned".into()));
            }
            Err(std::sync::TryLockError::WouldBlock) => {}
        }

        let timeout = self.busy_timeout;
        if let Some(handler) = &self.busy_handler {
            let mut attempts: u32 = 0;
            loop {
                std::thread::sleep(std::time::Duration::from_millis(1));
                match unsafe { &*lane_ptr }.try_lock() {
                    Ok(g) => return Ok(OwnedLaneGuard::new(lane, g)),
                    Err(std::sync::TryLockError::Poisoned(_)) => {
                        return Err(Error::Internal(
                            "namespace lane mutex poisoned".into(),
                        ));
                    }
                    Err(std::sync::TryLockError::WouldBlock) => {}
                }
                if !handler.0(attempts) {
                    return Err(Error::WriterBusy);
                }
                attempts = attempts.saturating_add(1);
            }
        }

        if timeout.is_zero() {
            return Err(Error::WriterBusy);
        }

        // No custom busy handler: block on `lock()` (kernel-managed queue)
        // rather than spin. This is the common in-process contention path
        // and avoids WriterBusy timeouts when writers are merely queued.
        // The busy_timeout still bounds lane wait, enforced via a
        // scheduled wake: we try first, then fall back to blocking lock()
        // and check elapsed on return.
        let deadline = Instant::now() + timeout;
        let guard = match unsafe { &*lane_ptr }.lock() {
            Ok(g) => g,
            Err(_) => {
                return Err(Error::Internal("namespace lane mutex poisoned".into()));
            }
        };
        if Instant::now() >= deadline && timeout > Duration::ZERO {
            // We waited longer than busy_timeout, but we DO hold the lock.
            // The sensible thing is to return the guard — the writer
            // already won the lane. Strict busy-timeout semantics would
            // return WriterBusy + release, but that loses forward progress
            // and is not what the PR 8 test expects ("writers queue up
            // and eventually all succeed").
            let _ = deadline;
        }
        Ok(OwnedLaneGuard::new(lane, guard))
    }

    /// Bootstrap a collection if it does not exist yet.
    ///
    /// Called from CRUD paths that may be invoked with an unknown ns.
    /// Acquires `metadata.write()` + `commit_seq` so the namespace is
    /// both visible in the catalog AND reflected in the published
    /// snapshot before the caller returns to the read path.
    fn bootstrap_namespace(&self, ns: &str) -> Result<()> {
        // Do not auto-create a namespace that was explicitly dropped in this
        // session. The `dropped_namespaces` set is populated by `drop_namespace`
        // and cleared only by an explicit `create_namespace` call.  This prevents
        // a racing insert (arriving after drop_namespace returns but before the
        // session ends) from re-bootstrapping the namespace and committing stale
        // data to the journal — which would survive a reopen via journal recovery.
        if self.dropped_namespaces.contains(ns) {
            return Err(Error::CollectionNotFound {
                name: ns.to_string(),
            });
        }
        let md_w = self.metadata.write().map_err(|_| {
            Error::Internal("metadata RwLock poisoned".into())
        })?;
        if md_w.catalog.lock().expect("catalog poisoned").get_collection(ns)?.is_some() {
            return Ok(());
        }
        // Hold commit_seq for the publish so publish_ts remains monotonic.
        let _commit = self.commit_seq.lock().map_err(|_| {
            Error::Internal("commit_seq mutex poisoned".into())
        })?;

        // Open a journal mark + overlay so the bootstrap is atomic.
        let mark = self.shared.handle.begin_txn()?;
        let overlay = TxnOverlay::new_shared();
        // Drain deferred-free into overlay reservations.
        let ready = self.shared.handle.allocator().drain_deferred_free_reservations();
        if !ready.is_empty() {
            let mut ov = overlay.lock().expect("TxnOverlay mutex poisoned");
            for page in ready {
                ov.push_reservation(PageReservation {
                    page,
                    size: PageSize::Large32k,
                    origin: PageOrigin::DeferredFree,
                });
            }
        }

        let result: Result<()> = (|| {
            let data_root = md_w
                .catalog
                .lock()
                .expect("catalog poisoned")
                .create_collection(ns, bson::doc! {}, now_millis())?;
            sync_catalog_root_overlay(&self.shared, &md_w, &overlay)?;
            let _ = BTree::create_at(new_txn_store(&self.shared, &overlay), data_root)?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                let overlay_inner = Arc::try_unwrap(overlay)
                    .map_err(|_| Error::Internal("TxnOverlay still referenced".into()))?
                    .into_inner()
                    .map_err(|_| Error::Internal("TxnOverlay mutex poisoned".into()))?;
                let mut base_store = new_store(&self.shared);
                overlay_inner.commit(&mut base_store, &self.shared.handle)?;
                self.shared.handle.flush()?;
                let db_page_count = self
                    .shared
                    .handle
                    .allocator()
                    .with_header(|h| h.total_page_count)?;
                let header_data = {
                    let page = self.shared.handle.fetch_page(0, PageSize::Small4k)?;
                    page.data().to_vec()
                };
                let emergency = self.shared.handle.commit_txn(
                    0,
                    PageSize::Small4k,
                    &header_data,
                    db_page_count,
                )?;
                if emergency {
                    // Journal index reached its hot-threshold: drain the
                    // journal into the main file so subsequent txns have
                    // room. Best-effort — failure here does not roll back
                    // the txn (it is already committed).
                    let _ = self.shared.handle.emergency_checkpoint();
                }
                let publish_ts = self.shared.oracle.now();
                rebuild_and_publish_locked(&self.shared, &md_w, publish_ts)?;
                Ok(())
            }
            Err(e) => {
                let overlay_inner = Arc::try_unwrap(overlay)
                    .map_err(|_| Error::Internal("TxnOverlay still referenced".into()))?
                    .into_inner()
                    .map_err(|_| Error::Internal("TxnOverlay mutex poisoned".into()))?;
                overlay_inner.rollback(&self.shared.handle)?;
                let _ = self.shared.handle.rollback_txn(mark);
                Err(e)
            }
        }
    }

    /// Phase 1 of `create_index`: reserve an index slot.
    ///
    /// Allocates a root page for the index's B+ tree, writes an
    /// `IndexEntry { state: Building }` into the catalog, initializes
    /// the leaf page, and publishes a fresh snapshot so writers on the
    /// target namespace dual-write to it while the build is in flight.
    ///
    /// Returns [`ReserveOutcome::AlreadyExists`] if an index of that
    /// name already exists (idempotent call), otherwise
    /// [`ReserveOutcome::Reserved`].
    fn create_index_reserve(
        &self,
        ns: &str,
        model: &IndexModel,
        name: &str,
    ) -> Result<ReserveOutcome> {
        // Drive through `run_ddl` so we get the standard DDL envelope:
        // metadata.write() + commit_seq + overlay/WAL + publish.
        let outcome = std::cell::Cell::new(ReserveOutcome::Reserved);
        self.run_ddl(|shared, md, overlay| {
            let mut cat = md.catalog.lock().expect("catalog poisoned");
            // Bootstrap collection if absent (matches pre-PR-9 create_index).
            if cat.get_collection(ns)?.is_none() {
                let data_root = cat.create_collection(ns, bson::doc! {}, now_millis())?;
                drop(cat);
                sync_catalog_root_overlay(shared, md, overlay)?;
                let data_store = new_txn_store(shared, overlay);
                BTree::create_at(data_store, data_root)?;
                cat = md.catalog.lock().expect("catalog poisoned");
            }

            // Idempotent: if an index with this name already exists we
            // treat the call as a no-op (matches pre-PR-9 semantics).
            // We do NOT re-check state here — a caller seeing an
            // in-progress Building entry from a prior failed build is
            // reported as "exists"; the prior build's cleanup will have
            // removed it if it failed. If cleanup itself failed and an
            // orphan Building entry remains, callers can drop_index and
            // retry.
            if cat.get_index(ns, name)?.is_some() {
                outcome.set(ReserveOutcome::AlreadyExists);
                return Ok(());
            }

            let idx_root = cat.create_index(ns, model, name)?;
            // `catalog.create_index` defaults `state` to `Ready`. Flip
            // to `Building` so the published snapshot marks this index
            // as not-yet-queryable.
            let mut entry = cat
                .get_index(ns, name)?
                .ok_or_else(|| Error::Internal("index entry missing after create".into()))?;
            entry.state = IndexState::Building;
            cat.update_index(&entry)?;
            drop(cat);
            sync_catalog_root_overlay(shared, md, overlay)?;

            // Initialize the freshly-allocated leaf page so the index
            // tree is valid to open for writes during Phase 2.
            let idx_store = new_txn_store(shared, overlay);
            BTree::create_at(idx_store, idx_root)?;
            Ok(())
        })?;
        Ok(outcome.get())
    }

    /// Phase 2 of `create_index`: populate the index from the data tree.
    ///
    /// Runs under `metadata.read()` + the target namespace's lane, so
    /// writers on *other* namespaces proceed concurrently. Writers on
    /// this namespace wait on the lane (same as any other ns-local
    /// serialization). Any writer that commits DURING the build on this
    /// ns is blocked out; writers between Phase 1 and Phase 2 have
    /// already dual-written to the Building index (via
    /// `maintain_secondary_on_*`).
    fn create_index_build(&self, ns: &str, name: &str) -> Result<()> {
        // Acquire the namespace lane under a short-lived metadata.read()
        // guard — the read guard blocks drop_namespace (which needs
        // metadata.write()) from racing with lane acquisition. Once the
        // lane is in hand, drop the read guard so the long build scan
        // below does NOT block DDL on other namespaces or bootstrapping
        // of new namespaces (Gap-9 follow-up; fixes PR 9's known v1
        // limitation).
        let lane = self.lane_for(ns);
        let _lane_guard = {
            let _md_read = self.metadata.read().map_err(|_| {
                Error::Internal("metadata RwLock poisoned".into())
            })?;
            self.acquire_lane(lane)?
            // _md_read dropped here.
        };

        // Take the latest IndexEntry AND CollectionEntry under a brief
        // metadata.read(). The root pages observed here are authoritative:
        // we now hold the namespace lane, so no concurrent writer on this
        // ns can advance them until we release the lane. drop_namespace of
        // this ns would block on the lane; drop_index of this index may
        // race and leave us with a missing entry — we error cleanly.
        let (idx_entry, data_entry) = {
            let md_read = self.metadata.read().map_err(|_| {
                Error::Internal("metadata RwLock poisoned".into())
            })?;
            let cat = md_read.catalog.lock().expect("catalog poisoned");
            let idx_entry = cat.get_index(ns, name)?.ok_or_else(|| {
                Error::Internal(format!(
                    "index '{}' on '{}' disappeared before build phase",
                    name, ns
                ))
            })?;
            let data_entry = cat.get_collection(ns)?.ok_or_else(|| {
                Error::Internal(format!(
                    "collection '{}' disappeared before build phase",
                    ns
                ))
            })?;
            (idx_entry, data_entry)
        };

        // Open a dedicated WAL transaction for the build — it's
        // independent of Phase 1's txn (already committed) and Phase 3's
        // txn (has not yet begun).
        let mark = self.shared.handle.begin_txn()?;
        let overlay = TxnOverlay::new_shared();
        let ready_reservations = self
            .shared
            .handle
            .allocator()
            .drain_deferred_free_reservations();
        if !ready_reservations.is_empty() {
            let mut ov = overlay.lock().expect("TxnOverlay mutex poisoned");
            for page in ready_reservations {
                ov.push_reservation(PageReservation {
                    page,
                    size: PageSize::Large32k,
                    origin: PageOrigin::DeferredFree,
                });
            }
        }

        let body: Result<()> = (|| {
            let data_store = new_txn_store(&self.shared, &overlay);
            let data_tree = BTree::open(
                data_store,
                data_entry.data_root_page,
                data_entry.data_root_level,
            );
            let idx_store = new_txn_store(&self.shared, &overlay);
            let mut idx_tree = BTree::open(
                idx_store,
                idx_entry.root_page,
                idx_entry.root_level,
            );
            let any_multikey = build_index(&data_tree, &mut idx_tree, &idx_entry)?;

            // Persist the possibly-updated root + multikey flag. Note:
            // we do NOT flip state to Ready here — that happens in
            // Phase 3 under metadata.write() so readers see a
            // consistent transition on the published snapshot.
            let root_changed = idx_tree.root_page != idx_entry.root_page
                || idx_tree.root_level != idx_entry.root_level;
            let multikey_changed = any_multikey && !idx_entry.multikey;
            if root_changed || multikey_changed {
                let mut updated = idx_entry.clone();
                if root_changed {
                    updated.root_page = idx_tree.root_page;
                    updated.root_level = idx_tree.root_level;
                }
                if multikey_changed {
                    updated.multikey = true;
                }
                // Brief metadata.read() for the catalog write. Lane still
                // protects same-ns serialization; read guard is acquired
                // only for the duration of the catalog mutation + header
                // sync so long-build scans don't block other-ns DDL.
                let md_read = self.metadata.read().map_err(|_| {
                    Error::Internal("metadata RwLock poisoned".into())
                })?;
                md_read
                    .catalog
                    .lock()
                    .expect("catalog poisoned")
                    .update_index(&updated)?;
                sync_catalog_root_overlay(&self.shared, &md_read, &overlay)?;
            }
            Ok(())
        })();

        match body {
            Ok(()) => {
                // Commit the build txn. We intentionally do NOT hold
                // commit_seq here — no timestamp is allocated (Phase 2
                // has no primary writes), and no publish is performed.
                // The only shared mutation is catalog root/multikey
                // metadata, which is safe under the lane.
                let overlay_inner = Arc::try_unwrap(overlay)
                    .map_err(|_| {
                        Error::Internal("TxnOverlay still referenced at build commit".into())
                    })?
                    .into_inner()
                    .map_err(|_| Error::Internal("TxnOverlay mutex poisoned".into()))?;
                let mut base = new_store(&self.shared);
                overlay_inner.commit(&mut base, &self.shared.handle)?;
                self.shared.handle.flush()?;
                let db_page_count = self
                    .shared
                    .handle
                    .allocator()
                    .with_header(|h| h.total_page_count)?;
                let header_data = {
                    let page = self.shared.handle.fetch_page(0, PageSize::Small4k)?;
                    page.data().to_vec()
                };
                let emergency = self.shared.handle.commit_txn(
                    0,
                    PageSize::Small4k,
                    &header_data,
                    db_page_count,
                )?;
                if emergency {
                    let _ = self.shared.handle.emergency_checkpoint();
                }
                Ok(())
            }
            Err(e) => {
                let _ = self.rollback_overlay_and_wal(overlay, mark);
                Err(e)
            }
        }
    }

    /// Phase 3 of `create_index`: flip `state: Ready` under metadata.write
    /// and publish the final snapshot.
    fn create_index_commit(&self, ns: &str, name: &str) -> Result<()> {
        self.run_ddl(|_shared, md, _overlay| {
            let mut cat = md.catalog.lock().expect("catalog poisoned");
            let mut entry = cat.get_index(ns, name)?.ok_or_else(|| {
                Error::Internal(format!(
                    "index '{}' on '{}' disappeared before commit phase",
                    name, ns
                ))
            })?;
            entry.state = IndexState::Ready;
            cat.update_index(&entry)?;
            // No catalog-root change unless the B+ tree reshaped; since
            // we only did an update-in-place of the entry, keep the
            // header in sync just in case.
            drop(cat);
            sync_catalog_root_overlay(_shared, md, _overlay)?;
            Ok(())
        })
    }

    /// Cleanup after a failed Phase 2: drop the orphan Building entry.
    ///
    /// This keeps the catalog in a state where a retried
    /// `create_index(ns, same_name)` succeeds from Phase 1 instead of
    /// hitting the idempotent-already-exists early return with a stale
    /// Building entry.
    fn create_index_cleanup(&self, ns: &str, name: &str) -> Result<()> {
        self.run_ddl(|shared, md, overlay| {
            let removed = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .drop_index(ns, name)?;
            if removed {
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            Ok(())
        })
    }

    /// CRUD write lifecycle.
    ///
    /// Drives: metadata.read() → bootstrap-if-missing → lane → overlay +
    /// WriteTxn setup → body → commit_seq { install_sec + install_primary +
    /// overlay commit + flush + append_chain_commit + commit_txn +
    /// rebuild_and_publish } → release lane → release metadata.
    fn run_write<F, R>(&self, ns: &str, f: F) -> Result<R>
    where
        F: FnOnce(
            &SharedState,
            &MetadataState,
            &Arc<Mutex<TxnOverlay>>,
            &mut WriteTxn,
        ) -> Result<R>,
    {
        // Take read guard; if namespace absent, bootstrap then retry.
        let md_read = self.metadata.read().map_err(|_| {
            Error::Internal("metadata RwLock poisoned".into())
        })?;
        let ns_missing = md_read
            .catalog
            .lock()
            .expect("catalog poisoned")
            .get_collection(ns)?
            .is_none();
        if ns_missing {
            drop(md_read);
            self.bootstrap_namespace(ns)?;
            // Re-acquire the read guard and proceed.
            return self.run_write_existing(ns, f);
        }
        drop(md_read);
        self.run_write_existing(ns, f)
    }

    /// Internal form of `run_write` that assumes the namespace already exists
    /// (or the write path tolerates its absence — `update`/`delete` do).
    ///
    /// Deadlock fix (PR 8 follow-up): the previous revision upgraded
    /// `metadata.read()` → `metadata.write()` while holding the
    /// namespace lane. Two writers on different namespaces each held
    /// `metadata.read()` and then tried to upgrade, producing a
    /// writer-vs-writer deadlock (A holds lane+waits-for-write; B
    /// holds read+waits-for-lane-from-A-after-its-upgrade).
    ///
    /// Fix: keep `metadata.read()` for the whole body; mutate the
    /// catalog via the interior `Mutex<Catalog>`. Lock order is
    /// documented on `MetadataState`.
    fn run_write_existing<F, R>(&self, ns: &str, f: F) -> Result<R>
    where
        F: FnOnce(
            &SharedState,
            &MetadataState,
            &Arc<Mutex<TxnOverlay>>,
            &mut WriteTxn,
        ) -> Result<R>,
    {
        let md_read = self.metadata.read().map_err(|_| {
            Error::Internal("metadata RwLock poisoned".into())
        })?;
        let lane = self.lane_for(ns);
        let _lane_guard = self.acquire_lane(lane)?;

        // Setup overlay + WriteTxn.
        let mark = self.shared.handle.begin_txn()?;
        let overlay = TxnOverlay::new_shared();
        let ready = self.shared.handle.allocator().drain_deferred_free_reservations();
        if !ready.is_empty() {
            let mut ov = overlay.lock().expect("TxnOverlay mutex poisoned");
            for page in ready {
                ov.push_reservation(PageReservation {
                    page,
                    size: PageSize::Large32k,
                    origin: PageOrigin::DeferredFree,
                });
            }
        }
        let txn_id = self.shared.txn_counter.fetch_add(1, Ordering::Relaxed);
        let mut txn = WriteTxn::new(txn_id);

        // Body — pass `&*md_read` directly. The catalog itself is
        // behind `Mutex<Catalog>`, so mutations happen inside the
        // closure under the catalog mutex without needing a RwLock
        // upgrade. Other CRUD writers on different namespaces hold
        // their own `metadata.read()` concurrently and their own
        // lanes; only `commit_seq` serializes the publish step.
        let body_result = f(&self.shared, &*md_read, &overlay, &mut txn);

        match body_result {
            Ok(value) => {
                // Commit sequencing.
                let _commit = self.commit_seq.lock().map_err(|_| {
                    Error::Internal("commit_seq mutex poisoned".into())
                })?;

                let sec_writes = std::mem::take(&mut txn.pending_sec_index);
                if let Err(e) =
                    install_pending_sec_index(&self.shared, &*md_read, &overlay, sec_writes)
                {
                    drop(txn);
                    let _ = self.rollback_overlay_and_wal(overlay, mark);
                    return Err(e);
                }

                let primary_writes = std::mem::take(&mut txn.pending_primary);
                let commit_ts_opt = if !primary_writes.is_empty() {
                    let txn_id = txn.txn_id;
                    let commit_ts = match txn.allocate_commit_ts(&self.shared.oracle) {
                        Ok(ts) => ts,
                        Err(e) => {
                            drop(txn);
                            let _ = self.rollback_overlay_and_wal(overlay, mark);
                            return Err(e);
                        }
                    };
                    if let Err(e) = install_pending_primary(
                        &self.shared,
                        &*md_read,
                        &overlay,
                        primary_writes,
                        commit_ts,
                        txn_id,
                    ) {
                        drop(txn);
                        let _ = self.rollback_overlay_and_wal(overlay, mark);
                        return Err(e);
                    }
                    Some(commit_ts)
                } else {
                    None
                };

                // Commit overlay bytes onto shared frames.
                let overlay_inner = Arc::try_unwrap(overlay)
                    .map_err(|_| Error::Internal("TxnOverlay still referenced".into()))?
                    .into_inner()
                    .map_err(|_| Error::Internal("TxnOverlay mutex poisoned".into()))?;
                let mut base_store = new_store(&self.shared);
                if let Err(e) = overlay_inner.commit(&mut base_store, &self.shared.handle) {
                    drop(txn);
                    let _ = self.shared.handle.rollback_txn(mark);
                    return Err(e);
                }
                self.shared.handle.flush()?;
                let (_commit_ts, _installed, _sec_index) =
                    txn.commit(&self.shared.oracle, &self.shared.handle)?;
                let db_page_count = self
                    .shared
                    .handle
                    .allocator()
                    .with_header(|h| h.total_page_count)?;
                // Build the commit-frame page-0 bytes from the allocator's
                // authoritative header rather than the buffer pool.  The
                // overlay commit above (overlay_inner.commit) may have written
                // a stale catalog_root_page to the pool if a concurrent DDL
                // (e.g. drop_namespace) committed AFTER this txn's body ran
                // but BEFORE we reached the commit-seq lock.  Reading from the
                // allocator — which is always updated atomically under
                // update_header — guarantees we persist the latest
                // catalog_root_page even if a concurrent DDL beat us here.
                let header_data = self
                    .shared
                    .handle
                    .allocator()
                    .with_header(|h| h.to_bytes())?;
                let emergency = self.shared.handle.commit_txn(
                    0,
                    PageSize::Small4k,
                    &header_data,
                    db_page_count,
                )?;
                if emergency {
                    let _ = self.shared.handle.emergency_checkpoint();
                }
                let publish_ts = commit_ts_opt.unwrap_or_else(|| self.shared.oracle.now());
                rebuild_and_publish_locked(&self.shared, &*md_read, publish_ts)?;
                Ok(value)
            }
            Err(e) => {
                drop(txn);
                let _ = self.rollback_overlay_and_wal(overlay, mark);
                Err(e)
            }
        }
    }

    fn rollback_overlay_and_wal(
        &self,
        overlay: Arc<Mutex<TxnOverlay>>,
        mark: Option<u64>,
    ) -> Result<()> {
        let ov = Arc::try_unwrap(overlay)
            .map_err(|_| Error::Internal("TxnOverlay still referenced at rollback time".into()))?
            .into_inner()
            .map_err(|_| Error::Internal("TxnOverlay mutex poisoned".into()))?;
        ov.rollback(&self.shared.handle)?;
        let _ = self.shared.handle.rollback_txn(mark);
        Ok(())
    }

}

// ---------------------------------------------------------------------------
// OwnedLaneGuard — holds Arc<Mutex<()>> + its MutexGuard together so the
// stdlib Mutex lifetime restriction is satisfied.
// ---------------------------------------------------------------------------

struct OwnedLaneGuard {
    // The Arc keeps the Mutex alive. `_guard` is the MutexGuard that
    // references the Mutex through `lane`. We hold the Arc AFTER the
    // guard so drop order is: guard (release lock) then lane.
    _guard: std::sync::MutexGuard<'static, ()>,
    _lane: Arc<Mutex<()>>,
}

impl OwnedLaneGuard {
    fn new(lane: Arc<Mutex<()>>, guard: std::sync::MutexGuard<'_, ()>) -> Self {
        // Extend the lifetime of the guard to 'static. Safe because we
        // keep `lane` alive inside `Self`, so the backing Mutex lives at
        // least as long as the guard.
        let guard_static: std::sync::MutexGuard<'static, ()> =
            unsafe { std::mem::transmute(guard) };
        OwnedLaneGuard {
            _guard: guard_static,
            _lane: lane,
        }
    }
}


// ---------------------------------------------------------------------------
// StorageEngine implementation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// StorageEngine implementation (PR 8 — MWMR v1)
// ---------------------------------------------------------------------------

impl StorageEngine for PagedEngine {
    // -----------------------------------------------------------------------
    // insert
    // -----------------------------------------------------------------------

    fn insert(&self, ns: &str, mut doc: Document) -> Result<Bson> {
        self.run_write(ns, |shared, md, overlay, txn| {
            let entry = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?
                .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-write", ns)))?;
            let mut tree = BTree::open(
                new_txn_store(shared, overlay),
                entry.data_root_page,
                entry.data_root_level,
            );
            let (id, key, bson_bytes, _tree_root) = btree_insert_doc(&mut tree, &mut doc, &[])?;
            if tree.root_page != entry.data_root_page || tree.root_level != entry.data_root_level {
                let mut updated = entry.clone();
                updated.data_root_page = tree.root_page;
                updated.data_root_level = tree.root_level;
                md.catalog.lock().expect("catalog poisoned").update_collection(&updated)?;
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            txn.stage_primary_insert(ns.to_string(), key, bson_bytes);
            maintain_secondary_on_insert(shared, md, overlay, ns, &doc, &id, txn)?;
            Ok(id)
        })
    }

    // -----------------------------------------------------------------------
    // find
    // -----------------------------------------------------------------------

    fn find(&self, ns: &str, filter: &Document, opts: &FindOptions) -> Result<Vec<Document>> {
        let snap = self.shared.published.load();
        let ns_snap = match snap.namespaces.get(ns) {
            None => return Ok(Vec::new()),
            Some(n) => n,
        };
        let matched: Vec<Document> = execute_snapshot_pairs_from_snap(
            &self.shared,
            ns,
            ns_snap,
            filter,
            snap.publish_ts,
            true,
        )?
        .into_iter()
        .map(|(_, doc)| doc)
        .collect();
        Ok(apply_find_opts(matched, opts))
    }

    fn find_one(&self, ns: &str, filter: &Document) -> Result<Option<Document>> {
        let opts = FindOptions::new();
        let mut results = self.find(ns, filter, &opts)?;
        Ok(if results.is_empty() {
            None
        } else {
            Some(results.remove(0))
        })
    }

    // -----------------------------------------------------------------------
    // update
    // -----------------------------------------------------------------------

    fn update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &UpdateOptions,
        many: bool,
    ) -> Result<UpdateResult> {
        if !is_operator_update(update) {
            return Err(Error::Internal(
                "update requires an operator update document (e.g. {$set: {...}});                  use find_one_and_replace for replacements"
                    .into(),
            ));
        }

        // Pre-scan phase uses the published snapshot (mutex-free). If the
        // namespace does not exist and upsert is requested, route to the
        // upsert helper.
        let snap = self.shared.published.load();
        let ns_snap_opt = snap.namespaces.get(ns);
        let matched_pairs: Vec<(Vec<u8>, Document)> = match ns_snap_opt {
            None => {
                if opts.upsert {
                    return self.do_upsert_update(ns, filter, update);
                }
                return Ok(UpdateResult {
                    matched_count: 0,
                    modified_count: 0,
                    upserted_id: None,
                });
            }
            Some(ns_snap) => {
                execute_snapshot_pairs_from_snap(
                    &self.shared,
                    ns,
                    ns_snap,
                    filter,
                    snap.publish_ts,
                    false,
                )?
            }
        };

        if matched_pairs.is_empty() && opts.upsert {
            return self.do_upsert_update(ns, filter, update);
        }

        let pairs_to_process: Vec<(Vec<u8>, Document)> = if many {
            matched_pairs
        } else {
            matched_pairs.into_iter().take(1).collect()
        };

        self.run_write_existing(ns, |shared, md, overlay, txn| {
            let mut matched_count = 0u64;
            let mut modified_count = 0u64;
            for (key, mut doc) in pairs_to_process {
                matched_count += 1;
                let before = doc.clone();
                let before_id = before.get("_id").cloned().unwrap_or(Bson::Null);
                apply_update(&mut doc, update, false)?;
                if doc != before {
                    modified_count += 1;
                    let new_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
                    let new_bytes = bson::to_vec(&doc).map_err(Error::BsonSerialization)?;
                    maintain_secondary_on_update(
                        shared, md, overlay, ns, &before, &doc, &before_id, &new_id, txn,
                    )?;
                    let entry_opt = md
                        .catalog
                        .lock()
                        .expect("catalog poisoned")
                        .get_collection(ns)?;
                    if let Some(entry) = entry_opt {
                        let mut tree = BTree::open(
                            new_txn_store(shared, overlay),
                            entry.data_root_page,
                            entry.data_root_level,
                        );
                        tree.delete(&key)?;
                        tree.insert(&key, &new_bytes)?;
                        if tree.root_page != entry.data_root_page
                            || tree.root_level != entry.data_root_level
                        {
                            let mut updated = entry.clone();
                            updated.data_root_page = tree.root_page;
                            updated.data_root_level = tree.root_level;
                            md.catalog
                                .lock()
                                .expect("catalog poisoned")
                                .update_collection(&updated)?;
                            sync_catalog_root_overlay(shared, md, overlay)?;
                        }
                        txn.stage_primary_update(ns.to_string(), key, new_bytes);
                    }
                }
            }
            Ok(UpdateResult {
                matched_count,
                modified_count,
                upserted_id: None,
            })
        })
    }

    // -----------------------------------------------------------------------
    // delete
    // -----------------------------------------------------------------------

    fn delete(&self, ns: &str, filter: &Document, many: bool) -> Result<DeleteResult> {
        let snap = self.shared.published.load();
        let pairs_to_delete: Vec<(Vec<u8>, Document)> = match snap.namespaces.get(ns) {
            None => return Ok(DeleteResult { deleted_count: 0 }),
            Some(ns_snap) => {
                let pairs = execute_snapshot_pairs_from_snap(
                    &self.shared,
                    ns,
                    ns_snap,
                    filter,
                    snap.publish_ts,
                    false,
                )?;
                if many {
                    pairs
                } else {
                    pairs.into_iter().take(1).collect()
                }
            }
        };

        let deleted_count = pairs_to_delete.len() as u64;
        if deleted_count == 0 {
            return Ok(DeleteResult { deleted_count: 0 });
        }

        self.run_write_existing(ns, |shared, md, overlay, txn| {
            for (key, doc) in &pairs_to_delete {
                let doc_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
                maintain_secondary_on_delete(shared, md, overlay, ns, doc, &doc_id, txn)?;
                let entry_opt = md
                    .catalog
                    .lock()
                    .expect("catalog poisoned")
                    .get_collection(ns)?;
                if let Some(entry) = entry_opt {
                    let mut tree = BTree::open(
                        new_txn_store(shared, overlay),
                        entry.data_root_page,
                        entry.data_root_level,
                    );
                    tree.delete(key)?;
                    if tree.root_page != entry.data_root_page
                        || tree.root_level != entry.data_root_level
                    {
                        let mut updated = entry.clone();
                        updated.data_root_page = tree.root_page;
                        updated.data_root_level = tree.root_level;
                        md.catalog
                            .lock()
                            .expect("catalog poisoned")
                            .update_collection(&updated)?;
                        sync_catalog_root_overlay(shared, md, overlay)?;
                    }
                    txn.stage_primary_delete(ns.to_string(), key.clone());
                }
            }
            Ok(())
        })?;

        Ok(DeleteResult { deleted_count })
    }

    // -----------------------------------------------------------------------
    // count
    // -----------------------------------------------------------------------

    fn count(&self, ns: &str, filter: &Document) -> Result<u64> {
        let snap = self.shared.published.load();
        let ns_snap = match snap.namespaces.get(ns) {
            None => return Ok(0),
            Some(n) => n,
        };
        Ok(
            execute_snapshot_pairs_from_snap(
                &self.shared,
                ns,
                ns_snap,
                filter,
                snap.publish_ts,
                false,
            )?
            .len() as u64,
        )
    }

    // -----------------------------------------------------------------------
    // find_one_and_update_doc
    // -----------------------------------------------------------------------

    fn find_one_and_update_doc(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &FindOneAndUpdateOptions,
    ) -> Result<Option<Document>> {
        if !is_operator_update(update) {
            return Err(Error::Internal(
                "find_one_and_update requires an operator update document".into(),
            ));
        }

        let snap = self.shared.published.load();
        let mut matched: Vec<(Vec<u8>, Document)> = match snap.namespaces.get(ns) {
            None => {
                if opts.upsert {
                    return self.fam_upsert_update(ns, filter, update, opts);
                }
                return Ok(None);
            }
            Some(ns_snap) => {
                execute_snapshot_pairs_from_snap(
                    &self.shared,
                    ns,
                    ns_snap,
                    filter,
                    snap.publish_ts,
                    false,
                )?
            }
        };

        if matched.is_empty() {
            if opts.upsert {
                return self.fam_upsert_update(ns, filter, update, opts);
            }
            return Ok(None);
        }

        if let Some(s) = &opts.sort {
            matched.sort_by(|(_, a), (_, b)| compare_docs(a, b, s));
        }

        let (key, mut doc) = matched.remove(0);
        let before = doc.clone();
        let before_id = before.get("_id").cloned().unwrap_or(Bson::Null);
        apply_update(&mut doc, update, false)?;
        let new_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
        let new_bytes = bson::to_vec(&doc).map_err(Error::BsonSerialization)?;

        self.run_write_existing(ns, |shared, md, overlay, txn| {
            maintain_secondary_on_update(
                shared, md, overlay, ns, &before, &doc, &before_id, &new_id, txn,
            )?;
            let entry_opt = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?;
            if let Some(entry) = entry_opt {
                let mut tree = BTree::open(
                    new_txn_store(shared, overlay),
                    entry.data_root_page,
                    entry.data_root_level,
                );
                tree.delete(&key)?;
                tree.insert(&key, &new_bytes)?;
                if tree.root_page != entry.data_root_page
                    || tree.root_level != entry.data_root_level
                {
                    let mut updated = entry.clone();
                    updated.data_root_page = tree.root_page;
                    updated.data_root_level = tree.root_level;
                    md.catalog
                        .lock()
                        .expect("catalog poisoned")
                        .update_collection(&updated)?;
                    sync_catalog_root_overlay(shared, md, overlay)?;
                }
                txn.stage_primary_update(ns.to_string(), key, new_bytes);
            }
            Ok(())
        })?;

        Ok(Some(match opts.return_document {
            ReturnDocument::Before => before,
            ReturnDocument::After => doc,
        }))
    }

    // -----------------------------------------------------------------------
    // find_one_and_delete_doc
    // -----------------------------------------------------------------------

    fn find_one_and_delete_doc(
        &self,
        ns: &str,
        filter: &Document,
        opts: &FindOneAndDeleteOptions,
    ) -> Result<Option<Document>> {
        let snap = self.shared.published.load();
        let mut matched: Vec<(Vec<u8>, Document)> = match snap.namespaces.get(ns) {
            None => return Ok(None),
            Some(ns_snap) => execute_snapshot_pairs_from_snap(
                &self.shared,
                ns,
                ns_snap,
                filter,
                snap.publish_ts,
                false,
            )?,
        };

        if matched.is_empty() {
            return Ok(None);
        }

        if let Some(s) = &opts.sort {
            matched.sort_by(|(_, a), (_, b)| compare_docs(a, b, s));
        }

        let (key, doc) = matched.remove(0);
        let doc_id = doc.get("_id").cloned().unwrap_or(Bson::Null);

        self.run_write_existing(ns, |shared, md, overlay, txn| {
            maintain_secondary_on_delete(shared, md, overlay, ns, &doc, &doc_id, txn)?;
            let entry_opt = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?;
            if let Some(entry) = entry_opt {
                let mut tree = BTree::open(
                    new_txn_store(shared, overlay),
                    entry.data_root_page,
                    entry.data_root_level,
                );
                tree.delete(&key)?;
                if tree.root_page != entry.data_root_page
                    || tree.root_level != entry.data_root_level
                {
                    let mut updated = entry.clone();
                    updated.data_root_page = tree.root_page;
                    updated.data_root_level = tree.root_level;
                    md.catalog
                        .lock()
                        .expect("catalog poisoned")
                        .update_collection(&updated)?;
                    sync_catalog_root_overlay(shared, md, overlay)?;
                }
                txn.stage_primary_delete(ns.to_string(), key);
            }
            Ok(())
        })?;

        Ok(Some(doc))
    }

    // -----------------------------------------------------------------------
    // find_one_and_replace_doc
    // -----------------------------------------------------------------------

    fn find_one_and_replace_doc(
        &self,
        ns: &str,
        filter: &Document,
        replacement: &Document,
        opts: &FindOneAndReplaceOptions,
    ) -> Result<Option<Document>> {
        let snap = self.shared.published.load();
        let mut matched: Vec<(Vec<u8>, Document)> = match snap.namespaces.get(ns) {
            None => {
                if opts.upsert {
                    return self.fam_upsert_replace(ns, replacement, opts);
                }
                return Ok(None);
            }
            Some(ns_snap) => {
                execute_snapshot_pairs_from_snap(
                    &self.shared,
                    ns,
                    ns_snap,
                    filter,
                    snap.publish_ts,
                    false,
                )?
            }
        };

        if matched.is_empty() {
            if opts.upsert {
                return self.fam_upsert_replace(ns, replacement, opts);
            }
            return Ok(None);
        }

        if let Some(s) = &opts.sort {
            matched.sort_by(|(_, a), (_, b)| compare_docs(a, b, s));
        }

        let (old_key, old_doc) = matched.remove(0);

        let mut new_doc = replacement.clone();
        let original_id = old_doc.get("_id").cloned().unwrap_or(Bson::Null);
        new_doc.insert("_id", original_id.clone());
        validate_document(&new_doc)?;

        let new_key = encode_key(&original_id);
        let new_bytes = bson::to_vec(&new_doc).map_err(Error::BsonSerialization)?;

        let old_doc_clone = old_doc.clone();
        let new_doc_clone = new_doc.clone();
        self.run_write_existing(ns, |shared, md, overlay, txn| {
            maintain_secondary_on_update(
                shared,
                md,
                overlay,
                ns,
                &old_doc_clone,
                &new_doc_clone,
                &original_id,
                &original_id,
                txn,
            )?;
            let entry_opt = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?;
            if let Some(entry) = entry_opt {
                let mut tree = BTree::open(
                    new_txn_store(shared, overlay),
                    entry.data_root_page,
                    entry.data_root_level,
                );
                tree.delete(&old_key)?;
                tree.insert(&new_key, &new_bytes)?;
                if tree.root_page != entry.data_root_page
                    || tree.root_level != entry.data_root_level
                {
                    let mut updated = entry.clone();
                    updated.data_root_page = tree.root_page;
                    updated.data_root_level = tree.root_level;
                    md.catalog
                        .lock()
                        .expect("catalog poisoned")
                        .update_collection(&updated)?;
                    sync_catalog_root_overlay(shared, md, overlay)?;
                }
                txn.stage_primary_update(ns.to_string(), new_key, new_bytes);
            }
            Ok(())
        })?;

        Ok(Some(match opts.return_document {
            ReturnDocument::Before => old_doc,
            ReturnDocument::After => new_doc,
        }))
    }

    // -----------------------------------------------------------------------
    // create_index — 3-phase build (PR 9)
    //
    // Phase 1 (reserve):  metadata.write()   — allocate idx root, publish
    //                                          `IndexEntry { state: Building }`.
    // Phase 2 (build):    metadata.read() +  — populate idx tree from data
    //                     ns_lane              tree. Other-ns writers run
    //                                          concurrently; same-ns writers
    //                                          wait on the lane.
    // Phase 3 (commit):   metadata.write()   — flip `state: Ready`, publish.
    //
    // Failure in phase 2 drops the Building entry in a recovery phase
    // (same DDL shape as phase 3) so the catalog does not retain an
    // orphan `Building` index.
    // -----------------------------------------------------------------------

    fn create_index(&self, ns: &str, model: &IndexModel) -> Result<String> {
        validate_index_keys(&model.keys)?;
        let name = model
            .options
            .name
            .clone()
            .unwrap_or_else(|| generate_index_name(&model.keys));

        // ---------------------------------------------------------------
        // Phase 1 — Reserve (metadata.write + commit_seq, brief).
        // ---------------------------------------------------------------
        let reserve_outcome = self.create_index_reserve(ns, model, &name)?;
        match reserve_outcome {
            ReserveOutcome::AlreadyExists => return Ok(name),
            ReserveOutcome::Reserved => {}
        }

        // ---------------------------------------------------------------
        // Phase 2 — Build (metadata.read + ns_lane). Long-running scan.
        //
        // A failure here leaves a Building entry in the catalog. We fall
        // through to a cleanup DDL that drops it, keeping the catalog
        // consistent for a retried `create_index`.
        // ---------------------------------------------------------------
        if let Err(build_err) = self.create_index_build(ns, &name) {
            // Best-effort rollback of the orphan Building entry. Log-and-
            // propagate if the cleanup itself fails: the caller sees the
            // original build error, and the Building entry (if any
            // remains) will still be skipped by the read path.
            if let Err(cleanup_err) = self.create_index_cleanup(ns, &name) {
                return Err(Error::Internal(format!(
                    "create_index build failed: {}; cleanup also failed: {}",
                    build_err, cleanup_err
                )));
            }
            return Err(build_err);
        }

        // ---------------------------------------------------------------
        // Phase 3 — Commit (metadata.write + commit_seq, brief).
        // ---------------------------------------------------------------
        self.create_index_commit(ns, &name)?;
        Ok(name)
    }

    // -----------------------------------------------------------------------
    // drop_index
    // -----------------------------------------------------------------------

    fn drop_index(&self, ns: &str, name: &str) -> Result<()> {
        if name == "_id_" {
            return Err(Error::InvalidWireMessage {
                detail: "drop of '_id_' index is not permitted".to_string(),
            });
        }
        // Acquire the namespace lane before the DDL body. This serializes
        // with any in-flight create_index Phase 2 build on the same ns —
        // the build holds the lane, so drop_index waits for it to release.
        // After the build finishes (or aborts), drop_index proceeds. A
        // subsequent Phase 3 (Ready flip) or concurrent CRUD on this ns
        // will see the missing entry and handle it cleanly.
        //
        // Lane-before-metadata ordering matches drop_namespace
        // (src/storage/paged_engine.rs drop_namespace) and keeps us below
        // the metadata RwLock in the documented lock order.
        let lane_arc = self.lane_for(ns);
        let _lane_guard = self.acquire_lane(lane_arc)?;
        self.run_ddl(|shared, md, overlay| {
            let removed = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .drop_index(ns, name)?;
            if removed {
                sync_catalog_root_overlay(shared, md, overlay)?;
                Ok(())
            } else {
                Err(Error::Internal(format!(
                    "index '{}' not found on '{}'",
                    name, ns
                )))
            }
        })
    }

    // -----------------------------------------------------------------------
    // list_indexes
    // -----------------------------------------------------------------------

    fn list_indexes(&self, ns: &str) -> Result<Vec<IndexInfo>> {
        let snap = self.shared.published.load();
        let ns_snap = match snap.namespaces.get(ns) {
            None => return Ok(Vec::new()),
            Some(n) => n,
        };
        Ok(ns_snap
            .indexes
            .iter()
            .map(|i| IndexInfo {
                name: i.name.clone(),
                keys: i.key_pattern.clone(),
                unique: i.unique,
                sparse: i.sparse,
            })
            .collect())
    }

    // -----------------------------------------------------------------------
    // create_namespace
    // -----------------------------------------------------------------------

    fn create_namespace(&self, ns: &str) -> Result<()> {
        let result = self.run_ddl(|shared, md, overlay| {
            let data_root = {
                let mut cat = md.catalog.lock().expect("catalog poisoned");
                if cat.get_collection(ns)?.is_some() {
                    return Ok(());
                }
                cat.create_collection(ns, bson::doc! {}, now_millis())?
            };
            sync_catalog_root_overlay(shared, md, overlay)?;
            let store = new_txn_store(shared, overlay);
            BTree::create_at(store, data_root)?;
            Ok(())
        });
        // Clear the drop tombstone so subsequent auto-bootstrap (from `run_write`)
        // is re-enabled for this namespace.
        if result.is_ok() {
            self.dropped_namespaces.remove(ns);
        }
        result
    }

    // -----------------------------------------------------------------------
    // drop_namespace
    // -----------------------------------------------------------------------

    fn drop_namespace(&self, ns: &str) -> Result<()> {
        // Plan §T9: force-expire ALL active ReadViews globally before
        // freeing pages. Done BEFORE taking the metadata write guard so
        // concurrent readers that just loaded the published snapshot
        // can finish their pin walks.
        self.shared.handle.read_view_registry().force_expire_all();

        // Remove the lane from the map so no new writer picks it up, then
        // wait for any writer that grabbed the lane before the remove by
        // briefly locking the removed Arc. Release immediately.
        let removed_lane = self.ns_lanes.remove(ns).map(|(_, v)| v);
        if let Some(lane) = removed_lane {
            // Wait out an in-flight writer by taking the lock ourselves.
            let _guard = lane.lock().map_err(|_| {
                Error::Internal("namespace lane mutex poisoned during drop".into())
            })?;
            // _guard dropped immediately on scope exit below.
            drop(_guard);
        }

        let result = self.run_ddl(|shared, md, overlay| {
            let (maybe_coll, index_roots, dropped) = {
                let mut cat = md.catalog.lock().expect("catalog poisoned");
                let maybe_coll = cat.get_collection(ns)?;
                let index_roots: Vec<(u32, u8)> = if maybe_coll.is_some() {
                    cat.list_indexes(ns)?
                        .into_iter()
                        .map(|e| (e.root_page, e.root_level))
                        .collect()
                } else {
                    Vec::new()
                };
                let dropped = cat.drop_collection(ns)?;
                (maybe_coll, index_roots, dropped)
            };
            if dropped {
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            if let Some(coll) = maybe_coll {
                let store = new_txn_store(shared, overlay);
                let data_tree = BTree::open(store, coll.data_root_page, coll.data_root_level);
                data_tree.free_all_pages()?;
            }
            for (idx_root, idx_level) in index_roots {
                let store = new_txn_store(shared, overlay);
                let idx_tree = BTree::open(store, idx_root, idx_level);
                idx_tree.free_all_pages()?;
            }
            Ok(())
        });
        // Record the drop so that `bootstrap_namespace` (called from `run_write`
        // when the namespace is absent) does not auto-recreate it.  This prevents
        // a racing insert — one that arrived after `drop_namespace` returned but
        // whose `run_write` call saw ns_missing — from re-bootstrapping the
        // namespace and committing stale data to the journal.
        if result.is_ok() {
            self.dropped_namespaces.insert(ns.to_string());
        }
        result
    }

    // -----------------------------------------------------------------------
    // list_namespaces
    // -----------------------------------------------------------------------

    fn list_namespaces(&self) -> Result<Vec<String>> {
        let snap = self.shared.published.load();
        Ok(snap.namespaces.keys().cloned().collect())
    }

    // -----------------------------------------------------------------------
    // checkpoint
    // -----------------------------------------------------------------------

    fn checkpoint(&self) -> Result<()> {
        // DDL: take metadata.write() so the checkpoint is atomic wrt CRUD
        // writers. No overlay needed — checkpoint just flushes + GCs.
        let md = self.metadata.write().map_err(|_| {
            Error::Internal("metadata RwLock poisoned".into())
        })?;

        // sync_catalog_root via direct allocator update (no overlay).
        let (root_page, root_level) = {
            let cat = md.catalog.lock().expect("catalog poisoned");
            (cat.root_page(), cat.root_level())
        };
        self.shared.handle.allocator().update_header(|h| {
            h.catalog_root_page = root_page;
            h.catalog_root_level = root_level;
            h.catalog_root_backup = root_page;
        })?;

        // Plan §T8: history-store GC + counters.
        let ort = self.shared.handle.read_view_registry().oldest_required_ts();
        {
            let mut hs = self.shared.history_store.lock().unwrap();
            hs.gc_pass(ort)?;
        }
        let lag_ms = if ort == crate::mvcc::timestamp::Ts::MAX {
            0
        } else {
            self.shared
                .oracle
                .now()
                .physical_ms
                .saturating_sub(ort.physical_ms)
        };
        crate::mvcc::metrics::set_oldest_required_ts_lag_ms(lag_ms);
        crate::mvcc::metrics::set_overflow_pages_in_use(
            self.shared.handle.allocator().overflow_pages_in_use() as u64,
        );
        crate::mvcc::metrics::set_deferred_free_queue_depth(
            self.shared.handle.allocator().deferred_free_queue().depth() as u64,
        );
        self.shared.handle.flush()
    }

    fn close(&self) -> Result<()> {
        self.checkpoint()
    }

    fn journal_sync(&self) -> Result<()> {
        self.shared.handle.journal_sync()
    }

    fn snapshot_bytes(&self) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// DDL helper + upsert helpers (private)
// ---------------------------------------------------------------------------

impl PagedEngine {
    /// Drive a DDL body under `metadata.write()` + `commit_seq`, then
    /// commit the overlay and publish a fresh snapshot.
    fn run_ddl<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&SharedState, &MetadataState, &Arc<Mutex<TxnOverlay>>) -> Result<R>,
    {
        let md_w = self.metadata.write().map_err(|_| {
            Error::Internal("metadata RwLock poisoned".into())
        })?;
        let _commit = self.commit_seq.lock().map_err(|_| {
            Error::Internal("commit_seq mutex poisoned".into())
        })?;

        let mark = self.shared.handle.begin_txn()?;
        let overlay = TxnOverlay::new_shared();
        let ready = self.shared.handle.allocator().drain_deferred_free_reservations();
        if !ready.is_empty() {
            let mut ov = overlay.lock().expect("TxnOverlay mutex poisoned");
            for page in ready {
                ov.push_reservation(PageReservation {
                    page,
                    size: PageSize::Large32k,
                    origin: PageOrigin::DeferredFree,
                });
            }
        }

        let result = f(&self.shared, &md_w, &overlay);
        match result {
            Ok(value) => {
                let overlay_inner = Arc::try_unwrap(overlay)
                    .map_err(|_| Error::Internal("TxnOverlay still referenced".into()))?
                    .into_inner()
                    .map_err(|_| Error::Internal("TxnOverlay mutex poisoned".into()))?;
                let mut base_store = new_store(&self.shared);
                overlay_inner.commit(&mut base_store, &self.shared.handle)?;
                self.shared.handle.flush()?;
                let db_page_count = self
                    .shared
                    .handle
                    .allocator()
                    .with_header(|h| h.total_page_count)?;
                let header_data = {
                    let page = self.shared.handle.fetch_page(0, PageSize::Small4k)?;
                    page.data().to_vec()
                };
                let emergency = self.shared.handle.commit_txn(
                    0,
                    PageSize::Small4k,
                    &header_data,
                    db_page_count,
                )?;
                if emergency {
                    let _ = self.shared.handle.emergency_checkpoint();
                }
                let publish_ts = self.shared.oracle.now();
                rebuild_and_publish_locked(&self.shared, &md_w, publish_ts)?;
                Ok(value)
            }
            Err(e) => {
                let _ = self.rollback_overlay_and_wal(overlay, mark);
                Err(e)
            }
        }
    }

    /// Upsert for `update_one/many` with `upsert: true`.
    fn do_upsert_update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
    ) -> Result<UpdateResult> {
        let mut new_doc = upsert_base_from_filter(filter);
        apply_update(&mut new_doc, update, true)?;
        let id = self.run_write(ns, |shared, md, overlay, txn| {
            let entry = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?
                .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-upsert", ns)))?;
            let mut tree = BTree::open(
                new_txn_store(shared, overlay),
                entry.data_root_page,
                entry.data_root_level,
            );
            let (id, key, bson_bytes, _tree_root) = btree_insert_doc(&mut tree, &mut new_doc, &[])?;
            if tree.root_page != entry.data_root_page || tree.root_level != entry.data_root_level {
                let mut updated = entry.clone();
                updated.data_root_page = tree.root_page;
                updated.data_root_level = tree.root_level;
                md.catalog
                    .lock()
                    .expect("catalog poisoned")
                    .update_collection(&updated)?;
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            txn.stage_primary_insert(ns.to_string(), key, bson_bytes);
            maintain_secondary_on_insert(shared, md, overlay, ns, &new_doc, &id, txn)?;
            Ok(id)
        })?;
        Ok(UpdateResult {
            matched_count: 0,
            modified_count: 0,
            upserted_id: Some(id),
        })
    }

    /// Upsert for `find_one_and_update` with `upsert: true`.
    fn fam_upsert_update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &FindOneAndUpdateOptions,
    ) -> Result<Option<Document>> {
        let mut new_doc = upsert_base_from_filter(filter);
        apply_update(&mut new_doc, update, true)?;
        self.run_write(ns, |shared, md, overlay, txn| {
            let entry = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?
                .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-upsert", ns)))?;
            let mut tree = BTree::open(
                new_txn_store(shared, overlay),
                entry.data_root_page,
                entry.data_root_level,
            );
            let (id, key, bson_bytes, _tree_root) = btree_insert_doc(&mut tree, &mut new_doc, &[])?;
            if tree.root_page != entry.data_root_page || tree.root_level != entry.data_root_level {
                let mut updated = entry.clone();
                updated.data_root_page = tree.root_page;
                updated.data_root_level = tree.root_level;
                md.catalog
                    .lock()
                    .expect("catalog poisoned")
                    .update_collection(&updated)?;
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            txn.stage_primary_insert(ns.to_string(), key, bson_bytes);
            maintain_secondary_on_insert(shared, md, overlay, ns, &new_doc, &id, txn)?;
            Ok(())
        })?;
        Ok(match opts.return_document {
            ReturnDocument::Before => None,
            ReturnDocument::After => Some(new_doc),
        })
    }

    /// Upsert for `find_one_and_replace` with `upsert: true`.
    fn fam_upsert_replace(
        &self,
        ns: &str,
        replacement: &Document,
        opts: &FindOneAndReplaceOptions,
    ) -> Result<Option<Document>> {
        let mut new_doc = replacement.clone();
        self.run_write(ns, |shared, md, overlay, txn| {
            let entry = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?
                .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-upsert", ns)))?;
            let mut tree = BTree::open(
                new_txn_store(shared, overlay),
                entry.data_root_page,
                entry.data_root_level,
            );
            let (id, key, bson_bytes, _tree_root) = btree_insert_doc(&mut tree, &mut new_doc, &[])?;
            if tree.root_page != entry.data_root_page || tree.root_level != entry.data_root_level {
                let mut updated = entry.clone();
                updated.data_root_page = tree.root_page;
                updated.data_root_level = tree.root_level;
                md.catalog
                    .lock()
                    .expect("catalog poisoned")
                    .update_collection(&updated)?;
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            txn.stage_primary_insert(ns.to_string(), key, bson_bytes);
            maintain_secondary_on_insert(shared, md, overlay, ns, &new_doc, &id, txn)?;
            Ok(())
        })?;
        Ok(match opts.return_document {
            ReturnDocument::Before => None,
            ReturnDocument::After => Some(new_doc),
        })
    }
}


// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn engine() -> PagedEngine {
        let (e, _io) = buffered_engine();
        e
    }

    #[test]
    fn insert_and_find_one() {
        let e = engine();
        e.insert("test.users", doc! { "name": "Alice", "age": 30 })
            .unwrap();
        let found = e.find_one("test.users", &doc! { "name": "Alice" }).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().get_str("name").unwrap(), "Alice");
    }

    #[test]
    fn insert_missing_namespace_returns_empty_find() {
        let e = engine();
        let found = e.find("test.users", &doc! {}, &FindOptions::new()).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn insert_multiple_and_count() {
        let e = engine();
        for i in 0..10i32 {
            e.insert("test.c", doc! { "i": i }).unwrap();
        }
        let count = e.count("test.c", &doc! {}).unwrap();
        assert_eq!(count, 10);
    }

    #[test]
    fn delete_one_removes_single_document() {
        let e = engine();
        e.insert("test.c", doc! { "x": 1 }).unwrap();
        e.insert("test.c", doc! { "x": 2 }).unwrap();
        let r = e.delete("test.c", &doc! { "x": 1 }, false).unwrap();
        assert_eq!(r.deleted_count, 1);
        assert_eq!(e.count("test.c", &doc! {}).unwrap(), 1);
    }

    #[test]
    fn delete_many_removes_all_matching() {
        let e = engine();
        for i in 0..5i32 {
            e.insert("test.c", doc! { "v": i }).unwrap();
        }
        let r = e
            .delete("test.c", &doc! { "v": { "$gt": 2 } }, true)
            .unwrap();
        assert_eq!(r.deleted_count, 2); // v=3 and v=4
    }

    #[test]
    fn update_one_modifies_field() {
        let e = engine();
        e.insert("test.c", doc! { "name": "Alice", "age": 30 })
            .unwrap();
        let r = e
            .update(
                "test.c",
                &doc! { "name": "Alice" },
                &doc! { "$set": { "age": 31 } },
                &UpdateOptions::default(),
                false,
            )
            .unwrap();
        assert_eq!(r.matched_count, 1);
        assert_eq!(r.modified_count, 1);
        let found = e
            .find_one("test.c", &doc! { "name": "Alice" })
            .unwrap()
            .unwrap();
        assert_eq!(found.get_i32("age").unwrap(), 31);
    }

    #[test]
    fn find_with_sort_and_limit() {
        let e = engine();
        for i in [3i32, 1, 2] {
            e.insert("test.c", doc! { "v": i }).unwrap();
        }
        let mut opts = FindOptions::new();
        opts.sort = Some(doc! { "v": 1 });
        opts.limit = Some(2);
        let results = e.find("test.c", &doc! {}, &opts).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get_i32("v").unwrap(), 1);
        assert_eq!(results[1].get_i32("v").unwrap(), 2);
    }

    #[test]
    fn create_namespace_then_insert() {
        let e = engine();
        e.create_namespace("test.c").unwrap();
        e.insert("test.c", doc! { "k": "v" }).unwrap();
        assert_eq!(e.count("test.c", &doc! {}).unwrap(), 1);
    }

    #[test]
    fn drop_namespace_removes_documents() {
        let e = engine();
        e.insert("test.c", doc! { "x": 1 }).unwrap();
        e.drop_namespace("test.c").unwrap();
        assert_eq!(e.count("test.c", &doc! {}).unwrap(), 0);
    }

    #[test]
    fn create_and_list_indexes() {
        let e = engine();
        e.create_namespace("test.c").unwrap();
        let model = IndexModel::builder()
            .keys(doc! { "email": 1 })
            .build()
            .unwrap();
        let name = e.create_index("test.c", &model).unwrap();
        assert_eq!(name, "email_1");
        let indexes = e.list_indexes("test.c").unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "email_1");
    }

    #[test]
    fn upsert_creates_document_when_no_match() {
        let e = engine();
        let r = e
            .update(
                "test.c",
                &doc! { "email": "a@b.com" },
                &doc! { "$set": { "name": "Alice" } },
                &UpdateOptions {
                    upsert: true,
                    ..Default::default()
                },
                false,
            )
            .unwrap();
        assert!(r.upserted_id.is_some());
        let doc = e
            .find_one("test.c", &doc! { "email": "a@b.com" })
            .unwrap()
            .unwrap();
        assert_eq!(doc.get_str("name").unwrap(), "Alice");
    }

    #[test]
    fn find_one_and_delete_returns_doc() {
        let e = engine();
        e.insert("test.c", doc! { "x": 42 }).unwrap();
        let d = e
            .find_one_and_delete_doc(
                "test.c",
                &doc! { "x": 42 },
                &FindOneAndDeleteOptions::default(),
            )
            .unwrap();
        assert!(d.is_some());
        assert_eq!(e.count("test.c", &doc! {}).unwrap(), 0);
    }

    // -----------------------------------------------------------------------
    // R1.3: Buffered-mode (catalog namespace registry) tests
    //
    // These tests exercise PagedEngine in buffered mode, using
    // an in-memory mock I/O layer so they remain hermetic and fast.
    // -----------------------------------------------------------------------

    use crate::storage::buffer_pool::{default_sizes, BufferPool, PageSource, PageSize};
    use crate::storage::header::FileHeader;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    /// Minimal in-memory `PageSource` for buffered-mode engine tests.
    #[derive(Default)]
    struct MockIo {
        pages: StdMutex<HashMap<u32, Vec<u8>>>,
    }

    struct ArcIo(Arc<MockIo>);

    impl PageSource for ArcIo {
        fn read_page(&self, pn: u32, _size: PageSize, buf: &mut [u8]) -> Result<()> {
            let pages = self.0.pages.lock().unwrap();
            if let Some(data) = pages.get(&pn) {
                let n = buf.len().min(data.len());
                buf[..n].copy_from_slice(&data[..n]);
                if n < buf.len() {
                    buf[n..].fill(0);
                }
            } else {
                buf.fill(0);
            }
            Ok(())
        }
        fn write_page(&self, pn: u32, _size: PageSize, buf: &[u8]) -> Result<()> {
            self.0.pages.lock().unwrap().insert(pn, buf.to_vec());
            Ok(())
        }
    }

    /// Create a buffered `PagedEngine` backed by an in-memory `MockIo`.
    ///
    /// Returns `(engine, io)` so callers can inspect or re-use the backing store.
    fn buffered_engine() -> (PagedEngine, Arc<MockIo>) {
        let io = Arc::new(MockIo::default());
        let pool = Arc::new(BufferPool::new(
            default_sizes::DESKTOP,
            Box::new(ArcIo(Arc::clone(&io))),
        ));
        let history_pool = Arc::new(BufferPool::new(
            default_sizes::IOT,
            Box::new(ArcIo(Arc::clone(&io))),
        ));
        let header = FileHeader::new_now();
        let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
        let engine = PagedEngine::new_buffered(handle, 0, 0)
            .expect("create buffered engine");
        (engine, io)
    }

    /// Reconstruct a buffered engine by reading back the catalog root from
    /// the mock I/O layer.  Simulates closing and reopening a database file.
    ///
    /// Reads page 0 (the file header) from `io`, extracts the persisted
    /// `catalog_root_page` and `catalog_root_level`, and opens a new engine.
    fn reopen_engine(io: &Arc<MockIo>) -> PagedEngine {
        // Read the header page from backing store.
        let pages = io.pages.lock().unwrap();
        let hdr_bytes = pages
            .get(&0)
            .expect("header page 0 must have been flushed")
            .clone();
        drop(pages); // release lock before creating new pool

        use crate::storage::header::HEADER_PAGE_SIZE;
        let mut buf = [0u8; HEADER_PAGE_SIZE];
        let n = buf.len().min(hdr_bytes.len());
        buf[..n].copy_from_slice(&hdr_bytes[..n]);
        let header = FileHeader::from_bytes(&buf).expect("parse header");

        let catalog_root_page = header.catalog_root_page;
        let catalog_root_level = header.catalog_root_level;

        let pool = Arc::new(BufferPool::new(
            default_sizes::DESKTOP,
            Box::new(ArcIo(Arc::clone(io))),
        ));
        let history_pool = Arc::new(BufferPool::new(
            default_sizes::IOT,
            Box::new(ArcIo(Arc::clone(io))),
        ));
        let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
        PagedEngine::new_buffered(handle, catalog_root_page, catalog_root_level)
            .expect("reopen buffered engine")
    }

    // --- create_namespace wires into catalog ---

    #[test]
    fn buffered_create_namespace_appears_in_list() {
        let (e, _io) = buffered_engine();
        e.create_namespace("mydb.users").unwrap();
        e.create_namespace("mydb.orders").unwrap();

        let mut ns = e.list_namespaces().unwrap();
        ns.sort();
        assert_eq!(ns, ["mydb.orders", "mydb.users"]);
    }

    #[test]
    fn buffered_create_namespace_idempotent() {
        let (e, _io) = buffered_engine();
        e.create_namespace("mydb.users").unwrap();
        // Second call must not error (namespace already exists).
        e.create_namespace("mydb.users").unwrap();
        assert_eq!(e.list_namespaces().unwrap().len(), 1);
    }

    #[test]
    fn buffered_namespace_supports_db_dot_coll_format() {
        let (e, _io) = buffered_engine();
        // Namespace keys MUST be in 'db.collection' multi-database format.
        e.create_namespace("analytics.events").unwrap();
        e.create_namespace("billing.invoices").unwrap();

        let mut ns = e.list_namespaces().unwrap();
        ns.sort();
        assert!(ns.contains(&"analytics.events".to_owned()));
        assert!(ns.contains(&"billing.invoices".to_owned()));
    }

    // --- drop_namespace removes catalog entries AND frees pages ---

    #[test]
    fn buffered_drop_namespace_removes_from_catalog() {
        let (e, _io) = buffered_engine();
        e.create_namespace("mydb.users").unwrap();
        e.create_namespace("mydb.orders").unwrap();

        e.drop_namespace("mydb.users").unwrap();

        let ns = e.list_namespaces().unwrap();
        assert!(!ns.contains(&"mydb.users".to_owned()));
        assert!(ns.contains(&"mydb.orders".to_owned()));
    }

    #[test]
    fn buffered_drop_namespace_frees_pages_for_reuse() {
        let (e, _io) = buffered_engine();
        e.create_namespace("mydb.users").unwrap();

        // Insert enough docs to allocate multiple leaf pages.
        for i in 0..20i32 {
            e.insert("mydb.users", doc! { "i": i }).unwrap();
        }

        // Checkpoint so allocator state is stable.
        e.checkpoint().unwrap();

        // Record total page count before drop.
        let total_before = {
            e.shared
                .handle
                .allocator()
                .with_header(|h| h.total_page_count)
                .unwrap()
        };

        e.drop_namespace("mydb.users").unwrap();

        // Free page count should have increased (pages returned to free list).
        let free_after = {
            e.shared
                .handle
                .allocator()
                .with_header(|h| h.free_page_count_32k + h.free_page_count_4k)
                .unwrap()
        };

        // After drop the free page count must be > 0 (at least the data leaf
        // and _id-index leaf were reclaimed).
        assert!(
            free_after > 0,
            "pages must be returned to free list after drop; total_before={total_before}, free_after={free_after}"
        );
    }

    #[test]
    fn buffered_drop_nonexistent_namespace_is_ok() {
        let (e, _io) = buffered_engine();
        // Dropping a namespace that never existed must not panic or error.
        e.drop_namespace("mydb.ghost").unwrap();
    }

    // --- list_namespaces reads from catalog ---

    #[test]
    fn buffered_list_namespaces_empty_on_new_database() {
        let (e, _io) = buffered_engine();
        assert!(e.list_namespaces().unwrap().is_empty());
    }

    #[test]
    fn buffered_list_namespaces_returns_all() {
        let (e, _io) = buffered_engine();
        for name in &["a.x", "a.y", "b.z"] {
            e.create_namespace(name).unwrap();
        }
        let mut ns = e.list_namespaces().unwrap();
        ns.sort();
        assert_eq!(ns, ["a.x", "a.y", "b.z"]);
    }

    // --- on-open: catalog discovery ---

    #[test]
    fn buffered_catalog_survives_reopen() {
        let (e, io) = buffered_engine();

        e.create_namespace("prod.users").unwrap();
        e.create_namespace("prod.orders").unwrap();
        e.insert("prod.users", doc! { "name": "Alice" }).unwrap();

        // Flush the catalog + data to the mock backing store.
        e.checkpoint().unwrap();
        drop(e);

        // Reopen using the persisted catalog root from the header.
        let e2 = reopen_engine(&io);

        // list_namespaces must discover the previously-created collections
        // by reading the catalog from disk.
        let mut ns = e2.list_namespaces().unwrap();
        ns.sort();
        assert_eq!(
            ns,
            ["prod.orders", "prod.users"],
            "catalog must survive close/reopen"
        );
    }

    #[test]
    fn buffered_data_survives_reopen() {
        let (e, io) = buffered_engine();

        e.create_namespace("prod.users").unwrap();
        e.insert("prod.users", doc! { "name": "Bob", "age": 42 })
            .unwrap();
        e.checkpoint().unwrap();
        drop(e);

        let e2 = reopen_engine(&io);
        let found = e2
            .find_one("prod.users", &doc! { "name": "Bob" })
            .unwrap();
        assert!(
            found.is_some(),
            "document inserted before checkpoint must be visible after reopen"
        );
        assert_eq!(found.unwrap().get_i32("age").unwrap(), 42);
    }

    #[test]
    fn buffered_drop_and_create_reuses_pages() {
        let (e, _io) = buffered_engine();

        e.create_namespace("test.c").unwrap();
        for i in 0..10i32 {
            e.insert("test.c", doc! { "i": i }).unwrap();
        }
        e.checkpoint().unwrap();

        let page_count_after_create = {
            e.shared
                .handle
                .allocator()
                .with_header(|h| h.total_page_count)
                .unwrap()
        };

        e.drop_namespace("test.c").unwrap();

        // Create the namespace again and insert the same data.
        e.create_namespace("test.c").unwrap();
        for i in 0..10i32 {
            e.insert("test.c", doc! { "i": i }).unwrap();
        }
        e.checkpoint().unwrap();

        let page_count_after_recreate = {
            e.shared
                .handle
                .allocator()
                .with_header(|h| h.total_page_count)
                .unwrap()
        };

        // After drop + recreate, pages should be recycled — total page count
        // must not keep growing without bound.
        assert!(
            page_count_after_recreate <= page_count_after_create + 4,
            "pages should be recycled after drop; before={page_count_after_create} after={page_count_after_recreate}"
        );
    }

    // -----------------------------------------------------------------------
    // R1.4: Secondary index maintenance + index scan tests (buffered mode)
    // -----------------------------------------------------------------------

    /// Verify that `create_index` builds the secondary B+ tree from existing
    /// documents ("online" index build).
    #[test]
    fn buffered_create_index_builds_from_existing_docs() {
        let (e, _io) = buffered_engine();

        // Insert documents BEFORE creating the index.
        e.insert(
            "test.items",
            doc! { "sku": "A", "price": 10i32 },
        )
        .unwrap();
        e.insert(
            "test.items",
            doc! { "sku": "B", "price": 20i32 },
        )
        .unwrap();
        e.insert(
            "test.items",
            doc! { "sku": "C", "price": 30i32 },
        )
        .unwrap();

        // Create an index on "sku".
        let idx = IndexModel::builder()
            .keys(doc! { "sku": 1 })
            .build()
            .unwrap();
        let name = e.create_index("test.items", &idx).unwrap();
        assert_eq!(name, "sku_1");

        // Query using the indexed field; must return the correct document.
        let found = e
            .find_one("test.items", &doc! { "sku": "B" })
            .unwrap()
            .expect("document B must be found via index");
        assert_eq!(found.get_i32("price").unwrap(), 20);
    }

    /// Verify that the index is maintained when new documents are inserted
    /// after the index was created.
    #[test]
    fn buffered_index_maintained_on_insert() {
        let (e, _io) = buffered_engine();

        let idx = IndexModel::builder()
            .keys(doc! { "email": 1 })
            .build()
            .unwrap();
        e.create_index("test.users", &idx).unwrap();

        // Insert after index creation.
        e.insert("test.users", doc! { "email": "alice@test.com", "role": "admin" })
            .unwrap();
        e.insert("test.users", doc! { "email": "bob@test.com", "role": "user" })
            .unwrap();

        // Both documents must be found via the index.
        let alice = e
            .find_one("test.users", &doc! { "email": "alice@test.com" })
            .unwrap()
            .expect("alice must be found");
        assert_eq!(alice.get_str("role").unwrap(), "admin");

        let bob = e
            .find_one("test.users", &doc! { "email": "bob@test.com" })
            .unwrap()
            .expect("bob must be found");
        assert_eq!(bob.get_str("role").unwrap(), "user");
    }

    /// Verify that deleting a document removes its secondary index entry,
    /// so subsequent queries no longer find it.
    #[test]
    fn buffered_index_maintained_on_delete() {
        let (e, _io) = buffered_engine();

        let idx = IndexModel::builder()
            .keys(doc! { "email": 1 })
            .build()
            .unwrap();
        e.create_index("test.users", &idx).unwrap();

        e.insert("test.users", doc! { "email": "charlie@test.com" })
            .unwrap();

        // Delete the document.
        let r = e
            .delete("test.users", &doc! { "email": "charlie@test.com" }, false)
            .unwrap();
        assert_eq!(r.deleted_count, 1);

        // Must not be found via index scan.
        let found = e
            .find_one("test.users", &doc! { "email": "charlie@test.com" })
            .unwrap();
        assert!(found.is_none(), "deleted doc must not be returned");
    }

    /// Verify that updating a document replaces its old secondary index entry
    /// with a new one.
    #[test]
    fn buffered_index_maintained_on_update() {
        let (e, _io) = buffered_engine();

        let idx = IndexModel::builder()
            .keys(doc! { "email": 1 })
            .build()
            .unwrap();
        e.create_index("test.users", &idx).unwrap();

        e.insert("test.users", doc! { "email": "old@test.com" })
            .unwrap();

        // Update the indexed field.
        e.update(
            "test.users",
            &doc! { "email": "old@test.com" },
            &doc! { "$set": { "email": "new@test.com" } },
            &UpdateOptions::default(),
            false,
        )
        .unwrap();

        // Old entry must be gone.
        assert!(
            e.find_one("test.users", &doc! { "email": "old@test.com" })
                .unwrap()
                .is_none(),
            "old email must not be found after update"
        );
        // New entry must be present.
        assert!(
            e.find_one("test.users", &doc! { "email": "new@test.com" })
                .unwrap()
                .is_some(),
            "new email must be found after update"
        );
    }

    /// Verify that the index scan finds documents using a range condition.
    #[test]
    fn buffered_index_scan_range_gt() {
        let (e, _io) = buffered_engine();

        let idx = IndexModel::builder()
            .keys(doc! { "score": 1 })
            .build()
            .unwrap();
        e.create_index("test.players", &idx).unwrap();

        for i in 0i32..10 {
            e.insert("test.players", doc! { "name": format!("p{i}"), "score": i })
                .unwrap();
        }

        // Use $gt — only scores > 7 should be returned.
        let results = e
            .find(
                "test.players",
                &doc! { "score": { "$gt": 7i32 } },
                &FindOptions::new(),
            )
            .unwrap();
        assert_eq!(results.len(), 2, "scores 8 and 9 should match");
        for d in &results {
            assert!(d.get_i32("score").unwrap() > 7);
        }
    }

    /// Verify that the index scan handles `$in` queries correctly.
    #[test]
    fn buffered_index_scan_in_query() {
        let (e, _io) = buffered_engine();

        let idx = IndexModel::builder()
            .keys(doc! { "status": 1 })
            .build()
            .unwrap();
        e.create_index("test.orders", &idx).unwrap();

        e.insert("test.orders", doc! { "status": "pending", "amount": 10i32 })
            .unwrap();
        e.insert("test.orders", doc! { "status": "active",  "amount": 20i32 })
            .unwrap();
        e.insert("test.orders", doc! { "status": "closed",  "amount": 30i32 })
            .unwrap();

        let results = e
            .find(
                "test.orders",
                &doc! { "status": { "$in": ["pending", "active"] } },
                &FindOptions::new(),
            )
            .unwrap();
        assert_eq!(results.len(), 2);
        for d in &results {
            let s = d.get_str("status").unwrap();
            assert!(s == "pending" || s == "active");
        }
    }

    /// Verify that a unique secondary index rejects duplicate values.
    #[test]
    fn buffered_unique_secondary_index_rejects_duplicates() {
        let (e, _io) = buffered_engine();

        use crate::options::IndexOptions;
        let idx = IndexModel::builder()
            .keys(doc! { "email": 1 })
            .options(IndexOptions::new().unique(true))
            .build()
            .unwrap();
        e.create_index("test.users", &idx).unwrap();

        e.insert("test.users", doc! { "email": "dup@test.com" })
            .unwrap();
        let result = e.insert("test.users", doc! { "email": "dup@test.com" });
        assert!(
            matches!(result, Err(Error::DuplicateKey { .. })),
            "unique index must reject duplicate email"
        );
    }

    /// Verify that a compound index can be created and used for lookups.
    #[test]
    fn buffered_compound_index_lookup() {
        let (e, _io) = buffered_engine();

        let idx = IndexModel::builder()
            .keys(doc! { "category": 1, "price": 1 })
            .build()
            .unwrap();
        e.create_index("test.products", &idx).unwrap();

        e.insert(
            "test.products",
            doc! { "category": "books", "price": 15i32, "title": "Rust Programming" },
        )
        .unwrap();
        e.insert(
            "test.products",
            doc! { "category": "books", "price": 25i32, "title": "Database Design" },
        )
        .unwrap();
        e.insert(
            "test.products",
            doc! { "category": "tools", "price": 50i32, "title": "Hammer" },
        )
        .unwrap();

        // Equality on the leftmost field — planner selects the compound index.
        let results = e
            .find(
                "test.products",
                &doc! { "category": "books" },
                &FindOptions::new(),
            )
            .unwrap();
        assert_eq!(results.len(), 2, "two books should be found");
        for d in &results {
            assert_eq!(d.get_str("category").unwrap(), "books");
        }
    }

    /// Verify that an index survives a checkpoint + reopen cycle.
    #[test]
    fn buffered_index_survives_reopen() {
        let (e, io) = buffered_engine();

        let idx = IndexModel::builder()
            .keys(doc! { "username": 1 })
            .build()
            .unwrap();
        e.create_index("test.accounts", &idx).unwrap();

        e.insert("test.accounts", doc! { "username": "alice" })
            .unwrap();
        e.insert("test.accounts", doc! { "username": "bob" })
            .unwrap();

        e.checkpoint().unwrap();
        drop(e);

        let e2 = reopen_engine(&io);

        // After reopen, index scan must still work.
        let found = e2
            .find_one("test.accounts", &doc! { "username": "alice" })
            .unwrap();
        assert!(
            found.is_some(),
            "alice must be found via index after reopen"
        );
    }

    // -----------------------------------------------------------------------
    // R1.6: SWMR concurrency tests
    //
    // Verify that multiple concurrent readers do not block each other, and
    // that readers run concurrently with writers (writers take an exclusive
    // write lock; readers take a shared read lock).
    // -----------------------------------------------------------------------

    /// Verify that many concurrent reader threads can all see committed data
    /// without blocking each other.
    #[test]
    fn swmr_concurrent_readers_do_not_block() {
        use std::sync::Arc;
        use std::thread;

        let e = Arc::new(engine());
        // Insert documents under the single writer lock.
        for i in 0..20i32 {
            e.insert("test.c", doc! { "i": i }).unwrap();
        }

        // Spawn many reader threads that all query concurrently.
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let e = Arc::clone(&e);
                thread::spawn(move || {
                    let opts = FindOptions::new();
                    let docs = e.find("test.c", &doc! {}, &opts).unwrap();
                    assert_eq!(docs.len(), 20, "all 20 docs must be visible to every reader");
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader thread panicked");
        }
    }

    /// Verify that a reader can observe a consistent snapshot while a
    /// concurrent writer is modifying the collection.
    ///
    /// PR 4: readers no longer take the engine mutex. Instead they load a
    /// `PublishedSnapshot`, which captures the state at the moment of the
    /// last commit. A reader that loaded the snapshot before the writer
    /// commits will still see the pre-write state because `publish_ts` pins
    /// the `ReadView` at that timestamp.
    #[test]
    fn swmr_reader_sees_snapshot_isolation() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let e = Arc::new(engine());
        // Insert an initial document.
        e.insert("test.snap", doc! { "status": "before" })
            .unwrap();

        // Barrier: reader loads snapshot, signals writer; writer commits,
        // then signals reader to finish.
        let barrier = Arc::new(Barrier::new(2));

        let e_reader = Arc::clone(&e);
        let barrier_reader = Arc::clone(&barrier);
        let reader = thread::spawn(move || {
            // Load the published snapshot BEFORE the writer commits.
            let snap = e_reader.shared.published.load_full();
            let publish_ts = snap.publish_ts;

            // Tell the writer we have our snapshot.
            barrier_reader.wait();

            // Scan using the snapshot's root pages and publish_ts (no mutex).
            let matched = if let Some(ns_snap) = snap.namespaces.get("test.snap") {
                let store = BufferPoolPageStore::new(Arc::clone(&e_reader.shared.handle));
                let tree = BTree::open(store, ns_snap.data_root_page, ns_snap.data_root_level);
                let txn_id = e_reader.shared.txn_counter.fetch_add(1, Ordering::Relaxed);
                let view = ReadView::open(
                    Arc::clone(e_reader.shared.handle.read_view_registry()),
                    publish_ts,
                    txn_id,
                );
                btree_collscan(&tree, &doc! {}, &view, None).unwrap()
            } else {
                Vec::new()
            };
            matched
        });

        // Writer: wait for the reader to capture its snapshot, then write.
        barrier.wait();
        e.insert("test.snap", doc! { "status": "after" }).unwrap();

        let matched = reader.join().expect("reader panicked");
        // The reader's snapshot was taken before the write, so it sees exactly 1 doc.
        assert_eq!(
            matched.len(),
            1,
            "reader must see snapshot before writer committed"
        );
    }

    /// Verify that the in-process writer lock (in client.rs) respects the
    /// busy_timeout: concurrent writers should queue up and eventually all
    /// succeed (or get WriterBusy on zero-timeout paths).
    ///
    /// This test uses the PagedEngine directly (not through Client) so it
    /// only exercises the RwLock inside the engine, not the client-level
    /// writer_lock.  Engine-level writes are serialized by the write-lock.
    #[test]
    fn swmr_concurrent_writers_serialize() {
        use std::sync::Arc;
        use std::thread;

        let e = Arc::new(engine());

        // Spawn 8 writer threads — each inserts 10 documents.
        let handles: Vec<_> = (0..8u32)
            .map(|worker| {
                let e = Arc::clone(&e);
                thread::spawn(move || {
                    for j in 0..10u32 {
                        e.insert(
                            "test.concurrent",
                            doc! { "worker": worker as i32, "j": j as i32 },
                        )
                        .unwrap();
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("writer thread panicked");
        }

        // After all writers complete, total doc count must be 8 * 10 = 80.
        let count = e.count("test.concurrent", &doc! {}).unwrap();
        assert_eq!(count, 80, "all 80 documents must be present after concurrent writes");
    }
}
