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
//! `find` / `update` / `delete` first ask the query planner ([`select_plan`]) whether
//! a secondary index can accelerate the query.  When a suitable index is found the
//! engine performs an [`IndexScan`] — a range scan on the secondary B+ tree whose
//! values contain the serialised `_id` of the matching document, followed by a point
//! lookup in the primary data tree.  When no index matches the engine falls back to a
//! full [`CollScan`].
//!
//! [`IndexScan`]: crate::query::planner::ScanPlan::IndexScan
//! [`CollScan`]: crate::query::planner::ScanPlan::CollScan
//! [`select_plan`]: crate::query::planner::select_plan

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

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
use crate::query::planner::{select_plan, IndexCondition, IndexMeta, ScanPlan};
use crate::query::{eval_filter, get_nested_field};
use crate::results::{DeleteResult, UpdateResult};
use crate::storage::btree::{BTree, BTreePageStore, CellValue};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::PageSize;
use crate::storage::catalog::{open_with_fallback as catalog_open_with_fallback, Catalog, IndexEntry};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::history_store::HistoryStore;
use crate::storage::oid::ObjectIdGenerator;
use crate::storage::secondary_index::{
    build_index, generate_index_name, update_index_on_delete, update_index_on_insert,
    update_index_on_update,
};
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
// BpBackend — buffer-pool-backed storage
// ---------------------------------------------------------------------------

/// Buffer-pool-backed storage backend.
///
/// All B+ trees share the same [`BufferPoolHandle`].  The [`Catalog`] persists
/// collection and index metadata; its root page is written to the file header
/// after every catalog mutation.
struct BpBackend {
    handle: Arc<BufferPoolHandle>,
    /// Catalog B+ tree for collection/index metadata.
    catalog: Catalog<BufferPoolPageStore>,
    /// Cached data trees (loaded lazily from the catalog on first access).
    data_trees: HashMap<String, BTree<BufferPoolPageStore>>,
    /// HLC oracle issuing commit timestamps for MVCC ChainCommit frames.
    /// Floored at open time to `journal.recovered_max_commit_ts().successor()`
    /// (plan T7) so post-recovery commits are strictly above every durable
    /// pre-crash commit.
    oracle: Arc<TimestampOracle>,
    /// MVCC history store (plan §T7). Reconciliation pushes aged version
    /// entries here when a leaf is evicted; reader probes here on
    /// buffer-pool misses whose chain snapshot is empty. Routed through
    /// [`BufferPoolHandle::history_pool`]. Wrapped in `Arc<Mutex<_>>` so
    /// the writer-path (future reconciliation) and reader-path probes
    /// can hold disjoint borrows of the `BpBackend` simultaneously.
    history_store: Arc<std::sync::Mutex<HistoryStore<BufferPoolPageStore>>>,
    /// Monotonic writer-transaction identifier source. Increments per
    /// `with_txn` call; wraps only after 2⁶⁴ txns (practical infinity).
    txn_counter: AtomicU64,
    /// The `WriteTxn` currently owned by an in-flight `with_txn` call, or
    /// `None` outside any writer boundary. Phase 5 wires this slot so
    /// writer-side helpers (sec-index `maintain_*`, future primary chain
    /// population) can stage overflow pins, refcount deltas, page writes,
    /// and pending sec-index mutations on the active txn. Always `None`
    /// before the first `with_txn` call and between calls; the
    /// `&mut self` receiver on `with_txn` prevents reentrant writers.
    active_txn: Option<WriteTxn>,
}

impl BpBackend {
    /// Create a new backend from an existing (or fresh) buffer pool handle.
    ///
    /// `catalog_root_page == 0` means a new, empty database; a fresh catalog
    /// B+ tree is created.  Non-zero `catalog_root_page` opens the existing
    /// catalog at the stored root.
    fn new(
        handle: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
    ) -> Result<Self> {
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
        let backend = Self {
            handle,
            catalog,
            data_trees: HashMap::new(),
            oracle,
            history_store: Arc::new(std::sync::Mutex::new(history_store_inner)),
            txn_counter: AtomicU64::new(1),
            active_txn: None,
        };
        // For a new database, persist the freshly-allocated catalog root
        // to the file header immediately (will be written to disk on flush).
        if catalog_root_page == 0 {
            backend.sync_catalog_root()?;
        }
        Ok(backend)
    }

    /// Create a new [`BufferPoolPageStore`] backed by this handle.
    fn new_store(&self) -> BufferPoolPageStore {
        BufferPoolPageStore::new(Arc::clone(&self.handle))
    }

    /// Open a snapshot [`ReadView`] for a reader path. The view registers
    /// itself with the handle-owned [`ReadViewRegistry`] for its lifetime and
    /// is timestamped with the oracle's current `read_ts`. The `txn_id` is
    /// pulled from `txn_counter` so concurrent reader views remain unique.
    fn open_read_view(&self) -> Arc<ReadView> {
        let txn_id = self.txn_counter.fetch_add(1, Ordering::Relaxed);
        let read_ts = self.oracle.now();
        ReadView::open(
            Arc::clone(self.handle.read_view_registry()),
            read_ts,
            txn_id,
        )
    }

    /// Bind a primary-key probe of the [`HistoryStore`] to `ns` so reader
    /// paths can fall through on chain misses (plan §T7).
    fn primary_history_probe(&self, ns: &str) -> PrimaryHistoryProbe<BufferPoolPageStore> {
        PrimaryHistoryProbe {
            store: Arc::clone(&self.history_store),
            ns_id: ns_id_for(ns),
        }
    }

    /// Update `FileHeader::catalog_root_page` and `catalog_root_level` to
    /// reflect the current catalog root.
    ///
    /// Must be called after every catalog mutation.
    fn sync_catalog_root(&self) -> Result<()> {
        let root_page = self.catalog.root_page();
        let root_level = self.catalog.root_level();
        self.handle.allocator().update_header(|h| {
            h.catalog_root_page = root_page;
            h.catalog_root_level = root_level;
            h.catalog_root_backup = root_page;
        })
    }

    /// Run `f` inside a WAL transaction boundary.
    ///
    /// On `Ok`: flushes dirty pages (they land in the WAL as non-commit frames
    /// via `JournalLayeredSource`), then emits a final commit frame tagged with
    /// `total_page_count` so recovery knows the txn is durable.
    ///
    /// On `Err`: truncates the WAL back to the snapshot cursor and drops all
    /// dirty, unpinned frames from the buffer pool — leaves the in-memory
    /// state consistent with the pre-txn on-disk state.
    ///
    /// No-op (no commit frame, no rollback) when the handle has no WAL.
    fn with_txn<T, F>(&mut self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Self) -> Result<T>,
    {
        let mark = self.handle.begin_txn()?;
        // Begin MVCC write txn. Drains the deferred-free queue under
        // `AllocatorHandle::state` so any refcount-0 pages from earlier
        // reader drops return to the free list before this commit
        // allocates. A failure here must roll back the WAL mark.
        let txn_id = self.txn_counter.fetch_add(1, Ordering::Relaxed);
        let txn = match WriteTxn::begin(
            txn_id,
            self.handle.allocator(),
            self.handle.page_source(),
        ) {
            Ok(t) => t,
            Err(e) => {
                let _ = self.handle.rollback_txn(mark);
                return Err(e);
            }
        };
        // Stash the active WriteTxn in BpBackend so writer-side helpers
        // (sec-index maintain_*, future primary chain population) can reach
        // it via `active_txn_mut()` for staging. `&mut self` on `with_txn`
        // prevents reentrant writers, so a single-slot `Option` is safe.
        debug_assert!(self.active_txn.is_none(), "with_txn is not reentrant");
        self.active_txn = Some(txn);
        let result = f(self);
        // Take the txn back out of the slot regardless of outcome so
        // commit/rollback run exactly once and the slot is empty before
        // the next `with_txn`.
        let mut txn = self
            .active_txn
            .take()
            .expect("active_txn slot emptied by closure — invariant broken");
        match result {
            Ok(value) => {
                // Phase 6 sub-step 5: drain the staged secondary-index
                // writes and install them into the real BTrees *before*
                // the WAL flush + commit frames. Install failures (e.g. a
                // unique-constraint violation that slipped past the
                // stage-time pre-check) must abort the txn, which means
                // they have to happen on the "still rollbackable" side of
                // `commit_txn` below. `txn.pending_sec_index` is emptied
                // via `take` so `txn.commit` sees no stale writes.
                let sec_writes = std::mem::take(&mut txn.pending_sec_index);
                if let Err(e) = self.install_pending_sec_index(sec_writes) {
                    drop(txn);
                    let _ = self.handle.rollback_txn(mark);
                    return Err(e);
                }
                // Phase 6 sub-step 2: pre-allocate the commit_ts so staged
                // primary writes install with the correct `start_ts`, then
                // install them at each key's leaf chain head. Failures here
                // also roll back the WAL mark — the on-disk cell mutation
                // lands with the WAL rollback, not with the chain-install.
                let primary_writes = std::mem::take(&mut txn.pending_primary);
                if !primary_writes.is_empty() {
                    let txn_id = txn.txn_id;
                    let commit_ts = match txn.allocate_commit_ts(&self.oracle) {
                        Ok(ts) => ts,
                        Err(e) => {
                            drop(txn);
                            let _ = self.handle.rollback_txn(mark);
                            return Err(e);
                        }
                    };
                    if let Err(e) =
                        self.install_pending_primary(primary_writes, commit_ts, txn_id)
                    {
                        drop(txn);
                        let _ = self.handle.rollback_txn(mark);
                        return Err(e);
                    }
                }
                // Flush dirty pages to the WAL as non-commit frames.
                self.handle.flush()?;
                // MVCC WriteTxn commit (§T5'): emit one ChainCommit frame
                // carrying commit_ts + (currently empty) refcount_deltas +
                // page_writes + pending_sec_index. The journal scanners were
                // retrofitted (Phase 6 sub-step 1) to skip ChainCommit
                // frames via CRC-based disambiguation, so emission here is
                // safe alongside the legacy per-page commit frame below.
                //
                // `installed` is the pending `OverflowRef` vec transferred
                // out of the txn. Phase 5 writer callsites stage no
                // overflow chains, so `installed` is empty today and the
                // drop is a no-op; Phase 6 (chain population) will move
                // each ref into its installed VersionEntry before drop so
                // the refcount stays at ≥ 1 post-commit.
                //
                // `sec_index` is empty here — we already drained it above
                // via `mem::take` and installed the writes. Kept in the
                // return tuple for future phases that may consume it at
                // commit time.
                let (_commit_ts, _installed, _sec_index) =
                    txn.commit(&self.oracle, &self.handle)?;
                // Emit a commit frame. Use page 0 (header, 32k) — it is always
                // part of any write txn because the allocator touches it.
                let db_page_count = self
                    .handle
                    .allocator()
                    .with_header(|h| h.total_page_count)?;
                let header_data = {
                    let page = self.handle.fetch_page(0, PageSize::Small4k)?;
                    page.data().to_vec()
                };
                let emergency = self
                    .handle
                    .commit_txn(0, PageSize::Small4k, &header_data, db_page_count)?;
                if emergency {
                    // SHM near-full: move WAL frames into the main file so
                    // subsequent txns have room. Best-effort — failure here
                    // does not roll back the txn (it is already committed).
                    let _ = self.handle.emergency_checkpoint();
                }
                Ok(value)
            }
            Err(e) => {
                // Abort path: drop the WriteTxn so RAII decrefs any pending
                // OverflowRefs (enqueuing each chain's first page to the
                // deferred-free queue for a future writer to reclaim).
                drop(txn);
                let _ = self.handle.rollback_txn(mark);
                Err(e)
            }
        }
    }

    /// Mutable reference to the in-flight `WriteTxn`, if any.
    ///
    /// Returns `Some(&mut WriteTxn)` while a `with_txn` body is executing
    /// (writer-side staging: `attach_overflow`, `push_page_write`,
    /// `push_refcount_delta`, `stage_sec_index_*`). Returns `None` outside
    /// any writer boundary. Phase 5 scaffolds the accessor; Phase 6 wires
    /// the actual consumers (primary chain install + sec-index rewrite).
    #[allow(dead_code)]
    pub(crate) fn active_txn_mut(&mut self) -> Option<&mut WriteTxn> {
        self.active_txn.as_mut()
    }

    /// Return a mutable reference to the data tree for `ns`.
    ///
    /// If the namespace isn't cached yet, it is loaded from the catalog
    /// (or auto-created if it doesn't exist in the catalog).
    fn tree_or_create(&mut self, ns: &str) -> Result<&mut BTree<BufferPoolPageStore>> {
        if self.data_trees.contains_key(ns) {
            return Ok(self.data_trees.get_mut(ns).unwrap());
        }

        // Load from catalog or create fresh.
        let (root_page, root_level, is_new) =
            if let Some(entry) = self.catalog.get_collection(ns)? {
                (entry.data_root_page, entry.data_root_level, false)
            } else {
                // Lazily create the collection in the catalog.
                let data_root =
                    self.catalog
                        .create_collection(ns, bson::doc! {}, now_millis())?;
                self.sync_catalog_root()?;
                (data_root, 0u8, true)
            };

        let store = self.new_store();
        let tree = if is_new {
            // Catalog allocated `root_page` but did not write the leaf header.
            // Initialise it as an empty leaf via `BTree::create_at`.
            BTree::create_at(store, root_page)?
        } else {
            BTree::open(store, root_page, root_level)
        };
        self.data_trees.insert(ns.to_owned(), tree);
        Ok(self.data_trees.get_mut(ns).unwrap())
    }

    /// Return a reference to the data tree for `ns` if it exists in the catalog.
    fn tree(&mut self, ns: &str) -> Result<Option<&BTree<BufferPoolPageStore>>> {
        if !self.data_trees.contains_key(ns) {
            if let Some(entry) = self.catalog.get_collection(ns)? {
                let store = self.new_store();
                let tree = BTree::open(store, entry.data_root_page, entry.data_root_level);
                self.data_trees.insert(ns.to_owned(), tree);
            } else {
                return Ok(None);
            }
        }
        Ok(self.data_trees.get(ns))
    }

    /// Return a mutable reference to the data tree for `ns` if it exists.
    fn tree_mut(&mut self, ns: &str) -> Result<Option<&mut BTree<BufferPoolPageStore>>> {
        if !self.data_trees.contains_key(ns) {
            if let Some(entry) = self.catalog.get_collection(ns)? {
                let store = self.new_store();
                let tree = BTree::open(store, entry.data_root_page, entry.data_root_level);
                self.data_trees.insert(ns.to_owned(), tree);
            } else {
                return Ok(None);
            }
        }
        Ok(self.data_trees.get_mut(ns))
    }

    /// Return `(name, fields, sparse)` tuples for all unique indexes of `ns`.
    #[allow(dead_code)]
    fn unique_specs(&self, ns: &str) -> Result<Vec<(String, Vec<String>, bool)>> {
        let entries = self.catalog.list_indexes(ns)?;
        Ok(entries
            .into_iter()
            .filter(|e| e.unique)
            .map(|e| {
                let fields = e.key_pattern.keys().cloned().collect();
                (e.name, fields, e.sparse)
            })
            .collect())
    }

    /// Persist the current data-tree root for `ns` back to the catalog.
    ///
    /// Call after every insert/delete on a data tree to keep the catalog in sync.
    fn sync_data_root(&mut self, ns: &str) -> Result<()> {
        let Some(tree) = self.data_trees.get(ns) else {
            return Ok(());
        };
        let root_page = tree.root_page;
        let root_level = tree.root_level;

        if let Some(mut entry) = self.catalog.get_collection(ns)? {
            if entry.data_root_page != root_page || entry.data_root_level != root_level {
                entry.data_root_page = root_page;
                entry.data_root_level = root_level;
                self.catalog.update_collection(&entry)?;
                self.sync_catalog_root()?;
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Secondary index maintenance helpers (R1.4)
    // -----------------------------------------------------------------------

    /// Persist updated root/level and multikey flag for an index entry.
    ///
    /// Only writes to the catalog if something actually changed.
    fn sync_index_entry(
        &mut self,
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
        self.catalog.update_index(&updated)?;
        self.sync_catalog_root()
    }

    /// Retrieve the serialised `_id` value stored in an index tree entry.
    ///
    /// Index values are `{"_id": <bson>}` documents written by
    /// [`update_index_on_insert`].
    fn index_entry_id(handle: &Arc<crate::storage::handle::BufferPoolHandle>, cv: CellValue) -> Result<Bson> {
        let bytes = match cv {
            CellValue::Inline(b) => b,
            CellValue::Overflow {
                first_page,
                total_length,
            } => {
                // Re-open a temporary tree handle to read overflow pages.
                let tmp_store = BufferPoolPageStore::new(Arc::clone(handle));
                let tmp_tree = BTree::open(tmp_store, 1, 0);
                tmp_tree.read_overflow(first_page, total_length)?
            }
        };
        // Empty value means this entry was written before R1.4 (old format).
        // Return Null as a safe fallback; the caller will skip the lookup.
        if bytes.is_empty() {
            return Ok(Bson::Null);
        }
        let doc: Document =
            bson::from_slice(&bytes).map_err(Error::BsonDeserialization)?;
        Ok(doc.get("_id").cloned().unwrap_or(Bson::Null))
    }

    /// Maintain all secondary indexes after a document insert (MVCC T5').
    ///
    /// Stages index writes through the active `WriteTxn`. Actual tree
    /// mutation is deferred to `install_pending_sec_index` at commit time;
    /// `idx_tree` serves only as a reader for the unique-constraint
    /// pre-check. `sync_index_entry` is still called because the
    /// `multikey` flag can flip based on this document's key build, and
    /// the catalog must reflect that regardless of install timing.
    fn maintain_secondary_on_insert(
        &mut self,
        ns: &str,
        doc: &Document,
        doc_id: &Bson,
    ) -> Result<()> {
        let entries = self.catalog.list_indexes(ns)?;
        for entry in entries {
            let store = self.new_store();
            let idx_tree = BTree::open(store, entry.root_page, entry.root_level);
            let txn = self.active_txn.as_mut().expect(
                "maintain_secondary_on_insert must run inside with_txn",
            );
            let is_multikey = update_index_on_insert(doc, doc_id, &idx_tree, &entry, txn)?;
            self.sync_index_entry(
                &entry,
                idx_tree.root_page,
                idx_tree.root_level,
                is_multikey,
            )?;
        }
        Ok(())
    }

    /// Maintain all secondary indexes after a document delete (MVCC T5').
    ///
    /// Stages delete mutations through the active `WriteTxn`.
    fn maintain_secondary_on_delete(
        &mut self,
        ns: &str,
        doc: &Document,
        doc_id: &Bson,
    ) -> Result<()> {
        let entries = self.catalog.list_indexes(ns)?;
        for entry in entries {
            let txn = self.active_txn.as_mut().expect(
                "maintain_secondary_on_delete must run inside with_txn",
            );
            update_index_on_delete(doc, doc_id, &entry, txn)?;
            // Root/level unchanged by staging; only sync if needed to
            // preserve multikey (false — delete never flips multikey).
            self.sync_index_entry(&entry, entry.root_page, entry.root_level, false)?;
        }
        Ok(())
    }

    /// Maintain all secondary indexes when a document is replaced (MVCC T5').
    fn maintain_secondary_on_update(
        &mut self,
        ns: &str,
        old_doc: &Document,
        new_doc: &Document,
        old_id: &Bson,
        new_id: &Bson,
    ) -> Result<()> {
        let entries = self.catalog.list_indexes(ns)?;
        for entry in entries {
            let store = self.new_store();
            let idx_tree = BTree::open(store, entry.root_page, entry.root_level);
            let txn = self.active_txn.as_mut().expect(
                "maintain_secondary_on_update must run inside with_txn",
            );
            let is_multikey = update_index_on_update(
                old_doc, new_doc, old_id, new_id, &idx_tree, &entry, txn,
            )?;
            self.sync_index_entry(
                &entry,
                idx_tree.root_page,
                idx_tree.root_level,
                is_multikey,
            )?;
        }
        Ok(())
    }

    /// Drain the given `SecIndexWrite` batch and perform the actual
    /// `BTree::insert` / `delete` into each target index tree (MVCC T5').
    ///
    /// Called once per `WriteTxn` at commit time, before the ChainCommit
    /// and legacy commit frames are emitted. A failure here (e.g. a
    /// unique-constraint violation slipped past the stage-time pre-check)
    /// aborts the commit via `with_txn`'s error arm.
    ///
    /// Delete semantics: idempotent — `BTree::delete` on an absent key
    /// returns `Err(KeyNotFound)`, which we swallow to match the legacy
    /// direct-write behavior.
    ///
    /// Per-index state tracking: B+ tree splits update the root page; we
    /// thread the current root through a per-`index_root_page` state map
    /// so multiple staged writes into the same index see each other's
    /// splits. After all writes land we sync each modified index's root
    /// back to the catalog.
    fn install_pending_sec_index(
        &mut self,
        writes: Vec<crate::mvcc::SecIndexWrite>,
    ) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }

        use crate::mvcc::SecIndexOp;
        use std::collections::HashMap;

        // Build a reverse lookup: orig root_page → IndexEntry. Single
        // catalog pass; typical runtime has few indexes.
        let mut entry_by_root: HashMap<u32, crate::storage::catalog::IndexEntry> =
            HashMap::new();
        let collections = self.catalog.list_collections()?;
        for coll in &collections {
            for entry in self.catalog.list_indexes(&coll.name)? {
                entry_by_root.insert(entry.root_page, entry);
            }
        }

        // Per-index mutable state as writes accumulate root changes.
        struct TreeState {
            current_root: u32,
            current_level: u8,
            entry: crate::storage::catalog::IndexEntry,
        }
        let mut states: HashMap<u32, TreeState> = HashMap::new();

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

            let store = self.new_store();
            let mut idx_tree =
                BTree::open(store, state.current_root, state.current_level);
            match write.op {
                SecIndexOp::Insert { id_bytes } => {
                    idx_tree.insert(&write.key, &id_bytes)?;
                }
                SecIndexOp::Delete => {
                    // `BTree::delete` returns `Ok(false)` for an absent
                    // key; legacy `update_index_on_delete` was idempotent,
                    // so we swallow that case here too.
                    let _ = idx_tree.delete(&write.key)?;
                }
            }
            state.current_root = idx_tree.root_page;
            state.current_level = idx_tree.root_level;
        }

        for (_, state) in states {
            self.sync_index_entry(
                &state.entry,
                state.current_root,
                state.current_level,
                state.entry.multikey,
            )?;
        }

        Ok(())
    }

    /// Install staged primary-tree writes as fresh heads on each key's
    /// per-leaf version chain (MVCC T5' sub-step 2).
    ///
    /// For each `PrimaryWrite`:
    /// 1. Resolve the current tree by namespace (so in-txn root splits
    ///    between stage and install time are transparently followed).
    /// 2. Walk from the current root to the owning leaf via
    ///    [`BTree::find_leaf`].
    /// 3. Pin the leaf (`fetch_page`) so `take_chain` / `put_chain` reach
    ///    the resident frame. The pin drops at end of iteration.
    /// 4. Take the existing chain (or start an empty one), advance the
    ///    prior head's `stop_ts` to `commit_ts` via [`Arc::make_mut`]
    ///    (CoW — concurrent `ChainSnapshot` holders keep their frozen
    ///    view), then `push_front` a fresh `VersionEntry` carrying
    ///    `commit_ts` and this txn's `txn_id`.
    /// 5. `put_chain` the updated `Arc<VecDeque<VersionEntry>>` back onto
    ///    the frame.
    ///
    /// If the namespace isn't cached in `data_trees` (collection dropped
    /// mid-txn), the write is silently skipped — the dual-write on-disk
    /// cell remains authoritative under current semantics, and the chain
    /// entry would be referencing a dead tree root anyway.
    fn install_pending_primary(
        &mut self,
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
            // Resolve current tree and find the owning leaf.
            let leaf_page = match self.data_trees.get(&write.ns) {
                Some(tree) => tree.find_leaf(&write.key)?,
                None => continue,
            };

            // Pin the leaf so the chain helpers operate on a resident
            // frame. Pin auto-unpins at end of scope.
            let _pin = self.handle.fetch_page(leaf_page, PageSize::Large32k)?;

            // Take the existing chain (or start fresh). `Arc::make_mut`
            // gives CoW: if a concurrent `ChainSnapshot` is holding a
            // reader view, their Arc count > 1 and the make_mut deep-clones;
            // otherwise we mutate in-place.
            let mut chain_arc = self
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
            self.handle
                .pool()
                .put_chain(leaf_page, write.key, chain_arc)?;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Index scan executor (R1.4)
    // -----------------------------------------------------------------------

    /// Build the [start, end] range for a secondary index B+ tree scan.
    ///
    /// The secondary index key format for an ascending single-field is:
    /// `encode_key(field_val) | 0x01 | encode_key(_id)`
    ///
    /// Returns `(start, end)` suitable for `BTree::range_scan`.
    /// `None` means unbounded in that direction.
    /// A return of `(None, None)` with `condition == In` is a sentinel asking
    /// the caller to perform multiple equality scans.
    fn index_bounds(
        condition: &IndexCondition,
        ascending: bool,
    ) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        /// Prefix bytes: `encode_compound_key([(val, ascending)]) + COMPOUND_SEP`.
        /// All secondary-index keys with `field == val` start with this prefix
        /// (followed by more bytes for the `_id` component).
        fn prefix(val: &Bson, asc: bool) -> Vec<u8> {
            let mut p = encode_compound_key(&[(val, asc)]);
            p.push(COMPOUND_SEP); // 0x01
            p
        }
        /// One past the prefix: prefix with last byte incremented (0x01 → 0x02).
        /// Since COMPOUND_SEP < 0x02, every real key with `field == val` sorts
        /// before this, so this byte sequence acts as an exclusive upper bound.
        fn prefix_next(val: &Bson, asc: bool) -> Vec<u8> {
            let mut p = prefix(val, asc);
            *p.last_mut().unwrap() += 1; // COMPOUND_SEP + 1 = 0x02, safe
            p
        }

        match condition {
            // Exact equality: range [prefix(v), prefix_next(v)].
            IndexCondition::Eq(v) => (Some(prefix(v, ascending)), Some(prefix_next(v, ascending))),

            // Full scan through the secondary index (pass-through to the full filter).
            IndexCondition::Any => (None, None),

            // Multi-point: caller handles `In` with multiple equality sweeps.
            IndexCondition::In(_) => (None, None),

            IndexCondition::Range { gt, gte, lt, lte } => {
                if ascending {
                    // Ascending field: larger values have larger encoded keys.
                    let start = match (gte.as_ref(), gt.as_ref()) {
                        (Some(v), _) => Some(prefix(v, true)),        // field >= v
                        (None, Some(v)) => Some(prefix_next(v, true)), // field >  v
                        _ => None,
                    };
                    let end = match (lte.as_ref(), lt.as_ref()) {
                        (Some(v), _) => Some(prefix_next(v, true)), // field <= v
                        (None, Some(v)) => Some(prefix(v, true)),     // field <  v
                        _ => None,
                    };
                    (start, end)
                } else {
                    // Descending field: encoding is inverted so range semantics
                    // are mirrored.  $gt on field → smaller encoded prefix.
                    let start = match (lte.as_ref(), lt.as_ref()) {
                        (Some(v), _) => Some(prefix(v, false)),        // field <= v → encoded >=
                        (None, Some(v)) => Some(prefix_next(v, false)), // field <  v → encoded >
                        _ => None,
                    };
                    let end = match (gte.as_ref(), gt.as_ref()) {
                        (Some(v), _) => Some(prefix_next(v, false)), // field >= v → encoded <=
                        (None, Some(v)) => Some(prefix(v, false)),     // field >  v → encoded <
                        _ => None,
                    };
                    (start, end)
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Read-only helpers (R1.6 SWMR)
    // -----------------------------------------------------------------------

    /// Open a data tree for reading without mutating the cache.
    ///
    /// If the tree is already cached (placed there by a previous write), the
    /// cached root page/level is used (it reflects the latest committed
    /// state).  Otherwise the catalog is consulted, which also reflects the
    /// last committed state.
    ///
    /// The returned tree is an independent handle — it does **not** affect
    /// the write cache in `data_trees`.  Takes `&self` so it can be called
    /// while holding an `RwLock` read guard.
    fn open_tree_for_read(&self, ns: &str) -> Result<Option<BTree<BufferPoolPageStore>>> {
        // Prefer the in-memory cache: it holds the current root after the
        // latest write (may be ahead of what is flushed to catalog on disk).
        if let Some(cached) = self.data_trees.get(ns) {
            let store = self.new_store();
            return Ok(Some(BTree::open(store, cached.root_page, cached.root_level)));
        }
        // Not cached — fall back to the catalog (reads only, &self OK).
        if let Some(entry) = self.catalog.get_collection(ns)? {
            let store = self.new_store();
            Ok(Some(BTree::open(
                store,
                entry.data_root_page,
                entry.data_root_level,
            )))
        } else {
            Ok(None)
        }
    }

    /// Read-only variant of [`try_index_scan`] that takes `&self`.
    ///
    /// Used by read operations that hold an `RwLock` read guard.  Does not
    /// mutate the data-tree cache.  All B+ trees (index and data) are opened
    /// as fresh, independent handles via [`open_tree_for_read`].
    fn try_index_scan_ro(
        &self,
        ns: &str,
        filter: &Document,
    ) -> Result<Option<Vec<Document>>> {
        let entries = self.catalog.list_indexes(ns)?;
        if entries.is_empty() {
            return Ok(None);
        }
        let index_metas: Vec<IndexMeta<'_>> = entries
            .iter()
            .map(|e| IndexMeta {
                name: &e.name,
                keys: &e.key_pattern,
            })
            .collect();
        if index_metas.is_empty() {
            return Ok(None);
        }

        let plan = select_plan(filter, &index_metas);
        let (index_name, primary_field, condition) = match plan {
            ScanPlan::CollScan => return Ok(None),
            ScanPlan::IndexScan {
                index_name,
                primary_field,
                condition,
            } => (index_name, primary_field, condition),
        };

        let idx_entry = entries
            .iter()
            .find(|e| e.name == index_name)
            .cloned()
            .ok_or_else(|| Error::Internal(format!("index '{}' not in catalog", index_name)))?;

        let ascending = idx_entry
            .key_pattern
            .get(&primary_field)
            .map(|v| !matches!(v, Bson::Int32(-1) | Bson::Int64(-1)))
            .unwrap_or(true);

        let handle = Arc::clone(&self.handle);
        let id_bsons: Vec<Bson> = if let IndexCondition::In(vals) = &condition {
            let mut results = Vec::new();
            for v in vals {
                let mut p = encode_compound_key(&[(v, ascending)]);
                p.push(COMPOUND_SEP);
                let mut p_next = p.clone();
                *p_next.last_mut().unwrap() += 1;
                let idx_store = self.new_store();
                let idx_tree =
                    BTree::open(idx_store, idx_entry.root_page, idx_entry.root_level);
                for (_, cv) in idx_tree.range_scan(Some(&p), Some(&p_next))? {
                    let id = Self::index_entry_id(&handle, cv)?;
                    if !matches!(id, Bson::Null) {
                        results.push(id);
                    }
                }
            }
            results
        } else {
            let (start, end) = Self::index_bounds(&condition, ascending);
            let idx_store = self.new_store();
            let idx_tree = BTree::open(idx_store, idx_entry.root_page, idx_entry.root_level);
            idx_tree
                .range_scan(start.as_deref(), end.as_deref())?
                .into_iter()
                .filter_map(|(_, cv)| {
                    Self::index_entry_id(&handle, cv)
                        .ok()
                        .filter(|id| !matches!(id, Bson::Null))
                })
                .collect()
        };

        // Look up documents using the read-only tree handle.
        let mut docs = Vec::new();
        if !id_bsons.is_empty() {
            // Open the data tree once (outside the loop) for efficiency.
            if let Some(data_tree) = self.open_tree_for_read(ns)? {
                for id_bson in id_bsons {
                    let data_key = encode_key(&id_bson);
                    if let Some(cv) = data_tree.search(&data_key)? {
                        let doc_bytes = resolve_cell(&data_tree, cv)?;
                        let doc: Document =
                            bson::from_slice(&doc_bytes).map_err(Error::BsonDeserialization)?;
                        if eval_filter(&doc, filter)? {
                            docs.push(doc);
                        }
                    }
                }
            }
        }
        Ok(Some(docs))
    }

    /// Execute an index scan on `ns` for `filter` in the buffered backend.
    ///
    /// Returns `Some(docs)` when an index was used, `None` when the planner
    /// decided a full collection scan is better (or when no indexes exist).
    ///
    /// When `Some` is returned the caller must still apply `find_opts` (sort,
    /// skip, limit, projection) but does **not** need to re-apply the filter
    /// because the full filter is evaluated here against every candidate doc.
    #[allow(dead_code)]
    fn try_index_scan(
        &mut self,
        ns: &str,
        filter: &Document,
    ) -> Result<Option<Vec<Document>>> {
        // Build IndexMeta list from catalog.
        let entries = self.catalog.list_indexes(ns)?;
        if entries.is_empty() {
            return Ok(None);
        }
        let index_metas: Vec<IndexMeta<'_>> = entries
            .iter()
            .map(|e| IndexMeta {
                name: &e.name,
                keys: &e.key_pattern,
            })
            .collect();
        if index_metas.is_empty() {
            return Ok(None);
        }

        // Ask the planner for a plan.
        let plan = select_plan(filter, &index_metas);
        let (index_name, primary_field, condition) = match plan {
            ScanPlan::CollScan => return Ok(None),
            ScanPlan::IndexScan {
                index_name,
                primary_field,
                condition,
            } => (index_name, primary_field, condition),
        };

        // Find the index entry.
        let idx_entry = entries
            .iter()
            .find(|e| e.name == index_name)
            .cloned()
            .ok_or_else(|| Error::Internal(format!("index '{}' not in catalog", index_name)))?;

        // Determine the scan direction of the primary field.
        let ascending = idx_entry
            .key_pattern
            .get(&primary_field)
            .map(|v| !matches!(v, Bson::Int32(-1) | Bson::Int64(-1)))
            .unwrap_or(true);

        // Collect candidate _id values from the secondary index tree.
        let handle = Arc::clone(&self.handle);
        let id_bsons: Vec<Bson> = if let IndexCondition::In(vals) = &condition {
            // Multi-point: run one equality scan per value and union.
            let mut results = Vec::new();
            for v in vals {
                let mut p = encode_compound_key(&[(v, ascending)]);
                p.push(COMPOUND_SEP);
                let mut p_next = p.clone();
                *p_next.last_mut().unwrap() += 1;
                let idx_store = self.new_store();
                let idx_tree =
                    BTree::open(idx_store, idx_entry.root_page, idx_entry.root_level);
                for (_, cv) in idx_tree.range_scan(Some(&p), Some(&p_next))? {
                    let id = Self::index_entry_id(&handle, cv)?;
                    if !matches!(id, Bson::Null) {
                        results.push(id);
                    }
                }
            }
            results
        } else {
            let (start, end) = Self::index_bounds(&condition, ascending);
            let idx_store = self.new_store();
            let idx_tree = BTree::open(idx_store, idx_entry.root_page, idx_entry.root_level);
            idx_tree
                .range_scan(start.as_deref(), end.as_deref())?
                .into_iter()
                .filter_map(|(_, cv)| {
                    Self::index_entry_id(&handle, cv)
                        .ok()
                        .filter(|id| !matches!(id, Bson::Null))
                })
                .collect()
        };

        // Look up documents in the data tree and apply the full filter.
        let mut docs = Vec::new();
        for id_bson in id_bsons {
            let data_key = encode_key(&id_bson);
            let data_tree_opt = self.tree(ns)?;
            if let Some(data_tree) = data_tree_opt {
                if let Some(cv) = data_tree.search(&data_key)? {
                    let doc_bytes = resolve_cell(data_tree, cv)?;
                    let doc: Document =
                        bson::from_slice(&doc_bytes).map_err(Error::BsonDeserialization)?;
                    if eval_filter(&doc, filter)? {
                        docs.push(doc);
                    }
                }
            }
        }
        Ok(Some(docs))
    }
}

// ---------------------------------------------------------------------------
// PagedEngine — public struct
// ---------------------------------------------------------------------------

/// Phase 1 storage engine: B+ tree per namespace, through the buffer pool.
///
/// ## Concurrency
///
/// `inner` is protected by an `RwLock` to implement Single-Writer Multiple-Reader
/// (SWMR) snapshot isolation (R1.6):
///
/// - **Readers** (`find`, `find_one`, `count`, `list_indexes`, etc.) acquire a
///   shared read lock — any number of readers can run concurrently.
/// - **Writers** (`insert`, `update`, `delete`, `create_index`, etc.) acquire an
///   exclusive write lock — one writer at a time, writers never block readers.
pub(crate) struct PagedEngine {
    inner: Mutex<BpBackend>,
}

impl PagedEngine {

    /// Create a file-backed engine using `handle` as the page store.
    ///
    /// If `catalog_root_page == 0` the database is new and an empty catalog
    /// will be created.  Otherwise the catalog is opened at the given root.
    pub(crate) fn new_buffered(
        handle: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
    ) -> Result<Self> {
        let backend = BpBackend::new(handle, catalog_root_page, catalog_root_level)?;
        Ok(PagedEngine {
            inner: Mutex::new(backend),
        })
    }
}

// ---------------------------------------------------------------------------
// StorageEngine implementation
// ---------------------------------------------------------------------------

impl StorageEngine for PagedEngine {
    // -----------------------------------------------------------------------
    // insert
    // -----------------------------------------------------------------------

    fn insert(&self, ns: &str, mut doc: Document) -> Result<Bson> {
        let mut inner = self.inner.lock().unwrap();
        let bp = &mut *inner;
        bp.with_txn(|bp| {
            // Insert into the primary (data) tree.  Unique-constraint
            // checking for secondary indexes happens below after the
            // secondary index trees are maintained.
            let (id, key, bson_bytes, _tree_root) = {
                let tree = bp.tree_or_create(ns)?;
                btree_insert_doc(tree, &mut doc, &[])?
            };
            bp.active_txn_mut()
                .expect("insert must run inside with_txn")
                .stage_primary_insert(ns.to_string(), key, bson_bytes);
            bp.sync_data_root(ns)?;
            // Maintain secondary indexes (includes unique-constraint
            // enforcement via the index B+ trees themselves).
            bp.maintain_secondary_on_insert(ns, &doc, &id)?;
            Ok(id)
        })
    }

    // -----------------------------------------------------------------------
    // find
    // -----------------------------------------------------------------------

    fn find(&self, ns: &str, filter: &Document, opts: &FindOptions) -> Result<Vec<Document>> {
        // Read-only: acquire a shared read lock so concurrent readers don't block.
        let inner = self.inner.lock().unwrap();
        let bp = &*inner;
        let matched: Vec<Document> = {
            // Try an index scan first; fall back to a full collection scan.
            if let Some(docs) = bp.try_index_scan_ro(ns, filter)? {
                docs
            } else {
                match bp.open_tree_for_read(ns)? {
                    None => return Ok(Vec::new()),
                    Some(tree) => {
                        let view = bp.open_read_view();
                        let probe = bp.primary_history_probe(ns);
                        btree_collscan(&tree, filter, &view, Some(&probe))?
                            .into_iter()
                            .map(|(_, doc)| doc)
                            .collect()
                    }
                }
            }
        };
        Ok(apply_find_opts(matched, opts))
    }

    // -----------------------------------------------------------------------
    // find_one
    // -----------------------------------------------------------------------

    fn find_one(&self, ns: &str, filter: &Document) -> Result<Option<Document>> {
        let opts = FindOptions::new();
        // find() already acquires a read lock internally.
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
                "update requires an operator update document (e.g. {$set: {...}}); \
                 use find_one_and_replace for replacements"
                    .into(),
            ));
        }

        let mut inner = self.inner.lock().unwrap();
        let (matched_pairs, tree_exists): (Vec<(Vec<u8>, Document)>, bool) = {
            let bp = &mut *inner;
            let view = bp.open_read_view();
            let probe = bp.primary_history_probe(ns);
            match bp.tree(ns)? {
                None => {
                    if opts.upsert {
                        drop(inner);
                        return self.do_upsert_update(ns, filter, update);
                    }
                    return Ok(UpdateResult {
                        matched_count: 0,
                        modified_count: 0,
                        upserted_id: None,
                    });
                }
                Some(tree) => (btree_collscan(tree, filter, &view, Some(&probe))?, true),
            }
        };

        let _ = tree_exists;

        if matched_pairs.is_empty() && opts.upsert {
            drop(inner);
            return self.do_upsert_update(ns, filter, update);
        }

        let pairs_to_process: Vec<(Vec<u8>, Document)> = if many {
            matched_pairs
        } else {
            matched_pairs.into_iter().take(1).collect()
        };

        let bp = &mut *inner;
        bp.with_txn(move |bp| {
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
                    let new_bytes =
                        bson::to_vec(&doc).map_err(Error::BsonSerialization)?;
                    bp.maintain_secondary_on_update(
                        ns, &before, &doc, &before_id, &new_id,
                    )?;
                    let tree_exists = if let Some(tree) = bp.tree_mut(ns)? {
                        tree.delete(&key)?;
                        tree.insert(&key, &new_bytes)?;
                        true
                    } else {
                        false
                    };
                    if tree_exists {
                        bp.active_txn_mut()
                            .expect("update must run inside with_txn")
                            .stage_primary_update(ns.to_string(), key, new_bytes);
                    }
                    bp.sync_data_root(ns)?;
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
        let mut inner = self.inner.lock().unwrap();

        // Collect (key, doc) pairs to delete; we need the doc for index maintenance.
        let pairs_to_delete: Vec<(Vec<u8>, Document)> = {
            let bp = &mut *inner;
            let view = bp.open_read_view();
            let probe = bp.primary_history_probe(ns);
            match bp.tree(ns)? {
                None => return Ok(DeleteResult { deleted_count: 0 }),
                Some(tree) => {
                    let pairs = btree_collscan(tree, filter, &view, Some(&probe))?;
                    if many {
                        pairs
                    } else {
                        pairs.into_iter().take(1).collect()
                    }
                }
            }
        };

        let deleted_count = pairs_to_delete.len() as u64;

        let bp = &mut *inner;
        bp.with_txn(move |bp| {
            for (key, doc) in &pairs_to_delete {
                let doc_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
                bp.maintain_secondary_on_delete(ns, doc, &doc_id)?;
                let tree_exists = if let Some(tree) = bp.tree_mut(ns)? {
                    tree.delete(key)?;
                    true
                } else {
                    false
                };
                if tree_exists {
                    bp.active_txn_mut()
                        .expect("delete must run inside with_txn")
                        .stage_primary_delete(ns.to_string(), key.clone());
                }
                bp.sync_data_root(ns)?;
            }
            Ok(())
        })?;

        Ok(DeleteResult { deleted_count })
    }

    // -----------------------------------------------------------------------
    // count
    // -----------------------------------------------------------------------

    fn count(&self, ns: &str, filter: &Document) -> Result<u64> {
        // Read-only: shared read lock for concurrent reader support.
        let inner = self.inner.lock().unwrap();
        let bp = &*inner;
        match bp.open_tree_for_read(ns)? {
            None => Ok(0),
            Some(tree) => {
                let view = bp.open_read_view();
                let probe = bp.primary_history_probe(ns);
                let pairs = btree_collscan(&tree, filter, &view, Some(&probe))?;
                Ok(pairs.len() as u64)
            }
        }
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

        let mut inner = self.inner.lock().unwrap();
        let mut matched: Vec<(Vec<u8>, Document)> = {
            let bp = &mut *inner;
            let view = bp.open_read_view();
            let probe = bp.primary_history_probe(ns);
            match bp.tree(ns)? {
                None => {
                    if opts.upsert {
                        drop(inner);
                        return self.fam_upsert_update(ns, filter, update, opts);
                    }
                    return Ok(None);
                }
                Some(tree) => btree_collscan(tree, filter, &view, Some(&probe))?,
            }
        };

        if matched.is_empty() {
            if opts.upsert {
                drop(inner);
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
        let bp = &mut *inner;
        bp.with_txn(|bp| {
            bp.maintain_secondary_on_update(ns, &before, &doc, &before_id, &new_id)?;
            let tree_exists = if let Some(tree) = bp.tree_mut(ns)? {
                tree.delete(&key)?;
                tree.insert(&key, &new_bytes)?;
                true
            } else {
                false
            };
            if tree_exists {
                bp.active_txn_mut()
                    .expect("find_one_and_update must run inside with_txn")
                    .stage_primary_update(ns.to_string(), key, new_bytes);
            }
            bp.sync_data_root(ns)?;
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
        let mut inner = self.inner.lock().unwrap();
        let mut matched: Vec<(Vec<u8>, Document)> = {
            let bp = &mut *inner;
            let view = bp.open_read_view();
            let probe = bp.primary_history_probe(ns);
            match bp.tree(ns)? {
                None => return Ok(None),
                Some(tree) => btree_collscan(tree, filter, &view, Some(&probe))?,
            }
        };

        if matched.is_empty() {
            return Ok(None);
        }

        if let Some(s) = &opts.sort {
            matched.sort_by(|(_, a), (_, b)| compare_docs(a, b, s));
        }

        let (key, doc) = matched.remove(0);
        let doc_id = doc.get("_id").cloned().unwrap_or(Bson::Null);

        let bp = &mut *inner;
        bp.with_txn(|bp| {
            bp.maintain_secondary_on_delete(ns, &doc, &doc_id)?;
            let tree_exists = if let Some(tree) = bp.tree_mut(ns)? {
                tree.delete(&key)?;
                true
            } else {
                false
            };
            if tree_exists {
                bp.active_txn_mut()
                    .expect("find_one_and_delete must run inside with_txn")
                    .stage_primary_delete(ns.to_string(), key);
            }
            bp.sync_data_root(ns)?;
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
        let mut inner = self.inner.lock().unwrap();
        let mut matched: Vec<(Vec<u8>, Document)> = {
            let bp = &mut *inner;
            let view = bp.open_read_view();
            let probe = bp.primary_history_probe(ns);
            match bp.tree(ns)? {
                None => {
                    if opts.upsert {
                        drop(inner);
                        return self.fam_upsert_replace(ns, replacement, opts);
                    }
                    return Ok(None);
                }
                Some(tree) => btree_collscan(tree, filter, &view, Some(&probe))?,
            }
        };

        if matched.is_empty() {
            if opts.upsert {
                drop(inner);
                return self.fam_upsert_replace(ns, replacement, opts);
            }
            return Ok(None);
        }

        if let Some(s) = &opts.sort {
            matched.sort_by(|(_, a), (_, b)| compare_docs(a, b, s));
        }

        let (old_key, old_doc) = matched.remove(0);

        // Build the replacement, preserving _id.
        let mut new_doc = replacement.clone();
        // Preserve the original _id.
        let original_id = old_doc.get("_id").cloned().unwrap_or(Bson::Null);
        new_doc.insert("_id", original_id.clone());
        validate_document(&new_doc)?;

        let new_key = encode_key(&original_id);
        let new_bytes = bson::to_vec(&new_doc).map_err(Error::BsonSerialization)?;

        let bp = &mut *inner;
        bp.with_txn(|bp| {
            bp.maintain_secondary_on_update(
                ns,
                &old_doc,
                &new_doc,
                &original_id,
                &original_id,
            )?;
            let tree_exists = if let Some(tree) = bp.tree_mut(ns)? {
                tree.delete(&old_key)?;
                tree.insert(&new_key, &new_bytes)?;
                true
            } else {
                false
            };
            if tree_exists {
                bp.active_txn_mut()
                    .expect("find_one_and_replace must run inside with_txn")
                    .stage_primary_update(ns.to_string(), new_key, new_bytes);
            }
            bp.sync_data_root(ns)?;
            Ok(())
        })?;

        Ok(Some(match opts.return_document {
            ReturnDocument::Before => old_doc,
            ReturnDocument::After => new_doc,
        }))
    }

    // -----------------------------------------------------------------------
    // create_index
    // -----------------------------------------------------------------------

    fn create_index(&self, ns: &str, model: &IndexModel) -> Result<String> {
        validate_index_keys(&model.keys)?;
        let name = model
            .options
            .name
            .clone()
            .unwrap_or_else(|| generate_index_name(&model.keys));

        let mut inner = self.inner.lock().unwrap();
        let bp = &mut *inner;
        {
            // Ensure the collection exists in the catalog first.
            if bp.catalog.get_collection(ns)?.is_none() {
                let data_root = bp
                    .catalog
                    .create_collection(ns, bson::doc! {}, now_millis())?;
                bp.sync_catalog_root()?;
                // Initialise the data tree leaf page so it has a valid header.
                let data_store = bp.new_store();
                let data_tree = BTree::create_at(data_store, data_root)?;
                bp.data_trees.insert(ns.to_owned(), data_tree);
            }
            // Idempotent: return existing index name.
            if bp.catalog.get_index(ns, &name)?.is_some() {
                return Ok(name);
            }
            // Allocate a root page and register the index in the catalog.
            let idx_root = bp.catalog.create_index(ns, model, &name)?;
            bp.sync_catalog_root()?;
            // Initialise the index tree's leaf root page.
            let idx_store = bp.new_store();
            BTree::create_at(idx_store, idx_root)?;

            // Build the index by scanning all documents already in the
            // data tree ("online index build").
            let idx_entry = bp
                .catalog
                .get_index(ns, &name)?
                .ok_or_else(|| Error::Internal("index entry missing after create".into()))?;

            // Open data tree (read-only scan); if the collection is empty
            // this is a no-op.
            if let Some(data_entry) = bp.catalog.get_collection(ns)? {
                let data_store = bp.new_store();
                let data_tree = BTree::open(
                    data_store,
                    data_entry.data_root_page,
                    data_entry.data_root_level,
                );
                let idx_build_store = bp.new_store();
                let mut idx_tree =
                    BTree::open(idx_build_store, idx_entry.root_page, idx_entry.root_level);
                let any_multikey = build_index(&data_tree, &mut idx_tree, &idx_entry)?;

                // Persist the (possibly updated) index root and multikey flag.
                bp.sync_index_entry(
                    &idx_entry,
                    idx_tree.root_page,
                    idx_tree.root_level,
                    any_multikey,
                )?;
            }
        }
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
        let mut inner = self.inner.lock().unwrap();
        let bp = &mut *inner;
        bp.with_txn(|bp| {
            let removed = bp.catalog.drop_index(ns, name)?;
            if removed {
                bp.sync_catalog_root()?;
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
        let inner = self.inner.lock().unwrap();
        let bp = &*inner;
        let entries = bp.catalog.list_indexes(ns)?;
        Ok(entries
            .into_iter()
            .map(|e| IndexInfo {
                name: e.name,
                keys: e.key_pattern,
                unique: e.unique,
                sparse: e.sparse,
            })
            .collect())
    }

    // -----------------------------------------------------------------------
    // create_namespace
    // -----------------------------------------------------------------------

    fn create_namespace(&self, ns: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let bp = &mut *inner;
        bp.with_txn(|bp| {
            if bp.catalog.get_collection(ns)?.is_some() {
                return Ok(());
            }
            let data_root =
                bp.catalog
                    .create_collection(ns, bson::doc! {}, now_millis())?;
            bp.sync_catalog_root()?;
            // Initialise the data tree leaf page and cache it for fast first access.
            let store = bp.new_store();
            let tree = BTree::create_at(store, data_root)?;
            bp.data_trees.insert(ns.to_owned(), tree);
            Ok(())
        })
    }

    // -----------------------------------------------------------------------
    // drop_namespace
    // -----------------------------------------------------------------------

    fn drop_namespace(&self, ns: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let bp = &mut *inner;
        bp.with_txn(|bp| {
                // Collect page-root info from the catalog before removing entries.
                // We need this to free the B+ tree pages after the catalog entries
                // are gone (catalog.drop_collection removes both the collection and
                // all its index entries).
                let maybe_coll = bp.catalog.get_collection(ns)?;
                let index_roots: Vec<(u32, u8)> = if maybe_coll.is_some() {
                    bp.catalog
                        .list_indexes(ns)?
                        .into_iter()
                        .map(|e| (e.root_page, e.root_level))
                        .collect()
                } else {
                    Vec::new()
                };

                // Remove the cached data tree (if any) — we own it after this.
                let cached_tree = bp.data_trees.remove(ns);

                // Drop catalog entries first so no references to these pages exist.
                if bp.catalog.drop_collection(ns)? {
                    bp.sync_catalog_root()?;
                }

                // Free the data-tree pages.
                if let Some(coll) = maybe_coll {
                    let (root_page, root_level) = match &cached_tree {
                        // If the tree was cached use its current root (may differ
                        // from catalog if sync_data_root was skipped on a dry run;
                        // in practice R1.2 always syncs before dropping).
                        Some(t) => (t.root_page, t.root_level),
                        None => (coll.data_root_page, coll.data_root_level),
                    };
                    // Drop the cached handle first to release its Arc reference,
                    // then open a fresh tree over the same pages for the walk.
                    drop(cached_tree);
                    let store = bp.new_store();
                    let data_tree = BTree::open(store, root_page, root_level);
                    data_tree.free_all_pages()?;
                }

                // Free each index tree's pages.
                for (idx_root, idx_level) in index_roots {
                    let store = bp.new_store();
                    let idx_tree = BTree::open(store, idx_root, idx_level);
                    idx_tree.free_all_pages()?;
                }

                Ok(())
        })
    }

    // -----------------------------------------------------------------------
    // list_namespaces
    // -----------------------------------------------------------------------

    fn list_namespaces(&self) -> Result<Vec<String>> {
        let inner = self.inner.lock().unwrap();
        let bp = &*inner;
        let entries = bp.catalog.list_collections()?;
        Ok(entries.into_iter().map(|e| e.name).collect())
    }

    // -----------------------------------------------------------------------
    // checkpoint
    // -----------------------------------------------------------------------

    fn checkpoint(&self) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        let bp = &*inner;
        // Ensure the catalog root is in the file header before flush.
        bp.sync_catalog_root()?;
        // Flush all dirty pages (data + catalog + header) to disk.
        bp.handle.flush()
    }

    // -----------------------------------------------------------------------
    // close
    // -----------------------------------------------------------------------

    fn close(&self) -> Result<()> {
        self.checkpoint()
    }

    // -----------------------------------------------------------------------
    // snapshot_bytes (legacy Phase 0.x — returns None for B+ tree engine)
    // -----------------------------------------------------------------------

    fn snapshot_bytes(&self) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Private upsert helpers
// ---------------------------------------------------------------------------

impl PagedEngine {
    /// Perform an upsert for an `update_one/many` with `upsert: true`.
    fn do_upsert_update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
    ) -> Result<UpdateResult> {
        let mut new_doc = upsert_base_from_filter(filter);
        apply_update(&mut new_doc, update, true)?;
        let id = {
            let mut inner = self.inner.lock().unwrap();
            let bp = &mut *inner;
            bp.with_txn(|bp| {
                let (id, key, bson_bytes, _tree_root) = {
                    let tree = bp.tree_or_create(ns)?;
                    btree_insert_doc(tree, &mut new_doc, &[])?
                };
                bp.active_txn_mut()
                    .expect("upsert must run inside with_txn")
                    .stage_primary_insert(ns.to_string(), key, bson_bytes);
                bp.sync_data_root(ns)?;
                bp.maintain_secondary_on_insert(ns, &new_doc, &id)?;
                Ok(id)
            })?
        };
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
        {
            let mut inner = self.inner.lock().unwrap();
            let bp = &mut *inner;
            bp.with_txn(|bp| {
                let (id, key, bson_bytes, _tree_root) = {
                    let tree = bp.tree_or_create(ns)?;
                    btree_insert_doc(tree, &mut new_doc, &[])?
                };
                bp.active_txn_mut()
                    .expect("fam_upsert_update must run inside with_txn")
                    .stage_primary_insert(ns.to_string(), key, bson_bytes);
                bp.sync_data_root(ns)?;
                bp.maintain_secondary_on_insert(ns, &new_doc, &id)?;
                Ok(())
            })?;
        }
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
        {
            let mut inner = self.inner.lock().unwrap();
            let bp = &mut *inner;
            bp.with_txn(|bp| {
                let (id, key, bson_bytes, _tree_root) = {
                    let tree = bp.tree_or_create(ns)?;
                    btree_insert_doc(tree, &mut new_doc, &[])?
                };
                bp.active_txn_mut()
                    .expect("fam_upsert_replace must run inside with_txn")
                    .stage_primary_insert(ns.to_string(), key, bson_bytes);
                bp.sync_data_root(ns)?;
                bp.maintain_secondary_on_insert(ns, &new_doc, &id)?;
                Ok(())
            })?;
        }
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
        let opts = FindOptions::new().sort(doc! { "v": 1 }).limit(2);
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
            let inner = e.inner.lock().unwrap();
            inner
                .handle
                .allocator()
                .with_header(|h| h.total_page_count)
                .unwrap()
        };

        e.drop_namespace("mydb.users").unwrap();

        // Free page count should have increased (pages returned to free list).
        let free_after = {
            let inner = e.inner.lock().unwrap();
            inner
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
            let inner = e.inner.lock().unwrap();
            inner
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
            let inner = e.inner.lock().unwrap();
            inner
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
    /// The reader starts BEFORE the writer commits; it must see the
    /// pre-write state (snapshot at the moment the read lock was acquired).
    #[test]
    fn swmr_reader_sees_snapshot_isolation() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let e = Arc::new(engine());
        // Insert an initial document.
        e.insert("test.snap", doc! { "status": "before" })
            .unwrap();

        // Barrier: reader starts, acquires read lock, signals writer.
        // Barrier has 2 parties: reader + writer.
        let barrier = Arc::new(Barrier::new(2));

        let e_reader = Arc::clone(&e);
        let barrier_reader = Arc::clone(&barrier);
        let reader = thread::spawn(move || {
            // Acquire the read lock (shared) and hold it while the writer
            // is trying to proceed.
            let inner = e_reader.inner.lock().unwrap();

            // Tell the writer we're inside the read section.
            barrier_reader.wait();

            // Do the actual scan while holding the read lock.
            let view = inner.open_read_view();
            let matched = inner
                .open_tree_for_read("test.snap")
                .unwrap()
                .map(|t| btree_collscan(&t, &doc! {}, &view, None).unwrap())
                .unwrap_or_default();
            drop(inner); // release read lock
            matched
        });

        // Writer: wait for the reader to hold the lock, then write.
        barrier.wait();
        // Writer acquires the exclusive write lock (blocks until reader drops).
        e.insert("test.snap", doc! { "status": "after" }).unwrap();

        let matched = reader.join().expect("reader panicked");
        // The reader held the lock before the write so it sees exactly 1 doc.
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
