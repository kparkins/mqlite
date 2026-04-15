//! `PagedEngine` — Phase 1 `StorageEngine` backed by B+ trees.
//!
//! ## Design
//!
//! Documents are stored in per-namespace B+ trees keyed by [`encode_key`]-encoded
//! `_id` values.  Two operating modes:
//!
//! | Mode | Backing store | Persistence |
//! |------|--------------|-------------|
//! | **Memory** | [`MemPageStore`] (independent per tree) | None (RAM only) |
//! | **Buffered** | [`BufferPoolPageStore`] (shared [`BufferPoolHandle`]) | Via buffer pool flush |
//!
//! ## Catalog (Buffered mode)
//!
//! A [`Catalog`] B+ tree stores [`CollectionEntry`] and [`IndexEntry`] records.
//! Its root page number is persisted to [`FileHeader::catalog_root_page`] after every
//! catalog mutation, so the catalog can be located on reopen.
//!
//! ## COLLSCAN
//!
//! `find` / `update` / `delete` traverse all leaf pages of the data tree, deserialise
//! each document, and apply [`eval_filter`].  Index-accelerated seeks are Phase 1.4.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::index::{IndexInfo, IndexModel};
use crate::key_encoding::encode_key;
use crate::options::{
    FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
    ReturnDocument, UpdateOptions,
};
use crate::query::{eval_filter, get_nested_field};
use crate::results::{DeleteResult, UpdateResult};
use crate::storage::btree::{BTree, BTreePageStore, CellValue, MemPageStore};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::Catalog;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::oid::ObjectIdGenerator;
use crate::storage::secondary_index::generate_index_name;
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
/// Returns the `_id` BSON value.
fn btree_insert_doc<S: BTreePageStore>(
    tree: &mut BTree<S>,
    doc: &mut Document,
    unique_specs: &[(String, Vec<String>, bool)],
) -> Result<Bson> {
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
    Ok(id_bson)
}

/// Scan every leaf in `tree` and return `(encoded_id_key, Document)` pairs
/// that satisfy `filter`.
fn btree_collscan<S: BTreePageStore>(
    tree: &BTree<S>,
    filter: &Document,
) -> Result<Vec<(Vec<u8>, Document)>> {
    let pairs = tree.range_scan(None, None)?;
    let mut result = Vec::new();
    for (key, cv) in pairs {
        let bson_bytes = resolve_cell(tree, cv)?;
        let doc: Document = bson::from_slice(&bson_bytes).map_err(Error::BsonDeserialization)?;
        if eval_filter(&doc, filter)? {
            result.push((key, doc));
        }
    }
    Ok(result)
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
// MemBackend — in-memory collections (no persistence)
// ---------------------------------------------------------------------------

/// Metadata held for an in-memory collection (no page-root tracking needed).
struct MemCollMeta {
    /// Index records: (name, model).
    indexes: Vec<(String, IndexModel)>,
}

impl MemCollMeta {
    /// Return `(name, fields, sparse)` tuples for all unique indexes.
    fn unique_specs(&self) -> Vec<(String, Vec<String>, bool)> {
        self.indexes
            .iter()
            .filter(|(_, m)| m.options.unique)
            .map(|(name, m)| {
                let fields = m.keys.keys().cloned().collect();
                (name.clone(), fields, m.options.sparse)
            })
            .collect()
    }
}

/// In-memory storage backend.
///
/// Each namespace gets an independent `BTree<MemPageStore>`.  There is no
/// catalog B+ tree; metadata is stored in a plain [`HashMap`].
struct MemBackend {
    /// Per-namespace data trees.
    data_trees: HashMap<String, BTree<MemPageStore>>,
    /// Per-namespace collection metadata (indexes).
    collections: HashMap<String, MemCollMeta>,
}

impl MemBackend {
    fn new() -> Self {
        Self {
            data_trees: HashMap::new(),
            collections: HashMap::new(),
        }
    }

    /// Return a mutable reference to the data tree for `ns`, creating it if absent.
    fn tree_or_create(&mut self, ns: &str) -> Result<&mut BTree<MemPageStore>> {
        if !self.data_trees.contains_key(ns) {
            let tree = BTree::create(MemPageStore::new())?;
            self.data_trees.insert(ns.to_owned(), tree);
            self.collections
                .entry(ns.to_owned())
                .or_insert_with(|| MemCollMeta {
                    indexes: Vec::new(),
                });
        }
        Ok(self.data_trees.get_mut(ns).unwrap())
    }

    /// Return a reference to the data tree for `ns`, or `None` if it doesn't exist.
    fn tree(&self, ns: &str) -> Option<&BTree<MemPageStore>> {
        self.data_trees.get(ns)
    }

    /// Return a mutable reference to the data tree for `ns`, or `None`.
    fn tree_mut(&mut self, ns: &str) -> Option<&mut BTree<MemPageStore>> {
        self.data_trees.get_mut(ns)
    }
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
        let catalog = if catalog_root_page == 0 {
            Catalog::create(store)?
        } else {
            Catalog::open(store, catalog_root_page, catalog_root_level)
        };
        let backend = Self {
            handle,
            catalog,
            data_trees: HashMap::new(),
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
        })
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
                let (data_root, _id_root) =
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
}

// ---------------------------------------------------------------------------
// DocBackend — unified enum
// ---------------------------------------------------------------------------

enum DocBackend {
    Memory(MemBackend),
    Buffered(BpBackend),
}

// ---------------------------------------------------------------------------
// PagedEngine — public struct
// ---------------------------------------------------------------------------

/// Phase 1 storage engine: B+ tree per namespace, through the buffer pool.
pub(crate) struct PagedEngine {
    inner: Mutex<DocBackend>,
}

impl PagedEngine {
    /// Create an in-memory engine with no persistence.
    ///
    /// Used by [`Client::open_in_memory`].
    pub(crate) fn new() -> Self {
        PagedEngine {
            inner: Mutex::new(DocBackend::Memory(MemBackend::new())),
        }
    }

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
            inner: Mutex::new(DocBackend::Buffered(backend)),
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
        match &mut *inner {
            DocBackend::Memory(m) => {
                // Collect unique specs before mutably borrowing the tree.
                let unique_specs = m
                    .collections
                    .get(ns)
                    .map(|meta| meta.unique_specs())
                    .unwrap_or_default();
                let tree = m.tree_or_create(ns)?;
                btree_insert_doc(tree, &mut doc, &unique_specs)
            }
            DocBackend::Buffered(bp) => {
                let unique_specs = bp.unique_specs(ns)?;
                let tree = bp.tree_or_create(ns)?;
                let id = btree_insert_doc(tree, &mut doc, &unique_specs)?;
                bp.sync_data_root(ns)?;
                Ok(id)
            }
        }
    }

    // -----------------------------------------------------------------------
    // find
    // -----------------------------------------------------------------------

    fn find(&self, ns: &str, filter: &Document, opts: &FindOptions) -> Result<Vec<Document>> {
        let mut inner = self.inner.lock().unwrap();
        let matched: Vec<Document> = match &mut *inner {
            DocBackend::Memory(m) => {
                let Some(tree) = m.tree(ns) else {
                    return Ok(Vec::new());
                };
                btree_collscan(tree, filter)?
                    .into_iter()
                    .map(|(_, doc)| doc)
                    .collect()
            }
            DocBackend::Buffered(bp) => match bp.tree(ns)? {
                None => return Ok(Vec::new()),
                Some(tree) => btree_collscan(tree, filter)?
                    .into_iter()
                    .map(|(_, doc)| doc)
                    .collect(),
            },
        };
        Ok(apply_find_opts(matched, opts))
    }

    // -----------------------------------------------------------------------
    // find_one
    // -----------------------------------------------------------------------

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
                "update requires an operator update document (e.g. {$set: {...}}); \
                 use find_one_and_replace for replacements"
                    .into(),
            ));
        }

        let mut inner = self.inner.lock().unwrap();
        let (matched_pairs, tree_exists): (Vec<(Vec<u8>, Document)>, bool) = match &mut *inner {
            DocBackend::Memory(m) => {
                let Some(tree) = m.tree(ns) else {
                    if opts.upsert {
                        drop(inner);
                        return self.do_upsert_update(ns, filter, update);
                    }
                    return Ok(UpdateResult {
                        matched_count: 0,
                        modified_count: 0,
                        upserted_id: None,
                    });
                };
                (btree_collscan(tree, filter)?, true)
            }
            DocBackend::Buffered(bp) => match bp.tree(ns)? {
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
                Some(tree) => (btree_collscan(tree, filter)?, true),
            },
        };

        let _ = tree_exists;

        if matched_pairs.is_empty() && opts.upsert {
            drop(inner);
            return self.do_upsert_update(ns, filter, update);
        }

        let mut matched_count = 0u64;
        let mut modified_count = 0u64;

        let pairs_to_process = if many {
            matched_pairs
        } else {
            matched_pairs.into_iter().take(1).collect()
        };

        for (key, mut doc) in pairs_to_process {
            matched_count += 1;
            let before = doc.clone();
            apply_update(&mut doc, update, false)?;
            if doc != before {
                modified_count += 1;
                // Re-serialize and replace in the tree.
                let new_bytes = bson::to_vec(&doc).map_err(Error::BsonSerialization)?;
                match &mut *inner {
                    DocBackend::Memory(m) => {
                        if let Some(tree) = m.tree_mut(ns) {
                            tree.delete(&key)?;
                            tree.insert(&key, &new_bytes)?;
                        }
                    }
                    DocBackend::Buffered(bp) => {
                        if let Some(tree) = bp.tree_mut(ns)? {
                            tree.delete(&key)?;
                            tree.insert(&key, &new_bytes)?;
                        }
                        bp.sync_data_root(ns)?;
                    }
                }
            }
        }

        Ok(UpdateResult {
            matched_count,
            modified_count,
            upserted_id: None,
        })
    }

    // -----------------------------------------------------------------------
    // delete
    // -----------------------------------------------------------------------

    fn delete(&self, ns: &str, filter: &Document, many: bool) -> Result<DeleteResult> {
        let mut inner = self.inner.lock().unwrap();

        // Collect keys to delete.
        let keys_to_delete: Vec<Vec<u8>> = match &mut *inner {
            DocBackend::Memory(m) => {
                let Some(tree) = m.tree(ns) else {
                    return Ok(DeleteResult { deleted_count: 0 });
                };
                let pairs = btree_collscan(tree, filter)?;
                if many {
                    pairs.into_iter().map(|(k, _)| k).collect()
                } else {
                    pairs.into_iter().take(1).map(|(k, _)| k).collect()
                }
            }
            DocBackend::Buffered(bp) => match bp.tree(ns)? {
                None => return Ok(DeleteResult { deleted_count: 0 }),
                Some(tree) => {
                    let pairs = btree_collscan(tree, filter)?;
                    if many {
                        pairs.into_iter().map(|(k, _)| k).collect()
                    } else {
                        pairs.into_iter().take(1).map(|(k, _)| k).collect()
                    }
                }
            },
        };

        let deleted_count = keys_to_delete.len() as u64;

        for key in &keys_to_delete {
            match &mut *inner {
                DocBackend::Memory(m) => {
                    if let Some(tree) = m.tree_mut(ns) {
                        tree.delete(key)?;
                    }
                }
                DocBackend::Buffered(bp) => {
                    if let Some(tree) = bp.tree_mut(ns)? {
                        tree.delete(key)?;
                    }
                    bp.sync_data_root(ns)?;
                }
            }
        }

        Ok(DeleteResult { deleted_count })
    }

    // -----------------------------------------------------------------------
    // count
    // -----------------------------------------------------------------------

    fn count(&self, ns: &str, filter: &Document) -> Result<u64> {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            DocBackend::Memory(m) => {
                let Some(tree) = m.tree(ns) else {
                    return Ok(0);
                };
                let pairs = btree_collscan(tree, filter)?;
                Ok(pairs.len() as u64)
            }
            DocBackend::Buffered(bp) => match bp.tree(ns)? {
                None => Ok(0),
                Some(tree) => {
                    let pairs = btree_collscan(tree, filter)?;
                    Ok(pairs.len() as u64)
                }
            },
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
        let mut matched: Vec<(Vec<u8>, Document)> = match &mut *inner {
            DocBackend::Memory(m) => {
                let Some(tree) = m.tree(ns) else {
                    if opts.upsert {
                        drop(inner);
                        return self.fam_upsert_update(ns, filter, update, opts);
                    }
                    return Ok(None);
                };
                btree_collscan(tree, filter)?
            }
            DocBackend::Buffered(bp) => match bp.tree(ns)? {
                None => {
                    if opts.upsert {
                        drop(inner);
                        return self.fam_upsert_update(ns, filter, update, opts);
                    }
                    return Ok(None);
                }
                Some(tree) => btree_collscan(tree, filter)?,
            },
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
        apply_update(&mut doc, update, false)?;

        let new_bytes = bson::to_vec(&doc).map_err(Error::BsonSerialization)?;
        match &mut *inner {
            DocBackend::Memory(m) => {
                if let Some(tree) = m.tree_mut(ns) {
                    tree.delete(&key)?;
                    tree.insert(&key, &new_bytes)?;
                }
            }
            DocBackend::Buffered(bp) => {
                if let Some(tree) = bp.tree_mut(ns)? {
                    tree.delete(&key)?;
                    tree.insert(&key, &new_bytes)?;
                }
                bp.sync_data_root(ns)?;
            }
        }

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
        let mut matched: Vec<(Vec<u8>, Document)> = match &mut *inner {
            DocBackend::Memory(m) => {
                let Some(tree) = m.tree(ns) else {
                    return Ok(None);
                };
                btree_collscan(tree, filter)?
            }
            DocBackend::Buffered(bp) => match bp.tree(ns)? {
                None => return Ok(None),
                Some(tree) => btree_collscan(tree, filter)?,
            },
        };

        if matched.is_empty() {
            return Ok(None);
        }

        if let Some(s) = &opts.sort {
            matched.sort_by(|(_, a), (_, b)| compare_docs(a, b, s));
        }

        let (key, doc) = matched.remove(0);

        match &mut *inner {
            DocBackend::Memory(m) => {
                if let Some(tree) = m.tree_mut(ns) {
                    tree.delete(&key)?;
                }
            }
            DocBackend::Buffered(bp) => {
                if let Some(tree) = bp.tree_mut(ns)? {
                    tree.delete(&key)?;
                }
                bp.sync_data_root(ns)?;
            }
        }

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
        let mut matched: Vec<(Vec<u8>, Document)> = match &mut *inner {
            DocBackend::Memory(m) => {
                let Some(tree) = m.tree(ns) else {
                    if opts.upsert {
                        drop(inner);
                        return self.fam_upsert_replace(ns, replacement, opts);
                    }
                    return Ok(None);
                };
                btree_collscan(tree, filter)?
            }
            DocBackend::Buffered(bp) => match bp.tree(ns)? {
                None => {
                    if opts.upsert {
                        drop(inner);
                        return self.fam_upsert_replace(ns, replacement, opts);
                    }
                    return Ok(None);
                }
                Some(tree) => btree_collscan(tree, filter)?,
            },
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

        match &mut *inner {
            DocBackend::Memory(m) => {
                if let Some(tree) = m.tree_mut(ns) {
                    tree.delete(&old_key)?;
                    tree.insert(&new_key, &new_bytes)?;
                }
            }
            DocBackend::Buffered(bp) => {
                if let Some(tree) = bp.tree_mut(ns)? {
                    tree.delete(&old_key)?;
                    tree.insert(&new_key, &new_bytes)?;
                }
                bp.sync_data_root(ns)?;
            }
        }

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
        match &mut *inner {
            DocBackend::Memory(m) => {
                let meta = m
                    .collections
                    .entry(ns.to_owned())
                    .or_insert_with(|| MemCollMeta {
                        indexes: Vec::new(),
                    });
                // Idempotent: return early if already exists.
                if meta.indexes.iter().any(|(n, _)| n == &name) {
                    return Ok(name);
                }
                meta.indexes.push((name.clone(), model.clone()));
                Ok(name)
            }
            DocBackend::Buffered(bp) => {
                // Ensure the collection exists in the catalog first.
                if bp.catalog.get_collection(ns)?.is_none() {
                    let (data_root, id_root) = bp
                        .catalog
                        .create_collection(ns, bson::doc! {}, now_millis())?;
                    bp.sync_catalog_root()?;
                    // Initialise both allocated tree pages so they have valid headers.
                    let data_store = bp.new_store();
                    let data_tree = BTree::create_at(data_store, data_root)?;
                    bp.data_trees.insert(ns.to_owned(), data_tree);
                    let id_store = bp.new_store();
                    BTree::create_at(id_store, id_root)?;
                }
                // Idempotent: return existing index name.
                if bp.catalog.get_index(ns, &name)?.is_some() {
                    return Ok(name);
                }
                let idx_root = bp.catalog.create_index(ns, model, &name)?;
                bp.sync_catalog_root()?;
                // Initialise the index tree leaf page.
                let idx_store = bp.new_store();
                BTree::create_at(idx_store, idx_root)?;
                Ok(name)
            }
        }
    }

    // -----------------------------------------------------------------------
    // drop_index
    // -----------------------------------------------------------------------

    fn drop_index(&self, ns: &str, name: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            DocBackend::Memory(m) => {
                if let Some(meta) = m.collections.get_mut(ns) {
                    let before = meta.indexes.len();
                    meta.indexes.retain(|(n, _)| n != name);
                    if meta.indexes.len() == before {
                        return Err(Error::Internal(format!(
                            "index '{}' not found on '{}'",
                            name, ns
                        )));
                    }
                }
                Ok(())
            }
            DocBackend::Buffered(bp) => {
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
            }
        }
    }

    // -----------------------------------------------------------------------
    // list_indexes
    // -----------------------------------------------------------------------

    fn list_indexes(&self, ns: &str) -> Result<Vec<IndexInfo>> {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            DocBackend::Memory(m) => {
                let Some(meta) = m.collections.get(ns) else {
                    return Ok(Vec::new());
                };
                Ok(meta
                    .indexes
                    .iter()
                    .map(|(name, model)| IndexInfo {
                        name: name.clone(),
                        keys: model.keys.clone(),
                        unique: model.options.unique,
                        sparse: model.options.sparse,
                    })
                    .collect())
            }
            DocBackend::Buffered(bp) => {
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
        }
    }

    // -----------------------------------------------------------------------
    // create_namespace
    // -----------------------------------------------------------------------

    fn create_namespace(&self, ns: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            DocBackend::Memory(m) => {
                m.collections
                    .entry(ns.to_owned())
                    .or_insert_with(|| MemCollMeta {
                        indexes: Vec::new(),
                    });
                if !m.data_trees.contains_key(ns) {
                    let tree = BTree::create(MemPageStore::new())?;
                    m.data_trees.insert(ns.to_owned(), tree);
                }
                Ok(())
            }
            DocBackend::Buffered(bp) => {
                if bp.catalog.get_collection(ns)?.is_some() {
                    return Ok(());
                }
                let (data_root, id_root) =
                    bp.catalog
                        .create_collection(ns, bson::doc! {}, now_millis())?;
                bp.sync_catalog_root()?;
                // Initialise the data tree leaf page and cache it for fast first access.
                let store = bp.new_store();
                let tree = BTree::create_at(store, data_root)?;
                bp.data_trees.insert(ns.to_owned(), tree);
                // Initialise the _id index leaf page so it has a valid header.
                // We do not cache index trees, but the page must be written
                // before it can be parsed (e.g., during drop_namespace page freeing).
                let id_store = bp.new_store();
                BTree::create_at(id_store, id_root)?;
                Ok(())
            }
        }
    }

    // -----------------------------------------------------------------------
    // drop_namespace
    // -----------------------------------------------------------------------

    fn drop_namespace(&self, ns: &str) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        match &mut *inner {
            DocBackend::Memory(m) => {
                m.data_trees.remove(ns);
                m.collections.remove(ns);
                Ok(())
            }
            DocBackend::Buffered(bp) => {
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
            }
        }
    }

    // -----------------------------------------------------------------------
    // list_namespaces
    // -----------------------------------------------------------------------

    fn list_namespaces(&self) -> Result<Vec<String>> {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            DocBackend::Memory(m) => Ok(m.collections.keys().cloned().collect()),
            DocBackend::Buffered(bp) => {
                let entries = bp.catalog.list_collections()?;
                Ok(entries.into_iter().map(|e| e.name).collect())
            }
        }
    }

    // -----------------------------------------------------------------------
    // checkpoint
    // -----------------------------------------------------------------------

    fn checkpoint(&self) -> Result<()> {
        let inner = self.inner.lock().unwrap();
        match &*inner {
            DocBackend::Memory(_) => Ok(()), // nothing to persist
            DocBackend::Buffered(bp) => {
                // Ensure the catalog root is in the file header before flush.
                bp.sync_catalog_root()?;
                // Flush all dirty pages (data + catalog + header) to disk.
                bp.handle.flush()
            }
        }
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
            match &mut *inner {
                DocBackend::Memory(m) => {
                    let tree = m.tree_or_create(ns)?;
                    btree_insert_doc(tree, &mut new_doc, &[])?;
                    ensure_id(&mut new_doc)
                }
                DocBackend::Buffered(bp) => {
                    let tree = bp.tree_or_create(ns)?;
                    let id = btree_insert_doc(tree, &mut new_doc, &[])?;
                    bp.sync_data_root(ns)?;
                    id
                }
            }
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
        let inserted = new_doc.clone();
        {
            let mut inner = self.inner.lock().unwrap();
            match &mut *inner {
                DocBackend::Memory(m) => {
                    let tree = m.tree_or_create(ns)?;
                    btree_insert_doc(tree, &mut new_doc, &[])?;
                }
                DocBackend::Buffered(bp) => {
                    let tree = bp.tree_or_create(ns)?;
                    btree_insert_doc(tree, &mut new_doc, &[])?;
                    bp.sync_data_root(ns)?;
                }
            }
        }
        Ok(match opts.return_document {
            ReturnDocument::Before => None,
            ReturnDocument::After => Some(inserted),
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
        let inserted = new_doc.clone();
        {
            let mut inner = self.inner.lock().unwrap();
            match &mut *inner {
                DocBackend::Memory(m) => {
                    let tree = m.tree_or_create(ns)?;
                    btree_insert_doc(tree, &mut new_doc, &[])?;
                }
                DocBackend::Buffered(bp) => {
                    let tree = bp.tree_or_create(ns)?;
                    btree_insert_doc(tree, &mut new_doc, &[])?;
                    bp.sync_data_root(ns)?;
                }
            }
        }
        Ok(match opts.return_document {
            ReturnDocument::Before => None,
            ReturnDocument::After => Some(inserted),
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
        PagedEngine::new()
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
    // These tests exercise PagedEngine in DocBackend::Buffered mode, using
    // an in-memory mock I/O layer so they remain hermetic and fast.
    // -----------------------------------------------------------------------

    use crate::storage::buffer_pool::{default_sizes, BufferPool, PageIo, PageSize};
    use crate::storage::header::FileHeader;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    /// Minimal in-memory `PageIo` for buffered-mode engine tests.
    #[derive(Default)]
    struct MockIo {
        pages: StdMutex<HashMap<u32, Vec<u8>>>,
    }

    struct ArcIo(Arc<MockIo>);

    impl PageIo for ArcIo {
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
        let header = FileHeader::new_now();
        let handle = Arc::new(BufferPoolHandle::new(pool, header));
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
        let handle = Arc::new(BufferPoolHandle::new(pool, header));
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
            match &*inner {
                DocBackend::Buffered(bp) => bp
                    .handle
                    .allocator()
                    .with_header(|h| h.total_page_count)
                    .unwrap(),
                _ => panic!("expected buffered backend"),
            }
        };

        e.drop_namespace("mydb.users").unwrap();

        // Free page count should have increased (pages returned to free list).
        let free_after = {
            let inner = e.inner.lock().unwrap();
            match &*inner {
                DocBackend::Buffered(bp) => bp
                    .handle
                    .allocator()
                    .with_header(|h| h.free_page_count_32k + h.free_page_count_4k)
                    .unwrap(),
                _ => panic!("expected buffered backend"),
            }
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
            match &*inner {
                DocBackend::Buffered(bp) => bp
                    .handle
                    .allocator()
                    .with_header(|h| h.total_page_count)
                    .unwrap(),
                _ => panic!("expected buffered"),
            }
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
            match &*inner {
                DocBackend::Buffered(bp) => bp
                    .handle
                    .allocator()
                    .with_header(|h| h.total_page_count)
                    .unwrap(),
                _ => panic!("expected buffered"),
            }
        };

        // After drop + recreate, pages should be recycled — total page count
        // must not keep growing without bound.
        assert!(
            page_count_after_recreate <= page_count_after_create + 4,
            "pages should be recycled after drop; before={page_count_after_create} after={page_count_after_recreate}"
        );
    }
}
