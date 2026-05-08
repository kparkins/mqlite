//! Catalog — collection and index metadata B+ tree.
//!
//! The catalog is a reserved B+ tree whose root page number is stored in
//! the file header at two locations:
//!
//! - **Primary**: header offset 32 (`catalog_root_page`)
//! - **Backup**:  header offset 72 (`catalog_root_backup`)
//!
//! On update the primary is written first; the backup is written after a
//! successful checkpoint so it always trails the primary by at most one
//! transaction.  On open, if the page at the primary root fails its page
//! checksum, the backup root is tried instead and a warning is logged.
//!
//! ## Key Format
//!
//! | Type | Byte prefix | Key |
//! |------|-------------|-----|
//! | Collection | `0x01` | `0x01 ‖ collection_name` |
//! | Index      | `0x02` | `0x02 ‖ collection_name ‖ 0x00 ‖ index_name` |
//!
//! The 0x00 separator between the collection name and the index name inside a
//! type-0x02 key makes all index entries for a given collection sort together
//! (since 0x00 < any printable character).  Collection names must not contain
//! null bytes (MongoDB restriction).
//!
//! ## Value Format
//!
//! Values are serialized BSON documents.  See [`CollectionEntry`] and
//! [`IndexEntry`] for the field set.
//!
//! ## WAL integration
//!
//! The catalog does **not** manage WAL writes directly.  Callers (the database
//! handle) are responsible for routing page writes through the WAL before
//! updating the in-memory catalog root.  The [`Catalog`] struct works against
//! the [`BTreePageStore`] abstraction so it can be driven by any backing store
//! (in-memory for tests, WAL-backed for production).

// Catalog operations use expect() only on fixed-size slice conversions that are
// statically guaranteed correct.  Allow the clippy lint for this module.
#![allow(clippy::expect_used)]

use bson::{doc, DateTime, Document};

use crate::error::{Error, Result};
use crate::index::IndexModel;
use crate::storage::btree::{BTree, BTreePageStore, MemPageStore};
use crate::storage::buffer_pool::PageSize;
use crate::storage::root_snapshot::{IndexId, NamespaceId};

// ---------------------------------------------------------------------------
// Key prefix constants
// ---------------------------------------------------------------------------

/// Key prefix byte for collection entries.
pub(crate) const KEY_TYPE_COLLECTION: u8 = 0x01;

/// Key prefix byte for index entries.
pub(crate) const KEY_TYPE_INDEX: u8 = 0x02;

/// Separator byte between collection name and index name in index keys.
const INDEX_KEY_SEP: u8 = 0x00;

// ---------------------------------------------------------------------------
// Entry types
// ---------------------------------------------------------------------------

/// Metadata stored in the catalog for a single collection.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CollectionEntry {
    /// Durable monotonic identifier. Allocated from the header counter
    /// `next_namespace_id` at create time; stable across splits and
    /// renames (Phase 1 §10.7).
    pub id: i64,
    /// Collection name (e.g. `"users"`).
    pub name: String,
    /// Root page of the `_id` index B+ tree (the primary data tree).
    pub data_root_page: u32,
    /// Root level of the `_id` index B+ tree.
    pub data_root_level: u8,
    /// Number of documents in the collection.
    pub document_count: i64,
    /// Average document size in bytes (updated at checkpoint time).
    pub avg_doc_size: i64,
    /// Unix milliseconds when the collection was created.
    pub created_at: i64,
    /// Serialized collection options (empty document if none).
    pub options: Document,
}

impl CollectionEntry {
    /// Serialize to BSON bytes.
    pub(crate) fn to_bson_bytes(&self) -> Result<Vec<u8>> {
        let doc = doc! {
            "id": self.id,
            "name": &self.name,
            "dataRootPage": self.data_root_page as i64,
            "dataRootLevel": self.data_root_level as i32,
            "documentCount": self.document_count,
            "avgDocSize": self.avg_doc_size,
            "createdAt": DateTime::from_millis(self.created_at),
            "options": self.options.clone(),
        };
        Ok(bson::to_vec(&doc)?)
    }

    /// Deserialize from BSON bytes.
    pub(crate) fn from_bson_bytes(bytes: &[u8]) -> Result<Self> {
        let doc: Document = bson::from_slice(bytes).map_err(Error::BsonDeserialization)?;
        let id = doc
            .get_i64("id")
            .map_err(|e| Error::Internal(format!("catalog: missing 'id': {e}")))?;
        let name = doc
            .get_str("name")
            .map_err(|e| Error::Internal(format!("catalog: missing 'name': {e}")))?
            .to_owned();
        let data_root_page = doc
            .get_i64("dataRootPage")
            .map_err(|e| Error::Internal(format!("catalog: missing 'dataRootPage': {e}")))?
            as u32;
        let data_root_level = doc
            .get_i32("dataRootLevel")
            .map_err(|e| Error::Internal(format!("catalog: missing 'dataRootLevel': {e}")))?
            as u8;
        let document_count = doc
            .get_i64("documentCount")
            .map_err(|e| Error::Internal(format!("catalog: missing 'documentCount': {e}")))?;
        let avg_doc_size = doc
            .get_i64("avgDocSize")
            .map_err(|e| Error::Internal(format!("catalog: missing 'avgDocSize': {e}")))?;
        let created_at = doc
            .get_datetime("createdAt")
            .map_err(|e| Error::Internal(format!("catalog: missing 'createdAt': {e}")))?
            .timestamp_millis();
        let options = doc
            .get_document("options")
            .map_err(|e| Error::Internal(format!("catalog: missing 'options': {e}")))?
            .clone();
        Ok(CollectionEntry {
            id,
            name,
            data_root_page,
            data_root_level,
            document_count,
            avg_doc_size,
            created_at,
            options,
        })
    }
}

/// Lifecycle state of an index.
///
/// An index in the `Building` state has a catalog entry and an allocated
/// root page, but its contents may not yet reflect all documents in the
/// parent collection. The read path (query planning) MUST skip
/// `Building` indexes. The write path MUST dual-write to them so the
/// build sees every concurrent mutation on the target namespace.
///
/// `Ready` is the default for backwards compatibility: catalog records
/// written before this field existed do not carry it and deserialize as
/// `Ready` (fully populated and usable).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum IndexState {
    /// Index has a catalog entry but its contents may be incomplete.
    /// Not usable for query planning; writers must still dual-write.
    Building,
    /// Index is fully populated and ready to serve queries.
    #[default]
    Ready,
}

/// Metadata stored in the catalog for a single index.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexEntry {
    /// Durable monotonic identifier. Allocated from the header counter
    /// `next_index_id` at create time; stable across splits (Phase 1 §10.7).
    pub id: i64,
    /// Index name (e.g. `"email_1"`).
    pub name: String,
    /// Name of the collection this index belongs to.
    pub collection: String,
    /// Root page of this index's B+ tree.
    pub root_page: u32,
    /// Root level of this index's B+ tree.
    pub root_level: u8,
    /// Key pattern document (e.g. `{ "email": 1 }`).
    pub key_pattern: Document,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
    /// Whether the index is sparse (skips documents lacking the indexed field).
    pub sparse: bool,
    /// Whether any indexed value is or was an array (set on first array insert).
    pub multikey: bool,
    /// Number of entries in the index.
    pub entry_count: i64,
    /// Lifecycle state. Defaults to `Ready` for backwards compatibility
    /// with pre-PR-9 catalog records.
    pub state: IndexState,
}

impl IndexEntry {
    /// Serialize to BSON bytes.
    pub(crate) fn to_bson_bytes(&self) -> Result<Vec<u8>> {
        let state_str = match self.state {
            IndexState::Building => "building",
            IndexState::Ready => "ready",
        };
        let doc = doc! {
            "id": self.id,
            "name": &self.name,
            "collection": &self.collection,
            "rootPage": self.root_page as i64,
            "rootLevel": self.root_level as i32,
            "keyPattern": self.key_pattern.clone(),
            "unique": self.unique,
            "sparse": self.sparse,
            "multikey": self.multikey,
            "entryCount": self.entry_count,
            "state": state_str,
        };
        Ok(bson::to_vec(&doc)?)
    }

    /// Deserialize from BSON bytes.
    pub(crate) fn from_bson_bytes(bytes: &[u8]) -> Result<Self> {
        let doc: Document = bson::from_slice(bytes).map_err(Error::BsonDeserialization)?;
        let id = doc
            .get_i64("id")
            .map_err(|e| Error::Internal(format!("catalog: missing 'id': {e}")))?;
        let name = doc
            .get_str("name")
            .map_err(|e| Error::Internal(format!("catalog: missing 'name': {e}")))?
            .to_owned();
        let collection = doc
            .get_str("collection")
            .map_err(|e| Error::Internal(format!("catalog: missing 'collection': {e}")))?
            .to_owned();
        let root_page = doc
            .get_i64("rootPage")
            .map_err(|e| Error::Internal(format!("catalog: missing 'rootPage': {e}")))?
            as u32;
        let root_level = doc
            .get_i32("rootLevel")
            .map_err(|e| Error::Internal(format!("catalog: missing 'rootLevel': {e}")))?
            as u8;
        let key_pattern = doc
            .get_document("keyPattern")
            .map_err(|e| Error::Internal(format!("catalog: missing 'keyPattern': {e}")))?
            .clone();
        let unique = doc
            .get_bool("unique")
            .map_err(|e| Error::Internal(format!("catalog: missing 'unique': {e}")))?;
        let sparse = doc
            .get_bool("sparse")
            .map_err(|e| Error::Internal(format!("catalog: missing 'sparse': {e}")))?;
        let multikey = doc
            .get_bool("multikey")
            .map_err(|e| Error::Internal(format!("catalog: missing 'multikey': {e}")))?;
        let entry_count = doc
            .get_i64("entryCount")
            .map_err(|e| Error::Internal(format!("catalog: missing 'entryCount': {e}")))?;
        // `state` is optional for backwards compatibility: older records
        // have no field, which we treat as `Ready` (their contents are
        // fully populated).
        let state = match doc.get_str("state") {
            Ok("building") => IndexState::Building,
            Ok("ready") => IndexState::Ready,
            Ok(other) => {
                return Err(Error::Internal(format!(
                    "catalog: unknown index state '{other}'"
                )))
            }
            Err(_) => IndexState::Ready,
        };
        Ok(IndexEntry {
            id,
            name,
            collection,
            root_page,
            root_level,
            key_pattern,
            unique,
            sparse,
            multikey,
            entry_count,
            state,
        })
    }
}

// ---------------------------------------------------------------------------
// Key helpers
// ---------------------------------------------------------------------------

/// Build the catalog key for a collection entry.
///
/// Format: `0x01 ‖ collection_name`
pub(crate) fn collection_key(name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + name.len());
    key.push(KEY_TYPE_COLLECTION);
    key.extend_from_slice(name.as_bytes());
    key
}

/// Build the catalog key for an index entry.
///
/// Format: `0x02 ‖ collection_name ‖ 0x00 ‖ index_name`
pub(crate) fn index_key(collection: &str, index_name: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + collection.len() + 1 + index_name.len());
    key.push(KEY_TYPE_INDEX);
    key.extend_from_slice(collection.as_bytes());
    key.push(INDEX_KEY_SEP);
    key.extend_from_slice(index_name.as_bytes());
    key
}

/// Build the inclusive start key for all index entries belonging to `collection`.
///
/// This is the key prefix `0x02 ‖ collection_name ‖ 0x00`; the range scan
/// ends when the prefix no longer matches.
fn index_prefix_start(collection: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + collection.len() + 1);
    key.push(KEY_TYPE_INDEX);
    key.extend_from_slice(collection.as_bytes());
    key.push(INDEX_KEY_SEP);
    key
}

/// Build the exclusive end key for all index entries belonging to `collection`.
///
/// We use `0x02 ‖ collection_name ‖ 0x01` (one past the separator) so that
/// all index keys for this collection fall strictly before the end key.
fn index_prefix_end(collection: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + collection.len() + 1);
    key.push(KEY_TYPE_INDEX);
    key.extend_from_slice(collection.as_bytes());
    key.push(INDEX_KEY_SEP + 1);
    key
}

// ---------------------------------------------------------------------------
// Catalog struct
// ---------------------------------------------------------------------------

/// The catalog B+ tree — stores all collection and index metadata.
///
/// The backing B+ tree is parameterized over a [`BTreePageStore`] so that
/// production code can use a WAL-backed store while tests use [`MemPageStore`].
///
/// ## Durability contract
///
/// [`Catalog`] does not write pages to the WAL directly.  The caller must:
///
/// 1. Wrap the page store in a WAL-routing layer before passing it to
///    [`Catalog::create`] or [`Catalog::open`].
/// 2. After every mutating operation, read [`Catalog::root_page`] and
///    [`Catalog::root_level`] to detect root changes (caused by B+ tree splits)
///    and update the file header accordingly.
///
/// ## Catalog hardening
///
/// On file open the caller should:
///
/// 1. Try to validate the page at `header.catalog_root_page`.
/// 2. If that page fails its checksum, fall back to `header.catalog_root_backup`
///    and log a warning.
/// 3. After every successful checkpoint, write the current `catalog_root_page`
///    into `header.catalog_root_backup` as a second write.
///
/// The [`open_with_fallback`] constructor encapsulates steps 1–2.
pub(crate) struct Catalog<S: BTreePageStore> {
    tree: BTree<S>,
    /// In-memory mirror of the file-header `next_namespace_id` counter
    /// (Phase 1 §10.7). Advanced by `allocate_namespace_id`; callers
    /// persist the post-alloc value to the header atomically with the
    /// catalog commit.
    next_namespace_id: i64,
    /// In-memory mirror of the file-header `next_index_id` counter
    /// (Phase 1 §10.7). Advanced by `allocate_index_id`; same
    /// persistence rules as `next_namespace_id`.
    next_index_id: i64,
}

impl<S: BTreePageStore> Catalog<S> {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Create a new empty catalog in `store`, allocating its root leaf page.
    ///
    /// Returns the catalog with its fresh root page allocated and durable
    /// id counters initialized to `1` (§10.7: id `0` is reserved).
    pub(crate) fn create(store: S) -> Result<Self> {
        let tree = BTree::create(store)?;
        Ok(Catalog {
            tree,
            next_namespace_id: 1,
            next_index_id: 1,
        })
    }

    /// Open an existing catalog at `root_page`/`root_level`.
    ///
    /// `next_namespace_id` / `next_index_id` are the persisted values
    /// from the file header (§10.7). Callers MUST pass the values from
    /// the header so the in-memory counter never returns an already
    /// allocated id on reopen.
    ///
    /// No I/O is performed here; the first operation will read pages lazily.
    pub(crate) fn open(
        store: S,
        root_page: u32,
        root_level: u8,
        next_namespace_id: i64,
        next_index_id: i64,
    ) -> Self {
        let tree = BTree::open(store, root_page, root_level);
        Catalog {
            tree,
            next_namespace_id,
            next_index_id,
        }
    }

    /// Page number of the current catalog root.
    ///
    /// This may change after any mutating operation (B+ tree root split).
    /// Callers must persist the updated value into the file header.
    pub(crate) fn root_page(&self) -> u32 {
        self.tree.root_page
    }

    /// Level of the current catalog root (0 = leaf, >0 = internal at that level).
    pub(crate) fn root_level(&self) -> u8 {
        self.tree.root_level
    }

    /// Return every page currently occupied by the catalog B+ tree.
    pub(crate) fn collect_pages_by_size(&mut self) -> Result<Vec<(u32, PageSize)>> {
        self.tree.collect_pages_by_size()
    }

    // -----------------------------------------------------------------------
    // Durable id allocation (Phase 1 §10.7)
    // -----------------------------------------------------------------------

    /// Current value of the in-memory `next_namespace_id` counter.
    ///
    /// Callers persist this to `FileHeader::next_namespace_id`
    /// atomically with the catalog commit.
    pub(crate) fn next_namespace_id(&self) -> i64 {
        self.next_namespace_id
    }

    /// Current value of the in-memory `next_index_id` counter.
    pub(crate) fn next_index_id(&self) -> i64 {
        self.next_index_id
    }

    /// Allocate a fresh durable `NamespaceId` (§10.7). Bumps the counter
    /// by 1 and returns the pre-bump value. Always returns a value `>= 1`
    /// because id `0` is reserved and the counter starts at `1` on fresh
    /// DB + is loaded from the header on reopen.
    pub(crate) fn allocate_namespace_id(&mut self) -> NamespaceId {
        let id = self.next_namespace_id;
        debug_assert!(id >= 1, "allocate_namespace_id must never return 0");
        self.next_namespace_id = self
            .next_namespace_id
            .checked_add(1)
            .expect("next_namespace_id overflow");
        id
    }

    /// Allocate a fresh durable `IndexId` (§10.7). Same protocol as
    /// `allocate_namespace_id`.
    pub(crate) fn allocate_index_id(&mut self) -> IndexId {
        let id = self.next_index_id;
        debug_assert!(id >= 1, "allocate_index_id must never return 0");
        self.next_index_id = self
            .next_index_id
            .checked_add(1)
            .expect("next_index_id overflow");
        id
    }

    // -----------------------------------------------------------------------
    // Collection operations
    // -----------------------------------------------------------------------

    /// Insert a collection entry into the catalog.
    ///
    /// Allocates a root page for the collection data tree using `alloc_leaf`
    /// on the provided `store`.  The caller is responsible for persisting the
    /// updated catalog root (and any new page count) to the file header.
    ///
    /// The `_id_` index is no longer pre-allocated here; it is synthesised
    /// at the wire layer on demand (see `handle_list_indexes`).
    ///
    /// Returns `data_root_page` so the caller can initialise the data tree.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DuplicateKey`] if the collection already exists.
    pub(crate) fn create_collection(
        &mut self,
        name: &str,
        id: NamespaceId,
        options: Document,
        now_millis: i64,
    ) -> Result<u32> {
        debug_assert!(
            id >= 1,
            "CollectionEntry.id must be >= 1 (id 0 is reserved, §10.7)"
        );
        // Reject if already present.
        let coll_key = collection_key(name);
        if self.tree.search(&coll_key)?.is_some() {
            return Err(Error::DuplicateKey {
                detail: format!("collection '{name}' already exists"),
            });
        }

        // Allocate a root page for the data B+ tree.
        let data_root_page = self.tree.store.alloc_leaf()?;

        // Insert collection entry.
        let coll_entry = CollectionEntry {
            id,
            name: name.to_owned(),
            data_root_page,
            data_root_level: 0,
            document_count: 0,
            avg_doc_size: 0,
            created_at: now_millis,
            options,
        };
        let coll_bytes = coll_entry.to_bson_bytes()?;
        self.tree.insert(&coll_key, &coll_bytes)?;

        Ok(data_root_page)
    }

    /// Remove a collection entry and **all** of its index entries.
    ///
    /// Does not free the underlying data pages (that is the caller's
    /// responsibility after obtaining the page roots from
    /// [`get_collection`](Self::get_collection) /
    /// [`list_indexes`](Self::list_indexes)).
    ///
    /// Returns `true` if the collection existed and was removed, `false` if it
    /// did not exist.
    pub(crate) fn drop_collection(&mut self, name: &str) -> Result<bool> {
        // Remove all index entries first (range delete).
        let start = index_prefix_start(name);
        let end_excl = index_prefix_end(name);
        let index_entries = self.tree.range_scan(Some(&start), Some(&end_excl))?;
        let index_keys: Vec<Vec<u8>> = index_entries
            .into_iter()
            .filter(|(k, _)| k.starts_with(&start) && k < &end_excl)
            .map(|(k, _)| k)
            .collect();
        for k in index_keys {
            self.tree.delete(&k)?;
        }

        // Remove the collection entry itself.
        let coll_key = collection_key(name);
        let removed = self.tree.delete(&coll_key)?;

        Ok(removed)
    }

    /// Fetch a single collection's metadata.
    ///
    /// Returns `None` if the collection does not exist.
    pub(crate) fn get_collection(&self, name: &str) -> Result<Option<CollectionEntry>> {
        let key = collection_key(name);
        match self.tree.get(&key)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(CollectionEntry::from_bson_bytes(&bytes)?)),
        }
    }

    /// Update (overwrite) an existing collection's metadata.
    ///
    /// This is a delete-then-insert on the underlying B+ tree.  Returns
    /// `false` if the collection did not exist.
    pub(crate) fn update_collection(&mut self, entry: &CollectionEntry) -> Result<bool> {
        let key = collection_key(&entry.name);
        let existed = self.tree.delete(&key)?;
        if !existed {
            return Ok(false);
        }
        let bytes = entry.to_bson_bytes()?;
        self.tree.insert(&key, &bytes)?;
        Ok(true)
    }

    /// List all collections in the catalog.
    ///
    /// Returns entries sorted by collection name (natural B+ tree order).
    pub(crate) fn list_collections(&self) -> Result<Vec<CollectionEntry>> {
        // All collection keys have prefix 0x01, so start at [0x01] and end
        // before [0x02].
        let start = vec![KEY_TYPE_COLLECTION];
        let end = vec![KEY_TYPE_INDEX]; // exclusive upper bound
        let raw = self.tree.range_scan(Some(&start), Some(&end))?;
        let mut result = Vec::with_capacity(raw.len());
        for (key, value) in raw {
            if key.first() != Some(&KEY_TYPE_COLLECTION) {
                break;
            }
            let bytes = match value {
                crate::storage::btree::CellValue::Inline(b) => b,
                crate::storage::btree::CellValue::Overflow {
                    first_page,
                    total_length,
                } => self.tree.read_overflow(first_page, total_length)?,
            };
            result.push(CollectionEntry::from_bson_bytes(&bytes)?);
        }
        Ok(result)
    }

    // -----------------------------------------------------------------------
    // Index operations
    // -----------------------------------------------------------------------

    /// Insert an index entry into the catalog.
    ///
    /// Allocates a root leaf page for the new index's B+ tree.
    ///
    /// Returns the newly allocated `root_page` for the index tree.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CollectionNotFound`] if the parent collection does not
    /// exist in the catalog.  Returns [`Error::DuplicateKey`] if an index with
    /// the same name already exists.
    pub(crate) fn create_index(
        &mut self,
        collection: &str,
        id: IndexId,
        model: &IndexModel,
        index_name: &str,
    ) -> Result<u32> {
        debug_assert!(
            id >= 1,
            "IndexEntry.id must be >= 1 (id 0 is reserved, §10.7)"
        );
        // Verify the collection exists.
        if self.get_collection(collection)?.is_none() {
            return Err(Error::CollectionNotFound {
                name: collection.to_owned(),
            });
        }

        // Reject duplicate index name.
        let idx_key = index_key(collection, index_name);
        if self.tree.search(&idx_key)?.is_some() {
            return Err(Error::DuplicateKey {
                detail: format!("index '{index_name}' already exists on collection '{collection}'"),
            });
        }

        // Allocate root page for the index.
        let root_page = self.tree.store.alloc_leaf()?;

        let entry = IndexEntry {
            id,
            name: index_name.to_owned(),
            collection: collection.to_owned(),
            root_page,
            root_level: 0,
            key_pattern: model.keys.clone(),
            unique: model.options.unique,
            sparse: model.options.sparse,
            multikey: false,
            entry_count: 0,
            // Default is `Ready`. Callers that want a multi-phase build
            // (see `PagedEngine::create_index`) transition the entry to
            // `Building` before publishing.
            state: IndexState::Ready,
        };
        let bytes = entry.to_bson_bytes()?;
        self.tree.insert(&idx_key, &bytes)?;

        Ok(root_page)
    }

    // -----------------------------------------------------------------------
    // Durable id lookup helpers (Phase 1 §10.7, US-003)
    // -----------------------------------------------------------------------

    /// Find a collection by its durable id (Phase 1 §10.7). Linear scan
    /// over the in-memory catalog. Acceptable because catalogs are small
    /// (tens to low hundreds of entries) and this helper runs off the
    /// hot path (Phase 2 post-open validation, diagnostics, Phase 4
    /// reconcile-time resolution). Phase 1 intentionally does NOT add a
    /// sidecar `HashMap<NamespaceId, …>` inside `Catalog`.
    ///
    /// Returns `None` for any id that was never allocated (including the
    /// reserved id `0`). Callers handle `None` per Phase 2 §5
    /// (log-and-proceed) or Phase 4 (hard error).
    #[allow(dead_code)]
    pub(crate) fn find_collection_by_id(&self, id: NamespaceId) -> Result<Option<CollectionEntry>> {
        if id <= 0 {
            return Ok(None);
        }
        for entry in self.list_collections()? {
            if entry.id == id {
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }

    /// Find an index by its durable id (Phase 1 §10.7). Returns the
    /// owning `CollectionEntry` alongside the `IndexEntry`. Same linear-
    /// scan discipline as `find_collection_by_id`; same `None` handling
    /// rule for callers (Phase 2 §5 / Phase 4).
    #[allow(dead_code)]
    pub(crate) fn find_index_by_id(
        &self,
        id: IndexId,
    ) -> Result<Option<(CollectionEntry, IndexEntry)>> {
        if id <= 0 {
            return Ok(None);
        }
        for coll in self.list_collections()? {
            for idx in self.list_indexes(&coll.name)? {
                if idx.id == id {
                    return Ok(Some((coll, idx)));
                }
            }
        }
        Ok(None)
    }

    /// Remove an index entry from the catalog.
    ///
    /// Does not free the index's B+ tree pages (the caller must do that using
    /// the root page obtained from [`get_index`](Self::get_index) before
    /// calling this).
    ///
    /// Returns `true` if the index existed and was removed.
    pub(crate) fn drop_index(&mut self, collection: &str, index_name: &str) -> Result<bool> {
        let key = index_key(collection, index_name);
        self.tree.delete(&key)
    }

    /// Fetch a single index's metadata.
    pub(crate) fn get_index(
        &self,
        collection: &str,
        index_name: &str,
    ) -> Result<Option<IndexEntry>> {
        let key = index_key(collection, index_name);
        match self.tree.get(&key)? {
            None => Ok(None),
            Some(bytes) => Ok(Some(IndexEntry::from_bson_bytes(&bytes)?)),
        }
    }

    /// Update (overwrite) an existing index's metadata.
    ///
    /// Returns `false` if the index did not exist.
    pub(crate) fn update_index(&mut self, entry: &IndexEntry) -> Result<bool> {
        let key = index_key(&entry.collection, &entry.name);
        let existed = self.tree.delete(&key)?;
        if !existed {
            return Ok(false);
        }
        let bytes = entry.to_bson_bytes()?;
        self.tree.insert(&key, &bytes)?;
        Ok(true)
    }

    /// List all indexes for a collection.
    ///
    /// Returns entries in index-name order.
    pub(crate) fn list_indexes(&self, collection: &str) -> Result<Vec<IndexEntry>> {
        let start = index_prefix_start(collection);
        let end = index_prefix_end(collection);
        let raw = self.tree.range_scan(Some(&start), Some(&end))?;
        let mut result = Vec::with_capacity(raw.len());
        for (key, value) in raw {
            // Only include entries that genuinely belong to this collection.
            if !key.starts_with(&start) || key >= end {
                break;
            }
            let bytes = match value {
                crate::storage::btree::CellValue::Inline(b) => b,
                crate::storage::btree::CellValue::Overflow {
                    first_page,
                    total_length,
                } => self.tree.read_overflow(first_page, total_length)?,
            };
            result.push(IndexEntry::from_bson_bytes(&bytes)?);
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// In-memory constructor helper
// ---------------------------------------------------------------------------

/// Create a [`Catalog`] backed by an in-memory [`MemPageStore`].
///
/// Useful for tests and for building the catalog before wiring it to a
/// file-backed store.
#[allow(dead_code)]
pub(crate) fn new_mem_catalog() -> Result<Catalog<MemPageStore>> {
    Catalog::create(MemPageStore::new())
}

// ---------------------------------------------------------------------------
// Catalog hardening helpers
// ---------------------------------------------------------------------------

/// Try to open a catalog using the primary root page; fall back to the backup
/// root page if the primary is unavailable (returns `None` for a corrupt page).
///
/// `try_open_page` receives a page number and returns `true` if the page is
/// healthy (valid checksum), `false` if it is corrupt or missing.  In
/// production this would attempt to read the page through the buffer pool and
/// verify its CRC32C checksum.
///
/// If both roots are 0 (new database), a fresh catalog is created.
#[allow(clippy::too_many_arguments)]
pub(crate) fn open_with_fallback<S, F>(
    store: S,
    primary_root: u32,
    primary_level: u8,
    backup_root: u32,
    backup_level: u8,
    next_namespace_id: i64,
    next_index_id: i64,
    try_open_page: F,
) -> Result<(Catalog<S>, bool)>
where
    S: BTreePageStore,
    F: Fn(u32) -> bool,
{
    // New database — no catalog yet.
    if primary_root == 0 && backup_root == 0 {
        let cat = Catalog::create(store)?;
        return Ok((cat, false));
    }

    // Try primary.
    if primary_root != 0 && try_open_page(primary_root) {
        return Ok((
            Catalog::open(
                store,
                primary_root,
                primary_level,
                next_namespace_id,
                next_index_id,
            ),
            false,
        ));
    }

    // Primary failed — try backup.
    if backup_root != 0 && try_open_page(backup_root) {
        // Signal to the caller that the backup was used (log a warning).
        return Ok((
            Catalog::open(
                store,
                backup_root,
                backup_level,
                next_namespace_id,
                next_index_id,
            ),
            true,
        ));
    }

    // Both roots are corrupt.
    Err(Error::CorruptDatabase {
        path: std::path::PathBuf::new(),
        detail:
            "catalog root page failed checksum; backup root also failed — database is unrecoverable"
                .into(),
        recoverable: false,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/catalog.rs"]
mod tests;
