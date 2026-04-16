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

/// Metadata stored in the catalog for a single index.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IndexEntry {
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
}

impl IndexEntry {
    /// Serialize to BSON bytes.
    pub(crate) fn to_bson_bytes(&self) -> Result<Vec<u8>> {
        let doc = doc! {
            "name": &self.name,
            "collection": &self.collection,
            "rootPage": self.root_page as i64,
            "rootLevel": self.root_level as i32,
            "keyPattern": self.key_pattern.clone(),
            "unique": self.unique,
            "sparse": self.sparse,
            "multikey": self.multikey,
            "entryCount": self.entry_count,
        };
        Ok(bson::to_vec(&doc)?)
    }

    /// Deserialize from BSON bytes.
    pub(crate) fn from_bson_bytes(bytes: &[u8]) -> Result<Self> {
        let doc: Document = bson::from_slice(bytes).map_err(Error::BsonDeserialization)?;
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
        Ok(IndexEntry {
            name,
            collection,
            root_page,
            root_level,
            key_pattern,
            unique,
            sparse,
            multikey,
            entry_count,
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
}

impl<S: BTreePageStore> Catalog<S> {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Create a new empty catalog in `store`, allocating its root leaf page.
    ///
    /// Returns the catalog with its fresh root page allocated.
    pub(crate) fn create(store: S) -> Result<Self> {
        let tree = BTree::create(store)?;
        Ok(Catalog { tree })
    }

    /// Open an existing catalog at `root_page`/`root_level`.
    ///
    /// No I/O is performed here; the first operation will read pages lazily.
    pub(crate) fn open(store: S, root_page: u32, root_level: u8) -> Self {
        let tree = BTree::open(store, root_page, root_level);
        Catalog { tree }
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
        options: Document,
        now_millis: i64,
    ) -> Result<u32> {
        // Reject if already present.
        let coll_key = collection_key(name);
        if self.tree.search(&coll_key)?.is_some() {
            return Err(Error::DuplicateKey {
                detail: format!("collection '{}' already exists", name),
            });
        }

        // Allocate a root page for the data B+ tree.
        let data_root_page = self.tree.store.alloc_leaf()?;

        // Insert collection entry.
        let coll_entry = CollectionEntry {
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
        model: &IndexModel,
        index_name: &str,
    ) -> Result<u32> {
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
                detail: format!(
                    "index '{}' already exists on collection '{}'",
                    index_name, collection
                ),
            });
        }

        // Allocate root page for the index.
        let root_page = self.tree.store.alloc_leaf()?;

        let entry = IndexEntry {
            name: index_name.to_owned(),
            collection: collection.to_owned(),
            root_page,
            root_level: 0,
            key_pattern: model.keys.clone(),
            unique: model.options.unique,
            sparse: model.options.sparse,
            multikey: false,
            entry_count: 0,
        };
        let bytes = entry.to_bson_bytes()?;
        self.tree.insert(&idx_key, &bytes)?;

        Ok(root_page)
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
pub(crate) fn open_with_fallback<S, F>(
    store: S,
    primary_root: u32,
    primary_level: u8,
    backup_root: u32,
    backup_level: u8,
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
        return Ok((Catalog::open(store, primary_root, primary_level), false));
    }

    // Primary failed — try backup.
    if backup_root != 0 && try_open_page(backup_root) {
        // Signal to the caller that the backup was used (log a warning).
        return Ok((Catalog::open(store, backup_root, backup_level), true));
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
mod tests {
    use super::*;
    use bson::doc;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn now() -> i64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    fn make_catalog() -> Catalog<MemPageStore> {
        new_mem_catalog().expect("create catalog")
    }

    fn index_model(keys: Document) -> IndexModel {
        IndexModel::builder()
            .keys(keys)
            .build()
            .expect("build index model")
    }

    // -----------------------------------------------------------------------
    // Key encoding
    // -----------------------------------------------------------------------

    #[test]
    fn collection_key_has_prefix_0x01() {
        let k = collection_key("users");
        assert_eq!(k[0], KEY_TYPE_COLLECTION);
        assert_eq!(&k[1..], b"users");
    }

    #[test]
    fn index_key_has_prefix_0x02_and_null_sep() {
        let k = index_key("users", "email_1");
        assert_eq!(k[0], KEY_TYPE_INDEX);
        let sep_pos = 1 + b"users".len();
        assert_eq!(k[sep_pos], INDEX_KEY_SEP);
        assert_eq!(&k[sep_pos + 1..], b"email_1");
    }

    #[test]
    fn index_keys_sort_after_collection_keys() {
        let ck = collection_key("zzzz");
        let ik = index_key("aaaa", "_id_");
        // 0x02 > 0x01 → index keys always sort after collection keys
        assert!(ik > ck, "index keys must sort after collection keys");
    }

    #[test]
    fn index_keys_for_same_collection_group_together() {
        let k1 = index_key("users", "_id_");
        let k2 = index_key("users", "email_1");
        let k_other = index_key("widgets", "_id_");
        assert!(k1 < k_other);
        assert!(k2 < k_other);
    }

    // -----------------------------------------------------------------------
    // CollectionEntry round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn collection_entry_roundtrip() {
        let entry = CollectionEntry {
            name: "orders".to_owned(),
            data_root_page: 42,
            data_root_level: 1,
            document_count: 1000,
            avg_doc_size: 256,
            created_at: now(),
            options: doc! {},
        };
        let bytes = entry.to_bson_bytes().unwrap();
        let decoded = CollectionEntry::from_bson_bytes(&bytes).unwrap();
        assert_eq!(decoded, entry);
    }

    // -----------------------------------------------------------------------
    // IndexEntry round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn index_entry_roundtrip() {
        let entry = IndexEntry {
            name: "email_1".to_owned(),
            collection: "users".to_owned(),
            root_page: 99,
            root_level: 0,
            key_pattern: doc! { "email": 1 },
            unique: true,
            sparse: false,
            multikey: false,
            entry_count: 5000,
        };
        let bytes = entry.to_bson_bytes().unwrap();
        let decoded = IndexEntry::from_bson_bytes(&bytes).unwrap();
        assert_eq!(decoded, entry);
    }

    // -----------------------------------------------------------------------
    // create_collection / get_collection
    // -----------------------------------------------------------------------

    #[test]
    fn create_and_get_collection() {
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();

        let entry = cat.get_collection("users").unwrap().expect("should exist");
        assert_eq!(entry.name, "users");
        assert_eq!(entry.document_count, 0);
    }

    #[test]
    fn create_collection_allocates_data_root_page() {
        let mut cat = make_catalog();
        let data_page = cat.create_collection("users", doc! {}, now()).unwrap();
        assert!(data_page > 0, "data root page must be > 0");
    }

    #[test]
    fn create_collection_does_not_create_id_index_entry() {
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();

        let idx = cat.get_index("users", "_id_").unwrap();
        assert!(idx.is_none(), "_id_ index must not exist");
    }

    #[test]
    fn create_collection_duplicate_returns_error() {
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();
        let result = cat.create_collection("users", doc! {}, now());
        assert!(matches!(result, Err(Error::DuplicateKey { .. })));
    }

    // -----------------------------------------------------------------------
    // list_collections
    // -----------------------------------------------------------------------

    #[test]
    fn list_collections_empty() {
        let cat = make_catalog();
        assert!(cat.list_collections().unwrap().is_empty());
    }

    #[test]
    fn list_collections_returns_all() {
        let mut cat = make_catalog();
        cat.create_collection("alpha", doc! {}, now()).unwrap();
        cat.create_collection("beta", doc! {}, now()).unwrap();
        cat.create_collection("gamma", doc! {}, now()).unwrap();

        let names: Vec<String> = cat
            .list_collections()
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(names, ["alpha", "beta", "gamma"]);
    }

    // -----------------------------------------------------------------------
    // drop_collection
    // -----------------------------------------------------------------------

    #[test]
    fn drop_collection_removes_collection_and_indexes() {
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();
        cat.create_index("users", &index_model(doc! { "email": 1 }), "email_1")
            .unwrap();

        let removed = cat.drop_collection("users").unwrap();
        assert!(removed);

        assert!(cat.get_collection("users").unwrap().is_none());
        assert!(cat.list_indexes("users").unwrap().is_empty());
    }

    #[test]
    fn drop_collection_nonexistent_returns_false() {
        let mut cat = make_catalog();
        assert!(!cat.drop_collection("nonexistent").unwrap());
    }

    // -----------------------------------------------------------------------
    // create_index / list_indexes / drop_index
    // -----------------------------------------------------------------------

    #[test]
    fn create_index_requires_collection() {
        let mut cat = make_catalog();
        let result = cat.create_index("users", &index_model(doc! { "email": 1 }), "email_1");
        assert!(matches!(result, Err(Error::CollectionNotFound { .. })));
    }

    #[test]
    fn create_index_allocates_root_page() {
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();
        let page = cat
            .create_index("users", &index_model(doc! { "email": 1 }), "email_1")
            .unwrap();
        assert!(page > 0);
    }

    #[test]
    fn create_index_duplicate_returns_error() {
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();
        cat.create_index("users", &index_model(doc! { "email": 1 }), "email_1")
            .unwrap();
        let result = cat.create_index("users", &index_model(doc! { "email": 1 }), "email_1");
        assert!(matches!(result, Err(Error::DuplicateKey { .. })));
    }

    #[test]
    fn list_indexes_returns_only_user_indexes() {
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();
        cat.create_index("users", &index_model(doc! { "email": 1 }), "email_1")
            .unwrap();
        cat.create_index("users", &index_model(doc! { "age": -1 }), "age_-1")
            .unwrap();

        let names: Vec<String> = cat
            .list_indexes("users")
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.contains(&"email_1".to_owned()));
        assert!(names.contains(&"age_-1".to_owned()));
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn list_indexes_empty_for_unknown_collection() {
        let cat = make_catalog();
        assert!(cat.list_indexes("ghost").unwrap().is_empty());
    }

    #[test]
    fn indexes_from_different_collections_dont_leak() {
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();
        cat.create_collection("orders", doc! {}, now()).unwrap();
        cat.create_index("users", &index_model(doc! { "email": 1 }), "email_1")
            .unwrap();
        cat.create_index("orders", &index_model(doc! { "total": 1 }), "total_1")
            .unwrap();

        let user_idxs = cat.list_indexes("users").unwrap();
        assert!(user_idxs.iter().all(|e| e.collection == "users"));

        let order_idxs = cat.list_indexes("orders").unwrap();
        assert!(order_idxs.iter().all(|e| e.collection == "orders"));
    }

    #[test]
    fn drop_index_removes_only_target_index() {
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();
        cat.create_index("users", &index_model(doc! { "email": 1 }), "email_1")
            .unwrap();
        cat.create_index("users", &index_model(doc! { "age": 1 }), "age_1")
            .unwrap();

        let removed = cat.drop_index("users", "email_1").unwrap();
        assert!(removed);

        assert!(cat.get_index("users", "email_1").unwrap().is_none());
        assert!(cat.get_index("users", "age_1").unwrap().is_some());
    }

    #[test]
    fn drop_index_nonexistent_returns_false() {
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();
        assert!(!cat.drop_index("users", "ghost").unwrap());
    }

    // -----------------------------------------------------------------------
    // update_collection / update_index
    // -----------------------------------------------------------------------

    #[test]
    fn update_collection_changes_document_count() {
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();
        let mut entry = cat.get_collection("users").unwrap().unwrap();
        entry.document_count = 42;
        let updated = cat.update_collection(&entry).unwrap();
        assert!(updated);

        let fetched = cat.get_collection("users").unwrap().unwrap();
        assert_eq!(fetched.document_count, 42);
    }

    #[test]
    fn update_index_changes_entry_count() {
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();
        cat.create_index("users", &index_model(doc! { "email": 1 }), "email_1")
            .unwrap();
        let mut entry = cat.get_index("users", "email_1").unwrap().unwrap();
        entry.entry_count = 777;
        let updated = cat.update_index(&entry).unwrap();
        assert!(updated);

        let fetched = cat.get_index("users", "email_1").unwrap().unwrap();
        assert_eq!(fetched.entry_count, 777);
    }

    // -----------------------------------------------------------------------
    // Root page tracking
    // -----------------------------------------------------------------------

    #[test]
    fn root_page_is_nonzero_after_create() {
        let cat = make_catalog();
        assert!(cat.root_page() > 0);
    }

    #[test]
    fn root_page_may_change_after_inserts() {
        let mut cat = make_catalog();
        let initial_root = cat.root_page();
        // Insert enough collections to potentially trigger a root split.
        // A single leaf page holds dozens of entries; 30 should be enough.
        for i in 0..30 {
            cat.create_collection(&format!("coll_{i:03}"), doc! {}, now())
                .unwrap();
        }
        // We don't assert a specific root page; just that the method is accessible.
        let _ = cat.root_page();
        let _ = initial_root;
    }

    // -----------------------------------------------------------------------
    // open_with_fallback
    // -----------------------------------------------------------------------

    #[test]
    fn open_with_fallback_new_db_creates_empty_catalog() {
        let store = MemPageStore::new();
        let (cat, used_backup) = open_with_fallback(store, 0, 0, 0, 0, |_| true).unwrap();
        assert!(!used_backup);
        assert!(cat.list_collections().unwrap().is_empty());
    }

    #[test]
    fn open_with_fallback_uses_backup_when_primary_fails() {
        // Build a real catalog first to get a valid root page.
        let mut cat = make_catalog();
        cat.create_collection("users", doc! {}, now()).unwrap();
        let backup_root = cat.root_page();
        let backup_level = cat.root_level();

        // Simulate: primary root is corrupt (page checker returns false),
        // backup root is healthy.
        let store = MemPageStore::new();
        let corrupt_primary = 999u32;
        let (opened, used_backup) = open_with_fallback(
            store,
            corrupt_primary,
            0,
            backup_root,
            backup_level,
            |page| page != corrupt_primary,
        )
        .unwrap();
        assert!(used_backup, "should have fallen back to backup");
        assert_eq!(opened.root_page(), backup_root);
    }

    #[test]
    fn open_with_fallback_both_corrupt_returns_error() {
        let store = MemPageStore::new();
        let result = open_with_fallback(store, 1, 0, 2, 0, |_| false);
        assert!(matches!(result, Err(Error::CorruptDatabase { .. })));
    }

    // -----------------------------------------------------------------------
    // Catalog hardening: multiple collections + drop stress
    // -----------------------------------------------------------------------

    #[test]
    fn create_drop_create_same_collection() {
        let mut cat = make_catalog();
        let ts = now();
        cat.create_collection("users", doc! {}, ts).unwrap();
        cat.drop_collection("users").unwrap();
        // Must succeed (not duplicate-key) after drop.
        cat.create_collection("users", doc! {}, ts).unwrap();
        assert!(cat.get_collection("users").unwrap().is_some());
    }

    #[test]
    fn dropping_one_collection_leaves_others_intact() {
        let mut cat = make_catalog();
        let ts = now();
        cat.create_collection("alpha", doc! {}, ts).unwrap();
        cat.create_collection("beta", doc! {}, ts).unwrap();
        cat.create_collection("gamma", doc! {}, ts).unwrap();

        cat.drop_collection("beta").unwrap();

        let names: Vec<String> = cat
            .list_collections()
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(names, ["alpha", "gamma"]);
    }
}
