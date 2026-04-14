use bson::Document;
use serde::{de::DeserializeOwned, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::{
    collection::Collection,
    cursor::Cursor,
    error::{Error, Result},
    index::{IndexInfo, IndexModel},
    options::{FindOptions, InsertManyOptions, OpenOptions, UpdateOptions},
    results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
};

/// Internal shared state for the database.
///
/// Wrapped in `Arc` and shared across `Database` clones, `Collection` handles, etc.
/// This is the single-writer multi-reader (SWMR) synchronization point.
pub(crate) struct DatabaseInner {
    /// Path to the database file. `None` for in-memory databases.
    pub path: Option<PathBuf>,
    /// Configuration options.
    pub opts: OpenOptions,
    /// Writer mutex — only one write can proceed at a time.
    writer_lock: Mutex<()>,
}

impl DatabaseInner {
    fn new(path: Option<PathBuf>, opts: OpenOptions) -> Self {
        DatabaseInner {
            path,
            opts,
            writer_lock: Mutex::new(()),
        }
    }
}

// ---------------------------------------------------------------------------
// Stub implementations for DatabaseInner methods called by Collection.
//
// These are intentional stubs: Phase 0 initializes the crate structure and
// verifies that it compiles. Storage engine, query engine, WAL, and buffer
// pool implementations are Phase 1 work items (hq-9vo, hq-apk, hq-6d0, etc.).
// ---------------------------------------------------------------------------

impl DatabaseInner {
    pub(crate) fn insert_one<T: Serialize>(&self, _name: &str, _doc: &T) -> Result<InsertOneResult> {
        Err(Error::Internal("insert_one: storage engine not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn insert_many<T: Serialize>(
        &self,
        _name: &str,
        _docs: &[T],
        _opts: InsertManyOptions,
    ) -> Result<InsertManyResult> {
        Err(Error::Internal("insert_many: storage engine not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn find_one<T: DeserializeOwned>(
        &self,
        _name: &str,
        _filter: Document,
    ) -> Result<Option<T>> {
        Err(Error::Internal("find_one: query engine not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn find<T: DeserializeOwned>(
        &self,
        _name: &str,
        _filter: Document,
        _opts: FindOptions,
    ) -> Result<Cursor<T>> {
        Err(Error::Internal("find: query engine not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn update_one(
        &self,
        _name: &str,
        _filter: Document,
        _update: Document,
        _opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        Err(Error::Internal("update_one: storage engine not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn update_many(
        &self,
        _name: &str,
        _filter: Document,
        _update: Document,
        _opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        Err(Error::Internal("update_many: storage engine not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn delete_one(&self, _name: &str, _filter: Document) -> Result<DeleteResult> {
        Err(Error::Internal("delete_one: storage engine not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn delete_many(&self, _name: &str, _filter: Document) -> Result<DeleteResult> {
        Err(Error::Internal("delete_many: storage engine not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn find_one_and_update<T: Serialize + DeserializeOwned>(
        &self,
        _name: &str,
        _filter: Document,
        _update: Document,
    ) -> Result<Option<T>> {
        Err(Error::Internal("find_one_and_update: not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn find_one_and_delete<T: DeserializeOwned>(
        &self,
        _name: &str,
        _filter: Document,
    ) -> Result<Option<T>> {
        Err(Error::Internal("find_one_and_delete: not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn find_one_and_replace<T: Serialize + DeserializeOwned>(
        &self,
        _name: &str,
        _filter: Document,
        _replacement: &T,
    ) -> Result<Option<T>> {
        Err(Error::Internal("find_one_and_replace: not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn estimated_document_count(&self, _name: &str) -> Result<u64> {
        Err(Error::Internal("estimated_document_count: not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn count_documents(&self, _name: &str, _filter: Document) -> Result<u64> {
        Err(Error::Internal("count_documents: not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn create_index(&self, _name: &str, _model: IndexModel) -> Result<String> {
        Err(Error::Internal("create_index: not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn drop_index(&self, _name: &str, _index_name: &str) -> Result<()> {
        Err(Error::Internal("drop_index: not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn list_indexes(&self, _name: &str) -> Result<Vec<IndexInfo>> {
        Err(Error::Internal("list_indexes: not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn list_collection_names(&self) -> Result<Vec<String>> {
        Err(Error::Internal("list_collection_names: not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn drop_collection(&self, _name: &str) -> Result<()> {
        Err(Error::Internal("drop_collection: not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn create_collection(&self, _name: &str) -> Result<()> {
        Err(Error::Internal("create_collection: not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn checkpoint(&self) -> Result<()> {
        Err(Error::Internal("checkpoint: WAL not yet implemented (Phase 1)".into()))
    }

    pub(crate) fn backup(&self, _dest: &Path) -> Result<()> {
        Err(Error::Internal("backup: not yet implemented (Phase 1)".into()))
    }
}

// ---------------------------------------------------------------------------
// Database (public handle)
// ---------------------------------------------------------------------------

/// An open mqlite database.
///
/// `Database` is cheaply cloneable — all clones share the same underlying storage,
/// writer lock, and buffer pool.
///
/// # Opening
///
/// ```no_run
/// use mqlite::Database;
///
/// // Open (or create) a database file
/// let db = Database::open("myapp.mqlite")?;
///
/// // In-memory database (for tests, no persistence)
/// let db = Database::open_in_memory()?;
/// # Ok::<(), mqlite::Error>(())
/// ```
///
/// # Collections
///
/// ```no_run
/// use mqlite::{Database, doc};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct User { name: String }
///
/// # fn main() -> mqlite::Result<()> {
/// let db = Database::open_in_memory()?;
///
/// // Typed collection
/// let users = db.collection::<User>("users");
///
/// // Untyped collection
/// let raw = db.collection::<bson::Document>("events");
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Database {
    pub(crate) inner: Arc<DatabaseInner>,
}

impl Database {
    /// Open a database file. Creates the file if it does not exist.
    ///
    /// Automatically replays the WAL on recovery. Uses sensible defaults
    /// (64MB buffer pool, 100ms durability interval, 5s busy timeout).
    pub fn open(path: impl AsRef<Path>) -> Result<Database> {
        Database::open_with_options(path, OpenOptions::new())
    }

    /// Open a database file with explicit configuration.
    pub fn open_with_options(path: impl AsRef<Path>, opts: OpenOptions) -> Result<Database> {
        let path = path.as_ref().to_owned();
        // Phase 0: structural stub. Storage engine initialization is Phase 1.
        let inner = Arc::new(DatabaseInner::new(Some(path), opts));
        Ok(Database { inner })
    }

    /// Create an in-memory database with no persistence.
    ///
    /// In-memory databases are ideal for testing — no files are created and
    /// everything is released when the `Database` handle is dropped.
    pub fn open_in_memory() -> Result<Database> {
        let inner = Arc::new(DatabaseInner::new(None, OpenOptions::new()));
        Ok(Database { inner })
    }

    /// Get a typed collection handle.
    ///
    /// This call is infallible — the collection is not created until the first write.
    pub fn collection<T: Serialize + DeserializeOwned>(&self, name: &str) -> Collection<T> {
        Collection {
            name: name.to_owned(),
            inner: Arc::clone(&self.inner),
            _phantom: std::marker::PhantomData,
        }
    }

    /// List the names of all collections in this database.
    pub fn list_collection_names(&self) -> Result<Vec<String>> {
        self.inner.list_collection_names()
    }

    /// Drop a collection and all its indexes.
    pub fn drop_collection(&self, name: &str) -> Result<()> {
        self.inner.drop_collection(name)
    }

    /// Create a collection explicitly.
    ///
    /// This is optional — collections are created automatically on first write.
    pub fn create_collection(&self, name: &str) -> Result<()> {
        self.inner.create_collection(name)
    }

    /// Force a WAL checkpoint.
    ///
    /// After this returns, the main database file is safe to copy as a backup.
    /// See also [`Database::backup`] for hot backup support.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }

    /// Hot backup to a destination file.
    ///
    /// Copies the current database state (including any uncommitted WAL data) to
    /// the destination path. The destination file can be opened immediately.
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        self.inner.backup(dest.as_ref())
    }

    /// Flush the WAL, checkpoint, and close the database.
    ///
    /// Use this when you need a guarantee that all committed data is in the main
    /// file (e.g., before copying the file as a backup). `Drop` performs a
    /// non-blocking close.
    pub fn close(self) -> Result<()> {
        self.inner.checkpoint()
    }
}
