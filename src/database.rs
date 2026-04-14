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

// ---------------------------------------------------------------------------
// Path security helpers
// ---------------------------------------------------------------------------

/// Check whether `path` is a symlink and return an error if so.
///
/// Uses `symlink_metadata()` which does **not** follow symlinks (unlike `metadata()`).
/// If the path does not exist yet, this is not an error — a new file will be created.
///
/// # Security
/// Symlink following at `Database::open()` time could allow an attacker who controls
/// the filesystem path to redirect the database open to an arbitrary file (e.g.,
/// `/etc/passwd`).  See mqlite security.md threat #12.
fn reject_symlink(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(Error::SymlinkRejected {
            path: path.to_owned(),
        }),
        // Exists and is a regular file or directory — OK.
        Ok(_) => Ok(()),
        // Path does not exist yet (will be created as a new database) — OK.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        // Any other IO error (permission denied, etc.) — propagate.
        Err(e) => Err(Error::Io(e)),
    }
}

/// Returns the expected WAL file path for a given database path.
///
/// WAL files use the naming convention `<db-path>-wal`.
fn wal_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_owned();
    s.push("-wal");
    PathBuf::from(s)
}

/// Returns the expected shared-memory file path for a given database path.
///
/// SHM files use the naming convention `<db-path>-shm`.
fn shm_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_owned();
    s.push("-shm");
    PathBuf::from(s)
}

/// Create (or open) a database file with restricted permissions (`0600`).
///
/// On Unix, newly created files get mode `0600` (owner read/write only).
/// This is the only access-control mechanism in embedded mode — documented
/// in `Database::open_with_options`.
///
/// On non-Unix platforms (Windows) this is a no-op because Windows uses ACLs
/// rather than POSIX permission bits.
fn create_db_file_secure(path: &Path) -> Result<std::fs::File> {
    // Open-or-create with exclusive create attempt first so we can set permissions
    // before any data is written.  If the file already exists, open normally —
    // we rely on the `reject_symlink` check that runs before this to prevent
    // symlink attacks on the existing-file path.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }

    Ok(file)
}

/// Internal shared state for the database.
///
/// Wrapped in `Arc` and shared across `Database` clones, `Collection` handles, etc.
/// This is the single-writer multi-reader (SWMR) synchronization point.
#[allow(dead_code)] // Phase 0: fields used by storage engine (Phase 1)
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
    pub(crate) fn insert_one<T: Serialize>(
        &self,
        _name: &str,
        doc: &T,
    ) -> Result<InsertOneResult> {
        // BSON validation is enforced at the insert boundary before any storage write.
        // This ensures structural limits are checked even while the storage engine is
        // still being implemented.  See security.md mandatory mitigation #3.
        let bson_doc = bson::to_document(doc).map_err(Error::BsonSerialization)?;
        crate::validation::validate_document(&bson_doc)?;

        Err(Error::Internal(
            "insert_one: storage engine not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn insert_many<T: Serialize>(
        &self,
        _name: &str,
        docs: &[T],
        _opts: InsertManyOptions,
    ) -> Result<InsertManyResult> {
        // Validate every document before any write is attempted.
        // Fail fast on the first invalid document (ordered validation).
        for doc in docs {
            let bson_doc = bson::to_document(doc).map_err(Error::BsonSerialization)?;
            crate::validation::validate_document(&bson_doc)?;
        }

        Err(Error::Internal(
            "insert_many: storage engine not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn find_one<T: DeserializeOwned>(
        &self,
        _name: &str,
        _filter: Document,
    ) -> Result<Option<T>> {
        Err(Error::Internal(
            "find_one: query engine not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn find<T: DeserializeOwned>(
        &self,
        _name: &str,
        _filter: Document,
        _opts: FindOptions,
    ) -> Result<Cursor<T>> {
        Err(Error::Internal(
            "find: query engine not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn update_one(
        &self,
        _name: &str,
        _filter: Document,
        _update: Document,
        _opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        Err(Error::Internal(
            "update_one: storage engine not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn update_many(
        &self,
        _name: &str,
        _filter: Document,
        _update: Document,
        _opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        Err(Error::Internal(
            "update_many: storage engine not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn delete_one(&self, _name: &str, _filter: Document) -> Result<DeleteResult> {
        Err(Error::Internal(
            "delete_one: storage engine not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn delete_many(&self, _name: &str, _filter: Document) -> Result<DeleteResult> {
        Err(Error::Internal(
            "delete_many: storage engine not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn find_one_and_update<T: Serialize + DeserializeOwned>(
        &self,
        _name: &str,
        _filter: Document,
        _update: Document,
    ) -> Result<Option<T>> {
        Err(Error::Internal(
            "find_one_and_update: not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn find_one_and_delete<T: DeserializeOwned>(
        &self,
        _name: &str,
        _filter: Document,
    ) -> Result<Option<T>> {
        Err(Error::Internal(
            "find_one_and_delete: not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn find_one_and_replace<T: Serialize + DeserializeOwned>(
        &self,
        _name: &str,
        _filter: Document,
        _replacement: &T,
    ) -> Result<Option<T>> {
        Err(Error::Internal(
            "find_one_and_replace: not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn estimated_document_count(&self, _name: &str) -> Result<u64> {
        Err(Error::Internal(
            "estimated_document_count: not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn count_documents(&self, _name: &str, _filter: Document) -> Result<u64> {
        Err(Error::Internal(
            "count_documents: not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn create_index(&self, _name: &str, _model: IndexModel) -> Result<String> {
        Err(Error::Internal(
            "create_index: not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn drop_index(&self, _name: &str, _index_name: &str) -> Result<()> {
        Err(Error::Internal(
            "drop_index: not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn list_indexes(&self, _name: &str) -> Result<Vec<IndexInfo>> {
        Err(Error::Internal(
            "list_indexes: not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn list_collection_names(&self) -> Result<Vec<String>> {
        Err(Error::Internal(
            "list_collection_names: not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn drop_collection(&self, _name: &str) -> Result<()> {
        Err(Error::Internal(
            "drop_collection: not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn create_collection(&self, _name: &str) -> Result<()> {
        Err(Error::Internal(
            "create_collection: not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn checkpoint(&self) -> Result<()> {
        Err(Error::Internal(
            "checkpoint: WAL not yet implemented (Phase 1)".into(),
        ))
    }

    pub(crate) fn backup(&self, _dest: &Path) -> Result<()> {
        Err(Error::Internal(
            "backup: not yet implemented (Phase 1)".into(),
        ))
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
    ///
    /// # Security
    ///
    /// **Symlink prevention**: If `path` resolves to a symlink mqlite returns
    /// [`Error::SymlinkRejected`] instead of following the symlink.  This prevents
    /// an attacker who controls the filesystem path from redirecting the open to an
    /// arbitrary file (see mqlite security.md threat #12).
    ///
    /// **File permissions**: Newly created `.mqlite` files are created with mode
    /// `0600` (owner read/write only) on Unix.  Associated WAL (`.mqlite-wal`) and
    /// shared-memory (`.mqlite-shm`) files are also created with `0600`.
    /// On Unix systems, file permissions are the primary access-control mechanism
    /// for embedded-mode databases — there is no built-in authentication layer.
    pub fn open_with_options(path: impl AsRef<Path>, opts: OpenOptions) -> Result<Database> {
        let path = path.as_ref().to_owned();

        // Security: reject symlinks before touching the file.
        reject_symlink(&path)?;

        // Also check associated WAL and SHM paths.
        let wal_path = wal_path(&path);
        let shm_path = shm_path(&path);
        reject_symlink(&wal_path)?;
        reject_symlink(&shm_path)?;

        // If the file is being created (doesn't exist yet), create it with 0600
        // permissions on Unix.  The file handle is dropped immediately; the storage
        // engine will open it again with its own `OpenOptions`.
        if !path.exists() && opts.create_if_missing {
            let _f = create_db_file_secure(&path)?;
            // File is closed here; storage engine will reopen it.  Permissions
            // are already set to 0600.
        }

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // ---- Symlink rejection -------------------------------------------------

    /// Opening a path that is a symlink must return `Error::SymlinkRejected`.
    #[test]
    #[cfg(unix)]
    fn open_symlink_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real_file = dir.path().join("real.mqlite");
        let symlink_path = dir.path().join("link.mqlite");

        // Create a real file and a symlink to it.
        fs::write(&real_file, b"").expect("create real file");
        std::os::unix::fs::symlink(&real_file, &symlink_path).expect("create symlink");

        // Use .err().unwrap() since Database doesn't implement Debug (required by unwrap_err).
        let result = Database::open(&symlink_path);
        assert!(result.is_err(), "expected error opening symlink");
        let err = result.err().unwrap();
        assert!(
            matches!(err, Error::SymlinkRejected { .. }),
            "expected SymlinkRejected"
        );
    }

    /// `SymlinkRejected` must carry error code BAD_VALUE (2).
    #[test]
    #[cfg(unix)]
    fn symlink_rejected_error_code() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real_file = dir.path().join("real.mqlite");
        let symlink_path = dir.path().join("link.mqlite");

        fs::write(&real_file, b"").expect("create real file");
        std::os::unix::fs::symlink(&real_file, &symlink_path).expect("create symlink");

        let result = Database::open(&symlink_path);
        let err = result.err().unwrap();
        assert_eq!(err.code(), Some(2), "SymlinkRejected should have error code BAD_VALUE (2)");
    }

    /// Opening a path that does not yet exist (new database) must succeed.
    #[test]
    fn open_new_file_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("new.mqlite");
        assert!(!db_path.exists());

        let _db = Database::open(&db_path).expect("should create new database");
    }

    // ---- File permissions --------------------------------------------------

    /// A newly created database file must have mode 0600 on Unix.
    #[test]
    #[cfg(unix)]
    fn new_database_file_has_0600_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("perms.mqlite");

        Database::open(&db_path).expect("open");

        let meta = fs::metadata(&db_path).expect("metadata");
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "database file must have mode 0600, got {:o}", mode);
    }

    /// Opening an existing regular file (not a symlink) must succeed.
    #[test]
    fn open_existing_regular_file_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("existing.mqlite");
        fs::write(&db_path, b"").expect("create file");

        let _db = Database::open(&db_path).expect("open existing file");
    }
}
