//! # mqlite Client — top-level entry point
//!
//! [`Client`] is the root of the mqlite object model.  It matches the MongoDB
//! Rust driver hierarchy:
//!
//! ```text
//! Client::open(path)          ← file-level handle (this module)
//!   └─ client.database(name)  ← database namespace handle (database.rs)
//!        └─ db.collection::<T>(name)  ← typed CRUD handle (collection.rs)
//! ```
//!
//! `Client` holds `Arc<ClientInner>` which owns the storage engine, file lock,
//! and write-serialisation mutex.  `Database` and `Collection<T>` handles each
//! hold a clone of the same `Arc<ClientInner>`, so they are cheap to create
//! and share the same underlying state.

use bson::{Bson, Document};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{
    cursor::Cursor,
    database::Database,
    error::{Error, Result},
    index::{IndexInfo, IndexModel},
    options::{
        DurabilityMode, FindOneAndDeleteOptions, FindOneAndReplaceOptions,
        FindOneAndUpdateOptions, FindOptions, InsertManyOptions, OpenOptions, UpdateOptions,
    },
    results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
    storage::{
        buffer_pool::BufferPool,
        file_io::FilePageSource,
        handle::BufferPoolHandle,
        header::{FileHeader, HEADER_PAGE_SIZE},
        lock::{self, FileLock, NoopFileLock},
        paged_engine::PagedEngine,
    },
    storage_engine::StorageEngine,
    wal::{WalLayeredSource, WalManager},
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
/// Symlink following at `Client::open()` time could allow an attacker who controls
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

/// Read and validate the page-0 [`FileHeader`] from the backing file via the
/// lock file descriptor.
fn read_and_validate_header(
    lock: &dyn crate::storage::lock::FileLock,
    path: &Path,
) -> Result<FileHeader> {
    let mut buf = [0u8; HEADER_PAGE_SIZE];
    lock.read_exact_at(0, &mut buf)?;
    let header = FileHeader::from_bytes(&buf).map_err(|e| enrich_path(e, path))?;
    header.validate().map_err(|e| enrich_path(e, path))?;
    Ok(header)
}

/// Write a fresh [`FileHeader`] as page 0 via the lock file descriptor.
fn write_initial_header(lock: &dyn crate::storage::lock::FileLock) -> Result<()> {
    let header = FileHeader::new_now();
    let bytes = header.to_bytes();
    lock.write_at(0, &bytes)
}

/// Attach the real on-disk path to a [`Error::CorruptDatabase`] whose `path`
/// field was left empty by the parser (which doesn't know the path).
fn enrich_path(e: Error, path: &Path) -> Error {
    match e {
        Error::CorruptDatabase {
            path: ref p,
            ref detail,
            recoverable,
        } if p == std::path::Path::new("") => Error::CorruptDatabase {
            path: path.to_owned(),
            detail: detail.clone(),
            recoverable,
        },
        other => other,
    }
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
fn create_db_file_secure(path: &Path) -> Result<std::fs::File> {
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

// ---------------------------------------------------------------------------
// ClientInner — shared internal state
// ---------------------------------------------------------------------------

/// Internal shared state for a [`Client`].
///
/// Wrapped in `Arc` and shared across [`Client`] clones, [`Database`] handles,
/// and [`crate::collection::Collection`] handles.  This is the single-writer
/// multi-reader (SWMR) synchronization point.
///
/// ## Locking hierarchy
///
/// 1. **`file_lock`** (OS advisory lock) — serializes writers *across processes*.
/// 2. **`writer_lock`** (in-process `Mutex`) — serializes concurrent writer
///    threads within a single process.
///
/// Callers acquiring both locks must always take `writer_lock` *after*
/// `file_lock` to avoid deadlocks.
///
/// ## Storage engine
///
/// `engine` is a `Box<dyn StorageEngine>` — the concrete type is always
/// [`PagedEngine`] in Phase 1, but `ClientInner` never knows this.  This
/// abstraction lets future phases swap in a different engine without changing
/// the public API layer.
pub(crate) struct ClientInner {
    /// Path to the database file. `None` for in-memory databases.
    pub path: Option<PathBuf>,
    /// Configuration options.
    pub opts: OpenOptions,
    /// In-process writer mutex — one write at a time within this process.
    ///
    /// `PagedEngine` already serialises all access via its own `Mutex`, but
    /// `writer_lock` provides an extra guarantee: multi-step operations in
    /// `ClientInner` (e.g., `insert_many`) are atomic at the process level.
    /// This will be revisited in Phase 1.6 (SWMR).
    writer_lock: Mutex<()>,
    /// OS advisory file lock.
    ///
    /// Stored as `Arc` so the same fd can be shared with the `FilePageSource`
    /// backing the buffer pool.  Opening a second fd would release the POSIX
    /// advisory lock when the second fd is closed (POSIX footgun).
    file_lock: Arc<dyn FileLock>,
    /// Buffer pool handle — file I/O infrastructure wired in by R1.1.
    ///
    /// `None` for in-memory clients.  File-backed clients always have `Some`.
    #[allow(dead_code)]
    pub(crate) buffer_pool: Option<Arc<BufferPoolHandle>>,
    /// Storage engine.  All CRUD operations are dispatched through this trait.
    pub(crate) engine: Box<dyn StorageEngine>,
    /// Dedicated file handle for WAL→main-file checkpoint I/O.
    ///
    /// Kept separate from `file_lock` to avoid the POSIX advisory-lock fd
    /// sharing footgun: we never use this fd for locking, only for writing
    /// checkpointed pages.  Both fds are closed together when `ClientInner`
    /// is dropped, so the lock lifetime is unaffected.
    ///
    /// `None` for in-memory clients.
    wal_main_file: Option<Arc<Mutex<std::fs::File>>>,
}

impl ClientInner {
    fn new(path: Option<PathBuf>, opts: OpenOptions, file_lock: Arc<dyn FileLock>) -> Self {
        ClientInner {
            path,
            opts,
            writer_lock: Mutex::new(()),
            file_lock,
            buffer_pool: None,
            engine: Box::new(PagedEngine::new()),
            wal_main_file: None,
        }
    }

    fn new_with_buffer_pool(
        path: Option<PathBuf>,
        opts: OpenOptions,
        file_lock: Arc<dyn FileLock>,
        buffer_pool: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
        wal_main_file: Option<Arc<Mutex<std::fs::File>>>,
    ) -> Result<Self> {
        let engine = PagedEngine::new_buffered(
            Arc::clone(&buffer_pool),
            catalog_root_page,
            catalog_root_level,
        )?;
        Ok(ClientInner {
            path,
            opts,
            writer_lock: Mutex::new(()),
            file_lock,
            buffer_pool: Some(buffer_pool),
            engine: Box::new(engine),
            wal_main_file,
        })
    }
}

// ---------------------------------------------------------------------------
// ClientInner CRUD method implementations
// ---------------------------------------------------------------------------
//
// All storage operations are routed through `self.engine` (a `Box<dyn
// StorageEngine>`).  `ClientInner` owns the serialisation / deserialisation
// layer (generic `T` parameters) on top of the Document-level trait.
// ---------------------------------------------------------------------------

impl ClientInner {
    /// Acquire the in-process writer lock, respecting the configured
    /// `busy_timeout` and `busy_handler`.
    ///
    /// Uses a spin-loop with `try_lock()` instead of a blocking `lock()` so
    /// that reader threads (which hold no writer lock) are never delayed by
    /// this wait.  Writer threads spin briefly, then return
    /// [`Error::WriterBusy`] on timeout.
    fn acquire_writer_lock(&self) -> Result<std::sync::MutexGuard<'_, ()>> {
        // Fast path: try without any spin first.
        match self.writer_lock.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(std::sync::TryLockError::Poisoned(e)) => {
                return Err(Error::Internal(format!("writer_lock poisoned: {e}")));
            }
            Err(std::sync::TryLockError::WouldBlock) => {}
        }

        let timeout = self.opts.busy_timeout;

        // If a custom busy handler is configured, delegate to it.
        if let Some(handler) = &self.opts.busy_handler {
            let mut attempts: u32 = 0;
            loop {
                std::thread::sleep(std::time::Duration::from_millis(1));
                match self.writer_lock.try_lock() {
                    Ok(guard) => return Ok(guard),
                    Err(std::sync::TryLockError::Poisoned(e)) => {
                        return Err(Error::Internal(format!("writer_lock poisoned: {e}")));
                    }
                    Err(std::sync::TryLockError::WouldBlock) => {}
                }
                if !handler.0(attempts) {
                    return Err(Error::WriterBusy);
                }
                attempts = attempts.saturating_add(1);
            }
        }

        // Default: spin until busy_timeout expires.
        if timeout.is_zero() {
            return Err(Error::WriterBusy);
        }
        let deadline = Instant::now() + timeout;
        loop {
            std::thread::sleep(std::time::Duration::from_millis(1));
            match self.writer_lock.try_lock() {
                Ok(guard) => return Ok(guard),
                Err(std::sync::TryLockError::Poisoned(e)) => {
                    return Err(Error::Internal(format!("writer_lock poisoned: {e}")));
                }
                Err(std::sync::TryLockError::WouldBlock) => {}
            }
            if Instant::now() >= deadline {
                return Err(Error::WriterBusy);
            }
        }
    }

    pub(crate) fn insert_one<T: serde::Serialize>(
        &self,
        name: &str,
        doc: &T,
    ) -> Result<InsertOneResult> {
        #[cfg(feature = "tracing")]
        tracing::debug!(target: "mqlite", collection = name, doc_count = 1u64, "mqlite::insert");

        let bson_doc = bson::to_document(doc).map_err(Error::BsonSerialization)?;
        let _guard = self.acquire_writer_lock()?;
        let id = self.engine.insert(name, bson_doc)?;
        let oid = match id {
            Bson::ObjectId(o) => o,
            // For non-ObjectId _id values, generate a surrogate ObjectId to
            // satisfy the `InsertOneResult` type.  The document retains its
            // original `_id`.  This is a pre-existing limitation.
            _ => crate::storage::oid::ObjectIdGenerator::generate(),
        };
        // MF-5: FullSync guarantees data survives a process crash after this
        // call returns.  Flush dirty pages then fsync.
        self.flush_and_sync_if_fullsync()?;
        Ok(InsertOneResult { inserted_id: oid })
    }

    pub(crate) fn insert_many<T: serde::Serialize>(
        &self,
        name: &str,
        docs: &[T],
        opts: InsertManyOptions,
    ) -> Result<InsertManyResult> {
        use crate::results::BulkWriteError;
        use std::collections::HashMap;

        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: "mqlite",
            collection = name,
            doc_count = docs.len() as u64,
            "mqlite::insert"
        );

        let _guard = self.acquire_writer_lock()?;
        let mut inserted_ids: HashMap<usize, Bson> = HashMap::new();
        let mut errors: Vec<BulkWriteError> = Vec::new();

        'outer: for (i, doc) in docs.iter().enumerate() {
            let bson_doc = match bson::to_document(doc).map_err(Error::BsonSerialization) {
                Ok(d) => d,
                Err(e) => {
                    errors.push(BulkWriteError {
                        index: i,
                        code: e.code().unwrap_or(1),
                        message: e.to_string(),
                    });
                    if opts.ordered {
                        break 'outer;
                    }
                    continue;
                }
            };
            match self.engine.insert(name, bson_doc) {
                Ok(id) => {
                    inserted_ids.insert(i, id);
                }
                Err(e) => {
                    errors.push(BulkWriteError {
                        index: i,
                        code: e.code().unwrap_or(1),
                        message: e.to_string(),
                    });
                    if opts.ordered {
                        break 'outer;
                    }
                }
            }
        }

        // MF-5: FullSync guarantees all successfully inserted documents
        // survive a process crash after this call returns.
        self.flush_and_sync_if_fullsync()?;
        Ok(InsertManyResult {
            inserted_ids,
            errors,
        })
    }

    pub(crate) fn find_one<T: DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
    ) -> Result<Option<T>> {
        #[cfg(feature = "tracing")]
        {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            for k in filter.keys() {
                k.hash(&mut h);
            }
            tracing::debug!(
                target: "mqlite",
                collection = name,
                filter_hash = h.finish(),
                doc_count = 0u64,
                "mqlite::find"
            );
        }
        match self.engine.find_one(name, &filter)? {
            None => Ok(None),
            Some(doc) => bson::from_document(doc)
                .map(Some)
                .map_err(Error::BsonDeserialization),
        }
    }

    pub(crate) fn find<T: DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
        opts: FindOptions,
    ) -> Result<Cursor<T>> {
        #[cfg(feature = "tracing")]
        {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            for k in filter.keys() {
                k.hash(&mut h);
            }
            tracing::debug!(
                target: "mqlite",
                collection = name,
                filter_hash = h.finish(),
                doc_count = 0u64,
                "mqlite::find"
            );
        }
        let docs = self.engine.find(name, &filter, &opts)?;
        let docs_examined = docs.len() as u64;
        Ok(Cursor::new(docs, docs_examined))
    }

    pub(crate) fn update_one(
        &self,
        name: &str,
        filter: Document,
        update: Document,
        opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        let _guard = self.acquire_writer_lock()?;
        self.engine.update(name, &filter, &update, &opts, false)
    }

    pub(crate) fn update_many(
        &self,
        name: &str,
        filter: Document,
        update: Document,
        opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        let _guard = self.acquire_writer_lock()?;
        self.engine.update(name, &filter, &update, &opts, true)
    }

    pub(crate) fn delete_one(&self, name: &str, filter: Document) -> Result<DeleteResult> {
        let _guard = self.acquire_writer_lock()?;
        self.engine.delete(name, &filter, false)
    }

    pub(crate) fn delete_many(&self, name: &str, filter: Document) -> Result<DeleteResult> {
        let _guard = self.acquire_writer_lock()?;
        self.engine.delete(name, &filter, true)
    }

    pub(crate) fn find_one_and_update<T: Serialize + DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
        update: Document,
    ) -> Result<Option<T>> {
        let opts = FindOneAndUpdateOptions::new();
        self.find_one_and_update_with_options(name, filter, update, opts)
    }

    pub(crate) fn find_one_and_update_with_options<T: Serialize + DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
        update: Document,
        opts: FindOneAndUpdateOptions,
    ) -> Result<Option<T>> {
        let _guard = self.acquire_writer_lock()?;
        match self
            .engine
            .find_one_and_update_doc(name, &filter, &update, &opts)?
        {
            None => Ok(None),
            Some(doc) => bson::from_document(doc)
                .map(Some)
                .map_err(Error::BsonDeserialization),
        }
    }

    pub(crate) fn find_one_and_delete<T: DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
    ) -> Result<Option<T>> {
        let opts = FindOneAndDeleteOptions::new();
        self.find_one_and_delete_with_options(name, filter, opts)
    }

    pub(crate) fn find_one_and_delete_with_options<T: DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
        opts: FindOneAndDeleteOptions,
    ) -> Result<Option<T>> {
        let _guard = self.acquire_writer_lock()?;
        match self.engine.find_one_and_delete_doc(name, &filter, &opts)? {
            None => Ok(None),
            Some(doc) => bson::from_document(doc)
                .map(Some)
                .map_err(Error::BsonDeserialization),
        }
    }

    pub(crate) fn find_one_and_replace<T: Serialize + DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
        replacement: &T,
    ) -> Result<Option<T>> {
        let opts = FindOneAndReplaceOptions::new();
        self.find_one_and_replace_with_options(name, filter, replacement, opts)
    }

    pub(crate) fn find_one_and_replace_with_options<T: Serialize + DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
        replacement: &T,
        opts: FindOneAndReplaceOptions,
    ) -> Result<Option<T>> {
        let replacement_doc = bson::to_document(replacement).map_err(Error::BsonSerialization)?;
        let _guard = self.acquire_writer_lock()?;
        match self
            .engine
            .find_one_and_replace_doc(name, &filter, &replacement_doc, &opts)?
        {
            None => Ok(None),
            Some(doc) => bson::from_document(doc)
                .map(Some)
                .map_err(Error::BsonDeserialization),
        }
    }

    pub(crate) fn estimated_document_count(&self, name: &str) -> Result<u64> {
        // Estimated count = exact count for the stub engine.
        self.engine.count(name, &Document::new())
    }

    pub(crate) fn count_documents(&self, name: &str, filter: Document) -> Result<u64> {
        self.engine.count(name, &filter)
    }

    pub(crate) fn create_index(&self, name: &str, model: IndexModel) -> Result<String> {
        let _guard = self.acquire_writer_lock()?;
        self.engine.create_index(name, &model)
    }

    pub(crate) fn drop_index(&self, name: &str, index_name: &str) -> Result<()> {
        let _guard = self.acquire_writer_lock()?;
        self.engine.drop_index(name, index_name)
    }

    pub(crate) fn list_indexes(&self, name: &str) -> Result<Vec<IndexInfo>> {
        self.engine.list_indexes(name)
    }

    pub(crate) fn list_collection_names(&self) -> Result<Vec<String>> {
        self.engine.list_namespaces()
    }

    pub(crate) fn drop_collection(&self, name: &str) -> Result<()> {
        let _guard = self.acquire_writer_lock()?;
        self.engine.drop_namespace(name)
    }

    pub(crate) fn create_collection(&self, name: &str) -> Result<()> {
        let _guard = self.acquire_writer_lock()?;
        self.engine.create_namespace(name)
    }

    pub(crate) fn checkpoint(&self) -> Result<()> {
        if self.path.is_none() {
            return Ok(());
        }

        // Flush dirty buffer-pool pages (B+ tree nodes + file header) to the
        // WAL (if attached) or directly to the main file (legacy path).
        self.engine.checkpoint()?;

        // WAL checkpoint: move all committed WAL frames into the main file
        // and reset the WAL to empty.
        if let (Some(bp), Some(wal_file_mutex)) = (&self.buffer_pool, &self.wal_main_file) {
            let mut wal_file = wal_file_mutex
                .lock()
                .map_err(|_| Error::Internal("WAL main file mutex poisoned".into()))?;
            bp.checkpoint_through_wal(&mut *wal_file)?;
        }

        Ok(())
    }

    /// Flush dirty pages to disk and, if configured for `FullSync`, call
    /// `fsync(2)` to ensure data reaches the storage device.
    ///
    /// Called after every write operation when
    /// [`DurabilityMode::FullSync`] is active.  This is the MF-5 guarantee:
    /// after this method returns, the written data survives a process crash.
    fn flush_and_sync_if_fullsync(&self) -> Result<()> {
        if self.opts.durability != DurabilityMode::FullSync {
            return Ok(());
        }
        // Flush dirty pages to WAL (or directly to file on the non-WAL path).
        self.engine.checkpoint()?;
        // WAL checkpoint: immediately move WAL frames into the main file so
        // this write survives a process crash without relying on commit-frame
        // recovery.  After this call the main file is complete and durable.
        if let (Some(bp), Some(wal_file_mutex)) = (&self.buffer_pool, &self.wal_main_file) {
            let mut wal_file = wal_file_mutex
                .lock()
                .map_err(|_| Error::Internal("WAL main file mutex poisoned".into()))?;
            bp.checkpoint_through_wal(&mut *wal_file)?;
        }
        // fsync: push OS page-cache to the storage device.
        self.file_lock.sync()
    }

    pub(crate) fn backup(&self, dest: &Path) -> Result<()> {
        // Backup of an in-memory database is not supported.
        let src_path = match &self.path {
            Some(p) => p.as_path(),
            None => {
                return Err(Error::Internal(
                    "backup: in-memory databases cannot be backed up to a file".into(),
                ));
            }
        };

        // Security: reject symlinks at the destination path.
        reject_symlink(dest)?;

        // Reject backup-to-self: canonicalize both paths if dest already
        // exists.  If dest does not exist yet, it cannot be the same file.
        if dest.exists() {
            let dest_canon = std::fs::canonicalize(dest).unwrap_or_default();
            let src_canon = std::fs::canonicalize(src_path).unwrap_or_default();
            if !dest_canon.as_os_str().is_empty()
                && !src_canon.as_os_str().is_empty()
                && dest_canon == src_canon
            {
                return Err(Error::Internal(
                    "backup: destination is the same file as the source".into(),
                ));
            }
        }

        // Acquire the in-process writer lock so no writes can interleave with
        // our checkpoint and copy.
        let _guard = self.acquire_writer_lock()?;

        // Checkpoint: flush dirty buffer-pool pages to the WAL, then move all
        // WAL frames into the main file.  After this, the main file contains
        // the complete committed state and is safe to copy.
        self.engine.checkpoint()?;
        if let (Some(bp), Some(wal_file_mutex)) = (&self.buffer_pool, &self.wal_main_file) {
            let mut wal_file = wal_file_mutex
                .lock()
                .map_err(|_| Error::Internal("WAL main file mutex poisoned".into()))?;
            bp.checkpoint_through_wal(&mut *wal_file)?;
        }

        // Determine the byte length of the database file.
        let file_size = std::fs::metadata(src_path)?.len();

        // Copy the database file to dest using the *existing* file_lock fd
        // for reads.  We must NOT open a new file descriptor to the source
        // while the advisory lock is held: POSIX guarantees that closing ANY
        // fd to a file releases ALL advisory locks the process holds on that
        // file (the "POSIX advisory lock footgun").
        let mut dest_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dest)
            .map_err(Error::Io)?;

        // Create the destination file with restricted permissions (0600) on
        // Unix, matching the behaviour of Client::open for new database files.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            dest_file
                .set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(Error::Io)?;
        }

        // Stream the source file contents in 64 KB chunks through the lock fd.
        use std::io::Write;
        const CHUNK: usize = 64 * 1024;
        let mut buf = vec![0u8; CHUNK];
        let mut offset: u64 = 0;

        while offset < file_size {
            let remaining = (file_size - offset) as usize;
            let read_len = remaining.min(CHUNK);
            let chunk = &mut buf[..read_len];

            self.file_lock.read_exact_at(offset, chunk)?;
            dest_file.write_all(chunk).map_err(Error::Io)?;

            offset += read_len as u64;
        }

        // Flush the destination file's data to the OS page cache.
        dest_file.flush().map_err(Error::Io)?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Client — public handle
// ---------------------------------------------------------------------------

/// A connection to an mqlite database file.
///
/// `Client` is cheaply cloneable — all clones share the same underlying storage,
/// writer lock, and buffer pool.  It is the root of the mqlite object model,
/// matching the MongoDB Rust driver hierarchy:
///
/// ```text
/// Client::open("myapp.mqlite")?
///   └─ client.database("mydb")
///        └─ db.collection::<User>("users")
///             └─ col.insert_one(...) / col.find(...) / ...
/// ```
///
/// # Opening
///
/// ```no_run
/// use mqlite::Client;
///
/// // Open (or create) a database file
/// let client = Client::open("myapp.mqlite")?;
///
/// // In-memory (for tests — no files created)
/// let client = Client::open_in_memory()?;
/// # Ok::<(), mqlite::Error>(())
/// ```
///
/// # Databases and Collections
///
/// ```no_run
/// use mqlite::{Client, doc};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct User { name: String }
///
/// # fn main() -> mqlite::Result<()> {
/// let client = Client::open_in_memory()?;
/// let db = client.database("myapp");
/// let users = db.collection::<User>("users");
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

impl Client {
    /// Open a database file. Creates the file if it does not exist.
    ///
    /// Automatically replays the WAL on recovery. Uses sensible defaults
    /// (64MB buffer pool, 100ms durability interval, 5s busy timeout).
    pub fn open(path: impl AsRef<Path>) -> Result<Client> {
        Client::open_with_options(path, OpenOptions::new())
    }

    /// Open a database file with explicit configuration.
    ///
    /// # Security
    ///
    /// **Symlink prevention**: If `path` resolves to a symlink mqlite returns
    /// [`Error::SymlinkRejected`] instead of following the symlink.
    ///
    /// **File permissions**: Newly created `.mqlite` files are created with mode
    /// `0600` (owner read/write only) on Unix.
    ///
    /// # Multi-process locking
    ///
    /// `open_with_options` acquires an OS-level advisory lock on the database
    /// file before returning.  Returns [`Error::WriterBusy`] on timeout.
    pub fn open_with_options(path: impl AsRef<Path>, opts: OpenOptions) -> Result<Client> {
        let path = path.as_ref().to_owned();

        // Security: reject symlinks before touching the file.
        reject_symlink(&path)?;

        // Also check associated WAL and SHM paths.
        let wal_path = wal_path(&path);
        let shm_path = shm_path(&path);
        reject_symlink(&wal_path)?;
        reject_symlink(&shm_path)?;

        // Create file with 0600 permissions if new.
        if !path.exists() && opts.create_if_missing {
            let _f = create_db_file_secure(&path)?;
        }

        // Acquire OS advisory file lock.
        // Store as Arc so the same fd can be shared with FilePageSource.
        let file_lock: Arc<dyn FileLock> = Arc::from(lock::open_file_lock(&path)?);
        let busy_timeout = opts.busy_timeout;
        #[cfg(feature = "tracing")]
        let _lock_t = std::time::Instant::now();
        let was_contended = if opts.read_only {
            file_lock.acquire_shared(busy_timeout)?
        } else {
            file_lock.acquire_exclusive(busy_timeout)?
        };

        #[cfg(feature = "tracing")]
        {
            let wait_ms = _lock_t.elapsed().as_millis() as u64;
            tracing::debug!(
                target: "mqlite",
                wait_duration_ms = wait_ms,
                acquired = true,
                "mqlite::writer_lock"
            );
        }
        if was_contended {
            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: "mqlite",
                path = %path.display(),
                "database writer lock was contended on open"
            );
        }

        // Header initialization / validation.
        let file_size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        if file_size == 0 {
            if !opts.read_only {
                write_initial_header(file_lock.as_ref())?;
            }
        } else if (file_size as usize) < HEADER_PAGE_SIZE {
            return Err(Error::CorruptDatabase {
                path: path.clone(),
                detail: format!(
                    "file is truncated: {} bytes (minimum {} required for a \
                     valid page-0 header)",
                    file_size, HEADER_PAGE_SIZE,
                ),
                recoverable: false,
            });
        } else {
            read_and_validate_header(file_lock.as_ref(), &path)?;
        }

        // R1.2: Construct the buffer pool handle wired to the database file and
        // create a B+ tree engine backed by it.
        //
        // The pool is backed by FilePageSource which shares the lock fd (Arc clone)
        // to avoid the POSIX advisory-lock footgun.  OpenOptions::buffer_pool_size
        // controls the total byte budget split between 4 KB and 32 KB partitions.
        //
        // For an existing file, the catalog root page is read from the file header.
        // For a new file, catalog_root_page == 0 means a fresh catalog is created.
        let file_header = if file_size == 0 {
            FileHeader::new_now()
        } else {
            let mut hdr_buf = [0u8; HEADER_PAGE_SIZE];
            file_lock.read_exact_at(0, &mut hdr_buf)?;
            FileHeader::from_bytes(&hdr_buf).unwrap_or_else(|_| FileHeader::new_now())
        };
        let catalog_root_page = file_header.catalog_root_page;
        let catalog_root_level = file_header.catalog_root_level;

        // Open a dedicated file handle for WAL checkpoint I/O.  This fd is
        // never used for advisory locking — only for writing checkpointed
        // pages back to the main file.  Both fds live for the same duration
        // as ClientInner so the advisory lock lifetime is unaffected.
        let mut wal_io_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(Error::Io)?;

        let wal = Arc::new(Mutex::new(WalManager::open_or_create(
            &path,
            &file_header,
            &mut wal_io_file,
        )?));

        let file_src = Arc::new(FilePageSource::new(Arc::clone(&file_lock)));
        let layered_source: Box<dyn crate::storage::buffer_pool::PageSource> =
            Box::new(WalLayeredSource::new(
                Arc::clone(&file_src) as Arc<dyn crate::storage::buffer_pool::PageSource>,
                Arc::clone(&wal),
            ));
        let pool = Arc::new(BufferPool::new(opts.buffer_pool_size, layered_source));
        let wal_main_file = Arc::new(Mutex::new(wal_io_file));
        let buffer_pool = Arc::new(BufferPoolHandle::with_wal(
            pool,
            file_header,
            wal,
            Arc::clone(&wal_main_file),
        ));

        let inner = Arc::new(ClientInner::new_with_buffer_pool(
            Some(path.clone()),
            opts,
            file_lock,
            buffer_pool,
            catalog_root_page,
            catalog_root_level,
            Some(wal_main_file),
        )?);
        let _ = file_size; // used above, suppress warning
        #[cfg(feature = "tracing")]
        tracing::info!(
            target: "mqlite",
            path = %path.display(),
            format_version = crate::storage::header::FORMAT_VERSION,
            "mqlite::open"
        );
        Ok(Client { inner })
    }

    /// Create an in-memory client with no persistence.
    ///
    /// In-memory databases are ideal for testing — no files are created and
    /// everything is released when the last handle is dropped.
    ///
    /// File locking is a no-op for in-memory databases (there is no file to lock).
    pub fn open_in_memory() -> Result<Client> {
        let noop_lock: Arc<dyn FileLock> = Arc::new(NoopFileLock);
        let inner = Arc::new(ClientInner::new(None, OpenOptions::new(), noop_lock));
        Ok(Client { inner })
    }

    /// Get a handle to a named database.
    ///
    /// This call is infallible — the database namespace is logical only.
    /// No I/O occurs; a [`Database`] handle is returned immediately.
    ///
    /// # Example
    /// ```no_run
    /// use mqlite::Client;
    ///
    /// # fn main() -> mqlite::Result<()> {
    /// let client = Client::open_in_memory()?;
    /// let db = client.database("myapp");
    /// # Ok(())
    /// # }
    /// ```
    pub fn database(&self, name: &str) -> Database {
        Database {
            inner: Arc::clone(&self.inner),
            db_name: name.to_owned(),
        }
    }

    /// Force a WAL checkpoint.
    ///
    /// After this returns, the main database file is safe to copy as a backup.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }

    /// Hot backup to a destination file.
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        self.inner.backup(dest.as_ref())
    }

    /// Flush the WAL, checkpoint, and close the client.
    ///
    /// Use this when you need a guarantee that all committed data is in the main
    /// file (e.g., before copying the file as a backup). `Drop` performs a
    /// non-blocking close.
    pub fn close(self) -> Result<()> {
        self.inner.checkpoint()
    }
}

impl Drop for Client {
    /// Non-blocking close.
    ///
    /// Checkpoints when this is the last handle. WAL data remains on disk
    /// otherwise and will be replayed automatically on the next `Client::open`.
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1 {
            let _ = self.inner.checkpoint();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use libc;
    use std::fs;

    // ---- Symlink rejection -------------------------------------------------

    #[test]
    #[cfg(unix)]
    fn open_symlink_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real_file = dir.path().join("real.mqlite");
        let symlink_path = dir.path().join("link.mqlite");

        fs::write(&real_file, b"").expect("create real file");
        std::os::unix::fs::symlink(&real_file, &symlink_path).expect("create symlink");

        let result = Client::open(&symlink_path);
        assert!(result.is_err(), "expected error opening symlink");
        let err = result.err().unwrap();
        assert!(
            matches!(err, Error::SymlinkRejected { .. }),
            "expected SymlinkRejected"
        );
    }

    #[test]
    #[cfg(unix)]
    fn symlink_rejected_error_code() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real_file = dir.path().join("real.mqlite");
        let symlink_path = dir.path().join("link.mqlite");

        fs::write(&real_file, b"").expect("create real file");
        std::os::unix::fs::symlink(&real_file, &symlink_path).expect("create symlink");

        let result = Client::open(&symlink_path);
        let err = result.err().unwrap();
        assert_eq!(
            err.code(),
            Some(2),
            "SymlinkRejected should have error code BAD_VALUE (2)"
        );
    }

    #[test]
    fn open_new_file_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("new.mqlite");
        assert!(!db_path.exists());

        let _client = Client::open(&db_path).expect("should create new database");
    }

    // ---- File permissions --------------------------------------------------

    #[test]
    #[cfg(unix)]
    fn new_database_file_has_0600_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("perms.mqlite");

        Client::open(&db_path).expect("open");

        let meta = fs::metadata(&db_path).expect("metadata");
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "database file must have mode 0600, got {:o}",
            mode
        );
    }

    #[test]
    fn open_existing_regular_file_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("existing.mqlite");
        fs::write(&db_path, b"").expect("create file");

        let _client = Client::open(&db_path).expect("open existing file");
    }

    // ---- Multi-process file locking ----------------------------------------

    #[test]
    #[cfg(unix)]
    fn in_memory_open_does_not_lock() {
        let _c1 = Client::open_in_memory().expect("first open");
        let _c2 = Client::open_in_memory().expect("second open");
    }

    #[test]
    #[cfg(unix)]
    fn cross_process_second_writer_gets_writer_busy() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("locked.mqlite");

        let (read_fd, write_fd) = {
            let mut fds = [0i32; 2];
            assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
            (fds[0], fds[1])
        };

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork() failed");

        if pid == 0 {
            unsafe { libc::close(read_fd) };
            let _client = Client::open(&db_path).expect("child: Client::open");
            let ready: u8 = 1;
            unsafe { libc::write(write_fd, &ready as *const u8 as *const libc::c_void, 1) };
            std::thread::sleep(std::time::Duration::from_secs(5));
            unsafe { libc::_exit(0) };
        }

        unsafe { libc::close(write_fd) };
        let mut buf = 0u8;
        let n = unsafe { libc::read(read_fd, &mut buf as *mut u8 as *mut libc::c_void, 1) };
        assert_eq!(n, 1, "parent: did not receive child ready signal");
        unsafe { libc::close(read_fd) };

        let result = Client::open_with_options(
            &db_path,
            OpenOptions::new().busy_timeout(std::time::Duration::ZERO),
        );
        assert!(
            matches!(result, Err(Error::WriterBusy)),
            "expected WriterBusy, got: {:?}",
            result.err()
        );

        unsafe { libc::kill(pid, libc::SIGKILL) };
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };
    }

    #[test]
    #[cfg(unix)]
    fn writer_crash_releases_lock_for_next_opener() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("crash.mqlite");

        let (read_fd, write_fd) = {
            let mut fds = [0i32; 2];
            assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
            (fds[0], fds[1])
        };

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork() failed");

        if pid == 0 {
            unsafe { libc::close(read_fd) };
            let _client = Client::open(&db_path).expect("child: Client::open");
            let ready: u8 = 1;
            unsafe { libc::write(write_fd, &ready as *const u8 as *const libc::c_void, 1) };
            std::thread::sleep(std::time::Duration::from_secs(60));
            unsafe { libc::_exit(0) };
        }

        unsafe { libc::close(write_fd) };
        let mut buf = 0u8;
        let n = unsafe { libc::read(read_fd, &mut buf as *mut u8 as *mut libc::c_void, 1) };
        assert_eq!(n, 1);
        unsafe { libc::close(read_fd) };

        unsafe { libc::kill(pid, libc::SIGKILL) };
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };

        Client::open(&db_path).expect("should open after writer crash");
    }

    // ---- Header initialization / corruption detection --------------------

    #[test]
    fn new_file_has_valid_header_on_disk() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("init.mqlite");

        Client::open(&db_path).expect("open new database");

        let file_size = std::fs::metadata(&db_path).expect("metadata").len();
        assert!(
            file_size >= HEADER_PAGE_SIZE as u64,
            "header must be written: file size is {file_size} bytes"
        );

        let mut buf = [0u8; HEADER_PAGE_SIZE];
        let mut f = std::fs::File::open(&db_path).expect("open file");
        use std::io::Read;
        f.read_exact(&mut buf).expect("read header");
        let header = FileHeader::from_bytes(&buf).expect("parse header");
        header.validate().expect("validate header");
    }

    #[test]
    fn open_corrupt_file_returns_corrupt_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("corrupt.mqlite");

        let garbage = vec![0xDE_u8; HEADER_PAGE_SIZE];
        fs::write(&db_path, &garbage).expect("write garbage");

        let result = Client::open(&db_path);
        assert!(result.is_err(), "expected error opening corrupt file");
        assert!(
            matches!(result.err().unwrap(), Error::CorruptDatabase { .. }),
            "expected CorruptDatabase"
        );
    }

    #[test]
    fn open_bad_magic_returns_corrupt_database_with_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("bad_magic.mqlite");

        let good_header = FileHeader::new_now();
        let mut bytes = good_header.to_bytes();
        bytes[0] = b'X';
        let checksum = FileHeader::compute_checksum(bytes[..60].try_into().expect("60 bytes"));
        bytes[60..64].copy_from_slice(&checksum.to_le_bytes());
        fs::write(&db_path, &bytes).expect("write bad-magic file");

        let result = Client::open(&db_path);
        match result.err().expect("expected an error") {
            Error::CorruptDatabase { path, .. } => {
                assert_eq!(path, db_path, "path must be attached to the error");
            }
            other => panic!("expected CorruptDatabase, got: {:?}", other),
        }
    }

    #[test]
    fn open_truncated_file_returns_corrupt_database() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("truncated.mqlite");

        fs::write(&db_path, b"MQLT").expect("write truncated file");

        let result = Client::open(&db_path);
        assert!(
            matches!(
                result.err().expect("expected error"),
                Error::CorruptDatabase { .. }
            ),
            "expected CorruptDatabase for truncated file"
        );
    }

    #[test]
    fn reopen_after_close_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("reopen.mqlite");

        let client = Client::open(&db_path).expect("first open");
        client.close().ok();

        let _c2 = Client::open(&db_path).expect("second open after close");
    }

    // ---- Drop behavior -----------------------------------------------------

    #[test]
    fn drop_releases_file_lock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("drop_lock.mqlite");

        {
            let _client = Client::open(&db_path).expect("first open");
        }

        let _c2 = Client::open(&db_path).expect("reopen after drop");
    }

    #[test]
    fn drop_does_not_corrupt_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("drop_intact.mqlite");

        Client::open(&db_path).expect("open");

        let mut buf = [0u8; HEADER_PAGE_SIZE];
        let mut f = std::fs::File::open(&db_path).expect("open file");
        use std::io::Read;
        f.read_exact(&mut buf).expect("read header");
        let header = FileHeader::from_bytes(&buf).expect("parse header after drop");
        header.validate().expect("validate header after drop");
    }

    #[test]
    fn in_memory_creates_no_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_count_before = fs::read_dir(dir.path()).expect("read dir").count();

        {
            let _client = Client::open_in_memory().expect("in-memory open");
        }

        let file_count_after = fs::read_dir(dir.path()).expect("read dir").count();

        assert_eq!(
            file_count_before, file_count_after,
            "in-memory client must not create files"
        );
    }

    #[test]
    fn clone_keeps_inner_alive_after_original_drop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db_path = dir.path().join("clone.mqlite");

        let c1 = Client::open(&db_path).expect("open");
        let c2 = c1.clone();
        drop(c1);

        let db = c2.database("test");
        let _: Vec<String> = db.list_collection_names().unwrap_or_default();
    }

    // ---- database() API -------------------------------------------------------

    #[test]
    fn database_returns_handle_with_correct_name() {
        let client = Client::open_in_memory().expect("open");
        let db = client.database("myapp");
        assert_eq!(db.name(), "myapp");
    }

    #[test]
    fn multiple_databases_are_independent() {
        let client = Client::open_in_memory().expect("open");
        use bson::doc;
        use serde::{Deserialize, Serialize};
        #[derive(Serialize, Deserialize, Debug)]
        struct Item {
            x: i32,
        }

        let db_a = client.database("alpha");
        let db_b = client.database("beta");

        let col_a = db_a.collection::<Item>("things");
        let col_b = db_b.collection::<Item>("things");

        col_a.insert_one(&Item { x: 1 }).expect("insert into alpha");
        col_b.insert_one(&Item { x: 2 }).expect("insert into beta");

        // alpha.things has x=1, beta.things has x=2
        let a_doc = col_a.find_one(doc! {}).expect("find_one alpha").unwrap();
        let b_doc = col_b.find_one(doc! {}).expect("find_one beta").unwrap();

        assert_eq!(a_doc.x, 1, "alpha collection should have x=1");
        assert_eq!(b_doc.x, 2, "beta collection should have x=2");
    }

    // -----------------------------------------------------------------------
    // R1.6: SWMR — concurrent reader tests
    // -----------------------------------------------------------------------

    /// Verify that concurrent reads via the public `Client` API do not block
    /// each other: multiple reader threads run simultaneously without the
    /// writer_lock being a bottleneck (reads don't acquire writer_lock at all).
    #[test]
    fn swmr_concurrent_reads_via_client_do_not_deadlock() {
        use bson::doc;
        use std::sync::Arc;
        use std::thread;

        let client = Arc::new(Client::open_in_memory().expect("open"));
        let db = client.database("test");
        let col = db.collection::<bson::Document>("data");

        // Seed data.
        for i in 0..50i32 {
            col.insert_one(&doc! { "v": i }).expect("insert");
        }

        // 16 concurrent readers.
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let c = Arc::clone(&client);
                thread::spawn(move || {
                    let db = c.database("test");
                    let col = db.collection::<bson::Document>("data");
                    let docs: Vec<_> = col
                        .find(doc! {})
                        .expect("find")
                        .filter_map(|r| r.ok())
                        .collect();
                    assert_eq!(docs.len(), 50, "all 50 docs must be visible");
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader panicked");
        }
    }

    /// Verify that concurrent writes via Client all eventually succeed:
    /// the `acquire_writer_lock` spin-loop serialises them.
    #[test]
    fn swmr_concurrent_writes_via_client_all_succeed() {
        use bson::doc;
        use std::sync::Arc;
        use std::thread;

        let client = Arc::new(Client::open_in_memory().expect("open"));

        // 8 writer threads, each inserts 10 docs.
        let handles: Vec<_> = (0..8u32)
            .map(|w| {
                let c = Arc::clone(&client);
                thread::spawn(move || {
                    let col = c.database("test").collection::<bson::Document>("data");
                    for j in 0..10u32 {
                        col.insert_one(&doc! { "w": w as i32, "j": j as i32 })
                            .expect("insert");
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("writer panicked");
        }

        let count = client
            .database("test")
            .collection::<bson::Document>("data")
            .count_documents(bson::doc! {})
            .expect("count");
        assert_eq!(count, 80, "all 80 documents from 8 writers must be present");
    }

    // -----------------------------------------------------------------------
    // R4.4: Database::backup — consistent hot copy
    // -----------------------------------------------------------------------

    /// Basic hot backup: insert data, backup, reopen the copy, verify data.
    #[test]
    fn backup_produces_consistent_copy() {
        use bson::doc;

        let dir = tempfile::tempdir().expect("tempdir");
        let src_path = dir.path().join("src.mqlite");
        let dst_path = dir.path().join("dst.mqlite");

        // Seed the source database.
        {
            let client = Client::open(&src_path).expect("open source");
            let col = client
                .database("mydb")
                .collection::<bson::Document>("items");
            for i in 0..100i32 {
                col.insert_one(&doc! { "n": i }).expect("insert");
            }
            // Hot backup while the database is open.
            client.backup(&dst_path).expect("backup");
        }

        // Reopen the backup and verify the document count.
        {
            let client = Client::open(&dst_path).expect("open backup");
            let count = client
                .database("mydb")
                .collection::<bson::Document>("items")
                .count_documents(doc! {})
                .expect("count");
            assert_eq!(count, 100, "backup must contain all 100 documents");
        }
    }

    /// backup() on an in-memory database must return an error.
    #[test]
    fn backup_in_memory_returns_error() {
        let client = Client::open_in_memory().expect("open");
        let dir = tempfile::tempdir().expect("tempdir");
        let dst = dir.path().join("dst.mqlite");
        let result = client.backup(&dst);
        assert!(
            result.is_err(),
            "backup of in-memory database must fail, got: {:?}",
            result
        );
        let err = result.unwrap_err();
        assert!(
            matches!(err, Error::Internal(_)),
            "expected Internal error, got: {:?}",
            err
        );
    }

    /// backup() to the same path as the source must return an error.
    #[test]
    fn backup_to_self_returns_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("db.mqlite");
        let client = Client::open(&path).expect("open");
        let result = client.backup(&path);
        assert!(
            result.is_err(),
            "backup to self must fail, got: {:?}",
            result
        );
    }

    /// backup() to a symlink destination must be rejected.
    #[test]
    #[cfg(unix)]
    fn backup_symlink_dest_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let src_path = dir.path().join("src.mqlite");
        let real_dst = dir.path().join("real.mqlite");
        let sym_dst = dir.path().join("link.mqlite");

        fs::write(&real_dst, b"").expect("create real dst");
        std::os::unix::fs::symlink(&real_dst, &sym_dst).expect("create symlink");

        let client = Client::open(&src_path).expect("open source");
        let result = client.backup(&sym_dst);
        assert!(
            matches!(result, Err(Error::SymlinkRejected { .. })),
            "expected SymlinkRejected, got: {:?}",
            result
        );
    }

    /// backup() overwrites an existing destination file.
    #[test]
    fn backup_overwrites_existing_dest() {
        use bson::doc;

        let dir = tempfile::tempdir().expect("tempdir");
        let src_path = dir.path().join("src.mqlite");
        let dst_path = dir.path().join("dst.mqlite");

        // Seed source.
        let client = Client::open(&src_path).expect("open source");
        let col = client
            .database("db")
            .collection::<bson::Document>("col");
        col.insert_one(&doc! { "x": 1i32 }).expect("insert");

        // First backup.
        client.backup(&dst_path).expect("first backup");
        // Second backup — must overwrite the first without error.
        col.insert_one(&doc! { "x": 2i32 }).expect("insert again");
        client.backup(&dst_path).expect("second backup");

        // Verify both docs are in the second backup.
        let bkup = Client::open(&dst_path).expect("open backup");
        let count = bkup
            .database("db")
            .collection::<bson::Document>("col")
            .count_documents(doc! {})
            .expect("count");
        assert_eq!(count, 2, "second backup must contain both documents");
    }

    /// backup() destination file must have 0600 permissions on Unix.
    #[test]
    #[cfg(unix)]
    fn backup_dest_has_restricted_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let src_path = dir.path().join("src.mqlite");
        let dst_path = dir.path().join("dst.mqlite");

        let client = Client::open(&src_path).expect("open source");
        client.backup(&dst_path).expect("backup");

        let meta = fs::metadata(&dst_path).expect("metadata");
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "backup file must have mode 0600, got {:o}",
            mode
        );
    }
}
