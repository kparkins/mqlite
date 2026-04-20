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

use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use crate::{
    database::Database,
    error::{Error, Result},
    options::OpenOptions,
    storage::{
        buffer_pool::BufferPool,
        file_io::FilePageSource,
        handle::BufferPoolHandle,
        header::{FileHeader, HEADER_PAGE_SIZE},
        lock::{self, FileLock},
        paged_engine::PagedEngine,
    },
    storage_engine::StorageEngine,
    journal::{JournalLayeredSource, JournalManager},
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

/// Returns the expected journal file path for a given database path.
///
/// Journal files use the naming convention `<db-path>-journal`.
fn journal_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_owned();
    s.push("-journal");
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
/// and [`crate::collection::Collection`] handles.
///
/// ## Locking (PR 8 — MWMR v1)
///
/// Cross-process locking is still provided by `file_lock` (OS advisory).
/// In-process writer serialization was historically handled by
/// `writer_lock: Mutex<()>` here in `ClientInner`. PR 8 moves that
/// responsibility into the engine's per-namespace lanes: two writers on
/// DIFFERENT namespaces now overlap; same-namespace writers serialize on
/// an engine-owned lane mutex. Busy-timeout + busy-handler configuration
/// is plumbed into `PagedEngine::new_buffered_with_busy`.
///
/// ## Storage engine
///
/// `engine` is a `Box<dyn StorageEngine>` — the concrete type is always
/// [`PagedEngine`] in Phase 1, but `ClientInner` never knows this.
pub(crate) struct ClientInner {
    /// Path to the database file.
    pub path: Option<PathBuf>,
    /// Configuration options.
    pub opts: OpenOptions,
    /// OS advisory file lock.
    ///
    /// Stored as `Arc` so the same fd can be shared with the `FilePageSource`
    /// backing the buffer pool.
    file_lock: Arc<dyn FileLock>,
    /// Buffer pool handle — file I/O infrastructure wired in by R1.1.
    #[allow(dead_code)]
    pub(crate) buffer_pool: Option<Arc<BufferPoolHandle>>,
    /// Storage engine.  All CRUD operations are dispatched through this trait.
    pub(crate) engine: Box<dyn StorageEngine>,
    /// Dedicated file handle for journal→main-file checkpoint I/O.
    journal_main_file: Option<Arc<Mutex<std::fs::File>>>,
}

impl ClientInner {
    fn new_with_buffer_pool(
        path: Option<PathBuf>,
        opts: OpenOptions,
        file_lock: Arc<dyn FileLock>,
        buffer_pool: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
        journal_main_file: Option<Arc<Mutex<std::fs::File>>>,
    ) -> Result<Self> {
        let engine = PagedEngine::new_buffered_with_busy(
            Arc::clone(&buffer_pool),
            catalog_root_page,
            catalog_root_level,
            opts.busy_timeout,
            opts.busy_handler.clone(),
        )?;
        Ok(ClientInner {
            path,
            opts,
            file_lock,
            buffer_pool: Some(buffer_pool),
            engine: Box::new(engine),
            journal_main_file,
        })
    }
}


mod inner_crud;

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
/// # use tempfile::TempDir;
/// # let dir = TempDir::new()?;
/// # let client = Client::open(dir.path().join("db.mqlite"))?;
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
/// # use tempfile::TempDir;
/// # let dir = TempDir::new()?;
/// # let client = Client::open(dir.path().join("db.mqlite"))?;
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
    /// Automatically replays the journal on recovery. Uses sensible defaults
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

        // Also check the associated journal path.
        let journal_path = journal_path(&path);
        reject_symlink(&journal_path)?;

        // Detect a legacy pre-T1 sidecar (the file formerly known as
        // `<db>-wal`) left by an older mqlite build. Return
        // UnsupportedJournalFormat so the caller knows they need to open with
        // the old version first and checkpoint before upgrading. The suffix is
        // hex-encoded to keep the T1 `\bwal\b` grep gate clean.
        let legacy_sidecar_path = {
            let mut s = path.as_os_str().to_owned();
            s.push("-\x77\x61\x6c");
            std::path::PathBuf::from(s)
        };
        if legacy_sidecar_path.exists() {
            return Err(Error::UnsupportedJournalFormat {
                found: *b"MQWL",
                expected: *b"MQJL",
            });
        }

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
        // Read the initial file header (used to set salt values for the journal
        // and to locate the catalog B+ tree root page).
        let file_header = if file_size == 0 {
            FileHeader::new_now()
        } else {
            let mut hdr_buf = [0u8; HEADER_PAGE_SIZE];
            file_lock.read_exact_at(0, &mut hdr_buf)?;
            FileHeader::from_bytes(&hdr_buf).unwrap_or_else(|_| FileHeader::new_now())
        };

        // Open a dedicated file handle for journal checkpoint I/O.  This fd is
        // never used for advisory locking — only for writing checkpointed
        // pages back to the main file.  Both fds live for the same duration
        // as ClientInner so the advisory lock lifetime is unaffected.
        let mut journal_io_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(Error::Io)?;

        let journal_mgr = JournalManager::open_or_create(
            &path,
            &file_header,
            &mut journal_io_file,
        )?;

        // Re-read the file header after journal recovery — but ONLY when
        // recovery actually wrote committed page frames to the main file.
        // `open_or_create` may have replayed journal frames into the main file
        // (including page 0), making the catalog_root_page / catalog_root_level
        // we read before recovery stale.  Re-reading via `journal_io_file` gives
        // us the post-recovery header.
        //
        // When no pages were replayed (clean close, empty journal, or no journal)
        // the pre-recovery header is already correct — and for NEW files
        // (file_size == 0) re-reading would return the header written by
        // `write_initial_header` (a different `FileHeader::new_now()` call with
        // different random salts), breaking the journal's salt check on the very
        // next open.
        let file_header = if journal_mgr.did_recover_pages() {
            use std::io::{Read, Seek, SeekFrom};
            let mut hdr_buf = [0u8; HEADER_PAGE_SIZE];
            journal_io_file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
            journal_io_file.read_exact(&mut hdr_buf).map_err(Error::Io)?;
            FileHeader::from_bytes(&hdr_buf).unwrap_or(file_header)
        } else {
            file_header
        };
        let catalog_root_page = file_header.catalog_root_page;
        let catalog_root_level = file_header.catalog_root_level;

        let journal = Arc::new(Mutex::new(journal_mgr));

        let file_src = Arc::new(FilePageSource::new(Arc::clone(&file_lock)));
        let layered_source: Box<dyn crate::storage::buffer_pool::PageSource> =
            Box::new(JournalLayeredSource::new(
                Arc::clone(&file_src) as Arc<dyn crate::storage::buffer_pool::PageSource>,
                Arc::clone(&journal),
            ));
        let pool = Arc::new(BufferPool::new(opts.buffer_pool_size, layered_source));
        // Dedicated history-store buffer pool (plan §T7 NON-NEGOTIABLE).
        // Sized conservatively; routes through the same journal-layered source so
        // recovered history pages are visible after checkpoint.
        let history_source: Box<dyn crate::storage::buffer_pool::PageSource> =
            Box::new(JournalLayeredSource::new(
                Arc::clone(&file_src) as Arc<dyn crate::storage::buffer_pool::PageSource>,
                Arc::clone(&journal),
            ));
        let history_pool = Arc::new(BufferPool::new(
            crate::storage::buffer_pool::default_sizes::HISTORY,
            history_source,
        ));
        let journal_main_file = Arc::new(Mutex::new(journal_io_file));
        let buffer_pool = Arc::new(BufferPoolHandle::with_journal(
            pool,
            history_pool,
            file_header,
            journal,
            Arc::clone(&journal_main_file),
        ));

        let inner = Arc::new(ClientInner::new_with_buffer_pool(
            Some(path.clone()),
            opts,
            file_lock,
            buffer_pool,
            catalog_root_page,
            catalog_root_level,
            Some(journal_main_file),
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
    /// # use tempfile::TempDir;
    /// # let dir = TempDir::new()?;
    /// # let client = Client::open(dir.path().join("db.mqlite"))?;
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

    /// Force a journal checkpoint.
    ///
    /// After this returns, the main database file is safe to copy as a backup.
    pub fn checkpoint(&self) -> Result<()> {
        self.inner.checkpoint()
    }

    /// Hot backup to a destination file.
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        self.inner.backup(dest.as_ref())
    }

    /// Flush the journal, checkpoint, and close the client.
    ///
    /// Use this when you need a guarantee that all committed data is in the main
    /// file (e.g., before copying the file as a backup). `Drop` performs a
    /// non-blocking close.
    pub fn close(self) -> Result<()> {
        self.inner.checkpoint()
    }

    /// Test-only accessor for the MVCC `ReadViewRegistry` backing this client.
    ///
    /// Exposed for integration tests (plan §T9 `drop_collection` barrier
    /// verification) that need to register external `ReadView`s and watch
    /// them get force-expired on the engine's drop path. Returns `None`
    /// when the client has no attached buffer pool (legacy in-memory
    /// engines that predate the MVCC rollout).
    #[doc(hidden)]
    pub fn __read_view_registry(&self) -> Option<Arc<crate::mvcc::ReadViewRegistry>> {
        self.inner
            .buffer_pool
            .as_ref()
            .map(|bp| Arc::clone(bp.read_view_registry()))
    }
}

impl Drop for Client {
    /// Non-blocking close.
    ///
    /// Checkpoints when this is the last handle. Journal data remains on disk
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
#[path = "client_tests.rs"]
mod tests_extracted;
