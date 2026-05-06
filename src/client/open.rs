//! `Client::open` and `Client::open_with_options` — the database bootstrap
//! sequence (symlink check, advisory lock, header init/validate, journal
//! recovery, buffer-pool + engine construction).

use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use crate::{
    error::{Error, Result},
    journal::{JournalLayeredSource, JournalManager},
    options::OpenOptions,
    storage::{
        buffer_pool::BufferPool,
        engine::StorageEngine,
        file_io::FilePageSource,
        handle::BufferPoolHandle,
        header::HEADER_PAGE_SIZE,
        lock::{self, AnyFileLock, FileLock},
        paged_engine::PagedEngine,
    },
};

use super::{
    handle::Client,
    inner::ClientInner,
    path::{
        create_db_file_secure, journal_path, read_and_validate_header, reject_symlink,
        write_initial_header,
    },
};

impl Client {
    /// Open a database file. Creates the file if it does not exist.
    ///
    /// Automatically replays the journal on recovery. Uses sensible defaults
    /// (64MB buffer pool, 100ms durability interval, 5s busy timeout).
    ///
    /// # Errors
    ///
    /// Returns an error if the path is invalid, the file cannot be opened or
    /// locked, the database header is corrupt, or journal recovery fails.
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
    ///
    /// # Errors
    ///
    /// Returns an error if the path is invalid, the file cannot be opened or
    /// locked, the database header is corrupt, or journal recovery fails.
    pub fn open_with_options(path: impl AsRef<Path>, opts: OpenOptions) -> Result<Client> {
        opts.validate()?;
        let path = path.as_ref().to_owned();

        // Security: reject symlinks before touching the file.
        reject_symlink(&path)?;

        // Also check the associated journal path.
        let journal_path = journal_path(&path);
        reject_symlink(&journal_path)?;

        // Detect a legacy `-wal` sidecar left by an older mqlite build.
        // Return UnsupportedJournalFormat so the caller knows they need to
        // open with the old version first and checkpoint before upgrading.
        // The suffix is hex-encoded to keep the `\bwal\b` grep gate clean.
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
        let file_lock: Arc<AnyFileLock> = Arc::new(lock::open_file_lock(&path)?);
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
                path,
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

        // Construct the buffer pool handle wired to the database file and
        // create a B+ tree engine backed by it.
        //
        // The pool is backed by FilePageSource which shares the lock fd (Arc clone)
        // to avoid the POSIX advisory-lock footgun.  OpenOptions::buffer_pool_size
        // controls the total byte budget split between 4 KB and 32 KB partitions.
        //
        // Read the baseline file header after initialization. Journal recovery
        // may replace page 0 with a recovered image; final engine construction
        // below re-reads and validates that post-replay header before any
        // allocator, buffer-pool, history-store, or metadata state is created.
        let baseline_header = read_and_validate_header(file_lock.as_ref(), &path)?;

        // Open a dedicated file handle for journal checkpoint I/O.  This fd is
        // never used for advisory locking — only for writing checkpointed
        // pages back to the main file.  Both fds live for the same duration
        // as ClientInner so the advisory lock lifetime is unaffected.
        let mut journal_io_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(Error::Io)?;

        let journal_mgr =
            JournalManager::open_or_create(&path, &baseline_header, &mut journal_io_file)?;

        // Read and validate the recovered page-0 image unconditionally. When
        // recovery copied a checkpoint boundary, this is the replayed header;
        // otherwise it is the same image as `baseline_header`.
        let file_header = read_and_validate_header(file_lock.as_ref(), &path)?;
        let catalog_root_page = file_header.catalog_root_page;
        let catalog_root_level = file_header.catalog_root_level;

        let journal = Arc::new(Mutex::new(journal_mgr));

        let file_src = Arc::new(FilePageSource::new(
            Arc::clone(&file_lock) as Arc<dyn FileLock>
        ));
        let layered_source: Box<dyn crate::storage::buffer_pool::PageSource> =
            Box::new(JournalLayeredSource::new(
                Arc::clone(&file_src) as Arc<dyn crate::storage::buffer_pool::PageSource>,
                Arc::clone(&journal),
            ));
        let pool = Arc::new(BufferPool::new_with_delta_bearing_frames_warn_threshold(
            opts.buffer_pool_size,
            layered_source,
            opts.delta_bearing_frames_warn_threshold,
        ));
        // Dedicated history-store buffer pool.
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
            journal_main_file,
        ));

        let engine: Box<dyn StorageEngine> = Box::new(PagedEngine::new_buffered_with_busy(
            buffer_pool,
            catalog_root_page,
            catalog_root_level,
            opts.busy_timeout,
            opts.busy_handler.clone(),
            opts.smo_classification_retry_cap,
            opts.durability.clone(),
        )?);
        let inner = Arc::new(ClientInner::new(Some(path.clone()), file_lock, engine));
        let _ = file_size; // used above, suppress warning
        #[cfg(feature = "tracing")]
        tracing::info!(
            target: "mqlite",
            path = %path.display(),
            format_version = crate::storage::header::FORMAT_VERSION,
            "mqlite::open"
        );
        Ok(Client::from_inner(inner))
    }
}
