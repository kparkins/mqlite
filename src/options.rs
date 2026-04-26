use bson::Document;
use std::sync::Arc;
use std::time::Duration;

/// Durability mode controls when data is fsynced to disk.
#[derive(Debug, Clone, PartialEq)]
pub enum DurabilityMode {
    /// fsync after every commit. Safest, slowest.
    FullSync,
    /// Flush WAL at a configurable interval. Fast, small loss window.
    Interval(Duration),
    /// No durability guarantees (in-memory behavior for file-backed DBs).
    None,
}

impl Default for DurabilityMode {
    fn default() -> Self {
        DurabilityMode::Interval(Duration::from_millis(100))
    }
}

/// A boxed busy-handler callback.
///
/// Wrapped in `Arc` so that `OpenOptions` can implement `Clone`.
#[derive(Clone)]
pub struct BusyHandler(pub Arc<dyn Fn(u32) -> bool + Send + Sync>);

impl std::fmt::Debug for BusyHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BusyHandler").field(&"<fn>").finish()
    }
}

/// Configuration for [`crate::Database::open_with_options`].
///
/// Use the builder methods for ergonomic construction:
/// ```no_run
/// use mqlite::{OpenOptions, DurabilityMode};
/// use std::time::Duration;
///
/// let opts = OpenOptions::new()
///     .buffer_pool_size(64 * 1024 * 1024)
///     .durability(DurabilityMode::FullSync)
///     .busy_timeout(Duration::from_secs(5));
/// ```
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct OpenOptions {
    /// Buffer pool size in bytes. Default: 64MB.
    pub(crate) buffer_pool_size: usize,
    /// Durability mode. Default: `Interval(100ms)`.
    pub(crate) durability: DurabilityMode,
    /// Journal auto-checkpoint threshold in pages. Default: 1000.
    pub(crate) journal_auto_checkpoint: u32,
    /// Journal max size in bytes before forced checkpoint. Default: 100MB.
    pub(crate) journal_max_size: u64,
    /// Timeout for acquiring the writer lock. Default: 5 seconds.
    /// Set to `Duration::ZERO` for immediate failure (SQLite-style SQLITE_BUSY).
    pub(crate) busy_timeout: Duration,
    /// Optional busy handler callback. Called when the writer lock is contended.
    /// Return `true` to retry, `false` to fail with [`crate::Error::WriterBusy`].
    pub(crate) busy_handler: Option<BusyHandler>,
    /// Open in read-only mode. WAL replay is skipped.
    /// Default: false.
    pub(crate) read_only: bool,
    /// Create the file if it doesn't exist. Default: true.
    pub(crate) create_if_missing: bool,
    /// Maximum concurrent readers. Default: 64.
    pub(crate) max_readers: u32,
}

impl Default for OpenOptions {
    fn default() -> Self {
        OpenOptions {
            buffer_pool_size: 64 * 1024 * 1024, // 64MB
            durability: DurabilityMode::default(),
            journal_auto_checkpoint: 1000,
            journal_max_size: 100 * 1024 * 1024, // 100MB
            busy_timeout: Duration::from_secs(5),
            busy_handler: None,
            read_only: false,
            create_if_missing: true,
            max_readers: 64,
        }
    }
}

impl OpenOptions {
    /// Create a new `OpenOptions` with sensible defaults.
    #[must_use]
    pub fn new() -> Self {
        OpenOptions::default()
    }

    /// Set the buffer pool size in bytes.
    #[must_use]
    pub fn buffer_pool_size(mut self, bytes: usize) -> Self {
        self.buffer_pool_size = bytes;
        self
    }

    /// Set the durability mode.
    #[must_use]
    pub fn durability(mut self, mode: DurabilityMode) -> Self {
        self.durability = mode;
        self
    }

    /// Set the journal auto-checkpoint threshold in pages.
    #[must_use]
    pub fn journal_auto_checkpoint(mut self, pages: u32) -> Self {
        self.journal_auto_checkpoint = pages;
        self
    }

    /// Set the maximum journal size in bytes before a forced checkpoint.
    #[must_use]
    pub fn journal_max_size(mut self, bytes: u64) -> Self {
        self.journal_max_size = bytes;
        self
    }

    /// Set the timeout for acquiring the writer lock.
    /// Use `Duration::ZERO` for immediate failure on contention.
    #[must_use]
    pub fn busy_timeout(mut self, duration: Duration) -> Self {
        self.busy_timeout = duration;
        self
    }

    /// Set a callback invoked when the writer lock is contended.
    /// `attempts` is the number of retries so far.
    /// Return `true` to retry, `false` to fail with [`crate::Error::WriterBusy`].
    #[must_use]
    pub fn busy_handler(mut self, handler: impl Fn(u32) -> bool + Send + Sync + 'static) -> Self {
        self.busy_handler = Some(BusyHandler(Arc::new(handler)));
        self
    }

    /// Open in read-only mode. WAL replay is skipped.
    #[must_use]
    pub fn read_only(mut self, val: bool) -> Self {
        self.read_only = val;
        self
    }

    /// Create the file if it doesn't exist.
    #[must_use]
    pub fn create_if_missing(mut self, val: bool) -> Self {
        self.create_if_missing = val;
        self
    }

    /// Set the maximum number of concurrent readers.
    #[must_use]
    pub fn max_readers(mut self, count: u32) -> Self {
        self.max_readers = count;
        self
    }
}

/// Options for `find` and `find_one` operations.
/// All fields are optional — omit to use defaults.
#[derive(Debug, Clone, Default)]
pub(crate) struct FindOptions {
    /// Sort order. Documents are returned in insertion order if not specified.
    pub sort: Option<Document>,
    /// Maximum number of documents to return.
    pub limit: Option<i64>,
    /// Number of documents to skip before returning results.
    pub skip: Option<u64>,
    /// Projection — fields to include or exclude.
    pub projection: Option<Document>,
    /// Number of documents to fetch per internal batch. Default: 101.
    pub batch_size: Option<u32>,
}

impl FindOptions {
    #[must_use]
    pub fn new() -> Self {
        FindOptions::default()
    }
}

/// Options for `update_one` and `update_many` operations.
#[derive(Debug, Clone, Default)]
pub(crate) struct UpdateOptions {
    /// If true, insert a new document when no document matches the filter.
    pub upsert: bool,
}

/// Options for `insert_many` operations.
#[derive(Debug, Clone)]
pub(crate) struct InsertManyOptions {
    /// If `true` (default), stop at the first error. If `false`, attempt all documents
    /// and collect all errors.
    pub ordered: bool,
}

impl Default for InsertManyOptions {
    fn default() -> Self {
        InsertManyOptions { ordered: true }
    }
}

/// Options for `create_index` operations.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct IndexOptions {
    /// If true, the index enforces a unique constraint.
    pub(crate) unique: bool,
    /// Custom name for the index.
    pub(crate) name: Option<String>,
    /// If true, only index documents where the key field exists.
    pub(crate) sparse: bool,
}

impl IndexOptions {
    /// Create default `IndexOptions`.
    #[must_use]
    pub fn new() -> Self {
        IndexOptions::default()
    }

    /// Set whether the index enforces a unique constraint.
    #[must_use]
    pub fn unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    /// Set a custom name for the index.
    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set whether to only index documents where the key field exists.
    #[must_use]
    pub fn sparse(mut self, sparse: bool) -> Self {
        self.sparse = sparse;
        self
    }
}

// ---------------------------------------------------------------------------
// FindOneAnd* option types
// ---------------------------------------------------------------------------

/// Controls which version of a document is returned by `find_one_and_update` /
/// `find_one_and_replace`.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ReturnDocument {
    /// Return the document **before** the modification (default).
    #[default]
    Before,
    /// Return the document **after** the modification.
    After,
}

/// Options for `find_one_and_update` operations.
#[derive(Debug, Clone, Default)]
pub(crate) struct FindOneAndUpdateOptions {
    /// Which version of the document to return. Default: [`ReturnDocument::Before`].
    pub return_document: ReturnDocument,
    /// If `true`, insert a new document when no document matches the filter. Default: false.
    pub upsert: bool,
    /// Sort order — determines which document to operate on when multiple match.
    pub sort: Option<Document>,
}

/// Options for `find_one_and_delete` operations.
#[derive(Debug, Clone, Default)]
pub(crate) struct FindOneAndDeleteOptions {
    /// Sort order — determines which document to delete when multiple match.
    pub sort: Option<Document>,
}

/// Options for `find_one_and_replace` operations.
#[derive(Debug, Clone, Default)]
pub(crate) struct FindOneAndReplaceOptions {
    /// Which version of the document to return. Default: [`ReturnDocument::Before`].
    pub return_document: ReturnDocument,
    /// If `true`, insert a new document when no document matches the filter. Default: false.
    pub upsert: bool,
    /// Sort order — determines which document to replace when multiple match.
    pub sort: Option<Document>,
}
