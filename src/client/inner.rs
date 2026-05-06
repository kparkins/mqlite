//! `ClientInner` — shared state held behind `Arc` by every client-side handle
//! (`Client`, `Database`, `Collection<T>`, and the wire-protocol server).

use std::{path::PathBuf, sync::Arc};

use crate::storage::{engine::StorageEngine, lock::AnyFileLock};

/// Internal shared state for a [`super::Client`].
///
/// Wrapped in `Arc` and shared across [`super::Client`] clones,
/// [`super::Database`] handles, and
/// [`super::Collection`] handles.
///
/// ## Locking
///
/// Cross-process locking is provided by `file_lock` (OS advisory).
/// In-process writer serialization is handled by the engine's per-namespace
/// lanes: two writers on different namespaces overlap; same-namespace writers
/// serialize on an engine-owned lane mutex. Busy-timeout + busy-handler
/// configuration is plumbed into `PagedEngine::new_buffered_with_busy`.
///
/// ## Storage engine
///
/// `engine` is a `Box<dyn StorageEngine>` — the concrete type is always
/// [`crate::storage::paged_engine::PagedEngine`], but `ClientInner` never
/// knows this.
pub(crate) struct ClientInner {
    /// Path to the database file.
    pub path: Option<PathBuf>,
    /// OS advisory file lock.
    ///
    /// Stored as `Arc` so the same fd can be shared with the `FilePageSource`
    /// backing the buffer pool.
    pub(super) file_lock: Arc<AnyFileLock>,
    /// Storage engine.  All CRUD operations are dispatched through this trait.
    pub(crate) engine: Box<dyn StorageEngine>,
}

impl ClientInner {
    pub(super) fn new(
        path: Option<PathBuf>,
        file_lock: Arc<AnyFileLock>,
        engine: Box<dyn StorageEngine>,
    ) -> Self {
        ClientInner {
            path,
            file_lock,
            engine,
        }
    }
}
