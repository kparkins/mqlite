//! `PagedEngine` — Phase 1 `StorageEngine` implementation.
//!
//! ## Current status: STUB
//!
//! `PagedEngine` is the Phase 1 concrete implementation of
//! [`crate::storage_engine::StorageEngine`].  It is currently a **stub**
//! backed by the in-memory `Vec<Document>` engine ([`crate::engine::EngineState`]).
//!
//! Subsequent beads wire in the real storage stack:
//!
//! | Bead | What it adds |
//! |------|-------------|
//! | hq-bhon (R1.1) | Buffer pool + page allocator + file I/O |
//! | R1.2 | B+ tree document storage |
//! | R1.3 | Catalog |
//! | R1.4 | Secondary indexes |
//! | R1.5 | WAL integration |
//! | R1.6 | SWMR concurrency |
//!
//! When each layer is ready, the corresponding stub implementation below
//! is replaced.  The `StorageEngine` trait signature does **not** change.
//!
//! ## Thread safety
//!
//! All mutable state is held behind a `Mutex<EngineState>`.  Every trait
//! method acquires the lock, performs the operation, and releases it.
//! This serialises all access (reads and writes) through a single lock,
//! providing correct (if not optimal) behaviour for Phase 1.  The SWMR
//! upgrade (R1.6) will replace the global lock with snapshot isolation.

use std::sync::Mutex;

use bson::{Bson, Document};

use crate::{
    engine::EngineState,
    error::Result,
    index::{IndexInfo, IndexModel},
    options::{
        FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
        UpdateOptions,
    },
    results::{DeleteResult, UpdateResult},
    storage_engine::StorageEngine,
};

// ---------------------------------------------------------------------------
// PagedEngine
// ---------------------------------------------------------------------------

/// Phase 1 storage engine (stub).
///
/// Currently delegates all operations to [`EngineState`] (an in-memory
/// `Vec<Document>` engine) via a `Mutex`.  This is replaced by a real
/// B+ tree / buffer pool / WAL implementation in Phase 1.x beads.
pub(crate) struct PagedEngine {
    inner: Mutex<EngineState>,
}

impl PagedEngine {
    /// Create a new, empty `PagedEngine`.
    pub(crate) fn new() -> Self {
        PagedEngine {
            inner: Mutex::new(EngineState::new()),
        }
    }

    /// Restore a `PagedEngine` from a previously-serialised [`EngineState`].
    ///
    /// Used by `Client::open_with_options` to replay the BSON-blob snapshot
    /// from disk.  This load path is temporary — Phase 1.x will replace it
    /// with WAL replay + page file scanning.
    pub(crate) fn from_state(state: EngineState) -> Self {
        PagedEngine {
            inner: Mutex::new(state),
        }
    }
}

// ---------------------------------------------------------------------------
// StorageEngine implementation
// ---------------------------------------------------------------------------

impl StorageEngine for PagedEngine {
    // --- CRUD ---

    fn insert(&self, ns: &str, doc: Document) -> Result<Bson> {
        self.inner.lock().unwrap().insert_doc(ns, doc)
    }

    fn find(&self, ns: &str, filter: &Document, opts: &FindOptions) -> Result<Vec<Document>> {
        // `EngineState::find` returns a `Cursor<Document>`.  Collect it into
        // a `Vec<Document>` for the trait's simpler return type.
        let cursor = self
            .inner
            .lock()
            .unwrap()
            .find::<Document>(ns, filter.clone(), opts.clone())?;
        cursor.collect::<Result<Vec<_>>>()
    }

    fn find_one(&self, ns: &str, filter: &Document) -> Result<Option<Document>> {
        self.inner
            .lock()
            .unwrap()
            .find_one::<Document>(ns, filter.clone())
    }

    fn update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &UpdateOptions,
        many: bool,
    ) -> Result<UpdateResult> {
        let mut inner = self.inner.lock().unwrap();
        if many {
            inner.update_many(ns, filter.clone(), update.clone(), opts.clone())
        } else {
            inner.update_one(ns, filter.clone(), update.clone(), opts.clone())
        }
    }

    fn delete(&self, ns: &str, filter: &Document, many: bool) -> Result<DeleteResult> {
        let mut inner = self.inner.lock().unwrap();
        if many {
            inner.delete_many(ns, filter.clone())
        } else {
            inner.delete_one(ns, filter.clone())
        }
    }

    fn count(&self, ns: &str, filter: &Document) -> Result<u64> {
        self.inner
            .lock()
            .unwrap()
            .count_documents(ns, filter.clone())
    }

    // --- Atomic find-and-modify ---

    fn find_one_and_update_doc(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &FindOneAndUpdateOptions,
    ) -> Result<Option<Document>> {
        self.inner
            .lock()
            .unwrap()
            .find_one_and_update_with_options::<Document>(
                ns,
                filter.clone(),
                update.clone(),
                opts.clone(),
            )
    }

    fn find_one_and_delete_doc(
        &self,
        ns: &str,
        filter: &Document,
        opts: &FindOneAndDeleteOptions,
    ) -> Result<Option<Document>> {
        self.inner
            .lock()
            .unwrap()
            .find_one_and_delete_with_options::<Document>(ns, filter.clone(), opts.clone())
    }

    fn find_one_and_replace_doc(
        &self,
        ns: &str,
        filter: &Document,
        replacement: &Document,
        opts: &FindOneAndReplaceOptions,
    ) -> Result<Option<Document>> {
        self.inner
            .lock()
            .unwrap()
            .find_one_and_replace_with_options::<Document>(
                ns,
                filter.clone(),
                replacement,
                opts.clone(),
            )
    }

    // --- Index management ---

    fn create_index(&self, ns: &str, model: &IndexModel) -> Result<String> {
        self.inner
            .lock()
            .unwrap()
            .create_index(ns, model.clone())
    }

    fn drop_index(&self, ns: &str, name: &str) -> Result<()> {
        self.inner.lock().unwrap().drop_index(ns, name)
    }

    fn list_indexes(&self, ns: &str) -> Result<Vec<IndexInfo>> {
        self.inner.lock().unwrap().list_indexes(ns)
    }

    // --- Namespace management ---

    fn create_namespace(&self, ns: &str) -> Result<()> {
        self.inner.lock().unwrap().create_collection(ns)
    }

    fn drop_namespace(&self, ns: &str) -> Result<()> {
        self.inner.lock().unwrap().drop_collection(ns)
    }

    fn list_namespaces(&self) -> Result<Vec<String>> {
        self.inner.lock().unwrap().list_collection_names()
    }

    // --- Lifecycle ---

    fn checkpoint(&self) -> Result<()> {
        // Stub: no-op.  The real checkpoint (flush dirty pages to disk) is
        // implemented in Phase 1.5 (WAL integration).
        //
        // `Client::checkpoint` handles the BSON-blob snapshot write directly
        // via the file lock — that path does not go through the engine trait.
        Ok(())
    }

    fn close(&self) -> Result<()> {
        // Stub: no-op.  The real close (checkpoint + WAL truncation + SHM
        // deletion) is implemented in Phase 1.5.
        Ok(())
    }

    fn snapshot_bytes(&self) -> Result<Option<Vec<u8>>> {
        // Delegate to `EngineState::to_bson_bytes` for the legacy snapshot path.
        // This is called by `ClientInner::checkpoint` to persist state to disk.
        let bytes = self.inner.lock().unwrap().to_bson_bytes()?;
        Ok(Some(bytes))
    }
}
