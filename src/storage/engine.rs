//! `StorageEngine` trait — the stable contract between the public API layer and storage.
//!
//! [`ClientInner`] holds `Box<dyn StorageEngine>`.  All storage access goes through
//! this trait.  The concrete engine implementation can be swapped without touching
//! the public API layer (`Collection`, `Database`, `Client`).
//!
//! # Namespace format
//!
//! All `ns` (namespace) parameters are fully-qualified strings in the format
//! `"db.collection"` (e.g., `"myapp.users"`).  This mirrors the MongoDB wire
//! protocol's `$db` + collection name convention and supports multiple named
//! databases within a single mqlite file.
//!
//! # Thread safety
//!
//! Implementations must be `Send + Sync`.  Engines are shared across `Client`,
//! `Database`, and `Collection<T>` handles which may be used concurrently from
//! multiple threads.  Implementations handle their own synchronization (interior
//! mutability — typically a `Mutex<Inner>`).
//!
//! # Concrete implementation
//!
//! The concrete implementation is [`crate::storage::paged_engine::PagedEngine`],
//! backed by a B+ tree / buffer pool / WAL stack.

use bson::{Bson, Document};

#[cfg(any(test, feature = "test-hooks"))]
use super::paged_engine::test_accessors::WriteBodyEntryHookGuard;
#[cfg(any(test, feature = "test-hooks"))]
use super::phase0_probe::{Phase0ProbeCut, Phase0ProbeReport};
use crate::{
    error::Result,
    index::{IndexInfo, IndexModel},
    options::{
        FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
        UpdateOptions,
    },
    results::{DeleteResult, UpdateResult},
};
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::{atomic::AtomicBool, Arc};

/// The stable interface between the mqlite public API and storage.
///
/// All methods take `&self` — the implementation is expected to use interior
/// mutability for write operations.
///
/// ## Namespace format
///
/// The `ns` parameter is always `"db.collection"` (e.g., `"myapp.users"`).
///
/// ## Error handling
///
/// All methods return [`crate::error::Result`].  Engine-specific errors should
/// be wrapped in [`crate::error::Error::Internal`] unless a more specific
/// variant applies.
pub trait StorageEngine: Send + Sync {
    // -------------------------------------------------------------------------
    // CRUD
    // -------------------------------------------------------------------------

    /// Insert a single pre-serialised document into `ns`.
    ///
    /// The `doc` MUST already have an `_id` field set (the engine will generate
    /// one if it is missing, but callers should set it before calling to avoid
    /// the generation overhead and to get a predictable type).
    ///
    /// Returns the inserted `_id` as [`Bson`].
    fn insert(&self, ns: &str, doc: Document) -> Result<Bson>;

    /// Return all documents in `ns` that match `filter`, along with the
    /// executed query plan.
    ///
    /// Applies sort, skip, limit, and projection from `opts` if set.
    /// Returns an empty `Vec` when the namespace does not exist; the
    /// accompanying [`ExplainResult`] still reflects the plan the planner
    /// would have chosen.
    fn find(
        &self,
        ns: &str,
        filter: &Document,
        opts: &FindOptions,
    ) -> Result<(Vec<Document>, crate::query::explain::ExplainResult)>;

    /// Return the first document in `ns` that matches `filter`, or `None`.
    fn find_one(&self, ns: &str, filter: &Document) -> Result<Option<Document>>;

    /// Apply an update to documents in `ns` matching `filter`.
    ///
    /// If `many` is `true`, all matching documents are updated; otherwise only
    /// the first match is updated.  `opts.upsert` controls upsert behaviour.
    fn update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &UpdateOptions,
        many: bool,
    ) -> Result<UpdateResult>;

    /// Delete documents in `ns` matching `filter`.
    ///
    /// If `many` is `false`, only the first matching document is deleted.
    fn delete(&self, ns: &str, filter: &Document, many: bool) -> Result<DeleteResult>;

    /// Count documents in `ns` matching `filter`.
    ///
    /// Passing an empty `filter` (`&Document::new()`) counts all documents.
    fn count(&self, ns: &str, filter: &Document) -> Result<u64>;

    // -------------------------------------------------------------------------
    // Atomic find-and-modify operations
    //
    // These operate at the `Document` level (no generics).  `ClientInner`
    // handles serialisation/deserialisation between `T` and `Document`.
    // -------------------------------------------------------------------------

    /// Atomically find a document, apply an operator update, and return the
    /// document before or after modification (as specified by `opts`).
    ///
    /// Returns `None` when no document matches (and upsert is disabled).
    fn find_one_and_update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &FindOneAndUpdateOptions,
    ) -> Result<Option<Document>>;

    /// Atomically find a document, remove it, and return the removed document.
    ///
    /// Returns `None` when no document matches.
    fn find_one_and_delete(
        &self,
        ns: &str,
        filter: &Document,
        opts: &FindOneAndDeleteOptions,
    ) -> Result<Option<Document>>;

    /// Atomically find a document, replace it with `replacement`, and return
    /// the document before or after replacement (as specified by `opts`).
    ///
    /// Returns `None` when no document matches (and upsert is disabled).
    fn find_one_and_replace(
        &self,
        ns: &str,
        filter: &Document,
        replacement: &Document,
        opts: &FindOneAndReplaceOptions,
    ) -> Result<Option<Document>>;

    // -------------------------------------------------------------------------
    // Index management
    // -------------------------------------------------------------------------

    /// Create an index on `ns` according to `model`.
    ///
    /// Returns the index name.  If an identical index already exists the call
    /// is a no-op and the existing name is returned.
    fn create_index(&self, ns: &str, model: &IndexModel) -> Result<String>;

    /// Drop the named index from `ns`.
    ///
    /// Returns an error if the index does not exist.
    fn drop_index(&self, ns: &str, name: &str) -> Result<()>;

    /// List all indexes defined on `ns`.
    ///
    /// Returns an empty `Vec` when the namespace does not exist or has no
    /// user-created indexes.
    fn list_indexes(&self, ns: &str) -> Result<Vec<IndexInfo>>;

    // -------------------------------------------------------------------------
    // Namespace management
    //
    // A "namespace" is the fully-qualified `"db.collection"` key used as the
    // engine's unit of storage.
    // -------------------------------------------------------------------------

    /// Create `ns` if it does not already exist.
    ///
    /// This is a no-op when the namespace already exists.
    fn create_namespace(&self, ns: &str) -> Result<()>;

    /// Drop `ns` and all its documents and indexes.
    ///
    /// Returns an error if the namespace does not exist.
    fn drop_namespace(&self, ns: &str) -> Result<()>;

    /// Return all namespaces currently managed by the engine.
    ///
    /// Namespaces are returned as fully-qualified `"db.collection"` strings.
    /// The result may be empty if no data has been written yet.
    fn list_namespaces(&self) -> Result<Vec<String>>;

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /// Flush all dirty state and write a stable on-disk checkpoint.
    ///
    /// After this returns, the main database file is in a consistent state and
    /// is safe to copy as a backup.
    fn checkpoint(&self) -> Result<()>;

    /// fsync the journal — make all committed-but-unsynced txns durable.
    ///
    /// On FullSync writes this is called per write instead of a full
    /// checkpoint. The journal IS the durability point; main-file checkpoint
    /// runs separately via `checkpoint()` (admin) or background GC.
    fn journal_sync(&self) -> Result<()>;

    /// Flush, checkpoint, and release all engine resources.
    ///
    /// After `close()` returns, the engine must not be used again.  Calling
    /// any method on a closed engine is undefined behaviour.
    #[allow(dead_code)]
    fn close(&self) -> Result<()>;

    /// Serialise the current engine state to a BSON snapshot blob.
    ///
    /// Returns `Ok(None)` when the engine does not use blob-based persistence.
    #[allow(dead_code)]
    fn snapshot_bytes(&self) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    /// Test-only accessor for the MVCC `ReadViewRegistry`.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn read_view_registry(&self) -> Option<std::sync::Arc<crate::mvcc::ReadViewRegistry>> {
        None
    }

    /// Test-only accessor: sample the timestamp oracle's current value.
    ///
    /// Returns `(physical_ms, logical)` from the oracle's last-issued timestamp.
    /// After a commit the oracle's value is >= that commit's `commit_ts`, so
    /// callers can use this to observe a monotone lower-bound on "the highest
    /// commit_ts issued so far".  Used by `recovery_timestamp_floor` to verify
    /// Contract 3.4 without reaching into `pub(crate)` internals.
    ///
    /// # Note
    ///
    /// This method is `#[doc(hidden)]` and `#[allow(unused)]` — it is intended
    /// only for integration tests.  It must not be called from production code.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn oracle_now(&self) -> (u64, u32) {
        (0, 0)
    }

    /// Test-only accessor: sample the current `PublishedEpoch.visible_ts`
    /// from the published `ArcSwap`, encoded as `(physical_ms, logical)`.
    ///
    /// Used by the §10.6 / §10.8 #23 reopen-bootstrap tests to verify
    /// that the initial PublishedEpoch carries a Ts >= the pre-crash max
    /// commit's successor. Not intended for production code.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn published_visible_ts(&self) -> (u64, u32) {
        (0, 0)
    }

    /// Test-only accessor: return the published-catalog rebuild generation.
    ///
    /// The generation advances on every fresh `Arc<PublishedCatalog>` publish
    /// and stays unchanged when a commit reuses the prior catalog Arc. This is
    /// safer than comparing raw allocation addresses from integration tests.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn published_catalog_gen(&self) -> u64 {
        0
    }

    /// Test-only accessor: sample the current `PublishedEpoch.sequencer_frontier`.
    ///
    /// Used by Phase 3 recovery tests to verify the open-time published epoch
    /// binds `visible_ts` and `sequencer_frontier` coherently.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn published_sequencer_frontier(&self) -> (u64, u32) {
        (0, 0)
    }

    /// Test-only accessor: return how many post-open recovery epoch stores
    /// this engine performed.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn recovery_open_published_store_count(&self) -> u64 {
        0
    }

    /// Test-only accessor: return the highest `ChainCommit.commit_ts` recovered
    /// from the journal on the most recent `open_or_create`, encoded as
    /// `Some((physical_ms, logical))`, or `None` when the journal was fresh or
    /// carried no `ChainCommit` frames.
    ///
    /// The US-002 crash harness uses this to verify that after a journal-tail
    /// truncation the recovered timestamp floor drops accordingly.
    ///
    /// # Note
    ///
    /// This method is `#[doc(hidden)]` — it is intended only for integration
    /// tests.  It must not be called from production code.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn recovered_max_commit_ts(&self) -> Option<(u64, u32)> {
        None
    }

    /// Hidden US-019 test hook: fail the next N primary install attempts.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us019_set_primary_install_failures(&self, _failures: u8) {}

    /// Hidden US-019 test hook: count primary install attempts.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us019_primary_install_attempts(&self) -> u64 {
        0
    }

    /// Hidden US-021c test hook: pause the next write body for `ns`.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn install_write_body_entry_hook(
        &self,
        ns: &str,
        observe_flag: Option<Arc<AtomicBool>>,
    ) -> WriteBodyEntryHookGuard;

    /// Hidden Phase 0 probe for integration tests that must pin the current
    /// write-envelope ordering without adding runtime hooks to normal CRUD.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn phase0_probe_insert(
        &self,
        _ns: &str,
        _doc: Document,
        _cut: Phase0ProbeCut,
    ) -> Result<Phase0ProbeReport> {
        Err(crate::error::Error::Internal(
            "phase0 probe is unsupported by this engine".into(),
        ))
    }
}
