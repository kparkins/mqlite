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
use super::crash_cut_test_probe::{Phase0ProbeCut, Phase0ProbeReport};
#[cfg(any(test, feature = "test-hooks"))]
use super::paged_engine::group_commit_test_probe::{
    Us017GroupCommitObservations, Us017GroupCommitPauseGuard,
};
#[cfg(any(test, feature = "test-hooks"))]
use super::paged_engine::test_accessors::{
    CreateIndexBuildHookGuard, Us007JournalBeginHookGuard, Us007JournalObservations,
    Us026CleanupObservations, Us026PostRegisterFailpoint, WriteBodyEntryHookGuard,
};
#[cfg(any(test, feature = "test-hooks"))]
use crate::journal::append_sync_test_probe::Us039AppendSyncObservations;
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
use std::sync::{
    atomic::AtomicBool,
    mpsc::{Receiver, Sender},
    Arc,
};

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

    /// Create `ns`.
    ///
    /// Returns [`Error::DuplicateKey`] when the namespace already exists.
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
    #[allow(
        dead_code,
        reason = "FullSync CRUD now syncs inside the engine group-commit path; \
                  the trait method remains for explicit admin/test sync callers"
    )]
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

    /// Test-only accessor: sample the live `PublishSequencer.published_frontier`
    /// (§10.19 C-1). Used by Phase 3 / Phase 5 recovery tests to verify the
    /// reopen-time engine seeds the live frontier from the recovered HLC.
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

    /// Hidden US-009 test hook: inspect resident primary-chain states.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us009_primary_chain_states(&self, _ns: &str, _id: &Bson) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    /// Hidden US-009 test hook: inject a committed resident primary head.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us009_inject_primary_committed_head(
        &self,
        _ns: &str,
        _doc: &Document,
        _commit_ts: crate::mvcc::Ts,
        _txn_id: u64,
    ) -> Result<()> {
        Ok(())
    }

    /// Hidden US-009 test hook: inspect resident secondary-chain states.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us009_secondary_chain_states(
        &self,
        _ns: &str,
        _index_name: &str,
        _doc: &Document,
        _id: &Bson,
    ) -> Result<Vec<String>> {
        Ok(Vec::new())
    }

    /// Hidden US-009 test hook: reset pending-flip / publish ordering probes.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us009_reset_flip_publish_order(&self) {}

    /// Hidden US-009 test hook: pending-flip order and publish-ready order.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us009_flip_publish_order(&self) -> (u64, u64) {
        (0, 0)
    }

    /// Hidden US-009 test hook: fail after committed flip before publish.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us009_fail_after_committed_flip_once(&self) {}

    /// Hidden US-028 test hook: resolve the primary leaf for `_id`.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us028_primary_leaf_for_id(&self, _ns: &str, _id: &Bson) -> Result<u32> {
        Err(crate::error::Error::Internal(
            "us028 primary-leaf probe is unsupported by this engine".into(),
        ))
    }

    /// Hidden US-028 test hook: hold the reconcile exclusive latch for a leaf.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us028_hold_primary_leaf_reconcile_latch(
        &self,
        _ns: &str,
        _id: &Bson,
        _ready: Sender<()>,
        _release: Receiver<()>,
    ) -> Result<()> {
        Err(crate::error::Error::Internal(
            "us028 reconcile-latch probe is unsupported by this engine".into(),
        ))
    }

    /// Hidden US-028 test hook: hold the writer exclusive latch for a leaf.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us028_hold_primary_leaf_writer_latch(
        &self,
        _ns: &str,
        _id: &Bson,
        _ready: Sender<()>,
        _release: Receiver<()>,
    ) -> Result<()> {
        Err(crate::error::Error::Internal(
            "us028 writer-latch probe is unsupported by this engine".into(),
        ))
    }

    /// Hidden US-025 test hook: hold the reader shared latch for a leaf.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us025_hold_primary_leaf_reader_latch(
        &self,
        _ns: &str,
        _id: &Bson,
        _ready: Sender<()>,
        _release: Receiver<()>,
    ) -> Result<()> {
        Err(crate::error::Error::Internal(
            "us025 reader-latch probe is unsupported by this engine".into(),
        ))
    }

    /// Hidden US-026 test hook: arm one post-register cleanup failpoint.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us026_arm_post_register_failpoint(&self, _failpoint: Us026PostRegisterFailpoint) {}

    /// Hidden US-026 test hook: snapshot cleanup observations.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us026_cleanup_observations(&self) -> Us026CleanupObservations {
        Us026CleanupObservations::default()
    }

    /// Hidden US-021c test hook: pause the next write body for `ns`.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn install_write_body_entry_hook(
        &self,
        ns: &str,
        observe_flag: Option<Arc<AtomicBool>>,
    ) -> WriteBodyEntryHookGuard;

    /// Hidden US-013 test hook: pause `create_index_build` at scan entry.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn install_create_index_build_hook(
        &self,
        ns: &str,
        index_name: &str,
    ) -> CreateIndexBuildHookGuard;

    /// Hidden US-038 test hook: pause `create_index_build`, then fail it
    /// after release so the production cleanup path runs.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn install_create_index_build_failure_hook(
        &self,
        ns: &str,
        index_name: &str,
    ) -> CreateIndexBuildHookGuard;

    /// Hidden US-007 test hook: pause immediately after `begin_txn`
    /// while `journal_mutex` is held.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us007_install_journal_begin_hook(
        &self,
        _fail_after_release: bool,
    ) -> Us007JournalBeginHookGuard;

    /// Hidden US-007 test hook: reset journal-envelope counters.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us007_reset_journal_observations(&self) {}

    /// Hidden US-007 test hook: snapshot journal-envelope counters.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us007_journal_observations(&self) -> Us007JournalObservations {
        Us007JournalObservations {
            guarded_flushes: 0,
            unguarded_flushes: 0,
            guarded_syncs: 0,
            unguarded_syncs: 0,
        }
    }

    /// Hidden US-039 test hook: reset append/sync ownership counters.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us039_reset_append_sync_observations(&self) {
        crate::journal::append_sync_test_probe::reset();
    }

    /// Hidden US-039 test hook: snapshot append/sync ownership counters.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us039_append_sync_observations(&self) -> Us039AppendSyncObservations {
        crate::journal::append_sync_test_probe::snapshot()
    }

    /// Hidden US-017 test hook: reset group-commit probe state.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us017_reset_group_commit_probe(&self) {}

    /// Hidden US-017 test hook: make the next leader wait for a cohort size.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us017_expect_group_commit_cohort_size(&self, _expected: u64) {}

    /// Hidden US-017 test hook: fail the next group-commit fsync.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us017_fail_next_group_commit_fsync(&self) {}

    /// Hidden US-017 test hook: pause the next leader after closing a cohort.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us017_pause_next_group_commit_after_close(&self) -> Us017GroupCommitPauseGuard;

    /// Hidden US-017 test hook: snapshot group-commit observations.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us017_group_commit_observations(&self) -> Us017GroupCommitObservations {
        Us017GroupCommitObservations::default()
    }

    /// Hidden US-008 test hook: reset committed structural leaf-byte accounting.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us008_reset_structural_page_observations(&self) {}

    /// Hidden US-008 test hook: committed structural leaf bytes since reset.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us008_committed_structural_leaf_bytes(&self) -> u64 {
        0
    }

    /// Hidden US-011 hook: install one pending unique email secondary entry.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us011_install_pending_unique_email(
        &self,
        _ns: &str,
        _index_name: &str,
        _id: Bson,
        _email: &str,
        _txn_id: u64,
    ) -> Result<()> {
        Err(crate::error::Error::Internal(
            "us011 pending unique probe is unsupported by this engine".into(),
        ))
    }

    /// Hidden US-011 hook: compute unique-prefix sibling pages on a probe leaf.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us011_unique_prefix_sibling_pages(&self) -> Result<Vec<u32>> {
        Err(crate::error::Error::Internal(
            "us011 sibling-prefix probe is unsupported by this engine".into(),
        ))
    }

    /// Hidden Phase 0 probe for integration tests that must pin the current
    /// write-envelope ordering without adding runtime hooks to normal CRUD.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn crash_cut_probe_insert(
        &self,
        _ns: &str,
        _doc: Document,
        _cut: Phase0ProbeCut,
    ) -> Result<Phase0ProbeReport> {
        Err(crate::error::Error::Internal(
            "phase0 probe is unsupported by this engine".into(),
        ))
    }

    /// Hidden US-022 hook: stage two inserts in one storage write txn.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us022_insert_two_docs_one_txn(
        &self,
        _ns: &str,
        _left: Document,
        _right: Document,
    ) -> Result<()> {
        Err(crate::error::Error::Internal(
            "us022 multi-insert probe is unsupported by this engine".into(),
        ))
    }

    /// Hidden US-036 hook: poison the live engine with `reason`.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us036_test_poison_engine(&self, _reason: crate::error::EngineFatalReason) {}

    /// Hidden US-036 hook: read the current poison reason.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us036_test_poisoned_reason(&self) -> Option<crate::error::EngineFatalReason> {
        None
    }

    /// Hidden US-036 hook: register a publish slot. Returns
    /// [`crate::Error::EngineFatal`] when the sequencer is poisoned.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us036_test_register_publish_slot(
        &self,
    ) -> Result<crate::storage::paged_engine::engine_fatal_test_probe::Us036PublishSlot> {
        Err(crate::error::Error::Internal(
            "us036 publish-slot probe is unsupported by this engine".into(),
        ))
    }

    /// Hidden US-036 hook: admit a writer ticket on `ns_id`.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us036_test_admit_writer(
        &self,
        _ns_id: i64,
        _timeout_ms: u64,
    ) -> Result<crate::storage::paged_engine::engine_fatal_test_probe::Us036WriterTicket> {
        Err(crate::error::Error::Internal(
            "us036 writer-admit probe is unsupported by this engine".into(),
        ))
    }

    /// Hidden US-036 hook: close-and-drain a namespace lane.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us036_test_close_and_drain(&self, _ns_id: i64, _timeout_ms: u64) -> Result<()> {
        Err(crate::error::Error::Internal(
            "us036 drain probe is unsupported by this engine".into(),
        ))
    }

    /// Hidden US-036 hook: resolve the durable `ns_id` for `ns`.
    ///
    /// AC #7: fail-closed once the engine is poisoned.
    ///
    /// # Errors
    /// Returns [`crate::Error::EngineFatal`] when the live engine is
    /// poisoned.
    #[cfg(any(test, feature = "test-hooks"))]
    #[doc(hidden)]
    fn us036_test_namespace_id(&self, _ns: &str) -> crate::error::Result<Option<i64>> {
        Ok(None)
    }
}
