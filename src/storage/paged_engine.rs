//! `PagedEngine` — storage engine backed by B+ trees.
//!
//! ## Design
//!
//! Documents are stored in per-namespace B+ trees keyed by [`encode_key`]-encoded
//! `_id` values, backed by a [`BufferPoolPageStore`] (shared [`BufferPoolHandle`])
//! with persistence via buffer pool flush.
//!
//! ## Catalog
//!
//! A [`Catalog`] B+ tree stores [`CollectionEntry`] and [`IndexEntry`] records.
//! Its root page number is persisted to [`FileHeader::catalog_root_page`] after every
//! catalog mutation, so the catalog can be located on reopen.
//!
//! ## Query execution
//!
//! `find` first asks the query planner ([`select_plan`]) whether the query can use
//! the implicit primary `_id` key or a secondary index.  When a suitable secondary
//! index is found the engine performs an [`IndexScan`] — a range scan on the
//! secondary B+ tree whose values contain the serialised `_id` of the matching
//! document, followed by a point lookup in the primary data tree.  Other read-like
//! paths reuse the same primary-key / collection-scan executor to avoid ad hoc `_id`
//! special cases. When no access path matches the engine falls back to a full
//! [`CollScan`].
//!
//! [`IndexScan`]: crate::query::planner::ScanPlan::IndexScan
//! [`CollScan`]: crate::query::planner::ScanPlan::CollScan
//! [`select_plan`]: crate::query::planner::select_plan

mod btree_ops;
mod checkpoint_gate;
mod commit_envelope;
pub(crate) mod doc_helpers;
mod doc_ops;
mod durability;
mod ns_ddl;
/// Test-only engine-fatal probe — engine-fatal poison + sequencer + writer
/// admission handles. Kept in a separate module so intrusive test plumbing
/// stays out of production paths.
#[cfg(any(test, feature = "test-hooks"))]
#[path = "paged_engine/tests/engine_fatal_harness.rs"]
pub mod engine_fatal_harness;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "paged_engine/tests/group_commit_observations.rs"]
pub mod group_commit_observations;
/// Test-only `impl PagedEngine` accessors — isolated from the
/// production code path in a separate file so the boundary is
/// visible at a glance.
#[cfg(any(test, feature = "test-hooks"))]
#[path = "paged_engine/tests/hidden_accessors.rs"]
pub(crate) mod hidden_accessors;
mod checkpoint_materialize;
mod index_ddl;
mod index_maint;
mod index_read_helpers;
mod index_write_maint;
mod pending_install;
pub(crate) mod publish;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "paged_engine/tests/publish_registry_harness.rs"]
pub mod publish_registry_harness;
pub(crate) mod publish_sequencer;
mod recovery_apply;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "paged_engine/tests/smo_classification_observations.rs"]
pub mod smo_classification_observations;
mod smo_latch;
mod snapshot_ops;
mod state;
mod ttl_sweep;
mod visibility;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "paged_engine/tests/write_crash_cut_harness.rs"]
mod write_crash_cut_harness;
pub(crate) mod writer_registry;

#[cfg(test)]
#[path = "paged_engine/tests/allocator_freeze_boundary.rs"]
mod allocator_freeze_boundary;
#[cfg(test)]
#[path = "paged_engine/tests/bug5_drop_namespace_stale_epoch_read.rs"]
mod bug5_drop_namespace_stale_epoch_read;
#[cfg(test)]
#[path = "paged_engine/tests/bug5_retired_free_pool_coherence.rs"]
mod bug5_retired_free_pool_coherence;
#[cfg(test)]
#[path = "paged_engine/tests/bug5_retired_refcount_reader_fence.rs"]
mod bug5_retired_refcount_reader_fence;
#[cfg(test)]
#[path = "paged_engine/tests/bug6_ddl_torn_catalog_commit.rs"]
mod bug6_ddl_torn_catalog_commit;
#[cfg(test)]
#[path = "paged_engine/tests/bug6_ddl_torn_drop_commit.rs"]
mod bug6_ddl_torn_drop_commit;
#[cfg(test)]
#[path = "paged_engine/tests/bug7_checkpoint_snapshot_isolation.rs"]
mod bug7_checkpoint_snapshot_isolation;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_boundary_replay.rs"]
mod checkpoint_boundary_replay;
#[cfg(test)]
#[path = "paged_engine/tests/close_quadratic_probe_harness.rs"]
mod close_quadratic_probe_harness;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_dirty_leaf_reconcile.rs"]
mod checkpoint_dirty_leaf_reconcile;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_flush_set.rs"]
mod checkpoint_flush_set;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_gate.rs"]
mod checkpoint_gate_tests;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_incomplete_metrics.rs"]
mod checkpoint_incomplete_metrics;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_pool_saturation.rs"]
mod checkpoint_pool_saturation;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_reconcile_plan.rs"]
mod checkpoint_reconcile_plan;
#[cfg(test)]
#[path = "paged_engine/tests/dirty_leaf_integration.rs"]
mod dirty_leaf_integration;
#[cfg(test)]
#[path = "paged_engine/tests/flip_committed_concurrent_observers.rs"]
mod flip_committed_concurrent_observers;
#[cfg(test)]
#[path = "paged_engine/tests/index_build_recovery.rs"]
mod index_build_recovery;
#[cfg(test)]
#[path = "paged_engine/tests/bugsuspect_resume_building_index_brick.rs"]
mod bugsuspect_resume_building_index_brick;
#[cfg(test)]
#[path = "paged_engine/tests/bugsuspect_readview_prune_race.rs"]
mod bugsuspect_readview_prune_race;
#[cfg(test)]
#[path = "paged_engine/tests/logical_replay_frontier.rs"]
mod logical_replay_frontier;
#[cfg(test)]
#[path = "paged_engine/tests/pending_write_visibility.rs"]
mod pending_write_visibility;
#[cfg(test)]
#[path = "paged_engine/tests/published_epoch_coherence.rs"]
mod published_epoch_coherence;
#[cfg(test)]
#[path = "paged_engine/tests/retired_sequence_source_audit.rs"]
mod retired_sequence_source_audit;
#[cfg(test)]
#[path = "paged_engine/tests/secondary_index_delta_scan.rs"]
mod secondary_index_delta_scan;
#[cfg(test)]
#[path = "paged_engine/tests/secondary_index_pending_write.rs"]
mod secondary_index_pending_write;
#[cfg(test)]
mod tests;
#[cfg(test)]
#[path = "paged_engine/tests/unique_constraint_delta.rs"]
mod unique_constraint_delta;
#[cfg(test)]
#[path = "paged_engine/tests/write_order.rs"]
mod write_order;
#[cfg(test)]
#[path = "paged_engine/tests/write_visibility_epoch.rs"]
mod write_visibility_epoch;

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::options::BusyHandler;

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::index::{IndexInfo, IndexModel};
use crate::options::{
    DurabilityMode, FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions,
    FindOptions, UpdateOptions,
};
use crate::results::{DeleteResult, UpdateResult};
use crate::storage::btree::BTree;
use crate::storage::buffer_pool::PageSize;
use crate::storage::handle::BufferPoolHandle;

use self::state::{MetadataState, SharedState};
use crate::storage::catalog::IndexState;

pub(crate) use self::commit_envelope::InsertManyBatchError;

// ---------------------------------------------------------------------------
// PagedEngine — public struct
// ---------------------------------------------------------------------------

/// Storage engine: B+ tree per namespace, through the buffer pool.
///
/// ## Concurrency
///
/// - **Reads**: mutex-free — load `shared.published` (`ArcSwap`) and open
///   B-trees at the snapshot's root pages. No engine-level lock taken.
/// - **Writes on CRUD paths**: take `metadata.read()` across the private
///   write body, resident Pending install, journal envelope, and ordered
///   publish. The guard is shared by CRUD writers, so it blocks DDL without
///   serializing ordinary writes against each other.
/// - **DDL** (`create_namespace`, `drop_namespace`, `create_index`,
///   `drop_index`, `checkpoint`, `close`, `backup`): takes `metadata.write()`
///   exclusively, blocking all writers globally for the duration.
pub(crate) struct PagedEngine {
    /// Shared state accessible by the mutex-free read path and every writer.
    pub(crate) shared: Arc<SharedState>,
    /// Coarse DDL/CRUD fence. DDL paths take `metadata.write()` to gain
    /// exclusive access during catalog mutation; ordinary CRUD takes
    /// `metadata.read()` across its full private write lifecycle. Because
    /// read guards are shared, CRUD writers can still overlap while DDL waits
    /// for all in-flight writers to finish before mutating catalog identity.
    pub(crate) metadata: RwLock<()>,
    /// Catalog state. Protected internally by `Mutex<Catalog>`; the
    /// direct field placement (no `RwLock` wrapping) lets CRUD paths mutate
    /// catalog internals while holding only the shared DDL/CRUD read fence.
    pub(crate) metadata_state: Arc<MetadataState>,
    /// Writer busy-timeout applied on lane contention.
    pub(crate) busy_timeout: Duration,
    /// Durability mode chosen at open time.
    durability_mode: DurabilityMode,
    /// Monotonic origin used by interval-mode sync deadline accounting.
    interval_sync_origin: Instant,
    /// Next monotonic millisecond deadline for interval-mode sync attempts.
    next_interval_sync_ms: AtomicU64,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct NamespaceCatalogIdentity {
    ns_id: i64,
    indexes: Vec<IndexCatalogIdentity>,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct IndexCatalogIdentity {
    id: i64,
    name: String,
    key_pattern: Document,
    unique: bool,
    sparse: bool,
    state: IndexState,
}

pub(super) fn duration_millis_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl PagedEngine {
    /// Escalate a post-durable failure to engine-fatal poison. Routes through
    /// [`state::poison_after_durable_commit`] so the live sequencer's blocked
    /// successors wake with `Error::EngineFatal`.
    pub(super) fn engine_fatal(&self, reason: crate::error::EngineFatalReason) -> Error {
        state::poison_after_durable_commit(&self.shared, reason)
    }

    /// Create a file-backed engine using `handle` as the page store.
    ///
    /// If `catalog_root_page == 0` the database is new and an empty catalog
    /// will be created. Otherwise the catalog is opened at the given root.
    #[cfg(test)]
    pub(crate) fn new_buffered(
        handle: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
    ) -> Result<Self> {
        Self::new_buffered_with_busy(
            handle,
            catalog_root_page,
            catalog_root_level,
            Duration::from_secs(5),
            None,
            3,
            DurabilityMode::default(),
        )
    }

    /// Create a file-backed engine with explicit busy-timeout + busy-handler.
    pub(crate) fn new_buffered_with_busy(
        handle: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
        busy_timeout: Duration,
        _busy_handler: Option<BusyHandler>,
        smo_classification_retry_cap: u32,
        durability_mode: DurabilityMode,
    ) -> Result<Self> {
        // R5b: wire the live reader low-water into the allocator so
        // page-lifetime drains can gate `RetiredTree*` releases on
        // `oldest_required_ts()` — a dropped tree's pages stay un-reusable
        // while any live ReadView still predates the drop.
        {
            let read_view_registry = Arc::clone(handle.read_view_registry());
            handle
                .allocator()
                .install_retired_page_reader_floor(move || {
                    read_view_registry.oldest_required_ts()
                })?;
        }
        let (md, shared) = MetadataState::new(
            handle,
            catalog_root_page,
            catalog_root_level,
            smo_classification_retry_cap,
        )?;
        let next_interval_sync_ms = match &durability_mode {
            DurabilityMode::Interval(interval) => duration_millis_saturating(*interval),
            DurabilityMode::FullSync | DurabilityMode::None => u64::MAX,
        };
        let engine = PagedEngine {
            shared,
            metadata: RwLock::new(()),
            metadata_state: Arc::new(md),
            busy_timeout,
            durability_mode,
            interval_sync_origin: Instant::now(),
            next_interval_sync_ms: AtomicU64::new(next_interval_sync_ms),
        };
        engine.resume_building_indexes_after_open()?;
        // Run one TTL sweep now that recovery has fully completed (catalog
        // restored, Building indexes resumed). A non-fatal sweep error must
        // not brick open — the next sweep (on-demand or the wire timer) will
        // retry — but an `EngineFatal` is a genuine poison and must propagate.
        if let Err(error) = engine.sweep_expired() {
            if matches!(error, Error::EngineFatal { .. }) {
                return Err(error);
            }
            #[cfg(feature = "tracing")]
            tracing::warn!(
                target: "mqlite",
                error = %error,
                "mqlite::ttl_sweep open-time sweep failed (non-fatal)"
            );
        }
        Ok(engine)
    }
}

// ---------------------------------------------------------------------------
// Document operations
// ---------------------------------------------------------------------------

impl PagedEngine {
    pub(crate) fn insert(&self, ns: &str, doc: Document) -> Result<Bson> {
        self.shared.check_engine_not_poisoned()?;
        self.run_write(ns, |shared, md, txn, vis| {
            doc_ops::stage_insert(shared, md, txn, vis, ns, doc)
        })
    }

    pub(crate) fn find(
        &self,
        ns: &str,
        filter: &Document,
        opts: &FindOptions,
    ) -> Result<(Vec<Document>, crate::query::explain::ExplainResult)> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::find(self, ns, filter, opts)
    }

    pub(crate) fn find_one(&self, ns: &str, filter: &Document) -> Result<Option<Document>> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::find_first(self, ns, filter)
    }

    pub(crate) fn aggregate(
        &self,
        ns: &str,
        pipeline: &crate::query::aggregate::Pipeline,
    ) -> Result<Vec<Document>> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::aggregate(self, ns, pipeline)
    }

    pub(crate) fn update(
        &self,
        ns: &str,
        filter: &Document,
        mods: &crate::update::UpdateModifications,
        array_filters: Option<&[Document]>,
        opts: &UpdateOptions,
        many: bool,
    ) -> Result<UpdateResult> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::update(self, ns, filter, mods, array_filters, opts, many)
    }

    pub(crate) fn delete(&self, ns: &str, filter: &Document, many: bool) -> Result<DeleteResult> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::delete(self, ns, filter, many)
    }

    pub(crate) fn count(&self, ns: &str, filter: &Document) -> Result<u64> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::count(self, ns, filter)
    }

    pub(crate) fn distinct(
        &self,
        ns: &str,
        field_name: &str,
        filter: &Document,
    ) -> Result<Vec<Bson>> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::distinct(self, ns, field_name, filter)
    }

    pub(crate) fn find_one_and_update(
        &self,
        ns: &str,
        filter: &Document,
        mods: &crate::update::UpdateModifications,
        array_filters: Option<&[Document]>,
        opts: &FindOneAndUpdateOptions,
    ) -> Result<Option<Document>> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::find_one_and_update(self, ns, filter, mods, array_filters, opts)
    }

    pub(crate) fn find_one_and_delete(
        &self,
        ns: &str,
        filter: &Document,
        opts: &FindOneAndDeleteOptions,
    ) -> Result<Option<Document>> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::find_one_and_delete(self, ns, filter, opts)
    }

    pub(crate) fn find_one_and_replace(
        &self,
        ns: &str,
        filter: &Document,
        replacement: &Document,
        opts: &FindOneAndReplaceOptions,
    ) -> Result<Option<Document>> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::find_one_and_replace(self, ns, filter, replacement, opts)
    }

    pub(crate) fn replace_one(
        &self,
        ns: &str,
        filter: &Document,
        replacement: &Document,
        upsert: bool,
    ) -> Result<UpdateResult> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::replace_one(self, ns, filter, replacement, upsert)
    }

    pub(crate) fn create_index(&self, ns: &str, model: &IndexModel) -> Result<String> {
        self.shared.check_engine_not_poisoned()?;
        index_maint::create_index(self, ns, model)
    }

    pub(crate) fn drop_index(&self, ns: &str, name: &str) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        index_maint::drop_index(self, ns, name)
    }

    pub(crate) fn list_indexes(&self, ns: &str) -> Result<Vec<IndexInfo>> {
        self.shared.check_engine_not_poisoned()?;
        index_maint::list_indexes(self, ns)
    }

    // -----------------------------------------------------------------------
    // list_namespaces
    // -----------------------------------------------------------------------

    pub(crate) fn list_namespaces(&self) -> Result<Vec<String>> {
        self.shared.check_engine_not_poisoned()?;
        let snap = self.shared.load_published();
        let keys = snap.catalog.namespace_id_by_name.keys();
        let mut out = Vec::with_capacity(keys.len());
        out.extend(keys.cloned());
        Ok(out)
    }

    pub(crate) fn checkpoint(&self) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        snapshot_ops::checkpoint(self)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn read_view_registry(&self) -> Option<Arc<crate::mvcc::ReadViewRegistry>> {
        Some(Arc::clone(self.shared.handle.read_view_registry()))
    }

    #[allow(dead_code)]
    pub(crate) fn close(&self) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        snapshot_ops::checkpoint(self)
    }

    #[allow(
        dead_code,
        reason = "FullSync CRUD now syncs inside the engine group-commit path; \
                  the method remains for explicit admin/test sync callers"
    )]
    pub(crate) fn journal_sync(&self) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        snapshot_ops::journal_sync(self)
    }

    #[allow(dead_code)]
    pub(crate) fn snapshot_bytes(&self) -> Result<Option<Vec<u8>>> {
        self.shared.check_engine_not_poisoned()?;
        snapshot_ops::snapshot_bytes(self)
    }
}

// ---------------------------------------------------------------------------
// DDL helper + upsert helpers (private)
// ---------------------------------------------------------------------------

impl PagedEngine {
    /// Collect every page (internals, leaves, and overflow chains referenced
    /// by reconciled leaf cells) of the tree rooted at `root_page`.
    ///
    /// Read-only walk: replaces the old in-body free sweep so the pages — and
    /// their resident version chains — stay fully intact until
    /// `retire_dropped_tree_pages` runs after the drop has committed and
    /// published.
    pub(super) fn collect_tree_pages(
        &self,
        root_page: u32,
        root_level: u8,
    ) -> Result<Vec<(u32, PageSize)>> {
        let mut tree = BTree::open(self.shared.new_btree_store(), root_page, root_level);
        tree.collect_pages_by_size()
    }

    /// Retire a dropped tree's pages AFTER the drop durably committed and the
    /// new epoch published.
    ///
    /// EVERY tree page — 4 KiB internals included — is routed through the
    /// page-lifetime deferred-free queue instead of being pushed straight
    /// onto the allocator free list: a reader that loaded the pre-drop
    /// `PublishedEpoch` before the publish can still open a fresh
    /// (non-poisoned) ReadView and scan the dropped tree, and an immediate
    /// free would let those pages be reused and silently satisfy that
    /// snapshot with empty/foreign results. Descent validates only the
    /// one-byte page type, so a freed 4 KiB internal reused by another
    /// tree's SMO is a valid internal again and silently routes a
    /// stale-epoch reader into a FOREIGN subtree — there is no fence-key or
    /// tree-identity check to fail cleanly on.
    ///
    /// Queue entries carry two release gates: a later checkpoint must
    /// advance the lifetime fence AND no live ReadView may predate the drop
    /// (`oldest_required_ts() >= reader_fence_ts`). Only the checkpoint
    /// drain (`advance_page_lifetime_checkpoint`, pool-coherent io) frees
    /// retired pages — hot drains skip them wholesale; reallocation
    /// re-zeroes the frame and clears chains (`BufferPoolHandle::alloc_page`).
    ///
    /// 32 KiB pages whose overflow refcount is still positive are not
    /// enqueued here: their lifetime stays owned by the `OverflowRef` RAII
    /// discipline, whose final decref enqueues them exactly once. A pending
    /// note (`note_retired_overflow_pending`) recorded BEFORE the refcount
    /// probe makes that final decref inherit this drop's `reader_fence_ts`
    /// (it enqueues `RetiredTree32k`, not a fence-less overflow entry), so
    /// a registered pre-drop reader that can still reach the chain through
    /// a base-leaf pointer keeps the page un-reusable. The note-then-probe
    /// order (paired with the SeqCst fences in the allocator) guarantees a
    /// final decref racing this walk either consumes the note or is
    /// observed by the probe — never both, never neither — so each page is
    /// enqueued exactly once.
    ///
    /// Leak honesty: the page-lifetime queue is in-memory only and there is
    /// NO startup scavenger that rebuilds the free list. If the process
    /// exits before an entry drains — or a drain's free fails — the page is
    /// leaked PERMANENTLY on disk. Refcount>0 pages leak the same way if
    /// the final decref never runs in this process lifetime. This is the
    /// accepted cost of never reusing a page a stale snapshot may still
    /// need.
    pub(super) fn retire_dropped_tree_pages(&self, pages: &[(u32, PageSize)]) {
        let allocator = self.shared.handle.allocator();
        // Post-publish visible_ts: every reader pinned to a pre-drop epoch
        // has read_ts strictly below it.
        let reader_fence_ts = self.shared.published.load_full().visible_ts;
        for (page_id, size) in pages {
            match size {
                PageSize::Small4k => {
                    allocator.enqueue_retired_tree_page(
                        *page_id,
                        PageSize::Small4k,
                        reader_fence_ts,
                    );
                }
                PageSize::Large32k => {
                    // Record the drop's reader fence BEFORE probing the
                    // refcount so a concurrent final decref cannot slip a
                    // fence-less entry into the queue.
                    allocator.note_retired_overflow_pending(*page_id, reader_fence_ts);
                    match allocator.overflow_refcount_slot(*page_id) {
                        // No refcount slot was ever created (plain leaf or
                        // never-referenced page): no decref will ever
                        // consume the note — reclaim it and enqueue here.
                        None => {
                            if let Some(fence_ts) =
                                allocator.take_retired_overflow_pending(*page_id)
                            {
                                allocator.enqueue_retired_tree_page(
                                    *page_id,
                                    PageSize::Large32k,
                                    fence_ts,
                                );
                            }
                        }
                        // Slot exists at refcount 0: the final decref's
                        // `fetch_sub` already ran, and its enqueue either
                        // consumed the note (RetiredTree32k queued) or
                        // pushed its entry before the note landed. Exactly
                        // one queue entry exists either way — do NOT add a
                        // second; just reclaim the note if it is still ours
                        // so it cannot mis-tag a future occupant of this
                        // page number.
                        Some(0) => {
                            let _ = allocator.take_retired_overflow_pending(*page_id);
                        }
                        // Live refcount (base image and/or resident chain
                        // entries): the note stays; the final decref
                        // enqueues the page as RetiredTree32k carrying this
                        // drop's reader fence.
                        Some(_) => {}
                    }
                }
            }
        }
    }
}

