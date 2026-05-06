//! `PagedEngine` — `StorageEngine` backed by B+ trees.
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
mod catalog_ops;
#[cfg(any(test, feature = "test-hooks"))]
mod crash_cut_test_probe;
mod doc_helpers;
mod doc_ops;
/// Test-only engine-fatal probe — engine-fatal poison + sequencer + writer
/// ticket handles. Kept in a separate module so intrusive test plumbing stays
/// out of production paths.
#[cfg(any(test, feature = "test-hooks"))]
pub mod engine_fatal_test_probe;
mod group_commit;
#[cfg(any(test, feature = "test-hooks"))]
pub mod group_commit_test_probe;
mod index_build;
mod index_maint;
pub(crate) mod publish;
#[cfg(any(test, feature = "test-hooks"))]
pub mod publish_registry_test_probe;
pub(crate) mod publish_sequencer;
mod recovery_apply;
#[cfg(any(test, feature = "test-hooks"))]
pub mod smo_classification_test_probe;
mod smo_latch;
mod snapshot_ops;
mod state;
/// Test-only `impl PagedEngine` accessors — isolated from the
/// production code path in a separate file so the boundary is
/// visible at a glance.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) mod test_accessors;
mod visibility;
pub(crate) mod writer_registry;

#[cfg(test)]
#[path = "paged_engine/tests/allocator_freeze_boundary_tests.rs"]
mod allocator_freeze_boundary_tests;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_boundary_replay_tests.rs"]
mod checkpoint_boundary_replay_tests;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_dirty_leaf_reconcile_tests.rs"]
mod checkpoint_dirty_leaf_reconcile_tests;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_flush_set_tests.rs"]
mod checkpoint_flush_set_tests;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_gate_tests.rs"]
mod checkpoint_gate_tests;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_incomplete_metrics_tests.rs"]
mod checkpoint_incomplete_metrics_tests;
#[cfg(test)]
#[path = "paged_engine/tests/checkpoint_reconcile_plan_tests.rs"]
mod checkpoint_reconcile_plan_tests;
#[cfg(test)]
#[path = "paged_engine/tests/dirty_leaf_integration_tests.rs"]
mod dirty_leaf_integration_tests;
#[cfg(test)]
#[path = "paged_engine/tests/index_build_recovery_tests.rs"]
mod index_build_recovery_tests;
#[cfg(test)]
#[path = "paged_engine/tests/logical_replay_frontier_tests.rs"]
mod logical_replay_frontier_tests;
#[cfg(test)]
#[path = "paged_engine/tests/orphan_chain_commit_recovery_tests.rs"]
mod orphan_chain_commit_recovery_tests;
#[cfg(test)]
#[path = "paged_engine/tests/pending_write_visibility_tests.rs"]
mod pending_write_visibility_tests;
#[cfg(test)]
#[path = "paged_engine/tests/published_epoch_coherence_tests.rs"]
mod published_epoch_coherence_tests;
#[cfg(test)]
#[path = "paged_engine/tests/retired_sequence_source_audit_tests.rs"]
mod retired_sequence_source_audit_tests;
#[cfg(test)]
#[path = "paged_engine/tests/secondary_index_delta_scan_tests.rs"]
mod secondary_index_delta_scan_tests;
#[cfg(test)]
#[path = "paged_engine/tests/secondary_index_pending_write_tests.rs"]
mod secondary_index_pending_write_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
#[path = "paged_engine/tests/unique_constraint_delta_tests.rs"]
mod unique_constraint_delta_tests;
#[cfg(test)]
#[path = "paged_engine/tests/write_commit_sequence_tests.rs"]
mod write_commit_sequence_tests;
#[cfg(test)]
#[path = "paged_engine/tests/write_visibility_epoch_tests.rs"]
mod write_visibility_epoch_tests;

use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[cfg(any(test, feature = "test-hooks"))]
use self::test_accessors::{Phase3CommitFailpoint, Us026PostRegisterFailpoint};
#[cfg(test)]
use std::sync::atomic::Ordering;

use crate::options::BusyHandler;

use bson::{Bson, Document};

#[cfg(any(test, feature = "test-hooks"))]
use super::crash_cut_test_probe::{Phase0ProbeCut, Phase0ProbeReport};
use super::engine::StorageEngine;
use crate::error::{Error, Result, WriteConflictReason};
use crate::index::{IndexInfo, IndexModel};
use crate::mvcc::transaction::WriteTxn;
use crate::options::{
    DurabilityMode, FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions,
    FindOptions, UpdateOptions,
};
use crate::results::{DeleteResult, UpdateResult};
use crate::storage::btree::{BTree, BTreePageStore};
use crate::storage::buffer_pool::PageSize;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::structural_page_batch::StructuralPageBatch;

use self::catalog_ops::{
    catalog_lock, new_store, new_structural_store, rebuild_and_publish_locked,
    sync_catalog_root_structural,
};
use self::doc_helpers::now_millis;
use self::index_maint::{
    flip_pending_to_aborted_for, flip_pending_to_committed_for, install_pending_primary,
    install_pending_sec_index,
};
use self::publish::PublishDirty;
use self::state::{MetadataState, SharedState};
use self::visibility::WriteVisibility;
use crate::storage::catalog::IndexState;

const FULLSYNC_GROUP_COMMIT_MAX_WAIT_MS: u64 = 2;

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
}

struct JournalMutexGuard<'a> {
    _guard: parking_lot::MutexGuard<'a, ()>,
    #[cfg(any(test, feature = "test-hooks"))]
    _scope: self::test_accessors::Us007JournalMutexScopeGuard,
}

#[derive(Clone, Debug, PartialEq)]
struct NamespaceCatalogIdentity {
    ns_id: i64,
    indexes: Vec<IndexCatalogIdentity>,
}

#[derive(Clone, Debug, PartialEq)]
struct IndexCatalogIdentity {
    id: i64,
    name: String,
    key_pattern: Document,
    unique: bool,
    sparse: bool,
    state: IndexState,
}

/// RAII guard that records the logical-frame-append duration sample and
/// recomputes the percentile gauges (p50/p95/p99) from the ring buffer.
///
/// Duration sampling reads `Instant::now()` outside the journal critical
/// section. This guard:
///
///   - Captures `start = Instant::now()` at construction (BEFORE
///     the journal mutex is acquired in `run_write_existing` — the
///     guard is declared before `_commit`, and Rust evaluates RHS in
///     order so the `Instant::now()` call here happens first).
///   - On `drop` (which runs AFTER the journal mutex is released
///     because of LIFO drop order), samples `elapsed` and records it
///     as one logical-frame-append duration sample, then recomputes
///     the p50/p95/p99 gauges.
///
/// Both `Instant::now()` reads happen with NO journal mutex held.
///
/// The recorded duration spans the full journal critical section rather than
/// just the journal-file I/O. Per §7 ("approximate latest-value gauge"), this
/// is an acceptable approximation: the critical section is dominated by the
/// logical transaction and ChainCommit journal writes, so the envelope duration
/// tracks the append duration to within a few microseconds of overhead.
struct LogicalTxnAppendPercentileRefresh {
    start: std::time::Instant,
}

impl LogicalTxnAppendPercentileRefresh {
    fn new() -> Self {
        Self {
            start: std::time::Instant::now(),
        }
    }
}

impl Drop for LogicalTxnAppendPercentileRefresh {
    fn drop(&mut self) {
        let elapsed_ms = self.start.elapsed().as_millis() as u64;
        crate::mvcc::metrics::record_logical_txn_append_duration_ms(elapsed_ms);
        crate::mvcc::metrics::recompute_logical_txn_append_percentiles();
    }
}

impl PagedEngine {
    /// Escalate a post-durable failure to engine-fatal poison. Routes through
    /// [`state::poison_after_durable_commit`] so the live sequencer's blocked
    /// successors wake with `Error::EngineFatal`.
    fn engine_fatal(&self, reason: crate::error::EngineFatalReason) -> Error {
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
        let (md, shared) = MetadataState::new(
            handle,
            catalog_root_page,
            catalog_root_level,
            smo_classification_retry_cap,
        )?;
        let engine = PagedEngine {
            shared,
            metadata: RwLock::new(()),
            metadata_state: Arc::new(md),
            busy_timeout,
            durability_mode,
        };
        engine.resume_building_indexes_after_open()?;
        Ok(engine)
    }

    fn lock_journal_mutex(&self) -> JournalMutexGuard<'_> {
        let wait_start = Instant::now();
        let guard = self.shared.journal_mutex.lock();
        let waited_ns = u64::try_from(wait_start.elapsed().as_nanos()).unwrap_or(u64::MAX);
        crate::mvcc::metrics::record_journal_mutex_wait_ns(waited_ns);
        #[cfg(any(test, feature = "test-hooks"))]
        let scope = self::test_accessors::us007_enter_journal_mutex_scope();
        JournalMutexGuard {
            _guard: guard,
            #[cfg(any(test, feature = "test-hooks"))]
            _scope: scope,
        }
    }

    fn flush_under_journal_mutex(&self) -> Result<()> {
        #[cfg(any(test, feature = "test-hooks"))]
        self::test_accessors::us007_record_flush();
        #[cfg(any(test, feature = "test-hooks"))]
        self::test_accessors::us026_fail_if_armed(&self.shared, Us026PostRegisterFailpoint::Flush)?;
        self.shared.handle.flush()
    }

    fn sync_journal_under_journal_mutex(&self) -> Result<()> {
        #[cfg(any(test, feature = "test-hooks"))]
        self::test_accessors::us007_record_sync();
        self.shared.handle.journal_sync()
    }

    fn fullsync_group_commit(&self) -> Result<()> {
        self.shared.group_commit.join_fsync_cohort(
            &self.shared,
            Duration::from_millis(FULLSYNC_GROUP_COMMIT_MAX_WAIT_MS),
            || {
                let _journal = self.lock_journal_mutex();
                self.flush_under_journal_mutex()?;
                self.sync_journal_under_journal_mutex()
            },
        )
    }

    fn namespace_catalog_identity(
        md: &MetadataState,
        ns: &str,
    ) -> Result<Option<NamespaceCatalogIdentity>> {
        let cat = catalog_lock(md);
        let Some(collection) = cat.get_collection(ns)? else {
            return Ok(None);
        };
        let indexes = cat
            .list_indexes(ns)?
            .into_iter()
            .map(|index| IndexCatalogIdentity {
                id: index.id,
                name: index.name,
                key_pattern: index.key_pattern,
                unique: index.unique,
                sparse: index.sparse,
                state: index.state,
            })
            .collect();
        Ok(Some(NamespaceCatalogIdentity {
            ns_id: collection.id,
            indexes,
        }))
    }

    /// Bootstrap a collection if it does not exist yet and return its
    /// durable namespace id.
    ///
    /// Called from CRUD paths that may be invoked with an unknown ns.
    /// Acquires `metadata.write()` through the namespace-create DDL
    /// envelope so the namespace is both visible in the catalog AND
    /// reflected in the published snapshot before the caller admits a
    /// writer ticket on the returned id.
    fn bootstrap_namespace(&self, ns: &str) -> Result<i64> {
        // §10.1.1 F5 retirement: the legacy name-keyed drop tombstone has been
        // retired. Durable monotonic ids (Phase 1 §10.7) are the resurrection
        // barrier; a freshly-bootstrapped collection of a previously-dropped
        // name receives a fresh `CollectionEntry.id`.
        self.run_namespace_create_ddl(|shared, md, batch| {
            let data_root = {
                let mut cat = catalog_lock(md);
                if let Some(entry) = cat.get_collection(ns)? {
                    return Ok(entry.id);
                }
                // Phase 1 §10.7 — allocate durable namespace id from the
                // header counter atomically with the catalog commit.
                let id = cat.allocate_namespace_id();
                let data_root = cat.create_collection(ns, id, bson::doc! {}, now_millis())?;
                (id, data_root)
            };
            sync_catalog_root_structural(shared, md, batch)?;
            let _ = BTree::create_at(new_structural_store(shared, batch), data_root.1)?;
            Ok(data_root.0)
        })
    }

    /// CRUD write lifecycle.
    ///
    /// Drives: metadata.read() → bootstrap-if-missing → private logical
    /// WriteTxn setup → body → install Pending deltas → journal_mutex {
    /// logical/chain journal appends + non-FullSync final flush } → flip
    /// Pending to Committed → publish.
    /// FullSync flush/sync ownership is the explicit sync boundary after the
    /// API write batch.
    fn run_write<F, R>(&self, ns: &str, f: F) -> Result<R>
    where
        F: FnOnce(&SharedState, &MetadataState, &mut WriteTxn, &WriteVisibility<'_>) -> Result<R>,
    {
        self.shared.check_engine_not_poisoned()?;
        // Take read guard; if namespace absent, bootstrap then retry.
        let md_read = self
            .metadata
            .read()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        let ns_missing = catalog_lock(&self.metadata_state)
            .get_collection(ns)?
            .is_none();
        if ns_missing {
            drop(md_read);
            let ns_id = self.bootstrap_namespace(ns)?;
            return self.run_write_bootstrapped(ns, ns_id, f);
        }
        drop(md_read);
        self.run_write_existing(ns, f)
    }

    fn run_write_existing<F, R>(&self, ns: &str, f: F) -> Result<R>
    where
        F: FnOnce(&SharedState, &MetadataState, &mut WriteTxn, &WriteVisibility<'_>) -> Result<R>,
    {
        self.run_write_inner(ns, None, f)
    }

    fn run_write_bootstrapped<F, R>(&self, ns: &str, ns_id: i64, f: F) -> Result<R>
    where
        F: FnOnce(&SharedState, &MetadataState, &mut WriteTxn, &WriteVisibility<'_>) -> Result<R>,
    {
        self.run_write_inner(ns, Some(ns_id), f)
    }

    /// Internal form of `run_write` that assumes the namespace already exists
    /// (or the write path tolerates its absence — `update`/`delete` do).
    ///
    /// Metadata-guard protocol: there is exactly one `metadata.read()`
    /// acquisition in this function. It is held across ordinary CRUD's full
    /// private-logical lifecycle so DDL cannot mutate the catalog identity
    /// while the writer installs resident Pending deltas, appends logical
    /// durability records, and publishes through the sequencer.
    /// `NsWriterRegistry` is no longer ordinary CRUD's same-namespace
    /// serialization authority.
    ///
    /// AC #4 captured-identity gate (§10.17.1): immediately before the
    /// durable journal envelope this function compares the
    /// `catalog_generation` captured in the S1 scope against the current
    /// published catalog generation. A mismatch triggers a target-namespace
    /// identity revalidation; if that namespace/index identity changed, the
    /// writer returns `WriteConflict { CatalogGenerationChanged }` while
    /// rollback is still purely in-memory. Catalog DDL on unrelated
    /// namespaces does not invalidate the writer's captured identity.
    fn run_write_inner<F, R>(&self, ns: &str, bootstrapped_ns_id: Option<i64>, f: F) -> Result<R>
    where
        F: FnOnce(&SharedState, &MetadataState, &mut WriteTxn, &WriteVisibility<'_>) -> Result<R>,
    {
        self.shared.check_engine_not_poisoned()?;
        let _checkpoint_writer_admission = self.shared.checkpoint_admission.admit_writer()?;
        let _crud_read = self
            .metadata
            .read()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        let (captured_ns_id, captured_catalog_gen, captured_catalog_identity) =
            if let Some(ns_id) = bootstrapped_ns_id {
                let captured_identity = Self::namespace_catalog_identity(&self.metadata_state, ns)?;
                let captured_gen = self.shared.published.load_full().catalog_generation;
                (Some(ns_id), captured_gen, captured_identity)
            } else {
                let captured_identity = Self::namespace_catalog_identity(&self.metadata_state, ns)?;
                let captured_ns_id_opt = captured_identity.as_ref().map(|identity| identity.ns_id);
                // §10.17.1 — published `catalog_generation` is the cheap
                // dirty bit for catalog DDL. CRUD never advances it; only
                // DDL does through the `next_catalog_gen` reservation. If it
                // changes before the AC #4 gate, we revalidate the captured
                // target namespace/index identity before deciding whether
                // this writer is stale. Use `published.load_full()` directly
                // because the §10.5 single-load gate is scoped to reads.
                let captured_gen = self.shared.published.load_full().catalog_generation;
                (captured_ns_id_opt, captured_gen, captured_identity)
            };
        // S2: create the writer visibility context held through S12.
        // COMMIT-ENVELOPE-RESIDUE: A (visibility setup fails before journal append).
        let vis = self.write_visibility_after_capture(ns, captured_ns_id)?;

        // Setup private logical write state. Journal rollback marks are
        // captured later, inside `journal_mutex`, immediately before this
        // transaction can append journal frames.
        let txn_id = vis.read_view.txn_id;
        let mut txn = WriteTxn::new(txn_id);

        // S3: execute the write body under the CRUD metadata read guard. The
        // catalog itself is behind `Mutex<Catalog>`, while page latches and
        // expected-head checks serialize resident chain mutation.
        // `journal_mutex` serializes only the durability envelope.
        #[cfg(any(test, feature = "test-hooks"))]
        self::test_accessors::write_body_entry_if_installed(&self.shared, ns);
        let body_result = f(&self.shared, &self.metadata_state, &mut txn, &vis);

        match body_result {
            Ok(value) => {
                // Root-neutral vs root-changing classification. Header sync
                // still captures catalog-root movement; logical-chain installs
                // can also make primary B-tree structural progress without
                // forcing a fresh catalog header.
                let mut root_changing = txn.structural_tree_change();

                // Refresh the logical-txn append-duration percentiles after
                // the journal envelope releases.
                // `LogicalTxnAppendPercentileRefresh::drop` runs the
                // sort+store work outside the critical section.
                let _logical_txn_append_pct_refresh = LogicalTxnAppendPercentileRefresh::new();

                let sec_writes = std::mem::take(&mut txn.pending_sec_index);
                let primary_writes = std::mem::take(&mut txn.pending_primary);

                // S3.5 captured-identity gate. Compare the
                // `catalog_generation` we snapshotted in the S1 metadata-read
                // scope against the live published generation. A DDL that
                // completed between capture and now
                // bumped `PublishedEpoch.catalog_generation` via the
                // `next_catalog_gen` reservation; ordinary CRUD never
                // bumps it. Because the generation is global, a mismatch is
                // only a signal to revalidate the target namespace/index
                // identity, not an automatic conflict for unrelated DDL.
                //
                // This gate runs before `register_with_oracle`, before any
                // Pending install, and before `journal_mutex` durability
                // begins. Rollback is purely in-memory.
                {
                    // Write-path direct load (§10.5 single-load gate is
                    // read-path only); see the matching note at the S1
                    // capture above.
                    let current_gen = self.shared.published.load_full().catalog_generation;
                    if current_gen != captured_catalog_gen
                        && Self::namespace_catalog_identity(&self.metadata_state, ns)?
                            != captured_catalog_identity
                    {
                        drop(txn);
                        return Err(Error::WriteConflict {
                            reason: WriteConflictReason::CatalogGenerationChanged,
                        });
                    }
                }

                let txn_id = txn.txn_id;

                let slot = match self.register_ordinary_crud_slot() {
                    Ok(slot) => slot,
                    Err(e) => {
                        drop(txn);
                        return Err(e);
                    }
                };
                let commit_ts = slot.commit_ts();
                txn.commit_ts.set(Some(commit_ts));
                let prev_published = self.shared.published.load_full();
                assert!(
                    commit_ts > prev_published.visible_ts,
                    "commit_ts must advance beyond previous PublishedEpoch"
                );
                drop(prev_published);

                let frame =
                    txn.build_logical_txn_frame(&self.shared.handle, &primary_writes, &sec_writes);

                let dirty = txn.publish_dirty();

                let sec_pages = match self.install_pending_sec_index_with_retry(
                    &self.metadata_state,
                    &sec_writes,
                    &vis,
                    commit_ts,
                    txn_id,
                ) {
                    Ok(pages) => pages,
                    Err(e) => {
                        drop(txn);
                        return Err(
                            self.cleanup_registered_pre_durable_failure(txn_id, slot, None, e)
                        );
                    }
                };

                let primary_install = self.install_pending_primary_with_retry(
                    &self.metadata_state,
                    &primary_writes,
                    &vis,
                    commit_ts,
                    txn_id,
                );
                let (primary_pages, primary_structural_tree_change) = match primary_install {
                    Ok(result) => result,
                    Err(e) => {
                        drop(txn);
                        return Err(
                            self.cleanup_registered_pre_durable_failure(txn_id, slot, None, e)
                        );
                    }
                };
                root_changing |= primary_structural_tree_change;
                let mut pending_pages = sec_pages;
                pending_pages.extend(primary_pages);

                {
                    let _journal = self.lock_journal_mutex();
                    #[cfg(any(test, feature = "test-hooks"))]
                    if let Err(e) = self::test_accessors::us026_fail_if_armed(
                        &self.shared,
                        Us026PostRegisterFailpoint::BeginTxnAfterRegister,
                    ) {
                        return Err(
                            self.cleanup_registered_pre_durable_failure(txn_id, slot, None, e)
                        );
                    }
                    let commit_mark = match self.shared.handle.begin_txn() {
                        Ok(mark) => mark,
                        Err(e) => {
                            return Err(
                                self.cleanup_registered_pre_durable_failure(txn_id, slot, None, e)
                            );
                        }
                    };
                    #[cfg(any(test, feature = "test-hooks"))]
                    if let Err(e) =
                        self::test_accessors::us007_after_begin_if_installed(&self.shared)
                    {
                        return Err(self.cleanup_registered_pre_durable_failure(
                            txn_id,
                            slot,
                            commit_mark,
                            e,
                        ));
                    }

                    #[cfg(any(test, feature = "test-hooks"))]
                    self::test_accessors::phase3_abort_if_armed(
                        Phase3CommitFailpoint::BeforeLogicalTxnAppend,
                    );

                    #[cfg(any(test, feature = "test-hooks"))]
                    if let Err(e) = self::test_accessors::us026_fail_if_armed(
                        &self.shared,
                        Us026PostRegisterFailpoint::EmitLogicalTxnFrame,
                    ) {
                        return Err(self.cleanup_registered_pre_durable_failure(
                            txn_id,
                            slot,
                            commit_mark,
                            e,
                        ));
                    }
                    if let Err(e) = self.shared.handle.append_logical_txn(frame) {
                        return Err(self.cleanup_registered_pre_durable_failure(
                            txn_id,
                            slot,
                            commit_mark,
                            e,
                        ));
                    }
                    #[cfg(any(test, feature = "test-hooks"))]
                    self::test_accessors::phase3_abort_if_armed(
                        Phase3CommitFailpoint::AfterLogicalTxnAppendBeforeFsync,
                    );
                    #[cfg(any(test, feature = "test-hooks"))]
                    self::test_accessors::phase3_abort_if_armed(
                        Phase3CommitFailpoint::AfterLogicalTxnFsyncBeforeChainCommit,
                    );

                    #[cfg(any(test, feature = "test-hooks"))]
                    if let Err(e) = self::test_accessors::us026_fail_if_armed(
                        &self.shared,
                        Us026PostRegisterFailpoint::ChainCommitAppend,
                    ) {
                        return Err(self.cleanup_registered_pre_durable_failure(
                            txn_id,
                            slot,
                            commit_mark,
                            e,
                        ));
                    }
                    if let Err(e) = txn.commit_chain_commit(&self.shared.handle, commit_ts) {
                        return Err(self.cleanup_registered_pre_durable_failure(
                            txn_id,
                            slot,
                            commit_mark,
                            e,
                        ));
                    }
                    #[cfg(any(test, feature = "test-hooks"))]
                    self::test_accessors::phase3_abort_if_armed(
                        Phase3CommitFailpoint::AfterChainCommitBeforeLegacyCommit,
                    );

                    // FullSync owns the final data flush at the explicit sync
                    // boundary; non-FullSync preserves the existing
                    // per-writer flush-before-publish behavior.
                    if !matches!(self.durability_mode, DurabilityMode::FullSync) {
                        if let Err(e) = self.flush_under_journal_mutex() {
                            return Err(self.cleanup_registered_pre_durable_failure(
                                txn_id,
                                slot,
                                commit_mark,
                                e,
                            ));
                        }
                    }
                }

                if matches!(self.durability_mode, DurabilityMode::FullSync) {
                    if let Err(e) = self.fullsync_group_commit() {
                        return Err(e);
                    }
                }

                #[cfg(test)]
                self::test_accessors::publish_pause_if_installed(&self.shared);

                // S9: journal_mutex has been released before the
                // Pending-to-Committed flip. The flip runs before publish so
                // no reader can observe the new epoch with uncommitted heads.
                flip_pending_to_committed_for(&self.shared, txn_id, commit_ts, &pending_pages)
                    .map_err(|_| {
                        self.engine_fatal(
                            crate::error::EngineFatalReason::PostDurablePendingFlipFailure,
                        )
                    })?;
                #[cfg(any(test, feature = "test-hooks"))]
                {
                    self::test_accessors::us009_record_committed_flip(&self.shared);
                    if self::test_accessors::us009_fail_after_committed_flip_if_armed(&self.shared)
                        .is_err()
                    {
                        return Err(self.engine_fatal(
                            crate::error::EngineFatalReason::PostDurablePendingFlipFailure,
                        ));
                    }
                }

                #[cfg(any(test, feature = "test-hooks"))]
                self::test_accessors::phase3_abort_if_armed(
                    Phase3CommitFailpoint::AfterLegacyCommitBeforePublish,
                );

                let shared = Arc::clone(&self.shared);
                let metadata_state = Arc::clone(&self.metadata_state);
                let publish_result =
                    self.shared
                        .publish_sequencer
                        .mark_ready(slot, move |publish_ts| {
                            #[cfg(any(test, feature = "test-hooks"))]
                            self::test_accessors::us009_record_publish_ready(&shared);
                            rebuild_and_publish_locked(
                                &shared,
                                &metadata_state,
                                publish_ts,
                                dirty,
                                None,
                            )
                        });
                match publish_result {
                    Ok(()) => {}
                    Err(Error::EngineFatal { reason }) => {
                        return Err(Error::EngineFatal { reason });
                    }
                    Err(_) => {
                        return Err(self.engine_fatal(
                            crate::error::EngineFatalReason::PostDurablePublishFailure,
                        ));
                    }
                }

                if root_changing {
                    crate::mvcc::metrics::record_crud_commit_root_changing();
                } else {
                    crate::mvcc::metrics::record_crud_commit_root_neutral();
                }
                Ok(value)
            }
            Err(e) => {
                // COMMIT-ENVELOPE-RESIDUE: A (S3 body failure before journal append).
                drop(txn);
                Err(e)
            }
        }
    }

    fn register_ordinary_crud_slot(&self) -> Result<self::publish_sequencer::PublishSlotGuard> {
        let publish_sequencer = &self.shared.publish_sequencer;
        publish_sequencer.register_with_oracle(&self.shared.oracle)
    }

    fn cleanup_registered_pre_durable_failure(
        &self,
        txn_id: u64,
        slot: self::publish_sequencer::PublishSlotGuard,
        mark: Option<u64>,
        error: Error,
    ) -> Error {
        let _ = flip_pending_to_aborted_for(&self.shared, txn_id);
        self.shared.publish_sequencer.mark_aborted(slot);
        #[cfg(any(test, feature = "test-hooks"))]
        self::test_accessors::us026_note_cleanup_rollback_attempt(&self.shared);
        let rollback_result = self.shared.handle.rollback_txn(mark);
        #[cfg(any(test, feature = "test-hooks"))]
        let rollback_result = self::test_accessors::us026_maybe_force_cleanup_rollback_failure(
            &self.shared,
            rollback_result,
        );
        let _ = rollback_result;
        error
    }

    fn write_visibility_after_capture(
        &self,
        ns: &str,
        captured_ns_id: Option<i64>,
    ) -> Result<WriteVisibility<'_>> {
        let start = Instant::now();
        loop {
            match WriteVisibility::new(&self.shared, ns) {
                Ok(vis) => return Ok(vis),
                Err(Error::CollectionNotFound { .. })
                    if captured_ns_id.is_some() && start.elapsed() < self.busy_timeout =>
                {
                    std::thread::yield_now();
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn install_pending_sec_index_with_retry(
        &self,
        md: &MetadataState,
        writes: &[crate::mvcc::SecIndexWrite],
        vis: &WriteVisibility<'_>,
        commit_ts: crate::mvcc::Ts,
        txn_id: u64,
    ) -> Result<Vec<u32>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        match install_pending_sec_index(&self.shared, md, writes.to_vec(), vis, commit_ts, txn_id) {
            Ok(pages) => return Ok(pages),
            Err(e @ Error::WriteConflict { .. }) => return Err(e),
            Err(_) => {}
        }
        match install_pending_sec_index(&self.shared, md, writes.to_vec(), vis, commit_ts, txn_id) {
            Ok(pages) => Ok(pages),
            Err(e @ Error::WriteConflict { .. }) => Err(e),
            Err(e) => Err(e),
        }
    }

    fn install_pending_primary_with_retry(
        &self,
        md: &MetadataState,
        writes: &[crate::mvcc::PrimaryWrite],
        vis: &WriteVisibility<'_>,
        commit_ts: crate::mvcc::Ts,
        txn_id: u64,
    ) -> Result<(Vec<u32>, bool)> {
        if writes.is_empty() {
            return Ok((Vec::new(), false));
        }
        #[cfg(any(test, feature = "test-hooks"))]
        let first_attempt = self::test_accessors::us019_maybe_fail_primary_install(&self.shared)
            .and_then(|()| {
                install_pending_primary(&self.shared, md, writes.to_vec(), vis, commit_ts, txn_id)
            });
        #[cfg(not(any(test, feature = "test-hooks")))]
        let first_attempt =
            install_pending_primary(&self.shared, md, writes.to_vec(), vis, commit_ts, txn_id);
        match first_attempt {
            Ok(pages) => return Ok(pages),
            Err(e @ Error::WriteConflict { .. }) => return Err(e),
            Err(_) => {}
        }
        #[cfg(any(test, feature = "test-hooks"))]
        let second_attempt = self::test_accessors::us019_maybe_fail_primary_install(&self.shared)
            .and_then(|()| {
                install_pending_primary(&self.shared, md, writes.to_vec(), vis, commit_ts, txn_id)
            });
        #[cfg(not(any(test, feature = "test-hooks")))]
        let second_attempt =
            install_pending_primary(&self.shared, md, writes.to_vec(), vis, commit_ts, txn_id);
        match second_attempt {
            Ok(pages) => Ok(pages),
            Err(e @ Error::WriteConflict { .. }) => Err(e),
            Err(e) => Err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// StorageEngine implementation
// ---------------------------------------------------------------------------

impl StorageEngine for PagedEngine {
    fn insert(&self, ns: &str, doc: Document) -> Result<Bson> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::insert(self, ns, doc)
    }

    fn find(
        &self,
        ns: &str,
        filter: &Document,
        opts: &FindOptions,
    ) -> Result<(Vec<Document>, crate::query::explain::ExplainResult)> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::find(self, ns, filter, opts)
    }

    fn find_one(&self, ns: &str, filter: &Document) -> Result<Option<Document>> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::find_one(self, ns, filter)
    }

    fn update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &UpdateOptions,
        many: bool,
    ) -> Result<UpdateResult> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::update(self, ns, filter, update, opts, many)
    }

    fn delete(&self, ns: &str, filter: &Document, many: bool) -> Result<DeleteResult> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::delete(self, ns, filter, many)
    }

    fn count(&self, ns: &str, filter: &Document) -> Result<u64> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::count(self, ns, filter)
    }

    fn find_one_and_update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &FindOneAndUpdateOptions,
    ) -> Result<Option<Document>> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::find_one_and_update_doc(self, ns, filter, update, opts)
    }

    fn find_one_and_delete(
        &self,
        ns: &str,
        filter: &Document,
        opts: &FindOneAndDeleteOptions,
    ) -> Result<Option<Document>> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::find_one_and_delete_doc(self, ns, filter, opts)
    }

    fn find_one_and_replace(
        &self,
        ns: &str,
        filter: &Document,
        replacement: &Document,
        opts: &FindOneAndReplaceOptions,
    ) -> Result<Option<Document>> {
        self.shared.check_engine_not_poisoned()?;
        doc_ops::find_one_and_replace_doc(self, ns, filter, replacement, opts)
    }

    fn create_index(&self, ns: &str, model: &IndexModel) -> Result<String> {
        self.shared.check_engine_not_poisoned()?;
        index_maint::create_index(self, ns, model)
    }

    fn drop_index(&self, ns: &str, name: &str) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        index_maint::drop_index(self, ns, name)
    }

    fn list_indexes(&self, ns: &str) -> Result<Vec<IndexInfo>> {
        self.shared.check_engine_not_poisoned()?;
        index_maint::list_indexes(self, ns)
    }

    // -----------------------------------------------------------------------
    // create_namespace
    // -----------------------------------------------------------------------

    fn create_namespace(&self, ns: &str) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        let result = self.run_namespace_create_ddl(|shared, md, batch| {
            let data_root = {
                let mut cat = catalog_lock(md);
                if cat.get_collection(ns)?.is_some() {
                    return Err(Error::DuplicateKey {
                        detail: format!("collection '{ns}' already exists"),
                    });
                }
                // Phase 1 §10.7 — allocate durable namespace id from the
                // header counter atomically with the catalog commit.
                let id = cat.allocate_namespace_id();
                cat.create_collection(ns, id, bson::doc! {}, now_millis())?
            };
            sync_catalog_root_structural(shared, md, batch)?;
            let store = new_structural_store(shared, batch);
            BTree::create_at(store, data_root)?;
            Ok(())
        });
        // §10.1.1 F5 retirement: no name-based drop tombstone to clear.
        result
    }

    // -----------------------------------------------------------------------
    // drop_namespace
    // -----------------------------------------------------------------------

    fn drop_namespace(&self, ns: &str) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        let stale_target = || Error::WriteConflict {
            reason: WriteConflictReason::CatalogGenerationChanged,
        };

        let _md_w = self
            .metadata
            .write()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        let Some(target_collection) = ({
            let cat = catalog_lock(&self.metadata_state);
            cat.get_collection(ns)?
        }) else {
            return Ok(());
        };
        let ns_id = target_collection.id;
        let data_root = target_collection.data_root_page;
        let data_level = target_collection.data_root_level;
        let index_roots: Vec<(u32, u8)> = {
            let cat = catalog_lock(&self.metadata_state);
            cat.list_indexes(ns)?
                .into_iter()
                .map(|entry| (entry.root_page, entry.root_level))
                .collect()
        };

        let mut guard = self
            .shared
            .ns_writers
            .close_and_drain_guard(ns_id, self.busy_timeout)?;
        // Force-expire ALL active ReadViews globally before freeing pages.
        self.shared.handle.read_view_registry().force_expire_all();

        let reserved_gen = self
            .shared
            .next_catalog_gen
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        let slot = self
            .shared
            .publish_sequencer
            .register_with_oracle(&self.shared.oracle)?;

        let mut durable = false;
        let drop_result = (|| -> Result<()> {
            let _journal = self.lock_journal_mutex();
            let mark = self.shared.handle.begin_txn()?;
            let mut batch = StructuralPageBatch::new(&self.shared.handle);

            let body = (|| -> Result<()> {
                {
                    let cat = catalog_lock(&self.metadata_state);
                    let collection =
                        cat.get_collection(ns)?
                            .ok_or_else(|| Error::CollectionNotFound {
                                name: ns.to_owned(),
                            })?;
                    if collection.id != ns_id {
                        return Err(stale_target());
                    }
                }

                self.free_tree_pages_exclusive(&mut batch, data_root, data_level)?;
                for (root_page, root_level) in &index_roots {
                    self.free_tree_pages_exclusive(&mut batch, *root_page, *root_level)?;
                }

                {
                    let mut cat = catalog_lock(&self.metadata_state);
                    let collection =
                        cat.get_collection(ns)?
                            .ok_or_else(|| Error::CollectionNotFound {
                                name: ns.to_owned(),
                            })?;
                    if collection.id != ns_id {
                        return Err(stale_target());
                    }
                    let dropped = cat.drop_collection(ns)?;
                    if !dropped {
                        return Err(Error::CollectionNotFound {
                            name: ns.to_owned(),
                        });
                    }
                }
                sync_catalog_root_structural(&self.shared, &self.metadata_state, &mut batch)?;
                self.shared.clear_dirty_collection(ns_id);
                Ok(())
            })();

            match body {
                Ok(()) => {
                    let mut base_store = new_store(&self.shared);
                    let commit_result = batch.commit(&mut base_store, &self.shared.handle);
                    commit_result.map_err(|_| {
                        self.engine_fatal(
                            crate::error::EngineFatalReason::PostDurableDdlPublishFailure,
                        )
                    })?;
                    self.flush_under_journal_mutex().map_err(|_| {
                        self.engine_fatal(
                            crate::error::EngineFatalReason::PostDurableDdlPublishFailure,
                        )
                    })?;
                    durable = true;
                    Ok(())
                }
                Err(e) => {
                    let _ = batch.abort(&self.shared.handle);
                    let _ = self.shared.handle.rollback_txn(mark);
                    Err(e)
                }
            }
        })();

        match drop_result {
            Ok(()) => {}
            Err(Error::EngineFatal { reason }) => return Err(Error::EngineFatal { reason }),
            Err(_e) if durable => {
                return Err(self
                    .engine_fatal(crate::error::EngineFatalReason::PostDurableDdlPublishFailure));
            }
            Err(e) => {
                self.shared.publish_sequencer.mark_aborted(slot);
                return Err(e);
            }
        }

        let dirty = PublishDirty {
            published_catalog_dirty: true,
            catalog_header_dirty: true,
        };
        let shared = Arc::clone(&self.shared);
        let metadata_state = Arc::clone(&self.metadata_state);
        let publish_result = self
            .shared
            .publish_sequencer
            .mark_ready(slot, move |publish_ts| {
                rebuild_and_publish_locked(
                    &shared,
                    &metadata_state,
                    publish_ts,
                    dirty,
                    Some(reserved_gen),
                )
            });
        match publish_result {
            Ok(()) => {}
            Err(Error::EngineFatal { reason }) => return Err(Error::EngineFatal { reason }),
            Err(_) => {
                return Err(self
                    .engine_fatal(crate::error::EngineFatalReason::PostDurableDdlPublishFailure))
            }
        }
        // §10.1.1 F5 retirement: durable monotonic ns_ids are the
        // resurrection barrier; no name-based drop tombstone is inserted.
        guard.mark_dropped();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // list_namespaces
    // -----------------------------------------------------------------------

    fn list_namespaces(&self) -> Result<Vec<String>> {
        self.shared.check_engine_not_poisoned()?;
        let snap = self.shared.load_published();
        let keys = snap.catalog.namespace_id_by_name.keys();
        let mut out = Vec::with_capacity(keys.len());
        out.extend(keys.cloned());
        Ok(out)
    }

    fn checkpoint(&self) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        snapshot_ops::checkpoint(self)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn read_view_registry(&self) -> Option<Arc<crate::mvcc::ReadViewRegistry>> {
        Some(Arc::clone(self.shared.handle.read_view_registry()))
    }

    // Test-only trait methods — implementations live in
    // `src/storage/paged_engine/test_accessors.rs` so the production
    // impl stays free of test-scaffolding logic.
    #[cfg(any(test, feature = "test-hooks"))]
    fn oracle_now(&self) -> (u64, u32) {
        self.test_oracle_now()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn published_visible_ts(&self) -> (u64, u32) {
        self.test_published_visible_ts()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn published_catalog_gen(&self) -> u64 {
        self.test_published_catalog_gen()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn published_sequencer_frontier(&self) -> (u64, u32) {
        self.test_published_sequencer_frontier()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn recovery_open_published_store_count(&self) -> u64 {
        self.test_recovery_open_published_store_count()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn recovered_max_commit_ts(&self) -> Option<(u64, u32)> {
        self.test_recovered_max_commit_ts()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us019_set_primary_install_failures(&self, failures: u8) {
        self.test_us019_set_primary_install_failures(failures);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us019_primary_install_attempts(&self) -> u64 {
        self.test_us019_primary_install_attempts()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us009_primary_chain_states(&self, ns: &str, id: &Bson) -> Result<Vec<String>> {
        self.test_us009_primary_chain_states(ns, id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us009_inject_primary_committed_head(
        &self,
        ns: &str,
        doc: &Document,
        commit_ts: crate::mvcc::Ts,
        txn_id: u64,
    ) -> Result<()> {
        self.test_us009_inject_primary_committed_head(ns, doc, commit_ts, txn_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us009_secondary_chain_states(
        &self,
        ns: &str,
        index_name: &str,
        doc: &Document,
        id: &Bson,
    ) -> Result<Vec<String>> {
        self.test_us009_secondary_chain_states(ns, index_name, doc, id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us009_reset_flip_publish_order(&self) {
        self.test_us009_reset_flip_publish_order();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us009_flip_publish_order(&self) -> (u64, u64) {
        self.test_us009_flip_publish_order()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us009_fail_after_committed_flip_once(&self) {
        self.test_us009_fail_after_committed_flip_once();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us028_primary_leaf_for_id(&self, ns: &str, id: &Bson) -> Result<u32> {
        self.test_us028_primary_leaf_for_id(ns, id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us022_insert_two_docs_one_txn(
        &self,
        ns: &str,
        left: Document,
        right: Document,
    ) -> Result<()> {
        self.test_us022_insert_two_docs_one_txn(ns, left, right)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us028_hold_primary_leaf_reconcile_latch(
        &self,
        ns: &str,
        id: &Bson,
        ready: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Result<()> {
        self.test_us028_hold_primary_leaf_reconcile_latch(ns, id, ready, release)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us028_hold_primary_leaf_writer_latch(
        &self,
        ns: &str,
        id: &Bson,
        ready: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Result<()> {
        self.test_us028_hold_primary_leaf_writer_latch(ns, id, ready, release)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us025_hold_primary_leaf_reader_latch(
        &self,
        ns: &str,
        id: &Bson,
        ready: std::sync::mpsc::Sender<()>,
        release: std::sync::mpsc::Receiver<()>,
    ) -> Result<()> {
        self.test_us025_hold_primary_leaf_reader_latch(ns, id, ready, release)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us026_arm_post_register_failpoint(
        &self,
        failpoint: self::test_accessors::Us026PostRegisterFailpoint,
    ) {
        self.test_us026_arm_post_register_failpoint(failpoint);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us026_cleanup_observations(&self) -> self::test_accessors::Us026CleanupObservations {
        self.test_us026_cleanup_observations()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn install_write_body_entry_hook(
        &self,
        ns: &str,
        observe_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> self::test_accessors::WriteBodyEntryHookGuard {
        self.test_install_write_body_entry_hook(ns, observe_flag)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn install_create_index_build_hook(
        &self,
        ns: &str,
        index_name: &str,
    ) -> self::test_accessors::CreateIndexBuildHookGuard {
        self.test_install_create_index_build_hook(ns, index_name)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn install_create_index_build_failure_hook(
        &self,
        ns: &str,
        index_name: &str,
    ) -> self::test_accessors::CreateIndexBuildHookGuard {
        self.test_install_create_index_build_failure_hook(ns, index_name)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us007_install_journal_begin_hook(
        &self,
        fail_after_release: bool,
    ) -> self::test_accessors::Us007JournalBeginHookGuard {
        self.test_us007_install_journal_begin_hook(fail_after_release)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us007_reset_journal_observations(&self) {
        self.test_us007_reset_journal_observations();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us007_journal_observations(&self) -> self::test_accessors::Us007JournalObservations {
        self.test_us007_journal_observations()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us017_reset_group_commit_probe(&self) {
        self::group_commit_test_probe::reset();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us017_expect_group_commit_cohort_size(&self, expected: u64) {
        self::group_commit_test_probe::set_expected_cohort_size(expected);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us017_fail_next_group_commit_fsync(&self) {
        self::group_commit_test_probe::fail_next_fsync();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us017_pause_next_group_commit_after_close(
        &self,
    ) -> self::group_commit_test_probe::Us017GroupCommitPauseGuard {
        self::group_commit_test_probe::install_pause_after_close()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us017_group_commit_observations(
        &self,
    ) -> self::group_commit_test_probe::Us017GroupCommitObservations {
        self::group_commit_test_probe::observations(&self.shared.group_commit)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us008_reset_structural_page_observations(&self) {
        self.test_us008_reset_structural_page_observations();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us008_committed_structural_leaf_bytes(&self) -> u64 {
        self.test_us008_committed_structural_leaf_bytes()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us011_install_pending_unique_email(
        &self,
        ns: &str,
        index_name: &str,
        id: Bson,
        email: &str,
        txn_id: u64,
    ) -> Result<()> {
        self.test_us011_install_pending_unique_email(ns, index_name, id, email, txn_id)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us011_unique_prefix_sibling_pages(&self) -> Result<Vec<u32>> {
        self.test_us011_unique_prefix_sibling_pages()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn crash_cut_probe_insert(
        &self,
        ns: &str,
        doc: Document,
        cut: Phase0ProbeCut,
    ) -> Result<Phase0ProbeReport> {
        self.crash_cut_probe_insert_impl(ns, doc, cut)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us036_test_poison_engine(&self, reason: crate::error::EngineFatalReason) {
        self.us036_test_poison_engine(reason);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us036_test_poisoned_reason(&self) -> Option<crate::error::EngineFatalReason> {
        self.us036_test_poisoned_reason()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us036_test_register_publish_slot(
        &self,
    ) -> Result<self::engine_fatal_test_probe::Us036PublishSlot> {
        self.us036_test_register_publish_slot()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us036_test_admit_writer(
        &self,
        ns_id: i64,
        timeout_ms: u64,
    ) -> Result<self::engine_fatal_test_probe::Us036WriterTicket> {
        self.us036_test_admit_writer(ns_id, timeout_ms)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us036_test_close_and_drain(&self, ns_id: i64, timeout_ms: u64) -> Result<()> {
        self.us036_test_close_and_drain(ns_id, timeout_ms)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn us036_test_namespace_id(&self, ns: &str) -> Result<Option<i64>> {
        self.us036_test_namespace_id(ns)
    }

    fn close(&self) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        snapshot_ops::close(self)
    }

    fn journal_sync(&self) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        snapshot_ops::journal_sync(self)
    }

    fn snapshot_bytes(&self) -> Result<Option<Vec<u8>>> {
        self.shared.check_engine_not_poisoned()?;
        snapshot_ops::snapshot_bytes(self)
    }
}

// ---------------------------------------------------------------------------
// DDL helper + upsert helpers (private)
// ---------------------------------------------------------------------------

impl PagedEngine {
    /// Drive the two namespace-create paths with the bootstrap DDL envelope.
    ///
    /// This helper is intentionally narrower than `run_ddl`: it handles
    /// standalone `create_namespace` and write-path bootstrap on allocated-id
    /// publication, while existing-namespace DDL drains remain separate.
    fn run_namespace_create_ddl<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&SharedState, &MetadataState, &mut StructuralPageBatch) -> Result<R>,
    {
        self.shared.check_engine_not_poisoned()?;
        let _md_w = self
            .metadata
            .write()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        let reserved_gen = self
            .shared
            .next_catalog_gen
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        let slot = self
            .shared
            .publish_sequencer
            .register_with_oracle(&self.shared.oracle)?;
        let publish_dirty = PublishDirty {
            published_catalog_dirty: true,
            catalog_header_dirty: true,
        };

        let mut batch = StructuralPageBatch::new(&self.shared.handle);

        let mut durable = false;
        let journal_result = {
            let _journal = self.lock_journal_mutex();
            let mark = self.shared.handle.begin_txn()?;
            match f(&self.shared, &self.metadata_state, &mut batch) {
                Ok(value) => {
                    let mut base_store = new_store(&self.shared);
                    if let Err(e) = batch.commit(&mut base_store, &self.shared.handle) {
                        return Err(match e {
                            Error::EngineFatal { reason } => Error::EngineFatal { reason },
                            _ => self.engine_fatal(
                                crate::error::EngineFatalReason::PostDurableDdlPublishFailure,
                            ),
                        });
                    }
                    self.flush_under_journal_mutex().map_err(|_| {
                        self.engine_fatal(
                            crate::error::EngineFatalReason::PostDurableDdlPublishFailure,
                        )
                    })?;
                    durable = true;
                    Ok(value)
                }
                Err(e) => {
                    batch.abort(&self.shared.handle)?;
                    let _ = self.shared.handle.rollback_txn(mark);
                    Err(e)
                }
            }
        };

        let value = match journal_result {
            Ok(value) => value,
            Err(Error::EngineFatal { reason }) => return Err(Error::EngineFatal { reason }),
            Err(_e) if durable => {
                return Err(self
                    .engine_fatal(crate::error::EngineFatalReason::PostDurableDdlPublishFailure));
            }
            Err(e) => return Err(e),
        };

        let shared = Arc::clone(&self.shared);
        let metadata_state = Arc::clone(&self.metadata_state);
        let publish_result = self
            .shared
            .publish_sequencer
            .mark_ready(slot, move |publish_ts| {
                rebuild_and_publish_locked(
                    &shared,
                    &metadata_state,
                    publish_ts,
                    publish_dirty,
                    Some(reserved_gen),
                )
            });
        match publish_result {
            Ok(()) => Ok(value),
            Err(Error::EngineFatal { reason }) => Err(Error::EngineFatal { reason }),
            Err(_) => {
                Err(self
                    .engine_fatal(crate::error::EngineFatalReason::PostDurableDdlPublishFailure))
            }
        }
    }

    fn free_tree_pages_exclusive(
        &self,
        batch: &mut StructuralPageBatch,
        root_page: u32,
        root_level: u8,
    ) -> Result<()> {
        let mut tree = BTree::open(
            new_structural_store(&self.shared, batch),
            root_page,
            root_level,
        );
        let mut pages = tree.collect_pages_by_size()?;
        pages.sort_by_key(|(page_id, _)| *page_id);
        let latches = pages
            .iter()
            .map(|(page_id, size)| {
                self.shared
                    .handle
                    .pool()
                    .pin_for_write_sized(*page_id, *size)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut store = new_structural_store(&self.shared, batch);
        for (page_id, size) in pages {
            match size {
                PageSize::Small4k => store.free_internal(page_id)?,
                PageSize::Large32k => {
                    store.clear_chains(page_id)?;
                    store.free_leaf(page_id)?;
                }
            }
        }
        drop(latches);
        Ok(())
    }
}
