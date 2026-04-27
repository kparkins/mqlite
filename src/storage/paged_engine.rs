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
mod doc_helpers;
mod doc_ops;
mod index_build;
mod index_maint;
#[cfg(any(test, feature = "test-hooks"))]
mod phase0_probe;
pub(crate) mod publish;
mod recovery_apply;
mod snapshot_ops;
mod state;
/// Test-only `impl PagedEngine` accessors — isolated from the
/// production code path in a separate file so the boundary is
/// visible at a glance.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) mod test_accessors;
mod visibility;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod us007_tests;
#[cfg(test)]
mod us008_tests;
#[cfg(test)]
mod us009_tests;
#[cfg(test)]
mod us011_tests;
#[cfg(test)]
mod us012_tests;
#[cfg(test)]
mod us018c_tests;
#[cfg(test)]
mod us020_tests;

#[cfg(test)]
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

#[cfg(any(test, feature = "test-hooks"))]
use self::test_accessors::Phase3CommitFailpoint;
use parking_lot::Mutex as ParkingMutex;

use dashmap::{DashMap, DashSet};

use crate::options::BusyHandler;

use bson::{Bson, Document};

use super::engine::StorageEngine;
#[cfg(any(test, feature = "test-hooks"))]
use super::phase0_probe::{Phase0ProbeCut, Phase0ProbeReport};
use crate::error::{Error, Result};
use crate::index::{IndexInfo, IndexModel};
use crate::mvcc::transaction::WriteTxn;
use crate::options::{
    FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
    UpdateOptions,
};
use crate::results::{DeleteResult, UpdateResult};
use crate::storage::btree::BTree;
use crate::storage::buffer_pool::PageSize;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::txn_page_store::{PageOrigin, PageReservation, TxnOverlay};

use self::catalog_ops::{
    catalog_lock, new_store, new_txn_store, rebuild_and_publish_locked, sync_catalog_root_overlay,
};
use self::doc_helpers::now_millis;
use self::index_maint::{
    commit_pending_primary_states, commit_pending_primary_states_with_overlay,
    commit_pending_sec_index_states, install_pending_primary, install_pending_sec_index,
};
use self::publish::PublishDirty;
use self::state::{MetadataState, OwnedLaneGuard, SharedState};
use self::visibility::WriteVisibility;

// ---------------------------------------------------------------------------
// PagedEngine — public struct
// ---------------------------------------------------------------------------

/// Storage engine: B+ tree per namespace, through the buffer pool.
///
/// ## Concurrency
///
/// - **Reads**: mutex-free — load `shared.published` (`ArcSwap`) and open
///   B-trees at the snapshot's root pages. No engine-level lock taken.
/// - **Writes on CRUD paths**: acquire the per-namespace lane from
///   `ns_lanes`, then `metadata.read()` (shared). Two writers on different
///   namespaces progress concurrently. Commit sequencing (S4-S12) is
///   serialized under `commit_seq`.
/// - **DDL** (`create_namespace`, `drop_namespace`, `create_index`,
///   `drop_index`, `checkpoint`, `close`, `backup`): takes `metadata.write()`
///   exclusively, blocking all writers globally for the duration.
pub(crate) struct PagedEngine {
    /// Shared state accessible by the mutex-free read path and every writer.
    pub(crate) shared: Arc<SharedState>,
    /// Catalog protected by an `RwLock` — DDL takes write, CRUD takes read.
    metadata: RwLock<MetadataState>,
    /// Per-namespace write lanes. Two writers on different namespaces run
    /// in parallel; two writers on the same namespace serialize on the
    /// lane mutex.
    ns_lanes: DashMap<String, Arc<ParkingMutex<()>>>,
    /// Commit-sequencing mutex. Ordinary CRUD writes acquire it at S4 and
    /// hold it through S12: allocate `commit_ts`, append/fsync logical
    /// transaction, append/fsync ChainCommit, install Pending heads, flush
    /// structural bytes, then publish one `PublishedEpoch`.
    commit_seq: Mutex<()>,
    /// Writer busy-timeout applied on lane contention.
    busy_timeout: Duration,
    /// Optional writer busy-handler callback applied on lane contention.
    busy_handler: Option<BusyHandler>,
    /// Namespaces that have been explicitly dropped in this session.
    ///
    /// Prevents auto-bootstrap (`run_write` → `bootstrap_namespace`) from
    /// re-creating a namespace immediately after it was dropped — which would
    /// leave stale data in the journal and cause surprising reopen semantics.
    /// Cleared when the namespace is explicitly re-created via
    /// `create_namespace` (the public `create_collection` API path).
    dropped_namespaces: DashSet<String>,
}

/// RAII recorder for lane-wait timing.
///
/// Keeps the observational counter fetch_add OUTSIDE the lane mutex's
/// critical section (§5 cross-boundary guardrail). The recorder is declared
/// BEFORE the lane `MutexGuard` so Rust's LIFO drop order guarantees the
/// guard is released first and `drop(self)` runs with no lock held.
struct LaneWaitRecord {
    /// Measured lane-wait duration. `None` before the guard is acquired;
    /// populated immediately after `acquire_lane` returns.
    elapsed: Option<Duration>,
}

impl Drop for LaneWaitRecord {
    fn drop(&mut self) {
        if let Some(d) = self.elapsed {
            crate::mvcc::metrics::record_lane_wait_ns(d.as_nanos() as u64);
        }
    }
}

/// RAII recorder for commit_seq-wait timing.
///
/// Mirrors [`LaneWaitRecord`] for the commit_seq mutex: declared BEFORE the
/// `MutexGuard`, so the atomic fetch_add runs after the mutex is released.
struct CommitSeqWaitRecord {
    /// Measured commit_seq-wait duration. `None` before the guard is
    /// acquired; populated immediately after `commit_seq.lock()` returns.
    elapsed: Option<Duration>,
}

impl Drop for CommitSeqWaitRecord {
    fn drop(&mut self) {
        if let Some(d) = self.elapsed {
            crate::mvcc::metrics::record_commit_seq_wait_ns(d.as_nanos() as u64);
        }
    }
}

/// RAII guard that records the §7 / US-024 logical-frame-append
/// duration sample AND recomputes the percentile gauges (p50/p95/p99)
/// from the ring buffer.
///
/// AC#3 demands `Instant::now()` reads OUTSIDE the commit_seq critical
/// section. This guard:
///
///   - Captures `start = Instant::now()` at construction (BEFORE
///     the commit_seq mutex is acquired in `run_write_existing` — the
///     guard is declared before `_commit`, and Rust evaluates RHS in
///     order so the `Instant::now()` call here happens first).
///   - On `drop` (which runs AFTER the commit_seq mutex is released
///     because of LIFO drop order), samples `elapsed` and records it
///     as one logical-frame-append duration sample, then recomputes
///     the p50/p95/p99 gauges.
///
/// Both `Instant::now()` reads happen with NO commit_seq mutex held.
/// Same pattern as `LaneWaitRecord` / `CommitSeqWaitRecord`.
///
/// The recorded duration spans the full commit_seq critical section
/// rather than just the journal-file I/O. Per §7 ("approximate
/// latest-value gauge"), this is an acceptable approximation: the
/// critical section is dominated by the logical+legacy+ChainCommit
/// journal writes, so the envelope duration tracks the append
/// duration to within a few microseconds of overhead.
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
    fn engine_fatal(&self) -> Error {
        self.shared.poison_engine();
        Error::EngineFatal
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
        )
    }

    /// Create a file-backed engine with explicit busy-timeout + busy-handler.
    pub(crate) fn new_buffered_with_busy(
        handle: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
        busy_timeout: Duration,
        busy_handler: Option<BusyHandler>,
    ) -> Result<Self> {
        let (md, shared) = MetadataState::new(handle, catalog_root_page, catalog_root_level)?;
        let engine = PagedEngine {
            shared,
            metadata: RwLock::new(md),
            ns_lanes: DashMap::new(),
            commit_seq: Mutex::new(()),
            busy_timeout,
            busy_handler,
            dropped_namespaces: DashSet::new(),
        };
        engine.resume_building_indexes_after_open()?;
        Ok(engine)
    }

    // -----------------------------------------------------------------------
    // Lane acquisition
    // -----------------------------------------------------------------------

    /// Resolve the per-namespace lane mutex, creating one if needed.
    fn lane_for(&self, ns: &str) -> Arc<ParkingMutex<()>> {
        state::lane_for(self, ns)
    }

    /// Acquire the namespace lane with busy-timeout / busy-handler semantics.
    fn acquire_lane(&self, lane: Arc<ParkingMutex<()>>) -> Result<OwnedLaneGuard> {
        state::acquire_lane(self, lane)
    }

    /// Bootstrap a collection if it does not exist yet.
    ///
    /// Called from CRUD paths that may be invoked with an unknown ns.
    /// Acquires `metadata.write()` + `commit_seq` so the namespace is
    /// both visible in the catalog AND reflected in the published
    /// snapshot before the caller returns to the read path.
    fn bootstrap_namespace(&self, ns: &str) -> Result<()> {
        // Do not auto-create a namespace that was explicitly dropped in this
        // session. The `dropped_namespaces` set is populated by `drop_namespace`
        // and cleared only by an explicit `create_namespace` call.  This prevents
        // a racing insert (arriving after drop_namespace returns but before the
        // session ends) from re-bootstrapping the namespace and committing stale
        // data to the journal — which would survive a reopen via journal recovery.
        if self.dropped_namespaces.contains(ns) {
            return Err(Error::CollectionNotFound {
                name: ns.to_string(),
            });
        }
        let md_w = self
            .metadata
            .write()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        if catalog_lock(&md_w).get_collection(ns)?.is_some() {
            return Ok(());
        }
        // Hold commit_seq for the publish so publish_ts remains monotonic.
        let _commit = self
            .commit_seq
            .lock()
            .map_err(|_| Error::Internal("commit_seq mutex poisoned".into()))?;

        // Open a journal mark + overlay so the bootstrap is atomic.
        let mark = self.shared.handle.begin_txn()?;
        let mut overlay = TxnOverlay::new();
        // Drain deferred-free into overlay reservations.
        let ready = self
            .shared
            .handle
            .allocator()
            .drain_deferred_free_reservations();
        for page in ready {
            overlay.push_reservation(PageReservation {
                page,
                size: PageSize::Large32k,
                origin: PageOrigin::DeferredFree,
            });
        }

        let result: Result<()> = (|| {
            let data_root = {
                let mut cat = catalog_lock(&md_w);
                // Phase 1 §10.7 — allocate durable namespace id from the
                // header counter atomically with the catalog commit.
                let id = cat.allocate_namespace_id();
                cat.create_collection(ns, id, bson::doc! {}, now_millis())?
            };
            sync_catalog_root_overlay(&self.shared, &md_w, &mut overlay)?;
            let _ = BTree::create_at(new_txn_store(&self.shared, &mut overlay), data_root)?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                let mut base_store = new_store(&self.shared);
                overlay.commit(&mut base_store, &self.shared.handle)?;
                self.shared.handle.flush()?;
                let db_page_count = self
                    .shared
                    .handle
                    .allocator()
                    .with_header(|h| h.total_page_count)?;
                let header_data = {
                    let page = self.shared.handle.fetch_page(0, PageSize::Small4k)?;
                    page.data().to_vec()
                };
                let emergency = self.shared.handle.commit_txn(
                    0,
                    PageSize::Small4k,
                    &header_data,
                    db_page_count,
                )?;
                if emergency {
                    // Journal index reached its hot-threshold: drain the
                    // journal into the main file so subsequent txns have
                    // room. Best-effort — failure here does not roll back
                    // the txn (it is already committed).
                    crate::mvcc::metrics::record_emergency_checkpoint_trigger();
                    let _ = self.shared.handle.emergency_checkpoint();
                }
                // Phase 1 §6.3: bootstrap_namespace is a metadata-only
                // DDL publish (no primary writes / no commit_ts), so use
                // `oracle.commit()` — NEVER `oracle.now()`. Two sub-ms
                // DDLs with `now()` can return equal Ts and break the
                // strict-monotonicity invariant enforced by
                // `publish_commit`'s debug_assert.
                let publish_ts = self.shared.oracle.commit()?;
                // Phase 1 §10.3 — bootstrap_namespace is a DDL-style
                // publish site that creates a new reader-visible
                // namespace: both `published_catalog_dirty` and
                // `catalog_header_dirty` are set.
                let dirty = PublishDirty {
                    published_catalog_dirty: true,
                    catalog_header_dirty: true,
                };
                rebuild_and_publish_locked(&self.shared, &md_w, publish_ts, dirty)?;
                Ok(())
            }
            Err(e) => {
                overlay.rollback(&self.shared.handle)?;
                let _ = self.shared.handle.rollback_txn(mark);
                Err(e)
            }
        }
    }

    /// CRUD write lifecycle.
    ///
    /// Drives: metadata.read() → bootstrap-if-missing → lane → overlay +
    /// WriteTxn setup → body → commit_seq { install_sec + install_primary +
    /// overlay commit + flush + append_chain_commit + commit_txn +
    /// rebuild_and_publish } → release lane → release metadata.
    fn run_write<F, R>(&self, ns: &str, f: F) -> Result<R>
    where
        F: FnOnce(
            &SharedState,
            &MetadataState,
            &mut TxnOverlay,
            &mut WriteTxn,
            &WriteVisibility<'_>,
        ) -> Result<R>,
    {
        self.shared.check_engine_not_poisoned()?;
        // Take read guard; if namespace absent, bootstrap then retry.
        let md_read = self
            .metadata
            .read()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        let ns_missing = catalog_lock(&md_read).get_collection(ns)?.is_none();
        if ns_missing {
            drop(md_read);
            self.bootstrap_namespace(ns)?;
            // Re-acquire the read guard and proceed.
            return self.run_write_existing(ns, f);
        }
        drop(md_read);
        self.run_write_existing(ns, f)
    }

    /// Internal form of `run_write` that assumes the namespace already exists
    /// (or the write path tolerates its absence — `update`/`delete` do).
    ///
    /// Keeps `metadata.read()` for the whole body and mutates the
    /// catalog via the interior `Mutex<Catalog>`. Lock order is
    /// documented on `MetadataState`.
    fn run_write_existing<F, R>(&self, ns: &str, f: F) -> Result<R>
    where
        F: FnOnce(
            &SharedState,
            &MetadataState,
            &mut TxnOverlay,
            &mut WriteTxn,
            &WriteVisibility<'_>,
        ) -> Result<R>,
    {
        self.shared.check_engine_not_poisoned()?;
        // COMMIT-ENVELOPE-RESIDUE: A (pre-logical; no current transaction bytes).
        // S0: acquire the namespace write lane before taking the CRUD
        // metadata read guard.
        let lane = self.lane_for(ns);
        // Observational lane-wait timing: the counter fetch_add must run
        // AFTER the lane mutex is released (§5 cross-boundary guardrail —
        // no atomic counter work while a lane/commit_seq mutex is held).
        // Strategy: declare `lane_wait_record` BEFORE `_lane_guard`. Rust's
        // LIFO drop order (last declared = first dropped) means
        // `_lane_guard` drops first (releasing the lane mutex) and then
        // `lane_wait_record` drops, performing the atomic fetch_add with
        // NO mutex held. The recorded duration is captured immediately
        // after acquire_lane returns, so it still reflects only the wait.
        let mut lane_wait_record = LaneWaitRecord { elapsed: None };
        let lane_wait_start = Instant::now();
        // COMMIT-ENVELOPE-RESIDUE: A (lane acquisition fails before journal append).
        let _lane_guard = self.acquire_lane(lane)?;
        lane_wait_record.elapsed = Some(lane_wait_start.elapsed());
        // S1: read metadata/catalog under the namespace lane.
        // COMMIT-ENVELOPE-RESIDUE: A (metadata read fails before journal append).
        let md_read = self
            .metadata
            .read()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        // S2: create the writer visibility context held through S12.
        // COMMIT-ENVELOPE-RESIDUE: A (visibility setup fails before journal append).
        let vis = WriteVisibility::new(&self.shared, ns)?;

        // Setup overlay + WriteTxn. Journal rollback marks are captured later,
        // inside `commit_seq`, immediately before this transaction can append
        // journal frames. Capturing a mark here is unsafe with independent
        // namespace lanes: another writer may commit after the mark and before
        // this body fails, and truncating to the old mark would erase that
        // committed writer's frames.
        let mut overlay = TxnOverlay::new();
        let ready = self
            .shared
            .handle
            .allocator()
            .drain_deferred_free_reservations();
        for page in ready {
            overlay.push_reservation(PageReservation {
                page,
                size: PageSize::Large32k,
                origin: PageOrigin::DeferredFree,
            });
        }
        let txn_id = vis.read_view.txn_id;
        let mut txn = WriteTxn::new(txn_id);

        // S3: execute the write body. Pass `&md_read` directly. The catalog itself is
        // behind `Mutex<Catalog>`, so mutations happen inside the
        // closure under the catalog mutex without needing a RwLock
        // upgrade. Other CRUD writers on different namespaces hold
        // their own `metadata.read()` concurrently and their own
        // lanes; only `commit_seq` serializes the publish step.
        #[cfg(any(test, feature = "test-hooks"))]
        self::test_accessors::write_body_entry_if_installed(&self.shared, ns);
        let body_result = f(&self.shared, &md_read, &mut overlay, &mut txn, &vis);

        match body_result {
            Ok(value) => {
                // Root-neutral vs root-changing classification: if the body
                // called `sync_catalog_root_overlay` (because a tree root moved
                // or the catalog root changed), the overlay captured the file
                // header pre-image. Observed OUTSIDE the commit_seq critical
                // section — reading `overlay.has_header_update()` takes no locks.
                let root_changing = overlay.has_header_update();

                // S4-S12: commit sequencing.
                // Observational commit_seq-wait timing: the counter fetch_add
                // must run AFTER the commit_seq mutex is released (§5
                // cross-boundary guardrail). `_commit` is dropped explicitly
                // at S13 before this recorder runs.
                let mut commit_seq_wait_record = CommitSeqWaitRecord { elapsed: None };
                let commit_seq_wait_start = Instant::now();
                // §7 / US-024 AC#3 — refresh the logical-txn append-duration
                // percentiles AFTER `_commit` releases the commit_seq mutex.
                // `LogicalTxnAppendPercentileRefresh::drop` runs the
                // sort+store work outside the critical section after `_commit`
                // is explicitly dropped at S13.
                let _logical_txn_append_pct_refresh = LogicalTxnAppendPercentileRefresh::new();
                // COMMIT-ENVELOPE-RESIDUE: A (commit_seq acquisition fails before S4).
                let _commit = self
                    .commit_seq
                    .lock()
                    .map_err(|_| Error::Internal("commit_seq mutex poisoned".into()))?;
                commit_seq_wait_record.elapsed = Some(commit_seq_wait_start.elapsed());

                let sec_writes = std::mem::take(&mut txn.pending_sec_index);
                let primary_writes = std::mem::take(&mut txn.pending_primary);
                let has_logical_ops = !sec_writes.is_empty() || !primary_writes.is_empty();

                if !has_logical_ops {
                    // COMMIT-ENVELOPE-RESIDUE: A (legacy-only branch begins before append).
                    let commit_mark = self.shared.handle.begin_txn()?;
                    let mut base_store = new_store(&self.shared);
                    if let Err(e) = overlay.commit(&mut base_store, &self.shared.handle) {
                        // COMMIT-ENVELOPE-RESIDUE: A (legacy-only overlay rolled back).
                        drop(txn);
                        let _ = self.shared.handle.rollback_txn(commit_mark);
                        return Err(e);
                    }
                    // COMMIT-ENVELOPE-RESIDUE: A (legacy-only flush can still roll back).
                    self.shared.handle.flush()?;
                    let dirty = txn.publish_dirty();
                    // COMMIT-ENVELOPE-RESIDUE: D (legacy-only commit before publish).
                    let (publish_ts, _pending, _sec) =
                        txn.commit(&self.shared.oracle, &self.shared.handle)?;
                    // COMMIT-ENVELOPE-RESIDUE: D (legacy header commit before publish).
                    self.commit_legacy_header_frame()?;
                    #[cfg(test)]
                    self::test_accessors::publish_pause_if_installed(&self.shared);
                    // COMMIT-ENVELOPE-RESIDUE: D (legacy committed, publish absent on Err).
                    rebuild_and_publish_locked(&self.shared, &md_read, publish_ts, dirty)?;
                    if root_changing {
                        crate::mvcc::metrics::record_crud_commit_root_changing();
                    } else {
                        crate::mvcc::metrics::record_crud_commit_root_neutral();
                    }
                    return Ok(value);
                }

                // COMMIT-ENVELOPE-RESIDUE: A (begin_txn fails before logical append).
                let commit_mark = self.shared.handle.begin_txn()?;
                let txn_id = txn.txn_id;

                // S4: allocate commit_ts and prove it advances the current
                // published epoch before any resident install.
                let commit_ts = match txn.allocate_commit_ts(&self.shared.oracle) {
                    Ok(ts) => ts,
                    Err(e) => {
                        // COMMIT-ENVELOPE-RESIDUE: A (S4 failure, no logical frame).
                        drop(txn);
                        let _ = self.rollback_overlay_and_wal(overlay, commit_mark);
                        return Err(e);
                    }
                };
                let prev_published = self.shared.published.load_full();
                assert!(
                    commit_ts > prev_published.visible_ts,
                    "S4 commit_ts must advance beyond previous PublishedEpoch"
                );
                drop(prev_published);

                // S5: build LogicalTxnFrame from staged primary and
                // secondary writes using stage-time ns_id/index_id.
                let frame =
                    txn.build_logical_txn_frame(&self.shared.handle, &primary_writes, &sec_writes);

                #[cfg(any(test, feature = "test-hooks"))]
                self::test_accessors::phase3_abort_if_armed(
                    Phase3CommitFailpoint::BeforeLogicalTxnAppend,
                );

                // S6: append logical transaction and fsync the logical tail.
                if let Err(e) = self.shared.handle.append_logical_txn(frame) {
                    // COMMIT-ENVELOPE-RESIDUE: A (S6 failure rolls back to mark).
                    drop(txn);
                    let _ = self.rollback_overlay_and_wal(overlay, commit_mark);
                    return Err(e);
                }
                #[cfg(any(test, feature = "test-hooks"))]
                self::test_accessors::phase3_abort_if_armed(
                    Phase3CommitFailpoint::AfterLogicalTxnAppendBeforeFsync,
                );
                if let Err(e) = self.shared.handle.fsync_logical_tail() {
                    // COMMIT-ENVELOPE-RESIDUE: A (S6 failure rolls back to mark).
                    drop(txn);
                    let _ = self.rollback_overlay_and_wal(overlay, commit_mark);
                    return Err(e);
                }
                #[cfg(any(test, feature = "test-hooks"))]
                self::test_accessors::phase3_abort_if_armed(
                    Phase3CommitFailpoint::AfterLogicalTxnFsyncBeforeChainCommit,
                );

                let dirty = txn.publish_dirty();

                // S7: append ChainCommit and fsync. Durable from here.
                let _installed_and_sec =
                    match txn.commit_chain_commit(&self.shared.handle, commit_ts) {
                        Ok(installed) => installed,
                        Err(e) => {
                            // COMMIT-ENVELOPE-RESIDUE: A (S7 failure rolls back to mark).
                            let _ = self.rollback_overlay_and_wal(overlay, commit_mark);
                            return Err(e);
                        }
                    };
                #[cfg(any(test, feature = "test-hooks"))]
                self::test_accessors::phase3_abort_if_armed(
                    Phase3CommitFailpoint::AfterChainCommitBeforeLegacyCommit,
                );

                // S8: install secondary delta heads as Pending.
                // COMMIT-ENVELOPE-RESIDUE: C (S8 repeated failure; durable ChainCommit).
                self.install_pending_sec_index_or_fatal(
                    &md_read,
                    &mut overlay,
                    &sec_writes,
                    &vis,
                    commit_ts,
                    txn_id,
                )?;

                // S9: install primary delta heads as Pending.
                // COMMIT-ENVELOPE-RESIDUE: C (S9 repeated failure; durable ChainCommit).
                self.install_pending_primary_or_fatal(
                    &md_read,
                    &mut overlay,
                    &primary_writes,
                    &vis,
                    commit_ts,
                    txn_id,
                )?;

                // S10: commit structural overlay bytes only. Root-neutral
                // ordinary CRUD defers its page-image commit out of the
                // pre-publish window so readers cannot fall through to
                // speculative primary base cells while the resident head is
                // Pending.
                let mut root_neutral_overlay = if root_changing {
                    let mut base_store = new_store(&self.shared);
                    // COMMIT-ENVELOPE-RESIDUE: C (S10 failure; durable ChainCommit).
                    overlay
                        .commit(&mut base_store, &self.shared.handle)
                        .map_err(|_| self.engine_fatal())?;
                    None
                } else {
                    Some(overlay)
                };

                // S11: flush structural effects and persist the legacy header
                // commit frame without changing ChainCommit authority.
                // COMMIT-ENVELOPE-RESIDUE: C (S11 flush failure before legacy commit frame).
                self.shared
                    .handle
                    .flush()
                    .map_err(|_| self.engine_fatal())?;
                if root_changing {
                    // COMMIT-ENVELOPE-RESIDUE: C (legacy header frame failure before publish).
                    self.commit_legacy_header_frame()
                        .map_err(|_| self.engine_fatal())?;
                }

                #[cfg(any(test, feature = "test-hooks"))]
                self::test_accessors::phase3_abort_if_armed(
                    Phase3CommitFailpoint::AfterLegacyCommitBeforePublish,
                );

                // S12: publish exactly one PublishedEpoch swap.
                #[cfg(test)]
                self::test_accessors::publish_pause_if_installed(&self.shared);
                // COMMIT-ENVELOPE-RESIDUE: D (legacy committed, publish absent on Err).
                rebuild_and_publish_locked(&self.shared, &md_read, commit_ts, dirty)
                    .map_err(|_| self.engine_fatal())?;
                // COMMIT-ENVELOPE-RESIDUE: E (publish complete; local secondary flip failed).
                commit_pending_sec_index_states(
                    &self.shared,
                    &md_read,
                    &sec_writes,
                    commit_ts,
                    txn_id,
                )
                .map_err(|_| self.engine_fatal())?;
                // COMMIT-ENVELOPE-RESIDUE: E (publish complete; local primary flip failed).
                if let Some(overlay) = root_neutral_overlay.as_mut() {
                    commit_pending_primary_states_with_overlay(
                        &self.shared,
                        &md_read,
                        overlay,
                        &primary_writes,
                        commit_ts,
                        txn_id,
                    )
                } else {
                    commit_pending_primary_states(
                        &self.shared,
                        &md_read,
                        &primary_writes,
                        commit_ts,
                        txn_id,
                    )
                }
                .map_err(|_| self.engine_fatal())?;

                // S13: release commit_seq after the S12 publish and local
                // Pending-to-Committed diagnostic flip. The namespace lane
                // releases when its guard drops at the end of this call.
                drop(_commit);

                if let Some(overlay) = root_neutral_overlay {
                    let mut base_store = new_store(&self.shared);
                    // COMMIT-ENVELOPE-RESIDUE: E (publish complete; delayed overlay commit failed).
                    overlay
                        .commit(&mut base_store, &self.shared.handle)
                        .map_err(|_| self.engine_fatal())?;
                    // COMMIT-ENVELOPE-RESIDUE: E (publish complete; delayed overlay flush failed).
                    self.shared
                        .handle
                        .flush()
                        .map_err(|_| self.engine_fatal())?;
                    // COMMIT-ENVELOPE-RESIDUE: E (publish complete; delayed header frame failed).
                    self.commit_legacy_header_frame()
                        .map_err(|_| self.engine_fatal())?;
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
                let _ = self.rollback_overlay_only(overlay);
                Err(e)
            }
        }
    }

    fn commit_legacy_header_frame(&self) -> Result<()> {
        let db_page_count = self
            .shared
            .handle
            .allocator()
            .with_header(|h| h.total_page_count)?;
        // Build the commit-frame page-0 bytes from the allocator's
        // authoritative header rather than the buffer pool. The overlay
        // commit may have written stale header bytes if concurrent catalog
        // work advanced the allocator header before this writer sequenced.
        let header_data = self
            .shared
            .handle
            .allocator()
            .with_header(|h| h.to_bytes())?;
        let emergency =
            self.shared
                .handle
                .commit_txn(0, PageSize::Small4k, &header_data, db_page_count)?;
        if emergency {
            crate::mvcc::metrics::record_emergency_checkpoint_trigger();
            let _ = self.shared.handle.emergency_checkpoint();
        }
        Ok(())
    }

    fn install_pending_sec_index_or_fatal(
        &self,
        md: &MetadataState,
        overlay: &mut TxnOverlay,
        writes: &[crate::mvcc::SecIndexWrite],
        vis: &WriteVisibility<'_>,
        commit_ts: crate::mvcc::Ts,
        txn_id: u64,
    ) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        if install_pending_sec_index(
            &self.shared,
            md,
            overlay,
            writes.to_vec(),
            vis,
            commit_ts,
            txn_id,
        )
        .is_ok()
        {
            return Ok(());
        }
        install_pending_sec_index(
            &self.shared,
            md,
            overlay,
            writes.to_vec(),
            vis,
            commit_ts,
            txn_id,
        )
        .map_err(|_| self.engine_fatal())
    }

    fn install_pending_primary_or_fatal(
        &self,
        md: &MetadataState,
        overlay: &mut TxnOverlay,
        writes: &[crate::mvcc::PrimaryWrite],
        vis: &WriteVisibility<'_>,
        commit_ts: crate::mvcc::Ts,
        txn_id: u64,
    ) -> Result<()> {
        if writes.is_empty() {
            return Ok(());
        }
        #[cfg(any(test, feature = "test-hooks"))]
        let first_attempt = self::test_accessors::us019_maybe_fail_primary_install(&self.shared)
            .and_then(|()| {
                install_pending_primary(
                    &self.shared,
                    md,
                    overlay,
                    writes.to_vec(),
                    vis,
                    commit_ts,
                    txn_id,
                )
            });
        #[cfg(not(any(test, feature = "test-hooks")))]
        let first_attempt = install_pending_primary(
            &self.shared,
            md,
            overlay,
            writes.to_vec(),
            vis,
            commit_ts,
            txn_id,
        );
        if first_attempt.is_ok() {
            return Ok(());
        }
        #[cfg(any(test, feature = "test-hooks"))]
        let second_attempt = self::test_accessors::us019_maybe_fail_primary_install(&self.shared)
            .and_then(|()| {
                install_pending_primary(
                    &self.shared,
                    md,
                    overlay,
                    writes.to_vec(),
                    vis,
                    commit_ts,
                    txn_id,
                )
            });
        #[cfg(not(any(test, feature = "test-hooks")))]
        let second_attempt = install_pending_primary(
            &self.shared,
            md,
            overlay,
            writes.to_vec(),
            vis,
            commit_ts,
            txn_id,
        );
        second_attempt.map_err(|_| self.engine_fatal())
    }

    fn rollback_overlay_only(&self, overlay: TxnOverlay) -> Result<()> {
        overlay.rollback(&self.shared.handle)
    }

    fn rollback_overlay_and_wal(&self, overlay: TxnOverlay, mark: Option<u64>) -> Result<()> {
        overlay.rollback(&self.shared.handle)?;
        let _ = self.shared.handle.rollback_txn(mark);
        Ok(())
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
        let result = self.run_ddl(|shared, md, overlay| {
            let data_root = {
                let mut cat = catalog_lock(md);
                if cat.get_collection(ns)?.is_some() {
                    return Ok(());
                }
                // Phase 1 §10.7 — allocate durable namespace id from the
                // header counter atomically with the catalog commit.
                let id = cat.allocate_namespace_id();
                cat.create_collection(ns, id, bson::doc! {}, now_millis())?
            };
            sync_catalog_root_overlay(shared, md, overlay)?;
            let store = new_txn_store(shared, overlay);
            BTree::create_at(store, data_root)?;
            Ok(())
        });
        // Clear the drop tombstone so subsequent auto-bootstrap (from `run_write`)
        // is re-enabled for this namespace.
        if result.is_ok() {
            self.dropped_namespaces.remove(ns);
        }
        result
    }

    // -----------------------------------------------------------------------
    // drop_namespace
    // -----------------------------------------------------------------------

    fn drop_namespace(&self, ns: &str) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        // Force-expire ALL active ReadViews globally before freeing pages.
        // Done BEFORE taking the metadata write guard so concurrent readers
        // that just loaded the published snapshot can finish their pin walks.
        self.shared.handle.read_view_registry().force_expire_all();

        // Insert the drop guard BEFORE taking the metadata write lock so that
        // any writer that starts after this point — including one that sneaks
        // in between the metadata-write release and the caller observing the
        // drop return — sees `dropped_namespaces` and refuses to re-bootstrap.
        // On failure below we remove the guard again so retries can proceed.
        let newly_dropped = self.dropped_namespaces.insert(ns.to_string());

        // Remove the lane from the map so no new writer picks it up, then
        // wait for any writer that grabbed the lane before the remove by
        // briefly locking the removed Arc. Release immediately.
        let removed_lane = self.ns_lanes.remove(ns).map(|(_, v)| v);
        if let Some(lane) = removed_lane {
            // Wait out an in-flight writer by taking the lock ourselves.
            // parking_lot::Mutex::lock() never panics or poisons.
            let _guard = lane.lock();
            // _guard dropped immediately on scope exit below.
            drop(_guard);
        }

        let result = self.run_ddl(|shared, md, overlay| {
            let (maybe_coll, index_roots, dropped) = {
                let mut cat = catalog_lock(md);
                let maybe_coll = cat.get_collection(ns)?;
                let index_roots: Vec<(u32, u8)> = if maybe_coll.is_some() {
                    cat.list_indexes(ns)?
                        .into_iter()
                        .map(|e| (e.root_page, e.root_level))
                        .collect()
                } else {
                    Vec::new()
                };
                let dropped = cat.drop_collection(ns)?;
                (maybe_coll, index_roots, dropped)
            };
            if dropped {
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            if let Some(coll) = maybe_coll {
                let store = new_txn_store(shared, overlay);
                let data_tree = BTree::open(store, coll.data_root_page, coll.data_root_level);
                data_tree.free_all_pages()?;
            }
            for (idx_root, idx_level) in index_roots {
                let store = new_txn_store(shared, overlay);
                let idx_tree = BTree::open(store, idx_root, idx_level);
                idx_tree.free_all_pages()?;
            }
            Ok(())
        });
        // The drop-guard was inserted before `run_ddl`.  If run_ddl failed and
        // we're the thread that newly inserted it, roll it back so retries can
        // proceed; otherwise leave the prior guard intact.
        if result.is_err() && newly_dropped {
            self.dropped_namespaces.remove(ns);
        }
        result
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
    fn install_write_body_entry_hook(
        &self,
        ns: &str,
        observe_flag: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) -> self::test_accessors::WriteBodyEntryHookGuard {
        self.test_install_write_body_entry_hook(ns, observe_flag)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn phase0_probe_insert(
        &self,
        ns: &str,
        doc: Document,
        cut: Phase0ProbeCut,
    ) -> Result<Phase0ProbeReport> {
        self.phase0_probe_insert_impl(ns, doc, cut)
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
    /// Drive a DDL body under `metadata.write()` + `commit_seq`, then
    /// commit the overlay and publish a fresh snapshot.
    fn run_ddl<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&SharedState, &MetadataState, &mut TxnOverlay) -> Result<R>,
    {
        self.shared.check_engine_not_poisoned()?;
        let md_w = self
            .metadata
            .write()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        let _commit = self
            .commit_seq
            .lock()
            .map_err(|_| Error::Internal("commit_seq mutex poisoned".into()))?;

        let mark = self.shared.handle.begin_txn()?;
        let mut overlay = TxnOverlay::new();
        let ready = self
            .shared
            .handle
            .allocator()
            .drain_deferred_free_reservations();
        for page in ready {
            overlay.push_reservation(PageReservation {
                page,
                size: PageSize::Large32k,
                origin: PageOrigin::DeferredFree,
            });
        }

        let result = f(&self.shared, &md_w, &mut overlay);
        match result {
            Ok(value) => {
                let mut base_store = new_store(&self.shared);
                overlay.commit(&mut base_store, &self.shared.handle)?;
                self.shared.handle.flush()?;
                let db_page_count = self
                    .shared
                    .handle
                    .allocator()
                    .with_header(|h| h.total_page_count)?;
                let header_data = {
                    let page = self.shared.handle.fetch_page(0, PageSize::Small4k)?;
                    page.data().to_vec()
                };
                let emergency = self.shared.handle.commit_txn(
                    0,
                    PageSize::Small4k,
                    &header_data,
                    db_page_count,
                )?;
                if emergency {
                    let _ = self.shared.handle.emergency_checkpoint();
                }
                // Phase 1 §6.3: DDL has no primary writes, so allocate a
                // fresh monotonic Ts from the oracle. `oracle.commit()`
                // advances the HLC; `oracle.now()` only peeks and can
                // return equal Ts across two sub-ms DDLs.
                let publish_ts = self.shared.oracle.commit()?;
                // Phase 1 §10.3 — every DDL body that runs to completion
                // changes the reader-visible published catalog (new /
                // dropped namespace, Building↔Ready flip, dropped index,
                // orphan Building cleanup). Both flags always set.
                let dirty = PublishDirty {
                    published_catalog_dirty: true,
                    catalog_header_dirty: true,
                };
                rebuild_and_publish_locked(&self.shared, &md_w, publish_ts, dirty)?;
                Ok(value)
            }
            Err(e) => {
                let _ = self.rollback_overlay_and_wal(overlay, mark);
                Err(e)
            }
        }
    }
}
