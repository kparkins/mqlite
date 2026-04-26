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
mod snapshot_ops;
mod state;
/// Test-only `impl PagedEngine` accessors — isolated from the
/// production code path in a separate file so the boundary is
/// visible at a glance.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) mod test_accessors;

#[cfg(test)]
mod tests;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

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
use self::index_maint::{install_pending_primary, install_pending_sec_index};
use self::publish::PublishDirty;
use self::state::{MetadataState, OwnedLaneGuard, SharedState};

// ---------------------------------------------------------------------------
// PagedEngine — public struct
// ---------------------------------------------------------------------------

/// Storage engine: B+ tree per namespace, through the buffer pool.
///
/// ## Concurrency
///
/// - **Reads**: mutex-free — load `shared.published` (`ArcSwap`) and open
///   B-trees at the snapshot's root pages. No engine-level lock taken.
/// - **Writes on CRUD paths**: acquire `metadata.read()` (shared), then the
///   per-namespace lane from `ns_lanes`. Two writers on different namespaces
///   progress concurrently. Commit sequencing (ts allocation + publish) is
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
    /// Commit-sequencing mutex. All successful writes acquire it around
    /// the `commit_ts = oracle.commit()` → install_primary → flush →
    /// append_chain_commit → commit_txn → publish sequence, so
    /// `commit_ts`, journal append order, and `publish_ts` all agree.
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
        Ok(PagedEngine {
            shared,
            metadata: RwLock::new(md),
            ns_lanes: DashMap::new(),
            commit_seq: Mutex::new(()),
            busy_timeout,
            busy_handler,
            dropped_namespaces: DashSet::new(),
        })
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
        F: FnOnce(&SharedState, &MetadataState, &mut TxnOverlay, &mut WriteTxn) -> Result<R>,
    {
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
        F: FnOnce(&SharedState, &MetadataState, &mut TxnOverlay, &mut WriteTxn) -> Result<R>,
    {
        let md_read = self
            .metadata
            .read()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
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
        let _lane_guard = self.acquire_lane(lane)?;
        lane_wait_record.elapsed = Some(lane_wait_start.elapsed());

        // Setup overlay + WriteTxn.
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
        let txn_id = self.shared.txn_counter.fetch_add(1, Ordering::Relaxed);
        let mut txn = WriteTxn::new(txn_id);

        // Body — pass `&md_read` directly. The catalog itself is
        // behind `Mutex<Catalog>`, so mutations happen inside the
        // closure under the catalog mutex without needing a RwLock
        // upgrade. Other CRUD writers on different namespaces hold
        // their own `metadata.read()` concurrently and their own
        // lanes; only `commit_seq` serializes the publish step.
        let body_result = f(&self.shared, &md_read, &mut overlay, &mut txn);

        match body_result {
            Ok(value) => {
                // Root-neutral vs root-changing classification: if the body
                // called `sync_catalog_root_overlay` (because a tree root moved
                // or the catalog root changed), the overlay captured the file
                // header pre-image. Observed OUTSIDE the commit_seq critical
                // section — reading `overlay.has_header_pre()` takes no locks.
                let root_changing = overlay.has_header_pre();

                // Commit sequencing.
                // Observational commit_seq-wait timing: the counter fetch_add
                // must run AFTER the commit_seq mutex is released (§5
                // cross-boundary guardrail). Same LIFO-drop pattern as the
                // lane-wait recorder above: `commit_seq_wait_record` is
                // declared BEFORE `_commit`, so `_commit` drops first (end
                // of this match-arm scope) and the fetch_add in
                // `CommitSeqWaitRecord::drop` runs with NO mutex held.
                let mut commit_seq_wait_record = CommitSeqWaitRecord { elapsed: None };
                let commit_seq_wait_start = Instant::now();
                // §7 / US-024 AC#3 — refresh the logical-txn append-duration
                // percentiles AFTER `_commit` releases the commit_seq mutex.
                // `LogicalTxnAppendPercentileRefresh::drop` runs the
                // sort+store work outside the critical section. Declared
                // BEFORE `_commit` so Rust's LIFO drop order makes
                // `_commit` drop first (releasing the mutex) and this
                // guard's Drop runs with NO mutex held.
                let _logical_txn_append_pct_refresh = LogicalTxnAppendPercentileRefresh::new();
                let _commit = self
                    .commit_seq
                    .lock()
                    .map_err(|_| Error::Internal("commit_seq mutex poisoned".into()))?;
                commit_seq_wait_record.elapsed = Some(commit_seq_wait_start.elapsed());

                let sec_writes = std::mem::take(&mut txn.pending_sec_index);
                if let Err(e) = install_pending_sec_index(
                    &self.shared,
                    &md_read,
                    &mut overlay,
                    sec_writes.to_vec(),
                    &mut txn,
                ) {
                    drop(txn);
                    let _ = self.rollback_overlay_and_wal(overlay, mark);
                    return Err(e);
                }

                let primary_writes = std::mem::take(&mut txn.pending_primary);
                // Phase 2 §3.7 commit envelope order: S4 allocate_commit_ts →
                // S5 emit LogicalTxnFrame → S6 flush → S7 ChainCommit. The
                // logical frame is emitted when ANY logical op (primary or
                // secondary) was staged; metadata-only commits skip both
                // the allocation and the emit (ChainCommit on those runs
                // picks up its ts via `WriteTxn::commit`'s lazy allocate).
                //
                // The `sec_writes` + `primary_writes` locals must survive
                // until after `emit_logical_txn_frame` has captured them —
                // `install_pending_primary` below consumes primary_writes
                // via `.to_vec()`, which clones into a throwaway copy, so
                // the original still drives the emit call.
                let has_logical_ops = !sec_writes.is_empty() || !primary_writes.is_empty();
                let commit_ts_opt = if has_logical_ops {
                    let txn_id = txn.txn_id;
                    let commit_ts = match txn.allocate_commit_ts(&self.shared.oracle) {
                        Ok(ts) => ts,
                        Err(e) => {
                            drop(txn);
                            let _ = self.rollback_overlay_and_wal(overlay, mark);
                            return Err(e);
                        }
                    };

                    if let Err(e) = txn.emit_logical_txn_frame(
                        &self.shared.handle,
                        &primary_writes,
                        &sec_writes,
                    ) {
                        drop(txn);
                        let _ = self.rollback_overlay_and_wal(overlay, mark);
                        return Err(e);
                    }

                    if !primary_writes.is_empty() {
                        if let Err(e) = install_pending_primary(
                            &self.shared,
                            &md_read,
                            &mut overlay,
                            primary_writes.to_vec(),
                            commit_ts,
                            txn_id,
                        ) {
                            drop(txn);
                            let _ = self.rollback_overlay_and_wal(overlay, mark);
                            return Err(e);
                        }
                    }
                    Some(commit_ts)
                } else {
                    None
                };

                // Commit overlay bytes onto shared frames.
                let mut base_store = new_store(&self.shared);
                if let Err(e) = overlay.commit(&mut base_store, &self.shared.handle) {
                    drop(txn);
                    let _ = self.shared.handle.rollback_txn(mark);
                    return Err(e);
                }
                self.shared.handle.flush()?;
                // Phase 1 §10.3 — capture the txn's dirty flags before
                // `txn.commit()` consumes the transaction. `install_*`
                // above may have called `mark_published` / `mark_header`
                // based on root-movement classification (§4.1 / §10.3).
                let dirty = txn.publish_dirty();
                // Phase 2 §3.7: the Cell was taken by `emit_logical_txn_frame`,
                // so use `commit_with_ts(commit_ts)` when we have a
                // pre-allocated ts. Metadata-only commits (no sec/primary
                // writes) fall through `commit()` to the lazy-allocate
                // path, preserving existing behavior.
                let _installed_and_sec = match commit_ts_opt {
                    Some(ts) => txn.commit_with_ts(ts, &self.shared.handle)?,
                    None => {
                        let (_ts, pending, sec) =
                            txn.commit(&self.shared.oracle, &self.shared.handle)?;
                        (pending, sec)
                    }
                };
                let db_page_count = self
                    .shared
                    .handle
                    .allocator()
                    .with_header(|h| h.total_page_count)?;
                // Build the commit-frame page-0 bytes from the allocator's
                // authoritative header rather than the buffer pool.  The
                // overlay commit above (overlay_inner.commit) may have written
                // a stale catalog_root_page to the pool if a concurrent DDL
                // (e.g. drop_namespace) committed AFTER this txn's body ran
                // but BEFORE we reached the commit-seq lock.  Reading from the
                // allocator — which is always updated atomically under
                // update_header — guarantees we persist the latest
                // catalog_root_page even if a concurrent DDL beat us here.
                let header_data = self
                    .shared
                    .handle
                    .allocator()
                    .with_header(|h| h.to_bytes())?;
                let emergency = self.shared.handle.commit_txn(
                    0,
                    PageSize::Small4k,
                    &header_data,
                    db_page_count,
                )?;
                if emergency {
                    crate::mvcc::metrics::record_emergency_checkpoint_trigger();
                    let _ = self.shared.handle.emergency_checkpoint();
                }
                // Phase 1 §6.3: strict-monotonic visible_ts rule.
                //   - txn had primary writes → use its allocated `commit_ts`.
                //   - metadata-only commit (no primary writes) → use
                //     `oracle.commit()?`, NOT `oracle.now()`. `now()` peeks
                //     the HLC without advancing it, so two sub-ms
                //     metadata-only commits could return equal Ts and
                //     violate `publish_commit`'s monotonicity debug_assert.
                let publish_ts = match commit_ts_opt {
                    Some(ts) => ts,
                    None => self.shared.oracle.commit()?,
                };
                // §10.8 #19 rendezvous: test-only hook placed AFTER
                // commit_txn and BEFORE publish_commit so a unit test
                // can deterministically observe the pre-publish
                // ReadEpoch. Gated under `#[cfg(test)]` — in
                // `cfg(not(test))` (production) builds this expands
                // to an inlined no-op; no new `Mutex` / `Arc`
                // appears on the commit path (§11 #10).
                #[cfg(test)]
                self::test_accessors::publish_pause_if_installed(&self.shared);
                rebuild_and_publish_locked(&self.shared, &md_read, publish_ts, dirty)?;
                if root_changing {
                    crate::mvcc::metrics::record_crud_commit_root_changing();
                } else {
                    crate::mvcc::metrics::record_crud_commit_root_neutral();
                }
                Ok(value)
            }
            Err(e) => {
                drop(txn);
                let _ = self.rollback_overlay_and_wal(overlay, mark);
                Err(e)
            }
        }
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
        doc_ops::insert(self, ns, doc)
    }

    fn find(
        &self,
        ns: &str,
        filter: &Document,
        opts: &FindOptions,
    ) -> Result<(Vec<Document>, crate::query::explain::ExplainResult)> {
        doc_ops::find(self, ns, filter, opts)
    }

    fn find_one(&self, ns: &str, filter: &Document) -> Result<Option<Document>> {
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
        doc_ops::update(self, ns, filter, update, opts, many)
    }

    fn delete(&self, ns: &str, filter: &Document, many: bool) -> Result<DeleteResult> {
        doc_ops::delete(self, ns, filter, many)
    }

    fn count(&self, ns: &str, filter: &Document) -> Result<u64> {
        doc_ops::count(self, ns, filter)
    }

    fn find_one_and_update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &FindOneAndUpdateOptions,
    ) -> Result<Option<Document>> {
        doc_ops::find_one_and_update_doc(self, ns, filter, update, opts)
    }

    fn find_one_and_delete(
        &self,
        ns: &str,
        filter: &Document,
        opts: &FindOneAndDeleteOptions,
    ) -> Result<Option<Document>> {
        doc_ops::find_one_and_delete_doc(self, ns, filter, opts)
    }

    fn find_one_and_replace(
        &self,
        ns: &str,
        filter: &Document,
        replacement: &Document,
        opts: &FindOneAndReplaceOptions,
    ) -> Result<Option<Document>> {
        doc_ops::find_one_and_replace_doc(self, ns, filter, replacement, opts)
    }

    fn create_index(&self, ns: &str, model: &IndexModel) -> Result<String> {
        index_maint::create_index(self, ns, model)
    }

    fn drop_index(&self, ns: &str, name: &str) -> Result<()> {
        index_maint::drop_index(self, ns, name)
    }

    fn list_indexes(&self, ns: &str) -> Result<Vec<IndexInfo>> {
        index_maint::list_indexes(self, ns)
    }

    // -----------------------------------------------------------------------
    // create_namespace
    // -----------------------------------------------------------------------

    fn create_namespace(&self, ns: &str) -> Result<()> {
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
        let snap = self.shared.load_published();
        let keys = snap.catalog.namespace_id_by_name.keys();
        let mut out = Vec::with_capacity(keys.len());
        out.extend(keys.cloned());
        Ok(out)
    }

    fn checkpoint(&self) -> Result<()> {
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
    fn published_catalog_ptr(&self) -> usize {
        self.test_published_catalog_ptr()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn recovered_max_commit_ts(&self) -> Option<(u64, u32)> {
        self.test_recovered_max_commit_ts()
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
        snapshot_ops::close(self)
    }

    fn journal_sync(&self) -> Result<()> {
        snapshot_ops::journal_sync(self)
    }

    fn snapshot_bytes(&self) -> Result<Option<Vec<u8>>> {
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
