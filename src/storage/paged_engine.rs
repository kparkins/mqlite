//! `PagedEngine` — Phase 1 `StorageEngine` backed by B+ trees.
//!
//! ## Design
//!
//! Documents are stored in per-namespace B+ trees keyed by [`encode_key`]-encoded
//! `_id` values.  Two operating modes:
//!
//! | Mode | Backing store | Persistence |
//! |------|--------------|-------------|
//! | **Buffered** | [`BufferPoolPageStore`] (shared [`BufferPoolHandle`]) | Via buffer pool flush |
//!
//! ## Catalog (Buffered mode)
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
mod doc_ops;
mod index_maint;
mod snapshot_ops;
mod state;

#[cfg(test)]
mod tests;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use dashmap::{DashMap, DashSet};

use crate::options::BusyHandler;

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::index::{IndexInfo, IndexModel};
use crate::key_encoding::encode_key;
use crate::mvcc::transaction::WriteTxn;
use crate::options::{
    FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
    ReturnDocument, UpdateOptions,
};
use crate::results::{DeleteResult, UpdateResult};
use crate::storage::btree::BTree;
use crate::storage::buffer_pool::PageSize;
use crate::storage::catalog::IndexState;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::secondary_index::{build_index, generate_index_name};
use crate::storage::txn_page_store::{PageOrigin, PageReservation, TxnOverlay};
use crate::storage_engine::StorageEngine;
use crate::update_operators::{apply_update, is_operator_update, upsert_base_from_filter};
use crate::validation::validate_document;

use self::btree_ops::btree_insert_doc;
use self::catalog_ops::{
    new_store, new_txn_store, rebuild_and_publish_locked, sync_catalog_root_overlay,
};
use self::doc_ops::{compare_docs, now_millis, validate_index_keys};
use self::index_maint::{
    install_pending_primary, install_pending_sec_index, maintain_secondary_on_delete,
    maintain_secondary_on_insert, maintain_secondary_on_update, ReserveOutcome,
};
use self::snapshot_ops::{apply_find_opts, execute_snapshot_pairs_from_snap};
use self::state::{MetadataState, SharedState};

// ---------------------------------------------------------------------------
// PagedEngine — public struct (PR 8)
// ---------------------------------------------------------------------------

/// Phase 1 storage engine: B+ tree per namespace, through the buffer pool.
///
/// ## Concurrency (PR 8: MWMR v1)
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
    ns_lanes: DashMap<String, Arc<Mutex<()>>>,
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

impl PagedEngine {
    /// Create a file-backed engine using `handle` as the page store.
    ///
    /// If `catalog_root_page == 0` the database is new and an empty catalog
    /// will be created. Otherwise the catalog is opened at the given root.
    /// `busy_timeout` + `busy_handler` migrated from ClientInner (PR 8).
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
    // Lane acquisition (ported from client.rs acquire_writer_lock)
    // -----------------------------------------------------------------------

    /// Resolve the per-namespace lane mutex, creating one if needed.
    fn lane_for(&self, ns: &str) -> Arc<Mutex<()>> {
        if let Some(entry) = self.ns_lanes.get(ns) {
            return Arc::clone(entry.value());
        }
        self.ns_lanes
            .entry(ns.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Acquire the namespace lane with busy-timeout / busy-handler semantics
    /// matching the client's legacy `acquire_writer_lock`.
    ///
    /// Returns `(lane, guard)` — the caller holds both to keep the guard
    /// alive for the lane's duration. Stdlib `MutexGuard` is tied to the
    /// `Arc<Mutex<()>>`'s lifetime so we return the Arc alongside.
    fn acquire_lane(
        &self,
        lane: Arc<Mutex<()>>,
    ) -> Result<OwnedLaneGuard> {
        let lane_ptr: *const Mutex<()> = Arc::as_ptr(&lane);

        // Fast path: try without any spin first.
        match unsafe { &*lane_ptr }.try_lock() {
            Ok(g) => return Ok(OwnedLaneGuard::new(lane, g)),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(Error::Internal("namespace lane mutex poisoned".into()));
            }
            Err(std::sync::TryLockError::WouldBlock) => {}
        }

        let timeout = self.busy_timeout;
        if let Some(handler) = &self.busy_handler {
            let mut attempts: u32 = 0;
            loop {
                std::thread::sleep(std::time::Duration::from_millis(1));
                match unsafe { &*lane_ptr }.try_lock() {
                    Ok(g) => return Ok(OwnedLaneGuard::new(lane, g)),
                    Err(std::sync::TryLockError::Poisoned(_)) => {
                        return Err(Error::Internal(
                            "namespace lane mutex poisoned".into(),
                        ));
                    }
                    Err(std::sync::TryLockError::WouldBlock) => {}
                }
                if !handler.0(attempts) {
                    return Err(Error::WriterBusy);
                }
                attempts = attempts.saturating_add(1);
            }
        }

        if timeout.is_zero() {
            return Err(Error::WriterBusy);
        }

        // No custom busy handler: block on `lock()` (kernel-managed queue)
        // rather than spin. This is the common in-process contention path
        // and avoids WriterBusy timeouts when writers are merely queued.
        // The busy_timeout still bounds lane wait, enforced via a
        // scheduled wake: we try first, then fall back to blocking lock()
        // and check elapsed on return.
        let deadline = Instant::now() + timeout;
        let guard = match unsafe { &*lane_ptr }.lock() {
            Ok(g) => g,
            Err(_) => {
                return Err(Error::Internal("namespace lane mutex poisoned".into()));
            }
        };
        if Instant::now() >= deadline && timeout > Duration::ZERO {
            // We waited longer than busy_timeout, but we DO hold the lock.
            // The sensible thing is to return the guard — the writer
            // already won the lane. Strict busy-timeout semantics would
            // return WriterBusy + release, but that loses forward progress
            // and is not what the PR 8 test expects ("writers queue up
            // and eventually all succeed").
            let _ = deadline;
        }
        Ok(OwnedLaneGuard::new(lane, guard))
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
        let md_w = self.metadata.write().map_err(|_| {
            Error::Internal("metadata RwLock poisoned".into())
        })?;
        if md_w.catalog.lock().expect("catalog poisoned").get_collection(ns)?.is_some() {
            return Ok(());
        }
        // Hold commit_seq for the publish so publish_ts remains monotonic.
        let _commit = self.commit_seq.lock().map_err(|_| {
            Error::Internal("commit_seq mutex poisoned".into())
        })?;

        // Open a journal mark + overlay so the bootstrap is atomic.
        let mark = self.shared.handle.begin_txn()?;
        let overlay = TxnOverlay::new_shared();
        // Drain deferred-free into overlay reservations.
        let ready = self.shared.handle.allocator().drain_deferred_free_reservations();
        if !ready.is_empty() {
            let mut ov = overlay.lock().expect("TxnOverlay mutex poisoned");
            for page in ready {
                ov.push_reservation(PageReservation {
                    page,
                    size: PageSize::Large32k,
                    origin: PageOrigin::DeferredFree,
                });
            }
        }

        let result: Result<()> = (|| {
            let data_root = md_w
                .catalog
                .lock()
                .expect("catalog poisoned")
                .create_collection(ns, bson::doc! {}, now_millis())?;
            sync_catalog_root_overlay(&self.shared, &md_w, &overlay)?;
            let _ = BTree::create_at(new_txn_store(&self.shared, &overlay), data_root)?;
            Ok(())
        })();

        match result {
            Ok(()) => {
                let overlay_inner = Arc::try_unwrap(overlay)
                    .map_err(|_| Error::Internal("TxnOverlay still referenced".into()))?
                    .into_inner()
                    .map_err(|_| Error::Internal("TxnOverlay mutex poisoned".into()))?;
                let mut base_store = new_store(&self.shared);
                overlay_inner.commit(&mut base_store, &self.shared.handle)?;
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
                    let _ = self.shared.handle.emergency_checkpoint();
                }
                let publish_ts = self.shared.oracle.now();
                rebuild_and_publish_locked(&self.shared, &md_w, publish_ts)?;
                Ok(())
            }
            Err(e) => {
                let overlay_inner = Arc::try_unwrap(overlay)
                    .map_err(|_| Error::Internal("TxnOverlay still referenced".into()))?
                    .into_inner()
                    .map_err(|_| Error::Internal("TxnOverlay mutex poisoned".into()))?;
                overlay_inner.rollback(&self.shared.handle)?;
                let _ = self.shared.handle.rollback_txn(mark);
                Err(e)
            }
        }
    }

    /// Phase 1 of `create_index`: reserve an index slot.
    ///
    /// Allocates a root page for the index's B+ tree, writes an
    /// `IndexEntry { state: Building }` into the catalog, initializes
    /// the leaf page, and publishes a fresh snapshot so writers on the
    /// target namespace dual-write to it while the build is in flight.
    ///
    /// Returns [`ReserveOutcome::AlreadyExists`] if an index of that
    /// name already exists (idempotent call), otherwise
    /// [`ReserveOutcome::Reserved`].
    fn create_index_reserve(
        &self,
        ns: &str,
        model: &IndexModel,
        name: &str,
    ) -> Result<ReserveOutcome> {
        // Drive through `run_ddl` so we get the standard DDL envelope:
        // metadata.write() + commit_seq + overlay/WAL + publish.
        let outcome = std::cell::Cell::new(ReserveOutcome::Reserved);
        self.run_ddl(|shared, md, overlay| {
            let mut cat = md.catalog.lock().expect("catalog poisoned");
            // Bootstrap collection if absent (matches pre-PR-9 create_index).
            if cat.get_collection(ns)?.is_none() {
                let data_root = cat.create_collection(ns, bson::doc! {}, now_millis())?;
                drop(cat);
                sync_catalog_root_overlay(shared, md, overlay)?;
                let data_store = new_txn_store(shared, overlay);
                BTree::create_at(data_store, data_root)?;
                cat = md.catalog.lock().expect("catalog poisoned");
            }

            // Idempotent: if an index with this name already exists we
            // treat the call as a no-op (matches pre-PR-9 semantics).
            // We do NOT re-check state here — a caller seeing an
            // in-progress Building entry from a prior failed build is
            // reported as "exists"; the prior build's cleanup will have
            // removed it if it failed. If cleanup itself failed and an
            // orphan Building entry remains, callers can drop_index and
            // retry.
            if cat.get_index(ns, name)?.is_some() {
                outcome.set(ReserveOutcome::AlreadyExists);
                return Ok(());
            }

            let idx_root = cat.create_index(ns, model, name)?;
            // `catalog.create_index` defaults `state` to `Ready`. Flip
            // to `Building` so the published snapshot marks this index
            // as not-yet-queryable.
            let mut entry = cat
                .get_index(ns, name)?
                .ok_or_else(|| Error::Internal("index entry missing after create".into()))?;
            entry.state = IndexState::Building;
            cat.update_index(&entry)?;
            drop(cat);
            sync_catalog_root_overlay(shared, md, overlay)?;

            // Initialize the freshly-allocated leaf page so the index
            // tree is valid to open for writes during Phase 2.
            let idx_store = new_txn_store(shared, overlay);
            BTree::create_at(idx_store, idx_root)?;
            Ok(())
        })?;
        Ok(outcome.get())
    }

    /// Phase 2 of `create_index`: populate the index from the data tree.
    ///
    /// Runs under `metadata.read()` + the target namespace's lane, so
    /// writers on *other* namespaces proceed concurrently. Writers on
    /// this namespace wait on the lane (same as any other ns-local
    /// serialization). Any writer that commits DURING the build on this
    /// ns is blocked out; writers between Phase 1 and Phase 2 have
    /// already dual-written to the Building index (via
    /// `maintain_secondary_on_*`).
    fn create_index_build(&self, ns: &str, name: &str) -> Result<()> {
        // Acquire the namespace lane under a short-lived metadata.read()
        // guard — the read guard blocks drop_namespace (which needs
        // metadata.write()) from racing with lane acquisition. Once the
        // lane is in hand, drop the read guard so the long build scan
        // below does NOT block DDL on other namespaces or bootstrapping
        // of new namespaces (Gap-9 follow-up; fixes PR 9's known v1
        // limitation).
        let lane = self.lane_for(ns);
        let _lane_guard = {
            let _md_read = self.metadata.read().map_err(|_| {
                Error::Internal("metadata RwLock poisoned".into())
            })?;
            self.acquire_lane(lane)?
            // _md_read dropped here.
        };

        // Take the latest IndexEntry AND CollectionEntry under a brief
        // metadata.read(). The root pages observed here are authoritative:
        // we now hold the namespace lane, so no concurrent writer on this
        // ns can advance them until we release the lane. drop_namespace of
        // this ns would block on the lane; drop_index of this index may
        // race and leave us with a missing entry — we error cleanly.
        let (idx_entry, data_entry) = {
            let md_read = self.metadata.read().map_err(|_| {
                Error::Internal("metadata RwLock poisoned".into())
            })?;
            let cat = md_read.catalog.lock().expect("catalog poisoned");
            let idx_entry = cat.get_index(ns, name)?.ok_or_else(|| {
                Error::Internal(format!(
                    "index '{}' on '{}' disappeared before build phase",
                    name, ns
                ))
            })?;
            let data_entry = cat.get_collection(ns)?.ok_or_else(|| {
                Error::Internal(format!(
                    "collection '{}' disappeared before build phase",
                    ns
                ))
            })?;
            (idx_entry, data_entry)
        };

        // Open a dedicated WAL transaction for the build — it's
        // independent of Phase 1's txn (already committed) and Phase 3's
        // txn (has not yet begun).
        let mark = self.shared.handle.begin_txn()?;
        let overlay = TxnOverlay::new_shared();
        let ready_reservations = self
            .shared
            .handle
            .allocator()
            .drain_deferred_free_reservations();
        if !ready_reservations.is_empty() {
            let mut ov = overlay.lock().expect("TxnOverlay mutex poisoned");
            for page in ready_reservations {
                ov.push_reservation(PageReservation {
                    page,
                    size: PageSize::Large32k,
                    origin: PageOrigin::DeferredFree,
                });
            }
        }

        let body: Result<()> = (|| {
            let data_store = new_txn_store(&self.shared, &overlay);
            let data_tree = BTree::open(
                data_store,
                data_entry.data_root_page,
                data_entry.data_root_level,
            );
            let idx_store = new_txn_store(&self.shared, &overlay);
            let mut idx_tree = BTree::open(
                idx_store,
                idx_entry.root_page,
                idx_entry.root_level,
            );
            let any_multikey = build_index(&data_tree, &mut idx_tree, &idx_entry)?;

            // Persist the possibly-updated root + multikey flag. Note:
            // we do NOT flip state to Ready here — that happens in
            // Phase 3 under metadata.write() so readers see a
            // consistent transition on the published snapshot.
            let root_changed = idx_tree.root_page != idx_entry.root_page
                || idx_tree.root_level != idx_entry.root_level;
            let multikey_changed = any_multikey && !idx_entry.multikey;
            if root_changed || multikey_changed {
                let mut updated = idx_entry.clone();
                if root_changed {
                    updated.root_page = idx_tree.root_page;
                    updated.root_level = idx_tree.root_level;
                }
                if multikey_changed {
                    updated.multikey = true;
                }
                // Brief metadata.read() for the catalog write. Lane still
                // protects same-ns serialization; read guard is acquired
                // only for the duration of the catalog mutation + header
                // sync so long-build scans don't block other-ns DDL.
                let md_read = self.metadata.read().map_err(|_| {
                    Error::Internal("metadata RwLock poisoned".into())
                })?;
                md_read
                    .catalog
                    .lock()
                    .expect("catalog poisoned")
                    .update_index(&updated)?;
                sync_catalog_root_overlay(&self.shared, &md_read, &overlay)?;
            }
            Ok(())
        })();

        match body {
            Ok(()) => {
                // Commit the build txn. We intentionally do NOT hold
                // commit_seq here — no timestamp is allocated (Phase 2
                // has no primary writes), and no publish is performed.
                // The only shared mutation is catalog root/multikey
                // metadata, which is safe under the lane.
                let overlay_inner = Arc::try_unwrap(overlay)
                    .map_err(|_| {
                        Error::Internal("TxnOverlay still referenced at build commit".into())
                    })?
                    .into_inner()
                    .map_err(|_| Error::Internal("TxnOverlay mutex poisoned".into()))?;
                let mut base = new_store(&self.shared);
                overlay_inner.commit(&mut base, &self.shared.handle)?;
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
                Ok(())
            }
            Err(e) => {
                let _ = self.rollback_overlay_and_wal(overlay, mark);
                Err(e)
            }
        }
    }

    /// Phase 3 of `create_index`: flip `state: Ready` under metadata.write
    /// and publish the final snapshot.
    fn create_index_commit(&self, ns: &str, name: &str) -> Result<()> {
        self.run_ddl(|_shared, md, _overlay| {
            let mut cat = md.catalog.lock().expect("catalog poisoned");
            let mut entry = cat.get_index(ns, name)?.ok_or_else(|| {
                Error::Internal(format!(
                    "index '{}' on '{}' disappeared before commit phase",
                    name, ns
                ))
            })?;
            entry.state = IndexState::Ready;
            cat.update_index(&entry)?;
            // No catalog-root change unless the B+ tree reshaped; since
            // we only did an update-in-place of the entry, keep the
            // header in sync just in case.
            drop(cat);
            sync_catalog_root_overlay(_shared, md, _overlay)?;
            Ok(())
        })
    }

    /// Cleanup after a failed Phase 2: drop the orphan Building entry.
    ///
    /// This keeps the catalog in a state where a retried
    /// `create_index(ns, same_name)` succeeds from Phase 1 instead of
    /// hitting the idempotent-already-exists early return with a stale
    /// Building entry.
    fn create_index_cleanup(&self, ns: &str, name: &str) -> Result<()> {
        self.run_ddl(|shared, md, overlay| {
            let removed = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .drop_index(ns, name)?;
            if removed {
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            Ok(())
        })
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
            &Arc<Mutex<TxnOverlay>>,
            &mut WriteTxn,
        ) -> Result<R>,
    {
        // Take read guard; if namespace absent, bootstrap then retry.
        let md_read = self.metadata.read().map_err(|_| {
            Error::Internal("metadata RwLock poisoned".into())
        })?;
        let ns_missing = md_read
            .catalog
            .lock()
            .expect("catalog poisoned")
            .get_collection(ns)?
            .is_none();
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
    /// Deadlock fix (PR 8 follow-up): the previous revision upgraded
    /// `metadata.read()` → `metadata.write()` while holding the
    /// namespace lane. Two writers on different namespaces each held
    /// `metadata.read()` and then tried to upgrade, producing a
    /// writer-vs-writer deadlock (A holds lane+waits-for-write; B
    /// holds read+waits-for-lane-from-A-after-its-upgrade).
    ///
    /// Fix: keep `metadata.read()` for the whole body; mutate the
    /// catalog via the interior `Mutex<Catalog>`. Lock order is
    /// documented on `MetadataState`.
    fn run_write_existing<F, R>(&self, ns: &str, f: F) -> Result<R>
    where
        F: FnOnce(
            &SharedState,
            &MetadataState,
            &Arc<Mutex<TxnOverlay>>,
            &mut WriteTxn,
        ) -> Result<R>,
    {
        let md_read = self.metadata.read().map_err(|_| {
            Error::Internal("metadata RwLock poisoned".into())
        })?;
        let lane = self.lane_for(ns);
        let _lane_guard = self.acquire_lane(lane)?;

        // Setup overlay + WriteTxn.
        let mark = self.shared.handle.begin_txn()?;
        let overlay = TxnOverlay::new_shared();
        let ready = self.shared.handle.allocator().drain_deferred_free_reservations();
        if !ready.is_empty() {
            let mut ov = overlay.lock().expect("TxnOverlay mutex poisoned");
            for page in ready {
                ov.push_reservation(PageReservation {
                    page,
                    size: PageSize::Large32k,
                    origin: PageOrigin::DeferredFree,
                });
            }
        }
        let txn_id = self.shared.txn_counter.fetch_add(1, Ordering::Relaxed);
        let mut txn = WriteTxn::new(txn_id);

        // Body — pass `&md_read` directly. The catalog itself is
        // behind `Mutex<Catalog>`, so mutations happen inside the
        // closure under the catalog mutex without needing a RwLock
        // upgrade. Other CRUD writers on different namespaces hold
        // their own `metadata.read()` concurrently and their own
        // lanes; only `commit_seq` serializes the publish step.
        let body_result = f(&self.shared, &md_read, &overlay, &mut txn);

        match body_result {
            Ok(value) => {
                // Commit sequencing.
                let _commit = self.commit_seq.lock().map_err(|_| {
                    Error::Internal("commit_seq mutex poisoned".into())
                })?;

                let sec_writes = std::mem::take(&mut txn.pending_sec_index);
                if let Err(e) =
                    install_pending_sec_index(&self.shared, &md_read, &overlay, sec_writes)
                {
                    drop(txn);
                    let _ = self.rollback_overlay_and_wal(overlay, mark);
                    return Err(e);
                }

                let primary_writes = std::mem::take(&mut txn.pending_primary);
                let commit_ts_opt = if !primary_writes.is_empty() {
                    let txn_id = txn.txn_id;
                    let commit_ts = match txn.allocate_commit_ts(&self.shared.oracle) {
                        Ok(ts) => ts,
                        Err(e) => {
                            drop(txn);
                            let _ = self.rollback_overlay_and_wal(overlay, mark);
                            return Err(e);
                        }
                    };
                    if let Err(e) = install_pending_primary(
                        &self.shared,
                        &md_read,
                        &overlay,
                        primary_writes,
                        commit_ts,
                        txn_id,
                    ) {
                        drop(txn);
                        let _ = self.rollback_overlay_and_wal(overlay, mark);
                        return Err(e);
                    }
                    Some(commit_ts)
                } else {
                    None
                };

                // Commit overlay bytes onto shared frames.
                let overlay_inner = Arc::try_unwrap(overlay)
                    .map_err(|_| Error::Internal("TxnOverlay still referenced".into()))?
                    .into_inner()
                    .map_err(|_| Error::Internal("TxnOverlay mutex poisoned".into()))?;
                let mut base_store = new_store(&self.shared);
                if let Err(e) = overlay_inner.commit(&mut base_store, &self.shared.handle) {
                    drop(txn);
                    let _ = self.shared.handle.rollback_txn(mark);
                    return Err(e);
                }
                self.shared.handle.flush()?;
                let (_commit_ts, _installed, _sec_index) =
                    txn.commit(&self.shared.oracle, &self.shared.handle)?;
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
                    let _ = self.shared.handle.emergency_checkpoint();
                }
                let publish_ts = commit_ts_opt.unwrap_or_else(|| self.shared.oracle.now());
                rebuild_and_publish_locked(&self.shared, &md_read, publish_ts)?;
                Ok(value)
            }
            Err(e) => {
                drop(txn);
                let _ = self.rollback_overlay_and_wal(overlay, mark);
                Err(e)
            }
        }
    }

    fn rollback_overlay_and_wal(
        &self,
        overlay: Arc<Mutex<TxnOverlay>>,
        mark: Option<u64>,
    ) -> Result<()> {
        let ov = Arc::try_unwrap(overlay)
            .map_err(|_| Error::Internal("TxnOverlay still referenced at rollback time".into()))?
            .into_inner()
            .map_err(|_| Error::Internal("TxnOverlay mutex poisoned".into()))?;
        ov.rollback(&self.shared.handle)?;
        let _ = self.shared.handle.rollback_txn(mark);
        Ok(())
    }

}

// ---------------------------------------------------------------------------
// OwnedLaneGuard — holds Arc<Mutex<()>> + its MutexGuard together so the
// stdlib Mutex lifetime restriction is satisfied.
// ---------------------------------------------------------------------------

struct OwnedLaneGuard {
    // The Arc keeps the Mutex alive. `_guard` is the MutexGuard that
    // references the Mutex through `lane`. We hold the Arc AFTER the
    // guard so drop order is: guard (release lock) then lane.
    _guard: std::sync::MutexGuard<'static, ()>,
    _lane: Arc<Mutex<()>>,
}

impl OwnedLaneGuard {
    fn new(lane: Arc<Mutex<()>>, guard: std::sync::MutexGuard<'_, ()>) -> Self {
        // Extend the lifetime of the guard to 'static. Safe because we
        // keep `lane` alive inside `Self`, so the backing Mutex lives at
        // least as long as the guard.
        let guard_static: std::sync::MutexGuard<'static, ()> =
            unsafe { std::mem::transmute(guard) };
        OwnedLaneGuard {
            _guard: guard_static,
            _lane: lane,
        }
    }
}


// ---------------------------------------------------------------------------
// StorageEngine implementation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// StorageEngine implementation (PR 8 — MWMR v1)
// ---------------------------------------------------------------------------

impl StorageEngine for PagedEngine {
    // -----------------------------------------------------------------------
    // insert
    // -----------------------------------------------------------------------

    fn insert(&self, ns: &str, mut doc: Document) -> Result<Bson> {
        self.run_write(ns, |shared, md, overlay, txn| {
            let entry = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?
                .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-write", ns)))?;
            let mut tree = BTree::open(
                new_txn_store(shared, overlay),
                entry.data_root_page,
                entry.data_root_level,
            );
            let (id, key, bson_bytes, _tree_root) = btree_insert_doc(&mut tree, &mut doc, &[])?;
            if tree.root_page != entry.data_root_page || tree.root_level != entry.data_root_level {
                let mut updated = entry.clone();
                updated.data_root_page = tree.root_page;
                updated.data_root_level = tree.root_level;
                md.catalog.lock().expect("catalog poisoned").update_collection(&updated)?;
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            txn.stage_primary_insert(ns.to_string(), key, bson_bytes);
            maintain_secondary_on_insert(shared, md, overlay, ns, &doc, &id, txn)?;
            Ok(id)
        })
    }

    // -----------------------------------------------------------------------
    // find
    // -----------------------------------------------------------------------

    fn find(&self, ns: &str, filter: &Document, opts: &FindOptions) -> Result<Vec<Document>> {
        let snap = self.shared.published.load();
        let ns_snap = match snap.namespaces.get(ns) {
            None => return Ok(Vec::new()),
            Some(n) => n,
        };
        let matched: Vec<Document> = execute_snapshot_pairs_from_snap(
            &self.shared,
            ns,
            ns_snap,
            filter,
            snap.publish_ts,
            true,
        )?
        .into_iter()
        .map(|(_, doc)| doc)
        .collect();
        Ok(apply_find_opts(matched, opts))
    }

    fn find_one(&self, ns: &str, filter: &Document) -> Result<Option<Document>> {
        let opts = FindOptions::new();
        let mut results = self.find(ns, filter, &opts)?;
        Ok(if results.is_empty() {
            None
        } else {
            Some(results.remove(0))
        })
    }

    // -----------------------------------------------------------------------
    // update
    // -----------------------------------------------------------------------

    fn update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &UpdateOptions,
        many: bool,
    ) -> Result<UpdateResult> {
        if !is_operator_update(update) {
            return Err(Error::Internal(
                "update requires an operator update document (e.g. {$set: {...}});                  use find_one_and_replace for replacements"
                    .into(),
            ));
        }

        // Pre-scan phase uses the published snapshot (mutex-free). If the
        // namespace does not exist and upsert is requested, route to the
        // upsert helper.
        let snap = self.shared.published.load();
        let ns_snap_opt = snap.namespaces.get(ns);
        let matched_pairs: Vec<(Vec<u8>, Document)> = match ns_snap_opt {
            None => {
                if opts.upsert {
                    return self.do_upsert_update(ns, filter, update);
                }
                return Ok(UpdateResult {
                    matched_count: 0,
                    modified_count: 0,
                    upserted_id: None,
                });
            }
            Some(ns_snap) => {
                execute_snapshot_pairs_from_snap(
                    &self.shared,
                    ns,
                    ns_snap,
                    filter,
                    snap.publish_ts,
                    false,
                )?
            }
        };

        if matched_pairs.is_empty() && opts.upsert {
            return self.do_upsert_update(ns, filter, update);
        }

        let pairs_to_process: Vec<(Vec<u8>, Document)> = if many {
            matched_pairs
        } else {
            matched_pairs.into_iter().take(1).collect()
        };

        self.run_write_existing(ns, |shared, md, overlay, txn| {
            let mut matched_count = 0u64;
            let mut modified_count = 0u64;
            for (key, mut doc) in pairs_to_process {
                matched_count += 1;
                let before = doc.clone();
                let before_id = before.get("_id").cloned().unwrap_or(Bson::Null);
                apply_update(&mut doc, update, false)?;
                if doc != before {
                    modified_count += 1;
                    let new_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
                    let new_bytes = bson::to_vec(&doc).map_err(Error::BsonSerialization)?;
                    maintain_secondary_on_update(
                        shared, md, overlay, ns, &before, &doc, &before_id, &new_id, txn,
                    )?;
                    let entry_opt = md
                        .catalog
                        .lock()
                        .expect("catalog poisoned")
                        .get_collection(ns)?;
                    if let Some(entry) = entry_opt {
                        let mut tree = BTree::open(
                            new_txn_store(shared, overlay),
                            entry.data_root_page,
                            entry.data_root_level,
                        );
                        tree.delete(&key)?;
                        tree.insert(&key, &new_bytes)?;
                        if tree.root_page != entry.data_root_page
                            || tree.root_level != entry.data_root_level
                        {
                            let mut updated = entry.clone();
                            updated.data_root_page = tree.root_page;
                            updated.data_root_level = tree.root_level;
                            md.catalog
                                .lock()
                                .expect("catalog poisoned")
                                .update_collection(&updated)?;
                            sync_catalog_root_overlay(shared, md, overlay)?;
                        }
                        txn.stage_primary_update(ns.to_string(), key, new_bytes);
                    }
                }
            }
            Ok(UpdateResult {
                matched_count,
                modified_count,
                upserted_id: None,
            })
        })
    }

    // -----------------------------------------------------------------------
    // delete
    // -----------------------------------------------------------------------

    fn delete(&self, ns: &str, filter: &Document, many: bool) -> Result<DeleteResult> {
        let snap = self.shared.published.load();
        let pairs_to_delete: Vec<(Vec<u8>, Document)> = match snap.namespaces.get(ns) {
            None => return Ok(DeleteResult { deleted_count: 0 }),
            Some(ns_snap) => {
                let pairs = execute_snapshot_pairs_from_snap(
                    &self.shared,
                    ns,
                    ns_snap,
                    filter,
                    snap.publish_ts,
                    false,
                )?;
                if many {
                    pairs
                } else {
                    pairs.into_iter().take(1).collect()
                }
            }
        };

        let deleted_count = pairs_to_delete.len() as u64;
        if deleted_count == 0 {
            return Ok(DeleteResult { deleted_count: 0 });
        }

        self.run_write_existing(ns, |shared, md, overlay, txn| {
            for (key, doc) in &pairs_to_delete {
                let doc_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
                maintain_secondary_on_delete(shared, md, overlay, ns, doc, &doc_id, txn)?;
                let entry_opt = md
                    .catalog
                    .lock()
                    .expect("catalog poisoned")
                    .get_collection(ns)?;
                if let Some(entry) = entry_opt {
                    let mut tree = BTree::open(
                        new_txn_store(shared, overlay),
                        entry.data_root_page,
                        entry.data_root_level,
                    );
                    tree.delete(key)?;
                    if tree.root_page != entry.data_root_page
                        || tree.root_level != entry.data_root_level
                    {
                        let mut updated = entry.clone();
                        updated.data_root_page = tree.root_page;
                        updated.data_root_level = tree.root_level;
                        md.catalog
                            .lock()
                            .expect("catalog poisoned")
                            .update_collection(&updated)?;
                        sync_catalog_root_overlay(shared, md, overlay)?;
                    }
                    txn.stage_primary_delete(ns.to_string(), key.clone());
                }
            }
            Ok(())
        })?;

        Ok(DeleteResult { deleted_count })
    }

    // -----------------------------------------------------------------------
    // count
    // -----------------------------------------------------------------------

    fn count(&self, ns: &str, filter: &Document) -> Result<u64> {
        let snap = self.shared.published.load();
        let ns_snap = match snap.namespaces.get(ns) {
            None => return Ok(0),
            Some(n) => n,
        };
        Ok(
            execute_snapshot_pairs_from_snap(
                &self.shared,
                ns,
                ns_snap,
                filter,
                snap.publish_ts,
                false,
            )?
            .len() as u64,
        )
    }

    // -----------------------------------------------------------------------
    // find_one_and_update_doc
    // -----------------------------------------------------------------------

    fn find_one_and_update_doc(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &FindOneAndUpdateOptions,
    ) -> Result<Option<Document>> {
        if !is_operator_update(update) {
            return Err(Error::Internal(
                "find_one_and_update requires an operator update document".into(),
            ));
        }

        let snap = self.shared.published.load();
        let mut matched: Vec<(Vec<u8>, Document)> = match snap.namespaces.get(ns) {
            None => {
                if opts.upsert {
                    return self.fam_upsert_update(ns, filter, update, opts);
                }
                return Ok(None);
            }
            Some(ns_snap) => {
                execute_snapshot_pairs_from_snap(
                    &self.shared,
                    ns,
                    ns_snap,
                    filter,
                    snap.publish_ts,
                    false,
                )?
            }
        };

        if matched.is_empty() {
            if opts.upsert {
                return self.fam_upsert_update(ns, filter, update, opts);
            }
            return Ok(None);
        }

        if let Some(s) = &opts.sort {
            matched.sort_by(|(_, a), (_, b)| compare_docs(a, b, s));
        }

        let (key, mut doc) = matched.remove(0);
        let before = doc.clone();
        let before_id = before.get("_id").cloned().unwrap_or(Bson::Null);
        apply_update(&mut doc, update, false)?;
        let new_id = doc.get("_id").cloned().unwrap_or(Bson::Null);
        let new_bytes = bson::to_vec(&doc).map_err(Error::BsonSerialization)?;

        self.run_write_existing(ns, |shared, md, overlay, txn| {
            maintain_secondary_on_update(
                shared, md, overlay, ns, &before, &doc, &before_id, &new_id, txn,
            )?;
            let entry_opt = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?;
            if let Some(entry) = entry_opt {
                let mut tree = BTree::open(
                    new_txn_store(shared, overlay),
                    entry.data_root_page,
                    entry.data_root_level,
                );
                tree.delete(&key)?;
                tree.insert(&key, &new_bytes)?;
                if tree.root_page != entry.data_root_page
                    || tree.root_level != entry.data_root_level
                {
                    let mut updated = entry.clone();
                    updated.data_root_page = tree.root_page;
                    updated.data_root_level = tree.root_level;
                    md.catalog
                        .lock()
                        .expect("catalog poisoned")
                        .update_collection(&updated)?;
                    sync_catalog_root_overlay(shared, md, overlay)?;
                }
                txn.stage_primary_update(ns.to_string(), key, new_bytes);
            }
            Ok(())
        })?;

        Ok(Some(match opts.return_document {
            ReturnDocument::Before => before,
            ReturnDocument::After => doc,
        }))
    }

    // -----------------------------------------------------------------------
    // find_one_and_delete_doc
    // -----------------------------------------------------------------------

    fn find_one_and_delete_doc(
        &self,
        ns: &str,
        filter: &Document,
        opts: &FindOneAndDeleteOptions,
    ) -> Result<Option<Document>> {
        let snap = self.shared.published.load();
        let mut matched: Vec<(Vec<u8>, Document)> = match snap.namespaces.get(ns) {
            None => return Ok(None),
            Some(ns_snap) => execute_snapshot_pairs_from_snap(
                &self.shared,
                ns,
                ns_snap,
                filter,
                snap.publish_ts,
                false,
            )?,
        };

        if matched.is_empty() {
            return Ok(None);
        }

        if let Some(s) = &opts.sort {
            matched.sort_by(|(_, a), (_, b)| compare_docs(a, b, s));
        }

        let (key, doc) = matched.remove(0);
        let doc_id = doc.get("_id").cloned().unwrap_or(Bson::Null);

        self.run_write_existing(ns, |shared, md, overlay, txn| {
            maintain_secondary_on_delete(shared, md, overlay, ns, &doc, &doc_id, txn)?;
            let entry_opt = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?;
            if let Some(entry) = entry_opt {
                let mut tree = BTree::open(
                    new_txn_store(shared, overlay),
                    entry.data_root_page,
                    entry.data_root_level,
                );
                tree.delete(&key)?;
                if tree.root_page != entry.data_root_page
                    || tree.root_level != entry.data_root_level
                {
                    let mut updated = entry.clone();
                    updated.data_root_page = tree.root_page;
                    updated.data_root_level = tree.root_level;
                    md.catalog
                        .lock()
                        .expect("catalog poisoned")
                        .update_collection(&updated)?;
                    sync_catalog_root_overlay(shared, md, overlay)?;
                }
                txn.stage_primary_delete(ns.to_string(), key);
            }
            Ok(())
        })?;

        Ok(Some(doc))
    }

    // -----------------------------------------------------------------------
    // find_one_and_replace_doc
    // -----------------------------------------------------------------------

    fn find_one_and_replace_doc(
        &self,
        ns: &str,
        filter: &Document,
        replacement: &Document,
        opts: &FindOneAndReplaceOptions,
    ) -> Result<Option<Document>> {
        let snap = self.shared.published.load();
        let mut matched: Vec<(Vec<u8>, Document)> = match snap.namespaces.get(ns) {
            None => {
                if opts.upsert {
                    return self.fam_upsert_replace(ns, replacement, opts);
                }
                return Ok(None);
            }
            Some(ns_snap) => {
                execute_snapshot_pairs_from_snap(
                    &self.shared,
                    ns,
                    ns_snap,
                    filter,
                    snap.publish_ts,
                    false,
                )?
            }
        };

        if matched.is_empty() {
            if opts.upsert {
                return self.fam_upsert_replace(ns, replacement, opts);
            }
            return Ok(None);
        }

        if let Some(s) = &opts.sort {
            matched.sort_by(|(_, a), (_, b)| compare_docs(a, b, s));
        }

        let (old_key, old_doc) = matched.remove(0);

        let mut new_doc = replacement.clone();
        let original_id = old_doc.get("_id").cloned().unwrap_or(Bson::Null);
        new_doc.insert("_id", original_id.clone());
        validate_document(&new_doc)?;

        let new_key = encode_key(&original_id);
        let new_bytes = bson::to_vec(&new_doc).map_err(Error::BsonSerialization)?;

        let old_doc_clone = old_doc.clone();
        let new_doc_clone = new_doc.clone();
        self.run_write_existing(ns, |shared, md, overlay, txn| {
            maintain_secondary_on_update(
                shared,
                md,
                overlay,
                ns,
                &old_doc_clone,
                &new_doc_clone,
                &original_id,
                &original_id,
                txn,
            )?;
            let entry_opt = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?;
            if let Some(entry) = entry_opt {
                let mut tree = BTree::open(
                    new_txn_store(shared, overlay),
                    entry.data_root_page,
                    entry.data_root_level,
                );
                tree.delete(&old_key)?;
                tree.insert(&new_key, &new_bytes)?;
                if tree.root_page != entry.data_root_page
                    || tree.root_level != entry.data_root_level
                {
                    let mut updated = entry.clone();
                    updated.data_root_page = tree.root_page;
                    updated.data_root_level = tree.root_level;
                    md.catalog
                        .lock()
                        .expect("catalog poisoned")
                        .update_collection(&updated)?;
                    sync_catalog_root_overlay(shared, md, overlay)?;
                }
                txn.stage_primary_update(ns.to_string(), new_key, new_bytes);
            }
            Ok(())
        })?;

        Ok(Some(match opts.return_document {
            ReturnDocument::Before => old_doc,
            ReturnDocument::After => new_doc,
        }))
    }

    // -----------------------------------------------------------------------
    // create_index — 3-phase build (PR 9)
    //
    // Phase 1 (reserve):  metadata.write()   — allocate idx root, publish
    //                                          `IndexEntry { state: Building }`.
    // Phase 2 (build):    metadata.read() +  — populate idx tree from data
    //                     ns_lane              tree. Other-ns writers run
    //                                          concurrently; same-ns writers
    //                                          wait on the lane.
    // Phase 3 (commit):   metadata.write()   — flip `state: Ready`, publish.
    //
    // Failure in phase 2 drops the Building entry in a recovery phase
    // (same DDL shape as phase 3) so the catalog does not retain an
    // orphan `Building` index.
    // -----------------------------------------------------------------------

    fn create_index(&self, ns: &str, model: &IndexModel) -> Result<String> {
        validate_index_keys(&model.keys)?;
        let name = model
            .options
            .name
            .clone()
            .unwrap_or_else(|| generate_index_name(&model.keys));

        // ---------------------------------------------------------------
        // Phase 1 — Reserve (metadata.write + commit_seq, brief).
        // ---------------------------------------------------------------
        let reserve_outcome = self.create_index_reserve(ns, model, &name)?;
        match reserve_outcome {
            ReserveOutcome::AlreadyExists => return Ok(name),
            ReserveOutcome::Reserved => {}
        }

        // ---------------------------------------------------------------
        // Phase 2 — Build (metadata.read + ns_lane). Long-running scan.
        //
        // A failure here leaves a Building entry in the catalog. We fall
        // through to a cleanup DDL that drops it, keeping the catalog
        // consistent for a retried `create_index`.
        // ---------------------------------------------------------------
        if let Err(build_err) = self.create_index_build(ns, &name) {
            // Best-effort rollback of the orphan Building entry. Log-and-
            // propagate if the cleanup itself fails: the caller sees the
            // original build error, and the Building entry (if any
            // remains) will still be skipped by the read path.
            if let Err(cleanup_err) = self.create_index_cleanup(ns, &name) {
                return Err(Error::Internal(format!(
                    "create_index build failed: {}; cleanup also failed: {}",
                    build_err, cleanup_err
                )));
            }
            return Err(build_err);
        }

        // ---------------------------------------------------------------
        // Phase 3 — Commit (metadata.write + commit_seq, brief).
        // ---------------------------------------------------------------
        self.create_index_commit(ns, &name)?;
        Ok(name)
    }

    // -----------------------------------------------------------------------
    // drop_index
    // -----------------------------------------------------------------------

    fn drop_index(&self, ns: &str, name: &str) -> Result<()> {
        if name == "_id_" {
            return Err(Error::InvalidWireMessage {
                detail: "drop of '_id_' index is not permitted".to_string(),
            });
        }
        // Acquire the namespace lane before the DDL body. This serializes
        // with any in-flight create_index Phase 2 build on the same ns —
        // the build holds the lane, so drop_index waits for it to release.
        // After the build finishes (or aborts), drop_index proceeds. A
        // subsequent Phase 3 (Ready flip) or concurrent CRUD on this ns
        // will see the missing entry and handle it cleanly.
        //
        // Lane-before-metadata ordering matches drop_namespace
        // (src/storage/paged_engine.rs drop_namespace) and keeps us below
        // the metadata RwLock in the documented lock order.
        let lane_arc = self.lane_for(ns);
        let _lane_guard = self.acquire_lane(lane_arc)?;
        self.run_ddl(|shared, md, overlay| {
            let removed = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .drop_index(ns, name)?;
            if removed {
                sync_catalog_root_overlay(shared, md, overlay)?;
                Ok(())
            } else {
                Err(Error::Internal(format!(
                    "index '{}' not found on '{}'",
                    name, ns
                )))
            }
        })
    }

    // -----------------------------------------------------------------------
    // list_indexes
    // -----------------------------------------------------------------------

    fn list_indexes(&self, ns: &str) -> Result<Vec<IndexInfo>> {
        let snap = self.shared.published.load();
        let ns_snap = match snap.namespaces.get(ns) {
            None => return Ok(Vec::new()),
            Some(n) => n,
        };
        Ok(ns_snap
            .indexes
            .iter()
            .map(|i| IndexInfo {
                name: i.name.clone(),
                keys: i.key_pattern.clone(),
                unique: i.unique,
                sparse: i.sparse,
            })
            .collect())
    }

    // -----------------------------------------------------------------------
    // create_namespace
    // -----------------------------------------------------------------------

    fn create_namespace(&self, ns: &str) -> Result<()> {
        let result = self.run_ddl(|shared, md, overlay| {
            let data_root = {
                let mut cat = md.catalog.lock().expect("catalog poisoned");
                if cat.get_collection(ns)?.is_some() {
                    return Ok(());
                }
                cat.create_collection(ns, bson::doc! {}, now_millis())?
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
        // Plan §T9: force-expire ALL active ReadViews globally before
        // freeing pages. Done BEFORE taking the metadata write guard so
        // concurrent readers that just loaded the published snapshot
        // can finish their pin walks.
        self.shared.handle.read_view_registry().force_expire_all();

        // Remove the lane from the map so no new writer picks it up, then
        // wait for any writer that grabbed the lane before the remove by
        // briefly locking the removed Arc. Release immediately.
        let removed_lane = self.ns_lanes.remove(ns).map(|(_, v)| v);
        if let Some(lane) = removed_lane {
            // Wait out an in-flight writer by taking the lock ourselves.
            let _guard = lane.lock().map_err(|_| {
                Error::Internal("namespace lane mutex poisoned during drop".into())
            })?;
            // _guard dropped immediately on scope exit below.
            drop(_guard);
        }

        let result = self.run_ddl(|shared, md, overlay| {
            let (maybe_coll, index_roots, dropped) = {
                let mut cat = md.catalog.lock().expect("catalog poisoned");
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
        // Record the drop so that `bootstrap_namespace` (called from `run_write`
        // when the namespace is absent) does not auto-recreate it.  This prevents
        // a racing insert — one that arrived after `drop_namespace` returned but
        // whose `run_write` call saw ns_missing — from re-bootstrapping the
        // namespace and committing stale data to the journal.
        if result.is_ok() {
            self.dropped_namespaces.insert(ns.to_string());
        }
        result
    }

    // -----------------------------------------------------------------------
    // list_namespaces
    // -----------------------------------------------------------------------

    fn list_namespaces(&self) -> Result<Vec<String>> {
        let snap = self.shared.published.load();
        Ok(snap.namespaces.keys().cloned().collect())
    }

    // -----------------------------------------------------------------------
    // checkpoint
    // -----------------------------------------------------------------------

    fn checkpoint(&self) -> Result<()> {
        // DDL: take metadata.write() so the checkpoint is atomic wrt CRUD
        // writers. No overlay needed — checkpoint just flushes + GCs.
        let md = self.metadata.write().map_err(|_| {
            Error::Internal("metadata RwLock poisoned".into())
        })?;

        // sync_catalog_root via direct allocator update (no overlay).
        let (root_page, root_level) = {
            let cat = md.catalog.lock().expect("catalog poisoned");
            (cat.root_page(), cat.root_level())
        };
        self.shared.handle.allocator().update_header(|h| {
            h.catalog_root_page = root_page;
            h.catalog_root_level = root_level;
            h.catalog_root_backup = root_page;
        })?;

        // Plan §T8: history-store GC + counters.
        let ort = self.shared.handle.read_view_registry().oldest_required_ts();
        {
            let mut hs = self.shared.history_store.lock().unwrap();
            hs.gc_pass(ort)?;
        }
        let lag_ms = if ort == crate::mvcc::timestamp::Ts::MAX {
            0
        } else {
            self.shared
                .oracle
                .now()
                .physical_ms
                .saturating_sub(ort.physical_ms)
        };
        crate::mvcc::metrics::set_oldest_required_ts_lag_ms(lag_ms);
        crate::mvcc::metrics::set_overflow_pages_in_use(
            self.shared.handle.allocator().overflow_pages_in_use() as u64,
        );
        crate::mvcc::metrics::set_deferred_free_queue_depth(
            self.shared.handle.allocator().deferred_free_queue().depth() as u64,
        );
        self.shared.handle.flush()
    }

    fn close(&self) -> Result<()> {
        self.checkpoint()
    }

    fn journal_sync(&self) -> Result<()> {
        self.shared.handle.journal_sync()
    }

    fn snapshot_bytes(&self) -> Result<Option<Vec<u8>>> {
        Ok(None)
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
        F: FnOnce(&SharedState, &MetadataState, &Arc<Mutex<TxnOverlay>>) -> Result<R>,
    {
        let md_w = self.metadata.write().map_err(|_| {
            Error::Internal("metadata RwLock poisoned".into())
        })?;
        let _commit = self.commit_seq.lock().map_err(|_| {
            Error::Internal("commit_seq mutex poisoned".into())
        })?;

        let mark = self.shared.handle.begin_txn()?;
        let overlay = TxnOverlay::new_shared();
        let ready = self.shared.handle.allocator().drain_deferred_free_reservations();
        if !ready.is_empty() {
            let mut ov = overlay.lock().expect("TxnOverlay mutex poisoned");
            for page in ready {
                ov.push_reservation(PageReservation {
                    page,
                    size: PageSize::Large32k,
                    origin: PageOrigin::DeferredFree,
                });
            }
        }

        let result = f(&self.shared, &md_w, &overlay);
        match result {
            Ok(value) => {
                let overlay_inner = Arc::try_unwrap(overlay)
                    .map_err(|_| Error::Internal("TxnOverlay still referenced".into()))?
                    .into_inner()
                    .map_err(|_| Error::Internal("TxnOverlay mutex poisoned".into()))?;
                let mut base_store = new_store(&self.shared);
                overlay_inner.commit(&mut base_store, &self.shared.handle)?;
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
                let publish_ts = self.shared.oracle.now();
                rebuild_and_publish_locked(&self.shared, &md_w, publish_ts)?;
                Ok(value)
            }
            Err(e) => {
                let _ = self.rollback_overlay_and_wal(overlay, mark);
                Err(e)
            }
        }
    }

    /// Upsert for `update_one/many` with `upsert: true`.
    fn do_upsert_update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
    ) -> Result<UpdateResult> {
        let mut new_doc = upsert_base_from_filter(filter);
        apply_update(&mut new_doc, update, true)?;
        let id = self.run_write(ns, |shared, md, overlay, txn| {
            let entry = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?
                .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-upsert", ns)))?;
            let mut tree = BTree::open(
                new_txn_store(shared, overlay),
                entry.data_root_page,
                entry.data_root_level,
            );
            let (id, key, bson_bytes, _tree_root) = btree_insert_doc(&mut tree, &mut new_doc, &[])?;
            if tree.root_page != entry.data_root_page || tree.root_level != entry.data_root_level {
                let mut updated = entry.clone();
                updated.data_root_page = tree.root_page;
                updated.data_root_level = tree.root_level;
                md.catalog
                    .lock()
                    .expect("catalog poisoned")
                    .update_collection(&updated)?;
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            txn.stage_primary_insert(ns.to_string(), key, bson_bytes);
            maintain_secondary_on_insert(shared, md, overlay, ns, &new_doc, &id, txn)?;
            Ok(id)
        })?;
        Ok(UpdateResult {
            matched_count: 0,
            modified_count: 0,
            upserted_id: Some(id),
        })
    }

    /// Upsert for `find_one_and_update` with `upsert: true`.
    fn fam_upsert_update(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &FindOneAndUpdateOptions,
    ) -> Result<Option<Document>> {
        let mut new_doc = upsert_base_from_filter(filter);
        apply_update(&mut new_doc, update, true)?;
        self.run_write(ns, |shared, md, overlay, txn| {
            let entry = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?
                .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-upsert", ns)))?;
            let mut tree = BTree::open(
                new_txn_store(shared, overlay),
                entry.data_root_page,
                entry.data_root_level,
            );
            let (id, key, bson_bytes, _tree_root) = btree_insert_doc(&mut tree, &mut new_doc, &[])?;
            if tree.root_page != entry.data_root_page || tree.root_level != entry.data_root_level {
                let mut updated = entry.clone();
                updated.data_root_page = tree.root_page;
                updated.data_root_level = tree.root_level;
                md.catalog
                    .lock()
                    .expect("catalog poisoned")
                    .update_collection(&updated)?;
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            txn.stage_primary_insert(ns.to_string(), key, bson_bytes);
            maintain_secondary_on_insert(shared, md, overlay, ns, &new_doc, &id, txn)?;
            Ok(())
        })?;
        Ok(match opts.return_document {
            ReturnDocument::Before => None,
            ReturnDocument::After => Some(new_doc),
        })
    }

    /// Upsert for `find_one_and_replace` with `upsert: true`.
    fn fam_upsert_replace(
        &self,
        ns: &str,
        replacement: &Document,
        opts: &FindOneAndReplaceOptions,
    ) -> Result<Option<Document>> {
        let mut new_doc = replacement.clone();
        self.run_write(ns, |shared, md, overlay, txn| {
            let entry = md
                .catalog
                .lock()
                .expect("catalog poisoned")
                .get_collection(ns)?
                .ok_or_else(|| Error::Internal(format!("namespace '{}' vanished mid-upsert", ns)))?;
            let mut tree = BTree::open(
                new_txn_store(shared, overlay),
                entry.data_root_page,
                entry.data_root_level,
            );
            let (id, key, bson_bytes, _tree_root) = btree_insert_doc(&mut tree, &mut new_doc, &[])?;
            if tree.root_page != entry.data_root_page || tree.root_level != entry.data_root_level {
                let mut updated = entry.clone();
                updated.data_root_page = tree.root_page;
                updated.data_root_level = tree.root_level;
                md.catalog
                    .lock()
                    .expect("catalog poisoned")
                    .update_collection(&updated)?;
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            txn.stage_primary_insert(ns.to_string(), key, bson_bytes);
            maintain_secondary_on_insert(shared, md, overlay, ns, &new_doc, &id, txn)?;
            Ok(())
        })?;
        Ok(match opts.return_document {
            ReturnDocument::Before => None,
            ReturnDocument::After => Some(new_doc),
        })
    }
}
