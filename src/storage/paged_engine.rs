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
mod doc_helpers;
mod doc_ops;
mod index_build;
mod index_maint;
mod snapshot_ops;
mod state;

#[cfg(test)]
mod tests;

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use parking_lot::Mutex as ParkingMutex;

use dashmap::{DashMap, DashSet};

use crate::options::BusyHandler;

use bson::{Bson, Document};

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
use crate::storage_engine::StorageEngine;

use self::catalog_ops::{
    new_store, new_txn_store, rebuild_and_publish_locked, sync_catalog_root_overlay,
};
use self::doc_helpers::now_millis;
use self::index_maint::{install_pending_primary, install_pending_sec_index};
use self::state::{MetadataState, OwnedLaneGuard, SharedState};

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
    fn lane_for(&self, ns: &str) -> Arc<ParkingMutex<()>> {
        state::lane_for(self, ns)
    }

    /// Acquire the namespace lane with busy-timeout / busy-handler semantics
    /// matching the client's legacy `acquire_writer_lock`.
    fn acquire_lane(
        &self,
        lane: Arc<ParkingMutex<()>>,
    ) -> Result<OwnedLaneGuard> {
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
        let mut overlay = TxnOverlay::new();
        // Drain deferred-free into overlay reservations.
        let ready = self.shared.handle.allocator().drain_deferred_free_reservations();
        for page in ready {
            overlay.push_reservation(PageReservation {
                page,
                size: PageSize::Large32k,
                origin: PageOrigin::DeferredFree,
            });
        }

        let result: Result<()> = (|| {
            let data_root = md_w
                .catalog
                .lock()
                .expect("catalog poisoned")
                .create_collection(ns, bson::doc! {}, now_millis())?;
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
                    let _ = self.shared.handle.emergency_checkpoint();
                }
                let publish_ts = self.shared.oracle.now();
                rebuild_and_publish_locked(&self.shared, &md_w, publish_ts)?;
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
            &mut TxnOverlay,
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
        let mut overlay = TxnOverlay::new();
        let ready = self.shared.handle.allocator().drain_deferred_free_reservations();
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
                // Commit sequencing.
                let _commit = self.commit_seq.lock().map_err(|_| {
                    Error::Internal("commit_seq mutex poisoned".into())
                })?;

                let sec_writes = std::mem::take(&mut txn.pending_sec_index);
                if let Err(e) =
                    install_pending_sec_index(&self.shared, &md_read, &mut overlay, sec_writes.to_vec())
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
                        &mut overlay,
                        primary_writes.to_vec(),
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
                let mut base_store = new_store(&self.shared);
                if let Err(e) = overlay.commit(&mut base_store, &self.shared.handle) {
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
        overlay: TxnOverlay,
        mark: Option<u64>,
    ) -> Result<()> {
        overlay.rollback(&self.shared.handle)?;
        let _ = self.shared.handle.rollback_txn(mark);
        Ok(())
    }

}

// ---------------------------------------------------------------------------
// StorageEngine implementation
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// StorageEngine implementation (PR 8 — MWMR v1)
// ---------------------------------------------------------------------------

impl StorageEngine for PagedEngine {
    fn insert(&self, ns: &str, doc: Document) -> Result<Bson> {
        doc_ops::insert(self, ns, doc)
    }

    fn find(&self, ns: &str, filter: &Document, opts: &FindOptions) -> Result<Vec<Document>> {
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

    fn find_one_and_update_doc(
        &self,
        ns: &str,
        filter: &Document,
        update: &Document,
        opts: &FindOneAndUpdateOptions,
    ) -> Result<Option<Document>> {
        doc_ops::find_one_and_update_doc(self, ns, filter, update, opts)
    }

    fn find_one_and_delete_doc(
        &self,
        ns: &str,
        filter: &Document,
        opts: &FindOneAndDeleteOptions,
    ) -> Result<Option<Document>> {
        doc_ops::find_one_and_delete_doc(self, ns, filter, opts)
    }

    fn find_one_and_replace_doc(
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
        let snap = self.shared.published.load();
        let keys = snap.namespaces.keys();
        let mut out = Vec::with_capacity(keys.len());
        out.extend(keys.cloned());
        Ok(out)
    }

    fn checkpoint(&self) -> Result<()> {
        snapshot_ops::checkpoint(self)
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
        let md_w = self.metadata.write().map_err(|_| {
            Error::Internal("metadata RwLock poisoned".into())
        })?;
        let _commit = self.commit_seq.lock().map_err(|_| {
            Error::Internal("commit_seq mutex poisoned".into())
        })?;

        let mark = self.shared.handle.begin_txn()?;
        let mut overlay = TxnOverlay::new();
        let ready = self.shared.handle.allocator().drain_deferred_free_reservations();
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

}
