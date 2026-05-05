//! Three-phase `create_index` lifecycle (extracted from paged_engine.rs).
//!
//! Phases:
//! - [`PagedEngine::create_index_reserve`] — allocate a root page, write an
//!   `IndexEntry { state: Building }`, publish.
//! - [`PagedEngine::create_index_build`] — populate the index from the data
//!   tree under the namespace lane.
//! - [`PagedEngine::create_index_commit`] — flip `state: Ready` under
//!   `metadata.write()` and publish.
//! - [`PagedEngine::create_index_cleanup`] — drop an orphan Building entry
//!   on Phase-2 failure.

use crate::error::{Error, Result, WriteConflictReason};
use crate::index::IndexModel;
use crate::storage::btree::{BTree, BTreePageStore};
use crate::storage::buffer_pool::PageSize;
use crate::storage::catalog::{IndexEntry, IndexState};
use crate::storage::reconcile::plan::{TreeIdent, TreeKind};
use crate::storage::secondary_index::build_index_mvcc;
use crate::storage::txn_page_store::{PageOrigin, PageReservation, TxnOverlay};

use super::catalog_ops::{
    catalog_lock, new_store, new_txn_store, rebuild_and_publish_locked, sync_catalog_root_overlay,
};
use super::doc_helpers::now_millis;
use super::index_maint::{CreateIndexReservation, ReserveOutcome};
use super::publish::PublishDirty;
use super::snapshot_ops::{open_snapshot_read_view, primary_history_probe};
use super::PagedEngine;

impl PagedEngine {
    /// Reserve step of `create_index`: reserve an index slot.
    ///
    /// Allocates a root page for the index's B+ tree, writes an
    /// `IndexEntry { state: Building }` into the catalog, initializes
    /// the leaf page, and publishes a fresh snapshot so writers on the
    /// target namespace dual-write to it while the build is in flight.
    ///
    /// Returns [`ReserveOutcome::AlreadyExists`] if an index of that
    /// name already exists (idempotent call), otherwise
    /// [`ReserveOutcome::Reserved`].
    pub(super) fn create_index_reserve(
        &self,
        ns: &str,
        model: &IndexModel,
        name: &str,
    ) -> Result<ReserveOutcome> {
        self.shared.check_engine_not_poisoned()?;
        let _md_w = self
            .metadata
            .write()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        let mut reservation = None;
        let mut already_exists = false;

        let ns_id_to_drain = {
            let cat = catalog_lock(&self.metadata_state);
            cat.get_collection(ns)?.map(|collection| collection.id)
        };
        let ddl_guard = if let Some(ns_id) = ns_id_to_drain {
            Some(
                self.shared
                    .ns_writers
                    .close_and_drain_guard(ns_id, self.busy_timeout)?,
            )
        } else {
            None
        };

        let reserved_gen = self
            .shared
            .next_catalog_gen
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        let slot = self
            .shared
            .publish_sequencer
            .register_with_oracle(&self.shared.oracle)?;

        let reserve_result = (|| -> Result<()> {
            let _journal = self.lock_journal_mutex();
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

            let body = (|| {
                let mut cat = catalog_lock(&self.metadata_state);
                if cat.get_collection(ns)?.is_none() {
                    // Phase 1 §10.7 — allocate durable namespace id.
                    let ns_id = cat.allocate_namespace_id();
                    let data_root =
                        cat.create_collection(ns, ns_id, bson::doc! {}, now_millis())?;
                    drop(cat);
                    sync_catalog_root_overlay(&self.shared, &self.metadata_state, &mut overlay)?;
                    let data_store = new_txn_store(&self.shared, &mut overlay);
                    BTree::create_at(data_store, data_root)?;
                    cat = catalog_lock(&self.metadata_state);
                    cat.get_collection(ns)?.ok_or_else(|| {
                        Error::Internal(format!(
                            "collection '{}' missing after index bootstrap",
                            ns
                        ))
                    })?;
                }

                // Idempotent: if an index with this name already exists we
                // treat the call as a no-op.
                // We do NOT re-check state here — a caller seeing an
                // in-progress Building entry from a prior failed build is
                // reported as "exists"; the prior build's cleanup will have
                // removed it if it failed. If cleanup itself failed and an
                // orphan Building entry remains, callers can drop_index and
                // retry.
                if cat.get_index(ns, name)?.is_some() {
                    already_exists = true;
                    return Ok(());
                }

                // Phase 1 §10.7 — allocate durable index id.
                let ns_id = cat
                    .get_collection(ns)?
                    .ok_or_else(|| {
                        Error::Internal(format!(
                            "collection '{}' missing before index reservation",
                            ns
                        ))
                    })?
                    .id;
                let idx_id = cat.allocate_index_id();
                let idx_root = cat.create_index(ns, idx_id, model, name)?;
                // `catalog.create_index` defaults `state` to `Ready`. Flip
                // to `Building` so the published snapshot marks this index
                // as not-yet-queryable.
                let mut entry = cat
                    .get_index(ns, name)?
                    .ok_or_else(|| Error::Internal("index entry missing after create".into()))?;
                entry.state = IndexState::Building;
                cat.update_index(&entry)?;
                reservation = Some(CreateIndexReservation {
                    ns_id,
                    index_id: entry.id,
                    root_page: entry.root_page,
                    root_level: entry.root_level,
                });
                drop(cat);
                sync_catalog_root_overlay(&self.shared, &self.metadata_state, &mut overlay)?;

                // Initialize the freshly-allocated leaf page so the index
                // tree is valid to open for writes during the build step.
                let idx_store = new_txn_store(&self.shared, &mut overlay);
                BTree::create_at(idx_store, idx_root)?;
                Ok(())
            })();

            match body {
                Ok(()) => {
                    let mut base_store = new_store(&self.shared);
                    overlay.commit(&mut base_store, &self.shared.handle)?;
                    self.flush_under_journal_mutex()?;
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
        })();

        if let Err(e) = reserve_result {
            self.shared.publish_sequencer.mark_aborted(slot);
            return Err(e);
        }

        let dirty = PublishDirty {
            published_catalog_dirty: true,
            catalog_header_dirty: true,
        };
        let shared = std::sync::Arc::clone(&self.shared);
        let metadata_state = std::sync::Arc::clone(&self.metadata_state);
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
        if let Some(guard) = ddl_guard {
            guard.commit();
        }
        if already_exists {
            Ok(ReserveOutcome::AlreadyExists)
        } else {
            reservation
                .map(ReserveOutcome::Reserved)
                .ok_or_else(|| Error::Internal("missing create-index reservation".into()))
        }
    }

    /// Build step of `create_index`: populate the index from the data tree.
    ///
    /// Runs outside `metadata.write()` and outside a closed writer-registry
    /// gate. Same-namespace CRUD admitted after the Building publish can
    /// dual-write to the Building index while this scan is in flight.
    pub(super) fn create_index_build(&self, ns: &str, name: &str) -> Result<()> {
        self.create_index_build_inner(ns, name, false)
    }

    fn create_index_build_inner(
        &self,
        ns: &str,
        name: &str,
        rebuild_derived_pages: bool,
    ) -> Result<()> {
        // Take the latest IndexEntry AND CollectionEntry under a brief
        // metadata.read(). The root pages observed here identify the
        // Building tree. Same-namespace CRUD may advance the tree after
        // this point through the ordinary dual-write path.
        let (idx_entry, data_entry) = {
            let _md_read = self
                .metadata
                .read()
                .map_err(|_| Error::Internal("metadata lock poisoned".into()))?;
            let cat = catalog_lock(&self.metadata_state);
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
        let stale_target = || Error::WriteConflict {
            reason: WriteConflictReason::CatalogGenerationChanged,
        };

        #[cfg(any(test, feature = "test-hooks"))]
        super::test_accessors::create_index_build_if_installed(&self.shared, ns, name)?;

        {
            let _md_read = self
                .metadata
                .read()
                .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
            let cat = catalog_lock(&self.metadata_state);
            let collection = cat.get_collection(ns)?.ok_or_else(stale_target)?;
            if collection.id != data_entry.id {
                return Err(stale_target());
            }
            let index = cat.get_index(ns, name)?.ok_or_else(stale_target)?;
            if index.id != idx_entry.id || index.state != IndexState::Building {
                return Err(stale_target());
            }
        }

        // Stage the build in an overlay first. Journal rollback marks are not
        // captured during the body because no journal frames are appended here,
        // and other namespace lanes may commit while this long build runs.
        let mut overlay = TxnOverlay::new();
        let ready_reservations = self
            .shared
            .handle
            .allocator()
            .drain_deferred_free_reservations();
        for page in ready_reservations {
            overlay.push_reservation(PageReservation {
                page,
                size: PageSize::Large32k,
                origin: PageOrigin::DeferredFree,
            });
        }

        let body: Result<()> = (|| {
            // The data tree is read-only during index build — we use a
            // plain BufferPoolPageStore (no overlay) so the idx_store
            // can hold the sole &mut TxnOverlay borrow simultaneously.
            let data_store = new_store(&self.shared);
            let data_tree = BTree::open(
                data_store,
                data_entry.data_root_page,
                data_entry.data_root_level,
            );
            let epoch = self.shared.load_published_coherent();
            let read_view = open_snapshot_read_view(&self.shared, epoch);
            let primary_history = primary_history_probe(&self.shared, data_entry.id);
            let original_root_page = idx_entry.root_page;
            let original_root_level = idx_entry.root_level;
            let (root_page, root_level, any_multikey) = {
                let idx_store = new_txn_store(&self.shared, &mut overlay);
                let mut idx_tree = if rebuild_derived_pages {
                    BTree::create(idx_store)?
                } else {
                    BTree::open(idx_store, idx_entry.root_page, idx_entry.root_level)
                };
                let any_multikey = build_index_mvcc(
                    &data_tree,
                    &mut idx_tree,
                    &idx_entry,
                    &read_view,
                    Some(&primary_history),
                )?;
                (idx_tree.root_page, idx_tree.root_level, any_multikey)
            };
            if rebuild_derived_pages {
                let old_store = new_txn_store(&self.shared, &mut overlay);
                BTree::open(old_store, original_root_page, original_root_level).free_all_pages()?;
            }

            // Persist the possibly-updated root + multikey flag. Note:
            // we do NOT flip state to Ready here — that happens in the
            // commit step under metadata.write() so readers see a
            // consistent transition on the published snapshot.
            let root_changed =
                root_page != idx_entry.root_page || root_level != idx_entry.root_level;
            let multikey_changed = any_multikey && !idx_entry.multikey;
            if rebuild_derived_pages || root_changed || multikey_changed {
                let mut updated = idx_entry.clone();
                updated.root_page = root_page;
                updated.root_level = root_level;
                if multikey_changed {
                    updated.multikey = true;
                }
                // Brief metadata.read() for the catalog write. The read guard
                // is acquired only for the duration of the catalog mutation +
                // header sync so long-build scans don't block unrelated DDL.
                let _md_read = self
                    .metadata
                    .read()
                    .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
                let mut cat = catalog_lock(&self.metadata_state);
                let collection = cat.get_collection(ns)?.ok_or_else(stale_target)?;
                if collection.id != data_entry.id {
                    return Err(stale_target());
                }
                let index = cat.get_index(ns, name)?.ok_or_else(stale_target)?;
                if index.id != idx_entry.id || index.state != IndexState::Building {
                    return Err(stale_target());
                }
                if !cat.update_index(&updated)? {
                    return Err(stale_target());
                }
                drop(cat);
                sync_catalog_root_overlay(&self.shared, &self.metadata_state, &mut overlay)?;
            }
            Ok(())
        })();

        match body {
            Ok(()) => {
                // Commit the build txn. No timestamp is allocated (the
                // build step has no primary writes), and no publish is
                // performed. The data flush still runs under
                // `journal_mutex` so all page-store flush paths share the
                // same Phase 5 rollback/persist exclusion.
                let mut base = new_store(&self.shared);
                overlay.commit(&mut base, &self.shared.handle)?;
                {
                    let _journal = self.lock_journal_mutex();
                    self.flush_under_journal_mutex()?;
                }
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
                // The build body has not appended any journal frames yet. Do
                // not truncate to `mark`: other namespace lanes may have
                // committed since this build transaction began.
                let _ = self.rollback_overlay_only(overlay);
                Err(e)
            }
        }
    }

    /// Phase 3 of `create_index`: flip `state: Ready` under metadata.write
    /// and publish the final snapshot.
    pub(super) fn create_index_commit(
        &self,
        ns: &str,
        name: &str,
        target: &CreateIndexReservation,
    ) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        let stale_target = || Error::WriteConflict {
            reason: WriteConflictReason::CatalogGenerationChanged,
        };

        let _md_w = self
            .metadata
            .write()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        {
            let cat = catalog_lock(&self.metadata_state);
            let collection = cat
                .get_collection(ns)?
                .ok_or_else(|| Error::CollectionNotFound {
                    name: ns.to_owned(),
                })?;
            if collection.id != target.ns_id {
                return Err(stale_target());
            }
            let index = cat.get_index(ns, name)?.ok_or_else(stale_target)?;
            if index.id != target.index_id || index.state != IndexState::Building {
                return Err(stale_target());
            }
        }

        let guard = self
            .shared
            .ns_writers
            .close_and_drain_guard(target.ns_id, self.busy_timeout)?;

        {
            let cat = catalog_lock(&self.metadata_state);
            let collection = cat
                .get_collection(ns)?
                .ok_or_else(|| Error::CollectionNotFound {
                    name: ns.to_owned(),
                })?;
            if collection.id != target.ns_id {
                return Err(stale_target());
            }
            let index = cat.get_index(ns, name)?.ok_or_else(stale_target)?;
            if index.id != target.index_id || index.state != IndexState::Building {
                return Err(stale_target());
            }
        }

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
        let commit_result = (|| -> Result<()> {
            let _journal = self.lock_journal_mutex();
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

            let body = (|| -> Result<()> {
                {
                    let mut cat = catalog_lock(&self.metadata_state);
                    let collection =
                        cat.get_collection(ns)?
                            .ok_or_else(|| Error::CollectionNotFound {
                                name: ns.to_owned(),
                            })?;
                    if collection.id != target.ns_id {
                        return Err(stale_target());
                    }
                    let mut entry = cat.get_index(ns, name)?.ok_or_else(stale_target)?;
                    if entry.id != target.index_id || entry.state != IndexState::Building {
                        return Err(stale_target());
                    }
                    entry.state = IndexState::Ready;
                    if !cat.update_index(&entry)? {
                        return Err(stale_target());
                    }
                }
                sync_catalog_root_overlay(&self.shared, &self.metadata_state, &mut overlay)?;
                Ok(())
            })();

            match body {
                Ok(()) => {
                    let mut base_store = new_store(&self.shared);
                    overlay.commit(&mut base_store, &self.shared.handle)?;
                    self.flush_under_journal_mutex()?;
                    let db_page_count = self
                        .shared
                        .handle
                        .allocator()
                        .with_header(|h| h.total_page_count)?;
                    let header_data = {
                        let page = self.shared.handle.fetch_page(0, PageSize::Small4k)?;
                        page.data().to_vec()
                    };
                    let emergency = match self.shared.handle.commit_txn(
                        0,
                        PageSize::Small4k,
                        &header_data,
                        db_page_count,
                    ) {
                        Ok(emergency) => {
                            durable = true;
                            emergency
                        }
                        Err(e) => return Err(e),
                    };
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
        })();

        match commit_result {
            Ok(()) => {}
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
        let shared = std::sync::Arc::clone(&self.shared);
        let metadata_state = std::sync::Arc::clone(&self.metadata_state);
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
        guard.commit();
        Ok(())
    }

    /// Resume every catalog-visible Building index after reopen.
    ///
    /// Building index pages are derived state. Recovery ignores any prior
    /// partial build pages by bulk-loading into a fresh tree, releases the
    /// previous derived tree, and then promotes the catalog entry to Ready via
    /// the ordinary DDL commit envelope.
    pub(super) fn resume_building_indexes_after_open(&self) -> Result<()> {
        let builds = {
            let _md_read = self
                .metadata
                .read()
                .map_err(|_| Error::Internal("metadata lock poisoned".into()))?;
            let cat = catalog_lock(&self.metadata_state);
            let mut builds = Vec::new();
            for coll in cat.list_collections()? {
                for idx in cat.list_indexes(&coll.name)? {
                    if idx.state == IndexState::Building {
                        builds.push((
                            coll.name.clone(),
                            idx.name.clone(),
                            CreateIndexReservation {
                                ns_id: coll.id,
                                index_id: idx.id,
                                root_page: idx.root_page,
                                root_level: idx.root_level,
                            },
                        ));
                    }
                }
            }
            builds
        };

        for (ns, name, target) in builds {
            self.create_index_build_inner(&ns, &name, true)?;
            self.create_index_commit(&ns, &name, &target)?;
        }
        Ok(())
    }

    pub(super) fn free_index_pages_exclusive(
        &self,
        overlay: &mut TxnOverlay,
        index: &IndexEntry,
    ) -> Result<()> {
        let mut tree = BTree::open(
            new_txn_store(&self.shared, overlay),
            index.root_page,
            index.root_level,
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
        let mut store = new_txn_store(&self.shared, overlay);
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

    /// Cleanup after a failed build step: drop the orphan Building entry.
    ///
    /// `create_index_reserve` publishes Building before the long scan starts,
    /// so cleanup is a separate DDL event. It must revalidate the exact durable
    /// namespace/index identity, drain writers that may be dual-writing to the
    /// Building index, and publish the delete with a fresh catalog generation.
    pub(super) fn create_index_cleanup(
        &self,
        ns: &str,
        name: &str,
        target: &CreateIndexReservation,
    ) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        let stale_target = || Error::WriteConflict {
            reason: WriteConflictReason::CatalogGenerationChanged,
        };

        let _md_w = self
            .metadata
            .write()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        {
            let cat = catalog_lock(&self.metadata_state);
            let Some(collection) = cat.get_collection(ns)? else {
                return Err(Error::CollectionNotFound {
                    name: ns.to_owned(),
                });
            };
            if collection.id != target.ns_id {
                return Err(stale_target());
            }
            let Some(index) = cat.get_index(ns, name)? else {
                return Ok(());
            };
            if index.id != target.index_id
                || index.root_page != target.root_page
                || index.root_level != target.root_level
            {
                return Err(stale_target());
            }
            if index.state != IndexState::Building {
                return Ok(());
            }
        }

        let guard = self
            .shared
            .ns_writers
            .close_and_drain_guard(target.ns_id, self.busy_timeout)?;

        let target_index = {
            let cat = catalog_lock(&self.metadata_state);
            let Some(collection) = cat.get_collection(ns)? else {
                return Err(Error::CollectionNotFound {
                    name: ns.to_owned(),
                });
            };
            if collection.id != target.ns_id {
                return Err(stale_target());
            }
            let Some(index) = cat.get_index(ns, name)? else {
                return Ok(());
            };
            if index.id != target.index_id
                || index.root_page != target.root_page
                || index.root_level != target.root_level
            {
                return Err(stale_target());
            }
            if index.state != IndexState::Building {
                return Ok(());
            }
            index
        };

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
        let cleanup_result = (|| -> Result<()> {
            let _journal = self.lock_journal_mutex();
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

            let body = (|| -> Result<()> {
                self.free_index_pages_exclusive(&mut overlay, &target_index)?;
                {
                    let mut cat = catalog_lock(&self.metadata_state);
                    let removed = cat.drop_index(ns, name)?;
                    if !removed {
                        return Ok(());
                    }
                }
                sync_catalog_root_overlay(&self.shared, &self.metadata_state, &mut overlay)?;
                self.shared.clear_dirty_tree(&TreeIdent {
                    collection_id: target.ns_id,
                    kind: TreeKind::Secondary {
                        index_id: target.index_id,
                    },
                });
                Ok(())
            })();

            match body {
                Ok(()) => {
                    let mut base_store = new_store(&self.shared);
                    overlay.commit(&mut base_store, &self.shared.handle)?;
                    self.flush_under_journal_mutex()?;
                    let db_page_count = self
                        .shared
                        .handle
                        .allocator()
                        .with_header(|h| h.total_page_count)?;
                    let header_data = {
                        let page = self.shared.handle.fetch_page(0, PageSize::Small4k)?;
                        page.data().to_vec()
                    };
                    let emergency = match self.shared.handle.commit_txn(
                        0,
                        PageSize::Small4k,
                        &header_data,
                        db_page_count,
                    ) {
                        Ok(emergency) => {
                            durable = true;
                            emergency
                        }
                        Err(e) => return Err(e),
                    };
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
        })();

        match cleanup_result {
            Ok(()) => {}
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
        let shared = std::sync::Arc::clone(&self.shared);
        let metadata_state = std::sync::Arc::clone(&self.metadata_state);
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
        guard.commit();
        Ok(())
    }
}
