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

use crate::error::{Error, Result};
use crate::index::IndexModel;
use crate::storage::btree::BTree;
use crate::storage::buffer_pool::PageSize;
use crate::storage::catalog::IndexState;
use crate::storage::secondary_index::build_index;
use crate::storage::txn_page_store::{PageOrigin, PageReservation, TxnOverlay};

use super::catalog_ops::{catalog_lock, new_store, new_txn_store, sync_catalog_root_overlay};
use super::doc_helpers::now_millis;
use super::index_maint::ReserveOutcome;
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
        // Drive through `run_ddl` so we get the standard DDL envelope:
        // metadata.write() + commit mutex + overlay/WAL + publish.
        let outcome = std::cell::Cell::new(ReserveOutcome::Reserved);
        self.run_ddl(|shared, md, overlay| {
            let mut cat = catalog_lock(md);
            // Bootstrap collection if absent.
            if cat.get_collection(ns)?.is_none() {
                // Phase 1 §10.7 — allocate durable namespace id.
                let ns_id = cat.allocate_namespace_id();
                let data_root = cat.create_collection(ns, ns_id, bson::doc! {}, now_millis())?;
                drop(cat);
                sync_catalog_root_overlay(shared, md, overlay)?;
                let data_store = new_txn_store(shared, overlay);
                BTree::create_at(data_store, data_root)?;
                cat = catalog_lock(md);
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
                outcome.set(ReserveOutcome::AlreadyExists);
                return Ok(());
            }

            // Phase 1 §10.7 — allocate durable index id.
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
            drop(cat);
            sync_catalog_root_overlay(shared, md, overlay)?;

            // Initialize the freshly-allocated leaf page so the index
            // tree is valid to open for writes during the build step.
            let idx_store = new_txn_store(shared, overlay);
            BTree::create_at(idx_store, idx_root)?;
            Ok(())
        })?;
        Ok(outcome.get())
    }

    /// Build step of `create_index`: populate the index from the data tree.
    ///
    /// Runs under `metadata.read()` + the target namespace's lane, so
    /// writers on *other* namespaces proceed concurrently. Writers on
    /// this namespace wait on the lane (same as any other ns-local
    /// serialization). Any writer that commits DURING the build on this
    /// ns is blocked out; writers between the reserve and build steps have
    /// already dual-written to the Building index (via
    /// `maintain_secondary_on_*`).
    pub(super) fn create_index_build(&self, ns: &str, name: &str) -> Result<()> {
        self.create_index_build_inner(ns, name, false)
    }

    fn create_index_build_inner(
        &self,
        ns: &str,
        name: &str,
        rebuild_derived_pages: bool,
    ) -> Result<()> {
        // Acquire the namespace lane under a short-lived metadata.read()
        // guard — the read guard blocks drop_namespace (which needs
        // metadata.write()) from racing with lane acquisition. Once the
        // lane is in hand, drop the read guard so the long build scan
        // below does NOT block DDL on other namespaces or bootstrapping
        // of new namespaces.
        let lane = self.lane_for(ns);
        let _lane_guard = {
            let _md_read = self
                .metadata
                .read()
                .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
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
            let md_read = self
                .metadata
                .read()
                .map_err(|_| Error::Internal("metadata lock poisoned".into()))?;
            let cat = catalog_lock(&md_read);
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
            let original_root_page = idx_entry.root_page;
            let original_root_level = idx_entry.root_level;
            let (root_page, root_level, any_multikey) = {
                let idx_store = new_txn_store(&self.shared, &mut overlay);
                let mut idx_tree = if rebuild_derived_pages {
                    BTree::create(idx_store)?
                } else {
                    BTree::open(idx_store, idx_entry.root_page, idx_entry.root_level)
                };
                let any_multikey = build_index(&data_tree, &mut idx_tree, &idx_entry)?;
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
                // Brief metadata.read() for the catalog write. Lane still
                // protects same-ns serialization; read guard is acquired
                // only for the duration of the catalog mutation + header
                // sync so long-build scans don't block other-ns DDL.
                let md_read = self
                    .metadata
                    .read()
                    .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
                catalog_lock(&md_read).update_index(&updated)?;
                sync_catalog_root_overlay(&self.shared, &md_read, &mut overlay)?;
            }
            Ok(())
        })();

        match body {
            Ok(()) => {
                // Commit the build txn. We intentionally do NOT hold
                // the global commit sequencer here — no timestamp is
                // allocated (the build step has no primary writes), and
                // no publish is performed.
                // The only shared mutation is catalog root/multikey
                // metadata, which is safe under the lane.
                let mut base = new_store(&self.shared);
                overlay.commit(&mut base, &self.shared.handle)?;
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
    pub(super) fn create_index_commit(&self, ns: &str, name: &str) -> Result<()> {
        // US-018c: the Building -> Ready catalog rewrite flows through
        // `run_ddl`, so it acquires commit_seq. DDL does not call
        // allocate_commit_ts.
        self.run_ddl(|_shared, md, _overlay| {
            let mut cat = catalog_lock(md);
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

    /// Resume every catalog-visible Building index after reopen.
    ///
    /// Building index pages are derived state. Recovery ignores any prior
    /// partial build pages by bulk-loading into a fresh tree, releases the
    /// previous derived tree, and then promotes the catalog entry to Ready via
    /// the ordinary DDL commit envelope.
    pub(super) fn resume_building_indexes_after_open(&self) -> Result<()> {
        let builds = {
            let md_read = self
                .metadata
                .read()
                .map_err(|_| Error::Internal("metadata lock poisoned".into()))?;
            let cat = catalog_lock(&md_read);
            let mut builds = Vec::new();
            for coll in cat.list_collections()? {
                for idx in cat.list_indexes(&coll.name)? {
                    if idx.state == IndexState::Building {
                        builds.push((coll.name.clone(), idx.name.clone()));
                    }
                }
            }
            builds
        };

        for (ns, name) in builds {
            self.create_index_build_inner(&ns, &name, true)?;
            self.create_index_commit(&ns, &name)?;
        }
        Ok(())
    }

    /// Cleanup after a failed build step: drop the orphan Building entry.
    ///
    /// This keeps the catalog in a state where a retried
    /// `create_index(ns, same_name)` succeeds from the reserve step instead of
    /// hitting the idempotent-already-exists early return with a stale
    /// Building entry.
    pub(super) fn create_index_cleanup(&self, ns: &str, name: &str) -> Result<()> {
        self.run_ddl(|shared, md, overlay| {
            let removed = catalog_lock(md).drop_index(ns, name)?;
            if removed {
                sync_catalog_root_overlay(shared, md, overlay)?;
            }
            Ok(())
        })
    }
}
