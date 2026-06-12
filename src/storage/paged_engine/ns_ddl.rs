//! Namespace DDL paths extracted from `paged_engine.rs`.
//!
//! Owns the catalog-commit envelope shared by all structural DDL
//! (`commit_catalog_batch_to_log` / `catalog_commit_payload`), the
//! namespace create/bootstrap/drop lifecycle, and the drop-restore failpoint.
//! Kept out of the root engine file so the structural catalog-commit policy is
//! visible at a glance.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[cfg(any(test, feature = "test-hooks"))]
use super::hidden_accessors::Us026PostRegisterFailpoint;

use crate::error::{Error, Result, WriteConflictReason};
use crate::index::IndexModel;
use crate::journal::wire::{
    CatalogCommitKind, CatalogCommitPage, CatalogCommitPayload, JournalPageSize, LogRecordDraft,
    PageId,
};
use crate::storage::btree::BTree;
use crate::storage::buffer_pool::PageSize;
use crate::storage::structural_page_batch::StructuralPageBatch;

use super::doc_helpers::now_millis;
use super::publish::PublishDirty;
use super::publish::{rebuild_and_publish, sync_catalog_root_structural};
use super::publish_sequencer::PublishSlotGuard;
use super::state::{MetadataState, SharedState};
use super::PagedEngine;

/// Map a buffer-pool `PageSize` to its journal `JournalPageSize` plus the
/// stable `size_order` discriminant used as the secondary key when
/// deduplicating catalog-commit pages by `(page_number, size_order)`.
fn journal_page_size_and_order(page_size: PageSize) -> (JournalPageSize, u8) {
    match page_size {
        PageSize::Small4k => (JournalPageSize::Small4k, 0),
        PageSize::Large32k => (JournalPageSize::Large32k, 1),
    }
}

impl PagedEngine {
    pub(super) fn commit_catalog_batch_to_log(
        &self,
        kind: CatalogCommitKind,
        catalog_generation_before: u64,
        catalog_generation_after: u64,
        slot: &PublishSlotGuard,
        batch: StructuralPageBatch,
    ) -> Result<()> {
        let (payload, direct_dirty_pages) = match self.catalog_commit_payload(
            kind,
            catalog_generation_before,
            catalog_generation_after,
            &batch,
        ) {
            Ok(parts) => parts,
            Err(error) => {
                // Non-fatal failure before any journal reservation: consume
                // the batch so staged allocations return to the allocator and
                // header changes roll back. Error-preserving idiom — a failed
                // abort must never replace the original error.
                let _ = batch.abort(&self.shared.handle);
                return Err(error);
            }
        };
        let draft = LogRecordDraft::catalog(0, slot.publish_seq(), slot.commit_ts(), payload);
        // Test-only failpoint modelling a non-fatal `reserve_log_record`
        // failure; aborts the batch exactly like the production error path
        // on the reservation below.
        #[cfg(any(test, feature = "test-hooks"))]
        if let Err(error) = super::hidden_accessors::fail_catalog_commit_reserve_if_armed(&self.shared)
        {
            let _ = batch.abort(&self.shared.handle);
            return Err(error);
        }
        let reserved = match self.shared.handle.reserve_log_record(draft) {
            Ok(reserved) => reserved,
            Err(error) => {
                let _ = batch.abort(&self.shared.handle);
                return Err(error);
            }
        };
        let commit_end_lsn = reserved.end_lsn();

        let mut base_store = self.shared.new_btree_store();
        if let Err(error) =
            batch.commit_lsn_fenced(&mut base_store, &self.shared.handle, commit_end_lsn)
        {
            return Err(self.poison_after_reserved_log_failure(&reserved, error));
        }
        if let Err(error) = self
            .shared
            .handle
            .stamp_dirty_pages_lsn_all_pools(&direct_dirty_pages, commit_end_lsn)
        {
            return Err(self.poison_after_reserved_log_failure(&reserved, error));
        }
        let written_end_lsn = reserved
            .write_and_mark()
            .map_err(|error| self.poison_after_log_manager_failure(error))?;
        debug_assert_eq!(written_end_lsn, commit_end_lsn);
        self.wait_for_commit_durability(commit_end_lsn)?;
        #[cfg(any(test, feature = "test-hooks"))]
        if super::hidden_accessors::us026_fail_if_armed(
            &self.shared,
            Us026PostRegisterFailpoint::Flush,
        )
        .is_err()
        {
            return Err(
                self.engine_fatal(crate::error::EngineFatalReason::PostDurableDdlPublishFailure)
            );
        }
        Ok(())
    }

    fn catalog_commit_payload(
        &self,
        kind: CatalogCommitKind,
        catalog_generation_before: u64,
        catalog_generation_after: u64,
        batch: &StructuralPageBatch,
    ) -> Result<(Vec<u8>, Vec<u32>)> {
        let header = self.shared.handle.allocator().with_header(Clone::clone)?;
        let mut catalog_page_ids: BTreeSet<PageId> = {
            let mut cat = self.metadata_state.catalog_lock();
            cat.collect_pages_by_size()?
                .into_iter()
                .map(|(page, _size)| PageId(page))
                .collect()
        };
        if header.catalog_root_page != 0 {
            catalog_page_ids.insert(PageId(header.catalog_root_page));
        }
        if header.catalog_root_backup != 0 {
            catalog_page_ids.insert(PageId(header.catalog_root_backup));
        }
        if header.history_store_root_page != 0 {
            catalog_page_ids.insert(PageId(header.history_store_root_page));
        }

        let direct_dirty = self
            .shared
            .handle
            .dirty_frame_snapshots_for_pages(&catalog_page_ids)?;
        let direct_dirty_pages: Vec<u32> = direct_dirty
            .iter()
            .map(|(page, _size, _data)| *page)
            .collect();
        let mut pages_by_key: BTreeMap<(u32, u8), CatalogCommitPage> = BTreeMap::new();
        for (page_number, page_size, data) in direct_dirty {
            let (page_size, size_order) = journal_page_size_and_order(page_size);
            pages_by_key.insert(
                (page_number, size_order),
                CatalogCommitPage {
                    page_number,
                    page_size,
                    data,
                },
            );
        }
        for page in batch.page_images() {
            let (page_size, size_order) = journal_page_size_and_order(page.page_size);
            pages_by_key.insert(
                (page.page_number, size_order),
                CatalogCommitPage {
                    page_number: page.page_number,
                    page_size,
                    data: page.data,
                },
            );
        }
        let pages = pages_by_key.into_values().collect();
        let payload = CatalogCommitPayload {
            kind,
            catalog_generation_before,
            catalog_generation_after,
            header,
            pages,
        }
        .encode()?;
        Ok((payload, direct_dirty_pages))
    }

    /// R8 — the shared catalog-DDL commit/publish envelope.
    ///
    /// Captures the ~70-line skeleton every catalog-generation-advancing DDL
    /// shares once the caller has already taken `metadata.write()`, resolved
    /// and (where applicable) drained the target identity, and force-expired
    /// readers: reserve the next catalog generation, register the publish
    /// slot, stage the body in a `StructuralPageBatch`, drive the
    /// pre/post-durable error matrix through `commit_catalog_batch_to_log`,
    /// and publish via `mark_ready` / `rebuild_and_publish`.
    ///
    /// The two genuinely per-site error-path variants are parameterized, not
    /// erased:
    /// - `body` stages the catalog mutation into the batch and returns the
    ///   site's value `R`.
    /// - `undo` is the site's symmetric in-memory rollback (e.g.
    ///   `restore_dropped_namespace_catalog`, the create-side
    ///   `drop_collection` undo). It runs ONLY on a non-fatal failure after
    ///   the body has mutated the live metadata `Catalog` — both before the
    ///   reservation (body error / abort) and after a non-fatal commit
    ///   failure — and is skipped on `EngineFatal`, exactly matching the
    ///   pre-unification placement at every joined site. Sites with no
    ///   in-memory mutation pass a no-op closure.
    ///
    /// Per-site concerns that are NOT part of the skeleton stay with the
    /// caller: the `NsDdlBarrierGuard` lifecycle (`commit` / `mark_dropped`),
    /// drop-namespace's post-publish page retirement, and any site-specific
    /// identity revalidation. The `EngineFatalReason` on a post-durable
    /// publish failure is uniformly [`PostDurableDdlPublishFailure`]; the
    /// build path's unchanged-generation no-op publish does NOT route through
    /// here (see `create_index_build_inner`).
    pub(super) fn run_catalog_ddl_envelope<F, R, U>(
        &self,
        kind: CatalogCommitKind,
        body: F,
        undo: U,
    ) -> Result<R>
    where
        F: FnOnce(&mut StructuralPageBatch) -> Result<R>,
        U: FnOnce(),
    {
        let catalog_generation_before = self.shared.published.load_full().catalog_generation;
        let reserved_gen = self
            .shared
            .next_catalog_gen
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        let slot = self
            .shared
            .publish_sequencer
            .register_with_oracle(&self.shared.oracle)?;

        let mut batch = StructuralPageBatch::new(&self.shared.handle);

        let value = match body(&mut batch) {
            Ok(value) => value,
            Err(e) => {
                // Error-preserving abort: a failed abort must neither replace
                // the body error nor skip the undo / slot abort below.
                let _ = batch.abort(&self.shared.handle);
                if !matches!(e, Error::EngineFatal { .. }) {
                    undo();
                }
                self.shared.publish_sequencer.mark_aborted(slot);
                return Err(e);
            }
        };

        if let Err(error) = self.commit_catalog_batch_to_log(
            kind,
            catalog_generation_before,
            reserved_gen,
            &slot,
            batch,
        ) {
            if !matches!(error, Error::EngineFatal { .. }) {
                undo();
                self.shared.publish_sequencer.mark_aborted(slot);
            }
            return Err(error);
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
                rebuild_and_publish(
                    &shared,
                    &metadata_state,
                    publish_ts,
                    dirty,
                    Some(reserved_gen),
                )
            });
        match publish_result {
            Ok(()) => Ok(value),
            Err(Error::EngineFatal { reason }) => Err(Error::EngineFatal { reason }),
            Err(_) => Err(self
                .engine_fatal(crate::error::EngineFatalReason::PostDurableDdlPublishFailure)),
        }
        // NOTE: `maybe_sync_interval_after_publish` is intentionally NOT called
        // here. The caller must invoke it AFTER any post-publish bookkeeping
        // (e.g. drop_namespace's retire_dropped_tree_pages) so that a sync
        // failure cannot skip those steps. Each of the six joined sites calls it
        // immediately after the envelope returns Ok, in the correct position.
    }

    /// Bootstrap a collection if it does not exist yet and return its
    /// durable namespace id.
    ///
    /// Called from CRUD paths that may be invoked with an unknown ns.
    /// Acquires `metadata.write()` through the namespace-create DDL
    /// envelope so the namespace is both visible in the catalog AND
    /// reflected in the published snapshot before the caller admits a
    /// writer ticket on the returned id.
    pub(super) fn bootstrap_namespace(&self, ns: &str) -> Result<i64> {
        // §10.1.1 F5 retirement: the legacy name-keyed drop tombstone has been
        // retired. Durable monotonic ids (Phase 1 §10.7) are the resurrection
        // barrier; a freshly-bootstrapped collection of a previously-dropped
        // name receives a fresh `CollectionEntry.id`.
        let created = Cell::new(false);
        self.run_namespace_create_ddl(
            |shared, md, batch| {
                let data_root = {
                    let mut cat = md.catalog_lock();
                    if let Some(entry) = cat.get_collection(ns)? {
                        return Ok(entry.id);
                    }
                    // Phase 1 §10.7 — allocate durable namespace id from the
                    // header counter atomically with the catalog commit.
                    let id = cat.allocate_namespace_id();
                    let data_root = cat.create_collection(ns, id, bson::doc! {}, now_millis())?;
                    created.set(true);
                    (id, data_root)
                };
                sync_catalog_root_structural(shared, md, batch)?;
                let _ = BTree::create_at(shared.new_structural_store(batch), data_root.1)?;
                Ok(data_root.0)
            },
            |md| {
                if created.get() {
                    // Undo the in-memory create so the metadata catalog and
                    // the published epoch agree the namespace does not exist.
                    let _ = md.catalog_lock().drop_collection(ns);
                }
            },
        )
    }

    // -----------------------------------------------------------------------
    // create_namespace
    // -----------------------------------------------------------------------

    pub(crate) fn create_namespace(&self, ns: &str) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        let created = Cell::new(false);
        let result = self.run_namespace_create_ddl(
            |shared, md, batch| {
                let data_root = {
                    let mut cat = md.catalog_lock();
                    if cat.get_collection(ns)?.is_some() {
                        return Err(Error::DuplicateKey {
                            detail: format!("collection '{ns}' already exists"),
                        });
                    }
                    // Phase 1 §10.7 — allocate durable namespace id from the
                    // header counter atomically with the catalog commit.
                    let id = cat.allocate_namespace_id();
                    let data_root = cat.create_collection(ns, id, bson::doc! {}, now_millis())?;
                    created.set(true);
                    data_root
                };
                sync_catalog_root_structural(shared, md, batch)?;
                let store = shared.new_structural_store(batch);
                BTree::create_at(store, data_root)?;
                Ok(())
            },
            |md| {
                if created.get() {
                    // Undo the in-memory create so the metadata catalog and
                    // the published epoch agree the namespace does not exist
                    // and the next insert bootstraps cleanly.
                    let _ = md.catalog_lock().drop_collection(ns);
                }
            },
        );
        // §10.1.1 F5 retirement: no name-based drop tombstone to clear.
        result
    }

    // -----------------------------------------------------------------------
    // drop_namespace
    // -----------------------------------------------------------------------

    pub(crate) fn drop_namespace(&self, ns: &str) -> Result<()> {
        self.shared.check_engine_not_poisoned()?;
        let stale_target = || Error::WriteConflict {
            reason: WriteConflictReason::CatalogGenerationChanged,
        };

        let _md_w = self
            .metadata
            .write()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        let Some(target_collection) = ({
            let cat = self.metadata_state.catalog_lock();
            cat.get_collection(ns)?
        }) else {
            return Ok(());
        };
        let ns_id = target_collection.id;
        let data_root = target_collection.data_root_page;
        let data_level = target_collection.data_root_level;
        // R6: capture the FULL index entries (not just the roots) so a
        // non-fatal body/commit failure below can re-insert the exact
        // catalog records under the still-held `metadata.write()`.
        let index_entries: Vec<crate::storage::catalog::IndexEntry> = {
            let cat = self.metadata_state.catalog_lock();
            cat.list_indexes(ns)?
        };
        let index_roots: Vec<(u32, u8)> = index_entries
            .iter()
            .map(|entry| (entry.root_page, entry.root_level))
            .collect();

        let mut guard = self
            .shared
            .ns_writers
            .close_and_drain_guard(ns_id, self.busy_timeout)?;
        // Force-expire ALL active ReadViews globally before the drop
        // commits/publishes; fresh views opened off a stale epoch are
        // protected by the deferred page retirement below instead.
        self.shared.handle.read_view_registry().force_expire_all();

        // R6: Cell-guarded undo marker, symmetric with the create-side
        // `undo_catalog`. F37: set immediately BEFORE `drop_collection`
        // runs — the drop is multi-step (per-index record deletes precede
        // the collection record delete), so a mid-step failure has already
        // mutated the live metadata catalog. Any non-fatal failure after
        // the marker is set must re-insert the captured entries or the
        // catalog disagrees with the published epoch (index-granularity
        // tear) and a retried drop ghosts on `get_collection == None`.
        // The restore is idempotent for still-present records: its
        // `create_*` calls fail on existing keys and are skipped.
        let catalog_dropped = Cell::new(false);

        // R8: drop runs through the shared catalog-DDL envelope. The per-site
        // variant is the symmetric `restore_dropped_namespace_catalog` undo,
        // preserved at its exact placement: the envelope invokes it on every
        // non-fatal failure once `catalog_dropped` is set (body error after
        // the abort, or a non-fatal commit failure), re-inserting the
        // captured `CollectionEntry` + `IndexEntry` records under the
        // still-held `metadata.write()`.
        let retired_pages = self.run_catalog_ddl_envelope(
            CatalogCommitKind::NamespaceDrop,
            |batch| -> Result<Vec<(u32, PageSize)>> {
                {
                    let cat = self.metadata_state.catalog_lock();
                    let collection =
                        cat.get_collection(ns)?
                            .ok_or_else(|| Error::CollectionNotFound {
                                name: ns.to_owned(),
                            })?;
                    if collection.id != ns_id {
                        return Err(stale_target());
                    }
                }

                // BUG-5: collect the tree pages here but do NOT free them (and
                // do NOT clear their resident chains). The physical retirement
                // is deferred until after the drop has durably committed AND
                // published, so a reader that loaded the pre-drop epoch can
                // still resolve the namespace and scan an intact snapshot.
                let mut retired_pages = self.collect_tree_pages(data_root, data_level)?;
                for (root_page, root_level) in &index_roots {
                    retired_pages.extend(self.collect_tree_pages(*root_page, *root_level)?);
                }

                {
                    let mut cat = self.metadata_state.catalog_lock();
                    let collection =
                        cat.get_collection(ns)?
                            .ok_or_else(|| Error::CollectionNotFound {
                                name: ns.to_owned(),
                            })?;
                    if collection.id != ns_id {
                        return Err(stale_target());
                    }
                    // F37: marker first — `drop_collection` deletes the index
                    // records before the collection record, so a mid-loop
                    // failure must still trigger the restore.
                    catalog_dropped.set(true);
                    let dropped = cat.drop_collection(ns)?;
                    if !dropped {
                        return Err(Error::CollectionNotFound {
                            name: ns.to_owned(),
                        });
                    }
                }
                sync_catalog_root_structural(&self.shared, &self.metadata_state, batch)?;
                // Test-only pause point: the tree pages have been collected for
                // deferred retirement and the catalog mutation is staged, but
                // the new epoch is not yet published.
                #[cfg(any(test, feature = "test-hooks"))]
                super::hidden_accessors::drop_namespace_before_commit_if_installed(&self.shared);
                Ok(retired_pages)
            },
            || {
                // R6: symmetric undo — re-insert the captured catalog entries
                // under the still-held `metadata.write()` so the metadata
                // catalog keeps agreeing with the still-published pre-drop
                // epoch and a retried drop actually runs.
                if catalog_dropped.get() {
                    self.restore_dropped_namespace_catalog(&target_collection, &index_entries);
                }
            },
        )?;

        // R6: drop-side bookkeeping that cannot be restored on a failed commit
        // runs only once the drop is durable AND published.
        self.shared.clear_dirty_collection(ns_id);
        // BUG-5: only now — after the durable commit and the epoch publish —
        // may the dropped tree's pages move toward the free list, and ALL of
        // them only via the page-lifetime queue (checkpoint-fence + reader
        // low-water gated) so they cannot be reused while a stale epoch could
        // still hand them to a fresh ReadView.
        self.retire_dropped_tree_pages(&retired_pages);
        // FIX1: interval sync runs AFTER clear_dirty_collection +
        // retire_dropped_tree_pages so a sync failure cannot skip page
        // retirement. On sync-Err we propagate before guard.mark_dropped so
        // the RAII guard reopens admissions via Drop (correct baseline order).
        self.maybe_sync_interval_after_publish()?;
        // §10.1.1 F5 retirement: durable monotonic ns_ids are the
        // resurrection barrier; no name-based drop tombstone is inserted.
        guard.mark_dropped();
        Ok(())
    }

    /// R6: re-insert a dropped namespace's captured catalog entries after a
    /// non-fatal `drop_namespace` body/commit failure.
    ///
    /// Runs under the still-held `metadata.write()` guard, so no concurrent
    /// catalog mutation can interleave. `Catalog` exposes no raw-entry
    /// insert, so the restore goes create-then-overwrite:
    /// `create_collection` / `create_index` re-insert the keyed records
    /// (allocating scratch root pages), then `update_collection` /
    /// `update_index` overwrite them with the captured entries so the
    /// durable ids, data roots, and levels match the still-published
    /// pre-drop epoch exactly.
    ///
    /// F10: a scratch root is freed (after the catalog lock drops) ONLY
    /// when the matching overwrite returned `Ok(true)` — only then is the
    /// scratch page unreachable from the catalog. If the overwrite fails,
    /// the entry is left pointing at the scratch root: a valid, still-
    /// allocated empty leaf, i.e. a safe empty collection/index. Freeing it
    /// anyway would leave the surviving entry referencing a recyclable page
    /// — the next open/insert would descend a foreign or zeroed page,
    /// strictly worse than no restore.
    ///
    /// Best-effort by design: this runs on an error path that must preserve
    /// the caller's original error, so internal failures are swallowed.
    /// Known residual tears, accepted under best-effort: (a) an overwrite
    /// failure leaves the entry on its (safe, empty) scratch root while the
    /// published epoch still maps the captured root; (b) `update_collection`
    /// / `update_index` are delete-then-insert on the catalog tree
    /// (catalog.rs), so a failure BETWEEN the two deletes the record
    /// outright — the captured root then leaks rather than dangles.
    fn restore_dropped_namespace_catalog(
        &self,
        entry: &crate::storage::catalog::CollectionEntry,
        indexes: &[crate::storage::catalog::IndexEntry],
    ) {
        let mut scratch_roots: Vec<u32> = Vec::new();
        {
            let mut cat = self.metadata_state.catalog_lock();
            if let Ok(scratch) = cat.create_collection(
                &entry.name,
                entry.id,
                entry.options.clone(),
                entry.created_at,
            ) {
                #[cfg(any(test, feature = "test-hooks"))]
                let skip_update = restore_update_failpoint::consume_if_armed(&entry.name);
                #[cfg(not(any(test, feature = "test-hooks")))]
                let skip_update = false;
                // F10: free the scratch root only when the overwrite
                // landed; on failure the entry keeps the scratch root (a
                // valid allocated empty leaf) and nothing is freed.
                if !skip_update && matches!(cat.update_collection(entry), Ok(true)) {
                    scratch_roots.push(scratch);
                }
            }
            for index in indexes {
                let model = IndexModel::builder()
                    .keys(index.key_pattern.clone())
                    .options(
                        crate::options::IndexOptions::new()
                            .unique(index.unique)
                            .sparse(index.sparse),
                    )
                    .build();
                if let Ok(scratch) = cat.create_index(&entry.name, index.id, &model, &index.name)
                {
                    // F10: same rule as the collection branch — the scratch
                    // root is freed only when the overwrite landed.
                    if matches!(cat.update_index(index), Ok(true)) {
                        scratch_roots.push(scratch);
                    }
                }
            }
        }
        for page in scratch_roots {
            let _ = self.shared.handle.free_page(page, PageSize::Large32k);
        }
    }

    /// Drive the two namespace-create paths with the bootstrap DDL envelope.
    ///
    /// This helper is intentionally narrower than `run_ddl`: it handles
    /// standalone `create_namespace` and write-path bootstrap on allocated-id
    /// publication, while existing-namespace DDL drains remain separate.
    ///
    /// `undo_catalog` rolls back the body's in-memory `Catalog` mutation when
    /// the DDL fails non-fatally after the body ran (body error past the
    /// mutation, or commit failure). Without it the `Mutex<Catalog>` keeps the
    /// new entry while the published epoch never learns about it, so later
    /// inserts skip bootstrap and spin to `CollectionNotFound`. EngineFatal
    /// failures skip the undo — the poisoned engine rejects every subsequent
    /// operation, so the in-memory divergence is unobservable.
    fn run_namespace_create_ddl<F, R, U>(&self, f: F, undo_catalog: U) -> Result<R>
    where
        F: FnOnce(&SharedState, &MetadataState, &mut StructuralPageBatch) -> Result<R>,
        U: FnOnce(&MetadataState),
    {
        self.shared.check_engine_not_poisoned()?;
        let _md_w = self
            .metadata
            .write()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        // R8: the slot reservation, batch staging, pre/post-durable commit
        // matrix, and publish matrix are the shared catalog-DDL envelope. The
        // create-side `undo_catalog` (rolling back the in-memory `Catalog`
        // mutation so a non-fatal failure does not leave the metadata catalog
        // disagreeing with the published epoch) is the only per-site variant.
        let value = self.run_catalog_ddl_envelope(
            CatalogCommitKind::NamespaceCreate,
            |batch| f(&self.shared, &self.metadata_state, batch),
            || undo_catalog(&self.metadata_state),
        )?;
        // FIX1: interval sync after publish — no post-publish bookkeeping at
        // this site, so the call is positionally identical to the baseline.
        self.maybe_sync_interval_after_publish()?;
        Ok(value)
    }
}

// ---------------------------------------------------------------------------
// F10 failpoint — fail the captured-entry overwrite inside the drop restore
// ---------------------------------------------------------------------------

/// F10 failpoint: make `restore_dropped_namespace_catalog` treat the
/// collection `update_collection` overwrite as FAILED (the call is skipped,
/// modeling an update that errored before mutating the record), so tests
/// can pin that the surviving entry's scratch root is never freed.
///
/// Name-filtered and one-shot, like `catalog::drop_collection_failpoint`,
/// so parallel tests using unique namespaces cannot cross-fire.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) mod restore_update_failpoint {
    use std::sync::{Mutex, PoisonError};

    static ARMED: Mutex<Option<String>> = Mutex::new(None);

    /// Arm one skipped/failed collection overwrite for namespace `ns`.
    ///
    /// `cfg(test)` (not the wider test-hooks gate): the arming side is only
    /// reachable from in-crate unit tests; the consume side keeps the wider
    /// gate because the production restore references it.
    #[cfg(test)]
    pub(crate) fn arm(ns: &str) {
        *ARMED.lock().unwrap_or_else(PoisonError::into_inner) = Some(ns.to_owned());
    }

    /// Consume (and disarm) if armed for `ns`.
    pub(super) fn consume_if_armed(ns: &str) -> bool {
        let mut armed = ARMED.lock().unwrap_or_else(PoisonError::into_inner);
        if armed.as_deref() == Some(ns) {
            *armed = None;
            true
        } else {
            false
        }
    }
}
