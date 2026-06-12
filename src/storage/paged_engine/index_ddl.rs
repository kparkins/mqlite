//! Full create/drop index lifecycle (renamed from index_build.rs).
//!
//! This file holds the complete 4-phase create-index lifecycle and the
//! drop-index DDL so the create/drop index protocol reads in ONE place:
//! - [`create_index`] — the public driver that sequences reserve → build →
//!   commit (and cleanup on build failure).
//! - [`PagedEngine::create_index_reserve`] — allocate a root page, write an
//!   `IndexEntry { state: Building }`, publish.
//! - [`PagedEngine::create_index_build`] — populate the index from the data
//!   tree while same-namespace writers dual-write to the Building index.
//! - [`PagedEngine::create_index_commit`] — flip `state: Ready` under
//!   `metadata.write()` and publish.
//! - [`PagedEngine::create_index_cleanup`] — drop an orphan Building entry
//!   on build failure.
//! - [`drop_index`] — guarded DDL delete of an existing index.
//! - [`list_indexes`] — read the published index list for a namespace.

use crate::error::{Error, Result, WriteConflictReason};
use crate::index::{IndexInfo, IndexModel};
use crate::journal::wire::CatalogCommitKind;
use crate::storage::btree::{BTree, BTreePageStore};
use crate::storage::buffer_pool::PageSize;
use crate::storage::catalog::{IndexEntry, IndexState};
use crate::storage::reconcile::driver::{TreeIdent, TreeKind};
use crate::storage::secondary_index::{build_index_mvcc, generate_index_name};
use crate::storage::structural_page_batch::StructuralPageBatch;

use super::doc_helpers::{now_millis, validate_index_keys};
use super::publish::sync_catalog_root_structural;
use super::snapshot_ops::{open_snapshot_read_view, primary_history_probe};
use super::PagedEngine;

/// Durable identity captured when `create_index_reserve` publishes Building.
#[derive(Clone, Debug)]
pub(super) struct CreateIndexReservation {
    pub(super) ns_id: i64,
    pub(super) index_id: i64,
    pub(super) root_page: u32,
    pub(super) root_level: u8,
}

/// Outcome of `create_index_reserve` (reserve step of the 3-step build).
#[derive(Clone, Debug)]
pub(super) enum ReserveOutcome {
    /// A fresh Building entry was reserved; caller should proceed to
    /// the build and commit steps.
    Reserved(CreateIndexReservation),
    /// An index with the same name already exists; `create_index` is
    /// idempotent and returns Ok immediately.
    AlreadyExists,
}

pub(super) fn create_index(
    engine: &super::PagedEngine,
    ns: &str,
    model: &IndexModel,
) -> crate::error::Result<String> {
    validate_index_keys(&model.keys)?;
    validate_partial_filter_expression(model)?;
    validate_ttl(model)?;
    let name = model
        .options
        .name
        .clone()
        .unwrap_or_else(|| generate_index_name(&model.keys));

    let reserve_outcome = engine.create_index_reserve(ns, model, &name)?;
    let reservation = match reserve_outcome {
        ReserveOutcome::AlreadyExists => return Ok(name),
        ReserveOutcome::Reserved(reservation) => reservation,
    };

    if let Err(build_err) = engine.create_index_build(ns, &name) {
        if matches!(
            build_err,
            crate::error::Error::WriteConflict {
                reason: WriteConflictReason::CatalogGenerationChanged
            }
        ) {
            return Err(build_err);
        }
        if let Err(cleanup_err) = engine.create_index_cleanup(ns, &name, &reservation) {
            return Err(crate::error::Error::Internal(format!(
                "create_index build failed: {}; cleanup also failed: {}",
                build_err, cleanup_err
            )));
        }
        return Err(build_err);
    }

    engine.create_index_commit(ns, &name, &reservation)?;
    Ok(name)
}

/// Name of the immutable primary `_id` field — partial indexes are forbidden
/// on it (MongoDB restriction).
const ID_FIELD: &str = "_id";

/// Validate a partial index's `partialFilterExpression` at create time.
///
/// Enforces the MongoDB restrictions mqlite supports:
/// - the filter must be a non-empty document;
/// - the filter must be one the crate's evaluator accepts (mqlite admits ANY
///   evaluator-supported filter — a SUPERSET of MongoDB's restricted operator
///   set);
/// - it must not be combined with `sparse: true`;
/// - the index must not be on the `_id` field.
///
/// Returns `Ok(())` for an ordinary (non-partial) index.
///
/// # Errors
///
/// Returns [`Error::InvalidQuery`] (MongoDB `BadValue`, code 2) for any
/// violation. Divergence: MongoDB returns `CannotCreateIndex` (code 67) for the
/// sparse/`_id` conflicts; mqlite reports them all as `BadValue`.
fn validate_partial_filter_expression(model: &IndexModel) -> Result<()> {
    let Some(pfe) = &model.partial_filter_expression else {
        return Ok(());
    };

    if pfe.is_empty() {
        return Err(Error::InvalidQuery {
            detail: "partialFilterExpression must be a non-empty document".to_owned(),
        });
    }

    if model.options.sparse {
        return Err(Error::InvalidQuery {
            detail: "cannot specify both partialFilterExpression and sparse".to_owned(),
        });
    }

    if model.keys.keys().any(|field| field == ID_FIELD) {
        return Err(Error::InvalidQuery {
            detail: "_id index cannot be a partial index".to_owned(),
        });
    }

    // Validate that the evaluator accepts the filter. Evaluating against an
    // empty document surfaces structural/operator errors (e.g. an unknown `$`
    // operator) without needing a real document.
    crate::query::eval_filter(&bson::Document::new(), pfe).map_err(|e| Error::InvalidQuery {
        detail: format!("invalid partialFilterExpression: {e}"),
    })?;

    Ok(())
}

/// Validate a TTL index's `expireAfterSeconds` at create time.
///
/// Enforces the MongoDB restrictions mqlite supports:
/// - the value must be non-negative (`0` means expire at the field's date);
/// - TTL is only valid on single-field indexes (compound indexes are rejected);
/// - the index must not be on the `_id` field.
///
/// TTL combines freely with `unique`, `sparse`, and `partialFilterExpression`
/// (MongoDB permits all three with a TTL index). Integral/type validation of
/// the wire value happens at the wire layer; here `expire_after_seconds` is
/// already an `i64`.
///
/// Returns `Ok(())` for an ordinary (non-TTL) index.
///
/// # Errors
///
/// Returns [`Error::InvalidQuery`] (MongoDB `BadValue`, code 2) for any
/// violation.
fn validate_ttl(model: &IndexModel) -> Result<()> {
    let Some(seconds) = model.expire_after_seconds else {
        return Ok(());
    };

    if seconds < 0 {
        return Err(Error::InvalidQuery {
            detail: "TTL index expireAfterSeconds must be a non-negative number".to_owned(),
        });
    }

    if model.keys.len() != 1 {
        return Err(Error::InvalidQuery {
            detail: "TTL indexes are single-field indexes, compound indexes do not \
                     support the expireAfterSeconds option"
                .to_owned(),
        });
    }

    if model.keys.keys().any(|field| field == ID_FIELD) {
        return Err(Error::InvalidQuery {
            detail: "_id index cannot be a TTL index".to_owned(),
        });
    }

    Ok(())
}

pub(super) fn drop_index(
    engine: &super::PagedEngine,
    ns: &str,
    name: &str,
) -> crate::error::Result<()> {
    if name == "_id_" {
        return Err(crate::error::Error::InvalidWireMessage {
            detail: "drop of '_id_' index is not permitted".to_string(),
        });
    }
    engine.shared.check_engine_not_poisoned()?;
    let stale_target = || Error::WriteConflict {
        reason: WriteConflictReason::CatalogGenerationChanged,
    };

    let _md_w = engine
        .metadata
        .write()
        .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
    let (ns_id, target_index) = {
        let cat = engine.metadata_state.catalog_lock();
        let collection = cat
            .get_collection(ns)?
            .ok_or_else(|| Error::CollectionNotFound {
                name: ns.to_owned(),
            })?;
        let index = cat
            .get_index(ns, name)?
            .ok_or_else(|| Error::Internal(format!("index '{}' not found on '{}'", name, ns)))?;
        (collection.id, index)
    };

    let guard = engine
        .shared
        .ns_writers
        .close_and_drain_guard(ns_id, engine.busy_timeout)?;

    // R8: drop_index has no in-memory `Catalog` mutation to undo on a
    // non-fatal failure — `cat.drop_index` runs inside the batch body and the
    // envelope's batch abort + slot abort fully roll it back — so the undo
    // hook is a no-op. The RAII `guard` lifecycle stays a site concern.
    engine.run_catalog_ddl_envelope(
        CatalogCommitKind::IndexDrop,
        |batch| -> Result<()> {
            engine.free_index_pages_exclusive(batch, &target_index)?;
            {
                let mut cat = engine.metadata_state.catalog_lock();
                let collection =
                    cat.get_collection(ns)?
                        .ok_or_else(|| Error::CollectionNotFound {
                            name: ns.to_owned(),
                        })?;
                if collection.id != ns_id {
                    return Err(stale_target());
                }
                let index = cat.get_index(ns, name)?.ok_or_else(|| {
                    Error::Internal(format!("index '{}' not found on '{}'", name, ns))
                })?;
                if index.id != target_index.id
                    || index.root_page != target_index.root_page
                    || index.root_level != target_index.root_level
                {
                    return Err(stale_target());
                }
                let removed = cat.drop_index(ns, name)?;
                if !removed {
                    return Err(Error::Internal(format!(
                        "index '{}' not found on '{}'",
                        name, ns
                    )));
                }
            }
            sync_catalog_root_structural(&engine.shared, &engine.metadata_state, batch)?;
            engine.shared.clear_dirty_tree(&TreeIdent {
                collection_id: ns_id,
                kind: TreeKind::Secondary {
                    index_id: target_index.id,
                },
            });
            Ok(())
        },
        || {},
    )?;
    // FIX1: interval sync after publish — no post-publish bookkeeping at this
    // site, so the call is positionally identical to the baseline (immediately
    // after the envelope, before guard.commit). On sync-Err we propagate before
    // guard.commit so the RAII guard reopens admissions via Drop.
    engine.maybe_sync_interval_after_publish()?;

    guard.commit();
    Ok(())
}

pub(super) fn list_indexes(
    engine: &super::PagedEngine,
    ns: &str,
) -> crate::error::Result<Vec<IndexInfo>> {
    let snap = engine.shared.load_published();
    let ns_snap = match snap.catalog.get_by_name(ns) {
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
            partial_filter_expression: i.partial_filter_expression.clone(),
            expire_after_seconds: i.expire_after_seconds,
        })
        .collect())
}

impl PagedEngine {
    #[cfg(test)]
    pub(super) fn test_fail_after_build_catalog_update_once(&self) {
        self.shared
            .test_hooks
            .fail_after_build_catalog_update
            .store(1, std::sync::atomic::Ordering::Release);
    }

    #[cfg(test)]
    fn test_fail_after_build_catalog_update_if_armed(&self) -> Result<()> {
        if self
            .shared
            .test_hooks
            .fail_after_build_catalog_update
            .swap(0, std::sync::atomic::Ordering::AcqRel)
            == 1
        {
            return Err(Error::Internal(
                "injected failure after create-index build catalog update".into(),
            ));
        }
        Ok(())
    }

    fn rollback_build_catalog_update(
        &self,
        ns: &str,
        name: &str,
        original: &IndexEntry,
        updated: &IndexEntry,
    ) {
        let Ok(_md_read) = self
            .metadata
            .read()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))
        else {
            return;
        };
        let mut cat = self.metadata_state.catalog_lock();
        let Ok(Some(current)) = cat.get_index(ns, name) else {
            return;
        };
        if current == *updated {
            let _ = cat.update_index(original);
        }
    }

    /// Reserve step of `create_index`: reserve an index slot.
    ///
    /// Allocates a root page for the index's B+ tree, writes an
    /// `IndexEntry { state: Building }` into the catalog, initializes
    /// the leaf page, and publishes a fresh snapshot so writers on the
    /// target namespace dual-write to it while the build is in flight.
    ///
    /// Returns [`ReserveOutcome::AlreadyExists`] if a Ready index of that
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

        {
            let cat = self.metadata_state.catalog_lock();
            if cat.get_collection(ns)?.is_some() {
                if let Some(existing) = cat.get_index(ns, name)? {
                    if existing.state == IndexState::Ready {
                        return Ok(ReserveOutcome::AlreadyExists);
                    }
                    return Err(Error::WriteConflict {
                        reason: WriteConflictReason::CatalogGenerationChanged,
                    });
                }
            }
        }

        let mut reservation = None;

        let ns_id_to_drain = {
            let cat = self.metadata_state.catalog_lock();
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

        // R8: reserve has no in-memory undo (the `create_collection` /
        // `create_index` mutations live inside the batch body and the
        // envelope's abort path rolls them back), so the undo hook is a
        // no-op. The per-site variant is the `Option<ddl_guard>` whose
        // `commit` runs only after a successful publish; it stays a site
        // concern, committed below once the envelope returns Ok.
        self.run_catalog_ddl_envelope(
            CatalogCommitKind::IndexReserve,
            |batch| {
                let mut cat = self.metadata_state.catalog_lock();
                if cat.get_collection(ns)?.is_none() {
                    // Allocate a durable namespace id.
                    let ns_id = cat.allocate_namespace_id();
                    let data_root = cat.create_collection(ns, ns_id, bson::doc! {}, now_millis())?;
                    drop(cat);
                    sync_catalog_root_structural(&self.shared, &self.metadata_state, batch)?;
                    let data_store = self.shared.new_structural_store(batch);
                    BTree::create_at(data_store, data_root)?;
                    cat = self.metadata_state.catalog_lock();
                    cat.get_collection(ns)?.ok_or_else(|| {
                        Error::Internal(format!(
                            "collection '{}' missing after index bootstrap",
                            ns
                        ))
                    })?;
                }

                // Allocate a durable index id.
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
                sync_catalog_root_structural(&self.shared, &self.metadata_state, batch)?;

                // Initialize the freshly-allocated leaf page so the index
                // tree is valid to open for writes during the build step.
                let idx_store = self.shared.new_structural_store(batch);
                BTree::create_at(idx_store, idx_root)?;
                Ok(())
            },
            || {},
        )?;
        // FIX1: interval sync after publish — no post-publish bookkeeping at
        // this site, positionally identical to the baseline. On sync-Err we
        // propagate before guard.commit so the RAII guard reopens via Drop.
        self.maybe_sync_interval_after_publish()?;

        if let Some(guard) = ddl_guard {
            guard.commit();
        }
        reservation
            .map(ReserveOutcome::Reserved)
            .ok_or_else(|| Error::Internal("missing create-index reservation".into()))
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
            let cat = self.metadata_state.catalog_lock();
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
        super::hidden_accessors::create_index_build_if_installed(&self.shared, ns, name)?;

        {
            let _md_read = self
                .metadata
                .read()
                .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
            let cat = self.metadata_state.catalog_lock();
            let collection = cat.get_collection(ns)?.ok_or_else(stale_target)?;
            if collection.id != data_entry.id {
                return Err(stale_target());
            }
            let index = cat.get_index(ns, name)?.ok_or_else(stale_target)?;
            if index.id != idx_entry.id || index.state != IndexState::Building {
                return Err(stale_target());
            }
        }

        // Stage the build in the structural batch. Journal rollback marks are
        // not captured during the body because no journal frames are appended
        // here, and other namespaces may commit while this long build runs.
        let mut batch = StructuralPageBatch::new(&self.shared.handle);
        let mut catalog_update_rollback = None;

        let body: Result<()> = (|| {
            // The data tree is read-only during index build — we use a
            // plain BufferPoolPageStore (no structural batch) so the
            // idx_store can hold the sole mutable batch borrow simultaneously.
            let data_store = self.shared.new_btree_store();
            let data_tree = BTree::open(
                data_store,
                data_entry.data_root_page,
                data_entry.data_root_level,
            );
            // ITEM 1: open the view directly — it takes the conservative
            // registry pin before loading the published epoch itself.
            let read_view = open_snapshot_read_view(&self.shared)?;
            let primary_history = primary_history_probe(&self.shared, data_entry.id);
            let (root_page, root_level, any_multikey) = {
                let idx_store = self.shared.new_structural_store(&mut batch);
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
                let old_store = self.shared.new_structural_store(&mut batch);
                BTree::open(old_store, idx_entry.root_page, idx_entry.root_level)
                    .free_all_pages()?;
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
                let mut cat = self.metadata_state.catalog_lock();
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
                catalog_update_rollback = Some((idx_entry.clone(), updated));
                drop(cat);
                sync_catalog_root_structural(&self.shared, &self.metadata_state, &mut batch)?;

                #[cfg(test)]
                self.test_fail_after_build_catalog_update_if_armed()?;
            }
            Ok(())
        })();

        match body {
            Ok(()) => {
                let catalog_generation = self.shared.published.load_full().catalog_generation;
                let slot = self
                    .shared
                    .publish_sequencer
                    .register_with_oracle(&self.shared.oracle)?;
                if let Err(error) = self.commit_catalog_batch_to_log(
                    CatalogCommitKind::IndexBuild,
                    catalog_generation,
                    catalog_generation,
                    &slot,
                    batch,
                ) {
                    if !matches!(error, Error::EngineFatal { .. }) {
                        self.shared.publish_sequencer.mark_aborted(slot);
                    }
                    return Err(error);
                }
                let publish_result = self
                    .shared
                    .publish_sequencer
                    .mark_ready(slot, |_publish_ts| Ok(()));
                match publish_result {
                    Ok(()) => {
                        self.maybe_sync_interval_after_publish()?;
                    }
                    Err(Error::EngineFatal { reason }) => {
                        return Err(Error::EngineFatal { reason });
                    }
                    Err(_) => {
                        return Err(self.engine_fatal(
                            crate::error::EngineFatalReason::PostDurableDdlPublishFailure,
                        ));
                    }
                }
                Ok(())
            }
            Err(e) => {
                // The build body has not appended any journal frames yet. Do
                // not truncate to `mark`: other namespaces may have committed
                // since this build transaction began.
                if let Some((original, updated)) = &catalog_update_rollback {
                    self.rollback_build_catalog_update(ns, name, original, updated);
                }
                let _ = batch.abort(&self.shared.handle);
                Err(e)
            }
        }
    }

    /// Commit step of `create_index`: flip `state: Ready` under
    /// metadata.write and publish the final snapshot.
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
            let cat = self.metadata_state.catalog_lock();
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
            let cat = self.metadata_state.catalog_lock();
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

        // R8: the Ready flip has no in-memory undo (the `update_index`
        // mutation lives in the batch body and the envelope's abort path
        // rolls it back), so the undo hook is a no-op. The RAII `guard`
        // lifecycle stays a site concern, committed after a successful
        // publish.
        self.run_catalog_ddl_envelope(
            CatalogCommitKind::IndexBuildCommit,
            |batch| -> Result<()> {
                {
                    let mut cat = self.metadata_state.catalog_lock();
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
                sync_catalog_root_structural(&self.shared, &self.metadata_state, batch)?;
                Ok(())
            },
            || {},
        )?;
        // FIX1: interval sync after publish — no post-publish bookkeeping at
        // this site, positionally identical to the baseline. On sync-Err we
        // propagate before guard.commit so the RAII guard reopens via Drop.
        self.maybe_sync_interval_after_publish()?;

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
            let cat = self.metadata_state.catalog_lock();
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
            // A Building entry usually survives to reopen because the ORIGINAL
            // build failed deterministically (e.g. a unique violation over
            // duplicate data). Re-running the build here re-hits the same
            // error, so a bare `?` would propagate it out of `open()` and
            // brick reopen FOREVER. Mirror the live `create_index` driver
            // (this file, ~:71-90): on a non-fatal build/commit error drop the
            // orphan Building entry via `create_index_cleanup` and continue,
            // so the database opens and the un-buildable index simply does not
            // come back. `EngineFatal` is a genuine poison and must propagate.
            let resumed = self
                .create_index_build_inner(&ns, &name, true)
                .and_then(|()| self.create_index_commit(&ns, &name, &target));
            if let Err(error) = resumed {
                if matches!(error, Error::EngineFatal { .. }) {
                    return Err(error);
                }
                if let Err(cleanup_err) = self.create_index_cleanup(&ns, &name, &target) {
                    if matches!(cleanup_err, Error::EngineFatal { .. }) {
                        return Err(cleanup_err);
                    }
                    // Cleanup itself failed non-fatally: surface the original
                    // build error wrapped with the cleanup failure so the
                    // operator sees both, rather than silently leaving the
                    // orphan Building entry to brick the NEXT reopen too.
                    return Err(Error::Internal(format!(
                        "resume of Building index '{name}' on '{ns}' failed: {error}; \
                         orphan cleanup also failed: {cleanup_err}"
                    )));
                }
            }
        }
        Ok(())
    }

    pub(super) fn free_index_pages_exclusive(
        &self,
        batch: &mut StructuralPageBatch,
        index: &IndexEntry,
    ) -> Result<()> {
        let mut tree = BTree::open(
            self.shared.new_structural_store(batch),
            index.root_page,
            index.root_level,
        );
        let mut pages = tree.collect_pages_by_size()?;
        pages.sort_by_key(|(page_id, _)| *page_id);
        let mut latches = pages
            .iter()
            .map(|(page_id, size)| {
                self.shared
                    .handle
                    .pool()
                    .pin_for_write_sized(*page_id, *size)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut store = self.shared.new_structural_store(batch);
        // The latches above are exclusive — operate directly on the
        // already-latched pages via `LatchedPinnedPage::with_all_chains`
        // instead of routing through `store.with_all_chains_under_latch`,
        // which would deadlock when it tried to re-acquire the same
        // per-page latch via its internal `pin_then_latch`.
        for ((page_id, size), latch) in pages.iter().zip(latches.iter_mut()) {
            match size {
                PageSize::Small4k => store.free_internal(*page_id)?,
                PageSize::Large32k => {
                    latch.with_all_chains(|chains| chains.clear())?;
                    store.free_leaf(*page_id)?;
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
            let cat = self.metadata_state.catalog_lock();
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
            let cat = self.metadata_state.catalog_lock();
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

        // R8: cleanup has no in-memory undo (the `drop_index` mutation lives
        // in the batch body and the envelope's abort path rolls it back), so
        // the undo hook is a no-op. An empty batch (the `removed == false`
        // no-op revalidation) still commits and publishes through the
        // envelope exactly as before. The RAII `guard` lifecycle stays a site
        // concern, committed after a successful publish.
        self.run_catalog_ddl_envelope(
            CatalogCommitKind::IndexCleanup,
            |batch| -> Result<()> {
                self.free_index_pages_exclusive(batch, &target_index)?;
                {
                    let mut cat = self.metadata_state.catalog_lock();
                    let removed = cat.drop_index(ns, name)?;
                    if !removed {
                        return Ok(());
                    }
                }
                sync_catalog_root_structural(&self.shared, &self.metadata_state, batch)?;
                self.shared.clear_dirty_tree(&TreeIdent {
                    collection_id: target.ns_id,
                    kind: TreeKind::Secondary {
                        index_id: target.index_id,
                    },
                });
                Ok(())
            },
            || {},
        )?;
        // FIX1: interval sync after publish — no post-publish bookkeeping at
        // this site, positionally identical to the baseline. On sync-Err we
        // propagate before guard.commit so the RAII guard reopens via Drop.
        self.maybe_sync_interval_after_publish()?;

        guard.commit();
        Ok(())
    }
}
