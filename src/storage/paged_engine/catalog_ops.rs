//! Catalog / snapshot build + structural header helpers used by the engine.
//!
//! Phase 1 §10.2: the canonical publish helper lives in `publish.rs`
//! (`publish_commit`, `build_published_catalog`). This module exposes
//! `rebuild_and_publish_locked` as a thin wrapper that drives
//! `publish_commit` with a caller-provided `PublishDirty` (threaded
//! from the mutation sites per §10.3 / US-006).

use crate::error::Result;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::Catalog;
use crate::storage::structural_page_batch::{StructuralBatchStore, StructuralPageBatch};

use super::publish::{publish_commit, PublishDirty};
use super::state::{MetadataState, SharedState};

/// Lock the catalog with the existing panic-on-poison behavior.
#[allow(
    clippy::expect_used,
    reason = "catalog poisoning is an invariant breach; existing behavior is to panic"
)]
pub(super) fn catalog_lock(
    md: &MetadataState,
) -> std::sync::MutexGuard<'_, Catalog<BufferPoolPageStore>> {
    md.catalog.lock().expect("catalog poisoned")
}

/// Thin wrapper that takes the CRUD / DDL catalog lock, invokes
/// `publish_commit` with the threaded `PublishDirty`, and (for now)
/// records the Phase 0 rebuild counter when a fresh
/// `Arc<PublishedCatalog>` was actually built. US-012 will split this
/// into the four Phase 1 counters; for US-006 we only need to gate the
/// existing rebuild tick on `dirty.published_catalog_dirty`.
///
/// `reserved_catalog_gen` (Phase 5 §10.17.1, US-006):
///   - DDL callers pass `Some(reserved)` where `reserved` was returned
///     by `SharedState.next_catalog_gen.fetch_add(1, AcqRel) + 1` under
///     `metadata.write()` BEFORE this publish. The published epoch's
///     `catalog_generation` is stamped with that exact reservation.
///   - CRUD callers pass `None`. The published epoch's
///     `catalog_generation` inherits the prior published value, never
///     advancing through a CRUD publish (§10.21 CV-5).
pub(super) fn rebuild_and_publish_locked(
    shared: &SharedState,
    md: &MetadataState,
    publish_ts: crate::mvcc::timestamp::Ts,
    dirty: PublishDirty,
    reserved_catalog_gen: Option<u64>,
) -> Result<()> {
    let cat = catalog_lock(md);
    let _epoch = publish_commit(shared, &cat, publish_ts, dirty, reserved_catalog_gen)?;
    if dirty.published_catalog_dirty {
        crate::mvcc::metrics::record_published_snapshot_rebuild();
    }
    Ok(())
}

/// Create a new [`BufferPoolPageStore`] backed by `shared.handle`.
pub(super) fn new_store(shared: &SharedState) -> BufferPoolPageStore {
    BufferPoolPageStore::new(std::sync::Arc::clone(&shared.handle))
}

/// Create a structural writer-side page store borrowing the given batch.
pub(super) fn new_structural_store<'a>(
    shared: &SharedState,
    batch: &'a mut StructuralPageBatch,
) -> StructuralBatchStore<'a> {
    batch.store(new_store(shared))
}

/// Update catalog-root header fields through the structural header owner.
pub(super) fn sync_catalog_root_structural(
    shared: &SharedState,
    md: &MetadataState,
    batch: &mut StructuralPageBatch,
) -> Result<()> {
    let (root_page, root_level, next_namespace_id, next_index_id) = {
        let cat = catalog_lock(md);
        (
            cat.root_page(),
            cat.root_level(),
            cat.next_namespace_id() as u64,
            cat.next_index_id() as u64,
        )
    };
    batch.update_header(&shared.handle, |header| {
        header.catalog_root_page = root_page;
        header.catalog_root_level = root_level;
        header.catalog_root_backup = root_page;
        header.next_namespace_id = next_namespace_id;
        header.next_index_id = next_index_id;
    })
}
