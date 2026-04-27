//! Catalog / snapshot build + overlay helpers used by the engine.
//!
//! Phase 1 §10.2: the canonical publish helper lives in `publish.rs`
//! (`publish_commit`, `build_published_catalog`). This module exposes
//! `rebuild_and_publish_locked` as a thin wrapper that drives
//! `publish_commit` with a caller-provided `PublishDirty` (threaded
//! from the mutation sites per §10.3 / US-006).

use crate::error::Result;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::Catalog;
use crate::storage::txn_page_store::{TxnOverlay, TxnPageStore};

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
pub(super) fn rebuild_and_publish_locked(
    shared: &SharedState,
    md: &MetadataState,
    publish_ts: crate::mvcc::timestamp::Ts,
    dirty: PublishDirty,
) -> Result<()> {
    let cat = catalog_lock(md);
    let _epoch = publish_commit(shared, &cat, publish_ts, dirty)?;
    if dirty.published_catalog_dirty {
        crate::mvcc::metrics::record_published_snapshot_rebuild();
    }
    Ok(())
}

/// Create a new [`BufferPoolPageStore`] backed by `shared.handle`.
pub(super) fn new_store(shared: &SharedState) -> BufferPoolPageStore {
    BufferPoolPageStore::new(std::sync::Arc::clone(&shared.handle))
}

/// Create a new writer-side [`TxnPageStore`] borrowing the given overlay.
pub(super) fn new_txn_store<'a>(
    shared: &SharedState,
    overlay: &'a mut TxnOverlay,
) -> TxnPageStore<'a> {
    TxnPageStore::new(new_store(shared), overlay)
}

/// Update `FileHeader::catalog_root_page` and `catalog_root_level` to
/// reflect the current catalog root, routing through the provided txn
/// overlay so a rollback can restore only the txn's catalog-root change.
pub(super) fn sync_catalog_root_overlay(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
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
    txn_update_header(shared, overlay, |h| {
        h.catalog_root_page = root_page;
        h.catalog_root_level = root_level;
        h.catalog_root_backup = root_page;
        // Phase 1 §10.7 — persist the bumped id counters atomically with
        // the catalog root write. On reopen the header counter is always
        // `>=` every live id on disk (leak is acceptable, reuse is not).
        h.next_namespace_id = next_namespace_id;
        h.next_index_id = next_index_id;
    })
}

/// Mutate the file header, recording the txn-local catalog-root transition
/// into the overlay on first call.
pub(super) fn txn_update_header<F>(
    shared: &SharedState,
    overlay: &mut TxnOverlay,
    f: F,
) -> Result<()>
where
    F: FnOnce(&mut crate::storage::header::FileHeader),
{
    shared.handle.allocator().update_header(|header| {
        let before = header.clone();
        f(header);
        let after = header.clone();
        overlay.capture_header_change_once(&before, &after);
    })
}
