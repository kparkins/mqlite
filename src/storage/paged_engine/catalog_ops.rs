//! Catalog / snapshot build + overlay helpers used by the engine.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::Catalog;
use crate::storage::root_snapshot::{NamespaceSnapshot, PublishedIndex, PublishedSnapshot};
use crate::storage::txn_page_store::{TxnOverlay, TxnPageStore};

use super::state::{MetadataState, SharedState};

/// Build a `PublishedSnapshot` from the given catalog state.
pub(super) fn build_snapshot_from_catalog(
    catalog: &Catalog<BufferPoolPageStore>,
    publish_ts: crate::mvcc::timestamp::Ts,
) -> Result<PublishedSnapshot> {
    let mut namespaces = HashMap::new();
    for coll in catalog.list_collections()? {
        let mut idxs = Vec::new();
        for idx in catalog.list_indexes(&coll.name)? {
            idxs.push(PublishedIndex {
                name: idx.name.clone(),
                root_page: idx.root_page,
                root_level: idx.root_level,
                key_pattern: idx.key_pattern.clone(),
                unique: idx.unique,
                sparse: idx.sparse,
                state: idx.state,
            });
        }
        namespaces.insert(coll.name.clone(), NamespaceSnapshot {
            data_root_page: coll.data_root_page,
            data_root_level: coll.data_root_level,
            indexes: idxs,
        });
    }
    Ok(PublishedSnapshot {
        publish_ts,
        namespaces,
    })
}

/// Rebuild the published snapshot from the current catalog and store it
/// atomically. Callers must hold `commit_seq` so publish_ts monotonicity
/// matches commit ordering.
pub(super) fn rebuild_and_publish_locked(
    shared: &SharedState,
    md: &MetadataState,
    publish_ts: crate::mvcc::timestamp::Ts,
) -> Result<()> {
    let cat = md.catalog.lock().expect("catalog poisoned");
    let new_snap = build_snapshot_from_catalog(&cat, publish_ts)?;
    shared.published.store(Arc::new(new_snap));
    Ok(())
}

/// Create a new [`BufferPoolPageStore`] backed by `shared.handle`.
pub(super) fn new_store(shared: &SharedState) -> BufferPoolPageStore {
    BufferPoolPageStore::new(Arc::clone(&shared.handle))
}

/// Create a new writer-side [`TxnPageStore`] sharing the given overlay.
pub(super) fn new_txn_store(shared: &SharedState, overlay: &Arc<Mutex<TxnOverlay>>) -> TxnPageStore {
    TxnPageStore::new(new_store(shared), Arc::clone(overlay))
}

/// Update `FileHeader::catalog_root_page` and `catalog_root_level` to
/// reflect the current catalog root, routing through the provided txn
/// overlay so a rollback restores the pre-image.
pub(super) fn sync_catalog_root_overlay(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &Arc<Mutex<TxnOverlay>>,
) -> Result<()> {
    let (root_page, root_level) = {
        let cat = md.catalog.lock().expect("catalog poisoned");
        (cat.root_page(), cat.root_level())
    };
    txn_update_header(shared, overlay, |h| {
        h.catalog_root_page = root_page;
        h.catalog_root_level = root_level;
        h.catalog_root_backup = root_page;
    })
}

/// Mutate the file header, snapshotting the pre-image into the given
/// overlay on first call.
pub(super) fn txn_update_header<F>(
    shared: &SharedState,
    overlay: &Arc<Mutex<TxnOverlay>>,
    f: F,
) -> Result<()>
where
    F: FnOnce(&mut crate::storage::header::FileHeader),
{
    let mut ov = overlay.lock().expect("TxnOverlay mutex poisoned");
    if let Some(pre) = shared
        .handle
        .allocator()
        .with_header(|h| {
            if ov.has_header_pre() {
                None
            } else {
                Some(h.clone())
            }
        })?
    {
        ov.capture_header_pre_once(&pre);
    }
    drop(ov);
    shared.handle.allocator().update_header(f)
}
