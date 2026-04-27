//! Secondary-index maintenance + pending-write installation helpers.

use std::sync::Arc;

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::keys::{encode_compound_key, COMPOUND_SEP};
use crate::mvcc::transaction::WriteTxn;
use crate::query::planner::IndexCondition;
use crate::storage::btree::{BTree, BTreePageStore, CellValue, HistoryProbe};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::{CollectionEntry, IndexEntry, IndexState};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::secondary_index::{
    update_index_on_delete, update_index_on_insert, update_index_on_update,
};
use crate::storage::txn_page_store::TxnOverlay;

use super::catalog_ops::{catalog_lock, new_store, new_txn_store, sync_catalog_root_overlay};
use super::state::{MetadataState, SharedState};
use super::visibility::WriteVisibility;

/// Retrieve the serialised `_id` value stored in an index tree entry.
pub(super) fn index_entry_id_free(handle: &Arc<BufferPoolHandle>, cv: CellValue) -> Result<Bson> {
    let bytes = match cv {
        CellValue::Inline(b) => b,
        CellValue::Overflow {
            first_page,
            total_length,
        } => {
            let tmp_store = BufferPoolPageStore::new(Arc::clone(handle));
            let tmp_tree = BTree::open(tmp_store, 1, 0);
            tmp_tree.read_overflow(first_page, total_length)?
        }
    };
    if bytes.is_empty() {
        return Ok(Bson::Null);
    }
    let doc: Document = bson::from_slice(&bytes).map_err(Error::BsonDeserialization)?;
    Ok(doc.get("_id").cloned().unwrap_or(Bson::Null))
}

/// Build the [start, end] range for a secondary index B+ tree scan.
pub(super) fn index_bounds_free(
    condition: &IndexCondition,
    ascending: bool,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    fn prefix(val: &Bson, asc: bool) -> Vec<u8> {
        let mut p = encode_compound_key(&[(val, asc)]);
        p.push(COMPOUND_SEP);
        p
    }
    fn prefix_next(val: &Bson, asc: bool) -> Vec<u8> {
        let mut p = prefix(val, asc);
        if let Some(last) = p.last_mut() {
            *last += 1;
        }
        p
    }
    match condition {
        IndexCondition::Eq(v) => (Some(prefix(v, ascending)), Some(prefix_next(v, ascending))),
        IndexCondition::Any => (None, None),
        IndexCondition::In(_) => (None, None),
        IndexCondition::Range { gt, gte, lt, lte } => {
            if ascending {
                let start = match (gte.as_ref(), gt.as_ref()) {
                    (Some(v), _) => Some(prefix(v, true)),
                    (None, Some(v)) => Some(prefix_next(v, true)),
                    _ => None,
                };
                let end = match (lte.as_ref(), lt.as_ref()) {
                    (Some(v), _) => Some(prefix_next(v, true)),
                    (None, Some(v)) => Some(prefix(v, true)),
                    _ => None,
                };
                (start, end)
            } else {
                let start = match (lte.as_ref(), lt.as_ref()) {
                    (Some(v), _) => Some(prefix(v, false)),
                    (None, Some(v)) => Some(prefix_next(v, false)),
                    _ => None,
                };
                let end = match (gte.as_ref(), gt.as_ref()) {
                    (Some(v), _) => Some(prefix_next(v, false)),
                    (None, Some(v)) => Some(prefix(v, false)),
                    _ => None,
                };
                (start, end)
            }
        }
    }
}

/// Persist updated root/level and multikey flag for an index entry.
///
/// Phase 1 §10.3 — mutates `txn.publish_dirty` per the §10.3 table:
///   - root moved on a Ready index: mark_published + mark_header.
///   - root moved on a Building index: mark_header only (readers ignore
///     Building per §3.3 / §4.3, so the published payload does not
///     change).
///   - multikey flip (root unchanged): mark_header only — multikey is
///     not a published field (§10.3), but the catalog tree changed on
///     disk.
#[allow(
    clippy::too_many_arguments,
    reason = "index-root sync threads existing commit context without introducing a one-use args type"
)]
pub(super) fn sync_index_entry(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
    orig: &IndexEntry,
    new_root: u32,
    new_level: u8,
    new_multikey: bool,
    txn: &mut WriteTxn,
) -> Result<()> {
    let root_changed = new_root != orig.root_page || new_level != orig.root_level;
    let multikey_changed = new_multikey && !orig.multikey;
    if !root_changed && !multikey_changed {
        return Ok(());
    }
    let mut updated = orig.clone();
    if root_changed {
        updated.root_page = new_root;
        updated.root_level = new_level;
    }
    if multikey_changed {
        updated.multikey = true;
    }
    catalog_lock(md).update_index(&updated)?;
    sync_catalog_root_overlay(shared, md, overlay)?;
    // Phase 1 §10.3 — classify the catalog mutation we just persisted.
    if root_changed {
        txn.mark_header();
        if matches!(orig.state, IndexState::Ready) {
            txn.mark_published();
        }
    } else if multikey_changed {
        // multikey is NOT a published field — only the on-disk catalog
        // tree changed, so the reader-visible Arc<PublishedCatalog> may
        // be reused at publish time.
        txn.mark_header();
    }
    Ok(())
}

/// Maintain all secondary indexes after a document insert.
#[allow(
    clippy::too_many_arguments,
    reason = "US-010 threads writer visibility into the existing insert maintenance API"
)]
pub(super) fn maintain_secondary_on_insert(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
    ns: &str,
    doc: &Document,
    doc_id: &Bson,
    vis: &WriteVisibility<'_>,
    txn: &mut WriteTxn,
) -> Result<()> {
    let entries = catalog_lock(md).list_indexes(ns)?;
    for entry in entries {
        let store = new_txn_store(shared, overlay);
        let idx_tree = BTree::open(store, entry.root_page, entry.root_level);
        let history = vis
            .secondary_history
            .as_ref()
            .map(|probe| probe as &dyn HistoryProbe);
        let is_multikey = update_index_on_insert(
            doc,
            doc_id,
            &idx_tree,
            &entry,
            vis.read_view.as_ref(),
            history,
            txn,
        )?;
        // Extract root values before dropping idx_tree so the &mut overlay
        // borrow from its store is released before sync_index_entry borrows
        // overlay again.
        let (new_root, new_level) = (idx_tree.root_page, idx_tree.root_level);
        drop(idx_tree);
        sync_index_entry(
            shared,
            md,
            overlay,
            &entry,
            new_root,
            new_level,
            is_multikey,
            txn,
        )?;
    }
    Ok(())
}

/// Maintain all secondary indexes after a document delete.
pub(super) fn maintain_secondary_on_delete(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
    ns: &str,
    doc: &Document,
    doc_id: &Bson,
    txn: &mut WriteTxn,
) -> Result<()> {
    let entries = catalog_lock(md).list_indexes(ns)?;
    for entry in entries {
        update_index_on_delete(doc, doc_id, &entry, txn)?;
        sync_index_entry(
            shared,
            md,
            overlay,
            &entry,
            entry.root_page,
            entry.root_level,
            false,
            txn,
        )?;
    }
    Ok(())
}

/// Maintain all secondary indexes when a document is replaced.
#[allow(
    clippy::too_many_arguments,
    reason = "update maintenance mirrors the insert/delete helpers plus old/new document ids"
)]
pub(super) fn maintain_secondary_on_update(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
    ns: &str,
    old_doc: &Document,
    new_doc: &Document,
    old_id: &Bson,
    new_id: &Bson,
    vis: &WriteVisibility<'_>,
    txn: &mut WriteTxn,
) -> Result<()> {
    let entries = catalog_lock(md).list_indexes(ns)?;
    for entry in entries {
        let store = new_txn_store(shared, overlay);
        let idx_tree = BTree::open(store, entry.root_page, entry.root_level);
        let history = vis
            .secondary_history
            .as_ref()
            .map(|probe| probe as &dyn HistoryProbe);
        let is_multikey = update_index_on_update(
            old_doc,
            new_doc,
            old_id,
            new_id,
            &idx_tree,
            &entry,
            vis.read_view.as_ref(),
            history,
            txn,
        )?;
        let (new_root, new_level) = (idx_tree.root_page, idx_tree.root_level);
        drop(idx_tree);
        sync_index_entry(
            shared,
            md,
            overlay,
            &entry,
            new_root,
            new_level,
            is_multikey,
            txn,
        )?;
    }
    Ok(())
}

/// Drain the given `SecIndexWrite` batch into resident secondary-index
/// delta heads.
pub(super) fn install_pending_sec_index(
    shared: &SharedState,
    md: &MetadataState,
    _overlay: &mut TxnOverlay,
    writes: Vec<crate::mvcc::SecIndexWrite>,
    _vis: &WriteVisibility<'_>,
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<()> {
    if writes.is_empty() {
        return Ok(());
    }
    use crate::mvcc::{SecIndexOp, Ts, VersionData, VersionEntry, VersionState};
    use crate::storage::buffer_pool::PageSize;
    use std::collections::HashMap as StdHashMap;

    let mut entry_by_id: StdHashMap<i64, IndexEntry> = StdHashMap::new();
    {
        let cat = catalog_lock(md);
        let collections = cat.list_collections()?;
        for coll in &collections {
            for entry in cat.list_indexes(&coll.name)? {
                entry_by_id.insert(entry.id, entry);
            }
        }
    }

    for write in writes {
        let entry = entry_by_id.get(&write.index_id).ok_or_else(|| {
            Error::Internal(format!(
                "pending sec-index write references unknown index_id {}",
                write.index_id
            ))
        })?;
        let idx_tree = BTree::open(new_store(shared), write.index_root_page, entry.root_level);
        let leaf_page = idx_tree.find_leaf(&write.key)?;
        let _pin = shared.handle.fetch_page(leaf_page, PageSize::Large32k)?;
        let mut chain_arc = shared
            .handle
            .pool()
            .get_or_create_chain(leaf_page, &write.key)?;
        {
            let chain_mut = Arc::make_mut(&mut chain_arc);
            if let Some(prev_head) = chain_mut.front_mut() {
                prev_head.stop_ts = commit_ts;
            }
            let (data, is_tombstone) = match write.op {
                SecIndexOp::Insert { id_bytes } => (VersionData::Inline(id_bytes), false),
                SecIndexOp::Delete => (VersionData::Inline(Vec::new()), true),
            };
            chain_mut.push_front(VersionEntry {
                start_ts: commit_ts,
                stop_ts: Ts::MAX,
                txn_id,
                state: VersionState::Pending { txn_id },
                data,
                is_tombstone,
            });
        }
        shared
            .handle
            .pool()
            .put_chain(leaf_page, write.key, chain_arc)?;
    }

    Ok(())
}

/// Install staged primary-tree writes as fresh heads on each key's
/// per-leaf version chain.
pub(super) fn install_pending_primary(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
    writes: Vec<crate::mvcc::PrimaryWrite>,
    _vis: &WriteVisibility<'_>,
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<()> {
    #[cfg(test)]
    super::us009_tests::record_install_pending_primary_call();

    if writes.is_empty() {
        return Ok(());
    }
    use crate::mvcc::{PrimaryOp, Ts, VersionData, VersionEntry, VersionState};
    use crate::storage::buffer_pool::PageSize;

    for write in writes {
        let (root_page, root_level) = match catalog_lock(md).get_collection(&write.ns)? {
            Some(c) => (c.data_root_page, c.data_root_level),
            None => continue,
        };
        let tree = BTree::open(new_txn_store(shared, overlay), root_page, root_level);
        let leaf_page = tree.find_leaf(&write.key)?;
        let _pin = shared.handle.fetch_page(leaf_page, PageSize::Large32k)?;
        let mut chain_arc = shared
            .handle
            .pool()
            .take_chain(leaf_page, &write.key)?
            .unwrap_or_default();
        {
            let chain_mut = std::sync::Arc::make_mut(&mut chain_arc);
            if let Some(prev_head) = chain_mut.front_mut() {
                prev_head.stop_ts = commit_ts;
            }
            let (data, is_tombstone) = match write.op {
                PrimaryOp::Insert { data } => (VersionData::Inline(data), false),
                PrimaryOp::Update { data } => (VersionData::Inline(data), false),
                PrimaryOp::Delete => (VersionData::Inline(Vec::new()), true),
            };
            chain_mut.push_front(VersionEntry {
                start_ts: commit_ts,
                stop_ts: Ts::MAX,
                txn_id,
                state: VersionState::Pending { txn_id },
                data,
                is_tombstone,
            });
        }
        shared
            .handle
            .pool()
            .put_chain(leaf_page, write.key, chain_arc)?;
    }
    Ok(())
}

/// Flip secondary-index heads installed by this transaction from Pending to
/// Committed after the S12 published-epoch swap.
pub(super) fn commit_pending_sec_index_states(
    shared: &SharedState,
    md: &MetadataState,
    writes: &[crate::mvcc::SecIndexWrite],
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<()> {
    if writes.is_empty() {
        return Ok(());
    }
    use std::collections::HashMap as StdHashMap;

    let mut entry_by_id: StdHashMap<i64, IndexEntry> = StdHashMap::new();
    {
        let cat = catalog_lock(md);
        let collections = cat.list_collections()?;
        for coll in &collections {
            for entry in cat.list_indexes(&coll.name)? {
                entry_by_id.insert(entry.id, entry);
            }
        }
    }

    for write in writes {
        let entry = entry_by_id.get(&write.index_id).ok_or_else(|| {
            Error::Internal(format!(
                "pending sec-index flip references unknown index_id {}",
                write.index_id
            ))
        })?;
        commit_pending_chain_head(
            shared,
            entry.root_page,
            entry.root_level,
            &write.key,
            commit_ts,
            txn_id,
        )?;
    }
    Ok(())
}

/// Flip primary heads installed by this transaction from Pending to
/// Committed after the S12 published-epoch swap.
pub(super) fn commit_pending_primary_states(
    shared: &SharedState,
    md: &MetadataState,
    writes: &[crate::mvcc::PrimaryWrite],
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<()> {
    if writes.is_empty() {
        return Ok(());
    }

    for write in writes {
        let coll = match catalog_lock(md).get_collection(&write.ns)? {
            Some(coll) => coll,
            None => continue,
        };
        commit_pending_primary_head(shared, &coll, &write.key, commit_ts, txn_id)?;
    }
    Ok(())
}

/// Flip primary heads using an uncommitted overlay for leaf routing.
///
/// This is used after S12 when root-neutral compatibility page images are
/// intentionally delayed until after publish; the resident chain already lives
/// on the leaf selected through that overlay.
pub(super) fn commit_pending_primary_states_with_overlay(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
    writes: &[crate::mvcc::PrimaryWrite],
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<()> {
    if writes.is_empty() {
        return Ok(());
    }

    for write in writes {
        let coll = match catalog_lock(md).get_collection(&write.ns)? {
            Some(coll) => coll,
            None => continue,
        };
        let tree = BTree::open(
            new_txn_store(shared, overlay),
            coll.data_root_page,
            coll.data_root_level,
        );
        let leaf_page = tree.find_leaf(&write.key)?;
        drop(tree);
        commit_pending_chain_head_on_leaf(shared, leaf_page, &write.key, commit_ts, txn_id)?;
    }
    Ok(())
}

pub(super) fn commit_pending_primary_head(
    shared: &SharedState,
    coll: &CollectionEntry,
    key: &[u8],
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<()> {
    commit_pending_chain_head(
        shared,
        coll.data_root_page,
        coll.data_root_level,
        key,
        commit_ts,
        txn_id,
    )
}

fn commit_pending_chain_head(
    shared: &SharedState,
    root_page: u32,
    root_level: u8,
    key: &[u8],
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<()> {
    let tree = BTree::open(new_store(shared), root_page, root_level);
    let leaf_page = tree.find_leaf(key)?;
    commit_pending_chain_head_on_leaf(shared, leaf_page, key, commit_ts, txn_id)
}

fn commit_pending_chain_head_on_leaf(
    shared: &SharedState,
    leaf_page: u32,
    key: &[u8],
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<()> {
    use crate::mvcc::VersionState;
    use crate::storage::buffer_pool::PageSize;

    let _pin = shared.handle.fetch_page(leaf_page, PageSize::Large32k)?;
    let mut chain_arc = shared
        .handle
        .pool()
        .take_chain(leaf_page, key)?
        .ok_or_else(|| Error::Internal("pending version chain missing".into()))?;
    {
        let chain_mut = Arc::make_mut(&mut chain_arc);
        let entry = chain_mut
            .iter_mut()
            .find(|entry| entry.start_ts == commit_ts && entry.txn_id == txn_id)
            .ok_or_else(|| Error::Internal("pending version head missing".into()))?;
        match entry.state {
            VersionState::Pending { txn_id: id } if id == txn_id => {
                entry.state = VersionState::Committed;
            }
            VersionState::Committed => {}
            _ => {
                return Err(Error::Internal(
                    "pending version head state mismatch".into(),
                ))
            }
        }
    }
    shared
        .handle
        .pool()
        .put_chain(leaf_page, key.to_vec(), chain_arc)?;
    Ok(())
}

/// Fold visible secondary delta heads into base pages during checkpoint.
///
/// Live commits keep secondary writes as resident deltas. A checkpoint is the
/// point where those committed heads may be materialized into base cells so a
/// clean reopen does not need still-resident memory or uncheckpointed logical
/// frames to recover `Ready` index contents.
///
/// Returns `true` when any Ready index root changes and the published catalog
/// must be rebuilt after the overlay is committed.
pub(super) fn materialize_ready_secondary_deltas_for_checkpoint(
    shared: &SharedState,
    md: &MetadataState,
    overlay: &mut TxnOverlay,
) -> Result<bool> {
    let entries = {
        let cat = catalog_lock(md);
        let collections = cat.list_collections()?;
        let mut entries = Vec::new();
        for coll in &collections {
            for entry in cat.list_indexes(&coll.name)? {
                if matches!(entry.state, IndexState::Ready) {
                    entries.push(entry);
                }
            }
        }
        entries
    };

    if entries.is_empty() {
        return Ok(false);
    }

    let mut published_catalog_dirty = false;
    let epoch = shared.published.load_full();
    let view = crate::mvcc::read_view::ReadView::new_for_epoch(epoch, 0);
    for entry in entries {
        let read_tree = BTree::open(new_store(shared), entry.root_page, entry.root_level);
        let deltas = read_tree.visible_delta_entries(&view)?;
        if deltas.is_empty() {
            continue;
        }

        let mut tree = BTree::open(
            new_txn_store(shared, overlay),
            entry.root_page,
            entry.root_level,
        );
        for (key, value) in deltas {
            apply_secondary_checkpoint_delta(&mut tree, &key, value.as_deref())?;
        }
        let new_root = tree.root_page;
        let new_level = tree.root_level;
        drop(tree);

        if new_root != entry.root_page || new_level != entry.root_level {
            let mut updated = entry;
            updated.root_page = new_root;
            updated.root_level = new_level;
            if !catalog_lock(md).update_index(&updated)? {
                return Err(Error::Internal(
                    "checkpoint secondary materialization lost index metadata".into(),
                ));
            }
            published_catalog_dirty = true;
        }
    }

    Ok(published_catalog_dirty)
}

fn apply_secondary_checkpoint_delta<S: BTreePageStore>(
    tree: &mut BTree<S>,
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<()> {
    match value {
        Some(bytes) => {
            if let Err(e) = tree.insert(key, bytes) {
                if !matches!(e, Error::DuplicateKey { .. }) {
                    return Err(e);
                }
            }
        }
        None => {
            let _ = tree.delete(key)?;
        }
    }
    Ok(())
}

/// Outcome of `create_index_reserve` (reserve step of the 3-step build).
#[derive(Clone, Copy)]
pub(super) enum ReserveOutcome {
    /// A fresh Building entry was reserved; caller should proceed to
    /// the build and commit steps.
    Reserved,
    /// An index with the same name already exists; `create_index` is
    /// idempotent and returns Ok immediately.
    AlreadyExists,
}

// ---------------------------------------------------------------------------
// Engine-level index operation free functions
// ---------------------------------------------------------------------------

use crate::index::{IndexInfo, IndexModel};
use crate::storage::secondary_index::generate_index_name;

use super::doc_helpers::validate_index_keys;

pub(super) fn create_index(
    engine: &super::PagedEngine,
    ns: &str,
    model: &IndexModel,
) -> crate::error::Result<String> {
    validate_index_keys(&model.keys)?;
    let name = model
        .options
        .name
        .clone()
        .unwrap_or_else(|| generate_index_name(&model.keys));

    let reserve_outcome = engine.create_index_reserve(ns, model, &name)?;
    match reserve_outcome {
        ReserveOutcome::AlreadyExists => return Ok(name),
        ReserveOutcome::Reserved => {}
    }

    if let Err(build_err) = engine.create_index_build(ns, &name) {
        if let Err(cleanup_err) = engine.create_index_cleanup(ns, &name) {
            return Err(crate::error::Error::Internal(format!(
                "create_index build failed: {}; cleanup also failed: {}",
                build_err, cleanup_err
            )));
        }
        return Err(build_err);
    }

    engine.create_index_commit(ns, &name)?;
    Ok(name)
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
    let lane_arc = engine.lane_for(ns);
    let _lane_guard = engine.acquire_lane(lane_arc)?;
    engine.run_ddl(|shared, md, overlay| {
        let removed = catalog_lock(md).drop_index(ns, name)?;
        if removed {
            sync_catalog_root_overlay(shared, md, overlay)?;
            Ok(())
        } else {
            Err(crate::error::Error::Internal(format!(
                "index '{}' not found on '{}'",
                name, ns
            )))
        }
    })
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
        })
        .collect())
}
