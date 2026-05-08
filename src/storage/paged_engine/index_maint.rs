//! Secondary-index maintenance + pending-write installation helpers.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use bson::{Bson, Document};

use crate::error::{Error, Result, WriteConflictReason};
use crate::journal::log_file::CatalogCommitKind;
use crate::keys::{compound_prefix_range_excluding_trailing_id, encode_compound_key, COMPOUND_SEP};
use crate::mvcc::transaction::WriteTxn;
use crate::query::planner::IndexCondition;
use crate::storage::btree::{BTree, BTreePageStore, CellValue, HistoryProbe};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::{IndexEntry, IndexState};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::reconcile::plan::{DirtyReason, TreeIdent, TreeKind};
use crate::storage::secondary_index::{
    update_index_on_delete, update_index_on_insert, update_index_on_update,
};
use crate::storage::structural_page_batch::StructuralPageBatch;

use super::catalog_ops::{
    catalog_lock, new_store, new_structural_store, rebuild_and_publish_locked,
    sync_catalog_root_structural,
};
use super::publish::PublishDirty;
use super::smo_latch::{acquire_smo_latches, SmoWriteOp, SmoWriteTarget};
use super::state::{MetadataState, SharedState};
use super::visibility::WriteVisibility;

const KEY_PREVIEW_BYTES: usize = 32;

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

/// Maintain all secondary indexes after a document insert.
#[allow(
    clippy::too_many_arguments,
    reason = "US-010 threads writer visibility into the existing insert maintenance API"
)]
pub(super) fn maintain_secondary_on_insert(
    shared: &SharedState,
    md: &MetadataState,
    ns: &str,
    doc: &Document,
    doc_id: &Bson,
    vis: &WriteVisibility<'_>,
    txn: &mut WriteTxn,
) -> Result<()> {
    let entries = catalog_lock(md).list_indexes(ns)?;
    for entry in entries {
        let store = new_store(shared);
        let idx_tree = BTree::open(store, entry.root_page, entry.root_level);
        let history_probe = vis.secondary_history_probe(entry.id);
        let history = Some(&history_probe as &dyn HistoryProbe);
        let is_multikey = update_index_on_insert(
            doc,
            doc_id,
            &idx_tree,
            &entry,
            vis.read_view.as_ref(),
            history,
            txn,
        )?;
        sync_index_entry_metadata(
            md,
            &entry,
            idx_tree.root_page,
            idx_tree.root_level,
            is_multikey,
            txn,
        )?;
    }
    Ok(())
}

/// Maintain all secondary indexes after a document delete.
pub(super) fn maintain_secondary_on_delete(
    _shared: &SharedState,
    md: &MetadataState,
    ns: &str,
    doc: &Document,
    doc_id: &Bson,
    txn: &mut WriteTxn,
) -> Result<()> {
    let entries = catalog_lock(md).list_indexes(ns)?;
    for entry in entries {
        update_index_on_delete(doc, doc_id, &entry, txn)?;
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
        let store = new_store(shared);
        let idx_tree = BTree::open(store, entry.root_page, entry.root_level);
        let history_probe = vis.secondary_history_probe(entry.id);
        let history = Some(&history_probe as &dyn HistoryProbe);
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
        sync_index_entry_metadata(
            md,
            &entry,
            idx_tree.root_page,
            idx_tree.root_level,
            is_multikey,
            txn,
        )?;
    }
    Ok(())
}

fn sync_index_entry_metadata(
    md: &MetadataState,
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

    txn.mark_header();
    if root_changed && matches!(orig.state, IndexState::Ready) {
        txn.mark_published();
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InstallConflictScope {
    Primary,
    Secondary,
}

fn live_head(chain: &VecDeque<crate::mvcc::VersionEntry>) -> Option<&crate::mvcc::VersionEntry> {
    use crate::mvcc::{Ts, VersionState};

    chain
        .iter()
        .find(|entry| entry.stop_ts == Ts::MAX && !matches!(entry.state, VersionState::Aborted))
}

fn same_txn_pending(entry: &crate::mvcc::VersionEntry, txn_id: u64) -> bool {
    matches!(entry.state, crate::mvcc::VersionState::Pending { txn_id: id } if id == txn_id)
}

fn head_identity(entry: &crate::mvcc::VersionEntry) -> crate::mvcc::ExpectedHead {
    crate::mvcc::ExpectedHead {
        commit_ts: entry.start_ts,
        txn_id: entry.txn_id,
    }
}

fn key_preview(key: &[u8]) -> Vec<u8> {
    key.iter().copied().take(KEY_PREVIEW_BYTES).collect()
}

fn index_field_directions(entry: &IndexEntry) -> Vec<bool> {
    entry
        .key_pattern
        .iter()
        .map(|(_, dir)| !matches!(dir, Bson::Int32(-1) | Bson::Int64(-1)))
        .collect()
}

fn unique_prefix_preview(prefix_start: &[u8]) -> Vec<u8> {
    let prefix = prefix_start
        .strip_suffix(&[COMPOUND_SEP])
        .unwrap_or(prefix_start);
    key_preview(prefix)
}

fn classify_delta_install(
    chain: &VecDeque<crate::mvcc::VersionEntry>,
    expected_head: Option<crate::mvcc::ExpectedHead>,
    scope: InstallConflictScope,
    key: &[u8],
    txn_id: u64,
) -> Result<bool> {
    let Some(head) = live_head(chain) else {
        return Ok(false);
    };

    if same_txn_pending(head, txn_id) {
        return Ok(true);
    }

    match expected_head {
        Some(expected) if head_identity(head) == expected => Ok(false),
        Some(_) => Err(Error::WriteConflict {
            reason: WriteConflictReason::StaleSnapshot,
        }),
        None if scope == InstallConflictScope::Primary => {
            if matches!(head.state, crate::mvcc::VersionState::Committed) && head.is_tombstone {
                Ok(false)
            } else {
                Err(Error::WriteConflict {
                    reason: WriteConflictReason::SameKeyConflict {
                        key_preview: key_preview(key),
                    },
                })
            }
        }
        None => Ok(false),
    }
}

fn check_unique_prefix_install(
    smo_latches: &mut super::smo_latch::SmoLatchSet<'_>,
    leaf_page: u32,
    key: &[u8],
    start: &[u8],
    end: &[u8],
) -> Result<()> {
    let scan_pages = {
        let page = smo_latches.page_mut(leaf_page).ok_or_else(|| {
            Error::Internal(format!(
                "missing US-011 unique target latch for page {leaf_page}"
            ))
        })?;
        let mut pages = vec![leaf_page];
        let snapshot = page.data_snapshot();
        pages.extend(crate::storage::btree::leaf_unique_prefix_sibling_pages(
            snapshot.as_slice(),
            start,
            end,
        )?);
        pages.sort_unstable();
        pages.dedup();
        pages
    };

    for page_id in scan_pages {
        let page = smo_latches
            .page_mut(page_id)
            .ok_or_else(|| Error::WriteConflict {
                reason: WriteConflictReason::StructuralContention,
            })?;
        if page.has_live_delta_key_in_range(start, end, key)? {
            return Err(Error::WriteConflict {
                reason: WriteConflictReason::UniqueConflict {
                    key_prefix_preview: unique_prefix_preview(start),
                },
            });
        }
        let snapshot = page.data_snapshot();
        if crate::storage::btree::leaf_contains_key_in_range(snapshot.as_slice(), start, end, key)?
        {
            return Err(Error::WriteConflict {
                reason: WriteConflictReason::UniqueConflict {
                    key_prefix_preview: unique_prefix_preview(start),
                },
            });
        }
    }
    Ok(())
}

/// Drain the given `SecIndexWrite` batch into resident secondary-index
/// delta heads.
pub(super) fn install_pending_sec_index(
    shared: &SharedState,
    md: &MetadataState,
    writes: Vec<crate::mvcc::SecIndexWrite>,
    _vis: &WriteVisibility<'_>,
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<Vec<u32>> {
    if writes.is_empty() {
        return Ok(Vec::new());
    }
    use crate::mvcc::{SecIndexOp, Ts, VersionData, VersionEntry, VersionState};
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

    let mut targets = Vec::with_capacity(writes.len());
    for write in &writes {
        let entry = entry_by_id.get(&write.index_id).ok_or_else(|| {
            Error::Internal(format!(
                "pending sec-index write references unknown index_id {}",
                write.index_id
            ))
        })?;
        let unique_prefix_range =
            if entry.unique && matches!(write.op, crate::mvcc::SecIndexOp::Insert { .. }) {
                let directions = index_field_directions(entry);
                Some(compound_prefix_range_excluding_trailing_id(
                    &write.key,
                    &directions,
                )?)
            } else {
                None
            };
        targets.push(SmoWriteTarget {
            root_page: entry.root_page,
            root_level: entry.root_level,
            key: write.key.clone(),
            op: SmoWriteOp::from_secondary(&write.key, &write.op),
            unique_prefix_range,
        });
    }
    let mut smo_latches = acquire_smo_latches(shared, &targets)?;
    let mut installed_pages = Vec::with_capacity(writes.len());

    for (target_idx, write) in writes.into_iter().enumerate() {
        let ident = secondary_tree_ident(shared, write.index_id)?;
        let leaf_page = smo_latches
            .target_leaf(target_idx)
            .ok_or_else(|| Error::Internal("missing US-010 secondary target leaf".into()))?;
        let mut chain_arc = {
            let page = smo_latches.page_mut(leaf_page).ok_or_else(|| {
                Error::Internal(format!(
                    "missing US-010 secondary latch for page {leaf_page}"
                ))
            })?;
            page.get_or_create_chain(&write.key)?
        };
        if classify_delta_install(
            chain_arc.as_ref(),
            write.expected_head,
            InstallConflictScope::Secondary,
            &write.key,
            txn_id,
        )? {
            installed_pages.push(leaf_page);
            continue;
        }
        if let Some((start, end)) = targets[target_idx].unique_prefix_range.as_ref() {
            check_unique_prefix_install(&mut smo_latches, leaf_page, &write.key, start, end)?;
        }
        {
            let chain_mut = Arc::make_mut(&mut chain_arc);
            if let Some(prev_head) = chain_mut.iter_mut().find(|entry| {
                entry.stop_ts == Ts::MAX && !matches!(entry.state, VersionState::Aborted)
            }) {
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
        let page = smo_latches.page_mut(leaf_page).ok_or_else(|| {
            Error::Internal(format!(
                "missing US-010 secondary latch for page {leaf_page}"
            ))
        })?;
        page.put_chain(write.key, chain_arc)?;
        shared.mark_leaf_dirty(ident, leaf_page, DirtyReason::SecondaryWrite);
        installed_pages.push(leaf_page);
    }

    Ok(installed_pages)
}

fn secondary_tree_ident(shared: &SharedState, index_id: i64) -> Result<TreeIdent> {
    let epoch = shared.published.load_full();
    let collection_id = epoch.catalog.index_owner_by_id(index_id).ok_or_else(|| {
        Error::Internal(format!(
            "published catalog missing owner for secondary index_id {}",
            index_id
        ))
    })?;
    Ok(TreeIdent {
        collection_id,
        kind: TreeKind::Secondary { index_id },
    })
}

/// Install staged primary-tree writes as fresh heads on each key's
/// per-leaf version chain.
pub(super) fn install_pending_primary(
    shared: &SharedState,
    md: &MetadataState,
    writes: Vec<crate::mvcc::PrimaryWrite>,
    _vis: &WriteVisibility<'_>,
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<(Vec<u32>, bool)> {
    #[cfg(test)]
    super::unique_constraint_delta::record_install_pending_primary_call();

    if writes.is_empty() {
        return Ok((Vec::new(), false));
    }
    use crate::mvcc::{PrimaryOp, Ts, VersionData, VersionEntry, VersionState};

    let mut targets = Vec::with_capacity(writes.len());
    for write in &writes {
        let coll = match catalog_lock(md).get_collection(&write.ns)? {
            Some(c) => c,
            None => continue,
        };
        targets.push(SmoWriteTarget {
            root_page: coll.data_root_page,
            root_level: coll.data_root_level,
            key: write.key.clone(),
            op: SmoWriteOp::from_primary(&write.key, &write.op),
            unique_prefix_range: None,
        });
    }
    let mut smo_latches = acquire_smo_latches(shared, &targets)?;
    let mut installed_pages = Vec::with_capacity(writes.len());
    let mut structural_tree_change = false;

    let mut target_idx = 0usize;
    for write in writes {
        let coll = match catalog_lock(md).get_collection(&write.ns)? {
            Some(c) => c,
            None => continue,
        };
        let leaf_page = smo_latches
            .target_leaf(target_idx)
            .ok_or_else(|| Error::Internal("missing US-010 primary target leaf".into()))?;
        target_idx += 1;
        let page = smo_latches.page_mut(leaf_page).ok_or_else(|| {
            Error::Internal(format!("missing US-010 primary latch for page {leaf_page}"))
        })?;
        let mut chain_arc = page.get_or_create_chain(&write.key)?;
        if classify_delta_install(
            chain_arc.as_ref(),
            write.expected_head,
            InstallConflictScope::Primary,
            &write.key,
            txn_id,
        )? {
            installed_pages.push(leaf_page);
            continue;
        }
        {
            let chain_mut = std::sync::Arc::make_mut(&mut chain_arc);
            if let Some(prev_head) = chain_mut.iter_mut().find(|entry| {
                entry.stop_ts == Ts::MAX && !matches!(entry.state, VersionState::Aborted)
            }) {
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
        page.put_chain(write.key, chain_arc)?;
        structural_tree_change |= page.live_delta_payload_exceeds_leaf_budget()?;
        shared.mark_leaf_dirty(
            TreeIdent {
                collection_id: coll.id,
                kind: TreeKind::Primary,
            },
            leaf_page,
            DirtyReason::PrimaryWrite,
        );
        installed_pages.push(leaf_page);
    }
    Ok((installed_pages, structural_tree_change))
}

/// Flip pending entries installed by `txn_id` to Committed.
pub(super) fn flip_pending_to_committed_for(
    shared: &SharedState,
    txn_id: u64,
    commit_ts: crate::mvcc::Ts,
    page_ids: &[u32],
) -> Result<()> {
    let mut page_ids = page_ids.to_vec();
    page_ids.sort_unstable();
    page_ids.dedup();
    for page_id in page_ids {
        let mut page = shared.handle.pool().pin_for_write(page_id)?;
        page.flip_pending_for_txn(txn_id, Some(commit_ts))?;
    }
    Ok(())
}

/// Flip all resident pending entries for `txn_id` to Aborted.
pub(super) fn flip_pending_to_aborted_for(shared: &SharedState, txn_id: u64) -> Result<()> {
    for page_id in shared.handle.pool().pages_with_pending_txn(txn_id)? {
        let mut page = shared.handle.pool().pin_for_write(page_id)?;
        page.flip_pending_for_txn(txn_id, None)?;
    }
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
/// must be rebuilt after the structural batch is committed.
pub(super) fn materialize_ready_secondary_deltas_for_checkpoint(
    shared: &SharedState,
    md: &MetadataState,
    batch: &mut StructuralPageBatch,
) -> Result<(bool, HashSet<TreeIdent>, bool)> {
    let entries = {
        let cat = catalog_lock(md);
        let collections = cat.list_collections()?;
        let mut entries = Vec::new();
        for coll in &collections {
            for entry in cat.list_indexes(&coll.name)? {
                if matches!(entry.state, IndexState::Ready) {
                    entries.push((coll.id, entry));
                }
            }
        }
        entries
    };

    if entries.is_empty() {
        return Ok((false, HashSet::new(), false));
    }

    let mut published_catalog_dirty = false;
    let mut materialized_trees = HashSet::new();
    let mut requires_logical_tail = false;
    // §10.19 C-1 / US-037: coherent (epoch, frontier) load so
    // visibility checks through `view.sequencer_frontier()` cannot see
    // the gap between the publisher's two stores.
    let epoch = shared.load_published_coherent();
    let view = crate::mvcc::read_view::ReadView::new_for_epoch(
        epoch,
        0,
        Arc::clone(&shared.publish_sequencer),
    );
    for (collection_id, entry) in entries {
        let ident = TreeIdent {
            collection_id,
            kind: TreeKind::Secondary { index_id: entry.id },
        };
        let read_tree = BTree::open(new_store(shared), entry.root_page, entry.root_level);
        let deltas = read_tree.visible_delta_entries(&view)?;
        if deltas.is_empty() {
            if shared.dirty_leaves.contains_key(&ident) {
                requires_logical_tail = true;
            }
            continue;
        }
        materialized_trees.insert(ident);

        let mut tree = BTree::open(
            new_structural_store(shared, batch),
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

    Ok((
        published_catalog_dirty,
        materialized_trees,
        requires_logical_tail,
    ))
}

/// Fold committed primary resident deltas into the primary B+ tree during
/// checkpoint.
///
/// Ordinary CRUD keeps row bytes out of structural page batches, so a long run
/// of inserts can leave many committed versions resident on a base-empty leaf.
/// A leaf-local reconcile pass cannot split that leaf when the folded image is
/// too large. Checkpoint is a DDL-style materialization boundary, so it may
/// route those logical bytes through a structural batch, let the B+ tree split
/// as needed, and persist any resulting collection-root move before the
/// journal can be considered checkpointed.
pub(super) fn materialize_primary_deltas_for_checkpoint(
    shared: &SharedState,
    md: &MetadataState,
    batch: &mut StructuralPageBatch,
) -> Result<(bool, HashSet<TreeIdent>, bool)> {
    let collections = {
        let cat = catalog_lock(md);
        cat.list_collections()?
    };
    if collections.is_empty() {
        return Ok((false, HashSet::new(), false));
    }

    let epoch = shared.load_published_coherent();
    let view = crate::mvcc::read_view::ReadView::new_for_epoch(
        epoch,
        0,
        Arc::clone(&shared.publish_sequencer),
    );
    let mut published_catalog_dirty = false;
    let mut materialized_trees = HashSet::new();
    let mut requires_logical_tail = false;

    for coll in collections {
        let ident = TreeIdent {
            collection_id: coll.id,
            kind: TreeKind::Primary,
        };
        if !shared.dirty_leaves.contains_key(&ident) {
            continue;
        }

        let read_tree = BTree::open(new_store(shared), coll.data_root_page, coll.data_root_level);
        let deltas = read_tree.visible_delta_entries(&view)?;
        if deltas.is_empty() {
            requires_logical_tail = true;
            continue;
        }
        materialized_trees.insert(ident);

        let mut tree = BTree::open(
            new_structural_store(shared, batch),
            coll.data_root_page,
            coll.data_root_level,
        );
        for (key, value) in deltas {
            apply_primary_checkpoint_delta(&mut tree, &key, value.as_deref())?;
        }
        let new_root = tree.root_page;
        let new_level = tree.root_level;
        drop(tree);

        if new_root != coll.data_root_page || new_level != coll.data_root_level {
            let mut updated = coll;
            updated.data_root_page = new_root;
            updated.data_root_level = new_level;
            if !catalog_lock(md).update_collection(&updated)? {
                return Err(Error::Internal(
                    "checkpoint primary materialization lost collection metadata".into(),
                ));
            }
            published_catalog_dirty = true;
        }
    }

    Ok((
        published_catalog_dirty,
        materialized_trees,
        requires_logical_tail,
    ))
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

fn apply_primary_checkpoint_delta<S: BTreePageStore>(
    tree: &mut BTree<S>,
    key: &[u8],
    value: Option<&[u8]>,
) -> Result<()> {
    match value {
        Some(bytes) => {
            if !tree.replace_existing(key, bytes)? {
                tree.insert(key, bytes)?;
            }
        }
        None => {
            let _ = tree.delete(key)?;
        }
    }
    Ok(())
}

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
        let cat = catalog_lock(&engine.metadata_state);
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
    let catalog_generation_before = engine.shared.published.load_full().catalog_generation;
    let reserved_gen = engine
        .shared
        .next_catalog_gen
        .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
        + 1;
    let slot = engine
        .shared
        .publish_sequencer
        .register_with_oracle(&engine.shared.oracle)?;

    let mut batch = StructuralPageBatch::new(&engine.shared.handle);

    let body = (|| -> Result<()> {
        engine.free_index_pages_exclusive(&mut batch, &target_index)?;
        {
            let mut cat = catalog_lock(&engine.metadata_state);
            let collection = cat
                .get_collection(ns)?
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
        sync_catalog_root_structural(&engine.shared, &engine.metadata_state, &mut batch)?;
        engine.shared.clear_dirty_tree(&TreeIdent {
            collection_id: ns_id,
            kind: TreeKind::Secondary {
                index_id: target_index.id,
            },
        });
        Ok(())
    })();

    if let Err(e) = body {
        let _ = batch.abort(&engine.shared.handle);
        engine.shared.publish_sequencer.mark_aborted(slot);
        return Err(e);
    }

    if let Err(error) = engine.commit_catalog_batch_to_log(
        CatalogCommitKind::IndexDrop,
        catalog_generation_before,
        reserved_gen,
        &slot,
        batch,
    ) {
        if !matches!(error, Error::EngineFatal { .. }) {
            engine.shared.publish_sequencer.mark_aborted(slot);
        }
        return Err(error);
    }

    let dirty = PublishDirty {
        published_catalog_dirty: true,
        catalog_header_dirty: true,
    };
    let shared = Arc::clone(&engine.shared);
    let metadata_state = Arc::clone(&engine.metadata_state);
    let publish_result = engine
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
        Ok(()) => {
            engine.maybe_sync_interval_after_publish()?;
        }
        Err(Error::EngineFatal { reason }) => return Err(Error::EngineFatal { reason }),
        Err(_) => {
            return Err(
                engine.engine_fatal(crate::error::EngineFatalReason::PostDurableDdlPublishFailure)
            );
        }
    }
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
        })
        .collect())
}
