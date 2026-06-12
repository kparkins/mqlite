//! Per-write secondary-index maintenance (extracted from index_maint.rs).
//!
//! These free functions apply a single document mutation (insert / delete /
//! update) to every secondary index on a namespace, keeping each index tree's
//! root + multikey metadata in sync with the catalog.

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::mvcc::transaction::WriteTxn;
use crate::storage::btree::{BTree, HistoryProbe};
use crate::storage::catalog::{IndexEntry, IndexState};
use crate::storage::root_snapshot::PublishedIndex;
use crate::storage::secondary_index::{
    update_index_on_delete, update_index_on_insert, update_index_on_update,
};

use super::state::{MetadataState, SharedState};
use super::visibility::WriteVisibility;

/// Apply one document insert to a single secondary index tree.
///
/// Shared core of [`maintain_secondary_on_insert`] and
/// [`maintain_secondary_on_insert_snapshot`]: open the tree at `entry`'s
/// root, run the per-index history probe, and push the insert through
/// `update_index_on_insert`. Returns the resulting B+ tree roots plus the
/// `is_multikey` flag so each caller can apply its own metadata-sync
/// policy (the live path syncs unconditionally; the snapshot path only on
/// the multikey transition). Borrowing `entry` keeps this allocation-free.
fn apply_secondary_insert_to_index(
    shared: &SharedState,
    entry: &IndexEntry,
    doc: &Document,
    doc_id: &Bson,
    vis: &WriteVisibility<'_>,
    txn: &mut WriteTxn,
) -> Result<(u32, u8, bool)> {
    let store = shared.new_btree_store();
    let idx_tree = BTree::open(store, entry.root_page, entry.root_level);
    let history_probe = vis.secondary_history_probe(entry.id);
    let history = Some(&history_probe as &dyn HistoryProbe);
    let is_multikey = update_index_on_insert(
        doc,
        doc_id,
        &idx_tree,
        entry,
        vis.read_view.as_ref(),
        history,
        txn,
    )?;
    Ok((idx_tree.root_page, idx_tree.root_level, is_multikey))
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
    let entries = md.catalog_lock().list_indexes(ns)?;
    for entry in entries {
        let (root_page, root_level, is_multikey) =
            apply_secondary_insert_to_index(shared, &entry, doc, doc_id, vis, txn)?;
        sync_index_entry_metadata(md, &entry, root_page, root_level, is_multikey, txn)?;
    }
    Ok(())
}

/// Maintain secondary indexes from the published catalog snapshot.
///
/// Ordinary inserts only need stable index identity, roots, key pattern, and
/// uniqueness metadata. Those fields are already in the published snapshot, so
/// the hot path can avoid taking the live catalog mutex. The rare multikey
/// metadata transition still falls back to the live catalog entry so it does
/// not clobber non-published counters.
#[allow(
    clippy::too_many_arguments,
    reason = "mirrors maintain_secondary_on_insert while avoiding live catalog reads"
)]
pub(super) fn maintain_secondary_on_insert_snapshot(
    shared: &SharedState,
    md: &MetadataState,
    ns: &str,
    indexes: &[PublishedIndex],
    doc: &Document,
    doc_id: &Bson,
    vis: &WriteVisibility<'_>,
    txn: &mut WriteTxn,
) -> Result<()> {
    for index in indexes {
        let entry = IndexEntry {
            id: index.id,
            name: index.name.clone(),
            collection: ns.to_owned(),
            root_page: index.root_page,
            root_level: index.root_level,
            key_pattern: index.key_pattern.clone(),
            unique: index.unique,
            sparse: index.sparse,
            multikey: false,
            entry_count: 0,
            state: index.state,
        };
        let (root_page, root_level, is_multikey) =
            apply_secondary_insert_to_index(shared, &entry, doc, doc_id, vis, txn)?;
        if is_multikey {
            // Snapshot entries carry no live counters, so the multikey
            // transition re-reads the live catalog entry to avoid
            // clobbering non-published fields.
            let live_entry = md
                .catalog_lock()
                .get_index(ns, &entry.name)?
                .ok_or_else(|| {
                    Error::Internal(format!("index '{}' vanished mid-write", entry.name))
                })?;
            sync_index_entry_metadata(md, &live_entry, root_page, root_level, true, txn)?;
        }
    }
    Ok(())
}

/// Maintain all secondary indexes after a document delete.
pub(super) fn maintain_secondary_on_delete(
    md: &MetadataState,
    ns: &str,
    doc: &Document,
    doc_id: &Bson,
    txn: &mut WriteTxn,
) -> Result<()> {
    let entries = md.catalog_lock().list_indexes(ns)?;
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
    let entries = md.catalog_lock().list_indexes(ns)?;
    for entry in entries {
        let store = shared.new_btree_store();
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
    md.catalog_lock().update_index(&updated)?;

    txn.publish_dirty.mark_header();
    if root_changed && matches!(orig.state, IndexState::Ready) {
        txn.publish_dirty.mark_published();
    }

    Ok(())
}
