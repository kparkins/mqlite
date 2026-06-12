//! Checkpoint-time delta materialization (extracted from index_maint.rs).
//!
//! At a checkpoint, committed resident deltas on the primary and Ready
//! secondary trees are folded into base pages so a clean reopen does not
//! depend on still-resident memory or uncheckpointed logical frames. Both
//! materialize fns route the fold through the CHAIN-FREE structural store
//! (`new_structural_store_chain_free`) — reverting to the chain-carrying
//! `new_structural_store` silently reintroduces the O(n²) close-time
//! checkpoint, because each `read_leaf` would deep-clone the leaf's entire
//! resident delta map again.

use std::collections::HashSet;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::storage::btree::{BTree, BTreePageStore};
use crate::storage::catalog::IndexState;
use crate::storage::reconcile::driver::{TreeIdent, TreeKind};
use crate::storage::structural_page_batch::StructuralPageBatch;

use super::state::{MetadataState, SharedState};

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
        let cat = md.catalog_lock();
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
        let read_tree = BTree::open(shared.new_btree_store(), entry.root_page, entry.root_level);
        let deltas = read_tree.visible_delta_entries(&view)?;
        if deltas.is_empty() {
            if shared.dirty_leaves.contains_key(&ident) {
                requires_logical_tail = true;
            }
            continue;
        }
        materialized_trees.insert(ident);

        let mut tree = BTree::open(
            shared.new_structural_store_chain_free(batch),
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
            if !md.catalog_lock().update_index(&updated)? {
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
        let cat = md.catalog_lock();
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

        let read_tree = BTree::open(
            shared.new_btree_store(),
            coll.data_root_page,
            coll.data_root_level,
        );
        let deltas = read_tree.visible_delta_entries(&view)?;
        if deltas.is_empty() {
            requires_logical_tail = true;
            continue;
        }
        materialized_trees.insert(ident);

        let mut tree = BTree::open(
            shared.new_structural_store_chain_free(batch),
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
            if !md.catalog_lock().update_collection(&updated)? {
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
    #[cfg(any(test, feature = "test-hooks"))]
    crate::storage::close_quadratic_probe::record_materialize_delta_ops(1);
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
    #[cfg(any(test, feature = "test-hooks"))]
    crate::storage::close_quadratic_probe::record_materialize_delta_ops(1);
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
