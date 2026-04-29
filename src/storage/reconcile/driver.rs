//! Foreground checkpoint reconcile driver.

use crate::error::{Error, Result};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::ReplaceLeafError;
use crate::storage::history_store::{HistorySpillTxn, HistoryStore};
use crate::storage::paged_engine::PagedEngine;
use crate::storage::reconcile::plan::{TreeIdent, TreeKind};
use crate::storage::reconcile::synth::{synthesize_page, PageSynthesisResult};

/// Per-tree checkpoint reconcile statistics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReconcileTreeStats {
    /// Dirty leaves observed for the tree at reconcile start.
    pub(crate) dirty_leaves: usize,
    /// Leaves whose folded image was installed.
    pub(crate) installed: usize,
    /// Leaves attempted but not installable in this checkpoint pass.
    pub(crate) not_installable: usize,
    /// History-store records staged before successful installs.
    pub(crate) history_spills: usize,
}

enum LeafReconcileOutcome {
    Installed { history_spills: usize },
    NotInstallable,
}

/// Reconcile every dirty leaf currently recorded for one tree identity.
///
/// # Errors
///
/// Returns storage errors from leaf synthesis, history spill persistence, or
/// folded-leaf installation. Non-resident, pinned, or over-budget leaves are
/// counted as not installable and left dirty for a later checkpoint.
pub(crate) fn reconcile_tree_dirty_set<M>(
    engine: &PagedEngine,
    _md: &M,
    ident: TreeIdent,
    checkpoint_ts: crate::mvcc::Ts,
    oldest_required_ts: crate::mvcc::Ts,
) -> Result<ReconcileTreeStats> {
    let dirty_pages = match engine.shared.dirty_leaves.get(&ident) {
        Some(entry) => entry.keys().copied().collect::<Vec<_>>(),
        None => return Ok(ReconcileTreeStats::default()),
    };

    let mut stats = ReconcileTreeStats {
        dirty_leaves: dirty_pages.len(),
        ..ReconcileTreeStats::default()
    };
    let mut installed_pages = Vec::new();

    for page_id in dirty_pages {
        match reconcile_leaf(
            engine,
            ident.clone(),
            page_id,
            checkpoint_ts,
            oldest_required_ts,
        )? {
            LeafReconcileOutcome::Installed { history_spills } => {
                stats.installed += 1;
                stats.history_spills += history_spills;
                installed_pages.push(page_id);
            }
            LeafReconcileOutcome::NotInstallable => {
                stats.not_installable += 1;
            }
        }
    }

    clear_installed_dirty_pages(engine, &ident, &installed_pages);
    Ok(stats)
}

fn reconcile_leaf(
    engine: &PagedEngine,
    ident: TreeIdent,
    page_id: u32,
    checkpoint_ts: crate::mvcc::Ts,
    oldest_required_ts: crate::mvcc::Ts,
) -> Result<LeafReconcileOutcome> {
    let Some(snapshot) = engine
        .shared
        .handle
        .pool()
        .snapshot_leaf_for_reconcile(page_id)?
    else {
        return Ok(LeafReconcileOutcome::NotInstallable);
    };
    let synthesized = match synthesize_page(
        &snapshot.base_image,
        &snapshot.chains,
        checkpoint_ts,
        oldest_required_ts,
        ident.clone(),
    ) {
        Ok(synthesized) => synthesized,
        Err(_) => return Ok(LeafReconcileOutcome::NotInstallable),
    };
    let pin = match engine
        .shared
        .handle
        .pool()
        .pin_leaf_for_reconcile(ident, page_id)
    {
        Ok(pin) => pin,
        Err(err) => return map_replace_error(err),
    };

    let history_spills = synthesized.history_spill.len();
    commit_history_spills(engine, &synthesized)?;
    match engine.shared.handle.pool().replace_leaf_and_chains(
        pin,
        synthesized.new_base,
        synthesized.retained_chains,
    ) {
        Ok(()) => Ok(LeafReconcileOutcome::Installed { history_spills }),
        Err(err) => map_replace_error(err),
    }
}

fn commit_history_spills(engine: &PagedEngine, synthesized: &PageSynthesisResult) -> Result<()> {
    if synthesized.history_spill.is_empty() {
        return Ok(());
    }

    let mut spill_txn = HistorySpillTxn::new();
    for spill in &synthesized.history_spill {
        match &spill.ident.kind {
            TreeKind::Primary => HistoryStore::<BufferPoolPageStore>::spill_primary(
                &mut spill_txn,
                spill.ident.clone(),
                &spill.key,
                &spill.entry,
                spill.counter,
            )?,
            TreeKind::Secondary { .. } => HistoryStore::<BufferPoolPageStore>::spill_sec_index(
                &mut spill_txn,
                spill.ident.clone(),
                &spill.key,
                &spill.entry,
                spill.counter,
            )?,
        }
    }

    let mut history = engine
        .shared
        .history_store
        .lock()
        .map_err(|_| Error::StatePoisoned {
            component: "history_store",
        })?;
    history.commit_spill_txn_durable(spill_txn)
}

fn clear_installed_dirty_pages(engine: &PagedEngine, ident: &TreeIdent, pages: &[u32]) {
    if pages.is_empty() {
        return;
    }

    let mut remove_tree = false;
    if let Some(mut dirty) = engine.shared.dirty_leaves.get_mut(ident) {
        for page in pages {
            dirty.remove(page);
        }
        remove_tree = dirty.is_empty();
    }
    if remove_tree {
        engine.shared.dirty_leaves.remove(ident);
    }
}

fn map_replace_error(err: ReplaceLeafError<'_>) -> Result<LeafReconcileOutcome> {
    match err {
        ReplaceLeafError::NotResident | ReplaceLeafError::FrameCoWRefused(_) => {
            Ok(LeafReconcileOutcome::NotInstallable)
        }
        ReplaceLeafError::NotLeaf => Err(Error::Internal(
            "dirty-leaf reconcile target is not a leaf page".into(),
        )),
    }
}
