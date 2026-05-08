//! Foreground checkpoint reconcile driver.

use std::collections::BTreeSet;

use crate::error::{CheckpointIncompleteReason, Error, PoolExhaustedReason, Result};
use crate::journal::log_file::PageId;
use crate::journal::{CheckpointBatchId, CheckpointFlushSet};
use crate::mvcc::metrics;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::ReplaceLeafError;
use crate::storage::history_store::{HistorySpillTxn, HistoryStore};
use crate::storage::paged_engine::PagedEngine;
use crate::storage::reconcile::plan::{TreeIdent, TreeKind};
use crate::storage::reconcile::synth::{
    synthesize_page, visible_winners_fit_individual_leaf_pages, NotInstallable, PageSynthesisResult,
};

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

/// Mutation-free classification of checkpoint dirty leaves.
#[derive(Debug)]
pub(crate) struct CheckpointReconcilePlan {
    trees: Vec<CheckpointTreeReconcilePlan>,
}

impl CheckpointReconcilePlan {
    /// Return planned tree batches that are safe to mutate.
    pub(crate) fn trees(&self) -> &[CheckpointTreeReconcilePlan] {
        &self.trees
    }

    /// Build the checkpoint flush set derived from this reconcile plan.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if the derived page ownership is ambiguous.
    #[allow(
        dead_code,
        reason = "US-005 lands the flush-set builder before every checkpoint driver step consumes it"
    )]
    pub(crate) fn checkpoint_flush_set(
        &self,
        batch_id: CheckpointBatchId,
    ) -> Result<CheckpointFlushSet> {
        let mut main_pages = BTreeSet::new();
        let mut excluded_future_dirty_pages = BTreeSet::new();
        for tree in &self.trees {
            main_pages.extend(tree.mutation_ready_pages.iter().copied().map(PageId));
            excluded_future_dirty_pages
                .extend(tree.excluded_future_dirty_pages.iter().copied().map(PageId));
        }
        CheckpointFlushSet::new(
            batch_id,
            main_pages,
            BTreeSet::new(),
            excluded_future_dirty_pages,
        )
    }
}

/// Planned dirty pages for one tree identity.
#[derive(Debug)]
pub(crate) struct CheckpointTreeReconcilePlan {
    ident: TreeIdent,
    mutation_ready_pages: Vec<u32>,
    excluded_future_dirty_pages: Vec<u32>,
}

impl CheckpointTreeReconcilePlan {
    /// Return the tree identity this planned page set belongs to.
    pub(crate) fn ident(&self) -> &TreeIdent {
        &self.ident
    }

    /// Return pages with checkpoint-visible state ready for mutation.
    pub(crate) fn mutation_ready_pages(&self) -> &[u32] {
        &self.mutation_ready_pages
    }

    /// Return dirty pages that only contain changes above the checkpoint
    /// frontier and therefore must remain dirty after this checkpoint.
    pub(crate) fn excluded_future_dirty_pages(&self) -> &[u32] {
        &self.excluded_future_dirty_pages
    }
}

enum LeafReconcileOutcome {
    Installed { history_spills: usize },
    NotInstallable,
}

enum LeafPlanOutcome {
    MutationReady,
    ExcludedFutureDirty,
}

/// Mutation-free checkpoint blocker classes mapped by the checkpoint planner.
#[allow(
    dead_code,
    reason = "US-010 defines the full checkpoint taxonomy before every blocker source is wired"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointPlanBlocker {
    /// Planned in-place install would need forbidden frame CoW.
    FrameCoWRefused,
    /// Planned overflow ownership transfer cannot be made durable.
    OverflowSpillNotWired,
    /// Checkpoint-visible winners exceed one folded page.
    VisibleWinnerExceedsPageBudget,
    /// Retained tombstone predecessors keep the folded page over budget.
    TombstonePredecessorPressure,
    /// Pool pressure blocked mutation-free planning.
    PoolExhausted(PoolExhaustedReason),
    /// History spill would collide with a different existing value.
    HistoryDuplicateConflict,
    /// History spill duplicate counter cannot encode another same-ts entry.
    HistoryDuplicateCapExceeded,
    /// Dirty-leaf reachability needs repair before the frontier can advance.
    ReachabilityRepairRequired,
}

/// Convert a checkpoint planning blocker to its public incomplete reason.
pub(crate) fn checkpoint_reason_for_plan_blocker(
    blocker: CheckpointPlanBlocker,
) -> CheckpointIncompleteReason {
    match blocker {
        CheckpointPlanBlocker::FrameCoWRefused => CheckpointIncompleteReason::FrameCoWRefused,
        CheckpointPlanBlocker::OverflowSpillNotWired => {
            CheckpointIncompleteReason::OverflowSpillNotWired
        }
        CheckpointPlanBlocker::VisibleWinnerExceedsPageBudget => {
            CheckpointIncompleteReason::VisibleWinnerExceedsPageBudget
        }
        CheckpointPlanBlocker::TombstonePredecessorPressure => {
            CheckpointIncompleteReason::TombstonePredecessorPressure
        }
        CheckpointPlanBlocker::PoolExhausted(reason) => {
            CheckpointIncompleteReason::PoolExhausted(reason)
        }
        CheckpointPlanBlocker::HistoryDuplicateConflict => {
            CheckpointIncompleteReason::HistoryDuplicateConflict
        }
        CheckpointPlanBlocker::HistoryDuplicateCapExceeded => {
            CheckpointIncompleteReason::HistoryDuplicateCapExceeded
        }
        CheckpointPlanBlocker::ReachabilityRepairRequired => {
            CheckpointIncompleteReason::ReachabilityRepairRequired
        }
    }
}

/// Build a checkpoint-incomplete error and tick its observability signal.
pub(crate) fn checkpoint_incomplete_error(
    first_blocking_page: u32,
    reason: CheckpointIncompleteReason,
) -> Error {
    metrics::record_checkpoint_frontier_blocked();
    #[cfg(feature = "tracing")]
    tracing::warn!(
        target: "mqlite",
        first_blocking_page,
        reason = %reason,
        "mqlite::checkpoint_frontier_blocked"
    );
    Error::CheckpointIncomplete {
        first_blocking_page,
        reason,
    }
}

/// Build a mutation-free checkpoint reconcile plan for all dirty leaves.
///
/// # Errors
///
/// Returns [`Error::CheckpointIncomplete`] when a checkpoint-visible dirty
/// leaf cannot be included in the checkpoint batch before any reconcile,
/// history-store, allocator, or journal mutation is attempted.
pub(crate) fn build_checkpoint_reconcile_plan<M>(
    engine: &PagedEngine,
    _md: &M,
    checkpoint_ts: crate::mvcc::Ts,
    oldest_required_ts: crate::mvcc::Ts,
) -> Result<CheckpointReconcilePlan> {
    let dirty_trees = engine
        .shared
        .dirty_leaves
        .iter()
        .map(|entry| {
            let mut pages = entry.keys().copied().collect::<Vec<_>>();
            pages.sort_unstable();
            (entry.key().clone(), pages)
        })
        .collect::<Vec<_>>();
    let mut trees = Vec::new();

    for (ident, pages) in dirty_trees {
        let mut mutation_ready_pages = Vec::new();
        let mut excluded_future_dirty_pages = Vec::new();
        for page_id in pages {
            match plan_leaf(
                engine,
                ident.clone(),
                page_id,
                checkpoint_ts,
                oldest_required_ts,
            )? {
                LeafPlanOutcome::MutationReady => mutation_ready_pages.push(page_id),
                LeafPlanOutcome::ExcludedFutureDirty => {
                    excluded_future_dirty_pages.push(page_id);
                }
            }
        }
        if !mutation_ready_pages.is_empty() || !excluded_future_dirty_pages.is_empty() {
            trees.push(CheckpointTreeReconcilePlan {
                ident,
                mutation_ready_pages,
                excluded_future_dirty_pages,
            });
        }
    }

    Ok(CheckpointReconcilePlan { trees })
}

fn plan_leaf(
    engine: &PagedEngine,
    ident: TreeIdent,
    page_id: u32,
    checkpoint_ts: crate::mvcc::Ts,
    oldest_required_ts: crate::mvcc::Ts,
) -> Result<LeafPlanOutcome> {
    let snapshot_result = engine
        .shared
        .handle
        .pool()
        .snapshot_leaf_for_reconcile(page_id);
    let snapshot = match snapshot_result {
        Err(Error::PoolExhausted { reason }) => {
            let reason =
                checkpoint_reason_for_plan_blocker(CheckpointPlanBlocker::PoolExhausted(reason));
            return Err(checkpoint_incomplete_error(page_id, reason));
        }
        Err(err) => return Err(err),
        Ok(snapshot) => snapshot,
    };
    let snapshot = match snapshot {
        Some(snapshot) => snapshot,
        // A stale dirty-map page with no resident frame has no resident chain
        // to reconcile; the durable page image has already left the pool.
        None => return Ok(LeafPlanOutcome::ExcludedFutureDirty),
    };
    if !has_checkpoint_visible_committed_delta(&snapshot.chains, checkpoint_ts) {
        return Ok(LeafPlanOutcome::ExcludedFutureDirty);
    }
    match synthesize_page(
        &snapshot.base_image,
        &snapshot.chains,
        checkpoint_ts,
        oldest_required_ts,
        ident,
    ) {
        Ok(_) => Ok(LeafPlanOutcome::MutationReady),
        Err(NotInstallable::VisibleWinnerExceedsPageBudget) => {
            match visible_winners_fit_individual_leaf_pages(&snapshot.chains, checkpoint_ts) {
                Ok(true) => Ok(LeafPlanOutcome::MutationReady),
                Ok(false) => {
                    let reason = checkpoint_reason_for_plan_blocker(
                        CheckpointPlanBlocker::VisibleWinnerExceedsPageBudget,
                    );
                    Err(checkpoint_incomplete_error(page_id, reason))
                }
                Err(reason) => {
                    let reason =
                        checkpoint_reason_for_plan_blocker(checkpoint_blocker_for_synth(reason));
                    Err(checkpoint_incomplete_error(page_id, reason))
                }
            }
        }
        Err(reason) => {
            let reason = checkpoint_reason_for_plan_blocker(checkpoint_blocker_for_synth(reason));
            Err(checkpoint_incomplete_error(page_id, reason))
        }
    }
}

fn checkpoint_blocker_for_synth(reason: NotInstallable) -> CheckpointPlanBlocker {
    match reason {
        NotInstallable::VisibleWinnerExceedsPageBudget => {
            CheckpointPlanBlocker::VisibleWinnerExceedsPageBudget
        }
        NotInstallable::FoldedLeafExceedsPageByteBudget => {
            CheckpointPlanBlocker::TombstonePredecessorPressure
        }
    }
}

fn has_checkpoint_visible_committed_delta(
    chains: &crate::storage::buffer_pool::RetainedLeafChains,
    checkpoint_ts: crate::mvcc::Ts,
) -> bool {
    chains.values().any(|chain| {
        chain.iter().any(|entry| {
            matches!(entry.state, crate::mvcc::VersionState::Committed)
                && entry.start_ts <= checkpoint_ts
        })
    })
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
    dirty_pages: &[u32],
    checkpoint_ts: crate::mvcc::Ts,
    oldest_required_ts: crate::mvcc::Ts,
    allow_split_materialized_pages: bool,
) -> Result<ReconcileTreeStats> {
    let mut stats = ReconcileTreeStats {
        dirty_leaves: dirty_pages.len(),
        ..ReconcileTreeStats::default()
    };
    let mut installed_pages = Vec::new();

    for &page_id in dirty_pages {
        match reconcile_leaf(
            engine,
            ident.clone(),
            page_id,
            checkpoint_ts,
            oldest_required_ts,
            allow_split_materialized_pages,
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
    allow_split_materialized_pages: bool,
) -> Result<LeafReconcileOutcome> {
    let Some(snapshot) = engine
        .shared
        .handle
        .pool()
        .snapshot_leaf_for_reconcile(page_id)?
    else {
        if allow_split_materialized_pages {
            return Ok(LeafReconcileOutcome::Installed { history_spills: 0 });
        }
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
        Err(NotInstallable::VisibleWinnerExceedsPageBudget) => {
            let split_materialized =
                visible_winners_fit_individual_leaf_pages(&snapshot.chains, checkpoint_ts)
                    .unwrap_or(false);
            if allow_split_materialized_pages && split_materialized {
                return Ok(LeafReconcileOutcome::Installed { history_spills: 0 });
            }
            return Ok(LeafReconcileOutcome::NotInstallable);
        }
        Err(_) => return Ok(LeafReconcileOutcome::NotInstallable),
    };
    let mut latched_pages = match engine
        .shared
        .handle
        .pool()
        .pin_leaf_set_for_reconcile(ident, &[page_id])
    {
        Ok(pages) => pages,
        Err(err) => return map_replace_error(err),
    };
    let Some(page) = latched_pages.first_mut() else {
        return Ok(LeafReconcileOutcome::NotInstallable);
    };

    let history_spills = synthesized.history_spill.len();
    commit_history_spills(engine, &synthesized)?;
    match engine.shared.handle.pool().replace_leaf_and_chains(
        page,
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

fn map_replace_error(err: ReplaceLeafError) -> Result<LeafReconcileOutcome> {
    match err {
        ReplaceLeafError::NotResident => Ok(LeafReconcileOutcome::NotInstallable),
        ReplaceLeafError::NotLeaf => Err(Error::Internal(
            "dirty-leaf reconcile target is not a leaf page".into(),
        )),
    }
}
