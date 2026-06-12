//! Foreground checkpoint reconcile driver.

/// F1 spill-flush counters. Test plumbing lives in its own file; see the
/// module docs there.
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/spill_flush_observations.rs"]
pub(crate) mod spill_flush_observations;

// QUARANTINED dormant US-005 producer import — see docs/staged-work/us-005-incremental-checkpoint.md
#[cfg(any(test, feature = "us005-incremental-checkpoint"))]
use std::collections::BTreeSet;

use crate::error::{CheckpointIncompleteReason, Error, PoolExhaustedReason, Result};
// QUARANTINED dormant US-005 producer imports — see docs/staged-work/us-005-incremental-checkpoint.md
#[cfg(any(test, feature = "us005-incremental-checkpoint"))]
use crate::journal::wire::PageId;
#[cfg(any(test, feature = "us005-incremental-checkpoint"))]
use crate::journal::{CheckpointBatchId, CheckpointFlushSet};
use crate::mvcc::metrics;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::ReplaceLeafError;
use crate::storage::history_store::{HistorySpillTxn, HistoryStore};
use crate::storage::paged_engine::PagedEngine;
use crate::storage::reconcile::synth::{
    synthesize_page, visible_winners_fit_individual_leaf_pages, NotInstallable, PageSynthesisResult,
};

/// Stable identity for a tree whose leaves may need reconciliation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TreeIdent {
    /// Durable collection identifier that owns the tree.
    pub(crate) collection_id: i64,
    /// Primary or secondary tree discriminator.
    pub(crate) kind: TreeKind,
}

/// Kind of tree represented by a [`TreeIdent`].
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) enum TreeKind {
    /// Primary collection data tree.
    Primary,
    /// Secondary index tree.
    Secondary {
        /// Durable index identifier for the secondary tree.
        index_id: i64,
    },
}

/// Dirty-leaf metadata retained until a checkpoint reconcile pass consumes it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LeafState {
    /// Reason the leaf was marked dirty.
    pub(crate) dirty_reason: DirtyReason,
}

/// Source operation that made a leaf eligible for reconcile planning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DirtyReason {
    /// Primary data tree write.
    PrimaryWrite,
    /// Secondary index tree write.
    SecondaryWrite,
}

/// Per-pass checkpoint reconcile statistics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ReconcileTreeStats {
    /// Dirty leaves observed for the pass at reconcile start.
    pub(crate) dirty_leaves: usize,
    /// Leaves whose folded image was installed.
    pub(crate) installed: usize,
    /// Leaves attempted but not installable in this checkpoint pass.
    pub(crate) not_installable: usize,
    /// History-store records staged before successful installs.
    pub(crate) history_spills: usize,
}

impl ReconcileTreeStats {
    /// Fold another pass's counters into this aggregate (N2d: checkpoint
    /// keeps one aggregate across its spill, relief, and residual passes).
    pub(crate) fn merge(&mut self, other: &ReconcileTreeStats) {
        self.dirty_leaves += other.dirty_leaves;
        self.installed += other.installed;
        self.not_installable += other.not_installable;
        self.history_spills += other.history_spills;
    }
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
    // QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
    // No caller in any config it compiles in (no production driver, no test
    // exercises it directly), so the allow is unconditional within the gate.
    #[cfg(any(test, feature = "us005-incremental-checkpoint"))]
    #[allow(dead_code, reason = "dormant US-005 producer staged ahead of its driver")]
    pub(crate) fn checkpoint_flush_set(
        &self,
        batch_id: CheckpointBatchId,
    ) -> Result<CheckpointFlushSet> {
        let mut main_pages = BTreeSet::new();
        let mut excluded_future_dirty_pages = BTreeSet::new();
        for tree in &self.trees {
            main_pages.extend(tree.mutation_ready_pages.iter().copied().map(PageId));
            main_pages.extend(tree.spill_required_pages.iter().copied().map(PageId));
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
    spill_required_pages: Vec<u32>,
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

    /// Return checkpoint-visible pages whose chains still hold versions a
    /// registered reader at/above `oldest_required_ts` needs (or entries
    /// that must stay resident). These pages must take the reconcile path
    /// — which spills superseded committed versions into the history store
    /// — and must never be blanket chain-cleared by checkpoint
    /// materialization.
    pub(crate) fn spill_required_pages(&self) -> &[u32] {
        &self.spill_required_pages
    }

    /// Return dirty pages that only contain changes above the checkpoint
    /// frontier and therefore must remain dirty after this checkpoint.
    pub(crate) fn excluded_future_dirty_pages(&self) -> &[u32] {
        &self.excluded_future_dirty_pages
    }
}

/// One planned folded-leaf install awaiting the batched history commit.
struct PlannedLeafInstall {
    page_id: u32,
    synthesized: PageSynthesisResult,
}

/// Per-leaf outcome of the synthesis phase of a reconcile pass.
enum LeafInstallPlan {
    /// Install may proceed once the batched history spills are durable.
    Planned(PlannedLeafInstall),
    /// The page already lives durably in split-materialized form.
    AlreadyMaterialized,
    /// The page cannot be installed in this pass; it stays dirty.
    NotInstallable,
}

enum LeafPlanOutcome {
    MutationReady,
    SpillRequired,
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

/// Sentinel `first_blocking_page` for checkpoint-incomplete errors that
/// cannot be attributed to one specific planned dirty leaf (e.g. pool
/// exhaustion inside a catalog or tree walk during checkpoint
/// materialization).
///
/// N3: page id `0` is the live header page, so using it as the
/// "unattributed" marker conflated a real page with the sentinel.
/// `u32::MAX` can never name a planned dirty leaf.
pub(crate) const CHECKPOINT_BLOCKING_PAGE_UNATTRIBUTED: u32 = u32::MAX;

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
pub(crate) fn build_checkpoint_reconcile_plan(
    engine: &PagedEngine,
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
        let mut spill_required_pages = Vec::new();
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
                LeafPlanOutcome::SpillRequired => spill_required_pages.push(page_id),
                LeafPlanOutcome::ExcludedFutureDirty => {
                    excluded_future_dirty_pages.push(page_id);
                }
            }
        }
        if !mutation_ready_pages.is_empty()
            || !spill_required_pages.is_empty()
            || !excluded_future_dirty_pages.is_empty()
        {
            trees.push(CheckpointTreeReconcilePlan {
                ident,
                mutation_ready_pages,
                spill_required_pages,
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
    let snapshot = engine
        .shared
        .handle
        .pool()
        .snapshot_leaf_for_reconcile(page_id)
        .map_err(|err| match err {
            Error::PoolExhausted { reason } => checkpoint_incomplete_error(
                page_id,
                checkpoint_reason_for_plan_blocker(CheckpointPlanBlocker::PoolExhausted(reason)),
            ),
            other => other,
        })?;
    let Some(snapshot) = snapshot else {
        // A stale dirty-map page with no resident frame has no resident chain
        // to reconcile; the durable page image has already left the pool.
        return Ok(LeafPlanOutcome::ExcludedFutureDirty);
    };
    if !snapshot.chains.values().any(|chain| {
        chain.iter().any(|entry| {
            matches!(entry.state, crate::mvcc::VersionState::Committed)
                && entry.start_ts <= checkpoint_ts
        })
    }) {
        return Ok(LeafPlanOutcome::ExcludedFutureDirty);
    }
    let blocker_err =
        |blocker| checkpoint_incomplete_error(page_id, checkpoint_reason_for_plan_blocker(blocker));
    let synth_to_blocker = |reason: NotInstallable| match reason {
        NotInstallable::VisibleWinnerExceedsPageBudget => {
            CheckpointPlanBlocker::VisibleWinnerExceedsPageBudget
        }
        NotInstallable::FoldedLeafExceedsPageByteBudget => {
            CheckpointPlanBlocker::TombstonePredecessorPressure
        }
    };
    match synthesize_page(
        &snapshot.base_image,
        &snapshot.chains,
        checkpoint_ts,
        oldest_required_ts,
        ident,
    ) {
        // BUG-7: a page whose chains still hold superseded committed
        // versions needed by a reader at/above `oldest_required_ts`
        // (non-empty history spill) or entries that must stay resident
        // (non-empty retained chains) cannot be folded-and-cleared by
        // checkpoint materialization; it must take the reconcile path so
        // those versions reach the history store before the chains drop.
        Ok(synthesized)
            if synthesized.history_spill.is_empty() && synthesized.retained_chains.is_empty() =>
        {
            Ok(LeafPlanOutcome::MutationReady)
        }
        Ok(_) => Ok(LeafPlanOutcome::SpillRequired),
        Err(NotInstallable::VisibleWinnerExceedsPageBudget) => {
            match visible_winners_fit_individual_leaf_pages(&snapshot.chains, checkpoint_ts) {
                Ok(true) => Ok(LeafPlanOutcome::MutationReady),
                Ok(false) => Err(blocker_err(
                    CheckpointPlanBlocker::VisibleWinnerExceedsPageBudget,
                )),
                Err(reason) => Err(blocker_err(synth_to_blocker(reason))),
            }
        }
        Err(reason) => Err(blocker_err(synth_to_blocker(reason))),
    }
}

/// Fixed `(tree, page)` work-item chunk size for cross-tree reconcile
/// batching.
///
/// F1/F38: bounds both the number of durable spill flushes
/// (`ceil(total_spill_pages / RECONCILE_CHUNK_PAGES)` across ALL trees)
/// and the peak number of fully synthesized `PageSynthesisResult`s held in
/// memory at once.
const RECONCILE_CHUNK_PAGES: usize = 64;

/// Reconcile every dirty leaf currently recorded for one tree identity.
///
/// Thin single-tree wrapper over [`reconcile_trees_dirty_sets`]; see there
/// for the chunked batching and ordering contract.
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
    reconcile_trees_dirty_sets(
        engine,
        &[(ident, dirty_pages)],
        checkpoint_ts,
        oldest_required_ts,
        allow_split_materialized_pages,
    )
}

/// Reconcile planned dirty leaves for one or more trees in fixed-size
/// `(tree, page)` chunks.
///
/// R7 batched history spills per TREE: one durable
/// `commit_spill_txn_durable` flush per namespace with spills inside the
/// checkpoint writer-exclusion window (F1), and one fully synthesized
/// `PageSynthesisResult` per dirty page held until the tree's installs ran
/// (F38). This driver instead chunks the flattened cross-tree work list:
/// each chunk of at most [`RECONCILE_CHUNK_PAGES`] pages is synthesized
/// (phase 1), committed with ONE durable history flush (phase 2), installed
/// (phase 3), and dropped before the next chunk starts. Total spill flushes
/// become `ceil(total_spill_pages / chunk)` — never more than one per tree
/// for many small trees, bounded for huge trees — and peak synthesis memory
/// is `O(chunk)`. `HistorySpillTxn` carries a per-spill `TreeIdent`, so one
/// transaction spanning trees is structurally identical to the per-tree
/// shape.
///
/// History-before-leaf ordering holds per chunk: a page's spills are staged
/// in the same chunk as its install, and every install in chunk `i` runs
/// strictly after chunk `i`'s durable history commit — no folded leaf is
/// ever installed before its own superseded versions are durable. Widening
/// the snapshot-to-install window across chunk and tree boundaries is safe
/// for the same reason the per-tree widening was: checkpoint admission has
/// drained all writers, and readers never mutate resident chains.
///
/// A failure part-way leaves earlier chunks installed but still recorded in
/// the dirty map (cleanup runs at the end), exactly like a mid-pass failure
/// in the per-tree shape: a retry re-synthesizes those pages and
/// `apply_spill` tolerates identical duplicate history keys.
///
/// # Errors
///
/// Returns storage errors from leaf synthesis, history spill persistence, or
/// folded-leaf installation. Non-resident, pinned, or over-budget leaves are
/// counted as not installable and left dirty for a later checkpoint.
pub(crate) fn reconcile_trees_dirty_sets(
    engine: &PagedEngine,
    work: &[(TreeIdent, &[u32])],
    checkpoint_ts: crate::mvcc::Ts,
    oldest_required_ts: crate::mvcc::Ts,
    allow_split_materialized_pages: bool,
) -> Result<ReconcileTreeStats> {
    let items: Vec<(usize, u32)> = work
        .iter()
        .enumerate()
        .flat_map(|(tree_idx, (_, pages))| pages.iter().map(move |page| (tree_idx, *page)))
        .collect();
    let mut stats = ReconcileTreeStats {
        dirty_leaves: items.len(),
        ..ReconcileTreeStats::default()
    };
    let mut installed_pages: Vec<Vec<u32>> = vec![Vec::new(); work.len()];

    for chunk in items.chunks(RECONCILE_CHUNK_PAGES) {
        // Phase 1: snapshot + synthesize this chunk's leaves, staging every
        // page's history spills into one batched transaction.
        let mut spill_txn = HistorySpillTxn::new();
        let mut planned = Vec::new();
        for &(tree_idx, page_id) in chunk {
            match plan_leaf_install(
                engine,
                work[tree_idx].0.clone(),
                page_id,
                checkpoint_ts,
                oldest_required_ts,
                allow_split_materialized_pages,
            )? {
                LeafInstallPlan::Planned(install) => {
                    stage_history_spills(&mut spill_txn, &install.synthesized)?;
                    planned.push((tree_idx, install));
                }
                LeafInstallPlan::AlreadyMaterialized => {
                    stats.installed += 1;
                    installed_pages[tree_idx].push(page_id);
                }
                LeafInstallPlan::NotInstallable => {
                    stats.not_installable += 1;
                }
            }
        }

        // Phase 2: ONE durable history commit for the whole staged chunk.
        commit_staged_history_spills(engine, spill_txn)?;

        // Phase 3: install this chunk's folded leaves, strictly after the
        // durable history commit above (history-before-leaf).
        for (tree_idx, install) in planned {
            let page_id = install.page_id;
            let history_spills = install.synthesized.history_spill.len();
            if install_planned_leaf(engine, work[tree_idx].0.clone(), install)? {
                stats.installed += 1;
                stats.history_spills += history_spills;
                installed_pages[tree_idx].push(page_id);
            } else {
                stats.not_installable += 1;
            }
        }
        // Chunk memory (synthesized bases, retained chains, spills) drops
        // here before the next chunk is synthesized.
    }

    for ((ident, _), installed) in work.iter().zip(&installed_pages) {
        if installed.is_empty() {
            continue;
        }
        let mut remove_tree = false;
        if let Some(mut dirty) = engine.shared.dirty_leaves.get_mut(ident) {
            for page in installed {
                dirty.remove(page);
            }
            remove_tree = dirty.is_empty();
        }
        if remove_tree {
            engine.shared.dirty_leaves.remove(ident);
        }
    }
    Ok(stats)
}

/// Snapshot and synthesize one dirty leaf without mutating anything.
fn plan_leaf_install(
    engine: &PagedEngine,
    ident: TreeIdent,
    page_id: u32,
    checkpoint_ts: crate::mvcc::Ts,
    oldest_required_ts: crate::mvcc::Ts,
    allow_split_materialized_pages: bool,
) -> Result<LeafInstallPlan> {
    let Some(snapshot) = engine
        .shared
        .handle
        .pool()
        .snapshot_leaf_for_reconcile(page_id)?
    else {
        if allow_split_materialized_pages {
            return Ok(LeafInstallPlan::AlreadyMaterialized);
        }
        return Ok(LeafInstallPlan::NotInstallable);
    };
    match synthesize_page(
        &snapshot.base_image,
        &snapshot.chains,
        checkpoint_ts,
        oldest_required_ts,
        ident,
    ) {
        Ok(synthesized) => Ok(LeafInstallPlan::Planned(PlannedLeafInstall {
            page_id,
            synthesized,
        })),
        Err(NotInstallable::VisibleWinnerExceedsPageBudget) => {
            let split_materialized =
                visible_winners_fit_individual_leaf_pages(&snapshot.chains, checkpoint_ts)
                    .unwrap_or(false);
            if allow_split_materialized_pages && split_materialized {
                Ok(LeafInstallPlan::AlreadyMaterialized)
            } else {
                Ok(LeafInstallPlan::NotInstallable)
            }
        }
        Err(_) => Ok(LeafInstallPlan::NotInstallable),
    }
}

/// Install one planned folded leaf. Returns `false` when the frame is no
/// longer installable (left dirty for a later checkpoint).
///
/// Must only run after the staged history spills covering this page are
/// durable (history-before-leaf ordering).
fn install_planned_leaf(
    engine: &PagedEngine,
    ident: TreeIdent,
    install: PlannedLeafInstall,
) -> Result<bool> {
    let PlannedLeafInstall {
        page_id,
        synthesized,
    } = install;
    let replace_err = |err: ReplaceLeafError| match err {
        ReplaceLeafError::NotResident => Ok(false),
        ReplaceLeafError::NotLeaf => Err(Error::Internal(
            "dirty-leaf reconcile target is not a leaf page".into(),
        )),
    };
    let mut latched_pages = match engine
        .shared
        .handle
        .pool()
        .pin_leaves_for_reconcile(ident, &[page_id])
    {
        Ok(pages) => pages,
        Err(err) => return replace_err(err),
    };
    let Some(page) = latched_pages.first_mut() else {
        return Ok(false);
    };
    match engine.shared.handle.pool().replace_leaf_and_chains(
        page,
        synthesized.new_base,
        synthesized.retained_chains,
    ) {
        Ok(()) => Ok(true),
        Err(err) => replace_err(err),
    }
}

/// Stage one synthesized page's history spills into the shared transaction.
fn stage_history_spills(
    spill_txn: &mut HistorySpillTxn,
    synthesized: &PageSynthesisResult,
) -> Result<()> {
    for spill in &synthesized.history_spill {
        match &spill.ident.kind {
            TreeKind::Primary => HistoryStore::<BufferPoolPageStore>::spill_primary(
                spill_txn,
                spill.ident.clone(),
                &spill.key,
                &spill.entry,
                spill.counter,
            )?,
            TreeKind::Secondary { .. } => HistoryStore::<BufferPoolPageStore>::spill_sec_index(
                spill_txn,
                spill.ident.clone(),
                &spill.key,
                &spill.entry,
                spill.counter,
            )?,
        }
    }
    Ok(())
}

/// Commit the batched history spills durably (no-op when nothing staged).
fn commit_staged_history_spills(engine: &PagedEngine, spill_txn: HistorySpillTxn) -> Result<()> {
    if spill_txn.is_empty() {
        return Ok(());
    }
    #[cfg(any(test, feature = "test-hooks"))]
    spill_flush_observations::record_spill_commit_flush();
    let mut history = engine
        .shared
        .history_store
        .lock()
        .map_err(|_| Error::StatePoisoned {
            component: "history_store",
        })?;
    history.commit_spill_txn_durable(spill_txn)
}
