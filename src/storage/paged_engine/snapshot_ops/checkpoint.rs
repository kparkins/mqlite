//! Engine-level checkpoint / lifecycle free functions.
//!
//! WiredTiger semantics: a checkpoint here is the equivalent of
//! `WT_SESSION::checkpoint` — it quiesces structural mutation (closing the
//! checkpoint admission gate), folds committed delta chains into durable base
//! images, advances the durable `checkpoint_applied_lsn`, fsyncs the main
//! file, and writes a `CheckpointBoundary` log record so recovery can prune
//! the journal prefix. The mutation window is split into a recoverable
//! pre-commit stage and a poison-on-failure post-commit stage exactly as
//! WiredTiger separates checkpoint preparation from the atomic metadata
//! commit.

use std::collections::HashSet;
use std::sync::atomic::Ordering;

use crate::error::{CheckpointIncompleteReason, EngineFatalReason, Error, Result};
use crate::journal::wire::{CheckpointBoundaryPayload, LogRecordDraft};
use crate::storage::buffer_pool::LatchMode;
use crate::storage::reconcile::driver::{
    build_checkpoint_reconcile_plan, checkpoint_incomplete_error, reconcile_tree_dirty_set,
    reconcile_trees_dirty_sets, CheckpointReconcilePlan, ReconcileTreeStats, TreeIdent,
    CHECKPOINT_BLOCKING_PAGE_UNATTRIBUTED,
};
use crate::storage::structural_page_batch::StructuralPageBatch;

use super::super::index_maint::{
    materialize_primary_deltas_for_checkpoint, materialize_ready_secondary_deltas_for_checkpoint,
};
use super::super::publish::rebuild_and_publish;
use super::super::publish::PublishDirty;
#[cfg(any(test, feature = "test-hooks"))]
use super::checkpoint_stage_failpoint;

pub(in crate::storage::paged_engine) fn checkpoint(
    engine: &super::super::PagedEngine,
) -> crate::error::Result<()> {
    let _checkpoint_admission = engine
        .shared
        .checkpoint_admission
        .close_and_drain_all(engine.busy_timeout)?;
    let _md_w = engine
        .metadata
        .write()
        .map_err(|_| crate::error::Error::Internal("metadata RwLock poisoned".into()))?;
    let md = &engine.metadata_state;
    let checkpoint_ts = engine.shared.published.load_full().visible_ts;
    let ort = engine
        .shared
        .handle
        .read_view_registry()
        .oldest_required_ts();

    let checkpoint_plan = build_checkpoint_reconcile_plan(engine, checkpoint_ts, ort)?;

    if let Err(failure) =
        checkpoint_after_reconcile_plan(engine, md, checkpoint_ts, ort, checkpoint_plan)
    {
        return Err(match failure {
            CheckpointFailure::Recoverable(err) => recoverable_checkpoint_failure(err),
            CheckpointFailure::PostMutation(err) => poison_checkpoint_post_mutation(engine, err),
        });
    }
    Ok(())
}

/// Failure partition for the two checkpoint mutation windows.
///
/// `Recoverable` failures happened strictly BEFORE the structural batch
/// commit: the batch was aborted (or never committed) and the only
/// completed mutations are semantics-preserving reconcile installs whose
/// history spills were made durable first, so the engine stays consistent
/// and the checkpoint may simply be retried. `PostMutation` failures
/// happened at or after the structural batch commit and keep the existing
/// poison-and-reopen escalation.
enum CheckpointFailure {
    Recoverable(Error),
    PostMutation(Error),
}

/// Pre-mutation checkpoint state staged ahead of the structural batch
/// commit (the first durably visible checkpoint mutation).
struct StagedCheckpointMutation {
    batch: StructuralPageBatch,
    initial_checkpoint_applied_lsn: u64,
    published_catalog_dirty: bool,
    materialized_trees: HashSet<TreeIdent>,
    requires_logical_tail: bool,
    /// Aggregate reconcile counters across the pre-mutation spill and
    /// relief passes (N2d: no pass's stats are silently discarded).
    reconcile_stats: ReconcileTreeStats,
}

/// Mutable bookkeeping threaded through the post-mutation checkpoint phases.
///
/// WiredTiger semantics: a checkpoint either publishes a self-contained
/// durable image (a "named checkpoint" recovery can open directly) or it
/// leaves the logical journal tail as the authoritative redo source for this
/// round. `requires_logical_tail` is that decision bit. It starts from the
/// pre-mutation stage's verdict and is escalated to `true` by any
/// post-mutation phase that observes a page it could not fold into the
/// durable base (excluded future-dirty pages, a residual not-installable
/// reconcile, or a frame still dirty after the materialization flush).
///
/// `reconcile_stats` accumulates the checkpoint-wide reconcile counters
/// (spill + relief + residual passes) so the final tracing record reports the
/// true total instead of any single pass (N2d).
///
/// Replacing the previously free-standing `mut requires_logical_tail` /
/// `mut reconcile_stats` locals with this struct makes the three escalation
/// points explicit and keeps the phase functions from mutating distant
/// locals.
struct CheckpointRound {
    requires_logical_tail: bool,
    reconcile_stats: ReconcileTreeStats,
}

/// Map a recoverable pre-mutation checkpoint failure to its public shape.
///
/// R2: pool saturation reaching checkpoint materialization (a catalog read
/// or the `visible_delta_entries` leaf walk pinning a non-resident page
/// while every eviction candidate carries live committed delta heads) used
/// to escalate to `EngineFatal { CheckpointPostMutationFailure }` — even
/// though `Error::PoolExhausted`'s own guidance tells the operator to run
/// checkpoint for relief. Those failures happen before any structural
/// mutation commits, so they surface as the existing recoverable
/// `CheckpointIncomplete { PoolExhausted }` taxonomy instead.
/// `first_blocking_page` is [`CHECKPOINT_BLOCKING_PAGE_UNATTRIBUTED`]: the
/// failing pin happens inside a tree or catalog walk, not on a specific
/// planned dirty leaf (N3: page id 0 is the real header page, so it cannot
/// double as the "unattributed" marker).
fn recoverable_checkpoint_failure(err: Error) -> Error {
    match err {
        Error::PoolExhausted { reason } => checkpoint_incomplete_error(
            CHECKPOINT_BLOCKING_PAGE_UNATTRIBUTED,
            CheckpointIncompleteReason::PoolExhausted(reason),
        ),
        other => other,
    }
}

/// Run every checkpoint step that precedes the structural batch commit.
///
/// WiredTiger semantics: this is checkpoint "preparation" — flush-fence
/// bookkeeping plus the in-place delta folds that make the resident tree
/// state synthesizable — all of it ahead of the atomic structural commit.
///
/// Everything in this window is recoverable: LSN stamping is flush-fence
/// bookkeeping, reconcile installs fold committed winners in place only
/// after their history spills are durable (a spill whose install never
/// lands is redundant data history GC reclaims), and a materialization
/// failure aborts the structural batch before anything it staged becomes
/// visible.
fn stage_checkpoint_pre_mutation(
    engine: &super::super::PagedEngine,
    md: &super::super::state::MetadataState,
    checkpoint_ts: crate::mvcc::Ts,
    ort: crate::mvcc::Ts,
    checkpoint_plan: &CheckpointReconcilePlan,
) -> Result<StagedCheckpointMutation> {
    engine
        .shared
        .handle
        .sync_journal_ready_prefix()
        .map_err(|error| engine.poison_after_log_manager_failure(error))?;
    let initial_checkpoint_applied_lsn = engine.shared.handle.current_journal_durable_lsn()?;
    engine
        .shared
        .handle
        .set_main_file_flush_lsn(initial_checkpoint_applied_lsn);
    engine
        .shared
        .handle
        .stamp_unflushable_dirty_pages_lsn(initial_checkpoint_applied_lsn)?;
    // BUG-7: pages whose chains still hold versions a registered ReadView
    // needs (plan-classified as spill-required) take the reconcile path
    // BEFORE materialization. Reconcile spills the superseded committed
    // versions into the history store and installs the folded base in
    // place, so the materialized chain clear below can never destroy a
    // version an in-flight reader still requires. The plan proved these
    // pages synthesizable on this exact resident state, so reconciling
    // them ahead of any structural-batch restructuring is safe.
    //
    // F1: ONE cross-tree chunked reconcile pass instead of one per tree, so
    // the durable spill flushes inside this writer-exclusion window scale
    // with `ceil(spill_pages / chunk)` rather than with the namespace count.
    let spill_work: Vec<(TreeIdent, &[u32])> = checkpoint_plan
        .trees()
        .iter()
        .filter(|tree| !tree.spill_required_pages().is_empty())
        .map(|tree| (tree.ident().clone(), tree.spill_required_pages()))
        .collect();
    let spill_stats = reconcile_trees_dirty_sets(engine, &spill_work, checkpoint_ts, ort, false)?;
    let spill_requires_logical_tail = spill_stats.not_installable > 0;
    #[cfg(any(test, feature = "test-hooks"))]
    checkpoint_stage_failpoint::fail_if_armed()?;
    // R2 relief: the fail-closed eviction guard never evicts a frame
    // carrying live committed delta heads, so a saturated pool would leave
    // the materialization stage below with no evictable frame for any
    // non-resident page it must pin. Fold the plan's mutation-ready pages
    // in place first — `snapshot_leaf_for_reconcile` and
    // `pin_leaves_for_reconcile` operate on resident frames only and
    // mutation-ready pages stage no history spills, so this pass performs
    // no eviction and no extra flush. Pages whose folded image is over
    // budget stay dirty with their chains intact for split
    // materialization.
    let relief_work: Vec<(TreeIdent, &[u32])> = checkpoint_plan
        .trees()
        .iter()
        .filter(|tree| !tree.mutation_ready_pages().is_empty())
        .map(|tree| (tree.ident().clone(), tree.mutation_ready_pages()))
        .collect();
    let relief_stats = reconcile_trees_dirty_sets(engine, &relief_work, checkpoint_ts, ort, false)?;
    let mut reconcile_stats = spill_stats;
    reconcile_stats.merge(&relief_stats);
    // Folded-leaf installs mark their frames unflushable-if-clean; re-stamp
    // at the already-durable checkpoint LSN so those frames become
    // flushable — and therefore evictable — before materialization needs
    // frames.
    engine
        .shared
        .handle
        .stamp_unflushable_dirty_pages_lsn(initial_checkpoint_applied_lsn)?;
    // F0: create the structural batch only now, immediately before the
    // materialize closure. `StructuralPageBatch::new` eagerly drains the
    // allocator's deferred-free queue and the batch becomes the ONLY owner
    // of those pages (`abort` is the sole requeue path; there is no `Drop`
    // impl), so every fallible step above must run before the batch exists
    // — a `?` return that silently dropped the batch leaked the drained
    // pages until reopen. Below this point the only failure exit is the
    // materialize `Err` arm, which aborts the batch. Draining later is
    // equivalent: pages enqueued by the reconcile passes above carry the
    // current checkpoint fence and stay queued either way.
    let mut batch = StructuralPageBatch::new(&engine.shared.handle);
    let materialize_result = (|| -> Result<_> {
        let (primary_catalog_dirty, mut materialized_trees, primary_requires_logical_tail) =
            materialize_primary_deltas_for_checkpoint(&engine.shared, md, &mut batch)?;
        let (
            secondary_catalog_dirty,
            secondary_materialized_trees,
            secondary_requires_logical_tail,
        ) = materialize_ready_secondary_deltas_for_checkpoint(&engine.shared, md, &mut batch)?;
        materialized_trees.extend(secondary_materialized_trees);
        Ok((
            primary_catalog_dirty || secondary_catalog_dirty,
            materialized_trees,
            primary_requires_logical_tail || secondary_requires_logical_tail,
        ))
    })();
    let (published_catalog_dirty, materialized_trees, requires_logical_tail) =
        match materialize_result {
            Ok(result) => result,
            Err(err) => {
                // BUG (chain-migration abort safety): a rebuild `insert` of a
                // delta-only key can split a leaf that still carries live
                // committed chains, and `partition_chains_for_split` migrates
                // those chains onto a batch-allocated page THROUGH the batch
                // store. The migration is not staged copy-on-write, so the
                // abort below frees + invalidates the destination page and the
                // migrated committed chains are lost while the durable base
                // still routes their keys to the (now chain-less) source leaf
                // — silent data loss for committed-but-uncheckpointed versions.
                // Classifying this `Recoverable` is unsound: the live engine
                // keeps running with versions an in-flight reader may still
                // require dropped from memory. Escalate to a poison-and-reopen
                // so recovery rebuilds the lost chains from the journal.
                if batch.migrated_chains() {
                    let _ = batch.abort_after_chain_migration(&engine.shared.handle);
                    // Reuses the post-mutation reason although this site is
                    // pre-publish: both mean "in-memory state was mutated in a
                    // way only a reopen-and-recover can undo", and recovery
                    // treats them identically.
                    let reason = EngineFatalReason::CheckpointPostMutationFailure;
                    engine.shared.poison_engine(reason.clone());
                    return Err(Error::EngineFatal { reason });
                }
                let _ = batch.abort(&engine.shared.handle);
                return Err(err);
            }
        };
    Ok(StagedCheckpointMutation {
        batch,
        initial_checkpoint_applied_lsn,
        published_catalog_dirty,
        materialized_trees,
        requires_logical_tail: requires_logical_tail || spill_requires_logical_tail,
        reconcile_stats,
    })
}

/// Clear the resident delta chains of every materialized tree's
/// mutation-ready pages.
///
/// WiredTiger semantics: once the structural batch commit has made the new
/// base images durable, the in-memory delta updates that fed those images
/// are obsolete and are discarded — the post-commit analogue of WiredTiger
/// evicting the now-reconciled update lists. A page the pre-mutation relief
/// pass already folded (and possibly evicted) is skipped so pinning it back
/// in would not re-introduce pool pressure inside the poisoning window. Any
/// tree carrying excluded future-dirty pages escalates `requires_logical_tail`.
fn clear_materialized_chains(
    engine: &super::super::PagedEngine,
    checkpoint_plan: &CheckpointReconcilePlan,
    materialized_trees: &HashSet<TreeIdent>,
    round: &mut CheckpointRound,
) -> Result<()> {
    for tree in checkpoint_plan.trees() {
        if !materialized_trees.contains(tree.ident()) {
            continue;
        }
        for page_id in tree.mutation_ready_pages() {
            // R2: the relief pass may have folded this page already and
            // removed it from the dirty map; its chains are gone and the
            // frame may even have been evicted under walk pressure.
            // Pinning it back in just to clear nothing would re-introduce
            // pool-pressure failures inside the poisoning window. A page
            // still dirty here kept its chains, so the fail-closed
            // eviction guard kept it resident and this pin is a cache hit.
            let still_dirty = engine
                .shared
                .dirty_leaves
                .get(tree.ident())
                .is_some_and(|dirty| dirty.contains_key(page_id));
            if !still_dirty {
                continue;
            }
            engine.shared.handle.pool().with_all_chains_under_latch(
                *page_id,
                LatchMode::Exclusive,
                |chains| chains.clear(),
            )?;
        }
        engine
            .shared
            .clear_dirty_pages(tree.ident(), tree.mutation_ready_pages());
        if !tree.excluded_future_dirty_pages().is_empty() {
            round.requires_logical_tail = true;
        }
    }
    Ok(())
}

/// Publish the next catalog generation when the checkpoint materialized
/// Ready secondary deltas.
///
/// WiredTiger semantics: materializing Ready secondary indexes is a
/// metadata mutation, so the checkpoint behaves like a DDL publish —
/// reserving the next catalog generation BEFORE publish so readers observe
/// the same ordered identity-advance contract as create/drop_index.
fn publish_if_catalog_dirty(
    engine: &super::super::PagedEngine,
    md: &super::super::state::MetadataState,
    published_catalog_dirty: bool,
) -> Result<()> {
    if published_catalog_dirty {
        let mut dirty = PublishDirty::default();
        dirty.mark_published();
        dirty.mark_header();
        let publish_ts = engine.shared.oracle.commit()?;
        // Phase 5 §10.17.1 / US-006: checkpoint materializes Ready
        // secondary deltas under `metadata.write()` and is a DDL-style
        // publish. Reserve the next catalog generation BEFORE publish so
        // readers observe the same ordered identity advance contract as
        // create/drop_index.
        let reserved_gen = engine
            .shared
            .next_catalog_gen
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        rebuild_and_publish(&engine.shared, md, publish_ts, dirty, Some(reserved_gen))?;
    }
    Ok(())
}

/// Persist the catalog root page/level into the database header.
///
/// WiredTiger semantics: the checkpoint's durable metadata root must point at
/// the catalog tree that the just-published epoch routes to, so recovery and
/// reopen rebuild the same catalog. The backup slot mirrors the primary so a
/// torn header write still recovers a consistent root.
fn update_catalog_root_header(
    engine: &super::super::PagedEngine,
    md: &super::super::state::MetadataState,
) -> Result<()> {
    let (root_page, root_level) = {
        let cat = md.catalog_lock();
        (cat.root_page(), cat.root_level())
    };
    engine.shared.handle.allocator().update_header(|h| {
        h.catalog_root_page = root_page;
        h.catalog_root_level = root_level;
        h.catalog_root_backup = root_page;
    })?;
    Ok(())
}

/// Reconcile the residual dirty pages of trees the materialize stage did not
/// already fold.
///
/// WiredTiger semantics: pages outside the materialize set still need their
/// committed deltas reconciled into durable base images. The pre-mutation
/// relief pass already folded most mutation-ready pages, so only pages still
/// dirty are re-reconciled — an already-folded page that pool pressure
/// evicted afterwards must not be misreported as not-installable. A page that
/// becomes non-installable here keeps the logical journal tail as this
/// round's durable recovery source.
fn reconcile_residual_dirty_trees(
    engine: &super::super::PagedEngine,
    md: &super::super::state::MetadataState,
    checkpoint_plan: &CheckpointReconcilePlan,
    materialized_trees: &HashSet<TreeIdent>,
    checkpoint_ts: crate::mvcc::Ts,
    ort: crate::mvcc::Ts,
    round: &mut CheckpointRound,
) -> Result<()> {
    for tree in checkpoint_plan.trees() {
        if materialized_trees.contains(tree.ident()) {
            continue;
        }
        // R2: the pre-mutation relief pass already folded and un-dirtied
        // most (often all) mutation-ready pages. Only re-reconcile pages
        // still dirty so an already-folded page that pool pressure evicted
        // afterwards is not misreported as not-installable.
        let remaining: Vec<u32> = match engine.shared.dirty_leaves.get(tree.ident()) {
            Some(dirty) => tree
                .mutation_ready_pages()
                .iter()
                .copied()
                .filter(|page| dirty.contains_key(page))
                .collect(),
            None => Vec::new(),
        };
        let stats = reconcile_tree_dirty_set(
            engine,
            md,
            tree.ident().clone(),
            &remaining,
            checkpoint_ts,
            ort,
            materialized_trees.contains(tree.ident()),
        )?;
        round.reconcile_stats.merge(&stats);
        if stats.not_installable > 0 {
            // The pre-mutation plan rejected hard blockers up front. A page
            // that becomes non-installable here keeps the logical journal tail
            // as the durable recovery source for this checkpoint round.
            round.requires_logical_tail = true;
            continue;
        }
        debug_assert!(
            !tree.excluded_future_dirty_pages().is_empty()
                || !tree.mutation_ready_pages().is_empty()
                || !tree.spill_required_pages().is_empty()
        );
    }
    Ok(())
}

/// Run the history-store GC pass and publish the post-checkpoint metric
/// gauges.
///
/// WiredTiger semantics: after reconciliation the history store can discard
/// versions older than the oldest required timestamp (WiredTiger's
/// `oldest_timestamp`-driven history pruning), and the checkpoint reports the
/// current pool/allocator gauges (reader lag, overflow usage, deferred-free
/// depth) for observability.
fn gc_and_publish_metrics(
    engine: &super::super::PagedEngine,
    ort: crate::mvcc::Ts,
) -> Result<()> {
    {
        let mut hs =
            engine
                .shared
                .history_store
                .lock()
                .map_err(|_| crate::error::Error::StatePoisoned {
                    component: "history_store",
                })?;
        hs.gc_pass(ort)?;
    }
    let lag_ms = if ort == crate::mvcc::timestamp::Ts::MAX {
        0
    } else {
        engine
            .shared
            .oracle
            .now()
            .physical_ms
            .saturating_sub(ort.physical_ms)
    };
    crate::mvcc::metrics::set_oldest_required_ts_lag_ms(lag_ms);
    crate::mvcc::metrics::set_overflow_pages_in_use(
        engine.shared.handle.allocator().overflow_pages_in_use() as u64,
    );
    crate::mvcc::metrics::set_deferred_free_queue_depth(
        engine
            .shared
            .handle
            .allocator()
            .page_lifetime_queue()
            .depth() as u64,
    );
    Ok(())
}

/// Sync the journal-ready prefix and stamp dirty pages at the resulting
/// durable LSN, returning the LSN that becomes this checkpoint's
/// `checkpoint_applied_lsn`.
///
/// WiredTiger semantics: this fixes the redo fence — the LSN up to which the
/// just-flushed base images are guaranteed durable. Only invoked when the
/// round is producing a self-contained durable image (the logical-tail path
/// skips it because recovery replays the journal instead).
fn sync_and_stamp_checkpoint_lsn(engine: &super::super::PagedEngine) -> Result<u64> {
    engine
        .shared
        .handle
        .sync_journal_ready_prefix()
        .map_err(|error| engine.poison_after_log_manager_failure(error))?;
    let candidate_lsn = engine.shared.handle.current_journal_durable_lsn()?;
    engine.shared.handle.set_main_file_flush_lsn(candidate_lsn);
    engine
        .shared
        .handle
        .stamp_unflushable_dirty_pages_lsn(candidate_lsn)?;
    Ok(candidate_lsn)
}

fn checkpoint_after_reconcile_plan(
    engine: &super::super::PagedEngine,
    md: &super::super::state::MetadataState,
    checkpoint_ts: crate::mvcc::Ts,
    ort: crate::mvcc::Ts,
    checkpoint_plan: CheckpointReconcilePlan,
) -> std::result::Result<(), CheckpointFailure> {
    let staged = stage_checkpoint_pre_mutation(engine, md, checkpoint_ts, ort, &checkpoint_plan)
        .map_err(CheckpointFailure::Recoverable)?;
    let StagedCheckpointMutation {
        batch,
        initial_checkpoint_applied_lsn,
        published_catalog_dirty,
        materialized_trees,
        requires_logical_tail,
        reconcile_stats,
    } = staged;
    // CheckpointRound carries the two values the post-mutation phases mutate
    // from distant points: the logical-tail decision bit (seeded from the
    // pre-mutation verdict) and the checkpoint-wide reconcile aggregate.
    let mut round = CheckpointRound {
        requires_logical_tail,
        reconcile_stats,
    };
    // Post-mutation window: from the structural batch commit (the first
    // durably visible checkpoint mutation) onward, failures keep the
    // existing poison escalation — the in-memory chain clears below the
    // commit cannot be rolled back.
    let result = (|| -> Result<()> {
        let mut base_store = engine.shared.new_btree_store();
        batch.commit_lsn_fenced(
            &mut base_store,
            &engine.shared.handle,
            initial_checkpoint_applied_lsn,
        )?;
        // Phase: discard the now-durable delta chains of materialized trees.
        clear_materialized_chains(engine, &checkpoint_plan, &materialized_trees, &mut round)?;
        // Phase: DDL-style catalog publish when secondary deltas materialized.
        publish_if_catalog_dirty(engine, md, published_catalog_dirty)?;
        // Phase: persist the catalog root into the durable header.
        update_catalog_root_header(engine, md)?;
        // Phase: reconcile residual dirty pages of non-materialized trees.
        reconcile_residual_dirty_trees(
            engine,
            md,
            &checkpoint_plan,
            &materialized_trees,
            checkpoint_ts,
            ort,
            &mut round,
        )?;
        // Phase: history GC + metric gauges.
        gc_and_publish_metrics(engine, ort)?;

        // Phase: write_boundary_record. The durable boundary sequence stays
        // inline because extracting it would move the materialization-flush
        // and `sync_main_file` ordering relative to the logical-tail early
        // return, the exact ordering recovery depends on. `requires_logical_tail`
        // decides whether this round publishes a self-contained durable image
        // or defers to the logical journal tail.
        let mut checkpoint_applied_lsn = None;
        if !round.requires_logical_tail {
            checkpoint_applied_lsn = Some(sync_and_stamp_checkpoint_lsn(engine)?);
        }
        #[cfg(any(test, feature = "test-hooks"))]
        super::super::hidden_accessors::us026_fail_if_armed(
            &engine.shared,
            super::super::hidden_accessors::Us026PostRegisterFailpoint::Flush,
        )?;
        engine.shared.handle.flush()?;
        if engine.shared.handle.has_dirty_pages()? {
            round.requires_logical_tail = true;
        }
        if round.requires_logical_tail {
            engine
                .shared
                .handle
                .sync_journal_ready_prefix()
                .map_err(|error| engine.poison_after_log_manager_failure(error))?;
            return Ok(());
        }
        #[cfg(any(test, feature = "test-hooks"))]
        super::super::hidden_accessors::checkpoint_boundary_abort_if_armed(
            super::super::hidden_accessors::CheckpointBoundaryFailpoint::AfterMaterializationFlushBeforeBoundary,
        );
        let checkpoint_applied_lsn = checkpoint_applied_lsn.ok_or_else(|| {
            Error::Internal("checkpoint boundary requested without an applied LSN".into())
        })?;
        engine.shared.handle.allocator().update_header(|header| {
            header.last_checkpoint_ts = header.last_checkpoint_ts.max(checkpoint_ts);
            header.checkpoint_applied_lsn = checkpoint_applied_lsn;
        })?;
        engine.shared.handle.flush()?;
        engine.shared.handle.sync_main_file()?;
        let header = engine.shared.handle.allocator().with_header(Clone::clone)?;
        let batch_id = engine.shared.handle.consume_checkpoint_batch_id()?;
        let payload = CheckpointBoundaryPayload {
            checkpoint_applied_lsn: header.checkpoint_applied_lsn,
            batch_id,
            header,
        }
        .encode()?;
        let draft = LogRecordDraft::checkpoint_boundary(0, checkpoint_ts, payload);
        let reserved = engine.shared.handle.reserve_log_record(draft)?;
        let boundary_end_lsn = reserved.end_lsn();
        let written_end_lsn = reserved
            .write_and_mark()
            .map_err(|error| engine.poison_after_log_manager_failure(error))?;
        debug_assert_eq!(written_end_lsn, boundary_end_lsn);
        engine
            .shared
            .handle
            .wait_journal_durable(boundary_end_lsn)
            .map_err(|error| engine.poison_after_log_manager_failure(error))?;
        engine.shared.handle.advance_page_lifetime_checkpoint()?;
        Ok(())
    })();
    // N2d: the checkpoint-wide reconcile aggregate (spill + relief +
    // residual passes) is observable instead of discarded per pass.
    #[cfg(feature = "tracing")]
    tracing::debug!(
        target: "mqlite",
        dirty_leaves = round.reconcile_stats.dirty_leaves,
        installed = round.reconcile_stats.installed,
        not_installable = round.reconcile_stats.not_installable,
        history_spills = round.reconcile_stats.history_spills,
        "mqlite::checkpoint_reconcile_stats"
    );
    result.map_err(CheckpointFailure::PostMutation)
}

fn poison_checkpoint_post_mutation(engine: &super::super::PagedEngine, err: Error) -> Error {
    if matches!(err, Error::EngineFatal { .. }) {
        return err;
    }
    let reason = EngineFatalReason::CheckpointPostMutationFailure;
    engine.shared.poison_engine(reason.clone());
    Error::EngineFatal { reason }
}

#[allow(
    dead_code,
    reason = "FullSync CRUD now syncs through group commit before publish; \
              this helper backs the explicit journal_sync inherent method"
)]
pub(in crate::storage::paged_engine) fn journal_sync(
    engine: &super::super::PagedEngine,
) -> crate::error::Result<()> {
    engine
        .shared
        .handle
        .sync_journal_ready_prefix()
        .map_err(|error| engine.poison_after_log_manager_failure(error))?;
    let durable_lsn = engine.shared.handle.current_journal_durable_lsn()?;
    engine.shared.handle.set_main_file_flush_lsn(durable_lsn);
    engine
        .shared
        .handle
        .stamp_unflushable_dirty_pages_lsn(durable_lsn)
}

pub(in crate::storage::paged_engine) fn snapshot_bytes(
    _engine: &super::super::PagedEngine,
) -> crate::error::Result<Option<Vec<u8>>> {
    Ok(None)
}
