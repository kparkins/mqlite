//! Reopen logical replay for durable logical frames.
//!
//! This module is the recovery-only counterpart to the live write path. It
//! consumes the Phase 2 `ParsedLogicalFrames` hand-off after journal recovery
//! and installs final committed delta entries directly into resident leaf
//! frames.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::journal::wire::{LogicalOp, LogicalOpKind, LogicalTxnFrame};
use crate::journal::ParsedLogicalFrames;
use crate::mvcc::metrics::{
    record_logical_txn_pass2_resolved_op, record_logical_txn_pass2_unresolved_op,
};
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{LatchMode, PageSize};
use crate::storage::catalog::{Catalog, CollectionEntry, IndexEntry};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::reconcile::driver::{DirtyReason, TreeIdent, TreeKind};
use crate::storage::root_snapshot::PublishedEpoch;

use super::publish::build_published_catalog;
use super::state::{MetadataState, SharedState};

/// Apply Phase 2 parsed logical frames as committed recovery deltas.
///
/// The caller must run Pass 2 validation first. This function never consults
/// the journal and never routes through the live writer orchestration.
pub(crate) fn apply_parsed_logical_frames(
    shared: &SharedState,
    md: &MetadataState,
    parsed: &ParsedLogicalFrames,
) -> Result<()> {
    let catalog = md.catalog_lock();
    apply_parsed_logical_frames_locked(shared, &catalog, parsed)
}

/// Check that replay can make every needed delta-bearing frame resident within
/// the configured buffer-pool budget before any replay mutation happens.
pub(crate) fn check_recovery_replay_pool_bound(
    handle: &Arc<BufferPoolHandle>,
    catalog: &Catalog<BufferPoolPageStore>,
    parsed: &ParsedLogicalFrames,
) -> Result<()> {
    let delta_bearing_frames_count = estimate_recovery_replay_pool_usage(handle, catalog, parsed)?;
    let max_pool_bytes = handle.pool().max_pool_bytes();
    let max_leaf_frames_by_bytes = max_pool_bytes / PageSize::Large32k.bytes();
    if delta_bearing_frames_count > max_leaf_frames_by_bytes {
        return Err(Error::RecoveryPoolExhausted);
    }
    Ok(())
}

/// Install the single coherent `PublishedEpoch` for a completed open.
pub(crate) fn install_recovered_published_epoch(
    shared: &SharedState,
    md: &MetadataState,
    recovered_max_commit_ts: Option<Ts>,
) -> Result<()> {
    let visible_ts = recovered_max_commit_ts.unwrap_or_default();
    let catalog = md.catalog_lock();
    let epoch = Arc::new(PublishedEpoch {
        visible_ts,
        catalog: Arc::new(build_published_catalog(&catalog)?),
        catalog_generation: 1,
    });
    shared.published.store(epoch);
    #[cfg(any(test, feature = "test-hooks"))]
    shared
        .test_hooks
        .recovery_open_published_store_count
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

/// Phase 2 §5.2 Pass 2 — validate `ParsedLogicalFrames` against the live
/// catalog without mutating any durable state.
///
/// Per-op resolution taxonomy:
///   - `PrimaryInsert|PrimaryUpdate|PrimaryDelete` → `ns_id` must resolve
///     via `Catalog::find_collection_by_id`; a miss ticks the unresolved
///     counter.
///   - `SecondaryInsert|SecondaryDelete` → `index_id` must resolve via
///     `Catalog::find_index_by_id`; a miss ticks the unresolved counter.
///
/// Per-frame invariant: op ordinals MUST be dense `0..op_count-1` with
/// no gaps or duplicates. A violation is a Phase 2 invariant error
/// (Pass 1 should have already enforced this via the decoder, so
/// reaching this arm implies recovery-plus-catalog corruption).
///
/// Contract: the `&Catalog` receiver is the only durable-state access.
/// No mutation of the catalog tree, buffer pool, journal, HLC oracle,
/// or history store — the only observable side-effect is the Phase 2
/// `logical_txn_pass2_{resolved,unresolved}_ops_total` counters.
pub(super) fn validate_parsed_logical_frames_against_catalog<S>(
    catalog: &Catalog<S>,
    parsed: &ParsedLogicalFrames,
) -> Result<()>
where
    S: crate::storage::btree::BTreePageStore,
{
    for (_offset, frame) in &parsed.frames {
        validate_frame_ordinals_dense(frame)?;
        for op in &frame.ops {
            match &op.kind {
                LogicalOpKind::PrimaryInsert { ns_id, .. }
                | LogicalOpKind::PrimaryUpdate { ns_id, .. }
                | LogicalOpKind::PrimaryDelete { ns_id, .. } => {
                    if catalog.find_collection_by_id(*ns_id)?.is_some() {
                        record_logical_txn_pass2_resolved_op();
                    } else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            target: "mqlite",
                            ns_id = *ns_id,
                            commit_ts = ?frame.commit_ts,
                            "Pass 2: unresolved ns_id (Phase 2 tolerance — log-and-proceed; \
                             Phase 4 §8.13 hard-errors this)"
                        );
                        record_logical_txn_pass2_unresolved_op();
                    }
                }
                LogicalOpKind::SecondaryInsert { index_id, .. }
                | LogicalOpKind::SecondaryDelete { index_id, .. } => {
                    if catalog.find_index_by_id(*index_id)?.is_some() {
                        record_logical_txn_pass2_resolved_op();
                    } else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            target: "mqlite",
                            index_id = *index_id,
                            commit_ts = ?frame.commit_ts,
                            "Pass 2: unresolved index_id (Phase 2 tolerance — \
                             log-and-proceed; Phase 4 §8.13 hard-errors this)"
                        );
                        record_logical_txn_pass2_unresolved_op();
                    }
                }
            }
        }
    }
    Ok(())
}

/// §3.4 invariant: op_ordinal values form a dense sequence
/// `0..ops.len()-1` with no gaps and no duplicates. Pass 1 should
/// already have enforced this via `LogicalTxnFrame::decode`; we re-check
/// here because Pass 2 is the last gate before published-state open.
pub(super) fn validate_frame_ordinals_dense(frame: &LogicalTxnFrame) -> Result<()> {
    let n = frame.ops.len();
    let mut seen = vec![false; n];
    for op in &frame.ops {
        let ord = op.op_ordinal as usize;
        if ord >= n {
            return Err(Error::Internal(format!(
                "Pass 2: op_ordinal {} out of range 0..{} (commit_ts {:?})",
                op.op_ordinal, n, frame.commit_ts
            )));
        }
        if seen[ord] {
            return Err(Error::Internal(format!(
                "Pass 2: duplicate op_ordinal {} (commit_ts {:?})",
                op.op_ordinal, frame.commit_ts
            )));
        }
        seen[ord] = true;
    }
    Ok(())
}

fn apply_parsed_logical_frames_locked(
    shared: &SharedState,
    catalog: &Catalog<BufferPoolPageStore>,
    parsed: &ParsedLogicalFrames,
) -> Result<()> {
    // REPLAY-DISPOSITION:
    // (i) Logical transaction + matching durable chain marker: apply every op
    //     as committed. This covers the §10.16 cut bands where the legacy page
    //     commit may be absent.
    // (ii) Logical transaction without the durable chain marker: Phase 2 Pass 1
    //      has already discarded it as an orphan
    //      (`recovery_discards_logical_without_matching_chain_commit`).
    // (iii) Durable chain marker without a logical transaction: Phase 2 treats
    //       it as a legacy/structural commit and advances the HLC floor without
    //       logical effects (`recovery_tolerates_chain_commit_without_matching_logical`).
    // (iv) Logical transaction + matching durable chain marker but no legacy
    //      commit-frame bytes: apply as committed via (i); §10.16 cut bands
    //      make the logical+chain pair CRUD authority without requiring the
    //      legacy frame. Torn or CRC-invalid logical tails are rejected before
    //      this hand-off (`pass1_torn_logical_frame_halts_scan_at_offset`).
    for (_, frame) in &parsed.frames {
        for op in &frame.ops {
            replay_logical_op(shared, catalog, frame, op)?;
        }
    }
    Ok(())
}

fn replay_logical_op(
    shared: &SharedState,
    catalog: &Catalog<BufferPoolPageStore>,
    frame: &LogicalTxnFrame,
    op: &LogicalOp,
) -> Result<()> {
    match &op.kind {
        LogicalOpKind::PrimaryInsert {
            ns_id,
            key,
            value,
            overflow,
        }
        | LogicalOpKind::PrimaryUpdate {
            ns_id,
            key,
            value,
            overflow,
        } => {
            if overflow.is_some() {
                return Err(Error::Internal(
                    "logical replay overflow payloads are not supported in Phase 3".into(),
                ));
            }
            if let Some(coll) = catalog.find_collection_by_id(*ns_id)? {
                replay_primary_op(
                    shared,
                    &coll,
                    DeltaReplay {
                        key,
                        data: value.clone(),
                        is_tombstone: false,
                        commit_ts: frame.commit_ts,
                        op_ordinal: op.op_ordinal,
                    },
                )?;
            }
        }
        LogicalOpKind::PrimaryDelete { ns_id, key } => {
            if let Some(coll) = catalog.find_collection_by_id(*ns_id)? {
                replay_primary_op(
                    shared,
                    &coll,
                    DeltaReplay {
                        key,
                        data: Vec::new(),
                        is_tombstone: true,
                        commit_ts: frame.commit_ts,
                        op_ordinal: op.op_ordinal,
                    },
                )?;
            }
        }
        LogicalOpKind::SecondaryInsert {
            index_id,
            key,
            id_bytes,
        } => {
            if let Some((coll, index)) = catalog.find_index_by_id(*index_id)? {
                replay_secondary_op(
                    shared,
                    coll.id,
                    &index,
                    DeltaReplay {
                        key,
                        data: id_bytes.clone(),
                        is_tombstone: false,
                        commit_ts: frame.commit_ts,
                        op_ordinal: op.op_ordinal,
                    },
                )?;
            }
        }
        LogicalOpKind::SecondaryDelete { index_id, key } => {
            if let Some((coll, index)) = catalog.find_index_by_id(*index_id)? {
                replay_secondary_op(
                    shared,
                    coll.id,
                    &index,
                    DeltaReplay {
                        key,
                        data: Vec::new(),
                        is_tombstone: true,
                        commit_ts: frame.commit_ts,
                        op_ordinal: op.op_ordinal,
                    },
                )?;
            }
        }
    }
    // UNRESOLVED-ID-POLICY: Pass 2 owns
    // `logical_txn_pass2_unresolved_ops_total` before this applier runs. If a
    // namespace or index is still absent here, the op is skipped and open
    // continues per the Phase 2 fail-open contract.
    Ok(())
}

struct DeltaReplay<'a> {
    key: &'a [u8],
    data: Vec<u8>,
    is_tombstone: bool,
    commit_ts: Ts,
    op_ordinal: u32,
}

fn replay_primary_op(
    shared: &SharedState,
    coll: &CollectionEntry,
    delta: DeltaReplay<'_>,
) -> Result<()> {
    replay_chain_op(
        shared,
        TreeIdent {
            collection_id: coll.id,
            kind: TreeKind::Primary,
        },
        coll.data_root_page,
        coll.data_root_level,
        DirtyReason::PrimaryWrite,
        delta,
    )
}

fn replay_secondary_op(
    shared: &SharedState,
    collection_id: i64,
    index: &IndexEntry,
    delta: DeltaReplay<'_>,
) -> Result<()> {
    replay_chain_op(
        shared,
        TreeIdent {
            collection_id,
            kind: TreeKind::Secondary { index_id: index.id },
        },
        index.root_page,
        index.root_level,
        DirtyReason::SecondaryWrite,
        delta,
    )
}

fn replay_chain_op(
    shared: &SharedState,
    ident: TreeIdent,
    root_page: u32,
    root_level: u8,
    dirty_reason: DirtyReason,
    delta: DeltaReplay<'_>,
) -> Result<()> {
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&shared.handle)),
        root_page,
        root_level,
    );
    let leaf_page = tree.find_leaf(delta.key)?;
    let _pin = shared.handle.fetch_page(leaf_page, PageSize::Large32k)?;
    let recovered_txn_id = u64::from(delta.op_ordinal);
    shared.handle.pool().with_chain_under_latch(
        leaf_page,
        delta.key,
        LatchMode::Exclusive,
        |slot| {
            let mut chain_arc = slot.take().unwrap_or_default();
            let chain_mut = Arc::make_mut(&mut chain_arc);
            let already_replayed = chain_mut.front().is_some_and(|entry| {
                entry.start_ts == delta.commit_ts && entry.txn_id == recovered_txn_id
            });
            if !already_replayed {
                if let Some(prev_head) = chain_mut.front_mut() {
                    prev_head.stop_ts = delta.commit_ts;
                }
                chain_mut.push_front(VersionEntry {
                    start_ts: delta.commit_ts,
                    stop_ts: Ts::MAX,
                    txn_id: recovered_txn_id,
                    state: VersionState::Committed,
                    data: VersionData::Inline(delta.data),
                    is_tombstone: delta.is_tombstone,
                });
            }
            *slot = Some(chain_arc);
        },
    )?;
    shared.mark_leaf_dirty(ident, leaf_page, dirty_reason);
    Ok(())
}

fn estimate_recovery_replay_pool_usage(
    handle: &Arc<BufferPoolHandle>,
    catalog: &Catalog<BufferPoolPageStore>,
    parsed: &ParsedLogicalFrames,
) -> Result<usize> {
    let mut leaf_pages = BTreeSet::new();
    for (_, frame) in &parsed.frames {
        for op in &frame.ops {
            match &op.kind {
                LogicalOpKind::PrimaryInsert { ns_id, key, .. }
                | LogicalOpKind::PrimaryUpdate { ns_id, key, .. }
                | LogicalOpKind::PrimaryDelete { ns_id, key } => {
                    if let Some(coll) = catalog.find_collection_by_id(*ns_id)? {
                        leaf_pages.insert(replay_leaf_page(
                            handle,
                            coll.data_root_page,
                            coll.data_root_level,
                            key,
                        )?);
                    }
                }
                LogicalOpKind::SecondaryInsert { index_id, key, .. }
                | LogicalOpKind::SecondaryDelete { index_id, key } => {
                    if let Some((_coll, index)) = catalog.find_index_by_id(*index_id)? {
                        leaf_pages.insert(replay_leaf_page(
                            handle,
                            index.root_page,
                            index.root_level,
                            key,
                        )?);
                    }
                }
            }
        }
    }
    Ok(leaf_pages.len())
}

fn replay_leaf_page(
    handle: &Arc<BufferPoolHandle>,
    root_page: u32,
    root_level: u8,
    key: &[u8],
) -> Result<u32> {
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(handle)),
        root_page,
        root_level,
    );
    tree.find_leaf(key)
}

#[cfg(test)]
#[path = "tests/recovery_apply.rs"]
mod recovery_apply;
