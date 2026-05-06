//! Reopen logical replay for durable logical frames.
//!
//! This module is the recovery-only counterpart to the live write path. It
//! consumes the Phase 2 `ParsedLogicalFrames` hand-off after journal recovery
//! and installs final committed delta entries directly into resident leaf
//! frames.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::journal::log_file::{LogicalOp, LogicalOpKind, LogicalTxnFrame};
use crate::journal::ParsedLogicalFrames;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::PageSize;
use crate::storage::catalog::{Catalog, CollectionEntry, IndexEntry};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::reconcile::plan::{DirtyReason, TreeIdent, TreeKind};
use crate::storage::root_snapshot::PublishedEpoch;

use super::catalog_ops::catalog_lock;
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
    let catalog = catalog_lock(md);
    apply_parsed_logical_frames_locked(shared, &catalog, parsed)
}

/// Check that replay can make every needed delta-bearing frame resident within
/// the configured buffer-pool budget before any replay mutation happens.
pub(crate) fn check_recovery_replay_pool_bound(
    handle: &Arc<BufferPoolHandle>,
    catalog: &Catalog<BufferPoolPageStore>,
    parsed: &ParsedLogicalFrames,
) -> Result<()> {
    let estimate = estimate_recovery_replay_pool_usage(handle, catalog, parsed)?;
    let max_pool_bytes = handle.pool().max_pool_bytes();
    let max_leaf_frames_by_bytes = max_pool_bytes / PageSize::Large32k.bytes();
    if estimate.delta_bearing_frames_count > max_leaf_frames_by_bytes
        || estimate.byte_budget > max_pool_bytes
    {
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
    let catalog = catalog_lock(md);
    let epoch = Arc::new(PublishedEpoch {
        visible_ts,
        catalog: Arc::new(build_published_catalog(&catalog)?),
        catalog_generation: 1,
    });
    shared.published.store(epoch);
    #[cfg(any(test, feature = "test-hooks"))]
    shared
        .recovery_open_published_store_count
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
    let mut chain_arc = shared
        .handle
        .pool()
        .take_chain(leaf_page, delta.key)?
        .unwrap_or_default();
    {
        let recovered_txn_id = u64::from(delta.op_ordinal);
        let chain_mut = Arc::make_mut(&mut chain_arc);
        if chain_mut.front().is_some_and(|entry| {
            entry.start_ts == delta.commit_ts && entry.txn_id == recovered_txn_id
        }) {
            shared
                .handle
                .pool()
                .put_chain(leaf_page, delta.key.to_vec(), chain_arc)?;
            shared.mark_leaf_dirty(ident, leaf_page, dirty_reason);
            return Ok(());
        }
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
    shared
        .handle
        .pool()
        .put_chain(leaf_page, delta.key.to_vec(), chain_arc)?;
    shared.mark_leaf_dirty(ident, leaf_page, dirty_reason);
    Ok(())
}

#[derive(Debug, Default)]
struct RecoveryReplayPoolEstimate {
    delta_bearing_frames_count: usize,
    byte_budget: usize,
}

fn estimate_recovery_replay_pool_usage(
    handle: &Arc<BufferPoolHandle>,
    catalog: &Catalog<BufferPoolPageStore>,
    parsed: &ParsedLogicalFrames,
) -> Result<RecoveryReplayPoolEstimate> {
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
    let delta_bearing_frames_count = leaf_pages.len();
    Ok(RecoveryReplayPoolEstimate {
        delta_bearing_frames_count,
        byte_budget: delta_bearing_frames_count * PageSize::Large32k.bytes(),
    })
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
#[path = "recovery_apply_tests.rs"]
mod recovery_apply_tests;
