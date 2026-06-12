//! Pending-write installation + commit/abort flip machinery (extracted from
//! index_maint.rs).
//!
//! Live commits stage primary and secondary writes as resident delta heads on
//! each key's per-leaf version chain (`install_pending_*`), then flip those
//! heads to Committed (or Aborted) once the commit record is durable
//! (`flip_pending_*`). First-committer-wins conflict classification lives here
//! too (`classify_delta_install`).

use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::{Error, Result, WriteConflictReason};
use crate::keys::{compound_prefix_range_excluding_trailing_id, COMPOUND_SEP};
use crate::storage::reconcile::driver::{DirtyReason, TreeIdent, TreeKind};
use crate::storage::root_snapshot::PublishedEpoch;

use super::smo_latch::{acquire_smo_latches, SmoWriteOp, SmoWriteTarget};
use super::state::{MetadataState, SharedState};
use super::visibility::WriteVisibility;

const KEY_PREVIEW_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InstallConflictScope {
    Primary,
    Secondary,
}

fn live_head(chain: &VecDeque<crate::mvcc::VersionEntry>) -> Option<&crate::mvcc::VersionEntry> {
    chain.iter().find(|entry| entry.is_live_head())
}

fn key_preview(key: &[u8]) -> Vec<u8> {
    key.iter().copied().take(KEY_PREVIEW_BYTES).collect()
}

fn unique_prefix_preview(prefix_start: &[u8]) -> Vec<u8> {
    let prefix = prefix_start
        .strip_suffix(&[COMPOUND_SEP])
        .unwrap_or(prefix_start);
    key_preview(prefix)
}

/// First-committer-wins decision for installing a staged delta head on a
/// key's resident version chain.
///
/// Replaces the old anonymous `Result<bool>` so each call site names the
/// case it handles. Conflicts are still surfaced as `Err` (caller maps to
/// a `WriteConflict`); this enum covers only the two non-error outcomes.
/// A plain `enum` (no payload) keeps classification zero-cost on the
/// commit hot path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConflictClassification {
    /// The live head is already this txn's own `Pending` write: the install
    /// is idempotent, so the caller must skip re-pushing the head.
    AlreadyInstalledBySelf,
    /// First-committer-wins is satisfied (no live head, the expected head
    /// still matches a committed non-tombstone, a primary insert over a
    /// committed tombstone, or a non-conflicting secondary key): the caller
    /// proceeds to push the new head.
    Proceed,
}

fn classify_delta_install(
    chain: &VecDeque<crate::mvcc::VersionEntry>,
    expected_head: Option<crate::mvcc::transaction::ExpectedHead>,
    scope: InstallConflictScope,
    key: &[u8],
    txn_id: u64,
) -> Result<ConflictClassification> {
    let Some(head) = live_head(chain) else {
        return Ok(ConflictClassification::Proceed);
    };

    if matches!(head.state, crate::mvcc::VersionState::Pending { txn_id: id } if id == txn_id) {
        return Ok(ConflictClassification::AlreadyInstalledBySelf);
    }

    match expected_head {
        Some(expected)
            if (crate::mvcc::transaction::ExpectedHead {
                commit_ts: head.start_ts,
                txn_id: head.txn_id,
            }) == expected =>
        {
            if matches!(head.state, crate::mvcc::VersionState::Committed) && !head.is_tombstone {
                Ok(ConflictClassification::Proceed)
            } else {
                Err(Error::WriteConflict {
                    reason: WriteConflictReason::StaleSnapshot,
                })
            }
        }
        Some(_) => Err(Error::WriteConflict {
            reason: WriteConflictReason::StaleSnapshot,
        }),
        None if scope == InstallConflictScope::Primary => {
            if matches!(head.state, crate::mvcc::VersionState::Committed) && head.is_tombstone {
                Ok(ConflictClassification::Proceed)
            } else {
                Err(Error::WriteConflict {
                    reason: WriteConflictReason::SameKeyConflict {
                        key_preview: key_preview(key),
                    },
                })
            }
        }
        None => Ok(ConflictClassification::Proceed),
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

    let unique_conflict = || Error::WriteConflict {
        reason: WriteConflictReason::UniqueConflict {
            key_prefix_preview: unique_prefix_preview(start),
        },
    };

    for page_id in scan_pages {
        let page = smo_latches
            .page_mut(page_id)
            .ok_or_else(|| Error::WriteConflict {
                reason: WriteConflictReason::StructuralContention,
            })?;
        if page.has_live_delta_key_in_range(start, end, key)? {
            return Err(unique_conflict());
        }
        let snapshot = page.data_snapshot();
        if crate::storage::btree::leaf_contains_key_in_range(snapshot.as_slice(), start, end, key)?
        {
            return Err(unique_conflict());
        }
    }
    Ok(())
}

/// Drain the given `SecIndexWrite` batch into resident secondary-index
/// delta heads.
pub(super) fn install_pending_sec_index(
    shared: &SharedState,
    _md: &MetadataState,
    writes: Vec<crate::mvcc::transaction::SecIndexWrite>,
    _vis: &WriteVisibility<'_>,
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<Vec<u32>> {
    if writes.is_empty() {
        return Ok(Vec::new());
    }
    use crate::mvcc::transaction::SecIndexOp;
    use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};

    // Behavior tweak (R7): hoist the published-epoch load out of the
    // per-write loop. `secondary_tree_ident` previously reloaded
    // `shared.published.load_full()` for EVERY document write in the batch;
    // the published catalog is stable across this batch envelope, so load it
    // once here and resolve each index_id's owner against the same epoch's
    // `index_owner_by_id` map. One atomic load per batch instead of one per
    // write.
    let published_epoch = shared.published.load_full();

    let mut targets = Vec::with_capacity(writes.len());
    for write in &writes {
        let unique_prefix_range = match (&write.unique_directions, &write.op) {
            (Some(directions), crate::mvcc::transaction::SecIndexOp::Insert { .. }) => Some(
                compound_prefix_range_excluding_trailing_id(&write.key, directions)?,
            ),
            _ => None,
        };
        targets.push(SmoWriteTarget {
            root_page: write.index_root_page,
            root_level: write.index_root_level,
            key: write.key.clone(),
            op: SmoWriteOp::from_secondary(&write.key, &write.op),
            unique_prefix_range,
        });
    }
    let mut smo_latches = acquire_smo_latches(shared, &targets)?;
    let mut installed_pages = Vec::with_capacity(writes.len());

    for (target_idx, write) in writes.into_iter().enumerate() {
        let ident = secondary_tree_ident(&published_epoch, write.index_id)?;
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
        match classify_delta_install(
            chain_arc.as_ref(),
            write.expected_head,
            InstallConflictScope::Secondary,
            &write.key,
            txn_id,
        )? {
            ConflictClassification::AlreadyInstalledBySelf => {
                installed_pages.push(leaf_page);
                continue;
            }
            ConflictClassification::Proceed => {}
        }
        if let Some((start, end)) = targets[target_idx].unique_prefix_range.as_ref() {
            check_unique_prefix_install(&mut smo_latches, leaf_page, &write.key, start, end)?;
        }
        {
            let chain_mut = Arc::make_mut(&mut chain_arc);
            if let Some(prev_head) = chain_mut.iter_mut().find(|entry| entry.is_live_head()) {
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
        page.with_chain(&write.key, |slot| {
            *slot = Some(chain_arc);
        })?;
        shared.mark_leaf_dirty(ident, leaf_page, DirtyReason::SecondaryWrite);
        installed_pages.push(leaf_page);
    }

    Ok(installed_pages)
}

/// Resolve the [`TreeIdent`] for a secondary index against an already-loaded
/// published epoch.
///
/// R7 hoist: callers load `shared.published.load_full()` once per batch and
/// pass the borrowed epoch in, so this lookup adds no per-write atomic load.
fn secondary_tree_ident(published_epoch: &PublishedEpoch, index_id: i64) -> Result<TreeIdent> {
    let collection_id = published_epoch
        .catalog
        .index_owner_by_id
        .get(&index_id)
        .copied()
        .ok_or_else(|| {
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
    _md: &MetadataState,
    writes: Vec<crate::mvcc::transaction::PrimaryWrite>,
    _vis: &WriteVisibility<'_>,
    commit_ts: crate::mvcc::Ts,
    txn_id: u64,
) -> Result<(Vec<u32>, bool)> {
    #[cfg(test)]
    super::unique_constraint_delta::record_install_pending_primary_call();

    if writes.is_empty() {
        return Ok((Vec::new(), false));
    }
    use crate::mvcc::transaction::PrimaryOp;
    use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};

    let mut targets = Vec::with_capacity(writes.len());
    for write in &writes {
        targets.push(SmoWriteTarget {
            root_page: write.root_page,
            root_level: write.root_level,
            key: write.key.clone(),
            op: SmoWriteOp::from_primary(&write.key, &write.op),
            unique_prefix_range: None,
        });
    }
    let mut smo_latches = acquire_smo_latches(shared, &targets)?;
    let mut installed_pages = Vec::with_capacity(writes.len());
    let mut structural_tree_change = false;

    for (target_idx, write) in writes.into_iter().enumerate() {
        let leaf_page = smo_latches
            .target_leaf(target_idx)
            .ok_or_else(|| Error::Internal("missing US-010 primary target leaf".into()))?;
        let page = smo_latches.page_mut(leaf_page).ok_or_else(|| {
            Error::Internal(format!("missing US-010 primary latch for page {leaf_page}"))
        })?;
        let mut chain_arc = page.get_or_create_chain(&write.key)?;
        match classify_delta_install(
            chain_arc.as_ref(),
            write.expected_head,
            InstallConflictScope::Primary,
            &write.key,
            txn_id,
        )? {
            ConflictClassification::AlreadyInstalledBySelf => {
                installed_pages.push(leaf_page);
                continue;
            }
            ConflictClassification::Proceed => {}
        }
        {
            let chain_mut = std::sync::Arc::make_mut(&mut chain_arc);
            if let Some(prev_head) = chain_mut.iter_mut().find(|entry| entry.is_live_head()) {
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
        // Time the chain install and the leaf-budget check together: both
        // run under the same exclusive page latch, so the held-latch window
        // is what matters for write contention. They share the running-sum
        // delta-byte cache the latch protects — the install updates it and
        // the budget check reads it in O(1) instead of rescanning every
        // chain — so timing them as one section measures the true cost of
        // holding the latch. Counts each per-write iteration once.
        #[cfg(feature = "perf-counters")]
        let _install_start = std::time::Instant::now();
        page.with_chain(&write.key, |slot| {
            *slot = Some(chain_arc);
        })?;
        structural_tree_change |= page.live_delta_payload_exceeds_leaf_budget()?;
        #[cfg(feature = "perf-counters")]
        {
            use std::sync::atomic::Ordering;
            let elapsed_ns = _install_start.elapsed().as_nanos() as u64;
            crate::storage::buffer_pool::chains::INSTALL_HOLD_NS_TOTAL
                .fetch_add(elapsed_ns, Ordering::Relaxed);
            crate::storage::buffer_pool::chains::INSTALL_WRITES.fetch_add(1, Ordering::Relaxed);
        }
        shared.mark_leaf_dirty(
            TreeIdent {
                collection_id: write.ns_id,
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
///
/// **Post-durable contract**: this function runs only AFTER
/// `PagedEngine::wait_for_commit_durability` (in
/// `paged_engine/commit_envelope.rs`) has confirmed the commit record is
/// durable in the WAL. The transaction is therefore already committed for
/// recovery purposes when control reaches here. That is why the flip must
/// never be turned into an abort: recovery will replay this commit from the
/// durable log regardless, so making the resident chain disagree by aborting
/// it would corrupt the in-memory state relative to what the next reopen
/// reconstructs. Any error returned MUST instead be mapped by the caller to
/// `EngineFatal { PostDurablePendingFlipFailure }` (via the
/// `.map_err(|_| self.engine_fatal(...))?` at the Pending-to-Committed flip
/// in `PagedEngine::run_write_commit_envelope`), which fails the whole engine
/// rather than silently diverging from the durable truth.
/// **Do not abort a durably-committed txn from here.**
///
/// Algorithm:
///   1. Pin each page resident across the phase transition (outer
///      `pool.pin()` survives the latch drop between Phase A and Phase B,
///      so eviction cannot remove the frame mid-flip).
///   2. Phase A under SHARED latch: identify the keys with `Pending(txn_id)`
///      via `LatchedPinnedPage::pending_keys_for_txn`, snapshot their
///      `Arc`s via `snapshot_chain_arc`, and locally `Arc::make_mut` +
///      flip each clone (selective CoW — the legacy whole-frame
///      iteration over `frame.deltas.values_mut()` is gone).
///   3. Phase B under EXCLUSIVE latch: verify every prepared
///      `expected_old` is still `Arc::ptr_eq` to the resident chain via
///      `try_swap_chains_if_unchanged`, and atomically install all new
///      `Arc`s. If any one mismatches, retry Phase A with fresh
///      snapshots up to `MAX_FLIP_RETRIES` attempts.
///   4. On exhaustion, return `Err(Error::Internal(...))`. The caller
///      maps to `EngineFatal { PostDurablePendingFlipFailure }`.
///
/// Recovery is unchanged: `recovery_apply::replay_secondary_op` installs
/// `VersionState::Committed` directly from durable WAL frames; the flip
/// is a live-write-path concept that does not exist in recovery. The
/// end-state guarantee (durable commit becomes visible) is preserved.
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
        flip_pending_one_page(shared, page_id, txn_id, Some(commit_ts))?;
    }
    Ok(())
}

/// Flip all resident pending entries for `txn_id` to Aborted.
///
/// Same Phase A/B + bounded retry shape as
/// [`flip_pending_to_committed_for`], but the abort path is **NOT**
/// under the post-durable contract — abort runs on commit-path
/// failure paths before durability, so an `Err` here can be surfaced
/// to the caller as a normal failure (not an `EngineFatal`). Only the
/// per-page algorithm switched.
pub(super) fn flip_pending_to_aborted_for(shared: &SharedState, txn_id: u64) -> Result<()> {
    #[cfg(any(test, feature = "test-hooks"))]
    super::hidden_accessors::fail_abort_flip_if_armed(shared)?;
    for page_id in shared.handle.pool().pages_with_pending_txn(txn_id)? {
        flip_pending_one_page(shared, page_id, txn_id, None)?;
    }
    Ok(())
}

/// Maximum bounded-retry attempts per page before declaring conflict
/// exhaustion. Conflict requires another writer to commit / abort on
/// the same key between Phase A and Phase B, which is bounded by the
/// per-txn pending key set; 3 attempts is comfortably above the
/// expected steady-state retry count and below the engine-poison
/// threshold.
const MAX_FLIP_RETRIES: u32 = 3;

/// Outcome of a single Phase B exclusive-latch swap on one page.
///
/// Names the swap result explicitly so the retry driver reads as a small
/// state machine rather than a nested `match` on `SwapOutcome`. A plain
/// `enum` (no payload, no `Box`/`dyn`) keeps the attempt zero-cost. The
/// "nothing pending" case is handled before Phase B (empty prepared set),
/// so it is not a variant here.
enum FlipAttempt {
    /// Phase B atomically installed all prepared swaps; the page is done.
    Committed,
    /// Phase B saw a resident chain mismatch; retry Phase A with fresh
    /// snapshots.
    Conflict,
}

/// Per-page Phase A/B + bounded-retry driver shared by the commit and
/// abort flip paths. See [`flip_pending_to_committed_for`] for the
/// post-durable contract details.
fn flip_pending_one_page(
    shared: &SharedState,
    page_id: u32,
    txn_id: u64,
    commit_ts: Option<crate::mvcc::Ts>,
) -> Result<()> {
    use crate::storage::buffer_pool::PageSize;

    // Outer pin: keeps the frame resident across the Phase A → Phase B
    // latch transition. The inner `pin_for_read_sized` /
    // `pin_for_write_sized` calls bump pin_count further; this outer
    // pin guarantees that even if the inner pins drop first the frame
    // does not become evictable in between.
    let _outer_pin = shared.handle.pool().pin(page_id, PageSize::Large32k)?;

    #[cfg(feature = "perf-counters")]
    crate::storage::buffer_pool::chains::FLIP_TXN_TOTAL
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    for _attempt in 0..MAX_FLIP_RETRIES {
        // Phase A: snapshot + CoW-flip under a shared latch (dropped
        // before Phase B). Empty prepared set ⇒ nothing to flip.
        let prepared = flip_phase_a_prepare(shared, page_id, txn_id, commit_ts)?;
        if prepared.is_empty() {
            return Ok(());
        }

        // Phase B: atomic two-pass swap under an exclusive latch.
        // `_outer_pin` keeps the frame resident across the handoff.
        match flip_phase_b_swap(shared, page_id, prepared)? {
            FlipAttempt::Committed => return Ok(()),
            FlipAttempt::Conflict => {
                #[cfg(feature = "perf-counters")]
                crate::storage::buffer_pool::chains::FLIP_RETRY_TOTAL
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                // Loop and retry Phase A with fresh snapshots.
            }
        }
    }

    // MAX_FLIP_RETRIES reached without converging. Bump the
    // exhaustion counter for the AC harness, then return the local
    // Internal error. The caller in `PagedEngine::run_write_commit_envelope`
    // (paged_engine/commit_envelope.rs) maps it to
    // EngineFatal { PostDurablePendingFlipFailure } via the existing
    // .map_err(|_| self.engine_fatal(...)) at that site. We do
    // NOT change that mapper. Engine poison is the correct outcome
    // because the txn is already durably committed (commit path) or
    // already on the abort path; retrying forever is worse than
    // poisoning the in-memory engine.
    #[cfg(feature = "perf-counters")]
    crate::storage::buffer_pool::chains::FLIP_RETRY_EXHAUSTED
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Err(Error::Internal(format!(
        "flip_pending_one_page: bounded retry exhausted on page {page_id} after {MAX_FLIP_RETRIES} attempts (txn_id={txn_id})"
    )))
}

/// Phase A under shared latch — identify pending keys, snapshot their
/// Arcs, locally CoW + flip on the clones. The shared latch is dropped
/// when this returns (before Phase B) so concurrent readers can run.
///
/// An empty result means the frame had no pending entries for this txn;
/// the caller treats that as a completed page.
fn flip_phase_a_prepare(
    shared: &SharedState,
    page_id: u32,
    txn_id: u64,
    commit_ts: Option<crate::mvcc::Ts>,
) -> Result<Vec<crate::storage::buffer_pool::PreparedChainSwap>> {
    use crate::storage::buffer_pool::{flip_pending_in_chain, PageSize, PreparedChainSwap};

    let shared_latch = shared
        .handle
        .pool()
        .pin_for_read_sized(page_id, PageSize::Large32k)?;
    let pending_keys = shared_latch.pending_keys_for_txn(txn_id);
    let mut prepared = Vec::with_capacity(pending_keys.len());
    for key in pending_keys {
        let Some(expected_old) = shared_latch.snapshot_chain_arc(&key) else {
            // Cannot happen while we hold the shared latch on
            // this frame, but tolerate it defensively.
            continue;
        };
        let mut new = expected_old.clone();
        let new_chain_mut = std::sync::Arc::make_mut(&mut new);
        flip_pending_in_chain(new_chain_mut, txn_id, commit_ts);
        prepared.push(PreparedChainSwap {
            key,
            new_chain: new,
            expected_old,
        });
    }
    Ok(prepared)
}

/// Phase B under exclusive latch — atomic two-pass swap. Verifies every
/// prepared `expected_old` still matches the resident chain by pointer,
/// then installs all new `Arc`s; on any mismatch the whole batch is left
/// untouched and the caller retries Phase A.
fn flip_phase_b_swap(
    shared: &SharedState,
    page_id: u32,
    prepared: Vec<crate::storage::buffer_pool::PreparedChainSwap>,
) -> Result<FlipAttempt> {
    use crate::storage::buffer_pool::{PageSize, SwapOutcome};

    let mut excl_latch = shared
        .handle
        .pool()
        .pin_for_write_sized(page_id, PageSize::Large32k)?;
    match excl_latch.try_swap_chains_if_unchanged(prepared)? {
        SwapOutcome::Success => Ok(FlipAttempt::Committed),
        SwapOutcome::Conflict => Ok(FlipAttempt::Conflict),
    }
}
