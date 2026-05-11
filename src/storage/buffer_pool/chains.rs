//! MVCC per-frame delta-chain helpers on [`BufferPool`].
//!
//! The per-key delta chains live on the 32 KB leaf partition's frames.
//! After PR0.5 the only mutator surface is `with_chain_under_latch` /
//! `with_all_chains_under_latch` (on `BufferPool` and the
//! `BTreePageStore` trait); this module retains only the read-only
//! `chains_empty` inspector used by structural-cleanup guards.
//! Phase 5 reconcile and CRUD callers that already hold a page latch
//! must use [`super::LatchedPinnedPage`] helpers (`with_chain` /
//! `with_all_chains`) instead — resident chain mutation requires
//! `PageLatch::Exclusive`, while snapshots require
//! `LatchedPinnedPage::Shared` and copy/clone only.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::version::{VersionData, VersionEntry, VersionState};
use crate::mvcc::Ts;
use crate::storage::btree::reconcile::{
    CELL_INLINE_LEN_BYTES, CELL_KEY_LEN_BYTES, CELL_OVERFLOW_REF_BYTES, CELL_VALUE_TYPE_BYTES,
    SLOT_POINTER_BYTES,
};
use crate::storage::btree::OVERFLOW_THRESHOLD;

use super::BufferPool;

// ---------------------------------------------------------------------------
// PR1 perf counters (gated by `perf-counters` cargo feature)
//
// Counters are `pub(crate) static` (NOT `pub(super)`) so the read-side
// helpers in `super::metrics_perf` can name them. Visibility per
// `.omc/plans/mwmr-page-latch.md` rev 4 PR1 §AC and team-lead PR1
// numbered reminder #5. Gate via `#[cfg(feature = "perf-counters")]`
// keeps release builds without the feature at zero overhead.
// ---------------------------------------------------------------------------

/// Total number of `flip_pending_to_committed_for` /
/// `flip_pending_to_aborted_for` per-page invocations. Denominator for
/// [`super::metrics_perf::flip_retry_rate`].
#[cfg(feature = "perf-counters")]
pub(crate) static FLIP_TXN_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Total number of bounded-retry attempts that observed an `Arc::ptr_eq`
/// mismatch in Phase B and re-entered Phase A. Numerator for
/// [`super::metrics_perf::flip_retry_rate`].
#[cfg(feature = "perf-counters")]
pub(crate) static FLIP_RETRY_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Total number of pages where the bounded retry budget exhausted (3
/// attempts) without converging. Each increment indicates the engine
/// was poisoned via `EngineFatal { PostDurablePendingFlipFailure }`.
/// Read by [`super::metrics_perf::flip_retry_exhausted_count`]. AC
/// requires `== 0` over the perf workload — any exhaustion = revert.
#[cfg(feature = "perf-counters")]
pub(crate) static FLIP_RETRY_EXHAUSTED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

// ---------------------------------------------------------------------------
// PR2 per-write install critical-section hold-time counters
//
// Two counter pairs measure the PR2 cache replacement at different
// granularities:
//
//   INSTALL_HOLD_NS_TOTAL / INSTALL_WRITES
//     Macro: the per-write SmoLatchSet exclusive-latch critical
//     section in `index_maint.rs` (the `with_chain` install + the
//     `live_delta_payload_exceeds_leaf_budget` check). PR2's
//     structural verdict — must drop >= 30% from PR1 baseline.
//
//   LIVE_DELTA_CHECK_NS_TOTAL / LIVE_DELTA_CHECK_CALLS
//     Micro: just the `live_delta_payload_exceeds_leaf_budget` call.
//     Pre-PR2 was an O(N) walk of `frame.deltas`; post-PR2 is an
//     O(1) Acquire load. Falsifies / confirms the Amdahl theory.
// ---------------------------------------------------------------------------

/// Cumulative wall-clock nanoseconds spent inside the per-write
/// install critical section in `index_maint.rs` (with_chain +
/// live_delta_payload_exceeds_leaf_budget under exclusive latch).
/// Numerator for [`super::metrics_perf::install_phase_b_mean_hold_ns`].
#[cfg(feature = "perf-counters")]
pub(crate) static INSTALL_HOLD_NS_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Cumulative number of per-write install critical-section
/// invocations (one per CRUD write reaching the install loop).
/// Denominator for [`super::metrics_perf::install_phase_b_mean_hold_ns`].
#[cfg(feature = "perf-counters")]
pub(crate) static INSTALL_WRITES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Cumulative wall-clock nanoseconds spent inside
/// `live_delta_payload_exceeds_leaf_budget`. Pre-PR2 this was an
/// O(N) walk; post-PR2 it is an O(1) Acquire load on the running-sum
/// cache. Numerator for
/// [`super::metrics_perf::live_delta_check_mean_hold_ns`].
#[cfg(feature = "perf-counters")]
pub(crate) static LIVE_DELTA_CHECK_NS_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Cumulative number of `live_delta_payload_exceeds_leaf_budget`
/// invocations. Denominator for
/// [`super::metrics_perf::live_delta_check_mean_hold_ns`].
#[cfg(feature = "perf-counters")]
pub(crate) static LIVE_DELTA_CHECK_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Per-chain contribution to the leaf-budget running sum.
///
/// Mirrors the byte formula in the legacy
/// `LatchedPinnedPage::live_delta_payload_exceeds_leaf_budget` scanner
/// EXACTLY. The legacy scanner walked every chain, computed this
/// per-chain value inline, and summed. PR2 promotes the inline
/// formula to this single helper so the running-sum maintenance path
/// (in `with_chain` / `with_all_chains` / `replace_leaf_and_chains` /
/// `reconcile_frame_at`) and any fresh recompute (in the cache
/// invariant test) agree byte-for-byte. The function is the SINGLE
/// SOURCE OF TRUTH for the live-head byte cost — never duplicate it.
///
/// Returns 0 when:
///   - the chain is empty,
///   - the chain has no entry with `stop_ts == MAX` AND
///     `state != Aborted` (no "live head" candidate),
///   - the live head is a tombstone (deletes don't occupy a leaf cell).
///
/// Otherwise returns
/// `SLOT_POINTER_BYTES + CELL_KEY_LEN_BYTES + key.len() +
/// CELL_VALUE_TYPE_BYTES + value_bytes`, where `value_bytes` matches
/// the same dispatch as the legacy scanner:
///   - inline payload <= OVERFLOW_THRESHOLD: CELL_INLINE_LEN_BYTES + bytes.len()
///   - inline payload > OVERFLOW_THRESHOLD:  CELL_OVERFLOW_REF_BYTES
///   - overflow:                              CELL_OVERFLOW_REF_BYTES
pub(crate) fn chain_live_head_bytes(key: &[u8], chain: &VecDeque<VersionEntry>) -> u64 {
    let Some(entry) = chain.iter().find(|entry| {
        entry.stop_ts == Ts::MAX && !matches!(entry.state, VersionState::Aborted)
    }) else {
        return 0;
    };
    if entry.is_tombstone {
        return 0;
    }
    let value_bytes = match &entry.data {
        VersionData::Inline(bytes) if bytes.len() > OVERFLOW_THRESHOLD => CELL_OVERFLOW_REF_BYTES,
        VersionData::Inline(bytes) => CELL_INLINE_LEN_BYTES + bytes.len(),
        VersionData::Overflow(_) => CELL_OVERFLOW_REF_BYTES,
    };
    (SLOT_POINTER_BYTES + CELL_KEY_LEN_BYTES + key.len() + CELL_VALUE_TYPE_BYTES + value_bytes)
        as u64
}

/// Sum [`chain_live_head_bytes`] over every chain in `deltas`. Used by
/// the bulk-mutation paths that cannot incrementally maintain the
/// running sum (`with_all_chains`, `replace_leaf_and_chains`,
/// `reconcile_frame_at`).
pub(crate) fn frame_live_delta_payload_bytes(
    deltas: &std::collections::BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>,
) -> u64 {
    deltas
        .iter()
        .map(|(key, chain)| chain_live_head_bytes(key, chain))
        .sum()
}

impl BufferPool {
    /// True if no delta chains are attached to leaf page `page` (including
    /// the case where the page is not currently resident).
    pub(crate) fn chains_empty(&self, page: u32) -> Result<bool> {
        let guard = self
            .inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let Some(&idx) = guard.page_map.get(&page) else {
            return Ok(true);
        };
        let frame = guard.frames[idx].as_ref().ok_or_else(|| {
            Error::Internal("page_map invariant: frame must exist at mapped slot".into())
        })?;
        Ok(frame.deltas.is_empty())
    }

    /// Test-only accessor: snapshot the running-sum cache for `page`.
    /// Returns `None` when the page is not resident in the 32 KB pool.
    /// Used exclusively by the `running_sum_cache_invariant` test to
    /// compare cached values against fresh recomputes.
    #[cfg(test)]
    pub(crate) fn live_delta_payload_bytes_for_test(&self, page: u32) -> Option<u64> {
        let guard = self.inner_32k.lock().ok()?;
        let &idx = guard.page_map.get(&page)?;
        let frame = guard.frames[idx].as_ref()?;
        Some(
            frame
                .live_delta_payload_bytes
                .load(std::sync::atomic::Ordering::Acquire),
        )
    }

    /// Test-only accessor: fresh recompute of `frame_live_delta_payload_bytes`
    /// for `page`, walking every chain. The 10k stress test compares
    /// this against [`Self::live_delta_payload_bytes_for_test`] after
    /// every mutation; divergence = cache bug.
    #[cfg(test)]
    pub(crate) fn live_delta_payload_bytes_fresh_for_test(&self, page: u32) -> Option<u64> {
        let guard = self.inner_32k.lock().ok()?;
        let &idx = guard.page_map.get(&page)?;
        let frame = guard.frames[idx].as_ref()?;
        Some(frame_live_delta_payload_bytes(&frame.deltas))
    }
}

#[cfg(test)]
#[path = "tests/chains_accessors.rs"]
mod chains_accessors;

#[cfg(test)]
#[path = "tests/chains_latch_invariant.rs"]
mod chains_latch_invariant;

#[cfg(test)]
#[path = "tests/chains_reconcile.rs"]
mod chains_reconcile;

#[cfg(test)]
#[path = "tests/running_sum_cache_invariant.rs"]
mod running_sum_cache_invariant;
