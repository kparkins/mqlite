//! LogicalTxnFrame recovery-validation counters (Phase 2 §3.8 / §7) and the
//! durable-commit-envelope append observability signals (US-024).

use std::sync::atomic::{AtomicU64, Ordering};

use super::macros::{define_counter, define_gauge};

// ===========================================================================
// Section 4: Phase 2 §7 — LogicalTxnFrame Pass 2 post-open validation counters
//
// Pass 2 runs inside `SharedState::new` immediately after
// `catalog_open_with_fallback`. For each op in each parsed logical frame
// it resolves `ns_id` / `index_id` against the live catalog and records
// the outcome. Per §3.2 / §3.11 Pass 2 must never mutate durable state —
// these counters are the only observable side-effect.
// ===========================================================================

// logical_txn_pass2_resolved_ops_total (counter)
define_counter!(
    /// Total ops that Pass 2 resolved against the live catalog at open.
    ///
    /// Observation only — Pass 2 runs once during open, ticked per-op.
    LOGICAL_TXN_PASS2_RESOLVED_OPS_TOTAL,
    /// Record one resolved Pass 2 op (ns_id or index_id matched).
    record_logical_txn_pass2_resolved_op,
    /// Snapshot the Pass 2 resolved-ops counter.
    logical_txn_pass2_resolved_ops_snapshot,
    /// Reset the Pass 2 resolved-ops counter.
    reset_logical_txn_pass2_resolved_ops,
);

// logical_txn_pass2_unresolved_ops_total (counter)
define_counter!(
    /// Total ops that Pass 2 could not resolve (ns_id / index_id absent from
    /// the live catalog). Phase 2 treats these as log-and-proceed; Phase 4
    /// promotes to a hard error per §8.13.
    ///
    /// Observation only — ticked per-op during open.
    LOGICAL_TXN_PASS2_UNRESOLVED_OPS_TOTAL,
    /// Record one unresolved Pass 2 op.
    record_logical_txn_pass2_unresolved_op,
    /// Snapshot the Pass 2 unresolved-ops counter.
    logical_txn_pass2_unresolved_ops_snapshot,
    /// Reset the Pass 2 unresolved-ops counter.
    reset_logical_txn_pass2_unresolved_ops,
);

// ===========================================================================
// Section 5: Phase 2 §3.8 — Pass 1 sweep counters (US-014)
//
// Observable proof of the recovery warnings emitted by the orphan-logical
// sweep and the unmatched-ChainCommit detection. Tests use these to
// verify §3.8(b) and case (c) tolerance behavior without depending on
// the optional `tracing` feature.
// ===========================================================================

// logical_txn_pass1_orphan_logical_dropped_total (counter)
define_counter!(
    /// Total logical frames discarded by the Pass 1 orphan-sweep (§3.8(b)).
    /// Each tick corresponds to a logical frame whose commit_ts has no
    /// matching ChainCommit in the same recovery scan.
    LOGICAL_TXN_PASS1_ORPHAN_LOGICAL_DROPPED_TOTAL,
    /// Record one orphan-logical frame dropped by Pass 1.
    record_logical_txn_pass1_orphan_logical_dropped,
    /// Snapshot the orphan-logical-dropped counter.
    logical_txn_pass1_orphan_logical_dropped_snapshot,
    /// Reset the orphan-logical-dropped counter.
    reset_logical_txn_pass1_orphan_logical_dropped,
);

// logical_txn_pass1_unmatched_chain_commit_total (counter)
define_counter!(
    /// Total ChainCommit frames seen during Pass 1 that had no matching
    /// LogicalTxnFrame at the same `commit_ts` (case (c) Phase 2 tolerance,
    /// §3.7 envelope violation; Phase 4 §8.13.3 promotes this to hard error).
    LOGICAL_TXN_PASS1_UNMATCHED_CHAIN_COMMIT_TOTAL,
    /// Record one unmatched ChainCommit (no paired logical frame).
    record_logical_txn_pass1_unmatched_chain_commit,
    /// Snapshot the unmatched-ChainCommit counter.
    logical_txn_pass1_unmatched_chain_commit_snapshot,
    /// Reset the unmatched-ChainCommit counter.
    reset_logical_txn_pass1_unmatched_chain_commit,
);

// logical_txn_pass1_pre_boundary_dropped_total (counter) — §3.11
define_counter!(
    /// Total logical frames discarded by the Pass 1 checkpoint-boundary cull.
    /// Each tick corresponds to a logical frame whose `commit_ts <=` the
    /// recovered page-0 header's `last_checkpoint_ts`.
    LOGICAL_TXN_PASS1_PRE_BOUNDARY_DROPPED_TOTAL,
    /// Record one pre-boundary logical frame dropped by Pass 1.
    record_logical_txn_pass1_pre_boundary_dropped,
    /// Snapshot the pre-boundary-dropped counter.
    logical_txn_pass1_pre_boundary_dropped_snapshot,
    /// Reset the pre-boundary-dropped counter.
    reset_logical_txn_pass1_pre_boundary_dropped,
);

// ===========================================================================
// Section 6: Phase 2 §7 / US-024 observability counters
//
// The Phase 2 append-observability signals complement the Pass 1 / Pass 2
// counters above to satisfy the §7 signal set.
// ===========================================================================

// (1) logical_txn_append_bytes_total — counter

/// Total logical payload bytes appended through the durable commit envelope.
/// Increments by the encoded frame size on every successful append after I/O
/// completes; failures do not tick.
pub static LOGICAL_TXN_APPEND_BYTES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record an append of `n` bytes.
pub fn record_logical_txn_append_bytes(n: u64) {
    LOGICAL_TXN_APPEND_BYTES_TOTAL.fetch_add(n, Ordering::Relaxed);
}

/// Snapshot the append-bytes counter.
pub fn logical_txn_append_bytes_snapshot() -> u64 {
    LOGICAL_TXN_APPEND_BYTES_TOTAL.load(Ordering::Relaxed)
}

/// Reset the append-bytes counter.
pub fn reset_logical_txn_append_bytes() {
    LOGICAL_TXN_APPEND_BYTES_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// (2–4) logical_txn_append_duration_ms_p50/p95/p99 — ring-buffer percentile gauges
//
// Per §7 the three duration signals MAY be implemented as latest-value
// gauges if a full streaming-histogram is out of scope. This module
// keeps a small 64-slot ring of recent samples and computes p50/p95/p99
// in two phases:
//
//   1. `record_logical_txn_append_duration_ms(ms)` runs INSIDE the
//      commit-envelope critical section and performs only one atomic
//      `fetch_add` on the ring index plus one atomic `store` on the
//      slot. O(1).
//   2. The hot writer path calls
//      `record_logical_txn_append_duration_ms_and_maybe_recompute()`, which
//      refreshes p50/p95/p99 only when the 64-slot ring wraps. Tests and
//      maintenance callers can still force an immediate refresh through
//      `recompute_logical_txn_append_percentiles()`.
//
// Lock-free in both phases; uses three independent atomics for the
// gauges so the recompute window can race without locking. Approximate
// — the percentile values lag the true distribution by up to one ring of
// observations, which is acceptable for §7 observability.
// ---------------------------------------------------------------------------

/// Latest p50 of `append_logical_txn` durations in ms.
pub static LOGICAL_TXN_APPEND_DURATION_MS_P50: AtomicU64 = AtomicU64::new(0);
/// Latest p95 of `append_logical_txn` durations in ms.
pub static LOGICAL_TXN_APPEND_DURATION_MS_P95: AtomicU64 = AtomicU64::new(0);
/// Latest p99 of `append_logical_txn` durations in ms.
pub static LOGICAL_TXN_APPEND_DURATION_MS_P99: AtomicU64 = AtomicU64::new(0);

const APPEND_SAMPLE_RING: usize = 64;
#[allow(clippy::declare_interior_mutable_const)]
const ZERO_ATOMIC_U64: AtomicU64 = AtomicU64::new(0);
static APPEND_SAMPLE_RING_BUF: [AtomicU64; APPEND_SAMPLE_RING] =
    [ZERO_ATOMIC_U64; APPEND_SAMPLE_RING];
static APPEND_SAMPLE_RING_INDEX: AtomicU64 = AtomicU64::new(0);

/// Push one logical append-envelope duration sample (in milliseconds) into
/// the ring buffer. Cheap — only an atomic increment + atomic store.
/// Safe to call inside the commit-envelope hot path (§7 guardrail: keep
/// post-append bookkeeping minimal).
///
pub fn record_logical_txn_append_duration_ms(ms: u64) {
    let _ = record_logical_txn_append_duration_ms_sample(ms);
}

fn record_logical_txn_append_duration_ms_sample(ms: u64) -> u64 {
    let idx =
        APPEND_SAMPLE_RING_INDEX.fetch_add(1, Ordering::Relaxed) as usize % APPEND_SAMPLE_RING;
    APPEND_SAMPLE_RING_BUF[idx].store(ms, Ordering::Relaxed);
    idx as u64
}

/// Push one duration sample and refresh percentile gauges when the ring wraps.
///
/// This keeps the single-commit hot path O(1) for 63 out of every 64 samples,
/// while still advancing the exported p50/p95/p99 gauges regularly under
/// sustained write load.
pub fn record_logical_txn_append_duration_ms_and_maybe_recompute(ms: u64) {
    let idx = record_logical_txn_append_duration_ms_sample(ms);
    if idx + 1 == APPEND_SAMPLE_RING as u64 {
        recompute_logical_txn_append_percentiles();
    }
}

/// Recompute the p50/p95/p99 gauges from the ring buffer. Sorts a
/// 64-element u64 array in place — a few microseconds of work that
/// MUST run outside the hot append-envelope work so committers do not pay for
/// percentile maintenance (§7 guardrail / US-024 AC#3).
///
/// Idempotent. Safe to call from any thread; lock-free recompute.
pub fn recompute_logical_txn_append_percentiles() {
    let mut samples: [u64; APPEND_SAMPLE_RING] = [0; APPEND_SAMPLE_RING];
    for (i, slot) in APPEND_SAMPLE_RING_BUF.iter().enumerate() {
        samples[i] = slot.load(Ordering::Relaxed);
    }
    samples.sort_unstable();
    let p50 = samples[APPEND_SAMPLE_RING / 2];
    let p95 = samples[(APPEND_SAMPLE_RING * 95) / 100];
    let p99 = samples[(APPEND_SAMPLE_RING * 99) / 100];
    LOGICAL_TXN_APPEND_DURATION_MS_P50.store(p50, Ordering::Relaxed);
    LOGICAL_TXN_APPEND_DURATION_MS_P95.store(p95, Ordering::Relaxed);
    LOGICAL_TXN_APPEND_DURATION_MS_P99.store(p99, Ordering::Relaxed);
}

/// Snapshot the p50 gauge.
pub fn logical_txn_append_duration_ms_p50_snapshot() -> u64 {
    LOGICAL_TXN_APPEND_DURATION_MS_P50.load(Ordering::Relaxed)
}

/// Snapshot the p95 gauge.
pub fn logical_txn_append_duration_ms_p95_snapshot() -> u64 {
    LOGICAL_TXN_APPEND_DURATION_MS_P95.load(Ordering::Relaxed)
}

/// Snapshot the p99 gauge.
pub fn logical_txn_append_duration_ms_p99_snapshot() -> u64 {
    LOGICAL_TXN_APPEND_DURATION_MS_P99.load(Ordering::Relaxed)
}

/// Reset the p50/p95/p99 gauges and the underlying sample ring.
pub fn reset_logical_txn_append_durations() {
    for slot in APPEND_SAMPLE_RING_BUF.iter() {
        slot.store(0, Ordering::Relaxed);
    }
    APPEND_SAMPLE_RING_INDEX.store(0, Ordering::Relaxed);
    LOGICAL_TXN_APPEND_DURATION_MS_P50.store(0, Ordering::Relaxed);
    LOGICAL_TXN_APPEND_DURATION_MS_P95.store(0, Ordering::Relaxed);
    LOGICAL_TXN_APPEND_DURATION_MS_P99.store(0, Ordering::Relaxed);
}

// (5) parsed_logical_frames_len — gauge (reset per open)
define_gauge!(
    /// Length of the `ParsedLogicalFrames` vector handed off from Pass 1
    /// to Pass 2 at the most recent open. Reset on each open before Pass 1
    /// runs so the gauge reflects the current lifetime, not cumulative.
    PARSED_LOGICAL_FRAMES_LEN,
    /// Set the gauge.
    set_parsed_logical_frames_len(n: u64),
    /// Snapshot the gauge.
    parsed_logical_frames_len_snapshot,
    /// Reset the gauge.
    reset_parsed_logical_frames_len,
);

// (8) logical_txn_recovery_discarded_frames_total — counter
define_counter!(
    /// Total logical frames discarded by recovery for any reason
    /// (orphan-sweep + pre-boundary cull). Sum across the §3.8(b) and
    /// §3.11 paths.
    LOGICAL_TXN_RECOVERY_DISCARDED_FRAMES_TOTAL,
    /// Record one discarded frame.
    record_logical_txn_recovery_discarded_frame,
    /// Snapshot the discarded counter.
    logical_txn_recovery_discarded_frames_snapshot,
    /// Reset the discarded counter.
    reset_logical_txn_recovery_discarded_frames,
);

// (9) logical_txn_torn_frames_total — counter
define_counter!(
    /// Total torn LogicalTxnFrames observed by recovery — frames whose CRC
    /// or structural validation failed mid-scan. Tracks tail corruption
    /// against the §4.6 disposition table.
    LOGICAL_TXN_TORN_FRAMES_TOTAL,
    /// Record one torn frame.
    record_logical_txn_torn_frame,
    /// Snapshot the torn counter.
    logical_txn_torn_frames_snapshot,
    /// Reset the torn counter.
    reset_logical_txn_torn_frames,
);
