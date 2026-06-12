//! Phase-0 observability counters (P1–P5) and Phase 1 §10.10 publish-decision
//! counters (US-012).
//!
//! Each counter is observation only. Write-side updates are lock-free
//! atomic fetch_add/store calls; reads (`_snapshot` / `_reset`) are
//! test/admin surfaces and must not race with active writers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::macros::{define_batch_counter, define_counter};

// ===========================================================================
// Section 2: Phase-0 observability counters (5 signals, P1–P5)
// ===========================================================================

// P1 — published_snapshot_rebuilds_total  (counter)
define_counter!(
    /// Total number of full published-catalog rebuilds performed via
    /// `publish_commit` (Phase 1 §10.2 / §10.10). One tick per publish
    /// whose `dirty.published_catalog_dirty == true`. Phase 1 US-012 also
    /// exposes this via `published_catalog_rebuild_count`; the two
    /// counters tick in lock-step.
    ///
    /// Observation only — write-side updates are lock-free atomics; snapshot/reset
    /// calls are test/admin surfaces and must not race with active writers.
    PUBLISHED_SNAPSHOT_REBUILDS_TOTAL,
    /// Record one published-snapshot rebuild.
    record_published_snapshot_rebuild,
    /// Snapshot the published-snapshot-rebuilds counter.
    published_snapshot_rebuilds_snapshot,
    /// Reset the published-snapshot-rebuilds counter.
    reset_published_snapshot_rebuilds,
);

// P2a — crud_commits_root_neutral_total  (counter)
define_counter!(
    /// CRUD commits whose body did NOT persist updated tree-root metadata
    /// (no catalog-root header owner update during the txn).
    ///
    /// Observation only — write-side updates are lock-free atomics; snapshot/reset
    /// calls are test/admin surfaces and must not race with active writers.
    CRUD_COMMITS_ROOT_NEUTRAL_TOTAL,
    /// Record one root-neutral CRUD commit.
    record_crud_commit_root_neutral,
    /// Snapshot the root-neutral CRUD-commits counter.
    crud_commits_root_neutral_snapshot,
    /// Reset the root-neutral CRUD-commits counter.
    reset_crud_commits_root_neutral,
);

// P2b — crud_commits_root_changing_total  (counter)
define_counter!(
    /// CRUD commits whose body DID persist updated tree-root metadata (the
    /// catalog-root header owner path fired at least once during the txn).
    ///
    /// Observation only — write-side updates are lock-free atomics; snapshot/reset
    /// calls are test/admin surfaces and must not race with active writers.
    CRUD_COMMITS_ROOT_CHANGING_TOTAL,
    /// Record one root-changing CRUD commit.
    record_crud_commit_root_changing,
    /// Snapshot the root-changing CRUD-commits counter.
    crud_commits_root_changing_snapshot,
    /// Reset the root-changing CRUD-commits counter.
    reset_crud_commits_root_changing,
);

// P3a — lane_wait_ns_total  (counter, cumulative nanoseconds)
define_batch_counter!(
    /// Cumulative nanoseconds CRUD writers spent waiting on write admission.
    /// Timed with `Instant::now()` reads taken OUTSIDE the critical section; the
    /// write-side record call is a lock-free atomic add that does not extend the
    /// critical section.
    ///
    /// Observation only — write-side updates are lock-free atomics; snapshot/reset
    /// calls are test/admin surfaces and must not race with active writers.
    LANE_WAIT_NS_TOTAL,
    /// Record N nanoseconds of lane-acquisition wait.
    record_lane_wait_ns(ns: u64),
    /// Snapshot the lane-wait cumulative-nanoseconds counter.
    lane_wait_ns_snapshot,
    /// Reset the lane-wait counter.
    reset_lane_wait_ns,
);

// ---------------------------------------------------------------------------
// P3b — CRUD commit-envelope stage timings
// ---------------------------------------------------------------------------

/// Stage names for cumulative CRUD commit-envelope timing probes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum CommitEnvelopeStage {
    /// Time spent reserving the Phase 8 log-record byte range.
    LogReserve = 0,
    /// Time spent writing the reserved record and advancing the ready prefix.
    LogWriteReady = 1,
    /// Time spent waiting for the ready prefix in interval/none durability.
    JournalReadyWait = 2,
    /// Time spent waiting for durable fsync completion in FullSync.
    JournalDurableWait = 3,
    /// Time spent flipping Pending entries to Committed.
    PendingFlip = 4,
    /// Time spent in ordered `PublishSequencer::mark_ready`.
    PublishReady = 5,
    /// Time spent syncing the interval-mode ready prefix after publish.
    IntervalSync = 6,
}

const COMMIT_ENVELOPE_STAGE_COUNT: usize = 7;
#[allow(clippy::declare_interior_mutable_const)]
const ZERO_COMMIT_STAGE_ATOMIC_U64: AtomicU64 = AtomicU64::new(0);
static COMMIT_ENVELOPE_STAGE_NS_TOTAL: [AtomicU64; COMMIT_ENVELOPE_STAGE_COUNT] =
    [ZERO_COMMIT_STAGE_ATOMIC_U64; COMMIT_ENVELOPE_STAGE_COUNT];
static COMMIT_ENVELOPE_STAGE_SAMPLES_TOTAL: [AtomicU64; COMMIT_ENVELOPE_STAGE_COUNT] =
    [ZERO_COMMIT_STAGE_ATOMIC_U64; COMMIT_ENVELOPE_STAGE_COUNT];

fn duration_ns_saturating(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// Record one CRUD commit-envelope stage duration.
pub fn record_commit_envelope_stage_duration(stage: CommitEnvelopeStage, duration: Duration) {
    record_commit_envelope_stage_ns(stage, duration_ns_saturating(duration));
}

/// Record one CRUD commit-envelope stage duration in nanoseconds.
pub fn record_commit_envelope_stage_ns(stage: CommitEnvelopeStage, ns: u64) {
    let index = stage as usize;
    COMMIT_ENVELOPE_STAGE_NS_TOTAL[index].fetch_add(ns, Ordering::Relaxed);
    COMMIT_ENVELOPE_STAGE_SAMPLES_TOTAL[index].fetch_add(1, Ordering::Relaxed);
}

/// Snapshot cumulative nanoseconds recorded for one commit-envelope stage.
pub fn commit_envelope_stage_ns_snapshot(stage: CommitEnvelopeStage) -> u64 {
    COMMIT_ENVELOPE_STAGE_NS_TOTAL[stage as usize].load(Ordering::Relaxed)
}

/// Snapshot sample count recorded for one commit-envelope stage.
pub fn commit_envelope_stage_samples_snapshot(stage: CommitEnvelopeStage) -> u64 {
    COMMIT_ENVELOPE_STAGE_SAMPLES_TOTAL[stage as usize].load(Ordering::Relaxed)
}

/// Reset all CRUD commit-envelope stage timing probes.
pub fn reset_commit_envelope_stage_metrics() {
    for slot in &COMMIT_ENVELOPE_STAGE_NS_TOTAL {
        slot.store(0, Ordering::Relaxed);
    }
    for slot in &COMMIT_ENVELOPE_STAGE_SAMPLES_TOTAL {
        slot.store(0, Ordering::Relaxed);
    }
}

// P5 — emergency_checkpoint_triggers_total  (counter)
define_counter!(
    /// Total number of times the engine's emergency-checkpoint path fired after
    /// `commit_txn` reported the journal-index hot threshold was reached.
    ///
    /// Observation only — write-side updates are lock-free atomics; snapshot/reset
    /// calls are test/admin surfaces and must not race with active writers.
    EMERGENCY_CHECKPOINT_TRIGGERS_TOTAL,
    /// Record one emergency-checkpoint trigger.
    record_emergency_checkpoint_trigger,
    /// Snapshot the emergency-checkpoint triggers counter.
    emergency_checkpoint_triggers_snapshot,
    /// Reset the emergency-checkpoint triggers counter.
    reset_emergency_checkpoint_triggers,
);

// ===========================================================================
// Section 3: Phase 1 §10.10 counters (US-012)
//
// These four counters make the publish-decision table (§4.1 / §10.3)
// observable. They are wired inside `publish_commit` (src/storage/
// paged_engine/publish.rs) so every CRUD + DDL publish path ticks them
// consistently. Observation only — no lock held while reading or
// writing. Gated on `Ordering::Relaxed` since counters are advisory.
// ===========================================================================

// read_epoch_publish_count (counter)
define_counter!(
    /// Total number of `publish_commit` invocations (one tick per
    /// `published.store`). Advances monotonically with every CRUD + DDL
    /// publish, including root-neutral commits that reuse the prior
    /// `Arc<PublishedCatalog>`.
    ///
    /// Observation only — no lock held while reading or writing.
    READ_EPOCH_PUBLISH_COUNT,
    /// Record one publish (always ticked by `publish_commit`).
    record_read_epoch_publish,
    /// Snapshot the publish counter.
    read_epoch_publish_count_snapshot,
    /// Reset the publish counter.
    reset_read_epoch_publish_count,
);

// published_catalog_rebuild_count (counter)
define_counter!(
    /// Number of publishes that built a FRESH `Arc<PublishedCatalog>`
    /// (i.e. `dirty.published_catalog_dirty == true`). Root-neutral CRUD
    /// reuses the prior Arc and does NOT tick this counter.
    ///
    /// Observation only — no lock held while reading or writing.
    PUBLISHED_CATALOG_REBUILD_COUNT,
    /// Record one catalog-Arc rebuild.
    record_published_catalog_rebuild,
    /// Snapshot the catalog-rebuild counter.
    published_catalog_rebuild_count_snapshot,
    /// Reset the catalog-rebuild counter.
    reset_published_catalog_rebuild_count,
);

// catalog_header_sync_count (counter)
define_counter!(
    /// Number of publishes whose txn had `catalog_header_dirty == true`
    /// (i.e. the catalog-root header owner ran during the body). Ticks on
    /// every root-moving CRUD and on every DDL; does NOT tick on
    /// root-neutral CRUD or on multikey-only flips where the on-disk
    /// header did not change.
    ///
    /// Observation only — no lock held while reading or writing.
    CATALOG_HEADER_SYNC_COUNT,
    /// Record one catalog-header sync.
    record_catalog_header_sync,
    /// Snapshot the header-sync counter.
    catalog_header_sync_count_snapshot,
    /// Reset the header-sync counter.
    reset_catalog_header_sync_count,
);

// root_neutral_commit_count (counter)
define_counter!(
    /// Number of publishes whose txn had BOTH flags clear — i.e.
    /// root-neutral CRUD commits that reuse the prior catalog Arc and
    /// skip the catalog-root header owner.
    ///
    /// Observation only — no lock held while reading or writing.
    ROOT_NEUTRAL_COMMIT_COUNT,
    /// Record one root-neutral publish.
    record_root_neutral_commit,
    /// Snapshot the root-neutral counter.
    root_neutral_commit_count_snapshot,
    /// Reset the root-neutral counter.
    reset_root_neutral_commit_count,
);

// ---------------------------------------------------------------------------
// delta_bearing_frames_count / ratio (occupancy gauges)
//
// The ratio uses f64::to_bits/from_bits for atomic storage — unique logic,
// left as plain functions rather than macro-generated.
// ---------------------------------------------------------------------------

/// Latest observed number of frames carrying a live committed delta head.
pub static DELTA_BEARING_FRAMES_COUNT: AtomicU64 = AtomicU64::new(0);

/// Latest observed delta-bearing frame ratio encoded with `f64::to_bits`.
pub static DELTA_BEARING_FRAMES_RATIO_BITS: AtomicU64 = AtomicU64::new(0);

/// Record one delta-bearing frame during a buffer-pool occupancy scan.
pub fn record_delta_bearing_frame() {
    DELTA_BEARING_FRAMES_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the latest delta-bearing frame count.
pub fn delta_bearing_frames_count_snapshot() -> u64 {
    DELTA_BEARING_FRAMES_COUNT.load(Ordering::Relaxed)
}

/// Reset the delta-bearing frame count gauge.
pub fn reset_delta_bearing_frames_count() {
    DELTA_BEARING_FRAMES_COUNT.store(0, Ordering::Relaxed);
}

/// Set the latest delta-bearing frame ratio.
pub fn set_delta_bearing_frames_ratio(value: f64) {
    DELTA_BEARING_FRAMES_RATIO_BITS.store(value.to_bits(), Ordering::Relaxed);
}

/// Snapshot the latest delta-bearing frame ratio.
pub fn delta_bearing_frames_ratio_snapshot() -> f64 {
    f64::from_bits(DELTA_BEARING_FRAMES_RATIO_BITS.load(Ordering::Relaxed))
}

/// Reset the delta-bearing frame ratio gauge.
pub fn reset_delta_bearing_frames_ratio() {
    DELTA_BEARING_FRAMES_RATIO_BITS.store(0, Ordering::Relaxed);
}
