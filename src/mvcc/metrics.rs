//! MVCC counters — 12 mandatory + 6 diagnostic.
//!
//! Each counter is a process-global atomic exposed through
//! `record()` / `snapshot()` / `reset()` primitives (or `set()` for gauges).
//! Tests that observe counter transitions should `reset()` first to avoid
//! cross-test interference.
//!
//! ## 12 mandatory counters
//!
//! | # | Counter | Type |
//! |---|---------|------|
//! | 1 | `mvcc.oldest_required_ts_lag_ms` | gauge |
//! | 2 | `mvcc.active_read_views` | gauge |
//! | 3 | `mvcc.version_chain_depth_p99` | gauge |
//! | 4 | `mvcc.history_store_bytes` | gauge |
//! | 5 | `mvcc.reconcile.entries_dropped_total` | counter |
//! | 6 | `mvcc.journal.commits_total` | counter |
//! | 7 | `mvcc.read_views_force_expired_total` | counter |
//! | 8 | `mvcc.secondary_index.tombstone_hits_skipped_total` | counter |
//! | 9 | `mvcc.overflow.pages_in_use` | gauge |
//! | 10 | `mvcc.overflow.refcount_cas_retries_total` | counter |
//! | 11 | `mvcc.history_store.gc_passes_total` | counter |
//! | 12 | `mvcc.deferred_free_queue_depth` | gauge |
//!
//! ## 6 diagnostic counters
//!
//! - `mvcc.reconcile.duration_ms_p99` — gauge
//! - `mvcc.chain_migration.entries_moved_total` — counter
//! - `mvcc.hlc.advance_events_total` — counter
//! - `mvcc.journal.chain_commit_frames_total` — counter
//! - `mvcc.force_expire_spin_stalls_total` — counter
//! - `mvcc.checkpoint.frontier_blocked_total` — counter

use std::sync::atomic::{AtomicU64, Ordering};

// ===========================================================================
// 8 — mvcc.secondary_index.tombstone_hits_skipped_total  (counter)
// ===========================================================================

/// Incremented each time a reader observes a tombstone entry in a
/// secondary-index chain and skips the underlying primary fetch.
pub static SECONDARY_INDEX_TOMBSTONE_HITS_SKIPPED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one sec-index tombstone-elision event.
pub fn record_secondary_index_tombstone_hit() {
    SECONDARY_INDEX_TOMBSTONE_HITS_SKIPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the current counter value.
pub fn secondary_index_tombstone_hits_snapshot() -> u64 {
    SECONDARY_INDEX_TOMBSTONE_HITS_SKIPPED_TOTAL.load(Ordering::Relaxed)
}

/// Reset the counter to zero. Primarily for tests.
pub fn reset_secondary_index_tombstone_hits() {
    SECONDARY_INDEX_TOMBSTONE_HITS_SKIPPED_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// 5 — mvcc.reconcile.entries_dropped_total  (counter)
// ===========================================================================

/// Number of `VersionEntry` objects dropped from per-frame version chains
/// by `BufferPool::reconcile` (entries whose `stop_ts <= oldest_required_ts`).
pub static RECONCILE_ENTRIES_DROPPED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record N entries dropped by one reconcile pass.
pub fn record_reconcile_entries_dropped(count: u64) {
    if count > 0 {
        RECONCILE_ENTRIES_DROPPED_TOTAL.fetch_add(count, Ordering::Relaxed);
    }
}

/// Snapshot the reconcile-dropped counter.
pub fn reconcile_entries_dropped_snapshot() -> u64 {
    RECONCILE_ENTRIES_DROPPED_TOTAL.load(Ordering::Relaxed)
}

/// Reset the reconcile-dropped counter.
pub fn reset_reconcile_entries_dropped() {
    RECONCILE_ENTRIES_DROPPED_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// mvcc.checkpoint.frontier_blocked_total (counter)
// ===========================================================================

/// Number of checkpoint attempts blocked before mutation because at least one
/// checkpoint-visible dirty leaf could not safely advance the durable frontier.
pub static CHECKPOINT_FRONTIER_BLOCKED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one mutation-free checkpoint frontier blocker.
pub fn record_checkpoint_frontier_blocked() {
    CHECKPOINT_FRONTIER_BLOCKED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the checkpoint-frontier-blocked counter.
pub fn checkpoint_frontier_blocked_snapshot() -> u64 {
    CHECKPOINT_FRONTIER_BLOCKED_TOTAL.load(Ordering::Relaxed)
}

/// Reset the checkpoint-frontier-blocked counter.
pub fn reset_checkpoint_frontier_blocked() {
    CHECKPOINT_FRONTIER_BLOCKED_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// mvcc.overflow.pages_freed_total
// ===========================================================================

/// Number of overflow pages that `AllocatorHandle::drain_free_queue` has
/// returned to the free list.
pub static OVERFLOW_PAGES_FREED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one overflow-page free (called from the drain path per freed page).
pub fn record_overflow_page_freed() {
    OVERFLOW_PAGES_FREED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the overflow-freed counter.
pub fn overflow_pages_freed_snapshot() -> u64 {
    OVERFLOW_PAGES_FREED_TOTAL.load(Ordering::Relaxed)
}

/// Reset the overflow-freed counter.
pub fn reset_overflow_pages_freed() {
    OVERFLOW_PAGES_FREED_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// 12 — mvcc.deferred_free_queue_depth  (gauge)
// ===========================================================================

/// Current depth of the deferred-free queue. Gauge — set to the queue's
/// size after every drain cycle.
pub static DEFERRED_FREE_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);

/// Set the deferred-free-queue depth gauge.
pub fn set_deferred_free_queue_depth(depth: u64) {
    DEFERRED_FREE_QUEUE_DEPTH.store(depth, Ordering::Relaxed);
}

/// Snapshot the deferred-free-queue depth gauge.
pub fn deferred_free_queue_depth_snapshot() -> u64 {
    DEFERRED_FREE_QUEUE_DEPTH.load(Ordering::Relaxed)
}

/// Reset the deferred-free-queue depth gauge.
pub fn reset_deferred_free_queue_depth() {
    DEFERRED_FREE_QUEUE_DEPTH.store(0, Ordering::Relaxed);
}

// ===========================================================================
// 1 — mvcc.oldest_required_ts_lag_ms  (gauge)
// ===========================================================================

/// How far behind wall-clock the `oldest_required_ts` horizon is, in
/// milliseconds. Set by whoever computes the lag (typically checkpoint
/// or GC). Elevated values with zero active readers ⇒ stuck oracle.
pub static OLDEST_REQUIRED_TS_LAG_MS: AtomicU64 = AtomicU64::new(0);

/// Set the ort-lag gauge.
pub fn set_oldest_required_ts_lag_ms(lag_ms: u64) {
    OLDEST_REQUIRED_TS_LAG_MS.store(lag_ms, Ordering::Relaxed);
}

/// Snapshot the ort-lag gauge.
pub fn oldest_required_ts_lag_ms_snapshot() -> u64 {
    OLDEST_REQUIRED_TS_LAG_MS.load(Ordering::Relaxed)
}

/// Reset the ort-lag gauge.
pub fn reset_oldest_required_ts_lag_ms() {
    OLDEST_REQUIRED_TS_LAG_MS.store(0, Ordering::Relaxed);
}

// ===========================================================================
// 2 — mvcc.active_read_views  (gauge)
// ===========================================================================

/// Current number of live `ReadView`s registered with `ReadViewRegistry`.
/// Updated by `ReadViewRegistry::{register,unregister}`.
pub static ACTIVE_READ_VIEWS: AtomicU64 = AtomicU64::new(0);

/// Set the active-read-views gauge.
pub fn set_active_read_views(n: u64) {
    ACTIVE_READ_VIEWS.store(n, Ordering::Relaxed);
}

/// Snapshot the active-read-views gauge.
pub fn active_read_views_snapshot() -> u64 {
    ACTIVE_READ_VIEWS.load(Ordering::Relaxed)
}

/// Reset the active-read-views gauge.
pub fn reset_active_read_views() {
    ACTIVE_READ_VIEWS.store(0, Ordering::Relaxed);
}

// ===========================================================================
// 3 — mvcc.version_chain_depth_p99  (gauge)
// ===========================================================================

/// Latest observed p99 of per-frame `deltas[key].len()`. Caller
/// samples a histogram and publishes the p99 here.
pub static VERSION_CHAIN_DEPTH_P99: AtomicU64 = AtomicU64::new(0);

/// Set the version-chain-depth p99 gauge.
pub fn set_version_chain_depth_p99(depth: u64) {
    VERSION_CHAIN_DEPTH_P99.store(depth, Ordering::Relaxed);
}

/// Snapshot the version-chain-depth p99 gauge.
pub fn version_chain_depth_p99_snapshot() -> u64 {
    VERSION_CHAIN_DEPTH_P99.load(Ordering::Relaxed)
}

/// Reset the version-chain-depth p99 gauge.
pub fn reset_version_chain_depth_p99() {
    VERSION_CHAIN_DEPTH_P99.store(0, Ordering::Relaxed);
}

// ===========================================================================
// 4 — mvcc.history_store_bytes  (gauge)
// ===========================================================================

/// Current size of the history store in bytes (approximate — sampled after
/// GC or checkpoint).
pub static HISTORY_STORE_BYTES: AtomicU64 = AtomicU64::new(0);

/// Set the history-store-bytes gauge.
pub fn set_history_store_bytes(bytes: u64) {
    HISTORY_STORE_BYTES.store(bytes, Ordering::Relaxed);
}

/// Snapshot the history-store-bytes gauge.
pub fn history_store_bytes_snapshot() -> u64 {
    HISTORY_STORE_BYTES.load(Ordering::Relaxed)
}

/// Reset the history-store-bytes gauge.
pub fn reset_history_store_bytes() {
    HISTORY_STORE_BYTES.store(0, Ordering::Relaxed);
}

// ===========================================================================
// 6 — mvcc.journal.commits_total  (counter)
// ===========================================================================

/// Total number of journal commit-frame emissions (one per successful
/// `with_txn` commit path).
pub static JOURNAL_COMMITS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one journal commit.
pub fn record_journal_commit() {
    JOURNAL_COMMITS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the journal-commits counter.
pub fn journal_commits_snapshot() -> u64 {
    JOURNAL_COMMITS_TOTAL.load(Ordering::Relaxed)
}

/// Reset the journal-commits counter.
pub fn reset_journal_commits() {
    JOURNAL_COMMITS_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// 7 — mvcc.read_views_force_expired_total  (counter)
// ===========================================================================

/// Total number of ReadViews that were force-expired by `force_expire`.
pub static READ_VIEWS_FORCE_EXPIRED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one force-expire event.
pub fn record_read_view_force_expired() {
    READ_VIEWS_FORCE_EXPIRED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the force-expired counter.
pub fn read_views_force_expired_snapshot() -> u64 {
    READ_VIEWS_FORCE_EXPIRED_TOTAL.load(Ordering::Relaxed)
}

/// Reset the force-expired counter.
pub fn reset_read_views_force_expired() {
    READ_VIEWS_FORCE_EXPIRED_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// 9 — mvcc.overflow.pages_in_use  (gauge)
// ===========================================================================

/// Number of overflow chains with refcount ≥ 1. Gauge, sampled from the
/// allocator's per-chain refcount table.
pub static OVERFLOW_PAGES_IN_USE: AtomicU64 = AtomicU64::new(0);

/// Set the overflow-pages-in-use gauge.
pub fn set_overflow_pages_in_use(n: u64) {
    OVERFLOW_PAGES_IN_USE.store(n, Ordering::Relaxed);
}

/// Snapshot the overflow-pages-in-use gauge.
pub fn overflow_pages_in_use_snapshot() -> u64 {
    OVERFLOW_PAGES_IN_USE.load(Ordering::Relaxed)
}

/// Reset the overflow-pages-in-use gauge.
pub fn reset_overflow_pages_in_use() {
    OVERFLOW_PAGES_IN_USE.store(0, Ordering::Relaxed);
}

// ===========================================================================
// 10 — mvcc.overflow.refcount_cas_retries_total  (counter)
// ===========================================================================

/// Total number of observed CAS failures inside `incref_overflow`'s loop.
/// Each retry here is one contended CAS.
pub static OVERFLOW_REFCOUNT_CAS_RETRIES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one CAS retry in `incref_overflow`.
pub fn record_overflow_refcount_cas_retry() {
    OVERFLOW_REFCOUNT_CAS_RETRIES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the CAS-retries counter.
pub fn overflow_refcount_cas_retries_snapshot() -> u64 {
    OVERFLOW_REFCOUNT_CAS_RETRIES_TOTAL.load(Ordering::Relaxed)
}

/// Reset the CAS-retries counter.
pub fn reset_overflow_refcount_cas_retries() {
    OVERFLOW_REFCOUNT_CAS_RETRIES_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// 11 — mvcc.history_store.gc_passes_total  (counter)
// ===========================================================================

/// Total number of completed history-store GC passes.
pub static HISTORY_STORE_GC_PASSES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one completed GC pass.
pub fn record_history_store_gc_pass() {
    HISTORY_STORE_GC_PASSES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the GC-passes counter.
pub fn history_store_gc_passes_snapshot() -> u64 {
    HISTORY_STORE_GC_PASSES_TOTAL.load(Ordering::Relaxed)
}

/// Reset the GC-passes counter.
pub fn reset_history_store_gc_passes() {
    HISTORY_STORE_GC_PASSES_TOTAL.store(0, Ordering::Relaxed);
}

/// Test-only serialization lock shared by every test that resets and asserts
/// on `HISTORY_STORE_GC_PASSES_TOTAL`. Tests run in parallel within a single
/// process, so concurrent reset-then-assert sequences on the same global
/// counter race. All tests that reset this counter must hold this mutex for
/// the duration of their reset → record → snapshot sequence.
#[cfg(test)]
pub(crate) static GC_PASSES_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

// ===========================================================================
// D1 — mvcc.reconcile.duration_ms_p99  (gauge, diagnostic)
// ===========================================================================

/// Latest observed p99 of reconcile-pass durations in milliseconds.
pub static RECONCILE_DURATION_MS_P99: AtomicU64 = AtomicU64::new(0);

/// Set the reconcile-duration p99 gauge.
pub fn set_reconcile_duration_ms_p99(ms: u64) {
    RECONCILE_DURATION_MS_P99.store(ms, Ordering::Relaxed);
}

/// Snapshot the reconcile-duration p99 gauge.
pub fn reconcile_duration_ms_p99_snapshot() -> u64 {
    RECONCILE_DURATION_MS_P99.load(Ordering::Relaxed)
}

/// Reset the reconcile-duration p99 gauge.
pub fn reset_reconcile_duration_ms_p99() {
    RECONCILE_DURATION_MS_P99.store(0, Ordering::Relaxed);
}

// ===========================================================================
// D2 — mvcc.chain_migration.entries_moved_total  (counter, diagnostic)
// ===========================================================================

/// Entries moved from in-memory chains to the history store or across
/// splits. Incremented by migration code.
pub static CHAIN_MIGRATION_ENTRIES_MOVED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record N migrated entries.
pub fn record_chain_migration_entries_moved(count: u64) {
    if count > 0 {
        CHAIN_MIGRATION_ENTRIES_MOVED_TOTAL.fetch_add(count, Ordering::Relaxed);
    }
}

/// Snapshot the chain-migration counter.
pub fn chain_migration_entries_moved_snapshot() -> u64 {
    CHAIN_MIGRATION_ENTRIES_MOVED_TOTAL.load(Ordering::Relaxed)
}

/// Reset the chain-migration counter.
pub fn reset_chain_migration_entries_moved() {
    CHAIN_MIGRATION_ENTRIES_MOVED_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// D3 — mvcc.hlc.advance_events_total  (counter, diagnostic)
// ===========================================================================

/// Number of `TimestampOracle::advance` invocations. Single-node always
/// zero; multi-node replication increments.
pub static HLC_ADVANCE_EVENTS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one oracle-advance event.
pub fn record_hlc_advance() {
    HLC_ADVANCE_EVENTS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the hlc-advance counter.
pub fn hlc_advance_events_snapshot() -> u64 {
    HLC_ADVANCE_EVENTS_TOTAL.load(Ordering::Relaxed)
}

/// Reset the hlc-advance counter.
pub fn reset_hlc_advance_events() {
    HLC_ADVANCE_EVENTS_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// D4 — mvcc.journal.chain_commit_frames_total  (counter, diagnostic)
// ===========================================================================

/// Total number of ChainCommit frames appended to the journal.
pub static JOURNAL_CHAIN_COMMIT_FRAMES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one ChainCommit frame append.
pub fn record_journal_chain_commit_frame() {
    JOURNAL_CHAIN_COMMIT_FRAMES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the chain-commit-frames counter.
pub fn journal_chain_commit_frames_snapshot() -> u64 {
    JOURNAL_CHAIN_COMMIT_FRAMES_TOTAL.load(Ordering::Relaxed)
}

/// Reset the chain-commit-frames counter.
pub fn reset_journal_chain_commit_frames() {
    JOURNAL_CHAIN_COMMIT_FRAMES_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// D5 — mvcc.force_expire_spin_stalls_total  (counter, diagnostic)
// ===========================================================================

/// Number of times `ReadView::wait_pin_drain` spun past `TIMEOUT_MS`
/// waiting for `pin_ops_in_flight` to drain.
pub static FORCE_EXPIRE_SPIN_STALLS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one force-expire spin-stall event.
pub fn record_force_expire_spin_stall() {
    FORCE_EXPIRE_SPIN_STALLS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the force-expire spin-stalls counter.
pub fn force_expire_spin_stalls_snapshot() -> u64 {
    FORCE_EXPIRE_SPIN_STALLS_TOTAL.load(Ordering::Relaxed)
}

/// Reset the force-expire spin-stalls counter.
pub fn reset_force_expire_spin_stalls() {
    FORCE_EXPIRE_SPIN_STALLS_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// Phase-0 observability counters (5 signals)
//
// Each counter below is observation only. Write-side updates are lock-free
// atomic fetch_add/store calls; reads (`_snapshot` / `_reset`) are
// test/admin surfaces and must not race with active writers.
// ===========================================================================

// ---------------------------------------------------------------------------
// P1 — published_snapshot_rebuilds_total  (counter)
// ---------------------------------------------------------------------------

/// Total number of full published-catalog rebuilds performed via
/// `publish_commit` (Phase 1 §10.2 / §10.10). One tick per publish
/// whose `dirty.published_catalog_dirty == true`. Phase 1 US-012 also
/// exposes this via `published_catalog_rebuild_count`; the two
/// counters tick in lock-step.
///
/// Observation only — write-side updates are lock-free atomics; snapshot/reset
/// calls are test/admin surfaces and must not race with active writers.
pub static PUBLISHED_SNAPSHOT_REBUILDS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one published-snapshot rebuild.
pub fn record_published_snapshot_rebuild() {
    PUBLISHED_SNAPSHOT_REBUILDS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the published-snapshot-rebuilds counter.
pub fn published_snapshot_rebuilds_snapshot() -> u64 {
    PUBLISHED_SNAPSHOT_REBUILDS_TOTAL.load(Ordering::Relaxed)
}

/// Reset the published-snapshot-rebuilds counter.
pub fn reset_published_snapshot_rebuilds() {
    PUBLISHED_SNAPSHOT_REBUILDS_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// P2a — crud_commits_root_neutral_total  (counter)
// ---------------------------------------------------------------------------

/// CRUD commits whose body did NOT persist updated tree-root metadata
/// (no catalog-root header owner update during the txn).
///
/// Observation only — write-side updates are lock-free atomics; snapshot/reset
/// calls are test/admin surfaces and must not race with active writers.
pub static CRUD_COMMITS_ROOT_NEUTRAL_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one root-neutral CRUD commit.
pub fn record_crud_commit_root_neutral() {
    CRUD_COMMITS_ROOT_NEUTRAL_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the root-neutral CRUD-commits counter.
pub fn crud_commits_root_neutral_snapshot() -> u64 {
    CRUD_COMMITS_ROOT_NEUTRAL_TOTAL.load(Ordering::Relaxed)
}

/// Reset the root-neutral CRUD-commits counter.
pub fn reset_crud_commits_root_neutral() {
    CRUD_COMMITS_ROOT_NEUTRAL_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// P2b — crud_commits_root_changing_total  (counter)
// ---------------------------------------------------------------------------

/// CRUD commits whose body DID persist updated tree-root metadata (the
/// catalog-root header owner path fired at least once during the txn).
///
/// Observation only — write-side updates are lock-free atomics; snapshot/reset
/// calls are test/admin surfaces and must not race with active writers.
pub static CRUD_COMMITS_ROOT_CHANGING_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one root-changing CRUD commit.
pub fn record_crud_commit_root_changing() {
    CRUD_COMMITS_ROOT_CHANGING_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the root-changing CRUD-commits counter.
pub fn crud_commits_root_changing_snapshot() -> u64 {
    CRUD_COMMITS_ROOT_CHANGING_TOTAL.load(Ordering::Relaxed)
}

/// Reset the root-changing CRUD-commits counter.
pub fn reset_crud_commits_root_changing() {
    CRUD_COMMITS_ROOT_CHANGING_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// P3a — lane_wait_ns_total  (counter, cumulative nanoseconds)
// ---------------------------------------------------------------------------

/// Cumulative nanoseconds CRUD writers spent waiting on write admission.
/// Timed with `Instant::now()` reads taken OUTSIDE the critical section; the
/// write-side record call is a lock-free atomic add that does not extend the
/// critical section.
///
/// Observation only — write-side updates are lock-free atomics; snapshot/reset
/// calls are test/admin surfaces and must not race with active writers.
pub static LANE_WAIT_NS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record N nanoseconds of lane-acquisition wait.
pub fn record_lane_wait_ns(ns: u64) {
    if ns > 0 {
        LANE_WAIT_NS_TOTAL.fetch_add(ns, Ordering::Relaxed);
    }
}

/// Snapshot the lane-wait cumulative-nanoseconds counter.
pub fn lane_wait_ns_snapshot() -> u64 {
    LANE_WAIT_NS_TOTAL.load(Ordering::Relaxed)
}

/// Reset the lane-wait counter.
pub fn reset_lane_wait_ns() {
    LANE_WAIT_NS_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// P4a — recovery_legacy_page_frames_total  (counter)
// ---------------------------------------------------------------------------

/// Total number of retired page-replay records processed by older recovery
/// loops.
///
/// Observation only — write-side updates are lock-free atomics; snapshot/reset
/// calls are test/admin surfaces and must not race with active writers.
pub static RECOVERY_LEGACY_PAGE_FRAMES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one legacy page-frame seen by recovery.
pub fn record_recovery_legacy_page_frame() {
    RECOVERY_LEGACY_PAGE_FRAMES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the legacy-page-frames recovery counter.
pub fn recovery_legacy_page_frames_snapshot() -> u64 {
    RECOVERY_LEGACY_PAGE_FRAMES_TOTAL.load(Ordering::Relaxed)
}

/// Reset the legacy-page-frames recovery counter.
pub fn reset_recovery_legacy_page_frames() {
    RECOVERY_LEGACY_PAGE_FRAMES_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// P4b — recovery_chain_commit_frames_total  (counter)
// ---------------------------------------------------------------------------

/// Total number of `ChainCommit` frames processed by the recovery loop.
///
/// Observation only — write-side updates are lock-free atomics; snapshot/reset
/// calls are test/admin surfaces and must not race with active writers.
pub static RECOVERY_CHAIN_COMMIT_FRAMES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one ChainCommit frame seen by recovery.
pub fn record_recovery_chain_commit_frame() {
    RECOVERY_CHAIN_COMMIT_FRAMES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the ChainCommit-frames recovery counter.
pub fn recovery_chain_commit_frames_snapshot() -> u64 {
    RECOVERY_CHAIN_COMMIT_FRAMES_TOTAL.load(Ordering::Relaxed)
}

/// Reset the ChainCommit-frames recovery counter.
pub fn reset_recovery_chain_commit_frames() {
    RECOVERY_CHAIN_COMMIT_FRAMES_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// P5 — emergency_checkpoint_triggers_total  (counter)
// ---------------------------------------------------------------------------

/// Total number of times the engine's emergency-checkpoint path fired after
/// `commit_txn` reported the journal-index hot threshold was reached.
///
/// Observation only — write-side updates are lock-free atomics; snapshot/reset
/// calls are test/admin surfaces and must not race with active writers.
pub static EMERGENCY_CHECKPOINT_TRIGGERS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one emergency-checkpoint trigger.
pub fn record_emergency_checkpoint_trigger() {
    EMERGENCY_CHECKPOINT_TRIGGERS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the emergency-checkpoint triggers counter.
pub fn emergency_checkpoint_triggers_snapshot() -> u64 {
    EMERGENCY_CHECKPOINT_TRIGGERS_TOTAL.load(Ordering::Relaxed)
}

/// Reset the emergency-checkpoint triggers counter.
pub fn reset_emergency_checkpoint_triggers() {
    EMERGENCY_CHECKPOINT_TRIGGERS_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// Phase 1 §10.10 counters (US-012)
// ===========================================================================
//
// These four counters make the publish-decision table (§4.1 / §10.3)
// observable. They are wired inside `publish_commit` (src/storage/
// paged_engine/publish.rs) so every CRUD + DDL publish path ticks them
// consistently. Observation only — no lock held while reading or
// writing. Gated on `Ordering::Relaxed` since counters are advisory.

// ---------------------------------------------------------------------------
// read_epoch_publish_count (counter)
// ---------------------------------------------------------------------------

/// Total number of `publish_commit` invocations (one tick per
/// `published.store`). Advances monotonically with every CRUD + DDL
/// publish, including root-neutral commits that reuse the prior
/// `Arc<PublishedCatalog>`.
///
/// Observation only — no lock held while reading or writing.
pub static READ_EPOCH_PUBLISH_COUNT: AtomicU64 = AtomicU64::new(0);

/// Record one publish (always ticked by `publish_commit`).
pub fn record_read_epoch_publish() {
    READ_EPOCH_PUBLISH_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the publish counter.
pub fn read_epoch_publish_count_snapshot() -> u64 {
    READ_EPOCH_PUBLISH_COUNT.load(Ordering::Relaxed)
}

/// Reset the publish counter.
pub fn reset_read_epoch_publish_count() {
    READ_EPOCH_PUBLISH_COUNT.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// published_catalog_rebuild_count (counter)
// ---------------------------------------------------------------------------

/// Number of publishes that built a FRESH `Arc<PublishedCatalog>`
/// (i.e. `dirty.published_catalog_dirty == true`). Root-neutral CRUD
/// reuses the prior Arc and does NOT tick this counter.
///
/// Observation only — no lock held while reading or writing.
pub static PUBLISHED_CATALOG_REBUILD_COUNT: AtomicU64 = AtomicU64::new(0);

/// Record one catalog-Arc rebuild.
pub fn record_published_catalog_rebuild() {
    PUBLISHED_CATALOG_REBUILD_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the catalog-rebuild counter.
pub fn published_catalog_rebuild_count_snapshot() -> u64 {
    PUBLISHED_CATALOG_REBUILD_COUNT.load(Ordering::Relaxed)
}

/// Reset the catalog-rebuild counter.
pub fn reset_published_catalog_rebuild_count() {
    PUBLISHED_CATALOG_REBUILD_COUNT.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// catalog_header_sync_count (counter)
// ---------------------------------------------------------------------------

/// Number of publishes whose txn had `catalog_header_dirty == true`
/// (i.e. the catalog-root header owner ran during the body). Ticks on
/// every root-moving CRUD and on every DDL; does NOT tick on
/// root-neutral CRUD or on multikey-only flips where the on-disk
/// header did not change.
///
/// Observation only — no lock held while reading or writing.
pub static CATALOG_HEADER_SYNC_COUNT: AtomicU64 = AtomicU64::new(0);

/// Record one catalog-header sync.
pub fn record_catalog_header_sync() {
    CATALOG_HEADER_SYNC_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the header-sync counter.
pub fn catalog_header_sync_count_snapshot() -> u64 {
    CATALOG_HEADER_SYNC_COUNT.load(Ordering::Relaxed)
}

/// Reset the header-sync counter.
pub fn reset_catalog_header_sync_count() {
    CATALOG_HEADER_SYNC_COUNT.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// root_neutral_commit_count (counter)
// ---------------------------------------------------------------------------

/// Number of publishes whose txn had BOTH flags clear — i.e.
/// root-neutral CRUD commits that reuse the prior catalog Arc and
/// skip the catalog-root header owner.
///
/// Observation only — no lock held while reading or writing.
pub static ROOT_NEUTRAL_COMMIT_COUNT: AtomicU64 = AtomicU64::new(0);

/// Record one root-neutral publish.
pub fn record_root_neutral_commit() {
    ROOT_NEUTRAL_COMMIT_COUNT.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the root-neutral counter.
pub fn root_neutral_commit_count_snapshot() -> u64 {
    ROOT_NEUTRAL_COMMIT_COUNT.load(Ordering::Relaxed)
}

/// Reset the root-neutral counter.
pub fn reset_root_neutral_commit_count() {
    ROOT_NEUTRAL_COMMIT_COUNT.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// delta_bearing_frames_count / ratio (occupancy gauges)
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

// ===========================================================================
// Phase 2 §7 — LogicalTxnFrame Pass 2 post-open validation counters
// ===========================================================================
//
// Pass 2 runs inside `SharedState::new` immediately after
// `catalog_open_with_fallback`. For each op in each parsed logical frame
// it resolves `ns_id` / `index_id` against the live catalog and records
// the outcome. Per §3.2 / §3.11 Pass 2 must never mutate durable state —
// these counters are the only observable side-effect.

// ---------------------------------------------------------------------------
// logical_txn_pass2_resolved_ops_total (counter)
// ---------------------------------------------------------------------------

/// Total ops that Pass 2 resolved against the live catalog at open.
///
/// Observation only — Pass 2 runs once during open, ticked per-op.
pub static LOGICAL_TXN_PASS2_RESOLVED_OPS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one resolved Pass 2 op (ns_id or index_id matched).
pub fn record_logical_txn_pass2_resolved_op() {
    LOGICAL_TXN_PASS2_RESOLVED_OPS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the Pass 2 resolved-ops counter.
pub fn logical_txn_pass2_resolved_ops_snapshot() -> u64 {
    LOGICAL_TXN_PASS2_RESOLVED_OPS_TOTAL.load(Ordering::Relaxed)
}

/// Reset the Pass 2 resolved-ops counter.
pub fn reset_logical_txn_pass2_resolved_ops() {
    LOGICAL_TXN_PASS2_RESOLVED_OPS_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// logical_txn_pass2_unresolved_ops_total (counter)
// ---------------------------------------------------------------------------

/// Total ops that Pass 2 could not resolve (ns_id / index_id absent from
/// the live catalog). Phase 2 treats these as log-and-proceed; Phase 4
/// promotes to a hard error per §8.13.
///
/// Observation only — ticked per-op during open.
pub static LOGICAL_TXN_PASS2_UNRESOLVED_OPS_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one unresolved Pass 2 op.
pub fn record_logical_txn_pass2_unresolved_op() {
    LOGICAL_TXN_PASS2_UNRESOLVED_OPS_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the Pass 2 unresolved-ops counter.
pub fn logical_txn_pass2_unresolved_ops_snapshot() -> u64 {
    LOGICAL_TXN_PASS2_UNRESOLVED_OPS_TOTAL.load(Ordering::Relaxed)
}

/// Reset the Pass 2 unresolved-ops counter.
pub fn reset_logical_txn_pass2_unresolved_ops() {
    LOGICAL_TXN_PASS2_UNRESOLVED_OPS_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// Phase 2 §3.8 — Pass 1 sweep counters (US-014)
// ===========================================================================
//
// Observable proof of the recovery warnings emitted by the orphan-logical
// sweep and the unmatched-ChainCommit detection. Tests use these to
// verify §3.8(b) and case (c) tolerance behavior without depending on
// the optional `tracing` feature.

// ---------------------------------------------------------------------------
// logical_txn_pass1_orphan_logical_dropped_total (counter)
// ---------------------------------------------------------------------------

/// Total logical frames discarded by the Pass 1 orphan-sweep (§3.8(b)).
/// Each tick corresponds to a logical frame whose commit_ts has no
/// matching ChainCommit in the same recovery scan.
pub static LOGICAL_TXN_PASS1_ORPHAN_LOGICAL_DROPPED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one orphan-logical frame dropped by Pass 1.
pub fn record_logical_txn_pass1_orphan_logical_dropped() {
    LOGICAL_TXN_PASS1_ORPHAN_LOGICAL_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the orphan-logical-dropped counter.
pub fn logical_txn_pass1_orphan_logical_dropped_snapshot() -> u64 {
    LOGICAL_TXN_PASS1_ORPHAN_LOGICAL_DROPPED_TOTAL.load(Ordering::Relaxed)
}

/// Reset the orphan-logical-dropped counter.
pub fn reset_logical_txn_pass1_orphan_logical_dropped() {
    LOGICAL_TXN_PASS1_ORPHAN_LOGICAL_DROPPED_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// logical_txn_pass1_unmatched_chain_commit_total (counter)
// ---------------------------------------------------------------------------

/// Total ChainCommit frames seen during Pass 1 that had no matching
/// LogicalTxnFrame at the same `commit_ts` (case (c) Phase 2 tolerance,
/// §3.7 envelope violation; Phase 4 §8.13.3 promotes this to hard error).
pub static LOGICAL_TXN_PASS1_UNMATCHED_CHAIN_COMMIT_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one unmatched ChainCommit (no paired logical frame).
pub fn record_logical_txn_pass1_unmatched_chain_commit() {
    LOGICAL_TXN_PASS1_UNMATCHED_CHAIN_COMMIT_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the unmatched-ChainCommit counter.
pub fn logical_txn_pass1_unmatched_chain_commit_snapshot() -> u64 {
    LOGICAL_TXN_PASS1_UNMATCHED_CHAIN_COMMIT_TOTAL.load(Ordering::Relaxed)
}

/// Reset the unmatched-ChainCommit counter.
pub fn reset_logical_txn_pass1_unmatched_chain_commit() {
    LOGICAL_TXN_PASS1_UNMATCHED_CHAIN_COMMIT_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// logical_txn_pass1_pre_boundary_dropped_total (counter) — §3.11
// ---------------------------------------------------------------------------

/// Total logical frames discarded by the Pass 1 checkpoint-boundary cull.
/// Each tick corresponds to a logical frame whose `commit_ts <=` the
/// recovered page-0 header's `last_checkpoint_ts`.
pub static LOGICAL_TXN_PASS1_PRE_BOUNDARY_DROPPED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one pre-boundary logical frame dropped by Pass 1.
pub fn record_logical_txn_pass1_pre_boundary_dropped() {
    LOGICAL_TXN_PASS1_PRE_BOUNDARY_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the pre-boundary-dropped counter.
pub fn logical_txn_pass1_pre_boundary_dropped_snapshot() -> u64 {
    LOGICAL_TXN_PASS1_PRE_BOUNDARY_DROPPED_TOTAL.load(Ordering::Relaxed)
}

/// Reset the pre-boundary-dropped counter.
pub fn reset_logical_txn_pass1_pre_boundary_dropped() {
    LOGICAL_TXN_PASS1_PRE_BOUNDARY_DROPPED_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// recovery_page0_boundary_frames_total (counter)
// ---------------------------------------------------------------------------

/// Total valid page-0 checkpoint boundary frames parsed by recovery.
pub static RECOVERY_PAGE0_BOUNDARY_FRAMES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one valid page-0 boundary frame seen by recovery.
pub fn record_recovery_page0_boundary_frame() {
    RECOVERY_PAGE0_BOUNDARY_FRAMES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the page-0 boundary recovery counter.
pub fn recovery_page0_boundary_frames_snapshot() -> u64 {
    RECOVERY_PAGE0_BOUNDARY_FRAMES_TOTAL.load(Ordering::Relaxed)
}

/// Reset the page-0 boundary recovery counter.
pub fn reset_recovery_page0_boundary_frames() {
    RECOVERY_PAGE0_BOUNDARY_FRAMES_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// Phase 2 §7 / US-024 observability counters (9 named signals)
// ===========================================================================
//
// The five Phase 2-only counters added below complement the four already
// defined above (LOGICAL_TXN_PASS2_RESOLVED_OPS_TOTAL,
// LOGICAL_TXN_PASS2_UNRESOLVED_OPS_TOTAL, RECOVERY_TORN_CHECKPOINT_BOUNDARY_TOTAL,
// and the orphan/pre-boundary counters) to satisfy the §7 nine-signal set.

// ---------------------------------------------------------------------------
// (1) logical_txn_append_bytes_total — counter
// ---------------------------------------------------------------------------

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
// (2) logical_txn_append_duration_ms_p50 — gauge (latest-value approximation)
// (3) logical_txn_append_duration_ms_p95 — gauge (latest-value approximation)
// (4) logical_txn_append_duration_ms_p99 — gauge (latest-value approximation)
// ---------------------------------------------------------------------------
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
//   2. `recompute_logical_txn_append_percentiles()` sorts the 64-slot
//      ring and stores p50/p95/p99 into the gauge atomics. Called
//      after the hot append-envelope work by an RAII guard (US-024 AC#3 /
//      §7 guardrail).
//
// Lock-free in both phases; uses three independent atomics for the
// gauges so the recompute window can race without locking. Approximate
// — the percentile values lag the true distribution by up to ~SAMPLE_RING
// observations, which is acceptable for §7 observability.

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
/// The percentile gauges are NOT updated by this function. The caller
/// MUST call [`recompute_logical_txn_append_percentiles`] after the hot
/// append-envelope work to refresh the p50/p95/p99 gauges. This split keeps
/// append bookkeeping O(1) and the heavier sort/store work outside the
/// append path, per the §7 / US-024 AC#3 constraint.
pub fn record_logical_txn_append_duration_ms(ms: u64) {
    let idx =
        APPEND_SAMPLE_RING_INDEX.fetch_add(1, Ordering::Relaxed) as usize % APPEND_SAMPLE_RING;
    APPEND_SAMPLE_RING_BUF[idx].store(ms, Ordering::Relaxed);
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

// ---------------------------------------------------------------------------
// (5) parsed_logical_frames_len — gauge (reset per open)
// ---------------------------------------------------------------------------

/// Length of the `ParsedLogicalFrames` vector handed off from Pass 1
/// to Pass 2 at the most recent open. Reset on each open before Pass 1
/// runs so the gauge reflects the current lifetime, not cumulative.
pub static PARSED_LOGICAL_FRAMES_LEN: AtomicU64 = AtomicU64::new(0);

/// Set the gauge.
pub fn set_parsed_logical_frames_len(n: u64) {
    PARSED_LOGICAL_FRAMES_LEN.store(n, Ordering::Relaxed);
}

/// Snapshot the gauge.
pub fn parsed_logical_frames_len_snapshot() -> u64 {
    PARSED_LOGICAL_FRAMES_LEN.load(Ordering::Relaxed)
}

/// Reset the gauge.
pub fn reset_parsed_logical_frames_len() {
    PARSED_LOGICAL_FRAMES_LEN.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// (8) logical_txn_recovery_discarded_frames_total — counter
// ---------------------------------------------------------------------------

/// Total logical frames discarded by recovery for any reason
/// (orphan-sweep + pre-boundary cull). Sum across the §3.8(b) and
/// §3.11 paths.
pub static LOGICAL_TXN_RECOVERY_DISCARDED_FRAMES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one discarded frame.
pub fn record_logical_txn_recovery_discarded_frame() {
    LOGICAL_TXN_RECOVERY_DISCARDED_FRAMES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the discarded counter.
pub fn logical_txn_recovery_discarded_frames_snapshot() -> u64 {
    LOGICAL_TXN_RECOVERY_DISCARDED_FRAMES_TOTAL.load(Ordering::Relaxed)
}

/// Reset the discarded counter.
pub fn reset_logical_txn_recovery_discarded_frames() {
    LOGICAL_TXN_RECOVERY_DISCARDED_FRAMES_TOTAL.store(0, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// (9) logical_txn_torn_frames_total — counter
// ---------------------------------------------------------------------------

/// Total torn LogicalTxnFrames observed by recovery — frames whose CRC
/// or structural validation failed mid-scan. Tracks tail corruption
/// against the §4.6 disposition table.
pub static LOGICAL_TXN_TORN_FRAMES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Record one torn frame.
pub fn record_logical_txn_torn_frame() {
    LOGICAL_TXN_TORN_FRAMES_TOTAL.fetch_add(1, Ordering::Relaxed);
}

/// Snapshot the torn counter.
pub fn logical_txn_torn_frames_snapshot() -> u64 {
    LOGICAL_TXN_TORN_FRAMES_TOTAL.load(Ordering::Relaxed)
}

/// Reset the torn counter.
pub fn reset_logical_txn_torn_frames() {
    LOGICAL_TXN_TORN_FRAMES_TOTAL.store(0, Ordering::Relaxed);
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    // --- Counters ---

    #[test]
    fn counter_increments_and_resets() {
        reset_secondary_index_tombstone_hits();
        assert_eq!(secondary_index_tombstone_hits_snapshot(), 0);
        record_secondary_index_tombstone_hit();
        record_secondary_index_tombstone_hit();
        assert_eq!(secondary_index_tombstone_hits_snapshot(), 2);
        reset_secondary_index_tombstone_hits();
        assert_eq!(secondary_index_tombstone_hits_snapshot(), 0);
    }

    #[test]
    fn reconcile_counter_sums_multiple_batches() {
        reset_reconcile_entries_dropped();
        assert_eq!(reconcile_entries_dropped_snapshot(), 0);
        record_reconcile_entries_dropped(3);
        record_reconcile_entries_dropped(7);
        assert_eq!(reconcile_entries_dropped_snapshot(), 10);
        record_reconcile_entries_dropped(0); // no-op
        assert_eq!(reconcile_entries_dropped_snapshot(), 10);
        reset_reconcile_entries_dropped();
        assert_eq!(reconcile_entries_dropped_snapshot(), 0);
    }

    #[test]
    fn overflow_freed_counter_ticks() {
        reset_overflow_pages_freed();
        assert_eq!(overflow_pages_freed_snapshot(), 0);
        record_overflow_page_freed();
        record_overflow_page_freed();
        record_overflow_page_freed();
        assert_eq!(overflow_pages_freed_snapshot(), 3);
        reset_overflow_pages_freed();
        assert_eq!(overflow_pages_freed_snapshot(), 0);
    }

    #[test]
    fn deferred_free_depth_gauge_set_and_reset() {
        reset_deferred_free_queue_depth();
        assert_eq!(deferred_free_queue_depth_snapshot(), 0);
        set_deferred_free_queue_depth(42);
        assert_eq!(deferred_free_queue_depth_snapshot(), 42);
        set_deferred_free_queue_depth(0);
        assert_eq!(deferred_free_queue_depth_snapshot(), 0);
    }

    // --- Mandatory counters (1–4, 6, 7, 9, 10, 11) ---

    #[test]
    fn oldest_required_ts_lag_ms_gauge() {
        reset_oldest_required_ts_lag_ms();
        assert_eq!(oldest_required_ts_lag_ms_snapshot(), 0);
        set_oldest_required_ts_lag_ms(1234);
        assert_eq!(oldest_required_ts_lag_ms_snapshot(), 1234);
        reset_oldest_required_ts_lag_ms();
        assert_eq!(oldest_required_ts_lag_ms_snapshot(), 0);
    }

    #[test]
    fn active_read_views_gauge() {
        reset_active_read_views();
        assert_eq!(active_read_views_snapshot(), 0);
        set_active_read_views(3);
        assert_eq!(active_read_views_snapshot(), 3);
        set_active_read_views(0);
        assert_eq!(active_read_views_snapshot(), 0);
    }

    #[test]
    fn version_chain_depth_p99_gauge() {
        reset_version_chain_depth_p99();
        set_version_chain_depth_p99(17);
        assert_eq!(version_chain_depth_p99_snapshot(), 17);
        reset_version_chain_depth_p99();
        assert_eq!(version_chain_depth_p99_snapshot(), 0);
    }

    #[test]
    fn history_store_bytes_gauge() {
        reset_history_store_bytes();
        set_history_store_bytes(8192);
        assert_eq!(history_store_bytes_snapshot(), 8192);
        reset_history_store_bytes();
        assert_eq!(history_store_bytes_snapshot(), 0);
    }

    #[test]
    fn journal_commits_counter() {
        reset_journal_commits();
        record_journal_commit();
        record_journal_commit();
        assert_eq!(journal_commits_snapshot(), 2);
        reset_journal_commits();
        assert_eq!(journal_commits_snapshot(), 0);
    }

    #[test]
    fn read_views_force_expired_counter() {
        reset_read_views_force_expired();
        record_read_view_force_expired();
        assert_eq!(read_views_force_expired_snapshot(), 1);
        reset_read_views_force_expired();
        assert_eq!(read_views_force_expired_snapshot(), 0);
    }

    #[test]
    fn overflow_pages_in_use_gauge() {
        reset_overflow_pages_in_use();
        set_overflow_pages_in_use(42);
        assert_eq!(overflow_pages_in_use_snapshot(), 42);
        reset_overflow_pages_in_use();
        assert_eq!(overflow_pages_in_use_snapshot(), 0);
    }

    #[test]
    fn overflow_refcount_cas_retries_counter() {
        reset_overflow_refcount_cas_retries();
        record_overflow_refcount_cas_retry();
        record_overflow_refcount_cas_retry();
        assert_eq!(overflow_refcount_cas_retries_snapshot(), 2);
        reset_overflow_refcount_cas_retries();
        assert_eq!(overflow_refcount_cas_retries_snapshot(), 0);
    }

    #[test]
    fn history_store_gc_passes_counter() {
        let _lock = GC_PASSES_TEST_LOCK.lock().unwrap();
        reset_history_store_gc_passes();
        record_history_store_gc_pass();
        assert_eq!(history_store_gc_passes_snapshot(), 1);
        reset_history_store_gc_passes();
        assert_eq!(history_store_gc_passes_snapshot(), 0);
    }

    // --- Diagnostic counters (D1–D5) ---

    #[test]
    fn reconcile_duration_ms_p99_gauge() {
        reset_reconcile_duration_ms_p99();
        set_reconcile_duration_ms_p99(99);
        assert_eq!(reconcile_duration_ms_p99_snapshot(), 99);
        reset_reconcile_duration_ms_p99();
        assert_eq!(reconcile_duration_ms_p99_snapshot(), 0);
    }

    #[test]
    fn chain_migration_entries_moved_counter() {
        reset_chain_migration_entries_moved();
        record_chain_migration_entries_moved(5);
        record_chain_migration_entries_moved(0); // no-op
        record_chain_migration_entries_moved(3);
        assert_eq!(chain_migration_entries_moved_snapshot(), 8);
        reset_chain_migration_entries_moved();
        assert_eq!(chain_migration_entries_moved_snapshot(), 0);
    }

    #[test]
    fn hlc_advance_events_counter() {
        reset_hlc_advance_events();
        record_hlc_advance();
        record_hlc_advance();
        record_hlc_advance();
        assert_eq!(hlc_advance_events_snapshot(), 3);
        reset_hlc_advance_events();
        assert_eq!(hlc_advance_events_snapshot(), 0);
    }

    #[test]
    fn journal_chain_commit_frames_counter() {
        reset_journal_chain_commit_frames();
        record_journal_chain_commit_frame();
        assert_eq!(journal_chain_commit_frames_snapshot(), 1);
        reset_journal_chain_commit_frames();
        assert_eq!(journal_chain_commit_frames_snapshot(), 0);
    }

    #[test]
    fn force_expire_spin_stalls_counter() {
        reset_force_expire_spin_stalls();
        record_force_expire_spin_stall();
        assert_eq!(force_expire_spin_stalls_snapshot(), 1);
        reset_force_expire_spin_stalls();
        assert_eq!(force_expire_spin_stalls_snapshot(), 0);
    }

    // --- Phase-0 observability counters (unit-level reset/record/snapshot) ---

    #[test]
    fn published_snapshot_rebuilds_counter_unit() {
        reset_published_snapshot_rebuilds();
        record_published_snapshot_rebuild();
        record_published_snapshot_rebuild();
        assert_eq!(published_snapshot_rebuilds_snapshot(), 2);
        reset_published_snapshot_rebuilds();
        assert_eq!(published_snapshot_rebuilds_snapshot(), 0);
    }

    #[test]
    fn crud_commits_root_neutral_counter_unit() {
        reset_crud_commits_root_neutral();
        record_crud_commit_root_neutral();
        assert_eq!(crud_commits_root_neutral_snapshot(), 1);
        reset_crud_commits_root_neutral();
        assert_eq!(crud_commits_root_neutral_snapshot(), 0);
    }

    #[test]
    fn crud_commits_root_changing_counter_unit() {
        reset_crud_commits_root_changing();
        record_crud_commit_root_changing();
        assert_eq!(crud_commits_root_changing_snapshot(), 1);
        reset_crud_commits_root_changing();
        assert_eq!(crud_commits_root_changing_snapshot(), 0);
    }

    #[test]
    fn lane_wait_ns_counter_unit() {
        reset_lane_wait_ns();
        record_lane_wait_ns(500);
        record_lane_wait_ns(0); // no-op
        record_lane_wait_ns(250);
        assert_eq!(lane_wait_ns_snapshot(), 750);
        reset_lane_wait_ns();
        assert_eq!(lane_wait_ns_snapshot(), 0);
    }

    #[test]
    fn recovery_legacy_page_frames_counter_unit() {
        reset_recovery_legacy_page_frames();
        record_recovery_legacy_page_frame();
        record_recovery_legacy_page_frame();
        assert_eq!(recovery_legacy_page_frames_snapshot(), 2);
        reset_recovery_legacy_page_frames();
        assert_eq!(recovery_legacy_page_frames_snapshot(), 0);
    }

    #[test]
    fn recovery_chain_commit_frames_counter_unit() {
        reset_recovery_chain_commit_frames();
        record_recovery_chain_commit_frame();
        assert_eq!(recovery_chain_commit_frames_snapshot(), 1);
        reset_recovery_chain_commit_frames();
        assert_eq!(recovery_chain_commit_frames_snapshot(), 0);
    }

    #[test]
    fn emergency_checkpoint_triggers_counter_unit() {
        reset_emergency_checkpoint_triggers();
        record_emergency_checkpoint_trigger();
        assert_eq!(emergency_checkpoint_triggers_snapshot(), 1);
        reset_emergency_checkpoint_triggers();
        assert_eq!(emergency_checkpoint_triggers_snapshot(), 0);
    }
}
