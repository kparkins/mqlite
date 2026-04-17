//! MVCC counters — T8 finalises the 12 mandatory + 5 diagnostic counters.
//!
//! Each counter is a process-global atomic exposed through
//! `record()` / `snapshot()` / `reset()` primitives (or `set()` for gauges).
//! Tests that observe counter transitions should `reset()` first to avoid
//! cross-test interference.
//!
//! ## 12 mandatory counters (plan §T8 table, iter-5)
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
//! ## 5 diagnostic counters
//!
//! - `mvcc.reconcile.duration_ms_p99` — gauge
//! - `mvcc.chain_migration.entries_moved_total` — counter
//! - `mvcc.hlc.advance_events_total` — counter
//! - `mvcc.journal.chain_commit_frames_total` — counter
//! - `mvcc.force_expire_spin_stalls_total` — counter

use std::sync::atomic::{AtomicU64, Ordering};

// ===========================================================================
// 8 — mvcc.secondary_index.tombstone_hits_skipped_total  (counter, T5')
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
// 5 — mvcc.reconcile.entries_dropped_total  (counter, T6)
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
// (T6 extra) — mvcc.overflow.pages_freed_total
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
// 12 — mvcc.deferred_free_queue_depth  (gauge, T6)
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

/// Latest observed p99 of per-frame `version_chains[key].len()`. Caller
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
/// on `HISTORY_STORE_GC_PASSES_TOTAL`. Because tests run in parallel within a
/// single process, concurrent reset-then-assert sequences on the same global
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
// Tests
// ===========================================================================

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    // --- Pre-T8 counters (kept verbatim) ---

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

    // --- T8 mandatory counters (1–4, 6, 7, 9, 10, 11) ---

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

    // --- T8 diagnostic counters (D1–D5) ---

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
}
