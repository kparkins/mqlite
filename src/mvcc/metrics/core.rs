//! Mandatory (#1–#12) and diagnostic (D1–D5) MVCC counters.
//!
//! Each counter is a process-global atomic exposed through
//! `record()` / `snapshot()` / `reset()` primitives (or `set()` for gauges).
//! Tests that observe counter transitions should `reset()` first to avoid
//! cross-test interference.

use std::sync::atomic::{AtomicU64, Ordering};

use super::macros::{define_batch_counter, define_counter, define_gauge};

// 8 — mvcc.secondary_index.tombstone_hits_skipped_total  (counter)
define_counter!(
    /// Incremented each time a reader observes a tombstone entry in a
    /// secondary-index chain and skips the underlying primary fetch.
    SECONDARY_INDEX_TOMBSTONE_HITS_SKIPPED_TOTAL,
    /// Record one sec-index tombstone-elision event.
    record_secondary_index_tombstone_hit,
    /// Snapshot the current counter value.
    secondary_index_tombstone_hits_snapshot,
    /// Reset the counter to zero. Primarily for tests.
    reset_secondary_index_tombstone_hits,
);

// 5 — mvcc.reconcile.entries_dropped_total  (counter)
define_batch_counter!(
    /// Number of `VersionEntry` objects dropped from per-frame version chains
    /// by `BufferPool::reconcile` (entries whose `stop_ts <= oldest_required_ts`).
    RECONCILE_ENTRIES_DROPPED_TOTAL,
    /// Record N entries dropped by one reconcile pass.
    record_reconcile_entries_dropped(count: u64),
    /// Snapshot the reconcile-dropped counter.
    reconcile_entries_dropped_snapshot,
    /// Reset the reconcile-dropped counter.
    reset_reconcile_entries_dropped,
);

// mvcc.checkpoint.frontier_blocked_total (counter)
define_counter!(
    /// Number of checkpoint attempts blocked before mutation because at least one
    /// checkpoint-visible dirty leaf could not safely advance the durable frontier.
    CHECKPOINT_FRONTIER_BLOCKED_TOTAL,
    /// Record one mutation-free checkpoint frontier blocker.
    record_checkpoint_frontier_blocked,
    /// Snapshot the checkpoint-frontier-blocked counter.
    checkpoint_frontier_blocked_snapshot,
    /// Reset the checkpoint-frontier-blocked counter.
    reset_checkpoint_frontier_blocked,
);

// mvcc.overflow.pages_freed_total  (counter)
define_counter!(
    /// Number of overflow pages that `AllocatorHandle::drain_free_queue` has
    /// returned to the free list.
    OVERFLOW_PAGES_FREED_TOTAL,
    /// Record one overflow-page free (called from the drain path per freed page).
    record_overflow_page_freed,
    /// Snapshot the overflow-freed counter.
    overflow_pages_freed_snapshot,
    /// Reset the overflow-freed counter.
    reset_overflow_pages_freed,
);

// 12 — mvcc.deferred_free_queue_depth  (gauge)
define_gauge!(
    /// Current depth of the deferred-free queue. Gauge — set to the queue's
    /// size after every drain cycle.
    DEFERRED_FREE_QUEUE_DEPTH,
    /// Set the deferred-free-queue depth gauge.
    set_deferred_free_queue_depth(depth: u64),
    /// Snapshot the deferred-free-queue depth gauge.
    deferred_free_queue_depth_snapshot,
    /// Reset the deferred-free-queue depth gauge.
    reset_deferred_free_queue_depth,
);

// 1 — mvcc.oldest_required_ts_lag_ms  (gauge)
define_gauge!(
    /// How far behind wall-clock the `oldest_required_ts` horizon is, in
    /// milliseconds. Set by whoever computes the lag (typically checkpoint
    /// or GC). Elevated values with zero active readers ⇒ stuck oracle.
    OLDEST_REQUIRED_TS_LAG_MS,
    /// Set the ort-lag gauge.
    set_oldest_required_ts_lag_ms(lag_ms: u64),
    /// Snapshot the ort-lag gauge.
    oldest_required_ts_lag_ms_snapshot,
    /// Reset the ort-lag gauge.
    reset_oldest_required_ts_lag_ms,
);

// 2 — mvcc.active_read_views  (gauge)
define_gauge!(
    /// Current number of live `ReadView`s registered with `ReadViewRegistry`.
    /// Updated by `ReadViewRegistry::{register,unregister}`.
    ACTIVE_READ_VIEWS,
    /// Set the active-read-views gauge.
    set_active_read_views(n: u64),
    /// Snapshot the active-read-views gauge.
    active_read_views_snapshot,
    /// Reset the active-read-views gauge.
    reset_active_read_views,
);

// 3 — mvcc.version_chain_depth_p99  (gauge)
define_gauge!(
    /// Latest observed p99 of per-frame `deltas[key].len()`. Caller
    /// samples a histogram and publishes the p99 here.
    VERSION_CHAIN_DEPTH_P99,
    /// Set the version-chain-depth p99 gauge.
    set_version_chain_depth_p99(depth: u64),
    /// Snapshot the version-chain-depth p99 gauge.
    version_chain_depth_p99_snapshot,
    /// Reset the version-chain-depth p99 gauge.
    reset_version_chain_depth_p99,
);

// 4 — mvcc.history_store_bytes  (gauge)
define_gauge!(
    /// Current size of the history store in bytes (approximate — sampled after
    /// GC or checkpoint).
    HISTORY_STORE_BYTES,
    /// Set the history-store-bytes gauge.
    set_history_store_bytes(bytes: u64),
    /// Snapshot the history-store-bytes gauge.
    history_store_bytes_snapshot,
    /// Reset the history-store-bytes gauge.
    reset_history_store_bytes,
);

// 6 — mvcc.journal.commits_total  (counter)
define_counter!(
    /// Total number of journal commit-frame emissions (one per successful
    /// `with_txn` commit path).
    JOURNAL_COMMITS_TOTAL,
    /// Record one journal commit.
    record_journal_commit,
    /// Snapshot the journal-commits counter.
    journal_commits_snapshot,
    /// Reset the journal-commits counter.
    reset_journal_commits,
);

// 7 — mvcc.read_views_force_expired_total  (counter)
define_counter!(
    /// Total number of ReadViews that were force-expired by `force_expire`.
    READ_VIEWS_FORCE_EXPIRED_TOTAL,
    /// Record one force-expire event.
    record_read_view_force_expired,
    /// Snapshot the force-expired counter.
    read_views_force_expired_snapshot,
    /// Reset the force-expired counter.
    reset_read_views_force_expired,
);

// 9 — mvcc.overflow.pages_in_use  (gauge)
define_gauge!(
    /// Number of overflow chains with refcount ≥ 1. Gauge, sampled from the
    /// allocator's per-chain refcount table.
    OVERFLOW_PAGES_IN_USE,
    /// Set the overflow-pages-in-use gauge.
    set_overflow_pages_in_use(n: u64),
    /// Snapshot the overflow-pages-in-use gauge.
    overflow_pages_in_use_snapshot,
    /// Reset the overflow-pages-in-use gauge.
    reset_overflow_pages_in_use,
);

// 10 — mvcc.overflow.refcount_cas_retries_total  (counter)
define_counter!(
    /// Total number of observed CAS failures inside `incref_overflow`'s loop.
    /// Each retry here is one contended CAS.
    OVERFLOW_REFCOUNT_CAS_RETRIES_TOTAL,
    /// Record one CAS retry in `incref_overflow`.
    record_overflow_refcount_cas_retry,
    /// Snapshot the CAS-retries counter.
    overflow_refcount_cas_retries_snapshot,
    /// Reset the CAS-retries counter.
    reset_overflow_refcount_cas_retries,
);

// 11 — mvcc.history_store.gc_passes_total  (counter)
define_counter!(
    /// Total number of completed history-store GC passes.
    HISTORY_STORE_GC_PASSES_TOTAL,
    /// Record one completed GC pass.
    record_history_store_gc_pass,
    /// Snapshot the GC-passes counter.
    history_store_gc_passes_snapshot,
    /// Reset the GC-passes counter.
    reset_history_store_gc_passes,
);

/// Test-only serialization lock shared by every test that resets and asserts
/// on `HISTORY_STORE_GC_PASSES_TOTAL`. Tests run in parallel within a single
/// process, so concurrent reset-then-assert sequences on the same global
/// counter race. All tests that reset this counter must hold this mutex for
/// the duration of their reset → record → snapshot sequence.
#[cfg(test)]
pub(crate) static GC_PASSES_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

// D1 — mvcc.reconcile.duration_ms_p99  (gauge, diagnostic)
define_gauge!(
    /// Latest observed p99 of reconcile-pass durations in milliseconds.
    RECONCILE_DURATION_MS_P99,
    /// Set the reconcile-duration p99 gauge.
    set_reconcile_duration_ms_p99(ms: u64),
    /// Snapshot the reconcile-duration p99 gauge.
    reconcile_duration_ms_p99_snapshot,
    /// Reset the reconcile-duration p99 gauge.
    reset_reconcile_duration_ms_p99,
);

// D2 — mvcc.chain_migration.entries_moved_total  (counter, diagnostic)
define_batch_counter!(
    /// Entries moved from in-memory chains to the history store or across
    /// splits. Incremented by migration code.
    CHAIN_MIGRATION_ENTRIES_MOVED_TOTAL,
    /// Record N migrated entries.
    record_chain_migration_entries_moved(count: u64),
    /// Snapshot the chain-migration counter.
    chain_migration_entries_moved_snapshot,
    /// Reset the chain-migration counter.
    reset_chain_migration_entries_moved,
);

// D3 — mvcc.hlc.advance_events_total  (counter, diagnostic)
define_counter!(
    /// Number of `TimestampOracle::advance` invocations. Single-node always
    /// zero; multi-node replication increments.
    HLC_ADVANCE_EVENTS_TOTAL,
    /// Record one oracle-advance event.
    record_hlc_advance,
    /// Snapshot the hlc-advance counter.
    hlc_advance_events_snapshot,
    /// Reset the hlc-advance counter.
    reset_hlc_advance_events,
);

// D4 — mvcc.journal.chain_commit_frames_total  (counter, diagnostic)
define_counter!(
    /// Total number of ChainCommit frames appended to the journal.
    JOURNAL_CHAIN_COMMIT_FRAMES_TOTAL,
    /// Record one ChainCommit frame append.
    record_journal_chain_commit_frame,
    /// Snapshot the chain-commit-frames counter.
    journal_chain_commit_frames_snapshot,
    /// Reset the chain-commit-frames counter.
    reset_journal_chain_commit_frames,
);

// D5 — mvcc.force_expire_spin_stalls_total  (counter, diagnostic)
define_counter!(
    /// Number of times `ReadView::wait_pin_drain` spun past `TIMEOUT_MS`
    /// waiting for `pin_ops_in_flight` to drain.
    FORCE_EXPIRE_SPIN_STALLS_TOTAL,
    /// Record one force-expire spin-stall event.
    record_force_expire_spin_stall,
    /// Snapshot the force-expire spin-stalls counter.
    force_expire_spin_stalls_snapshot,
    /// Reset the force-expire spin-stalls counter.
    reset_force_expire_spin_stalls,
);
