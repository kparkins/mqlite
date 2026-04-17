//! MVCC counters.
//!
//! T8 formalises 12 mandatory + 5 diagnostic counters; T5' lands the
//! single counter called for by the sub-step 4 acceptance bullet —
//! `mvcc.secondary_index.tombstone_hits_skipped_total`. T6 adds three
//! more: `mvcc.reconcile.entries_dropped_total`,
//! `mvcc.overflow.pages_freed_total`, and `mvcc.deferred_free_queue_depth`.
//!
//! Each counter is a process-global atomic with `record()` / `snapshot()` /
//! `reset()` primitives. Tests that want to observe counter transitions
//! should `reset()` first to avoid cross-test interference.

use std::sync::atomic::{AtomicU64, Ordering};

/// Incremented each time a reader observes a tombstone entry in a
/// secondary-index chain and skips the underlying primary fetch. The
/// counter is the lightweight proxy T5' uses to assert that sec-index
/// visibility is respecting tombstones.
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

// ---------------------------------------------------------------------------
// T6 — Reconciliation counters
// ---------------------------------------------------------------------------

/// Number of `VersionEntry` objects dropped from per-frame version chains
/// by `BufferPool::reconcile` (entries whose `stop_ts <= oldest_required_ts`).
pub static RECONCILE_ENTRIES_DROPPED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Number of overflow pages that `AllocatorHandle::drain_free_queue` has
/// returned to the free list (one tick per actually-freed page; requeued
/// pages do not tick).
pub static OVERFLOW_PAGES_FREED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Current depth of the deferred-free queue. Gauge (not a counter) — set
/// to the queue's size after every drain cycle.
pub static DEFERRED_FREE_QUEUE_DEPTH: AtomicU64 = AtomicU64::new(0);

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

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

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
}
