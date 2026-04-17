//! MVCC counters.
//!
//! T8 formalises 12 mandatory + 5 diagnostic counters; T5' lands the
//! single counter called for by the sub-step 4 acceptance bullet —
//! `mvcc.secondary_index.tombstone_hits_skipped_total`.
//!
//! The counter is a process-global atomic with simple `record()` /
//! `snapshot()` / `reset()` primitives. Tests that want to observe
//! counter transitions should `reset()` first to avoid cross-test
//! interference.

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
}
