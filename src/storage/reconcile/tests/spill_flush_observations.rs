//! F1 test-only spill-flush accounting for checkpoint reconcile.
//!
//! Every non-empty `commit_staged_history_spills` call performs exactly one
//! durable two-pool LSN-fenced flush (`commit_spill_txn_durable` →
//! `BufferPoolHandle::flush`) inside the checkpoint writer-exclusion window.
//! The F1 regression pin counts those calls to assert the chunked
//! cross-tree batching bound (`ceil(spill_pages / chunk)`) instead of the
//! pre-fix one-flush-per-namespace multiplier. Kept in its own file so the
//! intrusive counter stays out of the production reconcile driver.
//!
//! The counter is thread-local: reconcile runs on the thread that called
//! `PagedEngine::checkpoint`, so one test's counts can never bleed into a
//! concurrently running test's checkpoint.

use std::cell::Cell;

thread_local! {
    static SPILL_COMMIT_FLUSHES: Cell<u64> = const { Cell::new(0) };
}

/// Reset this thread's spill-commit flush counter.
#[allow(
    dead_code,
    reason = "read only by cfg(test) suites; compiled under test-hooks for parity"
)]
pub(crate) fn reset_spill_commit_flushes() {
    SPILL_COMMIT_FLUSHES.with(|count| count.set(0));
}

/// Return how many durable spill commits this thread has performed since
/// the last reset.
#[allow(
    dead_code,
    reason = "read only by cfg(test) suites; compiled under test-hooks for parity"
)]
pub(crate) fn spill_commit_flushes() -> u64 {
    SPILL_COMMIT_FLUSHES.with(|count| count.get())
}

/// Record one non-empty durable spill commit (one spill-path flush).
pub(crate) fn record_spill_commit_flush() {
    SPILL_COMMIT_FLUSHES.with(|count| count.set(count.get() + 1));
}
