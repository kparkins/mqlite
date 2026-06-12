//! Recovery observability counters.
//!
//! These two counters describe the journal recovery loop, not the MVCC
//! version store, so they live alongside the journal. Each is a
//! process-global atomic exposed through `record()` / `snapshot()` /
//! `reset()`; tests that observe transitions should `reset()` first to avoid
//! cross-test interference.
//!
//! They are re-exported from [`crate::mvcc::metrics`] for the integration
//! tests and benches that reach them through the `#[doc(hidden)] pub mod mvcc`
//! boundary (the `journal` module is crate-private).

use std::sync::atomic::{AtomicU64, Ordering};

use crate::mvcc::metrics::macros::define_counter;

// P4a — recovery_legacy_page_frames_total  (counter)
define_counter!(
    /// Total number of retired page-replay records processed by older recovery
    /// loops.
    ///
    /// Observation only — write-side updates are lock-free atomics; snapshot/reset
    /// calls are test/admin surfaces and must not race with active writers.
    RECOVERY_LEGACY_PAGE_FRAMES_TOTAL,
    /// Record one legacy page-frame seen by recovery.
    record_recovery_legacy_page_frame,
    /// Snapshot the legacy-page-frames recovery counter.
    recovery_legacy_page_frames_snapshot,
    /// Reset the legacy-page-frames recovery counter.
    reset_recovery_legacy_page_frames,
);

// P4b — recovery_chain_commit_frames_total  (counter)
define_counter!(
    /// Total number of `ChainCommit` frames processed by the recovery loop.
    ///
    /// Observation only — write-side updates are lock-free atomics; snapshot/reset
    /// calls are test/admin surfaces and must not race with active writers.
    RECOVERY_CHAIN_COMMIT_FRAMES_TOTAL,
    /// Record one ChainCommit frame seen by recovery.
    record_recovery_chain_commit_frame,
    /// Snapshot the ChainCommit-frames recovery counter.
    recovery_chain_commit_frames_snapshot,
    /// Reset the ChainCommit-frames recovery counter.
    reset_recovery_chain_commit_frames,
);
