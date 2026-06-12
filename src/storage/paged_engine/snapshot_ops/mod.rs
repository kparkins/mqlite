//! Snapshot-based read path and engine checkpoint/lifecycle helpers.
//!
//! This module is the directory-form successor of the former
//! `snapshot_ops.rs`. It is split into two siblings:
//!
//! - [`read_exec`] — the mutex-free snapshot read executors
//!   (`open_snapshot_read_view`, the `*_from_snap` plan executors, the
//!   `PrimaryHistoryProbe`, `apply_find_opts`).
//! - [`checkpoint`] — the engine checkpoint driver and its phase functions,
//!   plus `journal_sync` and `snapshot_bytes`.
//!
//! Every path that callers used against the old flat module
//! (`snapshot_ops::checkpoint`, `snapshot_ops::open_snapshot_read_view`,
//! `snapshot_ops::PrimaryHistoryProbe`, `snapshot_ops::checkpoint_stage_failpoint`,
//! …) is preserved here via re-exports so the split is internal-only.

/// F0 failpoint inside the checkpoint spill/relief window. Test plumbing
/// lives in its own file; see the module docs there.
///
/// The `#[path]` reaches up out of this directory into the sibling `tests/`
/// directory of `paged_engine` — the failpoint file did not move with the
/// `snapshot_ops` split.
#[cfg(any(test, feature = "test-hooks"))]
#[path = "../tests/checkpoint_stage_failpoint.rs"]
pub(in crate::storage::paged_engine) mod checkpoint_stage_failpoint;

mod checkpoint;
mod read_exec;

// Re-exports preserving every `snapshot_ops::<symbol>` path used by the
// paged_engine root and sibling modules.
pub(in crate::storage::paged_engine) use checkpoint::{checkpoint, journal_sync, snapshot_bytes};
pub(in crate::storage::paged_engine) use read_exec::{
    apply_find_opts, open_snapshot_read_view, plan_and_collect_snapshot_pairs,
    plan_and_collect_snapshot_pairs_hinted, plan_and_collect_snapshot_pairs_limited,
    primary_history_probe, PrimaryHistoryProbe,
};
// `open_snapshot_read_view_for_epoch` (caller-supplied epoch variant) is
// used only by the F36 / stale-epoch regression tests, which run under
// `cfg(test)`; re-export on the same cfg so non-test builds see no unused
// re-export.
#[cfg(test)]
pub(in crate::storage::paged_engine) use read_exec::open_snapshot_read_view_for_epoch;
// `fetch_primary_pair` is exercised only by the `#[cfg(test)]` bug7
// snapshot-isolation test; re-export it on the same cfg so non-test builds
// (including `--features test-hooks` without `cfg(test)`) see no unused
// re-export.
#[cfg(test)]
pub(in crate::storage::paged_engine) use read_exec::fetch_primary_pair;
