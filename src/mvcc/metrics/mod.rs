//! MVCC counters — 12 mandatory + 5 diagnostic + the Phase-0/Phase-1/Phase-2
//! observability signals.
//!
//! Each counter is a process-global atomic exposed through
//! `record()` / `snapshot()` / `reset()` primitives (or `set()` for gauges).
//! Tests that observe counter transitions should `reset()` first to avoid
//! cross-test interference.
//!
//! This module is a thin facade: the counters are grouped by concern across
//! sibling files and re-exported here so every historical
//! `crate::mvcc::metrics::X` path resolves unchanged.
//!
//! | File | Concern |
//! |------|---------|
//! | [`macros`]   | `define_counter!` / `define_batch_counter!` / `define_gauge!` |
//! | [`core`]     | mandatory (#1–#12) + diagnostic (D1–D5) counters |
//! | [`publish`]  | Phase-0 (P1–P5) + Phase 1 §10.10 publish-decision counters |
//! | [`envelope`] | LogicalTxnFrame recovery-validation + US-024 append signals |
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
//! ## 5 diagnostic counters
//!
//! - `mvcc.reconcile.duration_ms_p99` — gauge
//! - `mvcc.chain_migration.entries_moved_total` — counter
//! - `mvcc.hlc.advance_events_total` — counter
//! - `mvcc.journal.chain_commit_frames_total` — counter
//! - `mvcc.force_expire_spin_stalls_total` — counter
//!
//! Recovery observability counters (`recovery_legacy_page_frames_total`,
//! `recovery_chain_commit_frames_total`) live in
//! [`crate::journal::metrics`] — they describe recovery, not MVCC. They are
//! re-exported below because the `journal` module is crate-private, so
//! integration tests and benches that observe them must reach them through
//! the `#[doc(hidden)] pub mod mvcc` boundary.

pub(crate) mod macros;

mod core;
mod envelope;
mod publish;

pub use core::{
    active_read_views_snapshot, chain_migration_entries_moved_snapshot,
    checkpoint_frontier_blocked_snapshot, deferred_free_queue_depth_snapshot,
    force_expire_spin_stalls_snapshot, history_store_bytes_snapshot,
    history_store_gc_passes_snapshot, hlc_advance_events_snapshot,
    journal_chain_commit_frames_snapshot, journal_commits_snapshot,
    oldest_required_ts_lag_ms_snapshot, overflow_pages_freed_snapshot,
    overflow_pages_in_use_snapshot, overflow_refcount_cas_retries_snapshot,
    read_views_force_expired_snapshot, reconcile_duration_ms_p99_snapshot,
    reconcile_entries_dropped_snapshot, record_chain_migration_entries_moved,
    record_checkpoint_frontier_blocked, record_hlc_advance, record_history_store_gc_pass,
    record_force_expire_spin_stall, record_journal_chain_commit_frame, record_journal_commit,
    record_overflow_page_freed, record_overflow_refcount_cas_retry,
    record_read_view_force_expired, record_reconcile_entries_dropped,
    record_secondary_index_tombstone_hit, reset_active_read_views,
    reset_chain_migration_entries_moved,
    reset_checkpoint_frontier_blocked, reset_deferred_free_queue_depth,
    reset_force_expire_spin_stalls, reset_history_store_bytes, reset_history_store_gc_passes,
    reset_hlc_advance_events, reset_journal_chain_commit_frames, reset_journal_commits,
    reset_oldest_required_ts_lag_ms, reset_overflow_pages_freed, reset_overflow_pages_in_use,
    reset_overflow_refcount_cas_retries, reset_read_views_force_expired,
    reset_reconcile_duration_ms_p99, reset_reconcile_entries_dropped,
    reset_secondary_index_tombstone_hits, reset_version_chain_depth_p99,
    secondary_index_tombstone_hits_snapshot, set_active_read_views,
    set_deferred_free_queue_depth, set_history_store_bytes, set_oldest_required_ts_lag_ms,
    set_overflow_pages_in_use, set_reconcile_duration_ms_p99, set_version_chain_depth_p99,
    version_chain_depth_p99_snapshot,
};
#[cfg(test)]
pub(crate) use core::GC_PASSES_TEST_LOCK;

pub use publish::{
    catalog_header_sync_count_snapshot, commit_envelope_stage_ns_snapshot,
    commit_envelope_stage_samples_snapshot, crud_commits_root_changing_snapshot,
    crud_commits_root_neutral_snapshot, delta_bearing_frames_count_snapshot,
    delta_bearing_frames_ratio_snapshot, emergency_checkpoint_triggers_snapshot,
    published_catalog_rebuild_count_snapshot, published_snapshot_rebuilds_snapshot,
    read_epoch_publish_count_snapshot, record_catalog_header_sync,
    record_commit_envelope_stage_duration, record_commit_envelope_stage_ns,
    record_crud_commit_root_changing, record_crud_commit_root_neutral,
    record_delta_bearing_frame, record_emergency_checkpoint_trigger, record_lane_wait_ns,
    record_published_catalog_rebuild, record_published_snapshot_rebuild,
    record_read_epoch_publish, record_root_neutral_commit, reset_catalog_header_sync_count,
    reset_commit_envelope_stage_metrics, reset_crud_commits_root_changing,
    reset_crud_commits_root_neutral, reset_delta_bearing_frames_count,
    reset_delta_bearing_frames_ratio, reset_emergency_checkpoint_triggers, reset_lane_wait_ns,
    reset_published_catalog_rebuild_count, reset_published_snapshot_rebuilds,
    reset_read_epoch_publish_count, reset_root_neutral_commit_count,
    root_neutral_commit_count_snapshot, set_delta_bearing_frames_ratio,
    lane_wait_ns_snapshot, CommitEnvelopeStage,
};

pub use envelope::{
    logical_txn_append_bytes_snapshot, logical_txn_append_duration_ms_p50_snapshot,
    logical_txn_append_duration_ms_p95_snapshot, logical_txn_append_duration_ms_p99_snapshot,
    logical_txn_pass1_orphan_logical_dropped_snapshot,
    logical_txn_pass1_pre_boundary_dropped_snapshot,
    logical_txn_pass1_unmatched_chain_commit_snapshot,
    logical_txn_pass2_resolved_ops_snapshot, logical_txn_pass2_unresolved_ops_snapshot,
    logical_txn_recovery_discarded_frames_snapshot, logical_txn_torn_frames_snapshot,
    parsed_logical_frames_len_snapshot, record_logical_txn_append_bytes,
    record_logical_txn_append_duration_ms, record_logical_txn_append_duration_ms_and_maybe_recompute,
    record_logical_txn_pass1_orphan_logical_dropped,
    record_logical_txn_pass1_pre_boundary_dropped,
    record_logical_txn_pass1_unmatched_chain_commit, record_logical_txn_pass2_resolved_op,
    record_logical_txn_pass2_unresolved_op, record_logical_txn_recovery_discarded_frame,
    record_logical_txn_torn_frame, recompute_logical_txn_append_percentiles,
    reset_logical_txn_append_bytes, reset_logical_txn_append_durations,
    reset_logical_txn_pass1_orphan_logical_dropped,
    reset_logical_txn_pass1_pre_boundary_dropped,
    reset_logical_txn_pass1_unmatched_chain_commit, reset_logical_txn_pass2_resolved_ops,
    reset_logical_txn_pass2_unresolved_ops, reset_logical_txn_recovery_discarded_frames,
    reset_logical_txn_torn_frames, reset_parsed_logical_frames_len, set_parsed_logical_frames_len,
};

// Recovery observability counters now live in `crate::journal::metrics`. The
// `journal` module is crate-private, so integration tests and benches reach
// these through the `pub mod mvcc` boundary — keep the re-export shim.
pub use crate::journal::metrics::{
    record_recovery_chain_commit_frame, record_recovery_legacy_page_frame,
    recovery_chain_commit_frames_snapshot, recovery_legacy_page_frames_snapshot,
    reset_recovery_chain_commit_frames, reset_recovery_legacy_page_frames,
};

#[cfg(test)]
#[cfg(not(loom))]
#[path = "../tests/metrics.rs"]
mod tests;
