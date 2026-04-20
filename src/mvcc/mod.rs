//! MVCC (multi-version concurrency control) subsystem.
//!
//! This module hosts the WiredTiger-style in-memory version chain,
//! Hybrid Logical Clock timestamp oracle, read-view registry, and
//! reconciliation / deferred-free plumbing.

pub mod deferred_free;
pub mod metrics;
pub mod read_view;
pub mod timestamp;
pub mod transaction;
pub mod version;

#[allow(unused_imports)]
pub use deferred_free::DeferredFreeQueue;
#[allow(unused_imports)]
pub use metrics::{
    active_read_views_snapshot, chain_migration_entries_moved_snapshot,
    deferred_free_queue_depth_snapshot, force_expire_spin_stalls_snapshot,
    history_store_bytes_snapshot, history_store_gc_passes_snapshot,
    hlc_advance_events_snapshot, journal_chain_commit_frames_snapshot,
    journal_commits_snapshot, oldest_required_ts_lag_ms_snapshot,
    overflow_pages_freed_snapshot, overflow_pages_in_use_snapshot,
    overflow_refcount_cas_retries_snapshot, read_views_force_expired_snapshot,
    reconcile_duration_ms_p99_snapshot, reconcile_entries_dropped_snapshot,
    record_chain_migration_entries_moved, record_force_expire_spin_stall,
    record_history_store_gc_pass, record_hlc_advance,
    record_journal_chain_commit_frame, record_journal_commit,
    record_overflow_page_freed, record_overflow_refcount_cas_retry,
    record_read_view_force_expired, record_reconcile_entries_dropped,
    record_secondary_index_tombstone_hit, reset_active_read_views,
    reset_chain_migration_entries_moved, reset_deferred_free_queue_depth,
    reset_force_expire_spin_stalls, reset_history_store_bytes,
    reset_history_store_gc_passes, reset_hlc_advance_events,
    reset_journal_chain_commit_frames, reset_journal_commits,
    reset_oldest_required_ts_lag_ms, reset_overflow_pages_freed,
    reset_overflow_pages_in_use, reset_overflow_refcount_cas_retries,
    reset_read_views_force_expired, reset_reconcile_duration_ms_p99,
    reset_reconcile_entries_dropped, reset_secondary_index_tombstone_hits,
    reset_version_chain_depth_p99, secondary_index_tombstone_hits_snapshot,
    set_active_read_views, set_deferred_free_queue_depth,
    set_history_store_bytes, set_oldest_required_ts_lag_ms,
    set_overflow_pages_in_use, set_reconcile_duration_ms_p99,
    set_version_chain_depth_p99, version_chain_depth_p99_snapshot,
};
#[allow(unused_imports)]
pub use read_view::{ChainSnapshot, ReadView, ReadViewRegistry};
#[allow(unused_imports)]
pub use timestamp::{HlcState, TimestampOracle, Ts};
#[allow(unused_imports)]
pub(crate) use transaction::{PrimaryOp, PrimaryWrite, SecIndexOp, SecIndexWrite, WriteTxn};
#[allow(unused_imports)]
pub use version::{OverflowRef, VersionData, VersionEntry};
