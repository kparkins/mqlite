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
fn commit_envelope_stage_timing_unit() {
    reset_commit_envelope_stage_metrics();
    assert_eq!(
        commit_envelope_stage_ns_snapshot(CommitEnvelopeStage::LogReserve),
        0
    );
    assert_eq!(
        commit_envelope_stage_samples_snapshot(CommitEnvelopeStage::LogReserve),
        0
    );
    record_commit_envelope_stage_ns(CommitEnvelopeStage::LogReserve, 500);
    record_commit_envelope_stage_ns(CommitEnvelopeStage::LogReserve, 250);
    assert_eq!(
        commit_envelope_stage_ns_snapshot(CommitEnvelopeStage::LogReserve),
        750
    );
    assert_eq!(
        commit_envelope_stage_samples_snapshot(CommitEnvelopeStage::LogReserve),
        2
    );
    reset_commit_envelope_stage_metrics();
    assert_eq!(
        commit_envelope_stage_ns_snapshot(CommitEnvelopeStage::LogReserve),
        0
    );
    assert_eq!(
        commit_envelope_stage_samples_snapshot(CommitEnvelopeStage::LogReserve),
        0
    );
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
