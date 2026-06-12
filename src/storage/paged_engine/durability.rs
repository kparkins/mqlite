//! Durability escalation and interval-sync helpers extracted from
//! `paged_engine.rs`.
//!
//! These methods own the post-reservation / post-durable poison escalation
//! matrix and the interval-mode sync deadline accounting. They are kept out of
//! the root engine file so the commit-envelope poison policy is visible at a
//! glance.

use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::error::{Error, Result};
use crate::options::DurabilityMode;

use super::state;
use super::PagedEngine;

impl PagedEngine {
    /// Commit-envelope failure escalation matrix.
    ///
    /// Three helpers handle failures at different points in the commit pipeline.
    /// Which one to call depends on how far the commit progressed:
    ///
    /// | Failure stage                     | Helper                                  | Cleanup action                                       | Error mapping                          |
    /// |-----------------------------------|-----------------------------------------|------------------------------------------------------|----------------------------------------|
    /// | pre-register (before oracle slot) | (none — just return the error)          | none needed; no state was reserved                   | propagate as-is                        |
    /// | registered, pre-durable           | `cleanup_registered_pre_durable_failure`| flip Pending→Aborted; mark_aborted(slot)             | propagate (EngineFatal → poison first) |
    /// | post-reserve                      | `poison_after_reserved_log_failure`     | poison reserved LSN slot (journaled) or engine direct| always EngineFatal                     |
    /// | post-durable                      | `poison_after_log_manager_failure`      | engine_fatal (irreversible journal state)            | always EngineFatal                     |
    ///
    /// Invariant (load-bearing): `wait_journal_durable` / `wait_journal_ready`
    /// can ONLY return `Error::EngineFatal` — the journal escalates all internal
    /// failures to fatal before surfacing them. This means bare `?` after
    /// `reserved.write_and_mark()` and `wait_for_commit_durability` is sound:
    /// the only possible error already IS EngineFatal, so no cleanup is missed.
    /// See also: `wait_for_commit_durability` doc comment.
    pub(super) fn poison_after_log_manager_failure(&self, error: Error) -> Error {
        if let Error::EngineFatal { reason } = error {
            return state::poison_after_durable_commit(&self.shared, reason);
        }
        error
    }

    /// Post-reservation failures are NEVER abortable: by the time this
    /// helper runs, batch application has started (`commit_lsn_fenced` /
    /// the dirty-LSN stamps that follow it may have applied staged page
    /// bytes), so the in-memory state no longer matches any consistent
    /// pre-failure prefix and cannot be rolled back. Journaled records
    /// poison their reserved LSN slot, which escalates through the log
    /// manager; journal-less handles poison the engine directly. Both arms
    /// surface `Error::EngineFatal` — `is_journaled` only selects HOW the
    /// poison propagates, never WHETHER it happens.
    pub(super) fn poison_after_reserved_log_failure(
        &self,
        reserved: &crate::journal::ReservedLogRecord,
        error: Error,
    ) -> Error {
        if reserved.is_journaled() {
            let fatal = reserved.poison_slot(error);
            return self.poison_after_log_manager_failure(fatal);
        }
        if let Error::EngineFatal { reason } = error {
            return state::poison_after_durable_commit(&self.shared, reason);
        }
        state::poison_after_durable_commit(
            &self.shared,
            crate::error::EngineFatalReason::PostReservationLogWriteFailure,
        )
    }

    /// Wait for the journal to reach the durability level required by this
    /// engine's `DurabilityMode`.
    ///
    /// **EngineFatal invariant**: both `wait_journal_durable` and
    /// `wait_journal_ready` escalate all internal failures to
    /// `Error::EngineFatal` before returning. This means that when callers
    /// write `self.wait_for_commit_durability(lsn)?`, the only possible error
    /// path is an already-fatal engine state — no additional cleanup (flip
    /// Pending→Aborted, mark_aborted) is required. The bare `?` is intentional
    /// and sound. See the escalation-matrix doc comment on
    /// `poison_after_log_manager_failure` for the full picture.
    pub(super) fn wait_for_commit_durability(&self, end_lsn: u64) -> Result<()> {
        let start = Instant::now();
        let (stage, result) = match self.durability_mode {
            DurabilityMode::FullSync => (
                crate::mvcc::metrics::CommitEnvelopeStage::JournalDurableWait,
                self.shared.handle.wait_journal_durable(end_lsn),
            ),
            DurabilityMode::Interval(_) | DurabilityMode::None => (
                crate::mvcc::metrics::CommitEnvelopeStage::JournalReadyWait,
                self.shared.handle.wait_journal_ready(end_lsn),
            ),
        };
        crate::mvcc::metrics::record_commit_envelope_stage_duration(stage, start.elapsed());
        result.map_err(|error| self.poison_after_log_manager_failure(error))
    }

    pub(super) fn maybe_sync_interval_after_publish(&self) -> Result<()> {
        let DurabilityMode::Interval(interval) = self.durability_mode else {
            return Ok(());
        };
        let interval_ms = super::duration_millis_saturating(interval);
        if interval_ms > 0 {
            let now_ms = super::duration_millis_saturating(self.interval_sync_origin.elapsed());
            let mut next_due = self.next_interval_sync_ms.load(Ordering::Acquire);
            loop {
                if now_ms < next_due {
                    return Ok(());
                }
                let next = now_ms.saturating_add(interval_ms);
                match self.next_interval_sync_ms.compare_exchange(
                    next_due,
                    next,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(actual) => next_due = actual,
                }
            }
        }
        let sync_start = Instant::now();
        let result = self.shared.handle.sync_journal_ready_prefix();
        crate::mvcc::metrics::record_commit_envelope_stage_duration(
            crate::mvcc::metrics::CommitEnvelopeStage::IntervalSync,
            sync_start.elapsed(),
        );
        result.map_err(|error| self.poison_after_log_manager_failure(error))
    }
}
