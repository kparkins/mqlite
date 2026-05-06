//! FullSync group-commit coordinator for ordinary CRUD writers.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::error::{EngineFatalReason, Error, Result};

use super::state::{poison_after_durable_commit, SharedState};

const GROUP_COMMIT_WAIT_POLL_MS: u64 = 1;
#[cfg(any(test, feature = "test-hooks"))]
const GROUP_COMMIT_TEST_HOOK_WAIT_MS: u64 = 5_000;

/// Coordinates one FullSync fsync for a closed cohort of CRUD writers.
pub(crate) struct GroupCommitManager {
    pub(crate) state: Mutex<GroupCommitState>,
    pub(crate) fsync_completed: Condvar,
    pub(crate) leader_elected: AtomicBool,
    pub(crate) last_fsync_seq: AtomicU64,
}

/// Mutable group-commit state guarded by [`GroupCommitManager::state`].
pub(crate) struct GroupCommitState {
    next_ticket: u64,
    open_cohort_id: u64,
    cohort_open: bool,
    open_cohort_joined: u64,
    closed_high_water: u64,
    failed_high_water: u64,
    failure_reason: Option<EngineFatalReason>,
    wait_window_deadline: Option<Instant>,
}

impl GroupCommitManager {
    /// Create an empty group-commit manager.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(GroupCommitState {
                next_ticket: 0,
                open_cohort_id: 0,
                cohort_open: false,
                open_cohort_joined: 0,
                closed_high_water: 0,
                failed_high_water: 0,
                failure_reason: None,
                wait_window_deadline: None,
            }),
            fsync_completed: Condvar::new(),
            leader_elected: AtomicBool::new(false),
            last_fsync_seq: AtomicU64::new(0),
        }
    }

    /// Join the current FullSync cohort and wait until this writer's
    /// ticket is covered by a leader fsync.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EngineFatal`] if a cohort leader cannot determine
    /// the durable outcome of the closed cohort. The live engine is
    /// poisoned before waiters are notified.
    pub(crate) fn join_fsync_cohort<F>(
        &self,
        shared: &SharedState,
        max_wait: Duration,
        mut fsync_closed_cohort: F,
    ) -> Result<()>
    where
        F: FnMut() -> Result<()>,
    {
        let (ticket, cohort_id) = self.allocate_ticket(max_wait)?;

        loop {
            let mut state = self.state.lock();
            if self.last_fsync_seq.load(Ordering::Acquire) >= ticket {
                return Ok(());
            }
            if let Some(reason) = state.failure_reason.clone() {
                return Err(Error::EngineFatal { reason });
            }

            if state.cohort_open
                && state.open_cohort_id == cohort_id
                && self
                    .leader_elected
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
            {
                drop(state);
                return self.run_leader(shared, cohort_id, max_wait, &mut fsync_closed_cohort);
            }

            self.fsync_completed
                .wait_for(&mut state, Duration::from_millis(GROUP_COMMIT_WAIT_POLL_MS));
        }
    }

    fn allocate_ticket(&self, max_wait: Duration) -> Result<(u64, u64)> {
        let mut state = self.state.lock();
        if let Some(reason) = state.failure_reason.clone() {
            return Err(Error::EngineFatal { reason });
        }
        if !state.cohort_open {
            state.open_cohort_id = state
                .open_cohort_id
                .checked_add(1)
                .ok_or_else(|| Error::Internal("group commit cohort id overflow".into()))?;
            state.cohort_open = true;
            state.open_cohort_joined = 0;
            state.wait_window_deadline = Some(Instant::now() + max_wait);
        }
        state.next_ticket = state
            .next_ticket
            .checked_add(1)
            .ok_or_else(|| Error::Internal("group commit ticket overflow".into()))?;
        let ticket = state.next_ticket;
        let cohort_id = state.open_cohort_id;
        state.open_cohort_joined = state
            .open_cohort_joined
            .checked_add(1)
            .ok_or_else(|| Error::Internal("group commit joined count overflow".into()))?;
        self.fsync_completed.notify_all();
        Ok((ticket, cohort_id))
    }

    fn run_leader<F>(
        &self,
        shared: &SharedState,
        cohort_id: u64,
        max_wait: Duration,
        fsync_closed_cohort: &mut F,
    ) -> Result<()>
    where
        F: FnMut() -> Result<()>,
    {
        #[cfg(any(test, feature = "test-hooks"))]
        let leader_guard = super::group_commit_test_probe::leader_entered();

        let high_water = self.close_current_cohort_after_wait(cohort_id, max_wait)?;

        #[cfg(any(test, feature = "test-hooks"))]
        let fsync_result = if super::group_commit_test_probe::take_fail_next_fsync() {
            Err(Error::Internal(
                "US-017 injected group-commit fsync failure".into(),
            ))
        } else {
            fsync_closed_cohort()
        };
        #[cfg(not(any(test, feature = "test-hooks")))]
        let fsync_result = fsync_closed_cohort();

        if fsync_result.is_err() {
            #[cfg(any(test, feature = "test-hooks"))]
            drop(leader_guard);
            return Err(self.record_failure_and_poison(shared, high_water));
        }

        self.last_fsync_seq.store(high_water, Ordering::Release);
        #[cfg(any(test, feature = "test-hooks"))]
        drop(leader_guard);
        self.leader_elected.store(false, Ordering::Release);
        self.fsync_completed.notify_all();
        Ok(())
    }

    fn close_current_cohort_after_wait(&self, cohort_id: u64, max_wait: Duration) -> Result<u64> {
        let mut state = self.state.lock();
        let production_deadline = Instant::now() + max_wait;
        #[cfg(any(test, feature = "test-hooks"))]
        let test_deadline = Instant::now() + Duration::from_millis(GROUP_COMMIT_TEST_HOOK_WAIT_MS);

        loop {
            if let Some(reason) = state.failure_reason.clone() {
                return Err(Error::EngineFatal { reason });
            }
            if !state.cohort_open || state.open_cohort_id != cohort_id {
                return Ok(state.closed_high_water);
            }

            #[cfg(any(test, feature = "test-hooks"))]
            {
                let joined = state.open_cohort_joined;
                if let Some(expected) = super::group_commit_test_probe::expected_cohort_size() {
                    if joined >= expected {
                        super::group_commit_test_probe::clear_expected_cohort_size();
                        break;
                    }
                    if Instant::now() < test_deadline {
                        self.fsync_completed
                            .wait_for(&mut state, Duration::from_millis(GROUP_COMMIT_WAIT_POLL_MS));
                        continue;
                    }
                }
            }

            if Instant::now() >= production_deadline {
                break;
            }
            self.fsync_completed
                .wait_for(&mut state, Duration::from_millis(GROUP_COMMIT_WAIT_POLL_MS));
        }

        let high_water = state.next_ticket;
        state.cohort_open = false;
        state.closed_high_water = high_water;
        state.open_cohort_joined = 0;
        state.wait_window_deadline = None;
        drop(state);

        #[cfg(any(test, feature = "test-hooks"))]
        super::group_commit_test_probe::pause_after_close_if_installed(cohort_id, high_water);

        Ok(high_water)
    }

    fn record_failure_and_poison(&self, shared: &SharedState, high_water: u64) -> Error {
        let reason = EngineFatalReason::PostDurablePublishFailure;
        {
            let mut state = self.state.lock();
            state.failed_high_water = state.failed_high_water.max(high_water);
            state.failure_reason = Some(reason.clone());
            state.cohort_open = false;
            state.wait_window_deadline = None;
        }
        #[cfg(any(test, feature = "test-hooks"))]
        super::group_commit_test_probe::record_fsync_failure();
        let fatal = poison_after_durable_commit(shared, reason);
        self.leader_elected.store(false, Ordering::Release);
        self.fsync_completed.notify_all();
        fatal
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn test_state_snapshot(&self) -> (bool, u64, u64) {
        let state = self.state.lock();
        (
            self.leader_elected.load(Ordering::Acquire),
            self.last_fsync_seq.load(Ordering::Acquire),
            state.failed_high_water,
        )
    }
}

impl Default for GroupCommitManager {
    fn default() -> Self {
        Self::new()
    }
}
