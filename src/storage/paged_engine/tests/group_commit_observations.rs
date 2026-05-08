//! US-017 group-commit test probes.
//!
//! Production group-commit logic lives in the journal `LogManager`; this
//! module owns intrusive rendezvous, failure-injection, and observation state
//! used by integration tests.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;
use std::time::Duration;

static EXPECTED_COHORT_SIZE: AtomicU64 = AtomicU64::new(0);
static FAIL_NEXT_FSYNC: AtomicBool = AtomicBool::new(false);
static ACTIVE_WAITERS: AtomicU64 = AtomicU64::new(0);
static ACTIVE_LEADERS: AtomicU64 = AtomicU64::new(0);
static MAX_ACTIVE_LEADERS: AtomicU64 = AtomicU64::new(0);
static LEADER_ENTRIES: AtomicU64 = AtomicU64::new(0);
static FSYNC_FAILURES: AtomicU64 = AtomicU64::new(0);
static LAST_FSYNC_LSN: AtomicU64 = AtomicU64::new(0);
static FAILED_FSYNC_LSN: AtomicU64 = AtomicU64::new(0);
static NEXT_PROBE_ID: AtomicU64 = AtomicU64::new(1);
static PAUSE_AFTER_CLOSE: Mutex<Option<PauseAfterCloseHook>> = Mutex::new(None);

struct PauseAfterCloseHook {
    cohort_id: Option<u64>,
    entered_tx: Sender<()>,
    release_rx: Receiver<()>,
}

/// Snapshot of US-017 group-commit test observations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[doc(hidden)]
pub struct Us017GroupCommitObservations {
    /// Whether a leader is currently elected.
    pub leader_elected: bool,
    /// Highest LSN frontier covered by a successful fsync.
    pub last_fsync_seq: u64,
    /// Highest LSN frontier covered by a failed fsync.
    pub failed_high_water: u64,
    /// Number of elected leader entries observed by the probe.
    pub leader_entries: u64,
    /// Maximum number of simultaneous leaders observed by the probe.
    pub max_active_leaders: u64,
    /// Number of injected or observed leader fsync failures.
    pub fsync_failures: u64,
}

/// RAII guard for pausing the next cohort leader after close and before fsync.
#[doc(hidden)]
pub struct Us017GroupCommitPauseGuard {
    entered_rx: Receiver<()>,
    release_tx: Option<Sender<()>>,
}

impl Us017GroupCommitPauseGuard {
    /// Wait until the leader has closed its cohort and paused before fsync.
    ///
    /// # Errors
    ///
    /// Returns a timeout or disconnect error if the leader never reaches
    /// the pause point.
    pub fn wait_until_paused_timeout(
        &self,
        timeout: Duration,
    ) -> std::result::Result<(), mpsc::RecvTimeoutError> {
        self.entered_rx.recv_timeout(timeout)
    }

    /// Release the paused leader.
    ///
    /// # Errors
    ///
    /// Returns a send error if the leader is no longer waiting.
    pub fn release(&mut self) -> std::result::Result<(), mpsc::SendError<()>> {
        if let Some(tx) = self.release_tx.take() {
            tx.send(())?;
        }
        Ok(())
    }
}

impl Drop for Us017GroupCommitPauseGuard {
    fn drop(&mut self) {
        let _ = self.release();
        if let Ok(mut hook) = PAUSE_AFTER_CLOSE.lock() {
            *hook = None;
        }
    }
}

pub(crate) struct Us017LeaderGuard;

impl Drop for Us017LeaderGuard {
    fn drop(&mut self) {
        decrement_if_active(&ACTIVE_LEADERS);
    }
}

pub(crate) struct Us017WaiterGuard;

impl Drop for Us017WaiterGuard {
    fn drop(&mut self) {
        decrement_if_active(&ACTIVE_WAITERS);
    }
}

pub(crate) fn reset() {
    EXPECTED_COHORT_SIZE.store(0, Ordering::Release);
    FAIL_NEXT_FSYNC.store(false, Ordering::Release);
    ACTIVE_WAITERS.store(0, Ordering::Release);
    ACTIVE_LEADERS.store(0, Ordering::Release);
    MAX_ACTIVE_LEADERS.store(0, Ordering::Release);
    LEADER_ENTRIES.store(0, Ordering::Release);
    FSYNC_FAILURES.store(0, Ordering::Release);
    LAST_FSYNC_LSN.store(0, Ordering::Release);
    FAILED_FSYNC_LSN.store(0, Ordering::Release);
    if let Ok(mut hook) = PAUSE_AFTER_CLOSE.lock() {
        *hook = None;
    }
}

pub(crate) fn set_expected_cohort_size(expected: u64) {
    EXPECTED_COHORT_SIZE.store(expected, Ordering::Release);
}

pub(crate) fn expected_cohort_size() -> Option<u64> {
    let expected = EXPECTED_COHORT_SIZE.load(Ordering::Acquire);
    (expected != 0).then_some(expected)
}

pub(crate) fn clear_expected_cohort_size() {
    EXPECTED_COHORT_SIZE.store(0, Ordering::Release);
}

pub(crate) fn active_waiters() -> u64 {
    ACTIVE_WAITERS.load(Ordering::Acquire)
}

pub(crate) fn fail_next_fsync() {
    FAIL_NEXT_FSYNC.store(true, Ordering::Release);
}

pub(crate) fn take_fail_next_fsync() -> bool {
    FAIL_NEXT_FSYNC.swap(false, Ordering::AcqRel)
}

pub(crate) fn next_probe_id() -> u64 {
    NEXT_PROBE_ID.fetch_add(1, Ordering::AcqRel)
}

pub(crate) fn install_pause_after_close() -> Us017GroupCommitPauseGuard {
    install_pause_after_close_matching(None)
}

#[cfg(test)]
pub(crate) fn install_pause_after_close_for(cohort_id: u64) -> Us017GroupCommitPauseGuard {
    install_pause_after_close_matching(Some(cohort_id))
}

fn install_pause_after_close_matching(cohort_id: Option<u64>) -> Us017GroupCommitPauseGuard {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    if let Ok(mut hook) = PAUSE_AFTER_CLOSE.lock() {
        *hook = Some(PauseAfterCloseHook {
            cohort_id,
            entered_tx,
            release_rx,
        });
    }
    Us017GroupCommitPauseGuard {
        entered_rx,
        release_tx: Some(release_tx),
    }
}

pub(crate) fn pause_after_close_if_installed(cohort_id: u64, _high_water: u64) {
    let hook = PAUSE_AFTER_CLOSE.lock().ok().and_then(|mut hook| {
        let matches = match hook.as_ref().and_then(|hook| hook.cohort_id) {
            Some(expected) => expected == cohort_id,
            None => hook.is_some(),
        };
        matches.then(|| hook.take()).flatten()
    });
    if let Some(hook) = hook {
        let _ = hook.entered_tx.send(());
        let _ = hook.release_rx.recv();
    }
}

pub(crate) fn leader_entered() -> Us017LeaderGuard {
    let active = ACTIVE_LEADERS.fetch_add(1, Ordering::AcqRel) + 1;
    LEADER_ENTRIES.fetch_add(1, Ordering::AcqRel);
    update_max_active_leaders(active);
    Us017LeaderGuard
}

pub(crate) fn waiter_entered() -> Us017WaiterGuard {
    ACTIVE_WAITERS.fetch_add(1, Ordering::AcqRel);
    Us017WaiterGuard
}

pub(crate) fn record_fsync_success(high_water_lsn: u64) {
    LAST_FSYNC_LSN.store(high_water_lsn, Ordering::Release);
}

pub(crate) fn record_fsync_failure(high_water_lsn: u64) {
    FSYNC_FAILURES.fetch_add(1, Ordering::AcqRel);
    FAILED_FSYNC_LSN.store(high_water_lsn, Ordering::Release);
}

pub(crate) fn observations() -> Us017GroupCommitObservations {
    Us017GroupCommitObservations {
        leader_elected: ACTIVE_LEADERS.load(Ordering::Acquire) != 0,
        last_fsync_seq: LAST_FSYNC_LSN.load(Ordering::Acquire),
        failed_high_water: FAILED_FSYNC_LSN.load(Ordering::Acquire),
        leader_entries: LEADER_ENTRIES.load(Ordering::Acquire),
        max_active_leaders: MAX_ACTIVE_LEADERS.load(Ordering::Acquire),
        fsync_failures: FSYNC_FAILURES.load(Ordering::Acquire),
    }
}

fn update_max_active_leaders(active: u64) {
    let mut current = MAX_ACTIVE_LEADERS.load(Ordering::Acquire);
    while active > current {
        match MAX_ACTIVE_LEADERS.compare_exchange(
            current,
            active,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}

fn decrement_if_active(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
        active.checked_sub(1)
    });
}
