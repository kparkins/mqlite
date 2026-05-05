//! US-017 group-commit test probes.
//!
//! Production group-commit logic lives in `group_commit.rs`; this module
//! owns intrusive rendezvous, failure-injection, and observation state used
//! by integration tests.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Mutex;
use std::time::Duration;

use super::group_commit::GroupCommitManager;

static EXPECTED_COHORT_SIZE: AtomicU64 = AtomicU64::new(0);
static FAIL_NEXT_FSYNC: AtomicBool = AtomicBool::new(false);
static ACTIVE_LEADERS: AtomicU64 = AtomicU64::new(0);
static MAX_ACTIVE_LEADERS: AtomicU64 = AtomicU64::new(0);
static LEADER_ENTRIES: AtomicU64 = AtomicU64::new(0);
static FSYNC_FAILURES: AtomicU64 = AtomicU64::new(0);
static PAUSE_AFTER_CLOSE: Mutex<Option<PauseAfterCloseHook>> = Mutex::new(None);

struct PauseAfterCloseHook {
    entered_tx: Sender<()>,
    release_rx: Receiver<()>,
}

/// Snapshot of US-017 group-commit test observations.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[doc(hidden)]
pub struct Us017GroupCommitObservations {
    /// Whether a leader is currently elected.
    pub leader_elected: bool,
    /// Highest ticket covered by a successful fsync.
    pub last_fsync_seq: u64,
    /// Highest ticket covered by a failed fsync cohort.
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
        ACTIVE_LEADERS.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) fn reset() {
    EXPECTED_COHORT_SIZE.store(0, Ordering::Release);
    FAIL_NEXT_FSYNC.store(false, Ordering::Release);
    ACTIVE_LEADERS.store(0, Ordering::Release);
    MAX_ACTIVE_LEADERS.store(0, Ordering::Release);
    LEADER_ENTRIES.store(0, Ordering::Release);
    FSYNC_FAILURES.store(0, Ordering::Release);
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

pub(crate) fn fail_next_fsync() {
    FAIL_NEXT_FSYNC.store(true, Ordering::Release);
}

pub(crate) fn take_fail_next_fsync() -> bool {
    FAIL_NEXT_FSYNC.swap(false, Ordering::AcqRel)
}

pub(crate) fn install_pause_after_close() -> Us017GroupCommitPauseGuard {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    if let Ok(mut hook) = PAUSE_AFTER_CLOSE.lock() {
        *hook = Some(PauseAfterCloseHook {
            entered_tx,
            release_rx,
        });
    }
    Us017GroupCommitPauseGuard {
        entered_rx,
        release_tx: Some(release_tx),
    }
}

pub(crate) fn pause_after_close_if_installed(_cohort_id: u64, _high_water: u64) {
    let hook = PAUSE_AFTER_CLOSE
        .lock()
        .ok()
        .and_then(|mut hook| hook.take());
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

pub(crate) fn record_fsync_failure() {
    FSYNC_FAILURES.fetch_add(1, Ordering::AcqRel);
}

pub(crate) fn observations(manager: &GroupCommitManager) -> Us017GroupCommitObservations {
    let (leader_elected, last_fsync_seq, failed_high_water) = manager.test_state_snapshot();
    Us017GroupCommitObservations {
        leader_elected,
        last_fsync_seq,
        failed_high_water,
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
