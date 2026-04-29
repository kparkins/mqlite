//! Test-only `impl PagedEngine` accessors — NOT part of the
//! production path.
//!
//! Every method here exists to let integration or unit tests observe
//! engine state that is deliberately kept out of the public
//! `StorageEngine` trait's production surface. They are reached from
//! integration tests through `#[doc(hidden)]` `Client::__*` accessors
//! in `src/client/test_accessors.rs`. None of these methods should
//! ever be invoked by application code.
//!
//! Isolated into its own file so the boundary between production
//! behavior (src/storage/paged_engine.rs) and test scaffolding is
//! visible at a glance. Matches the Phase 0 convention of keeping
//! test helpers out of the primary code path.

#[cfg(test)]
use std::sync::Barrier;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, Sender, TryRecvError},
    Arc,
};

#[cfg(any(test, feature = "test-hooks"))]
use super::state::SharedState;
use super::PagedEngine;
use crate::error::{Error, Result};

#[cfg(any(test, feature = "test-hooks"))]
static PHASE3_COMMIT_FAILPOINT: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

/// Phase 3 US-021b failpoints at the live commit-envelope boundaries.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase3CommitFailpoint {
    /// Before appending the `LogicalTxnFrame` at S5/S6.
    BeforeLogicalTxnAppend,
    /// After logical append returns and before the explicit S6 fsync.
    AfterLogicalTxnAppendBeforeFsync,
    /// After S6 fsync and before the S7 `ChainCommit`.
    AfterLogicalTxnFsyncBeforeChainCommit,
    /// After S7 `ChainCommit` and before S8/S9/S11 legacy effects.
    AfterChainCommitBeforeLegacyCommit,
    /// After S11 legacy effects and before S12 publish.
    AfterLegacyCommitBeforePublish,
    /// During S12 publish, immediately before the `PublishedEpoch` store.
    DuringPublishBeforeStore,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Phase3CommitFailpoint {
    /// Parse the stable environment name used by the self-reexec harness.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "before_logical_txn_append" => Some(Self::BeforeLogicalTxnAppend),
            "after_logical_txn_append_before_fsync" => Some(Self::AfterLogicalTxnAppendBeforeFsync),
            "after_logical_txn_fsync_before_chain_commit" => {
                Some(Self::AfterLogicalTxnFsyncBeforeChainCommit)
            }
            "after_chain_commit_before_legacy_commit" => {
                Some(Self::AfterChainCommitBeforeLegacyCommit)
            }
            "after_legacy_commit_before_publish" => Some(Self::AfterLegacyCommitBeforePublish),
            "during_publish_before_store" => Some(Self::DuringPublishBeforeStore),
            _ => None,
        }
    }

    fn slot(self) -> u8 {
        match self {
            Self::BeforeLogicalTxnAppend => 1,
            Self::AfterLogicalTxnAppendBeforeFsync => 2,
            Self::AfterLogicalTxnFsyncBeforeChainCommit => 3,
            Self::AfterChainCommitBeforeLegacyCommit => 4,
            Self::AfterLegacyCommitBeforePublish => 5,
            Self::DuringPublishBeforeStore => 6,
        }
    }
}

/// RAII guard that clears the armed Phase 3 failpoint on drop.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub struct Phase3CommitFailpointGuard {
    slot: u8,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for Phase3CommitFailpointGuard {
    fn drop(&mut self) {
        let _ = PHASE3_COMMIT_FAILPOINT.compare_exchange(
            self.slot,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

/// Arm one exclusive Phase 3 commit failpoint for the current process.
///
/// # Errors
///
/// Returns [`Error::Internal`] if another Phase 3 failpoint is already armed.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn arm_phase3_commit_failpoint(
    failpoint: Phase3CommitFailpoint,
) -> Result<Phase3CommitFailpointGuard> {
    let slot = failpoint.slot();
    PHASE3_COMMIT_FAILPOINT
        .compare_exchange(0, slot, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| Error::Internal("Phase 3 commit failpoint already armed".into()))?;
    Ok(Phase3CommitFailpointGuard { slot })
}

/// Abort the process if the requested Phase 3 commit failpoint is armed.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn phase3_abort_if_armed(failpoint: Phase3CommitFailpoint) {
    if PHASE3_COMMIT_FAILPOINT.load(Ordering::Acquire) == failpoint.slot() {
        std::process::abort();
    }
}

// ---------------------------------------------------------------------------
// US-021c write-body entry hook — test-only namespace-lane rendezvous.
// ---------------------------------------------------------------------------

/// Event emitted when a writer reaches the test-only body-entry hook.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub struct WriteBodyEntryEvent {
    observed_flag: Option<bool>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl WriteBodyEntryEvent {
    /// Return the optional boolean probe observed at body-entry time.
    #[must_use]
    pub fn observed_flag(&self) -> Option<bool> {
        self.observed_flag
    }
}

/// Pending test-only hook consumed by the next write body for a namespace.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct WriteBodyEntryHook {
    id: u64,
    entered_tx: Sender<WriteBodyEntryEvent>,
    release_rx: Receiver<()>,
    observe_flag: Option<Arc<AtomicBool>>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl WriteBodyEntryHook {
    fn fire(self) {
        let event = WriteBodyEntryEvent {
            observed_flag: self
                .observe_flag
                .as_ref()
                .map(|flag| flag.load(Ordering::Acquire)),
        };
        if self.entered_tx.send(event).is_ok() {
            // If the guard is dropped during test unwinding, the release
            // channel closes and the writer must not deadlock here.
            let _ = self.release_rx.recv();
        }
    }
}

/// RAII guard for a namespace write-body entry hook.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub struct WriteBodyEntryHookGuard {
    shared: Arc<SharedState>,
    ns: String,
    id: u64,
    entered_rx: Receiver<WriteBodyEntryEvent>,
    release_tx: Option<Sender<()>>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl WriteBodyEntryHookGuard {
    /// Wait until the hooked writer reaches the body-entry point.
    ///
    /// # Errors
    ///
    /// Returns [`mpsc::RecvError`] if the writer exits before reaching the hook.
    pub fn wait_until_entered(&self) -> std::result::Result<WriteBodyEntryEvent, mpsc::RecvError> {
        self.entered_rx.recv()
    }

    /// Return `Ok(())` when this hook has not been reached yet.
    ///
    /// # Errors
    ///
    /// Returns a static error string if the writer already entered the body or
    /// the hook channel disconnected before entry.
    pub fn assert_not_entered(&self) -> std::result::Result<(), &'static str> {
        match self.entered_rx.try_recv() {
            Ok(_) => Err("write body entry hook fired before expected"),
            Err(TryRecvError::Empty) => Ok(()),
            Err(TryRecvError::Disconnected) => {
                Err("write body entry hook disconnected before entry")
            }
        }
    }

    /// Release the hooked writer if it is blocked at body entry.
    ///
    /// # Errors
    ///
    /// Returns [`mpsc::SendError`] if the writer is no longer waiting.
    pub fn release(&mut self) -> std::result::Result<(), mpsc::SendError<()>> {
        if let Some(tx) = self.release_tx.take() {
            tx.send(())?;
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for WriteBodyEntryHookGuard {
    fn drop(&mut self) {
        let _ = self.release();
        clear_write_body_entry_hook(&self.shared, &self.ns, self.id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn clear_write_body_entry_hook(shared: &SharedState, ns: &str, id: u64) {
    let Ok(mut hooks) = shared.write_body_entry_hooks.lock() else {
        return;
    };
    let remove_ns = if let Some(queue) = hooks.get_mut(ns) {
        queue.retain(|hook| hook.id != id);
        queue.is_empty()
    } else {
        false
    };
    if remove_ns {
        hooks.remove(ns);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn install_write_body_entry_hook(
    shared: &Arc<SharedState>,
    ns: &str,
    observe_flag: Option<Arc<AtomicBool>>,
) -> WriteBodyEntryHookGuard {
    // Multiple hooks for one namespace are consumed in FIFO order.
    let id = shared
        .write_body_entry_hook_next_id
        .fetch_add(1, Ordering::AcqRel);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let hook = WriteBodyEntryHook {
        id,
        entered_tx,
        release_rx,
        observe_flag,
    };
    let mut hooks = shared
        .write_body_entry_hooks
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    hooks.entry(ns.to_owned()).or_default().push_back(hook);
    WriteBodyEntryHookGuard {
        shared: Arc::clone(shared),
        ns: ns.to_owned(),
        id,
        entered_rx,
        release_tx: Some(release_tx),
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn write_body_entry_if_installed(shared: &SharedState, ns: &str) {
    let hook = {
        let Ok(mut hooks) = shared.write_body_entry_hooks.lock() else {
            return;
        };
        let Some(queue) = hooks.get_mut(ns) else {
            return;
        };
        let hook = queue.pop_front();
        if queue.is_empty() {
            hooks.remove(ns);
        }
        hook
    };
    if let Some(hook) = hook {
        hook.fire();
    }
}

// ---------------------------------------------------------------------------
// §10.8 #19 rendezvous hook — test-only pause point between
// `commit_txn` and `publish_commit` in the CRUD commit path.
// ---------------------------------------------------------------------------
//
// Hook state lives on `SharedState::publish_pause_hook` (engine-local,
// `#[cfg(test)]`-gated), so parallel tests using independent
// `PagedEngine` instances cannot consume each other's barriers.
// Production builds carry neither the `Mutex` nor the `Arc<Barrier>`
// (§11 #10: no new `Mutex` / `Arc` on the `cfg(not(test))` commit
// path). The paired unit test lives in
// `src/storage/paged_engine/tests.rs::publish_happens_strictly_after_commit_txn`.

/// Install a 2-party `Barrier` that the next CRUD commit on `shared`
/// will `wait()` on between `commit_txn` and `publish_commit`.
/// Returns a guard whose `Drop` clears the hook so a panicking test
/// cannot leave the hook armed.
#[cfg(test)]
pub(crate) fn install_publish_pause(
    shared: &SharedState,
    barrier: Arc<Barrier>,
) -> PublishPauseGuard<'_> {
    *shared.publish_pause_hook.lock().unwrap() = Some(barrier);
    PublishPauseGuard { shared }
}

/// RAII guard that clears the publish-pause hook on drop.
#[cfg(test)]
#[doc(hidden)]
pub struct PublishPauseGuard<'a> {
    shared: &'a SharedState,
}

#[cfg(test)]
impl<'a> Drop for PublishPauseGuard<'a> {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.shared.publish_pause_hook.lock() {
            *slot = None;
        }
    }
}

/// Called by `run_write_existing` between `commit_txn` and
/// `publish_commit`. Test-only — compiles to a true no-op in release
/// builds.
#[cfg(test)]
pub(crate) fn publish_pause_if_installed(shared: &SharedState) {
    let maybe = shared
        .publish_pause_hook
        .lock()
        .ok()
        .and_then(|mut slot| slot.take());
    if let Some(barrier) = maybe {
        barrier.wait();
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn us019_maybe_fail_primary_install(shared: &SharedState) -> Result<()> {
    shared
        .us019_primary_install_attempts
        .fetch_add(1, Ordering::AcqRel);
    let decremented = shared.us019_primary_install_failures.fetch_update(
        Ordering::AcqRel,
        Ordering::Acquire,
        |remaining| {
            if remaining == 0 {
                None
            } else {
                Some(remaining - 1)
            }
        },
    );
    if decremented.is_ok() {
        return Err(Error::Internal(
            "US-019 injected primary install failure".into(),
        ));
    }
    Ok(())
}

impl PagedEngine {
    /// Test-only: sample the timestamp oracle's current
    /// `(physical_ms, logical)`. See §10.6 + Contract 3.4.
    pub(super) fn test_oracle_now(&self) -> (u64, u32) {
        let ts = self.shared.oracle.now();
        (ts.physical_ms, ts.logical)
    }

    /// Test-only: sample the current published
    /// `PublishedEpoch.visible_ts`. Used by §10.6 / US-010 reopen
    /// bootstrap checks.
    pub(super) fn test_published_visible_ts(&self) -> (u64, u32) {
        let snap = self.shared.load_published();
        let ts = snap.visible_ts;
        (ts.physical_ms, ts.logical)
    }

    /// Test-only: published-catalog rebuild generation. Advances on
    /// rebuild publishes and holds steady on catalog-Arc reuse.
    pub(super) fn test_published_catalog_gen(&self) -> u64 {
        self.shared.load_published().catalog_generation
    }

    /// Test-only: sample the current published sequencer frontier.
    pub(super) fn test_published_sequencer_frontier(&self) -> (u64, u32) {
        let ts = self.shared.load_published().sequencer_frontier;
        (ts.physical_ms, ts.logical)
    }

    /// Test-only: number of recovery post-open epoch stores performed by
    /// this engine instance.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_recovery_open_published_store_count(&self) -> u64 {
        self.shared
            .recovery_open_published_store_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Test-only: highest `ChainCommit.commit_ts` observed during the
    /// most recent journal recovery. See US-002 crash harness.
    pub(super) fn test_recovered_max_commit_ts(&self) -> Option<(u64, u32)> {
        self.shared
            .handle
            .recovered_max_commit_ts()
            .ok()
            .flatten()
            .map(|ts| (ts.physical_ms, ts.logical))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us019_set_primary_install_failures(&self, failures: u8) {
        self.shared
            .us019_primary_install_attempts
            .store(0, Ordering::Release);
        self.shared
            .us019_primary_install_failures
            .store(failures, Ordering::Release);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us019_primary_install_attempts(&self) -> u64 {
        self.shared
            .us019_primary_install_attempts
            .load(Ordering::Acquire)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_install_write_body_entry_hook(
        &self,
        ns: &str,
        observe_flag: Option<Arc<AtomicBool>>,
    ) -> WriteBodyEntryHookGuard {
        install_write_body_entry_hook(&self.shared, ns, observe_flag)
    }
}
