//! Test-only `impl PagedEngine` accessors — NOT part of the
//! production path.
//!
//! Every method here exists to let integration or unit tests observe
//! engine state that is deliberately kept out of the public
//! `StorageEngine` trait's production surface. They are reached from
//! integration tests through `#[doc(hidden)]` `Client::__*` accessors
//! in `src/client/tests/hidden_accessors.rs`. None of these methods should
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
    atomic::{AtomicBool, AtomicU8, Ordering},
    mpsc::{self, Receiver, Sender, TryRecvError},
    Arc,
};

#[cfg(any(test, feature = "test-hooks"))]
use super::state::SharedState;
use super::PagedEngine;
#[cfg(any(test, feature = "test-hooks"))]
use super::{catalog_ops::catalog_lock, catalog_ops::new_store};
#[cfg(any(test, feature = "test-hooks"))]
use super::{index_maint::install_pending_sec_index, visibility::WriteVisibility};
use crate::error::{Error, Result};
#[cfg(any(test, feature = "test-hooks"))]
use crate::keys::{compound_prefix_range_excluding_trailing_id, encode_compound_key, encode_key};
#[cfg(any(test, feature = "test-hooks"))]
use crate::mvcc::{SecIndexOp, SecIndexWrite, Ts, VersionData, VersionEntry, VersionState};
#[cfg(any(test, feature = "test-hooks"))]
use crate::storage::btree::reconcile::{encode_folded_leaf, FoldedLeafCell, FoldedLeafLinks};
#[cfg(any(test, feature = "test-hooks"))]
use crate::storage::btree::BTree;
#[cfg(any(test, feature = "test-hooks"))]
use crate::storage::reconcile::plan::{DirtyReason, TreeIdent, TreeKind};
#[cfg(any(test, feature = "test-hooks"))]
use crate::storage::secondary_index::build_index_keys;
#[cfg(any(test, feature = "test-hooks"))]
static PHASE3_COMMIT_FAILPOINT: AtomicU8 = AtomicU8::new(0);
#[cfg(any(test, feature = "test-hooks"))]
static PHASE8_CHECKPOINT_FAILPOINT: AtomicU8 = AtomicU8::new(0);

/// US-026 post-register cleanup failpoints.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[doc(hidden)]
pub enum Us026PostRegisterFailpoint {
    /// Fail after Pending install and before log reservation.
    BeforeLogReservation,
    /// Fail the final pre-durable flush.
    Flush,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Us026PostRegisterFailpoint {
    fn slot(self) -> u8 {
        match self {
            Self::BeforeLogReservation => 1,
            Self::Flush => 2,
        }
    }

    fn message(self) -> &'static str {
        match self {
            Self::BeforeLogReservation => "US-026 injected pre-reservation failure",
            Self::Flush => "US-026 injected flush failure",
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn us026_arm_post_register_failpoint(
    shared: &SharedState,
    failpoint: Us026PostRegisterFailpoint,
) {
    shared
        .us026_post_register_failpoint
        .store(failpoint.slot(), Ordering::Release);
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn us026_fail_if_armed(
    shared: &SharedState,
    failpoint: Us026PostRegisterFailpoint,
) -> Result<()> {
    if shared
        .us026_post_register_failpoint
        .compare_exchange(failpoint.slot(), 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        return Err(Error::Internal(failpoint.message().into()));
    }
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn phase8_arm_dirty_lsn_stamp_failure(shared: &SharedState) {
    shared
        .phase8_fail_next_dirty_lsn_stamp
        .store(1, Ordering::Release);
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn phase8_fail_dirty_lsn_stamp_if_armed(shared: &SharedState) -> Result<()> {
    if shared
        .phase8_fail_next_dirty_lsn_stamp
        .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        return Err(Error::Internal(
            "Phase 8 injected stamp_dirty_pages_lsn failure".into(),
        ));
    }
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn phase8_arm_after_dirty_lsn_stamp_failure(shared: &SharedState) {
    shared
        .phase8_fail_next_after_dirty_lsn_stamp
        .store(1, Ordering::Release);
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn phase8_fail_after_dirty_lsn_stamp_if_armed(shared: &SharedState) -> Result<()> {
    if shared
        .phase8_fail_next_after_dirty_lsn_stamp
        .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        return Err(Error::Internal(
            "Phase 8 injected after dirty LSN stamp before write failure".into(),
        ));
    }
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn phase8_arm_after_durable_before_flip_failure(shared: &SharedState) {
    shared
        .phase8_fail_next_after_durable_before_flip
        .store(1, Ordering::Release);
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn phase8_fail_after_durable_before_flip_if_armed(shared: &SharedState) -> Result<()> {
    if shared
        .phase8_fail_next_after_durable_before_flip
        .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        return Err(Error::Internal(
            "Phase 8 injected after durable sync before Pending flip failure".into(),
        ));
    }
    Ok(())
}

/// Pending test-only hook consumed after Pending install and before
/// reservation.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct Phase8BeforeReservationHook {
    entered_tx: Sender<()>,
    release_rx: Receiver<()>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Phase8BeforeReservationHook {
    fn fire(self) {
        if self.entered_tx.send(()).is_ok() {
            let _ = self.release_rx.recv();
        }
    }
}

/// RAII guard for the Phase 8 before-reservation pause hook.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub struct Phase8BeforeReservationHookGuard {
    shared: Arc<SharedState>,
    entered_rx: Receiver<()>,
    release_tx: Option<Sender<()>>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Phase8BeforeReservationHookGuard {
    /// Wait until the hooked writer reaches the before-reservation point.
    ///
    /// # Errors
    ///
    /// Returns [`mpsc::RecvError`] if the writer exits before reaching the hook.
    pub fn wait_until_entered(&self) -> std::result::Result<(), mpsc::RecvError> {
        self.entered_rx.recv()
    }

    /// Release the hooked writer if it is blocked before reservation.
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
impl Drop for Phase8BeforeReservationHookGuard {
    fn drop(&mut self) {
        let _ = self.release();
        if let Ok(mut hook) = self.shared.phase8_before_reservation_hook.lock() {
            *hook = None;
        }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn install_phase8_before_reservation_hook(
    shared: &Arc<SharedState>,
) -> Phase8BeforeReservationHookGuard {
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let hook = Phase8BeforeReservationHook {
        entered_tx,
        release_rx,
    };
    let mut slot = shared
        .phase8_before_reservation_hook
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *slot = Some(hook);
    Phase8BeforeReservationHookGuard {
        shared: Arc::clone(shared),
        entered_rx,
        release_tx: Some(release_tx),
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn phase8_before_reservation_if_installed(shared: &SharedState) {
    let hook = {
        let Ok(mut slot) = shared.phase8_before_reservation_hook.lock() else {
            return;
        };
        slot.take()
    };
    if let Some(hook) = hook {
        hook.fire();
    }
}

/// Test-hook failpoints around the remaining publish-boundary crash cuts.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase3CommitFailpoint {
    /// After pending heads flip committed and before publish.
    AfterLegacyCommitBeforePublish,
    /// During publish, immediately before the `PublishedEpoch` store.
    DuringPublishBeforeStore,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Phase3CommitFailpoint {
    fn slot(self) -> u8 {
        match self {
            Self::AfterLegacyCommitBeforePublish => 1,
            Self::DuringPublishBeforeStore => 2,
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

/// Phase 8 checkpoint crash-cut failpoints at durable-boundary edges.
#[cfg(any(test, feature = "test-hooks"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Phase8CheckpointFailpoint {
    /// After the checkpoint materialization flush and before any
    /// `checkpoint_applied_lsn` header advance or CheckpointBoundary record.
    AfterMaterializationFlushBeforeBoundary,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Phase8CheckpointFailpoint {
    fn slot(self) -> u8 {
        match self {
            Self::AfterMaterializationFlushBeforeBoundary => 1,
        }
    }
}

/// RAII guard that clears the armed Phase 8 checkpoint failpoint on drop.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub struct Phase8CheckpointFailpointGuard {
    slot: u8,
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for Phase8CheckpointFailpointGuard {
    fn drop(&mut self) {
        let _ = PHASE8_CHECKPOINT_FAILPOINT.compare_exchange(
            self.slot,
            0,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

/// Arm one exclusive Phase 8 checkpoint failpoint for the current process.
///
/// # Errors
///
/// Returns [`Error::Internal`] if another checkpoint failpoint is already armed.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub fn arm_phase8_checkpoint_failpoint(
    failpoint: Phase8CheckpointFailpoint,
) -> Result<Phase8CheckpointFailpointGuard> {
    let slot = failpoint.slot();
    PHASE8_CHECKPOINT_FAILPOINT
        .compare_exchange(0, slot, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| Error::Internal("Phase 8 checkpoint failpoint already armed".into()))?;
    Ok(Phase8CheckpointFailpointGuard { slot })
}

/// Abort the process if the requested Phase 8 checkpoint failpoint is armed.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn phase8_checkpoint_abort_if_armed(failpoint: Phase8CheckpointFailpoint) {
    if PHASE8_CHECKPOINT_FAILPOINT.load(Ordering::Acquire) == failpoint.slot() {
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

    /// Wait with a timeout until the hooked writer reaches the body-entry point.
    ///
    /// # Errors
    ///
    /// Returns [`mpsc::RecvTimeoutError`] on timeout or disconnect.
    pub fn wait_until_entered_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> std::result::Result<WriteBodyEntryEvent, mpsc::RecvTimeoutError> {
        self.entered_rx.recv_timeout(timeout)
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
// US-013 create-index build rendezvous hook.
// ---------------------------------------------------------------------------

/// Pending test-only hook consumed when `create_index_build` reaches the
/// long scan window for a specific namespace/index pair.
#[cfg(any(test, feature = "test-hooks"))]
pub(crate) struct CreateIndexBuildHook {
    id: u64,
    entered_tx: Sender<()>,
    release_rx: Receiver<()>,
    fail_after_release: bool,
}

#[cfg(any(test, feature = "test-hooks"))]
impl CreateIndexBuildHook {
    fn fire(self) -> Result<()> {
        if self.entered_tx.send(()).is_ok() {
            let _ = self.release_rx.recv();
        }
        if self.fail_after_release {
            return Err(Error::Internal(
                "US-038 injected create_index_build failure".into(),
            ));
        }
        Ok(())
    }
}

/// RAII guard for a create-index build-scan rendezvous hook.
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub struct CreateIndexBuildHookGuard {
    shared: Arc<SharedState>,
    ns: String,
    index_name: String,
    id: u64,
    entered_rx: Receiver<()>,
    release_tx: Option<Sender<()>>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl CreateIndexBuildHookGuard {
    /// Wait until `create_index_build` reaches the scan hook.
    ///
    /// # Errors
    ///
    /// Returns [`mpsc::RecvError`] if the build exits before reaching the hook.
    pub fn wait_until_entered(&self) -> std::result::Result<(), mpsc::RecvError> {
        self.entered_rx.recv()
    }

    /// Return `Ok(())` when this hook has not been reached yet.
    ///
    /// # Errors
    ///
    /// Returns a static error string if the hook fired or disconnected.
    pub fn assert_not_entered(&self) -> std::result::Result<(), &'static str> {
        match self.entered_rx.try_recv() {
            Ok(()) => Err("create-index build hook fired before expected"),
            Err(TryRecvError::Empty) => Ok(()),
            Err(TryRecvError::Disconnected) => {
                Err("create-index build hook disconnected before entry")
            }
        }
    }

    /// Release the blocked create-index build, if it is still waiting.
    ///
    /// # Errors
    ///
    /// Returns [`mpsc::SendError`] if the build is no longer waiting.
    pub fn release(&mut self) -> std::result::Result<(), mpsc::SendError<()>> {
        if let Some(tx) = self.release_tx.take() {
            tx.send(())?;
        }
        Ok(())
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for CreateIndexBuildHookGuard {
    fn drop(&mut self) {
        let _ = self.release();
        clear_create_index_build_hook(&self.shared, &self.ns, &self.index_name, self.id);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn clear_create_index_build_hook(shared: &SharedState, ns: &str, index_name: &str, id: u64) {
    let Ok(mut hooks) = shared.create_index_build_hooks.lock() else {
        return;
    };
    let key = (ns.to_owned(), index_name.to_owned());
    let remove_key = if let Some(queue) = hooks.get_mut(&key) {
        queue.retain(|hook| hook.id != id);
        queue.is_empty()
    } else {
        false
    };
    if remove_key {
        hooks.remove(&key);
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn install_create_index_build_hook(
    shared: &Arc<SharedState>,
    ns: &str,
    index_name: &str,
) -> CreateIndexBuildHookGuard {
    install_create_index_build_hook_with_failure(shared, ns, index_name, false)
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn install_create_index_build_hook_with_failure(
    shared: &Arc<SharedState>,
    ns: &str,
    index_name: &str,
    fail_after_release: bool,
) -> CreateIndexBuildHookGuard {
    let id = shared
        .write_body_entry_hook_next_id
        .fetch_add(1, Ordering::AcqRel);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let hook = CreateIndexBuildHook {
        id,
        entered_tx,
        release_rx,
        fail_after_release,
    };
    shared
        .create_index_build_hooks
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry((ns.to_owned(), index_name.to_owned()))
        .or_default()
        .push_back(hook);
    CreateIndexBuildHookGuard {
        shared: Arc::clone(shared),
        ns: ns.to_owned(),
        index_name: index_name.to_owned(),
        id,
        entered_rx,
        release_tx: Some(release_tx),
    }
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn create_index_build_if_installed(
    shared: &SharedState,
    ns: &str,
    index_name: &str,
) -> Result<()> {
    let hook = {
        let Ok(mut hooks) = shared.create_index_build_hooks.lock() else {
            return Ok(());
        };
        let key = (ns.to_owned(), index_name.to_owned());
        let Some(queue) = hooks.get_mut(&key) else {
            return Ok(());
        };
        let hook = queue.pop_front();
        if queue.is_empty() {
            hooks.remove(&key);
        }
        hook
    };
    if let Some(hook) = hook {
        hook.fire()?;
    }
    Ok(())
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

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn us009_record_committed_flip(shared: &SharedState) {
    let order = shared
        .us009_event_order_counter
        .fetch_add(1, Ordering::AcqRel)
        + 1;
    shared
        .us009_committed_flip_order
        .store(order, Ordering::Release);
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn us009_record_publish_ready(shared: &SharedState) {
    let order = shared
        .us009_event_order_counter
        .fetch_add(1, Ordering::AcqRel)
        + 1;
    shared
        .us009_publish_ready_order
        .store(order, Ordering::Release);
}

#[cfg(any(test, feature = "test-hooks"))]
pub(crate) fn us009_fail_after_committed_flip_if_armed(shared: &SharedState) -> Result<()> {
    if shared
        .us009_fail_after_committed_flip
        .compare_exchange(1, 0, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        return Err(Error::Internal(
            "US-009 injected failure after committed flip before publish".into(),
        ));
    }
    Ok(())
}

#[cfg(any(test, feature = "test-hooks"))]
fn us009_state_name(entry: &VersionEntry) -> String {
    match entry.state {
        VersionState::Pending { .. } => "Pending".to_owned(),
        VersionState::Committed => "Committed".to_owned(),
        VersionState::Aborted => "Aborted".to_owned(),
    }
}

#[cfg(any(test, feature = "test-hooks"))]
fn us009_state_names(entries: Vec<VersionEntry>) -> Vec<String> {
    entries.iter().map(us009_state_name).collect()
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

    /// Test-only: sample the live `PublishSequencer.published_frontier`
    /// (§10.19 C-1). Phase 5 makes the sequencer the single source of
    /// truth for the published frontier; the accessor name is preserved
    /// for callers that pre-date US-005.
    pub(super) fn test_published_sequencer_frontier(&self) -> (u64, u32) {
        let ts = self
            .shared
            .publish_sequencer
            .published_frontier
            .load(std::sync::atomic::Ordering::Acquire);
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
    pub(super) fn test_us009_primary_chain_states(
        &self,
        ns: &str,
        id: &bson::Bson,
    ) -> Result<Vec<String>> {
        let coll = catalog_lock(&self.metadata_state)
            .get_collection(ns)?
            .ok_or_else(|| Error::Internal(format!("namespace '{ns}' not found")))?;
        let key = encode_key(id);
        let tree = BTree::open(
            new_store(&self.shared),
            coll.data_root_page,
            coll.data_root_level,
        );
        let leaf = tree.find_leaf(&key)?;
        self.shared
            .handle
            .pool()
            .us009_chain_entries(leaf, &key)
            .map(us009_state_names)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us009_inject_primary_committed_head(
        &self,
        ns: &str,
        doc: &bson::Document,
        commit_ts: Ts,
        txn_id: u64,
    ) -> Result<()> {
        let coll = catalog_lock(&self.metadata_state)
            .get_collection(ns)?
            .ok_or_else(|| Error::Internal(format!("namespace '{ns}' not found")))?;
        let id = doc.get("_id").cloned().unwrap_or(bson::Bson::Null);
        let key = encode_key(&id);
        let tree = BTree::open(
            new_store(&self.shared),
            coll.data_root_page,
            coll.data_root_level,
        );
        let leaf = tree.find_leaf(&key)?;
        let mut page = self.shared.handle.pool().pin_for_write(leaf)?;
        let mut chain = page.get_or_create_chain(&key)?;
        let data = bson::to_vec(doc).map_err(Error::BsonSerialization)?;
        std::sync::Arc::make_mut(&mut chain).push_front(VersionEntry {
            start_ts: commit_ts,
            stop_ts: Ts::MAX,
            txn_id,
            state: VersionState::Committed,
            data: VersionData::Inline(data),
            is_tombstone: false,
        });
        page.put_chain(key, chain)?;
        self.shared.mark_leaf_dirty(
            TreeIdent {
                collection_id: coll.id,
                kind: TreeKind::Primary,
            },
            leaf,
            DirtyReason::PrimaryWrite,
        );
        Ok(())
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us009_secondary_chain_states(
        &self,
        ns: &str,
        index_name: &str,
        doc: &bson::Document,
        id: &bson::Bson,
    ) -> Result<Vec<String>> {
        let index = catalog_lock(&self.metadata_state)
            .get_index(ns, index_name)?
            .ok_or_else(|| Error::Internal(format!("index '{index_name}' not found")))?;
        let (keys, _) = build_index_keys(doc, &index.key_pattern, id, index.sparse)?;
        let key = keys
            .into_iter()
            .next()
            .ok_or_else(|| Error::Internal("US-009 secondary probe got no index key".into()))?;
        let tree = BTree::open(new_store(&self.shared), index.root_page, index.root_level);
        let leaf = tree.find_leaf(&key)?;
        self.shared
            .handle
            .pool()
            .us009_chain_entries(leaf, &key)
            .map(us009_state_names)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn test_us028_primary_leaf_ident(&self, ns: &str, id: &bson::Bson) -> Result<(TreeIdent, u32)> {
        let coll = catalog_lock(&self.metadata_state)
            .get_collection(ns)?
            .ok_or_else(|| Error::Internal(format!("namespace '{ns}' not found")))?;
        let key = encode_key(id);
        let tree = BTree::open(
            new_store(&self.shared),
            coll.data_root_page,
            coll.data_root_level,
        );
        let leaf = tree.find_leaf(&key)?;
        Ok((
            TreeIdent {
                collection_id: coll.id,
                kind: TreeKind::Primary,
            },
            leaf,
        ))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us028_primary_leaf_for_id(&self, ns: &str, id: &bson::Bson) -> Result<u32> {
        self.test_us028_primary_leaf_ident(ns, id)
            .map(|(_, leaf)| leaf)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us022_insert_two_docs_one_txn(
        &self,
        ns: &str,
        left: bson::Document,
        right: bson::Document,
    ) -> Result<()> {
        self.run_write(ns, |shared, md, txn, vis| {
            super::doc_ops::stage_insert_body(shared, md, txn, vis, ns, left)?;
            super::doc_ops::stage_insert_body(shared, md, txn, vis, ns, right)?;
            Ok(())
        })
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us028_hold_primary_leaf_reconcile_latch(
        &self,
        ns: &str,
        id: &bson::Bson,
        ready: Sender<()>,
        release: Receiver<()>,
    ) -> Result<()> {
        let (ident, leaf) = self.test_us028_primary_leaf_ident(ns, id)?;
        let _latched_pages = self
            .shared
            .handle
            .pool()
            .pin_leaf_set_for_reconcile(ident, &[leaf])
            .map_err(|err| Error::Internal(format!("US-028 reconcile latch failed: {err:?}")))?;
        ready
            .send(())
            .map_err(|_| Error::Internal("US-028 reconcile latch ready receiver dropped".into()))?;
        release
            .recv()
            .map_err(|_| Error::Internal("US-028 reconcile latch release dropped".into()))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us028_hold_primary_leaf_writer_latch(
        &self,
        ns: &str,
        id: &bson::Bson,
        ready: Sender<()>,
        release: Receiver<()>,
    ) -> Result<()> {
        let leaf = self.test_us028_primary_leaf_for_id(ns, id)?;
        let _page = self.shared.handle.pool().pin_for_write(leaf)?;
        ready
            .send(())
            .map_err(|_| Error::Internal("US-028 writer latch ready receiver dropped".into()))?;
        release
            .recv()
            .map_err(|_| Error::Internal("US-028 writer latch release dropped".into()))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us025_hold_primary_leaf_reader_latch(
        &self,
        ns: &str,
        id: &bson::Bson,
        ready: Sender<()>,
        release: Receiver<()>,
    ) -> Result<()> {
        let leaf = self.test_us028_primary_leaf_for_id(ns, id)?;
        let _page = self.shared.handle.pool().pin_for_read(leaf)?;
        ready
            .send(())
            .map_err(|_| Error::Internal("US-025 reader latch ready receiver dropped".into()))?;
        release
            .recv()
            .map_err(|_| Error::Internal("US-025 reader latch release dropped".into()))
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us009_reset_flip_publish_order(&self) {
        self.shared
            .us009_event_order_counter
            .store(0, Ordering::Release);
        self.shared
            .us009_committed_flip_order
            .store(0, Ordering::Release);
        self.shared
            .us009_publish_ready_order
            .store(0, Ordering::Release);
        self.shared
            .us009_fail_after_committed_flip
            .store(0, Ordering::Release);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us009_flip_publish_order(&self) -> (u64, u64) {
        (
            self.shared
                .us009_committed_flip_order
                .load(Ordering::Acquire),
            self.shared
                .us009_publish_ready_order
                .load(Ordering::Acquire),
        )
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us009_fail_after_committed_flip_once(&self) {
        self.shared
            .us009_fail_after_committed_flip
            .store(1, Ordering::Release);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us026_arm_post_register_failpoint(
        &self,
        failpoint: Us026PostRegisterFailpoint,
    ) {
        us026_arm_post_register_failpoint(&self.shared, failpoint);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_phase8_journal_lsn_snapshot(&self) -> Result<(u64, u64, u64)> {
        self.shared.handle.journal_lsn_snapshot()
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_phase8_fail_next_dirty_lsn_stamp(&self) {
        phase8_arm_dirty_lsn_stamp_failure(&self.shared);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_phase8_fail_next_after_dirty_lsn_stamp(&self) {
        phase8_arm_after_dirty_lsn_stamp_failure(&self.shared);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_phase8_fail_next_after_durable_before_flip(&self) {
        phase8_arm_after_durable_before_flip_failure(&self.shared);
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_install_phase8_before_reservation_hook(
        &self,
    ) -> Phase8BeforeReservationHookGuard {
        install_phase8_before_reservation_hook(&self.shared)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_install_write_body_entry_hook(
        &self,
        ns: &str,
        observe_flag: Option<Arc<AtomicBool>>,
    ) -> WriteBodyEntryHookGuard {
        install_write_body_entry_hook(&self.shared, ns, observe_flag)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_install_create_index_build_hook(
        &self,
        ns: &str,
        index_name: &str,
    ) -> CreateIndexBuildHookGuard {
        install_create_index_build_hook(&self.shared, ns, index_name)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_install_create_index_build_failure_hook(
        &self,
        ns: &str,
        index_name: &str,
    ) -> CreateIndexBuildHookGuard {
        install_create_index_build_hook_with_failure(&self.shared, ns, index_name, true)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us008_reset_structural_page_observations(&self) {
        crate::storage::structural_batch_observations::reset_committed_structural_leaf_bytes();
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn test_us008_committed_structural_leaf_bytes(&self) -> u64 {
        crate::storage::structural_batch_observations::committed_structural_leaf_bytes()
    }

    /// Test-only US-011 probe: install one pending unique email index entry
    /// directly through the production `install_pending_sec_index` path.
    pub(super) fn test_us011_install_pending_unique_email(
        &self,
        ns: &str,
        index_name: &str,
        id: bson::Bson,
        email: &str,
        txn_id: u64,
    ) -> Result<()> {
        let index = catalog_lock(&self.metadata_state)
            .get_index(ns, index_name)?
            .ok_or_else(|| Error::Internal(format!("index '{index_name}' not found")))?;
        if !index.unique {
            return Err(Error::Internal(format!(
                "US-011 probe requires unique index '{index_name}'"
            )));
        }

        let doc = bson::doc! { "_id": id.clone(), "email": email };
        let (keys, is_multikey) = build_index_keys(&doc, &index.key_pattern, &id, index.sparse)?;
        if is_multikey {
            return Err(Error::Internal(
                "US-011 probe expects a single-key email index entry".into(),
            ));
        }
        let key = keys
            .into_iter()
            .next()
            .ok_or_else(|| Error::Internal("US-011 probe generated no index key".into()))?;
        let id_bytes = bson::to_vec(&bson::doc! { "_id": id }).map_err(Error::BsonSerialization)?;
        let write = SecIndexWrite {
            index_id: index.id,
            index_root_page: index.root_page,
            key,
            expected_head: None,
            op: SecIndexOp::Insert { id_bytes },
        };
        let vis = WriteVisibility::new(&self.shared, ns)?;
        let commit_ts = self.shared.oracle.commit()?;
        install_pending_sec_index(
            &self.shared,
            &self.metadata_state,
            vec![write],
            &vis,
            commit_ts,
            txn_id,
        )?;
        Ok(())
    }

    /// Test-only US-011 probe: return sibling pages selected when a unique
    /// prefix range crosses both leaf boundaries.
    pub(super) fn test_us011_unique_prefix_sibling_pages(&self) -> Result<Vec<u32>> {
        let email = bson::Bson::String("sibling@example.test".to_owned());
        let lower_id = bson::Bson::Int32(1);
        let probe_id = bson::Bson::Int32(2);
        let upper_id = bson::Bson::Int32(3);
        let probe_key = encode_compound_key(&[(&email, true), (&probe_id, true)]);
        let (start, end) = compound_prefix_range_excluding_trailing_id(&probe_key, &[true])?;
        let lower_key = encode_compound_key(&[(&email, true), (&lower_id, true)]);
        let upper_key = encode_compound_key(&[(&email, true), (&upper_id, true)]);
        let image = encode_folded_leaf(
            &[
                FoldedLeafCell::inline(lower_key, vec![1]),
                FoldedLeafCell::inline(upper_key, vec![3]),
            ],
            FoldedLeafLinks {
                prev_leaf_page: 41,
                next_leaf_page: 43,
            },
        )?;
        crate::storage::btree::leaf_unique_prefix_sibling_pages(&image, &start, &end)
    }
}
