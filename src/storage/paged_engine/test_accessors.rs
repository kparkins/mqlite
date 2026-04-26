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

use std::sync::Arc;

#[cfg(test)]
use super::state::SharedState;
use super::PagedEngine;

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

#[cfg(test)]
use std::sync::Barrier;

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

impl PagedEngine {
    /// Test-only: sample the timestamp oracle's current
    /// `(physical_ms, logical)`. See §10.6 + Contract 3.4.
    pub(super) fn test_oracle_now(&self) -> (u64, u32) {
        let ts = self.shared.oracle.now();
        (ts.physical_ms, ts.logical)
    }

    /// Test-only: sample the current published
    /// `ReadEpoch.visible_ts`. Used by §10.6 / US-010 reopen
    /// bootstrap checks.
    pub(super) fn test_published_visible_ts(&self) -> (u64, u32) {
        let snap = self.shared.load_published();
        let ts = snap.visible_ts;
        (ts.physical_ms, ts.logical)
    }

    /// Test-only: pointer-identity of the currently-published
    /// `Arc<PublishedCatalog>` as `usize`. Stand-in for `Arc::ptr_eq`
    /// from integration tests (§10.8 / US-011 / US-014).
    ///
    /// Routes through the read-path wrapper `SharedState::load_published`
    /// so this accessor does NOT count as a second production
    /// `published.load_full()` call site (§11 #2 grep-gate).
    pub(super) fn test_published_catalog_ptr(&self) -> usize {
        let guard = self.shared.load_published();
        Arc::as_ptr(&guard.catalog) as usize
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
}
