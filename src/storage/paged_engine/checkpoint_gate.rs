//! Engine-wide writer-admission gate for checkpoint freeze windows.
//!
//! Extracted from `state.rs` (plan item R12) to keep the shared-state module
//! focused on `SharedState` / `MetadataState`. `SharedState` re-exports these
//! types (`pub(super) use super::checkpoint_gate::{...}`) so every existing
//! call site that reaches the gate through `state` keeps resolving unchanged.

use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex as PlMutex};

use crate::error::{EngineFatalReason, Error, Result};

/// Engine-wide writer-admission gate used by checkpoint freeze windows.
///
/// The namespace writer registry remains per-collection. This gate sits
/// before that namespace admission point so a checkpoint can close all new
/// writer admission, wait for already-admitted writers to publish or abort,
/// then run the mutation-free planning / freeze window without relying on
/// metadata-write exclusion.
///
/// # Admit/drain timeout asymmetry
///
/// The two entry points deliberately treat time differently:
///
/// - [`admit_writer`](Self::admit_writer) waits **unboundedly** while a
///   checkpoint holds admission closed. A CRUD writer is at the very start of
///   its lifecycle here — it holds no page latches, no journal reservation,
///   and no publish slot — so parking it has no liveness cost beyond its own
///   latency, and the wait is bounded in practice by the checkpoint freeze
///   window (which is itself busy-timeout bounded via `close_and_drain_all`).
///   Giving admission its own timeout would only convert a transient
///   checkpoint window into a spurious `WriterBusy` error for an operation
///   that has not yet acquired anything worth protecting.
///
/// - [`close_and_drain_all`](Self::close_and_drain_all) honors the caller's
///   `busy_timeout`. The checkpoint is waiting on *already-admitted* writers
///   that DO hold resources (page latches, an open journal envelope, a publish
///   slot in flight). An unbounded wait here could wedge the engine behind a
///   stuck or slow writer, so the checkpoint must be able to give up and
///   return [`Error::WriterBusy`] rather than block the admin path forever.
///
/// In short: the *enterer* can afford to wait forever because it holds
/// nothing; the *closer* must bound its wait because it is blocked on holders.
pub(crate) struct CheckpointAdmissionGate {
    inner: PlMutex<CheckpointAdmissionInner>,
    cvar: Condvar,
}

#[derive(Default)]
struct CheckpointAdmissionInner {
    admits: u64,
    releases: u64,
    close_count: u32,
    poisoned_reason: Option<EngineFatalReason>,
}

impl CheckpointAdmissionGate {
    /// Construct an open checkpoint admission gate.
    pub(crate) fn new() -> Self {
        Self {
            inner: PlMutex::new(CheckpointAdmissionInner::default()),
            cvar: Condvar::new(),
        }
    }

    /// Close writer admission and wait until every admitted writer exits.
    ///
    /// Honors `timeout` (the caller's `busy_timeout`) because it blocks on
    /// already-admitted writers that hold latches / journal / publish-slot
    /// resources — see the type-level "Admit/drain timeout asymmetry" note.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WriterBusy`] when already-admitted writers do not
    /// drain within `timeout`. Returns [`Error::EngineFatal`] when the live
    /// engine is already poisoned.
    pub(crate) fn close_and_drain_all(
        self: &Arc<Self>,
        timeout: Duration,
    ) -> Result<CheckpointAdmissionGuard> {
        let guard = CheckpointAdmissionGuard {
            gate: Arc::clone(self),
        };
        let start = Instant::now();
        let mut inner = self.inner.lock();
        inner.close_count = inner.close_count.saturating_add(1);
        loop {
            if let Some(reason) = inner.poisoned_reason.clone() {
                return Err(Error::EngineFatal { reason });
            }
            if inner.admits == inner.releases {
                return Ok(guard);
            }
            let remaining = timeout.checked_sub(start.elapsed()).unwrap_or_default();
            if remaining.is_zero() {
                return Err(Error::WriterBusy);
            }
            let wait = self.cvar.wait_for(&mut inner, remaining);
            if wait.timed_out() && inner.admits != inner.releases {
                return Err(Error::WriterBusy);
            }
        }
    }

    /// Admit one writer unless a checkpoint has closed admission.
    ///
    /// Waits **unboundedly** while admission is closed: the caller holds no
    /// latches/journal/publish resources yet, so parking it is safe and has no
    /// liveness cost — see the type-level "Admit/drain timeout asymmetry" note.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EngineFatal`] when the live engine has been poisoned.
    pub(crate) fn admit_writer(self: &Arc<Self>) -> Result<CheckpointWriterAdmission> {
        let mut inner = self.inner.lock();
        loop {
            if let Some(reason) = inner.poisoned_reason.clone() {
                return Err(Error::EngineFatal { reason });
            }
            if inner.close_count == 0 {
                inner.admits = inner.admits.saturating_add(1);
                return Ok(CheckpointWriterAdmission {
                    gate: Arc::clone(self),
                });
            }
            self.cvar.wait(&mut inner);
        }
    }

    fn reopen(&self) {
        let mut inner = self.inner.lock();
        if inner.poisoned_reason.is_none() {
            inner.close_count = inner.close_count.saturating_sub(1);
            if inner.close_count == 0 {
                self.cvar.notify_all();
            }
        }
    }

    pub(crate) fn poison(&self, reason: EngineFatalReason) {
        let mut inner = self.inner.lock();
        inner.close_count = inner.close_count.saturating_add(1);
        if inner.poisoned_reason.is_none() {
            inner.poisoned_reason = Some(reason);
        }
        self.cvar.notify_all();
    }
}

/// RAII guard returned by [`CheckpointAdmissionGate::close_and_drain_all`].
#[must_use = "CheckpointAdmissionGuard reopens checkpoint writer admission on drop"]
pub(crate) struct CheckpointAdmissionGuard {
    gate: Arc<CheckpointAdmissionGate>,
}

impl Drop for CheckpointAdmissionGuard {
    fn drop(&mut self) {
        self.gate.reopen();
    }
}

/// Writer admission token released after the writer publishes or aborts.
#[must_use = "dropping CheckpointWriterAdmission releases the admitted writer"]
pub(crate) struct CheckpointWriterAdmission {
    gate: Arc<CheckpointAdmissionGate>,
}

impl Drop for CheckpointWriterAdmission {
    fn drop(&mut self) {
        let mut inner = self.gate.inner.lock();
        inner.releases = inner.releases.saturating_add(1);
        if inner.admits == inner.releases {
            self.gate.cvar.notify_all();
        }
    }
}
