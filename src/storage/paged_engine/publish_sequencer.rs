//! `PublishSequencer` — Phase 5 §10.19 dense publish-slot sequencer.
//!
//! Owns the dense `publish_seq: u64` window that orders Phase 5 commit
//! publishes against each other independently of the sparse HLC
//! `commit_ts`. Allocation of `(publish_seq, commit_ts)` happens as a
//! single ordered pair under the sequencer mutex inside
//! [`PublishSequencer::register_with_oracle`]; the prior split sequence
//! `oracle.commit() -> register(commit_ts)` is forbidden by the §10.19
//! grep gate because the split would let publish-slot order diverge
//! from commit-timestamp order.
//!
//! The §10.19 protocol has three terminal states for a registered slot:
//!
//! 1. `Ready` — `mark_ready(guard, closure)` runs the publish closure
//!    once every earlier slot is also Ready (or Aborted) and advances
//!    `last_published`.
//! 2. `Aborted` (explicit) — `mark_aborted(guard)` flips the slot, runs
//!    the window-advance loop, and notifies waiters.
//! 3. `Drop before ready -> Aborted` — `Drop` for [`PublishSlotGuard`]
//!    aborts the slot if neither `mark_ready` nor `mark_aborted` was
//!    called. `mark_ready`/`mark_aborted` complete the guard before
//!    returning, so post-durability failures never reach this path.
//!    Post-durability failures route through the engine-fatal poison
//!    path owned by US-036 — see [`PublishSequencer::poison`].
//!
//! `PublishSequencer.published_frontier: AtomicTs` (§10.19 C-1) is the
//! lock-free live frontier consumed by foreign-Pending visibility in
//! §10.20. `mark_ready` publishes the new `ReadEpoch` and the frontier
//! as an ordered pair: the publish closure stores the new epoch first,
//! then `mark_ready` stores `published_frontier` with `Release`. Readers
//! load the epoch with Acquire, then load `published_frontier` with
//! Acquire and retry the pair if `frontier < epoch.visible_ts`.

#![allow(
    dead_code,
    reason = "US-005 lands the sequencer primitive; production CRUD/DDL call sites land in US-012 / US-031."
)]

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::error::{EngineFatalReason, Error, Result};
use crate::mvcc::timestamp::{AtomicTs, TimestampOracle, Ts};

use super::publish::PublishDirty;

/// Mutex backing the `PublishSequencer` inner state. Production code uses
/// `parking_lot::Mutex` for the unfair / fast path; loom permutation
/// harnesses substitute `loom::sync::Mutex` so the scheduler can interleave
/// the register / mark_ready / mark_aborted critical sections required by
/// the §10.19 dense-window contract (§10.13.9 loom-shim requirement).
#[cfg(loom)]
type SequencerMutex<T> = loom::sync::Mutex<T>;
/// Mutex backing the `PublishSequencer` inner state. Production code uses
/// `parking_lot::Mutex` for the unfair / fast path; loom permutation
/// harnesses substitute `loom::sync::Mutex` so the scheduler can interleave
/// the register / mark_ready / mark_aborted critical sections required by
/// the §10.19 dense-window contract (§10.13.9 loom-shim requirement).
#[cfg(not(loom))]
type SequencerMutex<T> = parking_lot::Mutex<T>;

#[cfg(loom)]
type SequencerMutexGuard<'a, T> = loom::sync::MutexGuard<'a, T>;
#[cfg(not(loom))]
type SequencerMutexGuard<'a, T> = parking_lot::MutexGuard<'a, T>;

/// Condvar paired with [`SequencerMutex`]. The loom variant returns a
/// `LockResult` from `wait`; the parking_lot variant takes the guard by
/// `&mut`. The [`wait_seq`] helper hides the API divergence so the
/// production code stays linear.
#[cfg(loom)]
type SequencerCondvar = loom::sync::Condvar;
/// Condvar paired with [`SequencerMutex`]. The loom variant returns a
/// `LockResult` from `wait`; the parking_lot variant takes the guard by
/// `&mut`. The [`wait_seq`] helper hides the API divergence so the
/// production code stays linear.
#[cfg(not(loom))]
type SequencerCondvar = parking_lot::Condvar;

#[cfg(loom)]
fn lock_seq<T>(m: &SequencerMutex<T>) -> SequencerMutexGuard<'_, T> {
    m.lock().expect("publish sequencer mutex not poisoned")
}
#[cfg(not(loom))]
fn lock_seq<T>(m: &SequencerMutex<T>) -> SequencerMutexGuard<'_, T> {
    m.lock()
}

#[cfg(loom)]
fn wait_seq<'a, T>(
    cvar: &SequencerCondvar,
    guard: SequencerMutexGuard<'a, T>,
) -> SequencerMutexGuard<'a, T> {
    cvar.wait(guard)
        .expect("publish sequencer mutex not poisoned across cvar wait")
}
#[cfg(not(loom))]
fn wait_seq<'a, T>(
    cvar: &SequencerCondvar,
    mut guard: SequencerMutexGuard<'a, T>,
) -> SequencerMutexGuard<'a, T> {
    cvar.wait(&mut guard);
    guard
}

/// Boxed publish closure stored in [`PublishSlotState::Ready`].
///
/// `Send + 'static` is required because `mark_ready` may run the closure
/// from a successor thread that owns the sequencer mutex (§10.19).
/// Callers satisfy `'static` by moving an `Arc<SharedState>` clone into
/// the closure rather than borrowing live state.
type PublishClosure = Box<dyn FnOnce(Ts) -> Result<()> + Send + 'static>;

/// State of a single registered publish slot. The sequencer drives slots
/// through the §10.19 three-terminal-state machine.
pub(crate) enum PublishSlotState {
    /// Writer has registered; install/journal not yet complete. Window
    /// advance halts here until the writer transitions to Ready or
    /// Aborted.
    Pending,
    /// Install + journal durable; ready to be published. The closure is
    /// run under the sequencer mutex during the window-advance loop.
    Ready {
        dirty: PublishDirty,
        publish: PublishClosure,
    },
    /// Writer rolled back, the guard was dropped before durability, or
    /// the writer explicitly aborted. Window advance skips this slot
    /// without running any closure.
    Aborted,
}

impl std::fmt::Debug for PublishSlotState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => f.write_str("Pending"),
            Self::Ready { dirty, .. } => f
                .debug_struct("Ready")
                .field("dirty", dirty)
                .finish_non_exhaustive(),
            Self::Aborted => f.write_str("Aborted"),
        }
    }
}

/// Single registered publish slot.
pub(crate) struct PublishSlot {
    pub commit_ts: Ts,
    pub state: PublishSlotState,
}

impl std::fmt::Debug for PublishSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishSlot")
            .field("commit_ts", &self.commit_ts)
            .field("state", &self.state)
            .finish()
    }
}

/// Dense publish-slot sequencer (§10.19).
pub(crate) struct PublishSequencer {
    inner: SequencerMutex<PublishSeqInner>,
    cvar: SequencerCondvar,
    /// §10.19 C-1: lock-free live frontier consumed by foreign-Pending
    /// visibility (§10.20). Monotonically non-decreasing; advanced after
    /// the publish closure stores the new `ReadEpoch`.
    pub(crate) published_frontier: AtomicTs,
}

struct PublishSeqInner {
    /// Next dense `publish_seq` to allocate. `register_with_oracle`
    /// returns this value and increments by 1.
    next_seq: u64,
    /// Highest `publish_seq` whose slot has been published or skipped.
    /// `last_published + 1` is the next slot eligible for window
    /// advancement.
    last_published: u64,
    /// Pending slots keyed by `publish_seq`. Dense in `publish_seq`,
    /// sparse in `commit_ts`.
    pending: BTreeMap<u64, PublishSlot>,
    /// First poison reason recorded by [`PublishSequencer::poison`].
    /// Once set, every register / wait / mark_ready / mark_aborted /
    /// window-advance check returns `Error::EngineFatal { reason }`
    /// without mutating slot state (§10.19.0 C-2 / US-036).
    poisoned: Option<EngineFatalReason>,
}

/// RAII handle returned by [`PublishSequencer::register_with_oracle`].
///
/// Carries the allocated dense `publish_seq` and HLC `commit_ts`.
/// `mark_ready` and `mark_aborted` complete the guard before returning;
/// `Drop` only fires when the guard is forgotten in the pre-durability
/// regime, in which case the slot is flipped to `Aborted` and the
/// sequencer window advances past it.
#[must_use = "PublishSlotGuard must reach mark_ready, mark_aborted, or Drop"]
pub(crate) struct PublishSlotGuard {
    /// Strong reference back to the owning sequencer so guard-drop can
    /// run the abort path without an extra parameter.
    sequencer: Arc<PublishSequencer>,
    seq: u64,
    commit_ts: Ts,
    completed: bool,
}

impl std::fmt::Debug for PublishSlotGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishSlotGuard")
            .field("seq", &self.seq)
            .field("commit_ts", &self.commit_ts)
            .field("completed", &self.completed)
            .finish_non_exhaustive()
    }
}

impl PublishSlotGuard {
    /// Dense publish slot allocated by the sequencer.
    ///
    /// PRD AC #3 names the accessor `publish_seq()` to disambiguate it
    /// from the sparse HLC `commit_ts` and to lock in the §10.19
    /// register/mark_ready contract.
    pub(crate) fn publish_seq(&self) -> u64 {
        self.seq
    }

    /// Sparse HLC commit timestamp allocated alongside the slot.
    pub(crate) fn commit_ts(&self) -> Ts {
        self.commit_ts
    }
}

impl Drop for PublishSlotGuard {
    fn drop(&mut self) {
        // §10.19: forgotten guard before durability becomes Aborted so
        // successors cannot block on a never-arriving slot. mark_ready /
        // mark_aborted set `completed = true` before returning, so
        // post-durable publish-closure failures never reach here.
        if !self.completed {
            self.sequencer.mark_aborted_from_drop(self.seq);
        }
    }
}

impl PublishSequencer {
    /// Construct a fresh sequencer with `published_frontier == Ts::default()`.
    pub(crate) fn new() -> Arc<Self> {
        Self::new_inner(Ts::default())
    }

    /// Construct a fresh sequencer for reopen recovery (§10.29 rule 3).
    ///
    /// The dense slot window is reinitialized (`next_seq = 1`,
    /// `last_published = 0`, empty `pending`) so recovered HLC
    /// timestamps never seed the dense slot counter; only the
    /// lock-free `published_frontier` carries the recovered HLC value.
    pub(crate) fn new_from(recovered_max_commit_ts: Ts) -> Arc<Self> {
        Self::new_inner(recovered_max_commit_ts)
    }

    fn new_inner(initial_frontier: Ts) -> Arc<Self> {
        Arc::new(Self {
            inner: SequencerMutex::new(PublishSeqInner {
                next_seq: 1,
                last_published: 0,
                pending: BTreeMap::new(),
                poisoned: None,
            }),
            cvar: SequencerCondvar::new(),
            published_frontier: AtomicTs::new(initial_frontier),
        })
    }

    /// Allocate `(publish_seq, commit_ts)` atomically and insert a
    /// Pending slot (§10.19).
    ///
    /// Holds the sequencer mutex across the poison check, the HLC
    /// `oracle.commit()` allocation, the dense-slot allocation, and the
    /// Pending-slot insertion. Splitting any of these steps would let
    /// publish-slot order diverge from commit-timestamp order — see
    /// §10.19 frontier-monotonicity rule.
    ///
    /// # Errors
    /// - [`Error::EngineFatal`] if the sequencer has been poisoned by
    ///   `poison_after_durable_commit`; no slot is allocated.
    /// - [`Error::TimestampExhausted`] if the HLC is saturated.
    /// - [`Error::Internal`] if `next_seq` overflows `u64`.
    pub(crate) fn register_with_oracle(
        self: &Arc<Self>,
        oracle: &TimestampOracle,
    ) -> Result<PublishSlotGuard> {
        let mut g = lock_seq(&self.inner);
        if let Some(reason) = g.poisoned.clone() {
            return Err(Error::EngineFatal { reason });
        }
        let commit_ts = oracle.commit()?;
        let seq = g.next_seq;
        g.next_seq = seq
            .checked_add(1)
            .ok_or_else(|| Error::Internal("publish_sequencer next_seq overflowed u64".into()))?;
        g.pending.insert(
            seq,
            PublishSlot {
                commit_ts,
                state: PublishSlotState::Pending,
            },
        );
        Ok(PublishSlotGuard {
            sequencer: Arc::clone(self),
            seq,
            commit_ts,
            completed: false,
        })
    }

    /// Block until either `seq`'s direct predecessor has completed
    /// (`last_published + 1 == seq`) or the sequencer is poisoned.
    ///
    /// # Errors
    /// - [`Error::EngineFatal`] when the sequencer is poisoned (either
    ///   on entry or while waiting).
    pub(crate) fn wait_until_predecessors_complete(&self, seq: u64) -> Result<()> {
        let mut g = lock_seq(&self.inner);
        loop {
            if let Some(reason) = g.poisoned.clone() {
                return Err(Error::EngineFatal { reason });
            }
            if g.last_published.saturating_add(1) >= seq {
                return Ok(());
            }
            g = wait_seq(&self.cvar, g);
        }
    }

    /// Transition `guard` to `Ready { closure }`, run the window-advance
    /// loop, wait until this guard's own dense slot completes, and publish
    /// `published_frontier` after each contiguous closure completes
    /// (§10.19, §10.21 S10).
    ///
    /// `mark_ready` consumes the guard by value and sets
    /// `completed = true` before storing the closure as Ready, so
    /// `Drop for PublishSlotGuard` cannot later flip a Ready slot to
    /// Aborted.
    ///
    /// # Errors
    /// - [`Error::EngineFatal`] when the sequencer is poisoned. The
    ///   slot stays in `pending` so reopen recovery — not the live
    ///   `mark_ready` — owns the durable state (§10.19.0 C-2).
    /// - Any error returned by the publish closure is propagated to the
    ///   caller. A pre-durability failure (caller has not yet completed
    ///   the journal envelope) is normal: the caller must route through
    ///   `mark_aborted` or guard-drop. A post-durable closure failure is
    ///   unrecoverable for the live engine — the caller is required to
    ///   route the error through `poison_after_durable_commit` (§10.19.0
    ///   C-2 / US-036).
    pub(crate) fn mark_ready<F>(
        self: &Arc<Self>,
        mut guard: PublishSlotGuard,
        publish: F,
    ) -> Result<()>
    where
        F: FnOnce(Ts) -> Result<()> + Send + 'static,
    {
        // Capture the Arc identity check before locking so a guard
        // created against a different sequencer is rejected loudly. The
        // sequencer is unique per engine; cross-sequencer mark_ready
        // would be a programming error.
        debug_assert!(
            Arc::ptr_eq(&guard.sequencer, self),
            "PublishSlotGuard mark_ready called on the wrong sequencer instance"
        );
        let target_seq = guard.seq;
        let publish_dirty = PublishDirty::default();
        let mut g = lock_seq(&self.inner);
        if let Some(reason) = g.poisoned.clone() {
            // Leave the slot pending; reopen recovery owns it.
            return Err(Error::EngineFatal { reason });
        }
        let slot = g
            .pending
            .get_mut(&guard.seq)
            .ok_or_else(|| Error::Internal("publish slot missing on mark_ready".into()))?;
        // §10.19 AC #5: disarm the guard BEFORE storing the Ready closure
        // so that a panic between the disarm and the slot mutation cannot
        // let `Drop for PublishSlotGuard` flip a Ready slot back to
        // Aborted. The slot's existence has been validated above.
        guard.completed = true;
        slot.state = PublishSlotState::Ready {
            dirty: publish_dirty,
            publish: Box::new(publish),
        };
        self.advance_window_locked(&mut g)?;
        while g.last_published < target_seq {
            if let Some(reason) = g.poisoned.clone() {
                return Err(Error::EngineFatal { reason });
            }
            g = wait_seq(&self.cvar, g);
            self.advance_window_locked(&mut g)?;
        }
        Ok(())
    }

    /// Transition `guard` to `Aborted`, run the window-advance loop,
    /// and notify waiters (§10.19).
    pub(crate) fn mark_aborted(&self, mut guard: PublishSlotGuard) {
        guard.completed = true;
        let mut g = lock_seq(&self.inner);
        if g.poisoned.is_some() {
            // Sequencer is poisoned; window-advance is a no-op and the
            // slot stays in `pending` for reopen ownership (§10.19.0
            // C-2). Successors are already woken via `poison`.
            return;
        }
        if let Some(slot) = g.pending.get_mut(&guard.seq) {
            slot.state = PublishSlotState::Aborted;
        }
        // Window-advance ignores errors: `mark_aborted` callers cannot
        // surface an `Err` and any failure inside the closure of a later
        // Ready slot must already have routed through poison.
        let _ = self.advance_window_locked(&mut g);
    }

    /// Drop-time abort path. Runs when a `PublishSlotGuard` is dropped
    /// before `mark_ready` or `mark_aborted`. Pre-durability semantics
    /// only — see §10.19.0 C-2 for the post-durable poison contract.
    fn mark_aborted_from_drop(&self, seq: u64) {
        let mut g = lock_seq(&self.inner);
        if g.poisoned.is_some() {
            return;
        }
        if let Some(slot) = g.pending.get_mut(&seq) {
            slot.state = PublishSlotState::Aborted;
        }
        let _ = self.advance_window_locked(&mut g);
    }

    /// Window-advance loop: while `pending.first_entry().key() ==
    /// last_published + 1`, run the slot's terminal action (publish
    /// closure for Ready, no-op for Aborted), advance `last_published`,
    /// and remove the slot. Halts at the first Pending slot.
    ///
    /// Holds the sequencer mutex throughout. The publish closure obeys
    /// the §10.19 closure contract: no `metadata.read()`, no
    /// `PageLatch`, no `journal_mutex`, no post-durable
    /// `WriteConflict`.
    fn advance_window_locked(
        &self,
        g: &mut SequencerMutexGuard<'_, PublishSeqInner>,
    ) -> Result<()> {
        loop {
            // Re-check poison every iteration — a publish closure may
            // route the engine into poison mid-loop, in which case we
            // stop advancing and let successors observe EngineFatal.
            if let Some(reason) = g.poisoned.clone() {
                self.cvar.notify_all();
                return Err(Error::EngineFatal { reason });
            }
            let next = g.last_published.saturating_add(1);
            let Some((&first_key, _)) = g.pending.iter().next() else {
                break;
            };
            if first_key != next {
                break;
            }
            // Take the slot out so the closure (which can re-enter the
            // sequencer through `published_frontier` loads) doesn't
            // borrow `g`.
            let slot = g
                .pending
                .remove(&next)
                .expect("first_entry was Some immediately above");
            match slot.state {
                PublishSlotState::Pending => {
                    // Re-insert and stop: writer hasn't transitioned yet.
                    g.pending.insert(next, slot);
                    break;
                }
                PublishSlotState::Ready { dirty: _, publish } => {
                    // Run publish under the sequencer mutex. §10.19
                    // C-1: store the new epoch first (inside the
                    // closure), then store `published_frontier` with
                    // Release here so readers see the pair coherently.
                    let publish_ts = slot.commit_ts;
                    if let Err(err) = publish(publish_ts) {
                        // Caller (US-005 production wiring lands in
                        // US-012) is responsible for translating
                        // post-durable errors into
                        // `poison_after_durable_commit`. Pre-durable
                        // closure failures bubble up here, but
                        // `mark_ready`'s contract reserves the closure
                        // for the post-flip publish step that runs
                        // after the durable journal envelope completes,
                        // so the live engine should already be moving
                        // toward poison. Notify waiters so they observe
                        // the error or the subsequent poison and stop.
                        self.cvar.notify_all();
                        return Err(err);
                    }
                    self.published_frontier.store(publish_ts, Ordering::Release);
                    g.last_published = next;
                }
                PublishSlotState::Aborted => {
                    g.last_published = next;
                }
            }
        }
        self.cvar.notify_all();
        Ok(())
    }

    /// Record `reason` as the engine-fatal poison reason for the live
    /// sequencer and wake every blocked waiter.
    ///
    /// Preserves the first reason if called more than once; later
    /// poison attempts notify waiters but do not overwrite the stored
    /// reason. Idempotent under repeated calls (§10.19.0 C-2 / US-036).
    pub(crate) fn poison(&self, reason: EngineFatalReason) {
        let mut g = lock_seq(&self.inner);
        if g.poisoned.is_none() {
            g.poisoned = Some(reason);
        }
        self.cvar.notify_all();
    }

    /// Return the recorded poison reason, if any.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn poisoned_reason(&self) -> Option<EngineFatalReason> {
        lock_seq(&self.inner).poisoned.clone()
    }

    /// Highest `publish_seq` whose slot has completed window-advance.
    pub(crate) fn last_published_seq(&self) -> u64 {
        lock_seq(&self.inner).last_published
    }

    /// Test-only: poison-aware Pending-slot register without an HLC
    /// allocation. Used by the US-036 successor-wake regression
    /// fixtures that exercise the poison path independently of the
    /// production register-with-oracle entry point.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn register_for_test(self: &Arc<Self>) -> Result<PublishSlotGuard> {
        let mut g = lock_seq(&self.inner);
        if let Some(reason) = g.poisoned.clone() {
            return Err(Error::EngineFatal { reason });
        }
        let seq = g.next_seq;
        g.next_seq = seq
            .checked_add(1)
            .ok_or_else(|| Error::Internal("publish_sequencer next_seq overflowed u64".into()))?;
        g.pending.insert(
            seq,
            PublishSlot {
                commit_ts: Ts::default(),
                state: PublishSlotState::Pending,
            },
        );
        Ok(PublishSlotGuard {
            sequencer: Arc::clone(self),
            seq,
            commit_ts: Ts::default(),
            completed: false,
        })
    }

    /// Test-only: advance the sequencer past `guard` without running a
    /// publish closure. Used by the US-036 fixtures where the test
    /// poisons the engine and observes successor wake-up; the durable
    /// commit envelope is not exercised so the closure variant is
    /// unnecessary.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn mark_ready_for_test(self: &Arc<Self>, mut guard: PublishSlotGuard) -> Result<()> {
        let mut g = lock_seq(&self.inner);
        if let Some(reason) = g.poisoned.clone() {
            return Err(Error::EngineFatal { reason });
        }
        guard.completed = true;
        // Drop the guard's slot directly: the test path doesn't store a
        // closure, so window-advance treats this as if the slot had
        // been Aborted (advance and remove without publishing).
        if let Some(slot) = g.pending.get_mut(&guard.seq) {
            slot.state = PublishSlotState::Aborted;
        }
        self.advance_window_locked(&mut g)
    }
}

#[cfg(test)]
#[path = "tests/publish_sequencer_tests.rs"]
mod publish_sequencer_tests;
