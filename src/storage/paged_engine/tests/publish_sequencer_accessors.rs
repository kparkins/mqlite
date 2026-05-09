//! Test-only helpers for driving `PublishSequencer` directly.
//!
//! The production engine uses `register_with_oracle`, `mark_ready`, and
//! `mark_aborted` through the commit path. These helpers exist for unit and
//! integration tests that need a standalone sequencer or direct visibility
//! into its dense window.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;

use super::{
    lock_seq, wait_seq, PublishSequencer, PublishSlot, PublishSlotGuard, PublishSlotState,
};

impl PublishSequencer {
    /// Construct a fresh test sequencer with `published_frontier == Ts::default()`.
    pub(crate) fn new() -> Arc<Self> {
        Self::new_inner(Ts::default(), 1)
    }

    /// Block until either `seq`'s direct predecessor has completed
    /// (`last_published + 1 == seq`) or the sequencer is poisoned.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EngineFatal`] when the sequencer is poisoned.
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

    /// Highest `publish_seq` whose slot has completed window-advance.
    pub(crate) fn last_published_seq(&self) -> u64 {
        lock_seq(&self.inner).last_published
    }

    /// Poison-aware pending-slot register without an HLC allocation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EngineFatal`] when the sequencer is poisoned, or
    /// [`Error::Internal`] if the dense sequence counter overflows.
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

    /// Advance the sequencer past `guard` without running a publish closure.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EngineFatal`] when the sequencer is poisoned.
    pub(crate) fn mark_ready_for_test(self: &Arc<Self>, mut guard: PublishSlotGuard) -> Result<()> {
        let mut g = lock_seq(&self.inner);
        if let Some(reason) = g.poisoned.clone() {
            return Err(Error::EngineFatal { reason });
        }
        guard.completed = true;
        if let Some(slot) = g.pending.get_mut(&guard.seq) {
            slot.state = PublishSlotState::Aborted;
        }
        self.advance_window_locked(&mut g)
    }
}
