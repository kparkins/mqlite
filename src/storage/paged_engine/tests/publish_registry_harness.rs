//! US-020 test probes for the publish sequencer and writer registry.
//!
//! The Phase 5 integration matrix needs to drive the internal publish
//! sequencer and namespace-writer registry directly while still running under
//! the plain `cargo test --release --tests` gate. These wrappers keep that
//! intrusive test surface outside the production modules they exercise.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::error::{EngineFatalReason, Error, Result};
use crate::mvcc::timestamp::{TimestampOracle, Ts};

use super::publish_sequencer::{PublishSequencer, PublishSlotGuard};
use super::writer_registry::{NsWriteTicket, NsWriterRegistry};

/// Test handle for a standalone Phase 5 publish sequencer.
#[doc(hidden)]
#[derive(Clone)]
pub struct Us020PublishSequencer {
    sequencer: Arc<PublishSequencer>,
    oracle: Arc<TimestampOracle>,
}

impl Us020PublishSequencer {
    /// Construct a fresh sequencer and timestamp oracle.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sequencer: PublishSequencer::new(),
            oracle: Arc::new(TimestampOracle::new()),
        }
    }

    /// Register one publish slot using the sequencer-owned timestamp oracle.
    ///
    /// # Errors
    ///
    /// Returns `Error::EngineFatal` when the sequencer is poisoned, or the
    /// timestamp-oracle error if the synthetic oracle is exhausted.
    pub fn register(&self) -> Result<Us020PublishSlot> {
        let guard = self.sequencer.register_with_oracle(&self.oracle)?;
        Ok(Us020PublishSlot { guard: Some(guard) })
    }

    /// Raise the oracle floor so the next registered slot skips ahead.
    pub fn set_oracle_min(&self, min: Ts) {
        self.oracle.set_min(min);
    }

    /// Mark a slot ready with a no-op publish closure.
    ///
    /// # Errors
    ///
    /// Returns any sequencer or publish-closure error.
    pub fn mark_ready(&self, slot: Us020PublishSlot) -> Result<()> {
        self.mark_ready_with(slot, |_seq| Ok(()))
    }

    /// Mark a slot ready and record the dense publish sequence when its
    /// publish closure actually runs.
    ///
    /// # Errors
    ///
    /// Returns any sequencer error or an internal error if the record log is
    /// poisoned.
    pub fn mark_ready_recording(
        &self,
        slot: Us020PublishSlot,
        log: Arc<Mutex<Vec<u64>>>,
    ) -> Result<()> {
        self.mark_ready_with(slot, move |seq| {
            let mut guard = log
                .lock()
                .map_err(|_| Error::Internal("US-020 record log poisoned".into()))?;
            guard.push(seq);
            Ok(())
        })
    }

    /// Mark a slot ready with a publish closure that returns an injected
    /// post-durable error.
    ///
    /// # Errors
    ///
    /// Always returns the injected internal error unless the sequencer is
    /// already poisoned.
    pub fn mark_ready_failing(&self, slot: Us020PublishSlot) -> Result<()> {
        self.mark_ready_with(slot, |_seq| {
            Err(Error::Internal(
                "US-020 injected post-journal publish failure".into(),
            ))
        })
    }

    fn mark_ready_with<F>(&self, mut slot: Us020PublishSlot, publish: F) -> Result<()>
    where
        F: FnOnce(u64) -> Result<()> + Send + 'static,
    {
        let seq = slot.seq()?;
        let guard = slot.take_guard()?;
        self.sequencer
            .mark_ready(guard, move |_publish_ts| publish(seq))
    }

    /// Mark a slot aborted, simulating pre-durability cleanup.
    pub fn mark_aborted(&self, mut slot: Us020PublishSlot) {
        if let Ok(guard) = slot.take_guard() {
            self.sequencer.mark_aborted(guard);
        }
    }

    /// Wait until every predecessor of `seq` has completed, or until poison.
    ///
    /// # Errors
    ///
    /// Returns `Error::EngineFatal` if the sequencer is poisoned while the
    /// caller is waiting.
    pub fn wait_for_predecessor_or_poison(&self, seq: u64) -> Result<()> {
        self.sequencer.wait_until_predecessors_complete(seq)
    }

    /// Poison the sequencer with an engine-fatal reason and wake waiters.
    pub fn poison(&self, reason: EngineFatalReason) {
        self.sequencer.poison(reason);
    }

    /// Highest dense publish sequence completed by the sequencer.
    #[must_use]
    pub fn last_published_seq(&self) -> u64 {
        self.sequencer.last_published_seq()
    }

    /// Current live published frontier timestamp.
    #[must_use]
    pub fn published_frontier(&self) -> Ts {
        self.sequencer
            .published_frontier
            .load(std::sync::atomic::Ordering::Acquire)
    }
}

impl Default for Us020PublishSequencer {
    fn default() -> Self {
        Self::new()
    }
}

/// Test handle for one registered publish slot.
#[doc(hidden)]
pub struct Us020PublishSlot {
    guard: Option<PublishSlotGuard>,
}

impl Us020PublishSlot {
    /// Dense publish sequence allocated for this slot.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the slot was already consumed.
    pub fn seq(&self) -> Result<u64> {
        self.guard
            .as_ref()
            .map(PublishSlotGuard::publish_seq)
            .ok_or_else(|| Error::Internal("US-020 publish slot already consumed".into()))
    }

    /// Commit timestamp allocated for this slot.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the slot was already consumed.
    pub fn commit_ts(&self) -> Result<Ts> {
        self.guard
            .as_ref()
            .map(PublishSlotGuard::commit_ts)
            .ok_or_else(|| Error::Internal("US-020 publish slot already consumed".into()))
    }

    fn take_guard(&mut self) -> Result<PublishSlotGuard> {
        self.guard
            .take()
            .ok_or_else(|| Error::Internal("US-020 publish slot already consumed".into()))
    }
}

/// Test handle for a standalone namespace-writer registry.
#[doc(hidden)]
#[derive(Clone)]
pub struct Us020WriterRegistry {
    registry: Arc<NsWriterRegistry>,
}

impl Us020WriterRegistry {
    /// Construct a fresh namespace-writer registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            registry: Arc::new(NsWriterRegistry::new()),
        }
    }

    /// Admit one writer to `ns_id`.
    ///
    /// # Errors
    ///
    /// Returns `Error::WriterBusy` if the namespace lane remains closed for
    /// the provided timeout.
    pub fn admit(&self, ns_id: i64, timeout_ms: u64) -> Result<Us020WriterTicket> {
        let ticket = self
            .registry
            .admit(ns_id, Duration::from_millis(timeout_ms))?;
        Ok(Us020WriterTicket {
            ticket: Some(ticket),
        })
    }

    /// Close the lane for `ns_id` and wait for admitted writers to drain.
    ///
    /// # Errors
    ///
    /// Returns `Error::WriterBusy` if drainage does not complete within the
    /// provided timeout.
    pub fn close_and_drain(&self, ns_id: i64, timeout_ms: u64) -> Result<()> {
        self.registry
            .close_and_drain(ns_id, Duration::from_millis(timeout_ms))
    }
}

impl Default for Us020WriterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Test handle for an admitted namespace-writer ticket.
#[doc(hidden)]
#[must_use = "dropping Us020WriterTicket releases the admitted writer"]
pub struct Us020WriterTicket {
    ticket: Option<NsWriteTicket>,
}

impl Us020WriterTicket {
    /// Release this writer ticket immediately.
    pub fn release(mut self) {
        let _ = self.ticket.take();
    }
}
