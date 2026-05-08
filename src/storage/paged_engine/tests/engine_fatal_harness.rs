//! US-036 test probe — engine-fatal poison + sequencer + writer-ticket
//! handles used by `tests/mwmr_crash_recovery.rs`.
//!
//! The probe wraps `pub(crate)` types from `state`, `publish_sequencer`,
//! and `writer_registry` in `pub` opaque handles so integration tests
//! can drive the §10.19.0 C-2 / US-036 contract without exposing the
//! internal sequencer or registry shapes. All entry points are
//! `#[doc(hidden)]` and reached via `Client::__us036_*` accessors.
//!
//! Per the Phase 5 PRD guardrail "Intrusive test code must live in a
//! separate file from the production code it exercises", every
//! US-036 specific test scaffold lives here rather than in
//! `state.rs`, `publish_sequencer.rs`, or `paged_engine.rs`.

#![cfg(any(test, feature = "test-hooks"))]

use std::sync::Arc;
use std::time::Duration;

use super::publish_sequencer::{PublishSequencer, PublishSlotGuard};
use super::state::SharedState;
use super::writer_registry::{NsWriteTicket, NsWriterRegistry};
use super::PagedEngine;

use crate::error::{EngineFatalReason, Result};

/// Opaque test-only handle to a registered publish slot.
///
/// Wraps a [`PublishSlotGuard`] so integration tests can hold the
/// guard across thread boundaries without re-exposing the internal
/// guard type. Drop without [`Self::mark_ready`] leaves the slot
/// pending — enough for US-036 successor-wake regression tests; the
/// pre-durable abort-on-drop semantics belong to US-005.
#[doc(hidden)]
pub struct Us036PublishSlot {
    sequencer: Arc<PublishSequencer>,
    guard: Option<PublishSlotGuard>,
}

impl Us036PublishSlot {
    /// Sequence number assigned to this slot by the sequencer.
    ///
    /// Panics only if the guard has already been taken via
    /// [`Self::mark_ready`]; tests never observe `seq` after that.
    #[must_use]
    #[allow(
        clippy::expect_used,
        reason = "test-only probe: seq() after mark_ready is a test bug worth panicking on"
    )]
    pub fn seq(&self) -> u64 {
        self.guard
            .as_ref()
            .map(PublishSlotGuard::publish_seq)
            .expect("PublishSlotGuard taken before seq() observed")
    }

    /// Block until either the slot's predecessor has marked ready or
    /// the sequencer has been poisoned. Returns
    /// [`crate::Error::EngineFatal`] in the poison case.
    ///
    /// # Errors
    /// Returns [`crate::Error::EngineFatal`] when the sequencer is
    /// poisoned (either on entry or while waiting).
    pub fn wait_for_predecessor_or_poison(&self) -> Result<()> {
        let seq = self.seq();
        self.sequencer.wait_until_predecessors_complete(seq)
    }

    /// Mark this slot ready, consuming the guard so it cannot be
    /// re-used. Advances the sequencer's window past the slot.
    ///
    /// # Errors
    /// Returns [`crate::Error::EngineFatal`] when the sequencer is
    /// poisoned. The slot is left in `pending` so reopen recovery owns
    /// the durable state.
    pub fn mark_ready(mut self) -> Result<()> {
        if let Some(guard) = self.guard.take() {
            return self.sequencer.mark_ready_for_test(guard);
        }
        Ok(())
    }
}

/// Opaque test-only handle to an admitted writer ticket.
///
/// Wraps an [`NsWriteTicket`] so integration tests can hold the ticket
/// across threads and drop it on demand to simulate the post-poison
/// "writer drops ticket and DDL drain unblocks" path from
/// `test_ddl_drain_completes_after_poisoned_successor_drops_ticket`.
#[doc(hidden)]
#[must_use = "Us036WriterTicket holds an admit slot; drop or call drop_ticket() to release"]
pub struct Us036WriterTicket {
    ticket: Option<NsWriteTicket>,
}

impl Us036WriterTicket {
    /// Drop the wrapped [`NsWriteTicket`] explicitly so the test sees
    /// the release at a deterministic point.
    pub fn drop_ticket(mut self) {
        let _ = self.ticket.take();
    }
}

impl PagedEngine {
    /// Test-only US-036 hook: poison the live engine with `reason`.
    /// Routes through [`super::state::poison_after_durable_commit`] so
    /// every blocked sequencer waiter wakes with `EngineFatal`.
    pub(crate) fn us036_test_poison_engine(&self, reason: EngineFatalReason) {
        // Drop the returned Error; the test hook only mutates state.
        let _ = super::state::poison_after_durable_commit(&self.shared, reason);
    }

    /// Test-only US-036 hook: read the current engine poison reason.
    pub(crate) fn us036_test_poisoned_reason(&self) -> Option<EngineFatalReason> {
        self.shared.engine_poisoned.lock().clone()
    }

    /// Test-only US-036 hook: register a publish slot directly.
    ///
    /// # Errors
    /// Returns [`crate::Error::EngineFatal`] when the sequencer is
    /// already poisoned.
    pub(crate) fn us036_test_register_publish_slot(&self) -> Result<Us036PublishSlot> {
        let sequencer: Arc<PublishSequencer> = Arc::clone(&self.shared.publish_sequencer);
        let guard = sequencer.register_for_test()?;
        Ok(Us036PublishSlot {
            sequencer,
            guard: Some(guard),
        })
    }

    /// Test-only US-036 hook: admit a writer ticket on `ns_id`.
    ///
    /// # Errors
    /// Returns [`crate::Error::WriterBusy`] when the lane is closed
    /// past `timeout`.
    pub(crate) fn us036_test_admit_writer(
        &self,
        ns_id: i64,
        timeout_ms: u64,
    ) -> Result<Us036WriterTicket> {
        let registry: Arc<NsWriterRegistry> = Arc::clone(&self.shared.ns_writers);
        let ticket = registry.admit(ns_id, Duration::from_millis(timeout_ms))?;
        Ok(Us036WriterTicket {
            ticket: Some(ticket),
        })
    }

    /// Test-only US-036 hook: close-and-drain a namespace lane.
    ///
    /// # Errors
    /// Returns [`crate::Error::WriterBusy`] when the lane fails to
    /// drain within `timeout`.
    pub(crate) fn us036_test_close_and_drain(&self, ns_id: i64, timeout_ms: u64) -> Result<()> {
        self.shared
            .ns_writers
            .close_and_drain(ns_id, Duration::from_millis(timeout_ms))
    }

    /// Test-only US-036 hook: resolve the durable `ns_id` for `ns` from
    /// the published catalog snapshot.
    ///
    /// §10.19.0 C-2 / US-036 (AC #7): test-hook entry points
    /// fail-closed once the engine is poisoned. Returns
    /// [`crate::Error::EngineFatal`] before reading any engine state.
    ///
    /// # Errors
    /// Returns [`crate::Error::EngineFatal`] when the live engine is
    /// poisoned.
    pub(crate) fn us036_test_namespace_id(&self, ns: &str) -> Result<Option<i64>> {
        self.shared.check_engine_not_poisoned()?;
        let snap = self.shared.load_published();
        Ok(snap.catalog.namespace_id_by_name.get(ns).copied())
    }

    /// Borrow the live `Arc<SharedState>` for cross-module probes.
    /// Currently unused outside the probe module — kept private.
    #[allow(
        dead_code,
        reason = "scaffold for future US-036 cross-probe extensions"
    )]
    pub(super) fn us036_test_shared(&self) -> Arc<SharedState> {
        Arc::clone(&self.shared)
    }
}
