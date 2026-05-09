//! `NsWriterRegistry` — per-collection DDL admission barrier.
//!
//! The current CRUD path is fenced by `PagedEngine::metadata.read()` and does
//! not admit through these lanes. The registry remains as a narrow DDL/probe
//! primitive for code paths that explicitly need close-and-drain semantics by
//! durable `CollectionEntry.id: i64`.
//!
//! Keys are `i64` `ns_id` values from the durable catalog. The registry
//! never keys by namespace name.

#![allow(
    dead_code,
    reason = "admission tickets are retained for DDL/probe paths and unit coverage"
)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::{Condvar, Mutex};

use crate::error::{Error, Result};

fn record_lane_wait_since(start: Instant) {
    let waited_ns = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    crate::mvcc::metrics::record_lane_wait_ns(waited_ns);
}

/// One entry per namespace. Populated lazily on first explicit admit.
///
/// Tests use `(admits - releases) == active_writers` to assert balance.
pub(crate) struct NsWriterLane {
    inner: Mutex<NsWriterLaneInner>,
    cvar: Condvar,
}

struct NsWriterLaneInner {
    /// Monotonic count of admitted writers.
    admits: u64,
    /// Monotonic count of released writers (via `NsWriteTicket::drop`).
    releases: u64,
    /// True while an admitted probe or specialized path is active.
    body_active: bool,
    /// True when a DDL caller has closed the lane to new admits.
    closed: bool,
}

/// Permit returned by [`NsWriterRegistry::admit`]. Drop bumps `releases`
/// and notifies any DDL thread waiting in `close_and_drain` if the lane
/// has fully drained.
pub(crate) struct NsWriteTicket {
    lane: Arc<NsWriterLane>,
    body_active: bool,
}

impl NsWriteTicket {
    fn clear_body_active(&mut self, g: &mut NsWriterLaneInner) -> bool {
        if !self.body_active {
            return false;
        }
        if g.body_active {
            g.body_active = false;
        }
        self.body_active = false;
        true
    }

    /// Release the same-namespace body-entry mutex while retaining the DDL
    /// drain ticket through the durability and publish envelope.
    pub(crate) fn finish_body(&mut self) {
        let lane = Arc::clone(&self.lane);
        let mut g = lane.inner.lock();
        if self.clear_body_active(&mut g) {
            lane.cvar.notify_all();
        }
    }
}

impl Drop for NsWriteTicket {
    fn drop(&mut self) {
        let lane = Arc::clone(&self.lane);
        let mut g = lane.inner.lock();
        self.clear_body_active(&mut g);
        g.releases = g.releases.saturating_add(1);
        if !g.body_active || g.admits == g.releases {
            lane.cvar.notify_all();
        }
    }
}

/// Per-collection writer admission registry.
pub(crate) struct NsWriterRegistry {
    /// Phase 1 `CollectionEntry.id` -> lane. Removed by `drop_namespace`
    /// under `metadata.write()` *after* the drain completes.
    lanes: DashMap<i64, Arc<NsWriterLane>>,
}

impl NsWriterRegistry {
    /// Construct an empty registry.
    pub(crate) fn new() -> Self {
        Self {
            lanes: DashMap::new(),
        }
    }

    /// Register the calling thread as active on the namespace identified by
    /// `ns_id`. Returns `Err(WriterBusy)` if the lane stays closed or
    /// occupied past `timeout`.
    ///
    /// # Errors
    /// - [`Error::WriterBusy`] if a DDL `close_and_drain_guard` keeps the
    ///   lane closed, or another explicit admit keeps the lane occupied, for
    ///   the full `timeout`.
    pub(crate) fn admit(&self, ns_id: i64, timeout: Duration) -> Result<NsWriteTicket> {
        let lane = self
            .lanes
            .entry(ns_id)
            .or_insert_with(|| {
                Arc::new(NsWriterLane {
                    inner: Mutex::new(NsWriterLaneInner {
                        admits: 0,
                        releases: 0,
                        body_active: false,
                        closed: false,
                    }),
                    cvar: Condvar::new(),
                })
            })
            .value()
            .clone();

        let start = Instant::now();
        let mut g = lane.inner.lock();
        loop {
            if g.closed || g.body_active {
                let remaining = timeout.checked_sub(start.elapsed()).unwrap_or_default();
                if remaining.is_zero() {
                    record_lane_wait_since(start);
                    return Err(Error::WriterBusy);
                }
                let wr = lane.cvar.wait_for(&mut g, remaining);
                if wr.timed_out() && (g.closed || g.body_active) {
                    record_lane_wait_since(start);
                    return Err(Error::WriterBusy);
                }
                continue;
            }
            g.admits = g.admits.saturating_add(1);
            record_lane_wait_since(start);
            return Ok(NsWriteTicket {
                lane: lane.clone(),
                body_active: true,
            });
        }
    }

    /// Close admissions on `ns_id` and block until `admits == releases`.
    /// Sets `closed = true` BEFORE waiting so new admits cannot starve
    /// the drain.
    ///
    /// # Errors
    /// - [`Error::WriterBusy`] if drainage does not complete within
    ///   `timeout`.
    pub(crate) fn close_and_drain(&self, ns_id: i64, timeout: Duration) -> Result<()> {
        let Some(lane) = self.lanes.get(&ns_id).map(|e| e.value().clone()) else {
            return Ok(());
        };
        let start = Instant::now();
        let mut g = lane.inner.lock();
        g.closed = true;
        while g.admits != g.releases {
            let remaining = timeout.checked_sub(start.elapsed()).unwrap_or_default();
            if remaining.is_zero() {
                return Err(Error::WriterBusy);
            }
            let wr = lane.cvar.wait_for(&mut g, remaining);
            if wr.timed_out() && g.admits != g.releases {
                return Err(Error::WriterBusy);
            }
        }
        Ok(())
    }

    /// Reopen a closed lane and wake any pending `admit` waiters.
    ///
    /// Only the [`NsDdlBarrierGuard`] implementation should call this —
    /// DDL call sites must go through the guard, never directly.
    pub(crate) fn reopen(&self, ns_id: i64) {
        if let Some(lane) = self.lanes.get(&ns_id).map(|e| e.value().clone()) {
            let mut g = lane.inner.lock();
            g.closed = false;
            lane.cvar.notify_all();
        }
    }

    /// Remove a lane entry. Called by `drop_namespace` AFTER
    /// `close_and_drain` returns and AFTER page-free completes
    /// (§10.1.3, §10.8.3).
    pub(crate) fn remove(&self, ns_id: i64) {
        self.lanes.remove(&ns_id);
    }

    /// Close + drain `ns_id` and return an RAII [`NsDdlBarrierGuard`].
    /// On guard `Drop` (without `commit`/`mark_dropped`) the lane is
    /// reopened so a panicking DDL — or a drain timeout — cannot
    /// permanently close a namespace.
    ///
    /// The guard is constructed BEFORE the drain wait so a drain
    /// timeout returns the lane to its open state via guard `Drop`.
    ///
    /// # Errors
    /// - [`Error::WriterBusy`] if drainage does not complete within
    ///   `timeout`. The guard's `Drop` reopens the lane.
    pub(crate) fn close_and_drain_guard(
        self: &Arc<Self>,
        ns_id: i64,
        timeout: Duration,
    ) -> Result<NsDdlBarrierGuard> {
        let guard = NsDdlBarrierGuard {
            registry: Arc::clone(self),
            ns_id,
            state: NsDdlBarrierState::Closed,
        };
        self.close_and_drain(ns_id, timeout)?;
        Ok(guard)
    }
}

/// RAII guard that ties DDL barrier close/reopen to scope (§10.27).
///
/// On `Drop`:
/// - [`NsDdlBarrierState::Closed`] → `reopen` (DDL aborted before
///   committing; lane must remain available).
/// - [`NsDdlBarrierState::MarkedDropped`] → `remove` (namespace dropped).
/// - [`NsDdlBarrierState::Committed`] → no-op (`commit()` already
///   reopened explicitly).
#[must_use = "NsDdlBarrierGuard must be committed or dropped explicitly"]
pub(crate) struct NsDdlBarrierGuard {
    registry: Arc<NsWriterRegistry>,
    ns_id: i64,
    state: NsDdlBarrierState,
}

enum NsDdlBarrierState {
    /// Gate closed; on Drop will reopen.
    Closed,
    /// DDL caller confirmed the namespace is being dropped; on Drop
    /// the lane will be removed instead of reopened.
    MarkedDropped,
    /// Explicitly settled: Drop is a no-op because `commit()` already
    /// performed the reopen.
    Committed,
}

impl NsDdlBarrierGuard {
    /// Mark this DDL as a namespace drop. Drop will `remove` the lane.
    pub(crate) fn mark_dropped(&mut self) {
        self.state = NsDdlBarrierState::MarkedDropped;
    }

    /// Successful non-drop DDL: reopen admissions immediately and flip
    /// to `Committed` so the subsequent Drop is a no-op.
    pub(crate) fn commit(mut self) {
        self.registry.reopen(self.ns_id);
        self.state = NsDdlBarrierState::Committed;
    }
}

impl Drop for NsDdlBarrierGuard {
    fn drop(&mut self) {
        match self.state {
            NsDdlBarrierState::Closed => self.registry.reopen(self.ns_id),
            NsDdlBarrierState::MarkedDropped => self.registry.remove(self.ns_id),
            NsDdlBarrierState::Committed => {}
        }
    }
}

#[cfg(test)]
#[path = "tests/writer_registry.rs"]
mod tests;
