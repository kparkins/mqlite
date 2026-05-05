//! `NsWriterRegistry` — per-collection writer admission lanes (§10.1).
//!
//! Phase 5 §10.1 / §10.27. Hot-path CRUD writers `admit` against the lane
//! identified by the Phase 1 durable `CollectionEntry.id: i64`; DDL paths
//! `close_and_drain_guard` to gate new admits and drain in-flight writers
//! before mutating the catalog.
//!
//! Keys are `i64` `ns_id` values from the durable catalog. The registry
//! never keys by namespace name (§10.1.2).
//!
//! US-004 ships the primitive in isolation. The CRUD callers in
//! `run_write_existing` (US-012) and the DDL callers in
//! `drop_namespace` / `create_index_*` / `drop_index` (US-008, US-013,
//! US-023, US-024) wire `admit`, `close_and_drain_guard`, `mark_dropped`,
//! and `commit` from later Phase 5 stories. Until then the methods are
//! exercised only by the `#[cfg(test)]` unit tests below.

#![allow(
    dead_code,
    reason = "US-004 ships the registry primitive; call sites land in US-005+"
)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::{Condvar, Mutex};

use crate::error::{Error, Result};

/// One entry per namespace. Populated lazily on first writer admit.
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
    /// True when a DDL caller has closed the lane to new admits.
    closed: bool,
}

/// Permit returned by [`NsWriterRegistry::admit`]. Drop bumps `releases`
/// and notifies any DDL thread waiting in `close_and_drain` if the lane
/// has fully drained.
pub(crate) struct NsWriteTicket {
    lane: Arc<NsWriterLane>,
}

impl Drop for NsWriteTicket {
    fn drop(&mut self) {
        let mut g = self.lane.inner.lock();
        g.releases = g.releases.saturating_add(1);
        if g.admits == g.releases {
            self.lane.cvar.notify_all();
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

    /// Register the calling thread as an active writer on the namespace
    /// identified by `ns_id`. Returns `Err(WriterBusy)` if the lane stays
    /// closed past `timeout`.
    ///
    /// # Errors
    /// - [`Error::WriterBusy`] if a DDL `close_and_drain_guard` keeps the
    ///   lane closed for the full `timeout`.
    pub(crate) fn admit(&self, ns_id: i64, timeout: Duration) -> Result<NsWriteTicket> {
        let lane = self
            .lanes
            .entry(ns_id)
            .or_insert_with(|| {
                Arc::new(NsWriterLane {
                    inner: Mutex::new(NsWriterLaneInner {
                        admits: 0,
                        releases: 0,
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
            if g.closed {
                let remaining = timeout.checked_sub(start.elapsed()).unwrap_or_default();
                if remaining.is_zero() {
                    return Err(Error::WriterBusy);
                }
                let wr = lane.cvar.wait_for(&mut g, remaining);
                if wr.timed_out() && g.closed {
                    return Err(Error::WriterBusy);
                }
                continue;
            }
            g.admits = g.admits.saturating_add(1);
            return Ok(NsWriteTicket { lane: lane.clone() });
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
mod tests {
    use super::*;
    use std::thread;

    const NS_ID: i64 = 42;
    const ADMIT_TIMEOUT: Duration = Duration::from_secs(5);
    const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
    const ZERO_TIMEOUT: Duration = Duration::from_millis(0);

    fn lane_counters(reg: &NsWriterRegistry, ns_id: i64) -> (u64, u64, bool) {
        let lane = reg
            .lanes
            .get(&ns_id)
            .map(|e| e.value().clone())
            .expect("lane present");
        let g = lane.inner.lock();
        (g.admits, g.releases, g.closed)
    }

    #[test]
    fn test_admit_and_release_counters_balance() {
        let reg = Arc::new(NsWriterRegistry::new());
        let t1 = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("admit 1");
        let t2 = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("admit 2");
        let t3 = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("admit 3");
        let (admits, releases, closed) = lane_counters(&reg, NS_ID);
        assert_eq!(admits, 3, "admits bumped per admit");
        assert_eq!(releases, 0, "no releases yet");
        assert!(!closed);
        drop(t2);
        let (admits, releases, _) = lane_counters(&reg, NS_ID);
        assert_eq!(admits, 3);
        assert_eq!(releases, 1, "one release after first drop");
        drop(t1);
        drop(t3);
        let (admits, releases, _) = lane_counters(&reg, NS_ID);
        assert_eq!(admits, releases, "all tickets released");
        assert_eq!(admits, 3);
    }

    #[test]
    fn test_close_and_drain_blocks_new_admits() {
        let reg = Arc::new(NsWriterRegistry::new());
        // Prime the lane.
        let t = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("prime admit");
        // Drain in a worker — it must block until the prime ticket drops.
        let reg_drain = Arc::clone(&reg);
        let drain_handle = thread::spawn(move || reg_drain.close_and_drain(NS_ID, DRAIN_TIMEOUT));
        // Give the drain thread time to set closed=true and start waiting.
        thread::sleep(Duration::from_millis(50));
        // While drain is waiting, admit must fail with WriterBusy at zero timeout
        // because closed=true.
        let busy = reg.admit(NS_ID, ZERO_TIMEOUT);
        assert!(matches!(busy, Err(Error::WriterBusy)));
        // Release the priming writer; drain should now finish.
        drop(t);
        drain_handle
            .join()
            .expect("drain thread joined")
            .expect("drain succeeds after release");
        // Lane stays closed until reopen is called by the guard.
        let (_, _, closed) = lane_counters(&reg, NS_ID);
        assert!(closed, "drain leaves lane closed");
    }

    #[test]
    fn test_close_cannot_be_starved_by_new_admits() {
        // §10.13.6 — close_and_drain sets closed=true BEFORE waiting, so
        // new admits queued after close cannot delay drain progress.
        let reg = Arc::new(NsWriterRegistry::new());
        let prime = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("prime admit");

        // Start the drain first; it will acquire the lane mutex, flip
        // closed=true, then wait for prime to drop. Hammer is spawned
        // only after closed=true is observable so the test can assert
        // that NO admit succeeds after the gate is set.
        let reg_drain = Arc::clone(&reg);
        let drain_handle = thread::spawn(move || reg_drain.close_and_drain(NS_ID, DRAIN_TIMEOUT));

        // Spin until the drain has flipped closed=true. Bounded by the
        // drain timeout itself so a buggy implementation cannot deadlock
        // the test.
        let close_observed_deadline = Instant::now() + DRAIN_TIMEOUT;
        loop {
            let (_, _, closed) = lane_counters(&reg, NS_ID);
            if closed {
                break;
            }
            assert!(
                Instant::now() < close_observed_deadline,
                "drain failed to set closed=true within DRAIN_TIMEOUT"
            );
            thread::sleep(Duration::from_millis(1));
        }

        // Now hammer admit() at zero timeout — every attempt must fail
        // because the gate is already closed.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop_hammer = Arc::clone(&stop);
        let reg_hammer = Arc::clone(&reg);
        let hammer = thread::spawn(move || {
            let mut admits_after_close: u64 = 0;
            while !stop_hammer.load(std::sync::atomic::Ordering::Acquire) {
                if reg_hammer.admit(NS_ID, ZERO_TIMEOUT).is_ok() {
                    admits_after_close = admits_after_close.saturating_add(1);
                }
            }
            admits_after_close
        });

        // Let hammer pound the closed gate for a measurable window.
        thread::sleep(Duration::from_millis(75));

        // Drop the prime ticket so drain can finish.
        drop(prime);
        let drain_res = drain_handle.join().expect("drain thread joined");
        assert!(
            drain_res.is_ok(),
            "drain must complete within timeout despite hammer"
        );

        stop.store(true, std::sync::atomic::Ordering::Release);
        let admits_after_close = hammer.join().expect("hammer joined");
        assert_eq!(
            admits_after_close, 0,
            "no new admits succeeded once close set the gate"
        );
    }

    #[test]
    fn test_barrier_guard_drop_without_commit_reopens() {
        let reg = Arc::new(NsWriterRegistry::new());
        // Seed a lane so close_and_drain_guard has something to act on.
        drop(reg.admit(NS_ID, ADMIT_TIMEOUT).expect("seed admit"));
        {
            let _guard = reg
                .close_and_drain_guard(NS_ID, DRAIN_TIMEOUT)
                .expect("close_and_drain_guard");
            let (_, _, closed) = lane_counters(&reg, NS_ID);
            assert!(closed, "guard scope keeps lane closed");
        }
        // After guard Drop without commit/mark_dropped → lane reopened.
        let (_, _, closed) = lane_counters(&reg, NS_ID);
        assert!(!closed, "Drop in Closed state reopens the lane");
        // And new admits succeed again.
        let _t = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("admit after reopen");
    }

    #[test]
    fn test_barrier_guard_commit_reopens_immediately() {
        let reg = Arc::new(NsWriterRegistry::new());
        drop(reg.admit(NS_ID, ADMIT_TIMEOUT).expect("seed admit"));
        let guard = reg
            .close_and_drain_guard(NS_ID, DRAIN_TIMEOUT)
            .expect("close_and_drain_guard");
        guard.commit();
        // After commit, the lane is open and admits succeed.
        let (_, _, closed) = lane_counters(&reg, NS_ID);
        assert!(!closed, "commit reopens immediately");
        let _t = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("admit after commit");
    }

    #[test]
    fn test_close_and_drain_guard_timeout_reopens() {
        // Regression: a drain timeout must not leave the lane closed.
        // `close_and_drain_guard` constructs the guard BEFORE the drain
        // wait, so a `WriterBusy` propagation drops the guard, which
        // reopens the lane in `Closed` state.
        let reg = Arc::new(NsWriterRegistry::new());
        let prime = reg.admit(NS_ID, ADMIT_TIMEOUT).expect("prime admit");
        // ZERO_TIMEOUT forces the drain to fail immediately because
        // prime is still in flight.
        match reg.close_and_drain_guard(NS_ID, ZERO_TIMEOUT) {
            Ok(_) => panic!("drain must time out while prime is still admitted"),
            Err(Error::WriterBusy) => {}
            Err(other) => panic!("expected WriterBusy, got: {other:?}"),
        }
        // Guard is dropped on the error path; reopen must have cleared
        // `closed` so subsequent admits succeed.
        let (_, _, closed) = lane_counters(&reg, NS_ID);
        assert!(!closed, "drain timeout must reopen the lane via guard Drop");
        // Drop prime and confirm a fresh admit + drain cycle works.
        drop(prime);
        let _t = reg
            .admit(NS_ID, ADMIT_TIMEOUT)
            .expect("admit after drain-timeout reopen");
    }

    #[test]
    fn test_barrier_guard_mark_dropped_removes_lane() {
        let reg = Arc::new(NsWriterRegistry::new());
        drop(reg.admit(NS_ID, ADMIT_TIMEOUT).expect("seed admit"));
        {
            let mut guard = reg
                .close_and_drain_guard(NS_ID, DRAIN_TIMEOUT)
                .expect("close_and_drain_guard");
            guard.mark_dropped();
        }
        // Drop in MarkedDropped state must remove the lane entry.
        assert!(
            reg.lanes.get(&NS_ID).is_none(),
            "MarkedDropped guard Drop removes the lane"
        );
    }
}
