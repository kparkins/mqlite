//! `ReadView` — the snapshot primitive every reader holds.
//!
//! See MVCC plan §T3 / §S13. Each open reader holds a `ReadView` pinning
//! its `read_ts`; the version-chain walker uses `read_ts` to pick the
//! visible entry. T4 adds the `ReadViewRegistry` that tracks live
//! `ReadView`s so the writer can compute `oldest_required_ts`.
//!
//! The `poisoned` flag and `pin_ops_in_flight` counter together support
//! T8 force-expiry (MAJOR-1): `force_expire` flips `poisoned`, then spins
//! until `pin_ops_in_flight` reaches 0, so no concurrent pin walk can be
//! mid-flight when pages are released.
//!
//! Production-path atomics use the cfg(loom) shim pattern so future
//! concurrency harnesses (T4, T8) can permute them.

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU32};

#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU32};

use crate::mvcc::timestamp::Ts;

/// A snapshot handle for an active reader.
///
/// Constructed by `ReadViewRegistry::open()` (T4) with `read_ts` taken
/// from the timestamp oracle. The visibility rule:
///
/// - Committed entry `E` is visible iff `E.start_ts <= read_ts < E.stop_ts`.
/// - Pending entry is visible only to its own `txn_id`.
///
/// `poisoned` is set by T8 force-expiry before touching any owned pins;
/// `pin_ops_in_flight` lets the force-expiry path wait for concurrent
/// pin walks to complete before releasing pages.
#[derive(Debug)]
pub struct ReadView {
    /// Snapshot timestamp for visibility checks.
    pub read_ts: Ts,
    /// Transaction identifier — also serves as the txn_id used to resolve
    /// visibility of this reader's own pending entries when the reader
    /// doubles as a writer.
    pub txn_id: u64,
    /// Set by `force_expire` (T8). Any subsequent pin-walk observes this
    /// via an Acquire load at the pre-check and again post-increment of
    /// `pin_ops_in_flight`; if poisoned, it bails without walking pins.
    pub poisoned: AtomicBool,
    /// Active pin-walk count. Incremented on entry to
    /// `pin_overflows`-style code and decremented on exit. `force_expire`
    /// spins until this reaches 0 before the caller is allowed to proceed
    /// with page-release.
    pub pin_ops_in_flight: AtomicU32,
}

impl ReadView {
    /// Construct a fresh, live `ReadView`.
    pub fn new(read_ts: Ts, txn_id: u64) -> Self {
        Self {
            read_ts,
            txn_id,
            poisoned: AtomicBool::new(false),
            pin_ops_in_flight: AtomicU32::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn new_read_view_is_live() {
        let rv = ReadView::new(
            Ts {
                physical_ms: 100,
                logical: 1,
            },
            42,
        );
        assert_eq!(rv.read_ts.physical_ms, 100);
        assert_eq!(rv.read_ts.logical, 1);
        assert_eq!(rv.txn_id, 42);
        assert!(!rv.poisoned.load(Ordering::Acquire));
        assert_eq!(rv.pin_ops_in_flight.load(Ordering::Acquire), 0);
    }

    #[test]
    fn poisoned_flag_transitions() {
        let rv = ReadView::new(Ts::PENDING, 0);
        assert!(!rv.poisoned.load(Ordering::Acquire));
        rv.poisoned.store(true, Ordering::Release);
        assert!(rv.poisoned.load(Ordering::Acquire));
    }

    #[test]
    fn pin_ops_counter_tracks_in_flight() {
        let rv = ReadView::new(Ts::PENDING, 0);
        rv.pin_ops_in_flight.fetch_add(1, Ordering::Release);
        rv.pin_ops_in_flight.fetch_add(1, Ordering::Release);
        assert_eq!(rv.pin_ops_in_flight.load(Ordering::Acquire), 2);
        rv.pin_ops_in_flight.fetch_sub(1, Ordering::Release);
        assert_eq!(rv.pin_ops_in_flight.load(Ordering::Acquire), 1);
    }
}
