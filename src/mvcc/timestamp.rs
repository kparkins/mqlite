//! Hybrid Logical Clock (HLC) timestamp oracle.
//!
//! Commit timestamps are 12 bytes: an 8-byte physical millisecond reading
//! followed by a 4-byte logical counter. The oracle guarantees strictly
//! monotonic, unique timestamps across concurrent callers even when the
//! physical wall clock regresses (node-local single-wall single-oracle path).
//!
//! On-disk / on-wire serialization:
//!
//! * **Ts-LE** — 12 bytes: `physical_ms` (8 B LE) || `logical` (4 B LE).
//!   Used in `VersionEntry`, journal `ChainCommit.commit_ts`, and the
//!   file-header `last_checkpoint_ts`.
//! * **Ts-BE** — 12 bytes: `physical_ms` (8 B BE) || `logical` (4 B BE).
//!   Used ONLY in history-store B-tree keys so that lexicographic sort
//!   equals chronological order.
//!
//! A `cfg(loom)` shim around `std::sync::Mutex` lets loom harnesses
//! permute the oracle's critical section without touching production code.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// cfg(loom) synchronization shim
// ---------------------------------------------------------------------------

#[cfg(loom)]
use loom::sync::Mutex;

#[cfg(not(loom))]
use std::sync::Mutex;

// ---------------------------------------------------------------------------
// Ts
// ---------------------------------------------------------------------------

/// 12-byte Hybrid Logical Clock timestamp.
///
/// Lexicographic ordering by `(physical_ms, logical)` is total and matches
/// "happens-before" across all well-formed commits from this oracle.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(C)]
pub struct Ts {
    /// Wall-clock milliseconds since Unix epoch (or any monotone external source).
    pub physical_ms: u64,
    /// Per-millisecond logical counter. Wraps into `physical_ms + 1` at u32::MAX.
    pub logical: u32,
}

impl Ts {
    /// Logical "infinity" timestamp — greater than any real commit.
    pub const MAX: Ts = Ts {
        physical_ms: u64::MAX,
        logical: u32::MAX,
    };

    /// Smallest `Ts` strictly greater than `self`, or `None` on overflow.
    #[must_use]
    pub fn successor(self) -> Option<Ts> {
        if self.logical < u32::MAX {
            Some(Ts {
                logical: self.logical + 1,
                ..self
            })
        } else if self.physical_ms < u64::MAX {
            Some(Ts {
                physical_ms: self.physical_ms + 1,
                logical: 0,
            })
        } else {
            None
        }
    }

    /// Serialize to 12 bytes in little-endian order (Ts-LE).
    ///
    /// Layout: `physical_ms` (8 B LE) || `logical` (4 B LE).
    #[must_use]
    pub fn to_le_bytes(self) -> [u8; 12] {
        let mut out = [0u8; 12];
        out[0..8].copy_from_slice(&self.physical_ms.to_le_bytes());
        out[8..12].copy_from_slice(&self.logical.to_le_bytes());
        out
    }

    /// Inverse of [`Ts::to_le_bytes`].
    #[must_use]
    pub fn from_le_bytes(bytes: [u8; 12]) -> Ts {
        let mut p = [0u8; 8];
        p.copy_from_slice(&bytes[0..8]);
        let mut l = [0u8; 4];
        l.copy_from_slice(&bytes[8..12]);
        Ts {
            physical_ms: u64::from_le_bytes(p),
            logical: u32::from_le_bytes(l),
        }
    }

    /// Serialize to 12 bytes in big-endian order (Ts-BE).
    ///
    /// Layout: `physical_ms` (8 B BE) || `logical` (4 B BE). Bytewise
    /// unsigned comparison of the result matches lexicographic ordering of
    /// the underlying `Ts`, which is why the history store uses this form.
    #[must_use]
    pub fn to_be_bytes(self) -> [u8; 12] {
        let mut out = [0u8; 12];
        out[0..8].copy_from_slice(&self.physical_ms.to_be_bytes());
        out[8..12].copy_from_slice(&self.logical.to_be_bytes());
        out
    }

    /// Inverse of [`Ts::to_be_bytes`].
    #[must_use]
    pub fn from_be_bytes(bytes: [u8; 12]) -> Ts {
        let mut p = [0u8; 8];
        p.copy_from_slice(&bytes[0..8]);
        let mut l = [0u8; 4];
        l.copy_from_slice(&bytes[8..12]);
        Ts {
            physical_ms: u64::from_be_bytes(p),
            logical: u32::from_be_bytes(l),
        }
    }
}

// ---------------------------------------------------------------------------
// AtomicTs — seqlock-style atomic Ts cell (Phase 5 §10.19 C-1)
// ---------------------------------------------------------------------------

/// Lock-free atomic [`Ts`] cell.
///
/// Phase 5 §10.19 C-1 requires a "lock-free `published_frontier: AtomicTs`"
/// behind the `PublishSequencer`. `Ts` is 96 bits wide, so a native
/// `AtomicU96`/`AtomicU128` is unavailable on stable Rust. `AtomicTs` is a
/// seqlock that pairs the 96-bit value with a 64-bit version counter; the
/// writer increments the version twice (odd while writing, even when
/// done), and readers retry if the version changes mid-load.
///
/// Concurrent writers are not supported: the `PublishSequencer` only
/// stores into `published_frontier` while holding the sequencer mutex, so
/// the seqlock writer-side is single-producer by construction.
#[derive(Debug)]
pub(crate) struct AtomicTs {
    seq: AtomicU64,
    physical_ms: AtomicU64,
    logical: AtomicU32,
}

impl AtomicTs {
    /// Construct an `AtomicTs` initialized to `ts`.
    pub(crate) fn new(ts: Ts) -> Self {
        Self {
            seq: AtomicU64::new(0),
            physical_ms: AtomicU64::new(ts.physical_ms),
            logical: AtomicU32::new(ts.logical),
        }
    }

    /// Atomically store `ts`. Single-producer (seqlock writer side).
    ///
    /// `_order` is accepted for API compatibility with the
    /// `published_frontier.store(_, Ordering::Release)` pattern in §10.19;
    /// the seqlock implementation always uses the necessary memory
    /// orderings internally and ignores the parameter.
    pub(crate) fn store(&self, ts: Ts, _order: Ordering) {
        // Bump version into the odd/"writer in progress" state.
        let prev = self.seq.fetch_add(1, Ordering::AcqRel);
        debug_assert!(
            prev % 2 == 0,
            "AtomicTs::store called concurrently with another writer (seq must be even)"
        );
        self.physical_ms.store(ts.physical_ms, Ordering::Release);
        self.logical.store(ts.logical, Ordering::Release);
        // Bump version back to the even/"writer done" state.
        self.seq.fetch_add(1, Ordering::AcqRel);
    }

    /// Atomically load the current `Ts`. Lock-free reader path: retries
    /// on a concurrent in-progress writer.
    ///
    /// `_order` is accepted for API compatibility; the seqlock always
    /// uses Acquire on the version load.
    pub(crate) fn load(&self, _order: Ordering) -> Ts {
        loop {
            let s1 = self.seq.load(Ordering::Acquire);
            // Odd version means a writer is mid-store; spin and retry.
            if s1 % 2 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let physical_ms = self.physical_ms.load(Ordering::Acquire);
            let logical = self.logical.load(Ordering::Acquire);
            let s2 = self.seq.load(Ordering::Acquire);
            if s1 == s2 {
                return Ts {
                    physical_ms,
                    logical,
                };
            }
            std::hint::spin_loop();
        }
    }
}

// ---------------------------------------------------------------------------
// HlcState
// ---------------------------------------------------------------------------

/// Internal HLC state protected by the oracle's mutex.
///
/// Exposed publicly (read-only via `TimestampOracle::now`) so tests and
/// recovery code can observe the oracle without round-tripping through
/// `commit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HlcState {
    /// Last issued (or observed) physical millisecond.
    pub physical_ms: u64,
    /// Last issued logical counter at `physical_ms`.
    pub logical: u32,
}

// ---------------------------------------------------------------------------
// Wall clock abstraction
// ---------------------------------------------------------------------------

/// Source of "now" in milliseconds since the Unix epoch. Wired through the
/// oracle so tests can inject regressions without touching global state.
pub trait WallClock: Send + Sync + 'static {
    /// Return a monotone-ish millisecond reading.
    fn now_ms(&self) -> u64;
}

/// Default wall clock that reads `SystemTime::now()`.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }
}

impl<T: WallClock + ?Sized> WallClock for Box<T> {
    fn now_ms(&self) -> u64 {
        (**self).now_ms()
    }
}

// ---------------------------------------------------------------------------
// TimestampOracle
// ---------------------------------------------------------------------------

/// Per-node Hybrid Logical Clock oracle.
///
/// All commit/read timestamps for the MVCC subsystem flow through
/// [`TimestampOracle::commit`]. The oracle guarantees:
///
/// 1. **Uniqueness** — no two `commit()` calls return equal `Ts`.
/// 2. **Strict monotonicity** — subsequent `commit()` returns `> previous`.
/// 3. **Wall-clock tolerance** — regressions in the wall clock bump only
///    the logical counter; `Ts::physical_ms` never regresses.
pub struct TimestampOracle<C: WallClock = SystemWallClock> {
    hlc: Mutex<HlcState>,
    clock: C,
}

impl<C: WallClock> std::fmt::Debug for TimestampOracle<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimestampOracle").finish_non_exhaustive()
    }
}

impl TimestampOracle {
    /// Construct an oracle backed by the system wall clock.
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(SystemWallClock)
    }
}

impl<C: WallClock> TimestampOracle<C> {
    /// Construct an oracle backed by a caller-supplied clock (tests / replay).
    #[must_use]
    pub fn with_clock(clock: C) -> Self {
        Self {
            hlc: Mutex::new(HlcState::default()),
            clock,
        }
    }

    /// Issue a fresh strictly-monotonic `Ts`.
    ///
    /// Returns [`Error::TimestampExhausted`] if the logical counter is
    /// saturated for the current millisecond AND the wall clock fails to
    /// advance past it. In practice this is only reachable under extreme
    /// synthetic load or a pathologically stuck clock.
    pub fn commit(&self) -> Result<Ts> {
        let wall = self.clock.now_ms();
        #[allow(clippy::unwrap_used)]
        let mut st = self.hlc.lock().unwrap();

        if wall > st.physical_ms {
            st.physical_ms = wall;
            st.logical = 0;
        } else if st.logical == u32::MAX {
            return Err(Error::TimestampExhausted);
        } else {
            st.logical += 1;
        }

        Ok(Ts {
            physical_ms: st.physical_ms,
            logical: st.logical,
        })
    }

    /// Fold an externally-received `Ts` into the oracle.
    ///
    /// In the single-node default this is a no-op: we have no peers
    /// whose timestamps could outrun ours. Callers pass the received Ts
    /// anyway so the signature is stable when multi-node is wired in later.
    /// Ticks `mvcc.hlc.advance_events_total` per call.
    pub fn advance(&self, _received: Ts) -> Result<()> {
        crate::mvcc::metrics::record_hlc_advance();
        Ok(())
    }

    /// Peek at the most recent `(physical_ms, logical)` pair without issuing
    /// a new timestamp.
    #[must_use]
    pub fn now(&self) -> Ts {
        #[allow(clippy::unwrap_used)]
        let st = self.hlc.lock().unwrap();
        Ts {
            physical_ms: st.physical_ms,
            logical: st.logical,
        }
    }

    /// Floor the oracle at `min`.
    ///
    /// Any subsequent `commit()` or `now()` will return a Ts `>= min`.
    /// Used at recovery to restore the oracle to the last durable commit.
    pub fn set_min(&self, min: Ts) {
        #[allow(clippy::unwrap_used)]
        let mut st = self.hlc.lock().unwrap();
        let cur = Ts {
            physical_ms: st.physical_ms,
            logical: st.logical,
        };
        if min > cur {
            st.physical_ms = min.physical_ms;
            st.logical = min.logical;
        }
    }
}

impl Default for TimestampOracle<SystemWallClock> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(not(loom))]
#[path = "tests/timestamp.rs"]
mod tests;
