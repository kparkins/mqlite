//! Hybrid Logical Clock (HLC) timestamp oracle.
//!
//! Commit timestamps are 12 bytes: an 8-byte physical millisecond reading
//! followed by a 4-byte logical counter. The oracle guarantees strictly
//! monotonic, unique timestamps across concurrent callers even when the
//! physical wall clock regresses (node-local single-wall single-oracle path,
//! aka T12.A from the design doc).
//!
//! Format Lock Appendix §A.3 pins the on-disk / on-wire serialization:
//!
//! * **Ts-LE** — 12 bytes: `physical_ms` (8 B LE) || `logical` (4 B LE).
//!   Used in `VersionEntry`, journal `ChainCommit.commit_ts`, and the
//!   file-header `last_checkpoint_ts`.
//! * **Ts-BE** — 12 bytes: `physical_ms` (8 B BE) || `logical` (4 B BE).
//!   Used ONLY in history-store B-tree keys so that lexicographic sort
//!   equals chronological order.
//!
//! A `cfg(loom)` shim around `std::sync::Mutex` lets T4+ loom harnesses
//! permute the oracle's critical section without touching production code.

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
    /// Sentinel "not yet committed" timestamp stored inline on pending version entries.
    pub const PENDING: Ts = Ts {
        physical_ms: 0,
        logical: 0,
    };

    /// Logical "infinity" timestamp — greater than any real commit.
    pub const MAX: Ts = Ts {
        physical_ms: u64::MAX,
        logical: u32::MAX,
    };

    /// Smallest `Ts` strictly greater than `self`, or `None` on overflow.
    pub fn successor(self) -> Option<Ts> {
        if self.logical < u32::MAX {
            Some(Ts {
                physical_ms: self.physical_ms,
                logical: self.logical + 1,
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
    pub fn to_le_bytes(self) -> [u8; 12] {
        let mut out = [0u8; 12];
        out[0..8].copy_from_slice(&self.physical_ms.to_le_bytes());
        out[8..12].copy_from_slice(&self.logical.to_le_bytes());
        out
    }

    /// Inverse of [`Ts::to_le_bytes`].
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
    pub fn to_be_bytes(self) -> [u8; 12] {
        let mut out = [0u8; 12];
        out[0..8].copy_from_slice(&self.physical_ms.to_be_bytes());
        out[8..12].copy_from_slice(&self.logical.to_be_bytes());
        out
    }

    /// Inverse of [`Ts::to_be_bytes`].
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
// HlcState
// ---------------------------------------------------------------------------

/// Internal HLC state protected by the oracle's mutex.
///
/// Exposed publicly (read-only via `TimestampOracle::now`) so tests and
/// recovery code can observe the oracle without round-tripping through
/// `commit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    pub fn new() -> Self {
        Self::with_clock(SystemWallClock)
    }
}

impl<C: WallClock> TimestampOracle<C> {
    /// Construct an oracle backed by a caller-supplied clock (tests / replay).
    pub fn with_clock(clock: C) -> Self {
        Self {
            hlc: Mutex::new(HlcState {
                physical_ms: 0,
                logical: 0,
            }),
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
    /// In the single-node default (T12.A) this is a no-op: we have no peers
    /// whose timestamps could outrun ours. Callers pass the received Ts
    /// anyway so the signature is stable when multi-node is wired in later.
    /// Ticks `mvcc.hlc.advance_events_total` per call.
    pub fn advance(&self, _received: Ts) -> Result<()> {
        crate::mvcc::metrics::record_hlc_advance();
        Ok(())
    }

    /// Peek at the most recent `(physical_ms, logical)` pair without issuing
    /// a new timestamp.
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
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // Test wall clock that returns successive values from a fixed script.
    // Once the script is exhausted it clamps at the last value.
    struct ScriptedClock {
        script: Vec<u64>,
        idx: AtomicUsize,
    }

    impl ScriptedClock {
        fn new(script: Vec<u64>) -> Self {
            Self {
                script,
                idx: AtomicUsize::new(0),
            }
        }
    }

    impl WallClock for ScriptedClock {
        fn now_ms(&self) -> u64 {
            let i = self.idx.fetch_add(1, Ordering::Relaxed);
            if i < self.script.len() {
                self.script[i]
            } else {
                *self.script.last().unwrap_or(&0)
            }
        }
    }

    // Wall clock stuck at a constant reading — useful for saturation tests.
    struct FixedClock(u64);
    impl WallClock for FixedClock {
        fn now_ms(&self) -> u64 {
            self.0
        }
    }

    // -----------------------------------------------------------------------
    // Ts byte layout
    // -----------------------------------------------------------------------

    #[test]
    fn ts_to_le_bytes_exact_layout() {
        let t = Ts {
            physical_ms: 0x0123_4567_89AB_CDEF,
            logical: 0x1122_3344,
        };
        assert_eq!(
            t.to_le_bytes(),
            [0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01, 0x44, 0x33, 0x22, 0x11],
        );
    }

    #[test]
    fn ts_to_be_bytes_exact_layout() {
        let t = Ts {
            physical_ms: 0x0123_4567_89AB_CDEF,
            logical: 0x1122_3344,
        };
        assert_eq!(
            t.to_be_bytes(),
            [0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x11, 0x22, 0x33, 0x44],
        );
    }

    #[test]
    fn ts_le_round_trip_identity() {
        let samples = [
            Ts::PENDING,
            Ts::MAX,
            Ts {
                physical_ms: 42,
                logical: 7,
            },
            Ts {
                physical_ms: 0xFFFF_FFFF_FFFF_FFFF,
                logical: 0,
            },
            Ts {
                physical_ms: 0,
                logical: u32::MAX,
            },
        ];
        for t in samples {
            assert_eq!(Ts::from_le_bytes(t.to_le_bytes()), t);
        }
    }

    #[test]
    fn ts_be_round_trip_identity() {
        let samples = [
            Ts::PENDING,
            Ts::MAX,
            Ts {
                physical_ms: 42,
                logical: 7,
            },
        ];
        for t in samples {
            assert_eq!(Ts::from_be_bytes(t.to_be_bytes()), t);
        }
    }

    #[test]
    fn ts_be_bytes_are_bytewise_comparable() {
        let pairs = [
            (
                Ts {
                    physical_ms: 10,
                    logical: 0,
                },
                Ts {
                    physical_ms: 10,
                    logical: 1,
                },
            ),
            (
                Ts {
                    physical_ms: 10,
                    logical: u32::MAX,
                },
                Ts {
                    physical_ms: 11,
                    logical: 0,
                },
            ),
            (
                Ts {
                    physical_ms: 0,
                    logical: 0,
                },
                Ts {
                    physical_ms: u64::MAX,
                    logical: 0,
                },
            ),
        ];
        for (a, b) in pairs {
            assert!(a < b);
            assert!(a.to_be_bytes() < b.to_be_bytes());
        }
    }

    #[test]
    fn ts_successor_wraps_logical_into_physical() {
        let t = Ts {
            physical_ms: 5,
            logical: u32::MAX,
        };
        assert_eq!(
            t.successor(),
            Some(Ts {
                physical_ms: 6,
                logical: 0
            })
        );
        assert!(Ts::MAX.successor().is_none());
    }

    // Ts serialized form is 12 bytes; in-memory size with `#[repr(C)]` is 16
    // due to u32 tail padding and is asserted structurally by the LE/BE
    // byte-layout tests above.

    // -----------------------------------------------------------------------
    // TimestampOracle behavior
    // -----------------------------------------------------------------------

    #[test]
    fn commit_strictly_monotonic_under_regressing_wall_clock_s7() {
        // Wall clock regresses at step 2 (100 → 50).
        let oracle = TimestampOracle::with_clock(Box::new(ScriptedClock::new(vec![
            100, 50, 100, 150,
        ])));

        let mut prev = None;
        for _ in 0..4 {
            let t = oracle.commit().unwrap();
            if let Some(p) = prev {
                assert!(t > p, "expected strictly monotonic, got {p:?} -> {t:?}");
            }
            prev = Some(t);
        }
    }

    #[test]
    fn commit_saturated_logical_returns_timestamp_exhausted_s8() {
        // Clock stuck at 100; state primed to u32::MAX on the same ms.
        let oracle = TimestampOracle::with_clock(Box::new(FixedClock(100)));
        {
            #[allow(clippy::unwrap_used)]
            let mut st = oracle.hlc.lock().unwrap();
            st.physical_ms = 100;
            st.logical = u32::MAX;
        }
        let err = oracle.commit().unwrap_err();
        assert!(matches!(err, Error::TimestampExhausted), "got {err:?}");
    }

    #[test]
    fn advance_is_noop_on_single_node() {
        let oracle = TimestampOracle::with_clock(Box::new(FixedClock(100)));
        let before = oracle.now();
        oracle
            .advance(Ts {
                physical_ms: 9_999,
                logical: 9_999,
            })
            .unwrap();
        assert_eq!(oracle.now(), before);
    }

    #[test]
    fn set_min_lifts_now() {
        let oracle = TimestampOracle::with_clock(Box::new(FixedClock(200)));
        // Prime at {200, 5}.
        {
            #[allow(clippy::unwrap_used)]
            let mut st = oracle.hlc.lock().unwrap();
            st.physical_ms = 200;
            st.logical = 5;
        }
        oracle.set_min(Ts {
            physical_ms: 300,
            logical: 0,
        });
        assert_eq!(
            oracle.now(),
            Ts {
                physical_ms: 300,
                logical: 0
            }
        );
    }

    #[test]
    fn set_min_is_a_floor_not_a_reset() {
        let oracle = TimestampOracle::with_clock(Box::new(FixedClock(500)));
        {
            #[allow(clippy::unwrap_used)]
            let mut st = oracle.hlc.lock().unwrap();
            st.physical_ms = 500;
            st.logical = 10;
        }
        oracle.set_min(Ts {
            physical_ms: 100,
            logical: 0,
        });
        assert_eq!(
            oracle.now(),
            Ts {
                physical_ms: 500,
                logical: 10
            }
        );
    }

    #[test]
    fn commit_8_threads_100k_unique_and_strictly_monotonic() {
        // 8 threads × 100k commits each, all sharing one oracle.
        // Use a stuck wall clock to maximize contention on logical.
        let oracle = Arc::new(TimestampOracle::with_clock(Box::new(FixedClock(1_000))));

        const THREADS: usize = 8;
        const PER_THREAD: usize = 100_000;

        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let oracle = oracle.clone();
                std::thread::spawn(move || {
                    let mut out = Vec::with_capacity(PER_THREAD);
                    for _ in 0..PER_THREAD {
                        out.push(oracle.commit().expect("commit"));
                    }
                    out
                })
            })
            .collect();

        let mut all: Vec<Ts> = handles
            .into_iter()
            .flat_map(|h| h.join().expect("join"))
            .collect();

        assert_eq!(all.len(), THREADS * PER_THREAD);
        all.sort();
        // Strict monotonicity ⇒ adjacent pairs are strictly increasing ⇒ uniqueness.
        for w in all.windows(2) {
            assert!(w[0] < w[1], "duplicate or regression at {:?}", w);
        }
    }
}
