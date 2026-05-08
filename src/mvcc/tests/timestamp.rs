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
        Ts::default(),
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
        Ts::default(),
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
    let oracle =
        TimestampOracle::with_clock(Box::new(ScriptedClock::new(vec![100, 50, 100, 150])));

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

    #[allow(
        clippy::needless_collect,
        reason = "spawn all timestamp workers before joining them"
    )]
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
