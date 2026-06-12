//! T4 acceptance bullet 4 — `ReadViewRegistry` thread-safety stress test.
//!
//! Spawns N threads that collectively open and drop 1000 `ReadView`s
//! against a shared `ReadViewRegistry`. Must:
//! - Complete within 10 s (deadlock gate).
//! - Leave the registry empty (every `open` matched by a Drop that
//!   unregisters).
//! - Observe `oldest_required_ts()` monotonically bounded above by any
//!   live view's `read_ts` at any sample point (best-effort — primary
//!   invariant is the post-join empty check).
//!
//! Run with: `cargo test --test registry_stress`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use mqlite::mvcc::{ReadView, ReadViewRegistry, Ts};

const TOTAL_VIEWS: usize = 1000;
const NUM_THREADS: usize = 10;
const DEADLOCK_GATE: Duration = Duration::from_secs(10);

#[test]
fn registry_stress_1000_concurrent_open_drop() {
    assert_eq!(
        TOTAL_VIEWS % NUM_THREADS,
        0,
        "test setup: TOTAL_VIEWS must divide evenly among threads"
    );
    let per_thread = TOTAL_VIEWS / NUM_THREADS;

    let registry = ReadViewRegistry::new();
    let barrier = Arc::new(Barrier::new(NUM_THREADS));
    let start = Instant::now();

    let mut handles = Vec::with_capacity(NUM_THREADS);
    for t in 0..NUM_THREADS {
        let reg = registry.clone();
        let bar = barrier.clone();
        handles.push(thread::spawn(move || {
            // Align thread start so contention actually happens.
            bar.wait();
            for i in 0..per_thread {
                // Unique txn_id across all (thread, iter) pairs.
                let txn_id = (t * per_thread + i + 1) as u64;
                let read_ts = Ts {
                    physical_ms: 1_000 + (i as u64),
                    logical: t as u32,
                };
                let view = ReadView::open_frontier_pinned_for_tests(reg.clone(), read_ts, txn_id);

                // Sanity: the registry horizon must be <= this view's ts.
                let horizon = reg.oldest_required_ts();
                assert!(
                    horizon <= read_ts,
                    "horizon {:?} exceeded live view ts {:?}",
                    horizon,
                    read_ts
                );

                // Drop releases the slot and unregisters.
                drop(view);
            }
        }));
    }

    for h in handles {
        h.join().expect("worker thread panicked");
    }

    let elapsed = start.elapsed();
    assert!(
        elapsed < DEADLOCK_GATE,
        "registry stress exceeded deadlock gate: {:?} >= {:?}",
        elapsed,
        DEADLOCK_GATE
    );

    assert!(
        registry.is_empty(),
        "registry not empty after all views dropped: {} live",
        registry.len()
    );
    assert_eq!(
        registry.oldest_required_ts(),
        Ts::MAX,
        "empty registry must report Ts::MAX"
    );
}
