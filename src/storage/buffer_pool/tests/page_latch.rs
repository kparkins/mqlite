#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test target uses assertion-style panics and setup unwraps"
)]

use super::*;

#[cfg(not(loom))]
use std::sync::atomic::{AtomicU32 as StdAtomicU32, Ordering as StdOrdering};
#[cfg(not(loom))]
use std::sync::{Arc, Barrier};
#[cfg(not(loom))]
use std::thread;
#[cfg(not(loom))]
use std::time::Duration;

/// Shared mode permits multiple concurrent readers (§10.18 row 1).
///
/// Both threads acquire shared, increment a live-reader counter, and
/// the test asserts the counter reached 2 while both shared guards
/// were live. A flat `let s1 = ...; let s2 = ...;` cannot prove this
/// because parking_lot does not document recursive same-thread
/// reads — the cross-thread observation is the actual contract.
#[cfg(not(loom))]
#[test]
fn test_acquire_shared_multiple_readers_succeed() {
    const PEAK_READERS_REQUIRED: u32 = 2;

    let latch = Arc::new(PageLatch::new());
    let live_readers = Arc::new(StdAtomicU32::new(0));
    let peak_seen = Arc::new(StdAtomicU32::new(0));
    let barrier = Arc::new(Barrier::new(2));

    let mut handles = Vec::new();
    for _ in 0..2 {
        let l = latch.clone();
        let live = live_readers.clone();
        let peak = peak_seen.clone();
        let b = barrier.clone();
        handles.push(thread::spawn(move || {
            let _shared = l.lock_shared();
            let now_live = live.fetch_add(1, StdOrdering::AcqRel) + 1;
            peak.fetch_max(now_live, StdOrdering::AcqRel);
            // Wait until both readers are inside the shared region so
            // peak observes the simultaneous hold.
            b.wait();
            live.fetch_sub(1, StdOrdering::AcqRel);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    assert!(
        peak_seen.load(StdOrdering::Acquire) >= PEAK_READERS_REQUIRED,
        "two readers must be able to hold PageLatch::lock_shared concurrently",
    );
}

/// Exclusive mode excludes all shared holders for the lifetime of the
/// exclusive guard (§10.18 row 2).
///
/// One thread takes exclusive and sets a flag; a second thread blocks
/// on `lock_shared` and asserts the flag has been cleared by the time
/// it acquires.
#[cfg(not(loom))]
#[test]
fn test_acquire_exclusive_excludes_shared() {
    let latch = Arc::new(PageLatch::new());
    let writer_inside = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Pre-acquire the writer's exclusive guard on the main thread so
    // the spawned reader observes a contended latch on entry.
    let exclusive = latch.lock_exclusive();
    writer_inside.store(true, StdOrdering::Release);

    let l = latch.clone();
    let inside = writer_inside.clone();
    let reader = thread::spawn(move || {
        let _shared = l.lock_shared();
        assert!(
            !inside.load(StdOrdering::Acquire),
            "shared acquire must wait until exclusive is released",
        );
    });

    // Hold the writer briefly to give the reader time to attempt
    // lock_shared and block.
    thread::sleep(Duration::from_millis(20));
    writer_inside.store(false, StdOrdering::Release);
    drop(exclusive);

    reader.join().unwrap();
}

/// A solo reader's `upgrade()` always succeeds; no contention means
/// the CAS wins on the first try.
#[cfg(not(loom))]
#[test]
fn test_upgrade_wins_when_alone() {
    let latch = PageLatch::new();
    let shared = latch.lock_shared();
    let _exclusive = shared
        .upgrade()
        .expect("solo upgrade must succeed when no contender exists");
}

/// Concurrent upgrades produce exactly one `Exclusive` and exactly one
/// `Err(WriteConflict { reason: UpgradeRace })` (§10.28).
///
/// Both threads acquire shared (multi-reader is OK), synchronize on a
/// barrier, then race the CAS. The loser must see `upgrade_intent ==
/// 1` and bail with `WriteConflictReason::UpgradeRace` before
/// touching the write guard; the winner drops its read guard first,
/// so the loser's read guard release on `upgrade(self)` consumption
/// is what unblocks the winner's `write()`.
#[cfg(not(loom))]
#[test]
fn test_concurrent_upgrade_one_loser_returns_upgrade_race() {
    const ATTEMPTS: usize = 64;

    for _ in 0..ATTEMPTS {
        let latch = Arc::new(PageLatch::new());
        let barrier = Arc::new(Barrier::new(2));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let l = latch.clone();
            let b = barrier.clone();
            handles.push(thread::spawn(move || -> Result<()> {
                let shared = l.lock_shared();
                b.wait();
                let _exclusive = shared.upgrade()?;
                Ok(())
            }));
        }

        let results: Vec<Result<()>> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let winners = results.iter().filter(|r| r.is_ok()).count() as u32;
        let losers = results
            .iter()
            .filter(|r| {
                matches!(
                    r,
                    Err(Error::WriteConflict {
                        reason: WriteConflictReason::UpgradeRace
                    })
                )
            })
            .count() as u32;
        assert_eq!(
            winners, 1,
            "exactly one upgrade winner per attempt (winners={winners}); double-success would mean upgrade_intent is not arbitrating",
        );
        assert_eq!(
            losers, 1,
            "the other thread must observe UpgradeRace per attempt (losers={losers})",
        );
    }
}

/// On the success path, `upgrade_intent` returns to `IDLE` before the
/// exclusive guard is handed back. A second upgrade after the first
/// is dropped therefore proceeds without seeing a stale PENDING.
#[cfg(not(loom))]
#[test]
fn test_upgrade_intent_clears_after_successful_upgrade() {
    let latch = PageLatch::new();
    {
        let shared = latch.lock_shared();
        let _exclusive = shared.upgrade().expect("solo upgrade succeeds");
        assert_eq!(
            latch.upgrade_intent.load(Ordering::Acquire),
            UPGRADE_INTENT_IDLE,
            "intent must be cleared while exclusive guard is still held",
        );
    }
    assert_eq!(
        latch.upgrade_intent.load(Ordering::Acquire),
        UPGRADE_INTENT_IDLE,
        "intent must remain idle after exclusive guard is released",
    );

    // A second upgrade must succeed — proving intent was cleared, not
    // permanently parked at PENDING.
    let shared = latch.lock_shared();
    let _exclusive = shared
        .upgrade()
        .expect("second upgrade must succeed because intent was cleared");
}

/// On the unwind path (winner panics between CAS and write-guard
/// return), `UpgradeUnwindGuard` clears `upgrade_intent` so the latch
/// is reusable. We exercise the guard directly under
/// `catch_unwind` because `parking_lot::RwLock::write` does not
/// itself panic in production; the contract being tested is the
/// armed-drop behavior of the unwind guard.
#[cfg(not(loom))]
#[test]
fn test_upgrade_intent_clears_after_unwind() {
    let latch = PageLatch::new();

    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        latch
            .upgrade_intent
            .store(UPGRADE_INTENT_PENDING, Ordering::Release);
        let _unwind = UpgradeUnwindGuard {
            intent: &latch.upgrade_intent,
            armed: true,
        };
        panic!("simulated mid-upgrade panic");
    }));

    assert!(
        panicked.is_err(),
        "the simulated panic must propagate out of catch_unwind",
    );
    assert_eq!(
        latch.upgrade_intent.load(Ordering::Acquire),
        UPGRADE_INTENT_IDLE,
        "armed UpgradeUnwindGuard must clear intent on panic",
    );

    // A subsequent upgrade still succeeds, proving the latch is not
    // permanently stuck in PENDING.
    let shared = latch.lock_shared();
    let _exclusive = shared
        .upgrade()
        .expect("post-unwind upgrade must succeed because intent cleared");
}

/// §10.13.9 — loom-shimmed two-thread upgrade race. Under
/// `cfg(loom)`, `PageLatchRwLock` aliases `loom::sync::RwLock`, so
/// the loom scheduler can permute the CAS / drop / acquire-write
/// interleavings. Every interleaving must produce exactly one
/// `Exclusive` and exactly one `UpgradeRace`; both succeeding is
/// forbidden by the contract and would fail this test on at least
/// one schedule.
///
/// To make the race actually concurrent under loom (instead of a
/// schedule where one thread fully completes its upgrade before the
/// other has acquired Shared), each thread acquires Shared then waits
/// on a `ready_count` barrier — both fetch_add and then spin until
/// both increments are visible. Only after the barrier is satisfied
/// does either call `upgrade()`. The barrier is built from a
/// loom-shimmed `AtomicU32` and `loom::thread::yield_now` so loom can
/// permute the synchronization itself.
#[cfg(loom)]
#[test]
fn loom_page_latch_shared_to_exclusive_upgrade() {
    const BOTH_THREADS_INSIDE_SHARED: u32 = 2;

    loom::model(|| {
        let latch = loom::sync::Arc::new(PageLatch::new());
        let ready_count = loom::sync::Arc::new(AtomicU32::new(0));

        let l1 = latch.clone();
        let r1_ready = ready_count.clone();
        let t1 = loom::thread::spawn(move || -> Result<()> {
            let shared = l1.lock_shared();
            r1_ready.fetch_add(1, Ordering::AcqRel);
            while r1_ready.load(Ordering::Acquire) < BOTH_THREADS_INSIDE_SHARED {
                loom::thread::yield_now();
            }
            let _exclusive = shared.upgrade()?;
            Ok(())
        });

        let l2 = latch.clone();
        let r2_ready = ready_count.clone();
        let t2 = loom::thread::spawn(move || -> Result<()> {
            let shared = l2.lock_shared();
            r2_ready.fetch_add(1, Ordering::AcqRel);
            while r2_ready.load(Ordering::Acquire) < BOTH_THREADS_INSIDE_SHARED {
                loom::thread::yield_now();
            }
            let _exclusive = shared.upgrade()?;
            Ok(())
        });

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        let winners = (r1.is_ok() as u32) + (r2.is_ok() as u32);
        let losers = matches!(
            &r1,
            Err(Error::WriteConflict {
                reason: WriteConflictReason::UpgradeRace
            })
        ) as u32
            + matches!(
                &r2,
                Err(Error::WriteConflict {
                    reason: WriteConflictReason::UpgradeRace
                })
            ) as u32;

        assert_eq!(
            winners, 1,
            "exactly one upgrade winner per interleaving (winners={winners})",
        );
        assert_eq!(
            losers, 1,
            "the other thread must observe UpgradeRace (losers={losers})",
        );
    });
}
