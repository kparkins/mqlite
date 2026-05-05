//! Phase 5 §10.28 — `PageLatch`, the page-local read/write latch primitive.
//!
//! This module owns the bare latch primitive only. The buffer-pool
//! integration (`Frame::latch`, `LatchedPinnedPage` pin-plus-latch RAII
//! handle, lock-order checks against partition mutexes, and eviction
//! coordination) lives in US-029 and US-015 — this file is intentionally
//! standalone so the upgrade-race contract can be tested in isolation
//! before any storage code references it.
//!
//! The latch wraps a `parking_lot::RwLock<()>` (or `loom::sync::RwLock<()>`
//! under `cfg(loom)`) plus a single `AtomicU32` (`upgrade_intent`) that
//! arbitrates concurrent shared-to-exclusive upgrades. The first reader to
//! CAS `upgrade_intent` from 0 → 1 wins; subsequent readers that observe
//! `upgrade_intent == 1` return `Error::WriteConflict { reason:
//! WriteConflictReason::UpgradeRace }` immediately so the caller can retry
//! against a fresh `ReadView` (§10.3.1 row B, §10.28).
//!
//! The winning upgrade releases the read guard, acquires the write guard,
//! clears `upgrade_intent` exactly once, and hands back a
//! `PageLatchExclusive`. An unwind guard restores `upgrade_intent` to 0 if
//! the winner panics between the CAS and the write-guard return so the
//! latch is never permanently locked into the upgrade-pending state.

#[cfg(loom)]
use loom::sync::atomic::{AtomicU32, Ordering};
#[cfg(not(loom))]
use std::sync::atomic::{AtomicU32, Ordering};

use crate::error::{Error, Result, WriteConflictReason};

/// `PageLatchRwLock` is the read/write lock backing `PageLatch`. Production
/// code uses `parking_lot::RwLock` for the unfair / fast path; loom
/// permutation harnesses substitute `loom::sync::RwLock` so the scheduler
/// can interleave critical sections (§10.13.9 loom-shim requirement).
#[cfg(loom)]
pub(crate) type PageLatchRwLock<T> = loom::sync::RwLock<T>;
/// `PageLatchRwLock` is the read/write lock backing `PageLatch`. Production
/// code uses `parking_lot::RwLock` for the unfair / fast path; loom
/// permutation harnesses substitute `loom::sync::RwLock` so the scheduler
/// can interleave critical sections (§10.13.9 loom-shim requirement).
#[cfg(not(loom))]
pub(crate) type PageLatchRwLock<T> = parking_lot::RwLock<T>;

#[cfg(loom)]
type PageLatchReadGuard<'a, T> = loom::sync::RwLockReadGuard<'a, T>;
#[cfg(loom)]
type PageLatchWriteGuard<'a, T> = loom::sync::RwLockWriteGuard<'a, T>;
#[cfg(not(loom))]
type PageLatchReadGuard<'a, T> = parking_lot::RwLockReadGuard<'a, T>;
#[cfg(not(loom))]
type PageLatchWriteGuard<'a, T> = parking_lot::RwLockWriteGuard<'a, T>;

const UPGRADE_INTENT_IDLE: u32 = 0;
const UPGRADE_INTENT_PENDING: u32 = 1;
#[cfg(loom)]
const EXCLUSIVE_HELD_FALSE: u32 = 0;
#[cfg(loom)]
const EXCLUSIVE_HELD_TRUE: u32 = 1;

/// Mode in which a `PageLatch` is currently held. Phase 5 §10.18 names
/// this enum; US-029 stores it inside `LatchedPinnedPage` to describe how
/// the wrapped frame is locked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatchMode {
    /// The latch is held shared (one or more readers).
    Shared,
    /// The latch is held exclusive (one writer; no other holders).
    Exclusive,
}

/// Page-local read/write latch primitive (§10.28).
///
/// `PageLatch` is the smallest unit of in-memory write serialization on a
/// single buffer-pool frame. It is intentionally narrow — see
/// `LatchedPinnedPage` (US-029) for the pin-plus-latch RAII handle that
/// production CRUD paths take.
pub(crate) struct PageLatch {
    lock: PageLatchRwLock<()>,
    upgrade_intent: AtomicU32,
    #[cfg(loom)]
    exclusive_held: AtomicU32,
}

impl PageLatch {
    /// Construct a fresh latch with no readers and no upgrade pending.
    pub(crate) fn new() -> Self {
        Self {
            lock: PageLatchRwLock::new(()),
            upgrade_intent: AtomicU32::new(UPGRADE_INTENT_IDLE),
            #[cfg(loom)]
            exclusive_held: AtomicU32::new(EXCLUSIVE_HELD_FALSE),
        }
    }

    /// Acquire the latch in shared mode. Blocks if a writer holds the
    /// latch exclusive; multiple shared holders are permitted.
    pub(crate) fn lock_shared(&self) -> PageLatchShared<'_> {
        PageLatchShared {
            latch: self,
            _guard: lock_read(&self.lock),
        }
    }

    /// Acquire the latch in exclusive mode. Blocks until all shared and
    /// exclusive holders have released.
    pub(crate) fn lock_exclusive(&self) -> PageLatchExclusive<'_> {
        #[cfg(loom)]
        self.exclusive_held
            .store(EXCLUSIVE_HELD_TRUE, Ordering::Release);
        PageLatchExclusive::new(self, lock_write(&self.lock))
    }

    /// Return whether an exclusive holder currently owns this latch.
    ///
    /// This is a guard-less observation for eviction. It must not acquire
    /// the underlying `RwLock`, because eviction calls it while holding a
    /// buffer-pool partition mutex.
    pub(crate) fn is_exclusively_held(&self) -> bool {
        #[cfg(loom)]
        {
            return self.exclusive_held.load(Ordering::Acquire) == EXCLUSIVE_HELD_TRUE;
        }
        #[cfg(not(loom))]
        {
            self.lock.is_locked_exclusive()
        }
    }
}

#[cfg(loom)]
fn lock_read<'a>(lock: &'a PageLatchRwLock<()>) -> PageLatchReadGuard<'a, ()> {
    lock.read().expect("page latch RwLock not poisoned")
}
#[cfg(not(loom))]
fn lock_read<'a>(lock: &'a PageLatchRwLock<()>) -> PageLatchReadGuard<'a, ()> {
    lock.read()
}

#[cfg(loom)]
fn lock_write<'a>(lock: &'a PageLatchRwLock<()>) -> PageLatchWriteGuard<'a, ()> {
    lock.write().expect("page latch RwLock not poisoned")
}
#[cfg(not(loom))]
fn lock_write<'a>(lock: &'a PageLatchRwLock<()>) -> PageLatchWriteGuard<'a, ()> {
    lock.write()
}

/// RAII guard for a shared `PageLatch` hold. Drops release the underlying
/// read guard; the typestate `upgrade(self) -> Result<PageLatchExclusive>`
/// converts the hold into an exclusive one with a single concurrent winner.
pub(crate) struct PageLatchShared<'a> {
    latch: &'a PageLatch,
    _guard: PageLatchReadGuard<'a, ()>,
}

impl<'a> PageLatchShared<'a> {
    /// Upgrade a shared hold to exclusive (§10.28).
    ///
    /// Exactly one concurrent caller wins via a CAS on `upgrade_intent`
    /// from 0 → 1; the winner drops its read guard, acquires the write
    /// guard, clears `upgrade_intent`, and returns
    /// `Ok(PageLatchExclusive)`. Losers observing `upgrade_intent == 1`
    /// return `Err(Error::WriteConflict { reason:
    /// WriteConflictReason::UpgradeRace })` and the caller retries
    /// (§10.3.1 row B).
    ///
    /// # Errors
    ///
    /// Returns `Error::WriteConflict { reason:
    /// WriteConflictReason::UpgradeRace }` when another shared holder
    /// already claimed the upgrade slot.
    pub(crate) fn upgrade(self) -> Result<PageLatchExclusive<'a>> {
        let latch = self.latch;
        if latch
            .upgrade_intent
            .compare_exchange(
                UPGRADE_INTENT_IDLE,
                UPGRADE_INTENT_PENDING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(Error::WriteConflict {
                reason: WriteConflictReason::UpgradeRace,
            });
        }
        // Winner: release the read guard so write() can proceed once the
        // last contending shared holder drops, then acquire the write
        // guard. The unwind guard clears `upgrade_intent` if `write()`
        // panics (or any future code in this branch panics) before the
        // exclusive guard is returned to the caller.
        drop(self._guard);
        let mut unwind = UpgradeUnwindGuard {
            intent: &latch.upgrade_intent,
            armed: true,
        };
        #[cfg(loom)]
        latch
            .exclusive_held
            .store(EXCLUSIVE_HELD_TRUE, Ordering::Release);
        let write_guard = lock_write(&latch.lock);
        latch
            .upgrade_intent
            .store(UPGRADE_INTENT_IDLE, Ordering::Release);
        unwind.armed = false;
        Ok(PageLatchExclusive::new(latch, write_guard))
    }
}

/// Restores `upgrade_intent` to `IDLE` if the upgrade winner unwinds
/// before the exclusive guard is handed back to the caller. Disarmed on
/// the success path so the post-success `store(IDLE)` clears the slot
/// exactly once (AC: "clears `upgrade_intent` exactly once").
struct UpgradeUnwindGuard<'a> {
    intent: &'a AtomicU32,
    armed: bool,
}

impl Drop for UpgradeUnwindGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.intent.store(UPGRADE_INTENT_IDLE, Ordering::Release);
        }
    }
}

/// RAII guard for an exclusive `PageLatch` hold. Drops release the
/// underlying write guard.
pub(crate) struct PageLatchExclusive<'a> {
    /// Held for symmetry with `PageLatchShared` and to keep the latch
    /// reachable from future getters (e.g. observability / debug). The
    /// underlying write guard is what enforces exclusion.
    #[allow(dead_code)]
    latch: &'a PageLatch,
    guard: Option<PageLatchWriteGuard<'a, ()>>,
}

impl<'a> PageLatchExclusive<'a> {
    fn new(latch: &'a PageLatch, guard: PageLatchWriteGuard<'a, ()>) -> Self {
        Self {
            latch,
            guard: Some(guard),
        }
    }
}

impl Drop for PageLatchExclusive<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        #[cfg(loom)]
        self.latch
            .exclusive_held
            .store(EXCLUSIVE_HELD_FALSE, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
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
}
