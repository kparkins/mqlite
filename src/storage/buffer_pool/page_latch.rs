//! Phase 5 §10.28 — `PageLatch`, the page-local read/write latch primitive.
//!
//! This module owns the bare latch primitive only. The buffer-pool
//! integration (`Frame::latch`, `LatchedPinnedPage` pin-plus-latch RAII
//! handle, lock-order checks against partition mutexes, and eviction
//! coordination) lives in US-029 and US-015. This file keeps the latch
//! primitive narrow so the upgrade-race contract can be tested in
//! isolation.
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
    #[cfg(loom)]
    latch: &'a PageLatch,
    guard: Option<PageLatchWriteGuard<'a, ()>>,
}

impl<'a> PageLatchExclusive<'a> {
    fn new(latch: &'a PageLatch, guard: PageLatchWriteGuard<'a, ()>) -> Self {
        #[cfg(not(loom))]
        let _ = latch;
        Self {
            #[cfg(loom)]
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
#[path = "tests/page_latch.rs"]
mod tests;
