//! S13 force-expiry ↔ `ChainSnapshot::new` race — loom harness.
//!
//! Background (plan §T3.75 acceptance bullet 7):
//!
//! *Given* force-expiry and concurrent `ChainSnapshot::new` (loom
//! interleaving), *when* all interleavings execute, *then* final refcount
//! equals initial refcount for every overflow page (no leak, no
//! double-decref).
//!
//! Run with:
//!
//! ```bash
//! RUSTFLAGS="--cfg loom" cargo test --release --features loom-tests \
//!     --test force_expiry_pin_race
//! ```
//!
//! Without `--cfg loom` the `loom` crate is a thin shim around `std::sync`
//! and its `model` does not permute interleavings; this file is additionally
//! gated on the `loom-tests` Cargo feature so ordinary `cargo test` does
//! not attempt to build it.
//!
//! ## Model fidelity
//!
//! This is a minimal fixture that exercises the same atomics `ChainSnapshot::new`
//! relies on — `ReadView::poisoned` (AtomicBool) and
//! `ReadView::pin_ops_in_flight` (AtomicU32) — alongside a CAS-loop refcount
//! that mirrors `OverflowRef::Clone` / `OverflowRef::Drop`. The real
//! production-code invariants rely on the same memory orderings this test
//! exercises: Release store on poison → Acquire load at the re-check,
//! Release `fetch_add` before clones → Release `fetch_sub` before the
//! re-check load.

#![cfg(feature = "loom-tests")]

#[cfg(loom)]
mod model {
    use loom::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use loom::sync::Arc;
    use loom::thread;

    /// Minimal saturating CAS-loop incref — same shape as the production
    /// `AllocatorHandle::incref_overflow`.
    fn incref(count: &AtomicU32) -> Result<u32, ()> {
        let mut cur = count.load(Ordering::Acquire);
        loop {
            if cur == u32::MAX {
                return Err(());
            }
            match count.compare_exchange_weak(cur, cur + 1, Ordering::Release, Ordering::Acquire) {
                Ok(_) => return Ok(cur + 1),
                Err(observed) => cur = observed,
            }
        }
    }

    /// Atomic decref — mirrors `AllocatorHandle::decref_overflow`.
    fn decref(count: &AtomicU32) -> u32 {
        let prev = count.fetch_sub(1, Ordering::Release);
        debug_assert!(prev > 0, "decref on already-zero refcount");
        prev - 1
    }

    /// Simulated `ChainSnapshot::new`. Performs the exact atomic protocol
    /// from `src/mvcc/read_view.rs`: pre-check poison, `fetch_add`, incref
    /// each entry, re-check poison, `fetch_sub`, decref-all on poisoned.
    ///
    /// Returns `true` if the snapshot ended up holding a pinned entry,
    /// `false` if it bailed (pre-check or post-bump poisoned).
    fn snapshot_new(poisoned: &AtomicBool, pin_ops: &AtomicU32, refcount: &AtomicU32) -> bool {
        // Pre-check.
        if poisoned.load(Ordering::Acquire) {
            return false;
        }
        pin_ops.fetch_add(1, Ordering::Release);

        // Deep-clone: for the 1-entry fixture, one incref.
        incref(refcount).expect("refcount must not saturate in fixture");

        // Re-check.
        let poisoned_after = poisoned.load(Ordering::Acquire);
        pin_ops.fetch_sub(1, Ordering::Release);
        if poisoned_after {
            // RAII drop: the cloned entry's Drop decrefs.
            decref(refcount);
            return false;
        }
        true
    }

    /// Simulated `force_expire`. Flips `poisoned`, then spins until
    /// `pin_ops_in_flight` reaches 0. In this fixture we do not actually
    /// walk any pins — the invariant we check is that after both threads
    /// complete, the refcount is either `initial` (snapshot bailed) or
    /// `initial + 1` (snapshot alive and still holds its pin).
    fn force_expire(poisoned: &AtomicBool, pin_ops: &AtomicU32) {
        poisoned.store(true, Ordering::Release);
        // Spin-wait for mid-flight pin-walks.
        while pin_ops.load(Ordering::Acquire) != 0 {
            loom::thread::yield_now();
        }
    }

    /// T3.75 acceptance bullet 7 — full loom permutation of
    /// `ChainSnapshot::new` racing `force_expire`.
    ///
    /// Starting refcount = 1 (the source chain's live pin). After both
    /// threads join:
    ///
    /// - If snapshot won the pre-check *before* poison flipped, and was
    ///   able to return an alive snapshot, final refcount = 2. We then
    ///   simulate the snapshot drop (decref) to return to 1.
    /// - If snapshot saw poisoned (either pre- or post-bump), it bailed
    ///   without net refcount change. Final refcount = 1.
    ///
    /// In *every* interleaving the net refcount delta is zero once the
    /// snapshot (if alive) is dropped.
    #[test]
    fn chain_snapshot_new_vs_force_expire_no_leak() {
        loom::model(|| {
            let poisoned = Arc::new(AtomicBool::new(false));
            let pin_ops = Arc::new(AtomicU32::new(0));
            let refcount = Arc::new(AtomicU32::new(1)); // source-chain pin

            let p_snap = poisoned.clone();
            let o_snap = pin_ops.clone();
            let r_snap = refcount.clone();
            let snap_thread = thread::spawn(move || snapshot_new(&p_snap, &o_snap, &r_snap));

            let p_fe = poisoned.clone();
            let o_fe = pin_ops.clone();
            let fe_thread = thread::spawn(move || force_expire(&p_fe, &o_fe));

            let alive = snap_thread.join().unwrap();
            fe_thread.join().unwrap();

            let after_race = refcount.load(Ordering::Acquire);
            if alive {
                assert_eq!(
                    after_race, 2,
                    "alive snapshot must hold exactly one pin above baseline"
                );
                // Now simulate the snapshot Drop that the real reader path
                // runs when it releases the snapshot.
                decref(&refcount);
            } else {
                assert_eq!(
                    after_race, 1,
                    "bailed snapshot must leave refcount at baseline (no leak, no double-decref)"
                );
            }

            assert_eq!(
                refcount.load(Ordering::Acquire),
                1,
                "final refcount must equal initial baseline under every interleaving",
            );
            assert_eq!(
                pin_ops.load(Ordering::Acquire),
                0,
                "pin_ops_in_flight must drain to 0",
            );
        });
    }
}

// Non-loom builds get a no-op placeholder so `cargo test --features
// loom-tests --test force_expiry_pin_race` (without `--cfg loom`) links
// cleanly. The assertion is vacuously true under std atomics; the real
// interleaving coverage requires `RUSTFLAGS="--cfg loom"`.
#[cfg(not(loom))]
#[test]
fn chain_snapshot_new_vs_force_expire_no_leak_requires_cfg_loom() {
    // Intentionally empty — see module docs for the run invocation.
}
