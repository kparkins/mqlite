//! Bug-suspect #1 (deep-refactor-2026-06-10, ranked entry IDX 16): the
//! store-buffering (SB) race between `ChainSnapshot::new` and
//! `ReadView::force_expire`.
//!
//! VERDICT: **REFUTED** (benign litmus shape). This file is a pinning model
//! that documents *why* the SB shape on `poisoned` / `pin_ops_in_flight` does
//! not produce a use-after-free, plus the refcount-neutrality invariant the
//! protocol actually owns.
//!
//! ## The suspected race
//!
//! `ChainSnapshot::new` does `pin_ops_in_flight.fetch_add(1, Release)` then
//! `poisoned.load(Acquire)`; `force_expire` does `poisoned.store(true,
//! Release)` then `pin_ops_in_flight.load(Acquire)`. That IS the canonical SB
//! litmus shape, and under Release/Acquire the "alive snapshot after
//! force_expire returned" interleaving is reachable (loom confirms it). The
//! suspect feared this lets `drop_namespace` free a namespace's pages while a
//! reader still walks them.
//!
//! ## Why it is benign — the lifetime of the cloned memory is NOT governed by
//!    these two atomics
//!
//! The page memory `ChainSnapshot::new` dereferences (`frame.deltas`) is
//! protected by a **shared page latch + a live frame pin** held by the
//! `LatchedPinnedPage` handle for the entire duration of the clone
//! (`src/storage/buffer_pool/latched_page.rs:146-176`, the `// SAFETY:` notes).
//! Any page-freeing path (`drop_namespace`'s tree sweep) must take an
//! **exclusive** page latch to clear/free a leaf, which is mutually exclusive
//! with the reader's shared latch — so the leaf frame cannot be freed
//! mid-clone, independent of `pin_ops` / `poisoned`.
//!
//! What the `pin_ops` / `poisoned` handoff actually guards is the
//! **overflow-page refcount** transfer: each cloned `VersionData::Overflow`
//! entry runs a CAS-loop incref during the clone, and `OverflowRef::Drop`
//! decrefs on snapshot drop. The protocol's only obligation is that this
//! incref/decref nets to zero under every interleaving — no leak, no
//! double-decref, no 0-enqueue while a live ref exists. A reader whose
//! post-bump recheck did NOT observe poison ("alive") has already incref'd its
//! overflow pages, so force_expire returning afterward cannot free them. That
//! is exactly why the existing harness (`tests/force_expiry_pin_race.rs`)
//! deliberately TOLERATES the alive-after-expire interleaving.
//!
//! ## What this model asserts
//!
//! The refcount-neutrality invariant the protocol genuinely owns, under the
//! production Release/Acquire orderings: across every interleaving of
//! `ChainSnapshot::new` racing `force_expire`, the overflow refcount returns
//! to its baseline once the (possibly-alive) snapshot is dropped, and
//! `pin_ops_in_flight` drains to 0. This passes — there is no bug to fix.
//!
//! ## Run
//!
//! ```bash
//! RUSTFLAGS="--cfg loom" cargo test --release --features loom-tests \
//!     --test bugsuspect_mvcc_force_expire_sb_race
//! ```
//!
//! Without `--cfg loom` the `loom` crate is a thin std shim that does not
//! permute interleavings; the model is vacuous. The whole file is gated on the
//! `loom-tests` Cargo feature so ordinary `cargo test` skips it.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test targets use assertion-style panics and setup unwraps"
)]
#![cfg(feature = "loom-tests")]

#[cfg(loom)]
mod model {
    use loom::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use loom::sync::Arc;
    use loom::thread;

    /// Saturating CAS-loop incref — same shape as production
    /// `AllocatorHandle::incref_overflow` and the existing
    /// `force_expiry_pin_race` model.
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

    /// Atomic decref — mirrors `OverflowRef::Drop`.
    fn decref(count: &AtomicU32) -> u32 {
        let prev = count.fetch_sub(1, Ordering::Release);
        debug_assert!(prev > 0, "decref on already-zero refcount");
        prev - 1
    }

    /// Production-faithful `ChainSnapshot::new` atomic protocol
    /// (`src/mvcc/chain_snapshot.rs:71-111`): pre-check poison, `fetch_add`,
    /// incref-during-clone, post-bump poison recheck, `fetch_sub`,
    /// decref-all-on-poisoned. Returns whether the snapshot is "alive" and
    /// therefore still pins its overflow entry.
    fn snapshot_new(poisoned: &AtomicBool, pin_ops: &AtomicU32, refcount: &AtomicU32) -> bool {
        if poisoned.load(Ordering::Acquire) {
            return false;
        }
        pin_ops.fetch_add(1, Ordering::Release);

        // Deep clone increfs the overflow page. The leaf frame this reads is
        // latch-pinned in production, so it cannot be freed here; only the
        // overflow refcount handoff is exercised by this model.
        incref(refcount).expect("refcount must not saturate in fixture");

        let poisoned_after = poisoned.load(Ordering::Acquire);
        pin_ops.fetch_sub(1, Ordering::Release);
        if poisoned_after {
            // RAII drop of the bailed snapshot decrefs the entry it incref'd.
            decref(refcount);
            return false;
        }
        true
    }

    /// Production-faithful `ReadView::force_expire` (`src/mvcc/read_view.rs:314`).
    fn force_expire(poisoned: &AtomicBool, pin_ops: &AtomicU32) {
        poisoned.store(true, Ordering::Release);
        while pin_ops.load(Ordering::Acquire) != 0 {
            thread::yield_now();
        }
    }

    /// The invariant the `poisoned` / `pin_ops` protocol genuinely owns:
    /// refcount neutrality across every interleaving. The alive-after-expire
    /// interleaving is reachable and SAFE (the alive snapshot holds an incref'd
    /// overflow page; the leaf frame is latch-pinned), so we explicitly allow
    /// it and only require the net refcount delta to be zero once the snapshot
    /// is dropped.
    #[test]
    fn force_expire_overflow_refcount_handoff_is_neutral() {
        loom::model(|| {
            let poisoned = Arc::new(AtomicBool::new(false));
            let pin_ops = Arc::new(AtomicU32::new(0));
            let refcount = Arc::new(AtomicU32::new(1)); // source-chain pin

            let p_snap = poisoned.clone();
            let o_snap = pin_ops.clone();
            let r_snap = refcount.clone();
            let snap = thread::spawn(move || snapshot_new(&p_snap, &o_snap, &r_snap));

            let p_fe = poisoned.clone();
            let o_fe = pin_ops.clone();
            let fe = thread::spawn(move || force_expire(&p_fe, &o_fe));

            let alive = snap.join().unwrap();
            fe.join().unwrap();

            let after_race = refcount.load(Ordering::Acquire);
            if alive {
                assert_eq!(
                    after_race, 2,
                    "an alive snapshot holds exactly one pin above baseline (its overflow page \
                     cannot have been freed by force_expire — it was incref'd before the recheck)"
                );
                decref(&refcount); // the reader eventually drops the snapshot
            } else {
                assert_eq!(
                    after_race, 1,
                    "a bailed snapshot leaves the refcount at baseline (no leak, no double-decref)"
                );
            }

            assert_eq!(
                refcount.load(Ordering::Acquire),
                1,
                "overflow refcount must round-trip to baseline under every interleaving"
            );
            assert_eq!(
                pin_ops.load(Ordering::Acquire),
                0,
                "pin_ops_in_flight must drain to 0"
            );
        });
    }
}

#[cfg(not(loom))]
#[test]
fn force_expire_sb_race_requires_cfg_loom() {
    // Intentionally empty — the SB shape is a weak-memory phenomenon only
    // loom's permutation engine (RUSTFLAGS="--cfg loom") can surface. See the
    // module-level docs: the shape is REFUTED as a UAF because page-memory
    // lifetime is governed by the page latch + overflow refcount, not by the
    // poisoned / pin_ops atomics.
}
