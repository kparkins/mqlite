//! Loom-based concurrency stress fixtures for MVCC scaffolding (T0).
//!
//! Run with:
//!   RUSTFLAGS="--cfg loom" cargo test --release --features loom-tests --test loom_common
//!
//! Without `--cfg loom`, the `loom` crate is a thin std-wrapper and its
//! `model()` does not permute interleavings; the rustc cfg is what activates
//! loom's permutation engine. The whole file is gated on the `loom-tests`
//! Cargo feature so that `cargo test --lib` (and ordinary `cargo test`) does
//! not attempt to build it.

#![cfg(feature = "loom-tests")]

/// Atomics + Mutex shim. Production code in `src/mvcc/*` (T2+) follows this
/// same pattern: under `cfg(loom)` it uses loom's instrumented primitives so
/// loom's `model()` can permute them; under `cfg(not(loom))` it uses
/// `std::sync` directly with zero overhead.
#[cfg(loom)]
#[allow(dead_code)]
pub mod loom_atomic {
    pub use loom::sync::atomic;
    pub use loom::sync::Mutex;
}

#[cfg(not(loom))]
#[allow(dead_code)]
pub mod loom_atomic {
    pub use std::sync::atomic;
    pub use std::sync::Mutex;
}

#[cfg(loom)]
mod refcount_model {
    use loom::sync::atomic::{AtomicU32, Ordering};
    use loom::sync::Arc;
    use loom::thread;

    /// Saturating CAS-loop incref. Mirrors the production `incref_overflow`
    /// shape from T3 (see plan §T3 types section). Returns `Err(())` if the
    /// pre-CAS observed value is `u32::MAX`.
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

    /// Atomic decref. Mirrors the production `decref_overflow` from T3.
    /// Returns the post-decrement value.
    fn decref(count: &AtomicU32) -> u32 {
        let prev = count.fetch_sub(1, Ordering::Release);
        debug_assert!(prev > 0, "decref on already-zero refcount");
        prev - 1
    }

    /// T0 acceptance fixture: explore all interleavings of a 2-thread
    /// CAS-incref / atomic-decref pair starting from refcount = 1, and
    /// assert round-trip identity (final refcount = 1).
    ///
    /// Models the smallest unit of `OverflowRef::Clone` racing
    /// `OverflowRef::Drop` on the same overflow page header.
    #[test]
    fn model_atomic_u32_refcount_round_trip() {
        loom::model(|| {
            let count = Arc::new(AtomicU32::new(1));
            let c_clone = count.clone();
            let c_drop = count.clone();

            let cloner = thread::spawn(move || {
                incref(&c_clone).expect("incref on non-saturated refcount must succeed");
            });

            let dropper = thread::spawn(move || {
                decref(&c_drop);
            });

            cloner.join().unwrap();
            dropper.join().unwrap();

            assert_eq!(
                count.load(Ordering::Acquire),
                1,
                "incref + decref must round-trip to the starting refcount under every interleaving",
            );
        });
    }

    /// T3 fixture: two concurrent Clones from a live baseline refcount=1
    /// must drive the final refcount to 3 under every interleaving. This
    /// models two callers simultaneously cloning an `OverflowRef` — neither
    /// may observe a lost update from the CAS retry loop.
    #[test]
    fn model_two_clones_from_refcount_one_reaches_three() {
        loom::model(|| {
            let count = Arc::new(AtomicU32::new(1));
            let a = count.clone();
            let b = count.clone();

            let t1 = thread::spawn(move || {
                incref(&a).expect("incref OK");
            });
            let t2 = thread::spawn(move || {
                incref(&b).expect("incref OK");
            });
            t1.join().unwrap();
            t2.join().unwrap();

            assert_eq!(
                count.load(Ordering::Acquire),
                3,
                "two clones from refcount=1 must reach refcount=3 on every interleaving",
            );
        });
    }

    /// T3 fixture: Clone racing Drop from baseline refcount=2 must always
    /// leave at least one live ref (final >= 1). A 0-enqueue in this
    /// scenario would mean the page could be deferred-freed while a live
    /// `OverflowRef` still exists — catastrophic use-after-free.
    #[test]
    fn model_clone_racing_drop_leaves_live_ref() {
        loom::model(|| {
            let count = Arc::new(AtomicU32::new(2));
            let a = count.clone();
            let b = count.clone();

            let cloner = thread::spawn(move || {
                incref(&a).expect("incref OK");
            });
            let dropper = thread::spawn(move || {
                decref(&b);
            });
            cloner.join().unwrap();
            dropper.join().unwrap();

            // One clone (+1) and one drop (-1) from 2 → final must be 2.
            // The key invariant is >= 1 in all reachable states after both
            // threads complete: no 0-enqueue possible.
            assert_eq!(count.load(Ordering::Acquire), 2);
        });
    }

    /// T3 fixture: when refcount is saturated at u32::MAX, concurrent
    /// `incref` calls must all fail without mutating the counter. Models
    /// the CAS-loop saturation bailout from `incref_overflow`.
    #[test]
    fn model_cas_saturation_never_mutates() {
        loom::model(|| {
            let count = Arc::new(AtomicU32::new(u32::MAX));
            let a = count.clone();
            let b = count.clone();

            let t1 = thread::spawn(move || incref(&a));
            let t2 = thread::spawn(move || incref(&b));
            let r1 = t1.join().unwrap();
            let r2 = t2.join().unwrap();

            assert!(r1.is_err(), "saturated incref must fail");
            assert!(r2.is_err(), "saturated incref must fail");
            assert_eq!(
                count.load(Ordering::Acquire),
                u32::MAX,
                "saturated refcount must not wrap or otherwise mutate",
            );
        });
    }
}
