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
            match count.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::Release,
                Ordering::Acquire,
            ) {
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
                incref(&c_clone)
                    .expect("incref on non-saturated refcount must succeed");
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
}
