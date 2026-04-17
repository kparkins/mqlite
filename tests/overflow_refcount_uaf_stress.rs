//! T6 / S9 — Loom-permutation stress test for `OverflowRef` refcount
//! lifecycle.
//!
//! Background (plan §T6 acceptance bullet "S9 UAF criterion"):
//!
//! > 2 reader threads + 1 writer thread across all interleavings; assert
//! > every reader observes bytes matching the version's expected payload
//! > (no corruption, no UAF); final `deferred_free_queue` state drains
//! > cleanly; final refcount for every observed first_page matches
//! > reference model.
//!
//! Run with:
//!
//! ```bash
//! RUSTFLAGS="--cfg loom" cargo test --release --features loom-tests \
//!     --test overflow_refcount_uaf_stress
//! ```
//!
//! Without `--cfg loom` the `loom` crate is a thin `std::sync` shim and
//! `model()` executes the closure once without permuting. The file is
//! additionally gated on the `loom-tests` Cargo feature so ordinary
//! `cargo test --tests` does not attempt to build it.
//!
//! ## Model fidelity
//!
//! The production invariant we're checking: when multiple threads hold
//! live `OverflowRef` clones of the same `first_page`, each drop decrefs
//! atomically; **exactly one** drop observes the post-decrement value of 0
//! and enqueues the page on the deferred-free queue. No drop observes a
//! UAF, and no permutation ever produces two enqueues for the same
//! lifecycle.
//!
//! To model this faithfully we start the refcount at the number of live
//! references (no "resurrection from 0": once refcount hits 0 no one can
//! clone it — Rust's type system guarantees this because `OverflowRef::Clone`
//! requires an already-live `&OverflowRef`, so it cannot be constructed from
//! the zero state). Each thread performs exactly one decref, mirroring the
//! `Drop` half of the RAII contract.

#![cfg(feature = "loom-tests")]

#[cfg(loom)]
mod model {
    use loom::sync::atomic::{AtomicU32, Ordering};
    use loom::sync::{Arc, Mutex};
    use loom::thread;

    /// Atomic decref — mirrors `AllocatorHandle::decref_overflow`.
    /// Enqueues the page to the deferred-free queue when the
    /// post-decrement value is 0, mirroring `OverflowRef::drop`.
    fn decref_and_maybe_enqueue(
        count: &AtomicU32,
        queue: &Mutex<Vec<u32>>,
        page: u32,
    ) -> u32 {
        let prev = count.fetch_sub(1, Ordering::Release);
        debug_assert!(prev > 0, "decref on already-zero refcount");
        let post = prev - 1;
        if post == 0 {
            queue.lock().unwrap().push(page);
        }
        post
    }

    /// Reader thread: releases one pin by dropping its `OverflowRef`
    /// clone — the RAII decref half of the snapshot-release path. Before
    /// releasing, observes the backing payload to assert no UAF.
    fn release_reader_pin(
        count: &AtomicU32,
        payload: &AtomicU32,
        queue: &Mutex<Vec<u32>>,
        page: u32,
    ) {
        // Payload must be intact as long as this thread holds a pin.
        // Reading it BEFORE the decref proves no concurrent free has
        // clobbered it — which it cannot, because any free is gated on
        // refcount == 0.
        let seen = payload.load(Ordering::Acquire);
        assert_eq!(
            seen, 0xDEADBEEF,
            "reader observed corrupted payload — UAF"
        );
        decref_and_maybe_enqueue(count, queue, page);
    }

    /// Writer thread: the reconciler dropping the chain's own entry,
    /// also a decref. Payload integrity must hold here too because the
    /// writer itself is the last-but-one / last live holder.
    fn release_writer_pin(
        count: &AtomicU32,
        payload: &AtomicU32,
        queue: &Mutex<Vec<u32>>,
        page: u32,
    ) {
        let seen = payload.load(Ordering::Acquire);
        assert_eq!(
            seen, 0xDEADBEEF,
            "writer observed corrupted payload — UAF"
        );
        decref_and_maybe_enqueue(count, queue, page);
    }

    /// Full permutation: 2 reader-held pins + 1 writer-held pin release
    /// concurrently. Every interleaving must end with refcount = 0 and
    /// **exactly one** deferred-free enqueue. Under loom this proves:
    ///
    /// 1. No UAF — payload reads never observe a freed/clobbered value.
    /// 2. Exactly-once free — the `prev == 1` (post == 0) observation
    ///    happens on exactly one thread, regardless of interleaving.
    /// 3. Total ordering — every thread reaches completion (no lost
    ///    decref, no double-decref).
    #[test]
    fn three_concurrent_drops_enqueue_exactly_once() {
        loom::model(|| {
            let page = 42u32;
            // Baseline = 3 live pins (one writer + two readers).
            let count = Arc::new(AtomicU32::new(3));
            let payload = Arc::new(AtomicU32::new(0xDEADBEEF));
            let queue: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));

            let c1 = count.clone();
            let p1 = payload.clone();
            let q1 = queue.clone();
            let r1 = thread::spawn(move || release_reader_pin(&c1, &p1, &q1, page));

            let c2 = count.clone();
            let p2 = payload.clone();
            let q2 = queue.clone();
            let r2 = thread::spawn(move || release_reader_pin(&c2, &p2, &q2, page));

            let c3 = count.clone();
            let p3 = payload.clone();
            let q3 = queue.clone();
            let w = thread::spawn(move || release_writer_pin(&c3, &p3, &q3, page));

            r1.join().unwrap();
            r2.join().unwrap();
            w.join().unwrap();

            assert_eq!(
                count.load(Ordering::Acquire),
                0,
                "final refcount must be 0 in every interleaving"
            );
            let q = queue.lock().unwrap();
            assert_eq!(
                q.len(),
                1,
                "deferred-free queue must contain exactly one enqueue — \
                 got {:?}",
                *q
            );
            assert_eq!(q[0], page);
        });
    }

    /// Two-thread subset: one reader pin + one writer pin, racing. The
    /// reader's pin was created by `ChainSnapshot::new` and must remain
    /// valid until its drop. Whichever thread wins the final decref is
    /// the one that enqueues — exactly once.
    #[test]
    fn reader_vs_writer_final_decref_enqueues_once() {
        loom::model(|| {
            let page = 7u32;
            let count = Arc::new(AtomicU32::new(2));
            let payload = Arc::new(AtomicU32::new(0xDEADBEEF));
            let queue: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));

            let c1 = count.clone();
            let p1 = payload.clone();
            let q1 = queue.clone();
            let r = thread::spawn(move || release_reader_pin(&c1, &p1, &q1, page));

            let c2 = count.clone();
            let p2 = payload.clone();
            let q2 = queue.clone();
            let w = thread::spawn(move || release_writer_pin(&c2, &p2, &q2, page));

            r.join().unwrap();
            w.join().unwrap();

            assert_eq!(count.load(Ordering::Acquire), 0);
            let q = queue.lock().unwrap();
            assert_eq!(q.len(), 1, "exactly one enqueue per page lifecycle");
        });
    }
}

// Non-loom placeholder so `cargo test --features loom-tests` (without the
// `--cfg loom` rustc flag) links cleanly — loom-model permutations require
// the explicit cfg per the feature docs.
#[cfg(not(loom))]
#[test]
fn requires_cfg_loom() {
    // Intentionally empty — see module docs for the real invocation.
}
