//! US-029 — `LatchedPinnedPage` buffer-pool integration tests.
//!
//! Locks down the §10.18 contract that US-029 introduces:
//!
//! 1. `Drop for LatchedPinnedPage` releases the latch BEFORE the pin
//!    (`test_latched_pinned_page_drop_order_is_latch_then_pin`).
//! 2. `BufferPool::pin_for_write` releases the partition mutex BEFORE
//!    acquiring the page-local latch — partition mutex and latch are
//!    never nested
//!    (`test_pin_for_write_releases_partition_before_latch`).
//! 3. `LatchedPinnedPage<'_>` is `!Send`, enforced at compile time by
//!    the `const _` static assertion below.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test target uses assertion-style panics and setup unwraps"
)]

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::latched_pinned_page_drop_order::{drain_events, EVENT_LATCH_RELEASE, EVENT_PIN_RELEASE};
use super::page_latch::LatchMode;
use super::{default_sizes, BufferPool, LatchedPinnedPage, PageSize};
use crate::storage::test_support::{ArcIo, MockIo};

const TEST_PAGE_ID: u32 = 0;
const TEST_OTHER_PAGE_ID: u32 = 1;
const PROBE_DRAIN_BUDGET: Duration = Duration::from_secs(2);
const PARTITION_LATCH_OBSERVE_DELAY: Duration = Duration::from_millis(150);
const SPIN_SLEEP: Duration = Duration::from_millis(5);

// ---------------------------------------------------------------------------
// Static assertion: LatchedPinnedPage<'_> must be !Send (§10.18 row 1).
// ---------------------------------------------------------------------------
//
// Stable-Rust !Send compile-time check using the type-parameter
// ambiguity pattern (mirrors `static_assertions::assert_not_impl_any!`).
// `AmbiguousIfImpl<()>` is implemented for every `T`; an additional
// `AmbiguousIfImpl<Invalid>` is provided for `T: Send`. The call below
// asks the compiler to resolve the type parameter for
// `LatchedPinnedPage<'static>`. If LPP is Send, both impls apply with
// different type parameters and the call is ambiguous (compile error).
// If LPP is !Send, only the first impl applies and the parameter
// resolves to `()`. Removing the `_not_send: PhantomData<*const ()>`
// marker would make LPP Send and break this build step.
const _: fn() = || {
    struct Invalid;
    trait AmbiguousIfSend<A> {
        fn some_item() {}
    }
    impl<T: ?Sized> AmbiguousIfSend<()> for T {}
    impl<T: ?Sized + Send> AmbiguousIfSend<Invalid> for T {}

    <LatchedPinnedPage<'static> as AmbiguousIfSend<_>>::some_item();
};

fn make_pool() -> Arc<BufferPool> {
    let io = MockIo::new();
    Arc::new(BufferPool::new(default_sizes::DESKTOP, Box::new(ArcIo(io))))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Construction wires the partition pin and the page-local latch and
/// exposes the requested mode through `latch_mode()`. The 1-arg
/// `pin_for_write` API matches the canonical signature in
/// `.omc/phase-05-prd.json` US-029 AC-4.
#[test]
fn test_pin_for_write_returns_exclusive_latched_pinned_page() {
    let pool = make_pool();
    let _ = drain_events(); // isolate from prior tests on this thread.

    let lpp = pool.pin_for_write(TEST_PAGE_ID).unwrap();
    assert_eq!(lpp.latch_mode(), LatchMode::Exclusive);
    assert_eq!(lpp.page_id(), TEST_PAGE_ID);
    drop(lpp);

    // Pin count must be back to zero — partition mutex re-acquired by Drop.
    // The 1-arg API defaults to the 32 KiB partition (Phase 5 leaf focus).
    assert_eq!(
        pool.inner_32k.lock().unwrap().pin_count(TEST_PAGE_ID),
        Some(0)
    );
}

#[test]
fn test_pin_for_read_returns_shared_latched_pinned_page() {
    let pool = make_pool();
    let _ = drain_events();

    let lpp = pool.pin_for_read(TEST_PAGE_ID).unwrap();
    assert_eq!(lpp.latch_mode(), LatchMode::Shared);
    drop(lpp);

    assert_eq!(
        pool.inner_32k.lock().unwrap().pin_count(TEST_PAGE_ID),
        Some(0)
    );
}

/// §10.18 rule 2 — `Drop for LatchedPinnedPage` releases the latch
/// BEFORE the pin.
///
/// **Strong proof.** The probe is wired through `LatchHoldRecorder`,
/// which records `EVENT_LATCH_RELEASE` only after the underlying
/// `parking_lot` guard's destructor has actually unlocked the latch.
/// `EVENT_PIN_RELEASE` is recorded only after `unpin_internal` returns.
/// The recorded sequence therefore mirrors the actual side-effect
/// order; a refactor that reordered drop vs. unpin would also reorder
/// the recorded events. The post-drop reacquisition step is a second,
/// independent verification: a fresh `pin_for_write` must succeed
/// (proving the latch is reacquirable) and `pin_count` must be `Some(0)`
/// (proving the pin was decremented).
#[test]
fn test_latched_pinned_page_drop_order_is_latch_then_pin() {
    let pool = make_pool();

    // Drain any events left over from earlier tests on this worker thread.
    let _ = drain_events();

    let lpp = pool.pin_for_write(TEST_OTHER_PAGE_ID).unwrap();
    drop(lpp);

    let events = drain_events();
    assert_eq!(
        events,
        vec![EVENT_LATCH_RELEASE, EVENT_PIN_RELEASE],
        "LatchedPinnedPage::drop must release the latch before the pin \
         (§10.18 rule 2); observed events = {events:?}",
    );

    // Side-effect proof: the latch is reacquirable and the pin is gone.
    let _reacquire = pool.pin_for_write(TEST_OTHER_PAGE_ID).expect(
        "pin_for_write must succeed after the prior LPP dropped — \
                 proves the latch was actually released",
    );
    drop(_reacquire);
    let _ = drain_events();

    assert_eq!(
        pool.inner_32k.lock().unwrap().pin_count(TEST_OTHER_PAGE_ID),
        Some(0),
        "pin_count must return to 0 after Drop completes — proves \
         unpin_internal ran and the pin was released",
    );
}

/// §10.18 — partition mutex and page-local latch are never nested.
/// `pin_for_write` must release the partition mutex BEFORE attempting to
/// acquire the latch. We instrument that contract by:
///
/// 1. Pinning the page once to load the frame, then dropping the handle
///    so we can capture a stable raw pointer to the resident `Frame`.
/// 2. Re-pinning with the standard `pin` so the frame stays resident
///    across the test.
/// 3. Acquiring the page-local latch directly via the captured pointer
///    so we control when it can be released.
/// 4. Spawning a worker that calls `pin_for_write`. With the latch held
///    by us, the worker must block at the latch-acquire step.
/// 5. While the worker is blocked, attempting `try_lock` on the partition
///    mutex. If the lock-order contract holds, it is FREE; if the
///    implementation kept the partition mutex across the latch wait,
///    `try_lock` fails and the test fails.
#[test]
fn test_pin_for_write_releases_partition_before_latch() {
    let pool = make_pool();

    // Step 1-2: load the frame and keep it resident with a regular pin
    // (no latch). `_resident` keeps `pin_count >= 1` for the whole test.
    // The 1-arg `pin_for_write` API targets the 32 KiB partition by
    // default — see `BufferPool::detect_page_size`.
    let lpp = pool.pin_for_write(TEST_PAGE_ID).unwrap();
    let frame_ptr = lpp.frame_ptr;
    drop(lpp);
    let _resident = pool.pin(TEST_PAGE_ID, PageSize::Large32k).unwrap();

    // Step 3: hold the page-local latch directly. SAFETY: `_resident`
    // keeps the frame slot from being evicted; the `Frame` lives at a
    // stable address inside the partition's pre-allocated slot vector.
    let latch_ref = unsafe { &(*frame_ptr).latch };
    let held = latch_ref.lock_exclusive();

    // Step 4: spawn a worker calling `pin_for_write`. It must reach the
    // latch-acquire step (where it blocks).
    let pool_for_thread = pool.clone();
    let pin_thread = thread::spawn(move || {
        let lpp = pool_for_thread
            .pin_for_write(TEST_PAGE_ID)
            .expect("pin_for_write must succeed once the latch is released");
        drop(lpp);
    });

    // Wait until the worker has progressed past the partition mutex
    // step — the only way `pin_count` reaches 2 is for `pin_page` to
    // have run, which means the partition mutex was acquired and then
    // released around the bump.
    let deadline = Instant::now() + PROBE_DRAIN_BUDGET;
    loop {
        let pc = pool.inner_32k.lock().unwrap().pin_count(TEST_PAGE_ID);
        if pc == Some(2) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "pin_for_write did not advance past the partition-mutex step \
             within {:?}; observed pin_count={pc:?}",
            PROBE_DRAIN_BUDGET,
        );
        thread::sleep(SPIN_SLEEP);
    }

    // Give the worker a beat to reach the latch-wait state after dropping
    // its partition guard.
    thread::sleep(PARTITION_LATCH_OBSERVE_DELAY);

    // Step 5: the partition mutex MUST be free while the worker is
    // blocked on the latch.
    let try_partition = pool.inner_32k.try_lock();
    let partition_was_free = try_partition.is_ok();
    drop(try_partition);

    // Release the latch so the worker completes; tear down.
    drop(held);
    pin_thread.join().unwrap();

    assert!(
        partition_was_free,
        "pin_for_write must release the partition mutex BEFORE blocking \
         on the page-local latch (§10.18 — partition mutex and latch \
         are never nested)",
    );
}
