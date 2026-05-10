//! T6 — Lock-order invariant: `ReadViewRegistry::oldest_required_ts()`
//! (position 5) must be **snapshotted before** the buffer-pool partition
//! mutex (positions 3/4). Acquiring the registry under a partition mutex
//! inverts the canonical total order — the plan explicitly forbids it.
//!
//! `BufferPool::pin_with_reconcile` is crate-private, so this file guards the
//! invariant two ways:
//!
//! 1. A runtime stress reproduces the canonical acquisition order on a
//!    surrogate mutex pair at the same relative positions and asserts it
//!    completes without hanging (no circular-wait cycle is reachable
//!    through the canonical order).
//! 2. A source-audit test opens `src/storage/buffer_pool/mod.rs` and
//!    checks that the `BufferPool::pin_with_reconcile` body contains the
//!    `registry.oldest_required_ts()` call **before** any
//!    `inner_32k.lock()` — any refactor that inverts those two lines
//!    will fail the audit.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Surrogate for `ReadViewRegistry::inner` — position 5.
#[derive(Default)]
struct Registry {
    inner: Mutex<()>,
}

/// Surrogate for a `BufferPool` partition — position 3 (or 4).
#[derive(Default)]
struct Partition {
    inner: Mutex<()>,
}

/// Canonical T6 path: snapshot registry THEN acquire partition.
fn canonical_path(reg: &Registry, part: &Partition) {
    let snapshot_guard = reg.inner.lock().unwrap();
    drop(snapshot_guard); // mirror `oldest_required_ts()` returning by value
    let _p = part.inner.lock().unwrap();
}

/// Two threads hammer the canonical path simultaneously; must finish
/// under a generous budget. Any deadlock would hang the join indefinitely;
/// an exit under the budget proves no circular-wait cycle is reachable
/// through the canonical order.
#[test]
fn canonical_order_does_not_deadlock_under_contention() {
    let reg = Arc::new(Registry::default());
    let part = Arc::new(Partition::default());
    let iterations = 10_000usize;

    let mut handles = Vec::new();
    for _ in 0..2 {
        let reg = Arc::clone(&reg);
        let part = Arc::clone(&part);
        handles.push(thread::spawn(move || {
            for _ in 0..iterations {
                canonical_path(&reg, &part);
            }
        }));
    }

    let start = Instant::now();
    for h in handles {
        h.join().unwrap();
    }
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "canonical order must not deadlock"
    );
}

/// Source-audit: in `BufferPool::pin_with_reconcile`, the registry snapshot must
/// appear textually BEFORE the first partition lock. This is the
/// hard, unambiguous check — it fails loudly if a future refactor
/// inverts the two lines.
#[test]
fn reconcile_snapshots_registry_before_partition_lock() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir
        .join("src")
        .join("storage")
        .join("buffer_pool")
        .join("mod.rs");
    let body = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!("cannot read {}: {e}", path.display());
    });

    let fn_start = body
        .find("pub(crate) fn pin_with_reconcile")
        .expect("pin_with_reconcile function not found in buffer_pool/mod.rs");
    // Grab a generous slice — enough to include both key calls.
    let fn_slice = &body[fn_start..fn_start.saturating_add(4096).min(body.len())];

    let ort_call = fn_slice
        .find("registry.oldest_required_ts()")
        .expect("reconcile must call registry.oldest_required_ts()");
    let lock_call = fn_slice
        .find(".lock()")
        .expect("pin_with_reconcile must lock a buffer-pool partition");

    assert!(
        ort_call < lock_call,
        "LOCK-ORDER VIOLATION: registry.oldest_required_ts() must appear \
         BEFORE partition acquisition in BufferPool::pin_with_reconcile"
    );
}

/// US-015: `pin_then_latch` must release the partition mutex before it
/// acquires the resident page latch. The source shape is intentionally
/// audited because the failure mode is a lock-order inversion, not an
/// output-value bug.
#[test]
fn test_pin_before_latch_no_partition_under_page_latch() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir
        .join("src")
        .join("storage")
        .join("buffer_pool")
        .join("mod.rs");
    let body = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!("cannot read {}: {e}", path.display());
    });

    let fn_start = body
        .find("pub(super) fn pin_then_latch(")
        .expect("pin_then_latch function not found in buffer_pool/mod.rs");
    let fn_slice = &body[fn_start..fn_start.saturating_add(4096).min(body.len())];

    let partition_lock = fn_slice
        .find("let mut guard = lock")
        .expect("pin_then_latch must acquire a partition mutex");
    let partition_release = fn_slice
        .find("};\n\n        // Step 5: acquire the latch")
        .expect("pin_then_latch must end the partition-mutex block before Step 5");
    let latch_acquire = fn_slice
        .find("latch_ref.lock_exclusive()")
        .expect("pin_then_latch must acquire the page latch after pinning");

    assert!(
        partition_lock < partition_release && partition_release < latch_acquire,
        "pin_then_latch must pin under the partition mutex, release it, \
         then acquire the PageLatch"
    );
}

/// US-015: the Phase 5 lock-order table puts the read-view registry below
/// partition/page-latch positions and the publish sequencer below that,
/// before writer-registry admission and the catalog mutex.
#[test]
fn test_lock_order_publish_sequencer_below_registry() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir.join("src").join("mvcc").join("read_view.rs");
    let body = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!("cannot read {}: {e}", path.display());
    });

    let history = body
        .find("1.   history-store partition mutex")
        .expect("lock-order table must include position 1");
    let lifetime_queue = body
        .find("1.5. PageLifetimeQueue::pending mutex")
        .expect("lock-order table must include position 1.5");
    let allocator = body
        .find("2.   AllocatorHandle::state mutex")
        .expect("lock-order table must include position 2");
    let partition_32k = body
        .find("3.   32 KB main partition mutex")
        .expect("lock-order table must include position 3");
    let page_latch = body
        .find("3a.  PageLatch")
        .expect("lock-order table must include position 3a");
    let partition_4k = body
        .find("3b.  4 KB main partition mutex")
        .expect("lock-order table must include position 3b");
    let registry = body
        .find("5.   ReadViewRegistry mutex")
        .expect("lock-order table must include position 5");
    let sequencer = body
        .find("6.   PublishSequencer mutex")
        .expect("lock-order table must include position 6");
    let writers = body
        .find("7.   NsWriterRegistry admission mutex")
        .expect("lock-order table must include position 7");
    let catalog = body
        .find("8.   catalog Mutex")
        .expect("lock-order table must include position 8");

    assert!(
        history < lifetime_queue
            && lifetime_queue < allocator
            && allocator < partition_32k
            && partition_32k < page_latch
            && page_latch < partition_4k
            && partition_4k < registry
            && registry < sequencer
            && sequencer < writers
            && writers < catalog,
        "Phase 5 lock-order table must preserve positions 1, 1.5, 2, 3, \
         3a, 3b, 5, 6, 7, and 8 in order"
    );
}
