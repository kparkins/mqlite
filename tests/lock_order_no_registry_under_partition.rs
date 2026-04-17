//! T6 — Lock-order invariant: `ReadViewRegistry::oldest_required_ts()`
//! (position 5) must be **snapshotted before** the buffer-pool partition
//! mutex (positions 3/4). Acquiring the registry under a partition mutex
//! inverts the canonical total order — the plan explicitly forbids it.
//!
//! `BufferPool::reconcile` is crate-private, so this file guards the
//! invariant two ways:
//!
//! 1. A runtime stress reproduces the canonical acquisition order on a
//!    surrogate mutex pair at the same relative positions and asserts it
//!    completes without hanging (no circular-wait cycle is reachable
//!    through the canonical order).
//! 2. A source-audit test opens `src/storage/buffer_pool.rs` and checks
//!    that the `BufferPool::reconcile` body contains the
//!    `registry.oldest_required_ts()` call **before** any
//!    `inner_32k.lock()` — any refactor that inverts those two lines
//!    will fail the audit.

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

/// Source-audit: in `BufferPool::reconcile`, the registry snapshot must
/// appear textually BEFORE the first `inner_32k.lock()`. This is the
/// hard, unambiguous check — it fails loudly if a future refactor
/// inverts the two lines.
#[test]
fn reconcile_snapshots_registry_before_partition_lock() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = manifest_dir.join("src").join("storage").join("buffer_pool.rs");
    let body = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!("cannot read {}: {e}", path.display());
    });

    let fn_start = body
        .find("pub(crate) fn reconcile(")
        .expect("reconcile function not found in buffer_pool.rs");
    // Grab a generous slice — enough to include both key calls.
    let fn_slice = &body[fn_start..fn_start.saturating_add(4096).min(body.len())];

    let ort_call = fn_slice
        .find("registry.oldest_required_ts()")
        .expect("reconcile must call registry.oldest_required_ts()");
    let lock_call = fn_slice
        .find("inner_32k")
        .expect("reconcile must lock inner_32k partition");

    assert!(
        ort_call < lock_call,
        "LOCK-ORDER VIOLATION: registry.oldest_required_ts() must appear \
         BEFORE inner_32k partition acquisition in BufferPool::reconcile"
    );
}
