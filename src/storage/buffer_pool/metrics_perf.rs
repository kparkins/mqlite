//! PR1 perf-counter readers, gated by the `perf-counters` cargo feature.
//!
//! All counter storage lives in [`super::chains`] (the AtomicU64
//! statics) plus the shared-latch-wait histogram below. This module is
//! the single read surface for [`examples/perf_axis`] and the AC
//! verification harness; production binaries that don't enable
//! `perf-counters` never compile this file and pay zero overhead.
//!
//! Counter visibility is `pub(crate) static` (NOT `pub(super)`) on the
//! source side so this cross-module reader can name them — see
//! `mwmr-page-latch.md` rev 4 PR1 §AC and team-lead PR1 reminder #5.

#![cfg(feature = "perf-counters")]

use std::sync::atomic::Ordering;

use hdrhistogram::Histogram;
use parking_lot::Mutex;

use super::chains::{FLIP_RETRY_EXHAUSTED, FLIP_RETRY_TOTAL, FLIP_TXN_TOTAL};

/// Reader-thread `pin_then_latch` shared-acquire latency in
/// nanoseconds, sampled by [`record_shared_latch_wait_ns`]. Drives
/// PR1's read-coupling AC: p50 and p99 must each stay within 1.05× of
/// PR0 baseline under `read_find_one_under_writers`.
///
/// Hdrhistogram is configured for [1 ns, 60 s] with 3-significant-digit
/// precision — typical shared-latch acquires are sub-microsecond, so
/// this gives ~1 % bucket resolution at 1 µs. Mutex-guarded because
/// hdrhistogram is `!Sync`; uncontended record path is a single
/// fetch-and-bump on the histogram's count array.
static SHARED_LATCH_WAIT_NS_HIST: Mutex<Option<Histogram<u64>>> = Mutex::new(None);

const SHARED_LATCH_WAIT_HIST_LO: u64 = 1; // 1 ns
const SHARED_LATCH_WAIT_HIST_HI: u64 = 60_000_000_000; // 60 s
const SHARED_LATCH_WAIT_HIST_SIGFIG: u8 = 3;

fn with_hist<R>(f: impl FnOnce(&mut Histogram<u64>) -> R) -> R {
    let mut guard = SHARED_LATCH_WAIT_NS_HIST.lock();
    let hist = guard.get_or_insert_with(|| {
        Histogram::<u64>::new_with_bounds(
            SHARED_LATCH_WAIT_HIST_LO,
            SHARED_LATCH_WAIT_HIST_HI,
            SHARED_LATCH_WAIT_HIST_SIGFIG,
        )
        .expect("hdrhistogram bounds are static and valid")
    });
    f(hist)
}

/// Record a single shared-latch acquire latency sample in nanoseconds.
///
/// Called by the reader thread in the `read_find_one_under_writers`
/// perf-axis scenario after each `pin_then_latch(_, _, Shared)` call.
/// The recording path is the only contended hot spot in this module;
/// production paths (writers) do NOT call this — only the read-side
/// scenario harness does.
pub fn record_shared_latch_wait_ns(ns: u64) {
    with_hist(|h| {
        // Saturating record so the AC harness never panics on outliers
        // larger than the configured upper bound.
        let _ = h.saturating_record(ns);
    });
}

/// Reset the histogram (for repeatable measurement runs). Called by
/// the perf-axis harness between repeats.
pub fn reset_shared_latch_wait_hist() {
    let mut guard = SHARED_LATCH_WAIT_NS_HIST.lock();
    if let Some(h) = guard.as_mut() {
        h.reset();
    }
}

/// Reset the flip-retry counters (for repeatable measurement runs).
pub fn reset_flip_counters() {
    FLIP_TXN_TOTAL.store(0, Ordering::Relaxed);
    FLIP_RETRY_TOTAL.store(0, Ordering::Relaxed);
    FLIP_RETRY_EXHAUSTED.store(0, Ordering::Relaxed);
}

/// Bounded-retry rate over the workload window.
///
/// Returned as `FLIP_RETRY_TOTAL / FLIP_TXN_TOTAL`; `0.0` when no
/// flips have happened (avoids division by zero). PR1's AC requires
/// `< 0.01` (1 %) over a 30 s `same_ns_single` workload.
pub fn flip_retry_rate() -> f64 {
    let total = FLIP_TXN_TOTAL.load(Ordering::Relaxed);
    if total == 0 {
        return 0.0;
    }
    let retries = FLIP_RETRY_TOTAL.load(Ordering::Relaxed);
    retries as f64 / total as f64
}

/// Total bounded-retry exhaustion events. PR1's AC requires `== 0` —
/// any exhaustion poisoned the engine via
/// `EngineFatal { PostDurablePendingFlipFailure }` and triggers PR
/// revert.
pub fn flip_retry_exhausted_count() -> u64 {
    FLIP_RETRY_EXHAUSTED.load(Ordering::Relaxed)
}

/// Median of recorded shared-latch acquire latencies in nanoseconds.
/// Returns 0 when no samples have been recorded.
pub fn shared_latch_wait_p50_ns() -> u64 {
    with_hist(|h| {
        if h.is_empty() {
            return 0;
        }
        h.value_at_percentile(50.0)
    })
}

/// 99th percentile of recorded shared-latch acquire latencies in
/// nanoseconds. Returns 0 when no samples have been recorded.
pub fn shared_latch_wait_p99_ns() -> u64 {
    with_hist(|h| {
        if h.is_empty() {
            return 0;
        }
        h.value_at_percentile(99.0)
    })
}
