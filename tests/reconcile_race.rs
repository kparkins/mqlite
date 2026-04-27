//! T6 / S1 race criterion — 4 reader threads + 1 reconciler, 60 s soak.
//!
//! The real `BufferPool::reconcile` keeps per-frame delta chains in a
//! map of `Vec<u8>` to `Arc<VecDeque<VersionEntry>>` and retains entries
//! with `stop_ts > oldest_required_ts` (plus the live head) while readers
//! concurrently construct `ChainSnapshot`s. Because `BufferPool` is
//! crate-private, this integration test exercises the same invariant at
//! the public MVCC layer: the reconciler mutates a shared chain map via
//! `Arc::make_mut`/`retain`; readers race to snapshot the same map via
//! `ChainSnapshot::new`; no reader may ever observe a "missing / mismatched"
//! visible version for its timestamp.
//!
//! Default duration 3s for CI; override via `MQLITE_RECONCILE_SOAK_SECS`
//! for a longer soak.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use mqlite::mvcc::{
    ChainSnapshot, ReadView, ReadViewRegistry, Ts, VersionData, VersionEntry, VersionState,
};

/// Shared shape mimicking a buffer-pool frame's delta-chain map.
type ChainMap = BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>;

/// Fixture keys — small enough that readers can exercise all of them.
const KEYS: &[&[u8]] = &[b"a", b"b", b"c", b"d"];

/// Number of aged entries per chain initially (above the single head).
const INITIAL_AGED_ENTRIES: usize = 4;

fn ts(ms: u64) -> Ts {
    Ts {
        physical_ms: ms,
        logical: 0,
    }
}

/// Build a chain with one live head (`stop_ts == Ts::MAX`) at `head_ts`
/// plus `aged` older committed entries whose `stop_ts` monotonically
/// increases from `aged_start`.
fn build_chain(head_ts: Ts, aged: usize, aged_start: u64) -> Arc<VecDeque<VersionEntry>> {
    let mut chain = VecDeque::with_capacity(aged + 1);
    chain.push_back(VersionEntry {
        start_ts: head_ts,
        stop_ts: Ts::MAX,
        txn_id: 1,
        state: VersionState::Committed,
        data: VersionData::Inline(b"HEAD".to_vec()),
        is_tombstone: false,
    });
    for i in 0..aged {
        chain.push_back(VersionEntry {
            start_ts: ts(aged_start + i as u64),
            stop_ts: ts(aged_start + i as u64 + 10),
            txn_id: 2 + i as u64,
            state: VersionState::Committed,
            data: VersionData::Inline(format!("v{i}").into_bytes()),
            is_tombstone: false,
        });
    }
    Arc::new(chain)
}

fn seeded_chain_map(head_ts: Ts) -> ChainMap {
    let mut m = BTreeMap::new();
    for k in KEYS {
        m.insert(k.to_vec(), build_chain(head_ts, INITIAL_AGED_ENTRIES, 100));
    }
    m
}

/// Reader step: open a read view, snapshot the shared chain map, assert
/// every key has at least one visible entry (the head is always visible),
/// and release. Running this under concurrent reconciler retain is the
/// S1 race criterion.
///
/// `oracle.load` happens INSIDE the chain-map mutex and right next to
/// `ReadView::open` so no writer can advance the oracle or push a new head
/// between the timestamp snapshot and the registry insert. That closes the
/// "reader preempted between load and register" window that production
/// tolerates via the HLC + history-store fallback (neither of which this
/// shim simulates). The Arc::make_mut / retain vs ChainSnapshot::new race
/// the test is actually validating is unchanged.
fn reader_step(
    chains: &Mutex<ChainMap>,
    registry: &Arc<ReadViewRegistry>,
    oracle: &AtomicU64,
    txn_id: u64,
    mismatches: &AtomicU64,
) {
    let (view, snap) = {
        let guard = chains.lock().unwrap();
        let read_ts = ts(oracle.load(Ordering::Acquire));
        let view = ReadView::open(Arc::clone(registry), read_ts, txn_id);
        let snap = ChainSnapshot::new(&*guard, Some(Arc::clone(&view)));
        (view, snap)
    };
    for k in KEYS {
        if snap.visible_at(k, &view).is_none() {
            mismatches.fetch_add(1, Ordering::Relaxed);
        }
    }
    // View drops → unregisters.
}

/// Reconciler step: under the chain-map mutex, compute ort from the
/// registry (snapshotted BEFORE the mutex — mirror of production), then
/// `Arc::make_mut` + `retain` each chain.
fn reconcile_step(chains: &Mutex<ChainMap>, registry: &Arc<ReadViewRegistry>) {
    // Snapshot ort BEFORE taking the chain-map lock (position 5 before 3).
    let ort = registry.oldest_required_ts();
    let mut guard = chains.lock().unwrap();
    for (_k, chain) in guard.iter_mut() {
        let chain_mut = Arc::make_mut(chain);
        chain_mut.retain(|e| e.stop_ts == Ts::MAX || e.stop_ts > ort);
    }
}

/// Writer step: append a new aged entry (old head gets a concrete
/// stop_ts, a new head is pushed) to simulate ongoing activity. Runs
/// under the chain-map mutex too.
fn writer_step(chains: &Mutex<ChainMap>, now: Ts) {
    let mut guard = chains.lock().unwrap();
    for (_k, chain) in guard.iter_mut() {
        let chain_mut = Arc::make_mut(chain);
        if let Some(head) = chain_mut.front_mut() {
            head.stop_ts = now;
        }
        chain_mut.push_front(VersionEntry {
            start_ts: now,
            stop_ts: Ts::MAX,
            txn_id: now.physical_ms,
            state: VersionState::Committed,
            data: VersionData::Inline(b"HEAD".to_vec()),
            is_tombstone: false,
        });
    }
}

fn soak_duration() -> Duration {
    let secs = std::env::var("MQLITE_RECONCILE_SOAK_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(3);
    Duration::from_secs(secs)
}

#[test]
fn readers_never_observe_missing_version_under_reconcile() {
    let chains = Arc::new(Mutex::new(seeded_chain_map(ts(1))));
    let registry = Arc::new(ReadViewRegistry::new());
    let stop = Arc::new(AtomicBool::new(false));
    let mismatches = Arc::new(AtomicU64::new(0));

    // Shared monotone "oracle" — both readers and writer pull timestamps
    // from this counter, mirroring production's HLC. The writer advances
    // the oracle BEFORE pushing each new head, so a reader's
    // `read_ts = oracle.load()` is always >= the `start_ts` of the chain
    // head that exists at the time the reader registers. Without this,
    // the reader's `read_ts` could drift below every retained chain
    // entry's `start_ts` and report a spurious "missing version" that
    // production's HLC invariant rules out.
    let oracle = Arc::new(AtomicU64::new(1));

    let mut handles = Vec::new();

    // Four reader threads.
    for i in 0..4u64 {
        let chains = Arc::clone(&chains);
        let registry = Arc::clone(&registry);
        let stop = Arc::clone(&stop);
        let mismatches = Arc::clone(&mismatches);
        let oracle = Arc::clone(&oracle);
        handles.push(thread::spawn(move || {
            let mut k: u64 = 0;
            let txn_id_base = 1_000 + i * 10_000;
            while !stop.load(Ordering::Relaxed) {
                reader_step(&chains, &registry, &oracle, txn_id_base + k, &mismatches);
                k = k.wrapping_add(1);
            }
        }));
    }

    // One reconciler thread; also drives a writer step so chain depth
    // stays interesting over time. Order: reconcile → advance oracle →
    // push new head. Advancing the oracle BEFORE pushing means a reader
    // that loaded the new oracle value either sees the previous head
    // (still visible at `read_ts >= prev_start_ts`) or, after the push,
    // sees the new head with `start_ts == read_ts`.
    {
        let chains = Arc::clone(&chains);
        let registry = Arc::clone(&registry);
        let stop = Arc::clone(&stop);
        let oracle = Arc::clone(&oracle);
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                reconcile_step(&chains, &registry);
                let now_ms = oracle.fetch_add(1, Ordering::AcqRel) + 1;
                writer_step(&chains, ts(now_ms));
                // Yield so readers actually get to run.
                thread::yield_now();
            }
        }));
    }

    let start = Instant::now();
    while start.elapsed() < soak_duration() {
        thread::sleep(Duration::from_millis(250));
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        mismatches.load(Ordering::Relaxed),
        0,
        "no reader may observe a missing / mismatched version under \
         reconcile-retain race"
    );
}
