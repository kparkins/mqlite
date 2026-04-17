//! T5' plan-line 800 acceptance test — **sec-index tombstone elision counter**.
//!
//! Contract:
//! - When an index probe finds a tombstone in the version chain for a
//!   sec-index key visible at `read_ts`, the probe MUST skip the primary
//!   fetch (avoiding a dangling-reference read) and the counter
//!   `mvcc.secondary_index.tombstone_hits_skipped_total` MUST tick.
//! - When the sec-index chain yields a live (non-tombstone) entry, the
//!   counter MUST NOT tick.
//!
//! T5' lands the single counter; T8 formalises all 12 mandatory + 5
//! diagnostic counters. This test exercises the public primitives
//! exposed on the `mqlite::mvcc` module.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use mqlite::mvcc::{
    record_secondary_index_tombstone_hit, reset_secondary_index_tombstone_hits,
    secondary_index_tombstone_hits_snapshot, ChainSnapshot, ReadView, Ts, VersionData,
    VersionEntry,
};

// The tombstone counter is a process-global atomic. Tests in this file
// run in parallel by default, so we serialize their counter probes with
// a file-local mutex to avoid cross-test contamination.
fn counter_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

const SEC_KEY: &[u8] = b"idx/status/active/pk-1";

fn entry(start: Ts, stop: Ts, txn_id: u64, bytes: &[u8], tombstone: bool) -> VersionEntry {
    VersionEntry {
        start_ts: start,
        stop_ts: stop,
        txn_id,
        data: VersionData::Inline(bytes.to_vec()),
        is_tombstone: tombstone,
    }
}

fn snap_with(chain_entries: Vec<VersionEntry>) -> ChainSnapshot {
    let mut chain = VecDeque::new();
    for e in chain_entries {
        chain.push_back(e);
    }
    let mut source = HashMap::new();
    source.insert(SEC_KEY.to_vec(), Arc::new(chain));
    ChainSnapshot::new(&source, None)
}

/// Model of the sec-index probe: mimics the production loop that looks
/// up a sec-index key, inspects the visible VersionEntry, and either
/// dereferences the primary or records the tombstone elision.
fn sec_index_probe(snap: &ChainSnapshot, view: &ReadView) -> Option<Vec<u8>> {
    let entry = snap.visible_at(SEC_KEY, view)?;
    if entry.is_tombstone {
        record_secondary_index_tombstone_hit();
        return None;
    }
    match &entry.data {
        VersionData::Inline(v) => Some(v.clone()),
        _ => panic!("test only uses inline data"),
    }
}

#[test]
fn tombstone_elision_ticks_counter() {
    let _g = counter_lock();
    reset_secondary_index_tombstone_hits();
    let before = secondary_index_tombstone_hits_snapshot();

    // Chain head is a tombstone committed at ts=200.
    let snap = snap_with(vec![
        entry(Ts { physical_ms: 200, logical: 0 }, Ts::MAX, 5, b"", true),
        entry(
            Ts { physical_ms: 100, logical: 0 },
            Ts { physical_ms: 200, logical: 0 },
            4,
            b"pk-1",
            false,
        ),
    ]);

    // Reader at ts>=200 sees the tombstone and elides the primary fetch.
    let reader = ReadView::new(Ts { physical_ms: 300, logical: 0 }, 99);
    let result = sec_index_probe(&snap, &reader);
    assert!(result.is_none(), "tombstone elision must skip the primary fetch");

    let after = secondary_index_tombstone_hits_snapshot();
    assert_eq!(
        after, before + 1,
        "tombstone hit must tick the counter"
    );
}

#[test]
fn live_sec_entry_does_not_tick_counter() {
    let _g = counter_lock();
    reset_secondary_index_tombstone_hits();
    let before = secondary_index_tombstone_hits_snapshot();

    // Chain head is a live (non-tombstone) entry.
    let snap = snap_with(vec![entry(
        Ts { physical_ms: 100, logical: 0 },
        Ts::MAX,
        4,
        b"pk-7",
        false,
    )]);

    let reader = ReadView::new(Ts { physical_ms: 150, logical: 0 }, 99);
    let result = sec_index_probe(&snap, &reader);
    assert_eq!(result.as_deref(), Some(&b"pk-7"[..]));

    let after = secondary_index_tombstone_hits_snapshot();
    assert_eq!(after, before, "live entry must not tick the counter");
}

#[test]
fn reader_below_tombstone_ts_sees_live_entry_and_no_tick() {
    let _g = counter_lock();
    reset_secondary_index_tombstone_hits();
    let before = secondary_index_tombstone_hits_snapshot();

    let snap = snap_with(vec![
        entry(Ts { physical_ms: 200, logical: 0 }, Ts::MAX, 5, b"", true),
        entry(
            Ts { physical_ms: 100, logical: 0 },
            Ts { physical_ms: 200, logical: 0 },
            4,
            b"pk-1",
            false,
        ),
    ]);

    // Reader at ts between 100 and 200 sees the live entry, not the tombstone.
    let reader = ReadView::new(Ts { physical_ms: 150, logical: 0 }, 99);
    let result = sec_index_probe(&snap, &reader);
    assert_eq!(result.as_deref(), Some(&b"pk-1"[..]));

    let after = secondary_index_tombstone_hits_snapshot();
    assert_eq!(after, before, "live-at-this-ts entry must not tick tombstone counter");
}
