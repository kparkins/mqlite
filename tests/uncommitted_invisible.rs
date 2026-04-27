//! T5' plan-line 792 acceptance test — **S4: uncommitted writes are
//! invisible to concurrent readers**.
//!
//! Contract:
//! - A version chain holds a committed entry v1 at ts=100.
//! - A second writer stages v2 as a `VersionState::Pending` entry
//!   (txn_id=A).
//! - A reader with a different txn_id (B), opened before commit, must
//!   observe v1 (never v2).
//! - Once the writer "commits" by stamping start_ts with a real ts, a
//!   new reader opened at a later read_ts observes v2.
//!
//! This exercises the visibility rule in
//! [`mqlite::mvcc::ChainSnapshot::visible_at`]:
//! - Pending: visible to its own `txn_id`, or to other readers once its
//!   start timestamp is within the pinned sequencer frontier.
//! - Committed: `start_ts <= read_ts < stop_ts`.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use mqlite::mvcc::{ChainSnapshot, ReadView, Ts, VersionData, VersionEntry, VersionState};

const KEY: &[u8] = b"doc/1";
const PENDING_START_TS: Ts = Ts {
    physical_ms: 125,
    logical: 0,
};

fn committed(ts: Ts, stop_ts: Ts, txn_id: u64, bytes: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: ts,
        stop_ts,
        txn_id,
        state: VersionState::Committed,
        data: VersionData::Inline(bytes.to_vec()),
        is_tombstone: false,
    }
}

fn pending(txn_id: u64, bytes: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: PENDING_START_TS,
        stop_ts: Ts::MAX,
        txn_id,
        state: VersionState::Pending { txn_id },
        data: VersionData::Inline(bytes.to_vec()),
        is_tombstone: false,
    }
}

fn build_chain(entries: Vec<VersionEntry>) -> BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> {
    let mut chain = VecDeque::new();
    for e in entries {
        chain.push_back(e);
    }
    let mut source = BTreeMap::new();
    source.insert(KEY.to_vec(), Arc::new(chain));
    source
}

#[test]
fn pending_write_is_invisible_to_concurrent_reader() {
    // Chain state mid-transaction: pending v2 at head, committed v1 below.
    let ts100 = Ts {
        physical_ms: 100,
        logical: 0,
    };
    let source = build_chain(vec![
        pending(/* writer txn A */ 7, b"v2"),
        committed(ts100, Ts::MAX, 6, b"v1"),
    ]);
    let snap = ChainSnapshot::new(&source, None);

    // Reader B with a distinct txn_id opens BEFORE the writer commits.
    let reader_b = ReadView::new(
        Ts {
            physical_ms: 150,
            logical: 0,
        },
        /* reader txn B */ 99,
    );
    let seen = snap.visible_at(KEY, &reader_b).expect("reader must see v1");
    assert_eq!(seen.txn_id, 6, "reader B must see v1's committer");
    match &seen.data {
        VersionData::Inline(v) => assert_eq!(v, b"v1"),
        _ => panic!("expected inline data"),
    }
}

#[test]
fn commit_stamps_pending_and_later_reader_sees_new_value() {
    // Simulate commit: v2 gets a real start_ts, v1's stop_ts advances.
    let ts100 = Ts {
        physical_ms: 100,
        logical: 0,
    };
    let ts200 = Ts {
        physical_ms: 200,
        logical: 0,
    };
    let source = build_chain(vec![
        committed(ts200, Ts::MAX, /* writer A */ 7, b"v2"),
        committed(ts100, ts200, /* old writer */ 6, b"v1"),
    ]);
    let snap = ChainSnapshot::new(&source, None);

    // Reader C opens at a later read_ts >= commit_ts.
    let reader_c = ReadView::new(
        Ts {
            physical_ms: 250,
            logical: 0,
        },
        100,
    );
    let seen = snap
        .visible_at(KEY, &reader_c)
        .expect("reader C must see v2 after commit");
    assert_eq!(seen.txn_id, 7);
    assert_eq!(seen.start_ts, ts200);

    // A reader opened with a read_ts BEFORE the commit still sees v1.
    let reader_old = ReadView::new(
        Ts {
            physical_ms: 150,
            logical: 0,
        },
        101,
    );
    let seen_old = snap
        .visible_at(KEY, &reader_old)
        .expect("old reader must still see v1");
    assert_eq!(seen_old.txn_id, 6);
}
