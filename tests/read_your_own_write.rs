//! T5' plan-line 793 acceptance test — **read-your-own-writes**.
//!
//! Contract:
//! - Within a single WriteTxn, staging an insert then reading the same
//!   key MUST return the just-written value.
//! - Visibility hinge: a PENDING entry (`start_ts == Ts::PENDING`) is
//!   visible iff the caller's `ReadView::txn_id` matches the entry's
//!   `txn_id`.
//! - A reader with a different `txn_id` must NOT see the pending entry.
//! - A committed entry is always visible once `read_ts >= start_ts`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use mqlite::mvcc::{ChainSnapshot, ReadView, Ts, VersionData, VersionEntry};

const KEY: &[u8] = b"order/42";
const WRITER_TXN_ID: u64 = 128;
const OTHER_READER_TXN_ID: u64 = 129;

fn pending_entry(bytes: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: Ts::PENDING,
        stop_ts: Ts::MAX,
        txn_id: WRITER_TXN_ID,
        data: VersionData::Inline(bytes.to_vec()),
        is_tombstone: false,
    }
}

fn snap_with(entries: Vec<VersionEntry>) -> ChainSnapshot {
    let mut chain = VecDeque::new();
    for e in entries {
        chain.push_back(e);
    }
    let mut source = HashMap::new();
    source.insert(KEY.to_vec(), Arc::new(chain));
    ChainSnapshot::new(&source, None)
}

#[test]
fn writer_sees_own_pending_insert() {
    let snap = snap_with(vec![pending_entry(b"new-value")]);

    let writer_view = ReadView::new(
        Ts {
            physical_ms: 500,
            logical: 0,
        },
        WRITER_TXN_ID,
    );
    let seen = snap
        .visible_at(KEY, &writer_view)
        .expect("writer must see its own pending insert");
    assert_eq!(seen.txn_id, WRITER_TXN_ID);
    match &seen.data {
        VersionData::Inline(v) => assert_eq!(v, b"new-value"),
        _ => panic!("expected inline data"),
    }
}

#[test]
fn pending_insert_hidden_from_other_txn() {
    let snap = snap_with(vec![pending_entry(b"new-value")]);

    let other_reader = ReadView::new(
        Ts {
            physical_ms: 500,
            logical: 0,
        },
        OTHER_READER_TXN_ID,
    );
    assert!(
        snap.visible_at(KEY, &other_reader).is_none(),
        "other readers must NOT observe a pending entry belonging to another txn"
    );
}

#[test]
fn writer_sees_own_pending_over_older_committed() {
    // Chain mid-txn: pending head (writer) above a committed prior version.
    let ts100 = Ts {
        physical_ms: 100,
        logical: 0,
    };
    let prior = VersionEntry {
        start_ts: ts100,
        stop_ts: Ts::MAX,
        txn_id: 1,
        data: VersionData::Inline(b"old".to_vec()),
        is_tombstone: false,
    };
    let snap = snap_with(vec![pending_entry(b"new"), prior]);

    // Writer reads its own new value.
    let writer_view = ReadView::new(
        Ts {
            physical_ms: 150,
            logical: 0,
        },
        WRITER_TXN_ID,
    );
    let seen = snap
        .visible_at(KEY, &writer_view)
        .expect("writer sees its pending entry");
    assert_eq!(seen.txn_id, WRITER_TXN_ID);

    // Concurrent reader ignores the pending head and falls through to "old".
    let concurrent = ReadView::new(
        Ts {
            physical_ms: 150,
            logical: 0,
        },
        OTHER_READER_TXN_ID,
    );
    let seen_other = snap
        .visible_at(KEY, &concurrent)
        .expect("concurrent reader sees prior committed version");
    assert_eq!(seen_other.txn_id, 1);
    match &seen_other.data {
        VersionData::Inline(v) => assert_eq!(v, b"old"),
        _ => panic!("expected inline data"),
    }
}
