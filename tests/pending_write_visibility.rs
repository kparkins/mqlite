//! US-020 public test host for Pending visibility before publish.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use mqlite::mvcc::{ChainSnapshot, ReadView, Ts, VersionData, VersionEntry, VersionState};

const KEY: &[u8] = b"phase3-us020-pending";
const PAYLOAD: &[u8] = b"writer-pending-payload";
const WRITER_TXN_ID: u64 = 52;
const FOREIGN_TXN_ID: u64 = 53;
const READ_BEFORE_PENDING: Ts = Ts {
    physical_ms: 199,
    logical: 0,
};
const READ_AFTER_PENDING: Ts = Ts {
    physical_ms: 201,
    logical: 0,
};
const PENDING_START: Ts = Ts {
    physical_ms: 200,
    logical: 0,
};

#[test]
fn test_writer_read_sees_own_pending_before_publish() {
    let pending = VersionEntry {
        start_ts: PENDING_START,
        stop_ts: Ts::MAX,
        txn_id: WRITER_TXN_ID,
        state: VersionState::Pending {
            txn_id: WRITER_TXN_ID,
        },
        data: VersionData::Inline(PAYLOAD.to_vec()),
        is_tombstone: false,
    };

    let mut source = BTreeMap::new();
    source.insert(KEY.to_vec(), Arc::new(VecDeque::from([pending])));

    let snapshot = ChainSnapshot::new(&source, None);
    assert_eq!(
        snapshot.chain_len(KEY),
        1,
        "US-020 relies on clone-all before visibility filtering"
    );

    let writer_view = ReadView::new_frontier_pinned_for_tests(READ_BEFORE_PENDING, WRITER_TXN_ID);
    let writer_entry = snapshot
        .visible_at(KEY, &writer_view)
        .expect("writer must see its own Pending entry before publish");
    match writer_entry.state {
        VersionState::Pending { txn_id } => assert_eq!(txn_id, WRITER_TXN_ID),
        VersionState::Committed | VersionState::Aborted => {
            panic!("writer must observe the Pending entry")
        }
    }
    match &writer_entry.data {
        VersionData::Inline(payload) => assert_eq!(payload.as_slice(), PAYLOAD),
        VersionData::Overflow(_) => panic!("US-020 fixture must keep payload inline"),
    }

    let foreign_view = ReadView::new_frontier_pinned_for_tests(READ_AFTER_PENDING, FOREIGN_TXN_ID);
    assert!(
        snapshot.visible_at(KEY, &foreign_view).is_none(),
        "foreign Pending must stay hidden until sequencer_frontier reaches start_ts"
    );
}
