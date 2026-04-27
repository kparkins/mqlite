use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use crate::mvcc::version::{VersionData, VersionEntry, VersionState};
use crate::storage::root_snapshot::{PublishedCatalog, PublishedEpoch};

use super::{ChainSnapshot, ReadView};

const KEY: &[u8] = b"k";
const WRITER_TXN_ID: u64 = 11;
const OTHER_TXN_ID: u64 = 22;

fn ts(physical_ms: u64) -> crate::mvcc::Ts {
    crate::mvcc::Ts {
        physical_ms,
        logical: 0,
    }
}

fn empty_catalog() -> Arc<PublishedCatalog> {
    Arc::new(PublishedCatalog {
        namespaces: HashMap::new(),
        namespace_id_by_name: HashMap::new(),
    })
}

fn view(read_ts: crate::mvcc::Ts, txn_id: u64, frontier: crate::mvcc::Ts) -> ReadView {
    ReadView::new_for_epoch(
        Arc::new(PublishedEpoch {
            visible_ts: read_ts,
            catalog: empty_catalog(),
            catalog_generation: 1,
            sequencer_frontier: frontier,
        }),
        txn_id,
    )
}

fn entry(
    start_ts: crate::mvcc::Ts,
    stop_ts: crate::mvcc::Ts,
    txn_id: u64,
    state: VersionState,
    value: &[u8],
) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts,
        txn_id,
        state,
        data: VersionData::Inline(value.to_vec()),
        is_tombstone: false,
    }
}

fn snapshot(entries: Vec<VersionEntry>) -> ChainSnapshot {
    let mut chain = std::collections::VecDeque::new();
    for entry in entries {
        chain.push_back(entry);
    }
    let mut source = BTreeMap::new();
    source.insert(KEY.to_vec(), Arc::new(chain));
    ChainSnapshot::new(&source, None)
}

#[test]
fn pending_visibility_uses_state_and_pinned_frontier() {
    let pending_start = ts(200);
    let snap = snapshot(vec![
        entry(
            pending_start,
            crate::mvcc::Ts::MAX,
            WRITER_TXN_ID,
            VersionState::Pending {
                txn_id: WRITER_TXN_ID,
            },
            b"pending",
        ),
        entry(
            ts(100),
            crate::mvcc::Ts::MAX,
            1,
            VersionState::Committed,
            b"committed",
        ),
    ]);

    let writer = view(ts(250), WRITER_TXN_ID, ts(0));
    let seen = snap
        .visible_at(KEY, &writer)
        .expect("writer sees own pending entry");
    assert_eq!(seen.txn_id, WRITER_TXN_ID);

    let foreign_before_frontier = view(ts(250), OTHER_TXN_ID, ts(199));
    let seen = snap
        .visible_at(KEY, &foreign_before_frontier)
        .expect("foreign reader falls through to committed entry");
    assert_eq!(seen.txn_id, 1);

    let foreign_after_frontier = view(ts(250), OTHER_TXN_ID, pending_start);
    let seen = snap
        .visible_at(KEY, &foreign_after_frontier)
        .expect("frontier-published pending entry is visible");
    assert_eq!(seen.txn_id, WRITER_TXN_ID);
}

#[test]
fn foreign_pending_visibility_still_honors_start_stop_window() {
    let snap = snapshot(vec![
        entry(
            ts(200),
            crate::mvcc::Ts::MAX,
            WRITER_TXN_ID,
            VersionState::Pending {
                txn_id: WRITER_TXN_ID,
            },
            b"pending",
        ),
        entry(
            ts(100),
            crate::mvcc::Ts::MAX,
            1,
            VersionState::Committed,
            b"committed",
        ),
    ]);

    let writer_before_start = view(ts(150), OTHER_TXN_ID, crate::mvcc::Ts::MAX);
    let seen = snap
        .visible_at(KEY, &writer_before_start)
        .expect("foreign reader sees prior committed entry before pending start_ts");
    assert_eq!(seen.txn_id, 1);
}

#[test]
fn visible_range_reuses_visible_at_predicate() {
    let snap = snapshot(vec![
        entry(
            ts(200),
            crate::mvcc::Ts::MAX,
            WRITER_TXN_ID,
            VersionState::Pending {
                txn_id: WRITER_TXN_ID,
            },
            b"pending",
        ),
        entry(
            ts(100),
            crate::mvcc::Ts::MAX,
            1,
            VersionState::Committed,
            b"committed",
        ),
    ]);
    let reader = view(ts(250), OTHER_TXN_ID, ts(199));

    let at = snap.visible_at(KEY, &reader).map(|entry| entry.txn_id);
    let range = snap
        .visible_range(
            std::ops::Bound::Included(KEY),
            std::ops::Bound::Included(KEY),
            &reader,
        )
        .next()
        .map(|(_, entry)| entry.txn_id);

    assert_eq!(range, at);
}
