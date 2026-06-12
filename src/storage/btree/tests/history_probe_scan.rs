use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::{ReadView, Ts, VersionData, VersionEntry, VersionState};
use crate::storage::buffer_pool::LatchMode;

use super::{BTree, BTreePageStore, HistoryProbe, MemPageStore};

fn ts(physical_ms: u64) -> Ts {
    Ts {
        physical_ms,
        logical: 0,
    }
}

fn version(start_ts: Ts, stop_ts: Ts, value: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts,
        txn_id: 1,
        state: VersionState::Committed,
        data: VersionData::Inline(value.to_vec()),
        is_tombstone: false,
    }
}

fn install_chain(
    tree: &mut BTree<MemPageStore>,
    key: &[u8],
    entries: Vec<VersionEntry>,
) -> Result<()> {
    let leaf = tree.find_leaf(key)?;
    let mut chain = VecDeque::new();
    for entry in entries {
        chain.push_back(entry);
    }
    tree.store
        .with_chain_under_latch(leaf, key, LatchMode::Exclusive, |slot| {
            *slot = Some(Arc::new(chain));
        })
}

struct RecordingHistoryProbe {
    calls: RefCell<Vec<Vec<u8>>>,
}

impl RecordingHistoryProbe {
    fn new() -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
        }
    }
}

impl HistoryProbe for RecordingHistoryProbe {
    fn probe_visible_version(&self, key: &[u8], _read_ts: Ts) -> Result<Option<VersionEntry>> {
        self.calls.borrow_mut().push(key.to_vec());
        Ok(None)
    }
}

#[test]
fn test_merged_scan_history_probe_fires_when_predicate_matches() {
    let mut tree = BTree::create(MemPageStore::new()).unwrap();
    for key in [
        b"a".as_slice(),
        b"b".as_slice(),
        b"c".as_slice(),
        b"d".as_slice(),
    ] {
        tree.insert(key, b"base").unwrap();
    }

    install_chain(
        &mut tree,
        b"b",
        vec![version(ts(50), ts(80), b"too-old-for-reader")],
    )
    .unwrap();
    install_chain(
        &mut tree,
        b"c",
        vec![version(ts(150), Ts::MAX, b"newer-than-reader")],
    )
    .unwrap();
    install_chain(
        &mut tree,
        b"d",
        vec![version(ts(90), Ts::MAX, b"visible-to-reader")],
    )
    .unwrap();

    let view = ReadView::new_frontier_pinned_for_tests(ts(100), 99);
    let history = RecordingHistoryProbe::new();
    let _rows = tree
        .range_scan_mvcc(None, None, &view, Some(&history))
        .unwrap();

    assert_eq!(
        history.calls.borrow().as_slice(),
        &[b"a".to_vec(), b"c".to_vec()]
    );
}
