use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::{ReadView, Ts, VersionData, VersionEntry, VersionState};
use crate::storage::buffer_pool::LatchMode;

use super::{BTree, BTreePageStore, HistoryProbe, MemPageStore};

const TXN_ID: u64 = 1;
const READER_TXN_ID: u64 = 99;
const T_VISIBLE: Ts = Ts {
    physical_ms: 100,
    logical: 0,
};
const T_READ: Ts = Ts {
    physical_ms: 150,
    logical: 0,
};
const T_FUTURE: Ts = Ts {
    physical_ms: 200,
    logical: 0,
};

fn live_entry(start_ts: Ts, stop_ts: Ts, value: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts,
        txn_id: TXN_ID,
        state: VersionState::Committed,
        data: VersionData::Inline(value.to_vec()),
        is_tombstone: false,
    }
}

fn tombstone_entry(start_ts: Ts, stop_ts: Ts) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts,
        txn_id: TXN_ID,
        state: VersionState::Committed,
        data: VersionData::Inline(Vec::new()),
        is_tombstone: true,
    }
}

fn install_chain(
    tree: &mut BTree<MemPageStore>,
    key: &[u8],
    entries: impl IntoIterator<Item = VersionEntry>,
) -> Result<()> {
    let leaf = tree.find_leaf(key)?;
    let chain = entries.into_iter().collect::<VecDeque<_>>();
    tree.store
        .with_chain_under_latch(leaf, key, LatchMode::Exclusive, |slot| {
            *slot = Some(Arc::new(chain));
        })
}

struct StaticHistoryProbe {
    calls: RefCell<Vec<Vec<u8>>>,
    entry: Option<VersionEntry>,
}

impl StaticHistoryProbe {
    fn returning(entry: Option<VersionEntry>) -> Self {
        Self {
            calls: RefCell::new(Vec::new()),
            entry,
        }
    }

    fn calls(&self) -> Vec<Vec<u8>> {
        self.calls.borrow().clone()
    }
}

impl HistoryProbe for StaticHistoryProbe {
    fn probe_visible_version(&self, key: &[u8], _read_ts: Ts) -> Result<Option<VersionEntry>> {
        self.calls.borrow_mut().push(key.to_vec());
        Ok(self.entry.clone())
    }
}

#[test]
fn test_read_fallthrough_uses_history_when_resident_chain_misses() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    tree.insert(b"doc", b"base-value")?;
    install_chain(
        &mut tree,
        b"doc",
        [live_entry(T_FUTURE, Ts::MAX, b"future-resident")],
    )?;

    let history =
        StaticHistoryProbe::returning(Some(live_entry(T_VISIBLE, T_FUTURE, b"history-value")));
    let view = ReadView::new_frontier_pinned_for_tests(T_READ, READER_TXN_ID);

    assert_eq!(
        tree.get_mvcc(b"doc", &view, Some(&history))?.as_deref(),
        Some(b"history-value".as_slice())
    );
    assert_eq!(history.calls(), vec![b"doc".to_vec()]);
    Ok(())
}

#[test]
fn test_resident_visible_chain_value_wins_without_history_probe() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    tree.insert(b"doc", b"base-value")?;
    install_chain(
        &mut tree,
        b"doc",
        [live_entry(T_VISIBLE, Ts::MAX, b"resident-value")],
    )?;

    let history =
        StaticHistoryProbe::returning(Some(live_entry(T_VISIBLE, Ts::MAX, b"history-value")));
    let view = ReadView::new_frontier_pinned_for_tests(T_READ, READER_TXN_ID);

    assert_eq!(
        tree.get_mvcc(b"doc", &view, Some(&history))?.as_deref(),
        Some(b"resident-value".as_slice())
    );
    assert!(history.calls().is_empty());
    Ok(())
}

#[test]
fn test_history_tombstone_hides_base_after_chain_miss() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    tree.insert(b"doc", b"base-value")?;

    let history = StaticHistoryProbe::returning(Some(tombstone_entry(T_VISIBLE, Ts::MAX)));
    let view = ReadView::new_frontier_pinned_for_tests(T_READ, READER_TXN_ID);

    assert!(tree.get_mvcc(b"doc", &view, Some(&history))?.is_none());
    assert_eq!(history.calls(), vec![b"doc".to_vec()]);
    Ok(())
}
