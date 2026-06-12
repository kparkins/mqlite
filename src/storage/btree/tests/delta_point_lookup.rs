use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::{ReadView, Ts, VersionData, VersionEntry, VersionState};
use crate::storage::buffer_pool::LatchMode;

use super::{BTree, BTreePageStore, HistoryProbe, MemPageStore};

const TXN_ID: u64 = 1;
const READER_TXN_ID: u64 = 99;
const T_CREATE: Ts = Ts {
    physical_ms: 100,
    logical: 0,
};
const T_DELETE: Ts = Ts {
    physical_ms: 200,
    logical: 0,
};
const T_AFTER_DELETE: Ts = Ts {
    physical_ms: 250,
    logical: 0,
};
const T_BEFORE_CREATE: Ts = Ts {
    physical_ms: 50,
    logical: 0,
};
const T_BEFORE_DELETE: Ts = Ts {
    physical_ms: 150,
    logical: 0,
};
const T_FUTURE_UPDATE: Ts = Ts {
    physical_ms: 300,
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

fn remove_chain(tree: &mut BTree<MemPageStore>, key: &[u8]) -> Result<()> {
    let leaf = tree.find_leaf(key)?;
    tree.store
        .with_chain_under_latch(leaf, key, LatchMode::Exclusive, |slot| {
            slot.take();
        })
}

struct EmptyHistoryProbe;

impl HistoryProbe for EmptyHistoryProbe {
    fn probe_visible_version(&self, _key: &[u8], _read_ts: Ts) -> Result<Option<VersionEntry>> {
        Ok(None)
    }
}

#[test]
fn test_delta_only_point_lookup_returns_visible_value() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    install_chain(
        &mut tree,
        b"delta-only",
        [live_entry(T_CREATE, Ts::MAX, b"delta-value")],
    )?;

    let view = ReadView::new_frontier_pinned_for_tests(T_AFTER_DELETE, READER_TXN_ID);

    assert_eq!(
        tree.get_mvcc(b"delta-only", &view, None)?.as_deref(),
        Some(b"delta-value".as_slice())
    );
    Ok(())
}

#[test]
fn test_point_lookup_falls_back_to_base_after_future_chain_miss() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    tree.insert(b"base-key", b"base-value")?;
    install_chain(
        &mut tree,
        b"base-key",
        [live_entry(T_FUTURE_UPDATE, Ts::MAX, b"future-value")],
    )?;
    let history = EmptyHistoryProbe;
    let view = ReadView::new_frontier_pinned_for_tests(T_BEFORE_CREATE, READER_TXN_ID);

    assert_eq!(
        tree.get_mvcc(b"base-key", &view, Some(&history))?
            .as_deref(),
        Some(b"base-value".as_slice())
    );
    Ok(())
}

#[test]
fn test_delta_only_tombstone_visible_after_delete_ts() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    install_chain(
        &mut tree,
        b"deleted-key",
        [
            tombstone_entry(T_DELETE, Ts::MAX),
            live_entry(T_CREATE, T_DELETE, b"old-value"),
        ],
    )?;
    let view = ReadView::new_frontier_pinned_for_tests(T_AFTER_DELETE, READER_TXN_ID);

    assert_eq!(tree.get_mvcc(b"deleted-key", &view, None)?, None);
    Ok(())
}

#[test]
fn test_delta_only_tombstone_preserves_pre_delete_view() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    install_chain(
        &mut tree,
        b"deleted-key",
        [
            tombstone_entry(T_DELETE, Ts::MAX),
            live_entry(T_CREATE, T_DELETE, b"old-value"),
        ],
    )?;
    let view = ReadView::new_frontier_pinned_for_tests(T_BEFORE_DELETE, READER_TXN_ID);

    assert_eq!(
        tree.get_mvcc(b"deleted-key", &view, None)?.as_deref(),
        Some(b"old-value".as_slice())
    );
    Ok(())
}

#[test]
fn test_delta_only_tombstone_boundary_read_ts_equals_delete_ts() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    install_chain(
        &mut tree,
        b"deleted-key",
        [
            tombstone_entry(T_DELETE, Ts::MAX),
            live_entry(T_CREATE, T_DELETE, b"old-value"),
        ],
    )?;
    let view = ReadView::new_frontier_pinned_for_tests(T_DELETE, READER_TXN_ID);

    assert_eq!(tree.get_mvcc(b"deleted-key", &view, None)?, None);
    Ok(())
}

#[test]
fn test_delta_only_tombstone_after_reconcile_prunes_chain() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    install_chain(
        &mut tree,
        b"deleted-key",
        [tombstone_entry(T_DELETE, T_AFTER_DELETE)],
    )?;
    remove_chain(&mut tree, b"deleted-key")?;
    let history = EmptyHistoryProbe;
    let view = ReadView::new_frontier_pinned_for_tests(T_AFTER_DELETE, READER_TXN_ID);

    assert_eq!(tree.get_mvcc(b"deleted-key", &view, Some(&history))?, None);
    Ok(())
}
