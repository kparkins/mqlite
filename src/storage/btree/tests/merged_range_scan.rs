use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::{ReadView, Ts, VersionData, VersionEntry, VersionState};
use crate::storage::buffer_pool::LatchMode;

use super::{BTree, BTreePageStore, MemPageStore};

const TXN_ID: u64 = 1;
const READER_TXN_ID: u64 = 99;
const READ_TS: Ts = Ts {
    physical_ms: 200,
    logical: 0,
};
const VERSION_START_TS: Ts = Ts {
    physical_ms: 100,
    logical: 0,
};
const BASE_SPLIT_VALUE_BYTES: usize = 200;
const BASE_SPLIT_KEYS: u64 = 160;
const RANGE_START_KEY: u64 = 70;
const RIGHT_DELTA_KEY: u64 = 170;

fn key(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

fn live_entry(value: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: VERSION_START_TS,
        stop_ts: Ts::MAX,
        txn_id: TXN_ID,
        state: VersionState::Committed,
        data: VersionData::Inline(value.to_vec()),
        is_tombstone: false,
    }
}

fn tombstone_entry() -> VersionEntry {
    VersionEntry {
        start_ts: VERSION_START_TS,
        stop_ts: Ts::MAX,
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

fn read_view() -> ReadView {
    ReadView::new_frontier_pinned_for_tests(READ_TS, READER_TXN_ID)
}

fn scan_keys(rows: &[(Vec<u8>, Vec<u8>)]) -> Vec<Vec<u8>> {
    rows.iter().map(|(key, _)| key.clone()).collect()
}

#[test]
fn test_merged_scan_interleaves_base_and_delta_keys() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    for (key, value) in [(b"b", b"base-b"), (b"d", b"base-d"), (b"f", b"base-f")] {
        tree.insert(key.as_slice(), value.as_slice())?;
    }
    for (key, value) in [
        (b"a", b"delta-a"),
        (b"c", b"delta-c"),
        (b"e", b"delta-e"),
        (b"g", b"delta-g"),
    ] {
        install_chain(&mut tree, key.as_slice(), [live_entry(value.as_slice())])?;
    }

    let rows = tree.range_scan_mvcc(None, None, &read_view(), None)?;

    assert_eq!(
        scan_keys(&rows),
        [b"a", b"b", b"c", b"d", b"e", b"f", b"g"]
            .into_iter()
            .map(|key| key.to_vec())
            .collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn test_merged_scan_chain_wins_over_base_cell() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    tree.insert(b"k", b"base-value")?;
    install_chain(&mut tree, b"k", [live_entry(b"delta-value")])?;

    let rows = tree.range_scan_mvcc(None, None, &read_view(), None)?;

    assert_eq!(rows, vec![(b"k".to_vec(), b"delta-value".to_vec())]);
    Ok(())
}

#[test]
fn test_merged_scan_tombstone_over_delta_only_skips() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    install_chain(&mut tree, b"k", [tombstone_entry()])?;

    let rows = tree.range_scan_mvcc(None, None, &read_view(), None)?;

    assert!(rows.is_empty());
    Ok(())
}

#[test]
fn test_merged_scan_tombstone_over_base_cell_skips() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    tree.insert(b"k", b"base-value")?;
    install_chain(&mut tree, b"k", [tombstone_entry()])?;

    let rows = tree.range_scan_mvcc(None, None, &read_view(), None)?;

    assert!(rows.is_empty());
    Ok(())
}

#[test]
fn test_merged_scan_across_leaf_boundary_one_side_delta_only() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    let value = vec![0u8; BASE_SPLIT_VALUE_BYTES];
    for n in 0..BASE_SPLIT_KEYS {
        tree.insert(&key(n), &value)?;
    }
    install_chain(
        &mut tree,
        &key(RIGHT_DELTA_KEY),
        [live_entry(b"right-delta")],
    )?;

    let rows = tree.range_scan_mvcc(
        Some(&key(RANGE_START_KEY)),
        Some(&key(RIGHT_DELTA_KEY)),
        &read_view(),
        None,
    )?;
    let mut expected = (RANGE_START_KEY..BASE_SPLIT_KEYS)
        .map(key)
        .collect::<Vec<_>>();
    expected.push(key(RIGHT_DELTA_KEY));

    assert_eq!(scan_keys(&rows), expected);
    Ok(())
}

#[test]
fn test_range_scan_mvcc_bounded_excluded_end() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    tree.insert(b"b", b"base-b")?;
    tree.insert(b"d", b"base-d")?;
    install_chain(&mut tree, b"c", [live_entry(b"delta-c")])?;
    install_chain(&mut tree, b"d", [live_entry(b"delta-d")])?;

    let rows = tree.range_scan_mvcc_bounded(
        Bound::Included(b"b".as_slice()),
        Bound::Excluded(b"d".as_slice()),
        &read_view(),
        None,
    )?;

    assert_eq!(scan_keys(&rows), vec![b"b".to_vec(), b"c".to_vec()]);
    Ok(())
}

#[test]
fn test_range_scan_mvcc_bounded_included_end() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    tree.insert(b"b", b"base-b")?;
    tree.insert(b"d", b"base-d")?;
    install_chain(&mut tree, b"d", [live_entry(b"delta-d")])?;

    let rows = tree.range_scan_mvcc_bounded(
        Bound::Included(b"b".as_slice()),
        Bound::Included(b"d".as_slice()),
        &read_view(),
        None,
    )?;

    assert_eq!(
        rows,
        vec![
            (b"b".to_vec(), b"base-b".to_vec()),
            (b"d".to_vec(), b"delta-d".to_vec())
        ]
    );
    Ok(())
}

#[test]
fn test_unique_prefix_scan_does_not_match_synthetic_end_sentinel() -> Result<()> {
    let mut tree = BTree::create(MemPageStore::new())?;
    let start = b"email:a\x01".to_vec();
    let matching_key = b"email:a\x01doc".to_vec();
    let end_sentinel = b"email:a\x02".to_vec();

    install_chain(&mut tree, &matching_key, [live_entry(b"match")])?;
    tree.insert(&end_sentinel, b"sentinel-base")?;
    install_chain(&mut tree, &end_sentinel, [live_entry(b"sentinel-delta")])?;

    let rows = tree.range_scan_mvcc_bounded(
        Bound::Included(start.as_slice()),
        Bound::Excluded(end_sentinel.as_slice()),
        &read_view(),
        None,
    )?;

    assert_eq!(rows, vec![(matching_key, b"match".to_vec())]);
    Ok(())
}
