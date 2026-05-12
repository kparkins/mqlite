//! Phase 3 US-016 split-routing tests.
//!
//! Tests 17 and 18 are moved to Phase 4 per
//! `docs/STORAGE-UPGRADE-PHASE-03-ORDERED-LIVE-DELTAS.md` Section 10.12.4.
//! This module covers only Phase 3 tests 15 and 16.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::buffer_pool::LatchMode;

use super::node::LeafNode;
use super::{BTree, BTreePageStore, MemPageStore};

const SPLIT_VALUE_BYTES: usize = 6_000;
const BASE_KEYS_BEFORE_SPLIT: [u64; 4] = [10, 20, 30, 40];
const BASE_KEYS_TO_FORCE_SPLIT: [u64; 2] = [50, 60];
const LEFT_BASE_BACKED_KEY: u64 = 20;
const PROMOTED_BASE_KEY: u64 = 40;
const LEFT_DELTA_ONLY_KEY: u64 = 25;
const RIGHT_DELTA_ONLY_KEY: u64 = 55;
const LIVE_MARKER: u8 = 0xA5;
const TOMBSTONE_MARKER: u8 = 0x5A;

fn key(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

fn entry(marker: u8, is_tombstone: bool) -> VersionEntry {
    VersionEntry {
        start_ts: Ts {
            physical_ms: marker as u64,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id: marker as u64,
        state: VersionState::Committed,
        data: VersionData::Inline(if is_tombstone {
            Vec::new()
        } else {
            vec![marker]
        }),
        is_tombstone,
    }
}

fn chain(marker: u8, is_tombstone: bool) -> Arc<VecDeque<VersionEntry>> {
    Arc::new([entry(marker, is_tombstone)].into_iter().collect())
}

fn tree_with_pre_split_chains(
    chains: impl IntoIterator<Item = (u64, Arc<VecDeque<VersionEntry>>)>,
) -> Result<BTree<MemPageStore>> {
    let mut tree = BTree::create(MemPageStore::new())?;
    let value = vec![0xCC; SPLIT_VALUE_BYTES];
    for n in BASE_KEYS_BEFORE_SPLIT {
        tree.insert(&key(n), &value)?;
    }

    let left_page = tree.root_page;
    for (raw_key, chain) in chains {
        let encoded = key(raw_key);
        tree.store
            .with_chain_under_latch(left_page, &encoded, LatchMode::Exclusive, |slot| {
                *slot = Some(chain);
            })?;
    }

    for n in BASE_KEYS_TO_FORCE_SPLIT {
        tree.insert(&key(n), &value)?;
    }
    assert_eq!(tree.root_level, 1, "test setup must force a leaf split");
    Ok(tree)
}

fn split_leaf_pages(tree: &BTree<MemPageStore>) -> Result<(u32, u32)> {
    let left_page = tree.leftmost_leaf()?;
    let (buf, _) = tree.store.read_leaf(left_page)?;
    let right_page = LeafNode::parse(&buf[..])?.next_leaf_page;
    assert_ne!(right_page, 0, "split should create a right sibling");
    Ok((left_page, right_page))
}

fn assert_chain_routed(
    tree: &mut BTree<MemPageStore>,
    raw_key: u64,
    expected_page: u32,
    other_page: u32,
    is_tombstone: bool,
) -> Result<()> {
    let encoded_key = key(raw_key);
    let other_chain = tree.store.with_chain_under_latch(
        other_page,
        &encoded_key,
        LatchMode::Exclusive,
        |slot| slot.take(),
    )?;
    assert!(
        other_chain.is_none(),
        "key {raw_key} should not be routed to the other split sibling"
    );
    let chain = tree
        .store
        .with_chain_under_latch(expected_page, &encoded_key, LatchMode::Exclusive, |slot| {
            slot.take()
        })?
        .expect("chain should be routed to the expected split sibling");
    assert_eq!(chain.len(), 1);
    assert_eq!(chain[0].is_tombstone, is_tombstone);
    Ok(())
}

#[test]
fn test_split_moves_delta_only_key_to_right_sibling() -> Result<()> {
    let mut tree = tree_with_pre_split_chains([
        (LEFT_BASE_BACKED_KEY, chain(LIVE_MARKER, false)),
        (PROMOTED_BASE_KEY, chain(LIVE_MARKER, false)),
        (LEFT_DELTA_ONLY_KEY, chain(LIVE_MARKER, false)),
        (RIGHT_DELTA_ONLY_KEY, chain(LIVE_MARKER, false)),
    ])?;
    let (left_page, right_page) = split_leaf_pages(&tree)?;

    assert_chain_routed(
        &mut tree,
        LEFT_BASE_BACKED_KEY,
        left_page,
        right_page,
        false,
    )?;
    assert_chain_routed(&mut tree, PROMOTED_BASE_KEY, right_page, left_page, false)?;
    assert_chain_routed(&mut tree, LEFT_DELTA_ONLY_KEY, left_page, right_page, false)?;
    assert_chain_routed(
        &mut tree,
        RIGHT_DELTA_ONLY_KEY,
        right_page,
        left_page,
        false,
    )?;
    Ok(())
}

#[test]
fn test_split_preserves_delta_only_tombstone_routing() -> Result<()> {
    let mut tree = tree_with_pre_split_chains([
        (LEFT_BASE_BACKED_KEY, chain(TOMBSTONE_MARKER, true)),
        (PROMOTED_BASE_KEY, chain(TOMBSTONE_MARKER, true)),
        (LEFT_DELTA_ONLY_KEY, chain(TOMBSTONE_MARKER, true)),
        (RIGHT_DELTA_ONLY_KEY, chain(TOMBSTONE_MARKER, true)),
    ])?;
    let (left_page, right_page) = split_leaf_pages(&tree)?;

    assert_chain_routed(&mut tree, LEFT_BASE_BACKED_KEY, left_page, right_page, true)?;
    assert_chain_routed(&mut tree, PROMOTED_BASE_KEY, right_page, left_page, true)?;
    assert_chain_routed(&mut tree, LEFT_DELTA_ONLY_KEY, left_page, right_page, true)?;
    assert_chain_routed(&mut tree, RIGHT_DELTA_ONLY_KEY, right_page, left_page, true)?;
    Ok(())
}
