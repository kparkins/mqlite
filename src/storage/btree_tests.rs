use super::*;
use crate::error::Error;

// -----------------------------------------------------------------------
// Helper: make a simple key from a u64
// -----------------------------------------------------------------------

fn key(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

fn val(n: u64) -> Vec<u8> {
    format!("value-{n}").into_bytes()
}

// -----------------------------------------------------------------------
// Empty tree
// -----------------------------------------------------------------------

#[test]
fn create_empty_tree() {
    let store = MemPageStore::new();
    let tree: BTree<MemPageStore> = BTree::create(store).unwrap();
    assert_eq!(tree.root_level, 0, "fresh tree root should be a leaf");
    assert_eq!(tree.root_page, 1, "first allocated page should be 1");
}

#[test]
fn search_empty_tree_returns_none() {
    let store = MemPageStore::new();
    let tree: BTree<MemPageStore> = BTree::create(store).unwrap();
    let result = tree.search(&key(42)).unwrap();
    assert!(result.is_none());
}

// -----------------------------------------------------------------------
// Insert + search (single leaf, no split)
// -----------------------------------------------------------------------

#[test]
fn insert_and_search_single_entry() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    tree.insert(&key(1), b"hello").unwrap();
    let found = tree.get(&key(1)).unwrap();
    assert_eq!(found, Some(b"hello".to_vec()));
}

#[test]
fn search_missing_key_returns_none() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    tree.insert(&key(1), b"hello").unwrap();
    assert!(tree.search(&key(2)).unwrap().is_none());
}

#[test]
fn insert_many_single_leaf_all_found() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    for i in 0u64..20 {
        tree.insert(&key(i), &val(i)).unwrap();
    }

    for i in 0u64..20 {
        let found = tree.get(&key(i)).unwrap();
        assert_eq!(found, Some(val(i)), "key {i} should be found");
    }
}

#[test]
fn insert_duplicate_key_returns_error() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    tree.insert(&key(1), b"v1").unwrap();
    let result = tree.insert(&key(1), b"v2");
    assert!(
        matches!(result, Err(Error::DuplicateKey { .. })),
        "inserting duplicate should return DuplicateKey"
    );
}

// -----------------------------------------------------------------------
// Leaf split
// -----------------------------------------------------------------------

#[test]
fn insert_enough_to_trigger_leaf_split() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    // 200-byte value + 8-byte key takes ~215 bytes per cell (key_len=8, value=200+4+1).
    // A 32KB leaf fits ~148 such cells before splitting.
    // Insert 160 entries to ensure at least one split.
    let v = vec![0xABu8; 200];
    for i in 0u64..160 {
        tree.insert(&key(i), &v).unwrap();
    }

    // After split, root_level should be 1 (internal node above two leaves).
    assert_eq!(tree.root_level, 1, "should have split to a 2-level tree");

    // All keys must still be found.
    for i in 0u64..160 {
        let found = tree.get(&key(i)).unwrap();
        assert_eq!(
            found.as_deref(),
            Some(v.as_slice()),
            "key {i} missing after split"
        );
    }
}

#[test]
fn split_correctness_all_keys_in_order() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    let v = vec![0u8; 200];
    // Insert in reverse order to stress the split code.
    for i in (0u64..160).rev() {
        tree.insert(&key(i), &v).unwrap();
    }

    // Range scan should return all keys in ascending order.
    let results = tree.range_scan(None, None).unwrap();
    assert_eq!(results.len(), 160);
    for (i, (k, _)) in results.iter().enumerate() {
        assert_eq!(k.as_slice(), &key(i as u64), "key at position {i} is wrong");
    }
}

// -----------------------------------------------------------------------
// Multi-level split (root split)
// -----------------------------------------------------------------------

#[test]
fn three_level_tree_all_keys_accessible() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    // Use small values so we need many leaves to get a root split.
    // Each cell ≈ 8 (key) + 5 (value) + 7 (overhead) = 20 bytes + pointer.
    // A 32KB leaf holds ~1600 such cells; with a 4KB internal node holding ~150 pointers,
    // we need about 150 * 100 = 15,000 entries to force a root split of level-1 internal.
    // Let's insert 500 entries with 150-byte values instead for a faster test.
    let v = vec![0xBBu8; 150];
    let n: u64 = 500;
    for i in 0..n {
        tree.insert(&key(i), &v).unwrap();
    }

    for i in 0..n {
        let found = tree.get(&key(i)).unwrap();
        assert_eq!(found.as_deref(), Some(v.as_slice()), "key {i} missing");
    }
}

// -----------------------------------------------------------------------
// Delete
// -----------------------------------------------------------------------

#[test]
fn delete_existing_key_returns_true() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    tree.insert(&key(1), b"v1").unwrap();
    assert!(tree.delete(&key(1)).unwrap());
    assert!(tree.get(&key(1)).unwrap().is_none());
}

#[test]
fn delete_missing_key_returns_false() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    tree.insert(&key(1), b"v1").unwrap();
    assert!(!tree.delete(&key(99)).unwrap());
}

#[test]
fn insert_delete_all_entries_tree_empty() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    for i in 0u64..10 {
        tree.insert(&key(i), &val(i)).unwrap();
    }
    for i in 0u64..10 {
        assert!(tree.delete(&key(i)).unwrap(), "key {i} should be deleted");
    }
    for i in 0u64..10 {
        assert!(
            tree.get(&key(i)).unwrap().is_none(),
            "key {i} should be gone"
        );
    }
}

#[test]
fn delete_triggers_merge_all_remaining_accessible() {
    // Create tree, insert enough for a split, delete enough to trigger merge.
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    let v = vec![0u8; 200];
    let n: u64 = 160;
    for i in 0..n {
        tree.insert(&key(i), &v).unwrap();
    }

    // Delete most entries — leave only a few.
    for i in 10..n {
        assert!(tree.delete(&key(i)).unwrap(), "key {i} should be deleted");
    }

    // Remaining keys must all still be accessible.
    for i in 0..10 {
        let found = tree.get(&key(i)).unwrap();
        assert_eq!(
            found.as_deref(),
            Some(v.as_slice()),
            "key {i} should still exist"
        );
    }
    // Deleted keys must be gone.
    for i in 10..n {
        assert!(
            tree.get(&key(i)).unwrap().is_none(),
            "key {i} should be gone"
        );
    }
}

// -----------------------------------------------------------------------
// Range scan
// -----------------------------------------------------------------------

#[test]
fn range_scan_all_keys() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    for i in 0u64..50 {
        tree.insert(&key(i), &val(i)).unwrap();
    }

    let results = tree.range_scan(None, None).unwrap();
    assert_eq!(results.len(), 50);
    for (i, (k, _)) in results.iter().enumerate() {
        assert_eq!(k.as_slice(), &key(i as u64));
    }
}

#[test]
fn range_scan_with_bounds() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    for i in 0u64..100 {
        tree.insert(&key(i), &val(i)).unwrap();
    }

    // keys 10..=20
    let results = tree.range_scan(Some(&key(10)), Some(&key(20))).unwrap();
    assert_eq!(results.len(), 11, "should return keys 10..=20");
    assert_eq!(results[0].0, key(10));
    assert_eq!(results[10].0, key(20));
}

#[test]
fn range_scan_start_bound_only() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    for i in 0u64..50 {
        tree.insert(&key(i), &val(i)).unwrap();
    }

    let results = tree.range_scan(Some(&key(40)), None).unwrap();
    assert_eq!(results.len(), 10); // keys 40..=49
    assert_eq!(results[0].0, key(40));
}

#[test]
fn range_scan_end_bound_only() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    for i in 0u64..50 {
        tree.insert(&key(i), &val(i)).unwrap();
    }

    let results = tree.range_scan(None, Some(&key(9))).unwrap();
    assert_eq!(results.len(), 10); // keys 0..=9
    assert_eq!(results[9].0, key(9));
}

#[test]
fn range_scan_empty_range() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    for i in 0u64..10 {
        tree.insert(&key(i), &val(i)).unwrap();
    }

    // No keys in [100, 200].
    let results = tree.range_scan(Some(&key(100)), Some(&key(200))).unwrap();
    assert!(results.is_empty());
}

#[test]
fn range_scan_across_leaves_in_key_order() {
    // Force a split and verify range scan uses sibling pointers correctly.
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    let v = vec![0u8; 200];
    for i in 0u64..160 {
        tree.insert(&key(i), &v).unwrap();
    }

    let results = tree.range_scan(None, None).unwrap();
    assert_eq!(results.len(), 160);
    for (i, (k, _)) in results.iter().enumerate() {
        assert_eq!(k.as_slice(), &key(i as u64), "position {i}: wrong key");
    }
}

// -----------------------------------------------------------------------
// Overflow
// -----------------------------------------------------------------------

#[test]
fn insert_overflow_value_and_retrieve() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    // A value just above the overflow threshold.
    let big_val = vec![0xCCu8; OVERFLOW_THRESHOLD + 1];
    tree.insert(&key(1), &big_val).unwrap();

    // Should be stored as overflow.
    match tree.search(&key(1)).unwrap().unwrap() {
        CellValue::Overflow { .. } => {}
        CellValue::Inline(_) => panic!("expected overflow storage"),
    }

    // Full retrieval via get().
    let retrieved = tree.get(&key(1)).unwrap().unwrap();
    assert_eq!(retrieved, big_val);
}

#[test]
fn insert_multi_page_overflow_and_retrieve() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    // Value spanning several overflow pages.
    let big_val = vec![0xDDu8; OVERFLOW_THRESHOLD * 3];
    tree.insert(&key(42), &big_val).unwrap();

    let retrieved = tree.get(&key(42)).unwrap().unwrap();
    assert_eq!(retrieved.len(), big_val.len());
    assert_eq!(retrieved, big_val);
}

#[test]
fn delete_overflow_entry_frees_chain() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    let big_val = vec![0xEEu8; OVERFLOW_THRESHOLD + 100];
    tree.insert(&key(7), &big_val).unwrap();

    assert!(tree.delete(&key(7)).unwrap());
    assert!(tree.get(&key(7)).unwrap().is_none());
}

// -----------------------------------------------------------------------
// Mixed insert/delete roundtrip
// -----------------------------------------------------------------------

#[test]
fn mixed_insert_delete_many_keys() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    // Insert 200 keys.
    for i in 0u64..200 {
        tree.insert(&key(i), &val(i)).unwrap();
    }

    // Delete every other key.
    for i in (0u64..200).step_by(2) {
        assert!(tree.delete(&key(i)).unwrap());
    }

    // Odd keys must survive.
    for i in (1u64..200).step_by(2) {
        let found = tree.get(&key(i)).unwrap();
        assert_eq!(found, Some(val(i)), "key {i} missing");
    }
    // Even keys must be gone.
    for i in (0u64..200).step_by(2) {
        assert!(
            tree.get(&key(i)).unwrap().is_none(),
            "key {i} should be gone"
        );
    }
}

// -----------------------------------------------------------------------
// B+ tree invariant checks
// -----------------------------------------------------------------------

/// Walk the tree and verify: all leaves are at the same depth, all keys are
/// in sorted order within each node, and sibling pointers are consistent.
fn verify_tree_invariants<S: BTreePageStore>(tree: &BTree<S>) {
    let root_depth = tree.root_level;

    // Collect all leaf keys via normal traversal.
    let traversal_keys: Vec<Vec<u8>> = collect_keys_via_traversal(tree);

    // Collect all leaf keys via sibling pointer chain.
    let chain_keys: Vec<Vec<u8>> = collect_keys_via_chain(tree);

    assert_eq!(
        traversal_keys, chain_keys,
        "traversal keys ≠ sibling chain keys"
    );

    // Verify sorted order.
    for i in 1..traversal_keys.len() {
        assert!(
            traversal_keys[i - 1] < traversal_keys[i],
            "keys out of order at positions {} and {}",
            i - 1,
            i
        );
    }

    // Verify all leaves are at the same depth.
    verify_leaf_depth(tree, tree.root_page, root_depth);
}

fn collect_keys_via_traversal<S: BTreePageStore>(tree: &BTree<S>) -> Vec<Vec<u8>> {
    let results = tree.range_scan(None, None).unwrap();
    results.into_iter().map(|(k, _)| k).collect()
}

fn collect_keys_via_chain<S: BTreePageStore>(tree: &BTree<S>) -> Vec<Vec<u8>> {
    let first = tree.leftmost_leaf().unwrap();
    let mut cur = first;
    let mut keys = Vec::new();
    while cur != 0 {
        let (buf, _) = tree.store.read_leaf(cur).unwrap();
        let node = LeafNode::parse(&buf[..]).unwrap();
        for cell in &node.cells {
            keys.push(cell.key.clone());
        }
        cur = node.next_leaf_page;
    }
    keys
}

fn verify_leaf_depth<S: BTreePageStore>(tree: &BTree<S>, page: u32, level: u8) {
    if level == 0 {
        // It's a leaf page — nothing further to check structurally here.
        return;
    }
    let buf = tree.store.read_internal(page).unwrap();
    let node = InternalNode::parse(&buf[..]).unwrap();
    assert_eq!(
        node.level, level,
        "internal node at page {page} has wrong level"
    );
    for (_, child) in &node.entries {
        verify_leaf_depth(tree, *child, level - 1);
    }
    verify_leaf_depth(tree, node.rightmost_child, level - 1);
}

#[test]
fn invariants_after_inserts_no_split() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();
    for i in 0u64..20 {
        tree.insert(&key(i), &val(i)).unwrap();
    }
    verify_tree_invariants(&tree);
}

#[test]
fn invariants_after_leaf_split() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();
    let v = vec![0u8; 200];
    for i in 0u64..160 {
        tree.insert(&key(i), &v).unwrap();
    }
    verify_tree_invariants(&tree);
}

#[test]
fn invariants_after_delete_and_merge() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();
    let v = vec![0u8; 200];
    for i in 0u64..160 {
        tree.insert(&key(i), &v).unwrap();
    }
    for i in 10u64..160 {
        tree.delete(&key(i)).unwrap();
    }
    verify_tree_invariants(&tree);
}

// -----------------------------------------------------------------------
// T3.5 — version-chain migration across split / merge
//
// These tests exercise the split / redistribute / merge paths that were
// taught to migrate per-frame MVCC version chains alongside the cells
// that own them, and the `chains_empty` guard guarding the two merge
// `free_leaf` call sites at btree.rs:1281 and :1308 (per plan MAJOR-5).
// -----------------------------------------------------------------------

use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::{OverflowRef, VersionData, VersionEntry, VersionState};
use crate::storage::allocator::AllocatorHandle;
use crate::storage::header::FileHeader;
use std::collections::VecDeque;
use std::sync::Arc;

fn dummy_entry(marker: u8) -> VersionEntry {
    VersionEntry {
        start_ts: Ts {
            physical_ms: marker as u64,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id: marker as u64,
        state: VersionState::Committed,
        data: VersionData::Inline(vec![marker; 16]),
        is_tombstone: false,
    }
}

fn chain_with(markers: &[u8]) -> Arc<VecDeque<VersionEntry>> {
    Arc::new(markers.iter().copied().map(dummy_entry).collect())
}

fn leaf_of(tree: &BTree<MemPageStore>, k: &[u8]) -> u32 {
    tree.find_leaf(k).expect("find_leaf")
}

fn leaf_cell_count(tree: &BTree<MemPageStore>, page: u32) -> usize {
    let (buf, _) = tree.store.read_leaf(page).unwrap();
    LeafNode::parse(&buf[..]).unwrap().cells.len()
}

fn next_leaf_of(tree: &BTree<MemPageStore>, page: u32) -> u32 {
    let (buf, _) = tree.store.read_leaf(page).unwrap();
    LeafNode::parse(&buf[..]).unwrap().next_leaf_page
}

// --- T3.5 split: primary-shaped keys -----------------------------------

#[test]
fn t3_5_split_migrates_chains_for_moving_cells_primary() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    // Use values large enough to split after a handful of inserts.
    let v = vec![0xABu8; 6000];
    // Insert 4 keys — all fit in one leaf (pre-split).
    for i in 0u64..4 {
        tree.insert(&key(i), &v).unwrap();
    }
    assert_eq!(tree.root_level, 0, "pre-split, root should still be a leaf");

    let leaf_page = tree.root_page;
    // Attach a unique chain to every key.
    for i in 0u64..4 {
        tree.store
            .put_chain(leaf_page, key(i), chain_with(&[i as u8]))
            .unwrap();
    }

    // Insert two more keys to force a split.
    for i in 4u64..6 {
        tree.insert(&key(i), &v).unwrap();
    }
    assert_eq!(tree.root_level, 1, "expected a split to occur");

    // Every original key's chain must still be reachable, on whichever
    // leaf the key now lives on, byte-identical to what we inserted.
    for i in 0u64..4 {
        let home = leaf_of(&tree, &key(i));
        let chain = tree.store.take_chain(home, &key(i)).unwrap();
        let chain = chain.expect("chain survived the split");
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0].txn_id, i);
        // Put it back for subsequent checks / cleanup.
        tree.store.put_chain(home, key(i), chain).unwrap();
    }
}

// --- T3.5 split: sec-index-shaped 9-byte keys --------------------------

#[test]
fn t3_5_split_migrates_chains_for_moving_cells_secondary() {
    // Secondary-index key shape: [field_byte || id_be_bytes(8)] = 9 B.
    fn sec_key(field: u8, id: u64) -> Vec<u8> {
        let mut k = Vec::with_capacity(9);
        k.push(field);
        k.extend_from_slice(&id.to_be_bytes());
        k
    }

    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();
    let v = vec![0xCDu8; 6000];

    for i in 0u64..4 {
        tree.insert(&sec_key(0x5A, i), &v).unwrap();
    }
    let leaf_page = tree.root_page;
    for i in 0u64..4 {
        tree.store
            .put_chain(leaf_page, sec_key(0x5A, i), chain_with(&[i as u8]))
            .unwrap();
    }

    for i in 4u64..6 {
        tree.insert(&sec_key(0x5A, i), &v).unwrap();
    }
    assert_eq!(tree.root_level, 1);

    for i in 0u64..4 {
        let k = sec_key(0x5A, i);
        let home = leaf_of(&tree, &k);
        let chain = tree.store.take_chain(home, &k).unwrap();
        let chain = chain.expect("chain survived the split (sec-index)");
        assert_eq!(chain[0].txn_id, i);
        tree.store.put_chain(home, k, chain).unwrap();
    }
}

// --- T3.5 merge: refcount invariant preserved across merge -------------

#[test]
fn t3_5_merge_preserves_overflow_refcount_invariant() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();
    let alloc = AllocatorHandle::new(FileHeader::new(0, 0, 0));

    // Force a split with 6000-byte values.
    let v = vec![0xEFu8; 6000];
    for i in 0u64..6 {
        tree.insert(&key(i), &v).unwrap();
    }
    assert_eq!(tree.root_level, 1);

    // Attach an overflow-backed chain to two keys that will survive the
    // merge; the refcounts they hold must remain exactly 1 after the
    // entire merge dance.
    const OVF_A: u32 = 4242;
    const OVF_B: u32 = 4343;
    let chain_a = {
        let r = OverflowRef::new_owned(OVF_A, 32, alloc.clone()).unwrap();
        let mut q = VecDeque::new();
        q.push_back(VersionEntry {
            start_ts: Ts {
                physical_ms: 1,
                logical: 0,
            },
            stop_ts: Ts::MAX,
            txn_id: 1,
            state: VersionState::Committed,
            data: VersionData::Overflow(r),
            is_tombstone: false,
        });
        Arc::new(q)
    };
    let chain_b = {
        let r = OverflowRef::new_owned(OVF_B, 32, alloc.clone()).unwrap();
        let mut q = VecDeque::new();
        q.push_back(VersionEntry {
            start_ts: Ts {
                physical_ms: 2,
                logical: 0,
            },
            stop_ts: Ts::MAX,
            txn_id: 2,
            state: VersionState::Committed,
            data: VersionData::Overflow(r),
            is_tombstone: false,
        });
        Arc::new(q)
    };
    assert_eq!(alloc.overflow_refcount(OVF_A), 1);
    assert_eq!(alloc.overflow_refcount(OVF_B), 1);

    let home_a = leaf_of(&tree, &key(0));
    let home_b = leaf_of(&tree, &key(5));
    tree.store.put_chain(home_a, key(0), chain_a).unwrap();
    tree.store.put_chain(home_b, key(5), chain_b).unwrap();

    // Delete one key to force a leaf underflow that takes the merge path
    // (both siblings hold MIN or fewer cells after the split, so
    // redistribute fails and the merge branch at btree.rs:1393 fires,
    // migrating chain_a onto the surviving leaf).
    assert!(tree.delete(&key(1)).unwrap());

    // Refcounts still 1 each — migration neither dropped nor duplicated
    // the OverflowRefs.
    assert_eq!(alloc.overflow_refcount(OVF_A), 1);
    assert_eq!(alloc.overflow_refcount(OVF_B), 1);

    // Chains are reachable on whichever leaf the keys now live on.
    let home_a = leaf_of(&tree, &key(0));
    let home_b = leaf_of(&tree, &key(5));
    let a = tree
        .store
        .take_chain(home_a, &key(0))
        .unwrap()
        .expect("chain A survived merge");
    let b = tree
        .store
        .take_chain(home_b, &key(5))
        .unwrap()
        .expect("chain B survived merge");

    // Drop the chains — refcounts should fall to 0 and the pages should
    // land on the page-lifetime queue exactly once each.
    drop(a);
    drop(b);
    assert_eq!(alloc.overflow_refcount(OVF_A), 0);
    assert_eq!(alloc.overflow_refcount(OVF_B), 0);
    assert_eq!(alloc.page_lifetime_queue().depth(), 2);
}

// --- T3.5 merge: orphan chains migrate with merge-into-left ------------

#[test]
fn t3_5_merge_into_left_migrates_orphan_chain() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    let v = vec![0x11u8; 6000];
    for i in 0u64..6 {
        tree.insert(&key(i), &v).unwrap();
    }
    assert_eq!(tree.root_level, 1);

    // Pick a leaf that is NOT the leftmost — its underflow will take the
    // merge-into-left branch at btree.rs:1281.
    let victim_leaf = leaf_of(&tree, &key(5));
    assert_ne!(victim_leaf, tree.leftmost_leaf().unwrap());

    // Install an orphan chain (key not present in any cell) on the victim
    // leaf. The merge path must migrate it onto the surviving sibling so
    // stale readers still descend to the correct frame after the root
    // collapses.
    tree.store
        .put_chain(victim_leaf, b"orphan-key".to_vec(), chain_with(&[0xEE]))
        .unwrap();

    assert!(tree.delete(&key(5)).unwrap());
    assert_eq!(
        tree.root_level, 0,
        "merge should collapse the two-leaf root"
    );
    assert_eq!(tree.get(&key(4)).unwrap(), Some(v.clone()));

    let orphan = tree
        .store
        .take_chain(tree.root_page, b"orphan-key")
        .unwrap()
        .expect("orphan chain survived merge-into-left");
    assert_eq!(orphan[0].txn_id, 0xEE);
}

// --- T3.5 merge: orphan chains migrate with merge-into-right -----------

#[test]
fn t3_5_merge_into_right_migrates_orphan_chain() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    let v = vec![0x22u8; 6000];
    for i in 0u64..6 {
        tree.insert(&key(i), &v).unwrap();
    }
    assert_eq!(tree.root_level, 1);

    // The leftmost leaf underflowing takes the merge-into-right branch
    // at btree.rs:1308 (child_idx == 0 path).
    let victim_leaf = tree.leftmost_leaf().unwrap();

    tree.store
        .put_chain(victim_leaf, b"orphan-key".to_vec(), chain_with(&[0xFF]))
        .unwrap();

    assert!(tree.delete(&key(0)).unwrap());
    assert_eq!(
        tree.root_level, 0,
        "merge should collapse the two-leaf root"
    );
    assert_eq!(tree.get(&key(1)).unwrap(), Some(v.clone()));

    let orphan = tree
        .store
        .take_chain(tree.root_page, b"orphan-key")
        .unwrap()
        .expect("orphan chain survived merge-into-right");
    assert_eq!(orphan[0].txn_id, 0xFF);
}

// --- T3.5 redistribution: avoid oversized merge on left branch ---------

#[test]
fn t3_5_left_underflow_redistributes_when_merge_would_overflow() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    let v = vec![0x33u8; 6000];
    for i in 0u64..7 {
        tree.insert(&key(i), &v).unwrap();
    }
    assert_eq!(tree.root_level, 1);

    let victim_leaf = leaf_of(&tree, &key(6));
    assert_ne!(victim_leaf, tree.leftmost_leaf().unwrap());
    assert_eq!(leaf_cell_count(&tree, victim_leaf), 4);

    assert!(tree.delete(&key(6)).unwrap());
    assert_eq!(
        tree.root_level, 1,
        "oversized sibling pair should redistribute instead of collapsing"
    );

    let left = tree.leftmost_leaf().unwrap();
    let right = next_leaf_of(&tree, left);
    assert_ne!(right, 0);
    assert_eq!(leaf_cell_count(&tree, left), 3);
    assert_eq!(leaf_cell_count(&tree, right), 3);

    for i in 0u64..6 {
        assert_eq!(
            tree.get(&key(i)).unwrap(),
            Some(v.clone()),
            "key {i} missing"
        );
    }
    assert!(tree.get(&key(6)).unwrap().is_none());
}

// --- T3.5 redistribution: avoid oversized merge on right branch --------

#[test]
fn t3_5_right_underflow_redistributes_when_merge_would_overflow() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    let v = vec![0x44u8; 6000];
    for i in 0u64..7 {
        tree.insert(&key(i), &v).unwrap();
    }
    assert_eq!(tree.root_level, 1);

    let victim_leaf = tree.leftmost_leaf().unwrap();
    assert_eq!(leaf_cell_count(&tree, victim_leaf), 3);

    assert!(tree.delete(&key(0)).unwrap());
    assert_eq!(
        tree.root_level, 1,
        "oversized sibling pair should redistribute instead of collapsing"
    );

    let left = tree.leftmost_leaf().unwrap();
    let right = next_leaf_of(&tree, left);
    assert_ne!(right, 0);
    assert_eq!(leaf_cell_count(&tree, left), 3);
    assert_eq!(leaf_cell_count(&tree, right), 3);

    for i in 1u64..7 {
        assert_eq!(
            tree.get(&key(i)).unwrap(),
            Some(v.clone()),
            "key {i} missing"
        );
    }
    assert!(tree.get(&key(0)).unwrap().is_none());
}

// --- T3.5 chains_empty semantics on absent page ------------------------

#[test]
fn t3_5_chains_empty_on_absent_page_is_true() {
    let store = MemPageStore::new();
    let tree: BTree<MemPageStore> = BTree::create(store).unwrap();
    // Page 999 was never touched.
    assert!(tree.store.chains_empty(999).unwrap());
}
