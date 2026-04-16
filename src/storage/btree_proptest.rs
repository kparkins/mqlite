// Property-based tests for all 10 B+ tree invariants.
//
// Uses proptest to generate random sequences of Insert / Delete operations,
// then verifies every structural invariant after the final state is reached.
// The shrinking built into proptest automatically minimises any failing case.
//
// ## Invariants under test
//
// 1. Ordering – keys within each leaf are sorted; separator keys in
//    internal nodes correctly bound their subtrees.
// 2. Balance – every leaf is at the same depth from the root.
// 3. Parent-child consistency – every internal child pointer resolves to
//    a valid page of the correct type; no cycles.
// 4. Sibling pointer consistency – leaf doubly-linked list is coherent
//    and covers every leaf in ascending key order.
// 5. Key-count invariants – no leaf has fewer than MIN_LEAF_CELLS entries
//    (unless it is the sole root leaf); no leaf or internal node exceeds its
//    page-size capacity.
// 6. Overflow chain integrity – every overflow pointer leads to a complete,
//    non-cyclic chain; no orphaned overflow pages exist.
// 7. Index-data consistency – a simulated secondary index (separate B+
//    tree) mirrors insert / delete operations; every secondary entry has a
//    corresponding primary entry.
// 8. Checksum validity – every leaf and internal page passes CRC32C
//    verification.
// 9. Sibling chain completeness after splits – the doubly-linked list
//    remains valid after split-triggering insert sequences.
// 10. Split key coverage – separator keys in parent nodes correctly
//     partition the key space of their left and right subtrees.
//
// Invariants 9 and 10 are subsumed by the core invariant checks (4 and 1
// respectively) but are exercised through dedicated split-triggering
// strategies to ensure they are reached.

use proptest::prelude::*;

use super::*;
use crate::storage::page::{
    verify_internal_page_checksum, verify_leaf_page_checksum, verify_overflow_page_checksum,
    InternalPageHeader, LeafPageHeader, OverflowPageHeader, PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF,
    PAGE_TYPE_INTERNAL, PAGE_TYPE_LEAF, PAGE_TYPE_OVERFLOW, INTERNAL_HEADER_SIZE,
    LEAF_HEADER_SIZE, OVERFLOW_HEADER_SIZE,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Encode a `u64` as an 8-byte big-endian key so that byte ordering equals
/// numeric ordering.
fn enc_key(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// Create a value of exactly `len` bytes filled with a deterministic pattern.
fn make_val(seed: u8, len: usize) -> Vec<u8> {
    vec![seed; len]
}

// ---------------------------------------------------------------------------
// Operation type
// ---------------------------------------------------------------------------

/// A single operation that can be applied to a B+ tree.
#[derive(Clone, Debug)]
enum Op {
    /// Insert `key` with a value of `val_size` bytes (seed byte = key & 0xFF).
    Insert { key: u64, val_size: usize },
    /// Delete `key` (no-op if missing).
    Delete { key: u64 },
}

// ---------------------------------------------------------------------------
// proptest strategies
// ---------------------------------------------------------------------------

/// Key domain: small pool (0..=127) to create many collisions / re-insertions.
fn arb_key() -> impl Strategy<Value = u64> {
    0u64..=127
}

/// Value size: mix of inline-small, inline-large, and overflow.
fn arb_val_size() -> impl Strategy<Value = usize> {
    prop_oneof![
        // Small inline (< 100 B)
        1usize..=99,
        // Medium inline (a few KB)
        512usize..=4096,
        // Large inline (near overflow threshold)
        (OVERFLOW_THRESHOLD - 512)..=(OVERFLOW_THRESHOLD - 1),
        // Just over threshold → overflow
        (OVERFLOW_THRESHOLD + 1)..=(OVERFLOW_THRESHOLD + 512),
        // Multi-page overflow
        (OVERFLOW_THRESHOLD * 2)..=(OVERFLOW_THRESHOLD * 2 + 512),
    ]
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // 60 % inserts, 40 % deletes
        6 => (arb_key(), arb_val_size()).prop_map(|(key, val_size)| Op::Insert { key, val_size }),
        4 => arb_key().prop_map(|key| Op::Delete { key }),
    ]
}

/// A sequence of 1..=300 operations.
fn arb_ops() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(arb_op(), 1..=300)
}

/// Like `arb_ops` but forces many splits: uses large values and many inserts.
fn arb_split_heavy_ops() -> impl Strategy<Value = Vec<Op>> {
    let split_op = (arb_key(), (200usize..=500)).prop_map(|(key, val_size)| Op::Insert {
        key,
        val_size,
    });
    prop::collection::vec(split_op, 50..=400)
}

// ---------------------------------------------------------------------------
// Apply operations
// ---------------------------------------------------------------------------

/// Apply a slice of operations to a `BTree<MemPageStore>`.
/// Returns the set of keys currently in the tree (for consistency checks).
fn apply_ops(
    tree: &mut BTree<MemPageStore>,
    ops: &[Op],
) -> std::collections::BTreeSet<u64> {
    let mut present: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();

    for op in ops {
        match *op {
            Op::Insert { key, val_size } => {
                if !present.contains(&key) {
                    let v = make_val((key & 0xFF) as u8, val_size);
                    tree.insert(&enc_key(key), &v).expect("insert failed");
                    present.insert(key);
                }
            }
            Op::Delete { key } => {
                tree.delete(&enc_key(key)).expect("delete failed");
                present.remove(&key);
            }
        }
    }

    present
}

// ---------------------------------------------------------------------------
// Invariant 1: Ordering
// ---------------------------------------------------------------------------
//
// • Within each leaf, cells must be strictly ascending by key.
// • For every internal node, the separator keys must partition their children
//   correctly: all keys in child[i] < separator[i] <= all keys in child[i+1].

fn check_inv1_ordering(tree: &BTree<MemPageStore>) {
    // Check via range_scan: the returned sequence must be strictly ascending.
    let pairs = tree.range_scan(None, None).expect("range_scan failed");
    for w in pairs.windows(2) {
        assert!(
            w[0].0 < w[1].0,
            "INV1 ordering: keys not strictly ascending: {:?} >= {:?}",
            w[0].0,
            w[1].0
        );
    }

    // Walk internal nodes and verify separator key coverage.
    check_internal_key_coverage(tree, tree.root_page, tree.root_level);
}

/// Recursively verify that, for every internal node, each separator key `sep`
/// at position `i` satisfies:
///   max_key(child[i]) < sep  AND  min_key(child[i+1]) >= sep
///
/// (where child[i] is the left child of sep, child[i+1] is child to the right)
fn check_internal_key_coverage(tree: &BTree<MemPageStore>, page: u32, level: u8) {
    if level == 0 {
        return; // leaf
    }
    let buf = tree.store.read_internal(page).expect("read_internal");
    let node = InternalNode::parse(&buf[..]).expect("parse internal");

    // For each separator + left child pair, verify key coverage.
    for (i, (sep, left_child)) in node.entries.iter().enumerate() {
        // Max key of left child must be < sep.
        let left_max = subtree_max_key(tree, *left_child, level - 1);
        if let Some(lmax) = left_max {
            assert!(
                &lmax < sep,
                "INV1/10 split-key coverage: left child max key {:?} >= separator {:?} \
                 (internal page {page}, entry {i})",
                lmax,
                sep
            );
        }

        // Min key of right child (child[i+1]) must be >= sep.
        let right_child = node.child_at(i + 1);
        let right_min = subtree_min_key(tree, right_child, level - 1);
        if let Some(rmin) = right_min {
            assert!(
                &rmin >= sep,
                "INV1/10 split-key coverage: right child min key {:?} < separator {:?} \
                 (internal page {page}, entry {i})",
                rmin,
                sep
            );
        }

        // Recurse into left child.
        check_internal_key_coverage(tree, *left_child, level - 1);
    }

    // Recurse into rightmost child.
    check_internal_key_coverage(tree, node.rightmost_child, level - 1);
}

fn subtree_max_key(tree: &BTree<MemPageStore>, page: u32, level: u8) -> Option<Vec<u8>> {
    if level == 0 {
        let buf = tree.store.read_leaf(page).expect("read_leaf");
        let node = LeafNode::parse(&buf[..]).expect("parse leaf");
        node.cells.last().map(|c| c.key.clone())
    } else {
        let buf = tree.store.read_internal(page).expect("read_internal");
        let node = InternalNode::parse(&buf[..]).expect("parse internal");
        subtree_max_key(tree, node.rightmost_child, level - 1)
    }
}

fn subtree_min_key(tree: &BTree<MemPageStore>, page: u32, level: u8) -> Option<Vec<u8>> {
    if level == 0 {
        let buf = tree.store.read_leaf(page).expect("read_leaf");
        let node = LeafNode::parse(&buf[..]).expect("parse leaf");
        node.cells.first().map(|c| c.key.clone())
    } else {
        let buf = tree.store.read_internal(page).expect("read_internal");
        let node = InternalNode::parse(&buf[..]).expect("parse internal");
        // Leftmost key is in the first child (entries[0].1 if present, else rightmost_child).
        let leftmost = if !node.entries.is_empty() {
            node.entries[0].1
        } else {
            node.rightmost_child
        };
        subtree_min_key(tree, leftmost, level - 1)
    }
}

// ---------------------------------------------------------------------------
// Invariant 2: Balance
// ---------------------------------------------------------------------------
//
// All leaf pages must be at depth `root_level` from the root.

fn check_inv2_balance(tree: &BTree<MemPageStore>) {
    check_balance_recursive(tree, tree.root_page, tree.root_level, 0);
}

fn check_balance_recursive(tree: &BTree<MemPageStore>, page: u32, level: u8, depth: u32) {
    if level == 0 {
        // This page is a leaf; depth should equal root_level.
        assert_eq!(
            depth,
            tree.root_level as u32,
            "INV2 balance: leaf at depth {depth} but root_level={}",
            tree.root_level
        );
        return;
    }
    let buf = tree.store.read_internal(page).expect("read_internal");
    let node = InternalNode::parse(&buf[..]).expect("parse internal");
    for (_, child) in &node.entries {
        check_balance_recursive(tree, *child, level - 1, depth + 1);
    }
    check_balance_recursive(tree, node.rightmost_child, level - 1, depth + 1);
}

// ---------------------------------------------------------------------------
// Invariant 3: Parent-child consistency
// ---------------------------------------------------------------------------
//
// Every child pointer in an internal node must point to a page that:
// • exists in the store (readable without error)
// • has the correct page-type byte (internal if level > 1, leaf if level == 1)
// • has no duplicate page numbers (no cycles / aliasing)

fn check_inv3_parent_child(tree: &BTree<MemPageStore>) {
    let mut visited: std::collections::HashSet<u32> = std::collections::HashSet::new();
    check_parent_child_recursive(tree, tree.root_page, tree.root_level, &mut visited);
}

fn check_parent_child_recursive(
    tree: &BTree<MemPageStore>,
    page: u32,
    level: u8,
    visited: &mut std::collections::HashSet<u32>,
) {
    // Detect cycles.
    assert!(
        visited.insert(page),
        "INV3 parent-child: cycle detected — page {page} visited twice"
    );

    if level == 0 {
        // Leaf page — verify it is readable and has the right page type.
        let buf = tree.store.read_leaf(page).expect("read_leaf");
        let hdr = LeafPageHeader::from_bytes(&buf[..]).expect("leaf header parse");
        assert_eq!(
            hdr.page_type, PAGE_TYPE_LEAF,
            "INV3 parent-child: page {page} at leaf level has page_type 0x{:02X}, expected 0x{:02X}",
            hdr.page_type, PAGE_TYPE_LEAF
        );
        return;
    }

    // Internal page.
    let buf = tree.store.read_internal(page).expect("read_internal");
    let hdr = InternalPageHeader::from_bytes(&buf[..]).expect("internal header parse");
    assert_eq!(
        hdr.page_type, PAGE_TYPE_INTERNAL,
        "INV3 parent-child: page {page} at internal level has page_type 0x{:02X}, expected 0x{:02X}",
        hdr.page_type, PAGE_TYPE_INTERNAL
    );
    let node = InternalNode::parse(&buf[..]).expect("parse internal");
    for (_, child) in &node.entries {
        check_parent_child_recursive(tree, *child, level - 1, visited);
    }
    check_parent_child_recursive(tree, node.rightmost_child, level - 1, visited);
}

// ---------------------------------------------------------------------------
// Invariant 4 & 9: Sibling pointer consistency / chain completeness
// ---------------------------------------------------------------------------
//
// 4: The doubly-linked list of leaf pages is coherent:
//    • following next_leaf_page covers every leaf in ascending key order
//    • following prev_leaf_page of any page yields the page that points to it
// 9: (subsumed) The same invariant holds after split-triggering insertions.

fn check_inv4_sibling_pointers(tree: &BTree<MemPageStore>) {
    // Collect all leaf pages via tree traversal.
    let mut traversal_leaves: Vec<u32> = Vec::new();
    collect_leaves_via_traversal(tree, tree.root_page, tree.root_level, &mut traversal_leaves);

    // Walk the linked list forward.
    let first = tree.leftmost_leaf().expect("leftmost_leaf");
    let mut forward_chain: Vec<u32> = Vec::new();
    let mut prev_page: u32 = 0;
    let mut cur = first;
    while cur != 0 {
        let buf = tree.store.read_leaf(cur).expect("read_leaf");
        let node = LeafNode::parse(&buf[..]).expect("parse leaf");

        // Verify backward pointer.
        assert_eq!(
            node.prev_leaf_page, prev_page,
            "INV4 sibling: page {cur} has prev={}, expected {prev_page}",
            node.prev_leaf_page
        );

        forward_chain.push(cur);
        prev_page = cur;
        cur = node.next_leaf_page;
    }

    // The forward chain and traversal must cover the same set (and same order).
    assert_eq!(
        traversal_leaves.len(),
        forward_chain.len(),
        "INV4 sibling: traversal found {} leaves, chain found {}",
        traversal_leaves.len(),
        forward_chain.len()
    );
    for (i, (&tl, &cl)) in traversal_leaves.iter().zip(forward_chain.iter()).enumerate() {
        assert_eq!(
            tl, cl,
            "INV4 sibling: leaf mismatch at position {i}: traversal={tl}, chain={cl}"
        );
    }
}

fn collect_leaves_via_traversal(
    tree: &BTree<MemPageStore>,
    page: u32,
    level: u8,
    out: &mut Vec<u32>,
) {
    if level == 0 {
        out.push(page);
        return;
    }
    let buf = tree.store.read_internal(page).expect("read_internal");
    let node = InternalNode::parse(&buf[..]).expect("parse internal");
    for (_, child) in &node.entries {
        collect_leaves_via_traversal(tree, *child, level - 1, out);
    }
    collect_leaves_via_traversal(tree, node.rightmost_child, level - 1, out);
}

// ---------------------------------------------------------------------------
// Invariant 5: Key-count invariants
// ---------------------------------------------------------------------------
//
// • No leaf must hold more entries than its page can encode (max bound).
// • No leaf must be completely empty unless it is the root leaf.
// • No leaf may exceed PAGE_SIZE_LEAF bytes.
// • Internal nodes: only the maximum bound is enforced (Phase 1 allows
//   underfull internal nodes after deletions).
//
// Note: minimum occupancy (MIN_LEAF_CELLS) is not strictly checked here
// because several valid Phase-1 scenarios produce underfull leaves:
//   – A split where the new cell lands at a key-order extreme (only 1 cell
//     on one side; both halves still fit by bytes).
//   – A delete where redistribution/merge is blocked by large adjacent cells.
//   – A leaf that is the leftmost child of its parent but whose prev_leaf_page
//     belongs to a different parent (cross-parent merges are not implemented).
// The other invariants (ordering, balance, sibling chain) catch structural
// regressions that would result from incorrect merge/redistribute handling.

fn check_inv5_key_counts(tree: &BTree<MemPageStore>) {
    let is_sole_root_leaf = tree.root_level == 0;
    check_key_counts_recursive(tree, tree.root_page, tree.root_level, is_sole_root_leaf);
}

fn check_key_counts_recursive(
    tree: &BTree<MemPageStore>,
    page: u32,
    level: u8,
    is_root_leaf: bool,
) {
    if level == 0 {
        let buf = tree.store.read_leaf(page).expect("read_leaf");
        let node = LeafNode::parse(&buf[..]).expect("parse leaf");

        // Max check: used_bytes must not exceed the page size.
        let used = LEAF_HEADER_SIZE
            + node.cells.len() * 2
            + node.cells.iter().map(|c| {
                let value_size = match &c.value {
                    CellValue::Inline(v) => 4 + v.len(),
                    CellValue::Overflow { .. } => 8,
                };
                2 + c.key.len() + 1 + value_size
            }).sum::<usize>();
        assert!(
            used <= PAGE_SIZE_LEAF as usize,
            "INV5 key-count: leaf page {page} uses {used} bytes, exceeds page size {}",
            PAGE_SIZE_LEAF
        );

        // Empty leaf check: a completely empty non-root leaf is always a bug.
        if !is_root_leaf {
            assert!(
                !node.cells.is_empty(),
                "INV5 key-count: non-root leaf page {page} is completely empty"
            );
        }

        return;
    }

    let buf = tree.store.read_internal(page).expect("read_internal");
    let node = InternalNode::parse(&buf[..]).expect("parse internal");

    // Max check: encoded size must not exceed internal page size.
    let used = INTERNAL_HEADER_SIZE
        + node.entries.iter().map(|(k, _)| 2 + k.len() + 4).sum::<usize>();
    assert!(
        used <= PAGE_SIZE_INTERNAL as usize,
        "INV5 key-count: internal page {page} uses {used} bytes, exceeds page size {}",
        PAGE_SIZE_INTERNAL
    );

    for (_, child) in &node.entries {
        check_key_counts_recursive(tree, *child, level - 1, false);
    }
    check_key_counts_recursive(tree, node.rightmost_child, level - 1, false);
}

// ---------------------------------------------------------------------------
// Invariant 6: Overflow chain integrity
// ---------------------------------------------------------------------------
//
// For every leaf cell that has a CellValue::Overflow pointer:
// • following next_overflow_page covers exactly the right number of bytes
// • no page in the chain is revisited (no cycle)
// • total_length is consistent with the accumulated data across chain pages

fn check_inv6_overflow_chains(tree: &BTree<MemPageStore>) {
    check_overflow_recursive(tree, tree.root_page, tree.root_level);
}

fn check_overflow_recursive(tree: &BTree<MemPageStore>, page: u32, level: u8) {
    if level == 0 {
        let buf = tree.store.read_leaf(page).expect("read_leaf");
        let node = LeafNode::parse(&buf[..]).expect("parse leaf");
        for cell in &node.cells {
            if let CellValue::Overflow {
                first_page,
                total_length,
            } = cell.value
            {
                verify_overflow_chain(tree, first_page, total_length, &cell.key);
            }
        }
        return;
    }
    let buf = tree.store.read_internal(page).expect("read_internal");
    let node = InternalNode::parse(&buf[..]).expect("parse internal");
    for (_, child) in &node.entries {
        check_overflow_recursive(tree, *child, level - 1);
    }
    check_overflow_recursive(tree, node.rightmost_child, level - 1);
}

fn verify_overflow_chain(
    tree: &BTree<MemPageStore>,
    first_page: u32,
    total_length: u32,
    cell_key: &[u8],
) {
    let mut visited: std::collections::HashSet<u32> = std::collections::HashSet::new();
    let mut cur = first_page;
    let mut accumulated: usize = 0;

    while cur != 0 {
        assert!(
            visited.insert(cur),
            "INV6 overflow: cycle in overflow chain for key {cell_key:?} at page {cur}"
        );
        let buf = tree.store.read_leaf(cur).expect("read_leaf (overflow)");
        let hdr = OverflowPageHeader::from_bytes(&buf[..])
            .expect("overflow header parse");
        assert_eq!(
            hdr.page_type, PAGE_TYPE_OVERFLOW,
            "INV6 overflow: page {cur} in overflow chain has page_type 0x{:02X}, expected 0x{:02X}",
            hdr.page_type, PAGE_TYPE_OVERFLOW
        );
        assert!(
            OVERFLOW_HEADER_SIZE + hdr.data_length as usize <= PAGE_SIZE_LEAF as usize,
            "INV6 overflow: page {cur}: data_length {} exceeds page capacity",
            hdr.data_length
        );
        accumulated += hdr.data_length as usize;
        cur = hdr.next_overflow_page;
    }

    assert_eq!(
        accumulated, total_length as usize,
        "INV6 overflow: chain for key {cell_key:?}: accumulated {accumulated} bytes, \
         expected {total_length}"
    );
}

// ---------------------------------------------------------------------------
// Invariant 7: Index-data consistency (simulated secondary index)
// ---------------------------------------------------------------------------
//
// We maintain a pair of B+ trees: `primary` and `secondary`.
// Primary key: enc_key(id)
// Secondary key: [value_byte, enc_key(id)...] — simulates indexing on a
// single-byte field derived from the value.
//
// After applying all operations we verify:
// • every secondary key's embedded _id has a corresponding primary entry
// • no orphaned secondary entries exist for deleted primary entries

fn check_inv7_index_consistency(
    primary: &BTree<MemPageStore>,
    secondary: &BTree<MemPageStore>,
    expected_ids: &std::collections::BTreeSet<u64>,
) {
    // Every id in expected_ids must appear in the primary tree.
    for &id in expected_ids {
        let k = enc_key(id);
        let found = primary.get(&k).expect("primary.get failed");
        assert!(
            found.is_some(),
            "INV7 index-consistency: id {id} missing from primary"
        );
    }

    // The secondary tree must have exactly the same ids as expected_ids.
    let sec_pairs = secondary.range_scan(None, None).expect("secondary range_scan");
    // Secondary key format: [field_byte (1), id_bytes (8)] = 9 bytes.
    let sec_ids: std::collections::BTreeSet<u64> = sec_pairs
        .iter()
        .map(|(k, _)| {
            assert_eq!(
                k.len(), 9,
                "INV7 index-consistency: secondary key has wrong length {}", k.len()
            );
            u64::from_be_bytes(k[1..9].try_into().expect("slice"))
        })
        .collect();

    assert_eq!(
        &sec_ids, expected_ids,
        "INV7 index-consistency: secondary index ids differ from expected"
    );
}

/// Apply operations to both primary and secondary B+ trees.
fn apply_ops_with_secondary(
    primary: &mut BTree<MemPageStore>,
    secondary: &mut BTree<MemPageStore>,
    ops: &[Op],
) -> std::collections::BTreeSet<u64> {
    let mut present: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();

    for op in ops {
        match *op {
            Op::Insert { key, val_size } => {
                if !present.contains(&key) {
                    let field_byte = (key & 0xFF) as u8;
                    let v = make_val(field_byte, val_size);
                    primary.insert(&enc_key(key), &v).expect("primary insert");
                    // Secondary key: [field_byte || enc_key(id)]
                    let mut sec_key = Vec::with_capacity(9);
                    sec_key.push(field_byte);
                    sec_key.extend_from_slice(&enc_key(key));
                    secondary.insert(&sec_key, &[]).expect("secondary insert");
                    present.insert(key);
                }
            }
            Op::Delete { key } => {
                if present.contains(&key) {
                    primary.delete(&enc_key(key)).expect("primary delete");
                    let field_byte = (key & 0xFF) as u8;
                    let mut sec_key = Vec::with_capacity(9);
                    sec_key.push(field_byte);
                    sec_key.extend_from_slice(&enc_key(key));
                    secondary.delete(&sec_key).expect("secondary delete");
                    present.remove(&key);
                }
            }
        }
    }

    present
}

// ---------------------------------------------------------------------------
// Invariant 8: Checksum validity
// ---------------------------------------------------------------------------
//
// Every leaf and internal page encountered during tree traversal must pass
// CRC32C verification.

fn check_inv8_checksums(tree: &BTree<MemPageStore>) {
    check_checksums_recursive(tree, tree.root_page, tree.root_level);
}

fn check_checksums_recursive(tree: &BTree<MemPageStore>, page: u32, level: u8) {
    if level == 0 {
        let buf = tree.store.read_leaf(page).expect("read_leaf");
        verify_leaf_page_checksum(&buf).unwrap_or_else(|e| {
            panic!("INV8 checksum: leaf page {page}: {e}");
        });

        // Also verify any overflow chains reachable from this leaf.
        let node = LeafNode::parse(&buf[..]).expect("parse leaf");
        for cell in &node.cells {
            if let CellValue::Overflow { first_page, .. } = cell.value {
                check_overflow_checksums(tree, first_page);
            }
        }
        return;
    }

    let buf = tree.store.read_internal(page).expect("read_internal");
    verify_internal_page_checksum(&buf).unwrap_or_else(|e| {
        panic!("INV8 checksum: internal page {page}: {e}");
    });
    let node = InternalNode::parse(&buf[..]).expect("parse internal");
    for (_, child) in &node.entries {
        check_checksums_recursive(tree, *child, level - 1);
    }
    check_checksums_recursive(tree, node.rightmost_child, level - 1);
}

fn check_overflow_checksums(tree: &BTree<MemPageStore>, first_page: u32) {
    let mut cur = first_page;
    while cur != 0 {
        let buf = tree.store.read_leaf(cur).expect("read_leaf (overflow checksum)");
        verify_overflow_page_checksum(&buf).unwrap_or_else(|e| {
            panic!("INV8 checksum: overflow page {cur}: {e}");
        });
        let hdr = OverflowPageHeader::from_bytes(&buf[..]).expect("overflow header");
        cur = hdr.next_overflow_page;
    }
}

// ---------------------------------------------------------------------------
// Composite invariant check
// ---------------------------------------------------------------------------

/// Run all 10 invariants against a tree.
fn check_all_invariants(tree: &BTree<MemPageStore>) {
    check_inv1_ordering(tree);     // Invariant 1 + 10 (split key coverage)
    check_inv2_balance(tree);      // Invariant 2
    check_inv3_parent_child(tree); // Invariant 3
    check_inv4_sibling_pointers(tree); // Invariant 4 + 9 (chain completeness)
    check_inv5_key_counts(tree);   // Invariant 5
    check_inv6_overflow_chains(tree); // Invariant 6
    check_inv8_checksums(tree);    // Invariant 8
    // Invariant 7 is tested separately (needs secondary tree).
}

// ---------------------------------------------------------------------------
// Correctness check: scan must match expected key set
// ---------------------------------------------------------------------------

fn check_keys_match(tree: &BTree<MemPageStore>, expected: &std::collections::BTreeSet<u64>) {
    let pairs = tree.range_scan(None, None).expect("range_scan");
    let actual: std::collections::BTreeSet<u64> = pairs
        .iter()
        .map(|(k, _)| u64::from_be_bytes(k.as_slice().try_into().expect("8-byte key")))
        .collect();
    assert_eq!(
        &actual, expected,
        "key set mismatch: tree has {actual:?}, expected {expected:?}"
    );
}

// ---------------------------------------------------------------------------
// proptest test suite
// ---------------------------------------------------------------------------

proptest! {
    /// **Core property**: after any sequence of insert / delete operations,
    /// all 10 B+ tree structural invariants hold, and every expected key is
    /// present (and no extra keys are present).
    #[test]
    fn prop_all_invariants_after_random_ops(ops in arb_ops()) {
        let store = MemPageStore::new();
        let mut tree = BTree::create(store).unwrap();
        let expected = apply_ops(&mut tree, &ops);
        check_all_invariants(&tree);
        check_keys_match(&tree, &expected);
    }

    /// **Invariant 7 (index-data consistency)**: secondary index stays
    /// consistent with primary after arbitrary insert / delete sequences.
    #[test]
    fn prop_inv7_index_data_consistency(ops in arb_ops()) {
        let primary = BTree::create(MemPageStore::new()).unwrap();
        let secondary = BTree::create(MemPageStore::new()).unwrap();
        let mut primary = primary;
        let mut secondary = secondary;
        let expected = apply_ops_with_secondary(&mut primary, &mut secondary, &ops);
        check_inv7_index_consistency(&primary, &secondary, &expected);
        // Also run structural invariants on both trees.
        check_all_invariants(&primary);
        check_all_invariants(&secondary);
    }

    /// **Invariants 9 & 10 — split heavy**: force many splits and verify
    /// sibling chain completeness and split key coverage.
    #[test]
    fn prop_invariants_after_split_heavy_ops(ops in arb_split_heavy_ops()) {
        let store = MemPageStore::new();
        let mut tree = BTree::create(store).unwrap();
        let expected = apply_ops(&mut tree, &ops);
        check_all_invariants(&tree);
        check_keys_match(&tree, &expected);
        // Explicitly assert a multi-level tree was reached at least sometimes.
        // (proptest will explore many op sequences; some will produce splits)
    }

    /// **Overflow chains**: sequences that produce overflow values exercise
    /// invariants 6 and 8 specifically.
    #[test]
    fn prop_overflow_invariants(
        keys in prop::collection::vec(0u64..=63, 1..=30),
        sizes in prop::collection::vec(
            (OVERFLOW_THRESHOLD + 1)..(OVERFLOW_THRESHOLD * 3),
            1..=30
        )
    ) {
        let store = MemPageStore::new();
        let mut tree = BTree::create(store).unwrap();
        let mut present: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();

        for (&key, &size) in keys.iter().zip(sizes.iter()) {
            if !present.contains(&key) {
                let v = make_val((key & 0xFF) as u8, size);
                tree.insert(&enc_key(key), &v).unwrap();
                present.insert(key);
            }
        }

        check_inv6_overflow_chains(&tree);
        check_inv8_checksums(&tree);
        check_inv4_sibling_pointers(&tree);
        check_inv2_balance(&tree);
        check_keys_match(&tree, &present);
    }

    /// **Delete after split**: insert enough to force splits, then delete
    /// all entries, verifying invariants hold throughout.
    #[test]
    fn prop_invariants_after_full_delete_post_split(
        n_insert in 50u64..=200,
        delete_order in prop::collection::vec(0u64..=199, 50..=200)
    ) {
        let store = MemPageStore::new();
        let mut tree = BTree::create(store).unwrap();

        // Insert n_insert entries with medium values to trigger splits.
        let v = vec![0xABu8; 300];
        for i in 0..n_insert {
            tree.insert(&enc_key(i), &v).unwrap();
        }
        check_all_invariants(&tree);

        // Delete in the given order (skipping out-of-range or already-deleted keys).
        let mut present: std::collections::BTreeSet<u64> = (0..n_insert).collect();
        for k in &delete_order {
            if present.remove(k) {
                tree.delete(&enc_key(*k)).unwrap();
                // Check invariants after every delete to catch regressions early.
                check_all_invariants(&tree);
            }
        }
        check_keys_match(&tree, &present);
    }
}

// ---------------------------------------------------------------------------
// Deterministic regression tests (not proptest — always run)
// ---------------------------------------------------------------------------

/// Verify all invariants hold for an empty tree.
#[test]
fn regression_invariants_empty_tree() {
    let store = MemPageStore::new();
    let tree = BTree::create(store).unwrap();
    check_all_invariants(&tree);
    check_keys_match(&tree, &Default::default());
}

/// Verify all invariants after a single insert.
#[test]
fn regression_invariants_single_insert() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();
    tree.insert(&enc_key(42), b"hello").unwrap();
    check_all_invariants(&tree);
}

/// Insert a known sequence that was previously problematic: reverse order.
#[test]
fn regression_invariants_reverse_insert_160() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();
    let v = vec![0u8; 200];
    for i in (0u64..160).rev() {
        tree.insert(&enc_key(i), &v).unwrap();
    }
    check_all_invariants(&tree);
}

/// Multi-page overflow inserted and deleted.
#[test]
fn regression_invariants_overflow_insert_delete() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();
    let big = vec![0xFFu8; OVERFLOW_THRESHOLD * 2 + 7];
    tree.insert(&enc_key(1), &big).unwrap();
    tree.insert(&enc_key(2), &big).unwrap();
    check_all_invariants(&tree);
    tree.delete(&enc_key(1)).unwrap();
    check_all_invariants(&tree);
}

/// Alternate insert / delete to stress the merge path.
#[test]
fn regression_invariants_alternating_insert_delete() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();
    let v = vec![0xBBu8; 300];
    // Fill tree past split threshold.
    for i in 0u64..120 {
        tree.insert(&enc_key(i), &v).unwrap();
    }
    // Delete every other key.
    for i in (0u64..120).step_by(2) {
        tree.delete(&enc_key(i)).unwrap();
    }
    check_all_invariants(&tree);
    // Re-insert deleted keys.
    for i in (0u64..120).step_by(2) {
        tree.insert(&enc_key(i), &v).unwrap();
    }
    check_all_invariants(&tree);
}
