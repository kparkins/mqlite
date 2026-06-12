//! BUG-9 repro: `split_leaf` picks its split point by cell *count*
//! (`split_at = total / 2`, insert.rs), not by encoded bytes. Leaf cells are
//! variable-sized up to just under `OVERFLOW_THRESHOLD` (~30 KB inline), so a
//! count-based midpoint can route two near-threshold cells onto one side,
//! whose encoded size then exceeds `PAGE_SIZE_LEAF` and `LeafNode::encode`
//! fails with "leaf node too large". In production, splits run inside
//! checkpoint materialization, so this error aborts the structural batch and
//! poisons the engine.
//!
//! The delete path already solved the same problem byte-aware
//! (`choose_leaf_redistribution_split` rejects halves exceeding
//! `PAGE_SIZE_LEAF`), so a byte-feasible split point exists for this layout:
//! left = [tiny, big1] (~29.6 KB) and right = [big2] (~29.5 KB) both fit.

use super::*;

use crate::storage::page::{LEAF_HEADER_SIZE, PAGE_SIZE_LEAF};

/// Inline value size chosen so each big cell encodes to ~29.5 KB:
/// cell = key_len(2) + key(1) + value_type(1) + bson_len(4) + value.
/// Stays under `OVERFLOW_THRESHOLD` (30 KiB) so the value remains inline.
const BIG_VALUE_LEN: usize = 29_500;

// Test setup: big values must stay inline (under the overflow threshold).
const _: () = assert!(BIG_VALUE_LEN <= OVERFLOW_THRESHOLD);

#[test]
fn split_leaf_with_two_near_threshold_inline_cells_succeeds() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    let tiny = vec![0xAAu8; 16];
    let big1 = vec![0xBBu8; BIG_VALUE_LEN];
    let big2 = vec![0xCCu8; BIG_VALUE_LEN];

    // Leaf layout before the split: cells sorted as [a(tiny), b(big1)].
    tree.insert(b"a", &tiny).unwrap();
    tree.insert(b"b", &big1).unwrap();

    // Inserting "c" overflows the leaf and triggers split_leaf with cells
    // [a, b, c]. The count-based midpoint (3 / 2 = 1) routes [b, c] — about
    // 59 KB of encoded cells — into the right sibling, whose encode fails
    // even though the byte-aware split left=[a, b] / right=[c] fits.
    tree.insert(b"c", &big2)
        .expect("leaf split must choose a split point whose halves both fit in a 32KB page");

    // The tree must remain fully readable after the split.
    assert_eq!(tree.get(b"a").unwrap().as_deref(), Some(tiny.as_slice()));
    assert_eq!(tree.get(b"b").unwrap().as_deref(), Some(big1.as_slice()));
    assert_eq!(tree.get(b"c").unwrap().as_deref(), Some(big2.as_slice()));
}

// ---------------------------------------------------------------------------
// R3a repro: no feasible *single* cut — split must go multi-way.
// ---------------------------------------------------------------------------
//
// A byte-full leaf of small cells receiving one large inline cell (> ~16.4 KB)
// at a mid-range key has NO feasible single cut: whichever side takes the big
// cell also takes ~half of the small cells and exceeds `PAGE_SIZE_LEAF`.
// `choose_leaf_redistribution_split` returns `None` for every such layout;
// mapping that to `Error::Internal` aborts the checkpoint structural batch and
// poisons the engine — and because the journal tail still holds the insert,
// recovery re-folds and re-poisons on every restart. The split must instead
// fall back to packing the cells into as many new leaves as needed (every
// individual cell fits a page by construction) and promote one separator per
// leaf boundary.

/// Small-cell footprint: key "kNNNN" (5 bytes) + 100-byte inline value
/// encodes to `2 + 5 + 1 + 4 + 100 = 112` bytes, plus a 2-byte cell pointer.
const SMALL_VALUE_LEN: usize = 100;
const SMALL_CELL_FOOTPRINT: usize = 2 + 5 + 1 + 4 + SMALL_VALUE_LEN + 2;

/// Number of small cells that byte-fill one leaf: floor((PAGE_SIZE_LEAF -
/// LEAF_HEADER_SIZE) / SMALL_CELL_FOOTPRINT). One more would not fit.
const SMALLS_PER_FULL_LEAF: usize = 287;
const _: () = assert!(
    LEAF_HEADER_SIZE + SMALLS_PER_FULL_LEAF * SMALL_CELL_FOOTPRINT <= PAGE_SIZE_LEAF as usize
);
const _: () = assert!(
    LEAF_HEADER_SIZE + (SMALLS_PER_FULL_LEAF + 1) * SMALL_CELL_FOOTPRINT > PAGE_SIZE_LEAF as usize
);

/// Mid-range inline value in the critic's 17–30 KB band. A side holding this
/// cell has room for at most ~111 small cells, but a mid-leaf key leaves
/// ~143 small cells on each side, so no single cut is byte-feasible.
const BIG_INLINE_LEN: usize = 20_000;
const _: () = assert!(BIG_INLINE_LEN <= OVERFLOW_THRESHOLD);

fn small_key(n: usize) -> Vec<u8> {
    format!("k{n:04}").into_bytes()
}

fn fill_smalls(tree: &mut BTree<MemPageStore>, start: usize, count: usize) {
    let val = vec![0x11u8; SMALL_VALUE_LEN];
    for i in start..start + count {
        tree.insert(&small_key(i), &val).unwrap();
    }
}

#[test]
fn split_leaf_falls_back_to_multiway_when_no_single_cut_fits() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    // Byte-full root leaf: 287 small cells; remaining free space (30 bytes)
    // is below one more small cell.
    fill_smalls(&mut tree, 0, SMALLS_PER_FULL_LEAF);
    assert_eq!(tree.root_level, 0, "fill must not split the root leaf");

    // "k0143x" sorts between k0143 and k0144: 144 small cells to the left of
    // the big cell, 143 to the right — both sides exceed the ~111 small
    // cells that can share a page with the big cell, so every single cut
    // overflows one side.
    let big = vec![0xEEu8; BIG_INLINE_LEN];
    tree.insert(b"k0143x", &big)
        .expect("split must fall back to a multi-way split when no single cut fits");

    // All keys readable (each leaf was re-encoded on write, so reaching here
    // also proves every leaf fit its page).
    let small = vec![0x11u8; SMALL_VALUE_LEN];
    for i in 0..SMALLS_PER_FULL_LEAF {
        assert_eq!(
            tree.get(&small_key(i)).unwrap().as_deref(),
            Some(small.as_slice()),
            "small key {i} must survive the multi-way split"
        );
    }
    assert_eq!(tree.get(b"k0143x").unwrap().as_deref(), Some(big.as_slice()));

    // The leaf sibling chain is intact and ordered across the new leaves.
    let scanned = tree.range_scan(None, None).unwrap();
    assert_eq!(scanned.len(), SMALLS_PER_FULL_LEAF + 1);
    for w in scanned.windows(2) {
        assert!(
            w[0].0 < w[1].0,
            "scan must stay key-ordered across the new leaves"
        );
    }
}

// ---------------------------------------------------------------------------
// F19: multi-way promotions while the parent itself splits mid-absorb.
// ---------------------------------------------------------------------------
//
// `insert_promotions_into_internal` (insert.rs) re-routes each later
// promotion through the `out` array when an earlier promotion already split
// the parent: promotion #2 must land in `out[idx - 1].right_page`, not in the
// original `page`. The two tests above never execute that path non-trivially
// (their parents absorb every promotion without splitting), and a routing bug
// here wires a child pointer to the wrong page — silent read-path data loss.
// This test pins the path: the parent (the level-1 root) is byte-filled to
// within one promoted-separator footprint of `PAGE_SIZE_INTERNAL`, then a
// no-single-cut multi-way leaf split underneath it promotes TWO separators —
// the first forces `split_internal`, the second arrives while the split is
// in flight and must be re-routed into the parent's new right half.

/// Filler value size: one filler cell encodes to `2 + 6 + 1 + 4 + 7000 =
/// 7013` bytes (+2-byte pointer), so a leaf holds four and ascending inserts
/// split it on the fifth — each split promoting one 6-byte separator into
/// the root. Big filler values reach the root's byte budget in ~1k inserts.
const FILLER_VALUE_LEN: usize = 7_000;
const _: () = assert!(FILLER_VALUE_LEN <= OVERFLOW_THRESHOLD);

/// Byte-fill cells for the rightmost leaf: `2 + 6 + 1 + 4 + 100 = 113`
/// encoded bytes.
const Z_VALUE_LEN: usize = 100;
const Z_CELL_SIZE: usize = 2 + 6 + 1 + 4 + Z_VALUE_LEN;

/// Every promoted separator in phase C is a 6-byte leaf key plus the `'x'`
/// suffix (the big key) or a plain 6-byte leaf key, so the parent-fullness
/// stop condition keys off this length.
const PROMO_KEY_LEN: usize = 7;

/// Cells-before-the-big-cell byte band with NO feasible single cut: the side
/// taking the ~20 KB big cell fits at most ~12.7 KB of other cells, and a
/// prefix at most 19 KB leaves the other side over budget (total ~52.6 KB),
/// so `choose_leaf_redistribution_split` returns `None` and the split goes
/// multi-way (three groups, two promoted separators).
const NO_CUT_BAND: std::ops::RangeInclusive<usize> = 13_000..=19_000;

fn filler_key(n: usize) -> Vec<u8> {
    format!("a{n:05}").into_bytes()
}

fn z_key(n: usize) -> Vec<u8> {
    format!("z{n:05}").into_bytes()
}

/// Recursively assert every separator/key respects the `[lo, hi)` range its
/// parent separators induce (each promoted separator's adjacent children
/// contain exactly the expected key ranges), levels are consistent, and leaf
/// cells stay sorted. Returns the subtree's key count.
fn assert_subtree_key_ranges(
    tree: &BTree<MemPageStore>,
    page: u32,
    level: u8,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> usize {
    fn in_range(k: &[u8], lo: Option<&[u8]>, hi: Option<&[u8]>) -> bool {
        lo.map_or(true, |lo| lo <= k) && hi.map_or(true, |hi| k < hi)
    }
    if level == 0 {
        let (buf, _) = tree.store.read_leaf(page).unwrap();
        let node = LeafNode::parse(&buf[..]).unwrap();
        for w in node.cells.windows(2) {
            assert!(w[0].key < w[1].key, "leaf cells must stay key-sorted");
        }
        for cell in &node.cells {
            assert!(
                in_range(&cell.key, lo, hi),
                "leaf key {:?} escaped its separator-bounded range [{lo:?}, {hi:?})",
                cell.key
            );
        }
        return node.cells.len();
    }
    let buf = tree.store.read_internal(page).unwrap();
    let node = InternalNode::parse(&buf[..]).unwrap();
    assert_eq!(node.level, level, "internal level must match its depth");
    let mut count = 0;
    let mut child_lo = lo;
    for (sep, child) in &node.entries {
        assert!(
            in_range(sep, lo, hi),
            "separator {sep:?} escaped its range [{lo:?}, {hi:?})"
        );
        count += assert_subtree_key_ranges(tree, *child, level - 1, child_lo, Some(sep));
        child_lo = Some(sep);
    }
    count += assert_subtree_key_ranges(tree, node.rightmost_child, level - 1, child_lo, hi);
    count
}

#[test]
fn multiway_promotions_reroute_into_the_parents_split_right_half() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    // Phase A: ascending filler inserts grow a level-1 root and byte-fill it
    // until it can no longer absorb one more promoted separator. Each filler
    // split promotes a 6-byte separator (footprint 12), and the loop only
    // inserts while a 7-byte separator (footprint 13) still fits, so filler
    // splits can never overflow the root themselves.
    let filler_val = vec![0x44u8; FILLER_VALUE_LEN];
    let mut fillers = 0usize;
    loop {
        assert!(fillers < 10_000, "filler loop failed to byte-fill the root");
        if tree.root_level == 1 {
            let buf = tree.store.read_internal(tree.root_page).unwrap();
            let root = InternalNode::parse(&buf[..]).unwrap();
            if !root.can_insert(PROMO_KEY_LEN) {
                break;
            }
        }
        assert!(
            tree.root_level <= 1,
            "the root must not grow past level 1 while filling"
        );
        tree.insert(&filler_key(fillers), &filler_val).unwrap();
        fillers += 1;
    }
    assert_eq!(tree.root_level, 1, "fill must leave a level-1 internal root");

    // Phase B: byte-fill the rightmost leaf (whose keys sort above every
    // existing root separator) with small cells, stopping before any split
    // so the root stays exactly as full as phase A left it.
    let zval = vec![0x55u8; Z_VALUE_LEN];
    let mut zs = 0usize;
    loop {
        assert!(zs < 1_000, "z-fill failed to byte-fill the rightmost leaf");
        let key = z_key(zs);
        let page = tree.find_leaf(&key).unwrap();
        let (buf, _) = tree.store.read_leaf(page).unwrap();
        let node = LeafNode::parse(&buf[..]).unwrap();
        if !node.can_insert(Z_CELL_SIZE) {
            break;
        }
        tree.insert(&key, &zval).unwrap();
        zs += 1;
    }

    // Phase C: place the ~20 KB inline cell so the bytes before it fall in
    // the no-single-cut band, forcing the three-group multi-way layout.
    let leaf_page = tree.find_leaf(&z_key(0)).unwrap();
    let (buf, _) = tree.store.read_leaf(leaf_page).unwrap();
    let leaf = LeafNode::parse(&buf[..]).unwrap();
    let mut prefix = 0usize;
    let mut big_key = None;
    for (idx, cell) in leaf.cells.iter().enumerate() {
        if idx > 0 && NO_CUT_BAND.contains(&prefix) {
            let mut k = leaf.cells[idx - 1].key.clone();
            k.push(b'x');
            big_key = Some(k);
            break;
        }
        prefix += cell.encoded_size() + 2;
    }
    let big_key = big_key.expect("a cell boundary inside the no-single-cut band must exist");
    assert_eq!(big_key.len(), PROMO_KEY_LEN, "stop condition matched promo");

    let root_page_before = tree.root_page;
    let buf = tree.store.read_internal(root_page_before).unwrap();
    let root_before = InternalNode::parse(&buf[..]).unwrap();
    let seps_before: std::collections::BTreeSet<Vec<u8>> = root_before
        .entries
        .iter()
        .map(|(k, _)| k.clone())
        .collect();
    assert!(
        !root_before.can_insert(big_key.len()),
        "precondition: the parent must be unable to absorb the first promoted \
         separator, so it splits mid-absorb"
    );

    let big = vec![0xEEu8; BIG_INLINE_LEN];
    tree.insert(&big_key, &big).expect(
        "multi-way split must re-route later promotions when the parent splits mid-absorb",
    );

    // The parent split mid-absorb: the root grew one level, carrying exactly
    // the parent split's promoted median (an old separator).
    assert_eq!(
        tree.root_level, 2,
        "the parent must have split while absorbing the multi-way promotions"
    );
    let buf = tree.store.read_internal(tree.root_page).unwrap();
    let new_root = InternalNode::parse(&buf[..]).unwrap();
    assert_eq!(new_root.level, 2);
    assert_eq!(
        new_root.entries.len(),
        1,
        "exactly one parent split must reach the root-growth path"
    );
    let median = new_root.entries[0].0.clone();
    assert_eq!(
        new_root.entries[0].1, root_page_before,
        "the old root must stay the grown root's left child"
    );
    assert!(
        seps_before.contains(&median),
        "the grown root's separator must be the parent split's promoted median, \
         not one of the multi-way separators"
    );

    // >= 2 promotions were in flight, and both sort above the parent's
    // median, so both must live in the parent's RIGHT half — promotion #2
    // can only have arrived there through the out-array re-route.
    let left_buf = tree.store.read_internal(root_page_before).unwrap();
    let left_half = InternalNode::parse(&left_buf[..]).unwrap();
    let right_buf = tree.store.read_internal(new_root.rightmost_child).unwrap();
    let right_half = InternalNode::parse(&right_buf[..]).unwrap();
    let new_in_left: Vec<&Vec<u8>> = left_half
        .entries
        .iter()
        .map(|(k, _)| k)
        .filter(|k| !seps_before.contains(*k))
        .collect();
    assert!(
        new_in_left.is_empty(),
        "no multi-way separator may land in the parent's left half: {new_in_left:?}"
    );
    let new_in_right: Vec<&Vec<u8>> = right_half
        .entries
        .iter()
        .map(|(k, _)| k)
        .filter(|k| !seps_before.contains(*k) && **k != median)
        .collect();
    assert_eq!(
        new_in_right.len(),
        2,
        "the no-single-cut layout must promote exactly two separators \
         (>= 2 promotions in flight), got {new_in_right:?}"
    );
    assert!(
        new_in_right.iter().any(|k| **k == big_key),
        "the big cell's key must be the first promoted separator"
    );
    let follow_up = new_in_right
        .iter()
        .find(|k| ***k != big_key)
        .expect("two new separators");
    assert!(
        follow_up.as_slice() > big_key.as_slice(),
        "the re-routed promotion must sort after the big key"
    );

    // Each promoted separator's adjacent children hold the expected ranges:
    // the left child's keys all precede the separator and the right child
    // starts exactly at it (separators are first keys of right groups).
    for sep in [&big_key, *follow_up] {
        let i = right_half
            .entries
            .iter()
            .position(|(k, _)| k == sep)
            .expect("promoted separator present in the parent's right half");
        let (lbuf, _) = tree.store.read_leaf(right_half.entries[i].1).unwrap();
        let lnode = LeafNode::parse(&lbuf[..]).unwrap();
        assert!(
            lnode.cells.last().expect("non-empty left group").key < *sep,
            "keys left of separator {sep:?} must precede it"
        );
        let (rbuf, _) = tree.store.read_leaf(right_half.child_at(i + 1)).unwrap();
        let rnode = LeafNode::parse(&rbuf[..]).unwrap();
        assert_eq!(
            &rnode.cells.first().expect("non-empty right group").key,
            sep,
            "each promoted separator must be the first key of its right child"
        );
    }

    // Full-tree walk: every separator-bounded range holds, and no key was
    // lost or duplicated by the re-route.
    let walked = assert_subtree_key_ranges(&tree, tree.root_page, tree.root_level, None, None);
    assert_eq!(walked, fillers + zs + 1, "walk must see every inserted key");

    // Every key stays readable through the normal read path.
    for i in 0..fillers {
        assert_eq!(
            tree.get(&filler_key(i)).unwrap().as_deref(),
            Some(filler_val.as_slice()),
            "filler key {i} must survive the parent split"
        );
    }
    for i in 0..zs {
        assert_eq!(
            tree.get(&z_key(i)).unwrap().as_deref(),
            Some(zval.as_slice()),
            "z key {i} must survive the parent split"
        );
    }
    assert_eq!(tree.get(&big_key).unwrap().as_deref(), Some(big.as_slice()));

    let scanned = tree.range_scan(None, None).unwrap();
    assert_eq!(scanned.len(), fillers + zs + 1);
    for w in scanned.windows(2) {
        assert!(
            w[0].0 < w[1].0,
            "scan must stay key-ordered across the re-routed leaves"
        );
    }
}

#[test]
fn multiway_split_under_internal_parent_promotes_every_separator() {
    let store = MemPageStore::new();
    let mut tree = BTree::create(store).unwrap();

    // Phase 1: same setup as above — the multi-way split grows an internal
    // root above the new leaves.
    fill_smalls(&mut tree, 0, SMALLS_PER_FULL_LEAF);
    let big1 = vec![0xEEu8; BIG_INLINE_LEN];
    tree.insert(b"k0143x", &big1)
        .expect("split must fall back to a multi-way split when no single cut fits");
    assert_eq!(tree.root_level, 1, "multi-way split must have grown the root");

    // Phase 2: refill the rightmost leaf with ascending small keys until it
    // is byte-full again (stop before the insert that would split it).
    let small = vec![0x11u8; SMALL_VALUE_LEN];
    let mut n = SMALLS_PER_FULL_LEAF;
    loop {
        let page = tree.find_leaf(&small_key(n)).unwrap();
        let (buf, _) = tree.store.read_leaf(page).unwrap();
        let node = LeafNode::parse(&buf[..]).unwrap();
        if !node.can_insert(SMALL_CELL_FOOTPRINT - 2) {
            break;
        }
        tree.insert(&small_key(n), &small).unwrap();
        n += 1;
    }

    // Phase 3: insert a big cell at the byte-full leaf's median key, so the
    // multi-way promotions flow into the *existing* internal parent rather
    // than the root-growth path.
    let page = tree.find_leaf(&small_key(n)).unwrap();
    let (buf, _) = tree.store.read_leaf(page).unwrap();
    let node = LeafNode::parse(&buf[..]).unwrap();
    assert!(
        node.cells.len() >= SMALLS_PER_FULL_LEAF,
        "refilled leaf must be byte-full of small cells"
    );
    let mut big_key = node.cells[node.cells.len() / 2].key.clone();
    big_key.push(b'x');

    let big2 = vec![0xDDu8; BIG_INLINE_LEN];
    tree.insert(&big_key, &big2)
        .expect("multi-way split must promote every separator into the existing parent");
    assert_eq!(
        tree.root_level, 1,
        "parent had room for the promoted separators; the root must not grow"
    );

    // Every key — both phases of smalls and both big cells — stays readable.
    for i in 0..n {
        assert_eq!(
            tree.get(&small_key(i)).unwrap().as_deref(),
            Some(small.as_slice()),
            "small key {i} must survive the multi-way split"
        );
    }
    assert_eq!(tree.get(b"k0143x").unwrap().as_deref(), Some(big1.as_slice()));
    assert_eq!(tree.get(&big_key).unwrap().as_deref(), Some(big2.as_slice()));

    let scanned = tree.range_scan(None, None).unwrap();
    assert_eq!(scanned.len(), n + 2);
    for w in scanned.windows(2) {
        assert!(
            w[0].0 < w[1].0,
            "scan must stay key-ordered across the new leaves"
        );
    }
}
