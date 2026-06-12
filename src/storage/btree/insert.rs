//! B+ tree insert path: recursive descent, leaf split, and internal
//! node split with key promotion to the parent.

use crate::error::{Error, Result};
use crate::storage::page::{LEAF_HEADER_SIZE, PAGE_SIZE_LEAF};

use super::node::{InternalNode, LeafCell, LeafNode, SplitResult};
use super::overflow::{free_overflow_chain, write_overflow_chain};
use super::store::BTreePageStore;
use super::{BTree, CellValue, OVERFLOW_THRESHOLD};

impl<S: BTreePageStore> BTree<S> {
    /// Insert `key` → `value` into the tree.
    ///
    /// If `value.len() > OVERFLOW_THRESHOLD`, the value is written to an
    /// overflow chain automatically and the leaf cell holds a pointer.
    ///
    /// Returns `Err(Error::DuplicateKey)` if the key already exists.
    pub(crate) fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let cell_value = if value.len() > OVERFLOW_THRESHOLD {
            let first_page = write_overflow_chain(&mut self.store, value)?;
            CellValue::Overflow {
                first_page,
                total_length: value.len() as u32,
            }
        } else {
            CellValue::Inline(value.to_vec())
        };

        let mut splits = self.insert_subtree(self.root_page, self.root_level, key, cell_value)?;

        // The root split: a multi-way leaf split can promote more than one
        // separator, so grow a new root from the first promotion and insert
        // the remaining ones into it. If the new root itself splits while
        // absorbing them (pathologically large keys), loop to grow another
        // level; each iteration consumes one promotion, so this terminates.
        while !splits.is_empty() {
            let first = splits.remove(0);
            let new_root = self.store.alloc_internal()?;
            let new_level = self.root_level + 1;
            let new_node = InternalNode {
                level: new_level,
                entries: vec![(first.promoted_key, self.root_page)],
                rightmost_child: first.right_page,
            };
            let buf = new_node.encode()?;
            self.store.write_internal(new_root, &buf)?;
            self.root_page = new_root;
            self.root_level = new_level;
            splits = self.insert_promotions_into_internal(new_root, splits)?;
        }

        Ok(())
    }

    /// Replace the value for an existing key without changing tree shape.
    ///
    /// Returns `Ok(false)` when the key is absent. This is used by checkpoint
    /// materialization to fold a resident MVCC head over a stale base cell
    /// without running the delete path first; the delete path can rebalance or
    /// merge leaves mid-fold. The replaced value's overflow chain is freed
    /// through the same path `delete_from_leaf` uses.
    pub(crate) fn replace_existing(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        let leaf_page = self.find_leaf(key)?;
        let (buf, _) = self.store.read_leaf(leaf_page)?;
        let mut node = LeafNode::parse(&buf[..])?;
        let idx = match node.binary_search(key) {
            Ok(idx) => idx,
            // Check existence before writing the new overflow chain so a miss
            // does not orphan freshly written overflow pages.
            Err(_) => return Ok(false),
        };

        let cell_value = if value.len() > OVERFLOW_THRESHOLD {
            let first_page = write_overflow_chain(&mut self.store, value)?;
            CellValue::Overflow {
                first_page,
                total_length: value.len() as u32,
            }
        } else {
            CellValue::Inline(value.to_vec())
        };

        // Free the replaced value's overflow chain if present, matching
        // delete_from_leaf.
        let old_value = std::mem::replace(&mut node.cells[idx].value, cell_value);
        if let CellValue::Overflow { first_page, .. } = old_value {
            free_overflow_chain(&mut self.store, first_page)?;
        }
        let encoded = node.encode()?;
        self.store.write_leaf_structural(leaf_page, &encoded)?;
        Ok(true)
    }

    /// Recursive insert into the subtree rooted at `page` (at `level`).
    ///
    /// Returns the splits of the node at `page` (empty when it did not
    /// split), ascending by promoted key. A node yields more than one split
    /// only on the multi-way leaf path.
    fn insert_subtree(
        &mut self,
        page: u32,
        level: u8,
        key: &[u8],
        value: CellValue,
    ) -> Result<Vec<SplitResult>> {
        if level == 0 {
            return self.insert_leaf(page, key, value);
        }

        // Internal node.
        #[cfg(any(test, feature = "test-hooks"))]
        crate::storage::close_quadratic_probe::record_descent_internal_reads(1);
        let buf = self.store.read_internal(page)?;
        let node = InternalNode::parse(&buf[..])?;
        let child_idx = node.find_child_idx(key);
        let child_page = node.child_at(child_idx);

        let child_splits = self.insert_subtree(child_page, level - 1, key, value)?;

        if child_splits.is_empty() {
            Ok(Vec::new())
        } else {
            self.insert_promotions_into_internal(page, child_splits)
        }
    }

    /// Insert one or more child-split promotions into the internal node at
    /// `page`, routing each promotion to whichever node covers its key as
    /// `page` itself splits.
    ///
    /// `promotions` must be ascending by key (the new right siblings of a
    /// single child split). Returns the splits of `page` — also ascending —
    /// for the caller to propagate upward.
    fn insert_promotions_into_internal(
        &mut self,
        page: u32,
        promotions: Vec<SplitResult>,
    ) -> Result<Vec<SplitResult>> {
        // Nodes at this level produced by splitting `page`, ascending by
        // separator: `page` covers keys below out[0], and out[i].right_page
        // covers keys in [out[i].promoted_key, out[i + 1].promoted_key).
        let mut out: Vec<SplitResult> = Vec::new();
        for sr in promotions {
            let idx = out
                .partition_point(|s| s.promoted_key.as_slice() <= sr.promoted_key.as_slice());
            let target = if idx == 0 { page } else { out[idx - 1].right_page };
            if let Some(split) =
                self.insert_into_internal(target, sr.promoted_key, sr.right_page)?
            {
                // The new separator lies inside `target`'s key range, so
                // position `idx` keeps `out` sorted.
                out.insert(idx, split);
            }
        }
        Ok(out)
    }

    /// Insert a key–value cell into the leaf at `page`.
    ///
    /// If the leaf is full, split it and return one [`SplitResult`] per new
    /// right sibling (usually one; more when no single cut is byte-feasible).
    fn insert_leaf(
        &mut self,
        page: u32,
        key: &[u8],
        value: CellValue,
    ) -> Result<Vec<SplitResult>> {
        let (buf, _) = self.store.read_leaf(page)?;
        let mut node = LeafNode::parse(&buf[..])?;

        let new_cell = LeafCell {
            key: key.to_vec(),
            value,
        };
        let cell_size = new_cell.encoded_size();

        let insert_pos = match node.binary_search(key) {
            Ok(_) => {
                return Err(Error::DuplicateKey {
                    detail: format!("key already exists (len={})", key.len()),
                });
            }
            Err(pos) => pos,
        };

        if node.can_insert(cell_size) {
            node.cells.insert(insert_pos, new_cell);
            let encoded = node.encode()?;
            self.store.write_leaf_structural(page, &encoded)?;
            Ok(Vec::new())
        } else {
            // Leaf is full: split.
            self.split_leaf(page, node, insert_pos, new_cell)
        }
    }

    /// Split a full leaf, inserting `new_cell`, and return one promoted key +
    /// right page per new sibling (ascending by key).
    ///
    /// Splits are two-way whenever a byte-feasible single cut exists. When no
    /// single cut keeps both halves within `PAGE_SIZE_LEAF` (a byte-full leaf
    /// of small cells receiving one large inline cell at a mid-range key —
    /// whichever side takes the big cell also takes ~half the small cells),
    /// the cells are greedily packed into as many leaves as needed instead;
    /// every individual cell fits a page by construction, so packing always
    /// succeeds where a single cut cannot.
    fn split_leaf(
        &mut self,
        left_page: u32,
        mut left_node: LeafNode,
        insert_pos: usize,
        new_cell: LeafCell,
    ) -> Result<Vec<SplitResult>> {
        #[cfg(any(test, feature = "test-hooks"))]
        crate::storage::close_quadratic_probe::record_leaf_splits(1);
        left_node.cells.insert(insert_pos, new_cell);

        // Byte-aware split point: leaf cells are variable-sized (up to just
        // under OVERFLOW_THRESHOLD inline), so a count-based midpoint can
        // route two near-threshold cells onto one side whose encoded size
        // exceeds PAGE_SIZE_LEAF. Reuse the delete path's chooser; seeding it
        // with the count midpoint keeps ordinary small-cell splits near-even
        // (its movement tiebreak prefers the midpoint when bytes are equal).
        let total = left_node.cells.len();
        let cells = std::mem::take(&mut left_node.cells);
        let groups: Vec<Vec<LeafCell>> =
            match Self::choose_leaf_redistribution_split(&cells, total / 2) {
                Some(split_at) => {
                    let mut left_cells = cells;
                    let right_cells = left_cells.split_off(split_at);
                    vec![left_cells, right_cells]
                }
                None => Self::pack_cells_multiway(cells)?,
            };

        // Group 0 stays on `left_page`; allocate one right sibling per
        // remaining group. The first key of each right group is its promoted
        // separator.
        let mut pages = Vec::with_capacity(groups.len());
        pages.push(left_page);
        for _ in 1..groups.len() {
            pages.push(self.store.alloc_leaf()?);
        }
        let separators: Vec<Vec<u8>> = groups[1..].iter().map(|g| g[0].key.clone()).collect();

        // Drain-and-partition window: split routing must move every resident
        // delta chain atomically with the leaf-page split (see
        // `super::chain_migration::partition_chains_for_split`). Each chain
        // goes to the last page whose separator is <= its key (below the first
        // separator stays on the left page).
        self.partition_chains_for_split(left_page, &pages, &separators)?;

        let old_next = left_node.next_leaf_page;
        // `groups.len() >= 2` here: a feasible cut yields two halves, and the
        // packing path only runs when the cells exceed one page's budget.
        let last_page = pages[pages.len() - 1];

        // Update the old right sibling's prev pointer (if any).
        if old_next != 0 {
            let (old_next_buf, _) = self.store.read_leaf(old_next)?;
            let mut old_next_node = LeafNode::parse(&old_next_buf[..])?;
            old_next_node.prev_leaf_page = last_page;
            let enc = old_next_node.encode()?;
            self.store.write_leaf_structural(old_next, &enc)?;
        }

        // Write every node, threading the sibling chain through the new pages.
        for (i, cells) in groups.into_iter().enumerate() {
            if i == 0 {
                left_node.cells = cells;
                left_node.next_leaf_page = pages[1];
                let enc = left_node.encode()?;
                self.store.write_leaf_structural(left_page, &enc)?;
            } else {
                let node = LeafNode {
                    flags: 0,
                    next_leaf_page: if i + 1 < pages.len() {
                        pages[i + 1]
                    } else {
                        old_next
                    },
                    prev_leaf_page: pages[i - 1],
                    cells,
                };
                let enc = node.encode()?;
                self.store.write_leaf_structural(pages[i], &enc)?;
            }
        }

        Ok(separators
            .into_iter()
            .zip(pages.into_iter().skip(1))
            .map(|(promoted_key, right_page)| SplitResult {
                promoted_key,
                right_page,
            })
            .collect())
    }

    /// Greedily pack sorted `cells` into groups that each fit one leaf page.
    ///
    /// Used by `split_leaf` when no single cut is byte-feasible. Errs only if
    /// a single cell exceeds the page budget, which valid inline cells (value
    /// <= `OVERFLOW_THRESHOLD`, bounded key) cannot.
    ///
    /// Underfull-tail policy: greedy first-fit can leave the LAST group
    /// deeply underfull (it carries whatever remainder did not fit the
    /// previous group — potentially a single small cell). This is accepted
    /// space amplification, bounded at one underfull trailing leaf per
    /// multi-way split, and never a correctness issue: ordering, the sibling
    /// chain, and promoted separators are unaffected. Nothing rebalances the
    /// leaf proactively; it fills up again from inserts landing in its key
    /// range, and the delete path's underflow handler
    /// (`LeafNode::needs_rebalance` → `handle_leaf_underflow`, delete.rs)
    /// merges or redistributes it the first time a delete touches it.
    fn pack_cells_multiway(cells: Vec<LeafCell>) -> Result<Vec<Vec<LeafCell>>> {
        let mut groups: Vec<Vec<LeafCell>> = Vec::new();
        let mut current: Vec<LeafCell> = Vec::new();
        let mut current_used = LEAF_HEADER_SIZE;
        for cell in cells {
            // Cell footprint = encoded bytes + its 2-byte cell pointer.
            let footprint = cell.encoded_size() + 2;
            if !current.is_empty() && current_used + footprint > PAGE_SIZE_LEAF as usize {
                groups.push(std::mem::take(&mut current));
                current_used = LEAF_HEADER_SIZE;
            }
            if current_used + footprint > PAGE_SIZE_LEAF as usize {
                return Err(Error::Internal(
                    "leaf split: a single cell exceeds the leaf page size".into(),
                ));
            }
            current_used += footprint;
            current.push(cell);
        }
        if !current.is_empty() {
            groups.push(current);
        }
        Ok(groups)
    }

    /// Insert a new separator key `promoted_key` into the internal node at `page`.
    ///
    /// `right_child` is the **right** sibling produced by the child split.  The
    /// **left** sibling is already referenced by the existing entry at position `p`
    /// (or by `rightmost_child` when `p == entries.len()`); we update that pointer
    /// to `right_child` and insert a new entry `(promoted_key, old_child)` where
    /// `old_child` was the former left sibling.
    ///
    /// If the internal node overflows, split it and return a [`SplitResult`].
    fn insert_into_internal(
        &mut self,
        page: u32,
        promoted_key: Vec<u8>,
        right_child: u32,
    ) -> Result<Option<SplitResult>> {
        let buf = self.store.read_internal(page)?;
        let mut node = InternalNode::parse(&buf[..])?;

        // Find insertion position: first index where entries[p].0 > promoted_key.
        let p = node
            .entries
            .partition_point(|(k, _)| k.as_slice() <= promoted_key.as_slice());

        // The child currently at position p is the left half of the split.
        // Update it to point to the right half, and remember the old (left) pointer.
        let left_child = if p < node.entries.len() {
            let old = node.entries[p].1;
            node.entries[p].1 = right_child;
            old
        } else {
            let old = node.rightmost_child;
            node.rightmost_child = right_child;
            old
        };

        if node.can_insert(promoted_key.len()) {
            node.entries.insert(p, (promoted_key, left_child));
            let encoded = node.encode()?;
            self.store.write_internal(page, &encoded)?;
            Ok(None)
        } else {
            // Internal node full: split.
            // `node` already has entries[p].1 updated to right_child.
            self.split_internal(page, node, promoted_key, left_child)
        }
    }

    /// Split a full internal node, inserting `(new_key, new_left_child)`.
    ///
    /// `node` has already been updated in `insert_into_internal` so that the
    /// child pointer for the new key's right sibling is correct.  We only need
    /// to insert `(new_key, new_left_child)` and then split at the midpoint.
    fn split_internal(
        &mut self,
        left_page: u32,
        mut left_node: InternalNode,
        new_key: Vec<u8>,
        new_left_child: u32, // left child of new_key (= left half of the child split)
    ) -> Result<Option<SplitResult>> {
        // Insert the new entry in sorted order.
        let pos = left_node
            .entries
            .partition_point(|(k, _)| k.as_slice() <= new_key.as_slice());
        left_node.entries.insert(pos, (new_key, new_left_child));

        let total = left_node.entries.len();
        let m = total / 2;

        // Promote the key at index m to the parent.
        // entries[m].1 (its left child) becomes the new rightmost of the left node.
        let (promoted_key, promoted_left_child) = left_node.entries.remove(m);

        // Drain right half (entries after m, which is now at old index m+1).
        let right_entries: Vec<(Vec<u8>, u32)> = left_node.entries.drain(m..).collect();
        let old_rightmost = left_node.rightmost_child;

        // Left node: entries[0..m-1], rightmost = promoted_left_child.
        left_node.rightmost_child = promoted_left_child;

        // Right node: entries[m+1..], rightmost = old rightmost_child.
        let right_page = self.store.alloc_internal()?;
        let right_node = InternalNode {
            level: left_node.level,
            entries: right_entries,
            rightmost_child: old_rightmost,
        };

        let left_enc = left_node.encode()?;
        let right_enc = right_node.encode()?;
        self.store.write_internal(left_page, &left_enc)?;
        self.store.write_internal(right_page, &right_enc)?;

        Ok(Some(SplitResult {
            promoted_key,
            right_page,
        }))
    }
}
