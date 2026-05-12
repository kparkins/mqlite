//! B+ tree insert path: recursive descent, leaf split, and internal
//! node split with key promotion to the parent.

use crate::error::{Error, Result};
use crate::storage::buffer_pool::LatchMode;
use crate::storage::page::{
    overflow_page_checksum, OverflowPageHeader, OVERFLOW_HEADER_SIZE, PAGE_SIZE_LEAF,
    PAGE_TYPE_OVERFLOW,
};

use super::node::{InternalNode, LeafCell, LeafNode, SplitResult};
use super::{BTree, BTreePageStore, CellValue, OVERFLOW_PAGE_DATA, OVERFLOW_THRESHOLD};

// ---------------------------------------------------------------------------
// Overflow write helper
// ---------------------------------------------------------------------------

pub(super) fn write_overflow_chain<S: BTreePageStore>(store: &mut S, data: &[u8]) -> Result<u32> {
    if data.is_empty() {
        return Err(Error::Internal("write_overflow_chain: empty data".into()));
    }
    let n = ((data.len() - 1) / OVERFLOW_PAGE_DATA) + 1;

    // Allocate all pages first.
    let mut pages = Vec::with_capacity(n);
    for _ in 0..n {
        pages.push(store.alloc_leaf()?);
    }

    // Write each page from last to first so we have next pointers.
    for (i, chunk) in data.chunks(OVERFLOW_PAGE_DATA).enumerate().rev() {
        let next = if i + 1 < n { pages[i + 1] } else { 0 };

        let mut buf = [0u8; PAGE_SIZE_LEAF as usize];
        let hdr = OverflowPageHeader {
            page_type: PAGE_TYPE_OVERFLOW,
            // Legacy non-MVCC writer: refcount semantics land in T5'/T6.
            // Starting at 0 preserves previous behaviour — no pins are
            // claimed here — and tracks the "unmanaged" state until the
            // MVCC writer path wraps these pages in OverflowRefs.
            refcount: 0,
            checksum: 0,
            next_overflow_page: next,
            data_length: chunk.len() as u32,
        };
        hdr.write_to(&mut buf);
        buf[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk.len()].copy_from_slice(chunk);

        let cs = overflow_page_checksum(&buf);
        // Post-T3 checksum field is at bytes 8..12 (Format Lock §A.1).
        buf[8..12].copy_from_slice(&cs.to_le_bytes());

        store.write_leaf_structural(pages[i], &buf)?;
    }

    Ok(pages[0])
}

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

        let split = self.insert_subtree(self.root_page, self.root_level, key, cell_value)?;

        if let Some(sr) = split {
            // The root split: allocate a new root internal node.
            let new_root = self.store.alloc_internal()?;
            let new_level = self.root_level + 1;
            let new_node = InternalNode {
                level: new_level,
                entries: vec![(sr.promoted_key, self.root_page)],
                rightmost_child: sr.right_page,
            };
            let buf = new_node.encode()?;
            self.store.write_internal(new_root, &buf)?;
            self.root_page = new_root;
            self.root_level = new_level;
        }

        Ok(())
    }

    /// Replace the value for an existing key without changing tree shape.
    ///
    /// Returns `Ok(false)` when the key is absent. This is used by checkpoint
    /// materialization to fold a resident MVCC head over a stale base cell
    /// without running the delete path first; deleting the stale cell can free
    /// pages that the stale base image still references until replacement is
    /// committed.
    pub(crate) fn replace_existing(&mut self, key: &[u8], value: &[u8]) -> Result<bool> {
        let cell_value = if value.len() > OVERFLOW_THRESHOLD {
            let first_page = write_overflow_chain(&mut self.store, value)?;
            CellValue::Overflow {
                first_page,
                total_length: value.len() as u32,
            }
        } else {
            CellValue::Inline(value.to_vec())
        };

        let leaf_page = self.find_leaf(key)?;
        let (buf, _) = self.store.read_leaf(leaf_page)?;
        let mut node = LeafNode::parse(&buf[..])?;
        let idx = match node.binary_search(key) {
            Ok(idx) => idx,
            Err(_) => return Ok(false),
        };
        node.cells[idx].value = cell_value;
        let encoded = node.encode()?;
        self.store.write_leaf_structural(leaf_page, &encoded)?;
        Ok(true)
    }

    /// Recursive insert into the subtree rooted at `page` (at `level`).
    ///
    /// Returns `Some(SplitResult)` if the node at `page` split.
    fn insert_subtree(
        &mut self,
        page: u32,
        level: u8,
        key: &[u8],
        value: CellValue,
    ) -> Result<Option<SplitResult>> {
        if level == 0 {
            return self.insert_leaf(page, key, value);
        }

        // Internal node.
        let buf = self.store.read_internal(page)?;
        let node = InternalNode::parse(&buf[..])?;
        let child_idx = node.find_child_idx(key);
        let child_page = node.child_at(child_idx);

        let child_split = self.insert_subtree(child_page, level - 1, key, value)?;

        if let Some(sr) = child_split {
            self.insert_into_internal(page, sr.promoted_key, sr.right_page)
        } else {
            Ok(None)
        }
    }

    /// Insert a key–value cell into the leaf at `page`.
    ///
    /// If the leaf is full, split it and return a [`SplitResult`].
    fn insert_leaf(
        &mut self,
        page: u32,
        key: &[u8],
        value: CellValue,
    ) -> Result<Option<SplitResult>> {
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
            Ok(None)
        } else {
            // Leaf is full: split.
            self.split_leaf(page, node, insert_pos, new_cell)
        }
    }

    /// Split a full leaf, inserting `new_cell`, and return the promoted key + right page.
    fn split_leaf(
        &mut self,
        left_page: u32,
        mut left_node: LeafNode,
        insert_pos: usize,
        new_cell: LeafCell,
    ) -> Result<Option<SplitResult>> {
        left_node.cells.insert(insert_pos, new_cell);

        let total = left_node.cells.len();
        let split_at = total / 2; // right half starts here

        // Allocate right sibling.
        let right_page = self.store.alloc_leaf()?;

        // Build right node with the upper half of cells.
        let right_cells: Vec<LeafCell> = left_node.cells.drain(split_at..).collect();
        let promoted_key = right_cells[0].key.clone();

        // Drain-and-partition window: split routing must move every resident
        // delta chain atomically with the leaf-page split.
        let all_chains: Vec<_> =
            self.store
                .with_all_chains_under_latch(left_page, LatchMode::Exclusive, |chains| {
                    std::mem::take(chains).into_iter().collect()
                })?;
        let (left_chains, right_chains): (Vec<_>, Vec<_>) = all_chains
            .into_iter()
            .partition(|(key, _)| key.as_slice() < promoted_key.as_slice());
        for (key, chain) in left_chains {
            self.store
                .with_chain_under_latch(left_page, &key, LatchMode::Exclusive, |slot| {
                    *slot = Some(chain);
                })?;
        }
        for (key, chain) in right_chains {
            self.store
                .with_chain_under_latch(right_page, &key, LatchMode::Exclusive, |slot| {
                    *slot = Some(chain);
                })?;
        }

        let right_node = LeafNode {
            flags: 0,
            next_leaf_page: left_node.next_leaf_page,
            prev_leaf_page: left_page,
            cells: right_cells,
        };

        // Update left node's next pointer.
        left_node.next_leaf_page = right_page;

        // Update the old right sibling's prev pointer (if any).
        if right_node.next_leaf_page != 0 {
            let (old_next_buf, _) = self.store.read_leaf(right_node.next_leaf_page)?;
            let mut old_next = LeafNode::parse(&old_next_buf[..])?;
            old_next.prev_leaf_page = right_page;
            let enc = old_next.encode()?;
            self.store
                .write_leaf_structural(right_node.next_leaf_page, &enc)?;
        }

        // Write both nodes.
        let left_enc = left_node.encode()?;
        let right_enc = right_node.encode()?;
        self.store.write_leaf_structural(left_page, &left_enc)?;
        self.store.write_leaf_structural(right_page, &right_enc)?;

        Ok(Some(SplitResult {
            promoted_key,
            right_page,
        }))
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
