//! B+ tree delete path: leaf removal, underflow handling via
//! redistribution or merge, and parent separator maintenance.

use crate::error::{Error, Result};
use crate::storage::page::{LEAF_HEADER_SIZE, PAGE_SIZE_LEAF};

use super::chain::free_overflow_chain;
use super::node::{InternalNode, LeafCell, LeafNode};
use super::{BTree, BTreePageStore, CellValue, MIN_LEAF_BYTES};

impl<S: BTreePageStore> BTree<S> {
    /// Delete `key` from the tree.  Returns `true` if the key existed, `false`
    /// if not found.
    ///
    /// Overflow chains are freed when a cell with an overflow pointer is deleted.
    ///
    /// After deletion, if a non-root leaf falls below the minimum occupancy by
    /// cell count or byte usage, the tree attempts to redistribute from or
    /// merge with a sibling. Parent separator keys are updated accordingly.
    pub(crate) fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let mut path: Vec<(u32, usize)> = Vec::new();
        let found = self.delete_subtree(self.root_page, self.root_level, key, &mut path)?;
        Ok(found)
    }

    /// Recursive delete.
    ///
    /// `path` accumulates `(internal_page, child_idx)` entries as we descend so that
    /// we can walk back up to fix separator keys after merges.
    fn delete_subtree(
        &mut self,
        page: u32,
        level: u8,
        key: &[u8],
        path: &mut Vec<(u32, usize)>,
    ) -> Result<bool> {
        if level == 0 {
            return self.delete_from_leaf(page, key, path);
        }

        let buf = self.store.read_internal(page)?;
        let node = InternalNode::parse(&buf[..])?;
        let child_idx = node.find_child_idx(key);
        let child_page = node.child_at(child_idx);

        path.push((page, child_idx));
        let found = self.delete_subtree(child_page, level - 1, key, path)?;
        path.pop();
        Ok(found)
    }

    /// Delete `key` from the leaf at `page`, then handle underflow.
    fn delete_from_leaf(&mut self, page: u32, key: &[u8], path: &[(u32, usize)]) -> Result<bool> {
        let (buf, _) = self.store.read_leaf(page)?;
        let mut node = LeafNode::parse(&buf[..])?;

        let idx = match node.binary_search(key) {
            Ok(i) => i,
            Err(_) => return Ok(false),
        };

        // Free overflow chain if present.
        let removed = node.cells.remove(idx);
        if let CellValue::Overflow { first_page, .. } = removed.value {
            free_overflow_chain(&mut self.store, first_page)?;
        }

        let encoded = node.encode()?;
        self.store.write_leaf_structural(page, &encoded)?;

        // Check for underflow and potentially merge/redistribute.
        if node.needs_rebalance() && !path.is_empty() {
            self.handle_leaf_underflow(page, node, path)?;
        }

        Ok(true)
    }

    /// Handle underflow in a leaf by redistributing from or merging with a sibling.
    fn handle_leaf_underflow(
        &mut self,
        page: u32,
        node: LeafNode,
        path: &[(u32, usize)],
    ) -> Result<()> {
        // Get parent info.
        let Some(&(parent_page, child_idx)) = path.last() else {
            return Err(Error::Internal(
                "btree delete: empty path in rebalance".into(),
            ));
        };
        let parent_buf = self.store.read_internal(parent_page)?;
        let parent = InternalNode::parse(&parent_buf[..])?;

        let key_count = parent.entries.len();

        // Prefer rebalancing with the left sibling. The decision is byte-aware:
        // merge only if the combined cells fit in one 32 KB leaf; otherwise
        // repartition the pair across the two existing pages.
        if child_idx > 0 {
            let left_sibling_idx = child_idx - 1;
            let left_page = parent.child_at(left_sibling_idx);
            let (left_buf, _) = self.store.read_leaf(left_page)?;
            let left_node = LeafNode::parse(&left_buf[..])?;

            if Self::can_merge_leaves(&left_node, &node) {
                return self.merge_leaf_into_left(
                    parent_page,
                    child_idx,
                    path,
                    left_page,
                    left_node,
                    page,
                    node,
                );
            }

            return self.redistribute_leaf_pair(
                left_page,
                left_node,
                page,
                node,
                parent_page,
                left_sibling_idx,
            );
        }

        // No left sibling: repair against the right sibling.
        if child_idx < key_count {
            let right_sibling_idx = child_idx + 1;
            let right_page = parent.child_at(right_sibling_idx);
            let (right_buf, _) = self.store.read_leaf(right_page)?;
            let right_node = LeafNode::parse(&right_buf[..])?;

            if Self::can_merge_leaves(&node, &right_node) {
                return self.merge_leaf_into_right(
                    parent_page,
                    path,
                    page,
                    node,
                    right_page,
                    right_node,
                );
            }

            return self.redistribute_leaf_pair(
                page,
                node,
                right_page,
                right_node,
                parent_page,
                child_idx,
            );
        }

        Err(Error::Internal(
            "leaf underflow reached a parent with no siblings".into(),
        ))
    }

    fn can_merge_leaves(left: &LeafNode, right: &LeafNode) -> bool {
        left.used_bytes() + right.used_bytes() - LEAF_HEADER_SIZE <= PAGE_SIZE_LEAF as usize
    }

    fn choose_leaf_redistribution_split(
        cells: &[LeafCell],
        original_left_len: usize,
    ) -> Option<usize> {
        let mut best: Option<((usize, usize, usize), usize)> = None;

        for split_at in 1..cells.len() {
            let left_used = LeafNode::used_bytes_for_cells(&cells[..split_at]);
            let right_used = LeafNode::used_bytes_for_cells(&cells[split_at..]);
            if left_used > PAGE_SIZE_LEAF as usize || right_used > PAGE_SIZE_LEAF as usize {
                continue;
            }

            let deficit = MIN_LEAF_BYTES.saturating_sub(left_used)
                + MIN_LEAF_BYTES.saturating_sub(right_used);
            let imbalance = left_used.abs_diff(right_used);
            let movement = split_at.abs_diff(original_left_len);
            let score = (deficit, imbalance, movement);

            match &best {
                Some((best_score, _)) if *best_score <= score => {}
                _ => best = Some((score, split_at)),
            }
        }

        best.map(|(_, split_at)| split_at)
    }

    fn move_all_leaf_chains(&mut self, from_page: u32, to_page: u32) -> Result<()> {
        // Phase 3 Section 10.6: merge drains every chain from the source leaf,
        // including delta-only chains, and keeps the existing transport shape.
        for (key, chain) in self.store.take_all_chains(from_page)? {
            self.store.put_chain(to_page, key, chain)?;
        }
        Ok(())
    }

    fn redistribute_leaf_chains(
        &mut self,
        left_page: u32,
        right_page: u32,
        separator_key: &[u8],
    ) -> Result<()> {
        // Phase 3 Section 10.6: redistribution remains a separator-key
        // partition, so delta-only chains route by the same raw key bytes as
        // base-backed chains.
        let mut chains = self.store.take_all_chains(left_page)?;
        chains.extend(self.store.take_all_chains(right_page)?);

        for (key, chain) in chains {
            if key.as_slice() < separator_key {
                self.store.put_chain(left_page, key, chain)?;
            } else {
                self.store.put_chain(right_page, key, chain)?;
            }
        }

        Ok(())
    }

    fn redistribute_leaf_pair(
        &mut self,
        left_page: u32,
        mut left_node: LeafNode,
        right_page: u32,
        mut right_node: LeafNode,
        parent_page: u32,
        separator_idx: usize,
    ) -> Result<()> {
        let original_left_len = left_node.cells.len();
        let mut combined = left_node.cells;
        combined.extend(right_node.cells);

        let split_at = Self::choose_leaf_redistribution_split(&combined, original_left_len)
            .ok_or_else(|| {
                Error::Internal("leaf redistribution could not find a valid split".into())
            })?;

        let right_cells = combined.split_off(split_at);
        let separator_key = right_cells[0].key.clone();

        left_node.cells = combined;
        left_node.next_leaf_page = right_page;
        right_node.cells = right_cells;
        right_node.prev_leaf_page = left_page;

        self.redistribute_leaf_chains(left_page, right_page, &separator_key)?;

        let left_enc = left_node.encode()?;
        let right_enc = right_node.encode()?;
        let parent_buf = self.store.read_internal(parent_page)?;
        let mut parent = InternalNode::parse(&parent_buf[..])?;
        parent.entries[separator_idx].0 = separator_key;
        let parent_enc = parent.encode()?;

        self.store.write_leaf_structural(left_page, &left_enc)?;
        self.store.write_leaf_structural(right_page, &right_enc)?;
        self.store.write_internal(parent_page, &parent_enc)?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn merge_leaf_into_left(
        &mut self,
        parent_page: u32,
        child_idx: usize,
        path: &[(u32, usize)],
        left_page: u32,
        mut left_node: LeafNode,
        page: u32,
        node: LeafNode,
    ) -> Result<()> {
        self.move_all_leaf_chains(page, left_page)?;

        left_node.cells.extend(node.cells);
        left_node.next_leaf_page = node.next_leaf_page;

        if node.next_leaf_page != 0 {
            let (next_buf, _) = self.store.read_leaf(node.next_leaf_page)?;
            let mut next_node = LeafNode::parse(&next_buf[..])?;
            next_node.prev_leaf_page = left_page;
            let enc = next_node.encode()?;
            self.store
                .write_leaf_structural(node.next_leaf_page, &enc)?;
        }

        let left_enc = left_node.encode()?;
        self.store.write_leaf_structural(left_page, &left_enc)?;
        if !self.store.chains_empty(page)? {
            return Err(Error::Internal(
                "free_leaf called with non-empty version chain".into(),
            ));
        }
        self.store.free_leaf(page)?;

        self.redirect_parent_child_pointer(parent_page, child_idx, left_page)?;
        self.remove_from_parent(parent_page, child_idx - 1, path)
    }

    fn merge_leaf_into_right(
        &mut self,
        parent_page: u32,
        path: &[(u32, usize)],
        page: u32,
        node: LeafNode,
        right_page: u32,
        mut right_node: LeafNode,
    ) -> Result<()> {
        self.move_all_leaf_chains(page, right_page)?;

        let mut merged_cells = node.cells;
        merged_cells.extend(right_node.cells);
        right_node.cells = merged_cells;
        right_node.prev_leaf_page = node.prev_leaf_page;

        if node.prev_leaf_page != 0 {
            let (prev_buf, _) = self.store.read_leaf(node.prev_leaf_page)?;
            let mut prev_node = LeafNode::parse(&prev_buf[..])?;
            prev_node.next_leaf_page = right_page;
            let enc = prev_node.encode()?;
            self.store
                .write_leaf_structural(node.prev_leaf_page, &enc)?;
        }

        let right_enc = right_node.encode()?;
        self.store.write_leaf_structural(right_page, &right_enc)?;
        if !self.store.chains_empty(page)? {
            return Err(Error::Internal(
                "free_leaf called with non-empty version chain".into(),
            ));
        }
        self.store.free_leaf(page)?;
        self.remove_from_parent(parent_page, 0, path)
    }

    /// Redirect the child pointer at `child_idx` in the internal node at
    /// `parent_page` to `new_child`, leaving every other field unchanged.
    ///
    /// Used by the left-sibling merge path to ensure the parent no longer
    /// references the just-freed leaf after the subsequent separator removal
    /// shifts slots down.
    fn redirect_parent_child_pointer(
        &mut self,
        parent_page: u32,
        child_idx: usize,
        new_child: u32,
    ) -> Result<()> {
        let buf = self.store.read_internal(parent_page)?;
        let mut parent = InternalNode::parse(&buf[..])?;
        if child_idx < parent.entries.len() {
            parent.entries[child_idx].1 = new_child;
        } else {
            parent.rightmost_child = new_child;
        }
        let enc = parent.encode()?;
        self.store.write_internal(parent_page, &enc)?;
        Ok(())
    }

    /// Remove the separator key at `separator_idx` from the internal node at
    /// `parent_page`.
    ///
    /// If that internal node is the tree root and becomes empty, collapse the
    /// root to its remaining child. Otherwise write the updated node back in
    /// place; internal-node underflow propagation is still intentionally
    /// deferred in this implementation.
    fn remove_from_parent(
        &mut self,
        parent_page: u32,
        separator_idx: usize,
        _path: &[(u32, usize)],
    ) -> Result<()> {
        let buf = self.store.read_internal(parent_page)?;
        let mut parent = InternalNode::parse(&buf[..])?;

        parent.entries.remove(separator_idx);

        if parent.entries.is_empty() {
            // The root has no more separator keys.  If it's the actual tree root,
            // we make rightmost_child the new root.
            if parent_page == self.root_page {
                self.root_page = parent.rightmost_child;
                if self.root_level > 0 {
                    self.root_level -= 1;
                }
                self.store.free_internal(parent_page)?;
                return Ok(());
            }
            // Not the root: need to propagate underflow upward.
            // We accept an underfull internal node (just write it back).
            // A more complete implementation would merge internal nodes too.
            let enc = parent.encode()?;
            self.store.write_internal(parent_page, &enc)?;
        } else {
            let enc = parent.encode()?;
            self.store.write_internal(parent_page, &enc)?;
        }

        Ok(())
    }
}
