//! Leaf-shape classifiers for SMO (structural-modification) gating.
//!
//! ## Consumer note
//!
//! These two predicates are consumed **only** by
//! `crate::storage::paged_engine::smo_latch`, which inspects a staged write's
//! target leaf bytes to decide whether the write is root-neutral (fits the
//! current leaf) or structural (would split / rebalance and therefore needs
//! the escalated SMO-latch set over the whole root-to-leaf path). They answer
//! the question "would this op change tree shape?" without performing the
//! mutation, so the classifier's fit arithmetic must match the real
//! insert/delete paths byte-for-byte — both route through
//! [`super::layout::leaf_cell_encoded_size`].

use crate::error::Result;

use super::layout::{leaf_cell_encoded_size, leaf_cell_value_size};
use super::node::LeafNode;

/// Return whether `data` has room for an inserted leaf cell.
pub(crate) fn leaf_can_insert_value(data: &[u8], key_len: usize, value_len: usize) -> Result<bool> {
    let node = LeafNode::parse(data)?;
    Ok(node.can_insert(leaf_cell_encoded_size(
        key_len,
        leaf_cell_value_size(value_len),
    )))
}

/// Return whether deleting `key` from `data` would make a non-root leaf rebalance.
pub(crate) fn leaf_needs_rebalance_after_delete(data: &[u8], key: &[u8]) -> Result<bool> {
    let mut node = LeafNode::parse(data)?;
    if let Ok(idx) = node.binary_search(key) {
        node.cells.remove(idx);
    }
    Ok(node.needs_rebalance())
}
