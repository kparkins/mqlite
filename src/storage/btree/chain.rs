use crate::error::Result;
use crate::storage::buffer_pool::{LatchMode, PageSize};
use crate::storage::page::OverflowPageHeader;

use super::{BTreePageStore, CellValue, LeafNode};

// ---------------------------------------------------------------------------
// Overflow chain helpers
// ---------------------------------------------------------------------------

pub(super) fn free_overflow_chain<S: BTreePageStore>(store: &mut S, first_page: u32) -> Result<()> {
    let mut cur = first_page;
    while cur != 0 {
        let (buf, _) = store.read_leaf(cur)?;
        let hdr = OverflowPageHeader::from_bytes(&buf[..])?;
        let next = hdr.next_overflow_page;
        // Overflow pages carry no MVCC data; clear any stale chain
        // remnants from a prior data-leaf life of this page number so
        // the T3.5 `chains_empty` guard inside `free_leaf` paths does
        // not trip. The frame may not be resident — that's a no-op.
        store.with_all_chains_under_latch(cur, LatchMode::Exclusive, |chains| chains.clear())?;
        store.free_leaf(cur)?;
        cur = next;
    }
    Ok(())
}

pub(super) fn collect_overflow_pages<S: BTreePageStore>(
    store: &S,
    first_page: u32,
    pages: &mut Vec<(u32, PageSize)>,
) -> Result<()> {
    let mut cur = first_page;
    while cur != 0 {
        let (buf, _) = store.read_leaf(cur)?;
        let hdr = OverflowPageHeader::from_bytes(&buf[..])?;
        pages.push((cur, PageSize::Large32k));
        cur = hdr.next_overflow_page;
    }
    Ok(())
}

pub(super) fn collect_subtree_pages<S: BTreePageStore>(
    store: &S,
    page: u32,
    level: u8,
    pages: &mut Vec<(u32, PageSize)>,
) -> Result<()> {
    use super::InternalNode;
    if level == 0 {
        let (buf, _) = store.read_leaf(page)?;
        let node = LeafNode::parse(&buf[..])?;
        for cell in &node.cells {
            if let CellValue::Overflow { first_page, .. } = cell.value {
                collect_overflow_pages(store, first_page, pages)?;
            }
        }
        pages.push((page, PageSize::Large32k));
        return Ok(());
    }

    let buf = store.read_internal(page)?;
    let node = InternalNode::parse(&buf[..])?;
    for &(_, child) in &node.entries {
        collect_subtree_pages(store, child, level - 1, pages)?;
    }
    collect_subtree_pages(store, node.rightmost_child, level - 1, pages)?;
    pages.push((page, PageSize::Small4k));
    Ok(())
}

/// Recursively free all pages in the B+ tree subtree rooted at `page` at `level`.
///
/// Level 0 = leaf page; levels > 0 = internal node at that height.
/// For leaf pages, all overflow chains referenced by cells are freed first.
/// For internal pages, all children are freed recursively before the parent.
pub(super) fn free_subtree<S: BTreePageStore>(store: &mut S, page: u32, level: u8) -> Result<()> {
    use super::InternalNode;
    if level == 0 {
        // We do NOT follow `next_leaf_page` here — the parent's child-pointer
        // traversal already enumerates every leaf exactly once.
        let (buf, _) = store.read_leaf(page)?;
        let node = LeafNode::parse(&buf[..])?;
        for cell in &node.cells {
            if let CellValue::Overflow { first_page, .. } = cell.value {
                free_overflow_chain(store, first_page)?;
            }
        }
        store.free_leaf(page)?;
        return Ok(());
    }

    let buf = store.read_internal(page)?;
    let node = InternalNode::parse(&buf[..])?;
    for &(_, child) in &node.entries {
        free_subtree(store, child, level - 1)?;
    }
    free_subtree(store, node.rightmost_child, level - 1)?;
    store.free_internal(page)?;
    Ok(())
}
