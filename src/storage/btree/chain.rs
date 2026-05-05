use crate::error::{Error, Result};
use crate::storage::buffer_pool::PageSize;
use crate::storage::page::{
    overflow_page_checksum, OverflowPageHeader, OVERFLOW_HEADER_SIZE, PAGE_SIZE_LEAF,
    PAGE_TYPE_OVERFLOW,
};

use super::{BTreePageStore, CellValue, LeafNode, OVERFLOW_PAGE_DATA};

// ---------------------------------------------------------------------------
// Overflow chain helpers
// ---------------------------------------------------------------------------

pub(super) fn write_overflow_chain<S: BTreePageStore>(store: &mut S, data: &[u8]) -> Result<u32> {
    let chunks: Vec<&[u8]> = data.chunks(OVERFLOW_PAGE_DATA).collect();
    let n = chunks.len();
    if n == 0 {
        return Err(Error::Internal("write_overflow_chain: empty data".into()));
    }

    // Allocate all pages first.
    let mut pages = Vec::with_capacity(n);
    for _ in 0..n {
        pages.push(store.alloc_leaf()?);
    }

    // Write each page from last to first so we have next pointers.
    for i in (0..n).rev() {
        let chunk = chunks[i];
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

pub(super) fn read_overflow_chain<S: BTreePageStore>(
    store: &S,
    first_page: u32,
    total_length: u32,
) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(total_length as usize);
    let mut cur = first_page;
    while cur != 0 {
        let (buf, _) = store.read_leaf(cur)?;
        let hdr = OverflowPageHeader::from_bytes(&buf[..])?;
        hdr.validate_type()?;
        let data_len = hdr.data_length as usize;
        if OVERFLOW_HEADER_SIZE + data_len > PAGE_SIZE_LEAF as usize {
            return Err(Error::Internal(format!(
                "overflow page {cur}: data_length {data_len} exceeds page size"
            )));
        }
        result.extend_from_slice(&buf[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + data_len]);
        cur = hdr.next_overflow_page;
    }
    result.truncate(total_length as usize);
    Ok(result)
}

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
        store.clear_chains(cur)?;
        store.free_leaf(cur)?;
        cur = next;
    }
    Ok(())
}

pub(super) fn collect_overflow_pages<S: BTreePageStore>(
    store: &mut S,
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
    store: &mut S,
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
    } else {
        let buf = store.read_internal(page)?;
        let node = InternalNode::parse(&buf[..])?;
        for &(_, child) in &node.entries {
            collect_subtree_pages(store, child, level - 1, pages)?;
        }
        collect_subtree_pages(store, node.rightmost_child, level - 1, pages)?;
        pages.push((page, PageSize::Small4k));
    }
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
        // Leaf node: free any overflow chains, then free the leaf page.
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
    } else {
        // Internal node: recurse into each child, then free this page.
        let buf = store.read_internal(page)?;
        let node = InternalNode::parse(&buf[..])?;
        for &(_, child) in &node.entries {
            free_subtree(store, child, level - 1)?;
        }
        free_subtree(store, node.rightmost_child, level - 1)?;
        store.free_internal(page)?;
    }
    Ok(())
}
