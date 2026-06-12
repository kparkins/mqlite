//! Overflow-chain I/O: read, write, free, and collect walks for documents
//! whose serialized BSON exceeds [`super::layout::OVERFLOW_THRESHOLD`].
//!
//! A large value is stored as a singly-linked chain of 32 KB overflow pages;
//! the owning leaf cell holds only `(first_page, total_length)`. Every walk
//! below follows `OverflowPageHeader::next_overflow_page` from `first_page`
//! until it reaches the sentinel page 0.

use crate::error::{Error, Result};
use crate::mvcc::version::VersionData;
use crate::storage::buffer_pool::{LatchMode, PageSize};
use crate::storage::page::{
    overflow_page_checksum, OverflowPageHeader, OVERFLOW_HEADER_SIZE, PAGE_SIZE_LEAF,
    PAGE_TYPE_OVERFLOW,
};

use super::layout::OVERFLOW_PAGE_DATA;
use super::store::BTreePageStore;
use super::CellValue;

/// Upper bound on the number of pages any overflow walk will visit before
/// declaring the chain corrupt.
///
/// A 32 KB page carries `OVERFLOW_PAGE_DATA` payload bytes and a `u32`
/// `total_length`, so a well-formed chain holds at most
/// `ceil(u32::MAX / OVERFLOW_PAGE_DATA)` pages. Exceeding this means the walk
/// is following a cycle or a `next_overflow_page` pointer into a non-overflow
/// page — either way the chain is corrupt and the walk must stop with an error
/// instead of looping forever.
pub(crate) const MAX_OVERFLOW_CHAIN_PAGES: usize = (u32::MAX as usize / OVERFLOW_PAGE_DATA) + 2;

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

pub(crate) fn read_overflow_chain<S: BTreePageStore>(
    store: &S,
    first_page: u32,
    total_length: u32,
) -> Result<Vec<u8>> {
    let mut result = Vec::with_capacity(total_length as usize);
    let mut cur = first_page;
    let mut visited = 0usize;
    while cur != 0 {
        guard_chain_progress(cur, &mut visited)?;
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

// ---------------------------------------------------------------------------
// Write
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
            // Non-MVCC structural writer: this helper allocates raw
            // overflow pages but claims no version-chain pin on them.
            // Starting the refcount at 0 marks the page "unmanaged" —
            // it stays at 0 until the MVCC writer path wraps these
            // pages in `OverflowRef`s, which is what actually pins a
            // chain and bumps the refcount to >= 1.
            refcount: 0,
            checksum: 0,
            next_overflow_page: next,
            data_length: chunk.len() as u32,
        };
        hdr.write_to(&mut buf);
        buf[OVERFLOW_HEADER_SIZE..OVERFLOW_HEADER_SIZE + chunk.len()].copy_from_slice(chunk);

        let cs = overflow_page_checksum(&buf);
        // The overflow-page checksum field occupies bytes 8..12 of the
        // page header; this offset is part of the frozen on-disk format
        // and must match the reader in `OverflowPageHeader`.
        buf[8..12].copy_from_slice(&cs.to_le_bytes());

        store.write_leaf_structural(pages[i], &buf)?;
    }

    Ok(pages[0])
}

// ---------------------------------------------------------------------------
// Free / collect walks
// ---------------------------------------------------------------------------

pub(super) fn free_overflow_chain<S: BTreePageStore>(store: &mut S, first_page: u32) -> Result<()> {
    let mut cur = first_page;
    let mut visited = 0usize;
    while cur != 0 {
        guard_chain_progress(cur, &mut visited)?;
        let (buf, _) = store.read_leaf(cur)?;
        let hdr = OverflowPageHeader::from_bytes(&buf[..])?;
        // Reject a non-overflow page reached through `next_overflow_page`: a
        // mis-typed link (or a cycle that re-enters a data leaf) would
        // otherwise feed garbage bytes back as the next pointer and free
        // live pages. The read path already validates this; the free walk
        // must too, before it commits the irreversible `free_leaf`.
        hdr.validate_type()?;
        let next = hdr.next_overflow_page;
        // Overflow pages carry no MVCC data; clear any stale chain
        // remnants from a prior data-leaf life of this page number so
        // the `chains_empty` guard inside `free_leaf` paths does not
        // trip (that guard refuses to free a leaf whose version chains
        // were not migrated, to avoid dropping live versions). The
        // frame may not be resident — that's a no-op.
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
    let mut visited = 0usize;
    while cur != 0 {
        guard_chain_progress(cur, &mut visited)?;
        let (buf, _) = store.read_leaf(cur)?;
        let hdr = OverflowPageHeader::from_bytes(&buf[..])?;
        // Same corruption guard as `free_overflow_chain`: a mis-typed link
        // must not enroll a non-overflow page into the latch-ordering set.
        hdr.validate_type()?;
        pages.push((cur, PageSize::Large32k));
        cur = hdr.next_overflow_page;
    }
    Ok(())
}

/// Advance the bounded-walk counter, returning `Err` if a chain has visited
/// more pages than any well-formed chain could contain (a cycle or a pointer
/// into a non-overflow page that keeps yielding a non-zero "next").
fn guard_chain_progress(cur: u32, visited: &mut usize) -> Result<()> {
    *visited += 1;
    if *visited > MAX_OVERFLOW_CHAIN_PAGES {
        return Err(Error::Internal(format!(
            "overflow chain at page {cur} exceeded {MAX_OVERFLOW_CHAIN_PAGES} pages \
             (cycle or corrupt next pointer)"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Version-data / cell-value resolution
// ---------------------------------------------------------------------------

/// Resolve an MVCC [`VersionData`] into its raw value bytes, reading the
/// overflow chain when the version points at one.
///
/// This is the single resolver behind every reader path that materializes a
/// visible version's payload (point lookup chain/history hits, range-scan
/// merge, and checkpoint delta folding) — they all previously inlined the same
/// `Inline => clone / Overflow => read_overflow_chain` match.
pub(crate) fn resolve_version_data<S: BTreePageStore>(
    store: &S,
    data: &VersionData,
) -> Result<Vec<u8>> {
    match data {
        VersionData::Inline(v) => Ok(v.clone()),
        VersionData::Overflow(oref) => {
            read_overflow_chain(store, oref.first_page(), oref.total_length() as u32)
        }
    }
}

/// Resolve a base-cell [`CellValue`] into its raw value bytes, reading the
/// overflow chain when the cell points at one. The base-image counterpart to
/// [`resolve_version_data`].
pub(crate) fn resolve_cell_value<S: BTreePageStore>(
    store: &S,
    value: &CellValue,
) -> Result<Vec<u8>> {
    match value {
        CellValue::Inline(v) => Ok(v.clone()),
        CellValue::Overflow {
            first_page,
            total_length,
        } => read_overflow_chain(store, *first_page, *total_length),
    }
}
