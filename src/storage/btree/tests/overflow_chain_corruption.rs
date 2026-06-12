//! R14 overflow-walk hardening repro: the free/collect overflow walks
//! (`free_overflow_chain`, `collect_overflow_pages`) follow
//! `OverflowPageHeader::next_overflow_page` from page to page. Before this
//! change they did NOT call `validate_type()` and had no cycle cap, so:
//!
//! 1. A **cycle** (page A's `next` points back to A, or A->B->A) is walked
//!    forever — `free_overflow_chain` would free the same page repeatedly and
//!    `collect_overflow_pages` would grow its page vector without bound.
//! 2. A **mis-typed link** (a `next_overflow_page` pointing at a data leaf
//!    whose `page_type` is `PAGE_TYPE_LEAF`) is followed anyway, reinterpreting
//!    the leaf header's bytes 12..16 as the next pointer and walking garbage.
//!
//! The read path (`read_overflow_chain`) already validated the page type, so
//! these two consolidation-siblings were the unguarded outliers.
//!
//! ## Bounded harness (no wall-clock hang)
//!
//! `CountingStore` wraps `MemPageStore` and counts `read_leaf` calls, aborting
//! with an `Err` once a HARD cap (`HARNESS_READ_CAP`) is reached. An unbounded
//! walk is therefore observable as "hit the harness cap" instead of hanging the
//! test runner. The harness cap sits ABOVE the real production cycle cap
//! (`MAX_OVERFLOW_CHAIN_PAGES` ≈ u32::MAX / page-payload ≈ 131 153) precisely
//! so the test can distinguish "walk terminated on its own validation" (reads
//! stay at or under the production cap) from "walk would have run away" (reads
//! reach the harness cap).

use std::cell::Cell;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use super::*;
use crate::error::{Error, Result};
use crate::mvcc::chain_snapshot::ChainSnapshot;
use crate::mvcc::version::VersionEntry;
use crate::storage::buffer_pool::{LatchMode, PageSize};
use crate::storage::page::{
    OverflowPageHeader, PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF, PAGE_TYPE_OVERFLOW,
};

/// Hard read cap enforced by the harness. Set ABOVE the production cycle cap
/// (`overflow::MAX_OVERFLOW_CHAIN_PAGES` ≈ u32::MAX / overflow-page-payload ≈
/// 131 153) so that:
///   - the GUARDED walk stops on its own cap (`reads <= prod cap`, an `Err`
///     whose message mentions the chain), well below this harness cap; while
///   - an UNGUARDED (pre-R14) walk runs forever and is only ever stopped here,
///     tripping the harness `Err` at `reads == HARNESS_READ_CAP + 1`.
///
/// The assertions below therefore distinguish "walk self-terminated" from
/// "walk ran away", without ever wall-clock hanging the test runner.
const HARNESS_READ_CAP: usize = 200_000;

// ---------------------------------------------------------------------------
// CountingStore: MemPageStore + a hard read cap that surfaces runaway walks.
// ---------------------------------------------------------------------------

struct CountingStore {
    inner: MemPageStore,
    reads: Cell<usize>,
}

impl CountingStore {
    fn new() -> Self {
        Self {
            inner: MemPageStore::new(),
            reads: Cell::new(0),
        }
    }

    fn bump_read(&self) -> Result<()> {
        let n = self.reads.get() + 1;
        self.reads.set(n);
        if n > HARNESS_READ_CAP {
            return Err(Error::Internal("HARNESS_READ_CAP exceeded".into()));
        }
        Ok(())
    }

    /// Write a self-referential single-page overflow chain (page -> page).
    fn write_overflow_self_cycle(&mut self, page: u32) {
        let mut buf = [0u8; PAGE_SIZE_LEAF as usize];
        let hdr = OverflowPageHeader {
            page_type: PAGE_TYPE_OVERFLOW,
            refcount: 0,
            checksum: 0,
            next_overflow_page: page, // cycle: points at itself
            data_length: 0,
        };
        hdr.write_to(&mut buf);
        self.inner.write_leaf_structural(page, &buf).unwrap();
    }
}

impl BTreePageStore for CountingStore {
    type SharedReadGuard<'a>
        = ()
    where
        Self: 'a;

    fn read_internal(&self, page: u32) -> Result<Box<[u8; PAGE_SIZE_INTERNAL as usize]>> {
        self.bump_read()?;
        self.inner.read_internal(page)
    }

    fn read_leaf(&self, page: u32) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        self.bump_read()?;
        self.inner.read_leaf(page)
    }

    fn pin_shared_for_read<'a>(
        &'a self,
        page: u32,
        size: PageSize,
    ) -> Result<Self::SharedReadGuard<'a>> {
        self.inner.pin_shared_for_read(page, size)
    }

    fn write_internal(
        &mut self,
        page: u32,
        data: &[u8; PAGE_SIZE_INTERNAL as usize],
    ) -> Result<()> {
        self.inner.write_internal(page, data)
    }

    fn write_leaf_structural(
        &mut self,
        page: u32,
        data: &[u8; PAGE_SIZE_LEAF as usize],
    ) -> Result<()> {
        self.inner.write_leaf_structural(page, data)
    }

    fn alloc_internal(&mut self) -> Result<u32> {
        self.inner.alloc_internal()
    }

    fn alloc_leaf(&mut self) -> Result<u32> {
        self.inner.alloc_leaf()
    }

    fn free_internal(&mut self, page: u32) -> Result<()> {
        self.inner.free_internal(page)
    }

    fn free_leaf(&mut self, page: u32) -> Result<()> {
        self.inner.free_leaf(page)
    }

    fn chains_empty(&self, page: u32) -> Result<bool> {
        self.inner.chains_empty(page)
    }

    fn with_chain_under_latch<R, F>(
        &mut self,
        page: u32,
        key: &[u8],
        mode: LatchMode,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut Option<Arc<VecDeque<VersionEntry>>>) -> R,
    {
        self.inner.with_chain_under_latch(page, key, mode, f)
    }

    fn with_all_chains_under_latch<R, F>(&mut self, page: u32, mode: LatchMode, f: F) -> Result<R>
    where
        F: FnOnce(&mut BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>) -> R,
    {
        self.inner.with_all_chains_under_latch(page, mode, f)
    }
}

// ---------------------------------------------------------------------------
// A cyclic overflow chain must terminate with Err, bounded well under the cap.
// ---------------------------------------------------------------------------

#[test]
fn free_overflow_chain_rejects_self_cycle() {
    let mut store = CountingStore::new();
    let page = store.alloc_leaf().unwrap();
    store.write_overflow_self_cycle(page);

    let err = overflow::free_overflow_chain(&mut store, page);
    assert!(
        err.is_err(),
        "free walk over a self-cycle must return Err, not loop forever"
    );
    // Self-terminated on the production cycle cap, not on the harness cap:
    // the guarded walk stops at <= MAX_OVERFLOW_CHAIN_PAGES reads, strictly
    // below HARNESS_READ_CAP. An unguarded walk would only ever stop at the
    // harness cap (reads == HARNESS_READ_CAP + 1), failing this bound.
    assert!(
        store.reads.get() <= overflow::MAX_OVERFLOW_CHAIN_PAGES + 1
            && store.reads.get() < HARNESS_READ_CAP,
        "free walk must self-terminate on its cycle cap; harness saw {} reads \
         (prod cap {})",
        store.reads.get(),
        overflow::MAX_OVERFLOW_CHAIN_PAGES
    );
}

#[test]
fn collect_overflow_pages_rejects_self_cycle() {
    let store = CountingStore::new();
    // alloc_leaf needs &mut, so build the store, then mutate via a fresh scope.
    let mut store = store;
    let page = store.alloc_leaf().unwrap();
    store.write_overflow_self_cycle(page);

    let mut pages = Vec::new();
    let res = overflow::collect_overflow_pages(&store, page, &mut pages);
    assert!(
        res.is_err(),
        "collect walk over a self-cycle must return Err, not grow unbounded"
    );
    assert!(
        store.reads.get() <= overflow::MAX_OVERFLOW_CHAIN_PAGES + 1
            && store.reads.get() < HARNESS_READ_CAP,
        "collect walk must self-terminate on its cycle cap; harness saw {} reads \
         (prod cap {})",
        store.reads.get(),
        overflow::MAX_OVERFLOW_CHAIN_PAGES
    );
}

// ---------------------------------------------------------------------------
// A next pointer into a non-overflow (data leaf) page must be rejected by type.
// ---------------------------------------------------------------------------

#[test]
fn free_overflow_chain_rejects_mistyped_next_page() {
    let mut store = CountingStore::new();

    // A real overflow page whose `next` points at a data-leaf page number.
    let ov = store.alloc_leaf().unwrap();
    let leaf = store.alloc_leaf().unwrap();

    // Write the data leaf (PAGE_TYPE_LEAF, empty).
    let empty_leaf = empty_leaf_page_bytes().unwrap();
    store.write_leaf_structural(leaf, &empty_leaf).unwrap();

    // Write the overflow page pointing at the data leaf.
    let mut ov_buf = [0u8; PAGE_SIZE_LEAF as usize];
    OverflowPageHeader {
        page_type: PAGE_TYPE_OVERFLOW,
        refcount: 0,
        checksum: 0,
        next_overflow_page: leaf, // mis-typed link into a data leaf
        data_length: 0,
    }
    .write_to(&mut ov_buf);
    store.write_leaf_structural(ov, &ov_buf).unwrap();

    let res = overflow::free_overflow_chain(&mut store, ov);
    assert!(
        res.is_err(),
        "free walk must reject a next pointer into a non-overflow page"
    );
}
