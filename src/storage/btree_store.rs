//! `BufferPoolPageStore` — `BTreePageStore` adapter for the buffer pool.
//!
//! This module provides the **bridge** between the B+ tree's
//! [`BTreePageStore`] trait (which owns its backing store generically) and the
//! [`BufferPoolHandle`] (which provides pin/unpin-based page access).
//!
//! ## Implementation
//!
//! [`BufferPoolPageStore`] holds `Arc<BufferPoolHandle>`.
//!
//! - **Reads**: pin the page, clone the immutable page-image `Arc`, snapshot
//!   resident chains, then unpin. Leaf readers carry the shared immutable image
//!   instead of cloning 32 KiB per reader.
//!
//! - **Writes**: pin the page, copy the supplied buffer in, mark dirty, unpin.
//!   The dirty bit causes the page to be written to disk on the next
//!   [`BufferPoolHandle::flush`] call.
//!
//! - **Alloc**: delegate to [`BufferPoolHandle::alloc_page`], which zeros the
//!   new frame and marks it dirty.
//!
//! - **Free**: delegate to [`BufferPoolHandle::free_page`].
//!
//! ## Note on lifetime
//!
//! `BufferPoolPageStore` owns an `Arc<BufferPoolHandle>`.  Multiple B+ tree
//! instances (e.g. one per collection namespace) can each hold their own
//! `BufferPoolPageStore` pointing to the same shared handle.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::read_view::ChainSnapshot;
use crate::mvcc::version::VersionEntry;
use crate::storage::btree::{BTreePageStore, LeafPageImage};
use crate::storage::buffer_pool::{LatchedPinnedPage, PageSize, PinnedPage};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

pub(crate) const INTERNAL_SIZE: usize = PAGE_SIZE_INTERNAL as usize;
pub(crate) const LEAF_SIZE: usize = PAGE_SIZE_LEAF as usize;

// ---------------------------------------------------------------------------
// LeafHoldScope — RAII wrapper for test-hook latch-hold instrumentation.
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "test-hooks"))]
struct LeafHoldScope {
    start: Option<crate::storage::btree::range_scan_latch_scope::Us016LeafHoldStart>,
}

#[cfg(any(test, feature = "test-hooks"))]
impl LeafHoldScope {
    fn new(page: u32) -> Self {
        let start =
            crate::storage::btree::range_scan_latch_scope::begin_leaf_hold(page, 0);
        Self { start: Some(start) }
    }
}

#[cfg(any(test, feature = "test-hooks"))]
impl Drop for LeafHoldScope {
    fn drop(&mut self) {
        if let Some(start) = self.start.take() {
            crate::storage::btree::range_scan_latch_scope::finish_leaf_hold(start);
        }
    }
}

#[cfg(not(any(test, feature = "test-hooks")))]
struct LeafHoldScope;

#[cfg(not(any(test, feature = "test-hooks")))]
impl LeafHoldScope {
    #[inline(always)]
    fn new(_page: u32) -> Self {
        Self
    }
}

// ---------------------------------------------------------------------------
// BufferPoolPageStore
// ---------------------------------------------------------------------------

/// Adapts [`BufferPoolHandle`] to the [`BTreePageStore`] trait.
///
/// Pass one of these to [`BTree::create`] or [`BTree::open`] to back a B+ tree
/// with the shared buffer pool.
///
/// See the module-level documentation for design details.
pub(crate) struct BufferPoolPageStore {
    handle: Arc<BufferPoolHandle>,
    /// When `true`, all page pins / allocations are routed through
    /// [`BufferPoolHandle::history_pool`] instead of the main pool. Pages
    /// allocated by a history-routed store live in a disjoint cache so that
    /// writes to the history store never re-enter main-pool partition mutexes.
    is_history: bool,
}

pub(crate) enum SharedReaderPage<'a> {
    Latched(LatchedPinnedPage<'a>),
    Pinned(PinnedPage<'a>),
}

impl BufferPoolPageStore {
    /// Create a `BufferPoolPageStore` backed by `handle`'s main pool.
    pub(crate) fn new(handle: Arc<BufferPoolHandle>) -> Self {
        Self {
            handle,
            is_history: false,
        }
    }

    /// Create a `BufferPoolPageStore` backed by `handle`'s dedicated
    /// history-store pool.
    pub(crate) fn new_history(handle: Arc<BufferPoolHandle>) -> Self {
        Self {
            handle,
            is_history: true,
        }
    }

    /// Borrow the underlying [`BufferPoolHandle`].
    pub(crate) fn handle(&self) -> &Arc<BufferPoolHandle> {
        &self.handle
    }

    /// Fetch a page through the appropriate pool. History-routed stores
    /// bypass chain reconciliation (no MVCC version chains live on history
    /// pages) and pin directly on `history_pool`.
    fn fetch<'a>(&'a self, page: u32, size: PageSize) -> Result<PinnedPage<'a>> {
        if self.is_history {
            self.handle.history_pool().pin(page, size)
        } else {
            self.handle.fetch_page(page, size)
        }
    }

    /// Allocate a page and pin it zeroed on the appropriate pool.
    fn alloc(&self, size: PageSize) -> Result<u32> {
        if self.is_history {
            self.handle.alloc_page_history(size)
        } else {
            self.handle.alloc_page(size)
        }
    }

    /// Copy page bytes and snapshot chains from a shared reader guard.
    ///
    /// `snap_chains` is called only for the `Latched` arm, where MVCC chains
    /// are present. The `Pinned` arm (history pages) always returns `None`.
    fn snapshot_leaf<F>(
        &self,
        guard: &SharedReaderPage<'_>,
        snap_chains: F,
    ) -> Result<(LeafPageImage, Option<ChainSnapshot>)>
    where
        F: FnOnce(&LatchedPinnedPage<'_>) -> Result<ChainSnapshot>,
    {
        match guard {
            SharedReaderPage::Latched(p) => {
                let _hold = LeafHoldScope::new(p.page_id());
                let data = p.data_snapshot();
                let snap = snap_chains(p)?;
                Ok((LeafPageImage::shared(data)?, Some(snap)))
            }
            SharedReaderPage::Pinned(p) => {
                let mut buf = Box::new([0u8; LEAF_SIZE]);
                buf.copy_from_slice(p.data());
                Ok((LeafPageImage::owned(buf), None))
            }
        }
    }
}

impl BTreePageStore for BufferPoolPageStore {
    type SharedReadGuard<'a>
        = SharedReaderPage<'a>
    where
        Self: 'a;

    // -----------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------

    fn read_internal(&self, page: u32) -> Result<Box<[u8; INTERNAL_SIZE]>> {
        let pinned = self.fetch(page, PageSize::Small4k)?;
        let mut buf = Box::new([0u8; INTERNAL_SIZE]);
        buf.copy_from_slice(pinned.data());
        Ok(buf)
        // pinned auto-unpins here
    }

    fn read_leaf(&self, page: u32) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        let guard = self.pin_shared_for_read(page, PageSize::Large32k)?;
        self.snapshot_leaf(&guard, |p| p.snapshot_chains(None))
    }

    fn pin_shared_for_read<'a>(
        &'a self,
        page: u32,
        size: PageSize,
    ) -> Result<Self::SharedReadGuard<'a>> {
        if self.is_history {
            return self.fetch(page, size).map(SharedReaderPage::Pinned);
        }
        self.handle
            .pool()
            .pin_for_read_sized(page, size)
            .map(SharedReaderPage::Latched)
    }

    fn read_internal_guarded(
        &self,
        _page: u32,
        guard: &Self::SharedReadGuard<'_>,
    ) -> Result<Box<[u8; INTERNAL_SIZE]>> {
        let mut buf = Box::new([0u8; INTERNAL_SIZE]);
        let src = match guard {
            SharedReaderPage::Latched(p) => p.data_snapshot(),
            SharedReaderPage::Pinned(p) => {
                buf.copy_from_slice(p.data());
                return Ok(buf);
            }
        };
        buf.copy_from_slice(&src[..INTERNAL_SIZE]);
        Ok(buf)
    }

    fn read_leaf_guarded(
        &self,
        _page: u32,
        guard: &Self::SharedReadGuard<'_>,
    ) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        self.snapshot_leaf(guard, |p| p.snapshot_chains(None))
    }

    fn read_leaf_for_key_guarded(
        &self,
        _page: u32,
        guard: &Self::SharedReadGuard<'_>,
        key: &[u8],
    ) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        self.snapshot_leaf(guard, |p| p.snapshot_chain_for_key(key, None))
    }

    // -----------------------------------------------------------------------
    // Writes
    // -----------------------------------------------------------------------

    fn write_internal(&mut self, page: u32, data: &[u8; INTERNAL_SIZE]) -> Result<()> {
        let mut pinned = self.fetch(page, PageSize::Small4k)?;
        pinned.data_mut().copy_from_slice(data);
        Ok(())
        // pinned auto-unpins here; dirty bit set by data_mut()
    }

    fn write_leaf_structural(&mut self, page: u32, data: &[u8; LEAF_SIZE]) -> Result<()> {
        let mut pinned = self.fetch(page, PageSize::Large32k)?;
        pinned.data_mut().copy_from_slice(data);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Allocation
    // -----------------------------------------------------------------------

    fn alloc_internal(&mut self) -> Result<u32> {
        self.alloc(PageSize::Small4k)
    }

    fn alloc_leaf(&mut self) -> Result<u32> {
        self.alloc(PageSize::Large32k)
    }

    // -----------------------------------------------------------------------
    // Deallocation
    // -----------------------------------------------------------------------

    fn free_internal(&mut self, page: u32) -> Result<()> {
        self.handle.free_page(page, PageSize::Small4k)
    }

    fn free_leaf(&mut self, page: u32) -> Result<()> {
        self.handle.free_page(page, PageSize::Large32k)
    }

    // -----------------------------------------------------------------------
    // MVCC delta-chain accessors (T3.5)
    //
    // Delegate to the buffer pool's chain helpers, which operate on the
    // `Frame::deltas` map for the 32 KB leaf partition.
    // -----------------------------------------------------------------------

    fn take_chain(&mut self, page: u32, key: &[u8]) -> Result<Option<Arc<VecDeque<VersionEntry>>>> {
        self.handle.pool().take_chain(page, key)
    }

    fn put_chain(
        &mut self,
        page: u32,
        key: Vec<u8>,
        chain: Arc<VecDeque<VersionEntry>>,
    ) -> Result<()> {
        self.handle.pool().put_chain(page, key, chain)
    }

    fn chains_empty(&self, page: u32) -> Result<bool> {
        self.handle.pool().chains_empty(page)
    }

    fn clear_chains(&mut self, page: u32) -> Result<()> {
        // Overflow pages live on the main 32 KB leaf pool (same
        // partition as data leaves). History-routed stores never
        // attach version chains to their pages, so the call is a
        // no-op there.
        if self.is_history {
            return Ok(());
        }
        self.handle
            .pool()
            .clear_chains_on_page(page, PageSize::Large32k)
    }

    fn take_all_chains(
        &mut self,
        page: u32,
    ) -> Result<Vec<(Vec<u8>, Arc<VecDeque<VersionEntry>>)>> {
        self.take_all_chains_on_page(page)
    }

    fn take_all_chains_on_page(
        &mut self,
        page: u32,
    ) -> Result<Vec<(Vec<u8>, Arc<VecDeque<VersionEntry>>)>> {
        if self.is_history {
            return Ok(Vec::new());
        }
        self.handle.pool().take_all_chains_on_page(page)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/btree_store_basic.rs"]
mod btree_store_basic;

#[cfg(test)]
#[path = "tests/buffer_pool_page_store_leaf_snapshot.rs"]
mod buffer_pool_page_store_leaf_snapshot;
