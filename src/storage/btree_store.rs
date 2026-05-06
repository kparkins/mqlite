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

// Local usize aliases for array dimensions.
const INTERNAL_SIZE: usize = PAGE_SIZE_INTERNAL as usize;
const LEAF_SIZE: usize = PAGE_SIZE_LEAF as usize;

// ---------------------------------------------------------------------------
// BufferPoolPageStore
// ---------------------------------------------------------------------------

/// Adapts [`BufferPoolHandle`] to the [`BTreePageStore`] trait.
///
/// Pass one of these to [`BTree::new`] or [`BTree::open`] to back a B+ tree
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
    #[allow(dead_code)]
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
        if !self.is_history {
            let latched = self.handle.pool().pin_for_read(page)?;
            #[cfg(any(test, feature = "test-hooks"))]
            let hold_start =
                crate::storage::btree::reader_latch_scope_test_probe::begin_leaf_hold(page, 0);
            let page_data = latched.data_snapshot();
            let snap = Some(latched.snapshot_chains(None)?);
            drop(latched);
            #[cfg(any(test, feature = "test-hooks"))]
            crate::storage::btree::reader_latch_scope_test_probe::finish_leaf_hold(hold_start);
            return Ok((LeafPageImage::shared(page_data)?, snap));
        }

        let pinned = self.fetch(page, PageSize::Large32k)?;
        let mut buf = Box::new([0u8; LEAF_SIZE]);
        buf.copy_from_slice(pinned.data());
        Ok((LeafPageImage::owned(buf), None))
        // pinned auto-unpins here
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
        match guard {
            SharedReaderPage::Latched(page) => {
                let page_data = page.data_snapshot();
                buf.copy_from_slice(&page_data[..INTERNAL_SIZE]);
            }
            SharedReaderPage::Pinned(page) => buf.copy_from_slice(page.data()),
        }
        Ok(buf)
    }

    fn read_leaf_guarded(
        &self,
        _page: u32,
        guard: &Self::SharedReadGuard<'_>,
    ) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        match guard {
            SharedReaderPage::Latched(page) => {
                #[cfg(any(test, feature = "test-hooks"))]
                let hold_start =
                    crate::storage::btree::reader_latch_scope_test_probe::begin_leaf_hold(
                        page.page_id(),
                        0,
                    );
                let page_data = page.data_snapshot();
                let snap = Some(page.snapshot_chains(None)?);
                #[cfg(any(test, feature = "test-hooks"))]
                crate::storage::btree::reader_latch_scope_test_probe::finish_leaf_hold(hold_start);
                Ok((LeafPageImage::shared(page_data)?, snap))
            }
            SharedReaderPage::Pinned(page) => {
                let mut buf = Box::new([0u8; LEAF_SIZE]);
                buf.copy_from_slice(page.data());
                Ok((LeafPageImage::owned(buf), None))
            }
        }
    }

    fn read_leaf_for_key_guarded(
        &self,
        page: u32,
        guard: &Self::SharedReadGuard<'_>,
        key: &[u8],
    ) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        match guard {
            SharedReaderPage::Latched(page) => {
                #[cfg(any(test, feature = "test-hooks"))]
                let hold_start =
                    crate::storage::btree::reader_latch_scope_test_probe::begin_leaf_hold(
                        page.page_id(),
                        0,
                    );
                let page_data = page.data_snapshot();
                let snap = Some(page.snapshot_chain_for_key(key, None)?);
                #[cfg(any(test, feature = "test-hooks"))]
                crate::storage::btree::reader_latch_scope_test_probe::finish_leaf_hold(hold_start);
                Ok((LeafPageImage::shared(page_data)?, snap))
            }
            SharedReaderPage::Pinned(_) => self.read_leaf(page),
        }
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
mod tests {
    use super::*;
    use crate::storage::btree::{BTree, BTreePageStore};
    use crate::storage::buffer_pool::default_sizes;
    use crate::storage::buffer_pool::BufferPool;
    use crate::storage::header::FileHeader;
    use crate::storage::test_support::{ArcIo, MockIo};

    fn make_store() -> BufferPoolPageStore {
        let io = MockIo::new();
        let pool = Arc::new(BufferPool::new(
            default_sizes::DESKTOP,
            Box::new(ArcIo(Arc::clone(&io))),
        ));
        let history_pool = Arc::new(BufferPool::new(
            default_sizes::IOT,
            Box::new(ArcIo(Arc::clone(&io))),
        ));
        let header = FileHeader::new_now();
        let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
        BufferPoolPageStore::new(handle)
    }

    // -----------------------------------------------------------------------
    // alloc_internal / alloc_leaf
    // -----------------------------------------------------------------------

    #[test]
    fn alloc_internal_returns_first_free_page() {
        let mut store = make_store();
        let pn = store.alloc_internal().unwrap();
        assert_eq!(pn, 1, "first internal page must be 1");
    }

    #[test]
    fn alloc_leaf_returns_first_free_page() {
        let mut store = make_store();
        let pn = store.alloc_leaf().unwrap();
        assert_eq!(pn, 1, "first leaf page must be 1");
    }

    #[test]
    fn sequential_allocs_return_consecutive_pages() {
        let mut store = make_store();
        let a = store.alloc_internal().unwrap();
        let b = store.alloc_leaf().unwrap();
        let c = store.alloc_internal().unwrap();

        assert_eq!(a, 1);
        assert_eq!(b, 2);
        assert_eq!(c, 3);
    }

    // -----------------------------------------------------------------------
    // write / read roundtrip
    // -----------------------------------------------------------------------

    #[test]
    fn write_and_read_internal_roundtrip() {
        let mut store = make_store();
        let pn = store.alloc_internal().unwrap();

        let mut data = [0u8; INTERNAL_SIZE];
        data[0] = 0xAA;
        data[4090] = 0xBB;
        store.write_internal(pn, &data).unwrap();

        let read_back = store.read_internal(pn).unwrap();
        assert_eq!(read_back[0], 0xAA);
        assert_eq!(read_back[4090], 0xBB);
    }

    #[test]
    fn write_and_read_leaf_roundtrip() {
        let mut store = make_store();
        let pn = store.alloc_leaf().unwrap();

        let mut data = [0u8; LEAF_SIZE];
        data[0] = 0xCC;
        data[32760] = 0xDD;
        store.write_leaf_structural(pn, &data).unwrap();

        let (read_back, _) = store.read_leaf(pn).unwrap();
        assert_eq!(read_back[0], 0xCC);
        assert_eq!(read_back[32760], 0xDD);
    }

    // -----------------------------------------------------------------------
    // free / realloc
    // -----------------------------------------------------------------------

    #[test]
    fn free_internal_recycles_on_next_alloc() {
        let mut store = make_store();
        let pn = store.alloc_internal().unwrap();
        store.free_internal(pn).unwrap();
        let recycled = store.alloc_internal().unwrap();
        assert_eq!(recycled, pn, "freed internal page must be recycled");
    }

    #[test]
    fn free_leaf_recycles_on_next_alloc() {
        let mut store = make_store();
        let pn = store.alloc_leaf().unwrap();
        store.free_leaf(pn).unwrap();
        let recycled = store.alloc_leaf().unwrap();
        assert_eq!(recycled, pn, "freed leaf page must be recycled");
    }

    // -----------------------------------------------------------------------
    // B+ tree smoke test through BufferPoolPageStore
    // -----------------------------------------------------------------------

    #[test]
    fn btree_insert_and_get_via_pool_store() {
        let store = make_store();
        let mut tree = BTree::create(store).unwrap();

        let key = b"hello";
        let val = b"world!";

        tree.insert(key, val).unwrap();

        let result = tree.get(key).unwrap();
        assert_eq!(result.as_deref(), Some(val.as_ref()));
    }

    #[test]
    fn btree_insert_multiple_keys_and_get_all() {
        let store = make_store();
        let mut tree = BTree::create(store).unwrap();

        for i in 0u8..50 {
            let key = [i];
            let val = [i, i + 1];
            tree.insert(&key, &val).unwrap();
        }

        for i in 0u8..50 {
            let key = [i];
            let expected = [i, i + 1];
            let result = tree.get(&key).unwrap();
            assert_eq!(
                result.as_deref(),
                Some(expected.as_ref()),
                "key {i} not found"
            );
        }
    }
}

#[cfg(test)]
#[path = "tests/buffer_pool_page_store_leaf_snapshot_tests.rs"]
mod buffer_pool_page_store_leaf_snapshot_tests;
