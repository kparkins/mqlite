//! `BufferPoolPageStore` — `BTreePageStore` adapter for the buffer pool.
//!
//! This module provides the **bridge** between the B+ tree's
//! [`BTreePageStore`] trait (which owns its backing store generically) and the
//! [`BufferPoolHandle`] (which provides pin/unpin-based page access).
//!
//! ## Design — RISK-01 Resolution
//!
//! The Phase 1 reconciliation plan identified **RISK-01**: the B+ tree uses a
//! `BTreePageStore` trait whose interface (`read_internal`, `write_internal`,
//! `alloc_internal`, etc.) is shaped differently from `BufferPool`'s
//! pin/unpin API.  The bridge must:
//!
//! 1. **Implement `BTreePageStore`** using `Arc<BufferPoolHandle>`.
//! 2. **Manage the pin/unpin lifecycle** across B+ tree calls (a single insert
//!    can pin many pages in a single recursive operation).
//! 3. **Keep the header in sync** with allocator state after every `alloc_*`
//!    call.
//!
//! ## Implementation
//!
//! [`BufferPoolPageStore`] holds `Arc<BufferPoolHandle>`.
//!
//! - **Reads**: pin the page, copy the data into a heap-allocated buffer, unpin.
//!   The copy is required to satisfy the `Box<[u8; SIZE]>` return type.
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
use crate::mvcc::version::VersionEntry;
use crate::storage::btree::BTreePageStore;
use crate::storage::buffer_pool::PageSize;
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
}

impl BufferPoolPageStore {
    /// Create a `BufferPoolPageStore` backed by `handle`.
    pub(crate) fn new(handle: Arc<BufferPoolHandle>) -> Self {
        Self { handle }
    }

    /// Borrow the underlying [`BufferPoolHandle`].
    #[allow(dead_code)]
    pub(crate) fn handle(&self) -> &Arc<BufferPoolHandle> {
        &self.handle
    }
}

impl BTreePageStore for BufferPoolPageStore {
    // -----------------------------------------------------------------------
    // Reads
    // -----------------------------------------------------------------------

    fn read_internal(&self, page: u32) -> Result<Box<[u8; INTERNAL_SIZE]>> {
        let pinned = self.handle.fetch_page(page, PageSize::Small4k)?;
        let mut buf = Box::new([0u8; INTERNAL_SIZE]);
        buf.copy_from_slice(pinned.data());
        Ok(buf)
        // pinned auto-unpins here
    }

    fn read_leaf(&self, page: u32) -> Result<Box<[u8; LEAF_SIZE]>> {
        let pinned = self.handle.fetch_page(page, PageSize::Large32k)?;
        let mut buf = Box::new([0u8; LEAF_SIZE]);
        buf.copy_from_slice(pinned.data());
        Ok(buf)
        // pinned auto-unpins here
    }

    // -----------------------------------------------------------------------
    // Writes
    // -----------------------------------------------------------------------

    fn write_internal(&mut self, page: u32, data: &[u8; INTERNAL_SIZE]) -> Result<()> {
        let mut pinned = self.handle.fetch_page(page, PageSize::Small4k)?;
        pinned.data_mut().copy_from_slice(data);
        Ok(())
        // pinned auto-unpins here; dirty bit set by data_mut()
    }

    fn write_leaf(&mut self, page: u32, data: &[u8; LEAF_SIZE]) -> Result<()> {
        let mut pinned = self.handle.fetch_page(page, PageSize::Large32k)?;
        pinned.data_mut().copy_from_slice(data);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Allocation
    // -----------------------------------------------------------------------

    fn alloc_internal(&mut self) -> Result<u32> {
        self.handle.alloc_page(PageSize::Small4k)
    }

    fn alloc_leaf(&mut self) -> Result<u32> {
        self.handle.alloc_page(PageSize::Large32k)
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
    // MVCC version-chain accessors (T3.5)
    //
    // Delegate to the buffer pool's chain helpers, which operate on the
    // `Frame::version_chains` map for the 32 KB leaf partition.
    // -----------------------------------------------------------------------

    fn take_chain(
        &mut self,
        page: u32,
        key: &[u8],
    ) -> Result<Option<Arc<VecDeque<VersionEntry>>>> {
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
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::btree::{BTree, BTreePageStore};
    use crate::storage::buffer_pool::default_sizes;
    use crate::storage::buffer_pool::{BufferPool, PageSource, PageSize};
    use crate::storage::header::FileHeader;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    // -----------------------------------------------------------------------
    // In-memory PageSource for tests
    // -----------------------------------------------------------------------

    #[derive(Default)]
    struct MockIo {
        pages: StdMutex<HashMap<u32, Vec<u8>>>,
    }

    impl MockIo {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }
    }

    struct ArcIo(Arc<MockIo>);

    impl PageSource for ArcIo {
        fn read_page(&self, pn: u32, _size: PageSize, buf: &mut [u8]) -> Result<()> {
            let pages = self.0.pages.lock().unwrap();
            if let Some(data) = pages.get(&pn) {
                let n = buf.len().min(data.len());
                buf[..n].copy_from_slice(&data[..n]);
                if n < buf.len() {
                    buf[n..].fill(0);
                }
            } else {
                buf.fill(0);
            }
            Ok(())
        }
        fn write_page(&self, pn: u32, _size: PageSize, buf: &[u8]) -> Result<()> {
            self.0.pages.lock().unwrap().insert(pn, buf.to_vec());
            Ok(())
        }
    }

    fn make_store() -> BufferPoolPageStore {
        let io = MockIo::new();
        let pool = Arc::new(BufferPool::new(default_sizes::DESKTOP, Box::new(ArcIo(io))));
        let header = FileHeader::new_now();
        let handle = Arc::new(BufferPoolHandle::new(pool, header));
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
        store.write_leaf(pn, &data).unwrap();

        let read_back = store.read_leaf(pn).unwrap();
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
