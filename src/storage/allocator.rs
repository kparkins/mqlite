//! Page allocator — dual free lists for 4 KB and 32 KB pages.
//!
//! The allocator manages two singly-linked free lists embedded in the on-disk
//! file header:
//!
//! - **4 KB free list** (`free_list_head_4k`): pages used for internal B+ tree nodes.
//! - **32 KB free list** (`free_list_head_32k`): pages used for leaf nodes, overflow
//!   pages, and the file header.
//!
//! ## Free-list on-disk encoding
//!
//! Each free page stores the page number of the **next** free page in the list as a
//! 4-byte little-endian `u32` at **byte offset 0** of the page.  A value of `0`
//! signals end-of-list.  All remaining bytes in the free page are zeroed.
//!
//! ## Allocation strategy
//!
//! 1. If the free list for the requested size is non-empty, pop the head and return it.
//! 2. Otherwise, extend the virtual file: return `total_page_count` and increment it.
//!    The caller is responsible for writing the page content and ensuring the file
//!    grows to accommodate it.
//!
//! ## Deallocation
//!
//! The freed page's first 4 bytes are written with the current free-list head (the
//! "next" pointer), and the remaining bytes are zeroed.  The header's free-list head
//! is updated to point to the newly freed page (LIFO / stack discipline).
//!
//! ## Header ownership
//!
//! [`PageAllocator`] holds a mutable borrow of the [`FileHeader`].  All mutations to
//! the free-list pointers and page counts are applied to the header in memory.  The
//! **caller** is responsible for writing the updated header back to page 0 after
//! any `allocate_*` or `free_*` call so that the changes are persisted.
//!
//! ## ENOSPC
//!
//! When extending the file would cause `total_page_count` to overflow a `u32`,
//! [`Error::DiskFull`] is returned with `available_bytes: 0`.

use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::storage::buffer_pool::{PageIo, PageSize};
use crate::storage::header::FileHeader;

// ---------------------------------------------------------------------------
// PageAllocator
// ---------------------------------------------------------------------------

/// Manages page allocation and deallocation for a mqlite database file.
///
/// Maintains two singly-linked free lists — one for 4 KB (internal node) pages
/// and one for 32 KB (leaf / overflow) pages — embedded in the in-memory
/// [`FileHeader`].
///
/// The caller must write the modified [`FileHeader`] back to page 0 after any
/// `allocate_*` or `free_*` call to persist the changes.
pub(crate) struct PageAllocator<'a> {
    header: &'a mut FileHeader,
    io: &'a dyn PageIo,
}

impl<'a> PageAllocator<'a> {
    /// Create a new `PageAllocator` that modifies `header` and uses `io` for
    /// reading and writing free-list link pages.
    pub(crate) fn new(header: &'a mut FileHeader, io: &'a dyn PageIo) -> Self {
        Self { header, io }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Allocate a 4 KB page (internal B+ tree node).
    ///
    /// Returns the page number of the allocated page.  The page contents are
    /// **undefined** — the caller must write the full page before relying on it.
    ///
    /// Updates `header.free_list_head_4k`, `header.free_page_count_4k`, and
    /// possibly `header.total_page_count`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DiskFull`] if `total_page_count` would overflow `u32`.
    pub(crate) fn allocate_4k(&mut self) -> Result<u32> {
        self.allocate(PageSize::Small4k)
    }

    /// Allocate a 32 KB page (leaf node or overflow page).
    ///
    /// Returns the page number of the allocated page.  The page contents are
    /// **undefined** — the caller must write the full page before relying on it.
    ///
    /// Updates `header.free_list_head_32k`, `header.free_page_count_32k`, and
    /// possibly `header.total_page_count`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DiskFull`] if `total_page_count` would overflow `u32`.
    pub(crate) fn allocate_32k(&mut self) -> Result<u32> {
        self.allocate(PageSize::Large32k)
    }

    /// Return a 4 KB page to the free list.
    ///
    /// The freed page's first 4 bytes are overwritten with the current free-list
    /// head (little-endian `u32`); remaining bytes are zeroed.  The new head
    /// becomes `page_number`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if:
    /// - `page_number` is 0 (the file header page — must never be freed).
    /// - `page_number` is beyond the current end of file.
    pub(crate) fn free_4k(&mut self, page_number: u32) -> Result<()> {
        self.free(page_number, PageSize::Small4k)
    }

    /// Return a 32 KB page to the free list.
    ///
    /// The freed page's first 4 bytes are overwritten with the current free-list
    /// head (little-endian `u32`); remaining bytes are zeroed.  The new head
    /// becomes `page_number`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if:
    /// - `page_number` is 0 (the file header page — must never be freed).
    /// - `page_number` is beyond the current end of file.
    pub(crate) fn free_32k(&mut self, page_number: u32) -> Result<()> {
        self.free(page_number, PageSize::Large32k)
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn allocate(&mut self, size: PageSize) -> Result<u32> {
        let free_head = match size {
            PageSize::Small4k => self.header.free_list_head_4k,
            PageSize::Large32k => self.header.free_list_head_32k,
        };

        if free_head != 0 {
            // Pop from the free list: read the "next" pointer stored in the
            // first 4 bytes of the free page.
            let next = self.read_free_link(free_head, size)?;

            match size {
                PageSize::Small4k => {
                    self.header.free_list_head_4k = next;
                    self.header.free_page_count_4k =
                        self.header.free_page_count_4k.saturating_sub(1);
                }
                PageSize::Large32k => {
                    self.header.free_list_head_32k = next;
                    self.header.free_page_count_32k =
                        self.header.free_page_count_32k.saturating_sub(1);
                }
            }

            Ok(free_head)
        } else {
            // Extend the virtual file: claim the next page number.
            let page_number = self.header.total_page_count;
            self.header.total_page_count =
                self.header.total_page_count.checked_add(1).ok_or_else(|| {
                    #[cfg(feature = "tracing")]
                    tracing::error!(target: "mqlite", "mqlite::disk_full");
                    Error::DiskFull {
                        path: std::path::PathBuf::new(),
                        required_bytes: 4096,
                        available_bytes: 0,
                        suggestion: "page count exhausted (u32 overflow); \
                                    database has reached maximum size"
                            .into(),
                    }
                })?;
            Ok(page_number)
        }
    }

    fn free(&mut self, page_number: u32, size: PageSize) -> Result<()> {
        // Guard: page 0 is the file header — must never be freed.
        if page_number == 0 {
            return Err(Error::Internal(
                "cannot free page 0 (file header page)".into(),
            ));
        }
        // Guard: page must be within the file.
        if page_number >= self.header.total_page_count {
            return Err(Error::Internal(format!(
                "cannot free page {page_number}: beyond end of file \
                 (total_page_count = {})",
                self.header.total_page_count
            )));
        }

        let old_head = match size {
            PageSize::Small4k => self.header.free_list_head_4k,
            PageSize::Large32k => self.header.free_list_head_32k,
        };

        // Write the link page: first 4 bytes = old_head (next pointer), rest zero.
        self.write_free_link(page_number, old_head, size)?;

        // Update header to point to the newly freed page.
        match size {
            PageSize::Small4k => {
                self.header.free_list_head_4k = page_number;
                self.header.free_page_count_4k = self
                    .header
                    .free_page_count_4k
                    .checked_add(1)
                    .ok_or_else(|| Error::Internal("free_page_count_4k overflow".into()))?;
            }
            PageSize::Large32k => {
                self.header.free_list_head_32k = page_number;
                self.header.free_page_count_32k = self
                    .header
                    .free_page_count_32k
                    .checked_add(1)
                    .ok_or_else(|| Error::Internal("free_page_count_32k overflow".into()))?;
            }
        }

        Ok(())
    }

    /// Read the free-list link (next-page pointer) from the first 4 bytes of
    /// `page_number`.
    fn read_free_link(&self, page_number: u32, size: PageSize) -> Result<u32> {
        let mut buf = vec![0u8; size.bytes()];
        self.io.read_page(page_number, size, &mut buf)?;
        Ok(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]))
    }

    /// Write a free-list link page: `next` in the first 4 bytes, rest zeroed.
    fn write_free_link(&self, page_number: u32, next: u32, size: PageSize) -> Result<()> {
        let mut buf = vec![0u8; size.bytes()];
        buf[0..4].copy_from_slice(&next.to_le_bytes());
        self.io.write_page(page_number, size, &buf)
    }
}

// ---------------------------------------------------------------------------
// AllocatorHandle — owned-state allocator for concurrent use
// ---------------------------------------------------------------------------

/// Owned state for the [`AllocatorHandle`].
struct AllocatorState {
    header: FileHeader,
    header_dirty: bool,
}

/// A `Clone`-able, `Arc`-wrapped allocator handle that owns the
/// [`FileHeader`] rather than borrowing it.
///
/// Resolves **RISK-03** from the Phase 1 reconciliation plan: the original
/// [`PageAllocator`] holds `header: &'a mut FileHeader`, which makes it
/// impossible to use concurrently from multiple threads or to store in an
/// `Arc`-shared struct like `DatabaseInner`.
///
/// `AllocatorHandle` wraps the header in `Arc<Mutex<AllocatorState>>`.  All
/// allocations and deallocations lock the mutex, perform the operation via a
/// short-lived [`PageAllocator`], and release the lock.
///
/// After any allocation or free, the in-memory header is marked dirty.  Call
/// [`flush_header`](AllocatorHandle::flush_header) to persist the updated
/// header to page 0 through a `PageIo`.
#[derive(Clone)]
pub(crate) struct AllocatorHandle {
    state: Arc<Mutex<AllocatorState>>,
}

impl AllocatorHandle {
    /// Create an `AllocatorHandle` from an existing [`FileHeader`].
    ///
    /// The header is placed in clean state (not dirty).  Call
    /// [`flush_header`](Self::flush_header) after any allocations to persist
    /// changes.
    pub(crate) fn new(header: FileHeader) -> Self {
        Self {
            state: Arc::new(Mutex::new(AllocatorState {
                header,
                header_dirty: false,
            })),
        }
    }

    // -----------------------------------------------------------------------
    // Allocation
    // -----------------------------------------------------------------------

    /// Allocate a 4 KB internal-node page.
    ///
    /// Updates the in-memory free list and marks the header dirty.  The
    /// caller must call [`flush_header`](Self::flush_header) (or flush the
    /// buffer pool) to persist the change to disk.
    pub(crate) fn alloc_4k(&self, io: &dyn PageIo) -> Result<u32> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("allocator mutex poisoned".into()))?;
        let mut alloc = PageAllocator::new(&mut state.header, io);
        let page_no = alloc.allocate_4k()?;
        state.header_dirty = true;
        Ok(page_no)
    }

    /// Allocate a 32 KB leaf / overflow page.
    ///
    /// Updates the in-memory free list and marks the header dirty.
    pub(crate) fn alloc_32k(&self, io: &dyn PageIo) -> Result<u32> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("allocator mutex poisoned".into()))?;
        let mut alloc = PageAllocator::new(&mut state.header, io);
        let page_no = alloc.allocate_32k()?;
        state.header_dirty = true;
        Ok(page_no)
    }

    // -----------------------------------------------------------------------
    // Deallocation
    // -----------------------------------------------------------------------

    /// Return a 4 KB page to the free list.
    ///
    /// Marks the header dirty.  The freed page's first 4 bytes are
    /// overwritten with the free-list head pointer via `io`.
    pub(crate) fn free_4k(&self, page_number: u32, io: &dyn PageIo) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("allocator mutex poisoned".into()))?;
        let mut alloc = PageAllocator::new(&mut state.header, io);
        alloc.free_4k(page_number)?;
        state.header_dirty = true;
        Ok(())
    }

    /// Return a 32 KB page to the free list.
    ///
    /// Marks the header dirty.  The freed page's first 4 bytes are
    /// overwritten with the free-list head pointer via `io`.
    pub(crate) fn free_32k(&self, page_number: u32, io: &dyn PageIo) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("allocator mutex poisoned".into()))?;
        let mut alloc = PageAllocator::new(&mut state.header, io);
        alloc.free_32k(page_number)?;
        state.header_dirty = true;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Header access
    // -----------------------------------------------------------------------

    /// Read the current in-memory file header.
    ///
    /// The closure receives a shared reference to the header; its return
    /// value is returned from this method.
    pub(crate) fn with_header<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&FileHeader) -> R,
    {
        let state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("allocator mutex poisoned".into()))?;
        Ok(f(&state.header))
    }

    /// Mutate the in-memory file header and mark it dirty.
    ///
    /// Use this to update fields such as `catalog_root_page` after a B+ tree
    /// root split, without going through the allocation path.
    pub(crate) fn update_header<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&mut FileHeader),
    {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("allocator mutex poisoned".into()))?;
        f(&mut state.header);
        state.header_dirty = true;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Flush
    // -----------------------------------------------------------------------

    /// Write the in-memory header back to page 0 via `io` if it is dirty.
    ///
    /// Clears the dirty flag on success.  If the header is clean, this is a
    /// no-op.
    ///
    /// Typically called after all B+ tree operations in a transaction are
    /// complete, before the WAL commit frame is written.
    pub(crate) fn flush_header(&self, io: &dyn PageIo) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| Error::Internal("allocator mutex poisoned".into()))?;
        if state.header_dirty {
            let bytes = state.header.to_bytes();
            io.write_page(0, PageSize::Small4k, &bytes)?;
            state.header_dirty = false;
        }
        Ok(())
    }

    /// Return `true` if the in-memory header has been modified since the
    /// last [`flush_header`](Self::flush_header) call.
    pub(crate) fn is_header_dirty(&self) -> bool {
        self.state
            .lock()
            .map(|s| s.header_dirty)
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    // -----------------------------------------------------------------------
    // MockIo — in-memory PageIo for tests
    // -----------------------------------------------------------------------

    /// In-memory page store for testing.  Pages are stored as raw byte vectors
    /// keyed by page number.  Reads of absent pages return zeroed bytes.
    struct MockIo {
        pages: Mutex<HashMap<u32, Vec<u8>>>,
    }

    impl MockIo {
        fn new() -> Self {
            Self {
                pages: Mutex::new(HashMap::new()),
            }
        }

        /// Return the raw bytes stored for `page_number`, or `None` if the
        /// page has never been written.
        fn get_raw(&self, page_number: u32) -> Option<Vec<u8>> {
            self.pages
                .lock()
                .expect("MockIo lock poisoned")
                .get(&page_number)
                .cloned()
        }
    }

    impl PageIo for MockIo {
        fn read_page(&self, page_number: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
            assert_eq!(buf.len(), size.bytes(), "buf.len() must equal size.bytes()");
            let pages = self.pages.lock().expect("MockIo lock poisoned");
            if let Some(stored) = pages.get(&page_number) {
                buf.copy_from_slice(&stored[..buf.len()]);
            }
            // Absent pages read as zeroes — buf already zero-initialised by caller.
            Ok(())
        }

        fn write_page(&self, page_number: u32, size: PageSize, buf: &[u8]) -> Result<()> {
            assert_eq!(buf.len(), size.bytes(), "buf.len() must equal size.bytes()");
            self.pages
                .lock()
                .expect("MockIo lock poisoned")
                .insert(page_number, buf.to_vec());
            Ok(())
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// A fresh header with total_page_count = 1 (page 0 = header).
    fn fresh_header() -> FileHeader {
        FileHeader::new(0, 0, 0)
    }

    // -----------------------------------------------------------------------
    // Allocate — empty free list (extend file)
    // -----------------------------------------------------------------------

    #[test]
    fn allocate_4k_from_empty_freelist_returns_page_1() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        let mut alloc = PageAllocator::new(&mut hdr, &io);

        let pn = alloc.allocate_4k().expect("should allocate");
        assert_eq!(pn, 1, "first allocated page must be 1");
    }

    #[test]
    fn allocate_32k_from_empty_freelist_returns_page_1() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        let mut alloc = PageAllocator::new(&mut hdr, &io);

        let pn = alloc.allocate_32k().expect("should allocate");
        assert_eq!(pn, 1);
    }

    #[test]
    fn allocate_extends_total_page_count() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        assert_eq!(hdr.total_page_count, 1);

        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.allocate_4k().unwrap();
            alloc.allocate_32k().unwrap();
            alloc.allocate_4k().unwrap();
        }

        assert_eq!(hdr.total_page_count, 4, "three allocations → page count 4");
    }

    #[test]
    fn sequential_allocations_return_consecutive_page_numbers() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        let mut alloc = PageAllocator::new(&mut hdr, &io);

        let a = alloc.allocate_4k().unwrap();
        let b = alloc.allocate_4k().unwrap();
        let c = alloc.allocate_32k().unwrap();

        assert_eq!(a, 1);
        assert_eq!(b, 2);
        assert_eq!(c, 3);
    }

    // -----------------------------------------------------------------------
    // Free — basic
    // -----------------------------------------------------------------------

    #[test]
    fn free_4k_updates_header_fields() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 3; // pretend pages 1 and 2 exist

        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.free_4k(1).unwrap();
        }

        assert_eq!(hdr.free_list_head_4k, 1, "head must point to freed page");
        assert_eq!(hdr.free_page_count_4k, 1);
        assert_eq!(hdr.free_list_head_32k, 0, "32k list untouched");
    }

    #[test]
    fn free_32k_updates_header_fields() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 3;

        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.free_32k(2).unwrap();
        }

        assert_eq!(hdr.free_list_head_32k, 2);
        assert_eq!(hdr.free_page_count_32k, 1);
        assert_eq!(hdr.free_list_head_4k, 0, "4k list untouched");
    }

    #[test]
    fn freed_page_stores_next_pointer_in_first_4_bytes_as_zero() {
        // When the free list was empty, the "next" link written to the freed
        // page must be 0 (end-of-list sentinel).
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 2;

        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.free_4k(1).unwrap();
        }

        let raw = io.get_raw(1).expect("page must have been written");
        let next = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
        assert_eq!(next, 0, "single free page → next = 0");
        // All bytes beyond the link must be zero.
        assert!(
            raw[4..].iter().all(|&b| b == 0),
            "tail bytes must be zeroed"
        );
    }

    #[test]
    fn freed_page_stores_next_pointer_when_list_nonempty() {
        // Free page 1 first (head = 1), then free page 2 (new head = 2, next = 1).
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 3;

        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.free_4k(1).unwrap();
            alloc.free_4k(2).unwrap();
        }

        // Page 2 is the new head; its link must point to page 1.
        let raw2 = io.get_raw(2).unwrap();
        let next2 = u32::from_le_bytes([raw2[0], raw2[1], raw2[2], raw2[3]]);
        assert_eq!(next2, 1);

        assert_eq!(hdr.free_list_head_4k, 2);
        assert_eq!(hdr.free_page_count_4k, 2);
    }

    // -----------------------------------------------------------------------
    // Allocate — from non-empty free list (recycle)
    // -----------------------------------------------------------------------

    #[test]
    fn free_then_alloc_recycles_page_4k() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 2;

        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.free_4k(1).unwrap();
            let recycled = alloc.allocate_4k().unwrap();
            assert_eq!(recycled, 1, "must reuse freed page");
            // Free list should be empty again.
            assert_eq!(hdr.free_list_head_4k, 0);
            assert_eq!(hdr.free_page_count_4k, 0);
            // total_page_count unchanged (no file extension needed).
            assert_eq!(hdr.total_page_count, 2);
        }
    }

    #[test]
    fn free_then_alloc_recycles_page_32k() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 2;

        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.free_32k(1).unwrap();
            let recycled = alloc.allocate_32k().unwrap();
            assert_eq!(recycled, 1);
            assert_eq!(hdr.free_list_head_32k, 0);
            assert_eq!(hdr.free_page_count_32k, 0);
        }
    }

    #[test]
    fn alloc_from_freelist_is_lifo_4k() {
        // Free pages 1, 2, 3 in order → list is [3 → 2 → 1].
        // Allocating must return 3, then 2, then 1.
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 4;

        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.free_4k(1).unwrap();
            alloc.free_4k(2).unwrap();
            alloc.free_4k(3).unwrap();

            assert_eq!(alloc.allocate_4k().unwrap(), 3);
            assert_eq!(alloc.allocate_4k().unwrap(), 2);
            assert_eq!(alloc.allocate_4k().unwrap(), 1);
            // List exhausted; next allocation extends file.
            assert_eq!(alloc.allocate_4k().unwrap(), 4);
        }

        assert_eq!(hdr.total_page_count, 5);
        assert_eq!(hdr.free_page_count_4k, 0);
    }

    #[test]
    fn alloc_from_freelist_is_lifo_32k() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 3;

        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.free_32k(1).unwrap();
            alloc.free_32k(2).unwrap();

            assert_eq!(alloc.allocate_32k().unwrap(), 2);
            assert_eq!(alloc.allocate_32k().unwrap(), 1);
        }
    }

    #[test]
    fn free_and_alloc_many_pages_no_leak_4k() {
        let io = MockIo::new();
        let mut hdr = fresh_header();

        // Allocate 10 pages.
        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            for _ in 1..=10 {
                alloc.allocate_4k().unwrap();
            }
        }
        assert_eq!(hdr.total_page_count, 11);

        // Free all 10 pages.
        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            for pn in 1..=10 {
                alloc.free_4k(pn).unwrap();
            }
        }
        assert_eq!(hdr.free_page_count_4k, 10);

        // Reallocate all 10; they must come from the free list, not extend
        // the file.
        let reclaimed = {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            let mut pages = Vec::new();
            for _ in 0..10 {
                pages.push(alloc.allocate_4k().unwrap());
            }
            pages
        };
        assert_eq!(hdr.total_page_count, 11, "file must not have grown");
        assert_eq!(hdr.free_page_count_4k, 0);

        // All reclaimed pages must be in [1, 10].
        for pn in &reclaimed {
            assert!(
                (1..=10).contains(pn),
                "reclaimed page {pn} out of expected range"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn free_page_0_returns_error() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 2;

        let mut alloc = PageAllocator::new(&mut hdr, &io);
        let result = alloc.free_4k(0);
        assert!(result.is_err(), "freeing page 0 must fail");
    }

    #[test]
    fn free_page_0_returns_error_32k() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 2;

        let mut alloc = PageAllocator::new(&mut hdr, &io);
        let result = alloc.free_32k(0);
        assert!(result.is_err(), "freeing page 0 (32k) must fail");
    }

    #[test]
    fn free_page_beyond_file_returns_error_4k() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 2; // pages 0 and 1 exist

        let mut alloc = PageAllocator::new(&mut hdr, &io);
        // Page 5 does not exist.
        let result = alloc.free_4k(5);
        assert!(result.is_err(), "freeing out-of-bounds page must fail");
    }

    #[test]
    fn free_page_beyond_file_returns_error_32k() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 2;

        let mut alloc = PageAllocator::new(&mut hdr, &io);
        let result = alloc.free_32k(99);
        assert!(result.is_err());
    }

    #[test]
    fn allocate_overflow_returns_disk_full() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = u32::MAX; // one short of overflow

        let mut alloc = PageAllocator::new(&mut hdr, &io);
        // Allocating would push total_page_count past u32::MAX.
        let result = alloc.allocate_4k();
        assert!(
            matches!(
                result,
                Err(Error::DiskFull {
                    available_bytes: 0,
                    ..
                })
            ),
            "u32 overflow must return DiskFull, got {result:?}"
        );
    }

    // -----------------------------------------------------------------------
    // Independent lists — 4 KB and 32 KB are separate
    // -----------------------------------------------------------------------

    #[test]
    fn lists_are_independent() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 3;

        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.free_4k(1).unwrap();
            alloc.free_32k(2).unwrap();
        }

        assert_eq!(hdr.free_list_head_4k, 1);
        assert_eq!(hdr.free_list_head_32k, 2);
        assert_eq!(hdr.free_page_count_4k, 1);
        assert_eq!(hdr.free_page_count_32k, 1);

        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            // Allocating 32k should not touch the 4k list.
            let p = alloc.allocate_32k().unwrap();
            assert_eq!(p, 2);
            assert_eq!(hdr.free_list_head_4k, 1, "4k list must be unchanged");
            assert_eq!(hdr.free_page_count_4k, 1);
        }
    }

    // -----------------------------------------------------------------------
    // Roundtrip: allocate → free → reallocate preserves page number
    // -----------------------------------------------------------------------

    #[test]
    fn roundtrip_single_4k() {
        let io = MockIo::new();
        let mut hdr = fresh_header();

        let page_number = {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.allocate_4k().unwrap()
        };
        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.free_4k(page_number).unwrap();
        }
        let recycled = {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.allocate_4k().unwrap()
        };
        assert_eq!(recycled, page_number);
    }

    #[test]
    fn roundtrip_single_32k() {
        let io = MockIo::new();
        let mut hdr = fresh_header();

        let page_number = {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.allocate_32k().unwrap()
        };
        {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.free_32k(page_number).unwrap();
        }
        let recycled = {
            let mut alloc = PageAllocator::new(&mut hdr, &io);
            alloc.allocate_32k().unwrap()
        };
        assert_eq!(recycled, page_number);
    }

    // -----------------------------------------------------------------------
    // AllocatorHandle tests
    // -----------------------------------------------------------------------

    #[test]
    fn handle_alloc_4k_returns_correct_page() {
        let io = MockIo::new();
        let hdr = fresh_header();
        let handle = AllocatorHandle::new(hdr);

        let page = handle.alloc_4k(&io).unwrap();
        assert_eq!(page, 1, "first alloc must be page 1");
    }

    #[test]
    fn handle_alloc_32k_returns_correct_page() {
        let io = MockIo::new();
        let hdr = fresh_header();
        let handle = AllocatorHandle::new(hdr);

        let page = handle.alloc_32k(&io).unwrap();
        assert_eq!(page, 1);
    }

    #[test]
    fn handle_sequential_allocs_return_consecutive_pages() {
        let io = MockIo::new();
        let hdr = fresh_header();
        let handle = AllocatorHandle::new(hdr);

        let a = handle.alloc_4k(&io).unwrap();
        let b = handle.alloc_32k(&io).unwrap();
        let c = handle.alloc_4k(&io).unwrap();

        assert_eq!(a, 1);
        assert_eq!(b, 2);
        assert_eq!(c, 3);
    }

    #[test]
    fn handle_marks_header_dirty_after_alloc() {
        let io = MockIo::new();
        let hdr = fresh_header();
        let handle = AllocatorHandle::new(hdr);

        assert!(!handle.is_header_dirty(), "header should be clean on create");
        handle.alloc_4k(&io).unwrap();
        assert!(handle.is_header_dirty(), "header must be dirty after alloc");
    }

    #[test]
    fn handle_flush_header_writes_to_page_0() {
        let io = MockIo::new();
        let hdr = fresh_header();
        let handle = AllocatorHandle::new(hdr);

        handle.alloc_4k(&io).unwrap();
        handle.flush_header(&io).unwrap();

        assert!(!handle.is_header_dirty(), "header must be clean after flush");
        // Page 0 must have been written.
        let raw = io.get_raw(0).expect("page 0 must be written on flush");
        assert_eq!(raw.len(), PageSize::Small4k.bytes());
    }

    #[test]
    fn handle_flush_header_noop_when_clean() {
        let io = MockIo::new();
        let hdr = fresh_header();
        let handle = AllocatorHandle::new(hdr);

        // No allocations — header is clean.
        handle.flush_header(&io).unwrap();

        assert!(
            io.get_raw(0).is_none(),
            "flush_header must not write page 0 when header is clean"
        );
    }

    #[test]
    fn handle_free_and_realloc_recycles_page() {
        let io = MockIo::new();
        let mut hdr = fresh_header();
        hdr.total_page_count = 3; // pretend pages 1 and 2 exist
        let handle = AllocatorHandle::new(hdr);

        handle.free_4k(1, &io).unwrap();
        let recycled = handle.alloc_4k(&io).unwrap();
        assert_eq!(recycled, 1, "freed page must be recycled");
    }

    #[test]
    fn handle_with_header_reads_total_page_count() {
        let io = MockIo::new();
        let hdr = fresh_header();
        let handle = AllocatorHandle::new(hdr);

        handle.alloc_4k(&io).unwrap();
        handle.alloc_32k(&io).unwrap();

        let count = handle.with_header(|h| h.total_page_count).unwrap();
        assert_eq!(count, 3, "two allocs from page 1 = total 3");
    }

    #[test]
    fn handle_update_header_marks_dirty_and_persists() {
        let io = MockIo::new();
        let hdr = fresh_header();
        let handle = AllocatorHandle::new(hdr);

        handle
            .update_header(|h| h.catalog_root_page = 42)
            .unwrap();

        assert!(handle.is_header_dirty());
        let root = handle.with_header(|h| h.catalog_root_page).unwrap();
        assert_eq!(root, 42);
    }

    #[test]
    fn handle_is_clone_and_shares_state() {
        let io = MockIo::new();
        let hdr = fresh_header();
        let handle = AllocatorHandle::new(hdr);
        let handle2 = handle.clone();

        // Alloc through clone 1.
        handle.alloc_4k(&io).unwrap();

        // Clone 2 sees the updated state.
        let count = handle2.with_header(|h| h.total_page_count).unwrap();
        assert_eq!(count, 2, "clone must share underlying state");
    }
}
