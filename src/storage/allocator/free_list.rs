//! Free-list link walk — the short-lived [`PageAllocator`] borrow.
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
//! ## Scratch buffer
//!
//! [`read_free_link`](PageAllocator::read_free_link) and
//! [`write_free_link`](PageAllocator::write_free_link) move exactly one page of
//! bytes between memory and the [`PageSource`]. They borrow a caller-owned
//! `scratch` slice (sized for the largest page, 32 KiB) rather than allocating
//! and zeroing a fresh `Vec` on every call — these run inside the global
//! allocator state mutex, so an allocation per free-list link walk would charge
//! the global allocator under the lock on hot DDL-drop free paths. The buffer is
//! owned by `AllocatorState` (inside the same `Mutex`) and handed in by `&mut`,
//! so only the single live `PageAllocator` borrow touches it (no aliasing).
//!
//! ## ENOSPC
//!
//! When extending the file would cause `total_page_count` to overflow a `u32`,
//! [`Error::DiskFull`] is returned with `available_bytes: 0`.

use crate::error::{Error, Result};
use crate::storage::buffer_pool::{PageSize, PageSource};
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
    io: &'a dyn PageSource,
    /// Reusable I/O staging buffer, sized for the largest page (32 KiB).
    ///
    /// Owned by `AllocatorState` and borrowed mutably here so the free-list
    /// link read/write paths stage their single page through it instead of
    /// allocating a fresh zeroed `Vec` inside the allocator state mutex on
    /// every call. Only `buf[..size.bytes()]` is used for a given page size.
    scratch: &'a mut [u8],
}

impl<'a> PageAllocator<'a> {
    /// Create a new `PageAllocator` that modifies `header`, uses `io` for
    /// reading and writing free-list link pages, and stages page I/O through
    /// the caller-owned `scratch` buffer (must be at least 32 KiB).
    pub(crate) fn new(
        header: &'a mut FileHeader,
        io: &'a dyn PageSource,
        scratch: &'a mut [u8],
    ) -> Self {
        debug_assert!(
            scratch.len() >= PageSize::Large32k.bytes(),
            "free-list scratch buffer must cover the largest page size"
        );
        Self {
            header,
            io,
            scratch,
        }
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
    ///
    /// Stages the page read through the struct-owned `scratch` buffer rather
    /// than a fresh allocation (see the module-level scratch-buffer note). The
    /// bytes that travel over `io` are identical to a zeroed `Vec` of the same
    /// page size: the leading `size.bytes()` window is fully overwritten by the
    /// read.
    fn read_free_link(&mut self, page_number: u32, size: PageSize) -> Result<u32> {
        let buf = &mut self.scratch[..size.bytes()];
        self.io.read_page(page_number, size, buf)?;
        Ok(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]))
    }

    /// Write a free-list link page: `next` in the first 4 bytes, rest zeroed.
    ///
    /// Stages the page through the struct-owned `scratch` buffer. The buffer is
    /// re-zeroed across its `size.bytes()` window on every call so the bytes
    /// written over `io` are byte-identical to the previous fresh-`Vec`
    /// implementation (next pointer || zero fill), independent of any residue
    /// left by a prior link read or write.
    fn write_free_link(&mut self, page_number: u32, next: u32, size: PageSize) -> Result<()> {
        let buf = &mut self.scratch[..size.bytes()];
        buf.fill(0);
        buf[0..4].copy_from_slice(&next.to_le_bytes());
        self.io.write_page(page_number, size, buf)
    }
}
