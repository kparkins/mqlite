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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::mvcc::deferred_free::DeferredFreeQueue;
use crate::storage::buffer_pool::{PageSource, PageSize};
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
}

impl<'a> PageAllocator<'a> {
    /// Create a new `PageAllocator` that modifies `header` and uses `io` for
    /// reading and writing free-list link pages.
    pub(crate) fn new(header: &'a mut FileHeader, io: &'a dyn PageSource) -> Self {
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

/// Inner state of an [`AllocatorHandle`], shared via a single `Arc`.
struct AllocatorInner {
    state: Mutex<AllocatorState>,
    /// Per-overflow-chain refcount table.
    ///
    /// Maps `first_page` → shared `AtomicU32` pin counter. Populated when
    /// the first `OverflowRef` for a chain is created. Cloned-out
    /// `Arc<AtomicU32>` handles let callers do atomic ops without holding
    /// the HashMap mutex.
    ///
    /// Atomic ops on the refcount happen OUTSIDE the allocator state mutex.
    overflow_refcounts: Mutex<HashMap<u32, Arc<AtomicU32>>>,
    /// Refcount-to-zero queue drained by the writer path.
    ///
    /// Lock-order position 1.5 (before `state` at position 2).
    deferred_free_queue: DeferredFreeQueue,
}

/// A `Clone`-able, `Arc`-wrapped allocator handle that owns the
/// [`FileHeader`] rather than borrowing it.
///
/// Wraps all shared state in a single `Arc<AllocatorInner>`. All
/// allocations and deallocations lock the state mutex, perform the
/// operation via a short-lived [`PageAllocator`], and release the lock.
///
/// After any allocation or free, the in-memory header is marked dirty.  Call
/// [`flush_header`](AllocatorHandle::flush_header) to persist the updated
/// header to page 0 through a `PageSource`.
#[derive(Clone)]
pub(crate) struct AllocatorHandle {
    inner: Arc<AllocatorInner>,
}

impl AllocatorHandle {
    /// Create an `AllocatorHandle` from an existing [`FileHeader`].
    ///
    /// The header is placed in clean state (not dirty).  Call
    /// [`flush_header`](Self::flush_header) after any allocations to persist
    /// changes.
    pub(crate) fn new(header: FileHeader) -> Self {
        Self {
            inner: Arc::new(AllocatorInner {
                state: Mutex::new(AllocatorState {
                    header,
                    header_dirty: false,
                }),
                overflow_refcounts: Mutex::new(HashMap::new()),
                deferred_free_queue: DeferredFreeQueue::new(),
            }),
        }
    }

    /// Borrow the deferred-free queue (used by `OverflowRef::drop` and the
    /// writer-path drain).
    pub(crate) fn deferred_free_queue(&self) -> &DeferredFreeQueue {
        &self.inner.deferred_free_queue
    }

    // -----------------------------------------------------------------------
    // Overflow refcount
    // -----------------------------------------------------------------------
    //
    // The refcount for each overflow chain lives in an AtomicU32 that is
    // logically bound to the first page of the chain. These methods access
    // the atomic OUTSIDE the allocator state mutex — only mutations of the
    // allocator state (`state.header`) take that mutex. The per-entry atomic
    // access pattern lets Clone / Drop on OverflowRef stay lock-free on the
    // hot path.
    //
    // `drain_free_queue` is the single writer-path that transitions a page
    // from refcount=0 → free list.

    /// Look up or create the shared `AtomicU32` refcount for `first_page`.
    fn refcount_handle(&self, first_page: u32) -> Arc<AtomicU32> {
        #[allow(clippy::unwrap_used)]
        let mut table = self.inner.overflow_refcounts.lock().unwrap();
        table
            .entry(first_page)
            .or_insert_with(|| Arc::new(AtomicU32::new(0)))
            .clone()
    }

    /// Get the refcount handle if one exists; do not create.
    fn refcount_handle_opt(&self, first_page: u32) -> Option<Arc<AtomicU32>> {
        #[allow(clippy::unwrap_used)]
        let table = self.inner.overflow_refcounts.lock().unwrap();
        table.get(&first_page).cloned()
    }

    /// Saturating CAS-loop incref on the overflow-chain refcount.
    ///
    /// Returns the new (post-bump) refcount on success. Returns
    /// [`Error::RefcountOverflow`] if the observed pre-bump value is
    /// `u32::MAX`; in that case the atomic is left unchanged under every
    /// interleaving.
    ///
    /// Ordering:
    /// * Acquire on the initial load and on failed CAS attempts —
    ///   synchronizes-with prior Release decrefs for visibility of
    ///   preceding writes to the page's metadata.
    /// * Release on successful CAS store — synchronizes-with subsequent
    ///   Acquire decrefs and the `drain_free_queue` Acquire recheck.
    pub(crate) fn incref_overflow(&self, first_page: u32) -> Result<u32> {
        let atomic = self.refcount_handle(first_page);
        let mut cur = atomic.load(Ordering::Acquire);
        loop {
            if cur == u32::MAX {
                return Err(Error::RefcountOverflow);
            }
            match atomic.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(cur + 1),
                Err(observed) => {
                    crate::mvcc::metrics::record_overflow_refcount_cas_retry();
                    cur = observed;
                }
            }
        }
    }

    /// Decref. Returns the post-decrement refcount.
    ///
    /// Ordering: Release on `fetch_sub`, synchronizing with subsequent
    /// Acquire loads by `overflow_refcount` / `drain_free_queue`.
    ///
    /// # Panics
    /// Debug-asserts that the pre-decrement count is > 0. In release
    /// builds a decref on an unknown / zeroed refcount returns 0 and has
    /// no net effect (defense-in-depth for a class of bugs that RAII is
    /// supposed to prevent).
    pub(crate) fn decref_overflow(&self, first_page: u32) -> u32 {
        let Some(atomic) = self.refcount_handle_opt(first_page) else {
            debug_assert!(
                false,
                "decref on unknown first_page {first_page} — pin accounting bug"
            );
            return 0;
        };
        let prev = atomic.fetch_sub(1, Ordering::Release);
        debug_assert!(prev > 0, "decref on already-zero refcount");
        prev.saturating_sub(1)
    }

    /// Read-only refcount probe. Uses Acquire so the reader sees all prior
    /// Release decrefs.
    pub(crate) fn overflow_refcount(&self, first_page: u32) -> u32 {
        self.refcount_handle_opt(first_page)
            .map_or(0, |a| a.load(Ordering::Acquire))
    }

    /// Number of overflow-chain `first_page`s whose refcount is currently
    /// `>= 1`. Backs the `mvcc.overflow.pages_in_use` gauge.
    ///
    /// Walks the refcount table under the table mutex; acceptable because
    /// this is called at checkpoint cadence, not on the hot path.
    pub(crate) fn overflow_pages_in_use(&self) -> usize {
        #[allow(clippy::unwrap_used)]
        let table = self.inner.overflow_refcounts.lock().unwrap();
        table
            .values()
            .filter(|a| a.load(Ordering::Acquire) >= 1)
            .count()
    }

    /// Enqueue a page for deferred free. Called by `OverflowRef::drop`
    /// when the decrement brings refcount to 0.
    pub(crate) fn enqueue_deferred_free(&self, first_page: u32) {
        self.inner.deferred_free_queue.push(first_page);
    }

    /// Writer-serialized drain of the deferred-free queue.
    ///
    /// Precondition: caller holds writer serialization. For each queued
    /// page, re-loads refcount with Acquire ordering and frees only if
    /// still 0. A non-zero count re-enqueues (defense-in-depth; should be
    /// unreachable under RAII correctness).
    ///
    /// Locks acquired in order: queue (1.5) → state (2). The queue is
    /// drained into a Vec before `state` is locked, so the two locks are
    /// never held simultaneously.
    ///
    /// Ticks `mvcc.overflow.pages_freed_total` per freed page and refreshes
    /// the `mvcc.deferred_free_queue_depth` gauge with the post-drain
    /// queue size (accounts for requeued entries).
    ///
    /// Returns the number of pages actually freed.
    pub(crate) fn drain_free_queue(&self, io: &dyn PageSource) -> Result<usize> {
        let pages = self.inner.deferred_free_queue.take_all();
        if pages.is_empty() {
            crate::mvcc::metrics::set_deferred_free_queue_depth(0);
            return Ok(0);
        }

        let mut state = self
            .inner.state
            .lock()
            .map_err(|_| Error::Internal("allocator mutex poisoned".into()))?;
        let mut freed = 0usize;
        let mut requeue: Vec<u32> = Vec::new();

        for page in pages {
            let cnt = self.overflow_refcount(page);
            if cnt == 0 {
                let mut alloc = PageAllocator::new(&mut state.header, io);
                alloc.free_32k(page)?;
                // Drop the refcount entry — the page is no longer live.
                #[allow(clippy::unwrap_used)]
                let mut table = self.inner.overflow_refcounts.lock().unwrap();
                table.remove(&page);
                freed += 1;
                crate::mvcc::metrics::record_overflow_page_freed();
            } else {
                requeue.push(page);
            }
        }

        state.header_dirty = true;
        drop(state);

        if !requeue.is_empty() {
            self.inner.deferred_free_queue.push_many(requeue);
        }
        crate::mvcc::metrics::set_deferred_free_queue_depth(
            self.inner.deferred_free_queue.depth() as u64,
        );
        Ok(freed)
    }

    /// Drain the deferred-free queue but hand the 0-refcount pages to the
    /// caller as a plain `Vec<u32>` rather than freeing them to the allocator.
    ///
    /// Writer-txn `begin` uses this so the drained pages become
    /// `PageOrigin::DeferredFree` reservations on the txn's overlay.
    /// On commit the overlay translates each reservation into a proper
    /// `free_*` call; on rollback the reservation pushes the page back
    /// onto the queue, preserving the "concurrent readers must observe
    /// refcount before free" invariant.
    ///
    /// Refcount recheck uses Acquire ordering (matches `drain_free_queue`).
    /// Entries whose refcount is still non-zero are re-enqueued.
    ///
    /// Precondition: caller holds writer serialization (same as
    /// `drain_free_queue`).
    pub(crate) fn drain_deferred_free_reservations(&self) -> Vec<u32> {
        let pages = self.inner.deferred_free_queue.take_all();
        if pages.is_empty() {
            crate::mvcc::metrics::set_deferred_free_queue_depth(0);
            return Vec::new();
        }
        let mut ready = Vec::new();
        let mut requeue: Vec<u32> = Vec::new();
        for page in pages {
            let cnt = self.overflow_refcount(page);
            if cnt == 0 {
                // Drop the refcount entry — the page is no longer live.
                #[allow(clippy::unwrap_used)]
                let mut table = self.inner.overflow_refcounts.lock().unwrap();
                table.remove(&page);
                ready.push(page);
            } else {
                requeue.push(page);
            }
        }
        if !requeue.is_empty() {
            self.inner.deferred_free_queue.push_many(requeue);
        }
        crate::mvcc::metrics::set_deferred_free_queue_depth(
            self.inner.deferred_free_queue.depth() as u64,
        );
        ready
    }

    /// Test-only: force-set the refcount for a page (used by CAS-saturation
    /// contract tests).
    #[cfg(test)]
    pub(crate) fn set_overflow_refcount_for_test(&self, first_page: u32, value: u32) {
        let atomic = self.refcount_handle(first_page);
        atomic.store(value, Ordering::Release);
    }

    // -----------------------------------------------------------------------
    // Allocation
    // -----------------------------------------------------------------------

    /// Allocate a 4 KB internal-node page.
    ///
    /// Updates the in-memory free list and marks the header dirty.  The
    /// caller must call [`flush_header`](Self::flush_header) (or flush the
    /// buffer pool) to persist the change to disk.
    pub(crate) fn alloc_4k(&self, io: &dyn PageSource) -> Result<u32> {
        let mut state = self
            .inner.state
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
    pub(crate) fn alloc_32k(&self, io: &dyn PageSource) -> Result<u32> {
        let mut state = self
            .inner.state
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
    pub(crate) fn free_4k(&self, page_number: u32, io: &dyn PageSource) -> Result<()> {
        let mut state = self
            .inner.state
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
    pub(crate) fn free_32k(&self, page_number: u32, io: &dyn PageSource) -> Result<()> {
        let mut state = self
            .inner.state
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
    #[allow(dead_code)]
    pub(crate) fn with_header<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&FileHeader) -> R,
    {
        let state = self
            .inner.state
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
            .inner.state
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
    pub(crate) fn flush_header(&self, io: &dyn PageSource) -> Result<()> {
        let mut state = self
            .inner.state
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
    #[allow(dead_code)]
    pub(crate) fn is_header_dirty(&self) -> bool {
        self.inner.state.lock().map_or(false, |s| s.header_dirty)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "allocator_tests.rs"]
mod tests;
