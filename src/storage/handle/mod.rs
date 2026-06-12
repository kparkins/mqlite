//! `BufferPoolHandle` — high-level page I/O combining buffer pool + allocator.
//!
//! ## Design
//!
//! [`BufferPoolHandle`] is the single access point for all page I/O in the
//! storage engine.  It wires together:
//!
//! - [`BufferPool`] — in-memory page cache with CLOCK eviction.
//! - [`AllocatorHandle`] — owned-state dual free-list page allocator.
//! - [`BufferPoolPageSource`] — thin adapter that routes all I/O (including
//!   allocator free-list maintenance) through the buffer pool.
//!
//! The intent is that `DatabaseInner` holds a single
//! `Arc<BufferPoolHandle>` and all storage-engine layers (B+ tree, catalog,
//! journal flush path) interact with pages exclusively through this handle.
//!
//! ## Module layout
//!
//! - this module — the [`BufferPoolHandle`] core: page I/O (`fetch_page`),
//!   allocation, deallocation, flush, and direct accessors.
//! - [`journal_io`] — the journal facade wrappers (LSN reservation, durability
//!   waits, stamps, salts). A second inherent impl block; no trait object and
//!   no wrapper struct, so the cached `Arc<LogManager>` reserve fast path stays
//!   monomorphized.
//! - `checkpoint_flush` — the dormant US-005 incremental-checkpoint producers,
//!   gated behind `#[cfg(any(test, feature = "us005-incremental-checkpoint"))]`.
//!
//! ## API
//!
//! | Method | Description |
//! |--------|-------------|
//! | `fetch_page(page_no, size)` | Pin a page (cache miss → disk read) |
//! | `alloc_page(size)` | Allocate a new page (free list or extend file) |
//! | `free_page(page_no, size)` | Return a page to the free list |
//! | `flush()` | Write all dirty pages + persist the file header |
//! | `allocator()` | Access the [`AllocatorHandle`] directly |
//! | `pool()` | Access the underlying [`BufferPool`] |
//!
//! ## Thread safety
//!
//! Both `BufferPool` and `AllocatorHandle` are `Send + Sync`; `BufferPoolHandle`
//! inherits this and can be wrapped in `Arc` for sharing across threads.
//! Write serialization lives a layer up in `PagedEngine`, not here: a metadata
//! `RwLock` separates catalog DDL (write guard) from CRUD (read guard), and
//! concurrent CRUD writers to the same key are ordered by per-page exclusive
//! latches plus expected-head checks on the resident version chain, while the
//! publish sequencer assigns the commit order readers observe. The handle
//! itself adds no locking beyond the buffer pool's own partition mutexes, so
//! its methods are safe to call from any of those layers concurrently.

use std::fs::File;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::journal::{JournalManager, LogManager};
use crate::mvcc::registry::ReadViewRegistry;
use crate::storage::allocator::AllocatorHandle;
use crate::storage::buffer_pool::{BufferPool, LatchMode, PageSize, PageSource, PinnedPage};
use crate::storage::header::FileHeader;

mod checkpoint_flush;
mod journal_io;

// ---------------------------------------------------------------------------
// BufferPoolPageSource — PageSource adapter for BufferPool
// ---------------------------------------------------------------------------

/// `PageSource` implementation that routes all reads and writes through the
/// [`BufferPool`].
///
/// Using this adapter as the `io` argument for [`AllocatorHandle`] ensures
/// that free-list link pages (the "next pointer" stored in freed pages) are
/// also managed through the buffer pool rather than bypassing it.
///
/// This solves the **single-point-of-I/O** requirement: every byte that
/// travels between memory and the database file passes through the pool's
/// pin/unpin mechanism.
pub(crate) struct BufferPoolPageSource {
    pool: Arc<BufferPool>,
}

impl BufferPoolPageSource {
    /// Wrap `pool` in a `PageSource` adapter.
    pub(crate) fn new(pool: Arc<BufferPool>) -> Self {
        Self { pool }
    }
}

impl PageSource for BufferPoolPageSource {
    /// Pin the page, copy its content into `buf`, then unpin.
    fn read_page(&self, page_number: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
        let page = self.pool.pin(page_number, size)?;
        buf.copy_from_slice(page.data());
        Ok(())
    }

    /// Pin the page, overwrite its content from `buf`, mark it dirty, then
    /// unpin (dirty bit persists in the pool until flush).
    fn write_page(&self, page_number: u32, size: PageSize, buf: &[u8]) -> Result<()> {
        let mut page = self.pool.pin(page_number, size)?;
        page.data_mut().copy_from_slice(buf);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BufferPoolHandle
// ---------------------------------------------------------------------------

/// High-level handle combining a [`BufferPool`] with an [`AllocatorHandle`].
///
/// `BufferPoolHandle` is the single point of contact for storage-engine
/// layers that need to read, write, allocate, or free pages.  It keeps all
/// I/O routed through the buffer pool so there is always exactly one
/// in-memory copy of each page.
///
/// See the module-level documentation for the full API overview.
pub(crate) struct BufferPoolHandle {
    pool: Arc<BufferPool>,
    /// Dedicated MVCC history-store buffer pool.
    ///
    /// Holds the history-store B-tree's cached pages. Lock-order position **1**
    /// (outermost) — partition mutexes on this pool are acquired BEFORE any
    /// main-pool partition mutex (positions 3/4), so reconciliation evicting
    /// a main-data leaf can install an aged `VersionEntry` here without
    /// re-entering the main-data partition. The non-recursion invariant is
    /// also checked at runtime via the thread-local sentinel in
    /// `crate::storage::history_store`.
    history_pool: Arc<BufferPool>,
    allocator: AllocatorHandle,
    /// `PageSource` adapter that routes allocator I/O through the pool.
    pool_io: BufferPoolPageSource,
    /// Registry of live reader `ReadView`s; [`ReadViewRegistry::oldest_required_ts`]
    /// feeds every chain-reconciliation path. Eager construction lets
    /// [`fetch_page`](Self::fetch_page) reconcile evictions on the
    /// buffer-pool miss path.
    read_view_registry: Arc<ReadViewRegistry>,
    /// Optional journal manager; `None` for in-memory / test handles.
    journal: Option<Arc<Mutex<JournalManager>>>,
    /// Cached `Arc<LogManager>` extracted at journal-attach time. Hot paths
    /// (CRUD `reserve_log_record`, FullSync) read this directly to skip the
    /// outer `journal.lock()` call that previously serialised every reserve.
    log_manager: Option<Arc<LogManager>>,
    /// Dedicated fd for writing checkpointed journal pages back to the main file.
    /// Shared with `ClientInner`; held here so `with_txn` can trigger an
    /// emergency checkpoint when the journal index reaches its hot-threshold.
    journal_main_file: Option<Arc<Mutex<File>>>,
}

impl BufferPoolHandle {
    /// Create a `BufferPoolHandle` with an attached [`JournalManager`].
    ///
    /// Journal rollback and checkpoint helpers become active when a journal is
    /// present.
    pub(crate) fn with_journal(
        pool: Arc<BufferPool>,
        history_pool: Arc<BufferPool>,
        header: FileHeader,
        journal: Arc<Mutex<JournalManager>>,
        journal_main_file: Arc<Mutex<File>>,
    ) -> Self {
        let allocator = AllocatorHandle::new(header);
        let pool_io = BufferPoolPageSource::new(Arc::clone(&pool));
        let log_manager = {
            #[allow(clippy::expect_used)]
            let guard = journal
                .lock()
                .expect("freshly attached journal mutex cannot be poisoned");
            guard.log_manager()
        };
        Self {
            pool,
            history_pool,
            allocator,
            pool_io,
            read_view_registry: ReadViewRegistry::new(),
            journal: Some(journal),
            log_manager: Some(log_manager),
            journal_main_file: Some(journal_main_file),
        }
    }

    // -----------------------------------------------------------------------
    // Page I/O
    // -----------------------------------------------------------------------

    /// Pin `page_number` in the buffer pool and return a [`PinnedPage`] guard.
    ///
    /// On a cache miss the page is loaded from the backing file via the
    /// `FilePageSource` backend.  Pages beyond the current end of file are loaded
    /// as zero-filled frames.
    ///
    /// The returned guard **automatically unpins the page on drop**.  Call
    /// [`PinnedPage::data_mut`] or [`PinnedPage::mark_dirty`] to modify the
    /// page and mark it for write-back on the next [`flush`](Self::flush).
    pub(crate) fn fetch_page<'a>(
        &'a self,
        page_number: u32,
        size: PageSize,
    ) -> Result<PinnedPage<'a>> {
        // Non-recursion invariant: reconciliation while evicting a main-data
        // leaf installs aged entries into `history_pool`. If the history store
        // could somehow re-enter fetch_page on the main pool we would risk
        // partition-lock recursion. The depth sentinel catches that in debug
        // builds.
        debug_assert!(
            crate::storage::history_store::history_store_depth() == 0,
            "BufferPoolHandle::fetch_page entered from within HistoryStore body \
             — non-recursion invariant violated"
        );
        self.refresh_main_file_flush_lsn()?;
        self.pool
            .pin_with_reconcile(page_number, size, &self.read_view_registry, &self.allocator)
    }

    // -----------------------------------------------------------------------
    // Allocation
    // -----------------------------------------------------------------------

    /// Allocate a new page of `size` and return its page number.
    ///
    /// 1. Pops from the appropriate free list (or extends the virtual file
    ///    by incrementing `total_page_count`) via [`AllocatorHandle`].
    /// 2. Pins the new page in the buffer pool.
    /// 3. Zeroes and marks the frame dirty so the page is written to disk on
    ///    the next flush even if the caller writes no further content.
    ///
    /// Callers should immediately pin the returned page via `fetch_page` to
    /// write their content before the next eviction sweep.
    pub(crate) fn alloc_page(&self, size: PageSize) -> Result<u32> {
        self.refresh_main_file_flush_lsn()?;
        let page_no = match size {
            PageSize::Small4k => self.allocator.alloc_4k(&self.pool_io)?,
            PageSize::Large32k => self.allocator.alloc_32k(&self.pool_io)?,
        };

        // A page recycled from the free list may still have a STALE frame in
        // the HISTORY pool (it was last owned by the history store). Drop it
        // so the new MAIN-pool occupant cannot be aliased across both pools.
        self.history_pool.invalidate_page(page_no, size)?;

        // Load the new page into the pool as a zeroed, dirty frame.
        //
        // This guarantees:
        // 1. The frame is in the pool when the caller immediately does
        //    `fetch_page(page_no)` (cache hit, no re-read from disk).
        // 2. Recycled pages (from the free list) have their old content
        //    cleared rather than silently surfacing stale data.
        // 3. The page is marked dirty so the zeroed content reaches the
        //    file on the next flush even if the caller never writes to it
        //    (a page that never appears in the file could confuse recovery).
        {
            let mut page = self.pool.pin(page_no, size)?;
            page.data_mut().fill(0);
        } // unpin — dirty bit persists in the pool

        // Bug B (T3.5): a page reborn from the free list inherits the
        // deltas map from its previous occupant. Those entries are
        // stale (they reference cells that no longer exist on this page)
        // and must not leak into the new occupant's MVCC bookkeeping —
        // they would trip the `chains_empty` guard at the next leaf
        // merge / split. Clear them now while the frame is fresh.
        // Only the 32 KiB partition can carry chains; 4 KiB internal
        // pages never get them, so the call is a no-op there.
        if size == PageSize::Large32k {
            self.pool
                .with_all_chains_under_latch(page_no, LatchMode::Exclusive, |chains| {
                    chains.clear()
                })?;
        }

        Ok(page_no)
    }

    /// Allocate a new page and pin it zeroed on the dedicated history-store
    /// pool. File-level allocation still goes through the single per-file
    /// `AllocatorHandle`, so history pages and main-data pages share one
    /// disjoint page-number namespace.
    pub(crate) fn alloc_page_history(&self, size: PageSize) -> Result<u32> {
        let page_no = match size {
            PageSize::Small4k => self.allocator.alloc_4k(&self.pool_io)?,
            PageSize::Large32k => self.allocator.alloc_32k(&self.pool_io)?,
        };
        // Symmetric to `alloc_page`: a page recycled from the free list may
        // still have a STALE frame in the MAIN pool (the allocator's free-list
        // link reads/writes route through `pool_io`). Drop it so the new
        // HISTORY-pool occupant cannot be aliased across both pools.
        self.pool.invalidate_page(page_no, size)?;
        {
            let mut page = self.history_pool.pin(page_no, size)?;
            page.data_mut().fill(0);
        }
        Ok(page_no)
    }

    // -----------------------------------------------------------------------
    // Deallocation
    // -----------------------------------------------------------------------

    /// Return `page_number` to the appropriate free list.
    ///
    /// The freed page's first 4 bytes are overwritten with the current
    /// free-list head (via the buffer pool) and the frame is marked dirty so
    /// the free-list link is persisted on the next flush.
    pub(crate) fn free_page(&self, page_number: u32, size: PageSize) -> Result<()> {
        self.refresh_main_file_flush_lsn()?;
        // The free-list link write ALWAYS routes through the main pool's
        // `pool_io` (the free list is a single shared structure). A page that
        // was last resident in the HISTORY pool (e.g. a history leaf freed by
        // a gc_pass merge) would otherwise be aliased into BOTH pools: the
        // fresh link bytes in the main frame plus a STALE history frame still
        // holding the old page content. On flush (main-then-history) the stale
        // history frame would overwrite the just-written free-list link on disk
        // — free-list / history-tree corruption on reopen. Drop the
        // non-routing pool's frame so exactly one resident copy (the main
        // frame carrying the link) reaches disk. The page now belongs to the
        // free list and has no owner, so discarding the history frame's
        // content is correct.
        self.history_pool.invalidate_page(page_number, size)?;
        match size {
            PageSize::Small4k => self.allocator.free_4k(page_number, &self.pool_io),
            PageSize::Large32k => self.allocator.free_32k(page_number, &self.pool_io),
        }
    }

    // -----------------------------------------------------------------------
    // Flush
    // -----------------------------------------------------------------------

    /// Write all dirty pages to disk and persist the file header if modified.
    ///
    /// ## WAL-before-data ordering contract
    ///
    /// The invariant this method enforces is the write-ahead rule: a data page
    /// whose newest change belongs to a commit ending at LSN X may only reach
    /// the main file once the journal is durable through X. The fence is
    /// physical: every dirty frame carries the end LSN of its newest commit,
    /// and `flush_lsn_fenced(durable_lsn)` writes back only frames whose stamp
    /// is `<= durable_lsn` — so this method first reads the journal's durable
    /// frontier and passes it as the fence. If a data page could be written
    /// ahead of its log record, a crash between the two writes would leave the
    /// main file holding an effect with no recoverable redo record: recovery
    /// replays only what the journal made durable, so it could never re-derive
    /// or undo that orphaned page, and the database would silently diverge from
    /// its last consistent state. Fencing on the durable LSN makes that
    /// ordering impossible to violate even when the OS reorders writes.
    ///
    /// Call order:
    /// 1. Flush all dirty main-data pages from the pool → `FilePageSource`.
    /// 2. Flush all dirty history-store pages from the history pool.
    /// 3. Write the updated file header (page 0) through the pool if it is
    ///    dirty (this re-marks page 0 as dirty in the pool).
    /// 4. Stamp `Unflushable` dirty frames in both pools at the durable LSN
    ///    so pages dirtied without a commit stamp (header page, history-spill
    ///    frames) become flushable.
    /// 5. Flush the history pool again so the freshly stamped history pages
    ///    reach the backing store BEFORE the header that references them.
    /// 6. Flush the main pool again to write the freshly dirtied header page.
    pub(crate) fn flush(&self) -> Result<()> {
        #[cfg(any(test, feature = "test-hooks"))]
        crate::journal::append_sync_observations::record_handle_flush();
        let Some(durable_lsn) = self.journal_durable_lsn()? else {
            return self.flush_journal_less_test_handle();
        };
        self.pool.set_main_file_flush_lsn(durable_lsn);
        self.history_pool.set_main_file_flush_lsn(durable_lsn);
        // Pass 1 — flush dirty data pages.
        self.pool.flush_lsn_fenced(durable_lsn)?;
        self.history_pool.flush_lsn_fenced(durable_lsn)?;
        // Persist the updated header (page 0) if any allocs / frees changed it.
        self.allocator.flush_header(&self.pool_io)?;
        self.stamp_unflushable_dirty_pages_lsn(durable_lsn)?;
        // Pass 2 — history pool first: frames the stamp just made flushable
        // (e.g. history-spill pages marked Unflushable on dirty unpin) reach
        // the backing store before the header write below is issued, so the
        // header never points at never-issued history pages
        // (history-before-leaf order). This is write-issue ordering through
        // the OS page cache only — no fsync separates the two passes; the
        // checkpoint boundary-record fsync is the durable barrier.
        self.history_pool.flush_lsn_fenced(durable_lsn)?;
        // Then the main pool — writes the header page that flush_header may
        // have dirtied, plus any main frames the stamp made flushable.
        self.pool.flush_lsn_fenced(durable_lsn)
    }

    #[cfg(any(test, feature = "test-hooks"))]
    fn flush_journal_less_test_handle(&self) -> Result<()> {
        debug_assert!(
            self.journal_main_file.is_none(),
            "journal-less test handles must not carry production main-file checkpoint I/O"
        );
        self.pool.flush()?;
        self.history_pool.flush()?;
        self.allocator.flush_header(&self.pool_io)?;
        self.pool.flush()
    }

    #[cfg(not(any(test, feature = "test-hooks")))]
    fn flush_journal_less_test_handle(&self) -> Result<()> {
        Err(Error::Internal(
            "journal-less main-file flush is not a production durability boundary".into(),
        ))
    }

    /// Return whether any resident frame still has unflushed dirty bytes.
    pub(crate) fn has_dirty_pages(&self) -> Result<bool> {
        Ok(!self.pool.dirty_page_ids()?.is_empty()
            || !self.history_pool.dirty_page_ids()?.is_empty())
    }

    /// Fsync the main database file after checkpoint has written a stable
    /// materialized frontier.
    pub(crate) fn sync_main_file(&self) -> Result<()> {
        let Some(file_mutex) = self.journal_main_file.as_ref() else {
            return Ok(());
        };
        let file_guard = file_mutex
            .lock()
            .map_err(|_| Error::Internal("journal main-file mutex poisoned".into()))?;
        file_guard.sync_data().map_err(Error::Io)?;
        #[cfg(any(test, feature = "test-hooks"))]
        crate::journal::append_sync_observations::record_main_file_sync();
        Ok(())
    }

    /// Advance the page-lifetime checkpoint fence and drain newly eligible pages.
    ///
    /// This is the ONLY path that releases a dropped tree's `RetiredTree*`
    /// entries (hot drains skip them wholesale): `pool_io` routes the
    /// free-list link writes through the buffer pool, so a retired page's
    /// still-resident frame is overwritten (and marked dirty) with the link
    /// bytes — the subsequent pool-coherent link reads in `allocate` stay
    /// consistent. Releasing retired pages through the pool's raw backing
    /// `PageSource` would leave stale tree bytes resident and corrupt the
    /// free list on the next pop (F8).
    pub(crate) fn advance_page_lifetime_checkpoint(&self) -> Result<usize> {
        self.allocator.advance_page_lifetime_checkpoint_fence();
        self.allocator.drain_free_queue_with_retired(&self.pool_io)
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Borrow the underlying [`AllocatorHandle`].
    ///
    /// Use this to read or update the file header directly (e.g., to store a
    /// new B+ tree root page number after a root split).
    pub(crate) fn allocator(&self) -> &AllocatorHandle {
        &self.allocator
    }

    /// Borrow the underlying [`BufferPool`].
    #[allow(dead_code)]
    pub(crate) fn pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }

    /// Borrow the dedicated MVCC history-store [`BufferPool`].
    ///
    /// A separate pool guarantees that `history_store` I/O never invalidates
    /// main-data frames and — combined with the outermost lock-order position —
    /// keeps reconciliation's installation of aged entries on a path that
    /// never re-enters the main pool's partition mutexes.
    pub(crate) fn history_pool(&self) -> &Arc<BufferPool> {
        &self.history_pool
    }

    /// Borrow the shared [`ReadViewRegistry`].
    pub(crate) fn read_view_registry(&self) -> &Arc<ReadViewRegistry> {
        &self.read_view_registry
    }

    /// Update the durable LSN fence used by buffer-pool eviction and flush.
    pub(crate) fn set_main_file_flush_lsn(&self, durable_lsn: u64) {
        self.pool.set_main_file_flush_lsn(durable_lsn);
        self.history_pool.set_main_file_flush_lsn(durable_lsn);
    }

    fn refresh_main_file_flush_lsn(&self) -> Result<()> {
        let durable_lsn = self.journal_durable_lsn()?.unwrap_or(u64::MAX);
        self.set_main_file_flush_lsn(durable_lsn);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/handle_accessors.rs"]
mod handle_accessors;

#[cfg(test)]
#[path = "../tests/handle.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/bugsuspect_storage_cross_pool_aliasing.rs"]
mod bugsuspect_storage_cross_pool_aliasing;
