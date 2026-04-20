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
//! Concurrent access is managed at the `PagedEngine` level via per-namespace
//! write lanes (`ns_lanes`) and a metadata `RwLock`.

use std::fs::File;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::mvcc::read_view::ReadViewRegistry;
use crate::storage::allocator::AllocatorHandle;
use crate::storage::buffer_pool::{BufferPool, PageSource, PageSize, PinnedPage};
use crate::storage::header::FileHeader;
use crate::journal::JournalManager;
use crate::journal::log_file::JournalPageSize;

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
    /// Dedicated fd for writing checkpointed journal pages back to the main file.
    /// Shared with `ClientInner`; held here so `with_txn` can trigger an
    /// emergency checkpoint when the journal index reaches its hot-threshold.
    journal_main_file: Option<Arc<Mutex<File>>>,
}

impl BufferPoolHandle {
    /// Create a `BufferPoolHandle` without a journal — test-only.
    ///
    /// Production code always wires a journal via [`Self::with_journal`].
    #[cfg(test)]
    pub(crate) fn new(
        pool: Arc<BufferPool>,
        history_pool: Arc<BufferPool>,
        header: FileHeader,
    ) -> Self {
        let allocator = AllocatorHandle::new(header);
        let pool_io = BufferPoolPageSource::new(Arc::clone(&pool));
        Self {
            pool,
            history_pool,
            allocator,
            pool_io,
            read_view_registry: ReadViewRegistry::new(),
            journal: None,
            journal_main_file: None,
        }
    }

    /// Create a `BufferPoolHandle` with an attached [`JournalManager`].
    ///
    /// All txn methods (`begin_txn`, `commit_txn`, `rollback_txn`,
    /// `checkpoint_through_journal`) become active when a journal is present.
    pub(crate) fn with_journal(
        pool: Arc<BufferPool>,
        history_pool: Arc<BufferPool>,
        header: FileHeader,
        journal: Arc<Mutex<JournalManager>>,
        journal_main_file: Arc<Mutex<File>>,
    ) -> Self {
        let allocator = AllocatorHandle::new(header);
        let pool_io = BufferPoolPageSource::new(Arc::clone(&pool));
        Self {
            pool,
            history_pool,
            allocator,
            pool_io,
            read_view_registry: ReadViewRegistry::new(),
            journal: Some(journal),
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
        let page_no = match size {
            PageSize::Small4k => self.allocator.alloc_4k(&self.pool_io)?,
            PageSize::Large32k => self.allocator.alloc_32k(&self.pool_io)?,
        };

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
        // version_chains map from its previous occupant. Those entries are
        // stale (they reference cells that no longer exist on this page)
        // and must not leak into the new occupant's MVCC bookkeeping —
        // they would trip the `chains_empty` guard at the next leaf
        // merge / split. Clear them now while the frame is fresh.
        self.pool.clear_chains_on_page(page_no, size)?;

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
    /// Call order:
    /// 1. Flush all dirty data pages from the pool → `FilePageSource`.
    /// 2. Write the updated file header (page 0) through the pool if it is
    ///    dirty (this re-marks page 0 as dirty in the pool).
    /// 3. Flush the pool again to write the freshly dirtied header page.
    pub(crate) fn flush(&self) -> Result<()> {
        // Pass 1 — flush dirty data pages.
        self.pool.flush()?;
        // Persist the updated header (page 0) if any allocs / frees changed it.
        self.allocator.flush_header(&self.pool_io)?;
        // Pass 2 — flush the header page that flush_header may have dirtied.
        self.pool.flush()
    }

    // -----------------------------------------------------------------------
    // Journal transaction primitives
    // -----------------------------------------------------------------------

    /// Snapshot the journal write cursor as the begin-of-transaction mark.
    ///
    /// Returns `Some(cursor)` when a journal is attached, `None` for journal-less
    /// handles (in-memory / test).  The returned value must be passed to
    /// [`rollback_txn`](Self::rollback_txn) on failure.
    pub(crate) fn begin_txn(&self) -> Result<Option<u64>> {
        let Some(journal) = &self.journal else {
            return Ok(None);
        };
        let guard = journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        Ok(Some(guard.write_cursor()))
    }

    /// Write the commit frame to the journal.
    ///
    /// `page_number` / `page_size` / `page_data` identify the committing page
    /// (typically the catalog root, which every write transaction touches).
    /// `db_page_count` is the total database page count after this txn.
    ///
    /// Returns `true` if the journal index has reached its hot-threshold
    /// (emergency checkpoint signal); `false` otherwise or when no journal
    /// is attached.
    pub(crate) fn commit_txn(
        &self,
        page_number: u32,
        page_size: PageSize,
        page_data: &[u8],
        db_page_count: u32,
    ) -> Result<bool> {
        match &self.journal {
            None => {
                crate::mvcc::metrics::record_journal_commit();
                Ok(false)
            }
            Some(journal) => {
                let journal_page_size = match page_size {
                    PageSize::Small4k => JournalPageSize::Small4k,
                    PageSize::Large32k => JournalPageSize::Large32k,
                };
                let mut guard = journal
                    .lock()
                    .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
                let result =
                    guard.commit(page_number, journal_page_size, page_data, db_page_count)?;
                crate::mvcc::metrics::record_journal_commit();
                Ok(result)
            }
        }
    }

    /// Roll back a transaction by truncating the journal and discarding dirty
    /// buffer pool frames.
    ///
    /// `mark` is the cursor value returned by the paired [`begin_txn`](Self::begin_txn)
    /// call.  When `mark` is `None` (no journal attached), only dirty frames are
    /// dropped from the pool.
    pub(crate) fn rollback_txn(&self, mark: Option<u64>) -> Result<()> {
        if let (Some(mark), Some(journal)) = (mark, &self.journal) {
            let mut guard = journal
                .lock()
                .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
            guard.truncate_to(mark)?;
        }
        self.pool.drop_all_dirty()
    }

    /// Checkpoint using the `journal_main_file` handle stored on this handle.
    ///
    /// Returns `Ok(false)` when no journal is attached. Used by [`with_txn`] after
    /// a commit frame signals the journal index has reached its hot-threshold,
    /// and by [`with_txn`]'s callers that do not hold the main-file fd directly.
    pub(crate) fn emergency_checkpoint(&self) -> Result<bool> {
        let Some(file_mutex) = self.journal_main_file.as_ref() else {
            return Ok(false);
        };
        let mut guard = file_mutex
            .lock()
            .map_err(|_| Error::Internal("journal main-file mutex poisoned".into()))?;
        self.checkpoint_through_journal(&mut guard)?;
        Ok(true)
    }

    /// Checkpoint all journal frames into `main_file` and reset the journal.
    ///
    /// Reads the current [`FileHeader`] from the allocator, passes it to
    /// [`JournalManager::checkpoint`] (which may update `total_page_count`), then
    /// writes the updated count back into the allocator header.
    ///
    /// No-op when no journal is attached.
    pub(crate) fn checkpoint_through_journal(&self, main_file: &mut File) -> Result<()> {
        match &self.journal {
            None => Ok(()),
            Some(journal) => {
                let mut header = self.allocator.with_header(|h| h.clone())?;
                let mut guard = journal
                    .lock()
                    .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
                guard.checkpoint(main_file, &mut header)?;
                drop(guard);
                // Propagate total_page_count update back into the allocator.
                let new_count = header.total_page_count;
                self.allocator.update_header(|h| {
                    h.total_page_count = new_count;
                })
            }
        }
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
    #[allow(dead_code)]
    pub(crate) fn history_pool(&self) -> &Arc<BufferPool> {
        &self.history_pool
    }

    /// Borrow the shared [`ReadViewRegistry`].
    pub(crate) fn read_view_registry(&self) -> &Arc<ReadViewRegistry> {
        &self.read_view_registry
    }

    /// Borrow the `BufferPoolPageSource` routing allocator I/O through the pool.
    ///
    /// Used by writer-path code that needs a `PageSource` for
    /// [`AllocatorHandle::drain_free_queue`] without re-constructing one.
    #[cfg(test)]
    pub(crate) fn page_source(&self) -> &BufferPoolPageSource {
        &self.pool_io
    }

    /// Highest `ChainCommit::commit_ts` that the journal observed during
    /// recovery, or `None` when no journal is attached or it carried no
    /// ChainCommit frames. The MVCC backend folds this into
    /// `TimestampOracle::set_min` at construction so post-recovery commits
    /// are strictly above every durable pre-crash commit (plan T7).
    pub(crate) fn recovered_max_commit_ts(&self) -> Result<Option<crate::mvcc::timestamp::Ts>> {
        let Some(journal) = &self.journal else {
            return Ok(None);
        };
        let guard = journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        Ok(guard.recovered_max_commit_ts())
    }

    /// fsync the journal file — make all committed-but-unsynced frames durable.
    ///
    /// Called by the engine's FullSync hot path. No-op when no journal is
    /// attached (in-memory / test handles).
    pub(crate) fn journal_sync(&self) -> Result<()> {
        let Some(journal) = &self.journal else {
            return Ok(());
        };
        let guard = journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        guard.sync_journal()
    }

    /// Append an MVCC `ChainCommit` frame (Format Lock §A.2).
    ///
    /// `commit_ts` is the transaction's commit timestamp from the
    /// [`TimestampOracle`](crate::mvcc::timestamp::TimestampOracle).
    /// `refcount_deltas` carries overflow-chain refcount adjustments;
    /// `page_writes` carries any durable page-writes this commit installs.
    ///
    /// Returns the byte offset at which the frame was written. No-op
    /// (returns `Ok(0)`) on journal-less handles.
    pub(crate) fn append_chain_commit(
        &self,
        commit_ts: crate::mvcc::timestamp::Ts,
        refcount_deltas: Vec<(u32, i32)>,
        page_writes: Vec<crate::journal::log_file::ChainPageWrite>,
    ) -> Result<u64> {
        match &self.journal {
            None => Ok(0),
            Some(journal) => {
                let mut guard = journal
                    .lock()
                    .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
                let offset = guard.append_chain_commit(commit_ts, refcount_deltas, page_writes)?;
                crate::mvcc::metrics::record_journal_chain_commit_frame();
                Ok(offset)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::buffer_pool::default_sizes;
    use crate::storage::header::FileHeader;
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;

    // -----------------------------------------------------------------------
    // Mock I/O
    // -----------------------------------------------------------------------

    /// Minimal in-memory `PageSource` used to back the `BufferPool` in tests.
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
                let copy_len = buf.len().min(data.len());
                buf[..copy_len].copy_from_slice(&data[..copy_len]);
                if copy_len < buf.len() {
                    buf[copy_len..].fill(0);
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

    fn make_handle() -> (Arc<MockIo>, BufferPoolHandle) {
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
        let handle = BufferPoolHandle::new(pool, history_pool, header);
        (io, handle)
    }

    // -----------------------------------------------------------------------
    // fetch_page
    // -----------------------------------------------------------------------

    #[test]
    fn fetch_page_returns_pinned_page() {
        let (io, handle) = make_handle();

        // Seed page 1 with a known pattern.
        {
            let mut pages = io.pages.lock().unwrap();
            let mut data = vec![0u8; PageSize::Large32k.bytes()];
            data[0] = 0xAB;
            pages.insert(1, data);
        }

        let page = handle.fetch_page(1, PageSize::Large32k).unwrap();
        assert_eq!(page.data()[0], 0xAB);
        assert_eq!(page.page_number(), 1);
    }

    // -----------------------------------------------------------------------
    // alloc_page
    // -----------------------------------------------------------------------

    #[test]
    fn alloc_page_returns_page_1_on_fresh_header() {
        let (_, handle) = make_handle();
        let pn = handle.alloc_page(PageSize::Large32k).unwrap();
        assert_eq!(pn, 1);
    }

    #[test]
    fn alloc_page_increments_total_page_count() {
        let (_, handle) = make_handle();
        handle.alloc_page(PageSize::Large32k).unwrap();
        handle.alloc_page(PageSize::Small4k).unwrap();

        let count = handle
            .allocator()
            .with_header(|h| h.total_page_count)
            .unwrap();
        assert_eq!(count, 3);
    }

    #[test]
    fn alloc_page_zeroes_the_new_frame() {
        let (io, handle) = make_handle();

        // Seed the backing store with a non-zero pattern at page 1.
        {
            let mut pages = io.pages.lock().unwrap();
            pages.insert(1, vec![0xFFu8; PageSize::Large32k.bytes()]);
        }

        let pn = handle.alloc_page(PageSize::Large32k).unwrap();
        assert_eq!(pn, 1);

        // The buffer pool should have the page zeroed (overriding the
        // backing store content) and marked dirty.
        let page = handle.fetch_page(pn, PageSize::Large32k).unwrap();
        assert!(
            page.data().iter().all(|&b| b == 0),
            "newly allocated page must be zeroed"
        );
    }

    // -----------------------------------------------------------------------
    // free_page
    // -----------------------------------------------------------------------

    #[test]
    fn free_and_realloc_recycles_page() {
        let (_, handle) = make_handle();

        // Allocate two pages, then free the first.
        let p1 = handle.alloc_page(PageSize::Large32k).unwrap();
        let _p2 = handle.alloc_page(PageSize::Large32k).unwrap();

        handle.free_page(p1, PageSize::Large32k).unwrap();

        // Next alloc must recycle p1.
        let recycled = handle.alloc_page(PageSize::Large32k).unwrap();
        assert_eq!(recycled, p1, "freed page must be recycled");
    }

    // -----------------------------------------------------------------------
    // flush
    // -----------------------------------------------------------------------

    #[test]
    fn flush_writes_dirty_data_page() {
        let (io, handle) = make_handle();

        let pn = handle.alloc_page(PageSize::Large32k).unwrap();
        {
            let mut page = handle.fetch_page(pn, PageSize::Large32k).unwrap();
            page.data_mut()[0] = 0x77;
        }

        handle.flush().unwrap();

        let pages = io.pages.lock().unwrap();
        let written = pages.get(&pn).expect("page must be written after flush");
        assert_eq!(written[0], 0x77, "flush must write modified page content");
    }

    #[test]
    fn flush_writes_header_page_0_when_dirty() {
        let (io, handle) = make_handle();

        handle.alloc_page(PageSize::Large32k).unwrap();
        handle.flush().unwrap();

        let pages = io.pages.lock().unwrap();
        assert!(
            pages.contains_key(&0),
            "flush must write header page 0 after allocation"
        );
    }

    #[test]
    fn flush_does_not_write_header_when_clean() {
        let (io, handle) = make_handle();

        // No allocations — header is clean.
        handle.flush().unwrap();

        let pages = io.pages.lock().unwrap();
        assert!(
            !pages.contains_key(&0),
            "flush must not write header when no allocations occurred"
        );
    }

    // -----------------------------------------------------------------------
    // BufferPoolPageSource
    // -----------------------------------------------------------------------

    #[test]
    fn pool_io_read_page_routes_through_pool() {
        let io = MockIo::new();
        let pool = Arc::new(BufferPool::new(
            default_sizes::DESKTOP,
            Box::new(ArcIo(Arc::clone(&io))),
        ));

        // Seed the backing store with a known pattern at page 5.
        {
            let mut pages = io.pages.lock().unwrap();
            let mut data = vec![0u8; PageSize::Large32k.bytes()];
            data[0] = 0x55;
            pages.insert(5, data);
        }

        let pool_io = BufferPoolPageSource::new(Arc::clone(&pool));
        let mut buf = vec![0u8; PageSize::Large32k.bytes()];
        pool_io.read_page(5, PageSize::Large32k, &mut buf).unwrap();

        assert_eq!(buf[0], 0x55);
    }

    #[test]
    fn pool_io_write_page_marks_frame_dirty() {
        let (io, handle) = make_handle();

        // Pre-pin page 2 into the pool (so it's in cache).
        let _ = handle.fetch_page(2, PageSize::Small4k).unwrap();

        let pool_io = BufferPoolPageSource::new(Arc::clone(handle.pool()));
        let data = vec![0xAAu8; PageSize::Small4k.bytes()];
        pool_io.write_page(2, PageSize::Small4k, &data).unwrap();

        // Flush should write the modified content to the backing store.
        handle.flush().unwrap();

        let pages = io.pages.lock().unwrap();
        let written = pages.get(&2).expect("page must be written after flush");
        assert_eq!(written[0], 0xAA);
    }
}
