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

use std::collections::BTreeSet;
use std::fs::File;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::journal::log_file::{JournalPageSize, LogRecordDraft, PageId};
use crate::journal::{
    CheckpointBatchCursor, CheckpointBatchId, CheckpointFlushSet, CheckpointPoolKind,
    JournalManager, LogManager, ReservedLogRecord,
};
use crate::mvcc::read_view::ReadViewRegistry;
use crate::storage::allocator::AllocatorHandle;
use crate::storage::buffer_pool::{BufferPool, PageSize, PageSource, PinnedPage};
use crate::storage::header::FileHeader;

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

#[allow(
    dead_code,
    reason = "US-005 lands flush_journal_durable before the full checkpoint driver consumes it"
)]
fn journal_page_size(size: PageSize) -> JournalPageSize {
    match size {
        PageSize::Small4k => JournalPageSize::Small4k,
        PageSize::Large32k => JournalPageSize::Large32k,
    }
}

#[allow(
    dead_code,
    reason = "US-005 lands flush_journal_durable before the full checkpoint driver consumes it"
)]
fn validate_dirty_subset(
    dirty_pages: &BTreeSet<PageId>,
    owned_pages: &BTreeSet<PageId>,
    excluded_pages: &BTreeSet<PageId>,
    pool_name: &str,
) -> Result<()> {
    for page in dirty_pages {
        if !owned_pages.contains(page) && !excluded_pages.contains(page) {
            return Err(Error::Internal(format!(
                "checkpoint flush set rejected {pool_name} foreign dirty frame {}",
                page.0
            )));
        }
    }
    Ok(())
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
        self.refresh_main_file_flush_lsn()?;
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
    /// 1. Flush all dirty main-data pages from the pool → `FilePageSource`.
    /// 2. Flush all dirty history-store pages from the history pool.
    /// 3. Write the updated file header (page 0) through the pool if it is
    ///    dirty (this re-marks page 0 as dirty in the pool).
    /// 4. Flush the pool again to write the freshly dirtied header page.
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
        // Pass 2 — flush the header page that flush_header may have dirtied.
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

    /// Copy the post-boundary checkpoint batch into the main file.
    ///
    /// This Phase 7 primitive runs only after the page-0 boundary has become
    /// durable. It fdatasyncs the main file before journal truncation and never
    /// mutates allocator header state.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if no journal/main-file pair is attached, no
    /// durable page-0 checkpoint boundary exists, or the boundary page count
    /// differs from `expected_total_page_count`.
    #[allow(
        dead_code,
        reason = "Phase 7 checkpoint recovery primitive is retained for checkpoint-only callers"
    )]
    pub(crate) fn emergency_checkpoint_after_boundary(
        &self,
        expected_total_page_count: u32,
    ) -> Result<()> {
        let Some(file_mutex) = self.journal_main_file.as_ref() else {
            return Err(Error::Internal(
                "post-boundary checkpoint requires an attached main file".into(),
            ));
        };
        let Some(journal) = &self.journal else {
            return Err(Error::Internal(
                "post-boundary checkpoint requires an attached journal".into(),
            ));
        };
        let mut file_guard = file_mutex
            .lock()
            .map_err(|_| Error::Internal("journal main-file mutex poisoned".into()))?;
        let mut journal_guard = journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        journal_guard
            .emergency_checkpoint_after_boundary(&mut file_guard, expected_total_page_count)
    }

    /// Advance the page-lifetime checkpoint fence and drain newly eligible pages.
    pub(crate) fn advance_page_lifetime_checkpoint(&self) -> Result<usize> {
        self.allocator.advance_page_lifetime_checkpoint_fence();
        self.allocator.drain_free_queue(&self.pool_io)
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

    /// Highest non-control Phase 8 `publish_seq` accepted during recovery.
    pub(crate) fn recovered_max_publish_seq(&self) -> Result<Option<u64>> {
        let Some(journal) = &self.journal else {
            return Ok(None);
        };
        let guard = journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        Ok(guard.recovered_max_publish_seq())
    }

    /// Return the batch id that the next checkpoint flush should carry.
    #[allow(
        dead_code,
        reason = "US-005 lands checkpoint batch ids before the full driver consumes them"
    )]
    pub(crate) fn next_checkpoint_batch_id(&self) -> Result<CheckpointBatchId> {
        let Some(journal) = &self.journal else {
            return Err(Error::Internal(
                "checkpoint flush requires an attached journal".into(),
            ));
        };
        let guard = journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        Ok(guard.next_checkpoint_batch_id())
    }

    /// Flush only checkpoint-owned dirty frames to the journal and sync it.
    ///
    /// The allocator header is intentionally not flushed here; Phase 7 stages
    /// page-0 authority separately at the boundary.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] when the flush set has ambiguous pool
    /// ownership, a foreign dirty frame, or no attached journal.
    #[allow(
        dead_code,
        reason = "US-005 lands durable checkpoint flushing before the full driver consumes it"
    )]
    pub(crate) fn flush_journal_durable(
        &self,
        checkpoint_flush_set: CheckpointFlushSet,
    ) -> Result<CheckpointBatchCursor> {
        let Some(journal) = &self.journal else {
            return Err(Error::Internal(
                "checkpoint flush requires an attached journal".into(),
            ));
        };
        let checkpoint_applied_lsn = self.journal_durable_lsn()?.unwrap_or(u64::MAX);
        self.validate_checkpoint_flush_set(&checkpoint_flush_set)?;
        let main_frames = self.pool.checkpoint_dirty_frame_snapshots(
            checkpoint_flush_set.main_pages(),
            checkpoint_flush_set.excluded_future_dirty_pages(),
            checkpoint_applied_lsn,
        )?;
        let history_frames = self.history_pool.checkpoint_dirty_frame_snapshots(
            checkpoint_flush_set.history_pages(),
            &BTreeSet::new(),
            checkpoint_applied_lsn,
        )?;

        let mut guard = journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        let cursor = guard.begin_checkpoint_batch()?;
        if cursor.batch_id() != checkpoint_flush_set.batch_id() {
            guard.abort_empty_checkpoint_batch(&cursor);
            return Err(Error::Internal(
                "checkpoint flush set batch id does not match journal cursor".into(),
            ));
        }

        for (page, size, data) in main_frames {
            guard.append_checkpoint_frame(
                cursor.batch_id(),
                CheckpointPoolKind::Main,
                page,
                journal_page_size(size),
                &data,
            )?;
        }
        for (page, size, data) in history_frames {
            guard.append_checkpoint_frame(
                cursor.batch_id(),
                CheckpointPoolKind::History,
                page,
                journal_page_size(size),
                &data,
            )?;
        }
        guard.sync_journal()?;
        Ok(cursor)
    }

    #[allow(
        dead_code,
        reason = "US-005 lands durable checkpoint flushing before the full driver consumes it"
    )]
    fn validate_checkpoint_flush_set(&self, flush_set: &CheckpointFlushSet) -> Result<()> {
        if let Some(page) = flush_set
            .main_pages()
            .intersection(flush_set.history_pages())
            .next()
        {
            return Err(Error::Internal(format!(
                "checkpoint flush set page {} is owned by both pools",
                page.0
            )));
        }

        let main_dirty = self.pool.dirty_page_ids()?;
        let history_dirty = self.history_pool.dirty_page_ids()?;
        if let Some(page) = main_dirty.intersection(&history_dirty).next() {
            return Err(Error::Internal(format!(
                "dirty page {} is resident in both checkpoint pools",
                page.0
            )));
        }
        validate_dirty_subset(
            &main_dirty,
            flush_set.main_pages(),
            flush_set.excluded_future_dirty_pages(),
            "main",
        )?;
        validate_dirty_subset(
            &history_dirty,
            flush_set.history_pages(),
            &BTreeSet::new(),
            "history",
        )
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

    fn journal_log_manager(&self) -> Result<Option<Arc<LogManager>>> {
        let Some(journal) = &self.journal else {
            return Ok(None);
        };
        let guard = journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        Ok(Some(guard.log_manager()))
    }

    /// Reserve and finalize a Phase 8 log record for later positioned write.
    pub(crate) fn reserve_log_record(&self, draft: LogRecordDraft) -> Result<ReservedLogRecord> {
        let Some(journal) = &self.journal else {
            return Ok(ReservedLogRecord::journalless(draft.finalize(0)?));
        };
        let mut guard = journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        guard.reserve_log_record(draft)
    }

    fn journal_durable_lsn(&self) -> Result<Option<u64>> {
        let Some(log_manager) = self.journal_log_manager()? else {
            return Ok(None);
        };
        Ok(Some(log_manager.durable_lsn()))
    }

    /// Return the current durable LSN, or `u64::MAX` for journal-less handles.
    pub(crate) fn current_journal_durable_lsn(&self) -> Result<u64> {
        Ok(self.journal_durable_lsn()?.unwrap_or(u64::MAX))
    }

    fn refresh_main_file_flush_lsn(&self) -> Result<()> {
        let durable_lsn = self.journal_durable_lsn()?.unwrap_or(u64::MAX);
        self.set_main_file_flush_lsn(durable_lsn);
        Ok(())
    }

    /// Update the durable LSN fence used by buffer-pool eviction and flush.
    pub(crate) fn set_main_file_flush_lsn(&self, durable_lsn: u64) {
        self.pool.set_main_file_flush_lsn(durable_lsn);
        self.history_pool.set_main_file_flush_lsn(durable_lsn);
    }

    /// Return the current contiguous ready journal frontier.
    #[allow(dead_code)]
    pub(crate) fn journal_ready_lsn(&self) -> Result<u64> {
        let Some(log_manager) = self.journal_log_manager()? else {
            return Ok(0);
        };
        Ok(log_manager.ready_lsn())
    }

    /// Return `(next_lsn, ready_lsn, durable_lsn)` for Phase 8 tests.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn journal_lsn_snapshot(&self) -> Result<(u64, u64, u64)> {
        let Some(log_manager) = self.journal_log_manager()? else {
            return Ok((0, 0, 0));
        };
        Ok((
            log_manager.next_lsn(),
            log_manager.ready_lsn(),
            log_manager.durable_lsn(),
        ))
    }

    /// Wait until the journal write prefix covers `end_lsn`.
    pub(crate) fn wait_journal_ready(&self, end_lsn: u64) -> Result<()> {
        let Some(log_manager) = self.journal_log_manager()? else {
            return Ok(());
        };
        log_manager.wait_ready(end_lsn)
    }

    /// Wait until the journal durable frontier covers `end_lsn`.
    pub(crate) fn wait_journal_durable(&self, end_lsn: u64) -> Result<()> {
        let Some(log_manager) = self.journal_log_manager()? else {
            return Ok(());
        };
        log_manager.wait_durable(end_lsn)
    }

    /// Sync the currently ready journal prefix.
    pub(crate) fn sync_journal_ready_prefix(&self) -> Result<()> {
        let Some(log_manager) = self.journal_log_manager()? else {
            return Ok(());
        };
        log_manager.ensure_sync(log_manager.ready_lsn())
    }

    /// Stamp resident dirty pages with the commit record end LSN.
    pub(crate) fn stamp_dirty_pages_lsn(&self, pages: &[u32], last_lsn: u64) -> Result<()> {
        self.pool.stamp_dirty_pages_lsn(pages, last_lsn)
    }

    /// Stamp resident dirty pages in both main and history pools.
    pub(crate) fn stamp_dirty_pages_lsn_all_pools(
        &self,
        pages: &[u32],
        last_lsn: u64,
    ) -> Result<()> {
        self.pool.stamp_dirty_pages_lsn(pages, last_lsn)?;
        self.history_pool.stamp_dirty_pages_lsn(pages, last_lsn)
    }

    /// Snapshot dirty resident frames in both main and history pools.
    pub(crate) fn dirty_frame_snapshots_for_pages(
        &self,
        pages: &BTreeSet<PageId>,
    ) -> Result<Vec<(u32, PageSize, Vec<u8>)>> {
        let mut frames = self.pool.dirty_frame_snapshots_for_pages(pages)?;
        frames.extend(self.history_pool.dirty_frame_snapshots_for_pages(pages)?);
        frames.sort_by_key(|(page, size, _data)| {
            let size_order = match size {
                PageSize::Small4k => 0u8,
                PageSize::Large32k => 1u8,
            };
            (*page, size_order)
        });
        Ok(frames)
    }

    /// Stamp unflushable resident dirty bytes after an explicit log sync.
    pub(crate) fn stamp_unflushable_dirty_pages_lsn(&self, last_lsn: u64) -> Result<()> {
        self.pool.stamp_unflushable_dirty_lsn(last_lsn)?;
        self.history_pool.stamp_unflushable_dirty_lsn(last_lsn)
    }

    /// Fsync the logical-transaction journal tail after appending a
    /// `LogicalTxnFrame`. This names the S6 durability point while reusing
    /// the same journal sync primitive as the rest of the handle.
    #[allow(
        dead_code,
        reason = "phase0 test probe still uses this hook; production US-012 path no longer does"
    )]
    pub(crate) fn fsync_logical_tail(&self) -> Result<()> {
        self.journal_sync()
    }

    /// Append an MVCC `ChainCommit` frame and return its exclusive end LSN.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn append_chain_commit_end_lsn(
        // allow-phase8-legacy-audit: test-only retired ChainCommit append probe
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
                let end_lsn =
                    guard.append_chain_commit_end_lsn(commit_ts, refcount_deltas, page_writes)?;
                crate::mvcc::metrics::record_journal_chain_commit_frame();
                Ok(end_lsn)
            }
        }
    }

    /// Append a Phase 2 `LogicalTxnFrame` (§3, §4, §6.4) between
    /// `allocate_commit_ts` and the subsequent `ChainCommit`. Encodes before
    /// any file I/O so an oversize frame returns [`Error::JournalFrameTooLarge`]
    /// without touching the journal.
    ///
    /// Returns the byte offset at which the frame was written. No-op
    /// (returns `Ok(0)`) on journal-less handles.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn append_logical_txn(
        // allow-phase8-legacy-audit: test-only retired logical append probe
        &self,
        frame: crate::journal::log_file::LogicalTxnFrame,
    ) -> Result<u64> {
        match &self.journal {
            None => Ok(0),
            Some(journal) => {
                let mut guard = journal
                    .lock()
                    .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
                guard.append_logical_txn(frame)
            }
        }
    }

    /// Expose the journal's database-lifetime salt values for callers that
    /// need to stamp new journal frames outside `JournalManager::append_*`.
    /// Returns `None` on journal-less handles.
    pub(crate) fn journal_salts(&self) -> Option<(u32, u32)> {
        let guard = self.journal.as_ref()?.lock().ok()?;
        Some(guard.salts())
    }

    /// Consume the Pass 1 `ParsedLogicalFrames` populated by journal
    /// recovery. Take-once semantics: after the first call the journal
    /// leaves `Default::default()` behind. Returns an empty struct on
    /// journal-less handles.
    pub(crate) fn take_parsed_logical_frames(&self) -> crate::journal::ParsedLogicalFrames {
        match &self.journal {
            None => crate::journal::ParsedLogicalFrames::default(),
            Some(journal) => match journal.lock() {
                Ok(mut guard) => guard.take_parsed_logical_frames(),
                Err(_) => crate::journal::ParsedLogicalFrames::default(),
            },
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
    use crate::storage::test_support::{ArcIo, MockIo};

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
