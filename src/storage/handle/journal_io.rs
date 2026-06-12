//! Journal facade wrappers for [`BufferPoolHandle`].
//!
//! A second inherent `impl BufferPoolHandle` block holding the thin journal
//! delegators: LSN reservation, durability waits, syncs, salts, and the
//! recovered-frontier readers. No trait object and no wrapper struct — these
//! are inherent methods on the same `BufferPoolHandle`, so the per-pin and
//! per-commit hot paths stay monomorphized and the cached `Arc<LogManager>`
//! fast path is preserved verbatim:
//!
//! - [`reserve_log_record`](BufferPoolHandle::reserve_log_record) reads the
//!   cached `self.log_manager` directly and calls
//!   `JournalManager::reserve_log_record_on` — it never takes `self.journal`'s
//!   outer mutex, exactly as before the split.
//! - [`journal_log_manager`](BufferPoolHandle::journal_log_manager) clones the
//!   cached `Arc`, never the journal lock.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::journal::wire::LogRecordDraft;
use crate::journal::{JournalManager, LogManager, ReservedLogRecord};

use crate::storage::handle::BufferPoolHandle;

impl BufferPoolHandle {
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

    /// Consume the next checkpoint batch id, advancing the journal counter.
    ///
    /// Returns `0` when no journal is attached (test handles).
    pub(crate) fn consume_checkpoint_batch_id(&self) -> Result<u64> {
        let Some(journal) = &self.journal else {
            return Ok(0);
        };
        let mut guard = journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        Ok(guard.consume_checkpoint_batch_id())
    }

    /// fsync the journal file — make all committed-but-unsynced frames durable.
    ///
    /// Called by the engine's FullSync hot path. No-op when no journal is
    /// attached (in-memory / test handles).
    pub(crate) fn journal_sync(&self) -> Result<()> {
        let Some(log_manager) = &self.log_manager else {
            return Ok(());
        };
        log_manager.ensure_sync(log_manager.next_lsn())
    }

    pub(super) fn journal_log_manager(&self) -> Result<Option<Arc<LogManager>>> {
        Ok(self.log_manager.as_ref().map(Arc::clone))
    }

    /// Reserve and finalize a Phase 8 log record for later positioned write.
    ///
    /// Lock-free on the outer journal mutex: the cached `Arc<LogManager>` owns
    /// the LSN-allocation atomic, and `JournalManager::reserve_log_record_on`
    /// drives the reservation without acquiring `self.journal`.
    pub(crate) fn reserve_log_record(&self, draft: LogRecordDraft) -> Result<ReservedLogRecord> {
        let Some(log_manager) = &self.log_manager else {
            return Ok(ReservedLogRecord::journalless(draft.finalize(0)?));
        };
        JournalManager::reserve_log_record_on(log_manager, draft)
    }

    pub(super) fn journal_durable_lsn(&self) -> Result<Option<u64>> {
        let Some(log_manager) = self.journal_log_manager()? else {
            return Ok(None);
        };
        Ok(Some(log_manager.durable_lsn()))
    }

    /// Return the current durable LSN, or `u64::MAX` for journal-less handles.
    pub(crate) fn current_journal_durable_lsn(&self) -> Result<u64> {
        Ok(self.journal_durable_lsn()?.unwrap_or(u64::MAX))
    }

    /// Return `(next_lsn, ready_lsn, durable_lsn)` for journal tests.
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
        pages: &std::collections::BTreeSet<crate::journal::wire::PageId>,
    ) -> Result<Vec<(u32, crate::storage::buffer_pool::PageSize, Vec<u8>)>> {
        use crate::storage::buffer_pool::PageSize;
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
        reason = "legacy crash-cut probe still uses this hook; production commit path no longer does"
    )]
    pub(crate) fn fsync_logical_tail(&self) -> Result<()> {
        self.journal_sync()
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
