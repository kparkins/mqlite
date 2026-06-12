//! Dormant US-005 incremental-checkpoint producer surface for
//! [`BufferPoolHandle`].
//!
//! Everything here is gated behind `#[cfg(any(test, feature =
//! "us005-incremental-checkpoint"))]` and carries the QUARANTINED markers —
//! these producers are staged ahead of their checkpoint driver and are not
//! reachable on the production commit path. See
//! `docs/staged-work/us-005-incremental-checkpoint.md`.
//!
//! Kept in a dedicated file so the quarantine gating stays self-contained and
//! the hot-path `BufferPoolHandle` core (`handle/mod.rs`) is not interleaved
//! with dormant code.
#![cfg(any(test, feature = "us005-incremental-checkpoint"))]

use std::collections::BTreeSet;

use crate::error::{Error, Result};
use crate::journal::wire::{JournalPageSize, PageId};
use crate::journal::{
    CheckpointBatchCursor, CheckpointBatchId, CheckpointFlushSet, CheckpointPoolKind,
};
use crate::storage::buffer_pool::PageSize;
use crate::storage::handle::BufferPoolHandle;

// QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
#[cfg_attr(
    all(not(test), feature = "us005-incremental-checkpoint"),
    allow(dead_code, reason = "dormant US-005 producer staged ahead of its driver")
)]
fn journal_page_size(size: PageSize) -> JournalPageSize {
    match size {
        PageSize::Small4k => JournalPageSize::Small4k,
        PageSize::Large32k => JournalPageSize::Large32k,
    }
}

// QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
#[cfg_attr(
    all(not(test), feature = "us005-incremental-checkpoint"),
    allow(dead_code, reason = "dormant US-005 producer staged ahead of its driver")
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

impl BufferPoolHandle {
    /// Return the batch id that the next checkpoint flush should carry.
    // QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
    #[cfg_attr(
        all(not(test), feature = "us005-incremental-checkpoint"),
        allow(dead_code, reason = "dormant US-005 producer staged ahead of its driver")
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
    // QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
    #[cfg_attr(
        all(not(test), feature = "us005-incremental-checkpoint"),
        allow(dead_code, reason = "dormant US-005 producer staged ahead of its driver")
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
        let Some(log_manager) = &self.log_manager else {
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

        let cursor = {
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
            cursor
        };

        // Per-page records — reserve through the lock-free LogManager and arm
        // the LSN-pin invariant so CLOCK eviction skips these dirty frames
        // until the matching boundary record is durable
        // (`src/storage/buffer_pool/partition.rs:118-120, 247-313`).
        let guard = journal
            .lock()
            .map_err(|_| Error::Internal("journal mutex poisoned".into()))?;
        for (page, size, data) in main_frames {
            let end_lsn = guard.append_checkpoint_page_frame(
                cursor.batch_id(),
                CheckpointPoolKind::Main,
                page,
                journal_page_size(size),
                &data,
            )?;
            self.pool.stamp_dirty_pages_lsn(&[page], end_lsn)?;
        }
        for (page, size, data) in history_frames {
            let end_lsn = guard.append_checkpoint_page_frame(
                cursor.batch_id(),
                CheckpointPoolKind::History,
                page,
                journal_page_size(size),
                &data,
            )?;
            self.history_pool.stamp_dirty_pages_lsn(&[page], end_lsn)?;
        }
        drop(guard);
        log_manager.ensure_sync(log_manager.next_lsn())?;
        Ok(cursor)
    }

    // QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
    #[cfg_attr(
        all(not(test), feature = "us005-incremental-checkpoint"),
        allow(dead_code, reason = "dormant US-005 producer staged ahead of its driver")
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
}
