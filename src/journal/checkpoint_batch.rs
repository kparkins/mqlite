//! Checkpoint-batch bookkeeping types: the batch identity, the clean-start
//! cursor, the dirty-page flush set, and the durable boundary token.
//!
//! These are the value types that move between [`JournalManager`] and the
//! allocator/checkpoint driver to fence one checkpoint batch. The byte-level
//! append machinery lives in [`super`]; this module only holds the typed
//! handshake objects that prove a batch was opened, populated, and committed
//! in the right order.

// All imports below feed only the QUARANTINED dormant US-005 producer types in
// this module — see docs/staged-work/us-005-incremental-checkpoint.md. The one
// always-live type (`CheckpointBatchId`) wraps a bare `u64` and needs none of
// them, so the imports are cfg-gated to match the types that consume them.
#[cfg(any(test, feature = "us005-incremental-checkpoint"))]
use std::collections::BTreeSet;

#[cfg(any(test, feature = "us005-incremental-checkpoint"))]
use crate::error::{Error, Result};
#[cfg(any(
    test,
    feature = "test-hooks",
    feature = "us005-incremental-checkpoint"
))]
use crate::mvcc::timestamp::Ts;

#[cfg(any(
    test,
    feature = "test-hooks",
    feature = "us005-incremental-checkpoint"
))]
use super::wire::JournalOffset;
#[cfg(any(test, feature = "us005-incremental-checkpoint"))]
use super::wire::PageId;

/// Durable checkpoint-boundary append token.
///
/// The token is produced only by
/// [`JournalManager::append_checkpoint_commit_boundary`] and consumed by the
/// allocator staged-header commit path.
// QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
#[cfg(any(
    test,
    feature = "test-hooks",
    feature = "us005-incremental-checkpoint"
))]
#[must_use = "BoundaryAppended must be consumed by commit_staged_header_after_boundary"]
#[derive(Debug)]
pub(crate) struct BoundaryAppended {
    pub(super) journal_offset: JournalOffset,
    pub(super) db_page_count: u32,
    pub(super) checkpoint_ts: Ts,
    pub(super) _private: (),
}

#[cfg(any(
    test,
    feature = "test-hooks",
    feature = "us005-incremental-checkpoint"
))]
impl BoundaryAppended {
    /// Database page count covered by the durable boundary.
    pub(crate) fn db_page_count(&self) -> u32 {
        self.db_page_count
    }

    /// Journal byte offset where the boundary starts.
    pub(crate) fn journal_offset(&self) -> JournalOffset {
        self.journal_offset
    }
}

/// Monotonic identity for a checkpoint-owned journal batch.
///
/// NOTE: the struct itself stays compiled in every config because the LIVE
/// `JournalManager::checkpoint_batch_active` field (`super::JournalManager`)
/// holds a `CheckpointBatchId` and is initialized by the production
/// `open_or_create` / recovery constructors. Only its `as_u64()` accessor —
/// called solely from the quarantined producer chain — is cfg-gated.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct CheckpointBatchId(pub(super) u64);

impl CheckpointBatchId {
    /// Wire-format identifier carried by Phase 8 `CheckpointPageFrame` and
    /// `CheckpointBoundary` records.
    // QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
    // Widened to include `test-hooks`: the `test-hooks`-gated producers
    // `append_checkpoint_page_frame` / `append_checkpoint_commit_boundary` call
    // it, so the accessor must exist wherever they compile.
    #[cfg(any(
        test,
        feature = "test-hooks",
        feature = "us005-incremental-checkpoint"
    ))]
    pub(crate) fn as_u64(self) -> u64 {
        self.0
    }
}

/// Non-clone cursor proving the clean start of one checkpoint batch.
// QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
#[cfg(any(
    test,
    feature = "test-hooks",
    feature = "us005-incremental-checkpoint"
))]
#[derive(Debug)]
pub(crate) struct CheckpointBatchCursor {
    pub(super) expected_pending_start: JournalOffset,
    pub(super) clean_start_offset: JournalOffset,
    pub(super) batch_id: CheckpointBatchId,
    pub(super) _private: (),
}

#[cfg(any(
    test,
    feature = "test-hooks",
    feature = "us005-incremental-checkpoint"
))]
impl CheckpointBatchCursor {
    /// Batch id assigned by [`JournalManager::begin_checkpoint_batch`].
    pub(crate) fn batch_id(&self) -> CheckpointBatchId {
        self.batch_id
    }

    /// Offset where checkpoint-owned pending frames must begin.
    pub(crate) fn expected_pending_start(&self) -> JournalOffset {
        self.expected_pending_start
    }
}

/// Checkpoint-owned dirty pages selected for step-8 journal flushing.
// QUARANTINED dormant US-005 producer — see docs/staged-work/us-005-incremental-checkpoint.md
#[cfg(any(test, feature = "us005-incremental-checkpoint"))]
#[derive(Debug)]
pub(crate) struct CheckpointFlushSet {
    batch_id: CheckpointBatchId,
    main_pages: BTreeSet<PageId>,
    history_pages: BTreeSet<PageId>,
    excluded_future_dirty_pages: BTreeSet<PageId>,
    _private: (),
}

#[cfg(any(test, feature = "us005-incremental-checkpoint"))]
impl CheckpointFlushSet {
    /// Build a flush set after validating page ownership is unambiguous.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if a page is owned by both pools.
    pub(crate) fn new(
        batch_id: CheckpointBatchId,
        main_pages: BTreeSet<PageId>,
        history_pages: BTreeSet<PageId>,
        excluded_future_dirty_pages: BTreeSet<PageId>,
    ) -> Result<Self> {
        if let Some(page) = main_pages.intersection(&history_pages).next() {
            return Err(Error::Internal(format!(
                "checkpoint flush set page {} is owned by both pools",
                page.0
            )));
        }
        Ok(Self {
            batch_id,
            main_pages,
            history_pages,
            excluded_future_dirty_pages,
            _private: (),
        })
    }

    /// Batch id that all flushed frames must carry.
    pub(crate) fn batch_id(&self) -> CheckpointBatchId {
        self.batch_id
    }

    /// Main-pool pages covered by this checkpoint batch.
    pub(crate) fn main_pages(&self) -> &BTreeSet<PageId> {
        &self.main_pages
    }

    /// History-pool pages covered by this checkpoint batch.
    pub(crate) fn history_pages(&self) -> &BTreeSet<PageId> {
        &self.history_pages
    }

    /// Dirty pages intentionally left out because they are above the frontier.
    pub(crate) fn excluded_future_dirty_pages(&self) -> &BTreeSet<PageId> {
        &self.excluded_future_dirty_pages
    }
}
