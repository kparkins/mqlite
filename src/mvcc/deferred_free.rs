//! Page-lifetime queue for refcount-to-zero overflow chains.
//!
//! `OverflowRef::drop` never frees pages directly. When a decref brings the
//! refcount to 0, the page number is enqueued here with the current
//! checkpoint fence. The actual free runs in the writer path
//! (`AllocatorHandle::drain_free_queue`) under the writer-serialization mutex
//! AND the allocator state mutex, rechecking the refcount under Acquire
//! ordering and requiring a later checkpoint fence before releasing the page
//! to the free list.
//!
//! Lock order: position 1.5 (before AllocatorHandle::state at position 2).
//!
//! The module uses the cfg(loom) shim pattern so its `Mutex` can be
//! permuted by loom's scheduler in concurrency harnesses.

#[cfg(loom)]
use loom::sync::Mutex;

#[cfg(not(loom))]
use std::sync::Mutex;

/// Page-lifetime work class.
///
/// `RetiredTree` is intentionally present for Phase 5 consumers, but Phase 4
/// production paths only enqueue `OverflowDeferredFree`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageLifetimeKind {
    /// Overflow-chain first page whose last `OverflowRef` dropped.
    OverflowDeferredFree,
    /// Future per-tree retired-page reclamation work.
    RetiredTree,
}

/// One queued page-lifetime work item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PageLifetimeEntry {
    kind: PageLifetimeKind,
    page: u32,
    enqueue_fence: u64,
}

impl PageLifetimeEntry {
    fn overflow_deferred_free(page: u32, enqueue_fence: u64) -> Self {
        Self {
            kind: PageLifetimeKind::OverflowDeferredFree,
            page,
            enqueue_fence,
        }
    }

    pub(crate) fn kind(self) -> PageLifetimeKind {
        self.kind
    }

    pub(crate) fn page(self) -> u32 {
        self.page
    }

    fn is_fence_eligible(self, checkpoint_fence: u64) -> bool {
        checkpoint_fence > self.enqueue_fence
    }
}

/// Checkpoint-owned lifetime drain staged before the durable boundary.
///
/// The contained entries stay quarantined until the boundary token is
/// consumed by the allocator. Publishing consumes the drain, making it
/// impossible for callers to apply the same staged lifetime delta twice.
#[derive(Debug, Default)]
pub(crate) struct CheckpointLifetimeDrain {
    entries: Vec<PageLifetimeEntry>,
}

impl CheckpointLifetimeDrain {
    /// Create a staged drain from checkpoint-safe lifetime entries.
    #[must_use]
    pub(crate) fn new(entries: Vec<PageLifetimeEntry>) -> Self {
        Self { entries }
    }

    /// Publish the staged lifetime-free deltas by consuming this drain.
    pub(crate) fn publish(self) {
        drop(self.entries);
    }
}

/// FIFO-ish queue of pages whose lifetime cannot end until a checkpoint
/// advances past the enqueue fence.
///
/// Populated by `OverflowRef::drop`. Drained by writer-path
/// `AllocatorHandle::drain_free_queue` which re-checks refcount under
/// Acquire ordering before actually freeing each page (defense-in-depth
/// against a late re-bump that shouldn't happen under RAII correctness
/// but is cheap to guard against).
#[derive(Debug, Default)]
pub(crate) struct PageLifetimeQueue {
    pending: Mutex<Vec<PageLifetimeEntry>>,
}

impl PageLifetimeQueue {
    /// Construct an empty queue.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(Vec::new()),
        }
    }

    /// Enqueue a single overflow-deferred-free page number.
    pub(crate) fn push_overflow_deferred_free(&self, page: u32, enqueue_fence: u64) {
        self.push_entry(PageLifetimeEntry::overflow_deferred_free(
            page,
            enqueue_fence,
        ));
    }

    /// Re-enqueue a previously drained entry without changing its fence.
    pub(crate) fn push_entry(&self, entry: PageLifetimeEntry) {
        #[allow(clippy::unwrap_used)]
        let mut q = self.pending.lock().unwrap();
        q.push(entry);
    }

    /// Drain entries whose enqueue fence is older than `checkpoint_fence`.
    ///
    /// Returns a snapshot of fence-eligible entries. Entries from the current
    /// or future checkpoint fence remain queued.
    #[must_use]
    pub(crate) fn take_eligible(&self, checkpoint_fence: u64) -> Vec<PageLifetimeEntry> {
        #[allow(clippy::unwrap_used)]
        let mut q = self.pending.lock().unwrap();
        let entries = std::mem::take(&mut *q);
        let mut eligible = Vec::new();
        for entry in entries {
            if entry.is_fence_eligible(checkpoint_fence) {
                eligible.push(entry);
            } else {
                q.push(entry);
            }
        }
        eligible
    }

    /// Return the current queue depth (metrics / gauge).
    #[must_use]
    pub fn depth(&self) -> usize {
        #[allow(clippy::unwrap_used)]
        let q = self.pending.lock().unwrap();
        q.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;

    #[test]
    fn new_queue_is_empty() {
        let q = PageLifetimeQueue::new();
        assert_eq!(q.depth(), 0);
        assert!(q.take_eligible(1).is_empty());
    }

    #[test]
    fn push_and_take_eligible_round_trip() {
        let q = PageLifetimeQueue::new();
        q.push_overflow_deferred_free(10, 1);
        q.push_overflow_deferred_free(20, 1);
        q.push_overflow_deferred_free(30, 1);
        assert_eq!(q.depth(), 3);

        assert!(q.take_eligible(1).is_empty());
        assert_eq!(q.depth(), 3);

        let drained: Vec<u32> = q
            .take_eligible(2)
            .into_iter()
            .map(PageLifetimeEntry::page)
            .collect();
        assert_eq!(drained, vec![10, 20, 30]);
        assert_eq!(q.depth(), 0);
    }

    #[test]
    fn push_many_appends() {
        let q = PageLifetimeQueue::new();
        q.push_overflow_deferred_free(1, 1);
        q.push_overflow_deferred_free(2, 1);
        q.push_overflow_deferred_free(3, 1);
        q.push_overflow_deferred_free(4, 1);
        let drained: Vec<u32> = q
            .take_eligible(2)
            .into_iter()
            .map(PageLifetimeEntry::page)
            .collect();
        assert_eq!(drained, vec![1, 2, 3, 4]);
    }

    #[test]
    fn take_eligible_preserves_later_entries() {
        let q = PageLifetimeQueue::new();
        q.push_overflow_deferred_free(42, 1);
        q.push_overflow_deferred_free(99, 2);
        let drained: Vec<u32> = q
            .take_eligible(2)
            .into_iter()
            .map(PageLifetimeEntry::page)
            .collect();
        assert_eq!(drained, vec![42]);
        assert_eq!(q.depth(), 1);

        let drained: Vec<u32> = q
            .take_eligible(3)
            .into_iter()
            .map(PageLifetimeEntry::page)
            .collect();
        assert_eq!(drained, vec![99]);
    }
}
