//! Page-lifetime queue for refcount-to-zero overflow chains and dropped
//! trees' retired pages.
//!
//! `OverflowRef::drop` never frees pages directly. When a decref brings the
//! refcount to 0, the page number is enqueued here with the current
//! checkpoint fence. The actual free runs in the writer path
//! (`AllocatorHandle::drain_free_queue`) under the writer-serialization mutex
//! AND the allocator state mutex, rechecking the refcount under Acquire
//! ordering and requiring a later checkpoint fence before releasing the page
//! to the free list.
//!
//! `drop_namespace` retirements (`RetiredTree4k` / `RetiredTree32k`) ride the
//! same queue object but live in their OWN segment with an additional reader
//! low-water gate: an entry is released only when a later checkpoint fence
//! has passed AND no live `ReadView` predates the drop
//! (`oldest_required_ts() >= reader_fence_ts`). The floor protects a reader
//! only once its `ReadView` is REGISTERED: a reader that loaded the pre-drop
//! epoch but has not yet registered is invisible to `oldest_required_ts()`
//! (empty registry ⇒ `Ts::MAX`). That load-to-register window is closed
//! separately by `open_snapshot_read_view`'s post-registration
//! catalog-generation revalidation (snapshot_ops.rs, F36): a snapshot view
//! either registers before any DDL publishes — and then holds the floor for
//! its lifetime — or its open fails cleanly with `ReadViewExpired`. Together
//! the two gates mean a stale-epoch snapshot never observes the dropped
//! tree's pages reused. Hot drains
//! ([`PageLifetimeQueue::take_eligible`] — write-txn begin,
//! `pin_with_reconcile`) never scan the retired segment and never evaluate
//! the reader floor; only the checkpoint drain
//! ([`PageLifetimeQueue::take_eligible_retired`], reached via
//! `BufferPoolHandle::advance_page_lifetime_checkpoint` with pool-coherent
//! io) releases retired entries. This keeps a dropped 100k-page namespace
//! from taxing every commit/pin with an O(queue-depth) scan plus a
//! `ReadViewRegistry` lock, and guarantees retired-tree frees are never
//! issued through a raw (pool-incoherent) `PageSource`.
//!
//! Lock order: position 1.5 (before AllocatorHandle::state at position 2).
//!
//! The module uses the cfg(loom) shim pattern so its `Mutex` can be
//! permuted by loom's scheduler in concurrency harnesses.

#[cfg(loom)]
use loom::sync::Mutex;

#[cfg(not(loom))]
use std::sync::Mutex;

use crate::mvcc::timestamp::Ts;

/// Page-lifetime work class.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageLifetimeKind {
    /// Overflow-chain first page whose last `OverflowRef` dropped.
    OverflowDeferredFree,
    /// 4 KiB internal page of a dropped tree (freed via `free_4k`).
    RetiredTree4k,
    /// 32 KiB leaf/overflow page of a dropped tree (freed via `free_32k`).
    RetiredTree32k,
}

/// One queued page-lifetime work item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PageLifetimeEntry {
    kind: PageLifetimeKind,
    page: u32,
    enqueue_fence: u64,
    /// Reader low-water requirement for retired-tree entries: the entry may
    /// only be released once no live `ReadView` has `read_ts <
    /// reader_fence_ts` (i.e. once `oldest_required_ts() >=
    /// reader_fence_ts`). Recorded as the published `visible_ts` observed
    /// right after the drop published, so every reader pinned to a pre-drop
    /// epoch sorts strictly below it. Unused (default) for
    /// `OverflowDeferredFree`, whose reader protection is the overflow
    /// refcount.
    reader_fence_ts: Ts,
}

impl PageLifetimeEntry {
    fn overflow_deferred_free(page: u32, enqueue_fence: u64) -> Self {
        Self {
            kind: PageLifetimeKind::OverflowDeferredFree,
            page,
            enqueue_fence,
            reader_fence_ts: Ts::default(),
        }
    }

    fn retired_tree(
        kind: PageLifetimeKind,
        page: u32,
        enqueue_fence: u64,
        reader_fence_ts: Ts,
    ) -> Self {
        debug_assert!(
            matches!(
                kind,
                PageLifetimeKind::RetiredTree4k | PageLifetimeKind::RetiredTree32k
            ),
            "retired_tree entries must use a RetiredTree* kind"
        );
        Self {
            kind,
            page,
            enqueue_fence,
            reader_fence_ts,
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

/// Segment storage for [`PageLifetimeQueue`]: one `Vec` per release class.
///
/// `overflow` holds `OverflowDeferredFree` entries (hot drains scan only
/// this segment); `retired` holds `RetiredTree4k` / `RetiredTree32k`
/// entries, released exclusively by the checkpoint drain via
/// [`PageLifetimeQueue::take_eligible_retired`]. One mutex guards both so
/// the lock-order story (position 1.5) is unchanged.
#[derive(Debug, Default)]
struct PendingSegments {
    overflow: Vec<PageLifetimeEntry>,
    retired: Vec<PageLifetimeEntry>,
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
    pending: Mutex<PendingSegments>,
}

impl PageLifetimeQueue {
    /// Construct an empty queue.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(PendingSegments::default()),
        }
    }

    /// Enqueue a single overflow-deferred-free page number.
    pub(crate) fn push_overflow_deferred_free(&self, page: u32, enqueue_fence: u64) {
        self.push_entry(PageLifetimeEntry::overflow_deferred_free(
            page,
            enqueue_fence,
        ));
    }

    /// Enqueue a dropped tree's retired page (either size class).
    ///
    /// `reader_fence_ts` is the reader low-water requirement: the entry is
    /// only released once no live `ReadView` predates it (see
    /// [`PageLifetimeEntry::reader_fence_ts`]).
    pub(crate) fn push_retired_tree(
        &self,
        kind: PageLifetimeKind,
        page: u32,
        enqueue_fence: u64,
        reader_fence_ts: Ts,
    ) {
        self.push_entry(PageLifetimeEntry::retired_tree(
            kind,
            page,
            enqueue_fence,
            reader_fence_ts,
        ));
    }

    /// Re-enqueue a previously drained entry without changing its fence,
    /// routed to the segment matching its kind.
    pub(crate) fn push_entry(&self, entry: PageLifetimeEntry) {
        #[allow(clippy::unwrap_used)]
        let mut q = self.pending.lock().unwrap();
        match entry.kind {
            PageLifetimeKind::OverflowDeferredFree => q.overflow.push(entry),
            PageLifetimeKind::RetiredTree4k | PageLifetimeKind::RetiredTree32k => {
                q.retired.push(entry);
            }
        }
    }

    /// Drain `OverflowDeferredFree` entries whose enqueue fence is older
    /// than `checkpoint_fence`.
    ///
    /// Returns a snapshot of fence-eligible overflow entries. Entries from
    /// the current or future checkpoint fence remain queued. `RetiredTree*`
    /// entries are NEVER touched by this method — hot drains (write-txn
    /// begin, `pin_with_reconcile`) pay zero scan cost for them and never
    /// evaluate the reader floor; the checkpoint drain releases them via
    /// [`Self::take_eligible_retired`].
    #[must_use]
    pub(crate) fn take_eligible(&self, checkpoint_fence: u64) -> Vec<PageLifetimeEntry> {
        #[allow(clippy::unwrap_used)]
        let mut q = self.pending.lock().unwrap();
        if q.overflow.is_empty() {
            return Vec::new();
        }
        let mut eligible = Vec::new();
        q.overflow.retain(|entry| {
            if entry.is_fence_eligible(checkpoint_fence) {
                eligible.push(*entry);
                false
            } else {
                true
            }
        });
        eligible
    }

    /// Drain fence-eligible `RetiredTree*` entries, additionally gated on
    /// the reader low-water. Checkpoint-drain only: the sole production
    /// caller is `AllocatorHandle::drain_free_queue_with_retired` (reached
    /// via `BufferPoolHandle::advance_page_lifetime_checkpoint`), whose io
    /// is the pool-coherent `BufferPoolPageSource` — retired-tree free-list
    /// links must never be written through a raw backing `PageSource` while
    /// the dropped tree's frames may still be resident.
    ///
    /// `reader_floor` is invoked lazily — at most once, and only when a
    /// fence-eligible retired-tree entry is encountered — and must return
    /// the current `ReadViewRegistry::oldest_required_ts()` (or `None` when
    /// no registry is wired up, which conservatively keeps retired entries
    /// queued). A retired entry is released only when `floor >=
    /// reader_fence_ts`, i.e. when no live `ReadView` predates the drop
    /// that retired the page.
    ///
    /// The closure runs while the queue mutex (lock-order position 1.5) is
    /// held; providers may take the `ReadViewRegistry` mutex (position 5),
    /// which is an ascending — legal — acquisition.
    #[must_use]
    pub(crate) fn take_eligible_retired<F>(
        &self,
        checkpoint_fence: u64,
        reader_floor: F,
    ) -> Vec<PageLifetimeEntry>
    where
        F: FnOnce() -> Option<Ts>,
    {
        #[allow(clippy::unwrap_used)]
        let mut q = self.pending.lock().unwrap();
        if q.retired.is_empty() {
            return Vec::new();
        }
        let mut floor_fn = Some(reader_floor);
        let mut floor_cache: Option<Option<Ts>> = None;
        let mut eligible = Vec::new();
        q.retired.retain(|entry| {
            let release = entry.is_fence_eligible(checkpoint_fence) && {
                let floor =
                    *floor_cache.get_or_insert_with(|| floor_fn.take().and_then(|f| f()));
                floor.is_some_and(|floor| floor >= entry.reader_fence_ts)
            };
            if release {
                eligible.push(*entry);
                false
            } else {
                true
            }
        });
        eligible
    }

    /// Return the current queue depth across both segments (metrics / gauge).
    #[must_use]
    pub fn depth(&self) -> usize {
        #[allow(clippy::unwrap_used)]
        let q = self.pending.lock().unwrap();
        q.overflow.len() + q.retired.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(not(loom))]
#[path = "tests/deferred_free.rs"]
mod tests;

#[cfg(test)]
#[cfg(not(loom))]
#[path = "tests/deferred_free_segments.rs"]
mod segment_tests;
