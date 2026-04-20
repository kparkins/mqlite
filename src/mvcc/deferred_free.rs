//! Deferred free queue for refcount-to-zero overflow chains.
//!
//! `OverflowRef::drop` never frees pages directly. When a decref brings the
//! refcount to 0, the page number is enqueued here. The actual free runs in
//! the writer path (`AllocatorHandle::drain_free_queue`) under the
//! writer-serialization mutex AND the allocator state mutex, rechecking the
//! refcount under Acquire ordering before releasing the page to the free list.
//!
//! Lock order: position 1.5 (before AllocatorHandle::state at position 2).
//!
//! The module uses the cfg(loom) shim pattern so its `Mutex` can be
//! permuted by loom's scheduler in concurrency harnesses.

#[cfg(loom)]
use loom::sync::Mutex;

#[cfg(not(loom))]
use std::sync::Mutex;

/// FIFO-ish queue of page numbers whose last `OverflowRef` has dropped.
///
/// Populated by `OverflowRef::drop`. Drained by writer-path
/// `AllocatorHandle::drain_free_queue` which re-checks refcount under
/// Acquire ordering before actually freeing each page (defense-in-depth
/// against a late re-bump that shouldn't happen under RAII correctness
/// but is cheap to guard against).
#[derive(Debug, Default)]
pub struct DeferredFreeQueue {
    pending: Mutex<Vec<u32>>,
}

impl DeferredFreeQueue {
    /// Construct an empty queue.
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(Vec::new()),
        }
    }

    /// Enqueue a single page number.
    pub fn push(&self, page: u32) {
        #[allow(clippy::unwrap_used)]
        let mut q = self.pending.lock().unwrap();
        q.push(page);
    }

    /// Enqueue multiple page numbers.
    pub fn push_many<I: IntoIterator<Item = u32>>(&self, pages: I) {
        #[allow(clippy::unwrap_used)]
        let mut q = self.pending.lock().unwrap();
        q.extend(pages);
    }

    /// Drain all currently queued page numbers.
    ///
    /// Returns the full snapshot. The caller should free each page under
    /// the appropriate writer-path serialization.
    pub fn take_all(&self) -> Vec<u32> {
        #[allow(clippy::unwrap_used)]
        let mut q = self.pending.lock().unwrap();
        std::mem::take(&mut *q)
    }

    /// Return the current queue depth (metrics / gauge).
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
        let q = DeferredFreeQueue::new();
        assert_eq!(q.depth(), 0);
        assert!(q.take_all().is_empty());
    }

    #[test]
    fn push_and_take_round_trip() {
        let q = DeferredFreeQueue::new();
        q.push(10);
        q.push(20);
        q.push(30);
        assert_eq!(q.depth(), 3);

        let drained = q.take_all();
        assert_eq!(drained, vec![10, 20, 30]);
        assert_eq!(q.depth(), 0);
    }

    #[test]
    fn push_many_appends() {
        let q = DeferredFreeQueue::new();
        q.push(1);
        q.push_many(vec![2, 3, 4]);
        assert_eq!(q.take_all(), vec![1, 2, 3, 4]);
    }

    #[test]
    fn take_all_clears_queue() {
        let q = DeferredFreeQueue::new();
        q.push(42);
        q.take_all();
        q.push(99);
        assert_eq!(q.take_all(), vec![99]);
    }
}
