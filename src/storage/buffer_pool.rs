//! Buffer pool — in-memory page cache with CLOCK sweep eviction.
//!
//! The buffer pool maintains two independent partitions:
//! - **4 KB partition** (25 % of total) — internal B+ tree nodes
//! - **32 KB partition** (75 % of total) — leaf, overflow, and header pages
//!
//! Each partition has its own CLOCK sweep hand and backing frame array.
//!
//! ## Usage pattern
//!
//! Call [`BufferPool::pin`] to obtain a [`PinnedPage`] guard for a given page
//! number and size.  Writing through [`PinnedPage::data_mut`] automatically
//! marks the page dirty.  The page is unpinned when the guard is dropped.
//! Call [`BufferPool::flush`] before a checkpoint or close to write all dirty
//! pages to disk.
//!
//! ## Thread safety
//!
//! Each partition is protected by a separate [`Mutex`].  The data pointer
//! inside [`PinnedPage`] is stable for the duration of the pin because:
//!
//! 1. CLOCK eviction skips frames whose `pin_count > 0`.
//! 2. Frame backing storage (`Box<[u8]>`) never moves — frame slots are
//!    pre-allocated at pool creation with fixed capacity and are never
//!    reallocated.
//!
//! Callers must ensure that at most one [`PinnedPage`] for a given page number
//! calls [`data_mut`](PinnedPage::data_mut) at a time.  Multiple concurrent
//! read-only pins (using only [`data`](PinnedPage::data)) are safe.  The
//! database-level single-writer lock enforces this at a higher level.
//
// LOCK-ORDER (CRITICAL-1; iter-4): this file owns positions **3** (32 KB
// partition mutex, `BufferPool::inner_32k`) and **4** (4 KB partition
// mutex, `BufferPool::inner_4k`) in the database-wide total order. Any
// path that acquires both partitions MUST acquire 32 KB before 4 KB, and
// must NOT re-enter the history-store partition (position 1),
// `DeferredFreeQueue::pending` (1.5), or `AllocatorHandle::state` (2)
// while holding either partition mutex. The canonical definition of the
// full order (positions 1 → 1.5 → 2 → 3 → 4 → 5 → 6) lives at the top of
// `src/mvcc/read_view.rs` — edit both blocks together or neither.
// T6 wires the reconciliation path that snapshots
// `ReadViewRegistry::oldest_required_ts()` (position 5) BEFORE acquiring
// a partition mutex; T4 only adds the primitive.

use std::collections::{HashMap, VecDeque};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::mvcc::metrics;
use crate::mvcc::read_view::{ChainSnapshot, ReadView, ReadViewRegistry};
use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::VersionEntry;
use crate::storage::allocator::AllocatorHandle;

// ---------------------------------------------------------------------------
// Main buffer-pool sharding (T6 / S12)
// ---------------------------------------------------------------------------

/// Number of independent main buffer pools in the engine.
///
/// The MVCC design (plan §T6, S12 criterion) mandates a single main pool
/// (two size-class partitions live *inside* that pool). T7 adds a dedicated
/// history-store pool but does not change this count. A second main pool
/// would require a second lock-order position at level 3 / 4 — intentionally
/// ruled out. Changes to this constant must be accompanied by a lock-order
/// audit; the compile-time assertion in
/// `tests/partition_pool_sharding_invariant.rs` guards the invariant.
#[allow(dead_code)]
pub(crate) const N_MAIN_POOLS: usize = 1;

// ---------------------------------------------------------------------------
// Page size
// ---------------------------------------------------------------------------

/// Indicates which page-size partition to use for a given page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageSize {
    /// 4 KiB — internal (branch) B+ tree nodes.
    Small4k,
    /// 32 KiB — leaf nodes, overflow pages, and the file header.
    Large32k,
}

impl PageSize {
    pub(crate) fn bytes(self) -> usize {
        match self {
            PageSize::Small4k => 4096,
            PageSize::Large32k => 32768,
        }
    }
}

// ---------------------------------------------------------------------------
// Page I/O abstraction
// ---------------------------------------------------------------------------

/// Abstraction over on-disk (or in-memory for tests) page read/write.
///
/// Implementors are responsible for knowing the page size from the `size`
/// parameter and performing the appropriate seek + I/O.
pub(crate) trait PageSource: Send + Sync {
    /// Read `size.bytes()` bytes for `page_number` into `buf`.
    ///
    /// `buf.len()` is guaranteed to equal `size.bytes()`.
    fn read_page(&self, page_number: u32, size: PageSize, buf: &mut [u8]) -> Result<()>;

    /// Write `buf` (length `size.bytes()`) to `page_number` on disk.
    fn write_page(&self, page_number: u32, size: PageSize, buf: &[u8]) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Frame (internal)
// ---------------------------------------------------------------------------

struct Frame {
    page_number: u32,
    /// Heap-allocated page data; length equals the partition's page size.
    /// The `Box` pointer is stable (never moved) for the lifetime of this slot.
    data: Box<[u8]>,
    pin_count: u32,
    dirty: bool,
    ref_bit: bool,
    /// Per-frame MVCC version chains, keyed by B+ tree key. Migrates with
    /// the frame's cells on split / merge (see T3.5). Empty for non-leaf
    /// frames and for leaf frames written by the pre-MVCC writer path.
    version_chains: HashMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>,
}

// ---------------------------------------------------------------------------
// Partition (internal)
// ---------------------------------------------------------------------------

/// One pool partition; all frames share the same page size.
struct Partition {
    /// Fixed-size slot array — pre-allocated, never reallocated.
    /// `None` denotes an empty slot.
    frames: Vec<Option<Frame>>,
    /// page_number → slot index.
    page_map: HashMap<u32, usize>,
    /// CLOCK sweep hand.
    clock_hand: usize,
    page_size: usize,
    capacity: usize,
}

impl Partition {
    fn new(capacity: usize, page_size: usize) -> Self {
        let capacity = capacity.max(1);
        let mut frames = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            frames.push(None);
        }
        Self {
            frames,
            page_map: HashMap::new(),
            clock_hand: 0,
            page_size,
            capacity,
        }
    }

    /// CLOCK sweep: find a victim slot for eviction.
    ///
    /// - Empty slot → immediate winner.
    /// - `pin_count > 0` → skipped entirely.
    /// - `ref_bit = 1` → cleared (second chance) and skipped.
    /// - `ref_bit = 0 && pin_count = 0` → victim.
    ///
    /// Scans at most `2 * capacity` frames (two full sweeps) before giving up.
    /// Returns `None` if all frames are pinned.
    fn find_victim(&mut self) -> Option<usize> {
        let n = self.capacity;
        for _ in 0..(2 * n) {
            let idx = self.clock_hand;
            self.clock_hand = (idx + 1) % n;

            match &mut self.frames[idx] {
                None => return Some(idx),
                Some(frame) => {
                    if frame.pin_count > 0 {
                        continue;
                    }
                    if frame.ref_bit {
                        frame.ref_bit = false;
                        continue;
                    }
                    return Some(idx);
                }
            }
        }
        None
    }

    /// Evict the frame at `idx`, flushing to disk if dirty.
    ///
    /// Lock-order note (T6): any caller that reaches this method along a
    /// reconciliation path MUST have snapshotted
    /// `ReadViewRegistry::oldest_required_ts()` *before* acquiring the
    /// partition mutex (see `BufferPool::reconcile`). Registry (position 5)
    /// is below the partition mutex (positions 3/4) in the total order, so
    /// re-acquiring it while holding the partition lock is forbidden.
    fn evict_frame(&mut self, idx: usize, io: &dyn PageSource, size: PageSize) -> Result<()> {
        if let Some(frame) = &self.frames[idx] {
            let was_dirty = frame.dirty;
            if was_dirty {
                io.write_page(frame.page_number, size, &frame.data)?;
            }
            self.page_map.remove(&frame.page_number);
            #[cfg(feature = "tracing")]
            tracing::debug!(
                target: "mqlite",
                pages_evicted = 1u64,
                dirty_pages_flushed = was_dirty as u64,
                "mqlite::eviction"
            );
        }
        Ok(())
    }

    /// Pin `page_number`.  Returns the frame slot index.
    fn pin_page(&mut self, page_number: u32, io: &dyn PageSource, size: PageSize) -> Result<usize> {
        // Cache hit path
        if let Some(&idx) = self.page_map.get(&page_number) {
            let frame = self.frames[idx]
                .as_mut()
                .expect("page_map invariant: frame must exist at mapped slot");
            frame.pin_count += 1;
            frame.ref_bit = true;
            return Ok(idx);
        }

        // Cache miss — find a victim
        let idx = self.find_victim().ok_or_else(|| {
            Error::Internal(
                "buffer pool exhausted: all frames are pinned; \
                 unpin unused pages or increase buffer_pool_size"
                    .into(),
            )
        })?;

        // Evict current occupant (if any)
        self.evict_frame(idx, io, size)?;

        // Load from disk
        let mut data = vec![0u8; self.page_size].into_boxed_slice();
        io.read_page(page_number, size, &mut data)?;

        self.frames[idx] = Some(Frame {
            page_number,
            data,
            pin_count: 1,
            dirty: false,
            ref_bit: true,
            version_chains: HashMap::new(),
        });
        self.page_map.insert(page_number, idx);

        Ok(idx)
    }

    /// Identical to `pin_page` but, on a cache miss, inline-reconciles the
    /// victim frame's version chains against `ort` BEFORE evicting it.
    ///
    /// Returns `(frame_idx, entries_dropped)`. `ort` must be snapshotted
    /// from `ReadViewRegistry::oldest_required_ts()` OUTSIDE the partition
    /// mutex (position 5 < positions 3/4 — see lock-order doc at top).
    fn pin_page_reconciling(
        &mut self,
        page_number: u32,
        ort: Ts,
        io: &dyn PageSource,
        size: PageSize,
    ) -> Result<(usize, usize)> {
        // Cache hit — no victim, no reconciliation.
        if let Some(&idx) = self.page_map.get(&page_number) {
            let frame = self.frames[idx]
                .as_mut()
                .expect("page_map invariant: frame must exist at mapped slot");
            frame.pin_count += 1;
            frame.ref_bit = true;
            return Ok((idx, 0));
        }

        let idx = self.find_victim().ok_or_else(|| {
            Error::Internal(
                "buffer pool exhausted: all frames are pinned; \
                 unpin unused pages or increase buffer_pool_size"
                    .into(),
            )
        })?;

        // Prune the victim's chains against the snapshotted horizon before
        // it is evicted. Entries with `stop_ts <= ort && stop_ts < Ts::MAX`
        // are invisible to every live reader; retain only the live head
        // and committed-replaced entries above the horizon.
        let dropped = self.reconcile_frame_at(idx, ort);

        // Evict current occupant (if any)
        self.evict_frame(idx, io, size)?;

        // Load from disk
        let mut data = vec![0u8; self.page_size].into_boxed_slice();
        io.read_page(page_number, size, &mut data)?;

        self.frames[idx] = Some(Frame {
            page_number,
            data,
            pin_count: 1,
            dirty: false,
            ref_bit: true,
            version_chains: HashMap::new(),
        });
        self.page_map.insert(page_number, idx);

        Ok((idx, dropped))
    }

    /// Prune the frame at slot `idx`'s version chains against horizon `ort`.
    /// Returns the number of `VersionEntry` objects dropped. No-op if the
    /// slot is empty.
    fn reconcile_frame_at(&mut self, idx: usize, ort: Ts) -> usize {
        let Some(frame) = self.frames[idx].as_mut() else {
            return 0;
        };
        let mut dropped = 0usize;
        let keys: Vec<Vec<u8>> = frame.version_chains.keys().cloned().collect();
        for key in keys {
            let Some(chain_arc) = frame.version_chains.get_mut(&key) else {
                continue;
            };
            let before = chain_arc.len();
            let chain_mut = Arc::make_mut(chain_arc);
            chain_mut.retain(|e| e.stop_ts == Ts::MAX || e.stop_ts > ort);
            let after = chain_arc.len();
            dropped += before - after;

            let collapse = chain_arc.len() == 1
                && chain_arc
                    .front()
                    .map(|e| e.stop_ts == Ts::MAX && !e.is_tombstone)
                    .unwrap_or(false);
            if collapse || chain_arc.is_empty() {
                frame.version_chains.remove(&key);
            }
        }
        dropped
    }

    /// Decrement `pin_count`; optionally mark the frame dirty.
    fn unpin_page(&mut self, page_number: u32, dirty: bool) -> Result<()> {
        let idx = self.page_map.get(&page_number).copied().ok_or_else(|| {
            Error::Internal(format!(
                "buffer pool unpin: page {page_number} is not in the pool"
            ))
        })?;

        let frame = self.frames[idx]
            .as_mut()
            .expect("page_map invariant: frame must exist at mapped slot");

        if frame.pin_count == 0 {
            return Err(Error::Internal(format!(
                "buffer pool unpin: page {page_number} pin_count is already 0"
            )));
        }
        frame.pin_count -= 1;
        if dirty {
            frame.dirty = true;
        }
        Ok(())
    }

    /// Write every dirty frame to disk and clear their dirty bits.
    fn flush_all(&mut self, io: &dyn PageSource, size: PageSize) -> Result<()> {
        for slot in self.frames.iter_mut() {
            if let Some(frame) = slot {
                if frame.dirty {
                    io.write_page(frame.page_number, size, &frame.data)?;
                    frame.dirty = false;
                }
            }
        }
        Ok(())
    }

    /// Discard all dirty, unpinned frames without writing them to disk.
    ///
    /// Used by the WAL rollback path: frames written during an aborted
    /// transaction must be evicted so subsequent reads fetch clean data from
    /// the WAL/file rather than seeing partial writes.
    fn drop_dirty_unpinned(&mut self) {
        let mut to_drop = Vec::new();
        for slot in self.frames.iter() {
            if let Some(frame) = slot {
                if frame.dirty && frame.pin_count == 0 {
                    to_drop.push(frame.page_number);
                }
            }
        }
        for pn in to_drop {
            if let Some(&idx) = self.page_map.get(&pn) {
                self.frames[idx] = None;
                self.page_map.remove(&pn);
            }
        }
    }

    /// Return a raw mutable pointer to the frame's data buffer.
    ///
    /// # Safety
    ///
    /// Caller must ensure `pin_count > 0` for the frame at `idx`
    /// (preventing eviction) and must not create concurrent mutable aliases.
    fn data_ptr_mut(&mut self, idx: usize) -> NonNull<[u8]> {
        let frame = self.frames[idx]
            .as_mut()
            .expect("data_ptr_mut: frame slot must be occupied");
        NonNull::from(frame.data.as_mut())
    }

    // -----------------------------------------------------------------------
    // Introspection helpers (tests only)
    // -----------------------------------------------------------------------

    #[cfg(test)]
    fn pin_count(&self, page_number: u32) -> Option<u32> {
        let idx = *self.page_map.get(&page_number)?;
        self.frames[idx].as_ref().map(|f| f.pin_count)
    }

    #[cfg(test)]
    fn is_dirty(&self, page_number: u32) -> Option<bool> {
        let idx = *self.page_map.get(&page_number)?;
        self.frames[idx].as_ref().map(|f| f.dirty)
    }

    #[cfg(test)]
    fn is_cached(&self, page_number: u32) -> bool {
        self.page_map.contains_key(&page_number)
    }
}

// ---------------------------------------------------------------------------
// PinnedPage guard
// ---------------------------------------------------------------------------

/// A handle to a page that has been pinned in the buffer pool.
///
/// - [`data`](PinnedPage::data) — shared (read-only) view.
/// - [`data_mut`](PinnedPage::data_mut) — exclusive view; automatically sets
///   the dirty bit so the page is written to disk on the next
///   [`flush`](BufferPool::flush).
/// - Drop — automatically unpins the page (decrements `pin_count`).
///
/// # Safety
///
/// The pointer inside is stable because CLOCK eviction refuses to evict
/// pinned frames and the frame's `Box<[u8]>` never moves.  Do not call
/// `data_mut` on two different `PinnedPage`s for the same page concurrently;
/// the database-level writer lock prevents this.
pub(crate) struct PinnedPage<'pool> {
    pool: &'pool BufferPool,
    page_number: u32,
    page_size: PageSize,
    ptr: NonNull<[u8]>,
    dirty: bool,
}

// SAFETY: The raw pointer points to a heap-allocated Box inside the pool.
// The pool is Send+Sync; the PinnedPage is only shared across threads under
// the database-level read lock (for immutable access).
unsafe impl Send for PinnedPage<'_> {}
unsafe impl Sync for PinnedPage<'_> {}

impl<'pool> PinnedPage<'pool> {
    /// Read-only view of the page data.
    #[inline]
    pub(crate) fn data(&self) -> &[u8] {
        // SAFETY: pointer is valid while pin_count > 0; no mutable alias
        // while holding only a shared reference to this guard.
        unsafe { self.ptr.as_ref() }
    }

    /// Mutable view of the page data; marks the page dirty.
    #[inline]
    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        self.dirty = true;
        // SAFETY: same stability guarantee; exclusivity enforced by the
        // single-writer database lock.
        unsafe { self.ptr.as_mut() }
    }

    /// Explicitly mark this page as modified without writing any bytes.
    #[allow(dead_code)]
    pub(crate) fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// The page number this handle refers to.
    #[allow(dead_code)]
    pub(crate) fn page_number(&self) -> u32 {
        self.page_number
    }
}

impl Drop for PinnedPage<'_> {
    fn drop(&mut self) {
        // Errors are intentionally swallowed — Drop must not panic.
        let _ = self
            .pool
            .unpin_internal(self.page_number, self.page_size, self.dirty);
    }
}

// ---------------------------------------------------------------------------
// BufferPool
// ---------------------------------------------------------------------------

/// In-memory page cache with CLOCK sweep eviction.
///
/// Frame counts are computed from `buffer_pool_size`:
/// - 25 % → 4 KB frames (internal nodes)
/// - 75 % → 32 KB frames (leaf / overflow / header pages)
///
/// # Usage
///
/// Create the pool with [`BufferPool::new`], specifying the total byte budget
/// and a [`PageSource`] backend.  Pin pages with [`BufferPool::pin`], read or
/// write them via [`PinnedPage::data`] / [`PinnedPage::data_mut`], then drop
/// the guard to unpin.  Call [`BufferPool::flush`] before a checkpoint or
/// close to write all dirty pages to disk.
pub(crate) struct BufferPool {
    inner_4k: Mutex<Partition>,
    inner_32k: Mutex<Partition>,
    io: Box<dyn PageSource>,
}

impl BufferPool {
    /// Create a new buffer pool backed by `io`.
    ///
    /// `buffer_pool_size` is the total byte budget.  Both partitions receive
    /// at least one frame even when the budget is very small.
    pub(crate) fn new(buffer_pool_size: usize, io: Box<dyn PageSource>) -> Self {
        let size_4k = buffer_pool_size / 4;
        let size_32k = buffer_pool_size - size_4k;

        let capacity_4k = (size_4k / PageSize::Small4k.bytes()).max(1);
        let capacity_32k = (size_32k / PageSize::Large32k.bytes()).max(1);

        Self {
            inner_4k: Mutex::new(Partition::new(capacity_4k, PageSize::Small4k.bytes())),
            inner_32k: Mutex::new(Partition::new(capacity_32k, PageSize::Large32k.bytes())),
            io,
        }
    }

    /// Pin `page_number` in the appropriate partition and return a
    /// [`PinnedPage`] guard.
    ///
    /// On cache hit the guard is returned immediately after updating
    /// `pin_count` and `ref_bit`.  On cache miss a victim is evicted (flushed
    /// to disk if dirty) and the page is loaded from the I/O backend.
    ///
    /// # Errors
    ///
    /// - Mutex poisoned (should not happen in normal operation).
    /// - All frames in the partition are currently pinned.
    /// - I/O backend error during load or eviction.
    pub(crate) fn pin(&self, page_number: u32, size: PageSize) -> Result<PinnedPage<'_>> {
        let (lock, size_enum) = match size {
            PageSize::Small4k => (&self.inner_4k, PageSize::Small4k),
            PageSize::Large32k => (&self.inner_32k, PageSize::Large32k),
        };

        let mut guard = lock
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;

        let idx = guard.pin_page(page_number, self.io.as_ref(), size_enum)?;

        // Obtain raw pointer while still holding the lock.
        // SAFETY: Vec backing does not reallocate (fixed capacity);
        // eviction is prevented by pin_count > 0 set above.
        let ptr = guard.data_ptr_mut(idx);

        Ok(PinnedPage {
            pool: self,
            page_number,
            page_size: size_enum,
            ptr,
            dirty: false,
        })
    }

    /// Pin `page_number` with chain reconciliation on the miss path (T7).
    ///
    /// Identical to [`BufferPool::pin`] on a cache hit. On a miss, the
    /// chosen victim frame's version chains are pruned against the current
    /// `ReadViewRegistry` horizon BEFORE eviction, so aged entries never
    /// outlive the frame that hosts them. After the pin returns, the
    /// writer-serialized [`DeferredFreeQueue`] drain is invoked to reclaim
    /// overflow pages whose refcount reached zero as a side-effect of the
    /// prune.
    ///
    /// **Lock-order contract (T4 / T6 / T7):**
    /// 1. `ReadViewRegistry::oldest_required_ts()` is snapshotted BEFORE
    ///    the partition mutex. Position 5 is below positions 3/4 in the
    ///    total order.
    /// 2. The partition mutex is released before `drain_free_queue` is
    ///    invoked, so the allocator-state mutex (position 2) is never
    ///    nested under a partition mutex (positions 3/4).
    ///
    /// Callers with access to a `ReadViewRegistry` and `AllocatorHandle`
    /// (the high-level reader/writer paths via `BufferPoolHandle`) must
    /// prefer this over `pin` so that eviction never drops a frame whose
    /// chains still carry versions visible to no live reader.
    pub(crate) fn pin_with_reconcile<'a>(
        &'a self,
        page_number: u32,
        size: PageSize,
        registry: &ReadViewRegistry,
        allocator: &AllocatorHandle,
    ) -> Result<PinnedPage<'a>> {
        // 1. Snapshot the horizon BEFORE any partition latch.
        let ort = registry.oldest_required_ts();

        let (lock, size_enum) = match size {
            PageSize::Small4k => (&self.inner_4k, PageSize::Small4k),
            PageSize::Large32k => (&self.inner_32k, PageSize::Large32k),
        };

        // 2. Pin + reconcile victim under the partition lock.
        let (ptr, dropped) = {
            let mut guard = lock
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            let (idx, dropped) =
                guard.pin_page_reconciling(page_number, ort, self.io.as_ref(), size_enum)?;
            // SAFETY: Vec backing does not reallocate (fixed capacity);
            // eviction is prevented by pin_count > 0 set above.
            (guard.data_ptr_mut(idx), dropped)
        };

        // 3. Tick counters + drain deferred-free queue outside the latch.
        if dropped > 0 {
            metrics::record_reconcile_entries_dropped(dropped as u64);
        }
        metrics::set_deferred_free_queue_depth(
            allocator.deferred_free_queue().depth() as u64,
        );
        allocator.drain_free_queue(self.io.as_ref())?;

        Ok(PinnedPage {
            pool: self,
            page_number,
            page_size: size_enum,
            ptr,
            dirty: false,
        })
    }

    /// Write all dirty pages in both partitions to disk and clear dirty bits.
    ///
    /// Must be called before a WAL checkpoint or `Database::close` to ensure
    /// in-flight modifications reach stable storage.
    pub(crate) fn flush(&self) -> Result<()> {
        self.inner_4k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?
            .flush_all(self.io.as_ref(), PageSize::Small4k)?;

        self.inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?
            .flush_all(self.io.as_ref(), PageSize::Large32k)?;

        Ok(())
    }

    /// Discard all dirty, unpinned frames in both partitions without writing
    /// them to disk.
    ///
    /// Called by the journal rollback path after [`crate::journal::JournalManager::truncate_to`] so
    /// that stale in-memory writes are not mistaken for committed data.
    pub(crate) fn drop_all_dirty(&self) -> Result<()> {
        self.inner_4k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?
            .drop_dirty_unpinned();

        self.inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?
            .drop_dirty_unpinned();

        Ok(())
    }

    /// Invalidate the cached frame for `page_number` (plan §M4b).
    ///
    /// Used by the writer-txn rollback path after returning a page to the
    /// allocator free list: the previous occupant's frame may still be
    /// resident with stale content from the failing txn, and the next
    /// allocator user who recycles this page number must not see it.
    ///
    /// Behavior:
    /// - Page not resident: no-op.
    /// - Page resident and unpinned: drop the frame (including its dirty
    ///   data and any version chains — a freshly-allocated page carries
    ///   no chains worth preserving).
    /// - Page resident and pinned: this is a programming error — rollback
    ///   runs after every `PinnedPage` from the txn has dropped, so the
    ///   pin count must be 0. Returns `Error::Internal` in release; the
    ///   partition stays untouched.
    pub(crate) fn invalidate_page(&self, page_number: u32, size: PageSize) -> Result<()> {
        let lock = match size {
            PageSize::Small4k => &self.inner_4k,
            PageSize::Large32k => &self.inner_32k,
        };
        let mut guard = lock
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let idx = match guard.page_map.get(&page_number).copied() {
            Some(i) => i,
            None => return Ok(()),
        };
        let pin_count = guard.frames[idx]
            .as_ref()
            .map(|f| f.pin_count)
            .unwrap_or(0);
        if pin_count > 0 {
            return Err(Error::Internal(format!(
                "buffer pool invalidate_page: page {page_number} is pinned \
                 (pin_count = {pin_count}); rollback must run after all \
                 PinnedPage guards for the txn have dropped"
            )));
        }
        guard.frames[idx] = None;
        guard.page_map.remove(&page_number);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // MVCC version-chain helpers (T3.5)
    //
    // Chains are stored on the 32 KB partition's frames (leaf pages). The
    // caller is responsible for having pinned the page (via `read_leaf` or
    // `write_leaf`) recently enough that the frame is still resident — the
    // MVCC writer lane sequences these calls synchronously after a leaf
    // read / write, so the frame has not yet been eligible for eviction.
    // -----------------------------------------------------------------------

    /// Remove and return the version chain for `key` on leaf page `page`.
    pub(crate) fn take_chain(
        &self,
        page: u32,
        key: &[u8],
    ) -> Result<Option<Arc<VecDeque<VersionEntry>>>> {
        let mut guard = self
            .inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let Some(&idx) = guard.page_map.get(&page) else {
            return Ok(None);
        };
        let frame = guard.frames[idx]
            .as_mut()
            .expect("page_map invariant: frame must exist at mapped slot");
        Ok(frame.version_chains.remove(key))
    }

    /// Install a version chain for `key` on leaf page `page`.
    pub(crate) fn put_chain(
        &self,
        page: u32,
        key: Vec<u8>,
        chain: Arc<VecDeque<VersionEntry>>,
    ) -> Result<()> {
        let mut guard = self
            .inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let idx = guard.page_map.get(&page).copied().ok_or_else(|| {
            Error::Internal(format!(
                "buffer pool put_chain: page {page} is not resident"
            ))
        })?;
        let frame = guard.frames[idx]
            .as_mut()
            .expect("page_map invariant: frame must exist at mapped slot");
        frame.version_chains.insert(key, chain);
        Ok(())
    }

    /// Build a [`ChainSnapshot`] from the per-key MVCC version chains on
    /// leaf page `page`. Returns `None` if the page is not currently
    /// resident (the caller must have the frame pinned via `pin_page` for
    /// the snapshot to reflect the live chains).
    ///
    /// Deep-clones every `VersionEntry` under the partition mutex,
    /// which runs `OverflowRef::Clone` on `VersionData::Overflow` entries
    /// (CAS-loop incref on the page's refcount header). The partition
    /// mutex and the overflow refcount atomics are orthogonal: the CAS
    /// loop touches an `AtomicU32` off the `AllocatorHandle::overflow_refcounts`
    /// table, never the partition mutex itself.
    pub(crate) fn snapshot_chains(
        &self,
        page: u32,
        view: Option<Arc<ReadView>>,
    ) -> Result<Option<ChainSnapshot>> {
        let guard = self
            .inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let Some(&idx) = guard.page_map.get(&page) else {
            return Ok(None);
        };
        let frame = guard.frames[idx]
            .as_ref()
            .expect("page_map invariant: frame must exist at mapped slot");
        Ok(Some(ChainSnapshot::new(&frame.version_chains, view)))
    }

    /// Clear all version chains attached to the resident frame for `page`
    /// in the partition selected by `size`.
    ///
    /// Used by the overflow-chain free path: overflow pages share the
    /// 32 KB leaf partition with data leaves, so a page reborn as an
    /// overflow page may inherit stale `version_chains` entries from
    /// its previous data-leaf life. Clearing them keeps the T3.5
    /// `chains_empty` guard inside `free_leaf` consumers sound.
    ///
    /// No-op when the page is not resident — there are no chains to
    /// clear in that case.
    pub(crate) fn clear_chains_on_page(&self, page: u32, size: PageSize) -> Result<()> {
        let lock = match size {
            PageSize::Small4k => &self.inner_4k,
            PageSize::Large32k => &self.inner_32k,
        };
        let mut guard = lock
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let Some(&idx) = guard.page_map.get(&page) else {
            return Ok(());
        };
        let frame = guard.frames[idx]
            .as_mut()
            .expect("page_map invariant: frame must exist at mapped slot");
        frame.version_chains.clear();
        Ok(())
    }

    /// Drain and return every version chain currently attached to the
    /// 32 KB leaf frame for `page`. Returns an empty vector if the page
    /// is not resident.
    ///
    /// Used by the leaf-merge migration path to move tombstone-chain
    /// entries (whose cells were already removed earlier in the txn)
    /// onto the merged-into sibling so MVCC readers whose ReadView
    /// predates the delete still observe them.
    pub(crate) fn take_all_chains_on_page(
        &self,
        page: u32,
    ) -> Result<Vec<(Vec<u8>, Arc<VecDeque<VersionEntry>>)>> {
        let mut guard = self
            .inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let Some(&idx) = guard.page_map.get(&page) else {
            return Ok(Vec::new());
        };
        let frame = guard.frames[idx]
            .as_mut()
            .expect("page_map invariant: frame must exist at mapped slot");
        Ok(std::mem::take(&mut frame.version_chains)
            .into_iter()
            .collect())
    }

    /// True if no version chains are attached to leaf page `page` (including
    /// the case where the page is not currently resident).
    pub(crate) fn chains_empty(&self, page: u32) -> Result<bool> {
        let guard = self
            .inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let Some(&idx) = guard.page_map.get(&page) else {
            return Ok(true);
        };
        let frame = guard.frames[idx]
            .as_ref()
            .expect("page_map invariant: frame must exist at mapped slot");
        Ok(frame.version_chains.is_empty())
    }

    // -----------------------------------------------------------------------
    // Reconciliation (T6)
    // -----------------------------------------------------------------------

    /// Reconcile the per-key version chains on leaf page `page`.
    ///
    /// Walks every chain on the frame and drops entries whose `stop_ts`
    /// is `<= oldest_required_ts` — no live reader can see them, so they
    /// are pure garbage. A chain that collapses to a single head entry
    /// (`stop_ts == Ts::MAX`) is removed from the frame entirely: the
    /// dual-write invariant guarantees the on-disk cell already reflects
    /// that head, so the chain is redundant.
    ///
    /// `OverflowRef::Drop` RAII runs on every dropped `VersionEntry`. When
    /// a drop brings an overflow refcount to 0, the page is enqueued on
    /// `DeferredFreeQueue` (lock position 1.5 — a leaf mutex, safe to
    /// acquire transiently while holding the partition mutex at position 3).
    /// After releasing the partition mutex, the caller's writer-serialization
    /// context guarantees it is safe to drain the queue via
    /// `AllocatorHandle::drain_free_queue`.
    ///
    /// **Lock-order contract (T4 / T6):**
    /// 1. `ReadViewRegistry::oldest_required_ts()` is snapshotted *before*
    ///    acquiring the partition mutex. Position 5 is below positions 3/4
    ///    in the total order; re-acquiring it under the partition mutex is
    ///    forbidden.
    /// 2. The partition mutex is released before `drain_free_queue` is
    ///    invoked, so the allocator-state mutex (position 2) is never
    ///    nested under a partition mutex (positions 3/4).
    ///
    /// Returns the number of `VersionEntry` objects dropped.
    #[cfg(test)]
    pub(crate) fn reconcile(
        &self,
        page: u32,
        registry: &ReadViewRegistry,
        allocator: &AllocatorHandle,
    ) -> Result<usize> {
        // 1. Snapshot the horizon BEFORE any partition latch.
        let ort = registry.oldest_required_ts();

        // 2. Walk chains under the partition mutex. `Arc::make_mut` clones
        //    only if a snapshot reader still holds the previous Arc — the
        //    old chain keeps its pinned refcounts, the reader stays safe,
        //    and we mutate a fresh copy in-place.
        let dropped = {
            let mut guard = self
                .inner_32k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            let Some(&idx) = guard.page_map.get(&page) else {
                return Ok(0);
            };
            let frame = guard.frames[idx]
                .as_mut()
                .expect("page_map invariant: frame must exist at mapped slot");

            let mut dropped_count = 0usize;
            let keys: Vec<Vec<u8>> = frame.version_chains.keys().cloned().collect();

            for key in keys {
                let Some(chain_arc) = frame.version_chains.get_mut(&key) else {
                    continue;
                };
                let before = chain_arc.len();

                // Retain the live head (`stop_ts == Ts::MAX`) unconditionally
                // and any committed-replaced entry whose `stop_ts` is still
                // above the horizon (so some reader can still see it).
                // Entries with `stop_ts <= ort && stop_ts < Ts::MAX` are
                // invisible to every live reader and get dropped.
                let chain_mut = Arc::make_mut(chain_arc);
                chain_mut.retain(|e| e.stop_ts == Ts::MAX || e.stop_ts > ort);

                let after = chain_arc.len();
                dropped_count += before - after;

                // Collapse-if-head-only: the dual-write invariant means the
                // on-disk cell mirrors the head. A single entry with
                // stop_ts == Ts::MAX is therefore redundant.
                let collapse = chain_arc.len() == 1
                    && chain_arc
                        .front()
                        .map(|e| e.stop_ts == Ts::MAX && !e.is_tombstone)
                        .unwrap_or(false);
                if collapse {
                    frame.version_chains.remove(&key);
                } else if chain_arc.is_empty() {
                    // A chain whose only entry was a tombstone that has
                    // aged out also drops away.
                    frame.version_chains.remove(&key);
                }
            }

            dropped_count
        };

        // 3. Tick the reconcile counter and refresh the queue-depth gauge
        //    using the current queue size (drain below is authoritative).
        metrics::record_reconcile_entries_dropped(dropped as u64);
        metrics::set_deferred_free_queue_depth(
            allocator.deferred_free_queue().depth() as u64,
        );

        // 4. Writer-serialized drain — caller holds the writer lock. The
        //    drain re-checks refcount under Acquire before freeing.
        allocator.drain_free_queue(self.io.as_ref())?;

        Ok(dropped)
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Decrement pin count; propagate `dirty` flag.  Called from
    /// [`PinnedPage::drop`].
    fn unpin_internal(&self, page_number: u32, size: PageSize, dirty: bool) -> Result<()> {
        let lock = match size {
            PageSize::Small4k => &self.inner_4k,
            PageSize::Large32k => &self.inner_32k,
        };
        lock.lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?
            .unpin_page(page_number, dirty)
    }
}

// ---------------------------------------------------------------------------
// Recommended buffer pool sizes
// ---------------------------------------------------------------------------

/// Recommended buffer pool byte sizes for common deployment tiers.
#[allow(dead_code)]
pub(crate) mod default_sizes {
    /// IoT / edge devices: 4 MiB.
    pub const IOT: usize = 4 * 1024 * 1024;
    /// Desktop / CLI applications: 64 MiB (library default).
    pub const DESKTOP: usize = 64 * 1024 * 1024;
    /// Server deployments: 256 MiB.
    pub const SERVER: usize = 256 * 1024 * 1024;
    /// Dedicated history-store pool (plan §T7): 8 MiB default.
    pub const HISTORY: usize = 8 * 1024 * 1024;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex as StdMutex};

    // -----------------------------------------------------------------------
    // Mock I/O backend
    // -----------------------------------------------------------------------

    #[derive(Default)]
    struct MockIo {
        pages: StdMutex<HashMap<u32, Vec<u8>>>,
        write_count: StdMutex<u32>,
        read_count: StdMutex<u32>,
    }

    impl MockIo {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn seed(&self, page_number: u32, data: Vec<u8>) {
            self.pages.lock().unwrap().insert(page_number, data);
        }

        fn write_count(&self) -> u32 {
            *self.write_count.lock().unwrap()
        }

        fn read_count(&self) -> u32 {
            *self.read_count.lock().unwrap()
        }

        fn read_back(&self, page_number: u32) -> Option<Vec<u8>> {
            self.pages.lock().unwrap().get(&page_number).cloned()
        }
    }

    impl PageSource for MockIo {
        fn read_page(&self, page_number: u32, _size: PageSize, buf: &mut [u8]) -> Result<()> {
            *self.read_count.lock().unwrap() += 1;
            let pages = self.pages.lock().unwrap();
            if let Some(data) = pages.get(&page_number) {
                let copy_len = buf.len().min(data.len());
                buf[..copy_len].copy_from_slice(&data[..copy_len]);
                if copy_len < buf.len() {
                    buf[copy_len..].fill(0);
                }
            } else {
                // Fill with a deterministic pattern derived from page_number
                for (i, b) in buf.iter_mut().enumerate() {
                    *b = page_number.wrapping_add(i as u32) as u8;
                }
            }
            Ok(())
        }

        fn write_page(&self, page_number: u32, _size: PageSize, buf: &[u8]) -> Result<()> {
            *self.write_count.lock().unwrap() += 1;
            self.pages.lock().unwrap().insert(page_number, buf.to_vec());
            Ok(())
        }
    }

    // Newtype wrapper so Arc<MockIo> can be boxed as `Box<dyn PageSource>`.
    struct ArcIo(Arc<MockIo>);
    impl PageSource for ArcIo {
        fn read_page(&self, p: u32, s: PageSize, buf: &mut [u8]) -> Result<()> {
            self.0.read_page(p, s, buf)
        }
        fn write_page(&self, p: u32, s: PageSize, buf: &[u8]) -> Result<()> {
            self.0.write_page(p, s, buf)
        }
    }

    fn make_pool_with(size_bytes: usize, io: Arc<MockIo>) -> BufferPool {
        BufferPool::new(size_bytes, Box::new(ArcIo(io)))
    }

    fn desktop_pool(io: Arc<MockIo>) -> BufferPool {
        make_pool_with(default_sizes::DESKTOP, io)
    }

    // -----------------------------------------------------------------------
    // Pin / unpin basics
    // -----------------------------------------------------------------------

    #[test]
    fn pin_32k_reads_page_from_io() {
        let io = MockIo::new();
        let mut seed = vec![0u8; PageSize::Large32k.bytes()];
        seed[0] = 0xAB;
        seed[1] = 0xCD;
        io.seed(1, seed);

        let pool = desktop_pool(Arc::clone(&io));
        let page = pool.pin(1, PageSize::Large32k).unwrap();
        assert_eq!(page.data()[0], 0xAB);
        assert_eq!(page.data()[1], 0xCD);
        assert_eq!(io.read_count(), 1);
    }

    #[test]
    fn pin_4k_reads_page_from_io() {
        let io = MockIo::new();
        let mut seed = vec![0u8; PageSize::Small4k.bytes()];
        seed[100] = 0x42;
        io.seed(5, seed);

        let pool = desktop_pool(Arc::clone(&io));
        let page = pool.pin(5, PageSize::Small4k).unwrap();
        assert_eq!(page.data()[100], 0x42);
        assert_eq!(io.read_count(), 1);
    }

    #[test]
    fn cache_hit_does_not_re_read_from_disk() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let p1 = pool.pin(10, PageSize::Large32k).unwrap();
        drop(p1); // unpin

        let _p2 = pool.pin(10, PageSize::Large32k).unwrap();

        assert_eq!(io.read_count(), 1, "second pin must be a cache hit");
    }

    #[test]
    fn drop_decrements_pin_count_to_zero() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let page = pool.pin(7, PageSize::Large32k).unwrap();
        {
            let g = pool.inner_32k.lock().unwrap();
            assert_eq!(g.pin_count(7), Some(1));
        }
        drop(page);
        {
            let g = pool.inner_32k.lock().unwrap();
            assert_eq!(g.pin_count(7), Some(0));
        }
    }

    #[test]
    fn double_pin_increments_count_twice() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let _p1 = pool.pin(3, PageSize::Small4k).unwrap();
        let _p2 = pool.pin(3, PageSize::Small4k).unwrap();
        {
            let g = pool.inner_4k.lock().unwrap();
            assert_eq!(g.pin_count(3), Some(2));
        }
        drop(_p1);
        {
            let g = pool.inner_4k.lock().unwrap();
            assert_eq!(g.pin_count(3), Some(1));
        }
    }

    // -----------------------------------------------------------------------
    // Dirty flag and data mutation
    // -----------------------------------------------------------------------

    #[test]
    fn data_mut_marks_page_dirty() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut page = pool.pin(20, PageSize::Large32k).unwrap();
        page.data_mut()[0] = 0xFF;
        // Check dirty bit before dropping
        assert!(page.dirty, "data_mut must set dirty=true");
    }

    #[test]
    fn mark_dirty_sets_dirty_flag() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut page = pool.pin(21, PageSize::Large32k).unwrap();
        assert!(!page.dirty);
        page.mark_dirty();
        assert!(page.dirty);
    }

    #[test]
    fn unpin_without_mutation_leaves_page_clean() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let page = pool.pin(22, PageSize::Large32k).unwrap();
        drop(page);

        let g = pool.inner_32k.lock().unwrap();
        assert_eq!(g.is_dirty(22), Some(false));
    }

    #[test]
    fn unpin_with_dirty_propagates_to_frame() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut page = pool.pin(23, PageSize::Large32k).unwrap();
        page.mark_dirty();
        drop(page); // dirty=true propagated to frame

        let g = pool.inner_32k.lock().unwrap();
        assert_eq!(g.is_dirty(23), Some(true));
    }

    // -----------------------------------------------------------------------
    // Flush
    // -----------------------------------------------------------------------

    #[test]
    fn flush_writes_dirty_pages_to_io() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut page = pool.pin(30, PageSize::Large32k).unwrap();
        page.data_mut()[0] = 0xDE;
        drop(page);

        pool.flush().unwrap();

        assert_eq!(io.write_count(), 1, "flush must write the dirty page");
        let written = io.read_back(30).unwrap();
        assert_eq!(written[0], 0xDE);
    }

    #[test]
    fn flush_clears_dirty_bit() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut page = pool.pin(31, PageSize::Large32k).unwrap();
        page.mark_dirty();
        drop(page);

        pool.flush().unwrap();

        let g = pool.inner_32k.lock().unwrap();
        assert_eq!(g.is_dirty(31), Some(false));
    }

    #[test]
    fn flush_skips_clean_pages() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let page = pool.pin(32, PageSize::Large32k).unwrap();
        drop(page); // clean

        pool.flush().unwrap();

        assert_eq!(io.write_count(), 0, "flush must not write clean pages");
    }

    #[test]
    fn flush_writes_both_partitions() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut p4k = pool.pin(1, PageSize::Small4k).unwrap();
        p4k.mark_dirty();
        drop(p4k);

        let mut p32k = pool.pin(2, PageSize::Large32k).unwrap();
        p32k.mark_dirty();
        drop(p32k);

        pool.flush().unwrap();

        assert_eq!(
            io.write_count(),
            2,
            "flush must write one page from each partition"
        );
    }

    // -----------------------------------------------------------------------
    // CLOCK eviction
    // -----------------------------------------------------------------------

    #[test]
    fn eviction_writes_dirty_victim_before_loading_new_page() {
        // Create a pool with only 1 frame in the 32 KB partition
        let io = MockIo::new();
        let mut seed = vec![0u8; PageSize::Large32k.bytes()];
        seed[0] = 0xAA;
        io.seed(1, seed);

        // 32 KB pool with capacity=1 (size_32k = 32768 * 1 = 32768, but we
        // set total = 32768 * 1 so capacity_32k = (32768 * 0.75 / 32768) = 0 → max(0,1) = 1)
        let pool = make_pool_with(PageSize::Large32k.bytes(), Arc::clone(&io));

        // Pin page 1 and mutate it
        {
            let mut page = pool.pin(1, PageSize::Large32k).unwrap();
            page.data_mut()[0] = 0xBB; // dirty
        }
        // pin_count back to 0; dirty=true

        // Pin page 2 — triggers eviction of page 1 (dirty → written to disk)
        let _page2 = pool.pin(2, PageSize::Large32k).unwrap();

        assert!(
            io.write_count() >= 1,
            "dirty victim must be written before eviction"
        );
        let written = io.read_back(1).unwrap();
        assert_eq!(written[0], 0xBB);
    }

    #[test]
    fn clean_victim_evicted_without_write() {
        let io = MockIo::new();
        let pool = make_pool_with(PageSize::Large32k.bytes(), Arc::clone(&io));

        // Pin page 1 (clean), unpin
        let _p = pool.pin(1, PageSize::Large32k).unwrap();
        drop(_p);

        // Pin page 2 — evicts page 1 (clean, no write)
        let _p2 = pool.pin(2, PageSize::Large32k).unwrap();

        assert_eq!(io.write_count(), 0, "clean victim must not be written");
    }

    #[test]
    fn evicted_page_no_longer_cached() {
        let io = MockIo::new();
        let pool = make_pool_with(PageSize::Large32k.bytes(), Arc::clone(&io));

        let _p = pool.pin(1, PageSize::Large32k).unwrap();
        drop(_p);

        let _p2 = pool.pin(2, PageSize::Large32k).unwrap();

        let g = pool.inner_32k.lock().unwrap();
        assert!(!g.is_cached(1), "evicted page must not remain in the map");
        assert!(g.is_cached(2), "newly loaded page must be in the map");
    }

    #[test]
    fn clock_second_chance_defers_eviction() {
        // Pool with exactly 2 frames in the 32k partition.
        // total=3*32768=98304 → size_32k=73728 → capacity_32k=2
        let io = MockIo::new();
        let pool = make_pool_with(3 * PageSize::Large32k.bytes(), Arc::clone(&io));

        // Fill both slots
        let p1 = pool.pin(1, PageSize::Large32k).unwrap();
        let p2 = pool.pin(2, PageSize::Large32k).unwrap();
        drop(p1);
        drop(p2);

        {
            let g = pool.inner_32k.lock().unwrap();
            assert!(g.is_cached(1));
            assert!(g.is_cached(2));
        }

        // Adding a third page must evict exactly one existing page
        let _p3 = pool.pin(3, PageSize::Large32k).unwrap();
        {
            let g = pool.inner_32k.lock().unwrap();
            assert!(g.is_cached(3), "new page must be in pool");
            // Exactly one of the original pages was evicted
            let cached_count = [1u32, 2u32].iter().filter(|&&pn| g.is_cached(pn)).count();
            assert_eq!(cached_count, 1, "exactly one original page should remain");
        }
    }

    // -----------------------------------------------------------------------
    // All-pinned error (graceful, not panic)
    // -----------------------------------------------------------------------

    #[test]
    fn all_pinned_returns_error_not_panic() {
        let io = MockIo::new();
        // 1-frame pool
        let pool = make_pool_with(PageSize::Large32k.bytes(), Arc::clone(&io));

        let _p1 = pool.pin(1, PageSize::Large32k).unwrap();
        // p1 is still pinned (pin_count=1); the single frame is occupied.
        // Trying to pin a different page must fail gracefully.
        let result = pool.pin(2, PageSize::Large32k);
        assert!(
            result.is_err(),
            "pinning when all frames are occupied must return Err"
        );
        match result {
            Err(Error::Internal(msg)) => {
                assert!(
                    msg.contains("pinned"),
                    "error message should mention 'pinned'"
                );
            }
            Err(e) => panic!("expected Error::Internal, got: {e}"),
            Ok(_) => panic!("expected error but got Ok"),
        }
    }

    #[test]
    fn all_pinned_4k_returns_error() {
        let io = MockIo::new();
        let pool = make_pool_with(PageSize::Small4k.bytes(), Arc::clone(&io));

        let _p1 = pool.pin(1, PageSize::Small4k).unwrap();
        let result = pool.pin(2, PageSize::Small4k);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Two partitions are independent
    // -----------------------------------------------------------------------

    #[test]
    fn partitions_are_independent() {
        let io = MockIo::new();
        // 1-frame-per-partition pool.
        // total=2*4096=8192 → size_4k=2048 → cap_4k=max(0,1)=1
        //                   → size_32k=6144 → cap_32k=max(0,1)=1
        let pool = make_pool_with(2 * PageSize::Small4k.bytes(), Arc::clone(&io));

        // Fill 4k partition
        let _p4k = pool.pin(1, PageSize::Small4k).unwrap();
        // Fill 32k partition
        let _p32k = pool.pin(1, PageSize::Large32k).unwrap();

        // Pinning another 4k page fails (4k partition full, p4k still pinned)
        let r4k = pool.pin(2, PageSize::Small4k);
        assert!(r4k.is_err(), "4k partition should be full");

        // Pinning another 32k page fails too
        let r32k = pool.pin(2, PageSize::Large32k);
        assert!(r32k.is_err(), "32k partition should be full");
    }

    // -----------------------------------------------------------------------
    // Page data integrity
    // -----------------------------------------------------------------------

    #[test]
    fn mutated_data_persists_while_pinned() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut page = pool.pin(40, PageSize::Large32k).unwrap();
        page.data_mut()[500] = 0x77;
        assert_eq!(page.data()[500], 0x77);
    }

    #[test]
    fn re_pinned_page_retains_in_memory_modifications() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        {
            let mut p = pool.pin(50, PageSize::Large32k).unwrap();
            p.data_mut()[0] = 0x55;
        }
        // page is still in the pool (not evicted — large pool)
        let p2 = pool.pin(50, PageSize::Large32k).unwrap();
        assert_eq!(
            p2.data()[0],
            0x55,
            "in-memory modification must survive re-pin"
        );
    }

    // -----------------------------------------------------------------------
    // Capacity / frame counts
    // -----------------------------------------------------------------------

    #[test]
    fn iot_pool_has_at_least_one_frame_per_partition() {
        let io = MockIo::new();
        let pool = make_pool_with(default_sizes::IOT, Arc::clone(&io));

        // Must be able to pin at least one page of each size
        let _p4k = pool.pin(1, PageSize::Small4k).unwrap();
        let _p32k = pool.pin(1, PageSize::Large32k).unwrap();
    }

    #[test]
    fn desktop_pool_can_hold_many_pages() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        // 64 MB pool; 32k partition = ~1500 frames; pin 100 pages
        let handles: Vec<_> = (0..100)
            .map(|i| pool.pin(i, PageSize::Large32k).unwrap())
            .collect();

        assert_eq!(handles.len(), 100);
    }

    // -----------------------------------------------------------------------
    // page_number accessor
    // -----------------------------------------------------------------------

    #[test]
    fn pinned_page_number_accessor() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let page = pool.pin(99, PageSize::Large32k).unwrap();
        assert_eq!(page.page_number(), 99);
    }

    // -----------------------------------------------------------------------
    // Reconciliation (T6)
    // -----------------------------------------------------------------------

    mod reconcile {
        use super::*;
        use crate::mvcc::metrics;
        use crate::mvcc::read_view::{ReadView, ReadViewRegistry};
        use crate::mvcc::timestamp::Ts;
        use crate::mvcc::version::{OverflowRef, VersionData, VersionEntry};
        use crate::storage::allocator::AllocatorHandle;
        use crate::storage::header::FileHeader;

        fn ts(ms: u64) -> Ts {
            Ts {
                physical_ms: ms,
                logical: 0,
            }
        }

        fn fresh_allocator() -> AllocatorHandle {
            AllocatorHandle::new(FileHeader::new(0, 0, 0))
        }

        /// Allocator whose header reports enough pages to legally free
        /// high-numbered overflow pages (the free-list link write goes to
        /// the pool's MockIo which tolerates any page number).
        fn allocator_with_capacity(total_pages: u32) -> AllocatorHandle {
            let alloc = fresh_allocator();
            alloc
                .update_header(|h| h.total_page_count = total_pages)
                .unwrap();
            alloc
        }

        /// Build a fresh pool + allocator pair and pin leaf page `page`
        /// so a version chain can be attached to the resident frame.
        fn pool_with_resident_leaf(page: u32) -> (BufferPool, AllocatorHandle, Arc<MockIo>) {
            let io = MockIo::new();
            let pool = desktop_pool(Arc::clone(&io));
            // Force the frame resident (PinnedPage dropped immediately; the
            // frame stays in the pool because the pool is large).
            let _p = pool.pin(page, PageSize::Large32k).unwrap();
            drop(_p);
            let alloc = fresh_allocator();
            (pool, alloc, io)
        }

        fn install_chain(pool: &BufferPool, page: u32, key: &[u8], chain: VecDeque<VersionEntry>) {
            pool.put_chain(page, key.to_vec(), Arc::new(chain)).unwrap();
        }

        fn entry_inline(start: Ts, stop: Ts, txn: u64, payload: &[u8]) -> VersionEntry {
            VersionEntry {
                start_ts: start,
                stop_ts: stop,
                txn_id: txn,
                data: VersionData::Inline(payload.to_vec()),
                is_tombstone: false,
            }
        }

        fn tombstone(start: Ts, stop: Ts, txn: u64) -> VersionEntry {
            VersionEntry {
                start_ts: start,
                stop_ts: stop,
                txn_id: txn,
                data: VersionData::Inline(Vec::new()),
                is_tombstone: true,
            }
        }

        #[test]
        fn drops_entries_below_oldest_required_ts() {
            let (pool, alloc, _io) = pool_with_resident_leaf(1);
            let registry = ReadViewRegistry::new();
            // No live readers → ort = Ts::MAX. Retain rule: keep the live
            // head and anything with stop_ts > ort; drop the rest. A
            // 10-entry chain (1 head + 9 aged) collapses entirely because
            // the lone survivor (head, stop_ts == Ts::MAX) matches the
            // on-disk cell.
            let mut chain = VecDeque::new();
            // Head — most recent
            chain.push_back(entry_inline(ts(100), Ts::MAX, 1, b"head"));
            // Nine older entries with concrete stop_ts values
            for i in 0..9 {
                chain.push_back(entry_inline(
                    ts(10 + i),
                    ts(20 + i),
                    1 + i as u64,
                    format!("v{i}").as_bytes(),
                ));
            }
            install_chain(&pool, 1, b"K", chain);

            let dropped = pool.reconcile(1, &registry, &alloc).unwrap();
            assert_eq!(dropped, 9, "nine aged entries must drop");

            // Only the head (Ts::MAX) survived — and because it's the only
            // entry and non-tombstone, the chain was collapsed entirely.
            assert!(pool.chains_empty(1).unwrap());
        }

        #[test]
        fn retains_entries_needed_by_live_reader() {
            let (pool, alloc, _io) = pool_with_resident_leaf(2);
            let registry = Arc::new(ReadViewRegistry::new());
            // Reader pinned at ts=5 — any entry whose stop_ts > ts(5) must
            // survive.
            let _view = ReadView::open(Arc::clone(&registry), ts(5), 77);

            let mut chain = VecDeque::new();
            chain.push_back(entry_inline(ts(100), Ts::MAX, 1, b"head"));
            chain.push_back(entry_inline(ts(50), ts(100), 2, b"middle")); // stop_ts > 5 — keep
            chain.push_back(entry_inline(ts(1), ts(3), 3, b"gone")); // stop_ts < 5 — drop
            install_chain(&pool, 2, b"K", chain);

            metrics::reset_reconcile_entries_dropped();
            let dropped = pool.reconcile(2, &registry, &alloc).unwrap();
            assert_eq!(dropped, 1);

            // Chain survives because it has > 1 entry now.
            assert!(!pool.chains_empty(2).unwrap());
        }

        #[test]
        fn collapse_when_only_head_entry_remains() {
            let (pool, alloc, _io) = pool_with_resident_leaf(3);
            let registry = ReadViewRegistry::new();

            let mut chain = VecDeque::new();
            chain.push_back(entry_inline(ts(100), Ts::MAX, 1, b"head"));
            chain.push_back(entry_inline(ts(10), ts(20), 2, b"old"));
            install_chain(&pool, 3, b"K", chain);

            let dropped = pool.reconcile(3, &registry, &alloc).unwrap();
            assert_eq!(dropped, 1);
            // Single head collapsed.
            assert!(pool.chains_empty(3).unwrap());
        }

        #[test]
        fn no_collapse_when_head_is_tombstone() {
            let (pool, alloc, _io) = pool_with_resident_leaf(4);
            let registry = ReadViewRegistry::new();

            let mut chain = VecDeque::new();
            chain.push_back(tombstone(ts(100), Ts::MAX, 1));
            chain.push_back(entry_inline(ts(10), ts(20), 2, b"old"));
            install_chain(&pool, 4, b"K", chain);

            let dropped = pool.reconcile(4, &registry, &alloc).unwrap();
            assert_eq!(dropped, 1);
            // Tombstone-only chain still needed to override on-disk cell —
            // do not collapse.
            assert!(!pool.chains_empty(4).unwrap());
        }

        #[test]
        fn reconciles_multi_key_frame_independently() {
            let (pool, alloc, _io) = pool_with_resident_leaf(5);
            let registry = ReadViewRegistry::new();

            let mut c_a = VecDeque::new();
            c_a.push_back(entry_inline(ts(100), Ts::MAX, 1, b"A-head"));
            c_a.push_back(entry_inline(ts(10), ts(20), 2, b"A-old"));
            install_chain(&pool, 5, b"A", c_a);

            let mut c_b = VecDeque::new();
            c_b.push_back(entry_inline(ts(200), Ts::MAX, 3, b"B-head"));
            c_b.push_back(entry_inline(ts(30), ts(40), 4, b"B-old-1"));
            c_b.push_back(entry_inline(ts(50), ts(60), 5, b"B-old-2"));
            install_chain(&pool, 5, b"B", c_b);

            let dropped = pool.reconcile(5, &registry, &alloc).unwrap();
            // 1 + 2 = 3 older entries dropped; both chains collapse.
            assert_eq!(dropped, 3);
            assert!(pool.chains_empty(5).unwrap());
        }

        #[test]
        fn overflow_refs_drop_and_enqueue_when_no_readers() {
            let io = MockIo::new();
            let pool = desktop_pool(Arc::clone(&io));
            let _p = pool.pin(6, PageSize::Large32k).unwrap();
            drop(_p);
            // Allocator with enough "pages" on the header so `free_32k`
            // accepts page 777 — the MockIo underneath accepts any write.
            let alloc = allocator_with_capacity(1024);
            let registry = ReadViewRegistry::new();

            // Overflow-backed entry that will age out.
            let oref = OverflowRef::new_owned(777, 1024, alloc.clone()).unwrap();
            assert_eq!(alloc.overflow_refcount(777), 1);

            let mut chain = VecDeque::new();
            chain.push_back(entry_inline(ts(200), Ts::MAX, 10, b"head")); // live
            chain.push_back(VersionEntry {
                start_ts: ts(10),
                stop_ts: ts(20),
                txn_id: 11,
                data: VersionData::Overflow(oref),
                is_tombstone: false,
            });
            install_chain(&pool, 6, b"K", chain);
            assert_eq!(alloc.overflow_refcount(777), 1);

            // No live readers → ort = Ts::MAX → older entry drops, its
            // OverflowRef decrefs to 0, page 777 lands on the deferred-free
            // queue, and drain_free_queue releases it to the allocator's
            // free list.
            let depth_before = metrics::overflow_pages_freed_snapshot();
            let dropped = pool.reconcile(6, &registry, &alloc).unwrap();
            assert_eq!(dropped, 1);
            assert_eq!(alloc.overflow_refcount(777), 0);
            assert!(
                metrics::overflow_pages_freed_snapshot() > depth_before,
                "drain must record at least one freed page"
            );
            assert_eq!(
                alloc.deferred_free_queue().depth(),
                0,
                "queue drained"
            );
        }

        #[test]
        fn reconcile_non_resident_page_is_noop() {
            let io = MockIo::new();
            let pool = desktop_pool(Arc::clone(&io));
            let alloc = fresh_allocator();
            let registry = ReadViewRegistry::new();
            // Page 99 was never pinned — not resident.
            let dropped = pool.reconcile(99, &registry, &alloc).unwrap();
            assert_eq!(dropped, 0);
        }

        #[test]
        fn sec_index_tombstone_chain_retains_when_reader_active() {
            // Tombstone-only chain with a single tombstone entry whose
            // stop_ts = Ts::MAX (live). A reader at ts=50 must still see
            // the tombstone — reconcile must leave it in place.
            let (pool, alloc, _io) = pool_with_resident_leaf(7);
            let registry = Arc::new(ReadViewRegistry::new());
            let _view = ReadView::open(Arc::clone(&registry), ts(50), 500);

            let mut chain = VecDeque::new();
            chain.push_back(tombstone(ts(100), Ts::MAX, 1));
            install_chain(&pool, 7, b"sec-idx-key", chain);

            let dropped = pool.reconcile(7, &registry, &alloc).unwrap();
            assert_eq!(dropped, 0);
            assert!(!pool.chains_empty(7).unwrap());
        }
    }
}
