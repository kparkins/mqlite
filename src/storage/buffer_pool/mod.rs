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
//! Each partition is protected by a separate [`Mutex`].  The page image inside
//! [`PinnedPage`] is an atomically loaded snapshot:
//!
//! 1. CLOCK eviction skips frames whose `pin_count > 0`.
//! 2. Readers hold an `Arc` to the loaded image, so a concurrent writer publish
//!    cannot mutate the bytes they are reading.
//!
//! Callers must ensure that at most one [`PinnedPage`] for a given page number
//! calls [`data_mut`](PinnedPage::data_mut) at a time. The database-level
//! single-writer lock enforces this at a higher level.
//
// LOCK-ORDER (CRITICAL-1): this file owns positions **3** (32 KB
// partition mutex, `BufferPool::inner_32k`) and **4** (4 KB partition
// mutex, `BufferPool::inner_4k`) in the database-wide total order. Any
// path that acquires both partitions MUST acquire 32 KB before 4 KB, and
// must NOT re-enter the history-store partition (position 1),
// `PageLifetimeQueue::pending` (1.5), or `AllocatorHandle::state` (2)
// while holding either partition mutex. The canonical definition of the
// full order (positions 1 → 1.5 → 2 → 3 → 4 → 5 → 6) lives at the top of
// `src/mvcc/read_view.rs` — edit both blocks together or neither.
// The reconciliation path snapshots `ReadViewRegistry::oldest_required_ts()`
// (position 5) BEFORE acquiring a partition mutex.

mod chains;
mod partition;

use std::collections::{BTreeMap, VecDeque};
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use crate::error::{Error, Result};
use crate::mvcc::metrics;
use crate::mvcc::read_view::ReadViewRegistry;
use crate::mvcc::version::VersionEntry;
use crate::storage::allocator::AllocatorHandle;
use crate::storage::page::PAGE_TYPE_LEAF;
use crate::storage::reconcile::plan::TreeIdent;

use partition::Partition;

/// Default warning threshold for delta-bearing frame density.
pub(crate) const DELTA_BEARING_FRAMES_WARN_THRESHOLD_DEFAULT: f64 = 0.75;

// ---------------------------------------------------------------------------
// Main buffer-pool sharding
// ---------------------------------------------------------------------------

/// Number of independent main buffer pools in the engine.
///
/// A single main pool is used (two size-class partitions live *inside* that
/// pool); a dedicated history-store pool is separate and does not count here.
/// A second main pool would require a second lock-order position at level 3 / 4
/// — intentionally ruled out. Changes to this constant must be accompanied by a
/// lock-order audit; the compile-time assertion in
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
/// Reads use the page snapshot loaded when this guard was pinned. Writes copy
/// that snapshot into a private buffer, then publish the replacement image when
/// the guard is dropped.
pub(crate) struct PinnedPage<'pool> {
    pool: &'pool BufferPool,
    page_number: u32,
    page_size: PageSize,
    snapshot: Arc<Vec<u8>>,
    write_buf: Option<Vec<u8>>,
    dirty: bool,
}

impl<'pool> PinnedPage<'pool> {
    /// Read-only view of the page data.
    #[inline]
    pub(crate) fn data(&self) -> &[u8] {
        self.write_buf
            .as_deref()
            .unwrap_or_else(|| self.snapshot.as_slice())
    }

    /// Mutable view of the page data; marks the page dirty.
    #[inline]
    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        self.dirty = true;
        self.write_buf
            .get_or_insert_with(|| self.snapshot.as_ref().clone())
            .as_mut_slice()
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
        let data = self.write_buf.take();
        // Errors are intentionally swallowed — Drop must not panic.
        let _ = self
            .pool
            .unpin_internal(self.page_number, self.page_size, self.dirty, data);
    }
}

/// Delta chains retained on a folded leaf after reconciliation.
#[allow(dead_code)]
pub(crate) type RetainedLeafChains = BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>;

/// Point-in-time resident leaf image and chains for checkpoint reconcile.
#[allow(dead_code)]
pub(crate) struct ReconcileLeafSnapshot {
    /// Current base leaf page image.
    pub(crate) base_image: Vec<u8>,
    /// Current resident per-key version chains.
    pub(crate) chains: RetainedLeafChains,
}

/// Typed pin guard accepted by [`BufferPool::replace_leaf_and_chains`].
///
/// The guard is intentionally non-`Clone`, non-`Copy`, and non-`Send`.
#[allow(dead_code)]
pub(crate) struct PinnedLeafForReconcile<'pool> {
    pool: &'pool BufferPool,
    ident: TreeIdent,
    page_number: u32,
    _not_send: PhantomData<Rc<()>>,
}

impl PinnedLeafForReconcile<'_> {
    /// Return the pinned leaf page number.
    #[allow(dead_code)]
    pub(crate) fn page_number(&self) -> u32 {
        self.page_number
    }
}

impl std::fmt::Debug for PinnedLeafForReconcile<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedLeafForReconcile")
            .field("ident", &self.ident)
            .field("page_number", &self.page_number)
            .finish_non_exhaustive()
    }
}

impl Drop for PinnedLeafForReconcile<'_> {
    fn drop(&mut self) {
        let _ = self
            .pool
            .unpin_internal(self.page_number, PageSize::Large32k, false, None);
    }
}

/// Errors from guarded folded-leaf replacement.
#[allow(dead_code)]
pub(crate) enum ReplaceLeafError<'pool> {
    /// The target leaf frame is no longer resident.
    NotResident,
    /// The replacement image is not a 32 KB leaf page.
    NotLeaf,
    /// Another pin is active; the caller receives the guard back.
    FrameCoWRefused(PinnedLeafForReconcile<'pool>),
}

impl std::fmt::Debug for ReplaceLeafError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotResident => f.write_str("NotResident"),
            Self::NotLeaf => f.write_str("NotLeaf"),
            Self::FrameCoWRefused(guard) => f.debug_tuple("FrameCoWRefused").field(guard).finish(),
        }
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
    max_pool_bytes: usize,
    #[allow(dead_code)]
    total_pool_frames: usize,
    #[allow(dead_code)]
    delta_bearing_frames_warn_threshold: f64,
    #[allow(dead_code)]
    delta_bearing_frames_warn_above_threshold: AtomicBool,
}

/// Point-in-time buffer-pool occupancy metrics.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct BufferPoolOccupancySnapshot {
    /// Configured number of frames across both buffer-pool partitions.
    pub(crate) total_pool_frames: usize,
    /// Number of currently resident frames across both partitions.
    pub(crate) resident_frames: usize,
    /// Number of resident frames with at least one active pin.
    pub(crate) pinned_frames: usize,
    /// Number of resident frames carrying a live committed delta head.
    pub(crate) delta_bearing_frames_count: u64,
    /// Delta-bearing frame count divided by configured total frames.
    pub(crate) delta_bearing_frames_ratio: f64,
}

impl BufferPool {
    /// Create a new buffer pool backed by `io`.
    ///
    /// `buffer_pool_size` is the total byte budget.  Both partitions receive
    /// at least one frame even when the budget is very small.
    pub(crate) fn new(buffer_pool_size: usize, io: Box<dyn PageSource>) -> Self {
        Self::new_with_delta_bearing_frames_warn_threshold(
            buffer_pool_size,
            io,
            DELTA_BEARING_FRAMES_WARN_THRESHOLD_DEFAULT,
        )
    }

    /// Create a buffer pool with a custom delta-bearing warning threshold.
    ///
    /// # Panics
    ///
    /// This constructor assumes its caller already validated that `threshold`
    /// is in `(0.0, 1.0]`; invalid values trip a debug assertion.
    pub(crate) fn new_with_delta_bearing_frames_warn_threshold(
        buffer_pool_size: usize,
        io: Box<dyn PageSource>,
        threshold: f64,
    ) -> Self {
        debug_assert!(threshold > 0.0 && threshold <= 1.0);

        let size_4k = buffer_pool_size / 4;
        let size_32k = buffer_pool_size - size_4k;

        let capacity_4k = (size_4k / PageSize::Small4k.bytes()).max(1);
        let capacity_32k = (size_32k / PageSize::Large32k.bytes()).max(1);
        let total_pool_frames = capacity_4k + capacity_32k;

        Self {
            inner_4k: Mutex::new(Partition::new(capacity_4k, PageSize::Small4k.bytes())),
            inner_32k: Mutex::new(Partition::new(capacity_32k, PageSize::Large32k.bytes())),
            io,
            max_pool_bytes: buffer_pool_size,
            total_pool_frames,
            delta_bearing_frames_warn_threshold: threshold,
            delta_bearing_frames_warn_above_threshold: AtomicBool::new(false),
        }
    }

    /// Return the caller-configured byte budget for this pool.
    #[allow(dead_code)]
    pub(crate) fn max_pool_bytes(&self) -> usize {
        self.max_pool_bytes
    }

    /// Return a point-in-time occupancy snapshot and publish its metrics.
    ///
    /// # Errors
    ///
    /// Returns an error if either buffer-pool partition mutex is poisoned.
    #[allow(dead_code)]
    pub(crate) fn occupancy_snapshot(&self) -> Result<BufferPoolOccupancySnapshot> {
        let (resident_frames, pinned_frames, delta_bearing_frames_count) = {
            // Lock order is 32 KB (position 3) before 4 KB (position 4).
            let large = self
                .inner_32k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            let small = self
                .inner_4k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;

            let large_occupancy = large.occupancy_snapshot();
            let small_occupancy = small.occupancy_snapshot();
            (
                large_occupancy.resident_frames + small_occupancy.resident_frames,
                large_occupancy.pinned_frames + small_occupancy.pinned_frames,
                large_occupancy.delta_bearing_frames + small_occupancy.delta_bearing_frames,
            )
        };

        let delta_bearing_frames_count = delta_bearing_frames_count as u64;
        let delta_bearing_frames_ratio =
            delta_bearing_frames_count as f64 / self.total_pool_frames as f64;

        metrics::reset_delta_bearing_frames_count();
        for _ in 0..delta_bearing_frames_count {
            metrics::record_delta_bearing_frame();
        }
        metrics::set_delta_bearing_frames_ratio(delta_bearing_frames_ratio);

        let snapshot = BufferPoolOccupancySnapshot {
            total_pool_frames: self.total_pool_frames,
            resident_frames,
            pinned_frames,
            delta_bearing_frames_count,
            delta_bearing_frames_ratio,
        };
        self.warn_on_delta_bearing_threshold_crossing(&snapshot);
        Ok(snapshot)
    }

    #[allow(dead_code)]
    fn warn_on_delta_bearing_threshold_crossing(&self, snapshot: &BufferPoolOccupancySnapshot) {
        let above_threshold =
            snapshot.delta_bearing_frames_ratio >= self.delta_bearing_frames_warn_threshold;
        let was_above = self
            .delta_bearing_frames_warn_above_threshold
            .swap(above_threshold, Ordering::Relaxed);
        if above_threshold && !was_above {
            Self::emit_delta_bearing_frames_warn(snapshot);
        }
    }

    #[allow(dead_code)]
    fn emit_delta_bearing_frames_warn(snapshot: &BufferPoolOccupancySnapshot) {
        #[cfg(feature = "tracing")]
        tracing::warn!(
            target: "mqlite",
            delta_bearing_frames_count = snapshot.delta_bearing_frames_count,
            total_pool_frames = snapshot.total_pool_frames as u64,
            delta_bearing_frames_ratio = snapshot.delta_bearing_frames_ratio,
            "mqlite::delta_bearing_frames_threshold_crossed"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = snapshot;
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

        let snapshot = guard.data_snapshot(idx);

        Ok(PinnedPage {
            pool: self,
            page_number,
            page_size: size_enum,
            snapshot,
            write_buf: None,
            dirty: false,
        })
    }

    /// Pin a resident 32 KB leaf for a checkpoint reconcile install.
    ///
    /// This pin path does not perform I/O. A dirty-leaf reconcile pass works
    /// from resident frames and fails closed if the frame disappeared.
    #[allow(dead_code)]
    pub(crate) fn pin_leaf_for_reconcile(
        &self,
        ident: TreeIdent,
        page_number: u32,
    ) -> std::result::Result<PinnedLeafForReconcile<'_>, ReplaceLeafError<'_>> {
        let mut guard = self
            .inner_32k
            .lock()
            .map_err(|_| ReplaceLeafError::NotResident)?;
        let idx = guard
            .page_map
            .get(&page_number)
            .copied()
            .ok_or(ReplaceLeafError::NotResident)?;
        let frame = guard.frames[idx]
            .as_mut()
            .ok_or(ReplaceLeafError::NotResident)?;
        if frame.data.load().first().copied() != Some(PAGE_TYPE_LEAF) {
            return Err(ReplaceLeafError::NotLeaf);
        }
        frame.pin_count += 1;
        frame.ref_bit = true;

        Ok(PinnedLeafForReconcile {
            pool: self,
            ident,
            page_number,
            _not_send: PhantomData,
        })
    }

    /// Snapshot a resident 32 KB leaf page image and its current chains.
    ///
    /// Returns `Ok(None)` when the page is no longer resident. A non-leaf
    /// resident frame is an invariant violation for dirty-leaf reconciliation.
    #[allow(dead_code)]
    pub(crate) fn snapshot_leaf_for_reconcile(
        &self,
        page_number: u32,
    ) -> Result<Option<ReconcileLeafSnapshot>> {
        let guard = self
            .inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let Some(&idx) = guard.page_map.get(&page_number) else {
            return Ok(None);
        };
        let frame = guard.frames[idx].as_ref().ok_or_else(|| {
            Error::Internal("page_map invariant: frame must exist at mapped slot".into())
        })?;
        let base_image = frame.data.load_full().as_ref().clone();
        if base_image.first().copied() != Some(PAGE_TYPE_LEAF) {
            return Err(Error::Internal(
                "dirty-leaf reconcile target is not a leaf page".into(),
            ));
        }
        Ok(Some(ReconcileLeafSnapshot {
            base_image,
            chains: frame.deltas.clone(),
        }))
    }

    /// Atomically replace a resident leaf page image and retained chains.
    ///
    /// The 32 KB partition mutex is held across both the `Frame::data`
    /// publication and the `Frame::deltas` replacement. If any other pin is
    /// active when the mutex is held, the guard is returned so callers can
    /// restore chains before dropping the pin.
    #[allow(dead_code)]
    pub(crate) fn replace_leaf_and_chains<'guard>(
        &self,
        guard: PinnedLeafForReconcile<'guard>,
        new_base: Vec<u8>,
        retained_chains: RetainedLeafChains,
    ) -> std::result::Result<(), ReplaceLeafError<'guard>> {
        if !std::ptr::eq(self, guard.pool) {
            return Err(ReplaceLeafError::NotResident);
        }
        if new_base.len() != PageSize::Large32k.bytes()
            || new_base.first().copied() != Some(PAGE_TYPE_LEAF)
        {
            return Err(ReplaceLeafError::NotLeaf);
        }

        let mut retained_chains = retained_chains;
        let mut partition = self
            .inner_32k
            .lock()
            .map_err(|_| ReplaceLeafError::NotResident)?;
        let idx = partition
            .page_map
            .get(&guard.page_number)
            .copied()
            .ok_or(ReplaceLeafError::NotResident)?;
        let frame = partition.frames[idx]
            .as_mut()
            .ok_or(ReplaceLeafError::NotResident)?;

        if frame.pin_count > 1 {
            return Err(ReplaceLeafError::FrameCoWRefused(guard));
        }

        for chain in retained_chains.values_mut() {
            let _ = Arc::make_mut(chain);
        }
        frame.data.store(Arc::new(new_base));
        frame.deltas = retained_chains;
        frame.dirty = true;

        Ok(())
    }

    /// Pin `page_number` with chain reconciliation on the miss path.
    ///
    /// Identical to [`BufferPool::pin`] on a cache hit. On a miss, the
    /// chosen victim frame's version chains are pruned against the current
    /// `ReadViewRegistry` horizon BEFORE eviction, so aged entries never
    /// outlive the frame that hosts them. After the pin returns, the
    /// writer-serialized [`PageLifetimeQueue`] drain is invoked to reclaim
    /// overflow pages whose refcount reached zero as a side-effect of the
    /// prune.
    ///
    /// **Lock-order contract:**
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
        let (snapshot, dropped) = {
            let mut guard = lock
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            let mut blocked = 0usize;
            let (idx, dropped) = loop {
                match guard.pin_page_reconciling(page_number, ort, self.io.as_ref(), size_enum) {
                    Ok(result) => break result,
                    Err(Error::BufferPoolEvictionBlocked { page, reason }) => {
                        blocked += 1;
                        let _ = (page, reason);
                        #[cfg(feature = "tracing")]
                        tracing::trace!(
                            target: "mqlite",
                            page,
                            reason,
                            "mqlite::eviction_candidate_blocked"
                        );
                        if blocked >= guard.capacity {
                            return Err(Error::Internal(
                                "buffer pool exhausted: all frames are pinned or \
                                 delta-bearing; unpin unused pages, wait for \
                                 Phase 4 reconcile, or increase buffer_pool_size"
                                    .into(),
                            ));
                        }
                    }
                    Err(err) => return Err(err),
                }
            };
            (guard.data_snapshot(idx), dropped)
        };

        // 3. Tick counters + drain page-lifetime queue outside the latch.
        if dropped > 0 {
            metrics::record_reconcile_entries_dropped(dropped as u64);
        }
        metrics::set_deferred_free_queue_depth(allocator.page_lifetime_queue().depth() as u64);
        allocator.drain_free_queue(self.io.as_ref())?;

        Ok(PinnedPage {
            pool: self,
            page_number,
            page_size: size_enum,
            snapshot,
            write_buf: None,
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

    /// Invalidate the cached frame for `page_number`.
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
        let pin_count = guard.frames[idx].as_ref().map(|f| f.pin_count).unwrap_or(0);
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
    // Private helpers
    // -----------------------------------------------------------------------

    /// Decrement pin count; propagate `dirty` flag.  Called from
    /// [`PinnedPage::drop`].
    fn unpin_internal(
        &self,
        page_number: u32,
        size: PageSize,
        dirty: bool,
        data: Option<Vec<u8>>,
    ) -> Result<()> {
        let lock = match size {
            PageSize::Small4k => &self.inner_4k,
            PageSize::Large32k => &self.inner_32k,
        };
        lock.lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?
            .unpin_page(page_number, dirty, data)
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
    /// Dedicated history-store pool: 8 MiB default.
    pub const HISTORY: usize = 8 * 1024 * 1024;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod delta_order_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod us005_tests;
#[cfg(test)]
mod us006_tests;
#[cfg(test)]
mod us013_tests;
#[cfg(test)]
mod us014_tests;
#[cfg(test)]
mod us015_tests;
