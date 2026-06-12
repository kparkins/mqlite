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
//! ## Resident MVCC version chains
//!
//! A frame is not just a cached page image. Each 32 KB leaf frame also hosts
//! the in-memory per-key MVCC version chains for the keys on that page (the
//! `deltas` map of [`partition::Frame`]) — the live history of recent writes
//! that has not yet been folded back into the page's base bytes. This is the
//! buffer pool's most important responsibility: a key's currently visible
//! version may live only in the resident chain, never on disk, so the frame
//! is the authority for reader visibility, not the page image alone. Chain
//! mutation is serialized by the page-local latch ([`LatchedPinnedPage`]);
//! readers clone a [`ChainSnapshot`] under a shared latch.
//!
//! ## Eviction semantics — what blocks a victim and why
//!
//! CLOCK eviction may not silently discard a frame that still owns live
//! state, because doing so would lose committed data or expose readers to a
//! torn snapshot. A frame is therefore protected from eviction when:
//!
//! - **It is pinned** (`pin_count > 0`): an active guard is reading or writing
//!   it. CLOCK skips pinned frames outright.
//! - **It is dirty**: the modified bytes have not reached disk. A dirty victim
//!   is flushed first, and the flush is LSN-fenced — bytes are written only up
//!   to the durable log frontier, so eviction can never push a page ahead of
//!   the WAL that records the change.
//! - **It carries a live MVCC version chain**: a chain holding a committed
//!   head (still visible to some reader) or a `Pending` entry (a commit in its
//!   install→flip window) blocks eviction; dropping the frame would lose that
//!   version. Only dead aborted residue never blocks. The reconcile-aware pin
//!   path ([`BufferPool::pin_with_reconcile`]) first prunes each chain against
//!   the reader horizon (`ReadViewRegistry::oldest_required_ts`), so versions
//!   no live reader can observe are reclaimed before eviction; a frame that
//!   still holds above-horizon committed versions after the prune stays
//!   resident until the horizon advances.
//!
//! ## Leaf-budget running-sum cache
//!
//! Deciding whether a leaf's live deltas still fit in one folded 32 KB page
//! must be cheap because it is checked on the hot write path. Rather than
//! re-scan every chain on the frame, each frame keeps a running byte sum of
//! its live-delta payload (`live_delta_payload_bytes`), maintained
//! incrementally by every chain mutator that flows through the page latch. The
//! over-budget check ([`LatchedPinnedPage::live_delta_payload_exceeds_leaf_budget`])
//! is then an O(1) atomic load instead of a full-frame walk.
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
//! calls [`data_mut`](PinnedPage::data_mut) at a time. Higher-level write paths
//! use page latches and ordered publish protocols to enforce this.
//
// LOCK-ORDER (CRITICAL-1): this file owns positions **3** (32 KB
// partition mutex, `BufferPool::inner_32k`), **3a** (`PageLatch`), and
// **3b** (4 KB partition mutex, `BufferPool::inner_4k`) in the
// database-wide total order. Any path that acquires both partitions MUST
// acquire 32 KB before 4 KB. A partition mutex is used only to
// find/pin/unpin a frame and is released before acquiring `PageLatch`.
// Paths must NOT re-enter the history-store partition (position 1),
// `PageLifetimeQueue::pending` (1.5), or `AllocatorHandle::state` (2)
// while holding either partition mutex or a page latch. The canonical
// definition of the full order lives at the top of
// `src/mvcc/read_view.rs` — edit both blocks together or neither.
// The reconciliation path snapshots `ReadViewRegistry::oldest_required_ts()`
// (position 5) BEFORE acquiring a partition mutex or page latch.

pub(crate) mod chains;
#[cfg(feature = "perf-counters")]
mod metrics_perf;
#[cfg(feature = "perf-counters")]
pub use metrics_perf::{
    flip_retry_exhausted_count, flip_retry_rate, install_phase_b_mean_hold_ns,
    live_delta_check_mean_hold_ns, reset_flip_counters, reset_shared_latch_wait_hist,
    shared_latch_wait_p50_ns, shared_latch_wait_p99_ns,
};
mod flush;
mod latched_page;
// `PageLatch` is consumed by higher-level write, reconciliation, and test
// paths. Some primitive APIs still look unused to the lib-only dead-code lint
// despite being exercised by buffer-pool and integration coverage.
#[allow(dead_code)]
mod page_latch;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/page_latch_fairness_harness.rs"]
pub mod page_latch_fairness_harness;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/page_latch_upgrade_race.rs"]
pub mod page_latch_upgrade_race;
mod partition;
mod pinned_page;
mod reconcile_access;

use std::marker::PhantomData;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Mutex,
};

use crate::error::{Error, PoolExhaustedReason, Result};
use crate::mvcc::metrics;
use crate::mvcc::registry::ReadViewRegistry;
use crate::storage::allocator::AllocatorHandle;

pub(crate) use latched_page::{
    flip_pending_in_chain, LatchedPinnedPage, PreparedChainSwap, SwapOutcome,
};
use latched_page::{LatchHold, LatchHoldRecorder};
pub(crate) use page_latch::LatchMode;
use page_latch::PageLatch;
use partition::{Frame, Partition};
pub(crate) use pinned_page::PinnedPage;
#[allow(unused_imports)]
pub(crate) use reconcile_access::{ReconcileLeafSnapshot, ReplaceLeafError, RetainedLeafChains};

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
    main_file_flush_lsn: AtomicU64,
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
            main_file_flush_lsn: AtomicU64::new(u64::MAX),
        }
    }

    /// Return the caller-configured byte budget for this pool.
    #[allow(dead_code)]
    pub(crate) fn max_pool_bytes(&self) -> usize {
        self.max_pool_bytes
    }

    /// Update the durable log frontier that main-file writes may materialize.
    pub(crate) fn set_main_file_flush_lsn(&self, durable_lsn: u64) {
        self.main_file_flush_lsn
            .store(durable_lsn, Ordering::Release);
    }

    fn main_file_flush_lsn(&self) -> u64 {
        self.main_file_flush_lsn.load(Ordering::Acquire)
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
        let lock = match size {
            PageSize::Small4k => &self.inner_4k,
            PageSize::Large32k => &self.inner_32k,
        };

        let mut guard = lock
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;

        let idx = guard.pin_page(
            page_number,
            self.io.as_ref(),
            size,
            self.main_file_flush_lsn(),
        )?;

        let snapshot = guard.data_snapshot(idx);

        Ok(PinnedPage {
            pool: self,
            page_number,
            page_size: size,
            snapshot,
            write_buf: None,
            dirty: false,
        })
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
    ///    the partition mutex or page latch. Position 5 is below
    ///    positions 3/3a/3b in the total order.
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

        let lock = match size {
            PageSize::Small4k => &self.inner_4k,
            PageSize::Large32k => &self.inner_32k,
        };

        // 2. Pin + reconcile victim under the partition lock.
        let (snapshot, dropped) = {
            let mut guard = lock
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            let mut blocked = 0usize;
            let (idx, dropped) = loop {
                match guard.pin_page_reconciling(
                    page_number,
                    ort,
                    self.io.as_ref(),
                    size,
                    self.main_file_flush_lsn(),
                ) {
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
                            return Err(Error::PoolExhausted {
                                reason: PoolExhaustedReason::DeltaBearingFrames,
                            });
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
        // Hot-path drain: releases fence-eligible `OverflowDeferredFree`
        // entries only — a dropped tree's `RetiredTree*` entries are
        // checkpoint-owned (`advance_page_lifetime_checkpoint`) so this
        // path never scans them and never evaluates the reader floor.
        //
        // Free-list/buffer-pool coherence: `free_*` writes the freed
        // page's next-free link through the io given here, while
        // `allocate` reads it back through the pool. A freed overflow page
        // may still have a resident frame (chains are read through this
        // pool), so the link write must go through this pool too — writing
        // it through the raw backing `self.io` under a resident frame
        // would leave stale bytes to be popped as the next free-list head.
        // The local adapter pins on `self`, mirroring `BufferPoolPageSource`.
        struct PoolCoherentIo<'p>(&'p BufferPool);
        impl PageSource for PoolCoherentIo<'_> {
            fn read_page(&self, page_number: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
                let page = self.0.pin(page_number, size)?;
                buf.copy_from_slice(page.data());
                Ok(())
            }

            fn write_page(&self, page_number: u32, size: PageSize, buf: &[u8]) -> Result<()> {
                let mut page = self.0.pin(page_number, size)?;
                page.data_mut().copy_from_slice(buf);
                Ok(())
            }
        }
        allocator.drain_free_queue(&PoolCoherentIo(self))?;

        Ok(PinnedPage {
            pool: self,
            page_number,
            page_size: size,
            snapshot,
            write_buf: None,
            dirty: false,
        })
    }

    /// Pin `page_id` and acquire its page-local latch in **exclusive** mode.
    /// Returns the sole legal pin-plus-latch RAII handle.
    ///
    /// This defers to [`BufferPool::pin_then_latch`] with the page size
    /// resolved by [`BufferPool::detect_page_size`] (32 KiB by default for
    /// cache misses; resident pages route to whichever partition currently
    /// holds them). CRUD targets leaf pages (32 KiB), so the default is
    /// correct for the first-class call sites.
    ///
    /// **Lock-order contract (the partition mutex and a page latch are
    /// never held at the same time — the mutex is dropped before the latch
    /// is taken, so the two can never deadlock against each other):**
    /// 1. Acquire the partition mutex.
    /// 2. Bump `pin_count` (and load the page on a cache miss).
    /// 3. Capture a stable raw pointer to the resident frame.
    /// 4. **Release** the partition mutex.
    /// 5. Acquire the page-local latch in exclusive mode.
    ///
    /// # Errors
    ///
    /// - Partition mutex poisoned.
    /// - All frames in the partition are pinned (pool exhaustion).
    /// - I/O backend error during a cache-miss load.
    #[allow(dead_code)]
    pub(crate) fn pin_for_write(&self, page_id: u32) -> Result<LatchedPinnedPage<'_>> {
        let size = self.detect_page_size(page_id);
        self.pin_then_latch(page_id, size, LatchMode::Exclusive)
    }

    /// Pin `page_id` with an explicit page-size partition and acquire its
    /// page-local latch in exclusive mode.
    ///
    /// B-tree DDL cleanup already knows each page's allocator size from the
    /// tree traversal, so it uses this size-explicit path instead of the
    /// resident-frame heuristic in [`BufferPool::pin_for_write`].
    pub(crate) fn pin_for_write_sized(
        &self,
        page_id: u32,
        size: PageSize,
    ) -> Result<LatchedPinnedPage<'_>> {
        self.pin_then_latch(page_id, size, LatchMode::Exclusive)
    }

    /// Pin `page_id` and acquire its page-local latch in **shared** mode.
    /// Returns the sole legal pin-plus-latch RAII handle.
    ///
    /// The page size is resolved via [`BufferPool::detect_page_size`]. Same
    /// lock-order contract as [`BufferPool::pin_for_write`].
    ///
    /// # Errors
    ///
    /// - Partition mutex poisoned.
    /// - All frames in the partition are pinned (pool exhaustion).
    /// - I/O backend error during a cache-miss load.
    #[allow(dead_code)]
    pub(crate) fn pin_for_read(&self, page_id: u32) -> Result<LatchedPinnedPage<'_>> {
        let size = self.detect_page_size(page_id);
        self.pin_then_latch(page_id, size, LatchMode::Shared)
    }

    /// Pin `page_id` with an explicit page-size partition and acquire its
    /// page-local latch in shared mode.
    pub(crate) fn pin_for_read_sized(
        &self,
        page_id: u32,
        size: PageSize,
    ) -> Result<LatchedPinnedPage<'_>> {
        self.pin_then_latch(page_id, size, LatchMode::Shared)
    }

    /// Resolve the size partition for a page id by probing residency.
    ///
    /// The 32 KiB partition is checked first per the lock-order rule in
    /// `src/mvcc/read_view.rs` (position 3 before position 4); residency
    /// hits in either partition route to that partition. A cache miss
    /// defaults to 32 KiB, matching leaf-focused CRUD.
    fn detect_page_size(&self, page_id: u32) -> PageSize {
        if let Ok(g) = self.inner_32k.lock() {
            if g.page_map.contains_key(&page_id) {
                return PageSize::Large32k;
            }
        }
        if let Ok(g) = self.inner_4k.lock() {
            if g.page_map.contains_key(&page_id) {
                return PageSize::Small4k;
            }
        }
        PageSize::Large32k
    }

    /// Internal helper backing [`BufferPool::pin_for_read`] and
    /// [`BufferPool::pin_for_write`]. Tests use this entry point when
    /// they need explicit-size control (e.g., to exercise the 4 KiB
    /// partition).
    pub(super) fn pin_then_latch(
        &self,
        page_id: u32,
        size: PageSize,
        mode: LatchMode,
    ) -> Result<LatchedPinnedPage<'_>> {
        let lock = match size {
            PageSize::Small4k => &self.inner_4k,
            PageSize::Large32k => &self.inner_32k,
        };

        // Step 1-4: pin under the partition mutex, capture frame_ptr,
        // release the mutex.
        let frame_ptr: *const Frame = {
            let mut guard = lock
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            let idx =
                guard.pin_page(page_id, self.io.as_ref(), size, self.main_file_flush_lsn())?;
            let frame = guard.frames[idx].as_ref().ok_or_else(|| {
                Error::Internal("page_map invariant: frame must exist at mapped slot".into())
            })?;
            frame as *const Frame
        };

        // Step 5: acquire the latch with the partition mutex already
        // released. SAFETY: `pin_count > 0` keeps the frame slot
        // resident; the partition slot vector is pre-allocated and
        // never reallocated, so `frame_ptr` remains valid for the
        // lifetime of `self` while the pin is live. Reads through the
        // pointer touch only the latch, which itself provides interior
        // mutability through `parking_lot::RwLock`.
        let latch_ref: &PageLatch = unsafe { &(*frame_ptr).latch };
        let hold = match mode {
            LatchMode::Shared => {
                #[cfg(feature = "perf-counters")]
                let acquire_start = std::time::Instant::now();
                let h = LatchHold::Shared(latch_ref.lock_shared());
                #[cfg(feature = "perf-counters")]
                metrics_perf::record_shared_latch_wait_ns(acquire_start.elapsed().as_nanos() as u64);
                h
            }
            LatchMode::Exclusive => LatchHold::Exclusive(latch_ref.lock_exclusive()),
        };

        Ok(LatchedPinnedPage {
            pool: self,
            frame_ptr,
            page_id,
            page_size: size,
            latch_mode: mode,
            latch_hold: Some(LatchHoldRecorder::new(hold)),
            _not_send: PhantomData,
        })
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
#[path = "tests/delta_eviction_policy.rs"]
mod delta_eviction_policy;
#[cfg(test)]
#[path = "tests/delta_occupancy_metrics.rs"]
mod delta_occupancy_metrics;
#[cfg(test)]
#[path = "tests/delta_order.rs"]
mod delta_order;
#[cfg(test)]
#[path = "tests/dirty_frame_snapshot.rs"]
mod dirty_frame_snapshot;
#[cfg(test)]
#[path = "tests/eviction_bug_suspects.rs"]
mod eviction_bug_suspects;
#[cfg(test)]
#[path = "tests/bugsuspect_detect_page_size_misclassification.rs"]
mod bugsuspect_detect_page_size_misclassification;
#[cfg(test)]
#[path = "tests/latched_dirty_frame.rs"]
mod latched_dirty_frame;
#[cfg(test)]
#[path = "tests/latched_pinned_page.rs"]
mod latched_pinned_page;
#[cfg(test)]
#[path = "tests/latched_pinned_page_drop_order.rs"]
mod latched_pinned_page_drop_order;
#[cfg(test)]
#[path = "tests/reconcile_delta_preservation.rs"]
mod reconcile_delta_preservation;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/resident_chain_snapshot.rs"]
mod resident_chain_snapshot;
#[cfg(test)]
mod tests;
