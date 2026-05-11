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

mod chains;
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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::marker::PhantomData;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};

use crate::error::{Error, PoolExhaustedReason, Result};
use crate::journal::log_file::PageId;
use crate::mvcc::metrics;
use crate::mvcc::read_view::{ChainSnapshot, ReadView, ReadViewRegistry};
use crate::mvcc::version::{VersionData, VersionEntry, VersionState};
use crate::mvcc::{ExpectedHead, Ts};
use crate::storage::allocator::AllocatorHandle;
use crate::storage::btree::reconcile::{
    CELL_INLINE_LEN_BYTES, CELL_KEY_LEN_BYTES, CELL_OVERFLOW_REF_BYTES, CELL_VALUE_TYPE_BYTES,
    SLOT_POINTER_BYTES,
};
use crate::storage::btree::OVERFLOW_THRESHOLD;
use crate::storage::page::{LEAF_HEADER_SIZE, PAGE_SIZE_LEAF, PAGE_TYPE_LEAF};
use crate::storage::reconcile::driver::TreeIdent;

use page_latch::{PageLatch, PageLatchExclusive, PageLatchShared};
pub(crate) use page_latch::LatchMode;
use partition::{Frame, Partition};

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

/// Errors from guarded folded-leaf replacement.
#[allow(dead_code)]
pub(crate) enum ReplaceLeafError {
    /// The target leaf frame is no longer resident.
    NotResident,
    /// The replacement image is not a 32 KB leaf page.
    NotLeaf,
}

impl std::fmt::Debug for ReplaceLeafError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotResident => f.write_str("NotResident"),
            Self::NotLeaf => f.write_str("NotLeaf"),
        }
    }
}

// ---------------------------------------------------------------------------
// LatchedPinnedPage — pin-plus-latch RAII handle
// ---------------------------------------------------------------------------

/// Internal latch hold for [`LatchedPinnedPage`]. The variant chosen on
/// construction matches the [`LatchMode`] requested by the caller; on drop
/// the embedded guard is released BEFORE the pin (§10.18 rule 2). The
/// guards are inhabited only for their `Drop` side effect (latch release);
/// the lint allow keeps the type-level wrapper expressive without a
/// compiler warning about the unread payload.
#[allow(dead_code)]
enum LatchHold<'pool> {
    Shared(PageLatchShared<'pool>),
    Exclusive(PageLatchExclusive<'pool>),
}

/// Wrapper around a [`LatchHold`] that ties the test-only
/// `EVENT_LATCH_RELEASE` event to the *actual* moment the underlying
/// `parking_lot` guard is dropped (§10.18 drop-order proof).
///
/// `Drop` first consumes `inner`, which runs the wrapped guard's
/// destructor and physically unlocks the `PageLatch`. Only after that
/// unlock has happened does the test probe record. A future refactor
/// that reordered drop versus recording would also reorder the
/// observable side effect: callers cannot mask a regression by moving
/// recording lines without also moving the actual unlock.
struct LatchHoldRecorder<'pool> {
    inner: Option<LatchHold<'pool>>,
}

impl<'pool> LatchHoldRecorder<'pool> {
    fn new(hold: LatchHold<'pool>) -> Self {
        Self { inner: Some(hold) }
    }
}

impl Drop for LatchHoldRecorder<'_> {
    fn drop(&mut self) {
        // Step 1 — physically drop the inner guard. parking_lot's
        // `RwLockReadGuard` / `RwLockWriteGuard` releases its lock at
        // this `drop` call.
        drop(self.inner.take());
        // Step 2 — record the latch-release event AFTER the unlock has
        // actually happened. In production the line is a no-op.
        #[cfg(test)]
        latched_pinned_page_drop_order::record_drop_event(
            latched_pinned_page_drop_order::EVENT_LATCH_RELEASE,
        );
    }
}

/// Pin-plus-latch RAII handle.
///
/// The sole legal way to hold both a buffer-pool pin and a `PageLatch`
/// simultaneously. Construction is via [`BufferPool::pin_for_read`] or
/// [`BufferPool::pin_for_write`]; the partition mutex is acquired, the
/// pin is bumped, the partition mutex is released, and only then is the
/// page-local latch acquired. Drop reverses that order: latch first,
/// pin second (§10.18 rule 2).
///
/// `LatchedPinnedPage` is `!Send` (the `_not_send: PhantomData<*const ()>`
/// marker rejects cross-thread transfer). The handle borrows from the
/// buffer pool and the `parking_lot` guard inside [`LatchHold`] is
/// thread-pinned by the underlying `parking_lot::RwLock`.
#[allow(dead_code)]
pub(crate) struct LatchedPinnedPage<'pool> {
    /// Buffer pool reference used by `Drop` to call back into
    /// `unpin_internal`. Tied to the same lifetime as the latch hold.
    pool: &'pool BufferPool,
    /// Frame pointer — stable while `pin_count > 0` because CLOCK
    /// eviction skips pinned frames and the partition slot vector is
    /// pre-allocated (no reallocation moves frames).
    frame_ptr: *const Frame,
    /// Page id (page number) wrapped by this handle.
    page_id: u32,
    /// Page-size partition that owns this frame (4 KiB or 32 KiB);
    /// recorded so `Drop` can re-enter the correct partition mutex
    /// when releasing the pin.
    page_size: PageSize,
    /// Mode in which the page-local latch is currently held.
    latch_mode: LatchMode,
    /// Live latch hold; taken (`None`) by `Drop` before the pin is
    /// released so the latch is dropped strictly first (§10.18 rule 2).
    /// Wrapped in [`LatchHoldRecorder`] so the test-only release event
    /// fires AFTER the underlying guard is physically unlocked.
    latch_hold: Option<LatchHoldRecorder<'pool>>,
    /// `*const ()` marker to make the handle `!Send` — page latches are
    /// thread-pinned in production (`parking_lot::RwLock` guards keep
    /// the acquiring thread on the lock owner list).
    _not_send: PhantomData<*const ()>,
}

impl<'pool> LatchedPinnedPage<'pool> {
    /// Page id (page number) this handle pins.
    #[allow(dead_code)]
    pub(crate) fn page_id(&self) -> u32 {
        self.page_id
    }

    /// Mode in which the page-local latch is currently held.
    #[allow(dead_code)]
    pub(crate) fn latch_mode(&self) -> LatchMode {
        self.latch_mode
    }

    /// Clone the current page-byte snapshot while this handle holds the
    /// page-local latch.
    #[allow(
        dead_code,
        reason = "public classifier uses this narrow latch read path"
    )]
    pub(crate) fn data_snapshot(&self) -> Arc<Vec<u8>> {
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted while the snapshot Arc is loaded.
        let frame = unsafe { &*self.frame_ptr };
        frame.data.load_full()
    }

    /// Copy resident delta chains while holding `LatchedPinnedPage::Shared`.
    ///
    /// This is a copies/clones only snapshot path: it never mutates the
    /// resident chain map and never acquires a buffer-pool partition mutex
    /// while the page latch is held.
    #[allow(dead_code)]
    pub(crate) fn snapshot_chains(&self, view: Option<Arc<ReadView>>) -> Result<ChainSnapshot> {
        if self.latch_mode != LatchMode::Shared {
            return Err(Error::Internal(
                "LatchedPinnedPage::snapshot_chains requires a shared page latch".into(),
            ));
        }
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The shared page latch prevents concurrent writers while
        // `ChainSnapshot::new` clones the map entries.
        let frame = unsafe { &*self.frame_ptr };
        Ok(ChainSnapshot::new(&frame.deltas, view))
    }

    /// Copy only the resident delta chain for `key` while holding the
    /// reader-side page latch.
    pub(crate) fn snapshot_chain_for_key(
        &self,
        key: &[u8],
        view: Option<Arc<ReadView>>,
    ) -> Result<ChainSnapshot> {
        if self.latch_mode != LatchMode::Shared {
            return Err(Error::Internal(
                "LatchedPinnedPage::snapshot_chain_for_key requires a shared page latch".into(),
            ));
        }
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The shared page latch prevents concurrent writers while
        // the single resident chain is cloned.
        let frame = unsafe { &*self.frame_ptr };
        Ok(ChainSnapshot::new_for_key(&frame.deltas, key, view))
    }

    /// Return the identity of the current live chain head for `key`.
    ///
    /// Aborted entries are ignored. Foreign pending entries still count as
    /// live heads for first-committer-wins checks.
    pub(crate) fn expected_head(&self, key: &[u8]) -> Option<ExpectedHead> {
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The page latch held by this handle serializes access to
        // the frame-local delta map for latch-aware callers.
        let frame = unsafe { &*self.frame_ptr };
        frame.deltas.get(key).and_then(|chain| {
            chain
                .iter()
                .find(|entry| {
                    entry.stop_ts == Ts::MAX && !matches!(entry.state, VersionState::Aborted)
                })
                .map(|entry| ExpectedHead {
                    commit_ts: entry.start_ts,
                    txn_id: entry.txn_id,
                })
        })
    }

    /// Return true when this page carries a pending entry for `txn_id`.
    pub(crate) fn has_pending_txn(&self, txn_id: u64) -> bool {
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The shared or exclusive page latch serializes access to
        // the frame-local delta map while this read walks the chains.
        let frame = unsafe { &*self.frame_ptr };
        frame.deltas.values().any(|chain| {
            chain.iter().any(
                |entry| matches!(entry.state, VersionState::Pending { txn_id: id } if id == txn_id),
            )
        })
    }

    /// Return the chain for `key`, or an empty caller-owned chain.
    pub(crate) fn get_or_create_chain(&self, key: &[u8]) -> Result<Arc<VecDeque<VersionEntry>>> {
        // This helper is used by exclusive install paths, even though it only
        // reads. Requiring exclusive keeps the install classifier and
        // mutation under one latch hold.
        self.require_exclusive("get_or_create_chain")?;
        // SAFETY: see `expected_head`.
        let frame = unsafe { &*self.frame_ptr };
        Ok(frame
            .deltas
            .get(key)
            .cloned()
            .unwrap_or_else(|| Arc::new(VecDeque::new())))
    }

    /// Return true when this leaf's resident delta map has a live key in range.
    pub(crate) fn has_live_delta_key_in_range(
        &self,
        start: &[u8],
        end: &[u8],
        exclude_key: &[u8],
    ) -> Result<bool> {
        self.require_exclusive("has_live_delta_key_in_range")?;
        // SAFETY: see `expected_head`; the exclusive page latch serializes
        // delta-map access for the install-time unique-prefix scan.
        let frame = unsafe { &*self.frame_ptr };
        for (key, chain) in frame.deltas.range(start.to_vec()..end.to_vec()) {
            if key.as_slice() == exclude_key {
                continue;
            }
            if chain.iter().any(|entry| {
                entry.stop_ts == Ts::MAX && !matches!(entry.state, VersionState::Aborted)
            }) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Install `chain` for `key`.
    pub(crate) fn put_chain(
        &mut self,
        key: Vec<u8>,
        chain: Arc<VecDeque<VersionEntry>>,
    ) -> Result<()> {
        self.require_exclusive("put_chain")?;
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The exclusive page latch serializes delta-map mutation.
        let frame = unsafe { &mut *self.frame_ptr.cast_mut() };
        frame.deltas.insert(key, chain);
        Ok(())
    }

    /// Read-modify-write the chain slot for `key` while holding this
    /// page's exclusive latch.
    ///
    /// The closure receives `&mut Option<Arc<...>>` — `None` when the
    /// frame currently has no chain for `key`. The closure may take,
    /// replace, or leave the slot. After it returns, the slot is written
    /// back into the frame's `deltas` map (insert if `Some`, leave
    /// removed if `None`).
    ///
    /// This is the canonical chain mutator surface that PR0.5 unifies on
    /// top of. The `pub(super)` `take_chain_locked` / `put_chain_locked`
    /// helpers in `chains.rs` exist only to back the legacy
    /// non-latch-aware free functions on `BufferPool`; once those are
    /// deleted in this PR's commit 3 every chain mutation flows through
    /// this method (or the all-chains variant).
    pub(crate) fn with_chain<R>(
        &mut self,
        key: &[u8],
        f: impl FnOnce(&mut Option<Arc<VecDeque<VersionEntry>>>) -> R,
    ) -> Result<R> {
        self.require_exclusive("with_chain")?;
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The exclusive page latch serializes delta-map mutation.
        let frame = unsafe { &mut *self.frame_ptr.cast_mut() };
        let mut slot = frame.deltas.remove(key);
        let result = f(&mut slot);
        if let Some(chain) = slot {
            frame.deltas.insert(key.to_vec(), chain);
        }
        Ok(result)
    }

    /// Read-modify-write the entire `deltas` map for this page while
    /// holding the exclusive latch.
    ///
    /// Used by the leaf-merge migration path to drain all chains and by
    /// the overflow-page repurpose path to clear inherited chains. The
    /// closure can mutate the map however it likes (insert / remove /
    /// drain).
    pub(crate) fn with_all_chains<R>(
        &mut self,
        f: impl FnOnce(&mut BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>) -> R,
    ) -> Result<R> {
        self.require_exclusive("with_all_chains")?;
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The exclusive page latch serializes delta-map mutation.
        let frame = unsafe { &mut *self.frame_ptr.cast_mut() };
        Ok(f(&mut frame.deltas))
    }

    /// Return true when live resident deltas no longer fit one folded leaf.
    pub(crate) fn live_delta_payload_exceeds_leaf_budget(&self) -> Result<bool> {
        self.require_exclusive("live_delta_payload_exceeds_leaf_budget")?;
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The exclusive page latch serializes delta-map access.
        let frame = unsafe { &*self.frame_ptr };
        let mut leaf_bytes = LEAF_HEADER_SIZE;
        for (key, chain) in &frame.deltas {
            let Some(entry) = chain.iter().find(|entry| {
                entry.stop_ts == Ts::MAX && !matches!(entry.state, VersionState::Aborted)
            }) else {
                continue;
            };
            if entry.is_tombstone {
                continue;
            }
            let value_bytes = match &entry.data {
                VersionData::Inline(bytes) if bytes.len() > OVERFLOW_THRESHOLD => {
                    CELL_OVERFLOW_REF_BYTES
                }
                VersionData::Inline(bytes) => CELL_INLINE_LEN_BYTES + bytes.len(),
                VersionData::Overflow(_) => CELL_OVERFLOW_REF_BYTES,
            };
            leaf_bytes += SLOT_POINTER_BYTES
                + CELL_KEY_LEN_BYTES
                + key.len()
                + CELL_VALUE_TYPE_BYTES
                + value_bytes;
            if leaf_bytes > PAGE_SIZE_LEAF as usize {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Flip every pending entry for `txn_id` on this page.
    pub(crate) fn flip_pending_for_txn(
        &mut self,
        txn_id: u64,
        commit_ts: Option<Ts>,
    ) -> Result<usize> {
        self.require_exclusive("flip_pending_for_txn")?;
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The exclusive page latch serializes delta-map mutation.
        let frame = unsafe { &mut *self.frame_ptr.cast_mut() };
        let mut flipped = 0usize;
        for chain_arc in frame.deltas.values_mut() {
            let chain = Arc::make_mut(chain_arc);
            for idx in 0..chain.len() {
                let pending_start_ts = match chain.get(idx) {
                    Some(entry)
                        if matches!(
                            entry.state,
                            VersionState::Pending { txn_id: pending } if pending == txn_id
                        ) =>
                    {
                        entry.start_ts
                    }
                    _ => continue,
                };

                let mut restore_after_abort = false;
                if let Some(entry) = chain.get_mut(idx) {
                    match commit_ts {
                        Some(ts) => {
                            entry.start_ts = ts;
                            entry.state = VersionState::Committed;
                        }
                        None => {
                            entry.state = VersionState::Aborted;
                            restore_after_abort = true;
                        }
                    }
                    flipped += 1;
                }
                if restore_after_abort {
                    restore_previous_head_after_abort(chain, idx, pending_start_ts);
                }
            }
        }
        Ok(flipped)
    }

    fn require_exclusive(&self, operation: &str) -> Result<()> {
        if self.latch_mode == LatchMode::Exclusive {
            return Ok(());
        }
        Err(Error::Internal(format!(
            "LatchedPinnedPage::{operation} requires an exclusive page latch"
        )))
    }
}

fn restore_previous_head_after_abort(
    chain: &mut VecDeque<VersionEntry>,
    aborted_idx: usize,
    aborted_start_ts: Ts,
) {
    if let Some(prev) = chain.iter_mut().skip(aborted_idx + 1).find(|entry| {
        !matches!(entry.state, VersionState::Aborted) && entry.stop_ts == aborted_start_ts
    }) {
        prev.stop_ts = Ts::MAX;
    }
}

impl Drop for LatchedPinnedPage<'_> {
    fn drop(&mut self) {
        // §10.18 rule 2 — release the latch BEFORE releasing the pin.
        // The recorder wrapper makes the latch-release event fire only
        // after the underlying parking_lot guard has unlocked, so
        // anybody who reorders this `drop(recorder)` line and the
        // `unpin_internal` call below will see the event order flip
        // too — the test asserts that order.
        debug_assert!(
            self.latch_hold.is_some(),
            "LatchedPinnedPage::drop: latch_hold must be Some on entry; \
             releasing the pin while the latch is still held would violate \
             §10.18 rule 2 (latch-before-pin drop order)",
        );
        let recorder = self.latch_hold.take();
        // Dropping `recorder` runs `LatchHoldRecorder::drop`, which
        // physically releases the latch THEN records the event.
        drop(recorder);
        debug_assert!(
            self.latch_hold.is_none(),
            "LatchedPinnedPage::drop: latch_hold must be released before \
             the pin (§10.18 rule 2)",
        );
        // Drop must not panic; swallow the unpin error like `PinnedPage`.
        let _ = self
            .pool
            .unpin_internal(self.page_id, self.page_size, false, None);
        // Pin-release event fires AFTER `unpin_internal` returns, so
        // the recorded order matches the actual side-effect order.
        #[cfg(test)]
        latched_pinned_page_drop_order::record_drop_event(
            latched_pinned_page_drop_order::EVENT_PIN_RELEASE,
        );
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

    /// Pin and exclusively latch one resident 32 KB leaf for checkpoint
    /// reconcile.
    ///
    /// This pin path does not perform I/O. A dirty-leaf reconcile pass works
    /// from resident frames and fails closed if the frame disappeared.
    #[allow(dead_code)]
    pub(crate) fn pin_leaf_for_reconcile(
        &self,
        ident: TreeIdent,
        page_number: u32,
    ) -> std::result::Result<LatchedPinnedPage<'_>, ReplaceLeafError> {
        let mut pages = self.pin_leaves_for_reconcile(ident, &[page_number])?;
        pages.pop().ok_or(ReplaceLeafError::NotResident)
    }

    /// Pin all planned reconcile leaf pages, then acquire exclusive latches.
    ///
    /// Reconciliation requires two-phase acquisition: all planned pages are
    /// pinned while holding the 32 KiB partition mutex, then that mutex is
    /// released, then `PageLatch::Exclusive` is acquired in the caller's
    /// ascending `page_id` order. If any planned page is unavailable, no
    /// partial acquisition is returned and any prior pins are released before
    /// the recoverable error is surfaced.
    pub(crate) fn pin_leaves_for_reconcile(
        &self,
        _ident: TreeIdent,
        planned_pages: &[u32],
    ) -> std::result::Result<Vec<LatchedPinnedPage<'_>>, ReplaceLeafError> {
        debug_assert!(
            planned_pages.windows(2).all(|pair| pair[0] < pair[1]),
            "reconcile planned page set must be sorted and unique"
        );

        let pinned = {
            let mut guard = self
                .inner_32k
                .lock()
                .map_err(|_| ReplaceLeafError::NotResident)?;
            let mut frame_indexes = Vec::with_capacity(planned_pages.len());
            for &page_number in planned_pages {
                let idx = guard
                    .page_map
                    .get(&page_number)
                    .copied()
                    .ok_or(ReplaceLeafError::NotResident)?;
                let frame = guard.frames[idx]
                    .as_ref()
                    .ok_or(ReplaceLeafError::NotResident)?;
                if frame.data.load().first().copied() != Some(PAGE_TYPE_LEAF) {
                    return Err(ReplaceLeafError::NotLeaf);
                }
                frame_indexes.push((page_number, idx));
            }

            let mut pinned = Vec::with_capacity(frame_indexes.len());
            for (page_number, idx) in frame_indexes {
                let frame = guard.frames[idx]
                    .as_mut()
                    .ok_or(ReplaceLeafError::NotResident)?;
                frame.pin_count += 1;
                frame.ref_bit = true;
                pinned.push((page_number, frame as *const Frame));
            }
            pinned
        };

        let mut latched = Vec::with_capacity(pinned.len());
        for (page_id, frame_ptr) in pinned {
            // SAFETY: each frame was pinned before the partition mutex was
            // released, so CLOCK eviction cannot remove the frame while this
            // handle is being constructed.
            let latch_ref: &PageLatch = unsafe { &(*frame_ptr).latch };
            latched.push(LatchedPinnedPage {
                pool: self,
                frame_ptr,
                page_id,
                page_size: PageSize::Large32k,
                latch_mode: LatchMode::Exclusive,
                latch_hold: Some(LatchHoldRecorder::new(LatchHold::Exclusive(
                    latch_ref.lock_exclusive(),
                ))),
                _not_send: PhantomData,
            });
        }

        Ok(latched)
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
    /// Requires the caller to pass a [`LatchedPinnedPage`] that holds
    /// `PageLatch::Exclusive` for the reconcile target. No partition mutex is
    /// acquired by this helper; the page-local latch is the resident chain
    /// mutation authority for checkpoint reconcile and CRUD writers.
    #[allow(dead_code)]
    pub(crate) fn replace_leaf_and_chains(
        &self,
        page: &mut LatchedPinnedPage<'_>,
        new_base: Vec<u8>,
        retained_chains: RetainedLeafChains,
    ) -> std::result::Result<(), ReplaceLeafError> {
        if !std::ptr::eq(self, page.pool) {
            return Err(ReplaceLeafError::NotResident);
        }
        page.require_exclusive("replace_leaf_and_chains")
            .map_err(|_| ReplaceLeafError::NotResident)?;
        if new_base.len() != PageSize::Large32k.bytes()
            || new_base.first().copied() != Some(PAGE_TYPE_LEAF)
        {
            return Err(ReplaceLeafError::NotLeaf);
        }

        let mut retained_chains = retained_chains;
        for chain in retained_chains.values_mut() {
            let _ = Arc::make_mut(chain);
        }
        // SAFETY: `page` owns a live pin, keeping the frame resident, and
        // `replace_leaf_and_chains` requires the exclusive page latch before
        // mutating resident page bytes or chains.
        let frame = unsafe { &mut *page.frame_ptr.cast_mut() };
        if frame.page_number != page.page_id {
            return Err(ReplaceLeafError::NotResident);
        }
        frame.data.store(Arc::new(new_base));
        frame.deltas = retained_chains;
        frame.mark_unflushable_if_clean();

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
        allocator.drain_free_queue(self.io.as_ref())?;

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
    /// **Lock-order contract (§10.18 — partition mutex and latch are never
    /// nested):**
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

    /// Pin a leaf page in exclusive latch mode, run `f` against its
    /// chain slot for `key`, and release the pin+latch.
    ///
    /// Canonical chain-slot mutator. Every production callsite that
    /// today reaches into `chains::take_chain` / `put_chain` should
    /// migrate to this entry point so per-page latch invariants hold.
    /// `mode` must be [`LatchMode::Exclusive`] — shared callers should
    /// use `pin_for_read_sized` + the snapshot APIs on
    /// [`LatchedPinnedPage`] instead. The mode parameter is preserved
    /// to mirror the trait signature on [`BTreePageStore`] but is
    /// validated runtime-side via `require_exclusive` inside
    /// [`LatchedPinnedPage::with_chain`].
    pub(crate) fn with_chain_under_latch<R>(
        &self,
        page: u32,
        key: &[u8],
        mode: LatchMode,
        f: impl FnOnce(&mut Option<Arc<VecDeque<VersionEntry>>>) -> R,
    ) -> Result<R> {
        let mut latched = self.pin_then_latch(page, PageSize::Large32k, mode)?;
        latched.with_chain(key, f)
    }

    /// Pin a leaf page in exclusive latch mode, run `f` against its
    /// entire chain map, and release the pin+latch.
    ///
    /// Companion to [`Self::with_chain_under_latch`] for callers that
    /// must drain or clear every chain on the page (leaf merge,
    /// overflow-page repurpose).
    pub(crate) fn with_all_chains_under_latch<R>(
        &self,
        page: u32,
        mode: LatchMode,
        f: impl FnOnce(&mut BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>) -> R,
    ) -> Result<R> {
        let mut latched = self.pin_then_latch(page, PageSize::Large32k, mode)?;
        latched.with_all_chains(f)
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
            LatchMode::Shared => LatchHold::Shared(latch_ref.lock_shared()),
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

    /// Pin a resident 32 KiB frame and acquire its shared page latch.
    ///
    /// Unlike [`Self::pin_for_read`], this helper never performs I/O or
    /// installs a cache-miss victim. It is for metadata walks over currently
    /// resident frame-local chains where a miss simply means another thread
    /// evicted the frame before the walk reached it.
    fn pin_resident_32k_for_read(&self, page_id: u32) -> Result<Option<LatchedPinnedPage<'_>>> {
        let frame_ptr: *const Frame = {
            let mut guard = self
                .inner_32k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            let Some(&idx) = guard.page_map.get(&page_id) else {
                return Ok(None);
            };
            let frame = guard.frames[idx].as_mut().ok_or_else(|| {
                Error::Internal("page_map invariant: frame must exist at mapped slot".into())
            })?;
            frame.pin_count += 1;
            frame.ref_bit = true;
            frame as *const Frame
        };

        // SAFETY: the frame was pinned before the partition mutex was
        // released, so CLOCK eviction cannot remove it while the latch is
        // acquired and wrapped.
        let latch_ref: &PageLatch = unsafe { &(*frame_ptr).latch };
        Ok(Some(LatchedPinnedPage {
            pool: self,
            frame_ptr,
            page_id,
            page_size: PageSize::Large32k,
            latch_mode: LatchMode::Shared,
            latch_hold: Some(LatchHoldRecorder::new(LatchHold::Shared(
                latch_ref.lock_shared(),
            ))),
            _not_send: PhantomData,
        }))
    }

    /// Return resident 32 KiB pages carrying a pending entry for `txn_id`.
    pub(crate) fn pages_with_pending_txn(&self, txn_id: u64) -> Result<Vec<u32>> {
        let resident_pages = {
            let guard = self
                .inner_32k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            guard
                .frames
                .iter()
                .filter_map(|frame| frame.as_ref().map(|frame| frame.page_number))
                .collect::<BTreeSet<_>>()
        };

        let mut pages = Vec::new();
        for page_id in resident_pages {
            let Some(page) = self.pin_resident_32k_for_read(page_id)? else {
                continue;
            };
            if page.has_pending_txn(txn_id) {
                pages.push(page_id);
            }
        }
        Ok(pages)
    }

    /// Write all dirty pages in both partitions to disk and clear dirty bits.
    ///
    /// Must be called before a WAL checkpoint or `Database::close` to ensure
    /// in-flight modifications reach stable storage.
    #[cfg(any(test, feature = "test-hooks"))]
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

    /// Write only dirty pages whose `last_lsn` is covered by `durable_lsn`.
    pub(crate) fn flush_lsn_fenced(&self, durable_lsn: u64) -> Result<()> {
        self.inner_4k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?
            .flush_all_lsn_fenced(self.io.as_ref(), PageSize::Small4k, durable_lsn)?;

        self.inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?
            .flush_all_lsn_fenced(self.io.as_ref(), PageSize::Large32k, durable_lsn)?;

        Ok(())
    }

    /// Return dirty resident page ids across both size partitions.
    #[allow(
        dead_code,
        reason = "flush-set validation exists before the full checkpoint driver consumes it"
    )]
    pub(crate) fn dirty_page_ids(&self) -> Result<BTreeSet<PageId>> {
        let mut pages = BTreeSet::new();
        {
            let guard = self
                .inner_32k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            for (page, _size, _data) in guard.dirty_frame_snapshots(PageSize::Large32k) {
                pages.insert(PageId(page));
            }
        }
        {
            let guard = self
                .inner_4k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            for (page, _size, _data) in guard.dirty_frame_snapshots(PageSize::Small4k) {
                pages.insert(PageId(page));
            }
        }
        Ok(pages)
    }

    /// Snapshot dirty resident frames for the requested page ids.
    pub(crate) fn dirty_frame_snapshots_for_pages(
        &self,
        pages: &BTreeSet<PageId>,
    ) -> Result<Vec<(u32, PageSize, Vec<u8>)>> {
        let mut frames = Vec::new();
        {
            let guard = self
                .inner_32k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            frames.extend(
                guard
                    .dirty_frame_snapshots(PageSize::Large32k)
                    .into_iter()
                    .filter(|(page, _, _)| pages.contains(&PageId(*page))),
            );
        }
        {
            let guard = self
                .inner_4k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            frames.extend(
                guard
                    .dirty_frame_snapshots(PageSize::Small4k)
                    .into_iter()
                    .filter(|(page, _, _)| pages.contains(&PageId(*page))),
            );
        }
        frames.sort_by_key(|(page, size, _data)| {
            let size_order = match size {
                PageSize::Small4k => 0u8,
                PageSize::Large32k => 1u8,
            };
            (*page, size_order)
        });
        Ok(frames)
    }

    /// Snapshot checkpoint-owned dirty frames without clearing dirty bits.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] if a dirty frame is neither owned by the
    /// checkpoint batch nor explicitly excluded as future-dirty residue.
    #[allow(
        dead_code,
        reason = "checkpoint-owned frame snapshots exist before the full driver consumes them"
    )]
    pub(crate) fn checkpoint_dirty_frame_snapshots(
        &self,
        owned_pages: &BTreeSet<PageId>,
        excluded_future_dirty_pages: &BTreeSet<PageId>,
        checkpoint_applied_lsn: u64,
    ) -> Result<Vec<(u32, PageSize, Vec<u8>)>> {
        let mut frames = Vec::new();
        {
            let guard = self
                .inner_32k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            frames.extend(
                guard.dirty_frame_snapshots_lsn_fenced(PageSize::Large32k, checkpoint_applied_lsn),
            );
        }
        {
            let guard = self
                .inner_4k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            frames.extend(
                guard.dirty_frame_snapshots_lsn_fenced(PageSize::Small4k, checkpoint_applied_lsn),
            );
        }

        let mut checkpoint_frames = Vec::new();
        for (page, size, data) in frames {
            let page_id = PageId(page);
            if owned_pages.contains(&page_id) {
                checkpoint_frames.push((page, size, data));
            } else if !excluded_future_dirty_pages.contains(&page_id) {
                return Err(Error::Internal(format!(
                    "checkpoint flush set rejected foreign dirty frame {page}"
                )));
            }
        }
        checkpoint_frames.sort_by_key(|(page, size, _data)| {
            let size_order = match size {
                PageSize::Small4k => 0u8,
                PageSize::Large32k => 1u8,
            };
            (*page, size_order)
        });
        Ok(checkpoint_frames)
    }

    /// Stamp resident dirty pages with the commit record end LSN.
    pub(crate) fn stamp_dirty_pages_lsn(&self, page_ids: &[u32], last_lsn: u64) -> Result<()> {
        let mut pages = page_ids.to_vec();
        pages.sort_unstable();
        pages.dedup();
        for page_id in pages {
            let size = self.detect_page_size(page_id);
            let lock = match size {
                PageSize::Small4k => &self.inner_4k,
                PageSize::Large32k => &self.inner_32k,
            };
            let guard = lock
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            let Some(&idx) = guard.page_map.get(&page_id) else {
                continue;
            };
            let frame = guard.frames[idx].as_ref().ok_or_else(|| {
                Error::Internal("page_map invariant: frame must exist at mapped slot".into())
            })?;
            frame.stamp_last_lsn(last_lsn);
        }
        Ok(())
    }

    /// Stamp every resident unflushable dirty frame with `last_lsn`.
    pub(crate) fn stamp_unflushable_dirty_lsn(&self, last_lsn: u64) -> Result<()> {
        self.inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?
            .stamp_unflushable_dirty_lsn(last_lsn);
        self.inner_4k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?
            .stamp_unflushable_dirty_lsn(last_lsn);
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
