//! Frame + Partition internals for the buffer pool.
//!
//! A [`Partition`] owns a fixed-size array of [`Frame`] slots that share a
//! single page size. CLOCK sweep eviction, pin/unpin, and reconciliation
//! walks all live here; the public [`BufferPool`](super::BufferPool) just
//! routes calls to the appropriate partition.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::error::{Error, PoolExhaustedReason, Result};
use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::{VersionEntry, VersionState};

use super::page_latch::PageLatch;
use super::{PageSize, PageSource};

// ---------------------------------------------------------------------------
// Frame (internal)
// ---------------------------------------------------------------------------

const PAGE_DIRTY_CLEAN: u64 = u64::MAX;
const PAGE_DIRTY_UNFLUSHABLE: u64 = u64::MAX - 1;
const MAX_PAGE_DIRTY_LSN: u64 = PAGE_DIRTY_UNFLUSHABLE - 1;

/// LSN fence for dirty page bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PageDirtyLsn {
    /// The resident page image has no unflushed dirty bytes.
    Clean,
    /// The page was dirtied before the covering commit `end_lsn` was known.
    Unflushable,
    /// The page may be written after the log is durable through `last_lsn`.
    Dirty {
        /// Exclusive end LSN of the newest commit represented by the page.
        last_lsn: u64,
    },
}

impl PageDirtyLsn {
    fn decode(raw: u64) -> Self {
        match raw {
            PAGE_DIRTY_CLEAN => Self::Clean,
            PAGE_DIRTY_UNFLUSHABLE => Self::Unflushable,
            last_lsn => Self::Dirty { last_lsn },
        }
    }

    fn encode(self) -> u64 {
        match self {
            Self::Clean => PAGE_DIRTY_CLEAN,
            Self::Unflushable => PAGE_DIRTY_UNFLUSHABLE,
            // Journal-less test handles use u64::MAX as an "already durable"
            // fence; keep the stored value out of the sentinel range.
            Self::Dirty { last_lsn } => last_lsn.min(MAX_PAGE_DIRTY_LSN),
        }
    }

    fn is_dirty(self) -> bool {
        !matches!(self, Self::Clean)
    }

    fn flushable_last_lsn(self, durable_lsn: u64) -> Option<u64> {
        match self {
            Self::Dirty { last_lsn } if last_lsn <= durable_lsn => Some(last_lsn),
            Self::Clean | Self::Unflushable | Self::Dirty { .. } => None,
        }
    }
}

pub(super) struct Frame {
    pub(super) page_number: u32,
    /// Atomically published page bytes; length equals the partition's page size.
    ///
    /// Readers clone an `Arc` snapshot and copy from it without holding the
    /// partition mutex. Writers publish a fresh `Arc` on unpin, so readers never
    /// observe an in-place half-write of a B-tree page.
    pub(super) data: ArcSwap<Vec<u8>>,
    pub(super) pin_count: u32,
    dirty_lsn: AtomicU64,
    pub(super) ref_bit: bool,
    /// Ordered per-key MVCC version chains keyed by B+ tree cell key bytes.
    /// Ordering is lexicographic on the raw key bytes — identical to the
    /// on-disk leaf cell ordering produced by `encode_key` /
    /// `encode_compound_key`.
    ///
    /// A chain is present when there is at least one staged or committed
    /// resident version for that key on this frame. A chain may exist without
    /// a matching base cell (delta-only key), and a base cell may exist
    /// without a matching chain. Both cases are legal; see Phase 3 §10.4 for
    /// the decision table.
    pub(super) deltas: BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>,
    /// PR2 running-sum cache: total `chain_live_head_bytes` over every
    /// chain in `deltas`. Maintained by every chain mutator that goes
    /// through `LatchedPinnedPage::with_chain` /
    /// `LatchedPinnedPage::with_all_chains` /
    /// `BufferPool::replace_leaf_and_chains` /
    /// `Partition::reconcile_frame_at`. Read by
    /// `LatchedPinnedPage::live_delta_payload_exceeds_leaf_budget`,
    /// which adds `LEAF_HEADER_SIZE` and compares to `PAGE_SIZE_LEAF`.
    ///
    /// **Lifecycle invariant:** every fresh `Frame` is constructed
    /// with `deltas: BTreeMap::new()` and this counter at 0. The
    /// invariant `cached == frame_live_delta_payload_bytes(&deltas)`
    /// holds across every mutator return — see
    /// `tests/running_sum_cache_invariant.rs` for the 10k-mutation
    /// stress proof.
    ///
    /// **Memory ordering:** writers update via `Acquire` load + arithmetic
    /// + `Release` store under the page-local exclusive latch (which
    /// already serializes mutator vs reader). The Acquire/Release pair
    /// is defense-in-depth: it makes the cache safe to read under a
    /// shared latch without a correctness migration if a future PR
    /// chooses to relax the read.
    pub(super) live_delta_payload_bytes: AtomicU64,
    /// Phase 5 §10.18 page-local latch. Acquired AFTER the partition mutex
    /// is released by `BufferPool::pin_for_read`/`pin_for_write` and held
    /// for the lifetime of the wrapping `LatchedPinnedPage`. The latch is
    /// scoped to a single resident `Frame`: cache hits reuse it across
    /// pin/unpin cycles, while a cache miss installs a fresh latch with
    /// the new page (§10.18 rule 1 — `PageLatch` is bound to the Frame).
    pub(super) latch: PageLatch,
}

impl Frame {
    pub(super) fn clean_dirty_lsn() -> AtomicU64 {
        AtomicU64::new(PAGE_DIRTY_CLEAN)
    }

    pub(super) fn dirty_lsn(&self) -> PageDirtyLsn {
        PageDirtyLsn::decode(self.dirty_lsn.load(Ordering::Acquire))
    }

    pub(super) fn is_dirty(&self) -> bool {
        self.dirty_lsn().is_dirty()
    }

    pub(super) fn can_flush_at(&self, durable_lsn: u64) -> bool {
        !self.is_dirty() || self.dirty_lsn().flushable_last_lsn(durable_lsn).is_some()
    }

    pub(super) fn flushable_last_lsn(&self, durable_lsn: u64) -> Option<u64> {
        self.dirty_lsn().flushable_last_lsn(durable_lsn)
    }

    pub(super) fn mark_unflushable(&self) {
        self.dirty_lsn
            .store(PageDirtyLsn::Unflushable.encode(), Ordering::Release);
    }

    pub(super) fn mark_unflushable_if_clean(&self) {
        let _ = self.dirty_lsn.compare_exchange(
            PageDirtyLsn::Clean.encode(),
            PageDirtyLsn::Unflushable.encode(),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(super) fn stamp_last_lsn(&self, last_lsn: u64) {
        let mut current = self.dirty_lsn.load(Ordering::Acquire);
        loop {
            let next = match PageDirtyLsn::decode(current) {
                PageDirtyLsn::Clean => return,
                PageDirtyLsn::Unflushable => PageDirtyLsn::Dirty { last_lsn }.encode(),
                PageDirtyLsn::Dirty {
                    last_lsn: current_lsn,
                } => {
                    if current_lsn >= last_lsn {
                        return;
                    }
                    PageDirtyLsn::Dirty { last_lsn }.encode()
                }
            };
            match self.dirty_lsn.compare_exchange(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    pub(super) fn stamp_unflushable_last_lsn(&self, last_lsn: u64) {
        let mut current = self.dirty_lsn.load(Ordering::Acquire);
        loop {
            match PageDirtyLsn::decode(current) {
                PageDirtyLsn::Unflushable => {}
                PageDirtyLsn::Clean | PageDirtyLsn::Dirty { .. } => return,
            }
            match self.dirty_lsn.compare_exchange(
                current,
                PageDirtyLsn::Dirty { last_lsn }.encode(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    pub(super) fn clear_dirty(&self) {
        self.dirty_lsn
            .store(PageDirtyLsn::Clean.encode(), Ordering::Release);
    }
}

fn has_live_committed_head(frame: &Frame) -> bool {
    frame.deltas.values().any(|chain| {
        chain.iter().any(|entry| {
            !matches!(entry.state, VersionState::Pending { .. }) && entry.stop_ts == Ts::MAX
        })
    })
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct PartitionOccupancySnapshot {
    pub(super) resident_frames: usize,
    pub(super) pinned_frames: usize,
    pub(super) delta_bearing_frames: usize,
}

// ---------------------------------------------------------------------------
// Partition (internal)
// ---------------------------------------------------------------------------

/// One pool partition; all frames share the same page size.
pub(super) struct Partition {
    /// Fixed-size slot array — pre-allocated, never reallocated.
    /// `None` denotes an empty slot.
    pub(super) frames: Vec<Option<Frame>>,
    /// page_number → slot index.
    pub(super) page_map: HashMap<u32, usize>,
    /// CLOCK sweep hand.
    pub(super) clock_hand: usize,
    pub(super) page_size: usize,
    pub(super) capacity: usize,
}

impl Partition {
    pub(super) fn new(capacity: usize, page_size: usize) -> Self {
        let capacity = capacity.max(1);
        let frames = std::iter::repeat_with(|| None).take(capacity).collect();
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
    /// - exclusive `PageLatch` held → skipped without acquiring a latch guard.
    /// - `ref_bit = 1` → cleared (second chance) and skipped.
    /// - `ref_bit = 0 && pin_count = 0` → victim.
    ///
    /// Scans at most `2 * capacity` frames (two full sweeps) before giving up.
    /// Returns `None` if all frames are pinned.
    fn find_victim(&mut self, durable_lsn: u64) -> Option<usize> {
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
                    if frame.latch.is_exclusively_held() {
                        continue;
                    }
                    if !frame.can_flush_at(durable_lsn) {
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
    /// partition mutex and before any page latch (see
    /// `BufferPool::reconcile`). Registry (position 5) is below the
    /// partition mutex / page-latch positions (3/3a/3b) in the total order,
    /// so re-acquiring it while holding those locks is forbidden.
    fn evict_frame(
        &mut self,
        idx: usize,
        io: &dyn PageSource,
        size: PageSize,
        durable_lsn: u64,
    ) -> Result<()> {
        if let Some(frame) = &self.frames[idx] {
            let was_dirty = frame.is_dirty();
            if let Some(_last_lsn) = frame.flushable_last_lsn(durable_lsn) {
                let data = frame.data.load_full();
                io.write_page(frame.page_number, size, data.as_slice())?;
                frame.clear_dirty();
            } else if was_dirty {
                return Err(Error::PoolExhausted {
                    reason: PoolExhaustedReason::AllFramesPinned,
                });
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
    pub(super) fn pin_page(
        &mut self,
        page_number: u32,
        io: &dyn PageSource,
        size: PageSize,
        durable_lsn: u64,
    ) -> Result<usize> {
        // Cache hit path
        if let Some(&idx) = self.page_map.get(&page_number) {
            let frame = self.frames[idx].as_mut().ok_or_else(|| {
                Error::Internal("page_map invariant: frame must exist at mapped slot".into())
            })?;
            frame.pin_count += 1;
            frame.ref_bit = true;
            return Ok(idx);
        }

        // Cache miss — find a victim
        let idx = self.find_victim(durable_lsn).ok_or(Error::PoolExhausted {
            reason: PoolExhaustedReason::AllFramesPinned,
        })?;

        // Evict current occupant (if any)
        self.evict_frame(idx, io, size, durable_lsn)?;

        // Load from disk
        let mut data = vec![0u8; self.page_size];
        io.read_page(page_number, size, &mut data)?;

        self.frames[idx] = Some(Frame {
            page_number,
            data: ArcSwap::from_pointee(data),
            pin_count: 1,
            dirty_lsn: Frame::clean_dirty_lsn(),
            ref_bit: true,
            deltas: BTreeMap::new(),
            // PR2 lifecycle invariant: empty `deltas` ↔ cache = 0.
            live_delta_payload_bytes: AtomicU64::new(0),
            latch: PageLatch::new(),
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
    pub(super) fn pin_page_reconciling(
        &mut self,
        page_number: u32,
        ort: Ts,
        io: &dyn PageSource,
        size: PageSize,
        durable_lsn: u64,
    ) -> Result<(usize, usize)> {
        // Cache hit — no victim, no reconciliation.
        if let Some(&idx) = self.page_map.get(&page_number) {
            let frame = self.frames[idx].as_mut().ok_or_else(|| {
                Error::Internal("page_map invariant: frame must exist at mapped slot".into())
            })?;
            frame.pin_count += 1;
            frame.ref_bit = true;
            return Ok((idx, 0));
        }

        let idx = self.find_victim(durable_lsn).ok_or(Error::PoolExhausted {
            reason: PoolExhaustedReason::AllFramesPinned,
        })?;

        if let Some(frame_ref) = self.frames[idx].as_ref() {
            if has_live_committed_head(frame_ref) {
                return Err(Error::BufferPoolEvictionBlocked {
                    page: frame_ref.page_number,
                    reason: "delta-bearing frame; Phase 4 reconcile not yet available",
                });
            }
        }

        // Prune the victim's chains against the snapshotted horizon before
        // it is evicted. Entries with `stop_ts <= ort && stop_ts < Ts::MAX`
        // are invisible to every live reader; retain only the live head
        // and committed-replaced entries above the horizon.
        let dropped = self.reconcile_frame_at(idx, ort);

        // Evict current occupant (if any)
        self.evict_frame(idx, io, size, durable_lsn)?;

        // Load from disk
        let mut data = vec![0u8; self.page_size];
        io.read_page(page_number, size, &mut data)?;

        self.frames[idx] = Some(Frame {
            page_number,
            data: ArcSwap::from_pointee(data),
            pin_count: 1,
            dirty_lsn: Frame::clean_dirty_lsn(),
            ref_bit: true,
            deltas: BTreeMap::new(),
            // PR2 lifecycle invariant: empty `deltas` ↔ cache = 0.
            live_delta_payload_bytes: AtomicU64::new(0),
            latch: PageLatch::new(),
        });
        self.page_map.insert(page_number, idx);

        Ok((idx, dropped))
    }

    /// Prune the frame at slot `idx`'s version chains against horizon `ort`.
    /// Returns the number of `VersionEntry` objects dropped. No-op if the
    /// slot is empty.
    ///
    /// PR2 cache invariant: this is the only `frame.deltas` mutator that
    /// bypasses the page-local exclusive latch (it runs under the
    /// partition mutex during eviction-prep). The running-sum cache is
    /// recomputed inline during the same retain pass so the function
    /// remains self-correcting — even though every current caller
    /// (`pin_page_reconciling`) immediately replaces the frame, a
    /// future caller that retains the frame would see a correct cache.
    fn reconcile_frame_at(&mut self, idx: usize, ort: Ts) -> usize {
        let Some(frame) = self.frames[idx].as_mut() else {
            return 0;
        };
        let mut dropped = 0usize;
        let mut new_payload_bytes = 0u64;
        frame.deltas.retain(|key, chain_arc| {
            let before = chain_arc.len();
            let chain_mut = Arc::make_mut(chain_arc);
            chain_mut.retain(|e| e.stop_ts == Ts::MAX || e.stop_ts > ort);
            let after = chain_arc.len();
            dropped += before - after;
            let keep = !chain_arc.is_empty();
            if keep {
                new_payload_bytes += super::chains::chain_live_head_bytes(key, chain_arc);
            }
            keep
        });
        frame
            .live_delta_payload_bytes
            .store(new_payload_bytes, Ordering::Release);
        dropped
    }

    /// Decrement `pin_count`; optionally publish a dirty page image.
    pub(super) fn unpin_page(
        &mut self,
        page_number: u32,
        dirty: bool,
        data: Option<Vec<u8>>,
    ) -> Result<()> {
        let idx = self.page_map.get(&page_number).copied().ok_or_else(|| {
            Error::Internal(format!(
                "buffer pool unpin: page {page_number} is not in the pool"
            ))
        })?;

        let frame = self.frames[idx].as_mut().ok_or_else(|| {
            Error::Internal("page_map invariant: frame must exist at mapped slot".into())
        })?;

        if frame.pin_count == 0 {
            return Err(Error::Internal(format!(
                "buffer pool unpin: page {page_number} pin_count is already 0"
            )));
        }
        frame.pin_count -= 1;
        if dirty {
            if let Some(data) = data {
                if data.len() != self.page_size {
                    return Err(Error::Internal(format!(
                        "buffer pool unpin: page {page_number} image has {} bytes, expected {}",
                        data.len(),
                        self.page_size
                    )));
                }
                frame.data.store(Arc::new(data));
            }
            frame.mark_unflushable();
        }
        Ok(())
    }

    /// Write every dirty frame to disk and clear their dirty bits.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(super) fn flush_all(&mut self, io: &dyn PageSource, size: PageSize) -> Result<()> {
        for slot in self.frames.iter_mut().flatten() {
            if slot.is_dirty() {
                let data = slot.data.load_full();
                io.write_page(slot.page_number, size, data.as_slice())?;
                slot.clear_dirty();
            }
        }
        Ok(())
    }

    /// Write dirty frames whose page LSN is covered by `durable_lsn`.
    pub(super) fn flush_all_lsn_fenced(
        &mut self,
        io: &dyn PageSource,
        size: PageSize,
        durable_lsn: u64,
    ) -> Result<()> {
        for slot in self.frames.iter_mut().flatten() {
            if slot.flushable_last_lsn(durable_lsn).is_some() {
                let data = slot.data.load_full();
                io.write_page(slot.page_number, size, data.as_slice())?;
                slot.clear_dirty();
            }
        }
        Ok(())
    }

    /// Return copied snapshots of dirty resident frames in this partition.
    pub(super) fn dirty_frame_snapshots(&self, size: PageSize) -> Vec<(u32, PageSize, Vec<u8>)> {
        self.frames
            .iter()
            .flatten()
            .filter(|frame| frame.is_dirty())
            .map(|frame| {
                (
                    frame.page_number,
                    size,
                    frame.data.load_full().as_ref().clone(),
                )
            })
            .collect()
    }

    /// Return dirty resident frame snapshots covered by `checkpoint_applied_lsn`.
    pub(super) fn dirty_frame_snapshots_lsn_fenced(
        &self,
        size: PageSize,
        checkpoint_applied_lsn: u64,
    ) -> Vec<(u32, PageSize, Vec<u8>)> {
        self.frames
            .iter()
            .flatten()
            .filter(|frame| frame.flushable_last_lsn(checkpoint_applied_lsn).is_some())
            .map(|frame| {
                (
                    frame.page_number,
                    size,
                    frame.data.load_full().as_ref().clone(),
                )
            })
            .collect()
    }

    /// Stamp all unflushable dirty frames in this partition with `last_lsn`.
    pub(super) fn stamp_unflushable_dirty_lsn(&self, last_lsn: u64) {
        for frame in self.frames.iter().flatten() {
            frame.stamp_unflushable_last_lsn(last_lsn);
        }
    }

    /// Return occupancy counts for resident frames in this partition.
    pub(super) fn occupancy_snapshot(&self) -> PartitionOccupancySnapshot {
        let mut snapshot = PartitionOccupancySnapshot::default();
        for frame in self.frames.iter().flatten() {
            snapshot.resident_frames += 1;
            if frame.pin_count > 0 {
                snapshot.pinned_frames += 1;
            }
            if has_live_committed_head(frame) {
                snapshot.delta_bearing_frames += 1;
            }
        }
        snapshot
    }

    /// Return an atomic snapshot of the frame's current page bytes.
    #[allow(clippy::expect_used)]
    pub(super) fn data_snapshot(&self, idx: usize) -> Arc<Vec<u8>> {
        let frame = self.frames[idx]
            .as_ref()
            .expect("data_snapshot: frame slot must be occupied");
        frame.data.load_full()
    }

    // -----------------------------------------------------------------------
    // Introspection helpers (tests only)
    // -----------------------------------------------------------------------

    #[cfg(test)]
    pub(super) fn pin_count(&self, page_number: u32) -> Option<u32> {
        let idx = *self.page_map.get(&page_number)?;
        self.frames[idx].as_ref().map(|f| f.pin_count)
    }

    #[cfg(test)]
    pub(super) fn is_dirty(&self, page_number: u32) -> Option<bool> {
        let idx = *self.page_map.get(&page_number)?;
        self.frames[idx].as_ref().map(Frame::is_dirty)
    }

    #[cfg(test)]
    pub(super) fn is_cached(&self, page_number: u32) -> bool {
        self.page_map.contains_key(&page_number)
    }
}

#[cfg(test)]
#[path = "tests/partition_latch_eviction.rs"]
mod partition_latch_eviction;
