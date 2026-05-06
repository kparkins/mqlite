//! Frame + Partition internals for the buffer pool.
//!
//! A [`Partition`] owns a fixed-size array of [`Frame`] slots that share a
//! single page size. CLOCK sweep eviction, pin/unpin, and reconciliation
//! walks all live here; the public [`BufferPool`](super::BufferPool) just
//! routes calls to the appropriate partition.

use std::collections::{BTreeMap, HashMap, VecDeque};
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

pub(super) struct Frame {
    pub(super) page_number: u32,
    /// Atomically published page bytes; length equals the partition's page size.
    ///
    /// Readers clone an `Arc` snapshot and copy from it without holding the
    /// partition mutex. Writers publish a fresh `Arc` on unpin, so readers never
    /// observe an in-place half-write of a B-tree page.
    pub(super) data: ArcSwap<Vec<u8>>,
    pub(super) pin_count: u32,
    pub(super) dirty: bool,
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
    /// Phase 5 §10.18 page-local latch. Acquired AFTER the partition mutex
    /// is released by `BufferPool::pin_for_read`/`pin_for_write` and held
    /// for the lifetime of the wrapping `LatchedPinnedPage`. The latch is
    /// scoped to a single resident `Frame`: cache hits reuse it across
    /// pin/unpin cycles, while a cache miss installs a fresh latch with
    /// the new page (§10.18 rule 1 — `PageLatch` is bound to the Frame).
    #[allow(dead_code)]
    pub(super) latch: PageLatch,
}

fn has_live_committed_head(frame: &Frame) -> bool {
    frame.deltas.values().any(|chain| {
        chain.iter().any(|entry| {
            !matches!(entry.state, VersionState::Pending { .. }) && entry.stop_ts == Ts::MAX
        })
    })
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(dead_code)]
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
    /// - exclusive `PageLatch` held → skipped without acquiring a latch guard.
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
                    if frame.latch.is_exclusively_held() {
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
    fn evict_frame(&mut self, idx: usize, io: &dyn PageSource, size: PageSize) -> Result<()> {
        if let Some(frame) = &self.frames[idx] {
            let was_dirty = frame.dirty;
            if was_dirty {
                let data = frame.data.load_full();
                io.write_page(frame.page_number, size, data.as_slice())?;
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
        let idx = self.find_victim().ok_or(Error::PoolExhausted {
            reason: PoolExhaustedReason::AllFramesPinned,
        })?;

        // Evict current occupant (if any)
        self.evict_frame(idx, io, size)?;

        // Load from disk
        let mut data = vec![0u8; self.page_size];
        io.read_page(page_number, size, &mut data)?;

        self.frames[idx] = Some(Frame {
            page_number,
            data: ArcSwap::from_pointee(data),
            pin_count: 1,
            dirty: false,
            ref_bit: true,
            deltas: BTreeMap::new(),
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

        let idx = self.find_victim().ok_or(Error::PoolExhausted {
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
        self.evict_frame(idx, io, size)?;

        // Load from disk
        let mut data = vec![0u8; self.page_size];
        io.read_page(page_number, size, &mut data)?;

        self.frames[idx] = Some(Frame {
            page_number,
            data: ArcSwap::from_pointee(data),
            pin_count: 1,
            dirty: false,
            ref_bit: true,
            deltas: BTreeMap::new(),
            latch: PageLatch::new(),
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
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(frame.deltas.len());
        keys.extend(frame.deltas.keys().cloned());
        for key in keys {
            let Some(chain_arc) = frame.deltas.get_mut(&key) else {
                continue;
            };
            let before = chain_arc.len();
            let chain_mut = Arc::make_mut(chain_arc);
            chain_mut.retain(|e| e.stop_ts == Ts::MAX || e.stop_ts > ort);
            let after = chain_arc.len();
            dropped += before - after;

            if chain_arc.is_empty() {
                frame.deltas.remove(&key);
            }
        }
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
            frame.dirty = true;
        }
        Ok(())
    }

    /// Write every dirty frame to disk and clear their dirty bits.
    pub(super) fn flush_all(&mut self, io: &dyn PageSource, size: PageSize) -> Result<()> {
        for slot in self.frames.iter_mut().flatten() {
            if slot.dirty {
                let data = slot.data.load_full();
                io.write_page(slot.page_number, size, data.as_slice())?;
                slot.dirty = false;
            }
        }
        Ok(())
    }

    /// Return copied snapshots of dirty resident frames in this partition.
    #[allow(
        dead_code,
        reason = "US-005 lands checkpoint-owned frame snapshots before the full driver consumes them"
    )]
    pub(super) fn dirty_frame_snapshots(&self, size: PageSize) -> Vec<(u32, PageSize, Vec<u8>)> {
        self.frames
            .iter()
            .flatten()
            .filter(|frame| frame.dirty)
            .map(|frame| {
                (
                    frame.page_number,
                    size,
                    frame.data.load_full().as_ref().clone(),
                )
            })
            .collect()
    }

    /// Return occupancy counts for resident frames in this partition.
    #[allow(dead_code)]
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

    /// Discard all dirty, unpinned frames without writing them to disk.
    ///
    /// Used by the WAL rollback path: frames written during an aborted
    /// transaction must be evicted so subsequent reads fetch clean data from
    /// the WAL/file rather than seeing partial writes.
    pub(super) fn drop_dirty_unpinned(&mut self) {
        for idx in 0..self.frames.len() {
            let page_number = match &self.frames[idx] {
                Some(frame) if frame.dirty && frame.pin_count == 0 => frame.page_number,
                _ => continue,
            };
            self.frames[idx] = None;
            self.page_map.remove(&page_number);
        }
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
        self.frames[idx].as_ref().map(|f| f.dirty)
    }

    #[cfg(test)]
    pub(super) fn is_cached(&self, page_number: u32) -> bool {
        self.page_map.contains_key(&page_number)
    }
}

#[cfg(test)]
#[path = "tests/partition_latch_eviction_tests.rs"]
mod partition_latch_eviction_tests;
