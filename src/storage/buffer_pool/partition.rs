//! Frame + Partition internals for the buffer pool.
//!
//! A [`Partition`] owns a fixed-size array of [`Frame`] slots that share a
//! single page size. CLOCK sweep eviction, pin/unpin, and reconciliation
//! walks all live here; the public [`BufferPool`](super::BufferPool) just
//! routes calls to the appropriate partition.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::VersionEntry;

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
    /// Per-frame MVCC version chains, keyed by B+ tree key. Migrates with
    /// the frame's cells on split / merge (see T3.5). Empty for non-leaf
    /// frames and for leaf frames written by the pre-MVCC writer path.
    pub(super) version_chains: HashMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>,
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
        let mut data = vec![0u8; self.page_size];
        io.read_page(page_number, size, &mut data)?;

        self.frames[idx] = Some(Frame {
            page_number,
            data: ArcSwap::from_pointee(data),
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
        let mut data = vec![0u8; self.page_size];
        io.read_page(page_number, size, &mut data)?;

        self.frames[idx] = Some(Frame {
            page_number,
            data: ArcSwap::from_pointee(data),
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
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(frame.version_chains.len());
        keys.extend(frame.version_chains.keys().cloned());
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
