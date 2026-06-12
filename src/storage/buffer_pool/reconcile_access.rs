//! Reconcile-access surface on [`BufferPool`]: the resident-leaf
//! snapshot/replace helpers and the resident-frame pin paths that feed
//! checkpoint reconciliation, plus the reconcile result/error types.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::marker::PhantomData;
use std::sync::{atomic::Ordering, Arc};

use crate::error::{Error, Result};
use crate::mvcc::version::VersionEntry;
use crate::storage::page::PAGE_TYPE_LEAF;
use crate::storage::reconcile::driver::TreeIdent;

use super::chains;
use super::latched_page::{LatchHold, LatchHoldRecorder, LatchedPinnedPage};
use super::page_latch::{LatchMode, PageLatch};
use super::partition::Frame;
use super::{BufferPool, PageSize};

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

impl BufferPool {
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
        // Leaf-budget running sum: full recompute since the entire
        // `deltas` map was replaced. This is the checkpoint dirty-leaf
        // reconcile path; it bypasses `with_all_chains` because it also
        // has to publish the new leaf-image bytes atomically with the
        // chain swap, so we maintain the cache inline here.
        let total = chains::frame_live_delta_payload_bytes(&frame.deltas);
        frame
            .live_delta_payload_bytes
            .store(total, Ordering::Release);
        frame.mark_unflushable_if_clean();

        Ok(())
    }

    /// Pin a resident 32 KiB frame and acquire its shared page latch.
    ///
    /// Unlike [`Self::pin_for_read`], this helper never performs I/O or
    /// installs a cache-miss victim. It is for metadata walks over currently
    /// resident frame-local chains where a miss simply means another thread
    /// evicted the frame before the walk reached it.
    pub(super) fn pin_resident_32k_for_read(
        &self,
        page_id: u32,
    ) -> Result<Option<LatchedPinnedPage<'_>>> {
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
}
