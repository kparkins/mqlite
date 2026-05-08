//! MVCC per-frame delta-chain helpers on [`BufferPool`].
//!
//! The per-key delta chains live on the 32 KB leaf partition's frames.
//! This module extends [`BufferPool`] with the take / put / snapshot /
//! clear / drain helpers used by legacy single-writer and structural paths.
//! Phase 5 reconcile and CRUD callers that already hold page latches must use
//! [`super::LatchedPinnedPage`] helpers instead: resident chain mutation
//! requires `PageLatch::Exclusive`, while snapshots require
//! `LatchedPinnedPage::Shared` and copy/clone only.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::read_view::{ChainSnapshot, ReadView};
use crate::mvcc::version::VersionEntry;

#[cfg(test)]
use crate::mvcc::metrics;
#[cfg(test)]
use crate::mvcc::read_view::ReadViewRegistry;
#[cfg(test)]
use crate::mvcc::timestamp::Ts;
#[cfg(test)]
use crate::storage::allocator::AllocatorHandle;
#[cfg(test)]
use crate::storage::reconcile::plan::{TreeIdent, TreeKind};

use super::{BufferPool, PageSize};
#[cfg(test)]
use super::{ReplaceLeafError, RetainedLeafChains};

type VersionChainDrain = Vec<(Vec<u8>, Arc<VecDeque<VersionEntry>>)>;

#[cfg(test)]
const RECONCILE_COMPAT_COLLECTION_ID: i64 = 0;

impl BufferPool {
    // -----------------------------------------------------------------------
    // MVCC delta-chain helpers (T3.5)
    //
    // Chains are stored on the 32 KB partition's frames (leaf pages). The
    // caller is responsible for having pinned the page (via `read_leaf` or
    // `write_leaf_structural`) recently enough that the frame is still resident — the
    // MVCC writer lane sequences these calls synchronously after a leaf
    // read / write, so the frame has not yet been eligible for eviction.
    // -----------------------------------------------------------------------

    /// Remove and return the delta chain for `key` on leaf page `page`.
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
        let frame = guard.frames[idx].as_mut().ok_or_else(|| {
            Error::Internal("page_map invariant: frame must exist at mapped slot".into())
        })?;
        Ok(frame.deltas.remove(key))
    }

    /// Install a delta chain for `key` on leaf page `page`.
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
        let frame = guard.frames[idx].as_mut().ok_or_else(|| {
            Error::Internal("page_map invariant: frame must exist at mapped slot".into())
        })?;
        frame.deltas.insert(key, chain);
        Ok(())
    }

    /// Build a [`ChainSnapshot`] from the per-key MVCC delta chains on
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
    #[allow(dead_code)]
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
        let frame = guard.frames[idx].as_ref().ok_or_else(|| {
            Error::Internal("page_map invariant: frame must exist at mapped slot".into())
        })?;
        Ok(Some(ChainSnapshot::new(&frame.deltas, view)))
    }

    /// Clear all delta chains attached to the resident frame for `page`
    /// in the partition selected by `size`.
    ///
    /// Used by the overflow-chain free path: overflow pages share the
    /// 32 KB leaf partition with data leaves, so a page reborn as an
    /// overflow page may inherit stale `deltas` entries from
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
        let frame = guard.frames[idx].as_mut().ok_or_else(|| {
            Error::Internal("page_map invariant: frame must exist at mapped slot".into())
        })?;
        frame.deltas.clear();
        Ok(())
    }

    /// Drain and return every delta chain currently attached to the
    /// 32 KB leaf frame for `page`. Returns an empty vector if the page
    /// is not resident.
    ///
    /// Used by the leaf-merge migration path to move tombstone-chain
    /// entries (whose cells were already removed earlier in the txn)
    /// onto the merged-into sibling so MVCC readers whose ReadView
    /// predates the delete still observe them.
    pub(crate) fn take_all_chains_on_page(&self, page: u32) -> Result<VersionChainDrain> {
        let mut guard = self
            .inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let Some(&idx) = guard.page_map.get(&page) else {
            return Ok(Vec::new());
        };
        let frame = guard.frames[idx].as_mut().ok_or_else(|| {
            Error::Internal("page_map invariant: frame must exist at mapped slot".into())
        })?;
        Ok(std::mem::take(&mut frame.deltas).into_iter().collect())
    }

    /// True if no delta chains are attached to leaf page `page` (including
    /// the case where the page is not currently resident).
    pub(crate) fn chains_empty(&self, page: u32) -> Result<bool> {
        let guard = self
            .inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let Some(&idx) = guard.page_map.get(&page) else {
            return Ok(true);
        };
        let frame = guard.frames[idx].as_ref().ok_or_else(|| {
            Error::Internal("page_map invariant: frame must exist at mapped slot".into())
        })?;
        Ok(frame.deltas.is_empty())
    }

    // -----------------------------------------------------------------------
    // Reconciliation (T6)
    // -----------------------------------------------------------------------

    /// Reconcile the per-key delta chains on leaf page `page`.
    ///
    /// Walks every chain on the frame and drops entries whose `stop_ts`
    /// is `<= oldest_required_ts` — no live reader can see them, so they
    /// are pure garbage. Phase 3 never collapses a live non-tombstone head;
    /// that value-equivalence rule returns with Phase 4 reconcile.
    ///
    /// `OverflowRef::Drop` RAII runs on every dropped `VersionEntry`. When
    /// a drop brings an overflow refcount to 0, the page is enqueued on
    /// `PageLifetimeQueue` (lock position 1.5 — a leaf mutex, safe to
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

        // 2. Snapshot retained chains and the current page image under the
        //    partition mutex. The actual install goes through the Phase 4
        //    guarded replacement primitive below.
        let Some((new_base, retained_chains, dropped)) = ({
            let guard = self
                .inner_32k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            let Some(&idx) = guard.page_map.get(&page) else {
                return Ok(0);
            };
            let frame = guard.frames[idx].as_ref().ok_or_else(|| {
                Error::Internal("page_map invariant: frame must exist at mapped slot".into())
            })?;

            let mut dropped_count = 0usize;
            let mut retained_chains = RetainedLeafChains::new();

            for (key, chain_arc) in &frame.deltas {
                let before = chain_arc.len();
                let retained: VecDeque<VersionEntry> = chain_arc
                    .iter()
                    .filter(|entry| entry.stop_ts == Ts::MAX || entry.stop_ts > ort)
                    .cloned()
                    .collect();
                dropped_count += before - retained.len();

                if !retained.is_empty() {
                    retained_chains.insert(key.clone(), Arc::new(retained));
                }
            }

            Some((
                frame.data.load_full().as_ref().clone(),
                retained_chains,
                dropped_count,
            ))
        }) else {
            return Ok(0);
        };

        if dropped > 0 {
            let pin = self
                .pin_leaf_for_reconcile(
                    TreeIdent {
                        collection_id: RECONCILE_COMPAT_COLLECTION_ID,
                        kind: TreeKind::Primary,
                    },
                    page,
                )
                .map_err(|err| match err {
                    ReplaceLeafError::NotResident => {
                        Error::Internal("buffer pool reconcile: resident frame disappeared".into())
                    }
                    ReplaceLeafError::NotLeaf => {
                        Error::Internal("buffer pool reconcile: target frame is not a leaf".into())
                    }
                })?;
            let mut pin = pin;
            self.replace_leaf_and_chains(&mut pin, new_base, retained_chains)
                .map_err(|err| match err {
                    ReplaceLeafError::NotResident => {
                        Error::Internal("buffer pool reconcile: resident frame disappeared".into())
                    }
                    ReplaceLeafError::NotLeaf => Error::Internal(
                        "buffer pool reconcile: replacement frame is not a leaf".into(),
                    ),
                })?;
        }

        // 3. Tick the reconcile counter and refresh the queue-depth gauge
        //    using the current queue size (drain below is authoritative).
        metrics::record_reconcile_entries_dropped(dropped as u64);
        metrics::set_deferred_free_queue_depth(allocator.page_lifetime_queue().depth() as u64);

        // 4. Writer-serialized drain — caller holds the writer lock. The
        //    drain re-checks refcount under Acquire before freeing.
        allocator.drain_free_queue(self.io.as_ref())?;

        Ok(dropped)
    }
}
