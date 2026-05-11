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
use crate::mvcc::version::VersionEntry;

use super::{BufferPool, LatchMode, PageSize};

type VersionChainDrain = Vec<(Vec<u8>, Arc<VecDeque<VersionEntry>>)>;

impl BufferPool {
    // -----------------------------------------------------------------------
    // MVCC delta-chain helpers (T3.5 → PR0.5)
    //
    // The four mutator helpers below (`take_chain`, `put_chain`,
    // `clear_chains_on_page`, `drain_leaf_chains`) are now THIN WRAPPERS
    // over the latch-aware `with_chain_under_latch` /
    // `with_all_chains_under_latch` entry points. They previously went
    // straight to the partition mutex without taking the per-page latch,
    // which let concurrent CRUD writers race on the same `frame.deltas`
    // map. Routing through the latch-aware entry points removes that
    // race; the wrappers exist only to keep PR0.5 commit 2's diff
    // surface tractable. PR0.5 commit 3 deletes them.
    //
    // The `chains_empty` reader at the bottom is unchanged — it is a
    // read-only inspection used by structural-cleanup guards and does
    // not need the latch.
    // -----------------------------------------------------------------------

    /// Remove and return the delta chain for `key` on leaf page `page`.
    pub(crate) fn take_chain(
        &self,
        page: u32,
        key: &[u8],
    ) -> Result<Option<Arc<VecDeque<VersionEntry>>>> {
        // Wrapper over the latch-aware path. Note the semantic change
        // vs the legacy implementation: this path now ERRORS if the
        // frame is not resident (because `pin_then_latch` cannot pin a
        // missing page), where the legacy returned `Ok(None)`. No
        // production caller relies on the silent-miss behaviour after
        // PR0.5 commit 1's migration; the remaining test callers all
        // pre-pin the page before calling.
        self.with_chain_under_latch(page, key, LatchMode::Exclusive, |slot| slot.take())
    }

    /// Install a delta chain for `key` on leaf page `page`.
    pub(crate) fn put_chain(
        &self,
        page: u32,
        key: Vec<u8>,
        chain: Arc<VecDeque<VersionEntry>>,
    ) -> Result<()> {
        self.with_chain_under_latch(page, &key, LatchMode::Exclusive, |slot| {
            *slot = Some(chain);
        })
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
    /// `Small4k` pages never carry chains, so the call is a no-op
    /// there. `Large32k` pages route through the latch-aware
    /// `with_all_chains_under_latch`.
    pub(crate) fn clear_chains_on_page(&self, page: u32, size: PageSize) -> Result<()> {
        if size != PageSize::Large32k {
            return Ok(());
        }
        self.with_all_chains_under_latch(page, LatchMode::Exclusive, |chains| chains.clear())
    }

    /// Drain and return every delta chain currently attached to the
    /// 32 KB leaf frame for `page`. Returns an empty vector if the page
    /// is not resident.
    ///
    /// Used by the leaf-merge migration path to move tombstone-chain
    /// entries (whose cells were already removed earlier in the txn)
    /// onto the merged-into sibling so MVCC readers whose ReadView
    /// predates the delete still observe them.
    pub(crate) fn drain_leaf_chains(&self, page: u32) -> Result<VersionChainDrain> {
        self.with_all_chains_under_latch(page, LatchMode::Exclusive, |chains| {
            std::mem::take(chains).into_iter().collect()
        })
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
}

#[cfg(test)]
#[path = "tests/chains_accessors.rs"]
mod chains_accessors;

#[cfg(test)]
#[path = "tests/chains_reconcile.rs"]
mod chains_reconcile;
