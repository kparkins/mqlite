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

use super::{BufferPool, PageSize};

type VersionChainDrain = Vec<(Vec<u8>, Arc<VecDeque<VersionEntry>>)>;

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
    pub(crate) fn drain_leaf_chains(&self, page: u32) -> Result<VersionChainDrain> {
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
}

#[cfg(test)]
#[path = "tests/chains_accessors.rs"]
mod chains_accessors;

#[cfg(test)]
#[path = "tests/chains_reconcile.rs"]
mod chains_reconcile;
