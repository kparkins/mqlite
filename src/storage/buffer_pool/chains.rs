//! MVCC per-frame delta-chain helpers on [`BufferPool`].
//!
//! The per-key delta chains live on the 32 KB leaf partition's frames.
//! After PR0.5 the only mutator surface is `with_chain_under_latch` /
//! `with_all_chains_under_latch` (on `BufferPool` and the
//! `BTreePageStore` trait); this module retains only the read-only
//! `chains_empty` inspector used by structural-cleanup guards.
//! Phase 5 reconcile and CRUD callers that already hold a page latch
//! must use [`super::LatchedPinnedPage`] helpers (`with_chain` /
//! `with_all_chains`) instead — resident chain mutation requires
//! `PageLatch::Exclusive`, while snapshots require
//! `LatchedPinnedPage::Shared` and copy/clone only.

use crate::error::{Error, Result};

use super::BufferPool;

impl BufferPool {
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
#[path = "tests/chains_latch_invariant.rs"]
mod chains_latch_invariant;

#[cfg(test)]
#[path = "tests/chains_reconcile.rs"]
mod chains_reconcile;
