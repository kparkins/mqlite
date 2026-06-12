//! Flush, dirty-frame snapshot, LSN stamping, and cache-invalidation
//! surface on [`BufferPool`]. These are the dirty-bytes management entry
//! points consumed by the checkpoint driver and the durability path.

use std::collections::BTreeSet;

use crate::error::{Error, Result};
use crate::journal::wire::PageId;

use super::{BufferPool, PageSize};

impl BufferPool {
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
}
