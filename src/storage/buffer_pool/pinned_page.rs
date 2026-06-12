//! [`PinnedPage`] — the basic pin-only RAII guard for the buffer pool.

use std::sync::Arc;

use super::{BufferPool, PageSize};

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
    pub(super) pool: &'pool BufferPool,
    pub(super) page_number: u32,
    pub(super) page_size: PageSize,
    pub(super) snapshot: Arc<Vec<u8>>,
    pub(super) write_buf: Option<Vec<u8>>,
    pub(super) dirty: bool,
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
