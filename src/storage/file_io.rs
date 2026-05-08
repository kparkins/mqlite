//! File-backed `PageSource` implementation.
//!
//! [`FilePageSource`] routes all page reads and writes through the database file
//! using the `FileLock` file descriptor.  Using the lock fd for all I/O is
//! **required** to avoid the POSIX advisory-lock footgun: closing *any* open
//! file descriptor for a file while the process holds a `fcntl` advisory lock
//! releases ALL advisory locks the process holds on that file.
//!
//! ## On-disk page layout
//!
//! All pages — both 4 KB internal-node pages and 32 KB leaf pages — occupy a
//! **uniform 32 KB slot** on disk.  The byte offset of page `N` is always:
//!
//! ```text
//! offset(N) = N * PAGE_SIZE_LEAF   (= N * 32 768)
//! ```
//!
//! A 4 KB internal page stored in slot `N` occupies only bytes
//! `offset(N) .. offset(N) + 4096`.  The remaining 28 KB of the slot is
//! left as-is (zeros for a freshly created file).  A 32 KB leaf page uses
//! the full slot.
//!
//! Rationale: a single fixed-size slot avoids the need to track each page's
//! size when computing file offsets.  At typical B+ tree fan-out (~150 per
//! internal node) internal nodes represent < 1 % of all pages, so the wasted
//! space is negligible.
//!
//! ## EOF handling
//!
//! When a page beyond the current end of file is read (e.g., a newly
//! allocated page that has not yet been written to disk), [`FilePageSource`]
//! returns a zeroed buffer instead of propagating the OS `UnexpectedEof`
//! error.  This matches the buffer pool's expectation that a freshly allocated
//! page contains zeroes.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::storage::buffer_pool::{PageSize, PageSource};
use crate::storage::lock::FileLock;
use crate::storage::page::PAGE_SIZE_LEAF;

/// File-backed `PageSource` implementation.
///
/// Wraps an `Arc<dyn FileLock>` so that I/O and advisory locking share the
/// same underlying file descriptor.  Using the same fd for both operations
/// prevents accidental lock release on fd close (POSIX footgun).
pub(crate) struct FilePageSource {
    lock: Arc<dyn FileLock>,
}

impl FilePageSource {
    /// Create a new `FilePageSource` backed by `lock`.
    ///
    /// All reads and writes go through `lock.read_exact_at` /
    /// `lock.write_at` so that the advisory lock fd is never inadvertently
    /// closed while the lock is held.
    pub(crate) fn new(lock: Arc<dyn FileLock>) -> Self {
        Self { lock }
    }

    /// Compute the byte offset in the file for `page_number`.
    ///
    /// All pages occupy a uniform 32 KB slot regardless of their size class.
    #[inline]
    fn file_offset(page_number: u32) -> u64 {
        page_number as u64 * PAGE_SIZE_LEAF as u64
    }
}

impl PageSource for FilePageSource {
    /// Read `size.bytes()` bytes for `page_number` from the file into `buf`.
    ///
    /// If the read would extend beyond the current end of file (the page has
    /// not yet been written), `buf` is zeroed and `Ok(())` is returned.  This
    /// is the correct behaviour for freshly allocated pages.
    fn read_page(&self, page_number: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
        debug_assert_eq!(
            buf.len(),
            size.bytes(),
            "buf.len() ({}) != size.bytes() ({})",
            buf.len(),
            size.bytes()
        );
        let offset = Self::file_offset(page_number);
        match self.lock.read_exact_at(offset, buf) {
            Ok(()) => Ok(()),
            // Page is beyond the current end of file — treat it as zero.
            // This is normal when a newly allocated page is first pinned
            // before any content has been written to disk.
            Err(Error::Io(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                buf.fill(0);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Write `buf` (length `size.bytes()`) to `page_number` in the file.
    fn write_page(&self, page_number: u32, size: PageSize, buf: &[u8]) -> Result<()> {
        debug_assert_eq!(
            buf.len(),
            size.bytes(),
            "buf.len() ({}) != size.bytes() ({})",
            buf.len(),
            size.bytes()
        );
        let offset = Self::file_offset(page_number);
        self.lock.write_at(offset, buf)
    }
}

#[cfg(test)]
#[path = "tests/file_io.rs"]
mod tests;
