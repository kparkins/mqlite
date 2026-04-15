//! File-backed `PageIo` implementation.
//!
//! [`FilePageIo`] routes all page reads and writes through the database file
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
//! allocated page that has not yet been written to disk), [`FilePageIo`]
//! returns a zeroed buffer instead of propagating the OS `UnexpectedEof`
//! error.  This matches the buffer pool's expectation that a freshly allocated
//! page contains zeroes.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::storage::buffer_pool::{PageIo, PageSize};
use crate::storage::lock::FileLock;
use crate::storage::page::PAGE_SIZE_LEAF;

// ---------------------------------------------------------------------------
// FilePageIo
// ---------------------------------------------------------------------------

/// File-backed `PageIo` implementation.
///
/// Wraps an `Arc<dyn FileLock>` so that I/O and advisory locking share the
/// same underlying file descriptor.  Using the same fd for both operations
/// prevents accidental lock release on fd close (POSIX footgun).
pub(crate) struct FilePageIo {
    lock: Arc<dyn FileLock>,
}

impl FilePageIo {
    /// Create a new `FilePageIo` backed by `lock`.
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
    pub(crate) fn file_offset(page_number: u32) -> u64 {
        page_number as u64 * PAGE_SIZE_LEAF as u64
    }
}

impl PageIo for FilePageIo {
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::lock::NoopFileLock;
    use std::sync::Mutex;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// In-memory `FileLock` implementation for testing `FilePageIo`.
    ///
    /// Backed by a `HashMap<u64, Vec<u8>>` keyed by byte offset + length.
    /// Supports positional reads and writes.
    struct MemFileLock {
        data: Mutex<Vec<u8>>,
    }

    impl MemFileLock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                data: Mutex::new(Vec::new()),
            })
        }

        fn snapshot(&self) -> Vec<u8> {
            self.data.lock().unwrap().clone()
        }
    }

    impl FileLock for MemFileLock {
        fn acquire_exclusive(&self, _: std::time::Duration) -> Result<bool> {
            Ok(false)
        }
        fn acquire_shared(&self, _: std::time::Duration) -> Result<bool> {
            Ok(false)
        }
        fn release(&self) -> Result<()> {
            Ok(())
        }

        fn write_at(&self, offset: u64, data: &[u8]) -> Result<()> {
            let mut buf = self.data.lock().unwrap();
            let end = offset as usize + data.len();
            if end > buf.len() {
                buf.resize(end, 0);
            }
            buf[offset as usize..end].copy_from_slice(data);
            Ok(())
        }

        fn read_exact_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
            let data = self.data.lock().unwrap();
            let start = offset as usize;
            let end = start + buf.len();
            if end > data.len() {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "read beyond end of mock file",
                )));
            }
            buf.copy_from_slice(&data[start..end]);
            Ok(())
        }

        fn sync(&self) -> Result<()> {
            // In-memory mock has no backing file to sync.
            Ok(())
        }
    }

    fn make_io() -> (Arc<MemFileLock>, FilePageIo) {
        let lock = MemFileLock::new();
        let io = FilePageIo::new(Arc::clone(&lock) as Arc<dyn FileLock>);
        (lock, io)
    }

    // -----------------------------------------------------------------------
    // file_offset
    // -----------------------------------------------------------------------

    #[test]
    fn file_offset_page_0_is_zero() {
        assert_eq!(FilePageIo::file_offset(0), 0);
    }

    #[test]
    fn file_offset_page_1_is_32768() {
        assert_eq!(FilePageIo::file_offset(1), PAGE_SIZE_LEAF as u64);
    }

    #[test]
    fn file_offset_is_uniform_32k_stride() {
        for n in 0u32..10 {
            assert_eq!(FilePageIo::file_offset(n), n as u64 * PAGE_SIZE_LEAF as u64);
        }
    }

    // -----------------------------------------------------------------------
    // write_page / read_page roundtrip
    // -----------------------------------------------------------------------

    #[test]
    fn write_and_read_32k_page_roundtrip() {
        let (_, io) = make_io();

        let mut data = vec![0u8; PageSize::Large32k.bytes()];
        data[0] = 0xAB;
        data[1000] = 0xCD;

        io.write_page(1, PageSize::Large32k, &data).unwrap();

        let mut buf = vec![0u8; PageSize::Large32k.bytes()];
        io.read_page(1, PageSize::Large32k, &mut buf).unwrap();

        assert_eq!(buf[0], 0xAB);
        assert_eq!(buf[1000], 0xCD);
    }

    #[test]
    fn write_and_read_4k_page_roundtrip() {
        let (_, io) = make_io();

        let mut data = vec![0u8; PageSize::Small4k.bytes()];
        data[42] = 0xFF;

        io.write_page(0, PageSize::Small4k, &data).unwrap();

        let mut buf = vec![0u8; PageSize::Small4k.bytes()];
        io.read_page(0, PageSize::Small4k, &mut buf).unwrap();

        assert_eq!(buf[42], 0xFF);
    }

    #[test]
    fn pages_at_different_numbers_do_not_overlap() {
        let (_, io) = make_io();

        let mut p1_data = vec![0u8; PageSize::Large32k.bytes()];
        p1_data[0] = 0x11;

        let mut p2_data = vec![0u8; PageSize::Large32k.bytes()];
        p2_data[0] = 0x22;

        io.write_page(1, PageSize::Large32k, &p1_data).unwrap();
        io.write_page(2, PageSize::Large32k, &p2_data).unwrap();

        let mut buf = vec![0u8; PageSize::Large32k.bytes()];
        io.read_page(1, PageSize::Large32k, &mut buf).unwrap();
        assert_eq!(buf[0], 0x11, "page 1 data corrupted by page 2 write");

        io.read_page(2, PageSize::Large32k, &mut buf).unwrap();
        assert_eq!(buf[0], 0x22, "page 2 data corrupted by page 1 write");
    }

    #[test]
    fn header_4k_and_page_1_32k_do_not_overlap() {
        let (mem, io) = make_io();

        // Write 4K header at page 0
        let mut header = vec![0u8; PageSize::Small4k.bytes()];
        header[0] = 0xAA;
        io.write_page(0, PageSize::Small4k, &header).unwrap();

        // Write 32K leaf at page 1 (should be at offset 32768)
        let mut leaf = vec![0u8; PageSize::Large32k.bytes()];
        leaf[0] = 0xBB;
        io.write_page(1, PageSize::Large32k, &leaf).unwrap();

        // Verify the file layout: header at 0, leaf at 32768
        let snap = mem.snapshot();
        assert_eq!(snap[0], 0xAA, "header first byte corrupted");
        assert_eq!(snap[32768], 0xBB, "leaf page at wrong offset");

        // Bytes 4096..32767 (between header and leaf slot) should be zero
        assert!(
            snap[4096..32768].iter().all(|&b| b == 0),
            "gap between header and page 1 must be zero"
        );
    }

    // -----------------------------------------------------------------------
    // EOF handling — reading a page beyond file end returns zeroes
    // -----------------------------------------------------------------------

    #[test]
    fn read_beyond_eof_returns_zeroes_32k() {
        let (_, io) = make_io(); // empty file

        let mut buf = vec![0xFFu8; PageSize::Large32k.bytes()];
        io.read_page(3, PageSize::Large32k, &mut buf).unwrap();

        assert!(buf.iter().all(|&b| b == 0), "EOF read must return zeroes");
    }

    #[test]
    fn read_beyond_eof_returns_zeroes_4k() {
        let (_, io) = make_io();

        let mut buf = vec![0xFFu8; PageSize::Small4k.bytes()];
        io.read_page(0, PageSize::Small4k, &mut buf).unwrap();

        assert!(
            buf.iter().all(|&b| b == 0),
            "EOF read of page 0 must return zeroes"
        );
    }

    // -----------------------------------------------------------------------
    // Multiple writes to same page
    // -----------------------------------------------------------------------

    #[test]
    fn second_write_overwrites_first() {
        let (_, io) = make_io();

        let first = vec![0x11u8; PageSize::Small4k.bytes()];
        let second = vec![0x22u8; PageSize::Small4k.bytes()];

        io.write_page(0, PageSize::Small4k, &first).unwrap();
        io.write_page(0, PageSize::Small4k, &second).unwrap();

        let mut buf = vec![0u8; PageSize::Small4k.bytes()];
        io.read_page(0, PageSize::Small4k, &mut buf).unwrap();

        assert!(
            buf.iter().all(|&b| b == 0x22),
            "second write must overwrite first"
        );
    }
}
