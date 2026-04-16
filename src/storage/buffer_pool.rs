//! Buffer pool — in-memory page cache with CLOCK sweep eviction.
//!
//! The buffer pool maintains two independent partitions:
//! - **4 KB partition** (25 % of total) — internal B+ tree nodes
//! - **32 KB partition** (75 % of total) — leaf, overflow, and header pages
//!
//! Each partition has its own CLOCK sweep hand and backing frame array.
//!
//! ## Usage pattern
//!
//! Call [`BufferPool::pin`] to obtain a [`PinnedPage`] guard for a given page
//! number and size.  Writing through [`PinnedPage::data_mut`] automatically
//! marks the page dirty.  The page is unpinned when the guard is dropped.
//! Call [`BufferPool::flush`] before a checkpoint or close to write all dirty
//! pages to disk.
//!
//! ## Thread safety
//!
//! Each partition is protected by a separate [`Mutex`].  The data pointer
//! inside [`PinnedPage`] is stable for the duration of the pin because:
//!
//! 1. CLOCK eviction skips frames whose `pin_count > 0`.
//! 2. Frame backing storage (`Box<[u8]>`) never moves — frame slots are
//!    pre-allocated at pool creation with fixed capacity and are never
//!    reallocated.
//!
//! Callers must ensure that at most one [`PinnedPage`] for a given page number
//! calls [`data_mut`](PinnedPage::data_mut) at a time.  Multiple concurrent
//! read-only pins (using only [`data`](PinnedPage::data)) are safe.  The
//! database-level single-writer lock enforces this at a higher level.

use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::Mutex;

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Page size
// ---------------------------------------------------------------------------

/// Indicates which page-size partition to use for a given page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageSize {
    /// 4 KiB — internal (branch) B+ tree nodes.
    Small4k,
    /// 32 KiB — leaf nodes, overflow pages, and the file header.
    Large32k,
}

impl PageSize {
    pub(crate) fn bytes(self) -> usize {
        match self {
            PageSize::Small4k => 4096,
            PageSize::Large32k => 32768,
        }
    }
}

// ---------------------------------------------------------------------------
// Page I/O abstraction
// ---------------------------------------------------------------------------

/// Abstraction over on-disk (or in-memory for tests) page read/write.
///
/// Implementors are responsible for knowing the page size from the `size`
/// parameter and performing the appropriate seek + I/O.
pub(crate) trait PageSource: Send + Sync {
    /// Read `size.bytes()` bytes for `page_number` into `buf`.
    ///
    /// `buf.len()` is guaranteed to equal `size.bytes()`.
    fn read_page(&self, page_number: u32, size: PageSize, buf: &mut [u8]) -> Result<()>;

    /// Write `buf` (length `size.bytes()`) to `page_number` on disk.
    fn write_page(&self, page_number: u32, size: PageSize, buf: &[u8]) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Frame (internal)
// ---------------------------------------------------------------------------

struct Frame {
    page_number: u32,
    /// Heap-allocated page data; length equals the partition's page size.
    /// The `Box` pointer is stable (never moved) for the lifetime of this slot.
    data: Box<[u8]>,
    pin_count: u32,
    dirty: bool,
    ref_bit: bool,
}

// ---------------------------------------------------------------------------
// Partition (internal)
// ---------------------------------------------------------------------------

/// One pool partition; all frames share the same page size.
struct Partition {
    /// Fixed-size slot array — pre-allocated, never reallocated.
    /// `None` denotes an empty slot.
    frames: Vec<Option<Frame>>,
    /// page_number → slot index.
    page_map: HashMap<u32, usize>,
    /// CLOCK sweep hand.
    clock_hand: usize,
    page_size: usize,
    capacity: usize,
}

impl Partition {
    fn new(capacity: usize, page_size: usize) -> Self {
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
    fn evict_frame(&mut self, idx: usize, io: &dyn PageSource, size: PageSize) -> Result<()> {
        if let Some(frame) = &self.frames[idx] {
            let was_dirty = frame.dirty;
            if was_dirty {
                io.write_page(frame.page_number, size, &frame.data)?;
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
    fn pin_page(&mut self, page_number: u32, io: &dyn PageSource, size: PageSize) -> Result<usize> {
        // Cache hit path
        if let Some(&idx) = self.page_map.get(&page_number) {
            let frame = self.frames[idx]
                .as_mut()
                .expect("page_map invariant: frame must exist at mapped slot");
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
        let mut data = vec![0u8; self.page_size].into_boxed_slice();
        io.read_page(page_number, size, &mut data)?;

        self.frames[idx] = Some(Frame {
            page_number,
            data,
            pin_count: 1,
            dirty: false,
            ref_bit: true,
        });
        self.page_map.insert(page_number, idx);

        Ok(idx)
    }

    /// Decrement `pin_count`; optionally mark the frame dirty.
    fn unpin_page(&mut self, page_number: u32, dirty: bool) -> Result<()> {
        let idx = self.page_map.get(&page_number).copied().ok_or_else(|| {
            Error::Internal(format!(
                "buffer pool unpin: page {page_number} is not in the pool"
            ))
        })?;

        let frame = self.frames[idx]
            .as_mut()
            .expect("page_map invariant: frame must exist at mapped slot");

        if frame.pin_count == 0 {
            return Err(Error::Internal(format!(
                "buffer pool unpin: page {page_number} pin_count is already 0"
            )));
        }
        frame.pin_count -= 1;
        if dirty {
            frame.dirty = true;
        }
        Ok(())
    }

    /// Write every dirty frame to disk and clear their dirty bits.
    fn flush_all(&mut self, io: &dyn PageSource, size: PageSize) -> Result<()> {
        for slot in self.frames.iter_mut() {
            if let Some(frame) = slot {
                if frame.dirty {
                    io.write_page(frame.page_number, size, &frame.data)?;
                    frame.dirty = false;
                }
            }
        }
        Ok(())
    }

    /// Return a raw mutable pointer to the frame's data buffer.
    ///
    /// # Safety
    ///
    /// Caller must ensure `pin_count > 0` for the frame at `idx`
    /// (preventing eviction) and must not create concurrent mutable aliases.
    fn data_ptr_mut(&mut self, idx: usize) -> NonNull<[u8]> {
        let frame = self.frames[idx]
            .as_mut()
            .expect("data_ptr_mut: frame slot must be occupied");
        NonNull::from(frame.data.as_mut())
    }

    // -----------------------------------------------------------------------
    // Introspection helpers (tests only)
    // -----------------------------------------------------------------------

    #[cfg(test)]
    fn pin_count(&self, page_number: u32) -> Option<u32> {
        let idx = *self.page_map.get(&page_number)?;
        self.frames[idx].as_ref().map(|f| f.pin_count)
    }

    #[cfg(test)]
    fn is_dirty(&self, page_number: u32) -> Option<bool> {
        let idx = *self.page_map.get(&page_number)?;
        self.frames[idx].as_ref().map(|f| f.dirty)
    }

    #[cfg(test)]
    fn is_cached(&self, page_number: u32) -> bool {
        self.page_map.contains_key(&page_number)
    }
}

// ---------------------------------------------------------------------------
// PinnedPage guard
// ---------------------------------------------------------------------------

/// A handle to a page that has been pinned in the buffer pool.
///
/// - [`data`](PinnedPage::data) — shared (read-only) view.
/// - [`data_mut`](PinnedPage::data_mut) — exclusive view; automatically sets
///   the dirty bit so the page is written to disk on the next
///   [`flush`](BufferPool::flush).
/// - Drop — automatically unpins the page (decrements `pin_count`).
///
/// # Safety
///
/// The pointer inside is stable because CLOCK eviction refuses to evict
/// pinned frames and the frame's `Box<[u8]>` never moves.  Do not call
/// `data_mut` on two different `PinnedPage`s for the same page concurrently;
/// the database-level writer lock prevents this.
pub(crate) struct PinnedPage<'pool> {
    pool: &'pool BufferPool,
    page_number: u32,
    page_size: PageSize,
    ptr: NonNull<[u8]>,
    dirty: bool,
}

// SAFETY: The raw pointer points to a heap-allocated Box inside the pool.
// The pool is Send+Sync; the PinnedPage is only shared across threads under
// the database-level read lock (for immutable access).
unsafe impl Send for PinnedPage<'_> {}
unsafe impl Sync for PinnedPage<'_> {}

impl<'pool> PinnedPage<'pool> {
    /// Read-only view of the page data.
    #[inline]
    pub(crate) fn data(&self) -> &[u8] {
        // SAFETY: pointer is valid while pin_count > 0; no mutable alias
        // while holding only a shared reference to this guard.
        unsafe { self.ptr.as_ref() }
    }

    /// Mutable view of the page data; marks the page dirty.
    #[inline]
    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        self.dirty = true;
        // SAFETY: same stability guarantee; exclusivity enforced by the
        // single-writer database lock.
        unsafe { self.ptr.as_mut() }
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
        // Errors are intentionally swallowed — Drop must not panic.
        let _ = self
            .pool
            .unpin_internal(self.page_number, self.page_size, self.dirty);
    }
}

// ---------------------------------------------------------------------------
// BufferPool
// ---------------------------------------------------------------------------

/// In-memory page cache with CLOCK sweep eviction.
///
/// Frame counts are computed from `buffer_pool_size`:
/// - 25 % → 4 KB frames (internal nodes)
/// - 75 % → 32 KB frames (leaf / overflow / header pages)
///
/// # Usage
///
/// Create the pool with [`BufferPool::new`], specifying the total byte budget
/// and a [`PageSource`] backend.  Pin pages with [`BufferPool::pin`], read or
/// write them via [`PinnedPage::data`] / [`PinnedPage::data_mut`], then drop
/// the guard to unpin.  Call [`BufferPool::flush`] before a checkpoint or
/// close to write all dirty pages to disk.
pub(crate) struct BufferPool {
    inner_4k: Mutex<Partition>,
    inner_32k: Mutex<Partition>,
    io: Box<dyn PageSource>,
}

impl BufferPool {
    /// Create a new buffer pool backed by `io`.
    ///
    /// `buffer_pool_size` is the total byte budget.  Both partitions receive
    /// at least one frame even when the budget is very small.
    pub(crate) fn new(buffer_pool_size: usize, io: Box<dyn PageSource>) -> Self {
        let size_4k = buffer_pool_size / 4;
        let size_32k = buffer_pool_size - size_4k;

        let capacity_4k = (size_4k / PageSize::Small4k.bytes()).max(1);
        let capacity_32k = (size_32k / PageSize::Large32k.bytes()).max(1);

        Self {
            inner_4k: Mutex::new(Partition::new(capacity_4k, PageSize::Small4k.bytes())),
            inner_32k: Mutex::new(Partition::new(capacity_32k, PageSize::Large32k.bytes())),
            io,
        }
    }

    /// Pin `page_number` in the appropriate partition and return a
    /// [`PinnedPage`] guard.
    ///
    /// On cache hit the guard is returned immediately after updating
    /// `pin_count` and `ref_bit`.  On cache miss a victim is evicted (flushed
    /// to disk if dirty) and the page is loaded from the I/O backend.
    ///
    /// # Errors
    ///
    /// - Mutex poisoned (should not happen in normal operation).
    /// - All frames in the partition are currently pinned.
    /// - I/O backend error during load or eviction.
    pub(crate) fn pin(&self, page_number: u32, size: PageSize) -> Result<PinnedPage<'_>> {
        let (lock, size_enum) = match size {
            PageSize::Small4k => (&self.inner_4k, PageSize::Small4k),
            PageSize::Large32k => (&self.inner_32k, PageSize::Large32k),
        };

        let mut guard = lock
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;

        let idx = guard.pin_page(page_number, self.io.as_ref(), size_enum)?;

        // Obtain raw pointer while still holding the lock.
        // SAFETY: Vec backing does not reallocate (fixed capacity);
        // eviction is prevented by pin_count > 0 set above.
        let ptr = guard.data_ptr_mut(idx);

        Ok(PinnedPage {
            pool: self,
            page_number,
            page_size: size_enum,
            ptr,
            dirty: false,
        })
    }

    /// Write all dirty pages in both partitions to disk and clear dirty bits.
    ///
    /// Must be called before a WAL checkpoint or `Database::close` to ensure
    /// in-flight modifications reach stable storage.
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

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Decrement pin count; propagate `dirty` flag.  Called from
    /// [`PinnedPage::drop`].
    fn unpin_internal(&self, page_number: u32, size: PageSize, dirty: bool) -> Result<()> {
        let lock = match size {
            PageSize::Small4k => &self.inner_4k,
            PageSize::Large32k => &self.inner_32k,
        };
        lock.lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?
            .unpin_page(page_number, dirty)
    }
}

// ---------------------------------------------------------------------------
// Recommended buffer pool sizes
// ---------------------------------------------------------------------------

/// Recommended buffer pool byte sizes for common deployment tiers.
#[allow(dead_code)]
pub(crate) mod default_sizes {
    /// IoT / edge devices: 4 MiB.
    pub const IOT: usize = 4 * 1024 * 1024;
    /// Desktop / CLI applications: 64 MiB (library default).
    pub const DESKTOP: usize = 64 * 1024 * 1024;
    /// Server deployments: 256 MiB.
    pub const SERVER: usize = 256 * 1024 * 1024;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex as StdMutex};

    // -----------------------------------------------------------------------
    // Mock I/O backend
    // -----------------------------------------------------------------------

    #[derive(Default)]
    struct MockIo {
        pages: StdMutex<HashMap<u32, Vec<u8>>>,
        write_count: StdMutex<u32>,
        read_count: StdMutex<u32>,
    }

    impl MockIo {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn seed(&self, page_number: u32, data: Vec<u8>) {
            self.pages.lock().unwrap().insert(page_number, data);
        }

        fn write_count(&self) -> u32 {
            *self.write_count.lock().unwrap()
        }

        fn read_count(&self) -> u32 {
            *self.read_count.lock().unwrap()
        }

        fn read_back(&self, page_number: u32) -> Option<Vec<u8>> {
            self.pages.lock().unwrap().get(&page_number).cloned()
        }
    }

    impl PageSource for MockIo {
        fn read_page(&self, page_number: u32, _size: PageSize, buf: &mut [u8]) -> Result<()> {
            *self.read_count.lock().unwrap() += 1;
            let pages = self.pages.lock().unwrap();
            if let Some(data) = pages.get(&page_number) {
                let copy_len = buf.len().min(data.len());
                buf[..copy_len].copy_from_slice(&data[..copy_len]);
                if copy_len < buf.len() {
                    buf[copy_len..].fill(0);
                }
            } else {
                // Fill with a deterministic pattern derived from page_number
                for (i, b) in buf.iter_mut().enumerate() {
                    *b = page_number.wrapping_add(i as u32) as u8;
                }
            }
            Ok(())
        }

        fn write_page(&self, page_number: u32, _size: PageSize, buf: &[u8]) -> Result<()> {
            *self.write_count.lock().unwrap() += 1;
            self.pages.lock().unwrap().insert(page_number, buf.to_vec());
            Ok(())
        }
    }

    // Newtype wrapper so Arc<MockIo> can be boxed as `Box<dyn PageSource>`.
    struct ArcIo(Arc<MockIo>);
    impl PageSource for ArcIo {
        fn read_page(&self, p: u32, s: PageSize, buf: &mut [u8]) -> Result<()> {
            self.0.read_page(p, s, buf)
        }
        fn write_page(&self, p: u32, s: PageSize, buf: &[u8]) -> Result<()> {
            self.0.write_page(p, s, buf)
        }
    }

    fn make_pool_with(size_bytes: usize, io: Arc<MockIo>) -> BufferPool {
        BufferPool::new(size_bytes, Box::new(ArcIo(io)))
    }

    fn desktop_pool(io: Arc<MockIo>) -> BufferPool {
        make_pool_with(default_sizes::DESKTOP, io)
    }

    // -----------------------------------------------------------------------
    // Pin / unpin basics
    // -----------------------------------------------------------------------

    #[test]
    fn pin_32k_reads_page_from_io() {
        let io = MockIo::new();
        let mut seed = vec![0u8; PageSize::Large32k.bytes()];
        seed[0] = 0xAB;
        seed[1] = 0xCD;
        io.seed(1, seed);

        let pool = desktop_pool(Arc::clone(&io));
        let page = pool.pin(1, PageSize::Large32k).unwrap();
        assert_eq!(page.data()[0], 0xAB);
        assert_eq!(page.data()[1], 0xCD);
        assert_eq!(io.read_count(), 1);
    }

    #[test]
    fn pin_4k_reads_page_from_io() {
        let io = MockIo::new();
        let mut seed = vec![0u8; PageSize::Small4k.bytes()];
        seed[100] = 0x42;
        io.seed(5, seed);

        let pool = desktop_pool(Arc::clone(&io));
        let page = pool.pin(5, PageSize::Small4k).unwrap();
        assert_eq!(page.data()[100], 0x42);
        assert_eq!(io.read_count(), 1);
    }

    #[test]
    fn cache_hit_does_not_re_read_from_disk() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let p1 = pool.pin(10, PageSize::Large32k).unwrap();
        drop(p1); // unpin

        let _p2 = pool.pin(10, PageSize::Large32k).unwrap();

        assert_eq!(io.read_count(), 1, "second pin must be a cache hit");
    }

    #[test]
    fn drop_decrements_pin_count_to_zero() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let page = pool.pin(7, PageSize::Large32k).unwrap();
        {
            let g = pool.inner_32k.lock().unwrap();
            assert_eq!(g.pin_count(7), Some(1));
        }
        drop(page);
        {
            let g = pool.inner_32k.lock().unwrap();
            assert_eq!(g.pin_count(7), Some(0));
        }
    }

    #[test]
    fn double_pin_increments_count_twice() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let _p1 = pool.pin(3, PageSize::Small4k).unwrap();
        let _p2 = pool.pin(3, PageSize::Small4k).unwrap();
        {
            let g = pool.inner_4k.lock().unwrap();
            assert_eq!(g.pin_count(3), Some(2));
        }
        drop(_p1);
        {
            let g = pool.inner_4k.lock().unwrap();
            assert_eq!(g.pin_count(3), Some(1));
        }
    }

    // -----------------------------------------------------------------------
    // Dirty flag and data mutation
    // -----------------------------------------------------------------------

    #[test]
    fn data_mut_marks_page_dirty() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut page = pool.pin(20, PageSize::Large32k).unwrap();
        page.data_mut()[0] = 0xFF;
        // Check dirty bit before dropping
        assert!(page.dirty, "data_mut must set dirty=true");
    }

    #[test]
    fn mark_dirty_sets_dirty_flag() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut page = pool.pin(21, PageSize::Large32k).unwrap();
        assert!(!page.dirty);
        page.mark_dirty();
        assert!(page.dirty);
    }

    #[test]
    fn unpin_without_mutation_leaves_page_clean() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let page = pool.pin(22, PageSize::Large32k).unwrap();
        drop(page);

        let g = pool.inner_32k.lock().unwrap();
        assert_eq!(g.is_dirty(22), Some(false));
    }

    #[test]
    fn unpin_with_dirty_propagates_to_frame() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut page = pool.pin(23, PageSize::Large32k).unwrap();
        page.mark_dirty();
        drop(page); // dirty=true propagated to frame

        let g = pool.inner_32k.lock().unwrap();
        assert_eq!(g.is_dirty(23), Some(true));
    }

    // -----------------------------------------------------------------------
    // Flush
    // -----------------------------------------------------------------------

    #[test]
    fn flush_writes_dirty_pages_to_io() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut page = pool.pin(30, PageSize::Large32k).unwrap();
        page.data_mut()[0] = 0xDE;
        drop(page);

        pool.flush().unwrap();

        assert_eq!(io.write_count(), 1, "flush must write the dirty page");
        let written = io.read_back(30).unwrap();
        assert_eq!(written[0], 0xDE);
    }

    #[test]
    fn flush_clears_dirty_bit() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut page = pool.pin(31, PageSize::Large32k).unwrap();
        page.mark_dirty();
        drop(page);

        pool.flush().unwrap();

        let g = pool.inner_32k.lock().unwrap();
        assert_eq!(g.is_dirty(31), Some(false));
    }

    #[test]
    fn flush_skips_clean_pages() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let page = pool.pin(32, PageSize::Large32k).unwrap();
        drop(page); // clean

        pool.flush().unwrap();

        assert_eq!(io.write_count(), 0, "flush must not write clean pages");
    }

    #[test]
    fn flush_writes_both_partitions() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut p4k = pool.pin(1, PageSize::Small4k).unwrap();
        p4k.mark_dirty();
        drop(p4k);

        let mut p32k = pool.pin(2, PageSize::Large32k).unwrap();
        p32k.mark_dirty();
        drop(p32k);

        pool.flush().unwrap();

        assert_eq!(
            io.write_count(),
            2,
            "flush must write one page from each partition"
        );
    }

    // -----------------------------------------------------------------------
    // CLOCK eviction
    // -----------------------------------------------------------------------

    #[test]
    fn eviction_writes_dirty_victim_before_loading_new_page() {
        // Create a pool with only 1 frame in the 32 KB partition
        let io = MockIo::new();
        let mut seed = vec![0u8; PageSize::Large32k.bytes()];
        seed[0] = 0xAA;
        io.seed(1, seed);

        // 32 KB pool with capacity=1 (size_32k = 32768 * 1 = 32768, but we
        // set total = 32768 * 1 so capacity_32k = (32768 * 0.75 / 32768) = 0 → max(0,1) = 1)
        let pool = make_pool_with(PageSize::Large32k.bytes(), Arc::clone(&io));

        // Pin page 1 and mutate it
        {
            let mut page = pool.pin(1, PageSize::Large32k).unwrap();
            page.data_mut()[0] = 0xBB; // dirty
        }
        // pin_count back to 0; dirty=true

        // Pin page 2 — triggers eviction of page 1 (dirty → written to disk)
        let _page2 = pool.pin(2, PageSize::Large32k).unwrap();

        assert!(
            io.write_count() >= 1,
            "dirty victim must be written before eviction"
        );
        let written = io.read_back(1).unwrap();
        assert_eq!(written[0], 0xBB);
    }

    #[test]
    fn clean_victim_evicted_without_write() {
        let io = MockIo::new();
        let pool = make_pool_with(PageSize::Large32k.bytes(), Arc::clone(&io));

        // Pin page 1 (clean), unpin
        let _p = pool.pin(1, PageSize::Large32k).unwrap();
        drop(_p);

        // Pin page 2 — evicts page 1 (clean, no write)
        let _p2 = pool.pin(2, PageSize::Large32k).unwrap();

        assert_eq!(io.write_count(), 0, "clean victim must not be written");
    }

    #[test]
    fn evicted_page_no_longer_cached() {
        let io = MockIo::new();
        let pool = make_pool_with(PageSize::Large32k.bytes(), Arc::clone(&io));

        let _p = pool.pin(1, PageSize::Large32k).unwrap();
        drop(_p);

        let _p2 = pool.pin(2, PageSize::Large32k).unwrap();

        let g = pool.inner_32k.lock().unwrap();
        assert!(!g.is_cached(1), "evicted page must not remain in the map");
        assert!(g.is_cached(2), "newly loaded page must be in the map");
    }

    #[test]
    fn clock_second_chance_defers_eviction() {
        // Pool with exactly 2 frames in the 32k partition.
        // total=3*32768=98304 → size_32k=73728 → capacity_32k=2
        let io = MockIo::new();
        let pool = make_pool_with(3 * PageSize::Large32k.bytes(), Arc::clone(&io));

        // Fill both slots
        let p1 = pool.pin(1, PageSize::Large32k).unwrap();
        let p2 = pool.pin(2, PageSize::Large32k).unwrap();
        drop(p1);
        drop(p2);

        {
            let g = pool.inner_32k.lock().unwrap();
            assert!(g.is_cached(1));
            assert!(g.is_cached(2));
        }

        // Adding a third page must evict exactly one existing page
        let _p3 = pool.pin(3, PageSize::Large32k).unwrap();
        {
            let g = pool.inner_32k.lock().unwrap();
            assert!(g.is_cached(3), "new page must be in pool");
            // Exactly one of the original pages was evicted
            let cached_count = [1u32, 2u32].iter().filter(|&&pn| g.is_cached(pn)).count();
            assert_eq!(cached_count, 1, "exactly one original page should remain");
        }
    }

    // -----------------------------------------------------------------------
    // All-pinned error (graceful, not panic)
    // -----------------------------------------------------------------------

    #[test]
    fn all_pinned_returns_error_not_panic() {
        let io = MockIo::new();
        // 1-frame pool
        let pool = make_pool_with(PageSize::Large32k.bytes(), Arc::clone(&io));

        let _p1 = pool.pin(1, PageSize::Large32k).unwrap();
        // p1 is still pinned (pin_count=1); the single frame is occupied.
        // Trying to pin a different page must fail gracefully.
        let result = pool.pin(2, PageSize::Large32k);
        assert!(
            result.is_err(),
            "pinning when all frames are occupied must return Err"
        );
        match result {
            Err(Error::Internal(msg)) => {
                assert!(
                    msg.contains("pinned"),
                    "error message should mention 'pinned'"
                );
            }
            Err(e) => panic!("expected Error::Internal, got: {e}"),
            Ok(_) => panic!("expected error but got Ok"),
        }
    }

    #[test]
    fn all_pinned_4k_returns_error() {
        let io = MockIo::new();
        let pool = make_pool_with(PageSize::Small4k.bytes(), Arc::clone(&io));

        let _p1 = pool.pin(1, PageSize::Small4k).unwrap();
        let result = pool.pin(2, PageSize::Small4k);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Two partitions are independent
    // -----------------------------------------------------------------------

    #[test]
    fn partitions_are_independent() {
        let io = MockIo::new();
        // 1-frame-per-partition pool.
        // total=2*4096=8192 → size_4k=2048 → cap_4k=max(0,1)=1
        //                   → size_32k=6144 → cap_32k=max(0,1)=1
        let pool = make_pool_with(2 * PageSize::Small4k.bytes(), Arc::clone(&io));

        // Fill 4k partition
        let _p4k = pool.pin(1, PageSize::Small4k).unwrap();
        // Fill 32k partition
        let _p32k = pool.pin(1, PageSize::Large32k).unwrap();

        // Pinning another 4k page fails (4k partition full, p4k still pinned)
        let r4k = pool.pin(2, PageSize::Small4k);
        assert!(r4k.is_err(), "4k partition should be full");

        // Pinning another 32k page fails too
        let r32k = pool.pin(2, PageSize::Large32k);
        assert!(r32k.is_err(), "32k partition should be full");
    }

    // -----------------------------------------------------------------------
    // Page data integrity
    // -----------------------------------------------------------------------

    #[test]
    fn mutated_data_persists_while_pinned() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let mut page = pool.pin(40, PageSize::Large32k).unwrap();
        page.data_mut()[500] = 0x77;
        assert_eq!(page.data()[500], 0x77);
    }

    #[test]
    fn re_pinned_page_retains_in_memory_modifications() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        {
            let mut p = pool.pin(50, PageSize::Large32k).unwrap();
            p.data_mut()[0] = 0x55;
        }
        // page is still in the pool (not evicted — large pool)
        let p2 = pool.pin(50, PageSize::Large32k).unwrap();
        assert_eq!(
            p2.data()[0],
            0x55,
            "in-memory modification must survive re-pin"
        );
    }

    // -----------------------------------------------------------------------
    // Capacity / frame counts
    // -----------------------------------------------------------------------

    #[test]
    fn iot_pool_has_at_least_one_frame_per_partition() {
        let io = MockIo::new();
        let pool = make_pool_with(default_sizes::IOT, Arc::clone(&io));

        // Must be able to pin at least one page of each size
        let _p4k = pool.pin(1, PageSize::Small4k).unwrap();
        let _p32k = pool.pin(1, PageSize::Large32k).unwrap();
    }

    #[test]
    fn desktop_pool_can_hold_many_pages() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        // 64 MB pool; 32k partition = ~1500 frames; pin 100 pages
        let handles: Vec<_> = (0..100)
            .map(|i| pool.pin(i, PageSize::Large32k).unwrap())
            .collect();

        assert_eq!(handles.len(), 100);
    }

    // -----------------------------------------------------------------------
    // page_number accessor
    // -----------------------------------------------------------------------

    #[test]
    fn pinned_page_number_accessor() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));

        let page = pool.pin(99, PageSize::Large32k).unwrap();
        assert_eq!(page.page_number(), 99);
    }
}
