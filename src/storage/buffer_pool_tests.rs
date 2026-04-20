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

// -----------------------------------------------------------------------
// Reconciliation (T6)
// -----------------------------------------------------------------------

mod reconcile {
    use super::*;
    use crate::mvcc::metrics;
    use crate::mvcc::read_view::{ReadView, ReadViewRegistry};
    use crate::mvcc::timestamp::Ts;
    use crate::mvcc::version::{OverflowRef, VersionData, VersionEntry};
    use crate::storage::allocator::AllocatorHandle;
    use crate::storage::header::FileHeader;

    fn ts(ms: u64) -> Ts {
        Ts {
            physical_ms: ms,
            logical: 0,
        }
    }

    fn fresh_allocator() -> AllocatorHandle {
        AllocatorHandle::new(FileHeader::new(0, 0, 0))
    }

    /// Allocator whose header reports enough pages to legally free
    /// high-numbered overflow pages (the free-list link write goes to
    /// the pool's MockIo which tolerates any page number).
    fn allocator_with_capacity(total_pages: u32) -> AllocatorHandle {
        let alloc = fresh_allocator();
        alloc
            .update_header(|h| h.total_page_count = total_pages)
            .unwrap();
        alloc
    }

    /// Build a fresh pool + allocator pair and pin leaf page `page`
    /// so a version chain can be attached to the resident frame.
    fn pool_with_resident_leaf(page: u32) -> (BufferPool, AllocatorHandle, Arc<MockIo>) {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));
        // Force the frame resident (PinnedPage dropped immediately; the
        // frame stays in the pool because the pool is large).
        let _p = pool.pin(page, PageSize::Large32k).unwrap();
        drop(_p);
        let alloc = fresh_allocator();
        (pool, alloc, io)
    }

    fn install_chain(pool: &BufferPool, page: u32, key: &[u8], chain: VecDeque<VersionEntry>) {
        pool.put_chain(page, key.to_vec(), Arc::new(chain)).unwrap();
    }

    fn entry_inline(start: Ts, stop: Ts, txn: u64, payload: &[u8]) -> VersionEntry {
        VersionEntry {
            start_ts: start,
            stop_ts: stop,
            txn_id: txn,
            data: VersionData::Inline(payload.to_vec()),
            is_tombstone: false,
        }
    }

    fn tombstone(start: Ts, stop: Ts, txn: u64) -> VersionEntry {
        VersionEntry {
            start_ts: start,
            stop_ts: stop,
            txn_id: txn,
            data: VersionData::Inline(Vec::new()),
            is_tombstone: true,
        }
    }

    #[test]
    fn drops_entries_below_oldest_required_ts() {
        let (pool, alloc, _io) = pool_with_resident_leaf(1);
        let registry = ReadViewRegistry::new();
        // No live readers → ort = Ts::MAX. Retain rule: keep the live
        // head and anything with stop_ts > ort; drop the rest. A
        // 10-entry chain (1 head + 9 aged) collapses entirely because
        // the lone survivor (head, stop_ts == Ts::MAX) matches the
        // on-disk cell.
        let mut chain = VecDeque::new();
        // Head — most recent
        chain.push_back(entry_inline(ts(100), Ts::MAX, 1, b"head"));
        // Nine older entries with concrete stop_ts values
        for i in 0..9 {
            chain.push_back(entry_inline(
                ts(10 + i),
                ts(20 + i),
                1 + i as u64,
                format!("v{i}").as_bytes(),
            ));
        }
        install_chain(&pool, 1, b"K", chain);

        let dropped = pool.reconcile(1, &registry, &alloc).unwrap();
        assert_eq!(dropped, 9, "nine aged entries must drop");

        // Only the head (Ts::MAX) survived — and because it's the only
        // entry and non-tombstone, the chain was collapsed entirely.
        assert!(pool.chains_empty(1).unwrap());
    }

    #[test]
    fn retains_entries_needed_by_live_reader() {
        let (pool, alloc, _io) = pool_with_resident_leaf(2);
        let registry = Arc::new(ReadViewRegistry::new());
        // Reader pinned at ts=5 — any entry whose stop_ts > ts(5) must
        // survive.
        let _view = ReadView::open(Arc::clone(&registry), ts(5), 77);

        let mut chain = VecDeque::new();
        chain.push_back(entry_inline(ts(100), Ts::MAX, 1, b"head"));
        chain.push_back(entry_inline(ts(50), ts(100), 2, b"middle")); // stop_ts > 5 — keep
        chain.push_back(entry_inline(ts(1), ts(3), 3, b"gone")); // stop_ts < 5 — drop
        install_chain(&pool, 2, b"K", chain);

        metrics::reset_reconcile_entries_dropped();
        let dropped = pool.reconcile(2, &registry, &alloc).unwrap();
        assert_eq!(dropped, 1);

        // Chain survives because it has > 1 entry now.
        assert!(!pool.chains_empty(2).unwrap());
    }

    #[test]
    fn collapse_when_only_head_entry_remains() {
        let (pool, alloc, _io) = pool_with_resident_leaf(3);
        let registry = ReadViewRegistry::new();

        let mut chain = VecDeque::new();
        chain.push_back(entry_inline(ts(100), Ts::MAX, 1, b"head"));
        chain.push_back(entry_inline(ts(10), ts(20), 2, b"old"));
        install_chain(&pool, 3, b"K", chain);

        let dropped = pool.reconcile(3, &registry, &alloc).unwrap();
        assert_eq!(dropped, 1);
        // Single head collapsed.
        assert!(pool.chains_empty(3).unwrap());
    }

    #[test]
    fn no_collapse_when_head_is_tombstone() {
        let (pool, alloc, _io) = pool_with_resident_leaf(4);
        let registry = ReadViewRegistry::new();

        let mut chain = VecDeque::new();
        chain.push_back(tombstone(ts(100), Ts::MAX, 1));
        chain.push_back(entry_inline(ts(10), ts(20), 2, b"old"));
        install_chain(&pool, 4, b"K", chain);

        let dropped = pool.reconcile(4, &registry, &alloc).unwrap();
        assert_eq!(dropped, 1);
        // Tombstone-only chain still needed to override on-disk cell —
        // do not collapse.
        assert!(!pool.chains_empty(4).unwrap());
    }

    #[test]
    fn reconciles_multi_key_frame_independently() {
        let (pool, alloc, _io) = pool_with_resident_leaf(5);
        let registry = ReadViewRegistry::new();

        let mut c_a = VecDeque::new();
        c_a.push_back(entry_inline(ts(100), Ts::MAX, 1, b"A-head"));
        c_a.push_back(entry_inline(ts(10), ts(20), 2, b"A-old"));
        install_chain(&pool, 5, b"A", c_a);

        let mut c_b = VecDeque::new();
        c_b.push_back(entry_inline(ts(200), Ts::MAX, 3, b"B-head"));
        c_b.push_back(entry_inline(ts(30), ts(40), 4, b"B-old-1"));
        c_b.push_back(entry_inline(ts(50), ts(60), 5, b"B-old-2"));
        install_chain(&pool, 5, b"B", c_b);

        let dropped = pool.reconcile(5, &registry, &alloc).unwrap();
        // 1 + 2 = 3 older entries dropped; both chains collapse.
        assert_eq!(dropped, 3);
        assert!(pool.chains_empty(5).unwrap());
    }

    #[test]
    fn overflow_refs_drop_and_enqueue_when_no_readers() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));
        let _p = pool.pin(6, PageSize::Large32k).unwrap();
        drop(_p);
        // Allocator with enough "pages" on the header so `free_32k`
        // accepts page 777 — the MockIo underneath accepts any write.
        let alloc = allocator_with_capacity(1024);
        let registry = ReadViewRegistry::new();

        // Overflow-backed entry that will age out.
        let oref = OverflowRef::new_owned(777, 1024, alloc.clone()).unwrap();
        assert_eq!(alloc.overflow_refcount(777), 1);

        let mut chain = VecDeque::new();
        chain.push_back(entry_inline(ts(200), Ts::MAX, 10, b"head")); // live
        chain.push_back(VersionEntry {
            start_ts: ts(10),
            stop_ts: ts(20),
            txn_id: 11,
            data: VersionData::Overflow(oref),
            is_tombstone: false,
        });
        install_chain(&pool, 6, b"K", chain);
        assert_eq!(alloc.overflow_refcount(777), 1);

        // No live readers → ort = Ts::MAX → older entry drops, its
        // OverflowRef decrefs to 0, page 777 lands on the deferred-free
        // queue, and drain_free_queue releases it to the allocator's
        // free list.
        let depth_before = metrics::overflow_pages_freed_snapshot();
        let dropped = pool.reconcile(6, &registry, &alloc).unwrap();
        assert_eq!(dropped, 1);
        assert_eq!(alloc.overflow_refcount(777), 0);
        assert!(
            metrics::overflow_pages_freed_snapshot() > depth_before,
            "drain must record at least one freed page"
        );
        assert_eq!(
            alloc.deferred_free_queue().depth(),
            0,
            "queue drained"
        );
    }

    #[test]
    fn reconcile_non_resident_page_is_noop() {
        let io = MockIo::new();
        let pool = desktop_pool(Arc::clone(&io));
        let alloc = fresh_allocator();
        let registry = ReadViewRegistry::new();
        // Page 99 was never pinned — not resident.
        let dropped = pool.reconcile(99, &registry, &alloc).unwrap();
        assert_eq!(dropped, 0);
    }

    #[test]
    fn sec_index_tombstone_chain_retains_when_reader_active() {
        // Tombstone-only chain with a single tombstone entry whose
        // stop_ts = Ts::MAX (live). A reader at ts=50 must still see
        // the tombstone — reconcile must leave it in place.
        let (pool, alloc, _io) = pool_with_resident_leaf(7);
        let registry = Arc::new(ReadViewRegistry::new());
        let _view = ReadView::open(Arc::clone(&registry), ts(50), 500);

        let mut chain = VecDeque::new();
        chain.push_back(tombstone(ts(100), Ts::MAX, 1));
        install_chain(&pool, 7, b"sec-idx-key", chain);

        let dropped = pool.reconcile(7, &registry, &alloc).unwrap();
        assert_eq!(dropped, 0);
        assert!(!pool.chains_empty(7).unwrap());
    }
}
