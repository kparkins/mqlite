use super::*;
use std::collections::HashMap;
use std::sync::Mutex;

// -----------------------------------------------------------------------
// MockIo — in-memory PageSource for tests
// -----------------------------------------------------------------------

/// In-memory page store for testing.  Pages are stored as raw byte vectors
/// keyed by page number.  Reads of absent pages return zeroed bytes.
struct MockIo {
    pages: Mutex<HashMap<u32, Vec<u8>>>,
}

impl MockIo {
    fn new() -> Self {
        Self {
            pages: Mutex::new(HashMap::new()),
        }
    }

    /// Return the raw bytes stored for `page_number`, or `None` if the
    /// page has never been written.
    fn get_raw(&self, page_number: u32) -> Option<Vec<u8>> {
        self.pages
            .lock()
            .expect("MockIo lock poisoned")
            .get(&page_number)
            .cloned()
    }
}

impl PageSource for MockIo {
    fn read_page(&self, page_number: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
        assert_eq!(buf.len(), size.bytes(), "buf.len() must equal size.bytes()");
        let pages = self.pages.lock().expect("MockIo lock poisoned");
        if let Some(stored) = pages.get(&page_number) {
            buf.copy_from_slice(&stored[..buf.len()]);
        }
        // Absent pages read as zeroes — buf already zero-initialised by caller.
        Ok(())
    }

    fn write_page(&self, page_number: u32, size: PageSize, buf: &[u8]) -> Result<()> {
        assert_eq!(buf.len(), size.bytes(), "buf.len() must equal size.bytes()");
        self.pages
            .lock()
            .expect("MockIo lock poisoned")
            .insert(page_number, buf.to_vec());
        Ok(())
    }
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

/// A fresh header with total_page_count = 1 (page 0 = header).
fn fresh_header() -> FileHeader {
    FileHeader::new(0, 0, 0)
}

/// A page-sized scratch buffer for the [`PageAllocator`] free-list link I/O.
///
/// Production code owns this on `AllocatorState` and reuses it across calls;
/// these direct-construction unit tests allocate a fresh one per `PageAllocator`.
fn link_scratch() -> Vec<u8> {
    vec![0u8; PageSize::Large32k.bytes()]
}

// -----------------------------------------------------------------------
// Allocate — empty free list (extend file)
// -----------------------------------------------------------------------

#[test]
fn allocate_4k_from_empty_freelist_returns_page_1() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    let mut scratch = link_scratch();
    let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);

    let pn = alloc.allocate_4k().expect("should allocate");
    assert_eq!(pn, 1, "first allocated page must be 1");
}

#[test]
fn allocate_32k_from_empty_freelist_returns_page_1() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    let mut scratch = link_scratch();
    let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);

    let pn = alloc.allocate_32k().expect("should allocate");
    assert_eq!(pn, 1);
}

#[test]
fn allocate_extends_total_page_count() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    assert_eq!(hdr.total_page_count, 1);

    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.allocate_4k().unwrap();
        alloc.allocate_32k().unwrap();
        alloc.allocate_4k().unwrap();
    }

    assert_eq!(hdr.total_page_count, 4, "three allocations → page count 4");
}

#[test]
fn sequential_allocations_return_consecutive_page_numbers() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    let mut scratch = link_scratch();
    let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);

    let a = alloc.allocate_4k().unwrap();
    let b = alloc.allocate_4k().unwrap();
    let c = alloc.allocate_32k().unwrap();

    assert_eq!(a, 1);
    assert_eq!(b, 2);
    assert_eq!(c, 3);
}

// -----------------------------------------------------------------------
// Free — basic
// -----------------------------------------------------------------------

#[test]
fn free_4k_updates_header_fields() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 3; // pretend pages 1 and 2 exist

    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.free_4k(1).unwrap();
    }

    assert_eq!(hdr.free_list_head_4k, 1, "head must point to freed page");
    assert_eq!(hdr.free_page_count_4k, 1);
    assert_eq!(hdr.free_list_head_32k, 0, "32k list untouched");
}

#[test]
fn free_32k_updates_header_fields() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 3;

    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.free_32k(2).unwrap();
    }

    assert_eq!(hdr.free_list_head_32k, 2);
    assert_eq!(hdr.free_page_count_32k, 1);
    assert_eq!(hdr.free_list_head_4k, 0, "4k list untouched");
}

#[test]
fn freed_page_stores_next_pointer_in_first_4_bytes_as_zero() {
    // When the free list was empty, the "next" link written to the freed
    // page must be 0 (end-of-list sentinel).
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 2;

    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.free_4k(1).unwrap();
    }

    let raw = io.get_raw(1).expect("page must have been written");
    let next = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
    assert_eq!(next, 0, "single free page → next = 0");
    // All bytes beyond the link must be zero.
    assert!(
        raw[4..].iter().all(|&b| b == 0),
        "tail bytes must be zeroed"
    );
}

#[test]
fn freed_page_stores_next_pointer_when_list_nonempty() {
    // Free page 1 first (head = 1), then free page 2 (new head = 2, next = 1).
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 3;

    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.free_4k(1).unwrap();
        alloc.free_4k(2).unwrap();
    }

    // Page 2 is the new head; its link must point to page 1.
    let raw2 = io.get_raw(2).unwrap();
    let next2 = u32::from_le_bytes([raw2[0], raw2[1], raw2[2], raw2[3]]);
    assert_eq!(next2, 1);

    assert_eq!(hdr.free_list_head_4k, 2);
    assert_eq!(hdr.free_page_count_4k, 2);
}

// -----------------------------------------------------------------------
// Allocate — from non-empty free list (recycle)
// -----------------------------------------------------------------------

#[test]
fn free_then_alloc_recycles_page_4k() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 2;

    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.free_4k(1).unwrap();
        let recycled = alloc.allocate_4k().unwrap();
        assert_eq!(recycled, 1, "must reuse freed page");
        // Free list should be empty again.
        assert_eq!(hdr.free_list_head_4k, 0);
        assert_eq!(hdr.free_page_count_4k, 0);
        // total_page_count unchanged (no file extension needed).
        assert_eq!(hdr.total_page_count, 2);
    }
}

#[test]
fn free_then_alloc_recycles_page_32k() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 2;

    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.free_32k(1).unwrap();
        let recycled = alloc.allocate_32k().unwrap();
        assert_eq!(recycled, 1);
        assert_eq!(hdr.free_list_head_32k, 0);
        assert_eq!(hdr.free_page_count_32k, 0);
    }
}

#[test]
fn alloc_from_freelist_is_lifo_4k() {
    // Free pages 1, 2, 3 in order → list is [3 → 2 → 1].
    // Allocating must return 3, then 2, then 1.
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 4;

    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.free_4k(1).unwrap();
        alloc.free_4k(2).unwrap();
        alloc.free_4k(3).unwrap();

        assert_eq!(alloc.allocate_4k().unwrap(), 3);
        assert_eq!(alloc.allocate_4k().unwrap(), 2);
        assert_eq!(alloc.allocate_4k().unwrap(), 1);
        // List exhausted; next allocation extends file.
        assert_eq!(alloc.allocate_4k().unwrap(), 4);
    }

    assert_eq!(hdr.total_page_count, 5);
    assert_eq!(hdr.free_page_count_4k, 0);
}

#[test]
fn alloc_from_freelist_is_lifo_32k() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 3;

    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.free_32k(1).unwrap();
        alloc.free_32k(2).unwrap();

        assert_eq!(alloc.allocate_32k().unwrap(), 2);
        assert_eq!(alloc.allocate_32k().unwrap(), 1);
    }
}

#[test]
fn free_and_alloc_many_pages_no_leak_4k() {
    let io = MockIo::new();
    let mut hdr = fresh_header();

    // Allocate 10 pages.
    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        for _ in 1..=10 {
            alloc.allocate_4k().unwrap();
        }
    }
    assert_eq!(hdr.total_page_count, 11);

    // Free all 10 pages.
    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        for pn in 1..=10 {
            alloc.free_4k(pn).unwrap();
        }
    }
    assert_eq!(hdr.free_page_count_4k, 10);

    // Reallocate all 10; they must come from the free list, not extend
    // the file.
    let reclaimed = {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        let mut pages = Vec::new();
        for _ in 0..10 {
            pages.push(alloc.allocate_4k().unwrap());
        }
        pages
    };
    assert_eq!(hdr.total_page_count, 11, "file must not have grown");
    assert_eq!(hdr.free_page_count_4k, 0);

    // All reclaimed pages must be in [1, 10].
    for pn in &reclaimed {
        assert!(
            (1..=10).contains(pn),
            "reclaimed page {pn} out of expected range"
        );
    }
}

// -----------------------------------------------------------------------
// Error cases
// -----------------------------------------------------------------------

#[test]
fn free_page_0_returns_error() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 2;

    let mut scratch = link_scratch();
    let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
    let result = alloc.free_4k(0);
    assert!(result.is_err(), "freeing page 0 must fail");
}

#[test]
fn free_page_0_returns_error_32k() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 2;

    let mut scratch = link_scratch();
    let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
    let result = alloc.free_32k(0);
    assert!(result.is_err(), "freeing page 0 (32k) must fail");
}

#[test]
fn free_page_beyond_file_returns_error_4k() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 2; // pages 0 and 1 exist

    let mut scratch = link_scratch();
    let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
    // Page 5 does not exist.
    let result = alloc.free_4k(5);
    assert!(result.is_err(), "freeing out-of-bounds page must fail");
}

#[test]
fn free_page_beyond_file_returns_error_32k() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 2;

    let mut scratch = link_scratch();
    let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
    let result = alloc.free_32k(99);
    assert!(result.is_err());
}

#[test]
fn allocate_overflow_returns_disk_full() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = u32::MAX; // one short of overflow

    let mut scratch = link_scratch();
    let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
    // Allocating would push total_page_count past u32::MAX.
    let result = alloc.allocate_4k();
    assert!(
        matches!(
            result,
            Err(Error::DiskFull {
                available_bytes: 0,
                ..
            })
        ),
        "u32 overflow must return DiskFull, got {result:?}"
    );
}

// -----------------------------------------------------------------------
// Independent lists — 4 KB and 32 KB are separate
// -----------------------------------------------------------------------

#[test]
fn lists_are_independent() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 3;

    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.free_4k(1).unwrap();
        alloc.free_32k(2).unwrap();
    }

    assert_eq!(hdr.free_list_head_4k, 1);
    assert_eq!(hdr.free_list_head_32k, 2);
    assert_eq!(hdr.free_page_count_4k, 1);
    assert_eq!(hdr.free_page_count_32k, 1);

    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        // Allocating 32k should not touch the 4k list.
        let p = alloc.allocate_32k().unwrap();
        assert_eq!(p, 2);
        assert_eq!(hdr.free_list_head_4k, 1, "4k list must be unchanged");
        assert_eq!(hdr.free_page_count_4k, 1);
    }
}

// -----------------------------------------------------------------------
// Roundtrip: allocate → free → reallocate preserves page number
// -----------------------------------------------------------------------

#[test]
fn roundtrip_single_4k() {
    let io = MockIo::new();
    let mut hdr = fresh_header();

    let page_number = {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.allocate_4k().unwrap()
    };
    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.free_4k(page_number).unwrap();
    }
    let recycled = {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.allocate_4k().unwrap()
    };
    assert_eq!(recycled, page_number);
}

#[test]
fn roundtrip_single_32k() {
    let io = MockIo::new();
    let mut hdr = fresh_header();

    let page_number = {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.allocate_32k().unwrap()
    };
    {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.free_32k(page_number).unwrap();
    }
    let recycled = {
        let mut scratch = link_scratch();
        let mut alloc = PageAllocator::new(&mut hdr, &io, &mut scratch);
        alloc.allocate_32k().unwrap()
    };
    assert_eq!(recycled, page_number);
}

// -----------------------------------------------------------------------
// AllocatorHandle tests
// -----------------------------------------------------------------------

#[test]
fn handle_alloc_4k_returns_correct_page() {
    let io = MockIo::new();
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);

    let page = handle.alloc_4k(&io).unwrap();
    assert_eq!(page, 1, "first alloc must be page 1");
}

#[test]
fn handle_alloc_32k_returns_correct_page() {
    let io = MockIo::new();
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);

    let page = handle.alloc_32k(&io).unwrap();
    assert_eq!(page, 1);
}

#[test]
fn handle_sequential_allocs_return_consecutive_pages() {
    let io = MockIo::new();
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);

    let a = handle.alloc_4k(&io).unwrap();
    let b = handle.alloc_32k(&io).unwrap();
    let c = handle.alloc_4k(&io).unwrap();

    assert_eq!(a, 1);
    assert_eq!(b, 2);
    assert_eq!(c, 3);
}

#[test]
fn handle_marks_header_dirty_after_alloc() {
    let io = MockIo::new();
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);

    assert!(
        !handle.is_header_dirty(),
        "header should be clean on create"
    );
    handle.alloc_4k(&io).unwrap();
    assert!(handle.is_header_dirty(), "header must be dirty after alloc");
}

#[test]
fn handle_flush_header_writes_to_page_0() {
    let io = MockIo::new();
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);

    handle.alloc_4k(&io).unwrap();
    handle.flush_header(&io).unwrap();

    assert!(
        !handle.is_header_dirty(),
        "header must be clean after flush"
    );
    // Page 0 must have been written.
    let raw = io.get_raw(0).expect("page 0 must be written on flush");
    assert_eq!(raw.len(), PageSize::Small4k.bytes());
}

#[test]
fn handle_flush_header_noop_when_clean() {
    let io = MockIo::new();
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);

    // No allocations — header is clean.
    handle.flush_header(&io).unwrap();

    assert!(
        io.get_raw(0).is_none(),
        "flush_header must not write page 0 when header is clean"
    );
}

#[test]
fn handle_free_and_realloc_recycles_page() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 3; // pretend pages 1 and 2 exist
    let handle = AllocatorHandle::new(hdr);

    handle.free_4k(1, &io).unwrap();
    let recycled = handle.alloc_4k(&io).unwrap();
    assert_eq!(recycled, 1, "freed page must be recycled");
}

#[test]
fn handle_with_header_reads_total_page_count() {
    let io = MockIo::new();
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);

    handle.alloc_4k(&io).unwrap();
    handle.alloc_32k(&io).unwrap();

    let count = handle.with_header(|h| h.total_page_count).unwrap();
    assert_eq!(count, 3, "two allocs from page 1 = total 3");
}

#[test]
fn handle_update_header_marks_dirty_and_persists() {
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);

    handle.update_header(|h| h.catalog_root_page = 42).unwrap();

    assert!(handle.is_header_dirty());
    let root = handle.with_header(|h| h.catalog_root_page).unwrap();
    assert_eq!(root, 42);
}

#[test]
fn handle_is_clone_and_shares_state() {
    let io = MockIo::new();
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);
    let handle2 = handle.clone();

    // Alloc through clone 1.
    handle.alloc_4k(&io).unwrap();

    // Clone 2 sees the updated state.
    let count = handle2.with_header(|h| h.total_page_count).unwrap();
    assert_eq!(count, 2, "clone must share underlying state");
}

// -----------------------------------------------------------------------
// MVCC T3 — overflow refcount contract
// -----------------------------------------------------------------------

#[test]
fn overflow_refcount_starts_at_zero_for_unknown_page() {
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);
    assert_eq!(handle.overflow_refcount(42), 0);
}

#[test]
fn incref_overflow_bumps_by_one() {
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);

    let n1 = handle.incref_overflow(42).unwrap();
    assert_eq!(n1, 1);
    let n2 = handle.incref_overflow(42).unwrap();
    assert_eq!(n2, 2);
    assert_eq!(handle.overflow_refcount(42), 2);
}

#[test]
fn decref_overflow_returns_post_decrement() {
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);

    handle.incref_overflow(7).unwrap();
    handle.incref_overflow(7).unwrap();
    let post1 = handle.decref_overflow(7);
    assert_eq!(post1, 1);
    let post0 = handle.decref_overflow(7);
    assert_eq!(post0, 0);
    assert_eq!(handle.overflow_refcount(7), 0);
}

#[test]
fn incref_overflow_saturates_at_u32_max_without_bumping() {
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);
    handle.set_overflow_refcount_for_test(99, u32::MAX);

    let err = handle.incref_overflow(99).unwrap_err();
    assert!(matches!(err, Error::RefcountOverflow));
    // Atomic value must remain u32::MAX — saturation bailout does not bump.
    assert_eq!(handle.overflow_refcount(99), u32::MAX);
}

#[test]
fn incref_overflow_contended_saturation_exact_500_winners() {
    // refcount = u32::MAX - 500, 8 threads x 125 calls each (1000 total).
    // Exactly 500 succeed, 500 return RefcountOverflow; final refcount == u32::MAX.
    use std::sync::Arc;
    let hdr = fresh_header();
    let handle = Arc::new(AllocatorHandle::new(hdr));
    handle.set_overflow_refcount_for_test(500, u32::MAX - 500);

    const THREADS: usize = 8;
    const PER_THREAD: usize = 125;

    #[allow(
        clippy::needless_collect,
        reason = "spawn all overflow refcount workers before joining them"
    )]
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let h = handle.clone();
            std::thread::spawn(move || {
                let mut oks = 0usize;
                let mut errs = 0usize;
                for _ in 0..PER_THREAD {
                    match h.incref_overflow(500) {
                        Ok(_) => oks += 1,
                        Err(Error::RefcountOverflow) => errs += 1,
                        Err(e) => panic!("unexpected error: {e:?}"),
                    }
                }
                (oks, errs)
            })
        })
        .collect();

    let (total_ok, total_err) = handles.into_iter().fold((0, 0), |acc, h| {
        let (ok, err) = h.join().unwrap();
        (acc.0 + ok, acc.1 + err)
    });

    assert_eq!(total_ok, 500, "exactly 500 incref calls must succeed");
    assert_eq!(total_err, 500, "exactly 500 calls must saturate");
    assert_eq!(handle.overflow_refcount(500), u32::MAX);
}

#[test]
fn drain_free_queue_frees_zero_refcount_pages() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 5; // pages 1..=4 exist
    let handle = AllocatorHandle::new(hdr);

    // Page 2 has refcount 0 (queued as if OverflowRef::drop happened).
    handle.incref_overflow(2).unwrap();
    let post = handle.decref_overflow(2);
    assert_eq!(post, 0);
    handle.enqueue_overflow_deferred_free(2);
    handle.advance_page_lifetime_checkpoint_fence();

    let freed = handle.drain_free_queue(&io).unwrap();
    assert_eq!(freed, 1);
    // Page 2 must now be on the 32k free list.
    let head = handle.with_header(|h| h.free_list_head_32k).unwrap();
    assert_eq!(head, 2);
}

#[test]
fn drain_free_queue_requeues_nonzero_refcount_pages() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 5;
    let handle = AllocatorHandle::new(hdr);

    // Page 3 enqueued but refcount is still 1 (e.g., a late re-bump).
    handle.incref_overflow(3).unwrap();
    handle.enqueue_overflow_deferred_free(3);
    handle.advance_page_lifetime_checkpoint_fence();

    let freed = handle.drain_free_queue(&io).unwrap();
    assert_eq!(freed, 0, "non-zero refcount page must not be freed");
    assert_eq!(handle.page_lifetime_queue().depth(), 1, "must be requeued");
    // Page 3 must NOT be on the free list.
    let head = handle.with_header(|h| h.free_list_head_32k).unwrap();
    assert_eq!(head, 0);
}

#[test]
fn drain_free_queue_empty_is_noop() {
    let io = MockIo::new();
    let hdr = fresh_header();
    let handle = AllocatorHandle::new(hdr);
    let freed = handle.drain_free_queue(&io).unwrap();
    assert_eq!(freed, 0);
}

#[test]
fn test_overflow_deferred_free_drain_after_fence() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 5;
    let handle = AllocatorHandle::new(hdr);

    handle.incref_overflow(2).unwrap();
    assert_eq!(handle.decref_overflow(2), 0);
    handle.enqueue_overflow_deferred_free(2);

    let freed_before_checkpoint = handle.drain_free_queue(&io).unwrap();
    assert_eq!(
        freed_before_checkpoint, 0,
        "refcount-zero page must wait until checkpoint fence advances"
    );
    assert_eq!(handle.page_lifetime_queue().depth(), 1);
    assert_eq!(handle.with_header(|h| h.free_list_head_32k).unwrap(), 0);

    handle.advance_page_lifetime_checkpoint_fence();

    let freed_after_checkpoint = handle.drain_free_queue(&io).unwrap();
    assert_eq!(freed_after_checkpoint, 1);
    assert_eq!(handle.page_lifetime_queue().depth(), 0);
    assert_eq!(handle.with_header(|h| h.free_list_head_32k).unwrap(), 2);
}

#[test]
fn drain_deferred_free_pages_wait_for_checkpoint_fence() {
    let mut hdr = fresh_header();
    hdr.total_page_count = 5;
    let handle = AllocatorHandle::new(hdr);

    handle.incref_overflow(4).unwrap();
    assert_eq!(handle.decref_overflow(4), 0);
    handle.enqueue_overflow_deferred_free(4);

    let reserved_before_checkpoint = handle.drain_deferred_free_pages();
    assert!(
        reserved_before_checkpoint.is_empty(),
        "lifetime drain must also honor the checkpoint fence"
    );
    assert_eq!(handle.page_lifetime_queue().depth(), 1);

    handle.advance_page_lifetime_checkpoint_fence();

    let reserved_after_checkpoint = handle.drain_deferred_free_pages();
    assert_eq!(reserved_after_checkpoint, vec![4]);
    assert_eq!(handle.page_lifetime_queue().depth(), 0);
}

#[test]
fn page_lifetime_queue_requeues_nonzero_refcount_after_checkpoint() {
    let io = MockIo::new();
    let mut hdr = fresh_header();
    hdr.total_page_count = 5;
    let handle = AllocatorHandle::new(hdr);

    handle.incref_overflow(3).unwrap();
    handle.enqueue_overflow_deferred_free(3);
    handle.advance_page_lifetime_checkpoint_fence();

    let freed = handle.drain_free_queue(&io).unwrap();
    assert_eq!(freed, 0, "non-zero refcount page must not be freed");
    assert_eq!(handle.page_lifetime_queue().depth(), 1, "must be requeued");
    assert_eq!(handle.with_header(|h| h.free_list_head_32k).unwrap(), 0);
}
