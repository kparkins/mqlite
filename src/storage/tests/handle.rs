use super::*;
use crate::storage::buffer_pool::default_sizes;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

fn make_handle() -> (Arc<MockIo>, BufferPoolHandle) {
    let io = MockIo::new();
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let header = FileHeader::new_now();
    let handle = BufferPoolHandle::new(pool, history_pool, header);
    (io, handle)
}

// -----------------------------------------------------------------------
// fetch_page
// -----------------------------------------------------------------------

#[test]
fn fetch_page_returns_pinned_page() {
    let (io, handle) = make_handle();

    // Seed page 1 with a known pattern.
    {
        let mut pages = io.pages.lock().unwrap();
        let mut data = vec![0u8; PageSize::Large32k.bytes()];
        data[0] = 0xAB;
        pages.insert(1, data);
    }

    let page = handle.fetch_page(1, PageSize::Large32k).unwrap();
    assert_eq!(page.data()[0], 0xAB);
    assert_eq!(page.page_number(), 1);
}

// -----------------------------------------------------------------------
// alloc_page
// -----------------------------------------------------------------------

#[test]
fn alloc_page_returns_page_1_on_fresh_header() {
    let (_, handle) = make_handle();
    let pn = handle.alloc_page(PageSize::Large32k).unwrap();
    assert_eq!(pn, 1);
}

#[test]
fn alloc_page_increments_total_page_count() {
    let (_, handle) = make_handle();
    handle.alloc_page(PageSize::Large32k).unwrap();
    handle.alloc_page(PageSize::Small4k).unwrap();

    let count = handle
        .allocator()
        .with_header(|h| h.total_page_count)
        .unwrap();
    assert_eq!(count, 3);
}

#[test]
fn alloc_page_zeroes_the_new_frame() {
    let (io, handle) = make_handle();

    // Seed the backing store with a non-zero pattern at page 1.
    {
        let mut pages = io.pages.lock().unwrap();
        pages.insert(1, vec![0xFFu8; PageSize::Large32k.bytes()]);
    }

    let pn = handle.alloc_page(PageSize::Large32k).unwrap();
    assert_eq!(pn, 1);

    // The buffer pool should have the page zeroed (overriding the
    // backing store content) and marked dirty.
    let page = handle.fetch_page(pn, PageSize::Large32k).unwrap();
    assert!(
        page.data().iter().all(|&b| b == 0),
        "newly allocated page must be zeroed"
    );
}

// -----------------------------------------------------------------------
// free_page
// -----------------------------------------------------------------------

#[test]
fn free_and_realloc_recycles_page() {
    let (_, handle) = make_handle();

    // Allocate two pages, then free the first.
    let p1 = handle.alloc_page(PageSize::Large32k).unwrap();
    let _p2 = handle.alloc_page(PageSize::Large32k).unwrap();

    handle.free_page(p1, PageSize::Large32k).unwrap();

    // Next alloc must recycle p1.
    let recycled = handle.alloc_page(PageSize::Large32k).unwrap();
    assert_eq!(recycled, p1, "freed page must be recycled");
}

// -----------------------------------------------------------------------
// flush
// -----------------------------------------------------------------------

#[test]
fn flush_writes_dirty_data_page() {
    let (io, handle) = make_handle();

    let pn = handle.alloc_page(PageSize::Large32k).unwrap();
    {
        let mut page = handle.fetch_page(pn, PageSize::Large32k).unwrap();
        page.data_mut()[0] = 0x77;
    }

    handle.flush().unwrap();

    let pages = io.pages.lock().unwrap();
    let written = pages.get(&pn).expect("page must be written after flush");
    assert_eq!(written[0], 0x77, "flush must write modified page content");
}

#[test]
fn flush_writes_header_page_0_when_dirty() {
    let (io, handle) = make_handle();

    handle.alloc_page(PageSize::Large32k).unwrap();
    handle.flush().unwrap();

    let pages = io.pages.lock().unwrap();
    assert!(
        pages.contains_key(&0),
        "flush must write header page 0 after allocation"
    );
}

#[test]
fn flush_does_not_write_header_when_clean() {
    let (io, handle) = make_handle();

    // No allocations — header is clean.
    handle.flush().unwrap();

    let pages = io.pages.lock().unwrap();
    assert!(
        !pages.contains_key(&0),
        "flush must not write header when no allocations occurred"
    );
}

// -----------------------------------------------------------------------
// BufferPoolPageSource
// -----------------------------------------------------------------------

#[test]
fn pool_io_read_page_routes_through_pool() {
    let io = MockIo::new();
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));

    // Seed the backing store with a known pattern at page 5.
    {
        let mut pages = io.pages.lock().unwrap();
        let mut data = vec![0u8; PageSize::Large32k.bytes()];
        data[0] = 0x55;
        pages.insert(5, data);
    }

    let pool_io = BufferPoolPageSource::new(Arc::clone(&pool));
    let mut buf = vec![0u8; PageSize::Large32k.bytes()];
    pool_io.read_page(5, PageSize::Large32k, &mut buf).unwrap();

    assert_eq!(buf[0], 0x55);
}

#[test]
fn pool_io_write_page_marks_frame_dirty() {
    let (io, handle) = make_handle();

    // Pre-pin page 2 into the pool (so it's in cache).
    let _ = handle.fetch_page(2, PageSize::Small4k).unwrap();

    let pool_io = BufferPoolPageSource::new(Arc::clone(handle.pool()));
    let data = vec![0xAAu8; PageSize::Small4k.bytes()];
    pool_io.write_page(2, PageSize::Small4k, &data).unwrap();

    // Flush should write the modified content to the backing store.
    handle.flush().unwrap();

    let pages = io.pages.lock().unwrap();
    let written = pages.get(&2).expect("page must be written after flush");
    assert_eq!(written[0], 0xAA);
}
