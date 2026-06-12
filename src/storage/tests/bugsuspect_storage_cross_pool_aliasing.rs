//! Bug-suspect: cross-pool page aliasing on the history-page free path.
//!
//! Suspect (deep-refactor-2026-06-10, rank ~5, handle/mod.rs `free_page`
//! ~:295-301 and `alloc_page_history` ~:274-284): free-list link I/O ALWAYS
//! routes through the MAIN pool's `pool_io`, regardless of which pool owns the
//! page, and nothing invalidates the other pool's frame. Freeing a history
//! leaf therefore writes the free-list link into a MAIN-pool frame for a page
//! that is still resident (and dirty) in the HISTORY pool — the page becomes
//! resident in BOTH pools at once.
//!
//! `BufferPoolHandle::validate_checkpoint_flush_set` (handle/checkpoint_flush.rs)
//! treats a page resident+dirty in both pools as a hard `Error::Internal`
//! ("dirty page N is resident in both checkpoint pools") — the exact invariant
//! these free/alloc paths structurally violate. On flush the order is
//! main-then-history, so a stale dirty history frame can later overwrite the
//! just-written free-list link bytes on disk; a stale resident main frame can
//! serve wrong bytes for a recycled page. Manifests on reopen as free-list /
//! history-tree corruption.
//!
//! This test reproduces the dual-residency directly at the handle level: a
//! 32 KiB page allocated + dirtied in the HISTORY pool, then freed, becomes
//! dirty in BOTH pools. FAILS today (the intersection is non-empty). The fix
//! must route the free-list link write through the owning pool (or invalidate
//! the other pool's frame on free).

use super::*;
use crate::storage::buffer_pool::default_sizes;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

fn make_handle() -> BufferPoolHandle {
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
    BufferPoolHandle::new(pool, history_pool, header)
}

#[test]
fn freeing_a_history_page_does_not_alias_it_into_the_main_pool() {
    let handle = make_handle();

    // 1. Allocate a 32 KiB page on the HISTORY pool (this is how spilled MVCC
    //    history leaves are allocated). It is now resident + dirty in the
    //    history pool.
    let p = handle
        .alloc_page_history(PageSize::Large32k)
        .expect("alloc history page");

    // 2. Write recognizable history bytes so the page is meaningfully dirty in
    //    the history pool and stays resident.
    {
        let mut page = handle
            .history_pool()
            .pin(p, PageSize::Large32k)
            .expect("pin history page");
        page.data_mut()[0] = 0xA7; // marker byte
    }

    // Precondition: the page is owned only by the history pool.
    let main_before = handle.pool().dirty_page_ids().expect("main dirty ids");
    let hist_before = handle
        .history_pool()
        .dirty_page_ids()
        .expect("history dirty ids");
    assert!(
        hist_before.iter().any(|id| id.0 == p),
        "history page must be dirty in the history pool"
    );
    assert!(
        !main_before.iter().any(|id| id.0 == p),
        "history page must NOT be resident in the main pool before the free"
    );

    // 3. Free the history page. `free_page` routes the free-list link write
    //    through the MAIN pool's `pool_io`, with no invalidation of the
    //    history-pool frame.
    handle
        .free_page(p, PageSize::Large32k)
        .expect("free history page");

    // 4. The page is now dirty in BOTH pools — the dual-residency invariant
    //    that `validate_checkpoint_flush_set` rejects as a hard error.
    let main_dirty = handle.pool().dirty_page_ids().expect("main dirty ids");
    let hist_dirty = handle
        .history_pool()
        .dirty_page_ids()
        .expect("history dirty ids");

    let aliased = main_dirty
        .iter()
        .find(|id| hist_dirty.iter().any(|h| h.0 == id.0))
        .map(|id| id.0);

    assert_eq!(
        aliased, None,
        "BUG: history page {p} is dirty in BOTH the main and history pools \
         after free_page routed the free-list link write through the main \
         pool without invalidating the history-pool frame. On flush \
         (main-then-history) the stale history frame overwrites the free-list \
         link on disk — free-list / history-tree corruption on reopen. \
         free_page must route through the owning pool or invalidate the \
         other pool's frame."
    );
}
