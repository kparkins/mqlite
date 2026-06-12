//! F9 regression: a dropped tree's overflow page whose refcount is still
//! positive at retire time must inherit the drop's reader fence when its
//! final decref enqueues it.
//!
//! `retire_dropped_tree_pages` SKIPS 32 KiB pages with `refcount > 0` —
//! their lifetime stays owned by the `OverflowRef` RAII discipline. But the
//! final decref used to enqueue a plain `OverflowDeferredFree` entry with NO
//! `reader_fence_ts`, released on the checkpoint fence alone. A registered
//! pre-drop reader can still reach those pages through a base-leaf overflow
//! pointer (the scan path takes no refcount), so a checkpoint running while
//! that reader lives freed the page out from under its snapshot.
//!
//! Scenario pinned here: overflow page P at refcount 2 (resident chain
//! entry + base image) in a dropped tree; retire skips P; both refs drop
//! (refcount -> 0, final decref enqueues); a checkpoint advances the fence
//! while a pre-drop reader is still registered — P must NOT be freed; after
//! the reader ends, the next checkpoint must free P.

use std::sync::Arc;

use bson::doc;

use super::*;
use crate::storage::buffer_pool::{default_sizes, BufferPool, PageSize};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

fn buffered_engine() -> PagedEngine {
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let header = FileHeader::new_now();
    let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
    PagedEngine::new_buffered(handle, 0, 0).expect("create buffered engine")
}

/// Walk the on-header 32 KiB free list (pool-coherent reads) and report
/// whether `page` is currently on it.
fn free_list_32k_contains(engine: &PagedEngine, page: u32) -> bool {
    let mut head = engine
        .shared
        .handle
        .allocator()
        .with_header(|h| h.free_list_head_32k)
        .expect("read free list head");
    let mut steps = 0u32;
    while head != 0 && steps < 10_000 {
        if head == page {
            return true;
        }
        let pin = engine
            .shared
            .handle
            .fetch_page(head, PageSize::Large32k)
            .expect("pin free-list link page");
        let d = pin.data();
        head = u32::from_le_bytes([d[0], d[1], d[2], d[3]]);
        steps += 1;
    }
    false
}

#[test]
fn refcounted_retired_page_final_decref_inherits_drop_reader_fence() {
    const NS: &str = "test.f9.aux";
    let engine = buffered_engine();
    engine.create_namespace(NS).expect("create namespace");

    // Reader pinned to the current (pre-"drop") epoch, registered in the
    // ReadViewRegistry so it feeds the reader floor.
    let pre_epoch = engine.shared.load_published();
    let view = super::snapshot_ops::open_snapshot_read_view_for_epoch(
        &engine.shared,
        Arc::clone(&pre_epoch),
    )
    .expect("register pre-drop reader");

    // Advance the published visible_ts past the reader so the drop's
    // reader fence sorts strictly above the registered view.
    engine
        .insert(NS, doc! { "_id": 1, "value": "advance-ts" })
        .expect("insert advances the published epoch");
    assert!(
        engine.shared.published.load_full().visible_ts > pre_epoch.visible_ts,
        "publish must advance past the registered reader"
    );

    // Overflow page P of the dropped tree at refcount 2 (resident chain
    // entry + base image).
    let p = engine
        .shared
        .handle
        .alloc_page(PageSize::Large32k)
        .expect("allocate overflow page");
    let alloc = engine.shared.handle.allocator().clone();
    alloc.incref_overflow(p).expect("base image ref");
    alloc.incref_overflow(p).expect("resident chain entry ref");

    // Retire after the (modeled) drop committed and published: refcount > 0,
    // so P is not enqueued here — its lifetime stays with the RAII decrefs.
    engine.retire_dropped_tree_pages(&[(p, PageSize::Large32k)]);
    assert!(
        !free_list_32k_contains(&engine, p),
        "retire must never free a page directly"
    );

    // Final RAII decrefs (chain entry drop + base image release) — this is
    // exactly what `OverflowRef::drop` runs.
    assert_eq!(alloc.decref_overflow(p), 1);
    assert_eq!(alloc.decref_overflow(p), 0);
    alloc.enqueue_overflow_deferred_free(p);

    // Checkpoint: the lifetime fence advances, but the pre-drop reader is
    // still registered — P must NOT be released to the free list.
    engine
        .checkpoint()
        .expect("checkpoint with registered pre-drop reader");
    assert!(
        !free_list_32k_contains(&engine, p),
        "page {p} was freed while a pre-drop reader was still registered — \
         the final decref's enqueue escaped the drop's reader fence"
    );

    // Reader ends; the next checkpoint may (and must) release P.
    drop(view);
    engine.checkpoint().expect("checkpoint after reader ends");
    engine
        .checkpoint()
        .expect("second checkpoint after reader ends");
    assert!(
        free_list_32k_contains(&engine, p),
        "page {p} must be released once no pre-drop reader remains"
    );
}
