//! F8 regression: a dropped tree's retired pages must be released to the
//! free list ONLY through pool-coherent io.
//!
//! `PageAllocator::free_*` writes the freed page's free-list link (the
//! "next free page" pointer in the first 4 bytes) through the CALLER'S
//! `PageSource`, while `PageAllocator::allocate` reads the link back
//! through the io its caller provides — in production always the
//! pool-coherent `BufferPoolPageSource`. The hot page-lifetime drain in
//! `BufferPool::pin_with_reconcile` used to release `RetiredTree*` entries
//! through the pool's RAW backing `PageSource`: nothing invalidates the
//! dropped tree's still-resident frames, so the raw link write landed on
//! disk while the pool kept serving OLD TREE BYTES. The next `alloc_*`
//! popped the freed page and read its next-free pointer from the stale
//! resident frame — the free-list head became arbitrary garbage and a
//! later alloc could hand out a live page (silent two-owner corruption).
//!
//! Interleaving pinned here: drop publishes -> retire enqueues ->
//! checkpoint advances the lifetime fence but the reader floor is blocked
//! by a registered stale view -> the view ends -> the next hot pin's drain
//! must NOT release the retired entries (it used to, through raw io) ->
//! the checkpoint drain (pool-coherent `pool_io`) releases them -> every
//! subsequent alloc must pop a real retired page exactly once.

use std::collections::BTreeSet;
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

fn free_counts(engine: &PagedEngine) -> (u32, u32) {
    engine
        .shared
        .handle
        .allocator()
        .with_header(|h| (h.free_page_count_4k, h.free_page_count_32k))
        .expect("read header free counts")
}

#[test]
fn released_retired_pages_allocate_back_without_free_list_corruption() {
    const NS: &str = "test.f8.poolcoherence";
    const DOCS: i32 = 30;
    let engine = buffered_engine();
    engine.create_namespace(NS).expect("create namespace");
    for id in 1..=DOCS {
        engine
            .insert(
                NS,
                doc! { "_id": id, "value": format!("doc-{id}"), "pad": "x".repeat(3000) },
            )
            .expect("insert padded document");
    }
    // Materialize the tree so the root splits into real 4 KiB internals.
    engine.checkpoint().expect("checkpoint materializes the tree");

    assert_eq!(
        free_counts(&engine),
        (0, 0),
        "setup precondition: both free lists empty before the drop"
    );

    let pre_drop_epoch = engine.shared.load_published();
    let ns_snap = pre_drop_epoch
        .catalog
        .get_by_name(NS)
        .expect("pre-drop epoch must map the namespace");
    assert!(
        ns_snap.data_root_level > 0,
        "setup must build a multi-level tree with 4 KiB internals \
         (got root level {})",
        ns_snap.data_root_level
    );

    // The dropped tree's full page set, by allocator size class.
    let collected = engine
        .collect_tree_pages(ns_snap.data_root_page, ns_snap.data_root_level)
        .expect("collect tree pages");
    let retired_4k: BTreeSet<u32> = collected
        .iter()
        .filter(|(_, s)| *s == PageSize::Small4k)
        .map(|(p, _)| *p)
        .collect();
    let retired_32k: BTreeSet<u32> = collected
        .iter()
        .filter(|(_, s)| *s == PageSize::Large32k)
        .map(|(p, _)| *p)
        .collect();
    assert!(
        !retired_4k.is_empty(),
        "multi-level tree must own at least one 4 KiB internal"
    );
    assert!(
        !retired_32k.is_empty(),
        "tree must own at least one 32 KiB page"
    );

    // Registered stale reader pinned to the pre-drop epoch: opened BEFORE
    // the drop (the drop's sweep force-expires it, but it stays REGISTERED
    // while held — the reader floor keys off read_ts, not poison), so it
    // blocks the reader floor and the retired entries survive the next
    // checkpoint. F36: a post-publish open would now fail the open-time
    // generation revalidation instead of registering.
    let view = super::snapshot_ops::open_snapshot_read_view_for_epoch(
        &engine.shared,
        Arc::clone(&pre_drop_epoch),
    )
    .expect("register pre-drop reader");

    engine.drop_namespace(NS).expect("drop namespace");

    // Fence advances; floor blocked -> nothing may be released.
    engine
        .checkpoint()
        .expect("checkpoint with blocked reader floor");
    assert_eq!(
        free_counts(&engine),
        (0, 0),
        "retired pages must stay queued while a pre-drop reader is registered"
    );

    // Reader ends; the floor clears.
    drop(view);

    // Hot pin path: `pin_with_reconcile` drains the page-lifetime queue
    // through the pool's RAW backing io. Retired entries must NOT be
    // released here — a raw-io `free_*` writes the free-list link straight
    // to the backing file underneath the dropped tree's still-resident
    // frames, and the pool-coherent link reads below then pop garbage.
    drop(
        engine
            .shared
            .handle
            .fetch_page(0, PageSize::Small4k)
            .expect("hot pin"),
    );

    // The checkpoint drain (pool-coherent io) is the only legal retired
    // release path.
    engine
        .checkpoint()
        .expect("checkpoint releases retired pages");

    let (n4, n32) = free_counts(&engine);
    assert_eq!(
        n4 as usize,
        retired_4k.len(),
        "all retired 4 KiB pages must be released once the floor clears"
    );
    assert!(
        n32 >= 1,
        "at least the dropped tree's leaves must be released"
    );

    // Pop the ENTIRE 4 KiB free list: every page must be one of the
    // dropped tree's 4 KiB pages, each handed out exactly once. Any other
    // page number means a next-free link was read from a stale resident
    // frame (raw-io free) — the F8 free-list corruption.
    let mut seen_4k = BTreeSet::new();
    for i in 0..n4 {
        let p = engine
            .shared
            .handle
            .alloc_page(PageSize::Small4k)
            .unwrap_or_else(|e| panic!("4 KiB alloc #{i} failed — free-list corruption: {e}"));
        assert!(
            retired_4k.contains(&p),
            "4 KiB alloc #{i} returned page {p}, which is not a retired tree \
             page — free-list link corruption"
        );
        assert!(
            seen_4k.insert(p),
            "4 KiB alloc #{i} returned page {p} twice — two-owner corruption"
        );
    }

    // Same integrity sweep for the 32 KiB list.
    let mut seen_32k = BTreeSet::new();
    for i in 0..n32 {
        let p = engine
            .shared
            .handle
            .alloc_page(PageSize::Large32k)
            .unwrap_or_else(|e| panic!("32 KiB alloc #{i} failed — free-list corruption: {e}"));
        assert!(
            retired_32k.contains(&p),
            "32 KiB alloc #{i} returned page {p}, which is not a retired tree \
             page — free-list link corruption"
        );
        assert!(
            seen_32k.insert(p),
            "32 KiB alloc #{i} returned page {p} twice — two-owner corruption"
        );
    }
}
