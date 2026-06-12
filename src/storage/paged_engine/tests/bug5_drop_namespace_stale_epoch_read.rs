//! BUG-5 regression tests: `drop_namespace` force-expires only the
//! ReadViews that already exist (read_view.rs `force_expire_all`), but a
//! reader whose `find` is in flight — production `find` (doc_ops.rs) loads
//! the published epoch first and only afterwards opens its ReadView and
//! scans — can resolve the namespace from the stale pre-drop epoch with a
//! fresh NON-poisoned view. The drop used to clear every leaf's resident
//! chains and free the tree pages BEFORE the new `PublishedEpoch`
//! published, so such a reader silently returned an empty result for a
//! namespace its snapshot should fully see. The physical frees are now
//! deferred (page-lifetime queue, checkpoint-fence gated) until after the
//! durable commit + publish, leaving the dropped tree intact for
//! stale-epoch snapshots.

use std::sync::Arc;

use bson::doc;

use super::*;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
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
            .fetch_page(head, crate::storage::buffer_pool::PageSize::Large32k)
            .expect("pin free-list link page");
        let d = pin.data();
        head = u32::from_le_bytes([d[0], d[1], d[2], d[3]]);
        steps += 1;
    }
    false
}

/// Walk the on-header 4 KiB free list and report whether `page` is on it.
fn free_list_4k_contains(engine: &PagedEngine, page: u32) -> bool {
    let mut head = engine
        .shared
        .handle
        .allocator()
        .with_header(|h| h.free_list_head_4k)
        .expect("read free list head");
    let mut steps = 0u32;
    while head != 0 && steps < 10_000 {
        if head == page {
            return true;
        }
        let pin = engine
            .shared
            .handle
            .fetch_page(head, crate::storage::buffer_pool::PageSize::Small4k)
            .expect("pin free-list link page");
        let d = pin.data();
        head = u32::from_le_bytes([d[0], d[1], d[2], d[3]]);
        steps += 1;
    }
    false
}

/// Open a registered, NON-poisoned ReadView over `epoch` from INSIDE the
/// `drop_namespace` pre-commit window (after the force-expiry sweep,
/// before the publish): the production interleaving that leaves a live
/// pre-drop reader registered for the drop's whole retirement window, and
/// the only stale-view construction that survives the F36 open-time
/// generation revalidation.
fn open_view_inside_drop_window(
    engine: &PagedEngine,
    ns: &str,
    epoch: &Arc<crate::storage::root_snapshot::PublishedEpoch>,
) -> Arc<crate::mvcc::read_view::ReadView> {
    let mut guard =
        super::hidden_accessors::install_drop_namespace_before_commit_hook(&engine.shared);
    std::thread::scope(|s| {
        let dropper = s.spawn(|| engine.drop_namespace(ns));
        guard
            .wait_until_entered()
            .expect("drop_namespace must reach the pre-commit pause point");
        let view =
            super::snapshot_ops::open_snapshot_read_view_for_epoch(&engine.shared, Arc::clone(epoch))
                .expect("a view opened inside the drop window must register cleanly");
        guard.release().expect("release paused drop_namespace");
        dropper
            .join()
            .expect("drop_namespace thread panicked")
            .expect("drop_namespace must succeed");
        view
    })
}

fn insert_three(engine: &PagedEngine, ns: &str) {
    engine.create_namespace(ns).expect("create namespace");
    for id in 1..=3 {
        engine
            .insert(ns, doc! { "_id": id, "value": format!("doc-{id}") })
            .expect("insert document");
    }
}

/// A reader executing inside drop_namespace's [force_expire_all, publish]
/// window: the tree pages are collected for deferred retirement, the new
/// epoch is not yet published, and the fresh ReadView is not poisoned.
/// The snapshot read must return the full pre-drop data set or fail
/// cleanly — never a silent empty result.
#[test]
fn reader_inside_drop_window_sees_full_snapshot_or_fails_cleanly() {
    const NS: &str = "test.bug5.window";
    let engine = buffered_engine();
    insert_three(&engine, NS);

    let mut guard =
        super::hidden_accessors::install_drop_namespace_before_commit_hook(&engine.shared);

    let scan = std::thread::scope(|s| {
        let dropper = s.spawn(|| engine.drop_namespace(NS));
        guard
            .wait_until_entered()
            .expect("drop_namespace must reach the pre-commit pause point");

        // The reader's `find` body, executed inside the window: one
        // published-epoch load, then plan + fresh ReadView + scan.
        let epoch = engine.shared.load_published();
        let ns_snap = epoch
            .catalog
            .get_by_name(NS)
            .expect("stale epoch inside the drop window still maps the namespace");
        let scan = super::snapshot_ops::open_snapshot_read_view_for_epoch(
            &engine.shared,
            Arc::clone(&epoch),
        )
        .and_then(|view| {
            super::snapshot_ops::plan_and_collect_snapshot_pairs(
                &engine.shared,
                ns_snap,
                &doc! {},
                &view,
                false,
            )
        });

        guard.release().expect("release paused drop_namespace");
        dropper
            .join()
            .expect("drop_namespace thread panicked")
            .expect("drop_namespace must succeed");
        scan
    });

    // Inside the pre-commit window nothing has been retired: the tree pages
    // and their resident chains are fully intact and the fresh ReadView was
    // opened AFTER the force-expiry sweep, so the deferred-retirement fix
    // promises the FULL snapshot here — a clean error would itself be a
    // regression.
    let (_plan, pairs) = scan
        .expect("a fresh reader inside the drop window must complete its snapshot scan");
    assert_eq!(
        pairs.len(),
        3,
        "a fresh reader inside the drop window must see its full snapshot \
         (3 documents); it returned {} document(s)",
        pairs.len()
    );
}

/// Pinning test for the post-drop interleaving: once `drop_namespace` has
/// fully committed and published, a reader still holding the pre-drop epoch
/// (it loaded the epoch and was descheduled before opening its ReadView)
/// does NOT silently observe an empty namespace. F36 makes the outcome
/// deterministic: the view open's post-registration generation revalidation
/// rejects the raced DDL with the retryable `ReadViewExpired`.
#[test]
fn reader_on_pre_drop_epoch_after_drop_completes_does_not_silently_see_empty() {
    const NS: &str = "test.bug5.postdrop";
    let engine = buffered_engine();
    insert_three(&engine, NS);

    // Model a concurrent `find` that performed its single published-epoch
    // load and was descheduled before opening its ReadView.
    let pre_drop_epoch = engine.shared.load_published();
    let ns_snap = pre_drop_epoch
        .catalog
        .get_by_name(NS)
        .expect("pre-drop epoch must map the namespace");

    engine.drop_namespace(NS).expect("drop namespace");

    let result = super::snapshot_ops::open_snapshot_read_view_for_epoch(
        &engine.shared,
        Arc::clone(&pre_drop_epoch),
    )
    .and_then(|view| {
        super::snapshot_ops::plan_and_collect_snapshot_pairs(
            &engine.shared,
            ns_snap,
            &doc! {},
            &view,
            false,
        )
    });

    match result {
        Err(crate::error::Error::ReadViewExpired) => {}
        Err(other) => panic!(
            "a reader on the pre-drop epoch must fail its view open with the \
             retryable ReadViewExpired after a completed drop, got {other:?}"
        ),
        Ok((_plan, pairs)) => panic!(
            "a reader pinned to the pre-drop epoch must fail cleanly after a \
             completed drop; it returned Ok with {} document(s)",
            pairs.len()
        ),
    }
}

/// Same stale-epoch reader, but another namespace is created (and written
/// to) between the drop and the scan. With immediate frees the new
/// namespace reused the dropped 32k leaf: the reused page was a valid leaf
/// again, so the freed-page type check no longer fired; the reader's older
/// read_ts hid the foreign namespace's newer versions and the scan
/// silently reported an empty collection for a namespace whose full
/// snapshot it should see. F36 makes the outcome deterministic: the
/// late-opened view is rejected at open time with `ReadViewExpired`
/// instead of ever reaching the (possibly reused) pages.
#[test]
fn reader_on_pre_drop_epoch_after_page_reuse_must_not_silently_see_empty() {
    const NS: &str = "test.bug5.reuse";
    const NS2: &str = "test.bug5.reuse2";
    let engine = buffered_engine();
    insert_three(&engine, NS);

    // Model a concurrent `find` that performed its single published-epoch
    // load and was descheduled before opening its ReadView.
    let pre_drop_epoch = engine.shared.load_published();
    let ns_snap = pre_drop_epoch
        .catalog
        .get_by_name(NS)
        .expect("pre-drop epoch must map the namespace");

    engine.drop_namespace(NS).expect("drop namespace");

    // Reuse the freed 32k leaf: the next namespace create allocates its
    // data-root leaf from the free list.
    engine.create_namespace(NS2).expect("create reuse namespace");
    for id in 10..=12 {
        engine
            .insert(NS2, doc! { "_id": id, "value": format!("foreign-{id}") })
            .expect("insert foreign document");
    }

    // Opening a view on the stale pre-drop epoch must fail cleanly with the
    // retryable ReadViewExpired (F36 generation revalidation). If it ever
    // succeeds, the subsequent scan must NOT silently return foreign/empty
    // data.
    let result = super::snapshot_ops::open_snapshot_read_view_for_epoch(
        &engine.shared,
        Arc::clone(&pre_drop_epoch),
    )
    .and_then(|view| {
        super::snapshot_ops::plan_and_collect_snapshot_pairs(
            &engine.shared,
            ns_snap,
            &doc! {},
            &view,
            false,
        )
    });

    match result {
        Err(crate::error::Error::ReadViewExpired) => {}
        Err(other) => panic!(
            "a reader on the pre-drop epoch must fail its view open with the \
             retryable ReadViewExpired after a completed drop, got {other:?}"
        ),
        Ok((_plan, pairs)) => {
            let values: Vec<String> = pairs
                .iter()
                .map(|(_, doc)| doc.get_str("value").unwrap_or("<non-string>").to_owned())
                .collect();
            panic!(
                "a reader pinned to the pre-drop epoch must fail cleanly after \
                 a completed drop; it returned Ok with {} document(s): {values:?}",
                pairs.len()
            );
        }
    }
}

/// R5a: `drop_namespace` used to free 4 KiB internal pages IMMEDIATELY after
/// publish. Tree descent validates only the one-byte page type
/// (`InternalHeader::validate_type`), with no fence-key or tree-identity
/// check, so a freed internal that another tree's SMO reuses is a perfectly
/// valid internal page again — a stale-epoch reader descending through it is
/// silently routed into the FOREIGN subtree, whose newer versions its older
/// read_ts hides, producing a silent empty/wrong result. All retired tree
/// pages (4 KiB internals included) must ride the page-lifetime queue and
/// stay un-reusable while a live ReadView still predates the drop.
///
/// N17 tightening: the held view is opened inside the drop window (the
/// production interleaving — a post-publish open now fails the F36
/// revalidation), the dropped root internal is asserted OFF the free list
/// after the foreign SMO + checkpoint (the reader floor must have blocked
/// the retired drain), and the scan must deterministically return the FULL
/// snapshot — no `Err(_)` escape hatch.
#[test]
fn stale_reader_with_held_view_survives_internal_page_reuse_by_foreign_tree() {
    const NS: &str = "test.bug5.internalreuse";
    const NS2: &str = "test.bug5.internalreuse2";
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
    // Materialize the tree so the root splits into a real 4 KiB internal.
    engine.checkpoint().expect("checkpoint materializes the tree");

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
    let old_root_internal = ns_snap.data_root_page;

    // The stale reader registers its (non-poisoned) ReadView inside the
    // drop window and holds it across the drop, the foreign SMO, and the
    // checkpoint; it must keep the dropped pages alive.
    let view = open_view_inside_drop_window(&engine, NS, &pre_drop_epoch);

    // Foreign-tree SMO + checkpoint: NS2 grows past a root split, which
    // allocates a 4 KiB internal. With an eager free, the only free 4 KiB
    // page is NS's old internal root — NS2's SMO reuses it.
    engine.create_namespace(NS2).expect("create reuse namespace");
    for id in 100..(100 + DOCS) {
        engine
            .insert(
                NS2,
                doc! { "_id": id, "value": format!("foreign-{id}"), "pad": "y".repeat(3000) },
            )
            .expect("insert foreign document");
    }
    engine
        .checkpoint()
        .expect("checkpoint materializes the foreign tree");

    // The reader floor must have blocked the retired drain: the dropped
    // tree's root internal stays off the free list, so the foreign SMO
    // cannot have reused it.
    assert!(
        !free_list_4k_contains(&engine, old_root_internal),
        "dropped root internal {old_root_internal} reached the 4 KiB free \
         list while a pre-drop ReadView was still registered"
    );

    let store = BufferPoolPageStore::new(Arc::clone(&engine.shared.handle));
    let tree = BTree::open(store, ns_snap.data_root_page, ns_snap.data_root_level);
    let probe = super::snapshot_ops::primary_history_probe(&engine.shared, ns_snap.id);
    let pairs = super::btree_ops::btree_collscan(&tree, &doc! {}, &view, Some(&probe), None)
        .expect("a held pre-drop ReadView must complete its scan");
    let values: Vec<String> = pairs
        .iter()
        .map(|(_, doc)| doc.get_str("value").unwrap_or("<non-string>").to_owned())
        .collect();
    assert!(
        pairs.len() == DOCS as usize && values.iter().all(|v| v.starts_with("doc-")),
        "a stale-epoch reader holding a live ReadView must see its full \
         snapshot ({DOCS} own documents); it returned {} document(s): {values:?}",
        pairs.len()
    );
}

/// F36: the load-to-register window. A reader that LOADED the pre-drop
/// `PublishedEpoch` but has not yet REGISTERED its ReadView is invisible to
/// `oldest_required_ts()` (empty registry ⇒ `Ts::MAX`): a drop + checkpoint
/// inside that window passes both the lifetime fence and the reader floor,
/// the retired-drain frees the dropped tree's pages, and page reuse then
/// hands the late-registering reader a foreign tree whose newer versions
/// its older read_ts hides — the silent-empty BUG-5 class, immune to the
/// reader-floor gate because the floor never saw this reader.
///
/// The fix is post-registration revalidation in `open_snapshot_read_view`:
/// after the view registers, one published-epoch load compares
/// `catalog_generation` against the captured epoch; on mismatch the open
/// fails cleanly with `ReadViewExpired` (a DDL raced the open — retry on a
/// fresh epoch). Reuse is asserted explicitly (N17) so the corruption arm
/// is genuinely exercised.
#[test]
fn late_registering_reader_after_drop_checkpoint_and_reuse_must_fail_cleanly() {
    const NS: &str = "test.bug5.lateregister";
    const NS2: &str = "test.bug5.lateregister2";
    let engine = buffered_engine();
    insert_three(&engine, NS);

    // The reader's single published-epoch load; descheduled BEFORE its
    // ReadView registers.
    let pre_drop_epoch = engine.shared.load_published();
    let ns_snap = pre_drop_epoch
        .catalog
        .get_by_name(NS)
        .expect("pre-drop epoch must map the namespace");
    let old_root = ns_snap.data_root_page;

    engine.drop_namespace(NS).expect("drop namespace");

    // No ReadView is registered: the checkpoint drain sees an empty
    // registry (floor = Ts::MAX) and a passed fence — the retired pages
    // are freed.
    engine.checkpoint().expect("checkpoint drains retired pages");
    assert!(
        free_list_32k_contains(&engine, old_root),
        "precondition: the empty-registry checkpoint drain must free the \
         dropped data root {old_root}"
    );

    // Force reuse: the next namespace create allocates its data-root leaf
    // from the free list (LIFO). N17: assert the reuse actually happened so
    // the corruption arm is genuinely exercised.
    engine.create_namespace(NS2).expect("create reuse namespace");
    for id in 10..=12 {
        engine
            .insert(NS2, doc! { "_id": id, "value": format!("foreign-{id}") })
            .expect("insert foreign document");
    }
    let ns2_root = engine
        .shared
        .load_published()
        .catalog
        .get_by_name(NS2)
        .expect("reuse namespace published")
        .data_root_page;
    assert_eq!(
        ns2_root, old_root,
        "precondition: the foreign namespace must reuse the retired root"
    );

    // Reader resumes: registers its ReadView off the stale epoch.
    // Documented outcome: the open fails cleanly with `ReadViewExpired`
    // (the post-registration generation revalidation detects the raced
    // DDL). If the open ever succeeds the scan must produce the FULL
    // snapshot — never a silent foreign/empty result.
    match super::snapshot_ops::open_snapshot_read_view_for_epoch(
        &engine.shared,
        Arc::clone(&pre_drop_epoch),
    ) {
        Err(crate::error::Error::ReadViewExpired) => {}
        Err(other) => panic!(
            "late-registering reader must fail its view open with the \
             retryable ReadViewExpired, got {other:?}"
        ),
        Ok(view) => {
            let store = BufferPoolPageStore::new(Arc::clone(&engine.shared.handle));
            let tree = BTree::open(store, ns_snap.data_root_page, ns_snap.data_root_level);
            let probe = super::snapshot_ops::primary_history_probe(&engine.shared, ns_snap.id);
            let pairs = super::btree_ops::btree_collscan(&tree, &doc! {}, &view, Some(&probe), None)
                .expect("a successfully opened view must complete its scan");
            let values: Vec<String> = pairs
                .iter()
                .map(|(_, doc)| doc.get_str("value").unwrap_or("<non-string>").to_owned())
                .collect();
            assert!(
                pairs.len() == 3 && values.iter().all(|v| v.starts_with("doc-")),
                "a late-registering reader on the pre-drop epoch must see its \
                 full snapshot (3 own documents) or fail cleanly; it silently \
                 returned {} document(s): {values:?}",
                pairs.len()
            );
        }
    }
}

/// R5b: the 32 KiB deferral was checkpoint-fence bounded ONLY. A checkpoint
/// running DURING a stale scan advances the fence and drains the queue,
/// freeing the dropped leaves out from under the still-live ReadView; page
/// reuse then re-exposes the silent-empty bug (long scans are the victims).
/// Queue release must additionally be gated on the reader low-water: no
/// entry may be freed while a live ReadView predates its enqueue.
///
/// N17 tightening: the held view is opened inside the drop window (a
/// post-publish open now fails the F36 revalidation), the dropped root is
/// asserted OFF the free list after the checkpoint and NOT reused by the
/// foreign namespace, and the scan must deterministically return the FULL
/// snapshot — no `Err(_)` escape hatch.
#[test]
fn checkpoint_between_drop_and_scan_must_not_silently_empty_held_stale_view() {
    const NS: &str = "test.bug5.ckptdrain";
    const NS2: &str = "test.bug5.ckptdrain2";
    let engine = buffered_engine();
    insert_three(&engine, NS);

    let pre_drop_epoch = engine.shared.load_published();
    let ns_snap = pre_drop_epoch
        .catalog
        .get_by_name(NS)
        .expect("pre-drop epoch must map the namespace");
    let old_root = ns_snap.data_root_page;

    // Long-scan model: the reader registers its ReadView off the stale
    // epoch inside the drop window (after the force-expiry sweep, so it is
    // NOT poisoned) and holds it across a checkpoint.
    let view = open_view_inside_drop_window(&engine, NS, &pre_drop_epoch);

    // The checkpoint advances the page-lifetime fence and drains the queue.
    // Without the reader low-water gate this frees the dropped leaves while
    // `view` is still live.
    engine.checkpoint().expect("checkpoint between drop and scan");
    assert!(
        !free_list_32k_contains(&engine, old_root),
        "dropped data root {old_root} reached the 32 KiB free list while a \
         pre-drop ReadView was still registered — the reader low-water gate \
         did not block the checkpoint drain"
    );

    // Reuse attempt: the next namespace create allocates its data-root leaf
    // from the free list — it must NOT receive the retired root.
    engine.create_namespace(NS2).expect("create reuse namespace");
    for id in 10..=12 {
        engine
            .insert(NS2, doc! { "_id": id, "value": format!("foreign-{id}") })
            .expect("insert foreign document");
    }
    let ns2_root = engine
        .shared
        .load_published()
        .catalog
        .get_by_name(NS2)
        .expect("reuse namespace published")
        .data_root_page;
    assert_ne!(
        ns2_root, old_root,
        "the foreign namespace reused the retired data root while a pre-drop \
         ReadView was still registered"
    );

    let store = BufferPoolPageStore::new(Arc::clone(&engine.shared.handle));
    let tree = BTree::open(store, ns_snap.data_root_page, ns_snap.data_root_level);
    let probe = super::snapshot_ops::primary_history_probe(&engine.shared, ns_snap.id);
    let pairs = super::btree_ops::btree_collscan(&tree, &doc! {}, &view, Some(&probe), None)
        .expect("a held stale ReadView scanned across a checkpoint must complete");
    let values: Vec<String> = pairs
        .iter()
        .map(|(_, doc)| doc.get_str("value").unwrap_or("<non-string>").to_owned())
        .collect();
    assert!(
        pairs.len() == 3 && values.iter().all(|v| v.starts_with("doc-")),
        "a held stale ReadView scanned across a checkpoint must see its full \
         snapshot (3 own documents); it returned {} document(s): {values:?}",
        pairs.len()
    );
}
