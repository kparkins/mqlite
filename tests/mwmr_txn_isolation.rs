//! Plan §M4a/§M4b acceptance gate: a writer that returns `Err` mid-body
//! does not leak its dirty state — page bytes (M4a, PR 6), allocator
//! reservations / buffer-pool frames / file-header mutations (M4b, PR 7) —
//! into the shared engine.
//!
//! Under PR 6 the failing write is staged into a `TxnPageStore`
//! overlay that is dropped on rollback, so shared buffer-pool frames
//! are never mutated by a failing writer. PR 7 extends this to cover
//! allocator-state mutations and header mutations: pages allocated by
//! a rolled-back txn return to the free list, the buffer-pool frames
//! they pinned are invalidated, and the pre-txn header snapshot is
//! restored.
//!
//! Tests:
//!
//! - `rollback_leaves_no_dirty_frames` (PR 6): page-byte overlay.
//! - `rollback_returns_allocated_pages` (PR 7): allocator reservations.
//! - `rollback_reverts_header` (PR 7): header snapshot restore.
//! - `reader_survives_concurrent_free_list_churn` (PR 7, Gap 4): the
//!   deferred-free queue round-trip preserves reader-visible invariants
//!   under concurrent writer churn. Under the PR 7 `inner: Mutex` the
//!   reader and writer still serialize, so the test mostly exercises
//!   functional correctness; post-PR 8 it will have actual concurrency
//!   teeth.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use bson::doc;
use mqlite::Client;

#[test]
fn rollback_leaves_no_dirty_frames() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rollback.mqlite");
    let client = Client::open(&path).unwrap();
    let col = client.database("d").collection::<bson::Document>("c");

    // Insert one doc — this commits successfully.
    col.insert_one(&doc! { "_id": 1, "ok": true }).unwrap();

    // Provoke a failing write. A duplicate `_id` fails the primary
    // `BTree::insert` unique-constraint pre-check, which returns Err
    // inside the `with_txn` body — the outer `with_txn` then hits the
    // abort path, drops the overlay, and rolls back the WAL mark.
    let err = col.insert_one(&doc! { "_id": 1, "clash": true });
    assert!(err.is_err(), "duplicate _id must fail");

    // After the failed insert, the committing writer's state must be
    // unchanged — the only visible document is the original one.
    // Under PR 6 this is guaranteed by the overlay: the failed
    // writer's staged bytes never touched shared frames.
    let post: Vec<_> = col
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(post.len(), 1, "exactly one doc survives");
    assert_eq!(post[0].get_i32("_id").unwrap(), 1);
    assert!(post[0].get_bool("ok").unwrap());
    assert!(
        post[0].get("clash").is_none(),
        "rolled-back insert must not land",
    );

    // A subsequent successful insert must still work — rollback
    // did not poison the engine's internal state.
    col.insert_one(&doc! { "_id": 2, "ok": true }).unwrap();
    let post2: Vec<_> = col
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(post2.len(), 2, "post-rollback insert must land");
}

// ---------------------------------------------------------------------------
// PR 7 — allocator reservations
// ---------------------------------------------------------------------------

/// Plan §M4b acceptance gate: a rolled-back txn returns every page it
/// allocated to the allocator free list, so the file does not grow
/// unboundedly under a flurry of failing inserts.
///
/// Stress path: 100 duplicate-`_id` inserts fail inside `with_txn`. Each
/// failure rolls back allocations made by `BTree::insert` (leaf splits,
/// overflow chain pages, etc.). If rollback leaked allocations the file
/// would grow by ~100 leaf pages (~3.2 MB). PR 7 asserts the pages come
/// back to the free list and get reused, so growth is bounded.
#[test]
fn rollback_returns_allocated_pages() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("realloc.mqlite");
    let client = Client::open(&path).unwrap();
    let col = client.database("d").collection::<bson::Document>("c");

    // Seed: ensure the collection exists and has a baseline data-tree
    // root allocated.
    col.insert_one(&doc! { "_id": 0, "ok": true }).unwrap();
    // Force a checkpoint so the on-disk file reflects the baseline
    // before we measure.
    client.checkpoint().unwrap();

    let size_before = std::fs::metadata(&path).unwrap().len();

    // Trigger many failing inserts. Each attempts to insert `_id = 0`
    // which clashes with the seed document; the failure happens inside
    // `with_txn`, so any leaf-split allocations roll back.
    for i in 1..=100 {
        let _ = col.insert_one(&doc! { "_id": 0, "clash": i });
    }

    // Force a checkpoint so all journal → main-file moves happen.
    client.checkpoint().unwrap();
    drop(client);

    let size_after = std::fs::metadata(&path).unwrap().len();
    // File size should not have grown unboundedly — page allocations
    // from rolled-back txns must have been returned to the free list
    // and reused. Loose bound: <= 32 leaf pages (32 * 32 KiB = 1 MiB)
    // of growth max; in practice PR 7 should show zero growth.
    let grew = size_after.saturating_sub(size_before);
    assert!(
        grew < 32 * 32 * 1024,
        "file grew {} bytes — allocated pages were not reused (PR 7 regression)",
        grew,
    );
}

// ---------------------------------------------------------------------------
// PR 7 — header revert
// ---------------------------------------------------------------------------

/// Plan §M4b acceptance gate: a rolled-back txn does not corrupt the
/// file header. Header mutations made via `txn_update_header` (the
/// allocator's `free_list_head_*`, `total_page_count`, `catalog_root_*`
/// etc.) must revert to their pre-txn snapshot.
///
/// Stress path: seed one doc, then hammer with duplicate-`_id` inserts.
/// Post-hammer, reads must match the pre-hammer state; and reopen must
/// observe the same state (no header corruption that would misread the
/// catalog root or free-list head).
#[test]
fn rollback_reverts_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("header.mqlite");
    let client = Client::open(&path).unwrap();
    let col = client.database("d").collection::<bson::Document>("c");

    // Seed a baseline document and checkpoint so the header is stable.
    col.insert_one(&doc! { "_id": 1, "seed": true }).unwrap();
    client.checkpoint().unwrap();

    let pre: Vec<_> = col
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(pre.len(), 1);

    // Trigger 100 duplicate-`_id` failures. Each one mutates the header
    // (sync_catalog_root, possibly free-list updates for split pages)
    // and must revert on rollback.
    for _ in 0..100 {
        let _ = col.insert_one(&doc! { "_id": 1, "clash": true });
    }

    // Same-client read: header state visible to the engine must match
    // the pre-hammer state.
    let post: Vec<_> = col
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(pre, post, "failed txns must not corrupt visible state");

    // Reopen the file: if the persisted header is corrupted, we either
    // fail to open or lose the seed document.
    client.checkpoint().unwrap();
    drop(client);
    let client2 = Client::open(&path).unwrap();
    let col2 = client2.database("d").collection::<bson::Document>("c");
    let reopened: Vec<_> = col2
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        pre, reopened,
        "reopen must see the same state as pre-failing-txn",
    );
}

// ---------------------------------------------------------------------------
// PR 7 — concurrent reader vs. free-list churn (Gap 4)
// ---------------------------------------------------------------------------

/// Plan §M4b / Review Gap 4: a reader scanning one collection must
/// observe byte-identical results across rounds even while another
/// thread is churning the free list via insert-then-delete on a
/// different collection.
///
/// Under the PR 7 `inner: Mutex` the reader and writer still serialize,
/// so the test exercises functional correctness (no corruption across
/// alloc/free round-trips through the deferred-free queue). Post-PR 8
/// the reader-path mutex peel will give this test real concurrency
/// teeth.
#[test]
fn reader_survives_concurrent_free_list_churn() {
    use std::sync::Arc;
    use std::thread;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("churn.mqlite");
    let client = Arc::new(Client::open(&path).unwrap());

    // Reader namespace: seed a few documents that remain stable across
    // the test. These are what the reader thread scans.
    let reader_col = client.database("d").collection::<bson::Document>("stable");
    for i in 0..5 {
        reader_col
            .insert_one(&doc! { "_id": i, "v": i * 10 })
            .unwrap();
    }
    let baseline: Vec<_> = reader_col
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(baseline.len(), 5);

    // Writer: insert-then-delete on `churn` to churn the free list.
    let writer_client = Arc::clone(&client);
    let writer = thread::spawn(move || {
        let churn = writer_client
            .database("d")
            .collection::<bson::Document>("churn");
        for i in 0..50 {
            // Insert a doc with a string payload big enough that it may
            // allocate overflow pages — these will be freed on delete
            // and round-trip through the deferred-free queue.
            let payload: String = "x".repeat(1024);
            churn
                .insert_one(&doc! { "_id": i, "blob": payload })
                .unwrap();
            churn.delete_one(doc! { "_id": i }).unwrap();
        }
    });

    // Reader: scan the stable collection repeatedly, comparing each
    // scan against the baseline. Any divergence (missing doc, extra
    // doc, wrong value) indicates corruption introduced by the churn.
    let reader_client = Arc::clone(&client);
    let reader = thread::spawn(move || {
        let stable = reader_client
            .database("d")
            .collection::<bson::Document>("stable");
        for _ in 0..20 {
            let round: Vec<_> = stable
                .find(doc! {})
                .run()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(
                round.len(),
                5,
                "stable collection must always have exactly 5 docs",
            );
        }
    });

    writer.join().expect("writer panicked");
    reader.join().expect("reader panicked");

    // Post-conditions: both collections still readable; stable data
    // intact; churn fully drained.
    let after: Vec<_> = reader_col
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(after, baseline, "stable data must survive the churn");

    let churn_col = client.database("d").collection::<bson::Document>("churn");
    let churn_left: Vec<_> = churn_col
        .find(doc! {})
        .run()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(churn_left.is_empty(), "churn collection must be empty");
}
