//! Plan §T7 — Non-recursion stress for the MVCC history store.
//!
//! The history store sits on its own [`BufferPool`] partition (lock-order
//! position 1, outermost). The non-recursion invariant is:
//!
//!   Reconciliation on the MAIN buffer pool may install aged entries into
//!   the history store; the history store itself must NOT re-enter the
//!   main pool's `fetch_page` path.
//!
//! This is guarded at runtime by a thread-local depth sentinel
//! (`crate::storage::history_store::history_store_depth`) combined with
//! the `debug_assert!` in `BufferPoolHandle::fetch_page`. If a history
//! operation recursed back into the main pool, that assertion would
//! fire.
//!
//! This integration test hammers the public `Client` API with 1000
//! inserts against an on-disk database and then issues a full range
//! scan. The insertion phase drives main-pool evictions (which run
//! reconciliation); the scan phase traverses every key, exercising
//! the reader-path history fallthrough (`BTree::range_scan_mvcc` →
//! [`HistoryProbe::probe`]). In a debug build either phase would
//! panic if the invariant were violated — the test asserts the loop
//! completes cleanly and every document is returned.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use mqlite::{doc, Client};
use tempfile::TempDir;

/// 1000 writes + 1000-key scan through the public API must complete
/// without tripping the debug-build non-recursion guard.
#[test]
fn one_thousand_primary_ops_do_not_recurse_history_store() {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("rec.mqlite");

    let client = Client::open(&path).expect("open client");
    let db = client.database("t7");
    let coll = db.collection::<mqlite::Document>("recursion");

    for i in 0..1000i32 {
        coll.insert_one(&doc! { "_id": i, "v": i * 2 })
            .expect("insert_one");
    }

    // A full scan forces a reader path that traverses every primary
    // key. In a debug build the guard in `BufferPoolHandle::fetch_page`
    // panics if history-store work ever re-enters the main pool.
    let cursor = coll.find(doc! {}).run().expect("find.run");
    let collected: Vec<mqlite::Document> = cursor.collect::<Result<_, _>>().expect("cursor");
    assert_eq!(collected.len(), 1000, "all 1000 docs must be visible");
}
