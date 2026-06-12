//! Black-box functional tests for the `$expr` query filter.
//!
//! Each test opens a temp-file-backed `Client` (the same pattern used by the
//! other integration suites) and drives `Collection::find` end-to-end with a
//! top-level `$expr`, including an indexed sibling condition to exercise the
//! planner's residual-predicate path.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test targets use assertion-style panics and setup unwraps"
)]

use mqlite::{doc, Client, Document, IndexModel};
use tempfile::TempDir;

/// Open a temp-file-backed collection for the given namespace.
fn open_collection(tempdir: &TempDir, db: &str, coll: &str) -> mqlite::Collection<Document> {
    let client = Client::open(tempdir.path().join("db.mqlite")).expect("open");
    client.database(db).collection::<Document>(coll)
}

/// Run `find(filter)` and return the matched `_id`s, sorted ascending.
fn matched_ids(col: &mqlite::Collection<Document>, filter: Document) -> Vec<i64> {
    let docs: Vec<Document> = col
        .find(filter)
        .run()
        .expect("find")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect");
    let mut ids: Vec<i64> = docs
        .iter()
        .map(|d| d.get_i64("_id").expect("_id is int64"))
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn expr_with_indexed_sibling_condition() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "orders");

    // Index a sibling field so the planner can choose an IndexScan on `status`
    // while the top-level $expr runs as a residual predicate.
    col.create_index(IndexModel::builder().keys(doc! { "status": 1 }).build())
        .expect("create index");

    col.insert_one(&doc! { "_id": 1_i64, "status": "open", "qty": 5_i64, "cap": 3_i64 })
        .expect("insert 1");
    col.insert_one(&doc! { "_id": 2_i64, "status": "open", "qty": 2_i64, "cap": 4_i64 })
        .expect("insert 2");
    col.insert_one(&doc! { "_id": 3_i64, "status": "closed", "qty": 9_i64, "cap": 1_i64 })
        .expect("insert 3");

    // status == "open" AND qty > cap. Only doc 1 satisfies both.
    let filter = doc! {
        "status": "open",
        "$expr": { "$gt": ["$qty", "$cap"] },
    };
    assert_eq!(matched_ids(&col, filter), vec![1]);
}

#[test]
fn expr_arithmetic_full_scan() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "orders");

    col.insert_one(&doc! { "_id": 1_i64, "qty": 1_i64, "cap": 4_i64 })
        .expect("insert 1");
    col.insert_one(&doc! { "_id": 2_i64, "qty": 1_i64, "cap": 10_i64 })
        .expect("insert 2");

    // qty + 5 > cap. Doc 1: 6 > 4 (match). Doc 2: 6 > 10 (no match).
    let filter = doc! { "$expr": { "$gt": [{ "$add": ["$qty", 5_i64] }, "$cap"] } };
    assert_eq!(matched_ids(&col, filter), vec![1]);
}
