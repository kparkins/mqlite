//! Regression tests: `_id: null` documents must be returned by index scans.
//!
//! MongoDB semantics: `_id: null` is a legal document identity (one per
//! collection since `_id` is unique). A collection scan (`find({})`) and a
//! secondary-index scan (`find({a: 1})`) must both return documents whose
//! `_id` field is `null`.
//!
//! Bug reproduced: `execute_index_scan_from_snap` (snapshot_ops.rs) calls
//! `index_entry_id_free` and then short-circuits on `Bson::Null`, treating
//! a genuine stored `_id: null` the same as a corrupt/empty payload. The
//! collection scan is unaffected and returns the document, creating an
//! inconsistency between the two read paths.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]

use bson::{doc, Bson, Document};
use mqlite::{Client, IndexModel};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn open(dir: &TempDir, name: &str) -> Client {
    Client::open(dir.path().join(name)).expect("open client")
}

fn doc_has_null_id(doc: &Document) -> bool {
    matches!(doc.get("_id"), Some(Bson::Null))
}

// ---------------------------------------------------------------------------
// Test (a): index created AFTER insert — exercises dual-write maintenance path
// ---------------------------------------------------------------------------

/// Collection-scan `find({})` must return `{_id: null, a: 1}`.
///
/// This passes today and documents the baseline consistency contract that the
/// test below asserts is also met by the index-scan path.
#[test]
fn collscan_returns_null_id_doc_insert_then_index() {
    let dir = TempDir::new().expect("tempdir");
    let client = open(&dir, "null_id_a.mqlite");
    let col = client.database("test").collection::<Document>("items");

    col.insert_one(&doc! { "_id": Bson::Null, "a": 1i32 })
        .expect("insert null-_id doc");
    col.insert_one(&doc! { "_id": 2i32, "a": 2i32 })
        .expect("insert normal doc 2");
    col.insert_one(&doc! { "_id": 3i32, "a": 3i32 })
        .expect("insert normal doc 3");

    let model = IndexModel::builder().keys(doc! { "a": 1i32 }).build();
    col.create_index(model).expect("create index on a");

    let all_docs: Vec<Document> = col
        .find(doc! {})
        .limit(0)
        .run()
        .expect("collscan find")
        .collect::<mqlite::error::Result<_>>()
        .expect("collect collscan results");

    assert_eq!(all_docs.len(), 3, "collscan must return all 3 docs");
    assert!(
        all_docs.iter().any(doc_has_null_id),
        "collscan must include the null-_id doc; got: {:?}",
        all_docs
    );
}

/// Index scan on `{a: 1}` must include `{_id: null, a: 1}`.
///
/// This is the failing test: the index scan silently skips the null-_id doc
/// because `execute_index_scan_from_snap` treats `Bson::Null` as a sentinel
/// meaning "no id / corrupt entry" and skips without fetching the primary row.
#[test]
fn index_scan_returns_null_id_doc_insert_then_index() {
    let dir = TempDir::new().expect("tempdir");
    let client = open(&dir, "null_id_b.mqlite");
    let col = client.database("test").collection::<Document>("items");

    col.insert_one(&doc! { "_id": Bson::Null, "a": 1i32 })
        .expect("insert null-_id doc");
    col.insert_one(&doc! { "_id": 2i32, "a": 2i32 })
        .expect("insert normal doc 2");
    col.insert_one(&doc! { "_id": 3i32, "a": 3i32 })
        .expect("insert normal doc 3");

    let model = IndexModel::builder().keys(doc! { "a": 1i32 }).build();
    col.create_index(model).expect("create index on a");

    // The planner must choose the index scan for an equality filter on `a`
    // when a ready index on `a` exists (same pattern as index_vs_scan_consistency_ne).
    let indexed_docs: Vec<Document> = col
        .find(doc! { "a": 1i32 })
        .limit(0)
        .run()
        .expect("index-scan find")
        .collect::<mqlite::error::Result<_>>()
        .expect("collect index-scan results");

    assert_eq!(
        indexed_docs.len(),
        1,
        "index scan must return exactly 1 document matching {{a:1}}; got: {:?}",
        indexed_docs
    );
    assert!(
        indexed_docs.iter().any(doc_has_null_id),
        "index scan must include the null-_id doc; got: {:?}",
        indexed_docs
    );
}

// ---------------------------------------------------------------------------
// Test (b): index created BEFORE insert — exercises index-build path
// ---------------------------------------------------------------------------

/// Collection-scan baseline for index-before-insert variant.
#[test]
fn collscan_returns_null_id_doc_index_then_insert() {
    let dir = TempDir::new().expect("tempdir");
    let client = open(&dir, "null_id_c.mqlite");
    let col = client.database("test").collection::<Document>("items");

    let model = IndexModel::builder().keys(doc! { "a": 1i32 }).build();
    col.create_index(model).expect("create index on a before inserts");

    col.insert_one(&doc! { "_id": Bson::Null, "a": 1i32 })
        .expect("insert null-_id doc");
    col.insert_one(&doc! { "_id": 2i32, "a": 2i32 })
        .expect("insert normal doc 2");
    col.insert_one(&doc! { "_id": 3i32, "a": 3i32 })
        .expect("insert normal doc 3");

    let all_docs: Vec<Document> = col
        .find(doc! {})
        .limit(0)
        .run()
        .expect("collscan find")
        .collect::<mqlite::error::Result<_>>()
        .expect("collect collscan results");

    assert_eq!(all_docs.len(), 3, "collscan must return all 3 docs");
    assert!(
        all_docs.iter().any(doc_has_null_id),
        "collscan must include the null-_id doc; got: {:?}",
        all_docs
    );
}

/// Index scan `{a: 1}` must return `{_id: null, a: 1}` when the index was
/// created before the document was inserted (dual-write path).
///
/// If this test passes without the fix it acts as a regression guard.
#[test]
fn index_scan_returns_null_id_doc_index_then_insert() {
    let dir = TempDir::new().expect("tempdir");
    let client = open(&dir, "null_id_d.mqlite");
    let col = client.database("test").collection::<Document>("items");

    let model = IndexModel::builder().keys(doc! { "a": 1i32 }).build();
    col.create_index(model).expect("create index on a before inserts");

    col.insert_one(&doc! { "_id": Bson::Null, "a": 1i32 })
        .expect("insert null-_id doc");
    col.insert_one(&doc! { "_id": 2i32, "a": 2i32 })
        .expect("insert normal doc 2");
    col.insert_one(&doc! { "_id": 3i32, "a": 3i32 })
        .expect("insert normal doc 3");

    let indexed_docs: Vec<Document> = col
        .find(doc! { "a": 1i32 })
        .limit(0)
        .run()
        .expect("index-scan find")
        .collect::<mqlite::error::Result<_>>()
        .expect("collect index-scan results");

    assert_eq!(
        indexed_docs.len(),
        1,
        "index scan must return exactly 1 document matching {{a:1}}; got: {:?}",
        indexed_docs
    );
    assert!(
        indexed_docs.iter().any(doc_has_null_id),
        "index scan must include the null-_id doc (index-before-insert path); got: {:?}",
        indexed_docs
    );
}

// ---------------------------------------------------------------------------
// Consistency contract: collscan and index scan must agree
// ---------------------------------------------------------------------------

/// Verify that `find({})` and `find({a: 1})` (which takes the index path)
/// both return the null-_id document. The set of `_id` values from the
/// index scan must be a subset of those from the collection scan.
#[test]
fn collscan_and_index_scan_agree_on_null_id_doc() {
    let dir = TempDir::new().expect("tempdir");
    let client = open(&dir, "null_id_e.mqlite");
    let col = client.database("test").collection::<Document>("items");

    col.insert_one(&doc! { "_id": Bson::Null, "a": 1i32 })
        .expect("insert null-_id doc");
    col.insert_one(&doc! { "_id": 10i32, "a": 1i32 })
        .expect("insert second doc with a=1");
    col.insert_one(&doc! { "_id": 20i32, "a": 2i32 })
        .expect("insert doc with a=2");

    let model = IndexModel::builder().keys(doc! { "a": 1i32 }).build();
    col.create_index(model).expect("create index on a");

    let all_docs: Vec<Document> = col
        .find(doc! {})
        .limit(0)
        .run()
        .expect("collscan")
        .collect::<mqlite::error::Result<Vec<Document>>>()
        .expect("collect");
    let collscan_a1: Vec<Document> = all_docs
        .into_iter()
        .filter(|d: &Document| d.get_i32("a").ok() == Some(1))
        .collect();

    let index_scan_a1: Vec<Document> = col
        .find(doc! { "a": 1i32 })
        .limit(0)
        .run()
        .expect("index scan")
        .collect::<mqlite::error::Result<_>>()
        .expect("collect");

    assert_eq!(
        collscan_a1.len(),
        2,
        "collscan must find 2 docs with a=1; got: {:?}",
        collscan_a1
    );
    assert_eq!(
        index_scan_a1.len(),
        2,
        "index scan must find 2 docs with a=1 (including null-_id); got: {:?}",
        index_scan_a1
    );

    let collscan_has_null = collscan_a1.iter().any(doc_has_null_id);
    let index_has_null = index_scan_a1.iter().any(doc_has_null_id);

    assert!(
        collscan_has_null,
        "collscan must include null-_id doc among a=1 matches"
    );
    assert!(
        index_has_null,
        "index scan must include null-_id doc among a=1 matches; collscan returned it but index scan did not"
    );
}
