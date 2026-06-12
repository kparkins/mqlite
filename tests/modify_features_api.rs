//! Black-box functional tests for the MongoDB 8.0 update features:
//! `arrayFilters` + `$[<identifier>]`, the positional `$` operator, and
//! pipeline-form updates.
//!
//! Each test opens a temp-file-backed `Client` (the same pattern used by the
//! other integration suites) and exercises the public API end-to-end.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test targets use assertion-style panics and setup unwraps"
)]

use mqlite::{doc, Client, Document, ReturnDocument, UpdateModifications};
use tempfile::TempDir;

/// Open a temp-file-backed collection for the given namespace.
fn open_collection(tempdir: &TempDir, db: &str, coll: &str) -> mqlite::Collection<Document> {
    let client = Client::open(tempdir.path().join("db.mqlite")).expect("open");
    client.database(db).collection::<Document>(coll)
}

// ---------------------------------------------------------------------------
// arrayFilters
// ---------------------------------------------------------------------------

#[test]
fn update_many_with_array_filters_across_docs() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "students");
    col.insert_one(&doc! { "_id": 1, "grades": [95i32, 102i32, 90i32] })
        .expect("insert");
    col.insert_one(&doc! { "_id": 2, "grades": [98i32, 101i32, 80i32] })
        .expect("insert");

    let result = col
        .update_many(
            doc! {},
            doc! { "$set": { "grades.$[elem]": 100i32 } },
        )
        .array_filters(vec![doc! { "elem": { "$gte": 100i32 } }])
        .run()
        .expect("update_many");
    assert_eq!(result.matched_count, 2);
    assert_eq!(result.modified_count, 2);

    let d1 = col.find_one(doc! { "_id": 1 }).expect("find").unwrap();
    let g1 = d1.get_array("grades").unwrap();
    assert_eq!(g1[1].as_i32().unwrap(), 100);

    let d2 = col.find_one(doc! { "_id": 2 }).expect("find").unwrap();
    let g2 = d2.get_array("grades").unwrap();
    assert_eq!(g2[1].as_i32().unwrap(), 100);
    assert_eq!(g2[2].as_i32().unwrap(), 80);
}

#[test]
fn find_one_and_update_with_array_filters() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "students2");
    col.insert_one(&doc! {
        "_id": 1,
        "grades": [
            { "grade": 80i32, "mean": 75i32 },
            { "grade": 90i32, "mean": 85i32 },
        ]
    })
    .expect("insert");

    let after = col
        .find_one_and_update(
            doc! { "_id": 1 },
            doc! { "$set": { "grades.$[elem].mean": 100i32 } },
        )
        .array_filters(vec![doc! { "elem.grade": { "$gte": 85i32 } }])
        .return_document(ReturnDocument::After)
        .run()
        .expect("find_one_and_update")
        .expect("doc returned");
    let grades = after.get_array("grades").unwrap();
    assert_eq!(grades[0].as_document().unwrap().get_i32("mean").unwrap(), 75);
    assert_eq!(grades[1].as_document().unwrap().get_i32("mean").unwrap(), 100);
}

// ---------------------------------------------------------------------------
// positional $
// ---------------------------------------------------------------------------

#[test]
fn positional_update_end_to_end() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "items");
    col.insert_one(&doc! {
        "_id": 1,
        "items": [
            { "k": "a", "v": 1i32 },
            { "k": "b", "v": 2i32 },
        ]
    })
    .expect("insert");

    // Dotted paths do not traverse arrays of documents in mqlite filters
    // (documented divergence); $elemMatch is the supported spelling.
    let result = col
        .update_one(
            doc! { "items": { "$elemMatch": { "k": "b" } } },
            doc! { "$set": { "items.$.v": 99i32 } },
        )
        .run()
        .expect("update_one");
    assert_eq!(result.matched_count, 1);
    assert_eq!(result.modified_count, 1);

    let found = col.find_one(doc! { "_id": 1 }).expect("find").unwrap();
    let items = found.get_array("items").unwrap();
    assert_eq!(items[0].as_document().unwrap().get_i32("v").unwrap(), 1);
    assert_eq!(items[1].as_document().unwrap().get_i32("v").unwrap(), 99);
}

// ---------------------------------------------------------------------------
// pipeline-form updates
// ---------------------------------------------------------------------------

#[test]
fn pipeline_update_set_computed_from_existing() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "orders");
    col.insert_one(&doc! { "_id": 1, "price": 4i32, "qty": 3i32 })
        .expect("insert");

    let result = col
        .update_one(
            doc! { "_id": 1 },
            vec![doc! { "$set": { "total": { "$multiply": ["$price", "$qty"] } } }],
        )
        .run()
        .expect("pipeline update");
    assert_eq!(result.modified_count, 1);

    let found = col.find_one(doc! { "_id": 1 }).expect("find").unwrap();
    assert_eq!(found.get_i32("total").unwrap(), 12);
}

#[test]
fn pipeline_update_unset() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "orders_unset");
    col.insert_one(&doc! { "_id": 1, "a": 1i32, "b": 2i32 })
        .expect("insert");

    col.update_one(doc! { "_id": 1 }, vec![doc! { "$unset": "b" }])
        .run()
        .expect("pipeline unset");

    let found = col.find_one(doc! { "_id": 1 }).expect("find").unwrap();
    assert!(found.get("b").is_none());
    assert_eq!(found.get_i32("a").unwrap(), 1);
}

#[test]
fn pipeline_update_replace_with() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "orders_replace");
    col.insert_one(&doc! { "_id": 5, "a": 1i32 }).expect("insert");

    col.update_one(
        doc! { "_id": 5 },
        vec![doc! { "$replaceWith": { "b": 9i32 } }],
    )
    .run()
    .expect("pipeline replaceWith");

    let found = col.find_one(doc! { "_id": 5 }).expect("find").unwrap();
    // _id is restored even though $replaceWith dropped it.
    assert_eq!(found.get_i32("_id").unwrap(), 5);
    assert_eq!(found.get_i32("b").unwrap(), 9);
    assert!(found.get("a").is_none());
}

#[test]
fn pipeline_update_upsert_inserts() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "orders_upsert");

    let result = col
        .update_one(
            doc! { "_id": 42 },
            vec![doc! { "$set": { "v": 1i32 } }],
        )
        .upsert(true)
        .run()
        .expect("pipeline upsert");
    assert!(result.upserted_id.is_some());

    let found = col.find_one(doc! { "_id": 42 }).expect("find").unwrap();
    assert_eq!(found.get_i32("v").unwrap(), 1);
}

#[test]
fn pipeline_update_disallowed_stage_errors() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "orders_bad");
    col.insert_one(&doc! { "_id": 1 }).expect("insert");

    let result = col
        .update_one(
            doc! { "_id": 1 },
            vec![doc! { "$match": { "_id": 1i32 } }],
        )
        .run();
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("is not allowed to be used within an update"), "{msg}");
}

#[test]
fn pipeline_update_id_mutation_errors() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "orders_id");
    col.insert_one(&doc! { "_id": 1, "v": 0i32 }).expect("insert");

    let result = col
        .update_one(
            doc! { "_id": 1 },
            vec![doc! { "$set": { "_id": 2i32 } }],
        )
        .run();
    assert!(result.is_err(), "expected immutable _id error");
}

#[test]
fn update_modifications_from_impls() {
    let from_doc: UpdateModifications = doc! { "$set": { "a": 1i32 } }.into();
    assert!(matches!(from_doc, UpdateModifications::Document(_)));
    let from_vec: UpdateModifications = vec![doc! { "$set": { "a": 1i32 } }].into();
    assert!(matches!(from_vec, UpdateModifications::Pipeline(_)));
}
