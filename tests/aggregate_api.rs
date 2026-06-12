//! Black-box functional tests for [`Collection::aggregate`].
//!
//! Each test opens a temp-file-backed [`mqlite::Client`] and exercises the
//! public aggregate API end-to-end.  Tests are deliberately focused on
//! observable output; internal engine state is not probed.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test targets use assertion-style panics and setup unwraps"
)]

use bson::Bson;
use mqlite::{doc, Client, Document, IndexModel};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// Open a temp-file-backed collection of raw documents.
fn open_col(dir: &TempDir, db: &str, coll: &str) -> mqlite::Collection<Document> {
    let client = Client::open(dir.path().join("db.mqlite")).expect("open");
    client.database(db).collection::<Document>(coll)
}

/// Open two collections in the same database from one client (for `$lookup`).
fn open_two(
    dir: &TempDir,
    db: &str,
    a: &str,
    b: &str,
) -> (mqlite::Collection<Document>, mqlite::Collection<Document>) {
    let client = Client::open(dir.path().join("db.mqlite")).expect("open");
    let database = client.database(db);
    (
        database.collection::<Document>(a),
        database.collection::<Document>(b),
    )
}

/// Collect aggregate results, panicking on any error.
fn run(col: &mqlite::Collection<Document>, pipeline: Vec<Document>) -> Vec<Document> {
    col.aggregate(pipeline)
        .run()
        .expect("aggregate")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect")
}

// ---------------------------------------------------------------------------
// Empty pipeline
// ---------------------------------------------------------------------------

#[test]
fn empty_pipeline_returns_all_documents() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "x": 1i32 }).unwrap();
    col.insert_one(&doc! { "x": 2i32 }).unwrap();
    let result = run(&col, vec![]);
    assert_eq!(result.len(), 2);
}

// ---------------------------------------------------------------------------
// $match only
// ---------------------------------------------------------------------------

#[test]
fn match_only_filters_correctly() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "v": 1i32, "ok": true }).unwrap();
    col.insert_one(&doc! { "v": 2i32, "ok": false }).unwrap();
    col.insert_one(&doc! { "v": 3i32, "ok": true }).unwrap();
    let result = run(&col, vec![doc! { "$match": { "ok": true } }]);
    assert_eq!(result.len(), 2);
    for doc in &result {
        assert!(doc.get_bool("ok").unwrap());
    }
}

// ---------------------------------------------------------------------------
// $match + $sort + $skip + $limit
// ---------------------------------------------------------------------------

#[test]
fn match_sort_skip_limit_pipeline() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for i in 0..10i32 {
        col.insert_one(&doc! { "n": i, "even": i % 2 == 0 }).unwrap();
    }
    let result = run(
        &col,
        vec![
            doc! { "$match": { "even": true } },
            doc! { "$sort": { "n": -1i32 } },
            doc! { "$skip": 1i32 },
            doc! { "$limit": 3i32 },
        ],
    );
    // Even numbers in descending order, skip 1: [6, 4, 2] (after skip of 8)
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].get_i32("n").unwrap(), 6);
    assert_eq!(result[1].get_i32("n").unwrap(), 4);
    assert_eq!(result[2].get_i32("n").unwrap(), 2);
}

// ---------------------------------------------------------------------------
// $project — inclusion
// ---------------------------------------------------------------------------

#[test]
fn project_include_keeps_selected_fields() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "a": 1i32, "b": 2i32, "c": 3i32 }).unwrap();
    let result = run(&col, vec![doc! { "$project": { "a": 1i32, "_id": 0i32 } }]);
    assert_eq!(result.len(), 1);
    assert!(result[0].contains_key("a"), "projected field 'a' must be present");
    assert!(!result[0].contains_key("b"), "excluded field 'b' must be absent");
    assert!(!result[0].contains_key("_id"), "_id must be excluded explicitly");
}

// ---------------------------------------------------------------------------
// $project — exclusion
// ---------------------------------------------------------------------------

#[test]
fn project_exclude_removes_fields() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "a": 1i32, "b": 2i32, "c": 3i32 }).unwrap();
    let result = run(&col, vec![doc! { "$project": { "b": 0i32 } }]);
    assert_eq!(result.len(), 1);
    assert!(result[0].contains_key("a"));
    assert!(!result[0].contains_key("b"));
    assert!(result[0].contains_key("c"));
}

// ---------------------------------------------------------------------------
// $count
// ---------------------------------------------------------------------------

#[test]
fn count_stage_replaces_stream_with_single_doc() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for _ in 0..7i32 {
        col.insert_one(&doc! { "x": 1i32 }).unwrap();
    }
    let result = run(&col, vec![doc! { "$count": "total" }]);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("total").unwrap(), 7);
}

// ---------------------------------------------------------------------------
// $group — each accumulator
// ---------------------------------------------------------------------------

#[test]
fn group_sum_accumulator() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for &v in &[1i32, 2, 3, 4] {
        col.insert_one(&doc! { "g": "x", "v": v }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "total": { "$sum": "$v" } } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("total").unwrap(), 10);
}

#[test]
fn group_avg_accumulator() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for &v in &[10i32, 20, 30] {
        col.insert_one(&doc! { "g": "x", "v": v }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "avg": { "$avg": "$v" } } }],
    );
    assert_eq!(result.len(), 1);
    let avg = result[0].get_f64("avg").unwrap();
    assert!((avg - 20.0).abs() < 1e-10);
}

#[test]
fn group_min_max_accumulator() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for &v in &[5i32, 2, 8, 1] {
        col.insert_one(&doc! { "v": v }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$group": {
            "_id": Bson::Null,
            "lo": { "$min": "$v" },
            "hi": { "$max": "$v" }
        } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("lo").unwrap(), 1);
    assert_eq!(result[0].get_i32("hi").unwrap(), 8);
}

#[test]
fn group_push_accumulator_collects_values() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for &v in &[1i32, 2, 3] {
        col.insert_one(&doc! { "v": v }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": Bson::Null, "vals": { "$push": "$v" } } }],
    );
    assert_eq!(result.len(), 1);
    let vals = result[0].get_array("vals").unwrap();
    assert_eq!(vals.len(), 3);
}

#[test]
fn group_add_to_set_deduplicates() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for &v in &[1i32, 2, 1, 3, 2] {
        col.insert_one(&doc! { "v": v }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": Bson::Null, "unique": { "$addToSet": "$v" } } }],
    );
    assert_eq!(result.len(), 1);
    let unique = result[0].get_array("unique").unwrap();
    assert_eq!(unique.len(), 3);
}

#[test]
fn group_first_and_last_after_sort() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for &v in &[3i32, 1, 4, 1, 5] {
        col.insert_one(&doc! { "g": "x", "v": v }).unwrap();
    }
    let result = run(
        &col,
        vec![
            doc! { "$sort": { "v": 1i32 } },
            doc! { "$group": {
                "_id": "$g",
                "first": { "$first": "$v" },
                "last": { "$last": "$v" }
            } },
        ],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("first").unwrap(), 1);
    assert_eq!(result[0].get_i32("last").unwrap(), 5);
}

// ---------------------------------------------------------------------------
// $group — _id forms
// ---------------------------------------------------------------------------

#[test]
fn group_id_null_aggregates_all_documents() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for &v in &[1i32, 2, 3] {
        col.insert_one(&doc! { "v": v }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": Bson::Null, "total": { "$sum": "$v" } } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("_id").unwrap(), &Bson::Null);
    assert_eq!(result[0].get_i32("total").unwrap(), 6);
}

#[test]
fn group_id_field_path_produces_one_group_per_key() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "cat": "a", "n": 1i32 }).unwrap();
    col.insert_one(&doc! { "cat": "b", "n": 2i32 }).unwrap();
    col.insert_one(&doc! { "cat": "a", "n": 3i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$cat", "s": { "$sum": "$n" } } }],
    );
    assert_eq!(result.len(), 2);
    let find = |key: &str| {
        result
            .iter()
            .find(|d| d.get_str("_id").ok() == Some(key))
            .expect("group not found")
    };
    assert_eq!(find("a").get_i32("s").unwrap(), 4);
    assert_eq!(find("b").get_i32("s").unwrap(), 2);
}

#[test]
fn group_id_composite_document() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "region": "us", "cat": "a", "v": 1i32 }).unwrap();
    col.insert_one(&doc! { "region": "us", "cat": "b", "v": 2i32 }).unwrap();
    col.insert_one(&doc! { "region": "eu", "cat": "a", "v": 3i32 }).unwrap();
    col.insert_one(&doc! { "region": "us", "cat": "a", "v": 4i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": {
            "_id": { "r": "$region", "c": "$cat" },
            "total": { "$sum": "$v" }
        } }],
    );
    assert_eq!(result.len(), 3);
}

// ---------------------------------------------------------------------------
// $group — missing + null grouping together
// ---------------------------------------------------------------------------

#[test]
fn group_missing_and_null_key_fold_together() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "v": 1i32 }).unwrap();          // missing "k"
    col.insert_one(&doc! { "k": Bson::Null, "v": 2i32 }).unwrap(); // explicit null
    col.insert_one(&doc! { "k": "x", "v": 3i32 }).unwrap(); // real key
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$k", "s": { "$sum": "$v" } } }],
    );
    // missing "k" and null "k" must land in the same group (_id: null)
    assert_eq!(result.len(), 2, "expected 2 groups (null and 'x'), got {result:?}");
    let null_group = result
        .iter()
        .find(|d| d.get("_id").unwrap() == &Bson::Null)
        .expect("null group");
    assert_eq!(null_group.get_i32("s").unwrap(), 3);
}

// ---------------------------------------------------------------------------
// Cross-numeric group keys
// ---------------------------------------------------------------------------

#[test]
fn group_cross_numeric_keys_collapse() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    // Int32(1) and Double(1.0) must map to the same group key.
    col.insert_one(&doc! { "k": 1i32, "v": 10i32 }).unwrap();
    col.insert_one(&doc! { "k": 1.0f64, "v": 20i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$k", "s": { "$sum": "$v" } } }],
    );
    assert_eq!(result.len(), 1, "Int32(1) and Double(1.0) must collapse into one group");
    assert_eq!(result[0].get_i32("s").unwrap(), 30);
}

// ---------------------------------------------------------------------------
// $sum document-count idiom {$sum: 1}
// ---------------------------------------------------------------------------

#[test]
fn group_sum_constant_one_counts_documents() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "a" }).unwrap();
    col.insert_one(&doc! { "g": "a" }).unwrap();
    col.insert_one(&doc! { "g": "b" }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "n": { "$sum": 1i32 } } }],
    );
    assert_eq!(result.len(), 2);
    let find = |key: &str| {
        result
            .iter()
            .find(|d| d.get_str("_id").ok() == Some(key))
            .unwrap()
    };
    assert_eq!(find("a").get_i32("n").unwrap(), 2);
    assert_eq!(find("b").get_i32("n").unwrap(), 1);
}

// ---------------------------------------------------------------------------
// $avg empty group yields null
// ---------------------------------------------------------------------------

#[test]
fn avg_of_empty_group_yields_null() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    // Insert docs with no numeric field so $avg sees nothing.
    col.insert_one(&doc! { "s": "hello" }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": {
            "_id": Bson::Null,
            "avg": { "$avg": "$missing_numeric" }
        } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("avg").unwrap(), &Bson::Null);
}

// ---------------------------------------------------------------------------
// Unknown stage error
// ---------------------------------------------------------------------------

#[test]
fn unknown_stage_returns_error() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    let result = col.aggregate(vec![doc! { "$nosuchstage": {} }]).run();
    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("Unrecognized pipeline stage name: '$nosuchstage'"),
                "unexpected error message: {msg}",
            );
        }
        Ok(_) => panic!("expected error for unknown stage, got Ok"),
    }
}

// ---------------------------------------------------------------------------
// Non-document pipeline element error
// ---------------------------------------------------------------------------

/// The native API accepts `Vec<Document>` so every element is already a
/// document.  The non-document element check is exercised via the wire layer
/// (see wire tests) and is also covered by the unknown-stage test which uses
/// a single-key doc with an unrecognised stage name.  This test verifies the
/// adjacent error case: an empty stage document is rejected.
#[test]
fn empty_stage_document_returns_error() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    // An empty stage document has no key, which the parser rejects.
    let result = col.aggregate(vec![Document::new()]).run();
    assert!(result.is_err(), "empty stage document must be rejected");
}

// ---------------------------------------------------------------------------
// $limit 0 error
// ---------------------------------------------------------------------------

#[test]
fn limit_zero_returns_error() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    match col.aggregate(vec![doc! { "$limit": 0i32 }]).run() {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("limit must be positive"),
                "unexpected error: {msg}"
            );
        }
        Ok(_) => panic!("expected error for $limit 0, got Ok"),
    }
}

// ---------------------------------------------------------------------------
// $group without _id error
// ---------------------------------------------------------------------------

#[test]
fn group_without_id_returns_error() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    match col
        .aggregate(vec![doc! { "$group": { "total": { "$sum": "$v" } } }])
        .run()
    {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("a group specification must include an _id"),
                "unexpected error: {msg}"
            );
        }
        Ok(_) => panic!("expected error for $group without _id, got Ok"),
    }
}

// ---------------------------------------------------------------------------
// Index-accelerated first-$match
// ---------------------------------------------------------------------------

#[test]
fn first_match_with_index_returns_correct_results() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    // Create an index on "status" so the leading $match can use it.
    col.create_index(
        IndexModel::builder()
            .keys(doc! { "status": 1i32 })
            .build(),
    )
    .unwrap();
    for i in 0..10i32 {
        col.insert_one(&doc! {
            "status": if i % 2 == 0 { "active" } else { "inactive" },
            "n": i
        })
        .unwrap();
    }
    let result = run(
        &col,
        vec![
            doc! { "$match": { "status": "active" } },
            doc! { "$count": "total" },
        ],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("total").unwrap(), 5);
}

// ---------------------------------------------------------------------------
// $count accumulator (group operator, distinct from the $count stage)
// ---------------------------------------------------------------------------

#[test]
fn group_count_accumulator_counts_documents() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "a" }).unwrap();
    col.insert_one(&doc! { "g": "a" }).unwrap();
    col.insert_one(&doc! { "g": "b" }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "n": { "$count": {} } } }],
    );
    assert_eq!(result.len(), 2);
    let find = |key: &str| {
        result
            .iter()
            .find(|d| d.get_str("_id").ok() == Some(key))
            .unwrap()
    };
    assert_eq!(find("a").get_i32("n").unwrap(), 2);
    assert_eq!(find("b").get_i32("n").unwrap(), 1);
}

#[test]
fn group_count_accumulator_nonempty_arg_errors() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "a" }).unwrap();
    match col
        .aggregate(vec![doc! {
            "$group": { "_id": "$g", "n": { "$count": { "x": 1i32 } } }
        }])
        .run()
    {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("$count requires an empty document"),
                "unexpected error: {msg}"
            );
        }
        Ok(_) => panic!("expected error for non-empty $count argument, got Ok"),
    }
}

// ---------------------------------------------------------------------------
// $mergeObjects accumulator
// ---------------------------------------------------------------------------

#[test]
fn group_merge_objects_overwrites_in_encounter_order() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "x", "d": { "a": 1i32, "b": 2i32 } }).unwrap();
    col.insert_one(&doc! { "g": "x", "d": { "b": 20i32, "c": 3i32 } }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "m": { "$mergeObjects": "$d" } } }],
    );
    assert_eq!(result.len(), 1);
    let merged = result[0].get_document("m").unwrap();
    assert_eq!(merged.get_i32("a").unwrap(), 1);
    // Later document overwrites "b".
    assert_eq!(merged.get_i32("b").unwrap(), 20);
    assert_eq!(merged.get_i32("c").unwrap(), 3);
}

#[test]
fn group_merge_objects_skips_null_and_missing() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "x", "d": { "a": 1i32 } }).unwrap();
    col.insert_one(&doc! { "g": "x", "d": Bson::Null }).unwrap(); // null -> ignored
    col.insert_one(&doc! { "g": "x" }).unwrap(); // missing "d" -> ignored
    col.insert_one(&doc! { "g": "x", "d": { "b": 2i32 } }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "m": { "$mergeObjects": "$d" } } }],
    );
    assert_eq!(result.len(), 1);
    let merged = result[0].get_document("m").unwrap();
    assert_eq!(merged.get_i32("a").unwrap(), 1);
    assert_eq!(merged.get_i32("b").unwrap(), 2);
}

#[test]
fn group_merge_objects_empty_group_yields_empty_document() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "x" }).unwrap(); // missing "d" for every doc
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "m": { "$mergeObjects": "$d" } } }],
    );
    assert_eq!(result.len(), 1);
    assert!(result[0].get_document("m").unwrap().is_empty());
}

#[test]
fn group_merge_objects_non_document_input_errors() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "x", "d": 5i32 }).unwrap();
    match col
        .aggregate(vec![doc! {
            "$group": { "_id": "$g", "m": { "$mergeObjects": "$d" } }
        }])
        .run()
    {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("$mergeObjects requires object inputs"),
                "unexpected error: {msg}"
            );
        }
        Ok(_) => panic!("expected error for non-document $mergeObjects input, got Ok"),
    }
}

// ---------------------------------------------------------------------------
// $stdDevPop / $stdDevSamp accumulators
// ---------------------------------------------------------------------------

#[test]
fn group_std_dev_pop_basic() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    // Population stddev of [2, 4, 4, 4, 5, 5, 7, 9] is exactly 2.0.
    for &v in &[2i32, 4, 4, 4, 5, 5, 7, 9] {
        col.insert_one(&doc! { "g": "x", "v": v }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "sd": { "$stdDevPop": "$v" } } }],
    );
    assert_eq!(result.len(), 1);
    let sd = result[0].get_f64("sd").unwrap();
    assert!((sd - 2.0).abs() < 1e-9, "expected 2.0, got {sd}");
}

#[test]
fn group_std_dev_pop_single_value_is_zero() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "x", "v": 42i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "sd": { "$stdDevPop": "$v" } } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_f64("sd").unwrap(), 0.0);
}

#[test]
fn group_std_dev_pop_zero_values_is_null() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "x", "s": "no-number" }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "sd": { "$stdDevPop": "$v" } } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("sd").unwrap(), &Bson::Null);
}

#[test]
fn group_std_dev_samp_single_value_is_null() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "x", "v": 42i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "sd": { "$stdDevSamp": "$v" } } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("sd").unwrap(), &Bson::Null);
}

#[test]
fn group_std_dev_samp_basic() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    // Sample stddev of [2, 4, 4, 4, 5, 5, 7, 9] is sqrt(32/7) ~= 2.1380899.
    for &v in &[2i32, 4, 4, 4, 5, 5, 7, 9] {
        col.insert_one(&doc! { "g": "x", "v": v }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "sd": { "$stdDevSamp": "$v" } } }],
    );
    assert_eq!(result.len(), 1);
    let sd = result[0].get_f64("sd").unwrap();
    assert!((sd - (32.0_f64 / 7.0).sqrt()).abs() < 1e-9, "got {sd}");
}

// ---------------------------------------------------------------------------
// $firstN / $lastN accumulators
// ---------------------------------------------------------------------------

#[test]
fn group_first_n_caps_in_encounter_order_and_includes_null() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "x", "v": 1i32 }).unwrap();
    col.insert_one(&doc! { "g": "x", "v": Bson::Null }).unwrap(); // null included
    col.insert_one(&doc! { "g": "x" }).unwrap(); // missing -> skipped
    col.insert_one(&doc! { "g": "x", "v": 3i32 }).unwrap();
    col.insert_one(&doc! { "g": "x", "v": 4i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": {
            "_id": "$g",
            "f": { "$firstN": { "input": "$v", "n": 3i32 } }
        } }],
    );
    assert_eq!(result.len(), 1);
    let f = result[0].get_array("f").unwrap();
    // First three non-missing values in order: 1, null, 3.
    assert_eq!(f.len(), 3);
    assert_eq!(f[0], Bson::Int32(1));
    assert_eq!(f[1], Bson::Null);
    assert_eq!(f[2], Bson::Int32(3));
}

#[test]
fn group_last_n_keeps_last_values() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for &v in &[1i32, 2, 3, 4, 5] {
        col.insert_one(&doc! { "g": "x", "v": v }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$group": {
            "_id": "$g",
            "l": { "$lastN": { "input": "$v", "n": 2i32 } }
        } }],
    );
    assert_eq!(result.len(), 1);
    let l = result[0].get_array("l").unwrap();
    assert_eq!(l.len(), 2);
    assert_eq!(l[0], Bson::Int32(4));
    assert_eq!(l[1], Bson::Int32(5));
}

// ---------------------------------------------------------------------------
// $minN / $maxN accumulators
// ---------------------------------------------------------------------------

#[test]
fn group_min_n_returns_smallest_sorted_ascending() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for &v in &[5i32, 2, 8, 1, 9, 3] {
        col.insert_one(&doc! { "g": "x", "v": v }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$group": {
            "_id": "$g",
            "m": { "$minN": { "input": "$v", "n": 3i32 } }
        } }],
    );
    assert_eq!(result.len(), 1);
    let m = result[0].get_array("m").unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m[0], Bson::Int32(1));
    assert_eq!(m[1], Bson::Int32(2));
    assert_eq!(m[2], Bson::Int32(3));
}

#[test]
fn group_max_n_returns_largest_sorted_descending() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for &v in &[5i32, 2, 8, 1, 9, 3] {
        col.insert_one(&doc! { "g": "x", "v": v }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$group": {
            "_id": "$g",
            "m": { "$maxN": { "input": "$v", "n": 3i32 } }
        } }],
    );
    assert_eq!(result.len(), 1);
    let m = result[0].get_array("m").unwrap();
    assert_eq!(m.len(), 3);
    assert_eq!(m[0], Bson::Int32(9));
    assert_eq!(m[1], Bson::Int32(8));
    assert_eq!(m[2], Bson::Int32(5));
}

#[test]
fn group_min_n_skips_null_and_missing() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "x", "v": 4i32 }).unwrap();
    col.insert_one(&doc! { "g": "x", "v": Bson::Null }).unwrap(); // skipped
    col.insert_one(&doc! { "g": "x" }).unwrap(); // skipped
    col.insert_one(&doc! { "g": "x", "v": 2i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": {
            "_id": "$g",
            "m": { "$minN": { "input": "$v", "n": 5i32 } }
        } }],
    );
    assert_eq!(result.len(), 1);
    let m = result[0].get_array("m").unwrap();
    // n exceeds the group size: return all (2) numeric values, sorted ascending.
    assert_eq!(m.len(), 2);
    assert_eq!(m[0], Bson::Int32(2));
    assert_eq!(m[1], Bson::Int32(4));
}

#[test]
fn group_n_accumulator_n_zero_errors() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "x", "v": 1i32 }).unwrap();
    match col
        .aggregate(vec![doc! {
            "$group": { "_id": "$g", "m": { "$firstN": { "input": "$v", "n": 0i32 } } }
        }])
        .run()
    {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("n must be a positive integer"),
                "unexpected error: {msg}"
            );
        }
        Ok(_) => panic!("expected error for n=0, got Ok"),
    }
}

// ---------------------------------------------------------------------------
// $sortByCount stage
// ---------------------------------------------------------------------------

#[test]
fn sort_by_count_groups_and_sorts_descending() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    // "a" x3, "b" x1, "c" x2 -> sorted by count descending: a, c, b.
    for tag in ["a", "a", "a", "b", "c", "c"] {
        col.insert_one(&doc! { "tag": tag }).unwrap();
    }
    let result = run(&col, vec![doc! { "$sortByCount": "$tag" }]);
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].get_str("_id").unwrap(), "a");
    assert_eq!(result[0].get_i32("count").unwrap(), 3);
    // Tie between "b" (1) and "c" (2): "c" must precede "b" by count.
    assert_eq!(result[1].get_str("_id").unwrap(), "c");
    assert_eq!(result[1].get_i32("count").unwrap(), 2);
    assert_eq!(result[2].get_str("_id").unwrap(), "b");
    assert_eq!(result[2].get_i32("count").unwrap(), 1);
}

#[test]
fn sort_by_count_operator_expression_now_works() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    // $sortByCount now accepts full expressions; group by an uppercased tag.
    for tag in ["a", "A", "b"] {
        col.insert_one(&doc! { "tag": tag }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$sortByCount": { "$toUpper": "$tag" } }],
    );
    // "a" and "A" both uppercase to "A" (count 2); "b" -> "B" (count 1).
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].get_str("_id").unwrap(), "A");
    assert_eq!(result[0].get_i32("count").unwrap(), 2);
    assert_eq!(result[1].get_str("_id").unwrap(), "B");
    assert_eq!(result[1].get_i32("count").unwrap(), 1);
}

// ---------------------------------------------------------------------------
// $group — expression _id and computed accumulator arguments
// ---------------------------------------------------------------------------

#[test]
fn group_expression_id_uppercases_key() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for tag in ["a", "A", "b"] {
        col.insert_one(&doc! { "g": tag, "v": 1i32 }).unwrap();
    }
    let result = run(
        &col,
        vec![doc! { "$group": {
            "_id": { "$toUpper": "$g" },
            "n": { "$sum": 1i32 }
        } }],
    );
    // "a"/"A" fold into "A" (n=2); "b" -> "B" (n=1). First-seen order preserved.
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].get_str("_id").unwrap(), "A");
    assert_eq!(result[0].get_i32("n").unwrap(), 2);
    assert_eq!(result[1].get_str("_id").unwrap(), "B");
    assert_eq!(result[1].get_i32("n").unwrap(), 1);
}

#[test]
fn group_accumulator_with_computed_expression() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": "x", "price": 2i32, "qty": 3i32 }).unwrap();
    col.insert_one(&doc! { "g": "x", "price": 5i32, "qty": 4i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$group": {
            "_id": "$g",
            "revenue": { "$sum": { "$multiply": ["$price", "$qty"] } }
        } }],
    );
    // 2*3 + 5*4 = 26
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("revenue").unwrap(), 26);
}

#[test]
fn group_null_and_missing_id_fold_together() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "g": Bson::Null, "v": 1i32 }).unwrap();
    col.insert_one(&doc! { "v": 2i32 }).unwrap(); // missing g
    let result = run(
        &col,
        vec![doc! { "$group": { "_id": "$g", "n": { "$sum": 1i32 } } }],
    );
    // Missing and explicit null fold into a single null group.
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get("_id").unwrap(), &Bson::Null);
    assert_eq!(result[0].get_i32("n").unwrap(), 2);
}

// ---------------------------------------------------------------------------
// $project — computed fields
// ---------------------------------------------------------------------------

#[test]
fn project_computed_field_arithmetic() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "price": 4i32, "qty": 5i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$project": {
            "_id": 0i32,
            "total": { "$multiply": ["$price", "$qty"] }
        } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("total").unwrap(), 20);
    assert!(!result[0].contains_key("price"));
}

#[test]
fn project_computed_field_with_cond() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "n": 8i32 }).unwrap();
    col.insert_one(&doc! { "n": 3i32 }).unwrap();
    let result = run(
        &col,
        vec![
            doc! { "$project": {
                "_id": 0i32,
                "big": { "$cond": [{ "$gte": ["$n", 5i32] }, "yes", "no"] }
            } },
            doc! { "$sort": { "big": 1i32 } },
        ],
    );
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].get_str("big").unwrap(), "no");
    assert_eq!(result[1].get_str("big").unwrap(), "yes");
}

#[test]
fn project_dotted_computed_target_creates_nested_doc() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "x": 7i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$project": {
            "_id": 0i32,
            "a.b": { "$add": ["$x", 1i32] }
        } }],
    );
    assert_eq!(result.len(), 1);
    let a = result[0].get_document("a").unwrap();
    assert_eq!(a.get_i32("b").unwrap(), 8);
}

#[test]
fn project_computed_missing_result_omits_field() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "x": 1i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$project": {
            "_id": 0i32,
            "y": "$missing"
        } }],
    );
    assert_eq!(result.len(), 1);
    // A missing computed result omits the field entirely.
    assert!(!result[0].contains_key("y"));
}

// ---------------------------------------------------------------------------
// $addFields / $set
// ---------------------------------------------------------------------------

#[test]
fn add_fields_basic() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "a": 2i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$addFields": { "b": { "$add": ["$a", 10i32] } } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("a").unwrap(), 2);
    assert_eq!(result[0].get_i32("b").unwrap(), 12);
}

#[test]
fn add_fields_dotted_creates_nested() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "a": 1i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$addFields": { "nested.value": "$a" } }],
    );
    assert_eq!(result.len(), 1);
    let nested = result[0].get_document("nested").unwrap();
    assert_eq!(nested.get_i32("value").unwrap(), 1);
}

#[test]
fn add_fields_overwrites_existing() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "a": 1i32 }).unwrap();
    let result = run(&col, vec![doc! { "$addFields": { "a": 99i32 } }]);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("a").unwrap(), 99);
}

#[test]
fn add_fields_sees_original_doc_snapshot() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "a": 1i32 }).unwrap();
    // `b` overwrites `a` to 5, but `c` references "$a": it must see the
    // ORIGINAL value (1), not the freshly-assigned 5.
    let result = run(
        &col,
        vec![doc! { "$addFields": { "a": 5i32, "c": "$a" } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("a").unwrap(), 5);
    assert_eq!(result[0].get_i32("c").unwrap(), 1);
}

#[test]
fn set_alias_behaves_like_add_fields() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "a": 3i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$set": { "b": { "$multiply": ["$a", 2i32] } } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("b").unwrap(), 6);
}

// ---------------------------------------------------------------------------
// $unset
// ---------------------------------------------------------------------------

#[test]
fn unset_string_removes_field() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "a": 1i32, "b": 2i32 }).unwrap();
    let result = run(&col, vec![doc! { "$unset": "b" }]);
    assert_eq!(result.len(), 1);
    assert!(result[0].contains_key("a"));
    assert!(!result[0].contains_key("b"));
    // _id is retained (no special-case removal).
    assert!(result[0].contains_key("_id"));
}

#[test]
fn unset_array_removes_multiple_fields() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "a": 1i32, "b": 2i32, "c": 3i32 }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$unset": [Bson::String("a".into()), Bson::String("c".into())] }],
    );
    assert_eq!(result.len(), 1);
    assert!(!result[0].contains_key("a"));
    assert!(result[0].contains_key("b"));
    assert!(!result[0].contains_key("c"));
}

#[test]
fn unset_dotted_removes_nested_field() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "a": { "b": 1i32, "c": 2i32 } }).unwrap();
    let result = run(&col, vec![doc! { "$unset": "a.b" }]);
    assert_eq!(result.len(), 1);
    let a = result[0].get_document("a").unwrap();
    assert!(!a.contains_key("b"));
    assert!(a.contains_key("c"));
}

// ---------------------------------------------------------------------------
// $replaceRoot / $replaceWith
// ---------------------------------------------------------------------------

#[test]
fn replace_root_promotes_subdocument() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "inner": { "x": 1i32, "y": 2i32 } }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$replaceRoot": { "newRoot": "$inner" } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("x").unwrap(), 1);
    assert_eq!(result[0].get_i32("y").unwrap(), 2);
    assert!(!result[0].contains_key("inner"));
}

#[test]
fn replace_root_non_document_errors() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "scalar": 7i32 }).unwrap();
    let result = col
        .aggregate(vec![doc! { "$replaceRoot": { "newRoot": "$scalar" } }])
        .run();
    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("'newRoot' expression must evaluate to an object"),
                "unexpected error: {msg}"
            );
        }
        Ok(_) => panic!("expected error for non-document newRoot"),
    }
}

#[test]
fn replace_with_shorthand_promotes_subdocument() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "inner": { "k": "v" } }).unwrap();
    let result = run(&col, vec![doc! { "$replaceWith": "$inner" }]);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_str("k").unwrap(), "v");
}

// ---------------------------------------------------------------------------
// $unwind
// ---------------------------------------------------------------------------

#[test]
fn unwind_basic_one_doc_per_element() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! {
        "k": "a",
        "items": [Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]
    })
    .unwrap();
    let result = run(&col, vec![doc! { "$unwind": "$items" }]);
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].get_i32("items").unwrap(), 1);
    assert_eq!(result[1].get_i32("items").unwrap(), 2);
    assert_eq!(result[2].get_i32("items").unwrap(), 3);
    // The sibling field is carried on every output document.
    assert_eq!(result[0].get_str("k").unwrap(), "a");
}

#[test]
fn unwind_include_array_index() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "items": [Bson::String("x".into()), Bson::String("y".into())] })
        .unwrap();
    let result = run(
        &col,
        vec![doc! { "$unwind": { "path": "$items", "includeArrayIndex": "idx" } }],
    );
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].get_i64("idx").unwrap(), 0);
    assert_eq!(result[1].get_i64("idx").unwrap(), 1);
}

#[test]
fn unwind_preserve_null_missing_and_empty() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "id": 1i32, "items": Bson::Null }).unwrap();
    col.insert_one(&doc! { "id": 2i32 }).unwrap(); // missing items
    col.insert_one(&doc! { "id": 3i32, "items": Vec::<Bson>::new() }).unwrap();
    let result = run(
        &col,
        vec![
            doc! { "$unwind": {
                "path": "$items",
                "preserveNullAndEmptyArrays": true,
                "includeArrayIndex": "idx"
            } },
            doc! { "$sort": { "id": 1i32 } },
        ],
    );
    // All three are preserved; idx is null for each.
    assert_eq!(result.len(), 3);
    // null stays null
    assert_eq!(result[0].get("items").unwrap(), &Bson::Null);
    assert_eq!(result[0].get("idx").unwrap(), &Bson::Null);
    // missing stays missing
    assert!(!result[1].contains_key("items"));
    assert_eq!(result[1].get("idx").unwrap(), &Bson::Null);
    // empty array -> the path field is removed
    assert!(!result[2].contains_key("items"));
    assert_eq!(result[2].get("idx").unwrap(), &Bson::Null);
}

#[test]
fn unwind_non_array_value_passes_through() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "items": 42i32 }).unwrap();
    let result = run(&col, vec![doc! { "$unwind": "$items" }]);
    // A present, non-array, non-null value passes through as one document.
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_i32("items").unwrap(), 42);
}

#[test]
fn unwind_null_missing_empty_dropped_without_preserve() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    col.insert_one(&doc! { "items": Bson::Null }).unwrap();
    col.insert_one(&doc! {}).unwrap();
    col.insert_one(&doc! { "items": Vec::<Bson>::new() }).unwrap();
    let result = run(&col, vec![doc! { "$unwind": "$items" }]);
    // Without preserve, all three are dropped.
    assert_eq!(result.len(), 0);
}

// ---------------------------------------------------------------------------
// $lookup
// ---------------------------------------------------------------------------

#[test]
fn lookup_basic_equality_join() {
    let dir = TempDir::new().unwrap();
    let (orders, products) = open_two(&dir, "shop", "orders", "products");
    products.insert_one(&doc! { "sku": "a", "name": "Apple" }).unwrap();
    products.insert_one(&doc! { "sku": "b", "name": "Banana" }).unwrap();
    orders.insert_one(&doc! { "sku": "a" }).unwrap();

    let result = run(
        &orders,
        vec![doc! { "$lookup": {
            "from": "products",
            "localField": "sku",
            "foreignField": "sku",
            "as": "matched"
        } }],
    );
    assert_eq!(result.len(), 1);
    let matched = result[0].get_array("matched").unwrap();
    assert_eq!(matched.len(), 1);
    let m = matched[0].as_document().unwrap();
    assert_eq!(m.get_str("name").unwrap(), "Apple");
}

#[test]
fn lookup_array_local_field_matches_per_element() {
    let dir = TempDir::new().unwrap();
    let (orders, products) = open_two(&dir, "shop", "orders", "products");
    products.insert_one(&doc! { "sku": "a", "n": 1i32 }).unwrap();
    products.insert_one(&doc! { "sku": "b", "n": 2i32 }).unwrap();
    products.insert_one(&doc! { "sku": "c", "n": 3i32 }).unwrap();
    // localField is an array: match each element against the scalar foreignField.
    orders
        .insert_one(&doc! { "skus": [Bson::String("a".into()), Bson::String("c".into())] })
        .unwrap();

    let result = run(
        &orders,
        vec![doc! { "$lookup": {
            "from": "products",
            "localField": "skus",
            "foreignField": "sku",
            "as": "matched"
        } }],
    );
    assert_eq!(result.len(), 1);
    let matched = result[0].get_array("matched").unwrap();
    assert_eq!(matched.len(), 2);
}

#[test]
fn lookup_missing_local_matches_null_and_missing_foreign() {
    let dir = TempDir::new().unwrap();
    let (orders, products) = open_two(&dir, "shop", "orders", "products");
    // One foreign doc with explicit null, one missing the field entirely.
    products.insert_one(&doc! { "sku": Bson::Null, "tag": "explicit-null" }).unwrap();
    products.insert_one(&doc! { "tag": "missing-field" }).unwrap();
    products.insert_one(&doc! { "sku": "a", "tag": "present" }).unwrap();
    // The input lacks `sku` -> treated as null, matches both null+missing foreign.
    orders.insert_one(&doc! { "other": 1i32 }).unwrap();

    let result = run(
        &orders,
        vec![doc! { "$lookup": {
            "from": "products",
            "localField": "sku",
            "foreignField": "sku",
            "as": "matched"
        } }],
    );
    assert_eq!(result.len(), 1);
    let matched = result[0].get_array("matched").unwrap();
    // Matches the explicit-null and missing-field foreign docs, not "present".
    assert_eq!(matched.len(), 2);
}

#[test]
fn lookup_nonexistent_from_yields_empty_arrays() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "shop", "orders");
    col.insert_one(&doc! { "sku": "a" }).unwrap();
    let result = run(
        &col,
        vec![doc! { "$lookup": {
            "from": "does_not_exist",
            "localField": "sku",
            "foreignField": "sku",
            "as": "matched"
        } }],
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].get_array("matched").unwrap().len(), 0);
}

#[test]
fn lookup_self_join() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "shop", "nodes");
    col.insert_one(&doc! { "id": 1i32, "parent": Bson::Null }).unwrap();
    col.insert_one(&doc! { "id": 2i32, "parent": 1i32 }).unwrap();
    // Self-lookup: each node joins to its parent node by id.
    let result = run(
        &col,
        vec![
            doc! { "$lookup": {
                "from": "nodes",
                "localField": "parent",
                "foreignField": "id",
                "as": "parent_node"
            } },
            doc! { "$sort": { "id": 1i32 } },
        ],
    );
    assert_eq!(result.len(), 2);
    // Node 1's parent is null -> no node has id null -> empty.
    assert_eq!(result[0].get_array("parent_node").unwrap().len(), 0);
    // Node 2's parent is 1 -> matches node id 1.
    let parents = result[1].get_array("parent_node").unwrap();
    assert_eq!(parents.len(), 1);
    assert_eq!(parents[0].as_document().unwrap().get_i32("id").unwrap(), 1);
}

#[test]
fn lookup_followed_by_unwind_flattens_join() {
    let dir = TempDir::new().unwrap();
    let (orders, products) = open_two(&dir, "shop", "orders", "products");
    products.insert_one(&doc! { "sku": "a", "name": "Apple" }).unwrap();
    products.insert_one(&doc! { "sku": "a", "name": "Apricot" }).unwrap();
    orders.insert_one(&doc! { "sku": "a" }).unwrap();

    let result = run(
        &orders,
        vec![
            doc! { "$lookup": {
                "from": "products",
                "localField": "sku",
                "foreignField": "sku",
                "as": "products"
            } },
            doc! { "$unwind": "$products" },
            doc! { "$sort": { "products.name": 1i32 } },
        ],
    );
    // Two foreign matches, flattened into two documents.
    assert_eq!(result.len(), 2);
    let p0 = result[0].get_document("products").unwrap();
    let p1 = result[1].get_document("products").unwrap();
    assert_eq!(p0.get_str("name").unwrap(), "Apple");
    assert_eq!(p1.get_str("name").unwrap(), "Apricot");
}

// ---------------------------------------------------------------------------
// pymongo-style countDocuments still works
// ---------------------------------------------------------------------------

#[test]
fn count_documents_still_works() {
    let dir = TempDir::new().unwrap();
    let col = open_col(&dir, "test", "c");
    for i in 0..5i32 {
        col.insert_one(&doc! { "n": i }).unwrap();
    }
    assert_eq!(col.count_documents(doc! {}).expect("count"), 5);
    assert_eq!(
        col.count_documents(doc! { "n": { "$gte": 3i32 } }).expect("count"),
        2
    );
}
