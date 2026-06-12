//! Black-box functional tests for `Collection::replace_one` and
//! `Collection::distinct`.
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

use bson::Bson;
use mqlite::{doc, Client, Document, Hint, IndexModel, IndexOptions};
use tempfile::TempDir;

/// Open a temp-file-backed collection for the given namespace.
fn open_collection(tempdir: &TempDir, db: &str, coll: &str) -> mqlite::Collection<Document> {
    let client = Client::open(tempdir.path().join("db.mqlite")).expect("open");
    client.database(db).collection::<Document>(coll)
}

// ---------------------------------------------------------------------------
// replace_one
// ---------------------------------------------------------------------------

#[test]
fn replace_one_basic_replace() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "users");
    col.insert_one(&doc! { "_id": 1, "name": "alice", "age": 30 })
        .expect("insert");

    let result = col
        .replace_one(doc! { "_id": 1 }, &doc! { "name": "bob", "city": "nyc" })
        .run()
        .expect("replace_one");

    assert_eq!(result.matched_count, 1);
    assert_eq!(result.modified_count, 1);
    assert!(result.upserted_id.is_none());

    let found = col.find_one(doc! { "_id": 1 }).expect("find").unwrap();
    assert_eq!(found.get_str("name").unwrap(), "bob");
    assert_eq!(found.get_str("city").unwrap(), "nyc");
    // The replaced document must not retain the old `age` field.
    assert!(found.get("age").is_none());
}

#[test]
fn replace_one_no_match() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "users");
    col.insert_one(&doc! { "_id": 1, "name": "alice" })
        .expect("insert");

    let result = col
        .replace_one(doc! { "_id": 999 }, &doc! { "name": "ghost" })
        .run()
        .expect("replace_one");

    assert_eq!(result.matched_count, 0);
    assert_eq!(result.modified_count, 0);
    assert!(result.upserted_id.is_none());
    assert_eq!(col.count_documents(doc! {}).expect("count"), 1);
}

#[test]
fn replace_one_upsert_insert_uses_filter_id() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "users");

    let result = col
        .replace_one(doc! { "_id": 42 }, &doc! { "name": "carol" })
        .upsert(true)
        .run()
        .expect("replace_one upsert");

    assert_eq!(result.matched_count, 0);
    assert_eq!(result.modified_count, 0);
    assert_eq!(result.upserted_id, Some(Bson::Int32(42)));

    let found = col.find_one(doc! { "_id": 42 }).expect("find").unwrap();
    assert_eq!(found.get_str("name").unwrap(), "carol");
}

#[test]
fn replace_one_upsert_insert_uses_replacement_id() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "users");

    let result = col
        .replace_one(doc! { "name": "dave" }, &doc! { "_id": 7, "name": "dave" })
        .upsert(true)
        .run()
        .expect("replace_one upsert");

    assert_eq!(result.upserted_id, Some(Bson::Int32(7)));
    let found = col.find_one(doc! { "_id": 7 }).expect("find").unwrap();
    assert_eq!(found.get_str("name").unwrap(), "dave");
}

#[test]
fn replace_one_id_preserved_when_replacement_has_none() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "users");
    col.insert_one(&doc! { "_id": 5, "name": "eve" })
        .expect("insert");

    col.replace_one(doc! { "name": "eve" }, &doc! { "name": "eve2" })
        .run()
        .expect("replace_one");

    // The matched `_id` must be preserved on the replacement document.
    let found = col.find_one(doc! { "_id": 5 }).expect("find").unwrap();
    assert_eq!(found.get_str("name").unwrap(), "eve2");
    assert_eq!(found.get_i32("_id").unwrap(), 5);
}

#[test]
fn replace_one_same_id_in_replacement_is_allowed() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "users");
    col.insert_one(&doc! { "_id": 9, "name": "frank" })
        .expect("insert");

    let result = col
        .replace_one(doc! { "_id": 9 }, &doc! { "_id": 9, "name": "frank2" })
        .run()
        .expect("replace_one");
    assert_eq!(result.matched_count, 1);
    assert_eq!(result.modified_count, 1);
}

#[test]
fn replace_one_different_id_is_immutable_error() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "users");
    col.insert_one(&doc! { "_id": 1, "name": "grace" })
        .expect("insert");

    let err = col
        .replace_one(doc! { "_id": 1 }, &doc! { "_id": 2, "name": "grace" })
        .run()
        .expect_err("changing _id must error");
    let msg = err.to_string();
    assert!(
        msg.contains("_id"),
        "error should mention immutable _id, got: {msg}"
    );

    // The original document must be untouched.
    let found = col.find_one(doc! { "_id": 1 }).expect("find").unwrap();
    assert_eq!(found.get_str("name").unwrap(), "grace");
}

#[test]
fn replace_one_with_dollar_keys_errors() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "users");
    col.insert_one(&doc! { "_id": 1, "name": "heidi" })
        .expect("insert");

    let err = col
        .replace_one(doc! { "_id": 1 }, &doc! { "$set": { "name": "x" } })
        .run()
        .expect_err("replacement with $ keys must error");
    let msg = err.to_string();
    assert!(
        msg.contains("update operators"),
        "error should reject update operators, got: {msg}"
    );
}

#[test]
fn replace_one_unique_index_conflict() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "users");
    col.create_index(
        IndexModel::builder()
            .keys(doc! { "email": 1 })
            .options(IndexOptions::new().unique(true))
            .build(),
    )
    .expect("create unique index");

    col.insert_one(&doc! { "_id": 1, "email": "a@x.com" })
        .expect("insert 1");
    col.insert_one(&doc! { "_id": 2, "email": "b@x.com" })
        .expect("insert 2");

    // Replacing doc 2 to collide with doc 1's email must violate the unique
    // index (the existing engine replace path maintains secondary indexes).
    let err = col
        .replace_one(doc! { "_id": 2 }, &doc! { "email": "a@x.com" })
        .run()
        .expect_err("unique conflict must error");
    assert!(
        err.code() == Some(11000) || err.to_string().to_lowercase().contains("duplicate"),
        "expected duplicate-key error, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// distinct
// ---------------------------------------------------------------------------

#[test]
fn distinct_scalar_field() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "items");
    col.insert_one(&doc! { "color": "red" }).expect("insert");
    col.insert_one(&doc! { "color": "blue" }).expect("insert");
    col.insert_one(&doc! { "color": "red" }).expect("insert");

    let mut values = col.distinct("color", doc! {}).expect("distinct");
    values.sort_by_key(|v| v.as_str().unwrap_or_default().to_owned());
    assert_eq!(
        values,
        vec![Bson::String("blue".into()), Bson::String("red".into())]
    );
}

#[test]
fn distinct_array_field_unwraps_elements() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "items");
    col.insert_one(&doc! { "tags": ["a", "b"] }).expect("insert");
    col.insert_one(&doc! { "tags": ["b", "c"] }).expect("insert");

    let mut values = col.distinct("tags", doc! {}).expect("distinct");
    values.sort_by_key(|v| v.as_str().unwrap_or_default().to_owned());
    assert_eq!(
        values,
        vec![
            Bson::String("a".into()),
            Bson::String("b".into()),
            Bson::String("c".into()),
        ]
    );
}

#[test]
fn distinct_dotted_path_through_array_of_docs() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "orders");
    col.insert_one(&doc! {
        "items": [ { "sku": "x" }, { "sku": "y" } ],
    })
    .expect("insert");
    col.insert_one(&doc! {
        "items": [ { "sku": "y" }, { "sku": "z" } ],
    })
    .expect("insert");

    let mut values = col.distinct("items.sku", doc! {}).expect("distinct");
    values.sort_by_key(|v| v.as_str().unwrap_or_default().to_owned());
    assert_eq!(
        values,
        vec![
            Bson::String("x".into()),
            Bson::String("y".into()),
            Bson::String("z".into()),
        ]
    );
}

#[test]
fn distinct_with_filter() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "items");
    col.insert_one(&doc! { "group": "a", "v": 1 }).expect("ins");
    col.insert_one(&doc! { "group": "a", "v": 2 }).expect("ins");
    col.insert_one(&doc! { "group": "b", "v": 3 }).expect("ins");

    let mut values = col.distinct("v", doc! { "group": "a" }).expect("distinct");
    values.sort_by_key(|v| v.as_i32().unwrap_or_default());
    assert_eq!(values, vec![Bson::Int32(1), Bson::Int32(2)]);
}

#[test]
fn distinct_cross_numeric_dedup() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "nums");
    // Int32 1 and Double 1.0 must collapse to a single distinct value; the
    // first-encountered representation (Int32) wins.
    col.insert_one(&doc! { "n": 1_i32 }).expect("insert");
    col.insert_one(&doc! { "n": 1.0_f64 }).expect("insert");

    let values = col.distinct("n", doc! {}).expect("distinct");
    assert_eq!(values, vec![Bson::Int32(1)]);
}

#[test]
fn distinct_null_vs_missing() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "items");
    // Explicit null contributes null; a missing field contributes nothing.
    col.insert_one(&doc! { "_id": 1, "k": Bson::Null }).expect("ins");
    col.insert_one(&doc! { "_id": 2 }).expect("ins");
    col.insert_one(&doc! { "_id": 3, "k": "present" }).expect("ins");

    let values = col.distinct("k", doc! {}).expect("distinct");
    assert!(values.contains(&Bson::Null), "explicit null must appear");
    assert!(values.contains(&Bson::String("present".into())));
    // Only null + "present" — the missing-field doc contributes nothing.
    assert_eq!(values.len(), 2, "got: {values:?}");
}

#[test]
fn distinct_empty_field_name_errors() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "items");
    col.insert_one(&doc! { "x": 1 }).expect("insert");
    let err = col.distinct("", doc! {}).expect_err("empty field errors");
    assert!(err.to_string().to_lowercase().contains("empty"));
}

#[test]
fn distinct_dollar_prefixed_field_errors() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "items");
    col.insert_one(&doc! { "x": 1 }).expect("insert");
    let err = col
        .distinct("$x", doc! {})
        .expect_err("$-prefixed field errors");
    assert!(err.to_string().contains('$'));
}

// ---------------------------------------------------------------------------
// find().projection() — dotted-path projection
// ---------------------------------------------------------------------------

/// Run `find(filter).projection(proj)` and return the single matched doc.
fn find_one_projected(
    col: &mqlite::Collection<Document>,
    filter: Document,
    proj: Document,
) -> Document {
    let mut docs: Vec<Document> = col
        .find(filter)
        .projection(proj)
        .run()
        .expect("find")
        .collect::<Result<Vec<_>, _>>()
        .expect("collect");
    assert_eq!(docs.len(), 1, "expected exactly one match, got {docs:?}");
    docs.pop().unwrap()
}

#[test]
fn projection_nested_inclusion() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "docs");
    col.insert_one(&doc! { "_id": 1, "a": { "b": 2, "c": 3 }, "d": 4 })
        .expect("insert");

    let out = find_one_projected(&col, doc! { "_id": 1 }, doc! { "a.b": 1 });
    assert_eq!(out, doc! { "_id": 1, "a": { "b": 2 } });
}

#[test]
fn projection_nested_exclusion() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "docs");
    col.insert_one(&doc! { "_id": 1, "a": { "b": 2, "c": 3 }, "d": 4 })
        .expect("insert");

    let out = find_one_projected(&col, doc! { "_id": 1 }, doc! { "a.b": 0 });
    assert_eq!(out, doc! { "_id": 1, "a": { "c": 3 }, "d": 4 });
}

#[test]
fn projection_array_of_docs_inclusion_retains_empty_drops_non_docs() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "docs");
    col.insert_one(&doc! {
        "_id": 1,
        "a": [
            doc! { "b": 1, "x": 9 },
            doc! { "x": 9 },
            Bson::Int32(7),
        ],
    })
    .expect("insert");

    let out = find_one_projected(&col, doc! { "_id": 1 }, doc! { "a.b": 1 });
    // Document elements keep only `b`; the doc without `b` becomes `{}` and
    // is retained; the scalar element is removed.
    assert_eq!(out, doc! { "_id": 1, "a": [doc! { "b": 1 }, doc! {}] });
}

#[test]
fn projection_array_of_docs_exclusion() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "docs");
    col.insert_one(&doc! {
        "_id": 1,
        "a": [
            doc! { "b": 1, "c": 2 },
            doc! { "c": 3 },
            Bson::Int32(7),
        ],
    })
    .expect("insert");

    let out = find_one_projected(&col, doc! { "_id": 1 }, doc! { "a.b": 0 });
    assert_eq!(
        out,
        doc! { "_id": 1, "a": [doc! { "c": 2 }, doc! { "c": 3 }, Bson::Int32(7)] }
    );
}

#[test]
fn projection_shared_prefix_merge() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "docs");
    col.insert_one(&doc! { "_id": 1, "a": { "b": 1, "c": 2, "d": 3 } })
        .expect("insert");

    let out = find_one_projected(&col, doc! { "_id": 1 }, doc! { "a.b": 1, "a.c": 1 });
    assert_eq!(out, doc! { "_id": 1, "a": { "b": 1, "c": 2 } });
}

#[test]
fn projection_three_level_depth() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "docs");
    col.insert_one(&doc! { "_id": 1, "a": { "b": { "c": 5, "x": 6 }, "y": 7 } })
        .expect("insert");

    let out = find_one_projected(&col, doc! { "_id": 1 }, doc! { "a.b.c": 1 });
    assert_eq!(out, doc! { "_id": 1, "a": { "b": { "c": 5 } } });
}

#[test]
fn projection_id_excluded_with_nested_inclusion() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "docs");
    col.insert_one(&doc! { "_id": 1, "a": { "b": 2, "c": 3 } })
        .expect("insert");

    let out = find_one_projected(&col, doc! { "_id": 1 }, doc! { "a.b": 1, "_id": 0 });
    assert_eq!(out, doc! { "a": { "b": 2 } });
}

#[test]
fn projection_scalar_at_prefix_omitted() {
    let dir = TempDir::new().expect("tempdir");
    let col = open_collection(&dir, "test", "docs");
    col.insert_one(&doc! { "_id": 1, "a": 7 })
        .expect("insert");

    let out = find_one_projected(&col, doc! { "_id": 1 }, doc! { "a.b": 1 });
    // `a` is a scalar, so the prefix projection omits it entirely.
    assert_eq!(out, doc! { "_id": 1 });
}

// ---------------------------------------------------------------------------
// find().hint()
// ---------------------------------------------------------------------------

/// Seed a collection with three documents and an ascending `email` index.
fn seed_with_email_index(dir: &TempDir) -> mqlite::Collection<Document> {
    let col = open_collection(dir, "test", "users");
    col.insert_one(&doc! { "_id": 1, "email": "a@x.com", "age": 30 })
        .expect("insert 1");
    col.insert_one(&doc! { "_id": 2, "email": "b@x.com", "age": 25 })
        .expect("insert 2");
    col.insert_one(&doc! { "_id": 3, "email": "c@x.com", "age": 40 })
        .expect("insert 3");
    col.create_index(IndexModel::builder().keys(doc! { "email": 1 }).build())
        .expect("create email index");
    col
}

/// Collect all `_id`s from a cursor into a sorted vector for order-independent
/// equality.
fn sorted_ids(cursor: mqlite::Cursor<Document>) -> Vec<i32> {
    let mut ids: Vec<i32> = cursor
        .map(|d| d.expect("doc").get_i32("_id").expect("_id"))
        .collect();
    ids.sort_unstable();
    ids
}

#[test]
fn hint_by_name_matches_unhinted_results() {
    let dir = TempDir::new().expect("tempdir");
    let col = seed_with_email_index(&dir);

    let unhinted = sorted_ids(col.find(doc! { "email": "b@x.com" }).run().expect("find"));
    let hinted = sorted_ids(
        col.find(doc! { "email": "b@x.com" })
            .hint(Hint::Name("email_1".to_owned()))
            .run()
            .expect("find hinted"),
    );
    assert_eq!(unhinted, vec![2]);
    assert_eq!(hinted, unhinted);
}

#[test]
fn hint_by_keys_matches_unhinted_results() {
    let dir = TempDir::new().expect("tempdir");
    let col = seed_with_email_index(&dir);

    let hinted = sorted_ids(
        col.find(doc! { "email": "c@x.com" })
            .hint(Hint::Keys(doc! { "email": 1 }))
            .run()
            .expect("find hinted by keys"),
    );
    assert_eq!(hinted, vec![3]);
}

#[test]
fn hint_explain_reports_chosen_index() {
    let dir = TempDir::new().expect("tempdir");
    let col = seed_with_email_index(&dir);

    let cursor = col
        .find(doc! { "email": "a@x.com" })
        .hint(Hint::Name("email_1".to_owned()))
        .run()
        .expect("find hinted");
    let plan = cursor.explain().expect("explain");
    assert!(!plan.full_scan);
    assert_eq!(plan.index_used.as_deref(), Some("email_1"));
}

#[test]
fn hint_with_no_filter_bound_scans_whole_index() {
    let dir = TempDir::new().expect("tempdir");
    let col = seed_with_email_index(&dir);

    // Empty filter + hint: unbounded full index scan returns every document.
    let cursor = col
        .find(doc! {})
        .hint(Hint::Name("email_1".to_owned()))
        .run()
        .expect("find hinted unbounded");
    let plan = cursor.explain().expect("explain");
    assert!(!plan.full_scan);
    assert_eq!(plan.index_used.as_deref(), Some("email_1"));

    let ids = sorted_ids(
        col.find(doc! {})
            .hint(Hint::Name("email_1".to_owned()))
            .run()
            .expect("find hinted unbounded"),
    );
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn natural_hint_forces_collscan_explain() {
    let dir = TempDir::new().expect("tempdir");
    let col = seed_with_email_index(&dir);

    let cursor = col
        .find(doc! { "email": "a@x.com" })
        .hint(Hint::Keys(doc! { "$natural": 1 }))
        .run()
        .expect("find natural");
    let plan = cursor.explain().expect("explain");
    assert!(plan.full_scan);
    assert!(plan.index_used.is_none());

    // Results are still correct.
    let ids = sorted_ids(
        col.find(doc! { "email": "a@x.com" })
            .hint(Hint::Keys(doc! { "$natural": 1 }))
            .run()
            .expect("find natural"),
    );
    assert_eq!(ids, vec![1]);
}

#[test]
fn bad_hint_surfaces_error() {
    let dir = TempDir::new().expect("tempdir");
    let col = seed_with_email_index(&dir);

    let result = col
        .find(doc! { "email": "a@x.com" })
        .hint(Hint::Name("does_not_exist".to_owned()))
        .run();
    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("bad hint must error"),
    };
    assert!(
        err.to_string()
            .contains("hint provided does not correspond to an existing index"),
        "unexpected error: {err}"
    );
}
