//! Wire-protocol tests for partial-index command handling.
//!
//! Exercises `createIndexes` accepting `partialFilterExpression` and
//! `listIndexes` echoing it back. Registered as a sibling test module of
//! `server_commands` in `wire::server`, so handler symbols resolve through
//! the same `super::*` import surface.

use super::*;
use bson::doc;

/// Fetch the user index document (the entry after the synthetic `_id_`) from a
/// `listIndexes` response.
fn first_user_index(list_res: &bson::Document) -> bson::Document {
    let batch = list_res
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap();
    batch[1].as_document().unwrap().clone()
}

#[test]
fn create_index_with_partial_filter_is_echoed_by_list_indexes() {
    let state = ServerState::default();
    let pfe = doc! { "rating": { "$gte": 4i32 } };
    let result = handle_create_indexes(
        &doc! {
            "createIndexes": "pfecoll",
            "indexes": [{
                "key": {"rating": 1i32},
                "name": "rating_1",
                "partialFilterExpression": pfe.clone(),
            }],
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");

    let list_res =
        handle_list_indexes(&doc! { "listIndexes": "pfecoll", "$db": "local" }, &state);
    let idx_doc = first_user_index(&list_res);
    assert_eq!(idx_doc.get_str("name").unwrap(), "rating_1");
    assert_eq!(
        idx_doc.get_document("partialFilterExpression").unwrap(),
        &pfe
    );
}

#[test]
fn create_partial_unique_index_echoes_both_options() {
    let state = ServerState::default();
    let pfe = doc! { "email": { "$exists": true } };
    handle_create_indexes(
        &doc! {
            "createIndexes": "puniqcoll",
            "indexes": [{
                "key": {"email": 1i32},
                "name": "email_1",
                "unique": true,
                "partialFilterExpression": pfe.clone(),
            }],
            "$db": "local",
        },
        &state,
    );
    let list_res =
        handle_list_indexes(&doc! { "listIndexes": "puniqcoll", "$db": "local" }, &state);
    let idx_doc = first_user_index(&list_res);
    assert!(idx_doc.get_bool("unique").unwrap());
    assert_eq!(
        idx_doc.get_document("partialFilterExpression").unwrap(),
        &pfe
    );
}

#[test]
fn ordinary_index_has_no_partial_filter_in_list_indexes() {
    let state = ServerState::default();
    handle_create_indexes(
        &doc! {
            "createIndexes": "plaincoll",
            "indexes": [{"key": {"name": 1i32}, "name": "name_1"}],
            "$db": "local",
        },
        &state,
    );
    let list_res =
        handle_list_indexes(&doc! { "listIndexes": "plaincoll", "$db": "local" }, &state);
    let idx_doc = first_user_index(&list_res);
    assert!(
        idx_doc.get("partialFilterExpression").is_none(),
        "non-partial index must not report partialFilterExpression"
    );
}

#[test]
fn create_partial_and_sparse_together_errors() {
    let state = ServerState::default();
    let result = handle_create_indexes(
        &doc! {
            "createIndexes": "conflictcoll",
            "indexes": [{
                "key": {"x": 1i32},
                "name": "x_1",
                "sparse": true,
                "partialFilterExpression": {"x": {"$gt": 1i32}},
            }],
            "$db": "local",
        },
        &state,
    );
    assert_eq!(
        result.get_f64("ok").unwrap(),
        0.0,
        "partial + sparse must be rejected: {result:?}"
    );
}

#[test]
fn create_partial_with_non_document_filter_errors() {
    let state = ServerState::default();
    let result = handle_create_indexes(
        &doc! {
            "createIndexes": "badpfecoll",
            "indexes": [{
                "key": {"x": 1i32},
                "name": "x_1",
                "partialFilterExpression": 42i32,
            }],
            "$db": "local",
        },
        &state,
    );
    assert_eq!(
        result.get_f64("ok").unwrap(),
        0.0,
        "non-document partialFilterExpression must be rejected: {result:?}"
    );
}

#[test]
fn create_ttl_index_is_echoed_by_list_indexes() {
    let state = ServerState::default();
    let result = handle_create_indexes(
        &doc! {
            "createIndexes": "ttlcoll",
            "indexes": [{
                "key": {"createdAt": 1i32},
                "name": "createdAt_1",
                "expireAfterSeconds": 3600i32,
            }],
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");

    let list_res =
        handle_list_indexes(&doc! { "listIndexes": "ttlcoll", "$db": "local" }, &state);
    let idx_doc = first_user_index(&list_res);
    assert_eq!(idx_doc.get_str("name").unwrap(), "createdAt_1");
    assert_eq!(idx_doc.get_i64("expireAfterSeconds").unwrap(), 3600);
}

#[test]
fn create_ttl_index_accepts_integral_double_seconds() {
    let state = ServerState::default();
    let result = handle_create_indexes(
        &doc! {
            "createIndexes": "ttldoublecoll",
            "indexes": [{
                "key": {"createdAt": 1i32},
                "name": "createdAt_1",
                "expireAfterSeconds": 3600.0f64,
            }],
            "$db": "local",
        },
        &state,
    );
    assert_eq!(result.get_f64("ok").unwrap(), 1.0, "{result:?}");

    let list_res =
        handle_list_indexes(&doc! { "listIndexes": "ttldoublecoll", "$db": "local" }, &state);
    let idx_doc = first_user_index(&list_res);
    assert_eq!(idx_doc.get_i64("expireAfterSeconds").unwrap(), 3600);
}

#[test]
fn create_ttl_index_with_fractional_seconds_errors() {
    let state = ServerState::default();
    let result = handle_create_indexes(
        &doc! {
            "createIndexes": "badttlcoll",
            "indexes": [{
                "key": {"createdAt": 1i32},
                "name": "createdAt_1",
                "expireAfterSeconds": 1.5f64,
            }],
            "$db": "local",
        },
        &state,
    );
    assert_eq!(
        result.get_f64("ok").unwrap(),
        0.0,
        "fractional expireAfterSeconds must be rejected: {result:?}"
    );
}

#[test]
fn create_ttl_index_with_non_numeric_seconds_errors() {
    let state = ServerState::default();
    let result = handle_create_indexes(
        &doc! {
            "createIndexes": "badttlcoll2",
            "indexes": [{
                "key": {"createdAt": 1i32},
                "name": "createdAt_1",
                "expireAfterSeconds": "soon",
            }],
            "$db": "local",
        },
        &state,
    );
    assert_eq!(
        result.get_f64("ok").unwrap(),
        0.0,
        "non-numeric expireAfterSeconds must be rejected: {result:?}"
    );
}

#[test]
fn create_ttl_index_on_compound_key_errors() {
    let state = ServerState::default();
    let result = handle_create_indexes(
        &doc! {
            "createIndexes": "ttlcompoundcoll",
            "indexes": [{
                "key": {"a": 1i32, "b": 1i32},
                "name": "a_1_b_1",
                "expireAfterSeconds": 60i32,
            }],
            "$db": "local",
        },
        &state,
    );
    assert_eq!(
        result.get_f64("ok").unwrap(),
        0.0,
        "TTL on a compound index must be rejected: {result:?}"
    );
}
