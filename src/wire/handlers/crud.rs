// === Command handlers: CRUD ===

use std::sync::{Arc, Mutex};

use bson::{doc, Document};

use super::super::errors::{err_bad_value, err_collation_unsupported, err_from_mqlite};
use super::super::server::{ConnectionCursors, ServerState};
use super::{batch_size, extract_db_name, get_i64, qualified_coll};
use crate::options::{
    FindOneAndDeleteOptions, FindOneAndUpdateOptions, FindOptions, Hint, InsertManyOptions,
    ReturnDocument, UpdateOptions,
};
use crate::update::UpdateModifications;

fn reject_collation(doc: &Document) -> Option<Document> {
    doc.contains_key("collation")
        .then(err_collation_unsupported)
}

/// Parse a wire update payload (`u` / `update`) into an [`UpdateModifications`].
///
/// A document value is a classic operator/replacement update; an array value is
/// an aggregation pipeline. Any other shape (or absence) yields `None`.
fn parse_update_modifications(value: Option<&bson::Bson>) -> Option<UpdateModifications> {
    match value? {
        bson::Bson::Document(d) => Some(UpdateModifications::Document(d.clone())),
        bson::Bson::Array(stages) => {
            let pipeline: Vec<Document> = stages
                .iter()
                .filter_map(|b| b.as_document().cloned())
                .collect();
            Some(UpdateModifications::Pipeline(pipeline))
        }
        _ => None,
    }
}

/// Parse a wire `arrayFilters` value into the engine's `Vec<Document>` form.
fn parse_array_filters(value: Option<&bson::Bson>) -> Option<Vec<Document>> {
    let arr = value?.as_array()?;
    Some(
        arr.iter()
            .filter_map(|b| b.as_document().cloned())
            .collect(),
    )
}

/// Parse a wire `hint` field into a [`Hint`].
///
/// MongoDB accepts the `hint` value as either a string (an index name) or a
/// document (an index key pattern). Any other BSON type is rejected as a
/// `BadValue` error, mirroring the server.
fn parse_hint(value: &bson::Bson) -> std::result::Result<Hint, Document> {
    match value {
        bson::Bson::String(name) => Ok(Hint::Name(name.clone())),
        bson::Bson::Document(keys) => Ok(Hint::Keys(keys.clone())),
        _ => Err(err_bad_value("hint must be a string or a document")),
    }
}

/// `insert` — insert one or more documents.
///
/// Accepts documents from either `body["documents"]` (Kind-0) or a Kind-1
/// `"documents"` section (pymongo bulk path); see `merge_doc_sequences_into_body`.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "n": <count>, "writeErrors": [...], "ok": 1.0 }
/// ```
pub(super) fn handle_insert(body: &Document, state: &ServerState) -> Document {
    if let Some(error) = reject_collation(body) {
        return error;
    }

    let coll_name = match body.get_str("insert") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("insert requires a collection name string"),
    };

    // Documents may arrive via Kind-1 merge or body array.
    let docs: Vec<Document> = match body.get_array("documents") {
        Ok(arr) => arr
            .iter()
            .filter_map(|b| b.as_document().cloned())
            .collect(),
        Err(_) => return err_bad_value("insert requires a \"documents\" array"),
    };

    if docs.is_empty() {
        // MongoDB allows empty inserts; return n=0 with ok:1.
        return doc! { "n": 0i32, "ok": 1.0_f64 };
    }

    let ns = qualified_coll(body, &coll_name);
    let ordered = body.get_bool("ordered").unwrap_or(true);
    let opts = InsertManyOptions { ordered };

    match state.database.insert_many(&ns, &docs, opts) {
        Ok(result) => {
            let n = result.inserted_ids.len() as i32;
            if result.errors.is_empty() {
                doc! { "n": n, "ok": 1.0_f64 }
            } else {
                let write_errors: bson::Array = result
                    .errors
                    .iter()
                    .map(|e| {
                        bson::Bson::Document(doc! {
                            "index": e.index as i32,
                            "code": e.code,
                            "errmsg": &e.message,
                        })
                    })
                    .collect();
                doc! {
                    "n": n,
                    "writeErrors": write_errors,
                    "ok": 1.0_f64,
                }
            }
        }
        Err(e) => err_from_mqlite(e),
    }
}

/// `find` — query documents with filter, sort, projection, limit, skip.
///
/// Returns a cursor response with `firstBatch` and a server-side cursor ID
/// (non-zero when there are more results than the requested `batchSize`).
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "cursor": { "firstBatch": [...], "id": <cursor_id>, "ns": "db.coll" }, "ok": 1.0 }
/// ```
pub(super) fn handle_find(
    body: &Document,
    state: &ServerState,
    cursors: &Arc<Mutex<ConnectionCursors>>,
) -> Document {
    if let Some(error) = reject_collation(body) {
        return error;
    }

    let coll_name = match body.get_str("find") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("find requires a collection name string"),
    };

    let filter = body.get_document("filter").cloned().unwrap_or_default();

    let mut opts = FindOptions::default();
    if let Ok(sort) = body.get_document("sort") {
        opts.sort = Some(sort.clone());
    }
    if let Ok(proj) = body.get_document("projection") {
        opts.projection = Some(proj.clone());
    }
    if let Some(limit) = get_i64(body, "limit") {
        opts.limit = Some(limit);
    }
    if let Some(skip) = get_i64(body, "skip") {
        opts.skip = Some(skip as u64);
    }
    if let Some(hint) = body.get("hint") {
        match parse_hint(hint) {
            Ok(h) => opts.hint = Some(h),
            Err(e) => return e,
        }
    }

    let batch_size = batch_size(body);

    let ns = qualified_coll(body, &coll_name);
    let cursor = match state.database.find::<Document>(&ns, filter, opts) {
        Ok(c) => c,
        Err(e) => return err_from_mqlite(e),
    };
    let plan = match cursor.explain() {
        Ok(plan) => plan,
        Err(e) => return err_from_mqlite(e),
    };

    // Collect all matching documents (cursor is already fully buffered in
    // memory by the storage engine, so this is a cheap move operation).
    let mut all_docs: Vec<Document> = Vec::with_capacity(batch_size);
    for result in cursor {
        match result {
            Ok(d) => all_docs.push(d),
            Err(e) => return err_from_mqlite(e),
        }
    }

    let split_at = batch_size.min(all_docs.len());
    let remaining: Vec<Document> = all_docs.drain(split_at..).collect();
    let first_batch: bson::Array = all_docs.into_iter().map(bson::Bson::Document).collect();

    // Store a server-side cursor for the remaining documents if any.
    let cursor_id: i64 = if remaining.is_empty() {
        0
    } else {
        let remaining_cursor = crate::Cursor::<Document>::new(remaining, plan);
        cursors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .store(remaining_cursor)
    };

    doc! {
        "cursor": {
            "firstBatch": first_batch,
            "id": bson::Bson::Int64(cursor_id),
            "ns": ns,
        },
        "ok": 1.0_f64,
    }
}

/// `update` — update matching documents.
///
/// Processes the `updates` array; each entry may set `multi` and `upsert`.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "n": <matched>, "nModified": <modified>, "upserted": [...], "ok": 1.0 }
/// ```
pub(super) fn handle_update(body: &Document, state: &ServerState) -> Document {
    if let Some(error) = reject_collation(body) {
        return error;
    }

    let coll_name = match body.get_str("update") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("update requires a collection name string"),
    };

    let updates = match body.get_array("updates") {
        Ok(arr) => arr.clone(),
        Err(_) => return err_bad_value("update requires an \"updates\" array"),
    };

    let mut total_matched: i64 = 0;
    let mut total_modified: i64 = 0;
    let mut upserted: bson::Array = Vec::with_capacity(updates.len());
    let mut write_errors: bson::Array = Vec::with_capacity(updates.len());
    let ns = qualified_coll(body, &coll_name);

    for (i, spec_bson) in updates.iter().enumerate() {
        let spec = match spec_bson.as_document() {
            Some(d) => d,
            None => continue,
        };

        if let Some(error) = reject_collation(spec) {
            return error;
        }

        let filter = spec.get_document("q").cloned().unwrap_or_default();
        let update_mods = match parse_update_modifications(spec.get("u")) {
            Some(m) => m,
            None => {
                write_errors.push(bson::Bson::Document(doc! {
                    "index": i as i32,
                    "code": crate::error::codes::BAD_VALUE,
                    "errmsg": "update spec missing or invalid \"u\" field",
                }));
                continue;
            }
        };
        let multi = spec.get_bool("multi").unwrap_or(false);
        let upsert = spec.get_bool("upsert").unwrap_or(false);
        let opts = UpdateOptions {
            upsert,
            array_filters: parse_array_filters(spec.get("arrayFilters")),
        };

        let result = if multi {
            state.database.update_many(&ns, filter, update_mods, opts)
        } else {
            state.database.update_one(&ns, filter, update_mods, opts)
        };

        match result {
            Ok(r) => {
                total_matched += r.matched_count as i64;
                total_modified += r.modified_count as i64;
                if let Some(id) = r.upserted_id {
                    upserted.push(bson::Bson::Document(doc! {
                        "index": i as i32,
                        "_id": id,
                    }));
                }
            }
            Err(e) => {
                let code = e.code().unwrap_or(crate::error::codes::INTERNAL_ERROR);
                write_errors.push(bson::Bson::Document(doc! {
                    "index": i as i32,
                    "code": code,
                    "errmsg": e.to_string(),
                }));
            }
        }
    }

    let mut response = doc! {
        "n": bson::Bson::Int64(total_matched),
        "nModified": bson::Bson::Int64(total_modified),
        "ok": 1.0_f64,
    };
    if !upserted.is_empty() {
        response.insert("upserted", bson::Bson::Array(upserted));
    }
    if !write_errors.is_empty() {
        response.insert("writeErrors", bson::Bson::Array(write_errors));
    }
    response
}

/// `delete` — delete matching documents.
///
/// Processes the `deletes` array; `limit: 1` means deleteOne, `limit: 0` means
/// deleteMany (matching MongoDB wire protocol semantics).
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "n": <deleted>, "ok": 1.0 }
/// ```
pub(super) fn handle_delete(body: &Document, state: &ServerState) -> Document {
    if let Some(error) = reject_collation(body) {
        return error;
    }

    let coll_name = match body.get_str("delete") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("delete requires a collection name string"),
    };

    let deletes = match body.get_array("deletes") {
        Ok(arr) => arr.clone(),
        Err(_) => return err_bad_value("delete requires a \"deletes\" array"),
    };

    let mut total_deleted: i64 = 0;
    let mut write_errors: bson::Array = Vec::with_capacity(deletes.len());
    let ns = qualified_coll(body, &coll_name);

    for (i, spec_bson) in deletes.iter().enumerate() {
        let spec = match spec_bson.as_document() {
            Some(d) => d,
            None => continue,
        };

        if let Some(error) = reject_collation(spec) {
            return error;
        }

        let filter = spec.get_document("q").cloned().unwrap_or_default();
        // `limit: 0` = delete all matching; `limit: 1` (or any non-zero) = delete one.
        let limit = get_i64(spec, "limit").unwrap_or(1);

        let result = if limit == 0 {
            state.database.delete_many(&ns, filter)
        } else {
            state.database.delete_one(&ns, filter)
        };

        match result {
            Ok(r) => total_deleted += r.deleted_count as i64,
            Err(e) => {
                let code = e.code().unwrap_or(crate::error::codes::INTERNAL_ERROR);
                write_errors.push(bson::Bson::Document(doc! {
                    "index": i as i32,
                    "code": code,
                    "errmsg": e.to_string(),
                }));
            }
        }
    }

    let mut response = doc! {
        "n": bson::Bson::Int64(total_deleted),
        "ok": 1.0_f64,
    };
    if !write_errors.is_empty() {
        response.insert("writeErrors", bson::Bson::Array(write_errors));
    }
    response
}

/// `findAndModify` — atomically find and modify (update or remove) a document.
///
/// The response uses the `value` field (not `document`) as required by
/// MongoDB 8.0 wire protocol semantics.
///
/// Response format (MongoDB 8.0):
/// ```json
/// {
///   "value": <doc_or_null>,
///   "lastErrorObject": { "n": 1, "updatedExisting": true },
///   "ok": 1.0
/// }
/// ```
pub(super) fn handle_find_and_modify(body: &Document, state: &ServerState) -> Document {
    if let Some(error) = reject_collation(body) {
        return error;
    }

    // Command key can be either "findAndModify" or "findandmodify" (case-insensitive
    // dispatch normalises to lowercase in route_command).
    let coll_name = body
        .get_str("findandmodify")
        .or_else(|_| body.get_str("findAndModify"))
        .map(|s| s.to_owned())
        .unwrap_or_default();
    if coll_name.is_empty() {
        return err_bad_value("findAndModify requires a collection name string");
    }

    let filter = body.get_document("query").cloned().unwrap_or_default();
    let remove = body.get_bool("remove").unwrap_or(false);
    let return_new = body.get_bool("new").unwrap_or(false);
    let upsert = body.get_bool("upsert").unwrap_or(false);
    let sort = body.get_document("sort").ok().cloned();
    let ns = qualified_coll(body, &coll_name);

    if remove {
        // ---- findAndModify + remove ----
        let opts = FindOneAndDeleteOptions { sort };
        match state
            .database
            .find_one_and_delete_with_options::<Document>(&ns, filter, opts)
        {
            Ok(Some(doc)) => doc! {
                "value": bson::Bson::Document(doc),
                "lastErrorObject": { "n": 1i32 },
                "ok": 1.0_f64,
            },
            Ok(None) => doc! {
                "value": bson::Bson::Null,
                "lastErrorObject": { "n": 0i32 },
                "ok": 1.0_f64,
            },
            Err(e) => err_from_mqlite(e),
        }
    } else {
        // ---- findAndModify + update ----
        let update_mods = match parse_update_modifications(body.get("update")) {
            Some(m) => m,
            None => {
                return err_bad_value("findAndModify requires either \"update\" or \"remove\"")
            }
        };

        let return_document = if return_new {
            ReturnDocument::After
        } else {
            ReturnDocument::Before
        };
        let opts = FindOneAndUpdateOptions {
            return_document,
            upsert,
            sort,
            array_filters: parse_array_filters(body.get("arrayFilters")),
        };

        match state
            .database
            .find_one_and_update_with_options::<Document>(&ns, filter, update_mods, opts)
        {
            Ok(Some(doc)) => {
                // A document was returned.
                // With ReturnDocument::Before this is the original (updatedExisting=true).
                // With ReturnDocument::After this is the post-update doc; we cannot
                // distinguish update-of-existing vs upsert from the return value alone,
                // so we conservatively report updatedExisting=true (the common path).
                doc! {
                    "value": bson::Bson::Document(doc),
                    "lastErrorObject": {
                        "n": 1i32,
                        "updatedExisting": true,
                    },
                    "ok": 1.0_f64,
                }
            }
            Ok(None) => {
                // No document found (or upsert with ReturnDocument::Before).
                doc! {
                    "value": bson::Bson::Null,
                    "lastErrorObject": {
                        "n": if upsert { 1i32 } else { 0i32 },
                        "updatedExisting": false,
                    },
                    "ok": 1.0_f64,
                }
            }
            Err(e) => err_from_mqlite(e),
        }
    }
}

/// `count` — count documents matching an optional query.
///
/// Applies `skip` first, then `limit`.  A `limit` of 0 (or absent) means
/// unlimited; a negative `limit` is treated as its absolute value.  `skip`
/// must be non-negative.  Counting a non-existent collection returns `n: 0`.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "n": <count>, "ok": 1.0 }
/// ```
pub(super) fn handle_count(body: &Document, state: &ServerState) -> Document {
    let coll_name = match body.get_str("count") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("count requires a collection name string"),
    };

    let filter = body.get_document("query").cloned().unwrap_or_default();
    let ns = qualified_coll(body, &coll_name);

    let skip = get_i64(body, "skip").unwrap_or(0);
    if skip < 0 {
        return err_bad_value("skip must be non-negative");
    }
    // limit 0 / absent = unlimited; negative limit = its absolute value.
    let limit = get_i64(body, "limit").unwrap_or(0).unsigned_abs();

    // Fast path: no skip/limit constraints — count directly.
    if skip == 0 && limit == 0 {
        return match state.database.count_documents(&ns, filter) {
            Ok(n) => doc! { "n": n as i64, "ok": 1.0_f64 },
            Err(e) => err_from_mqlite(e),
        };
    }

    // Constrained path: apply skip then limit via the find machinery.
    let mut opts = FindOptions {
        skip: Some(skip as u64),
        ..FindOptions::default()
    };
    if limit > 0 {
        opts.limit = Some(limit as i64);
    }

    let cursor = match state.database.find::<Document>(&ns, filter, opts) {
        Ok(c) => c,
        Err(e) => return err_from_mqlite(e),
    };
    let mut n: i64 = 0;
    for result in cursor {
        if let Err(e) = result {
            return err_from_mqlite(e);
        }
        n += 1;
    }
    doc! { "n": n, "ok": 1.0_f64 }
}

/// `distinct` — return the distinct values of a field across matching documents.
///
/// The `key` field names the target field; an optional `query` filters the
/// documents considered.  A missing or non-string `key` yields a `BadValue`
/// error.  A non-existent collection returns an empty `values` array.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "values": [...], "ok": 1.0 }
/// ```
pub(super) fn handle_distinct(body: &Document, state: &ServerState) -> Document {
    let coll_name = match body.get_str("distinct") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("distinct requires a collection name string"),
    };

    let key = match body.get_str("key") {
        Ok(k) => k,
        Err(_) => return err_bad_value("distinct requires a \"key\" string"),
    };

    let filter = body.get_document("query").cloned().unwrap_or_default();
    let ns = qualified_coll(body, &coll_name);

    match state.database.distinct(&ns, key, filter) {
        Ok(values) => doc! {
            "values": values,
            "ok": 1.0_f64,
        },
        Err(e) => err_from_mqlite(e),
    }
}

/// `aggregate` — run an aggregation pipeline and return a cursor batch.
///
/// Request shape:
/// ```json
/// { "aggregate": "<coll>", "pipeline": [ ... ], "cursor": { "batchSize": N } }
/// ```
///
/// The `cursor` field is required (absent yields a `BadValue`). A collectionless
/// `{aggregate: 1}` form is rejected because no collectionless stages are
/// supported. A nonexistent collection with an ordinary pipeline returns an
/// empty `firstBatch` with cursor id 0. Results beyond `batchSize` are stored
/// in a server-side cursor for `getMore`, exactly like [`handle_find`].
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "cursor": { "firstBatch": [...], "id": <cursor_id>, "ns": "db.coll" }, "ok": 1.0 }
/// ```
pub(super) fn handle_aggregate(
    body: &Document,
    state: &ServerState,
    cursors: &Arc<Mutex<ConnectionCursors>>,
) -> Document {
    if let Some(error) = reject_collation(body) {
        return error;
    }

    let coll_name = match body.get_str("aggregate") {
        Ok(s) => s.to_owned(),
        // A numeric `{aggregate: 1}` requests collectionless stages, which
        // mqlite does not support.
        Err(_) => {
            return err_bad_value(
                "$_internalListCollections and other collectionless aggregations \
                 are not supported",
            )
        }
    };

    // The `cursor` field is mandatory for aggregate (except with explain,
    // which mqlite does not wire for aggregate).
    if body.get_document("cursor").is_err() {
        return err_bad_value(
            "The 'cursor' option is required, except for aggregate with the \
             explain argument",
        );
    }

    let pipeline: Vec<Document> = match body.get_array("pipeline") {
        Ok(arr) => arr
            .iter()
            .filter_map(|b| b.as_document().cloned())
            .collect(),
        Err(_) => return err_bad_value("aggregate requires a \"pipeline\" array"),
    };

    let batch_size = batch_size(&body.get_document("cursor").cloned().unwrap_or_default());

    let ns = qualified_coll(body, &coll_name);
    let cursor = match state.database.aggregate(&ns, pipeline) {
        Ok(c) => c,
        Err(e) => return err_from_mqlite(e),
    };
    let plan = match cursor.explain() {
        Ok(plan) => plan,
        Err(e) => return err_from_mqlite(e),
    };

    let mut all_docs: Vec<Document> = Vec::with_capacity(batch_size);
    for result in cursor {
        match result {
            Ok(d) => all_docs.push(d),
            Err(e) => return err_from_mqlite(e),
        }
    }

    let split_at = batch_size.min(all_docs.len());
    let remaining: Vec<Document> = all_docs.drain(split_at..).collect();
    let first_batch: bson::Array = all_docs.into_iter().map(bson::Bson::Document).collect();

    let cursor_id: i64 = if remaining.is_empty() {
        0
    } else {
        let remaining_cursor = crate::Cursor::<Document>::new(remaining, plan);
        cursors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .store(remaining_cursor)
    };

    doc! {
        "cursor": {
            "firstBatch": first_batch,
            "id": bson::Bson::Int64(cursor_id),
            "ns": ns,
        },
        "ok": 1.0_f64,
    }
}

/// `explain` — report the query plan for an inner command without executing it.
///
/// Only an inner `find` command is supported.  Any other inner command yields
/// the same `CommandNotFound` shape as [`super::admin::handle_unknown`], naming
/// the unsupported explain target.  All `verbosity` values return the
/// `queryPlanner`-only response (no `executionStats`).
///
/// Response format (MongoDB 8.0, simplified):
/// ```json
/// {
///   "explainVersion": "1",
///   "queryPlanner": {
///     "namespace": "<db.coll>",
///     "indexFilterSet": false,
///     "parsedQuery": <filter>,
///     "winningPlan": <plan>,
///     "rejectedPlans": []
///   },
///   "command": <innerCommand>,
///   "ok": 1.0
/// }
/// ```
pub(super) fn handle_explain(body: &Document, state: &ServerState) -> Document {
    let inner = match body.get_document("explain") {
        Ok(d) => d.clone(),
        Err(_) => return err_bad_value("explain requires an inner command document"),
    };

    let inner_command = match inner.keys().next() {
        Some(name) => name.as_str(),
        None => return err_bad_value("explain inner command document is empty"),
    };

    if !inner_command.eq_ignore_ascii_case("find") {
        return super::admin::handle_unknown(&format!("explain :: {inner_command}"));
    }

    let coll_name = match inner.get_str("find") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("explain find requires a collection name string"),
    };

    let filter = inner.get_document("filter").cloned().unwrap_or_default();

    let mut opts = FindOptions::default();
    if let Ok(sort) = inner.get_document("sort") {
        opts.sort = Some(sort.clone());
    }
    if let Ok(proj) = inner.get_document("projection") {
        opts.projection = Some(proj.clone());
    }
    if let Some(limit) = get_i64(&inner, "limit") {
        opts.limit = Some(limit);
    }
    if let Some(skip) = get_i64(&inner, "skip") {
        opts.skip = Some(skip as u64);
    }
    if let Some(hint) = inner.get("hint") {
        match parse_hint(hint) {
            Ok(h) => opts.hint = Some(h),
            Err(e) => return e,
        }
    }

    // The namespace uses the outer `$db` (explain runs in the command's db).
    let ns = format!("{}.{}", extract_db_name(body), coll_name);
    let cursor = match state.database.find::<Document>(&ns, filter.clone(), opts) {
        Ok(c) => c,
        Err(e) => return err_from_mqlite(e),
    };
    let plan = match cursor.explain() {
        Ok(p) => p,
        Err(e) => return err_from_mqlite(e),
    };

    let winning_plan = if plan.full_scan {
        doc! {
            "stage": "COLLSCAN",
            "filter": filter.clone(),
        }
    } else {
        // Index scan: report the selected index name and its key pattern.
        // The synthetic `_id_` index is never returned by `list_indexes`
        // (the wire layer fabricates it), so its key pattern is hardcoded.
        let index_name = plan.index_used.unwrap_or_default();
        let key_pattern = if index_name == "_id_" {
            doc! { "_id": 1i32 }
        } else {
            state
                .database
                .list_indexes(&ns)
                .ok()
                .and_then(|idxs| idxs.into_iter().find(|i| i.name == index_name))
                .map(|i| i.keys)
                .unwrap_or_default()
        };
        doc! {
            "stage": "FETCH",
            "inputStage": {
                "stage": "IXSCAN",
                "indexName": index_name,
                "keyPattern": key_pattern,
            },
        }
    };

    doc! {
        "explainVersion": "1",
        "queryPlanner": {
            "namespace": ns,
            "indexFilterSet": false,
            "parsedQuery": filter,
            "winningPlan": winning_plan,
            "rejectedPlans": bson::Array::new(),
        },
        "command": inner,
        "ok": 1.0_f64,
    }
}
