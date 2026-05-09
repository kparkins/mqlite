// === Command handlers: CRUD ===

use std::sync::{Arc, Mutex};

use bson::{doc, Document};

use super::super::errors::{err_bad_value, err_collation_unsupported, err_from_mqlite};
use super::super::server::{ConnectionCursors, ServerState};
use super::{batch_size, get_i64, qualified_coll};
use crate::options::{
    FindOneAndDeleteOptions, FindOneAndUpdateOptions, FindOptions, InsertManyOptions,
    ReturnDocument, UpdateOptions,
};

fn reject_collation(doc: &Document) -> Option<Document> {
    doc.contains_key("collation")
        .then(err_collation_unsupported)
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
        let update_doc = match spec.get_document("u") {
            Ok(u) => u.clone(),
            Err(_) => {
                write_errors.push(bson::Bson::Document(doc! {
                    "index": i as i32,
                    "code": crate::error::codes::BAD_VALUE,
                    "errmsg": "update spec missing required \"u\" field",
                }));
                continue;
            }
        };
        let multi = spec.get_bool("multi").unwrap_or(false);
        let upsert = spec.get_bool("upsert").unwrap_or(false);
        let opts = UpdateOptions { upsert };

        let result = if multi {
            state.database.update_many(&ns, filter, update_doc, opts)
        } else {
            state.database.update_one(&ns, filter, update_doc, opts)
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
        let update_doc = match body.get_document("update") {
            Ok(u) => u.clone(),
            Err(_) => {
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
        };

        match state
            .database
            .find_one_and_update_with_options::<Document>(&ns, filter, update_doc, opts)
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
