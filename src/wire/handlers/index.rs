// === Command handlers: index operations ===

use bson::{doc, Document};

use super::super::errors::{err_bad_value, err_from_mqlite};
use super::super::server::ServerState;

/// Extract the database name from a command body's `$db` field.
fn extract_db_name(body: &Document) -> String {
    body.get_str("$db")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or("test")
        .to_owned()
}

/// Fully-qualify a collection name as `<db_name>.<coll_name>`.
fn qualified_coll(body: &Document, coll_name: &str) -> String {
    format!("{}.{}", extract_db_name(body), coll_name)
}

/// `createIndexes` — create one or more indexes on a collection.
///
/// Each index specification in `indexes` must contain at minimum a `key`
/// document.  Optionally: `name`, `unique`, `sparse`.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "numIndexesBefore": <n>, "numIndexesAfter": <n>, "ok": 1 }
/// ```
/// `numIndexesBefore` and `numIndexesAfter` both include the synthetic `_id_`
/// index (always present in every MongoDB collection).
pub(super) fn handle_create_indexes(body: &Document, state: &ServerState) -> Document {
    let coll_name = match body.get_str("createIndexes") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("createIndexes requires a collection name string"),
    };

    let indexes_arr = match body.get_array("indexes") {
        Ok(arr) => arr.clone(),
        Err(_) => return err_bad_value("createIndexes requires an \"indexes\" array"),
    };

    // Count existing user-created indexes before creation.
    // Add 1 for the always-present synthetic `_id_` index.
    let num_before = state
        .database
        .list_indexes(&qualified_coll(body, &coll_name))
        .map(|idxs| idxs.len() as i32 + 1)
        .unwrap_or(1);

    for idx_bson in &indexes_arr {
        let spec = match idx_bson.as_document() {
            Some(d) => d,
            None => continue,
        };

        let key = match spec.get_document("key") {
            Ok(k) => k.clone(),
            Err(_) => return err_bad_value("each index spec requires a \"key\" document"),
        };

        // Reject manual creation of the _id_ index.
        let is_id_key = key.len() == 1
            && key.get_i32("_id").ok() == Some(1);
        let is_id_name = spec.get_str("name").ok() == Some("_id_");
        if is_id_key || is_id_name {
            return err_bad_value("cannot manually create _id_ index");
        }

        let mut opts = crate::options::IndexOptions::new();
        if let Ok(b) = spec.get_bool("unique") {
            opts = opts.unique(b);
        }
        if let Ok(b) = spec.get_bool("sparse") {
            opts = opts.sparse(b);
        }
        if let Ok(name) = spec.get_str("name") {
            opts = opts.name(name);
        }

        let model = crate::index::IndexModel {
            keys: key,
            options: opts,
        };
        if let Err(e) = state
            .database
            .create_index(&qualified_coll(body, &coll_name), model)
        {
            return err_from_mqlite(e);
        }
    }

    // Count user-created indexes after creation (+1 for synthetic `_id_`).
    let num_after = state
        .database
        .list_indexes(&qualified_coll(body, &coll_name))
        .map(|idxs| idxs.len() as i32 + 1)
        .unwrap_or(1);

    doc! {
        "numIndexesBefore": num_before,
        "numIndexesAfter": num_after,
        "ok": 1.0_f64,
    }
}

/// `dropIndexes` — drop one or all user-created indexes on a collection.
///
/// The `index` field may be:
/// - `"*"` — drop all user-created indexes (the `_id_` index is never dropped).
/// - `"<name>"` — drop the named index.
/// - `{<key pattern>}` — drop the index with the matching key pattern.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "ok": 1 }
/// ```
pub(super) fn handle_drop_indexes(body: &Document, state: &ServerState) -> Document {
    let coll_name = match body.get_str("dropIndexes") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("dropIndexes requires a collection name string"),
    };

    match body.get("index") {
        Some(bson::Bson::String(name)) if name == "*" => {
            // Drop all user-created indexes.
            let indexes = match state
                .database
                .list_indexes(&qualified_coll(body, &coll_name))
            {
                Ok(idxs) => idxs,
                Err(e) => return err_from_mqlite(e),
            };
            for idx in &indexes {
                if let Err(e) = state
                    .database
                    .drop_index(&qualified_coll(body, &coll_name), &idx.name)
                {
                    return err_from_mqlite(e);
                }
            }
            doc! { "ok": 1.0_f64 }
        }
        Some(bson::Bson::String(name)) if name == "_id_" => {
            // Reject attempts to drop the immutable _id_ index.
            doc! {
                "ok": 0.0_f64,
                "errmsg": "cannot drop _id index",
                "code": 27i32,
                "codeName": "IndexNotFound",
            }
        }
        Some(bson::Bson::String(name)) => {
            // Drop a specific index by name.
            match state
                .database
                .drop_index(&qualified_coll(body, &coll_name), name)
            {
                Ok(_) => doc! { "ok": 1.0_f64 },
                Err(e) => err_from_mqlite(e),
            }
        }
        Some(bson::Bson::Document(key_doc)) => {
            // Drop by key pattern — find the index whose key matches.
            let key_doc = key_doc.clone();
            // Reject attempts to drop the immutable _id index by key pattern.
            if key_doc == doc! { "_id": 1i32 } {
                return doc! {
                    "ok": 0.0_f64,
                    "errmsg": "cannot drop _id index",
                    "code": 27i32,
                    "codeName": "IndexNotFound",
                };
            }
            let indexes = match state
                .database
                .list_indexes(&qualified_coll(body, &coll_name))
            {
                Ok(idxs) => idxs,
                Err(e) => return err_from_mqlite(e),
            };
            match indexes.iter().find(|idx| idx.keys == key_doc) {
                Some(idx) => match state
                    .database
                    .drop_index(&qualified_coll(body, &coll_name), &idx.name.clone())
                {
                    Ok(_) => doc! { "ok": 1.0_f64 },
                    Err(e) => err_from_mqlite(e),
                },
                None => doc! {
                    "ok": 0.0_f64,
                    "errmsg": "index not found with name",
                    "code": 27i32,
                    "codeName": "IndexNotFound",
                },
            }
        }
        _ => err_bad_value(
            "dropIndexes requires an \"index\" field (string name, \"*\", or key document)",
        ),
    }
}

/// `listIndexes` — list indexes on a collection.
///
/// Always returns the synthetic `_id_` index first (MongoDB always reports it),
/// followed by any user-created indexes.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "cursor": { "firstBatch": [{v, key, name, ...}], "id": 0 }, "ok": 1 }
/// ```
pub(super) fn handle_list_indexes(body: &Document, state: &ServerState) -> Document {
    let coll_name = match body.get_str("listIndexes") {
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("listIndexes requires a collection name string"),
    };

    let indexes = match state
        .database
        .list_indexes(&qualified_coll(body, &coll_name))
    {
        Ok(idxs) => idxs,
        Err(e) => return err_from_mqlite(e),
    };

    // The `_id_` index is always present in every MongoDB collection.
    let mut first_batch: bson::Array = vec![bson::Bson::Document(doc! {
        "v": 2i32,
        "key": {"_id": 1i32},
        "name": "_id_",
    })];

    for idx in &indexes {
        let mut idx_doc = doc! {
            "v": 2i32,
            "key": idx.keys.clone(),
            "name": &idx.name,
        };
        if idx.unique {
            idx_doc.insert("unique", true);
        }
        if idx.sparse {
            idx_doc.insert("sparse", true);
        }
        first_batch.push(bson::Bson::Document(idx_doc));
    }

    let ns = format!("{}.{}", extract_db_name(body), coll_name);
    doc! {
        "cursor": {
            "firstBatch": first_batch,
            "id": bson::Bson::Int64(0i64),
            "ns": ns,
        },
        "ok": 1.0_f64,
    }
}
