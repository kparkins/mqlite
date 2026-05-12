// === Command handlers: collection admin ===

use bson::{doc, Document};

use super::super::errors::{err_bad_value, err_from_mqlite};
use super::super::server::ServerState;
use super::{extract_db_name, qualified_coll};

/// `create` — explicitly create a collection.
///
/// This is idempotent: creating an already-existing collection returns `{ok: 1}`.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "ok": 1 }
/// ```
pub(super) fn handle_create(body: &Document, state: &ServerState) -> Document {
    let coll_name = match body.get_str("create") {
        Ok(s) => s,
        Err(_) => return err_bad_value("create requires a collection name string"),
    };

    match state
        .database
        .create_collection(&qualified_coll(body, coll_name))
    {
        Ok(_) => doc! { "ok": 1.0_f64 },
        Err(crate::error::Error::DuplicateKey { .. }) => doc! { "ok": 1.0_f64 },
        Err(e) => err_from_mqlite(e),
    }
}

/// `drop` — drop a collection and all its indexes.
///
/// Dropping a non-existent collection returns `{ok: 1}` (idempotent, matching
/// MongoDB 8.0 behaviour for `drop` on a missing namespace).
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "ok": 1 }
/// ```
pub(super) fn handle_drop(body: &Document, state: &ServerState) -> Document {
    let coll_name = match body.get_str("drop") {
        Ok(s) => s,
        Err(_) => return err_bad_value("drop requires a collection name string"),
    };

    match state
        .database
        .drop_collection(&qualified_coll(body, coll_name))
    {
        Ok(_) => doc! { "ok": 1.0_f64 },
        Err(e) => err_from_mqlite(e),
    }
}

/// `listCollections` — list collections in the current database.
///
/// Supports an optional `filter` document with a `name` equality filter.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "cursor": { "firstBatch": [{name, type, options, idIndex, info}], "id": 0 }, "ok": 1 }
/// ```
pub(super) fn handle_list_collections(body: &Document, state: &ServerState) -> Document {
    // Optional `filter: {name: "<name>"}` — only a simple equality filter on `name`
    // is supported.
    let name_filter = body
        .get_document("filter")
        .ok()
        .and_then(|f| f.get_str("name").ok());

    let all_names = match state.database.list_collection_names() {
        Ok(n) => n,
        Err(e) => return err_from_mqlite(e),
    };

    // Filter to collections in the database named by `$db`.
    let db_name = extract_db_name(body);
    let db_prefix = format!("{db_name}.");
    let first_batch: bson::Array = all_names
        .into_iter()
        .filter_map(|name| {
            // Names are stored as "db.collection" — strip the db prefix.
            let name = name.strip_prefix(&db_prefix)?;
            if name_filter.is_some_and(|filter| name != filter) {
                return None;
            }
            Some(bson::Bson::Document(doc! {
                "name": &name,
                "type": "collection",
                "options": {},
                "idIndex": {
                    "v": 2i32,
                    "key": {"_id": 1i32},
                    "name": "_id_",
                },
                "info": {
                    "readOnly": false,
                },
            }))
        })
        .collect();

    // The cursor namespace for listCollections uses `$cmd.listCollections`.
    let ns = format!("{}.$cmd.listCollections", db_name);
    doc! {
        "cursor": {
            "firstBatch": first_batch,
            "id": bson::Bson::Int64(0i64),
            "ns": ns,
        },
        "ok": 1.0_f64,
    }
}
