// === Command handlers: cursor management ===

use std::sync::{Arc, Mutex};

use bson::{doc, Document};

use super::super::errors::err_bad_value;
use super::super::server::{ConnectionCursors, ServerState};
use super::{batch_size, get_i64, qualified_coll};

/// `getMore` — fetch the next batch of results from an open server-side cursor.
///
/// Cursors are pinned to the TCP connection that created them via `find`.  A
/// `getMore` sent on a *different* connection will always get `CursorNotFound`
/// (code 43) because the cursor map is per-connection.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "cursor": { "nextBatch": [...], "id": <cursor_id_or_0>, "ns": "db.coll" }, "ok": 1 }
/// ```
/// When the cursor is exhausted the response contains `"id": 0` and the cursor
/// is removed from the per-connection map.
pub(super) fn handle_get_more(
    body: &Document,
    _state: &ServerState,
    cursors: &Arc<Mutex<ConnectionCursors>>,
) -> Document {
    let cursor_id = match get_i64(body, "getMore") {
        Some(id) => id,
        None => return err_bad_value("getMore requires a cursor id (Int64)"),
    };

    let coll_name = match body.get_str("collection") {
        Ok(s) => s,
        Err(_) => return err_bad_value("getMore requires a \"collection\" field"),
    };

    let batch_size = batch_size(body);

    // Drain up to `batch_size` documents from the cursor in a single critical section.
    let (next_batch, returned_id) = {
        let mut guard = cursors.lock().unwrap_or_else(|e| e.into_inner());
        match guard.get_mut(cursor_id) {
            None => {
                return doc! {
                    "ok": 0.0_f64,
                    "errmsg": format!("cursor id {} not found", cursor_id),
                    "code": crate::error::codes::CURSOR_NOT_FOUND,
                    "codeName": "CursorNotFound",
                };
            }
            Some(cursor) => {
                let batch: bson::Array = cursor
                    .by_ref()
                    .take(batch_size)
                    .filter_map(|r| r.ok().map(bson::Bson::Document))
                    .collect();
                let returned_id = if cursor.is_exhausted() {
                    guard.remove(cursor_id);
                    0
                } else {
                    cursor_id
                };
                (batch, returned_id)
            }
        }
    };

    let ns = qualified_coll(body, coll_name);
    doc! {
        "cursor": {
            "nextBatch": next_batch,
            "id": bson::Bson::Int64(returned_id),
            "ns": ns,
        },
        "ok": 1.0_f64,
    }
}

/// `killCursors` — close one or more open server-side cursors and release resources.
///
/// Response format (MongoDB 8.0):
/// ```json
/// { "cursorsKilled": [...], "cursorsNotFound": [...], "ok": 1 }
/// ```
pub(super) fn handle_kill_cursors(
    body: &Document,
    cursors: &Arc<Mutex<ConnectionCursors>>,
) -> Document {
    let cursor_ids = match body.get_array("cursors") {
        Ok(arr) => arr,
        Err(_) => return err_bad_value("killCursors requires a \"cursors\" array"),
    };

    let mut killed: bson::Array = Vec::with_capacity(cursor_ids.len());
    let mut not_found: bson::Array = Vec::with_capacity(cursor_ids.len());

    let mut guard = cursors.lock().unwrap_or_else(|e| e.into_inner());
    for id in cursor_ids.iter().filter_map(|b| match b {
        bson::Bson::Int64(id) => Some(*id),
        bson::Bson::Int32(id) => Some(*id as i64),
        _ => None,
    }) {
        if guard.remove(id).is_some() {
            killed.push(bson::Bson::Int64(id));
        } else {
            not_found.push(bson::Bson::Int64(id));
        }
    }

    doc! {
        "cursorsKilled": killed,
        "cursorsNotFound": not_found,
        "ok": 1.0_f64,
    }
}
