// === Command handlers: cursor management ===

use std::sync::{Arc, Mutex};

use bson::{doc, Document};

use super::super::errors::{err_bad_value, err_from_mqlite};
use super::super::server::{ConnectionCursors, ServerState};

/// Extract an integer value from a BSON document field, coercing `Int32`,
/// `Int64`, and `Double` variants to `i64`.
fn get_i64(doc: &Document, key: &str) -> Option<i64> {
    match doc.get(key) {
        Some(bson::Bson::Int32(i)) => Some(*i as i64),
        Some(bson::Bson::Int64(i)) => Some(*i),
        Some(bson::Bson::Double(f)) => Some(*f as i64),
        _ => None,
    }
}

/// Extract the database name from a command body's `$db` field.
fn extract_db_name(body: &Document) -> String {
    body.get_str("$db")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or("test")
        .to_owned()
}

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
        Ok(s) => s.to_owned(),
        Err(_) => return err_bad_value("getMore requires a \"collection\" field"),
    };

    // Default batch size mirrors MongoDB 8.0 (101 documents).
    let batch_size = get_i64(body, "batchSize")
        .map(|n| if n <= 0 { 101usize } else { n as usize })
        .unwrap_or(101);

    // Drain up to `batch_size` documents from the cursor in a single critical section.
    let (next_batch, exhausted) = {
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
                let done = cursor.is_exhausted();
                (batch, done)
            }
        }
    };

    // Remove the cursor from the map once it is exhausted.
    let returned_id: i64 = if exhausted {
        cursors
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(cursor_id);
        0
    } else {
        cursor_id
    };

    let ns = format!("{}.{}", extract_db_name(body), coll_name);
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
    let cursor_ids: Vec<i64> = match body.get_array("cursors") {
        Ok(arr) => arr
            .iter()
            .filter_map(|b| match b {
                bson::Bson::Int64(id) => Some(*id),
                bson::Bson::Int32(id) => Some(*id as i64),
                _ => None,
            })
            .collect(),
        Err(_) => return err_bad_value("killCursors requires a \"cursors\" array"),
    };

    let mut killed: bson::Array = Vec::new();
    let mut not_found: bson::Array = Vec::new();

    let mut guard = cursors.lock().unwrap_or_else(|e| e.into_inner());
    for id in &cursor_ids {
        if guard.remove(*id).is_some() {
            killed.push(bson::Bson::Int64(*id));
        } else {
            not_found.push(bson::Bson::Int64(*id));
        }
    }

    doc! {
        "cursorsKilled": killed,
        "cursorsNotFound": not_found,
        "ok": 1.0_f64,
    }
}
