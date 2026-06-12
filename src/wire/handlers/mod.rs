//! Wire protocol command handler submodules.

use std::sync::{Arc, Mutex};

use bson::Document;

use super::server::{ConnectionCursors, ServerState};

pub(super) mod admin;
pub(super) mod collection;
pub(super) mod crud;
pub(super) mod cursor;
pub(super) mod index;

const DEFAULT_CURSOR_BATCH_SIZE: usize = 101;

pub(super) fn batch_size(body: &Document) -> usize {
    get_i64(body, "batchSize")
        .map(|n| {
            if n <= 0 {
                DEFAULT_CURSOR_BATCH_SIZE
            } else {
                n as usize
            }
        })
        .unwrap_or(DEFAULT_CURSOR_BATCH_SIZE)
}

pub(super) fn handle_hello(state: &ServerState, connection_id: i32) -> Document {
    admin::handle_hello(state, connection_id)
}

pub(super) fn handle_ping() -> Document {
    admin::handle_ping()
}

pub(super) fn handle_build_info() -> Document {
    admin::handle_build_info()
}

pub(super) fn handle_server_status(state: &ServerState) -> Document {
    admin::handle_server_status(state)
}

pub(super) fn handle_list_databases(state: &ServerState) -> Document {
    admin::handle_list_databases(state)
}

pub(super) fn handle_drop_database(body: &Document, state: &ServerState) -> Document {
    admin::handle_drop_database(body, state)
}

pub(super) fn handle_end_sessions() -> Document {
    admin::handle_end_sessions()
}

pub(super) fn handle_unknown(name: &str) -> Document {
    admin::handle_unknown(name)
}

pub(super) fn handle_insert(body: &Document, state: &ServerState) -> Document {
    crud::handle_insert(body, state)
}

pub(super) fn handle_find(
    body: &Document,
    state: &ServerState,
    cursors: &Arc<Mutex<ConnectionCursors>>,
) -> Document {
    crud::handle_find(body, state, cursors)
}

pub(super) fn handle_aggregate(
    body: &Document,
    state: &ServerState,
    cursors: &Arc<Mutex<ConnectionCursors>>,
) -> Document {
    crud::handle_aggregate(body, state, cursors)
}

pub(super) fn handle_update(body: &Document, state: &ServerState) -> Document {
    crud::handle_update(body, state)
}

pub(super) fn handle_delete(body: &Document, state: &ServerState) -> Document {
    crud::handle_delete(body, state)
}

pub(super) fn handle_find_and_modify(body: &Document, state: &ServerState) -> Document {
    crud::handle_find_and_modify(body, state)
}

pub(super) fn handle_count(body: &Document, state: &ServerState) -> Document {
    crud::handle_count(body, state)
}

pub(super) fn handle_distinct(body: &Document, state: &ServerState) -> Document {
    crud::handle_distinct(body, state)
}

pub(super) fn handle_explain(body: &Document, state: &ServerState) -> Document {
    crud::handle_explain(body, state)
}

pub(super) fn handle_get_more(
    body: &Document,
    state: &ServerState,
    cursors: &Arc<Mutex<ConnectionCursors>>,
) -> Document {
    cursor::handle_get_more(body, state, cursors)
}

pub(super) fn handle_kill_cursors(
    body: &Document,
    cursors: &Arc<Mutex<ConnectionCursors>>,
) -> Document {
    cursor::handle_kill_cursors(body, cursors)
}

pub(super) fn handle_create(body: &Document, state: &ServerState) -> Document {
    collection::handle_create(body, state)
}

pub(super) fn handle_drop(body: &Document, state: &ServerState) -> Document {
    collection::handle_drop(body, state)
}

pub(super) fn handle_list_collections(body: &Document, state: &ServerState) -> Document {
    collection::handle_list_collections(body, state)
}

pub(super) fn handle_create_indexes(body: &Document, state: &ServerState) -> Document {
    index::handle_create_indexes(body, state)
}

pub(super) fn handle_drop_indexes(body: &Document, state: &ServerState) -> Document {
    index::handle_drop_indexes(body, state)
}

pub(super) fn handle_list_indexes(body: &Document, state: &ServerState) -> Document {
    index::handle_list_indexes(body, state)
}

/// Extract an integer value from a BSON document field, coercing `Int32`,
/// `Int64`, and `Double` variants to `i64`.
pub(super) fn get_i64(doc: &Document, key: &str) -> Option<i64> {
    match doc.get(key) {
        Some(bson::Bson::Int32(i)) => Some(*i as i64),
        Some(bson::Bson::Int64(i)) => Some(*i),
        Some(bson::Bson::Double(f)) => Some(*f as i64),
        _ => None,
    }
}

/// Extract the database name from a command body's `$db` field.
pub(super) fn extract_db_name(body: &Document) -> String {
    body.get_str("$db")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or("test")
        .to_owned()
}

/// Fully-qualify a collection name as `<db_name>.<coll_name>`.
pub(super) fn qualified_coll(body: &Document, coll_name: &str) -> String {
    format!("{}.{}", extract_db_name(body), coll_name)
}
