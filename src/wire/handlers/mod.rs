//! Wire protocol command handler submodules.

use std::sync::{Arc, Mutex};

use bson::Document;

use super::server::{ConnectionCursors, ServerState};

pub(super) mod admin;
pub(super) mod collection;
pub(super) mod crud;
pub(super) mod cursor;
pub(super) mod index;

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

pub(super) fn handle_update(body: &Document, state: &ServerState) -> Document {
    crud::handle_update(body, state)
}

pub(super) fn handle_delete(body: &Document, state: &ServerState) -> Document {
    crud::handle_delete(body, state)
}

pub(super) fn handle_find_and_modify(body: &Document, state: &ServerState) -> Document {
    crud::handle_find_and_modify(body, state)
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

#[cfg(test)]
pub(super) fn get_i64(doc: &Document, key: &str) -> Option<i64> {
    crud::get_i64(doc, key)
}
