//! Wire protocol command handler submodules.

pub(super) mod admin;
pub(super) mod collection;
pub(super) mod crud;
pub(super) mod cursor;
pub(super) mod index;

pub(super) use admin::{
    handle_build_info, handle_hello, handle_list_databases, handle_ping, handle_server_status,
    handle_unknown,
};
pub(super) use collection::{handle_create, handle_drop, handle_list_collections};
pub(super) use crud::{handle_delete, handle_find, handle_find_and_modify, handle_insert, handle_update};
pub(super) use cursor::{handle_get_more, handle_kill_cursors};
pub(super) use index::{handle_create_indexes, handle_drop_indexes, handle_list_indexes};
