//! # mqlite Client — top-level entry point
//!
//! [`Client`] is the root of the mqlite object model.  It matches the MongoDB
//! Rust driver hierarchy:
//!
//! ```text
//! Client::open(path)          ← file-level handle (this module)
//!   └─ client.database(name)  ← database namespace handle (database.rs)
//!        └─ db.collection::<T>(name)  ← typed CRUD handle (collection.rs)
//! ```
//!
//! `Client` holds `Arc<ClientInner>` which owns the storage engine, file lock,
//! and write-serialisation mutex.  `Database` and `Collection<T>` handles each
//! hold a clone of the same `Arc<ClientInner>`, so they are cheap to create
//! and share the same underlying state.
//!
//! ## Module layout
//!
//! - [`handle`]  — `Client` struct + lifecycle API (`database`, `checkpoint`,
//!   `backup`, `close`, `Drop`).
//! - [`open`]    — `Client::open` and `Client::open_with_options` (the
//!   disk-bootstrap sequence).
//! - [`inner`]   — `ClientInner` shared state struct.
//! - [`crud`]    — `impl ClientInner` CRUD and hot-backup methods.
//! - [`path`]    — private path/header helpers shared by `open` and `crud`.

mod crud;
mod handle;
mod inner;
mod open;
mod path;

#[cfg(test)]
mod tests;

pub use handle::Client;
pub(crate) use inner::ClientInner;
