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
//! - [`handle`]     — `Client` struct + lifecycle API (`database`, `checkpoint`,
//!   `backup`, `close`, `Drop`).
//! - [`database`]   — `Database` namespace handle returned by `Client::database`.
//! - [`collection`] — `Collection<T>` typed CRUD handle returned by
//!   `Database::collection`, plus its fluent action builders.
//! - [`open`]       — `Client::open` and `Client::open_with_options` (the
//!   disk-bootstrap sequence).
//! - [`inner`]      — `ClientInner` shared state struct.
//! - [`crud`]       — `impl ClientInner` CRUD and hot-backup methods.
//! - [`path`]       — private path/header helpers shared by `open` and `crud`.

/// Typed collection handles for CRUD operations.
pub mod collection;
/// Lightweight database-namespace handle (returned by `Client::database`).
pub mod database;

mod crud;
mod handle;
/// Test-only `impl Client` accessors — `__`-prefixed, `#[doc(hidden)]`,
/// and strictly NOT part of the public API. Isolated here so the
/// boundary between production code and test scaffolding is obvious.
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/hidden_accessors.rs"]
mod hidden_accessors;
mod inner;
mod open;
mod path;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/write_crash_cut_hook.rs"]
mod write_crash_cut_hook;

#[cfg(test)]
mod tests;

pub use collection::{
    Collection, Find, FindOneAndDelete, FindOneAndReplace, FindOneAndUpdate, InsertMany, Update,
};
pub use database::Database;
pub use handle::Client;
#[cfg(any(test, feature = "test-hooks"))]
#[doc(hidden)]
pub use hidden_accessors::{Phase8CatalogCommitKind, Phase8LogRecordKind, Phase8LogRecordSummary};
pub(crate) use inner::ClientInner;
