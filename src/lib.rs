//! # mqlite — Embedded MongoDB-compatible document store
//!
//! mqlite is a lightweight, embedded document database with MongoDB query semantics.
//! It is designed for:
//!
//! - **Embedded apps** — local storage without a server
//! - **Test doubles** — replace MongoDB containers with an in-memory database
//! - **mongosh interop** — inspect mqlite files with familiar MongoDB tooling (via `wire` feature)
//! - **Edge/IoT** — constrained environments, single-file databases, crash recovery
//!
//! # Quick Start
//!
//! ```toml
//! [dependencies]
//! mqlite = "0.1"
//! serde = { version = "1", features = ["derive"] }
//! ```
//!
//! ```no_run
//! use mqlite::{Client, doc};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Config { key: String, value: String }
//!
//! fn main() -> mqlite::Result<()> {
//!     let client = Client::open("myapp.mqlite")?;
//!     let db = client.database("myapp");
//!     let configs = db.collection::<Config>("config");
//!
//!     configs.insert_one(&Config {
//!         key: "theme".into(),
//!         value: "dark".into(),
//!     })?;
//!
//!     let theme = configs.find_one(doc! { "key": "theme" })?;
//!     println!("Theme: {:?}", theme.map(|c| c.value));
//!
//!     Ok(())
//! }
//! ```
//!
//! # Feature Flags
//!
//! | Flag | Description |
//! |------|-------------|
//! | `wire` | MongoDB wire protocol shim (requires tokio) |
//! | `tracing` | Observability via the `tracing` crate |
//!
//! # Async
//!
//! The base crate is **sync-only**. Enabling the `wire` feature adds an async
//! runtime dependency (tokio) for the TCP listener, but the core CRUD API remains
//! synchronous. This keeps the dependency footprint minimal for embedded and IoT use cases.
//!
//! # Thread Safety
//!
//! | Type | `Send` | `Sync` | Notes |
//! |------|--------|--------|-------|
//! | [`Client`] | ✅ | ✅ | Clone and share across threads freely |
//! | [`Database`] | ✅ | ✅ | Lightweight handle, same inner state as `Client` |
//! | [`Collection<T>`] | ✅ | ✅ | Same shared state as `Client`/`Database` |
//! | [`Cursor<T>`] | ✅ | ❌ | Move to another thread; use `Mutex` for concurrent access |
//! | [`Error`] | ✅ | ✅ | — |
//!
//! `Client`, `Database`, and `Collection<T>` can be cloned and sent to other threads without
//! any additional synchronization. mqlite serializes concurrent writes internally
//! through per-namespace lane mutexes in the engine.
//!
//! `Cursor<T>` is `Send` but not `Sync` — matching the MongoDB Rust driver contract.
//! Use `Mutex<Cursor<T>>` if you need to drive a cursor from multiple threads simultaneously.
//!
//! # File Lifecycle
//!
//! ```text
//! Client::open("myapp.mqlite")
//!   ├─ Creates myapp.mqlite            (main database file)
//!   └─ Creates myapp.mqlite-journal    (write-ahead journal; accumulates writes)
//!
//! Client::close(self)             (blocking flush + checkpoint)
//!   └─ myapp.mqlite-journal is checkpointed into myapp.mqlite and removed
//!      → "single file" state
//!
//! drop(client)                    (non-blocking)
//!   └─ myapp.mqlite-journal remains on disk
//!      → Replayed automatically on next Client::open
//! ```
//!
//! The `close()` method is the recommended shutdown path when you need a guaranteed-clean
//! single-file state (e.g., before copying the database as a backup).
//!
//! # Security Notes
//!
//! - **File permissions**: new `.mqlite` files are created with mode `0600` (Unix)
//! - **Symlink prevention**: [`Error::SymlinkRejected`] is returned if the path is a symlink
//! - **Wire protocol**: no authentication — bind to `127.0.0.1` only;
//!   see the [Wire Protocol Security Advisory](https://github.com/kyleparkinson/mqlite/blob/master/docs/WIRE-SECURITY.md)

// ---------------------------------------------------------------------------
// Public modules
// ---------------------------------------------------------------------------

/// BSON re-exports for ergonomic use without a direct `bson` dependency.
pub mod bson;
/// Client entry point: `Client::open(path)` → `client.database(name)` → `db.collection::<T>(name)`.
///
/// The `Client`, `Database`, and `Collection<T>` handles all live in this module —
/// they share the same `Arc<ClientInner>` and form a single ownership hierarchy.
pub mod client;
/// Lazy cursor for iterating query results.
pub mod cursor;
/// Error types and MongoDB-compatible error codes.
pub mod error;
/// Index definition and metadata types.
pub mod index;
/// BSON key encoding for B+ tree index storage.
pub mod keys;
/// Configuration options for database opening and query operations.
pub mod options;
/// Operation result types returned by write operations.
pub mod results;

// Internal modules (not public API)
// `mvcc` is `pub` but `#[doc(hidden)]` — integration tests need to
// reference `ReadView` / `ReadViewRegistry` / `Ts` through the crate root,
// but the module is not part of the stable surface.
#[doc(hidden)]
#[allow(dead_code)]
pub mod mvcc;
mod query;
mod storage;
mod update;
mod validation;
#[allow(dead_code)]
mod journal;

// Wire protocol shim (feature-gated)
#[cfg(feature = "wire")]
pub mod wire;

// ---------------------------------------------------------------------------
// Public re-exports — `use mqlite::*` or `use mqlite::Database;` etc.
// ---------------------------------------------------------------------------

// Core entry points
pub use client::{Client, Collection, Database};
pub use cursor::Cursor;
pub use query::explain::ExplainResult;

// Error and Result
pub use error::{Error, Result};

// Configuration
pub use options::{DurabilityMode, IndexOptions, OpenOptions, ReturnDocument};

// Collection action types (returned by Collection methods; users chain options onto them)
pub use client::{
    Find, FindOneAndDelete, FindOneAndReplace, FindOneAndUpdate, InsertMany, Update,
};

// Index
pub use index::{IndexInfo, IndexModel};

// Operation results
pub use results::{BulkWriteError, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};

// BSON re-exports — users don't need a direct `bson` dependency for basic usage
pub use bson::{doc, Bson, DateTime, Document, ObjectId};

// Wire protocol entry point (feature-gated)
#[cfg(feature = "wire")]
pub use wire::WireProtocol;

// ---------------------------------------------------------------------------
// Fuzz helpers (feature = "fuzz" only — never enable in production)
// ---------------------------------------------------------------------------

/// Evaluate a MongoDB filter document against a BSON document.
///
/// This is a thin shim over the internal `query::eval_filter` function,
/// exposed **only** under the `fuzz` feature so that fuzz targets in the
/// `fuzz/` crate can reach it without making it part of the stable API.
///
/// Do **not** call this from application code.
#[cfg(feature = "fuzz")]
pub fn fuzz_eval_filter(
    doc: &bson::Document,
    filter: &bson::Document,
) -> Result<bool> {
    query::eval_filter(doc, filter)
}
