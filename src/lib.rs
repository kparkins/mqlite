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
//! with a `Mutex`. Reads are also serialized through the engine `Mutex` in Phase 1.
//!
//! `Cursor<T>` is `Send` but not `Sync` — matching the MongoDB Rust driver contract.
//! Use `Mutex<Cursor<T>>` if you need to drive a cursor from multiple threads simultaneously.
//!
//! # File Lifecycle
//!
//! ```text
//! Client::open("myapp.mqlite")
//!   ├─ Creates myapp.mqlite        (main database file)
//!   ├─ Creates myapp.mqlite-wal    (write-ahead log; accumulates writes)
//!   └─ Creates myapp.mqlite-shm   (WAL shared-memory index; deleted on clean close)
//!
//! Client::close(self)             (blocking flush + checkpoint)
//!   └─ myapp.mqlite-wal is checkpointed into myapp.mqlite and removed
//!      → "single file" state
//!
//! drop(client)                    (non-blocking)
//!   └─ myapp.mqlite-wal remains on disk
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
//! - **Wire protocol**: no authentication in Phase 1 — bind to `127.0.0.1` only;
//!   see the [Wire Protocol Security Advisory](https://github.com/kyleparkinson/mqlite/blob/master/docs/WIRE-SECURITY.md)

// Lint policy: deny common footguns that indicate implementation errors.
// Unwrap-used is left as a warning (not deny) because stub implementations use it during
// Phase 0 and early Phase 1. It will be escalated to deny before Phase 1 ships.
#![warn(missing_docs)]
#![warn(clippy::unwrap_used)]

// ---------------------------------------------------------------------------
// Public modules
// ---------------------------------------------------------------------------

/// BSON re-exports for ergonomic use without a direct `bson` dependency.
pub mod bson_compat;
/// Client entry point: `Client::open(path)` → `client.database(name)` → `db.collection::<T>(name)`.
pub mod client;
/// Typed collection handles for CRUD operations.
pub mod collection;
/// Lazy cursor for iterating query results.
pub mod cursor;
/// Lightweight database-namespace handle (returned by `Client::database`).
pub mod database;
/// Error types and MongoDB-compatible error codes.
pub mod error;
/// Index definition and metadata types.
pub mod index;
/// BSON key encoding for B+ tree index storage.
pub mod key_encoding;
/// Configuration options for database opening and query operations.
pub mod options;
/// Operation result types returned by write operations.
pub mod results;

// Internal modules (not public API)
mod engine;
mod query;
mod storage;
mod storage_engine;
mod update_operators;
mod validation;
// Phase 1: WAL module — not yet wired into the main read/write paths.
#[allow(dead_code)]
mod wal;

// Crash recovery testing (hq-ele): 500 cycles, 10 scenarios.
// Unix-only; accesses pub(crate) WAL internals.
#[cfg(all(test, unix))]
mod crash_recovery_tests;

// Native API compatibility and persistence tests (hq-2yk).
#[cfg(test)]
mod compat_tests;

// Wire protocol shim (feature-gated)
#[cfg(feature = "wire")]
pub mod wire;

// ---------------------------------------------------------------------------
// Public re-exports — `use mqlite::*` or `use mqlite::Database;` etc.
// ---------------------------------------------------------------------------

// Core entry points
pub use client::Client;
pub use collection::Collection;
pub use cursor::{Cursor, ExplainResult};
pub use database::Database;

// Error and Result
pub use error::{Error, Result};

// Configuration
pub use options::{
    DurabilityMode, FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions,
    FindOptions, IndexOptions, InsertManyOptions, OpenOptions, ReturnDocument, UpdateOptions,
};

// Index
pub use index::{IndexInfo, IndexModel};

// Operation results
pub use results::{BulkWriteError, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};

// BSON re-exports — users don't need a direct `bson` dependency for basic usage
pub use bson_compat::{doc, Bson, DateTime, Document, ObjectId};

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
    doc: &bson_compat::Document,
    filter: &bson_compat::Document,
) -> Result<bool> {
    query::eval_filter(doc, filter)
}
