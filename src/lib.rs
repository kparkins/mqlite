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
//! use mqlite::{Database, doc};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Config { key: String, value: String }
//!
//! fn main() -> mqlite::Result<()> {
//!     let db = Database::open("myapp.mqlite")?;
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
/// Typed collection handles for CRUD operations.
pub mod collection;
/// Lazy cursor for iterating query results.
pub mod cursor;
/// The database entry point: open, clone, and manage the database.
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
mod query;
mod storage;
mod validation;
// Phase 1: WAL module — not yet wired into the main read/write paths.
#[allow(dead_code)]
mod wal;

// Wire protocol shim (feature-gated)
#[cfg(feature = "wire")]
pub mod wire;

// ---------------------------------------------------------------------------
// Public re-exports — `use mqlite::*` or `use mqlite::Database;` etc.
// ---------------------------------------------------------------------------

// Core entry points
pub use collection::Collection;
pub use cursor::Cursor;
pub use database::Database;

// Error and Result
pub use error::{Error, Result};

// Configuration
pub use options::{
    DurabilityMode, FindOptions, IndexOptions, InsertManyOptions, OpenOptions, UpdateOptions,
};

// Index
pub use index::{IndexInfo, IndexModel};

// Operation results
pub use results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult};

// BSON re-exports — users don't need a direct `bson` dependency for basic usage
pub use bson_compat::{doc, Bson, DateTime, Document, ObjectId};

// Wire protocol entry point (feature-gated)
#[cfg(feature = "wire")]
pub use wire::WireProtocol;
