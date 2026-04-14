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

pub mod database;
pub mod collection;
pub mod cursor;
pub mod error;
pub mod options;
pub mod index;
pub mod results;
pub mod bson_compat;

// Internal modules (not public API)
mod query;
mod storage;
mod wal;

// Wire protocol shim (feature-gated)
#[cfg(feature = "wire")]
pub mod wire;

// ---------------------------------------------------------------------------
// Public re-exports — `use mqlite::*` or `use mqlite::Database;` etc.
// ---------------------------------------------------------------------------

// Core entry points
pub use database::Database;
pub use collection::Collection;
pub use cursor::Cursor;

// Error and Result
pub use error::{Error, Result};

// Configuration
pub use options::{
    DurabilityMode,
    FindOptions,
    InsertManyOptions,
    IndexOptions,
    OpenOptions,
    UpdateOptions,
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
