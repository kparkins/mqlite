//! MongoDB wire protocol shim.
//!
//! This module is only available when the `wire` feature is enabled:
//! ```toml
//! [dependencies]
//! mqlite = { version = "0.1", features = ["wire"] }
//! ```
//!
//! The wire protocol shim allows `mongosh` and MongoDB drivers to connect to
//! an mqlite database over TCP using the MongoDB wire protocol.
//!
//! # Example
//! ```no_run
//! use mqlite::{Database, WireProtocol};
//!
//! let db = Database::open("myapp.mqlite")?;
//! let _server = WireProtocol::bind(&db, "127.0.0.1:27017")?;
//! println!("Connect with: mongosh mongodb://localhost:27017");
//! // Server runs in background until `_server` is dropped
//! # Ok::<(), mqlite::Error>(())
//! ```

pub mod server;
pub mod protocol;
pub mod commands;

pub use server::WireProtocol;
