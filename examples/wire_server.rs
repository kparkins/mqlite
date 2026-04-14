//! Minimal mqlite wire protocol server for manual testing.
//!
//! Starts a TCP listener on 127.0.0.1:27017 and handles MongoDB wire
//! protocol connections.  Useful for running the pymongo spike test:
//!
//! ```sh
//! # Terminal 1 – start the server:
//! cargo run --features wire --example wire_server
//!
//! # Terminal 2 – run the spike test:
//! python3 tests/pymongo_spike.py
//!
//! # Or connect with mongosh:
//! mongosh "mongodb://localhost:27017/?directConnection=true"
//! ```
//!
//! The server runs until you press Ctrl-C.

use mqlite::{Database, WireProtocol};

fn main() -> mqlite::Result<()> {
    let db = Database::open_in_memory()?;
    let addr = "127.0.0.1:27017";
    println!("mqlite wire server starting on {}", addr);
    let _server = WireProtocol::bind(&db, addr)?;
    println!("Listening on mongodb://{}/?directConnection=true", addr);
    println!("Press Ctrl-C to stop.");

    // Block the main thread until Ctrl-C.
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}
