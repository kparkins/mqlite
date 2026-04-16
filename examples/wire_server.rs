//! Minimal mqlite wire protocol server for manual testing and CI.
//!
//! Starts a TCP listener on 127.0.0.1:<port> and handles MongoDB wire
//! protocol connections.  Useful for running the pymongo compatibility tests:
//!
//! ```sh
//! # Terminal 1 – start the server (default port 27017):
//! cargo run --features wire --example wire_server
//!
//! # Terminal 2 – run the pymongo compatibility test suite:
//! python3 tests/pymongo_compat.py
//!
//! # Or connect with mongosh:
//! mongosh "mongodb://localhost:27017/?directConnection=true"
//! ```
//!
//! # Port configuration
//!
//! The port is selected in this order:
//!   1. `--port <N>` command-line argument
//!   2. `MQLITE_PORT` environment variable
//!   3. Default: 27017
//!
//! This allows CI scripts to find a free port dynamically:
//!
//! ```sh
//! MQLITE_PORT=27099 cargo run --features wire --example wire_server &
//! ```
//!
//! The server runs until terminated (Ctrl-C or SIGTERM).

use mqlite::{Client, WireProtocol};
use tempfile::TempDir;

fn main() -> mqlite::Result<()> {
    let port = parse_port();
    let addr = format!("127.0.0.1:{port}");
    let _tempdir = TempDir::new().expect("tempdir");
    let client = Client::open(_tempdir.path().join("db.mqlite"))?;
    println!("mqlite wire server starting on {addr}");
    let _server = WireProtocol::bind(&client, &addr)?;
    println!("Listening on mongodb://{addr}/?directConnection=true");
    println!("Press Ctrl-C to stop.");

    // Block the main thread until terminated.
    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

/// Resolve the port from CLI args, then MQLITE_PORT env, then default 27017.
fn parse_port() -> u16 {
    // Check for --port <N> argument.
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--port" {
            if let Some(val) = args.get(i + 1) {
                if let Ok(p) = val.parse::<u16>() {
                    return p;
                }
                eprintln!("wire_server: invalid --port value {:?}, ignoring", val);
            }
        } else if let Some(val) = args[i].strip_prefix("--port=") {
            if let Ok(p) = val.parse::<u16>() {
                return p;
            }
            eprintln!("wire_server: invalid --port= value {:?}, ignoring", val);
        }
        i += 1;
    }

    // Fall back to MQLITE_PORT env var.
    if let Ok(env_val) = std::env::var("MQLITE_PORT") {
        if let Ok(p) = env_val.parse::<u16>() {
            return p;
        }
        eprintln!(
            "wire_server: invalid MQLITE_PORT {:?}, using default",
            env_val
        );
    }

    27017
}
