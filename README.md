# mqlite

Embedded MongoDB-compatible document store for Rust.

[![crates.io](https://img.shields.io/crates/v/mqlite)](https://crates.io/crates/mqlite)
[![docs.rs](https://docs.rs/mqlite/badge.svg)](https://docs.rs/mqlite)

## Quick Start

```toml
[dependencies]
mqlite = "0.1"
serde = { version = "1", features = ["derive"] }
```

**Untyped documents:**

```rust
use mqlite::{Client, doc};

fn main() -> mqlite::Result<()> {
    let client = Client::open("myapp.mqlite")?;
    let db = client.database("myapp");
    let events = db.collection::<mqlite::Document>("events");
    events.insert_one(&doc! { "action": "login", "user": "alice" })?;
    let event = events.find_one(doc! { "user": "alice" })?;
    println!("{:?}", event);
    Ok(())
}
```

**Typed structs (serde):**

```rust
use mqlite::{Client, doc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Config { key: String, value: String }

fn main() -> mqlite::Result<()> {
    let client = Client::open("myapp.mqlite")?;
    let db = client.database("myapp");
    let configs = db.collection::<Config>("config");
    configs.insert_one(&Config { key: "theme".into(), value: "dark".into() })?;
    let theme = configs.find_one(doc! { "key": "theme" })?;
    println!("{:?}", theme);
    Ok(())
}
```

## Feature Flags

| Flag | Description |
|------|-------------|
| `wire` | MongoDB wire protocol shim — connect with `mongosh` or `pymongo` (requires `directConnection=true`) |
| `tracing` | Structured observability via the [`tracing`](https://docs.rs/tracing) crate |

## Files on Disk

| File | When present | Meaning |
|------|-------------|---------|
| `myapp.mqlite` | Always | Main database file |
| `myapp.mqlite-wal` | During write activity | Write-ahead log (WAL); safe to leave — replayed on next open |
| `myapp.mqlite-shm` | While database is open | Shared-memory WAL index; deleted on clean close |

A "single-file database" means a single file **after a clean close** (`Client::close()`).
Dropping the handle is non-blocking and leaves the WAL on disk; the next open recovers it.

## Documentation

- [API Reference (docs.rs)](https://docs.rs/mqlite)
- [Compatibility Matrix](docs/COMPATIBILITY.md)
- [Error Guide](docs/ERRORS.md)
- [Migration Guide](docs/MIGRATION.md)
- [Wire Protocol Security Advisory](docs/WIRE-SECURITY.md)
- [Test Double Cookbook](docs/TEST-DOUBLE-COOKBOOK.md)
- [File Management Guide](docs/FILE-MANAGEMENT.md)
- [IoT and Embedded Deployment Guide](docs/IOT-DEPLOYMENT.md)

## License

MIT OR Apache-2.0
