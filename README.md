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
| `myapp.mqlite-journal` | During write activity | Append-only write journal; safe to leave — replayed on next open |

A "single-file database" means a single file **after a clean close** (`Client::close()`).
Dropping the handle is non-blocking and leaves the journal on disk; the next open recovers it.

## Documentation

- [API Reference (docs.rs)](https://docs.rs/mqlite)
- [Architecture](ARCHITECTURE.md) — engine internals, lock order, MVCC read/write/reconcile paths
- [Capabilities](CAPABILITIES.md) — feature surface and what is not supported
- [Compatibility Matrix](docs/COMPATIBILITY.md) — operator- and command-level MongoDB compatibility
- [Concurrency Model](docs/CONCURRENCY.md) — MWMR reads, per-namespace write lanes, cross-process locking
- [Errors](docs/ERRORS.md) — `Error` variants and MongoDB error-code mapping
- [File Management](docs/FILE-MANAGEMENT.md) — backup, checkpoint, crash recovery
- [Wire Protocol Security Advisory](docs/WIRE-SECURITY.md)
- ADRs: [0001 MVCC](docs/adr/0001-mvcc.md), [0002 MWMR](docs/adr/0002-mwmr.md)

## Release policy

Prior to v1.0, mqlite makes no on-disk format stability guarantees between tagged releases. Any release may change the binary layout of the database file, the journal, or catalog entries in a way that requires existing files to be discarded and recreated. The phrase "pre-release format" in the internal design documents refers to this policy. Starting at v1.0, format changes will be gated behind explicit migration paths or version flags.

## License

MIT OR Apache-2.0
