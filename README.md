# mqlite

Embedded MongoDB-compatible document store for Rust.

Status: pre-1.0. Use a git dependency until the first published release.

## Capabilities

- Embedded Rust API backed by a local `.mqlite` file.
- Crash-safe write-ahead journal with checksum-based recovery and ordered
  publish.
- MongoDB query and update operators with serde-typed or untyped
  `Document` collections.
- Snapshot reads via WiredTiger-style MVCC; multi-writer, multi-reader
  in-process CRUD.
- B+ tree indexes (`create_index`, unique, sparse) with planner-driven
  index selection.
- Optional MongoDB wire-protocol shim for local `mongosh` / `pymongo`
  interop behind the `wire` feature.

## Quick Start

mqlite is pre-1.0 and not yet on crates.io; depend on git until the first
tagged release.

```toml
[dependencies]
mqlite = { git = "https://github.com/kparkins/mqlite" }
serde = { version = "1", features = ["derive"] }
```

## Integration Model

The primary integration surface is the embedded Rust API:
`Client::open(path)` -> `client.database(name)` -> `db.collection::<T>(name)`.
The storage engine is synchronous; async applications should run mqlite calls
on a blocking worker pool.

The MongoDB wire listener is an optional compatibility shim, not the core
embedding path. Enable it only when a local tool or test needs MongoDB wire
interop. It has no authentication or TLS; see
[WIRE-SECURITY.md](docs/WIRE-SECURITY.md).

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
| `wire` | MongoDB wire protocol shim; connect with `mongosh` or `pymongo` (requires `directConnection=true`) |
| `tracing` | Structured observability via the [`tracing`](https://docs.rs/tracing) crate |
| `test-hooks` | Internal crash-cut and state probes for the integration test suite |
| `fuzz` | Internal fuzz-target support |
| `loom-tests` | Internal loom concurrency test support |
| `perf-counters` | Internal performance-counter instrumentation for selected benches |

## Files on Disk

| File | When present | Meaning |
|------|-------------|---------|
| `myapp.mqlite` | Always | Main database file |
| `myapp.mqlite-journal` | During write activity | Append-only write journal; safe to leave and replayed on next open |

A "single-file database" means a single file after a successful checkpoint.
`Client::close()` runs the checkpoint and returns any error. Dropping the last
`Client` handle also attempts a checkpoint, but cannot report failures. If a
process exits, crashes, or cannot finish that checkpoint, the journal may remain
on disk and the next `Client::open` recovers it automatically.

## Documentation

- [Compatibility Matrix](docs/COMPATIBILITY.md) - operator- and
  command-level MongoDB compatibility
- [Concurrency Model](docs/CONCURRENCY.md) - MWMR reads, per-namespace
  write lanes, cross-process locking
- [Errors](docs/ERRORS.md) - `Error` variants and MongoDB error-code
  mapping
- [File Management](docs/FILE-MANAGEMENT.md) - backup, checkpoint,
  crash recovery
- [Verification Guide](docs/VERIFICATION.md) - tests, Jepsen, benchmarks,
  and perf-baseline sidecars
- [Wire Protocol Security Advisory](docs/WIRE-SECURITY.md)

## Verification

The README quick-start snippets are mirrored by `tests/readme_examples.rs`.
The main local smoke check is:

```sh
cargo test --test readme_examples
```

For broader gates, use `cargo test --release --all-targets --features
wire,test-hooks`, the embedded Jepsen suite under `tests/jepsen/`, and the
benchmark matrix documented in `benches/README.md`.

## Release policy

Prior to v1.0, mqlite makes no on-disk format stability guarantees between
tagged releases. Any release may change the binary layout of the database file,
the journal, or catalog entries in a way that requires existing files to be
discarded and recreated. The phrase "pre-release format" in the internal design
documents refers to this policy. Starting at v1.0, format changes will be gated
behind explicit migration paths or version flags.

## License

MIT OR Apache-2.0
