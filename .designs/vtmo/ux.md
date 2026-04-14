# User Experience Analysis

## Summary

mqlite's developer experience must navigate a unique positioning challenge: it is not MongoDB, not SQLite, but borrows mental models from both. Developers arriving from MongoDB expect familiar query semantics, collection-oriented thinking, and a rich operator vocabulary — but will encounter a dramatically simpler operational model (no server, no connection strings, no replica sets). Developers arriving from SQLite expect file-centric simplicity and `open(path)` ergonomics — but encounter a document model and query language they may not know. The UX must serve both audiences without confusing either.

The highest-leverage UX decisions for Phase 1 are: (1) a getting-started experience under 10 lines of code from `cargo add` to first query result, (2) error messages that teach — especially around unsupported MQL operators and writer contention, (3) a `Database::open("path.mqlite")` entry point that Just Works with sensible defaults (auto-WAL, auto-checkpoint, reasonable buffer pool), and (4) clear persona-specific documentation paths for the four primary user stories. The "test double" persona deserves special attention because it will likely be the first high-volume adoption vector, and its needs (in-memory mode, fast teardown, deterministic behavior) diverge sharply from the "embedded app" persona (durability, crash recovery, file management).

## Analysis

### Key Considerations

- **Two-ancestor mental model**: Developers will map mqlite onto either MongoDB or SQLite depending on their background. The API, docs, and error messages must bridge both mental models without creating a uncanny valley where neither model fully applies.
- **The "cargo add" moment**: First impression is the dependency line. A clean `mqlite = "0.1"` with zero feature flags required for basic use sets the tone. Heavy feature-flag configuration before "hello world" works is a UX anti-pattern.
- **Sync-first is a UX decision, not just a technical one**: Embedded databases are overwhelmingly used in synchronous contexts. Forcing `tokio` as a dependency for basic CRUD operations signals "this is a server library" — the wrong message.
- **Writer contention is the #1 surprise for MongoDB developers**: MongoDB developers have never encountered `SQLITE_BUSY`. The first time a concurrent write fails or blocks, the error message must explain the single-writer model and suggest solutions (retry, queue writes, restructure).
- **"Just copy the file" is the killer UX feature**: The ability to backup, version, email, or `scp` a database as a single file is mqlite's strongest differentiator. This UX must be protected — auxiliary WAL/SHM files undermine it unless the story is carefully communicated.
- **Unsupported operator behavior defines trust**: If a test suite silently passes because unsupported operators are ignored, mqlite becomes dangerous as a test double. Explicit errors with operator names and "not supported in mqlite" messages are essential for trust.
- **Progressive disclosure**: The API must be simple for the 80% case (open, insert, find, close) while providing escape hatches for power users (custom buffer pool size, WAL checkpoint control, durability mode selection, query plan explanation).
- **Crate naming signals scope**: `mqlite` as a crate name is excellent — short, memorable, conveys both "MQL" and "lite/SQLite". The module structure within should mirror the conceptual layers (storage, query, api, wire).
- **Error recovery must be self-service**: Users of embedded databases cannot file a support ticket with their DBA. Every error must include enough context for the developer to fix the problem themselves, or at minimum know whether the database is recoverable.

### Options Explored

#### Option A: MongoDB-Driver-Mirror API

- **Description**: Mirror the `mongodb` Rust driver's API surface as closely as possible. `Client` → `Database` (since there's no server), `Database` → direct access, `Collection<T>` generic with serde support, async cursors, `FindOptions`, `InsertOneResult`, etc.
- **Pros**: Maximum familiarity for MongoDB Rust driver users. Code could potentially be switched between `mongodb` and `mqlite` with minimal changes. Strong Story 2 (test double) support.
- **Cons**: The MongoDB driver API is designed around a client-server model (connection pools, read/write concerns, sessions, retryable writes). Mirroring it forces mqlite to either stub out server-oriented concepts (confusing) or break the mirror (defeating the purpose). Async-first driver API conflicts with sync-first embedded use case.
- **Effort**: High — maintaining API compatibility with an evolving upstream driver is ongoing work.

#### Option B: SQLite-Inspired Minimal API

- **Description**: Model the API on SQLite's simplicity. `Database::open(path)` returns a handle. Methods like `db.collection("name")` return a collection handle. CRUD methods are synchronous, return `Result<T, Error>`, and take BSON documents directly. No generics over document types. No options structs unless needed.
- **Pros**: Minimal API surface, easy to learn, hard to misuse. Matches the "embedded library" mental model. No forced async runtime dependency. Simple to document.
- **Cons**: Developers from MongoDB must learn a new (if simpler) API. No serde-based typed document mapping out of the box. Loses the "swap your MongoDB driver" migration story.
- **Effort**: Low — smaller API surface means less to build and maintain.

#### Option C: Hybrid — MongoDB-Shaped, SQLite-Spirited (Recommended)

- **Description**: Use MongoDB naming conventions and conceptual structure (`Database`, `Collection`, `find`, `insert_one`, `Cursor`) but design for embedded simplicity. Sync-first API. `Database::open("path.mqlite")` as the entry point (not `Client::connect`). Support serde-based typed documents via `Collection<T>` but also provide `Collection<Document>` as the untyped default. Options structs for progressive disclosure (`FindOptions`, `OpenOptions`) but with zero required fields. Error types that map to MongoDB error codes where applicable but add mqlite-specific variants (writer busy, unsupported operator, file corrupt).
- **Pros**: Familiar to MongoDB developers without being a false promise of drop-in compatibility. Simple enough for SQLite-oriented developers. Serde support enables type-safe document access. Progressive disclosure via optional options structs.
- **Cons**: Neither a perfect MongoDB driver mirror nor the absolute simplest possible API. Documentation must explicitly address "what's different from the MongoDB driver."
- **Effort**: Medium — requires careful API design up front but results in a maintainable surface.

#### Option D: Trait-Based Abstraction Layer

- **Description**: Define a `DocumentStore` trait that both mqlite and a MongoDB adapter could implement. Users write code against the trait; swap implementations at runtime or compile time.
- **Pros**: Maximum flexibility for the test-double story. Clean separation of concerns.
- **Cons**: Trait design is premature — mqlite's capabilities are a strict subset of MongoDB's, so the trait would either be too narrow (useless for MongoDB) or too broad (mqlite can't implement it). Adds abstraction without proven need. The community can build this later once the concrete API stabilizes.
- **Effort**: Medium, but with high risk of getting the abstraction wrong.

### Recommendation

**Option C: Hybrid — MongoDB-Shaped, SQLite-Spirited.** This balances familiarity with honesty about what mqlite is. The API should feel like "MongoDB's little sibling" — recognizable concepts, simpler signatures, no server-oriented baggage. Specifically:

1. **Entry point**: `Database::open("data.mqlite")` — not `Client::connect("mongodb://...")`. An `OpenOptions` builder for power users.
2. **Core types**: `Database`, `Collection<T>`, `Cursor<T>`, `Document` (re-exported from `bson`).
3. **CRUD methods**: `insert_one`, `insert_many`, `find_one`, `find`, `update_one`, `update_many`, `delete_one`, `delete_many` — same names as MongoDB driver.
4. **Sync-first**: All methods are synchronous. Wire protocol shim uses its own async runtime internally.
5. **Serde integration**: `Collection<MyStruct>` with automatic serialization, `Collection<Document>` for untyped access.
6. **Error model**: `mqlite::Error` enum with variants that map to MongoDB error codes where applicable, plus mqlite-specific variants.
7. **In-memory mode**: `Database::open_in_memory()` for the test-double persona.

## User Journeys

### Journey 1: The Embedded App Developer (Story 1)

**Context**: Building a Rust CLI tool that needs structured local storage. Has used SQLite before. Knows MongoDB exists but hasn't used the Rust driver.

**Getting started (target: under 5 minutes, under 10 lines):**

```toml
# Cargo.toml
[dependencies]
mqlite = "0.1"
serde = { version = "1", features = ["derive"] }
```

```rust
use mqlite::Database;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Config {
    key: String,
    value: String,
}

fn main() -> mqlite::Result<()> {
    let db = Database::open("myapp.mqlite")?;
    let configs = db.collection::<Config>("config");

    configs.insert_one(&Config {
        key: "theme".into(),
        value: "dark".into(),
    })?;

    let theme = configs.find_one(mqlite::doc! { "key": "theme" })?;
    println!("Theme: {:?}", theme);

    Ok(())
}
```

**What must feel right:**
- `Database::open` is the entire setup. No configuration files, no server URLs, no connection pools.
- The `doc!` macro (re-exported from `bson`) is the query language. No new DSL to learn.
- Errors are `Result`-based with `?` propagation. No panics, no unwraps required.
- The database file appears in the current directory. It's visible, copyable, deletable.
- Closing happens on `Drop`. No explicit `.close()` required (but available for explicit flush control).

**Progressive disclosure for power users:**

```rust
use mqlite::{Database, OpenOptions, DurabilityMode};

let db = Database::open_with_options(
    "myapp.mqlite",
    OpenOptions::new()
        .buffer_pool_size(64 * 1024 * 1024)  // 64MB buffer pool
        .durability(DurabilityMode::FullSync)  // fsync per commit
        .wal_auto_checkpoint(1000),            // checkpoint every 1000 pages
)?;
```

**Crash recovery UX:**
- On normal open after a crash: mqlite automatically replays the WAL. No user action needed. A log message (via `tracing` or similar) indicates recovery happened.
- On corrupt file detection: `Database::open` returns `Error::CorruptDatabase { path, detail }` with actionable detail: "WAL header checksum mismatch — database may be recoverable with `Database::repair(path)`".
- Repair API: `Database::repair("myapp.mqlite")` attempts to recover what it can. Returns a report of what was recovered and what was lost.

**File management UX:**
- "How do I back up?": `db.checkpoint()?` forces WAL merge, then the file is safe to copy. Or use `db.backup("backup.mqlite")?` for hot backup.
- "How big is my database?": `db.stats()` returns `DatabaseStats { file_size, doc_count, collection_count, index_count, wal_size, free_pages }`.
- "How do I shrink it?": `db.compact()?` reclaims free pages (like SQLite's `VACUUM`).

### Journey 2: The Test Double Developer (Story 2)

**Context**: Team uses MongoDB in production via the `mongodb` Rust driver. Wants to replace test MongoDB containers with mqlite for speed.

**Getting started:**

```rust
#[cfg(test)]
mod tests {
    use mqlite::Database;

    #[test]
    fn test_user_creation() {
        // In-memory database — no files, no cleanup
        let db = Database::open_in_memory().unwrap();
        let users = db.collection::<bson::Document>("users");

        users.insert_one(bson::doc! {
            "name": "Alice",
            "email": "alice@example.com",
            "role": "admin"
        }).unwrap();

        let user = users.find_one(bson::doc! { "email": "alice@example.com" }).unwrap();
        assert_eq!(user.unwrap().get_str("name").unwrap(), "Alice");
    }
}
```

**What must feel right:**
- `Database::open_in_memory()` is the entire test fixture setup. No temp directories, no cleanup.
- In-memory mode is fast — sub-millisecond for small operations. No disk I/O.
- `Drop` on the `Database` handle releases all memory. No state leaks between tests.
- Query behavior matches the file-backed mode exactly. No "it works in tests but fails in production" surprises.

**Fixture loading:**

```rust
/// Load test fixtures from a JSON file
fn setup_test_db() -> mqlite::Database {
    let db = mqlite::Database::open_in_memory().unwrap();
    let users = db.collection::<bson::Document>("users");

    let fixtures: Vec<bson::Document> = serde_json::from_str(
        include_str!("fixtures/users.json")
    ).unwrap();

    users.insert_many(&fixtures).unwrap();
    db
}
```

**The critical UX contract for test doubles:**
- **Unsupported operators fail loudly**: If production code uses `$lookup` or `$group` (aggregation, out of scope), mqlite must return `Error::UnsupportedOperator { operator: "$lookup", suggestion: "Aggregation pipeline is not supported in mqlite. See https://docs.rs/mqlite/latest/mqlite/compatibility" }`. Silent success would make tests pass that shouldn't.
- **Type coercion matches MongoDB**: If `$gt` on a string field works differently than MongoDB, it's a test-double bug. The compatibility matrix documentation must be brutally honest about known divergences.
- **Deterministic ObjectId generation in test mode**: Consider a `Database::open_in_memory_with_seed(42)` that produces deterministic ObjectIds for snapshot testing.

### Journey 3: The mongosh Interop Developer (Story 3)

**Context**: Wants to inspect an mqlite database file using familiar MongoDB tooling.

**Getting started:**

```rust
use mqlite::{Database, WireProtocol};

let db = Database::open("myapp.mqlite")?;

// Start wire protocol shim on localhost
let _server = WireProtocol::bind(&db, "127.0.0.1:27017")?;
// Server runs in background thread, stops on drop

println!("Connect with: mongosh mongodb://localhost:27017");
```

**Or via CLI tool (if provided):**

```bash
$ mqlite serve myapp.mqlite --port 27017
Serving myapp.mqlite on 127.0.0.1:27017
Connect with: mongosh mongodb://localhost:27017
^C to stop
```

**What must feel right:**
- `mongosh` connects and completes the handshake without errors or scary warnings.
- `show dbs`, `show collections`, `db.users.find()` work as expected.
- CRUD operations work: `db.users.insertOne({...})`, `db.users.find({name: "Alice"})`.
- Unsupported commands return clear errors: `"mqlite does not support aggregation pipeline"` — not cryptic protocol errors or hangs.
- The connection is localhost-only by default. No accidental exposure to the network.

**Discovery UX concerns:**
- The wire protocol shim reports a server version. This version determines what `mongosh` and drivers attempt. mqlite must report a version that doesn't trigger capabilities it can't support (e.g., don't report 7.0 if that implies sessions and transactions). Consider reporting a synthetic version like `"mqlite/0.1.0"` and handling the resulting driver behavior, or report a conservative MongoDB version (3.6) that predates most unsupported features.
- Connection strings: `mongodb://localhost:27017/?directConnection=true` should be documented. Drivers may attempt replica set discovery by default; `directConnection=true` prevents this.

### Journey 4: The Edge/IoT Developer (Story 4)

**Context**: Building a sensor gateway on a Raspberry Pi. Collects readings into a local database. Pushes to cloud MongoDB when connectivity is available.

**Getting started:**

```rust
use mqlite::{Database, OpenOptions};

let db = Database::open_with_options(
    "/var/lib/gateway/readings.mqlite",
    OpenOptions::new()
        .buffer_pool_size(4 * 1024 * 1024)  // 4MB — constrained device
        .durability(DurabilityMode::FullSync) // Cannot lose sensor data
)?;

let readings = db.collection::<bson::Document>("readings");

// Insert sensor reading
readings.insert_one(bson::doc! {
    "sensor_id": "temp-001",
    "value": 23.5,
    "ts": bson::DateTime::now(),
})?;

// Sync: read all documents since last sync
let cursor = readings.find(bson::doc! {
    "ts": { "$gt": last_sync_time }
})?;

for doc in cursor {
    let doc = doc?;
    // Push to cloud MongoDB...
}
```

**What must feel right:**
- Resource usage is predictable. No unbounded memory growth. Buffer pool size is configurable and respected.
- Disk usage is predictable. WAL doesn't grow without bound. Auto-checkpoint keeps file size stable.
- Crash recovery is automatic and reliable. Power cuts are expected in IoT; the database must recover without data loss (within the configured durability mode).
- The library footprint is small. No tokio, no heavy transitive dependencies. Cross-compiles to ARM without issues.
- `db.stats()` lets the gateway monitor disk usage and trigger alerts before the SD card fills up.

**Edge-specific concerns:**
- **Disk full handling**: `Error::DiskFull` must be clearly distinguishable from other I/O errors. The database must remain readable after a disk-full write failure. The developer must be able to delete old documents to free space and resume writes.
- **Read-only filesystem recovery**: If the device reboots into a read-only filesystem, `Database::open_read_only("readings.mqlite")` should work without attempting WAL replay or writes. This is critical for forensic access after failures.
- **No surprises at 2 AM**: The library must never panic in release mode. All failures must be surfaced as `Result::Err`. Resource exhaustion (memory, disk, file descriptors) must be handled gracefully, not with `unwrap` or `expect` inside the library.

## Failure Mode UX

### Writer Contention ("SQLITE_BUSY" equivalent)

This is the #1 UX surprise for MongoDB developers. Design it carefully:

```rust
// What the developer sees:
let result = collection.insert_one(doc);
// Err(Error::WriterBusy { held_for: Duration, suggestion: "..." })
```

**Error message design:**
```
Error: WriterBusy — another write operation is in progress.
The database uses a single-writer model. Only one write operation
can execute at a time.

The current writer has held the lock for 150ms.

To resolve:
  - If single-threaded: ensure previous write completed before starting a new one
  - If multi-threaded: serialize writes through a channel or mutex
  - Configure a busy timeout: OpenOptions::new().busy_timeout(Duration::from_secs(5))
```

**API for handling contention:**
```rust
// Option 1: Busy timeout (blocks up to N seconds, then fails)
let db = Database::open_with_options("data.mqlite",
    OpenOptions::new().busy_timeout(Duration::from_secs(5))
)?;

// Option 2: Busy handler callback (like SQLite)
let db = Database::open_with_options("data.mqlite",
    OpenOptions::new().busy_handler(|attempts| {
        if attempts < 10 {
            std::thread::sleep(Duration::from_millis(100));
            true  // retry
        } else {
            false // give up
        }
    })
)?;
```

### Unsupported Operator

```
Error: UnsupportedOperator("$lookup")
  The $lookup operator (aggregation pipeline) is not supported in mqlite.
  Phase 1 supports: $eq, $gt, $gte, $lt, $lte, $ne, $in, $nin,
                     $and, $or, $not, $nor, $exists, $type,
                     $all, $elemMatch, $regex
  See: https://docs.rs/mqlite/latest/mqlite/compatibility
```

### Corrupt Database

```
Error: CorruptDatabase {
    path: "data.mqlite",
    detail: "Page 4072: B+ tree internal node checksum mismatch
             (expected 0xa1b2c3d4, found 0x00000000).
             This may indicate a partial write due to power loss.",
    recoverable: true,
    suggestion: "Run Database::repair(\"data.mqlite\") to attempt recovery.
                 Consider keeping a backup before repair."
}
```

### Disk Full

```
Error: DiskFull {
    path: "data.mqlite",
    required_bytes: 32768,
    available_bytes: 4096,
    suggestion: "The write-ahead log cannot be extended. The database remains
                 readable. Free disk space and retry the write operation.
                 Current WAL size: 2.1 MB. Consider running db.checkpoint()
                 after freeing space to reclaim WAL space."
}
```

## Documentation Needs

### Tier 1: Must-Have for Launch

1. **README / Quick Start**: `cargo add` → open → insert → query → done. Under 30 lines. Both `Document` and typed serde examples.
2. **API Reference** (`docs.rs`): Every public type, method, and error variant documented with examples. `Database::open` docs must explain the file lifecycle (creation, WAL, checkpoint, close).
3. **MongoDB Compatibility Matrix**: A table listing every MQL operator with its support status: Supported, Partial (with notes), Not Supported. Updated with every release. This is the trust document for the test-double persona.
4. **Error Guide**: Every `Error` variant with causes, recovery steps, and example code. This replaces the DBA that embedded database users don't have.
5. **Concurrency Guide**: Explains the SWMR model, writer contention, busy timeout, and patterns for multi-threaded access. MongoDB developers need this because they've never encountered single-writer semantics.
6. **Migration Guide from MongoDB Rust Driver**: Side-by-side code comparisons. `mongodb::Client::with_uri_str("mongodb://...")` → `mqlite::Database::open("data.mqlite")`. What transfers, what's different, what's missing. **Note**: The PRD (Business/Adoption constraints) explicitly requires this as a Phase 1 deliverable: "Documentation must include migration guide from MongoDB driver to mqlite."

### Tier 2: Important for Adoption

7. **Test Double Cookbook**: Patterns for using mqlite in test suites. Fixture loading, parallel test isolation, asserting against query results.
8. **File Management Guide**: Backup strategies, file copying safety, WAL and auxiliary file explanations, compaction, size monitoring.
9. **IoT/Embedded Deployment Guide**: Resource configuration, cross-compilation notes, durability modes, disk-full handling.

### Tier 3: Power User / Later

10. **Query Plan Explanation**: How to use `explain()` to understand index usage and query execution.
11. **Wire Protocol Shim Guide**: Setup, `mongosh` connection, limitations, security considerations.
12. **Performance Tuning**: Buffer pool sizing, checkpoint frequency, durability vs. speed trade-offs.
13. **File Format Specification**: For advanced users building backup tools, forensic analysis, or third-party readers.

**Note on tier numbering**: Items 1-6 are Tier 1 (mandatory for Phase 1 launch), 7-9 are Tier 2 (important for adoption), 10-13 are Tier 3 (power user / later).

### Documentation as Implementation Tasks (PRD Phase 1 DoD #10)

Documentation is a Phase 1 deliverable, not an afterthought. The following documentation tasks must be tracked as explicit work items in the implementation plan:

| Doc | Phase | Depends On | Effort |
|-----|-------|-----------|--------|
| README / Quick Start | During API stabilization | Core CRUD API finalized | Low |
| API Reference (docs.rs) | During implementation | Each module as it's built | Medium (ongoing) |
| Compatibility Matrix | After query engine | All operators implemented | Low |
| Error Guide | After error taxonomy | All error variants defined | Medium |
| Concurrency Guide | After SWMR impl | WAL + writer lock working | Medium |
| Migration Guide | After API stable | Full API surface finalized | Medium |
| Wire Protocol Security Advisory | Before wire protocol release | Wire protocol working | Low |

Each Tier 1 doc (including the migration guide, per PRD mandate) must be complete before Phase 1 is declared done. Tier 2 docs should be drafted during Phase 1 and finalized shortly after.

## Observability UX

Developers debugging issues need visibility into mqlite's behavior:

```rust
// Database statistics
let stats = db.stats()?;
println!("File size: {} bytes", stats.file_size);
println!("Collections: {}", stats.collection_count);
println!("Total documents: {}", stats.document_count);
println!("WAL size: {} bytes", stats.wal_size);
println!("Buffer pool: {}/{} pages used", stats.buffer_pool_used, stats.buffer_pool_total);
println!("Free pages: {}", stats.free_page_count);

// Query plan explanation
let plan = collection.find(doc! { "email": "alice@example.com" })
    .explain()?;
println!("{}", plan);
// Output:
//   Query: { "email": "alice@example.com" }
//   Plan: IndexScan { index: "email_1", bounds: ["alice@example.com", "alice@example.com"] }
//   Estimated docs examined: 1
//   Index used: yes

// Collection statistics
let coll_stats = collection.stats()?;
println!("Documents: {}", coll_stats.document_count);
println!("Avg document size: {} bytes", coll_stats.avg_document_size);
println!("Indexes: {:?}", coll_stats.index_names);
println!("Total index size: {} bytes", coll_stats.total_index_size);
```

**Tracing integration**: mqlite should emit `tracing` spans/events (behind a feature flag) so applications using `tracing-subscriber` get automatic visibility into query execution, WAL operations, and checkpoint activity.

## Crate/Library Naming and Module Organization

### Crate name: `mqlite`

Short, memorable, descriptive. Conveys both "MQL" (MongoDB Query Language) and "lite" (lightweight, SQLite-class). Available on crates.io (verify before implementation).

### Module organization

```
mqlite/
├── lib.rs              # Re-exports: Database, Collection, Cursor, Error, doc!, bson types
├── database.rs         # Database::open, Database::open_in_memory, OpenOptions
├── collection.rs       # Collection<T>, CRUD methods
├── cursor.rs           # Cursor<T>, Iterator implementation
├── error.rs            # Error enum, error codes, Result type alias
├── options.rs          # FindOptions, InsertOptions, UpdateOptions, IndexOptions
├── index.rs            # IndexModel, create_index, drop_index
├── results.rs          # InsertOneResult, UpdateResult, DeleteResult
├── bson_compat.rs      # Re-exports from bson crate, doc! macro
│
├── wire/               # Wire protocol shim (feature-gated)
│   ├── mod.rs
│   ├── server.rs       # WireProtocol::bind, TCP listener
│   ├── protocol.rs     # OP_MSG parsing, hello handshake
│   └── commands.rs     # Command dispatch (find, insert, listCollections, etc.)
│
└── (internal modules — not public API)
    ├── query/          # Query engine, planner, operator evaluation
    ├── storage/        # B+ tree, page manager, buffer pool
    └── wal/            # Write-ahead log, checkpoint
```

### Feature flags

```toml
[features]
default = []
wire = ["tokio", "tokio-util"]     # Wire protocol shim (pulls in async runtime)
tracing = ["tracing"]               # Observability via tracing crate
```

The wire protocol is feature-gated so the base crate has zero async dependencies. This is a critical UX decision for the embedded and IoT personas.

### Public API surface (re-exported from `lib.rs`)

```rust
// Core types
pub use database::{Database, OpenOptions, DurabilityMode};
pub use collection::Collection;
pub use cursor::Cursor;
pub use error::{Error, Result};
pub use index::IndexModel;

// Result types
pub use results::{InsertOneResult, InsertManyResult, UpdateResult, DeleteResult};

// Options (progressive disclosure — all fields optional)
pub use options::{FindOptions, UpdateOptions, DeleteOptions, CountOptions};

// BSON re-exports (so users don't need a separate bson dependency)
pub use bson::{doc, Document, Bson, oid::ObjectId, DateTime};

// Wire protocol (feature-gated)
#[cfg(feature = "wire")]
pub use wire::WireProtocol;
```

Users should need only `use mqlite::*` or specific imports from `mqlite::` — no need to directly depend on the `bson` crate for basic usage.

## Constraints Identified

1. **Sync-first API is non-negotiable for the embedded persona.** Forcing an async runtime as a required dependency contradicts the "zero-server, lightweight" positioning. The wire protocol shim brings its own runtime behind a feature flag.

2. **Unsupported operators must fail explicitly, never silently.** The test-double persona's trust depends on this. A silent no-op for `$lookup` is worse than a crash — it creates false confidence.

3. **`Database::open(path)` must work with zero configuration.** Sensible defaults for buffer pool, WAL checkpoint, durability mode. Power user knobs exist but are never required.

4. **In-memory mode must be available from day one** if the test-double persona is a Phase 1 story. It's not optional — it's the core value proposition for the highest-volume adoption path.

5. **The `bson` crate must be re-exported.** Users should not need `bson = "2"` in their `Cargo.toml` for basic usage. Version mismatches between mqlite's bson and the user's bson are a predictable pain point — re-exporting prevents it.

6. **Writer contention must have a configurable timeout, not just immediate failure.** `SQLITE_BUSY` with no busy handler is the #1 complaint about SQLite. mqlite should ship with a default busy timeout (e.g., 5 seconds) rather than requiring users to discover and configure it.

7. **No panics in library code.** All failures must be `Result::Err`. IoT and embedded contexts cannot tolerate library panics. This is a hard constraint that must be enforced via CI (`#![deny(clippy::unwrap_used)]` or equivalent).

8. **Auxiliary files (WAL, SHM) must be documented.** If "single file" means "single file when cleanly closed, with transient WAL/SHM files during operation" (like SQLite), this must be front-and-center in documentation, not buried. Backup/copy guides must explain when it's safe to copy.

9. **MongoDB error codes should be used where applicable.** When mqlite returns an error that has a MongoDB equivalent (e.g., duplicate key: error code 11000), use the same code. This enables compatibility with error-handling code written for MongoDB.

## Open Questions

1. **Should `Collection<T>` require `T: Serialize + DeserializeOwned`, or should there be both typed and untyped collection handles?** The MongoDB Rust driver uses `Collection<T>` where `T` defaults to `Document`. mqlite should follow this pattern, but the specific trait bounds affect API ergonomics and compile times.

2. **What is the default busy timeout?** SQLite defaults to 0ms (immediate failure). This is widely considered a bad default. mqlite should pick a non-zero default (e.g., 5 seconds) but this is a behavioral decision that affects all users.

3. **Should `Database` implement `Clone`?** If `Database` is an `Arc<Inner>` internally, `Clone` is cheap and enables sharing across threads. This is the natural Rust pattern for shared resources. But it must be documented whether cloned handles share the same writer lock, buffer pool, etc.

4. **How should the wire protocol shim report its server version?** Reporting as a real MongoDB version (e.g., 7.0) triggers feature detection in drivers that expects capabilities mqlite doesn't have. Reporting a low version (3.6) limits driver features. Reporting a custom version string may confuse drivers entirely. This needs testing with actual `mongosh` and driver versions.

5. ~~**Is a CLI tool (`mqlite` binary) in scope for Phase 1?**~~ **RESOLVED**: Defer to Phase 1.1. Phase 1 ships with programmatic `WireProtocol::bind()` API only. A `mqlite serve <file>` CLI would improve the wire protocol UX but is not required — Story 3 can be satisfied with programmatic binding. The CLI (serve, stats, compact subcommands) is planned for Phase 1.1.

6. **Should `find()` return an `Iterator` or a custom `Cursor` type that implements `Iterator`?** A custom `Cursor` can expose additional methods (`explain()`, `count()`, `batch_size()`) while still being usable in `for` loops. The MongoDB driver uses a `Cursor` type — mqlite should follow suit but with a sync `Iterator` implementation, not an async `Stream`.

7. **What is the behavior of `Drop` on `Database`?** Options: (a) flush WAL and close cleanly (may block), (b) close without flush (risk of data loss for un-fsynced writes), (c) close without flush but WAL is recoverable on next open. The `Drop` behavior must be documented because Rust developers rely on RAII for cleanup.

8. **Should mqlite provide a `#[cfg(test)]` helper module?** Something like `mqlite::test::temp_db()` that creates an in-memory database with common test utilities (fixture loading, assertion helpers) could reduce boilerplate for the test-double persona.

## Integration Points

### With Storage Engine Design
- **Buffer pool sizing** and **WAL checkpoint thresholds** are both UX-visible configuration knobs. The storage engine must expose these as tunable parameters that the API layer surfaces via `OpenOptions`.
- **File lifecycle** (creation, WAL presence, checkpoint, close, auxiliary files) must be documented as user-facing behavior, not just implementation detail.
- **Repair and integrity check** APIs depend on storage engine capabilities.
- **`db.stats()` and `collection.stats()`** require the storage engine to expose page counts, free space, B+ tree depth, etc.

### With Query Engine Design
- **Operator support matrix** defines the compatibility contract. Every operator the query engine implements (or doesn't) is a UX surface.
- **`explain()` API** requires the query planner to produce human-readable plan descriptions.
- **Error messages for unsupported operators** require the query engine to identify and name operators it can't handle, not just fail generically.
- **Sort and projection** support directly affects which MongoDB code patterns work in mqlite.

### With Wire Protocol Design
- **Server version reporting** affects driver behavior and must be coordinated with the feature set.
- **Command allowlist** determines what `mongosh` operations work — this is a UX surface, not just a protocol concern.
- **Error responses** must use MongoDB wire protocol error format so tools display them correctly.
- **Binding configuration** (localhost-only default, port selection) is a UX decision with security implications.

### With Error Taxonomy Design
- **Error types** are the primary teaching surface when things go wrong. The error enum design must be coordinated with every layer that can fail.
- **Error codes** that map to MongoDB error codes enable reuse of error-handling code. The mapping should be explicit and documented.
- **Recovery guidance** in error messages requires understanding of what recovery is actually possible at each failure point.

### With Documentation
- **Compatibility matrix** requires continuous coordination with query engine development — every operator added or omitted must update the matrix.
- **Migration guide** requires finalized API surface — can't be written until the API is stable.
- **Concurrency guide** requires finalized writer contention behavior and busy timeout semantics.
