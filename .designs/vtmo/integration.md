# Integration Analysis

## Summary

mqlite's integration surface spans three ecosystems: the MongoDB tool/driver ecosystem (mongosh, pymongo, MongoDB Rust driver), the Rust crate ecosystem (bson, serde, tokio, tracing), and deployment environments (embedded apps, edge/IoT, CI test pipelines). The wire protocol shim is the highest-risk integration point — it must satisfy mongosh's handshake expectations and pymongo's command semantics while advertising only the capabilities mqlite actually implements. The compatibility target is MongoDB 8.0 (maxWireVersion 21), but mqlite must carefully omit capabilities it doesn't support (sessions, transactions, change streams, aggregation) from the hello response to prevent drivers from attempting unsupported operations. Phase 1 acceptance criteria are concrete: mongosh connects and runs basic CRUD, a non-trivial pymongo test suite passes (insert/find/update/delete with indexes), crash recovery survives kill -9, and the database is a single file after clean close.

The Rust ecosystem integration is straightforward but has one critical constraint: the base mqlite crate must have zero async runtime dependency. The wire protocol shim (behind the `wire` feature flag) pulls tokio; the base crate is sync-only. The official `bson` crate is used and re-exported so users don't need a separate bson dependency — but this pins the bson version as part of mqlite's public API (upgrading bson is semver-breaking). Testing integration is the most labor-intensive dimension: MongoDB's CRUD spec tests, pymongo integration tests, Jepsen-style crash testing, and BSON/wire-protocol fuzz testing each require dedicated infrastructure. The testing strategy must be designed from day one, not bolted on after implementation.

## Analysis

### Key Considerations

- **mongosh is the primary wire protocol validation tool.** If mongosh can connect, run `show dbs`, `show collections`, `db.collection.find()`, and basic CRUD — the wire protocol is working. mongosh exercises the hello handshake, cursor management (getMore), and command response parsing.
- **pymongo is the acceptance test driver.** Per Q11, a non-trivial pymongo test suite must pass. This means pymongo's connection handshake, CRUD operations, index management, and error handling must all work correctly through the wire protocol.
- **MongoDB 8.0 is the reference implementation.** All compatibility questions are resolved by "what does MongoDB 8.0 do?" Error codes, command responses, BSON type handling, and query semantics must match.
- **The bson crate is a public API dependency.** Types like `Document`, `Bson`, `ObjectId`, and the `doc!` macro are re-exported from mqlite. This means the bson crate version is part of mqlite's semver contract.
- **Feature flags partition the dependency tree.** `wire` pulls tokio + tokio-util. `tracing` pulls the tracing crate. Default features = none. This ensures the base crate is lightweight for embedded/IoT use.
- **directConnection=true is required for all driver connections.** Without it, drivers attempt replica set discovery (sending isMaster to discover other members), which mqlite cannot support. This must be prominently documented.
- **Error code compatibility is a testing surface.** When mqlite returns an error, the numeric code and codeName must match MongoDB 8.0. Applications and drivers use these codes for programmatic error handling.
- **Cross-compilation is a deployment requirement.** The edge/IoT persona requires ARM (aarch64, armv7) cross-compilation. Pure Rust with no C dependencies makes this feasible, but it must be tested in CI.
- **MongoDB trademark risk exists.** Reporting as "mongod" in the wire protocol or marketing as "MongoDB-compatible" may trigger trademark enforcement. Use "mqlite" in all handshake responses and "MQL-compatible" in documentation.

### Options Explored

#### Option 1: Minimal Wire Protocol Shim — mongosh Only

- **Description**: Implement the absolute minimum wire protocol surface for mongosh to connect and run basic CRUD. Skip OP_COMPRESSED. Implement only the commands mongosh uses: hello, find, insert, update, delete, listDatabases, listCollections, getMore, killCursors.
- **Pros**: Smallest implementation surface. Fastest to deliver. Meets the "debugging/interop" positioning.
- **Cons**: pymongo uses additional commands (findAndModify, createIndexes, serverStatus). Missing commands cause pymongo test failures. Doesn't meet Q11 acceptance criteria.
- **Effort**: Medium.

#### Option 2: Phase 1 Command Set — mongosh + pymongo (Recommended)

- **Description**: Implement all 18 Phase 1 commands (insert, find, update, delete, findAndModify, getMore, killCursors, createIndexes, dropIndexes, listIndexes, listCollections, create, drop, ping, hello, isMaster, buildInfo, serverStatus, listDatabases). Support OP_MSG framing. Defer OP_COMPRESSED to Phase 1.1.
- **Pros**: Meets Q11 acceptance criteria. Covers mongosh and pymongo. Clear command boundary — anything not in the list returns CommandNotFound (code 59). Sufficient for the test-double persona's basic CRUD validation.
- **Cons**: 18 commands is significant implementation work. Each command has nuanced response formats that must match MongoDB 8.0. findAndModify is particularly complex.
- **Effort**: Medium-High.

#### Option 3: Full MongoDB 8.0 Command Surface

- **Description**: Implement all MongoDB 8.0 commands including aggregation, count, distinct, createUser, etc.
- **Pros**: Maximum compatibility. Any MongoDB tool works.
- **Cons**: Aggregation alone is a multi-month effort. createUser requires an auth system. This is out of scope for Phase 1 and contradicts the PRD's non-goals.
- **Effort**: Very High (years of work).

#### Option 4: Custom Protocol with MongoDB Translation Proxy

- **Description**: Define a simpler custom protocol for mqlite. Provide a separate translation proxy that converts MongoDB wire protocol to the custom protocol.
- **Pros**: Simpler core implementation. Protocol can be optimized for mqlite's single-file model.
- **Cons**: The proxy is an additional component to maintain. Defeats the "just connect mongosh" value proposition. Adds operational complexity (start the proxy, configure ports). Drivers can't connect directly.
- **Effort**: Medium (core) + Medium (proxy) = High total.

### Recommendation

**Option 2: Phase 1 Command Set.** This satisfies the acceptance criteria with a bounded scope. Specific implementation plan:

1. **OP_MSG framing**: Parse and generate OP_MSG (opcode 2013). Support Section Kind 0 (body) and Kind 1 (document sequence). Validate message checksums if flagChecksumPresent is set. Defer OP_COMPRESSED.
2. **Handshake**: hello/isMaster responses advertise maxWireVersion=21 (MongoDB 8.0) with mqlite-specific metadata. Omit saslSupportedMechs, logicalSessionTimeoutMinutes, and other server-only fields.
3. **Command dispatch**: Match on command name string from the first document in Section Kind 0. Unknown commands → error code 59.
4. **Response format**: Every response includes `ok: 1` or `ok: 0` with `errmsg`, `code`, `codeName` for errors. Match MongoDB 8.0 response schemas exactly.
5. **Testing**: Validate against mongosh 2.x and pymongo 4.x. Build a compatibility test suite that runs the same operations against both real MongoDB 8.0 and mqlite, comparing results.

## MongoDB Wire Protocol Integration

### OP_MSG Framing (Opcode 2013)

```
┌─────────────────────────────────────────────────┐
│ MsgHeader (16 bytes)                            │
│   messageLength: int32                          │
│   requestID: int32                              │
│   responseTo: int32                             │
│   opCode: int32 (2013 = OP_MSG)                │
├─────────────────────────────────────────────────┤
│ flagBits: uint32                                │
│   bit 0: checksumPresent                        │
│   bit 1: moreToCome                             │
│   bit 16: exhaustAllowed                        │
├─────────────────────────────────────────────────┤
│ Sections (repeated)                             │
│   Kind 0 (body): single BSON document           │
│   Kind 1 (sequence): identifier + BSON docs     │
├─────────────────────────────────────────────────┤
│ [optional] Checksum: uint32 (CRC32C)            │
└─────────────────────────────────────────────────┘
```

Phase 1 implementation:
- Parse Kind 0 (body) sections — this carries the command document.
- Parse Kind 1 (document sequence) sections — used for bulk insert/update/delete documents.
- Validate checksum if present; generate checksum on responses if client sent one.
- Reject messages exceeding 48MB (maxMessageSizeBytes).
- Do NOT implement OP_COMPRESSED (opcode 2012) in Phase 1. If a client sends OP_COMPRESSED, return an error. Most drivers fall back to uncompressed when compression negotiation fails.

### OP_COMPRESSED Decision

| Compressor | Crate | Pure Rust | Notes |
|-----------|-------|-----------|-------|
| snappy | `snap` | Yes | Google's Snappy. Fast, moderate ratio. |
| zlib | `flate2` | Yes (miniz_oxide) | Standard. Slower, better ratio. |
| zstd | `zstd` | No (C library) | Best ratio. Requires C dependency. |

**Recommendation**: Defer OP_COMPRESSED to Phase 1.1. For localhost debugging (the primary wire protocol use case), compression adds overhead without benefit. If implemented later, support snappy only (pure Rust, fast, used by default in MongoDB drivers).

### Handshake Response

```javascript
// hello / isMaster response
{
    "isWritablePrimary": true,          // standalone, always writable
    "topologyVersion": {
        "processId": ObjectId("..."),   // generated on startup
        "counter": NumberLong(0)
    },
    "maxBsonObjectSize": 16777216,      // 16 MB
    "maxMessageSizeBytes": 48000000,    // 48 MB
    "maxWriteBatchSize": 100000,
    "localTime": ISODate("..."),
    "connectionId": 1,
    "minWireVersion": 0,
    "maxWireVersion": 21,               // MongoDB 8.0
    "readOnly": false,
    "ok": 1,
    // mqlite-specific (PRD trademark constraint: identify as mqlite, not MongoDB)
    "mqlite": {
        "version": "0.1.0"
    }
}
```

Fields deliberately **omitted** (mqlite doesn't support these):
- `logicalSessionTimeoutMinutes` — no sessions
- `saslSupportedMechs` — no authentication
- `setName`, `setVersion`, `secondary`, `hosts`, `passives`, `arbiters` — no replica sets
- `compression` — no OP_COMPRESSED in Phase 1
- `serviceId` — not a load-balanced deployment

### Server Version Reporting

mqlite does NOT report a MongoDB server version string. The `buildInfo` command returns:

```javascript
{
    "version": "0.1.0",                 // mqlite version
    "gitVersion": "abc123...",
    "modules": [],
    "allocator": "rust",
    "mqlite": true,                     // identifies this as mqlite
    "ok": 1
}
```

Drivers and tools that check `buildInfo.version` to determine capabilities will see a non-MongoDB version string. This is intentional — it prevents mqlite from claiming to be a specific MongoDB version with capabilities it lacks.

## MongoDB Driver Compatibility

### mongosh and Compass Compatibility

mongosh and MongoDB Compass exercise the following commands during typical sessions. Compass uses the same wire protocol as mongosh but may exercise additional commands for its GUI features (schema analysis, explain visualization). Phase 1 targets mongosh compatibility; Compass compatibility is a stretch goal.

mongosh exercises the following commands during a typical session:

| Action | Commands Used | Phase 1 Status |
|--------|-------------|----------------|
| Connect | hello (or legacy isMaster) | Supported |
| `show dbs` | listDatabases | Supported |
| `show collections` | listCollections | Supported |
| `use <db>` | (client-side only) | Works (single DB) |
| `db.coll.insertOne({})` | insert | Supported |
| `db.coll.find({})` | find + getMore | Supported |
| `db.coll.updateOne({}, {})` | update | Supported |
| `db.coll.deleteOne({})` | delete | Supported |
| `db.coll.createIndex({})` | createIndexes | Supported |
| `db.coll.getIndexes()` | listIndexes | Supported |
| `db.coll.aggregate([])` | aggregate | NOT supported (code 59) |
| `db.coll.count()` | count | NOT supported (code 59) |
| `db.coll.distinct()` | distinct | NOT supported (code 59) |

### pymongo Compatibility

pymongo (4.x) uses the following during typical CRUD:

| Operation | Wire Command | Notes |
|-----------|-------------|-------|
| `MongoClient()` | hello + buildInfo | Must succeed |
| `collection.insert_one()` | insert | Standard OP_MSG |
| `collection.find()` | find + getMore | Cursor iteration |
| `collection.update_one()` | update | With update operators |
| `collection.delete_one()` | delete | Standard |
| `collection.find_one_and_update()` | findAndModify | Atomic operation |
| `collection.create_index()` | createIndexes | Index management |
| `collection.list_indexes()` | listIndexes | Returns cursor |

pymongo connection requires `directConnection=True`:
```python
client = MongoClient("mongodb://localhost:27017/?directConnection=true")
```

Without `directConnection=True`, pymongo attempts replica set discovery and fails.

### MongoDB Rust Driver Interop

The `mongodb` Rust crate is async-first and designed for server MongoDB. Direct interop (using `mongodb` crate to connect to mqlite's wire protocol) is possible but not a Phase 1 target. The connection string would be:

```rust
let client = mongodb::Client::with_uri_str(
    "mongodb://localhost:27017/?directConnection=true"
).await?;
```

This works if mqlite's wire protocol correctly handles the Rust driver's handshake and CRUD commands. The Rust driver uses the same wire protocol as pymongo and mongosh.

## BSON Ecosystem Integration

### Official bson Crate

mqlite uses the `bson` crate (maintained by MongoDB Inc.) as its BSON layer:

```toml
[dependencies]
bson = "2"
```

Re-exported in `mqlite/lib.rs`:
```rust
pub use bson::{doc, Document, Bson, oid::ObjectId, DateTime};
```

This means users write:
```rust
use mqlite::{doc, Document};  // No separate bson dependency needed
```

### Version Pinning Strategy

The bson crate version is part of mqlite's public API (types appear in method signatures). Version policy:

- **mqlite 0.x**: Pin to `bson = "2"`. Upgrading to bson 3.x is a semver-breaking change for mqlite.
- **Feature: `bson-compat-3`**: If bson 3.x releases with breaking changes, provide a feature flag for early adopters while maintaining bson 2.x as default.
- **Re-export avoids version conflicts**: Users who also depend on `bson` directly get the same version mqlite uses, avoiding type incompatibilities.

### serde Integration

`Collection<T>` requires `T: Serialize + DeserializeOwned`. The bson crate's serde support handles BSON ↔ Rust struct conversion:

```rust
#[derive(Serialize, Deserialize)]
struct User {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    id: Option<ObjectId>,
    name: String,
    email: String,
}

let users = db.collection::<User>("users");
users.insert_one(&User { id: None, name: "Alice".into(), email: "a@b.com".into() })?;
```

The bson crate handles `DateTime`, `ObjectId`, `Decimal128`, and other BSON-specific types via serde custom serialization.

## Rust Ecosystem Integration

### Crate Structure and Feature Flags

```toml
[package]
name = "mqlite"
version = "0.1.0"
edition = "2021"
rust-version = "1.70"        # Minimum supported Rust version
license = "MIT OR Apache-2.0" # PRD constraint: must be Apache 2.0 or MIT

[features]
default = []
wire = ["dep:tokio", "dep:tokio-util"]
tracing = ["dep:tracing"]

[dependencies]
```

### Tracing Span Specification (PRD R11)

When the `tracing` feature flag is enabled, mqlite emits structured tracing spans and events for observability. The following operations emit spans:

| Operation | Span Name | Key Fields | Level |
|-----------|-----------|------------|-------|
| Database open | `mqlite::open` | path, format_version | INFO |
| WAL recovery | `mqlite::wal_recovery` | frames_replayed, duration_ms | WARN |
| Collection CRUD | `mqlite::find`, `mqlite::insert`, etc. | collection, filter_hash, doc_count | DEBUG |
| Index scan | `mqlite::index_scan` | index_name, bounds, docs_examined | DEBUG |
| Collection scan | `mqlite::collection_scan` | collection, docs_examined | DEBUG |
| WAL checkpoint | `mqlite::checkpoint` | pages_copied, duration_ms, wal_size_before | INFO |
| Buffer pool eviction | `mqlite::eviction` | pages_evicted, dirty_pages_flushed | DEBUG |
| Writer lock acquisition | `mqlite::writer_lock` | wait_duration_ms, acquired | DEBUG |
| Wire protocol command | `mqlite::wire::command` | command_name, duration_ms, ok | DEBUG |

Events (not spans):
- `mqlite::disk_full` (ERROR): when ENOSPC encountered
- `mqlite::corrupt_page` (ERROR): when CRC32C mismatch detected
- `mqlite::unsupported_op` (WARN): when unsupported operator/command requested

```toml
bson = "2"
thiserror = "1"
crc32c = "0.6"               # Page checksums

# Optional
tokio = { version = "1", features = ["net", "rt-multi-thread", "io-util"], optional = true }
tokio-util = { version = "0.7", features = ["codec"], optional = true }
tracing = { version = "0.1", optional = true }
```

### Dependency Budget

| Dependency | Required | Purpose | Transitive Deps |
|-----------|----------|---------|-----------------|
| `bson` | Yes | BSON types, serialization | serde, indexmap, uuid |
| `thiserror` | Yes | Error derive macro | syn, quote (build only) |
| `crc32c` | Yes | Page/frame checksums | Minimal |
| `tokio` | wire only | Async runtime for wire shim | Large (~30 deps) |
| `tracing` | tracing only | Observability | Minimal |

Base crate without features: ~10 transitive dependencies. With `wire`: ~40. This is acceptable for a database crate.

### Pure-Rust Dependency Enforcement (PRD Constraint)

The PRD requires no C/C++ code in mqlite's direct dependency tree. Transitive dependencies that optionally have C backends must use their Rust backend. Enforcement:

1. **Cargo.toml**: Use feature flags to force pure-Rust backends. Example: if `flate2` is ever added (e.g., for OP_COMPRESSED in Phase 1.1), specify `flate2 = { version = "1", default-features = false, features = ["rust_backend"] }`.
2. **`crc32c` crate**: Verify it uses a pure-Rust implementation. The `crc32c` crate may auto-detect hardware CRC32C instructions via Rust intrinsics (not C FFI) — this is acceptable.
3. **CI check**: Add a CI job that runs `cargo build` and verifies no `.c`, `.cpp`, or `build.rs` with `cc::Build` compilation appears in the build output. Alternatively, use `cargo tree --edges=normal -f '{p} {l}'` to audit for C dependencies.
4. **Policy**: Any new dependency that introduces C code must be explicitly approved and documented as an exception, or replaced with a pure-Rust alternative.

### Cross-Compilation Targets

| Target | Priority | Notes |
|--------|----------|-------|
| x86_64-unknown-linux-gnu | P0 | Primary development/CI target |
| aarch64-unknown-linux-gnu | P0 | ARM64 Linux (Raspberry Pi 4+, cloud ARM) |
| x86_64-apple-darwin | P0 | macOS Intel |
| aarch64-apple-darwin | P0 | macOS Apple Silicon |
| x86_64-pc-windows-msvc | P1 | Windows (needs fcntl → LockFileEx adaptation) |
| armv7-unknown-linux-gnueabihf | P1 | ARM32 (Raspberry Pi 3, older IoT) |
| wasm32-wasi | P2 | WASM (future, in-memory only) |

All P0 targets must be tested in CI. P1 targets should cross-compile cleanly. P2 is aspirational.

## Testing Integration

### Test Pyramid

```
┌───────────────────────────────────────────┐
│        Compatibility Tests (top)          │  pymongo suite, mongosh smoke tests
│      Run against real MongoDB 8.0 and     │  Compare results for parity
│           mqlite wire protocol            │
├───────────────────────────────────────────┤
│         Integration Tests (middle)        │  Multi-layer tests: API → storage
│      Cursor lifecycle, index+query,       │  Wire protocol end-to-end
│      WAL recovery, checkpoint cycle       │
├───────────────────────────────────────────┤
│          Unit Tests (base)                │  Per-module: B+ tree ops, BSON
│      Key encoding, page format,           │  encoding, WAL frame parse,
│      buffer pool eviction, query eval     │  command dispatch
├───────────────────────────────────────────┤
│       Property + Fuzz Tests (cross-cut)   │  B+ tree invariants, BSON parse,
│      Crash injection, wire protocol       │  random fault injection
│             fuzzing                       │
└───────────────────────────────────────────┘
```

### MongoDB CRUD Spec Tests

MongoDB publishes CRUD spec tests as YAML/JSON files. These define expected behavior for each CRUD operation with various inputs. mqlite should:

1. Import a subset of CRUD spec tests relevant to Phase 1 operators.
2. Run each test against mqlite's native API.
3. Run each test against mqlite's wire protocol via pymongo.
4. Run the same tests against real MongoDB 8.0 as a reference.
5. Compare results. Any divergence is a compatibility bug.

### Jepsen-Style Crash Testing

Per Q5, crash testing is required. Implementation:

1. **Crash injector**: Fork a child process running mqlite writes. Send SIGKILL at random intervals.
2. **Validator**: Open the database in the parent process. Verify: (a) database opens without error, (b) WAL replays successfully, (c) all committed data is present, (d) no uncommitted data leaks through, (e) all indexes are consistent with data.
3. **Fuzzer variations**: Kill during insert, update, delete, checkpoint, index build. Kill at various WAL sizes. Kill during multi-page overflow writes.
4. **Automation**: Run thousands of crash cycles in CI. Any failure is a P0 bug.

### Fuzz Testing

| Target | Fuzzer Input | Goal |
|--------|-------------|------|
| BSON parser | Random bytes | No panics, no memory safety violations |
| Wire protocol parser | Random OP_MSG frames | No panics, clean error responses |
| Query filter evaluator | Random BSON filter documents | No panics, correct match/no-match |
| Key encoder | Random BSON values | Encoding preserves comparison ordering |

Use `cargo-fuzz` with `libfuzzer`. Run continuously in CI with a corpus of known-good inputs.

## Deployment Integration

### Embedded in Rust Applications

Primary use case. No special deployment considerations — mqlite is a library:

```toml
[dependencies]
mqlite = "0.1"
```

The .mqlite file is created in the application's chosen directory. No daemon, no port, no configuration file.

### Wire Protocol in Containers

When the `wire` feature is enabled and the wire protocol shim is started:
- Default bind: `127.0.0.1:27017`
- In containers, `127.0.0.1` is container-local (safe).
- To expose to other containers in a pod: bind to `0.0.0.0` (requires explicit opt-in, with warning).
- No authentication in Phase 1 — document this prominently.

### File Management for Operators

| Task | Method | Notes |
|------|--------|-------|
| Backup (cold) | `cp data.mqlite backup.mqlite` | Only safe when DB is closed |
| Backup (hot) | `db.backup("backup.mqlite")` | API acquires snapshot, copies consistently |
| Backup (checkpoint) | `db.checkpoint()` then `cp` | Safe after checkpoint if no concurrent writes |
| Monitor size | `db.stats().file_size` | Track growth, alert before disk full |
| Shrink | `db.compact()` | Reclaims free pages, rewrites file |
| Migrate | Copy .mqlite file | Portable across same-endian platforms |

## Migration Paths

### From MongoDB Rust Driver to mqlite

| MongoDB Driver | mqlite Equivalent | Notes |
|---------------|-------------------|-------|
| `Client::with_uri_str(uri).await?` | `Database::open("data.mqlite")?` | Sync, no URI |
| `client.database("mydb")` | (implicit — one DB per file) | |
| `db.collection::<T>("coll")` | `db.collection::<T>("coll")` | Same signature |
| `coll.insert_one(doc).await?` | `coll.insert_one(&doc)?` | Sync, reference |
| `coll.find(filter).await?` | `coll.find(filter)?` | Sync, returns Iterator |
| `cursor.try_next().await?` | `cursor.next()` (Iterator) | Sync iteration |
| `coll.update_one(f, u).await?` | `coll.update_one(f, u)?` | Same semantics |

Key differences to document:
- No `await` — all operations are synchronous.
- No connection pool, no read/write concern, no sessions.
- `insert_one` takes a reference (`&doc`) not owned value.
- Cursor is a standard `Iterator`, not an async `Stream`.
- Writer contention: may return `Error::WriterBusy` (MongoDB never does).
- Unsupported operators fail explicitly.

### Data Import from MongoDB

For migrating data from MongoDB to mqlite:

1. **mongodump/BSON files**: Write a utility that reads BSON dump files and inserts into mqlite via the native API. Not a Phase 1 deliverable but straightforward to build.
2. **Via wire protocol**: Connect a migration script to MongoDB (source) and mqlite's wire protocol (destination). Read with pymongo, write with pymongo. Works with Phase 1 command set.
3. **JSON import**: Parse JSON/Extended JSON documents, convert to BSON, insert via native API.

## MQL Operator Implementation Matrix (PRD G2)

Each operator must be implemented, tested against MongoDB 8.0 for correctness, and verified via both the native API and wire protocol paths.

### Query Operators — Phase 1

| Operator | Category | Implementation Status | Test Coverage | Notes |
|----------|----------|----------------------|---------------|-------|
| `$eq` | Comparison | Required | Unit + compat | Implicit in `{ field: value }` syntax |
| `$ne` | Comparison | Required | Unit + compat | |
| `$gt` | Comparison | Required | Unit + compat | Must handle cross-type comparison per BSON ordering |
| `$gte` | Comparison | Required | Unit + compat | |
| `$lt` | Comparison | Required | Unit + compat | |
| `$lte` | Comparison | Required | Unit + compat | |
| `$in` | Comparison | Required | Unit + compat | Array of values |
| `$nin` | Comparison | Required | Unit + compat | |
| `$and` | Logical | Required | Unit + compat | Implicit (multiple conditions) and explicit |
| `$or` | Logical | Required | Unit + compat | |
| `$not` | Logical | Required | Unit + compat | |
| `$nor` | Logical | Required | Unit + compat | |
| `$exists` | Element | Required | Unit + compat | |
| `$type` | Element | Required | Unit + compat | Must support both string and numeric type identifiers |
| `$elemMatch` | Array | Required | Unit + compat | Query position only (not projection) |
| `$all` | Array | Required | Unit + compat | |
| `$size` | Array | Required | Unit + compat | |
| `$regex` | Evaluation | Required | Unit + compat | Rust `regex` crate only (no PCRE); document incompatibilities |

**Query operators — Phase 1 out-of-scope (must return error):**

The following query operators are explicitly excluded from Phase 1 (per PRD G2). When encountered in a find filter, they must return error code 9 (`FailedToParse`) with a message naming the unsupported operator and listing what IS supported:

`$expr`, `$jsonSchema`, `$mod`, `$text`, `$where`, `$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere`, `$elemMatch` (projection position), `$slice` (projection), `$meta`, `$comment`, `$rand`, `$natural`.

Of these, `$expr` deserves special attention: it allows aggregation expressions inside find filters. Since the aggregation pipeline is a PRD non-goal, `$expr` must be explicitly rejected — not silently ignored or treated as a passthrough.

### Update Operators — Phase 1

| Operator | Category | Implementation Status | Test Coverage | Notes |
|----------|----------|----------------------|---------------|-------|
| `$set` | Field | Required | Unit + compat | |
| `$unset` | Field | Required | Unit + compat | |
| `$rename` | Field | Required | Unit + compat | |
| `$inc` | Field | Required | Unit + compat | Numeric types only |
| `$min` | Field | Required | Unit + compat | Cross-type BSON comparison |
| `$max` | Field | Required | Unit + compat | Cross-type BSON comparison |
| `$mul` | Field | Required | Unit + compat | Numeric types only |
| `$currentDate` | Field | Required | Unit + compat | Date and Timestamp types |
| `$setOnInsert` | Field | Required | Unit + compat | Only applies during upsert |
| `$push` | Array | Required | Unit + compat | With modifiers: `$each`, `$position`, `$sort`, `$slice` |
| `$pull` | Array | Required | Unit + compat | Supports query expressions |
| `$addToSet` | Array | Required | Unit + compat | With `$each` modifier |
| `$pop` | Array | Required | Unit + compat | First (-1) or last (1) |
| `$pullAll` | Array | Required | Unit + compat | |

### Projection — Phase 1

| Feature | Implementation Status | Notes |
|---------|----------------------|-------|
| Field inclusion (`{ field: 1 }`) | Required | |
| Field exclusion (`{ field: 0 }`) | Required | Cannot mix inclusion/exclusion (except `_id`) |
| `_id` suppression (`{ _id: 0 }`) | Required | |

## Performance Regression Detection (PRD G7)

To satisfy PRD G7's acceptance criterion ("No operation regresses more than 2x between releases"), implement CI-based benchmark comparison:

1. **Benchmark suite**: Use `criterion` crate for statistically rigorous benchmarks covering all G7 target operations.
2. **Baseline storage**: Store benchmark results as JSON in the repository (`.benchmarks/` directory) keyed by git commit hash.
3. **CI comparison**: On each PR/MR, run benchmarks and compare against the baseline from the merge target (master). Flag any operation that regresses more than 2x.
4. **Noise tolerance**: Use criterion's statistical analysis (confidence intervals) to distinguish real regressions from measurement noise. Require 3 consecutive measurements above 2x threshold before flagging.
5. **Reference hardware**: Define a CI runner specification (CPU, SSD type, RAM) as the reference. Document that targets are specific to this hardware.

## Compatibility Testing Strategy

### Reference Implementation Comparison

For each Phase 1 command, maintain a test that:
1. Sends the same command to MongoDB 8.0 and mqlite.
2. Compares the response document structure (field names, types).
3. Compares error codes for invalid inputs.
4. Flags any divergence as a compatibility bug.

### Error Code Verification

Maintain a table of MongoDB error codes that mqlite uses:

| Code | Name | Triggered By |
|------|------|-------------|
| 9 | FailedToParse | Malformed command, unknown update operator |
| 11000 | DuplicateKey | Insert/update violates unique index |
| 22 | InvalidBSON | Malformed BSON in command |
| 26 | NamespaceNotFound | Operation on non-existent collection (when required) |
| 27 | IndexNotFound | dropIndexes on non-existent index |
| 48 | NamespaceExists | create on existing collection (when not idempotent) |
| 59 | CommandNotFound | Unsupported command |
| 10334 | BSONObjectTooLarge | Document exceeds 16MB |

Test each: send the triggering operation to both MongoDB 8.0 and mqlite, verify same code and codeName.

### BSON Round-Trip Testing

Insert a document via the native API, read it via the wire protocol (pymongo). Insert via pymongo, read via native API. Compare byte-for-byte BSON equality. This catches serialization/deserialization mismatches between the native path and wire protocol path.

## Constraints Identified

1. **directConnection=true is mandatory for all driver connections.** Without it, drivers attempt replica set topology discovery and fail. This must be documented in every connection example.

2. **bson crate version is part of the public API.** Upgrading bson is a semver-breaking change. Pin carefully.

3. **No OP_COMPRESSED in Phase 1.** Drivers that require compression will fail to connect. Most drivers negotiate and fall back to uncompressed. Verify this behavior with mongosh and pymongo.

4. **Wire protocol is unauthenticated.** Any process that can reach the port has full read/write access. Bind localhost-only by default.

5. **No sessions or transactions in wire protocol.** Drivers that assume session support (MongoDB 3.6+) may send session IDs. mqlite must ignore session-related fields gracefully, not error.

6. **Pure Rust constraint limits compression options.** zstd requires C bindings. If OP_COMPRESSED is added later, only snappy (pure Rust via `snap` crate) and zlib (pure Rust via `flate2`/`miniz_oxide`) are viable without C dependencies.

7. **pymongo test suite must be curated.** Not all pymongo tests apply — many test aggregation, change streams, transactions, etc. A curated subset covering Phase 1 operations must be selected and maintained.

8. **Windows file locking differs from POSIX.** POSIX uses `fcntl(F_SETLK)`. Windows uses `LockFileEx`. The multi-process locking layer must abstract this. Phase 1 can target POSIX-only if Windows is P1.

9. **MongoDB wire protocol version evolves.** maxWireVersion 21 is MongoDB 8.0. Future MongoDB releases may change handshake expectations. mqlite must track wire protocol changes that affect the Phase 1 command set.

## Open Questions

1. ~~**Should mqlite handle session IDs in commands gracefully?**~~ **RESOLVED**: Silently ignore `lsid` (logical session ID) fields in all commands. pymongo 4.x sends `lsid` with every command by default; rejecting it would break compatibility. Log at DEBUG level: `"Ignoring lsid field (sessions not supported)"`. Do NOT return an error.

2. ~~**How should mqlite handle `readConcern` and `writeConcern` in commands?**~~ **RESOLVED**: Accept and silently ignore. Drivers send these by default. For an embedded single-file database, there is only one copy of data, so concerns are meaningless. Log at DEBUG level: `"Ignoring readConcern/writeConcern (embedded mode)"`. Do NOT return an error. This enables MongoDB driver code to work unchanged.

3. **Should the wire protocol support `explain` command?** mongosh's `.explain()` sends an `explain` wrapper command. This is useful for debugging but adds implementation scope. Recommendation: Phase 1.1.

4. **What pymongo version is the compatibility target?** pymongo 4.x has different behavior from 3.x (sessions, retryable writes). Pin to pymongo 4.x for testing.

5. **Should mqlite provide a compatibility test harness as a developer tool?** A `mqlite-compat-test` binary that runs the curated test suite against a running mqlite instance would help contributors verify compatibility. This is developer tooling, not user-facing.

6. ~~**How does mqlite handle `$db` field in OP_MSG?**~~ **RESOLVED**: Validate `$db` and error on mismatch. mqlite is single-database (one file = one database). When `$db` in a command does not match the opened database name, return error code 13 (`Unauthorized`) with message: `"Database 'X' does not match opened database 'Y'. mqlite is single-database."` This catches configuration bugs (e.g., pymongo client using wrong database name) early rather than silently operating on a different-than-expected database.

7. ~~**Should the wire protocol support cursor pinning for getMore?**~~ **RESOLVED**: Yes, enforce cursor pinning. `getMore` must be sent on the same TCP connection that created the cursor via `find`. If `getMore` is sent on a different connection, return error code 43 (`CursorNotFound`). This matches MongoDB behavior and is required for correctness — cursors hold snapshot state tied to the originating connection. The wire protocol shim tracks cursor ownership per connection.

8. **What is the testing strategy for cross-platform file format compatibility?** A .mqlite file created on x86_64 Linux should be readable on aarch64 macOS. This requires consistent byte ordering (BSON is little-endian, page headers should be little-endian). Add a CI job that creates a file on one platform and reads it on another.

## Integration Points

### -> API Design
- Wire protocol command handlers map 1:1 to native API methods
- Error types are serialized to MongoDB wire protocol error format
- `Collection<T>` serde integration depends on bson crate re-export
- `Database::open()` configuration flows through to all integration surfaces

### -> Data Model
- BSON documents flow unchanged between wire protocol, native API, and storage
- ObjectId generation is shared across all integration paths
- Document size limits (16MB) are enforced at the BSON layer before reaching storage
- Index metadata from catalog is exposed via listIndexes command

### -> Security
- Wire protocol binds localhost-only by default (security integration)
- No authentication means every integration path has full access
- BSON validation (depth, size limits) protects against malicious wire protocol input
- OP_MSG size limits prevent memory exhaustion from oversized messages

### -> Scalability
- Wire protocol connections consume reader slots (bounded by max_readers)
- Cursor idle timeout prevents resource leaks from abandoned wire protocol clients
- OP_COMPRESSED (when added) trades CPU for bandwidth — relevant for non-localhost deployments
- Connection count limits prevent file descriptor exhaustion
