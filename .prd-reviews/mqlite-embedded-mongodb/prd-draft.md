# PRD: mqlite — Embedded, File-Based, MongoDB-Compatible Database

## Problem Statement

Application developers who use MongoDB's document model and query language (MQL) face a binary choice: run a full MongoDB server (heavy, operational burden, requires infrastructure) or abandon the MongoDB API entirely for something lighter. There is no embedded, zero-dependency, single-file alternative — the way SQLite serves as a lightweight drop-in for relational use cases.

**Who this is for:**
- Rust application developers who want document-oriented storage without running a database server
- Tool and CLI authors who need embedded persistence with a familiar query model
- Edge/IoT deployments where a full MongoDB instance is impractical
- Test/dev environments that want MongoDB API compatibility without infrastructure
- Applications that want to migrate between embedded and server MongoDB with minimal code changes

**Why now:**
- Rust's ecosystem is mature enough for a production-quality storage engine (stable async, mature B-tree crate landscape)
- MongoDB's wire protocol and MQL semantics are well-documented and stable
- SQLite's success has proven the "embedded database as a file" model is wildly useful — no equivalent exists for document databases
- Growing demand for local-first and edge computing where server-based databases are impractical

## Goals

Each goal has a measurable acceptance criterion. Phase 1 is complete when all criteria are met.

### G1: Single-file storage

One `.mqlite` file per database when cleanly closed. During operation, up to two auxiliary files (`.mqlite-wal` and `.mqlite-shm`) may exist for WAL and shared memory, following SQLite's model. On clean close (`Database::close()` or `Drop`), the WAL is checkpointed and auxiliary files are deleted, leaving a single `.mqlite` file.

**Acceptance criteria:**
- After `Database::open()` and `Drop` with no intervening writes, only one file exists on disk.
- After a write + close cycle, only the `.mqlite` file remains.
- Copying the single `.mqlite` file (when cleanly closed) to another location produces a valid, openable database.
- `Database::checkpoint()` forces WAL merge; calling it before backup ensures a safe cold copy.
- `Database::backup(dest)` produces a consistent copy even while the database is open.

### G2: MongoDB API compatibility (MongoDB 8.0 target)

Support a defined set of MQL query and update operators so existing MongoDB mental models and code transfer directly. The compatibility target is **MongoDB 8.0**. When an unsupported operation is encountered, mqlite returns a proper MongoDB error code — never silent success.

**MQL query operators — Phase 1 in-scope:**

| Category | Operators |
|----------|-----------|
| Comparison | `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin` |
| Logical | `$and`, `$or`, `$not`, `$nor` |
| Element | `$exists`, `$type` |
| Array | `$elemMatch` (query position), `$all`, `$size` |
| Evaluation | `$regex` |

**MQL query operators — Phase 1 out-of-scope:**

`$expr`, `$jsonSchema`, `$mod`, `$text`, `$where`, `$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere`, `$elemMatch` (projection position), `$slice` (projection), `$meta`, `$comment`, `$rand`, `$natural`.

**MQL update operators — Phase 1 in-scope:**

| Category | Operators |
|----------|-----------|
| Field | `$set`, `$unset`, `$rename`, `$inc`, `$min`, `$max`, `$mul`, `$currentDate`, `$setOnInsert` |
| Array | `$push`, `$pull`, `$addToSet`, `$pop`, `$pullAll` |
| Array modifiers | `$each`, `$position`, `$sort`, `$slice` (as `$push` modifiers) |

**MQL update operators — Phase 1 out-of-scope:**

`$bit`, `$[<identifier>]` (arrayFilters), `$[]` (all positional), positional `$` operator.

**Projection — Phase 1 in-scope:**

Field inclusion/exclusion (`{ field: 1 }`, `{ field: 0 }`), `_id` suppression (`{ _id: 0 }`).

**Projection — Phase 1 out-of-scope:**

`$elemMatch` projection, `$slice` projection, `$meta` projection, expression projections.

**Sort:** `.sort()` on `find` queries is supported. Single-field and compound sort by indexed or non-indexed fields. Collation is deferred to Phase 2.

**Unsupported operation behavior:** All unsupported operators and commands return an explicit error with the appropriate MongoDB 8.0 error code. For unknown commands: error code 59 (`CommandNotFound`). For unsupported operators within a supported command: error code 9 (`FailedToParse`) with a message naming the specific unsupported operator and listing what IS supported.

**Acceptance criteria:**
- All in-scope query operators produce results matching MongoDB 8.0 for the same input documents and filters.
- All in-scope update operators produce the same document mutations as MongoDB 8.0.
- Unsupported operators return the correct MongoDB error code — never silent success or partial results.
- A compatibility test suite runs the same operations against MongoDB 8.0 and mqlite, comparing results document-by-document with BSON field ordering tolerance.
- BSON type comparison ordering in indexes matches MongoDB 8.0's ordering: MinKey < Null < Numbers < Symbol < String < Object < Array < BinData < ObjectId < Boolean < Date < Timestamp < RegExp < MaxKey.

### G3: Zero-server operation

Embed directly into Rust applications as a library. No daemon, no port, no process management for the core database. The wire protocol shim is an optional feature (`wire` feature flag) that the application explicitly starts.

**API shape:** Sync-first. `Database::open("path.mqlite")` is the entry point. The base `mqlite` crate has zero async runtime dependencies. The `wire` feature flag pulls tokio for the wire protocol shim only.

**Thread safety:** `Database` and `Collection<T>` are `Send + Sync + Clone` (cheap, `Arc<Inner>` internally). `Cursor<T>` is `Send` but not `Sync` (single-threaded iteration, can be moved between threads).

**Acceptance criteria:**
- `mqlite` compiles with default features and no async runtime in the dependency tree.
- `Database::open()`, CRUD operations, and index management work without any background threads or services started by the caller.
- `Database` can be shared across threads via `Arc` or `Clone` and used concurrently from multiple reader threads + one writer thread.

### G4: Crash recovery

WAL-based durability with configurable guarantees. The database survives process crashes (`kill -9`) and power loss without corruption. Committed data in the WAL is always recoverable.

**Durability modes (configurable via `OpenOptions::durability()`):**

| Mode | Guarantee | Performance | Use case |
|------|-----------|-------------|----------|
| `FullSync` | Durable after API call returns (fsync per commit) | 500-2,000 writes/s (SSD) | Financial data, sensors |
| `Interval(Duration)` (default: 100ms) | Durable after next periodic WAL flush. Loss window = configured interval. | 10,000-50,000 writes/s | General applications |
| `None` | No durability. All data lost on crash. | 100,000+ writes/s | In-memory mode, testing |

**Default:** `Interval(100ms)` — at most 100ms of committed writes can be lost on crash. This is the recommended default for most applications.

**Acceptance criteria:**
- After `insert_one()` returns `Ok` in `FullSync` mode, `kill -9` the process. Reopen. The inserted document is present.
- After `insert_one()` returns `Ok` in `Interval(100ms)` mode, wait 200ms, `kill -9`. Reopen. The inserted document is present.
- After crash during a write (mid-WAL-append), reopen succeeds. The database contains only complete, committed transactions. No partial documents or corrupt pages.
- WAL replay on recovery is automatic — no manual intervention or repair tool required.
- CRC32C checksums detect torn pages from power loss. Torn WAL frames after the last commit marker are discarded.

### G5: Concurrent read access

Multiple readers operate simultaneously alongside a single writer (SWMR model), using WAL-based snapshot isolation. Readers never block writers. Writers never block readers.

**Concurrency semantics:**
- **Isolation level:** Snapshot isolation at cursor open time. A reader sees the database as of the last completed write transaction at the moment the cursor was created.
- **Writer contention:** When a second writer tries to acquire the lock, it blocks until the lock is available, with a configurable timeout (default 5 seconds). On timeout, returns `Error::WriterBusy`.
- **Multi-process access:** Supported via POSIX `fcntl(F_SETLK)` file locking. Two separate OS processes can safely open the same `.mqlite` file (one writer, many readers). Network filesystems (NFS, SMB) are unsupported.
- **Reader limit:** Up to 64 concurrent readers (configurable, max 256). Each reader holds a snapshot reference. Long-running readers prevent WAL truncation past their snapshot point.

**Acceptance criteria:**
- Start 10 reader threads and 1 writer thread. Writer inserts 10,000 documents. Readers perform concurrent find queries. No errors, no data races, no corruption.
- Two separate OS processes open the same file. One writes, one reads. Both operate correctly without corruption.
- A second writer attempting to acquire the lock blocks for up to `busy_timeout` and returns `Error::WriterBusy` on timeout.
- A reader's cursor sees a consistent snapshot even while the writer is actively modifying documents.

### G6: Wire protocol shim (Phase 1, feature-gated)

Optional OP_MSG-compatible layer so existing MongoDB drivers and tools can connect for debugging, migration, and interop. Enabled via the `wire` Cargo feature flag. Binds to `127.0.0.1` only by default (no authentication in Phase 1).

**Phase 1 command surface (18 commands):**

| Command | Category |
|---------|----------|
| `hello`, `isMaster` | Handshake |
| `ping`, `buildInfo`, `serverStatus` | Diagnostic |
| `listDatabases` | Admin |
| `insert`, `find`, `update`, `delete`, `findAndModify` | CRUD |
| `getMore`, `killCursors` | Cursor management |
| `create`, `drop`, `listCollections` | Collection management |
| `createIndexes`, `dropIndexes`, `listIndexes` | Index management |

**Wire protocol reports** `maxWireVersion: 21` (MongoDB 8.0) with `mqlite.version` in the hello response. Does NOT advertise sessions, transactions, change streams, or authentication mechanisms. `directConnection=true` is required for all client connections.

**Acceptance criteria:**
- `mongosh` (2.x) connects, runs `show dbs`, `show collections`, `db.collection.insertOne()`, `db.collection.find()`, `db.collection.updateOne()`, `db.collection.deleteOne()`.
- A non-trivial pymongo (4.x) test suite passes: insert, find, update, delete with indexes through the wire protocol.
- Unsupported commands (e.g., `aggregate`) return error code 59 (`CommandNotFound`).
- The shim binds only to `127.0.0.1` by default. Binding to `0.0.0.0` requires explicit opt-in.

### G7: Reasonable performance

Competitive with SQLite for comparable workloads, with concrete targets on reference hardware (SSD, 64MB buffer pool).

**Phase 1 performance targets:**

| Operation | Target | Notes |
|-----------|--------|-------|
| Point lookup by `_id` (cached) | < 10 us | Buffer pool hit |
| Point lookup by `_id` (uncached, SSD) | < 1 ms | One leaf page read |
| Indexed range scan (100 docs, cached) | < 5 ms | Sequential leaf reads |
| Single doc insert (`Interval` mode) | < 100 us | WAL append |
| Single doc insert (`FullSync` mode) | < 2 ms (SSD) | Dominated by fsync |
| Bulk insert 10K docs (`Interval`) | < 500 ms | Batched WAL writes |
| Index creation (100K docs) | < 5 s | Full collection scan + sort |

These are order-of-magnitude targets. "Competitive with SQLite" means within 5x for comparable operations. Benchmarks run on the same hardware against SQLite in WAL mode.

**Acceptance criteria:**
- A benchmark suite exists that measures all operations in the table above.
- Phase 1 release meets the stated targets on reference hardware (SSD, 64MB buffer pool, 1KB average document size).
- No operation regresses more than 2x between releases (regression detection in CI).

## Non-Goals

These are explicitly out of scope for Phase 1:

- **Replication / sharding**: No replica sets, no config servers, no distributed operation
- **Aggregation pipeline**: No `$group`, `$lookup`, `$unwind`, `$project` (aggregation), etc. — core CRUD only
- **Change streams**: No real-time event notification on data changes
- **Multi-document transactions**: Single-document atomicity only; no cross-document ACID transactions
- **Full MongoDB feature parity**: Not trying to implement every MongoDB feature — targeting the defined operator set above
- **Non-Rust language bindings**: Phase 1 is Rust-native; FFI/C bindings and language wrappers come later (non-Rust consumers can use the wire protocol shim)
- **Server mode**: The wire protocol shim is for compatibility/debugging, not for running as a production server replacement
- **Full-text search**: No `$text` or Atlas Search equivalent
- **Geospatial queries**: No `$geoWithin`, `$near`, etc.
- **Collation**: No locale-aware string ordering in Phase 1
- **Authentication / encryption at rest**: Phase 1 wire protocol is unauthenticated, data is plaintext on disk
- **no_std / WASM**: Phase 1 targets `std` Rust only

**Phase 2 candidates** (acknowledged, not committed):

Aggregation pipeline (`$group`, `$project`), `$lookup`/joins, Python/Node bindings via FFI, TTL indexes, unique indexes with partial filters, OP_COMPRESSED, async API wrapper, collation, `mongodump`/`mongorestore` compatibility, encryption at rest.

## User Stories / Scenarios

### Story 1: Embedded application storage
A Rust CLI tool needs to store structured configuration and user data. Developer adds `mqlite` as a cargo dependency, opens a `.mqlite` file, and does `insert_one`, `find`, `update_one` with familiar MQL semantics. No server to start, no Docker compose, no connection strings.

### Story 2: Test fixture database
A team uses MongoDB in production. For unit tests, they swap the connection to an in-memory mqlite instance (`Database::open_in_memory()`). Tests run in milliseconds with the same query logic, no MongoDB container needed. When a test uses an unsupported operator, mqlite returns an explicit error (not silent success), alerting the team to a compatibility gap.

### Story 3: MongoDB driver interop via wire protocol
A developer wants to inspect their mqlite database using `mongosh` or Compass. They enable the wire protocol shim on a local port (`--features wire`), connect with standard MongoDB tools using `directConnection=true`, and browse/query their data.

### Story 4: Edge/IoT data collection
A sensor gateway collects readings into a local mqlite database with `Interval(1s)` durability (acceptable 1s loss window for sensor data). When connectivity is available, a sync process reads documents and pushes them to a cloud MongoDB instance. The query model is identical on both sides.

### Story 5: Data migration tool
A migration utility reads from one mqlite file and writes to another, applying transformations. For safe copies of an active database, the utility calls `db.backup(dest)` for a hot backup, or `db.checkpoint()` followed by file copy for a cold backup.

## Requirements

### R1: Error model

mqlite defines a structured error taxonomy with MongoDB error code compatibility where applicable.

**MongoDB-compatible errors (with error codes):**
- Duplicate key violation: code 11000
- Document validation failure: code 121
- Index not found: code 27
- Namespace not found: code 26
- Namespace exists: code 48
- Command not found / unsupported: code 59
- Invalid BSON: code 22
- Document exceeds 16MB: code 10334

**mqlite-specific errors:**
- `WriterBusy`: writer lock timeout (no MongoDB equivalent — embedded-only concern)
- `CorruptDatabase`: file corruption detected (includes whether recovery is possible)
- `DiskFull`: write failed due to ENOSPC (includes path, required bytes, available bytes)
- `UnsupportedOperator`: names the specific unsupported operator and lists what IS supported

**Every error that has a MongoDB equivalent uses the same numeric code.** This enables error-handling code written for MongoDB to work unchanged.

### R2: Document validation

On insert and update:
1. **Well-formedness**: BSON must parse without errors.
2. **Size limit**: Maximum 16MB (16,777,216 bytes) after serialization. Error code 10334.
3. **Nesting depth**: Maximum 100 levels.
4. **`_id` field**: If absent on insert, auto-generate a MongoDB-compatible ObjectId (4-byte timestamp + 5-byte random + 3-byte counter). If present, enforce uniqueness via the `_id` index. Duplicate `_id` returns error code 11000.
5. **`_id` immutability**: Updates cannot modify the `_id` field.

### R3: Index support

Phase 1 supports the following index types:
- **Auto `_id` index**: Created automatically for every collection. Unique. Cannot be dropped. This IS the primary data store (clustered index — documents stored in `_id` order).
- **Single-field indexes**: User-created indexes on a single field. Ascending or descending.
- **Compound indexes**: Multi-field indexes with per-field sort direction. Supports prefix queries.
- **Multikey indexes**: Automatic when an indexed field contains an array. Required for `$elemMatch`, `$all`, `$size` to use indexes.
- **Unique indexes**: `IndexOptions { unique: true }`. Enforced on insert and update.
- **Sparse indexes**: `IndexOptions { sparse: true }`. Omit documents where the indexed field is missing.

**Out of scope for Phase 1:** TTL indexes, text indexes, geospatial indexes, partial indexes (with filter expressions), hashed indexes.

### R4: Storage architecture

- **Variable page B+ tree**: 4KB internal nodes (navigation), 32KB leaf nodes (document data), 32KB overflow pages (for documents exceeding leaf capacity).
- **BSON stored as-is**: Documents stored as serialized BSON. No intermediate format.
- **Catalog**: Reserved B+ tree at a fixed file header location. Maps collection names to root pages and index metadata. Checksummed for corruption detection.
- **Buffer pool**: Configurable size (default 64MB). CLOCK-sweep eviction. Separate pools for 4KB and 32KB pages (default ratio 25%/75%).
- **WAL**: Page-level redo log. CRC32C checksums per frame. Auto-checkpoint at configurable threshold (default 1000 pages). Forced checkpoint at configurable WAL max size (default 100MB).
- **`bson` crate**: Use the official `bson` crate from the MongoDB Rust driver. Re-export so users don't need a separate dependency.
- **ObjectId generation**: MongoDB-compatible format (4-byte timestamp + 5-byte random + 3-byte counter).

### R5: Resource limits

| Resource | Default | Min | Max | Configurable? |
|----------|---------|-----|-----|---------------|
| Buffer pool size | 64 MB | 512 KB | Unbounded | Yes (`OpenOptions`) |
| Max concurrent readers | 64 | 1 | 256 | Yes (`OpenOptions`) |
| WAL auto-checkpoint | 1000 pages | 100 pages | Unbounded | Yes (`OpenOptions`) |
| WAL max size | 100 MB | 10 MB | Unbounded | Yes (`OpenOptions`) |
| Writer lock timeout | 5 seconds | 0 (immediate fail) | Unbounded | Yes (`OpenOptions`) |
| Max document size | 16 MB | — | 16 MB | No (MongoDB compat) |
| Max nesting depth | 100 | — | 100 | No (safety limit) |
| Cursor batch size | 101 | 1 | 10,000 | Yes (`FindOptions`) |
| Wire protocol cursor idle timeout | 10 minutes | 10s | Unbounded | Yes (wire config) |
| Max active cursors | 1,000 | 10 | Unbounded | Yes |

### R6: Disk-full behavior

When a write (WAL append, checkpoint, or file extension) fails due to ENOSPC:
1. Return `Error::DiskFull` with the file path, required bytes, and available bytes.
2. The database remains readable. No corruption occurs.
3. The application can delete data, free disk space, and retry writes.
4. If the WAL has pages that overwrite existing main file pages, checkpoint can reclaim WAL space without growing the main file.

### R7: Platform targets (Phase 1)

- Linux x86_64
- macOS ARM64 (Apple Silicon)
- Linux aarch64 (ARM64, for edge/IoT)

Windows and WASM are Phase 2 candidates. Network filesystems (NFS, SMB) are unsupported.

### R8: In-memory mode

`Database::open_in_memory()` creates a database with no file backing. No durability, no WAL, no auxiliary files. Same API, same concurrency model. Suitable for testing (Story 2) and ephemeral data.

### R9: File format versioning

The `.mqlite` file starts with magic bytes `"MQLT"` (0x4D514C54) followed by a uint32 format version (starting at 1). Backward compatibility within a major version is required. Forward compatibility (newer files opened by older library) is a non-goal and should return a clear error.

**File format version changes constitute semver-breaking changes** (require mqlite major version bump) if they are not backward-compatible.

### R10: insert_many ordered/unordered semantics

`insert_many` supports `ordered: true` (default) and `ordered: false`:
- **Ordered**: Stop at the first error. Report the error plus a list of successfully inserted documents.
- **Unordered**: Attempt all inserts. Report all errors and all successes.

Matches MongoDB 8.0 behavior.

### R11: Observability (Phase 1 minimum)

- `Database::stats()` returns: file size, WAL size, buffer pool hit rate, page counts, checkpoint count.
- `Collection::stats()` returns: document count, average document size, index count, data size.
- `Cursor::explain()` returns the query plan (which index selected, scan type, estimated cost).
- Optional `tracing` feature flag integrates with the Rust `tracing` crate for structured logging.

### R12: Test strategy

Phase 1 requires the following test infrastructure:
- **Unit tests per layer**: Storage engine, query engine, wire protocol (independently testable).
- **Compatibility test suite**: Run identical operations against MongoDB 8.0 and mqlite, compare results.
- **Crash testing**: Deterministic fault injection or kill-at-random-point testing for WAL correctness.
- **Property-based testing**: B+ tree invariants (ordering, balance, parent-child consistency) via `proptest` or similar.
- **Fuzz testing**: BSON parser and wire protocol frame parser fuzz targets.

## Constraints

### Technical
- **Rust-only implementation**: No C/C++ dependencies in the base crate. "No C dependencies" means no C/C++ code in mqlite's direct dependency tree. Transitive pure-Rust dependencies that optionally have C backends (e.g., `flate2`) must use their Rust backend.
- **Single-writer / multiple-reader concurrency**: WAL-based, modeled on SQLite's approach. Multi-process via POSIX fcntl file locking.
- **File format stability**: The `.mqlite` file format is versioned from day one. Backward compatibility within a major version is required.
- **Page-based storage**: Variable-page B+ trees — 4KB internal nodes, 32KB leaf pages, 32KB overflow pages for large documents.
- **BSON document model**: Documents stored as BSON using the official `bson` crate. Max document size 16MB.

### Architecture (from design spec)
The system is a 5-layer stack:
1. **Wire Protocol Shim** (top, feature-gated) — OP_MSG framing, hello handshake, 18 Phase 1 commands, reports as standalone with mqlite version
2. **Native Rust API** — `Database::open()`, `Collection<T>` CRUD, index management, sync-first
3. **Query Engine** — MQL parser for defined operator set, heuristic query planner (select most selective single index, fall back to collection scan), cursor-based iteration
4. **Storage Engine** — B+ tree, page manager (4KB/32KB), WAL, buffer pool (CLOCK-sweep), SWMR concurrency
5. **File** (bottom) — single `.mqlite` file + transient `.mqlite-wal` and `.mqlite-shm`

### Resource
- Greenfield Rust project — no existing code to inherit
- Must build from storage engine up; cannot use an existing embedded database as a backend

### Business / Adoption
- Must be Apache 2.0 or MIT licensed
- API surface should feel immediately familiar to MongoDB Rust driver users: same method names (`find`, `insert_one`, etc.), same conceptual types (`Database`, `Collection<T>`, `Cursor<T>`), but sync signatures and embedded-appropriate defaults
- Documentation must include migration guide from MongoDB driver to mqlite
- **Trademark**: Use "mqlite" and "MQL-compatible" in all contexts. Do not claim to be "MongoDB" or report as "mongod" in wire protocol responses. Hello response includes `mqlite.version` field.

## Open Questions

Reduced from 11 to 4 — the remaining questions are implementation-level decisions that can be resolved during development:

1. **WAL checkpointing: synchronous or asynchronous?** Synchronous is simpler (inline after write crosses threshold). Asynchronous is better for write latency. Recommend synchronous for Phase 1.
2. **Should `Database` implement `Drop` with WAL flush?** Recommend: `Drop` does non-blocking close. Explicit `db.close()` for blocking flush. Document the difference.
3. **Should OP_COMPRESSED be Phase 1?** For localhost-only debugging, compression overhead may exceed benefit. Recommend: defer to Phase 1.1.
4. **Should compound indexes be Phase 1 MVP or Phase 1.1?** They add significant complexity. Single-field + auto `_id` may be sufficient for initial release. Current answer: Phase 1 (per design spec), but can be descoped if it jeopardizes delivery.

## Phase 1 Definition of Done

Phase 1 is complete when ALL of the following are true:

1. **Storage engine**: Open, close, crash recovery (kill -9 survives), WAL checkpoint, single-file-when-closed. CRC32C checksums pass on all pages.
2. **CRUD**: `insert_one`, `insert_many`, `find_one`, `find` (with filter, sort, limit, skip, projection), `update_one`, `update_many`, `delete_one`, `delete_many`, `find_one_and_update`, `find_one_and_delete`, `find_one_and_replace` — all working through the native API.
3. **Query operators**: All in-scope operators listed in G2 produce correct results verified against MongoDB 8.0.
4. **Indexes**: Auto `_id`, single-field, compound, multikey, unique, sparse — all create, drop, and are used by the query planner.
5. **Wire protocol**: 18 commands listed in G6 work. `mongosh` connects and runs basic CRUD. A pymongo test suite with insert/find/update/delete and indexes passes.
6. **Crash recovery**: WAL replay after kill -9 recovers all committed data. No data corruption. Verified by automated crash tests.
7. **Concurrency**: SWMR works. Multiple reader threads + one writer thread operate without errors. Multi-process access works.
8. **Performance**: Benchmark suite exists. Targets from G7 are met on reference hardware.
9. **In-memory mode**: `Database::open_in_memory()` works with the same API.
10. **Documentation**: API reference, migration guide from MongoDB driver, known limitations, security advisory for wire protocol.

## Rough Approach

### Phase 1 delivery (core embedded database)

**Bottom-up build order** — each layer builds on the one below:

0. **Project scaffolding and test infrastructure** *(before any implementation)*:
   - Cargo workspace: `mqlite` crate, feature flags `wire` and `tracing`, `rust-version = "1.70"`, `license = "MIT OR Apache-2.0"`
   - Module structure per ux.md: `database`, `collection`, `cursor`, `error`, `options`, `index`, `results`, `wire/` (feature-gated)
   - Clippy: `#![deny(clippy::unwrap_used, clippy::expect_used)]` in library code
   - CI: cross-compile jobs (x86_64-linux, aarch64-linux, aarch64-darwin), `cargo audit`, pure-Rust dependency enforcement
   - Dev dependencies: `proptest` (B+ tree property tests), `criterion` (benchmarks), `cargo-fuzz` (fuzz targets)
   - Fuzz targets scaffolded: BSON parser, OP_MSG frame parser
   - Error taxonomy implemented from api.md (`src/error.rs`) before any layer is built
   - Public API types scaffolded as stubs: `Database`, `Collection<T>`, `Cursor<T>`, result types, options types

1. **File format and page manager**: Define the `.mqlite` file header (MQLT magic, format version, page sizes, catalog root, free lists, CRC32C). Page allocator with two size classes (4KB, 32KB). **In-memory mode support**: the page manager must support `PageManagerMode::File(path)` and `PageManagerMode::InMemory` (allocates pages from `Vec<Vec<u8>>`). Set file permissions to 0600 on creation.

2. **WAL implementation**: Write-ahead log for crash recovery and SWMR concurrency. Page-level redo log with CRC32C per frame. Three-file model (main + WAL + SHM) collapsing to single file on clean close. Multi-process coordination via POSIX fcntl. **In-memory mode**: WAL and SHM are completely bypassed; writes go directly to the in-memory page store. **SHM clarification**: the SHM WAL index is a fixed-size mmap'd memory region, separate from the data buffer pool (which is built in Step 3b).

3a. **BSON key encoding** *(must complete before 3b/3c)*: Implement MongoDB BSON comparison ordering as byte-comparable key encoding for all 14 type categories. Unit tests for every type tag and edge cases (NaN, -0.0, Decimal128, cross-type comparison). Compound index key concatenation with per-field inversion for descending sort. Property test: for any two BSON values where A < B by MongoDB ordering, `encode(A) < encode(B)` by memcmp.

3b. **Buffer pool**: CLOCK-sweep eviction, separate pools for 4KB (internal nodes) and 32KB (leaf/overflow) pages, pin/unpin with dirty tracking. Available to WAL reader path (Step 2) and B+ tree (Step 3c).

3c. **B+ tree storage engine**: Variable-page B+ tree using key encoding from 3a and buffer pool from 3b. Overflow pages for large documents. Property-based tests for all 8 tree invariants from data.md written concurrent with implementation.

4. **Collection and index management**: Catalog B+ tree mapping collection names to root pages. Define catalog schema as a data contract before implementation (collection entry format, index entry format). Auto `_id` index (clustered — documents stored in `_id` order). Secondary indexes with `_id` appended for uniqueness. Multikey indexes for array fields.

5. **Query engine**: MQL operator implementation for the defined Phase 1 operator set. Heuristic planner: select most selective single index for the query shape, fall back to collection scan. Cursor-based iteration with snapshot isolation.

6. **Native Rust API**: `Database::open()`, `Collection<T>` with serde support, all CRUD methods (including `close()`), index management. Sync-first. Error model with MongoDB error code mapping. `Database::open_in_memory()` for testing. After Step 6: run Phase A native API compatibility tests.

7. **Wire protocol shim** (behind `wire` feature flag): OP_MSG framing, hello/isMaster handshake, 18 Phase 1 commands. Localhost-only binding with startup security warning. Internal tokio runtime via `spawn_blocking` to call sync native API. Silently ignore `lsid`, `readConcern`, `writeConcern` in all commands. After Step 7: run Phase B wire protocol compatibility tests (mongosh, pymongo).

### Sub-phases (if needed for timeline management)
- **0**: Scaffolding + test infrastructure
- **1a**: Storage engine (steps 1, 2, 3a, 3b, 3c) — file format, WAL, BSON key encoding, buffer pool, B+ tree
- **1b**: Query + API (steps 4-6) — catalog, query engine, native API. Phase A compat tests. Begin Tier 1 documentation.
- **1c**: Wire protocol (step 7) — OP_MSG, commands, mongosh/pymongo validation. Phase B compat tests. Complete Tier 1 documentation.
