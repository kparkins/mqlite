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

1. **Single-file storage**: One `.mqlite` file per database, just like SQLite's `.sqlite` — easy to copy, backup, version, and embed
2. **MongoDB API compatibility**: Support core MQL query and update operators so existing MongoDB mental models (and ideally code) transfer directly
3. **Zero-server operation**: Embed directly into Rust applications as a library — no daemon, no port, no process management
4. **Crash recovery**: WAL-based durability guarantees so the database survives process crashes and power loss without corruption
5. **Concurrent read access**: Multiple readers can operate simultaneously alongside a single writer (SWMR model), comparable to SQLite's WAL mode
6. **Wire protocol shim**: Optional OP_MSG-compatible layer so existing MongoDB drivers/tools can connect for debugging, migration, and interop
7. **Reasonable performance**: Competitive with SQLite for similar workloads (document insert, point query, range scan) — not targeting distributed/sharded scale

## Non-Goals

These are explicitly out of scope for Phase 1:

- **Replication / sharding**: No replica sets, no config servers, no distributed operation
- **Aggregation pipeline**: No `$group`, `$lookup`, `$unwind`, etc. — core CRUD only
- **Change streams**: No real-time event notification on data changes
- **Multi-document transactions**: Single-document atomicity only; no cross-document ACID transactions
- **Full MongoDB feature parity**: Not trying to implement every MongoDB feature — targeting the 80% of MQL that covers 95% of embedded use cases
- **Non-Rust language bindings**: Phase 1 is Rust-native; FFI/C bindings and language wrappers come later
- **Server mode**: The wire protocol shim is for compatibility/debugging, not for running as a production server replacement
- **Full-text search**: No `$text` or Atlas Search equivalent
- **Geospatial queries**: No `$geoWithin`, `$near`, etc.

## User Stories / Scenarios

### Story 1: Embedded application storage
A Rust CLI tool needs to store structured configuration and user data. Developer adds `mqlite` as a cargo dependency, opens a `.mqlite` file, and does `insert_one`, `find`, `update_one` with familiar MQL semantics. No server to start, no Docker compose, no connection strings.

### Story 2: Test fixture database
A team uses MongoDB in production. For unit tests, they swap the connection to an in-memory or temp-file mqlite instance. Tests run in milliseconds with the same query logic, no MongoDB container needed.

### Story 3: MongoDB driver interop via wire protocol
A developer wants to inspect their mqlite database using `mongosh` or Compass. They enable the wire protocol shim on a local port, connect with standard MongoDB tools, and browse/query their data.

### Story 4: Edge/IoT data collection
A sensor gateway collects readings into a local mqlite database. When connectivity is available, a sync process reads documents and pushes them to a cloud MongoDB instance. The query model is identical on both sides.

### Story 5: Data migration tool
A migration utility reads from one mqlite file and writes to another, applying transformations. The single-file model makes it trivial to handle database snapshots — just copy the file when the writer is idle.

## Constraints

### Technical
- **Rust-only implementation**: No C/C++ dependencies (pure Rust or Rust-safe FFI at most)
- **Single-writer / multiple-reader concurrency**: WAL-based, modeled on SQLite's approach — no need for complex multi-writer coordination
- **File format stability**: The `.mqlite` file format must be versioned from day one; forward compatibility is a non-goal but backward compatibility within a major version is required
- **Page-based storage**: WiredTiger-style variable page B+ trees — 4KB internal nodes, 32KB leaf pages, overflow pages for large documents
- **BSON document model**: Documents stored as BSON; max document size should match MongoDB's 16MB limit

### Architecture (from design spec)
The system is a 5-layer stack:
1. **Wire Protocol Shim** (top) — OP_MSG + OP_COMPRESSED, hello handshake, reports as standalone mongod
2. **Native Rust API** — open/close, collection CRUD, index management
3. **Query Engine** — MQL parser, heuristic query planner, cursor-based iteration
4. **Storage Engine** — B+ tree, page manager, WAL, buffer pool
5. **File** (bottom) — single `.mqlite` file on disk

### Resource
- This is a greenfield Rust project — no existing code to inherit or maintain compatibility with
- Must build from storage engine up; cannot use an existing embedded database as a backend (that would defeat the purpose)

### Business / Adoption
- Must be Apache 2.0 or MIT licensed to maximize adoption
- API surface should feel immediately familiar to anyone who has used the MongoDB Rust driver
- Documentation must include migration guide from MongoDB driver to mqlite

## Open Questions

1. **BSON library choice**: Use `bson` crate from the official MongoDB Rust driver, or implement a minimal BSON layer? The official crate brings dependencies but ensures compatibility.
2. **WAL checkpointing strategy**: When does the WAL get folded back into the main file? SQLite uses auto-checkpoint at page count thresholds — do we follow the same model?
3. **Index storage**: Store indexes in the same B+ tree file or separate structures? Same file is simpler for single-file guarantee; separate could be more performant.
4. **Memory-mapped I/O vs explicit reads**: Should the buffer pool use mmap (simpler, OS-managed) or explicit read/write (more control, portable)? SQLite moved away from mmap due to corruption risks.
5. **Max database size**: What's the practical limit? 32-bit page addresses with 4KB pages = 16TB, but do we need to support that in Phase 1?
6. **Wire protocol authentication**: The shim needs to handle the hello/handshake — does it support any auth, or is it unauthenticated-only for Phase 1?
7. **Collection metadata storage**: Where do collection definitions, index metadata, and database-level metadata live within the file?
8. **ObjectId generation**: Use MongoDB-compatible ObjectId generation, or is a simpler UUID acceptable for auto-generated `_id` fields?
9. **Query planner complexity**: The design mentions "heuristic planner" — how sophisticated? Simple index selection, or cost-based with statistics?
10. **Crash recovery guarantees**: Exactly what durability level? Write-ahead only (durable after WAL flush), or also support synchronous/fsync-per-commit modes?
11. **API async vs sync**: Should the native Rust API be async-first (tokio-based), sync-first, or dual? Embedded use cases often prefer sync for simplicity.

## Rough Approach

### Phase 1 delivery (core embedded database)

**Bottom-up build order** — each layer builds on the one below:

1. **File format and page manager**: Define the `.mqlite` file header, page layout (4KB internal, 32KB leaf, overflow), free-space management. This is the foundation everything else sits on.

2. **WAL implementation**: Write-ahead log for crash recovery and SWMR concurrency. Modeled on SQLite's WAL mode — readers see a consistent snapshot, writer appends to WAL, periodic checkpoint merges WAL back to main file.

3. **B+ tree storage engine**: Variable-page B+ tree on top of the page manager. Handles key-value storage where keys are BSON-encoded index entries and values are document data (or pointers to overflow pages for large docs).

4. **Collection and index management**: Catalog layer that maps collection names to root B+ tree pages, manages index metadata, handles auto `_id` index creation. Single-field and compound index support.

5. **Query engine**: MQL operator implementation (comparison: `$eq/$gt/$lt/etc`, logical: `$and/$or/$not`, element: `$exists/$type`, array: `$in/$all/$elemMatch`, regex: `$regex`). Update operators (`$set/$unset/$inc/$push/$pull/etc`). Heuristic planner that selects indexes based on query shape. Cursor-based result iteration.

6. **Native Rust API**: Public API surface — `Database::open()`, collection handles, `insert_one/many`, `find/find_one`, `update_one/many`, `delete_one/many`, `create_index`. Should mirror the MongoDB Rust driver's API shape where practical.

7. **Wire protocol shim** (optional, lower priority): OP_MSG framing, hello/handshake that reports as a standalone mongod, enough command support for `mongosh` basic operations (find, insert, listDatabases, listCollections).

### Key technical decisions to make early
- BSON serialization strategy (reuse vs. custom)
- Page size trade-offs (fixed vs. the proposed variable scheme)
- Buffer pool sizing and eviction policy
- Whether to support in-memory mode (no file, RAM-only) from the start
