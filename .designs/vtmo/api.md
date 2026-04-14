# API & Interface Design

## Summary

mqlite exposes two consumer interfaces: a native Rust API (primary) and a MongoDB wire protocol shim (secondary, Phase 1). The native API must balance MongoDB familiarity with embedded-database simplicity — using MongoDB naming conventions (`Collection`, `find`, `insert_one`) while rejecting server-oriented baggage (connection pools, read concerns, sessions). The entry point is `Database::open("path.mqlite")`, not `Client::connect(uri)`. The API is sync-first; the wire protocol shim manages its own async runtime behind a feature gate. Phase 1's wire protocol must support exactly 18 commands (insert, find, update, delete, findAndModify, getMore, killCursors, createIndexes, dropIndexes, listIndexes, listCollections, create, drop, ping, hello, isMaster, buildInfo, serverStatus, listDatabases) — enough for mongosh basic CRUD and pymongo acceptance tests. Unsupported commands return proper MongoDB 8.0 error codes, never silent success.

The critical API design tension is between the test-double persona (wants MongoDB driver API compatibility) and the embedded-app persona (wants minimal, zero-config simplicity). The recommended resolution is a "MongoDB-shaped, SQLite-spirited" hybrid: MongoDB method names and conceptual structure, but sync signatures, `Result<T, Error>` returns, and progressive disclosure via optional options structs. Writer contention blocks until the lock is available (with configurable timeout), matching the human's Q10 answer. Durability is configurable per Q4: `DurabilityMode::FullSync` (fsync per commit) or `DurabilityMode::Interval(Duration)` (periodic flush). The error taxonomy maps to MongoDB error codes where applicable (e.g., duplicate key = 11000) and adds mqlite-specific variants for conditions MongoDB never encounters (WriterBusy, CorruptDatabase, DiskFull).

## Analysis

### Key Considerations

- **Sync-first is non-negotiable for the embedded persona.** Forcing tokio as a base dependency signals "this is a server library." The wire protocol shim pulls its own async runtime via the `wire` feature flag. Base mqlite has zero async dependencies.
- **findAndModify is Phase 1.** This is a significant API addition — it's an atomic read-modify-write operation that requires coordination between the query engine and update engine within a single write lock acquisition.
- **MongoDB 8.0 is the compatibility target.** Error codes, command responses, and BSON type handling must match MongoDB 8.0 behavior. This is the reference implementation for compatibility testing.
- **Writer contention blocks, not fails.** Per Q10, the default behavior is to block until the writer lock is available. This differs from SQLite's default (immediate SQLITE_BUSY). A configurable `busy_timeout` provides an upper bound on blocking.
- **The `bson` crate is re-exported.** Users should not need a separate `bson` dependency in their Cargo.toml. Version mismatches between mqlite's bson and the user's bson are a predictable pain point.
- **Every unsupported operation returns a proper error code.** Per Q2, mqlite matches MongoDB 8.0's behavior for unknown/unsupported commands. This is critical for the test-double persona — silent success on unsupported operators makes tests pass that shouldn't.
- **Progressive disclosure via options structs.** `find()` works with just a filter document. `FindOptions` adds sort, limit, skip, projection, batch_size — all optional. Power users get knobs; simple cases stay simple.

### Options Explored

#### Option 1: MongoDB Driver Mirror API

- **Description**: Mirror the official `mongodb` Rust driver's API surface as closely as possible. `Client` → `Database`, same method signatures, same options structs, async-first.
- **Pros**: Maximum familiarity for existing MongoDB Rust users. Code could theoretically swap between `mongodb` and `mqlite` with minimal changes.
- **Cons**: The MongoDB driver is async-first (requires tokio). Its API models a client-server architecture (connection pools, read/write concerns, sessions, retryable writes) that mqlite does not have. Stubbing these out creates an uncanny valley. Maintaining compatibility with an evolving upstream driver is ongoing work.
- **Effort**: High — ongoing maintenance burden.

#### Option 2: SQLite-Minimal API

- **Description**: Minimal API modeled on SQLite's simplicity. `Database::open(path)` returns a handle. Raw BSON documents only, no generics, no options structs.
- **Pros**: Smallest possible API surface. Easy to learn, hard to misuse. No async dependency.
- **Cons**: Unfamiliar to MongoDB developers. No serde-based typed document mapping. Loses the "familiar MongoDB API" value proposition entirely.
- **Effort**: Low.

#### Option 3: Hybrid — MongoDB-Shaped, SQLite-Spirited (Recommended)

- **Description**: Use MongoDB naming conventions and conceptual structure (`Database`, `Collection<T>`, `find`, `insert_one`, `Cursor`) but design for embedded simplicity. Sync-first. `Database::open("path.mqlite")` entry point. Serde-based `Collection<T>` with `Collection<Document>` as the untyped default. Optional options structs with zero required fields.
- **Pros**: Familiar to MongoDB developers without being a false promise of drop-in compatibility. Simple enough for SQLite-oriented developers. Progressive disclosure. Serde support enables type-safe documents.
- **Cons**: Not a perfect mirror of either MongoDB driver or SQLite API. Documentation must explicitly address differences.
- **Effort**: Medium.

#### Option 4: Trait-Based Abstraction

- **Description**: Define a `DocumentStore` trait that both mqlite and a MongoDB adapter could implement.
- **Pros**: Maximum flexibility for test-double swapping.
- **Cons**: Premature abstraction — mqlite is a strict subset of MongoDB. The trait would be too narrow (useless for full MongoDB) or too broad (mqlite can't implement it). Community can build this later.
- **Effort**: Medium, high risk of wrong abstraction.

### Recommendation

**Option 3: Hybrid — MongoDB-Shaped, SQLite-Spirited.** Specific decisions:

1. **Entry point**: `Database::open("data.mqlite")` and `Database::open_in_memory()`. Power users use `Database::open_with_options(path, OpenOptions)`.
2. **Core types**: `Database`, `Collection<T>`, `Cursor<T>`, `Document` (re-exported from `bson`).
3. **CRUD methods**: `insert_one`, `insert_many`, `find_one`, `find`, `update_one`, `update_many`, `delete_one`, `delete_many`, `find_one_and_update`, `find_one_and_delete`, `find_one_and_replace` (for findAndModify).
4. **All methods sync.** Wire protocol shim uses its own tokio runtime internally.
5. **Serde integration**: `Collection<MyStruct>` with automatic serialization. `Collection<Document>` for untyped access.
6. **Error model**: `mqlite::Error` enum with MongoDB error code mapping where applicable.
7. **Feature flags**: `wire` (pulls tokio), `tracing` (observability). Default features = none.

## Native Rust API Surface

### Database Handle

```rust
pub struct Database { /* Arc<Inner> internally */ }

impl Database {
    /// Open a database file. Creates the file if it doesn't exist.
    /// Automatically replays WAL on recovery.
    pub fn open(path: impl AsRef<Path>) -> Result<Database>;

    /// Open with explicit configuration.
    pub fn open_with_options(path: impl AsRef<Path>, opts: OpenOptions) -> Result<Database>;

    /// Create an in-memory database (no file, no durability).
    pub fn open_in_memory() -> Result<Database>;

    /// Get a collection handle. Does not create the collection until first write.
    pub fn collection<T: Serialize + DeserializeOwned>(&self, name: &str) -> Collection<T>;

    /// List all collection names in this database.
    pub fn list_collection_names(&self) -> Result<Vec<String>>;

    /// Drop a collection and all its indexes.
    pub fn drop_collection(&self, name: &str) -> Result<()>;

    /// Create a collection explicitly (with options, e.g., capped).
    pub fn create_collection(&self, name: &str) -> Result<()>;

    /// Force WAL checkpoint. Safe to copy file after this returns.
    pub fn checkpoint(&self) -> Result<()>;

    /// Hot backup to a new file.
    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()>;

    /// Reclaim free pages (like SQLite VACUUM).
    pub fn compact(&self) -> Result<()>;

    /// Database statistics.
    pub fn stats(&self) -> Result<DatabaseStats>;

    /// Flush WAL, checkpoint, and close. Blocks until complete.
    /// Use this when you need a guarantee that all committed data is in the main file.
    /// `Drop` performs a non-blocking close; use `close()` for explicit durability guarantees
    /// (e.g., before copying the .mqlite file as a backup).
    pub fn close(self) -> Result<()>;
}

// Database is Send + Sync (Arc<Inner> internally).
// Clone is cheap — cloned handles share the same underlying database.
impl Clone for Database { ... }
```

### OpenOptions

```rust
pub struct OpenOptions {
    buffer_pool_size: Option<usize>,       // Default: 64MB
    durability: Option<DurabilityMode>,     // Default: Interval(100ms)
    wal_auto_checkpoint: Option<u32>,       // Default: 1000 pages
    wal_max_size: Option<u64>,              // Default: 100MB. Absolute WAL size that forces a checkpoint
                                            //          regardless of page count threshold.
    busy_timeout: Option<Duration>,         // Default: 5 seconds
    read_only: Option<bool>,               // Default: false (see note below)
    create_if_missing: Option<bool>,       // Default: true
    max_readers: Option<u32>,              // Default: 64
}

// Note on read_only mode:
// When `read_only: true`:
// - WAL replay is SKIPPED (database state is as of last checkpoint)
// - No writes are attempted, even for recovery
// - SHM file is not created or modified
// - Safe for opening databases on read-only filesystems (e.g., IoT forensic access after failure)
// - Useful for forensic access after IoT/edge failures
// - If WAL exists with uncommitted changes, they are NOT visible

pub enum DurabilityMode {
    /// fsync after every commit. Safest, slowest.
    FullSync,
    /// Flush WAL at a configurable interval. Fast, small loss window.
    Interval(Duration),
    /// No durability guarantees (in-memory behavior for file-backed DBs).
    None,
}
```

### Collection Handle

```rust
pub struct Collection<T> { /* lightweight, cloneable */ }

impl<T: Serialize + DeserializeOwned> Collection<T> {
    // --- Insert ---
    pub fn insert_one(&self, doc: &T) -> Result<InsertOneResult>;
    pub fn insert_many(&self, docs: &[T]) -> Result<InsertManyResult>;
    pub fn insert_many_with_options(&self, docs: &[T], opts: InsertManyOptions) -> Result<InsertManyResult>;

    // --- Find ---
    pub fn find_one(&self, filter: Document) -> Result<Option<T>>;
    pub fn find(&self, filter: Document) -> Result<Cursor<T>>;
    pub fn find_with_options(&self, filter: Document, opts: FindOptions) -> Result<Cursor<T>>;

    // --- Update ---
    pub fn update_one(&self, filter: Document, update: Document) -> Result<UpdateResult>;
    pub fn update_one_with_options(&self, filter: Document, update: Document, opts: UpdateOptions) -> Result<UpdateResult>;
    pub fn update_many(&self, filter: Document, update: Document) -> Result<UpdateResult>;
    pub fn update_many_with_options(&self, filter: Document, update: Document, opts: UpdateOptions) -> Result<UpdateResult>;

    // --- Delete ---
    pub fn delete_one(&self, filter: Document) -> Result<DeleteResult>;
    pub fn delete_many(&self, filter: Document) -> Result<DeleteResult>;

    // --- findAndModify variants ---
    /// Returns the document as it appeared BEFORE the update (pre-modification document).
    /// This matches MongoDB's findAndModify default behavior.
    /// To return the post-modification document, use find_one_and_update_with_options
    /// with FindOneAndUpdateOptions { return_document: Some(ReturnDocument::After) }.
    pub fn find_one_and_update(&self, filter: Document, update: Document) -> Result<Option<T>>;
    pub fn find_one_and_update_with_options(&self, filter: Document, update: Document, opts: FindOneAndUpdateOptions) -> Result<Option<T>>;
    pub fn find_one_and_delete(&self, filter: Document) -> Result<Option<T>>;
    pub fn find_one_and_delete_with_options(&self, filter: Document, opts: FindOneAndDeleteOptions) -> Result<Option<T>>;
    pub fn find_one_and_replace(&self, filter: Document, replacement: &T) -> Result<Option<T>>;
    pub fn find_one_and_replace_with_options(&self, filter: Document, replacement: &T, opts: FindOneAndReplaceOptions) -> Result<Option<T>>;

    // --- Indexes ---
    /// BLOCKING: Acquires the writer lock and holds it until the index is fully built.
    /// For large collections (e.g., 100K documents), this may take several seconds.
    /// No writes can proceed on this collection during index construction.
    /// Background (non-blocking) index builds are planned for Phase 2.
    pub fn create_index(&self, model: IndexModel) -> Result<String>;
    pub fn drop_index(&self, name: &str) -> Result<()>;
    pub fn list_indexes(&self) -> Result<Vec<IndexModel>>;

    // --- Count ---
    pub fn count_documents(&self, filter: Document) -> Result<u64>;
    pub fn estimated_document_count(&self) -> Result<u64>;

    // --- Stats ---
    pub fn stats(&self) -> Result<CollectionStats>;
}
```

### FindOneAnd* Options

```rust
/// Which version of the document to return from find_one_and_update / find_one_and_replace.
pub enum ReturnDocument {
    /// Return the document as it appeared BEFORE the modification (default, matches MongoDB).
    Before,
    /// Return the document as it appears AFTER the modification.
    After,
}

pub struct FindOneAndUpdateOptions {
    /// Default: ReturnDocument::Before (matches MongoDB findAndModify default behavior).
    pub return_document: Option<ReturnDocument>,
    /// If true and no document matches the filter, insert a new document from the filter
    /// plus the update operators. Default: false.
    pub upsert: Option<bool>,
    /// Sort order for selecting which document to update when filter matches multiple.
    pub sort: Option<Document>,
}

pub struct FindOneAndDeleteOptions {
    /// Sort order for selecting which document to delete when filter matches multiple.
    pub sort: Option<Document>,
}

pub struct FindOneAndReplaceOptions {
    /// Default: ReturnDocument::Before.
    pub return_document: Option<ReturnDocument>,
    /// If true and no document matches, insert the replacement document. Default: false.
    pub upsert: Option<bool>,
    /// Sort order for selecting which document to replace when filter matches multiple.
    pub sort: Option<Document>,
}
```

### FindOptions

```rust
pub struct FindOptions {
    pub sort: Option<Document>,          // e.g., doc! { "name": 1 }
    pub limit: Option<i64>,
    pub skip: Option<u64>,
    pub projection: Option<Document>,    // e.g., doc! { "name": 1, "_id": 0 }
    pub batch_size: Option<u32>,         // Default: 101 (matches MongoDB)
}
```

### InsertManyOptions

```rust
pub struct InsertManyOptions {
    /// If true (default), stop at the first error and report which documents were
    /// successfully inserted. If false, attempt all inserts and report all errors
    /// and all successes. Matches MongoDB 8.0 insert_many semantics (PRD R10).
    pub ordered: Option<bool>,           // Default: true (matches MongoDB)
}
```

`insert_many` behavior:
- **Ordered (default)**: Stop at the first error. Return `InsertManyResult` with successfully inserted document IDs plus the error. Documents after the error are not attempted.
- **Unordered**: Attempt all inserts. Return `InsertManyResult` with all successfully inserted document IDs and a `Vec<BulkWriteError>` for all failures.

The `InsertManyResult` must support partial success reporting:

```rust
pub struct InsertManyResult {
    pub inserted_ids: HashMap<usize, Bson>,
    /// Errors encountered during insert (non-empty only for partial failures)
    pub errors: Vec<BulkWriteError>,
}

pub struct BulkWriteError {
    pub index: usize,           // Index in the original document array
    pub code: i32,              // MongoDB error code (e.g., 11000 for duplicate key)
    pub message: String,
}
```

### UpdateOptions

```rust
pub struct UpdateOptions {
    /// If true and no documents match the filter, insert a new document constructed
    /// from the filter and update operators. Matches MongoDB 8.0 upsert behavior.
    pub upsert: Option<bool>,            // Default: false
}
```

Upsert is a common pattern required for real-world test suites and production workloads. When `upsert: true` and no document matches the filter, the update creates a new document by applying the update operators to a document derived from the filter's equality conditions. The generated `_id` (if not specified in the filter) is returned in `UpdateResult::upserted_id`.

### Cursor

```rust
pub struct Cursor<T> { /* holds read snapshot, iterates lazily */ }

impl<T: DeserializeOwned> Iterator for Cursor<T> {
    type Item = Result<T>;
}

impl<T> Cursor<T> {
    /// Explain the query plan without executing.
    pub fn explain(&self) -> Result<ExplainResult>;
}
```

### Result Types

```rust
pub struct InsertOneResult { pub inserted_id: Bson }

pub struct InsertManyResult {
    /// IDs of all successfully inserted documents, keyed by their index in the input array.
    pub inserted_ids: HashMap<usize, Bson>,
    /// Errors encountered during the operation. Non-empty only on partial failure.
    /// Populated in ordered mode (stops at first error) and unordered mode (collects all errors).
    pub errors: Vec<BulkWriteError>,
}

pub struct UpdateResult { pub matched_count: u64, pub modified_count: u64, pub upserted_id: Option<Bson> }
pub struct DeleteResult { pub deleted_count: u64 }
```

### IndexModel

```rust
pub struct IndexModel {
    pub keys: Document,              // e.g., doc! { "email": 1 } or doc! { "a": 1, "b": -1 }
    pub options: Option<IndexOptions>,
}

pub struct IndexOptions {
    pub name: Option<String>,        // Auto-generated if omitted
    pub unique: Option<bool>,        // Default: false
    pub sparse: Option<bool>,        // Default: false
}

// Note on create_index behavior:
// `create_index` is a BLOCKING operation in Phase 1. It acquires the writer lock
// and holds it until the entire collection is scanned and the index is built.
// For a collection with 100K documents, this may take several seconds.
// Background index builds (non-blocking) are planned for Phase 2.
```

**Unsupported index types**: `createIndex` requests specifying unsupported index options (TTL via `expireAfterSeconds`, text indexes, geospatial indexes via `2dsphere`/`2d`, partial indexes via `partialFilterExpression`, hashed indexes) must return an appropriate error. Use error code 67 (`CannotCreateIndex`) with a message naming the unsupported option and listing supported index types (single-field, compound, unique, sparse, multikey).

**Collation parameter handling (PRD non-goal NG10)**: Collation is explicitly out of scope for Phase 1. When a `collation` option is specified in any command (`find`, `createIndexes`, `update`, `delete`, `findAndModify`, `aggregate`), mqlite must return an error rather than silently ignoring it. Silent ignore would cause correctness issues — queries would return results in byte ordering rather than the locale-aware ordering the caller expects. Use error code 2 (`BadValue`) with message: `"Collation is not supported in mqlite Phase 1. Queries use binary (byte-order) comparison for strings."`

```rust
// Example error for unsupported index type
Error::UnsupportedIndexOption {
    option: "expireAfterSeconds".to_string(),
    suggestion: "TTL indexes are not supported in mqlite Phase 1. \
                 Supported index types: single-field, compound, unique, sparse, multikey.".to_string(),
}
```

## Error Taxonomy

```rust
pub enum Error {
    // --- MongoDB-compatible errors (with error codes) ---
    /// Duplicate key violation (code 11000)
    DuplicateKey { collection: String, key: Document },
    /// Document validation failure (code 121)
    DocumentValidationFailure { detail: String },
    /// Index not found (code 27)
    IndexNotFound { name: String },
    /// Namespace not found (code 26)
    NamespaceNotFound { ns: String },
    /// Namespace already exists (code 48)
    NamespaceExists { ns: String },
    /// Command not found / unsupported (code 59)
    CommandNotFound { command: String },
    /// Invalid BSON (code 22)
    InvalidBson { detail: String },
    /// Document exceeds max size (code 10334)
    DocumentTooLarge { size: usize, max: usize },

    // --- mqlite-specific errors ---
    /// Writer lock contention — timed out waiting for lock
    WriterBusy { held_for: Duration },
    /// Database file is corrupt
    CorruptDatabase { path: PathBuf, detail: String, recoverable: bool },
    /// Disk full during write
    DiskFull { path: PathBuf, required_bytes: u64, available_bytes: u64 },
    /// Unsupported MQL operator
    UnsupportedOperator { operator: String, suggestion: String },
    /// File I/O error
    Io(std::io::Error),
    /// BSON serialization/deserialization error
    BsonSerialization(bson::ser::Error),
    BsonDeserialization(bson::de::Error),
}

impl Error {
    /// Return the MongoDB error code, if this error has one.
    pub fn code(&self) -> Option<i32> { ... }

    /// Return the MongoDB error code name.
    pub fn code_name(&self) -> Option<&str> { ... }
}
```

## Wire Protocol Command Surface

### Phase 1 Command List

| Command | Category | Notes |
|---------|----------|-------|
| `hello` | Handshake | Primary handshake (MongoDB 5.0+). Returns server capabilities. |
| `isMaster` | Handshake | Legacy handshake. Alias behavior to `hello`. |
| `ping` | Diagnostic | Returns `{ ok: 1 }`. |
| `buildInfo` | Diagnostic | Returns mqlite version, git hash, Rust version. |
| `serverStatus` | Diagnostic | Returns connection count, uptime, storage stats. |
| `listDatabases` | Admin | Returns the single database name (mqlite is single-DB per file). |
| `insert` | CRUD | Bulk insert with ordered/unordered semantics. |
| `find` | CRUD | Query with filter, sort, projection, limit, skip. Returns cursor ID. |
| `update` | CRUD | Single or multi update with filter and update document. |
| `delete` | CRUD | Single or multi delete with filter. |
| `findAndModify` | CRUD | Atomic find-and-modify with returnDocument option. |
| `getMore` | Cursor | Fetch next batch from an open cursor. |
| `killCursors` | Cursor | Close open cursors and release resources. |
| `create` | Collection | Create a collection explicitly. |
| `drop` | Collection | Drop a collection and its indexes. |
| `listCollections` | Collection | List collections with optional name filter. |
| `createIndexes` | Index | Create one or more indexes on a collection. |
| `dropIndexes` | Index | Drop one or all indexes on a collection. |
| `listIndexes` | Index | List indexes on a collection. |

### Handshake Response Design

```javascript
// hello response
{
    "isWritablePrimary": true,
    "maxBsonObjectSize": 16777216,
    "maxMessageSizeBytes": 48000000,
    "maxWriteBatchSize": 100000,
    "localTime": ISODate("..."),
    "minWireVersion": 0,
    "maxWireVersion": 21,        // MongoDB 8.0 wire version
    "readOnly": false,
    "ok": 1,
    // mqlite-specific
    "mqlite": {
        "version": "0.1.0"
    }
}
```

Key decisions:
- Report `maxWireVersion: 21` (MongoDB 8.0) but strip capabilities mqlite doesn't support (sessions, transactions, change streams).
- Do NOT report as a replica set member. `isWritablePrimary: true` signals standalone mode.
- Include `mqlite.version` for tool detection — clients can check for mqlite-specific behavior.
- `directConnection=true` should be documented for all client connections.

### Unsupported Command Behavior

Per Q2: return proper MongoDB error codes. For any command not in the Phase 1 list:

```javascript
{
    "ok": 0,
    "errmsg": "no such command: 'aggregate'",
    "code": 59,
    "codeName": "CommandNotFound"
}
```

For commands that exist but use unsupported features (e.g., `update` with `$bit`):

```javascript
{
    "ok": 0,
    "errmsg": "Unknown modifier: $bit. mqlite Phase 1 supports: $set, $unset, $inc, $push, $pull, $rename, $min, $max, $currentDate, $addToSet, $pop",
    "code": 9,
    "codeName": "FailedToParse"
}
```

## Wire Protocol Architecture

```
TCP Listener (127.0.0.1:port)
    │
    ▼
Connection Handler (one per client, async)
    │
    ▼
OP_MSG Parser (frame extraction, section parsing, checksum validation)
    │
    ▼
Command Dispatcher (match on command name → handler fn)
    │
    ▼
Command Handler (translate to native API calls)
    │
    ▼
Native API (sync calls via spawn_blocking or dedicated thread pool)
    │
    ▼
Response Builder (construct OP_MSG reply with MongoDB-format result doc)
```

The wire protocol shim runs in a tokio runtime that is internal to the `wire` feature. It does not leak async into the public API. Each client connection gets a task. Command handlers call into the sync native API using `spawn_blocking` to avoid blocking the async runtime.

**Cursor pinning**: Cursors created via `find` are pinned to the TCP connection that created them. `getMore` requests must be sent on the same connection; otherwise, error code 43 (`CursorNotFound`) is returned. This matches MongoDB behavior and ensures cursor snapshot state is correctly associated with the client. The wire protocol shim tracks cursor ownership per connection and cleans up cursors when connections close.

## Thread Safety Contract

| Type | Send | Sync | Clone | Notes |
|------|------|------|-------|-------|
| `Database` | Yes | Yes | Yes (cheap) | `Arc<Inner>` internally. Shared across threads. |
| `Collection<T>` | Yes | Yes | Yes (cheap) | Lightweight handle, references Database. |
| `Cursor<T>` | Yes | No | No | Holds a read snapshot. Single-threaded iteration. |
| `OpenOptions` | Yes | Yes | Yes | Builder, no interior mutability. |

`Cursor` is not `Sync` because it maintains iteration state. It is `Send` so it can be moved to another thread, but should not be shared. This matches the MongoDB driver's cursor semantics.

## Atomicity Guarantees

mqlite provides **single-document atomicity only** (PRD non-goal NG4: no multi-document transactions). Each individual CRUD operation is atomic:

- `insert_one`: atomic — the document is either fully inserted or not at all.
- `update_one`, `find_one_and_update`, `find_one_and_delete`, `find_one_and_replace`: atomic read-modify-write on a single document (requires writer lock acquisition, query execution, mutation, and result return within a single lock hold).
- `delete_one`: atomic — the document is either fully deleted or not at all.
- `insert_many`: **NOT atomic as a whole**. With `ordered: true` (default), stops at first error — successfully inserted documents are committed; remaining documents are not attempted. With `ordered: false`, attempts all inserts — each individual insert is atomic, but the batch may partially succeed.
- `update_many`, `delete_many`: each affected document is updated/deleted atomically, but the overall operation is not transactional — a crash mid-operation may leave some documents updated and others not.

There are no multi-document ACID transactions, sessions, or `startTransaction`/`commitTransaction` APIs. The wire protocol omits `logicalSessionTimeoutMinutes` from the hello response and silently ignores `lsid` fields in commands (per integration.md).

## Constraints Identified

1. **Sync-first API is mandatory.** The base crate has zero async dependencies. Wire protocol feature gates tokio.

2. **findAndModify requires atomic read-modify-write.** The implementation must acquire the writer lock, execute the query, apply the modification, and return the result (pre- or post-modification) in a single atomic operation. This is more complex than separate find + update.

3. **MongoDB 8.0 error code compatibility.** Every error that has a MongoDB equivalent must use the same numeric code. This enables error-handling code written for MongoDB to work unchanged.

4. **Unsupported operators must fail loudly.** Silent success on an unsupported operator is a critical bug for the test-double persona. The error message must name the specific unsupported operator and list what IS supported.

5. **Wire protocol is single-database.** mqlite opens one file = one database. `listDatabases` returns one entry. The `$db` field in OP_MSG is validated but effectively ignored (or verified to match the opened database name).

6. **No authentication in Phase 1.** The wire protocol accepts all connections without auth. The `hello` response must not advertise SCRAM or any auth mechanism.

7. **Options structs must have all-optional fields.** No required options. The zero-config path (`find(filter)`) must work without any options struct.

8. **`bson` crate re-export pins the version.** mqlite's public API includes `bson` types (`Document`, `Bson`, `ObjectId`). Upgrading the `bson` dependency is a semver-breaking change for mqlite.

9. **Cursor batch size defaults to 101.** Matches MongoDB's default first-batch size. Subsequent batches can be larger. This prevents unbounded memory usage on large result sets.

10. **Writer busy timeout defaults to 5 seconds.** Unlike SQLite (default 0ms), mqlite blocks by default. This prevents the most common complaint about embedded databases: immediate write failures under light contention.

## Open Questions

1. ~~**Should `find_one_and_update` return the pre-modification or post-modification document by default?**~~ **RESOLVED**: Default is `ReturnDocument::Before` (return the pre-modification document), matching MongoDB's `findAndModify` behavior and the MongoDB Rust driver. Configurable via `FindOneAndUpdateOptions { return_document: ReturnDocument::Before | After }`. The `Before` default ensures backward compatibility with MongoDB code that relies on this behavior.

2. **Should `Database` implement `Drop` with WAL flush?** If `Drop` flushes the WAL, it may block. If it doesn't, data written since the last flush may be lost (but recoverable via WAL replay on next open). The recommended behavior: `Drop` does a non-blocking close. Explicit `db.close()` method for blocking flush. Document this clearly.

3. ~~**How does `insert_many` handle partial failures with `ordered: true`?**~~ **RESOLVED**: `InsertManyOptions { ordered: bool }` added. Ordered mode (default) stops at first error, reports successful inserts plus the error. Unordered mode attempts all, reports all errors and successes. `InsertManyResult` includes `errors: Vec<BulkWriteError>` for partial failure reporting. Matches MongoDB 8.0 behavior per PRD R10.

4. **Should the wire protocol support OP_COMPRESSED?** This reduces network bandwidth for large documents but adds compression library dependencies (snappy, zlib, zstd). For localhost-only debugging use, compression overhead may exceed benefit. Recommend: defer to Phase 1.1.

5. **What is the cursor timeout for idle cursors opened via wire protocol?** MongoDB defaults to 10 minutes. mqlite should implement cursor timeout to prevent resource leaks from abandoned cursors. Native API cursors are cleaned up by Rust's Drop.

6. ~~**Should `update_one`/`update_many` support upsert?**~~ **RESOLVED**: Yes, upsert is Phase 1. `UpdateOptions { upsert: bool }` added. When `upsert: true` and no documents match, a new document is created from the filter's equality conditions plus the update operators. The generated `_id` is returned in `UpdateResult::upserted_id`. Required for real-world test suites.

7. **How does `serverStatus` report for an embedded database?** MongoDB's `serverStatus` returns extensive server metrics. mqlite should return a useful subset: uptime, connection count (wire protocol), storage stats (file size, WAL size, buffer pool usage), operation counters.

## Integration Points

### -> Storage Engine
- `Database::open()` delegates to storage engine for file open, WAL recovery, buffer pool initialization
- `Database::checkpoint()` and `Database::compact()` are direct storage engine operations
- `OpenOptions` fields (buffer_pool_size, durability, wal_auto_checkpoint) configure the storage engine
- Cursor read snapshots are storage engine primitives (WAL snapshot)

### -> Query Engine
- CRUD methods delegate filter/update/projection parsing to the query engine
- `FindOptions` (sort, limit, skip, projection) are query planner inputs
- `Cursor::explain()` returns query planner output
- findAndModify requires query engine coordination with write lock

### -> Wire Protocol
- Wire protocol command handlers call native API methods directly
- Command dispatch maps command names to Collection/Database method calls
- Cursor IDs from native API are returned to wire protocol clients
- Error types are serialized into MongoDB wire protocol error format

### -> Error Taxonomy
- Every API method returns `Result<T, Error>`
- Error codes are shared between native API and wire protocol responses
- Wire protocol adds serialization to BSON error documents
- MongoDB error code mapping must be maintained as a table in the codebase

### -> Data Model
- `Collection<T>` depends on serde traits for T
- Document type is re-exported from `bson` crate
- IndexModel defines key patterns using BSON Document
- InsertOneResult returns the generated `_id` as `Bson`
