# mqlite Capabilities

Embedded MongoDB-compatible document store for Rust. Single-file database with WiredTiger-style MVCC over a paged B-tree engine, optional MongoDB wire-protocol shim, and a synchronous Rust API shaped after the MongoDB Rust driver.

---

## 1. Embedded Rust API

### Client / Database / Collection

- `Client::open(path)` opens (or creates) a database file at `path`.
- `client.database(name)` returns a lightweight `Database` handle.
- `db.collection::<T>(name)` returns a typed `Collection<T>` where `T: Serialize + DeserializeOwned`. Use `Collection<bson::Document>` for untyped access.
- `Client`, `Database`, and `Collection<T>` are all `Send + Sync` and cheap to clone. All clones share the same underlying engine state.
- `Client::close(self)` performs a blocking flush + checkpoint and leaves the database as a single file on disk. Dropping the handle is non-blocking and leaves the journal for automatic replay on next open.

### CRUD operations (from `Collection<T>`)

| Operation | Method |
|-----------|--------|
| Insert | `insert_one`, `insert_many` |
| Read | `find_one`, `find` (returns `Cursor<T>`) |
| Update | `update_one`, `update_many`, `find_one_and_update`, `find_one_and_replace` |
| Delete | `delete_one`, `delete_many`, `find_one_and_delete` |
| Count | `count_documents` |
| Indexes | `create_index`, `drop_index`, `list_indexes` |

`find()` returns a lazy `Cursor<T>` with chainable options (`filter`, `sort`, `limit`, `skip`, `projection`, `batch_size`). `find_one_and_*` methods accept `ReturnDocument::Before`/`::After` and `upsert` options.

### Query (filter) operators

Comparison: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`.
Logical: `$and`, `$or`, `$nor`, `$not` (field-level).
Element: `$exists`, `$type`.
Array: `$all`, `$elemMatch`, `$size`.
Evaluation: `$regex` (Rust `regex` crate — no PCRE lookahead/lookbehind), `$options`.

### Update operators

Field: `$set`, `$unset`, `$inc`, `$mul`, `$rename`, `$min`, `$max`, `$currentDate`, `$setOnInsert`.
Array: `$push` (with `$each`, `$position`, `$sort`, `$slice`), `$pull`, `$pullAll`, `$addToSet` (with `$each`), `$pop`.

### Indexes

- Single-field, compound, unique, sparse, and multikey (array-field) indexes.
- Indexes are stored as generic B+ trees alongside the primary data tree.
- `create_index` reserves, builds, and commits in distinct steps so concurrent writers can make progress during a background build.

### Durability modes

`DurabilityMode` on `OpenOptions` selects journal-sync behavior:
- `FullSync` — fsync the journal on every commit. Zero data-loss window.
- `Interval(Duration)` — fsync the journal at most once per configured interval (default `Interval(100ms)`).
- `None` — no explicit fsync. All commits since the last checkpoint may be lost on crash.

---

## 2. Storage engine

- **Paged B-tree store.** Pages are 32 KB leaves and 4 KB internal nodes, managed by a two-tier buffer pool (`inner_32k`, `inner_4k`) with partitioned locks for sharded contention.
- **MVCC (WiredTiger-style).** Every write creates a `VersionEntry` on a per-page version chain. Readers open a `ReadView` at an HLC timestamp and walk chains for their visible entry.
- **History store.** A dedicated B-tree holds evicted versions keyed by `(namespace, kind, key, ts)`. Reads that miss the in-memory chain probe the history store before falling back to the baseline on-disk cell.
- **Overflow storage.** Large documents are chained across overflow pages; each overflow cell is refcounted with CAS incref/decref and a deferred-free queue drained by the writer.
- **HLC oracle.** A 12-byte hybrid logical clock (millisecond physical + 32-bit logical) assigns commit timestamps and is persisted through journal frames so `oracle.set_min` advances monotonically across restarts.
- **Read-view registry.** A `BTreeMap<u64, Weak<ReadView>>` tracks live readers so the writer can compute `oldest_required_ts` and garbage-collect versions / history entries that no reader can see.
- **Deferred-free queue.** Overflow refcount → 0 enqueues a page id; the next writer drains the queue under the allocator mutex.
- **Namespace lanes.** Per-namespace mutexes allow concurrent writers against *different* collections while still serializing writes within a single collection.
- **Allocator.** Page-level allocator with a free-list header page; allocations and refcount updates are journaled.

### Locking and concurrency model

**Reads** are mutex-free: every read operation does a single `ArcSwap::load()` on the published `PagedEngine::shared.published: ArcSwap<PublishedSnapshot>` and opens B-trees at the snapshotted root pages. No engine-level lock is acquired.

**Writes** acquire, in this order:
1. `PagedEngine::metadata: RwLock<MetadataState>` — shared read-guard for CRUD (released before any inner acquire); exclusive write-guard only for DDL (`create_namespace`, `drop_namespace`, `drop_index`, `checkpoint`, `backup`).
2. `PagedEngine::ns_lanes[ns]` — per-namespace lane mutex. Writers on different namespaces run concurrently; writers on the same namespace serialize here.
3. `PagedEngine::commit_seq: Mutex<()>` — held across `commit_ts` allocation → primary install → journal append → snapshot publish, so `commit_ts`, journal-append order, and `publish_ts` agree.

Inside that, the MVCC/buffer-pool lock order is: history-store partition → `DeferredFreeQueue::pending` → `AllocatorHandle::state` → 32 KB main partition → 4 KB main partition → `ReadViewRegistry`.

Readers take none of these for a pure read other than a brief `DeferredFreeQueue::pending` on overflow-ref drop. The historical engine-global writer mutex (`PagedEngine::inner`) was retired in the v1 MWMR commit — see [ADR 0002](docs/adr/0002-mwmr.md).

### Durability and recovery

- **Write-ahead journal** (`.mqlite-journal`) records `ChainCommit` frames with CRC32 disambiguation. On open, the journal is replayed to restore committed state.
- **Checkpoint on `close()`** collapses the journal into the main file for a clean single-file state.
- **Crash recovery** is automatic on next `Client::open`.
- **Symlink rejection.** Opening a symlink path returns `Error::SymlinkRejected`. New files are created with Unix mode `0600`.

---

## 3. MongoDB wire protocol (optional `wire` feature)

Enabling `--features wire` starts a tokio-based TCP listener that speaks MongoDB's OP_MSG framing, allowing `mongosh`, `pymongo`, `motor`, and the Node.js driver to connect with `directConnection=true`.

### Supported wire commands

- Diagnostic: `hello` / `isMaster`, `ping`, `buildInfo`, `serverStatus`.
- Database: `listDatabases`.
- CRUD: `find`, `insert`, `update`, `delete`, `findAndModify`, `getMore`, `killCursors`.
- Collection admin: `create`, `drop`, `listCollections`.
- Index: `createIndexes`, `dropIndexes`, `listIndexes`.

### Driver compatibility

| Driver | Status |
|--------|--------|
| mongosh 2.x | Supported |
| pymongo 4.x | Supported |
| Node.js driver 6.x | Partial (cursor batching tested) |
| Motor 3.x | Partial |
| MongoDB Rust driver 3.x | Partial (sync wrappers required) |

All drivers require `directConnection=true` because mqlite is a single-node, non-replica-set endpoint.

### Security posture

The wire shim has **no authentication and no TLS**. It is intended for local test-double use (`127.0.0.1`) and embedded scenarios inside a trust boundary. See `docs/WIRE-SECURITY.md`.

---

## 4. Observability

- **`tracing` feature** emits structured events for CRUD operations, commit boundaries, reconcile passes, history-store probes, and read-view lifecycle.
- **17 MVCC metrics counters** track chain length, history hit/miss, read-view register/unregister, version GC, deferred frees, poisoning events, and reconcile outcomes.
- `ExplainResult` (native API) surfaces `IXSCAN` vs `COLLSCAN` plans for a given filter.

---

## 5. Testing and verification

- **93 Rust source files**, **32 integration-test files** in `tests/` covering MVCC snapshot visibility, namespace lane concurrency, overflow refcount UAF stress, reconcile races, panic rollback, durability recovery edges, index lifecycle edges, persistence e2e, and secondary index atomicity.
- **loom-based concurrency stress tests** (`--features loom-tests`) exercise MVCC primitives under full interleaving.
- **Fuzz targets** in `fuzz/`: `bson_parser`, `wire_protocol`, `query_evaluator`, `key_encoder`.
- **Property tests** for B-tree insertion/deletion invariants.
- **Examples** in `examples/`: `cli_demo`, `index_benchmark`, `wire_server`.
- **Smoke tests**: `mongosh_smoke.sh`, `pymongo_compat.py`, `run_wire_ci.sh`.

---

## 6. Deployment profiles

- **Embedded library.** Pure Rust crate, sync-only API, `rust-version = 1.70`, minimal dependency set (arc-swap, bson, dashmap, parking_lot, regex, smallvec, thiserror, crc32c, serde).
- **Edge / IoT.** Single-file database, no server process, no background threads in the base crate.
- **Test double.** Drop-in replacement for a MongoDB container in CI (open a `tempfile::TempDir`-backed `.mqlite` per test).
- **Local wire endpoint.** Run an embedded mongosh-accessible server with `--features wire`.

---

## 7. What is **not** supported

The following MongoDB features are intentionally out of scope or not implemented:

- **Aggregation pipeline** (`aggregate`, `$match/$group/$lookup/$project`).
- **Transactions** (multi-document `startTransaction` / `commitTransaction`).
- **Change streams.**
- **Geospatial queries and indexes** (`$near`, `$geoWithin`, `2dsphere`).
- **Full-text search** (`$text`, text indexes).
- **TTL, partial, hashed, and wildcard indexes.**
- **JSON Schema validation** (`$jsonSchema`).
- **JavaScript evaluation** (`$where` — rejected for security).
- **Aggregation expressions in queries** (`$expr`).
- **Capped collections.**
- **GridFS.**
- **Replica sets, sharding, oplog.**
- **Authentication** (SCRAM-SHA-256, x.509) and **TLS** on the wire shim.
- **Positional array update operators** (`$`, `$[]`, `$[<identifier>]`).
- **`$mod`, `$bit`** operators.

See `docs/COMPATIBILITY.md` for the full matrix.

---

## 8. Potential work areas

Concrete directions where mqlite could be extended. None are committed or scheduled — this is a map of adjacent territory, not a roadmap.

### Query and data-model surface

- **Aggregation pipeline.** The MVCC read path already produces point-in-time snapshots; adding a pipeline executor on top of cursors would unlock `$match → $group → $project`, `$lookup`, `$unwind`, and `$sort`/`$limit` pushdowns. The main design work is choosing whether to compile pipelines to iterator chains or to a small VM, and how to reuse index scans.
- **`$expr` and aggregation-expression evaluation in filters.** Shares the expression evaluator with aggregation; probably worth building once and sharing.
- **Positional array update operators** (`$`, `$[]`, `$[<identifier>]` with `arrayFilters`). The update-operator module already walks documents deeply; this is scoped work inside `src/update/`.
- **`$text` / text indexes.** Requires a tokenizer, posting-list storage, and scoring. Non-trivial but self-contained.
- **TTL indexes.** A background expirer thread scanning a TTL secondary index would round out the index story for session/cache workloads.
- **Partial and wildcard indexes.** Straightforward extensions of the secondary-index builder.
- **JSON Schema validation** (`$jsonSchema`). Useful for test-double parity.

### Transactions

- **Multi-statement / multi-document transactions.** MVCC gives snapshot isolation for reads; the missing piece is a write-set + conflict-detection layer and a `startTransaction` / `commit` / `abort` API plumbed through the wire shim. Per-namespace lanes and the HLC oracle make this achievable without redesigning the engine.
- **Read-your-own-writes within a session.** Currently each `ReadView` is opened fresh; a session-scoped view would make `read-after-write` deterministic across calls.

### Wire protocol and connectivity

- **Authentication** (SCRAM-SHA-256). Required to expose the wire shim outside `127.0.0.1`.
- **TLS termination.** Pairs with auth; rustls is a natural fit.
- **Change streams.** Would require an oplog-like append structure; the journal already records commit frames — exposing a tailable view is the smaller part; semantics (resume tokens, invalidation) are the larger part.
- **`explain` over the wire.** The native API has `ExplainResult`; a wire-level `explain` command would make query-plan inspection available to drivers.
- **Compression** (`zstd`, `snappy`) in OP_MSG framing.
- **Broader driver coverage.** Java, Go, and C# drivers are listed as untested — running the compatibility matrix against them and fixing any surfaced gaps is tractable scoped work.

### Storage engine

- **Group commit.** A prior attempt is parked — the commit-sequence serialization currently blocks MWMR throughput. Resolving this would improve write throughput under contention.
- **Compaction / vacuum.** Overflow pages and history-store entries are reclaimed incrementally; an explicit `db.compact()` that rewrites the database to a dense layout would be valuable for long-lived deployments.
- **Online backup.** A consistent snapshot copy via `ReadView` pinning + journal tailing, rather than requiring `Client::close()`.
- **Page compression.** 32 KB leaves are a natural unit for block compression; adds CPU but reduces file size meaningfully for text-heavy data.
- **Encryption at rest.** Per-file key with page-level AEAD.
- **Async API.** The base crate is intentionally sync; an async façade (tokio-aware) that wraps the engine could better match ecosystem expectations without changing the core.

### Operational and tooling

- **CLI tool** (`mqlite` binary) for file inspection, index listing, `explain`, manual checkpoint, and export/import.
- **Expanded `buildInfo` / `serverStatus`** surface for drivers that key behavior off these fields.
- **Prometheus-style metrics exporter** behind the `tracing` feature.
- **Benchmark suite beyond `index_benchmark`** — throughput/latency curves versus SQLite, sled, and a containerized MongoDB.

### Correctness and testing

- **Jepsen-style fault-injection harness.** loom covers in-process interleavings; a durability torture test that crashes mid-journal-write on random operations would harden the recovery path further.
- **MongoDB compatibility test suite.** Running the official driver tests against the wire shim would surface semantic gaps that unit tests miss.
- **Fuzz coverage expansion.** Update-operator fuzzing and secondary-index-key fuzzing are natural additions to the existing fuzz targets.
