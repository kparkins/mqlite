# mqlite Compatibility Matrix

> Last updated: Phase 1 (wire protocol complete). Updated with every release.

## MongoDB Version

mqlite targets **MongoDB 8.0** wire protocol semantics for Phase 1. The embedded
native API is driver-agnostic; the wire shim (`wire` feature) speaks the OP_MSG
framing used by MongoDB 8.0 drivers.

---

## Query (Filter) Operators

### Comparison Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$eq` | ✅ Supported | Implicit `{field: value}` also supported |
| `$ne` | ✅ Supported | |
| `$gt` | ✅ Supported | |
| `$gte` | ✅ Supported | |
| `$lt` | ✅ Supported | |
| `$lte` | ✅ Supported | |
| `$in` | ✅ Supported | Array unwrap semantics for array fields |
| `$nin` | ✅ Supported | |

### Logical Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$and` | ✅ Supported | Implicit `{a:1, b:2}` and explicit `{$and:[...]}` both work |
| `$or` | ✅ Supported | Must have at least one element |
| `$nor` | ✅ Supported | Must have at least one element |
| `$not` | ✅ Supported | Field-level only (`{field: {$not: {$gt: 5}}}`); top-level `$not` is invalid in MongoDB |

### Element Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$exists` | ✅ Supported | `{field: {$exists: true/false}}` |
| `$type` | ✅ Supported | Accepts MongoDB type strings and numeric type codes |

### Array Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$all` | ✅ Supported | Array field must contain all specified values |
| `$elemMatch` | ✅ Supported | Match array element against a sub-filter |
| `$size` | ✅ Supported | Match arrays with an exact element count |

### Evaluation Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$regex` | ✅ Supported | **No PCRE lookahead/lookbehind** (uses Rust `regex` crate, not PCRE). Options: `i`, `m`, `s`, `x`. |
| `$options` | ✅ Supported | Only valid alongside `$regex` |
| `$mod` | ❌ Not Supported | Planned Phase 2 |
| `$text` | ❌ Not Supported | Full-text search. Planned Phase 2 |
| `$where` | ❌ Not Supported | JavaScript evaluation. Not planned (security) |
| `$expr` | ❌ Not Supported | Aggregation expressions. Planned Phase 2 |
| `$jsonSchema` | ❌ Not Supported | JSON Schema validation. Planned Phase 2 |

### Geospatial Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$near` | ❌ Not Supported | Not planned |
| `$nearSphere` | ❌ Not Supported | Not planned |
| `$geoWithin` | ❌ Not Supported | Not planned |
| `$geoIntersects` | ❌ Not Supported | Not planned |

### Other

| Operator | Status | Notes |
|----------|--------|-------|
| `$comment` | ❌ Not Supported | Query annotation |
| `$rand` | ❌ Not Supported | Random sampling |
| `$natural` | ❌ Not Supported | Not valid in query filters |

---

## Update Operators

### Field Update Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$set` | ✅ Supported | Set field values |
| `$unset` | ✅ Supported | Remove fields |
| `$inc` | ✅ Supported | Increment numeric fields |
| `$mul` | ✅ Supported | Multiply numeric fields |
| `$rename` | ✅ Supported | Rename fields |
| `$min` | ✅ Supported | Update if new value < current |
| `$max` | ✅ Supported | Update if new value > current |
| `$currentDate` | ✅ Supported | Set to current date or timestamp |
| `$setOnInsert` | ✅ Supported | Applied only on upsert insert |
| `$bit` | ❌ Not Supported | Bitwise update. Not planned |

### Array Update Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$push` | ✅ Supported | Modifiers: `$each`, `$position`, `$sort`, `$slice` |
| `$pull` | ✅ Supported | Remove matching elements |
| `$pullAll` | ✅ Supported | Remove all occurrences of specified values |
| `$addToSet` | ✅ Supported | Add without duplicates; `$each` modifier supported |
| `$pop` | ✅ Supported | Remove first (`-1`) or last (`1`) array element |
| `$` (positional) | ❌ Not Supported | Planned Phase 2 |
| `$[]` (all positional) | ❌ Not Supported | Planned Phase 2 |
| `$[<identifier>]` (filtered) | ❌ Not Supported | Planned Phase 2 |

---

## Wire Protocol Commands

> Requires the `wire` Cargo feature. See [README](../README.md) for setup.

### Diagnostic Commands

| Command | Status | Notes |
|---------|--------|-------|
| `hello` / `isMaster` | ✅ Supported | Returns topology info; `ismaster: true` |
| `ping` | ✅ Supported | Returns `{ok: 1}` |
| `buildInfo` | ✅ Supported | Returns mqlite version info |
| `serverStatus` | ✅ Supported | Returns connection + database stats |

### Database Commands

| Command | Status | Notes |
|---------|--------|-------|
| `listDatabases` | ✅ Supported | Returns list of databases in the file |

### CRUD Commands

| Command | Status | Notes |
|---------|--------|-------|
| `find` | ✅ Supported | Supports filter, sort, limit, skip, projection, batchSize |
| `insert` | ✅ Supported | `ordered` flag supported |
| `update` | ✅ Supported | `upsert` supported; `arrayFilters` not supported |
| `delete` | ✅ Supported | `deleteOne` and `deleteMany` via `limit` field |
| `findAndModify` | ✅ Supported | `new`, `upsert`, `sort` options supported |
| `getMore` | ✅ Supported | Cursor continuation |
| `killCursors` | ✅ Supported | Explicit cursor cleanup |

### Collection Admin Commands

| Command | Status | Notes |
|---------|--------|-------|
| `create` | ✅ Supported | Create a collection |
| `drop` | ✅ Supported | Drop a collection |
| `listCollections` | ✅ Supported | List collections in the database |

### Index Commands

| Command | Status | Notes |
|---------|--------|-------|
| `createIndexes` | ✅ Supported | Supported index types only (see Index Types) |
| `dropIndexes` | ✅ Supported | Drop by name or `*` for all non-`_id` indexes |
| `listIndexes` | ✅ Supported | List indexes on a collection |

### Unsupported Commands

| Command | Status | Notes |
|---------|--------|-------|
| `aggregate` | ❌ Not Supported | Planned Phase 2 |
| `distinct` | ❌ Not Supported | Planned Phase 2 |
| `count` | ❌ Not Supported | Use `countDocuments` via native API |
| `mapReduce` | ❌ Not Supported | Not planned |
| `explain` | ❌ Not Supported | Planned Phase 2 (available in native API) |
| `currentOp` | ❌ Not Supported | |
| Authentication | ❌ Not Supported | See [WIRE-SECURITY.md](WIRE-SECURITY.md) |
| Replication commands | ❌ Not Supported | mqlite is standalone, not a replica set |
| `transaction` / `commitTransaction` | ❌ Not Supported | Planned Phase 2 |

---

## Index Types

| Type | Status | Notes |
|------|--------|-------|
| Single-field | ✅ Supported | |
| Compound | ✅ Supported | |
| Unique | ✅ Supported | Enforced on insert and upsert |
| Sparse | ✅ Supported | Only indexes documents where the key field exists |
| Multikey (array fields) | ✅ Supported | Automatically applied when indexing array fields |
| TTL | ❌ Not Supported | Planned Phase 2 |
| Text | ❌ Not Supported | Planned Phase 2 |
| Geospatial | ❌ Not Supported | Not planned |
| Hashed | ❌ Not Supported | Not planned |
| Partial | ❌ Not Supported | Planned Phase 2 |
| Wildcard | ❌ Not Supported | Not planned |

---

## Driver Compatibility

All drivers require `directConnection=true` because mqlite is a standalone node,
not a replica set or sharded cluster. Without this flag, drivers attempt topology
discovery (hello/isMaster polling) which mqlite does not support beyond the initial
handshake.

| Driver | Status | Required connection options |
|--------|--------|---------------------------|
| mongosh 2.x | ✅ Supported | `directConnection=true` |
| pymongo 4.x | ✅ Supported | `directConnection=True` |
| Node.js driver 6.x | 🟡 Partial | `directConnection: true` (cursor batching tested; change streams unsupported) |
| Motor (async pymongo) 3.x | 🟡 Partial | `directConnection=True` |
| MongoDB Rust driver 3.x | 🟡 Partial | `directConnection=true` (sync wrappers required; see [MIGRATION.md](MIGRATION.md)) |
| Java driver 5.x | 🔴 Untested | `directConnection=true` (expected to work for basic CRUD) |
| Go driver 1.x | 🔴 Untested | `directConnection=true` (expected to work for basic CRUD) |

---

## Known Divergences from MongoDB 8.0

| Feature | MongoDB 8.0 | mqlite Phase 1 |
|---------|-------------|----------------|
| `$regex` engine | PCRE2 (lookahead, lookbehind, named groups) | Rust `regex` crate (no lookahead/lookbehind, no backreferences) |
| Write concern | `w`, `j`, `wtimeout` | Ignored (all writes are single-writer committed) |
| Read concern | `local`, `majority`, `snapshot` | Ignored (MVCC snapshot per read) |
| Transactions | Multi-document ACID | Not supported (Phase 2) |
| Change streams | `$changeStream` aggregation | Not supported |
| Aggregation pipeline | Full `$match`, `$group`, `$lookup`, … | Not supported (Phase 2) |
| `ObjectId` generation | Server-generated | Client-generated (compatible format) |
| `_id` type enforcement | Any BSON type | Any BSON type |
| Capped collections | Fixed-size, oldest-doc-removal | Not supported |
| GridFS | Chunked file storage | Not supported |
| Authentication | SCRAM-SHA-256, x.509 | None (embedded trust model; see [WIRE-SECURITY.md](WIRE-SECURITY.md)) |
| `explain` output format | Rich query plan JSON | Simplified (`IXSCAN`/`COLLSCAN` via native API) |
