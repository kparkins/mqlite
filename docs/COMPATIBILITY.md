# mqlite Compatibility Matrix

## MongoDB Version

mqlite targets **MongoDB 8.0** wire protocol semantics. The embedded
native API is driver-agnostic; the wire shim (`wire` feature) speaks the OP_MSG
framing used by MongoDB 8.0 drivers.

---

## Query (Filter) Operators

Dotted paths traverse embedded documents and support numeric array indexes
(`"a.0.b"`), and the final path value unwraps arrays (equality and comparison
operators match any element). However, **dotted paths do not traverse arrays
of documents**: `{"items.k": "b"}` does not match `{items: [{k: "b"}]}` — use
`{items: {$elemMatch: {k: "b"}}}` instead. This is mqlite's most significant
filter divergence from MongoDB (see Known Divergences).

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
| `$mod` | ✅ Supported | `[divisor, remainder]`; doubles truncated toward zero; C-style remainder for negatives |
| `$text` | ❌ Not Supported | Full-text search |
| `$where` | ❌ Not Supported | JavaScript evaluation. Not planned (security) |
| `$expr` | ✅ Supported | Top-level only; full expression language (see Aggregation Pipeline → Expressions) |
| `$jsonSchema` | ❌ Not Supported | JSON Schema validation |

### Bitwise Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$bitsAllSet` | ✅ Supported | Mask forms: non-negative integer, bit-position array, BinData (little-endian) |
| `$bitsAnySet` | ✅ Supported | Same mask forms |
| `$bitsAllClear` | ✅ Supported | Same mask forms |
| `$bitsAnyClear` | ✅ Supported | Same mask forms |

Numeric field values are treated as 64-bit two's complement with sign extension
beyond bit 63; fractional doubles never match; non-numeric/non-BinData fields
never match (no error). Array fields unwrap.

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
| `$comment` | ✅ Supported | Top-level only; parsed and ignored (any BSON type accepted) |
| `$rand` | ✅ Supported | As an expression: `{$expr: {$lt: [{$rand: {}}, 0.33]}}` |
| `$natural` | ❌ Not Supported | Not valid in query filters |

### Projection Operators

Plain projections (`{field: 1}` / `{field: 0}` with `_id` handling) are
supported, including dotted paths (`{"a.b": 1}`) at arbitrary depth with
MongoDB's array-of-documents semantics. Path collisions
(`{"a": 1, "a.b": 1}`) resolve last-spec-wins instead of erroring (see
Divergences).

| Operator | Status | Notes |
|----------|--------|-------|
| `$` (positional projection) | ❌ Not Supported | |
| `$elemMatch` (projection) | ❌ Not Supported | |
| `$slice` (projection) | ❌ Not Supported | |
| `$meta` | ❌ Not Supported | No text search |

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
| `$bit` | ✅ Supported | `and`/`or`/`xor` on Int32/Int64; Int64 result if either side is Int64 |

### Array Update Operators

| Operator | Status | Notes |
|----------|--------|-------|
| `$push` | ✅ Supported | Modifiers: `$each`, `$position`, `$sort`, `$slice` |
| `$pull` | ✅ Supported | Remove matching elements |
| `$pullAll` | ✅ Supported | Remove all occurrences of specified values |
| `$addToSet` | ✅ Supported | Add without duplicates; `$each` modifier supported |
| `$pop` | ✅ Supported | Remove first (`-1`) or last (`1`) array element |
| `$` (positional) | ✅ Supported | First element matched by the query; resolved per document |
| `$[]` (all positional) | ✅ Supported | Works with every update operator, including nested (`a.$[].b.$[]`) |
| `$[<identifier>]` (filtered) | ✅ Supported | Via `arrayFilters`; nestable and mixable with `$[]` |

### Pipeline-Form Updates

The `update` and `findAndModify` commands and the native `update_one` /
`update_many` / `find_one_and_update` methods accept an aggregation pipeline
as the update (`u: [...]` / `UpdateModifications::Pipeline`), restricted to
`$addFields` / `$set` / `$project` / `$unset` / `$replaceRoot` /
`$replaceWith` exactly as MongoDB restricts them. `_id` is immutable.

---

## Aggregation Pipeline

An in-memory subset, available through the native
`Collection::aggregate(pipeline)` (returns raw `Document`s regardless of the
collection's type parameter) and the wire `aggregate` command (full cursor
protocol including `getMore`). A leading `$match` stage is index-accelerated
through the query planner; later stages evaluate in memory. All reads in one
aggregation (including `$lookup`) observe a single MVCC snapshot. Driver
`countDocuments()` implementations (which send `$match` + `$group`/`$sum`)
work over the wire.

### Stages

| Stage | Status | Notes |
|-------|--------|-------|
| `$match` | ✅ Supported | Same operator coverage as find filters; first stage uses indexes |
| `$sort` | ✅ Supported | Stable; same BSON ordering as find sort |
| `$skip` | ✅ Supported | |
| `$limit` | ✅ Supported | |
| `$project` | ✅ Supported | Include/exclude with dotted paths, plus computed fields via the full expression language |
| `$count` | ✅ Supported | |
| `$group` | ✅ Supported | `_id` and accumulator arguments accept the full expression language |
| `$addFields` / `$set` | ✅ Supported | Original-document snapshot semantics; dotted targets |
| `$unset` / `$replaceRoot` / `$replaceWith` | ✅ Supported | |
| `$unwind` | ✅ Supported | `includeArrayIndex`, `preserveNullAndEmptyArrays` |
| `$lookup` | ✅ Supported | Equality form (`localField`/`foreignField`), snapshot-consistent; `let`/pipeline form not supported |
| `$sortByCount` | ✅ Supported | |
| `$graphLookup` / `$unionWith` | ❌ Not Supported | |
| `$facet` / `$bucket` / `$bucketAuto` | ❌ Not Supported | |
| `$sample` | ❌ Not Supported | |
| `$out` / `$merge` | ❌ Not Supported | |
| `$geoNear` | ❌ Not Supported | Not planned (no geospatial) |
| `$densify` / `$fill` / `$setWindowFields` | ❌ Not Supported | |
| `$redact` / `$documents` / `$collStats` / `$indexStats` | ❌ Not Supported | |
| `$changeStream` | ❌ Not Supported | No change streams |
| `$search` / `$searchMeta` / `$vectorSearch` | ❌ Not Supported | Atlas-only in MongoDB |

### Group Accumulators

| Accumulator | Status | Notes |
|-------------|--------|-------|
| `$sum` | ✅ Supported | Ignores non-numeric values; `{$sum: 1}` counts documents |
| `$avg` | ✅ Supported | Ignores non-numeric; empty group yields `null`; result is Double |
| `$min` / `$max` | ✅ Supported | Ignore `null` and missing |
| `$first` / `$last` | ✅ Supported | Missing values yield `null` (matches server) |
| `$push` | ✅ Supported | Missing values contribute nothing |
| `$addToSet` | ✅ Supported | BSON-equality dedup; order unspecified |
| `$count` (accumulator) | ✅ Supported | Argument must be `{}` |
| `$stdDevPop` / `$stdDevSamp` | ✅ Supported | Welford; empty group yields `null` (`$stdDevPop` of one value is `0.0`) |
| `$mergeObjects` | ✅ Supported | Later fields overwrite; null/missing ignored |
| `$firstN` / `$lastN` / `$minN` / `$maxN` | ✅ Supported | `n` must be a positive integer constant (MongoDB allows expressions) |
| `$topN` / `$bottomN` / `$top` / `$bottom` | ❌ Not Supported | |
| `$median` / `$percentile` | ❌ Not Supported | |
| `$accumulator` | ❌ Not Supported | JavaScript; not planned |

### Expressions

The expression language is available in `$expr`, `$project`, `$addFields` /
`$set`, `$group` (`_id` and accumulator arguments), `$replaceRoot` /
`$replaceWith`, `$sortByCount`, and pipeline-form updates.

| Family | Operators |
|--------|-----------|
| Comparison | `$eq` `$ne` `$gt` `$gte` `$lt` `$lte` `$cmp` |
| Arithmetic | `$add` `$subtract` `$multiply` `$divide` `$mod` `$abs` `$ceil` `$floor` `$trunc` `$round` `$pow` `$sqrt` `$exp` `$ln` `$log10` |
| Boolean | `$and` `$or` `$not` |
| Conditional | `$cond` `$ifNull` `$switch` |
| String | `$concat` `$toUpper` `$toLower` `$strLenCP` `$substrCP` `$split` `$trim` `$ltrim` `$rtrim` `$toString` |
| Array | `$size` `$isArray` `$in` `$arrayElemAt` `$first` `$last` `$concatArrays` `$slice` `$filter` `$map` `$range` |
| Type | `$type` `$toInt` `$toLong` `$toDouble` `$toBool` `$toDate` |
| Date (UTC only) | `$year` `$month` `$dayOfMonth` `$hour` `$minute` `$second` `$millisecond` `$dayOfWeek` `$dayOfYear` |
| Other | `$literal` `$rand`; variables `$$ROOT` `$$CURRENT` `$$NOW` `$$this` (and `as`-bound) |

Not supported: `$convert`, `$dateToString` and timezone options, `$let`,
`$reduce`, `$zip`, `$objectToArray` / `$arrayToObject`, `$regexMatch`,
`$$REMOVE`, and any operator not listed above. Known divergences: expression
field paths do not collect values across arrays of documents (such a path
yields missing); `$trunc` takes one argument; `$toDate` accepts numeric
milliseconds only.

There is no 100 MB per-stage memory limit emulation: every stage materializes
in memory (embedded, single-node model).

---

## Wire Protocol Commands

> Requires the `wire` Cargo feature. See [README](../README.md) for setup.

### Diagnostic Commands

| Command | Status | Notes |
|---------|--------|-------|
| `hello` / `isMaster` | ✅ Supported | Returns topology info; `ismaster: true` |
| `ping` | ✅ Supported | Returns `{ok: 1}` |
| `endSessions` | ✅ Supported | No-op acknowledgement (drivers send it on close) |
| `buildInfo` | ✅ Supported | Returns mqlite version info |
| `serverStatus` | ✅ Supported | Returns connection + database stats |

### Database Commands

| Command | Status | Notes |
|---------|--------|-------|
| `listDatabases` | ✅ Supported | Returns list of databases in the file |
| `dropDatabase` | ✅ Supported | Drops every collection in the database; response includes `dropped` |

### CRUD Commands

| Command | Status | Notes |
|---------|--------|-------|
| `find` | ✅ Supported | filter, sort, limit, skip, projection, batchSize, hint (incl. `$natural`) |
| `insert` | ✅ Supported | `ordered` flag supported |
| `update` | ✅ Supported | `upsert`, `arrayFilters`, and pipeline-form updates (`u: [...]`) supported |
| `delete` | ✅ Supported | `deleteOne` and `deleteMany` via `limit` field |
| `findAndModify` | ✅ Supported | `new`, `upsert`, `sort`, `remove`, `arrayFilters`, pipeline updates |
| `aggregate` | ✅ Supported | Minimal stage subset; requires `cursor` option (see Aggregation Pipeline) |
| `getMore` | ✅ Supported | Cursor continuation |
| `killCursors` | ✅ Supported | Explicit cursor cleanup |
| `count` | ✅ Supported | `query`, `skip`, `limit` (negative limit = absolute value) |
| `distinct` | ✅ Supported | `key`, `query`; array values unwound per MongoDB semantics |
| `explain` | ✅ Supported | Inner `find` only; `queryPlanner` verbosity only (see Divergences) |

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
| `mapReduce` | ❌ Not Supported | Not planned |
| `currentOp` | ❌ Not Supported | |
| Authentication | ❌ Not Supported | See [WIRE-SECURITY.md](WIRE-SECURITY.md) |
| Replication commands | ❌ Not Supported | mqlite is standalone, not a replica set |
| `transaction` / `commitTransaction` | ❌ Not Supported | |

---

## Index Types

| Type | Status | Notes |
|------|--------|-------|
| Single-field | ✅ Supported | |
| Compound | ✅ Supported | |
| Unique | ✅ Supported | Enforced on insert and upsert |
| Sparse | ✅ Supported | Only indexes documents where the key field exists |
| Multikey (array fields) | ✅ Supported | Automatically applied when indexing array fields |
| TTL | ✅ Supported | `expireAfterSeconds` on single-field indexes; sweeps at open, on demand (`Client::sweep_expired`), and every 60 s under the wire server |
| Text | ❌ Not Supported | |
| Geospatial | ❌ Not Supported | Not planned |
| Hashed | ❌ Not Supported | Not planned |
| Partial | ✅ Supported | `partialFilterExpression` accepts any supported filter (superset of MongoDB); conservative planner subsumption |
| Wildcard | ❌ Not Supported | Not planned |

---

## Driver Compatibility

All drivers require `directConnection=true` because mqlite is a standalone node,
not a replica set or sharded cluster. Without this flag, drivers attempt topology
discovery (hello/isMaster polling) which mqlite does not support beyond the initial
handshake.

| Driver | Status | Required connection options |
|--------|--------|---------------------------|
| mongosh 2.x | ✅ Tested | `directConnection=true` (handshake + CRUD covered by `tests/wire_compat.rs` and `tests/mongosh_smoke.sh`) |
| pymongo 4.x | ✅ Tested | `directConnection=True` (covered by `tests/wire_compat.rs` and `tests/pymongo_compat.py`; disable compression: `compressors=[]`) |
| Node.js driver 6.x | 🔴 Untested | `directConnection: true` (expected to work for basic CRUD; change streams unsupported) |
| Motor (async pymongo) 3.x | 🔴 Untested | `directConnection=True` (expected to work for basic CRUD) |
| MongoDB Rust driver 3.x | 🔴 Untested | `directConnection=true` (mqlite's native API is sync; wrap in `tokio::task::spawn_blocking` when driving it from an async runtime) |
| Java driver 5.x | 🔴 Untested | `directConnection=true` (expected to work for basic CRUD) |
| Go driver 1.x | 🔴 Untested | `directConnection=true` (expected to work for basic CRUD) |

---

## Known Divergences from MongoDB 8.0

| Feature | MongoDB 8.0 | mqlite |
|---------|-------------|--------|
| `$regex` engine | PCRE2 (lookahead, lookbehind, named groups) | Rust `regex` crate (no lookahead/lookbehind, no backreferences) |
| Write concern | `w`, `j`, `wtimeout` | Ignored (all writes are single-writer committed) |
| Read concern | `local`, `majority`, `snapshot` | Ignored (MVCC snapshot per read) |
| Transactions | Multi-document ACID | Not supported |
| Change streams | `$changeStream` aggregation | Not supported |
| Aggregation pipeline | Full stage/expression language | Subset: 15 stages, 16 accumulators, broad expression language (see Aggregation Pipeline) |
| Filter dotted paths through arrays of documents | `{"items.k": "b"}` matches `{items: [{k: "b"}]}` | No match — use `$elemMatch` (leaf-level array unwrap still applies) |
| Expression field paths | Collect values across arrays of documents | Yield missing for paths into arrays of documents |
| `hint: {$natural: -1}` | Reverse collection scan | Forward scan (no reverse iteration) |
| Partial index planning | Logical implication of `partialFilterExpression` | Exact syntactic subsumption (conservative; equal conditions only) |
| TTL deletion | Continuous background monitor (60 s) | Sweep at open, on demand, and 60 s timer under the wire server |
| Projection path collision | Error (`Path collision at a.b`) | Last-spec-wins |
| `$firstN`/`$lastN`/`$minN`/`$maxN` `n` | Any expression | Positive integer constant |
| `ObjectId` generation | Server-generated | Client-generated (compatible format) |
| `_id` type enforcement | Any BSON type | Any BSON type |
| Capped collections | Fixed-size, oldest-doc-removal | Not supported |
| GridFS | Chunked file storage | Not supported |
| Authentication | SCRAM-SHA-256, x.509 | None (embedded trust model; see [WIRE-SECURITY.md](WIRE-SECURITY.md)) |
| `explain` output format | Rich query plan JSON; `executionStats` / `allPlansExecution` verbosities | Simplified `queryPlanner`-only output (`COLLSCAN` or `FETCH`+`IXSCAN`); all verbosities return the same shape |
