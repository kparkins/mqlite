# mqlite Compatibility Matrix

## MongoDB Version

mqlite targets MongoDB 6.0 wire protocol semantics.

## Query Operators

### Filter Operators

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$eq` | ✅ | |
| `$ne` | ✅ | |
| `$gt` / `$gte` | ✅ | |
| `$lt` / `$lte` | ✅ | |
| `$in` | ✅ | |
| `$nin` | ✅ | |
| `$and` | ✅ | |
| `$or` | ✅ | |
| `$nor` | ✅ | |
| `$not` | ✅ | |
| `$exists` | ✅ | |
| `$type` | ✅ | |
| `$all` | ✅ | |
| `$elemMatch` | ✅ | |
| `$regex` | ✅ | |
| `$where` | ❌ | Not planned (security) |
| `$text` | ❌ | Planned Phase 2 |
| `$near` / `$geoWithin` | ❌ | Not planned |
| `$expr` | ❌ | Planned Phase 2 |

### Update Operators

| Operator | Supported | Notes |
|----------|-----------|-------|
| `$set` | ✅ | |
| `$unset` | ✅ | |
| `$inc` | ✅ | |
| `$push` | ✅ | |
| `$pull` | ✅ | |
| `$addToSet` | ✅ | |
| `$rename` | ✅ | |
| `$mul` | ✅ | |
| `$min` / `$max` | ✅ | |
| `$currentDate` | ❌ | Planned Phase 2 |
| `$bit` | ❌ | Not planned |

## Index Types

| Type | Supported | Notes |
|------|-----------|-------|
| Single-field | ✅ | |
| Compound | ✅ | |
| Unique | ✅ | |
| Sparse | ✅ | |
| Multikey (array fields) | ✅ | |
| TTL | ❌ | Planned Phase 2 |
| Text | ❌ | Planned Phase 2 |
| Geospatial | ❌ | Not planned |
| Hashed | ❌ | Not planned |

## Wire Protocol Commands (requires `wire` feature)

| Command | Supported | Notes |
|---------|-----------|-------|
| `hello` / `isMaster` | ✅ | |
| `ping` | ✅ | |
| `buildInfo` | ✅ | |
| `serverStatus` | ✅ | |
| `listDatabases` | ✅ | |
| `find` | ✅ | |
| `insert` | ✅ | |
| `update` | ✅ | |
| `delete` | ✅ | |
| `aggregate` | ❌ | Planned Phase 2 |
| Authentication | ❌ | See [WIRE-SECURITY.md](WIRE-SECURITY.md) |

## Driver Compatibility

| Driver | Supported | Required options |
|--------|-----------|-----------------|
| pymongo 4.x | ✅ | `directConnection=True` |
| mongosh 2.x | ✅ | `directConnection=true` |
| Node.js driver | 🔶 Partial | `directConnection: true` |
| Motor (async pymongo) | 🔶 Partial | `directConnection=True` |

> **Note:** All drivers require `directConnection=true` because mqlite is a
> standalone node, not a replica set or sharded cluster.
