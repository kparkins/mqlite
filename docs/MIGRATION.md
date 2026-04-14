# mqlite Migration Guide

## Migrating from MongoDB

mqlite is API-compatible with common MongoDB patterns. The main differences are:

### Connection

| MongoDB | mqlite |
|---------|--------|
| `MongoClient("mongodb://localhost:27017")` | `Database::open("myapp.mqlite")` |
| Connection string | File path |
| Server process required | Embedded — no server |

### Collections

```rust
// MongoDB (Rust driver)
let collection = client.database("mydb").collection::<User>("users");

// mqlite
let collection = db.collection::<User>("users");
```

mqlite has a single database per file. There is no `database()` selector.

### CRUD

| Operation | MongoDB Rust driver | mqlite |
|-----------|---------------------|--------|
| Insert one | `collection.insert_one(doc, None)` | `collection.insert_one(&doc)` |
| Find one | `collection.find_one(filter, None)` | `collection.find_one(filter)` |
| Update one | `collection.update_one(filter, update, None)` | `collection.update_one(filter, update)` |
| Delete one | `collection.delete_one(filter, None)` | `collection.delete_one(filter)` |
| Find many | `collection.find(filter, None)` | `collection.find(filter)` |

The key differences:
- mqlite is synchronous; MongoDB driver is async (returns `Future`)
- mqlite passes documents by reference to `insert_one`
- No `options` parameter needed for basic operations (use `*_with_options` variants)

### Async vs Sync

mqlite's core API is synchronous. If you're migrating from an async codebase,
wrap mqlite calls in `tokio::task::spawn_blocking`:

```rust
let result = tokio::task::spawn_blocking(move || {
    collection.find_one(filter)
}).await??;
```

### Wire Protocol (mongosh / pymongo)

If you use the `wire` feature to connect external tools:

```
# MongoDB
mongosh "mongodb://localhost:27017/"

# mqlite (directConnection=true required)
mongosh "mongodb://localhost:27017/?directConnection=true"
```

The `directConnection=true` option is **required** because mqlite is a
standalone node, not a replica set. Without it, drivers attempt topology
discovery which mqlite does not support.

## Upgrading mqlite

### 0.x → 1.0 (future)

The 0.x API is not yet stable. Breaking changes may occur in minor versions.
Check the [CHANGELOG](../CHANGELOG.md) before upgrading.
