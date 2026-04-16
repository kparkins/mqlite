# mqlite Migration Guide

> Migrating from the **MongoDB Rust driver** (`mongodb` crate) to mqlite.

This guide provides side-by-side code comparisons and explains what transfers
directly, what needs adaptation, and what is not available in Phase 1.

---

## At a Glance

| | MongoDB Rust driver | mqlite |
|-|---------------------|--------|
| **API style** | Async (`async/await`) | Synchronous |
| **Connection model** | Remote server + connection pool | Embedded (in-process), one file |
| **Dependencies** | `tokio`, `mongodb` | `mqlite` (no runtime required) |
| **Entry point** | `Client::with_uri_str(uri).await?` | `Client::open("myapp.mqlite")?` |
| **Collection selector** | `client.database("mydb").collection("col")` | `client.database("mydb").collection("col")` |
| **BSON types** | `bson::doc!`, `bson::Document` | Same (mqlite re-exports from `bson`) |
| **Write concern** | Configurable (`w`, `j`, `wtimeout`) | Ignored (all writes committed) |
| **Read concern** | Configurable (`local`, `majority`) | Ignored (MVCC snapshot per read) |
| **Connection pool** | Yes (configurable pool size) | N/A (embedded, no network) |
| **Aggregation** | Full pipeline support | Not supported (Phase 2) |
| **Transactions** | Multi-document ACID | Not supported (Phase 2) |
| **Change streams** | `collection.watch()` | Not supported |
| **Authentication** | SCRAM-SHA-256, x.509 | None (file-level OS permissions) |

---

## Opening the Database

### MongoDB Rust driver (async)

```rust
use mongodb::{Client, options::ClientOptions};

#[tokio::main]
async fn main() -> mongodb::error::Result<()> {
    let client = Client::with_uri_str("mongodb://localhost:27017").await?;

    // Or with options:
    let mut opts = ClientOptions::parse("mongodb://localhost:27017").await?;
    opts.max_pool_size = Some(10);
    let client = Client::with_options(opts)?;

    Ok(())
}
```

### mqlite (sync)

```rust
use mqlite::{Client, OpenOptions, DurabilityMode};
use std::time::Duration;

fn main() -> mqlite::Result<()> {
    // Simple open (creates file if it doesn't exist)
    let client = Client::open("myapp.mqlite")?;

    // With options:
    let client = Client::open_with_options(
        "myapp.mqlite",
        OpenOptions::new()
            .busy_timeout(Duration::from_secs(10))
            .durability(DurabilityMode::FullSync),
    )?;

    // Temp-file (useful for tests — see docs/TEST-DOUBLE-COOKBOOK.md):
    use tempfile::TempDir;
    let tempdir = TempDir::new()?;
    let client = Client::open(tempdir.path().join("db.mqlite"))?;

    Ok(())
}
```

**What's different:**
- No `async/await` — the open is synchronous
- The connection string is replaced by a file path
- There is no connection pool — `Client` is a handle to a local file
- `Client` is cheaply clonable (`Clone`, `Send`, `Sync`); share it across threads

---

## Selecting a Collection

### MongoDB Rust driver

```rust
use mongodb::{Client, Collection};
use bson::Document;

async fn setup() -> mongodb::error::Result<()> {
    let client = Client::with_uri_str("mongodb://localhost:27017").await?;

    // Typed collection
    let users: Collection<User> = client.database("mydb").collection("users");

    // Untyped (Document)
    let users: Collection<Document> = client.database("mydb").collection("users");

    Ok(())
}
```

### mqlite

```rust
use mqlite::{Client, Collection};
use bson::Document;

fn setup() -> mqlite::Result<()> {
    let client = Client::open("myapp.mqlite")?;
    let db = client.database("mydb");

    // Typed collection
    let users = db.collection::<User>("users");

    // Untyped (Document)
    let users = db.collection::<Document>("users");

    Ok(())
}
```

**What's different:**
- The `client.database("mydb")` selector is the same API shape as the MongoDB driver
- Each `.mqlite` file supports multiple named database namespaces

---

## CRUD Operations

### Insert One

| | MongoDB Rust driver | mqlite |
|-|---------------------|--------|
| Method | `collection.insert_one(doc, None).await?` | `collection.insert_one(&doc)?` |
| Takes | Owned value | Reference |
| Returns | `InsertOneResult` | `InsertOneResult` |
| `_id` | Auto-generated `ObjectId` if absent | Auto-generated `ObjectId` if absent |

```rust
// MongoDB Rust driver
let result = collection.insert_one(
    User { name: "Alice".into(), email: "alice@example.com".into() },
    None,
).await?;
println!("Inserted _id: {}", result.inserted_id);

// mqlite
let result = collection.insert_one(
    &User { name: "Alice".into(), email: "alice@example.com".into() },
)?;
println!("Inserted _id: {}", result.inserted_id);
```

### Insert Many

| | MongoDB Rust driver | mqlite |
|-|---------------------|--------|
| Method | `collection.insert_many(docs, None).await?` | `collection.insert_many(&docs)?` |
| Ordered | Default true, configurable | Default true, configurable |

```rust
let docs = vec![
    doc! { "name": "Alice" },
    doc! { "name": "Bob" },
];

// MongoDB Rust driver
let result = collection.insert_many(&docs, None).await?;

// mqlite
let result = collection.insert_many(&docs)?;
println!("Inserted {} documents", result.inserted_ids.len());
```

### Find One

| | MongoDB Rust driver | mqlite |
|-|---------------------|--------|
| Method | `collection.find_one(filter, None).await?` | `collection.find_one(filter)?` |
| Returns | `Option<T>` | `Option<T>` |

```rust
// MongoDB Rust driver
let user = collection
    .find_one(doc! { "email": "alice@example.com" }, None)
    .await?;

// mqlite
let user = collection.find_one(doc! { "email": "alice@example.com" })?;
```

### Find Many

| | MongoDB Rust driver | mqlite |
|-|---------------------|--------|
| Method | `collection.find(filter, None).await?` | `collection.find(filter)?` |
| Returns | `Cursor<T>` | `Cursor<T>` |
| Iteration | `cursor.try_next().await?` | `cursor.next()` (sync Iterator) |

```rust
// MongoDB Rust driver (async cursor)
use futures::TryStreamExt;

let mut cursor = collection.find(doc! { "status": "active" }, None).await?;
while let Some(doc) = cursor.try_next().await? {
    println!("{doc:?}");
}

// mqlite (sync Iterator)
let cursor = collection.find(doc! { "status": "active" })?;
for doc in cursor {
    let doc = doc?;
    println!("{doc:?}");
}

// mqlite: collect into Vec
let users: Vec<_> = collection
    .find(doc! { "status": "active" })?
    .collect::<mqlite::Result<Vec<_>>>()?;
```

### Find with Options

```rust
use mqlite::options::FindOptions;

// MongoDB Rust driver
use mongodb::options::FindOptions as MongoFindOptions;
let opts = MongoFindOptions::builder()
    .sort(doc! { "name": 1 })
    .limit(10)
    .skip(20)
    .build();
let cursor = collection.find(doc! {}, opts).await?;

// mqlite
let opts = FindOptions::new()
    .sort(doc! { "name": 1 })
    .limit(10)
    .skip(20);
let cursor = collection.find_with_options(doc! {}, opts)?;
```

### Update One

| | MongoDB Rust driver | mqlite |
|-|---------------------|--------|
| Method | `collection.update_one(filter, update, None).await?` | `collection.update_one(filter, update)?` |
| Returns | `UpdateResult` | `UpdateResult` |
| Upsert | Via options | Via `update_one_with_options` |

```rust
// MongoDB Rust driver
let result = collection
    .update_one(
        doc! { "email": "alice@example.com" },
        doc! { "$set": { "status": "inactive" } },
        None,
    )
    .await?;
println!("Modified: {}", result.modified_count);

// mqlite
let result = collection.update_one(
    doc! { "email": "alice@example.com" },
    doc! { "$set": { "status": "inactive" } },
)?;
println!("Modified: {}", result.modified_count);
```

### Update One with Upsert

```rust
use mqlite::options::UpdateOptions;

// MongoDB Rust driver
use mongodb::options::UpdateOptions as MongoUpdateOptions;
let opts = MongoUpdateOptions::builder().upsert(true).build();
collection.update_one(filter, update, opts).await?;

// mqlite
collection.update_one_with_options(
    doc! { "email": "alice@example.com" },
    doc! { "$setOnInsert": { "created_at": bson::DateTime::now() }, "$set": { "name": "Alice" } },
    UpdateOptions::new().upsert(true),
)?;
```

### Update Many

```rust
// MongoDB Rust driver
collection.update_many(
    doc! { "status": "pending" },
    doc! { "$set": { "status": "processed" } },
    None,
).await?;

// mqlite
collection.update_many(
    doc! { "status": "pending" },
    doc! { "$set": { "status": "processed" } },
)?;
```

### Delete One / Delete Many

```rust
// MongoDB Rust driver
collection.delete_one(doc! { "_id": id }, None).await?;
collection.delete_many(doc! { "status": "archived" }, None).await?;

// mqlite
collection.delete_one(doc! { "_id": id })?;
collection.delete_many(doc! { "status": "archived" })?;
```

### Find One and Update (findAndModify)

```rust
// MongoDB Rust driver
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};
let opts = FindOneAndUpdateOptions::builder()
    .return_document(ReturnDocument::After)
    .upsert(true)
    .build();
let doc = collection.find_one_and_update(filter, update, opts).await?;

// mqlite
use mqlite::options::{FindOneAndUpdateOptions, ReturnDocument};
let doc = collection.find_one_and_update_with_options(
    filter,
    update,
    FindOneAndUpdateOptions::new()
        .return_document(ReturnDocument::After)
        .upsert(true),
)?;
```

### Count Documents

```rust
// MongoDB Rust driver
let count = collection.count_documents(doc! { "status": "active" }, None).await?;

// mqlite
let count = collection.count_documents(doc! { "status": "active" })?;

// Fast approximate count (no filter — reads metadata only):
let approx = collection.estimated_document_count()?;
```

---

## Indexes

```rust
use mqlite::{IndexModel, options::IndexOptions, doc};

// MongoDB Rust driver
use mongodb::IndexModel as MongoIndexModel;
use mongodb::options::IndexOptions as MongoIndexOptions;

let model = MongoIndexModel::builder()
    .keys(doc! { "email": 1 })
    .options(MongoIndexOptions::builder().unique(true).build())
    .build();
collection.create_index(model, None).await?;

// mqlite
let model = IndexModel {
    keys: doc! { "email": 1 },
    options: Some(IndexOptions::new().unique(true)),
};
collection.create_index(model)?;

// List indexes
let indexes = collection.list_indexes()?;

// Drop an index by name
collection.drop_index("email_1")?;
```

---

## BSON Types

mqlite re-exports the `bson` crate. All BSON types transfer directly:

| Type | Both use |
|------|----------|
| Documents | `bson::Document`, `bson::doc!` |
| ObjectId | `bson::oid::ObjectId` |
| DateTime | `bson::DateTime` |
| Binary | `bson::Binary` |
| Regex | `bson::Regex` |
| Decimal128 | `bson::Decimal128` |

```rust
// This code works unchanged in both MongoDB driver and mqlite:
use bson::{doc, DateTime, oid::ObjectId};

let doc = doc! {
    "_id": ObjectId::new(),
    "created_at": DateTime::now(),
    "tags": ["rust", "database"],
};
```

---

## Async vs Sync

The most significant API difference is that the MongoDB Rust driver is fully
async while mqlite is synchronous.

### Strategy 1: `spawn_blocking` (recommended for async apps)

```rust
use mqlite::Client;

async fn find_user(client: Client, email: String) -> mqlite::Result<Option<bson::Document>> {
    tokio::task::spawn_blocking(move || {
        let col = client.database("mydb").collection::<bson::Document>("users");
        col.find_one(bson::doc! { "email": email })
    })
    .await
    .expect("thread panicked")
}
```

### Strategy 2: Sync wrapper module

Centralize the `spawn_blocking` calls in a single module:

```rust
use mqlite::{Client, doc};

pub struct UserStore {
    client: Client,
}

impl UserStore {
    pub async fn get_by_email(&self, email: &str) -> mqlite::Result<Option<bson::Document>> {
        let client = self.client.clone();
        let email = email.to_owned();
        tokio::task::spawn_blocking(move || {
            client.database("mydb")
                .collection::<bson::Document>("users")
                .find_one(doc! { "email": email })
        })
        .await
        .unwrap()
    }
}
```

### Strategy 3: Sync application boundary

If only your database layer is sync (common in CLI tools or services that were
already sync), you can skip `spawn_blocking` entirely and call mqlite directly.

---

## Wire Protocol Connection (mongosh / pymongo)

If you use the `wire` feature to connect external tools (mongosh, pymongo),
note the required connection string change:

```bash
# MongoDB
mongosh "mongodb://localhost:27017/"
mongosh "mongodb://localhost:27017/mydb"

# mqlite — directConnection=true is REQUIRED
mongosh "mongodb://localhost:27017/?directConnection=true"
```

```python
# MongoDB
client = MongoClient("mongodb://localhost:27017/")

# mqlite — directConnection=True is REQUIRED
client = MongoClient("mongodb://localhost:27017/?directConnection=True")
# or equivalently:
client = MongoClient("localhost", 27017, directConnection=True)
```

`directConnection=true` is required because mqlite is a standalone node with
no replica set or sharding support. Without it, drivers attempt topology
discovery which mqlite does not support beyond the initial handshake.

See [WIRE-SECURITY.md](WIRE-SECURITY.md) for authentication and network security notes.

---

## What Transfers Directly

These patterns work identically in both the MongoDB driver and mqlite:

- BSON document construction (`bson::doc!`)
- All BSON types (`ObjectId`, `DateTime`, `Binary`, `Regex`, etc.)
- Filter operators: `$eq`, `$gt`, `$lt`, `$in`, `$and`, `$or`, `$regex`, etc.
- Update operators: `$set`, `$unset`, `$inc`, `$push`, `$pull`, `$addToSet`, etc.
- Sort documents (`doc! { "field": 1 }` for ascending, `-1` for descending)
- Projection documents (`doc! { "field": 1 }` to include, `0` to exclude)
- Upsert pattern (`update_one_with_options` + `UpdateOptions::upsert(true)`)
- `find_one_and_update` / `find_one_and_delete` / `find_one_and_replace`
- Index creation (single-field, compound, unique, sparse)
- Object model hierarchy: `Client::open(path)` → `client.database(name)` → `db.collection::<T>(name)`

---

## What's Different

| Feature | MongoDB Rust driver | mqlite |
|---------|---------------------|--------|
| API style | `async/await` | Synchronous |
| Entry point | `Client::with_uri_str(uri)` | `Client::open(path)` |
| Database selector | `client.database("mydb")` | `client.database("mydb")` |
| Options parameter | Always present (even `None`) | Only `*_with_options` variants |
| `insert_one` argument | Owned value | Reference (`&T`) |
| `find` result iteration | `cursor.try_next().await?` | `for doc in cursor` (sync Iterator) |
| Write concern | Configurable per operation | Ignored |
| Read concern | Configurable per operation | Ignored (MVCC snapshot) |
| Error type | `mongodb::error::Error` | `mqlite::Error` |
| Cargo feature for types | `mongodb::bson` | `mqlite::bson` (re-exported) |

---

## What's Missing (Phase 1 limitations)

| Feature | Status | Workaround |
|---------|--------|-----------|
| Aggregation pipeline | ❌ Not supported | Use `find` + process in Rust |
| `$lookup` (joins) | ❌ Not supported | Perform separate queries |
| `$group` / `$sum` | ❌ Not supported | Aggregate in application code |
| Multi-document transactions | ❌ Not supported | Design for single-document atomicity |
| Change streams | ❌ Not supported | Poll with `find` + a high-water mark |
| Full-text search (`$text`) | ❌ Not supported | Use `$regex` or external indexing |
| `distinct` | ❌ Not supported | Use `find` + deduplicate in Rust |
| `explain` (via wire) | ❌ Not supported | Use `collection.find(…)?.explain()` in Rust |
| Authentication | ❌ Not supported | Use OS file permissions |
| Replica set / sharding | ❌ Not planned | mqlite is embedded/standalone |

---

## Cargo.toml Changes

```toml
# Before (MongoDB driver)
[dependencies]
mongodb = "3"
bson = "2"
tokio = { version = "1", features = ["full"] }

# After (mqlite, sync application)
[dependencies]
mqlite = "0.1"
# bson is re-exported from mqlite — no separate dependency needed

# After (mqlite, async application)
[dependencies]
mqlite = "0.1"
tokio = { version = "1", features = ["full"] }  # for spawn_blocking

# With wire protocol support (mongosh/pymongo access):
[dependencies]
mqlite = { version = "0.1", features = ["wire"] }
```

---

## Upgrading mqlite

### 0.x → 1.0 (future)

The 0.x API is not yet stable. Breaking changes may occur in minor versions.
Check the [CHANGELOG](../CHANGELOG.md) before upgrading.
