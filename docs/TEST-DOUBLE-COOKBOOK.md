# mqlite Test Double Cookbook

Drop-in replacement for MongoDB in your test suite — no containers, no ports.

> **Note:** Previously mqlite exposed `Client::open_in_memory()`. That API is
> removed; use `tempfile::TempDir` + `Client::open` instead (shown throughout
> this guide).

---

## Why mqlite in Tests?

| | MongoDB in tests | mqlite in tests |
|-|-----------------|-----------------|
| **Startup** | Docker pull + container start (seconds) | `TempDir::new()` + `Client::open` (microseconds) |
| **Isolation** | Shared container or per-test client | Each test gets its own `Client` |
| **Cleanup** | `db.drop()` or container teardown | Automatic on `Drop` — no cleanup code |
| **CI** | Requires Docker daemon | Zero external dependencies |
| **Parallelism** | Shared state unless isolated carefully | Each `TempDir` is always isolated |
| **Wire compatibility** | Full MongoDB | Phase 1 operator set (see [Compatibility Matrix](COMPATIBILITY.md)) |

The `tempfile::TempDir` + `Client::open` pattern is designed for exactly this
use case. Each call creates a fresh, empty database backed by a temporary
directory that is automatically deleted when the `TempDir` handle is dropped —
there is nothing to clean up.

---

## Basic Test Setup

```rust
use mqlite::{Client, doc};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct User {
    name: String,
    email: String,
    active: bool,
}

#[test]
fn user_lookup_by_email() {
    // Open a fresh temp-file database — no ports, no cleanup.
    let tempdir = TempDir::new().expect("create tempdir");
    let client = Client::open(tempdir.path().join("db.mqlite"))
        .expect("open tempdir-backed client");
    let db = client.database("test");
    let users = db.collection::<User>("users");

    users.insert_one(&User {
        name: "Alice".into(),
        email: "alice@example.com".into(),
        active: true,
    }).unwrap();

    let found = users.find_one(doc! { "email": "alice@example.com" }).unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().name, "Alice");
}
// `tempdir` is dropped here — the temp directory is deleted automatically.
```

**Key points:**
- `TempDir::new()` creates an OS temp directory; `Client::open` on a path
  inside it practically never fails. Use descriptive `.expect` messages for
  clarity.
- No `#[teardown]`, no `after_each` hook. The `TempDir` handle owns cleanup.
- Keep `tempdir` alive for the duration of the test — dropping it early deletes
  the database files while the client still holds them open.
- `Client`, `Database`, and `Collection<T>` are all `Clone + Send + Sync` — move
  them into closures or helper functions freely.

---

## Drop as the Cleanup Hook

Temp-file databases clean up automatically when the `TempDir` handle is
dropped. There is no action required:

```rust
#[test]
fn database_is_self_cleaning() {
    let tempdir = TempDir::new().unwrap();
    let client = Client::open(tempdir.path().join("db.mqlite")).unwrap();
    let col = client.database("test").collection::<mqlite::Document>("items");
    col.insert_one(&doc! { "x": 1 }).unwrap();
    // `col`, `client`, and `tempdir` are dropped at the end of the block.
    // The temp directory (and its database files) is deleted automatically.
}
```

This means you can open a `Client` in a test helper function, pass a
`Collection` to the system under test, and the database files are automatically
removed when both `client` and `tempdir` go out of scope.

---

## Fixture Loading

### Inline fixtures with `doc!`

For small fixture sets, embed data directly in the test:

```rust
#[test]
fn order_totals_are_correct() {
    let tempdir = TempDir::new().unwrap();
    let client = Client::open(tempdir.path().join("db.mqlite")).unwrap();
    let orders = client.database("test").collection::<mqlite::Document>("orders");

    orders.insert_many(&[
        doc! { "customer": "alice", "amount": 120_i32, "status": "paid" },
        doc! { "customer": "bob",   "amount":  45_i32, "status": "pending" },
        doc! { "customer": "alice", "amount":  80_i32, "status": "paid" },
    ]).unwrap();

    let paid_count = orders.count_documents(doc! { "status": "paid" }).unwrap();
    assert_eq!(paid_count, 2);
}
```

### Loading from JSON fixture files with `include_str!`

For larger fixture sets, store JSON files alongside your tests and embed them
at compile time:

```json
// tests/fixtures/products.json
[
  { "_id": { "$oid": "000000000000000000000001" }, "sku": "WIDGET-A", "price": 9.99, "stock": 100 },
  { "_id": { "$oid": "000000000000000000000002" }, "sku": "WIDGET-B", "price": 19.99, "stock": 50 },
  { "_id": { "$oid": "000000000000000000000003" }, "sku": "GADGET-X", "price": 49.99, "stock": 25 }
]
```

```rust
use mqlite::{Client, Document};
use tempfile::TempDir;

fn load_fixture(db: &mqlite::Database, collection: &str, json: &str) {
    let docs: Vec<Document> = serde_json::from_str::<Vec<serde_json::Value>>(json)
        .expect("fixture is valid JSON")
        .into_iter()
        .map(|v| bson::from_bson(bson::to_bson(&v).unwrap()).unwrap())
        .collect();
    db.collection::<Document>(collection)
        .insert_many(&docs)
        .expect("fixture insert");
}

#[test]
fn low_stock_query() {
    let tempdir = TempDir::new().unwrap();
    let client = Client::open(tempdir.path().join("db.mqlite")).unwrap();
    let db = client.database("test");
    load_fixture(&db, "products", include_str!("fixtures/products.json"));

    let products = db.collection::<Document>("products");
    let low_stock = products.count_documents(doc! { "stock": { "$lt": 60_i32 } }).unwrap();
    assert_eq!(low_stock, 2); // WIDGET-B (50) and GADGET-X (25)
}
```

**`include_str!` tip:** The path is relative to the source file containing the
macro call. Files embedded with `include_str!` are tracked by the compiler —
changing the fixture file triggers a recompile.

### Loading from BSON fixture files

If your fixtures are stored as BSON (e.g., exported with `mongoexport --type bson`):

```rust
fn load_bson_fixture(db: &mqlite::Database, collection: &str, bytes: &[u8]) {
    let mut reader = bson::de::Deserializer::new(std::io::Cursor::new(bytes));
    let mut docs = Vec::new();
    while let Ok(doc) = Document::from_reader(&mut std::io::Cursor::new(bytes)) {
        docs.push(doc);
        // advance past the read document
    }
    db.collection::<Document>(collection).insert_many(&docs).expect("fixture insert");
}
```

For most test suites JSON fixtures are simpler — BSON fixtures are mainly
useful when preserving exact BSON types (e.g., `Date`, `Decimal128`) matters.

---

## Parallel Test Isolation

Rust's default test runner runs unit tests on multiple threads. Because each
`TempDir::new()` call creates a completely independent directory, parallel
tests need no synchronization at all:

```rust
// All three tests run in parallel — each has its own database.

#[test]
fn test_a() {
    let tempdir = TempDir::new().unwrap();
    let client = Client::open(tempdir.path().join("db.mqlite")).unwrap();
    let col = client.database("test").collection::<mqlite::Document>("items");
    col.insert_one(&doc! { "key": "a" }).unwrap();
    assert_eq!(col.count_documents(doc! {}).unwrap(), 1);
}

#[test]
fn test_b() {
    let tempdir = TempDir::new().unwrap();
    let client = Client::open(tempdir.path().join("db.mqlite")).unwrap();
    let col = client.database("test").collection::<mqlite::Document>("items");
    // Zero documents — test_a's insert is invisible here.
    assert_eq!(col.count_documents(doc! {}).unwrap(), 0);
}

#[test]
fn test_c() {
    let tempdir = TempDir::new().unwrap();
    let client = Client::open(tempdir.path().join("db.mqlite")).unwrap();
    let col = client.database("test").collection::<mqlite::Document>("items");
    col.insert_many(&[
        doc! { "val": 1_i32 },
        doc! { "val": 2_i32 },
        doc! { "val": 3_i32 },
    ]).unwrap();
    assert_eq!(col.count_documents(doc! {}).unwrap(), 3);
}
```

**No `#[serial]` attribute required.** Each `TempDir` is a separate path on
disk and databases do not share any in-process state. This is the primary
reason to prefer this pattern over a shared MongoDB container in tests.

---

## Asserting Against Query Results

### `find_one` — exact match

```rust
let user = users.find_one(doc! { "email": "alice@example.com" }).unwrap();
assert!(user.is_some(), "user must exist");
let user = user.unwrap();
assert_eq!(user.name, "Alice");
```

### `count_documents` — count matching records

```rust
let active_count = users.count_documents(doc! { "active": true }).unwrap();
assert_eq!(active_count, 3, "expected exactly 3 active users");
```

### `find` — iterate multiple results

```rust
use mqlite::options::FindOptions;

let mut cursor = users.find(doc! { "active": true }).unwrap();
let mut names: Vec<String> = Vec::new();
while let Some(user) = cursor.next().unwrap() {
    names.push(user.name);
}
names.sort();
assert_eq!(names, ["Alice", "Bob", "Carol"]);
```

### Checking that a document was *not* inserted

```rust
let missing = users.find_one(doc! { "email": "nobody@example.com" }).unwrap();
assert!(missing.is_none(), "should not find a non-existent user");
```

---

## Replacing MongoDB in Tests

If your production code uses the MongoDB Rust driver, you can swap in mqlite
by abstracting over the storage layer. A simple approach is a trait:

```rust
// In your library:
pub trait UserStore: Send + Sync {
    fn find_by_email(&self, email: &str) -> Option<User>;
    fn insert(&self, user: &User) -> mqlite::Result<()>;
}

// MongoDB implementation (async, in prod):
// pub struct MongoUserStore { ... }

// mqlite implementation (sync, in tests):
pub struct MqliteUserStore {
    col: mqlite::Collection<User>,
    // Keep tempdir alive so the database files are not deleted while in use.
    _tempdir: tempfile::TempDir,
}

impl MqliteUserStore {
    pub fn new() -> Self {
        let tempdir = tempfile::TempDir::new().unwrap();
        let client = Client::open(tempdir.path().join("db.mqlite")).unwrap();
        MqliteUserStore {
            col: client.database("test").collection("users"),
            _tempdir: tempdir,
        }
    }
}

impl UserStore for MqliteUserStore {
    fn find_by_email(&self, email: &str) -> Option<User> {
        self.col.find_one(doc! { "email": email }).unwrap()
    }
    fn insert(&self, user: &User) -> mqlite::Result<()> {
        self.col.insert_one(user).map(|_| ())
    }
}
```

### What changes vs MongoDB

| MongoDB driver | mqlite |
|----------------|--------|
| `async/await` throughout | Synchronous — no `.await` |
| `Client::with_uri_str(uri).await?` | `TempDir::new()` + `Client::open(path)?` |
| `db.collection::<T>("name")` | Same |
| `col.find_one(filter, None).await?` | `col.find_one(filter)?` (no `None`, no await) |
| `col.insert_one(doc, None).await?` | `col.insert_one(&doc)?` |

### What stays the same

- The `bson::doc!` macro and all BSON types.
- Collection and filter semantics (same operator names and behavior).
- Error handling patterns (`.unwrap()` or `?` on `Result<T, Error>`).
- Serde `Serialize`/`Deserialize` derives on your model types.

---

## Deterministic ObjectId Assignment in Tests

mqlite auto-generates `ObjectId` values for `_id` fields not explicitly set.
These are time-based and non-deterministic. For snapshot tests or tests that
assert on `_id` values, assign them explicitly:

```rust
use mqlite::{Client, ObjectId, doc};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct Item {
    #[serde(rename = "_id")]
    id: ObjectId,
    name: String,
}

#[test]
fn deterministic_id_assignment() {
    let tempdir = TempDir::new().unwrap();
    let client = Client::open(tempdir.path().join("db.mqlite")).unwrap();
    let items = client.database("test").collection::<Item>("items");

    // Construct predictable ObjectId values from hex strings.
    let id1 = ObjectId::parse_str("000000000000000000000001").unwrap();
    let id2 = ObjectId::parse_str("000000000000000000000002").unwrap();

    items.insert_many(&[
        Item { id: id1, name: "first".into() },
        Item { id: id2, name: "second".into() },
    ]).unwrap();

    // Now `_id` values are deterministic — safe to assert on.
    let item = items.find_one(doc! { "_id": id1 }).unwrap().unwrap();
    assert_eq!(item.name, "first");
}
```

> **Note:** Seeded ObjectId generation is planned for Phase 2. Until then,
> explicit `_id` assignment is the recommended approach for deterministic tests.

---

## Known Divergences from MongoDB

These are the cases where mqlite behaves differently from MongoDB in ways that
**matter in tests**. The full list is in [COMPATIBILITY.md](COMPATIBILITY.md).

### Unsupported operators fail immediately

mqlite returns `Error::UnsupportedOperator` for operators outside the Phase 1
set. This is **good for tests**: you find out instantly if you use an operator
that won't work in your deployment, rather than silently getting wrong results.

```rust
let result = col.find(doc! { "name": { "$where": "this.name == 'alice'" } });
assert!(matches!(result, Err(mqlite::Error::UnsupportedOperator { .. })));
```

Unsupported operators in Phase 1: `$where`, `$expr`, `$jsonSchema`, `$mod`,
`$text`, `$near`, `$geoWithin`, and the aggregation pipeline. See the
[Compatibility Matrix](COMPATIBILITY.md) for the complete list.

### `$regex` uses Rust's `regex` crate, not PCRE

- **No lookahead / lookbehind** (`(?=...)`, `(?!...)`, `(?<=...)`, `(?<!...)`)
- **No backreferences** (`\1`, `\k<name>`)
- Supported flags: `i`, `m`, `s`, `x`

If your production queries use PCRE-only features, your tests will fail on
those queries in mqlite, which is the correct signal.

### No aggregation pipeline

`$group`, `$lookup`, `$unwind`, `$project` etc. are not supported. If your
service uses aggregations, either:
1. Test the aggregation logic separately against a real MongoDB container.
2. Rewrite the logic as application-level code that uses `find` + `collect`.

### No multi-document transactions

`Session::start_transaction()` is not available. Test transaction-level
behavior against a real MongoDB instance.

### ObjectId auto-generation is time-based

`_id` fields that are omitted get a real `ObjectId` with the current timestamp.
This is non-deterministic. Assign explicit IDs (as shown above) for tests that
assert on `_id` values.
