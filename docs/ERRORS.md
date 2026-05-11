# mqlite Error Guide

All mqlite operations return `mqlite::Result<T>`, which is `Result<T, mqlite::Error>`.

```rust
use mqlite::{Client, Error};

let client = Client::open("myapp.mqlite")?;
let users = client.database("myapp").collection::<bson::Document>("users");

match users.find_one(bson::doc! { "_id": "missing" }) {
    Ok(Some(doc)) => println!("found: {doc:?}"),
    Ok(None)      => println!("not found"),
    Err(e)        => {
        eprintln!("error: {e}");
        // Access MongoDB-compatible error code if available:
        if let Some(code) = e.code() {
            eprintln!("MongoDB error code: {code}");
        }
    }
}
```

---

## Error Variants

### `Error::Io`

An OS-level I/O error (permissions, disk full, file not found).

**Common causes:**
- Path to the database directory does not exist
- Process lacks read/write permission on the database file or its directory
- The underlying storage device was removed

**Recovery:**
```rust
use mqlite::{Client, Error};
use std::io::ErrorKind;

match Client::open("myapp.mqlite") {
    Err(Error::Io(e)) if e.kind() == ErrorKind::PermissionDenied => {
        eprintln!("Permission denied — check file ownership and mode");
    }
    Err(Error::Io(e)) if e.kind() == ErrorKind::NotFound => {
        eprintln!("Parent directory does not exist — create it first");
    }
    Err(Error::Io(e)) => eprintln!("I/O error: {e}"),
    Ok(client) => { /* use client.database("name") */ }
    Err(e) => eprintln!("other error: {e}"),
}
```

---

### `Error::WriterBusy`

Another writer is holding the exclusive write lock on the database file.

**The cross-process writer lock:** mqlite uses an OS advisory lock to prevent
two writer processes from owning the same file at once. Inside one process,
ordinary CRUD writers share the engine and can overlap; DDL uses namespace
drain barriers when it must exclude writers on a target collection.

`WriterBusy` is returned when an advisory file lock or namespace drain barrier
cannot be acquired within the configured busy timeout.

**Common causes:**
- Another process has the database open for writing (check with `lsof myapp.mqlite`)
- DDL is draining the namespace while another thread tries to write it
- The busy timeout is too short for your workload

**Recovery options:**

```rust
use mqlite::{Client, OpenOptions};
use std::time::Duration;

// Option 1: set a busy timeout.
// Block until the lock is available or the timeout expires.
// Use this when you expect brief contention (e.g., background checkpoint).
let client = Client::open_with_options(
    "myapp.mqlite",
    OpenOptions::new().busy_timeout(Duration::from_secs(5)),
)?;

// Option 2: custom busy handler.
// Called repeatedly while contended. Return true to retry, false to give up.
// `attempts` counts how many times the handler has been called so far.
let client = Client::open_with_options(
    "myapp.mqlite",
    OpenOptions::new().busy_handler(|attempts| {
        std::thread::sleep(Duration::from_millis(50));
        attempts < 20  // retry up to 20 times (~1 second total)
    }),
)?;

// Option 3: immediate failure.
// Use Duration::ZERO to fail immediately on contention (SQLite-style BUSY).
let client = Client::open_with_options(
    "myapp.mqlite",
    OpenOptions::new().busy_timeout(Duration::ZERO),
)?;

// Option 4: open read-only.
// Readers never block writers. Multiple read-only opens are allowed concurrently.
let client = Client::open_with_options(
    "myapp.mqlite",
    OpenOptions::new().read_only(true),
)?
# Ok::<(), mqlite::Error>(())
```

> **Note:** For multi-threaded write patterns, see [CONCURRENCY.md](CONCURRENCY.md).

---

### `Error::BsonSerialization`

A Rust value could not be serialized to BSON.

**Common causes:**
- Using non-string map keys (BSON requires string keys)
- Serializing a type that doesn't implement `Serialize`
- A floating-point NaN or infinity (not valid in BSON)

**Recovery:** Fix the data model. Ensure all map keys are `String` and all values
are BSON-compatible.

---

### `Error::BsonDeserialization`

BSON data retrieved from the database could not be deserialized into your Rust type.

**Common causes:**
- Schema mismatch: document has a field with a different type than your struct expects
- Missing required fields in a document that predates a schema change
- Using `Collection<MyStruct>` when the collection was written via `Collection<Document>`

**Recovery:**
```rust
use mqlite::Client;
use bson::Document;  // Use Document for schema-agnostic access

let client = Client::open("myapp.mqlite")?;
// Use Document instead of a struct when the schema is uncertain
let col = client.database("myapp").collection::<Document>("users");
# Ok::<(), mqlite::Error>(())
```

---

### `Error::DuplicateKey`

A write violated a unique index constraint.

**MongoDB error code:** 11000

**Common causes:**
- Inserting a document whose `_id` already exists in the collection
- Inserting a document that violates a user-defined unique index

**Recovery:**

```rust
use mqlite::{Client, Error, doc};

use tempfile::TempDir;
let tempdir = TempDir::new()?;
let client = Client::open(tempdir.path().join("db.mqlite"))?;
let users = client.database("test").collection::<bson::Document>("users");

let result = users.insert_one(&doc! { "_id": "alice", "name": "Alice" });
match result {
    Err(Error::DuplicateKey { detail }) => {
        eprintln!("Duplicate key: {detail}");
        // Either skip the insert or use upsert to replace the existing document
    }
    Ok(_) => {}
    Err(e) => return Err(e),
}

// Use upsert to replace-or-insert in one operation:
use mqlite::options::UpdateOptions;
users.update_one_with_options(
    doc! { "_id": "alice" },
    doc! { "$set": { "name": "Alice Updated" } },
    UpdateOptions::new().upsert(true),
)?;
# Ok::<(), mqlite::Error>(())
```

---

### `Error::CorruptDatabase`

The database file is structurally invalid: the header magic is wrong, the file
is truncated, or a page checksum is bad.

**When it occurs:** Usually at `Client::open()` time when the header is read.
Can also occur mid-operation if storage pages are corrupted.

**Fields:**
- `path` — path to the corrupt file
- `detail` — human-readable description of the corruption
- `recoverable` — `true` if the last checkpointed pages may still be readable

**Recovery options:**

```rust
use mqlite::{Client, Error, OpenOptions};

match Client::open("myapp.mqlite") {
    Err(Error::CorruptDatabase { path, detail, recoverable }) => {
        eprintln!("Corrupt database at {path:?}: {detail}");
        if recoverable {
            // Try read-only mode to access the last good checkpoint
            let client = Client::open_with_options(
                &path,
                OpenOptions::new().read_only(true),
            )?;
            // Export what you can, then restore from backup
        } else {
            eprintln!("Restore from backup required");
        }
    }
    Ok(client) => { /* use client.database("name") */ }
    Err(e) => return Err(e),
}
# Ok::<(), mqlite::Error>(())
```

---

### `Error::DiskFull`

A write operation failed because the filesystem has no space remaining.

**Fields:**
- `path` — path to the database file
- `required_bytes` — bytes needed for the write
- `available_bytes` — bytes currently available on the device
- `suggestion` — human-readable remediation hint

**Recovery:**

```rust
use mqlite::{Client, Error, doc};

let client = Client::open("myapp.mqlite")?;
let logs = client.database("myapp").collection::<bson::Document>("logs");

match logs.insert_one(&doc! { "msg": "hello" }) {
    Err(Error::DiskFull { required_bytes, available_bytes, .. }) => {
        eprintln!(
            "Disk full: need {required_bytes} bytes, only {available_bytes} available"
        );
        // Free disk space, then retry
    }
    Ok(_) => {}
    Err(e) => return Err(e),
}
# Ok::<(), mqlite::Error>(())
```

---

### `Error::SymlinkRejected`

The database path points to a symlink. mqlite refuses to follow symlinks to
prevent symlink-based path traversal attacks (see [WIRE-SECURITY.md](WIRE-SECURITY.md)).

**MongoDB error code:** 2 (BAD_VALUE)

**Recovery:** Provide the real (non-symlink) path to the database file.

---

### `Error::CollectionNotFound`

A collection-level operation was attempted on a collection that doesn't exist.

**MongoDB error code:** 26 (NamespaceNotFound)

**When it occurs:** `drop_index`, or wire protocol commands that require the
collection to exist. `insert_one` and `find` create the collection implicitly.

**Recovery:**
```rust
use mqlite::{Client, Error};

use tempfile::TempDir;
let tempdir = TempDir::new()?;
let client = Client::open(tempdir.path().join("db.mqlite"))?;
let col = client.database("test").collection::<bson::Document>("events");

match col.drop_index("myindex") {
    Err(Error::CollectionNotFound { name }) => {
        eprintln!("Collection '{name}' does not exist — skipping drop");
    }
    Ok(_) => {}
    Err(e) => return Err(e),
}
# Ok::<(), mqlite::Error>(())
```

---

### `Error::CursorNotFound`

The cursor ID referenced by a `getMore` or `killCursors` command does not exist
or has already expired. This error only occurs via the wire protocol.

**MongoDB error code:** 43 (CursorNotFound)

**Common causes:**
- Cursor was already exhausted by a prior `getMore`
- Cursor was explicitly killed with `killCursors`
- Server restarted and in-memory cursor state was lost

**Recovery:** Re-run the original `find` command to get a new cursor.

---

### `Error::UnsupportedOperator`

A query filter or update document used an MQL operator not supported by mqlite.

**MongoDB error code:** 9

**Common causes:**
- Using `$text`, `$expr`, `$where`, or `$mod` in a filter
- Using `$bit` in an update
- Using positional operators (`$`, `$[]`) in an update

**Recovery:**
```rust
use mqlite::{Client, Error, doc};

use tempfile::TempDir;
let tempdir = TempDir::new()?;
let client = Client::open(tempdir.path().join("db.mqlite"))?;
let col = client.database("test").collection::<bson::Document>("items");

match col.find_one(doc! { "text": { "$text": { "$search": "hello" } } }) {
    Err(Error::UnsupportedOperator { operator }) => {
        eprintln!("Operator '{operator}' is not supported — see COMPATIBILITY.md");
    }
    Ok(result) => { /* ... */ }
    Err(e) => return Err(e),
}
# Ok::<(), mqlite::Error>(())
```

See [COMPATIBILITY.md](COMPATIBILITY.md) for the complete list of supported operators.

---

### `Error::UnsupportedCommand`

A wire protocol command is not supported by mqlite.
Commands return error code 59 (CommandNotFound) to the driver.

**Common causes:**
- Using `aggregate`, `distinct`, `count`, or `explain` via the wire protocol
- Using authentication commands (mqlite has no auth layer)

**Recovery:** Use the mqlite native Rust API for operations not yet supported
over the wire protocol.

---

### `Error::UnsupportedIndexOption`

`create_index` was called with an unsupported index type or option.

**MongoDB error code:** 67 (CannotCreateIndex)

**Common causes:**
- Requesting a TTL index (`expireAfterSeconds`)
- Requesting a text, geospatial, or hashed index
- Requesting a partial or wildcard index

**Recovery:**
```rust
use mqlite::{Client, IndexModel, options::IndexOptions, doc};

use tempfile::TempDir;
let tempdir = TempDir::new()?;
let client = Client::open(tempdir.path().join("db.mqlite"))?;
let col = client.database("test").collection::<bson::Document>("users");

// Supported: unique index on a field
col.create_index(IndexModel {
    keys: doc! { "email": 1 },
    options: Some(IndexOptions::new().unique(true)),
})?;
# Ok::<(), mqlite::Error>(())
```

Supported index types: single-field, compound, unique, sparse, multikey.
See [COMPATIBILITY.md](COMPATIBILITY.md#index-types).

---

### `Error::DocumentTooLarge`

The document exceeds the 16,777,216 byte (16MB) BSON-serialized size limit.

**MongoDB error code:** 10334

**Fields:**
- `size` — actual serialized size of the document in bytes
- `max` — maximum allowed size (16,777,216)

**Recovery:** Split large documents into smaller ones, or store large payloads
in separate files and reference them by path or ID.

---

### `Error::DocumentValidationFailure`

The document failed one of mqlite's structural validation checks.

**MongoDB error code:** 121

**What is checked:**
- Nesting depth (maximum 100 levels)
- Field count per document (maximum 100 fields at any level)
- Field names must not begin with `$` at the top level of an update

**Recovery:** Simplify deeply nested documents or split high-field-count documents.

---

### `Error::InvalidWireMessage`

The wire protocol received a message that is malformed, exceeds the maximum
message size, or uses an unsupported opcode.

**MongoDB error code:** 48 (IllegalOperation)

**Common causes:**
- Client sent OP_COMPRESSED (opcode 2012) — mqlite does not support compression
- Message header magic bytes are wrong (not a MongoDB message)
- Message size field exceeds the 48MB OP_MSG limit

**Recovery:** Ensure the client uses an uncompressed connection. With pymongo:
```python
client = MongoClient(
    "mongodb://localhost:27017",
    directConnection=True,
    compressors=[],  # disable compression
)
```

---

### `Error::Internal`

An internal invariant was violated. This indicates a bug in mqlite.

**MongoDB error code:** 1

This error should never occur during normal usage. If you encounter it, please
file a bug report at https://github.com/kparkins/mqlite/issues with:
- The full error message (including the internal detail string)
- A minimal reproduction case

---

### `Error::WriteConflict`

A concurrent writer committed a conflicting change. mqlite is a
first-committer-wins MVCC engine; the caller decides whether to retry against
a fresh `ReadView`. Distinct from `WriterBusy`, which signals lane contention
with no logical conflict.

**Field:** `reason: WriteConflictReason` — discriminant explaining why the
conflict was raised:
- `StaleSnapshot` — the writer's `ReadView` predates a concurrent committed
  head on the same key.
- `UpgradeRace` — two readers on the same page-local latch requested upgrade;
  one loses. Retry is immediate and does not require a new `ReadView`.
- `SameKeyConflict { key_preview }` — two writers installed deltas on the
  same primary key.
- `CatalogGenerationChanged` — the captured catalog generation no longer
  matches the published epoch when the writer revalidated.
- `StructuralContention` — multi-leaf install could not acquire all required
  exclusive page latches.
- `UniqueConflict { key_prefix_preview }` — a unique-index install observed
  another live entry whose prefix equals this writer's prefix.

**Recovery:** open a new `ReadView` (or re-run the operation against the live
client) and retry.

---

### `Error::InvalidConfig`

A caller supplied an invalid engine configuration value (e.g. via `OpenOptions`).

**MongoDB error code:** 2 (BAD_VALUE)

**Fields:**
- `field: &'static str` — configuration field that failed validation
- `detail: String` — human-readable reason

---

### `Error::UnsupportedJournalFormat`

The on-disk journal sidecar's magic bytes or format version do not match what
this build supports. Typically means the database was created by an older or
newer mqlite build.

**Fields:**
- `found: [u8; 4]` — magic bytes read from the journal
- `expected: [u8; 4]` — magic bytes this build expects (`MQJL`)

---

### `Error::JournalFrameTooLarge`

A logical-txn journal frame would exceed the hard byte cap. The encoder bails
before any byte is appended so the journal stays well-formed.

**Fields:**
- `logical_frame_bytes: usize`
- `max_bytes: usize`

---

### `Error::TimestampExhausted`

The HLC logical counter saturated at `u32::MAX` for the current millisecond
and the wall clock has not advanced past it. Only reachable under pathological
load (more than `u32::MAX` commits in the same millisecond) or a stuck clock.

---

### `Error::RefcountOverflow`

An overflow-page refcount would exceed `u32::MAX`. Indicates a pin leak
(≥ 4 billion live `OverflowRef`s on one chain); investigate long-lived
`ReadView`s or `OverflowRef` retention.

---

### `Error::ReadViewExpired`

A `ReadView` was force-expired by the engine (e.g. during a `drop_collection`
barrier). The caller must open a new `ReadView` to continue reading.

---

### `Error::StatePoisoned`

A shared-state mutex was poisoned by a panicking thread.

**Field:** `component: &'static str` — name of the poisoned component
(e.g. `"history_store"`).

---

### `Error::CatalogParse`

A catalog field could not be parsed from BSON.

**Fields:**
- `field: &'static str` — the BSON field name that failed to parse
- `source: bson::de::Error` — underlying deserialization error (via `#[source]`)

---

### `Error::UpdateOperatorTypeMismatch`

An update operator was applied to a field whose type does not match what the
operator requires (e.g. `$inc` on a string field).

**Fields:**
- `operator: &'static str` (e.g. `"$inc"`)
- `expected: &'static str`
- `got: &'static str`

---

### `Error::Recovery`

Open-time recovery found durable evidence that cannot be replayed safely.

**Field:** `detail: String` — operator-facing recovery detail.

---

### `Error::RecoveryPoolExhausted`

Reopen logical replay would exceed the configured buffer-pool size. Increase
`max_pool_bytes` or perform a forced reconcile on the previous open before
closing.

---

### `Error::PoolExhausted`

A live CRUD or reader path could not find an evictable buffer-pool frame.
Checkpoint frontier pressure is reported separately as
`Error::CheckpointIncomplete`.

**Field:** `reason: PoolExhaustedReason`:
- `AllFramesPinned` — every frame in the target pool partition is pinned.
- `DeltaBearingFrames` — every eviction candidate carries resident deltas
  that cannot be dropped without first reconciling them.

**Recovery:** close or expire long-lived readers/pins, wait for checkpoint
relief, or increase `buffer_pool_size`.

---

### `Error::CheckpointIncomplete`

A checkpoint cannot advance the durable frontier without losing
checkpoint-visible resident state.

**Fields:**
- `first_blocking_page: u32` — first dirty leaf that blocked checkpoint
  planning.
- `reason: CheckpointIncompleteReason` — `FrameCoWRefused`,
  `OverflowSpillNotWired`, `VisibleWinnerExceedsPageBudget`,
  `TombstonePredecessorPressure`, `PoolExhausted(PoolExhaustedReason)`,
  `HistoryDuplicateConflict`, `HistoryDuplicateCapExceeded`,
  `ReachabilityRepairRequired`.

**Recovery:** close or expire long readers/pins, enable overflow spill if
blocking, raise pool or cap limits, then retry the checkpoint.

---

### `Error::BufferPoolEvictionBlocked`

Internal-only eviction refusal for a delta-bearing frame. Not produced by
public engine APIs; those still surface `Error::PoolExhausted` when every
eviction candidate is blocked.

**Fields:**
- `page: u32`
- `reason: &'static str`

---

### `Error::EngineFatal`

The engine reached a post-durable state that requires reopening. The durable
journal commit has already completed when this is raised; in-memory state
cannot be repaired, so the engine is poisoned, refuses new operations, and
must be reopened.

**Field:** `reason: EngineFatalReason`:
- `PostReservationLogWriteFailure` — log writer failed after reserving a
  byte-LSN range and before marking the record written.
- `PostDurablePublishFailure` — failure during the ordinary CRUD `mark_ready`
  publish closure or its surrounding post-durable scope.
- `PostDurablePendingFlipFailure` — failure flipping `VersionState::Pending`
  to `Committed`.
- `PostDurableDdlPublishFailure` — failure during a DDL publish closure
  (create/drop index, drop namespace, create-index cleanup).
- `CheckpointPostMutationFailure` — checkpoint failed after its mutation
  phase began.

**Recovery:** close the `Client` and reopen the database; recovery replays
from the last durable checkpoint boundary.

---

## Matching on `Error`

`Error` is `#[non_exhaustive]`, so match arms should include a catch-all:

```rust
use mqlite::{Error, doc};

fn handle_write(db: &mqlite::Database) -> mqlite::Result<()> {
    let col = db.collection::<bson::Document>("items");
    match col.insert_one(&doc! { "x": 1 }) {
        Ok(_) => {}
        Err(Error::WriterBusy) => {
            eprintln!("Writer busy — retry with backoff");
        }
        Err(Error::DuplicateKey { detail }) => {
            eprintln!("Duplicate: {detail}");
        }
        Err(Error::DiskFull { required_bytes, .. }) => {
            eprintln!("Disk full — need {required_bytes} more bytes");
        }
        Err(e) => return Err(e),  // propagate other errors
    }
    Ok(())
}
```

---

## MongoDB Error Codes

mqlite maps errors to MongoDB error codes for wire protocol compatibility.
Drivers that inspect the `code` field of the error response will receive these values.

| Code | Constant | Variant |
|------|----------|---------|
| 1 | `INTERNAL_ERROR` | `Error::Internal` |
| 2 | `BAD_VALUE` | `Error::SymlinkRejected`, `Error::InvalidConfig` |
| 9 | `UNSUPPORTED_OPERATOR` | `Error::UnsupportedOperator` |
| 26 | `NAMESPACE_NOT_FOUND` | `Error::CollectionNotFound` |
| 43 | `CURSOR_NOT_FOUND` | `Error::CursorNotFound` |
| 48 | `ILLEGAL_OP` | `Error::InvalidWireMessage` |
| 67 | `CANNOT_CREATE_INDEX` | `Error::UnsupportedIndexOption` |
| 121 | `DOCUMENT_VALIDATION_FAILURE` | `Error::DocumentValidationFailure` |
| 10334 | `DOCUMENT_TOO_LARGE` | `Error::DocumentTooLarge` |
| 11000 | `DUPLICATE_KEY` | `Error::DuplicateKey` |

All other variants — including `WriteConflict`, `WriterBusy`, `Io`, BSON
serialization errors, and the engine/recovery variants — return `None` from
`Error::code()` and surface as `INTERNAL_ERROR` (1) when emitted via the wire
protocol's generic conversion path.

Use `error.code()` to retrieve the code from a Rust `Error` value:

```rust
use mqlite::Error;

fn log_error(e: &Error) {
    match e.code() {
        Some(code) => eprintln!("mqlite error (code {code}): {e}"),
        None        => eprintln!("mqlite error: {e}"),
    }
}
```
