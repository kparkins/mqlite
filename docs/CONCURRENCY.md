# mqlite Concurrency Guide

mqlite is **MWMR in-process**: reads are mutex-free (atomic snapshot load),
and writers on different namespaces run concurrently. Every read operation
opens a `ReadView` that pins a point-in-time snapshot; concurrent writes
do not disturb an in-progress read. Writers on the same namespace serialize
on a per-namespace lane mutex.

---

## Current behavior

### Readers: MVCC snapshot isolation

When a read operation begins, the engine opens a `ReadView` at the HLC
oracle's current timestamp and walks per-document version chains held in
the buffer pool.

- **No torn reads, no phantom reads.** Every version entry satisfies
  `start_ts <= read_ts < stop_ts`; the reader always sees a
  point-in-time consistent view.
- **Concurrent writes do not disturb an in-progress read.** A write
  that commits after a `ReadView` was opened is invisible to that view.
  The next read (a fresh `ReadView`) will see the new data.
- **History store fallback.** If a version chain is evicted from the
  buffer pool before `oldest_required_ts` advances past it, the entry is
  pushed into a B-tree history store. Readers probe the history store on
  an in-memory miss.

```rust
use mqlite::{Client, doc};
use std::thread;

use tempfile::TempDir;
let tempdir = TempDir::new()?;
let client = Client::open(tempdir.path().join("db.mqlite"))?;
let db = client.database("mydb");
let users = db.collection::<bson::Document>("users");

// Insert initial data
users.insert_one(&doc! { "_id": 1, "status": "active" })?;

// Thread A: long-running read (holds snapshot at T₁)
let client_a = client.clone();
let reader = thread::spawn(move || {
    let users_a = client_a.database("mydb").collection::<bson::Document>("users");
    // This read sees "active" — the snapshot when the read started
    users_a.find_one(doc! { "_id": 1 })
});

// Main thread: write happens concurrently
users.update_one(
    doc! { "_id": 1 },
    doc! { "$set": { "status": "inactive" } },
)?;

// Thread A's read result: still "active" (snapshot at T₁)
let result = reader.join().unwrap()?;
println!("{:?}", result.unwrap().get("status")); // "active"

// A new read after the write sees "inactive"
let result2 = users.find_one(doc! { "_id": 1 })?;
println!("{:?}", result2.unwrap().get("status")); // "inactive"
# Ok::<(), mqlite::Error>(())
```

### Writers: `WriteTxn` + per-namespace lanes

Each write operation creates a `WriteTxn` that stages primary-key
updates, secondary-index updates, and refcount deltas under a single
`commit_ts`. At commit, all staged changes become atomically visible to
new `ReadView`s.

**Lock acquisition on the write path** (see `src/storage/paged_engine.rs`):

1. `PagedEngine::metadata: RwLock<MetadataState>` — shared read-guard, released before the lane is acquired.
2. `PagedEngine::ns_lanes[ns]: Mutex<()>` — per-namespace lane. Writers on **different** namespaces run concurrently; writers on the **same** namespace serialize here.
3. `PagedEngine::commit_seq: Mutex<()>` — held around `commit_ts` allocation → primary install → journal append → snapshot publish, so `commit_ts`, journal-append order, and `publish_ts` always agree across concurrent commits.

**Reads take none of the above locks.** A read loads `shared.published:
ArcSwap<PublishedSnapshot>` atomically, opens B-trees at the snapshot's
root pages, and opens a `ReadView`. Reads and writes never block each
other; writers on different namespaces never block each other.

The historical engine-global writer mutex (`PagedEngine::inner:
Mutex<BpBackend>`) was retired in v1 MWMR (see [ADR 0002](adr/0002-mwmr.md)).
DDL operations (`create_collection`, `drop_collection`, `drop_index`,
`checkpoint`, `backup`) still take `metadata.write()` exclusively and
block all concurrent writers for their duration.

**Writer exclusivity** is additionally enforced by an OS-level advisory
file lock acquired at `Client::open()` time:

- **Unix**: `fcntl(F_SETLK)` exclusive lock
- **Windows**: `LockFileEx` exclusive lock

This prevents two separate **processes** from writing simultaneously.

> **Important:** POSIX advisory locks are **per-process**, not per-thread.
> The engine-level mutex handles cross-thread serialization within a
> single process.

### `drop_collection` force-expire barrier

`drop_collection` is the one DDL operation that cannot proceed while any
`ReadView` is live on the target collection. Under `metadata.write()` the
engine calls `read_view_registry().force_expire_all()`, which poisons
every open `ReadView` globally and waits for `pin_ops_in_flight == 0`
before freeing the collection's B-tree pages (see `src/storage/paged_engine.rs`).

Any caller that holds a `ReadView` across a `drop_collection` will receive
`Error::ReadViewExpired` on its next read and must open a fresh view.
Subsequent reads on the dropped collection return zero rows.

---

## Writer Contention

### What Causes It

| Scenario | Likely Cause |
|----------|-------------|
| `WriterBusy` from another process | Another process has the file open for writing |
| `WriterBusy` from another thread | Two threads writing without coordination |
| Intermittent `WriterBusy` | Short bursts of write contention |

### Configuring the Busy Timeout

By default, mqlite waits up to **5 seconds** for the writer lock before
returning `Error::WriterBusy`. Adjust this based on your workload:

```rust
use mqlite::{Client, OpenOptions};
use std::time::Duration;

// High-throughput application: longer timeout to ride out bursts
let client = Client::open_with_options(
    "myapp.mqlite",
    OpenOptions::new().busy_timeout(Duration::from_secs(30)),
)?;

// Latency-sensitive application: fail fast, retry at app level
let client = Client::open_with_options(
    "myapp.mqlite",
    OpenOptions::new().busy_timeout(Duration::from_millis(50)),
)?;

// Zero timeout: fail immediately on contention (SQLite-style BUSY)
let client = Client::open_with_options(
    "myapp.mqlite",
    OpenOptions::new().busy_timeout(Duration::ZERO),
)?;
# Ok::<(), mqlite::Error>(())
```

### Custom Busy Handler

For more control, use a callback that is called each time the lock is contended:

```rust
use mqlite::{Client, OpenOptions};
use std::time::Duration;

let client = Client::open_with_options(
    "myapp.mqlite",
    OpenOptions::new().busy_handler(|attempts| {
        // `attempts` = number of retries so far (starts at 0)
        if attempts >= 100 {
            return false;  // Give up after 100 retries
        }
        // Exponential backoff with jitter
        let delay_ms = std::cmp::min(1 << attempts, 500);
        std::thread::sleep(Duration::from_millis(delay_ms));
        true  // Keep retrying
    }),
)?;
# Ok::<(), mqlite::Error>(())
```

The busy handler and `busy_timeout` are mutually exclusive — if both are set,
the busy handler takes precedence.

---

## Multi-Threaded Write Patterns

Writers on different namespaces (collections) run concurrently. Writers on
the **same** namespace serialize on that namespace's lane mutex. The two
patterns below are useful when same-namespace write throughput matters:

### Pattern 1: Dedicated Writer Thread with Channel

Funnel all writes through a single dedicated thread. Other threads send work
to it via a channel. This is the most natural pattern for high-throughput
write workloads.

```rust
use mqlite::{Client, doc};
use bson::Document;
use std::sync::mpsc;
use std::thread;

enum WriteRequest {
    Insert(Document),
    Shutdown,
}

let client = Client::open("myapp.mqlite")?;
let (tx, rx) = mpsc::channel::<WriteRequest>();

// Spawn the writer thread
let writer_client = client.clone();
let writer = thread::spawn(move || {
    let col = writer_client.database("mydb").collection::<Document>("events");
    for req in rx {
        match req {
            WriteRequest::Insert(doc) => {
                if let Err(e) = col.insert_one(&doc) {
                    eprintln!("Write failed: {e}");
                }
            }
            WriteRequest::Shutdown => break,
        }
    }
});

// Other threads send writes via the channel
let tx1 = tx.clone();
thread::spawn(move || {
    tx1.send(WriteRequest::Insert(doc! { "event": "login", "user": "alice" })).ok();
});

let tx2 = tx.clone();
thread::spawn(move || {
    tx2.send(WriteRequest::Insert(doc! { "event": "login", "user": "bob" })).ok();
});

// Shutdown
tx.send(WriteRequest::Shutdown).ok();
writer.join().unwrap();
# Ok::<(), mqlite::Error>(())
```

### Pattern 2: Shared Database Handle with Mutex Coordination

If you need writes to return results (e.g., the inserted `_id`), wrap the
write operations in a `Mutex` at the application level. mqlite's internal
writer mutex already serializes them, but this pattern lets you batch
application-level logic atomically.

```rust
use mqlite::{Client, doc};
use bson::Document;
use std::sync::{Arc, Mutex};
use std::thread;

// Client is already cheaply clonable (Arc-backed)
let client = Client::open("myapp.mqlite")?;

// For application-level atomic batches, add your own Mutex
let write_lock = Arc::new(Mutex::new(()));

let handles: Vec<_> = (0..4).map(|i| {
    let client = client.clone();
    let lock = Arc::clone(&write_lock);
    thread::spawn(move || {
        let col = client.database("mydb").collection::<Document>("items");
        // Hold app mutex for the duration of the logical operation
        let _guard = lock.lock().unwrap();
        col.insert_one(&doc! { "worker": i, "ts": bson::DateTime::now() })
    })
}).collect();

for h in handles {
    h.join().unwrap()?;
}
# Ok::<(), mqlite::Error>(())
```

> **Tip:** For simple use cases, you can skip the application-level `Mutex`.
> mqlite's internal writer mutex already ensures correctness — the only reason
> to add your own is if you need to keep multiple operations logically atomic.

### Pattern 3: Multiple Processes (Read-Heavy Workloads)

For applications where multiple processes need database access, use one writer
process and multiple read-only processes:

```rust
// Process A: writer (e.g., data ingestion service)
use mqlite::Client;
let writer = Client::open("shared.mqlite")?;
let db = writer.database("mydb");
// ... perform writes

// Process B, C, D: readers (e.g., query services)
use mqlite::{Client, OpenOptions};
let reader = Client::open_with_options(
    "shared.mqlite",
    OpenOptions::new().read_only(true),
)?;
let db = reader.database("mydb");
// ... read-only queries
```

Readers opened with `read_only(true)` never contend with the writer and can
run unlimited concurrently.

---

## Async Integration

mqlite's core API is synchronous. In an async context, wrap write operations
in `spawn_blocking` to avoid blocking the async executor:

```rust
use mqlite::{Client, doc};

async fn insert_event(client: Client, user: String) -> mqlite::Result<()> {
    tokio::task::spawn_blocking(move || {
        let col = client.database("mydb").collection::<bson::Document>("events");
        col.insert_one(&doc! { "user": user, "ts": bson::DateTime::now() })?;
        Ok(())
    })
    .await
    .expect("spawn_blocking panicked")
}
```

For read-heavy async workloads, reads are also synchronous but cheaper (no
exclusive lock). Still wrap them in `spawn_blocking` for correctness:

```rust
use mqlite::{Client, doc};

async fn get_user(client: Client, id: &str) -> mqlite::Result<Option<bson::Document>> {
    let id = id.to_owned();
    tokio::task::spawn_blocking(move || {
        client.database("mydb").collection::<bson::Document>("users")
            .find_one(doc! { "_id": id })
    })
    .await
    .expect("spawn_blocking panicked")
}
```

> **Rayon:** The same `spawn_blocking` pattern applies when using Rayon thread
> pools. The mqlite `Client` handle is `Send + Sync` and can be cloned freely
> across Rayon tasks.

---

## Frequently Asked Questions

**Q: Can I open the same `.mqlite` file from multiple threads?**

Yes. `Client` is `Clone`, `Send`, and `Sync`. Clone it and share across
threads freely. The internal writer mutex serializes writes automatically.

**Q: Can I open the same `.mqlite` file from multiple processes?**

Yes, but only one process can be the writer. Open additional processes with
`read_only(true)`. If a second process opens for writing, the first write will
succeed, and subsequent writes from either process will race — the OS advisory
lock ensures only one wins at a time, with the loser receiving `WriterBusy`.

**Q: Do I need to manually checkpoint the journal?**

No. mqlite auto-checkpoints the journal after every `journal_auto_checkpoint` pages
(default: 1000 pages). On clean close, a full checkpoint is performed and the
journal file is removed.

**Q: Can readers see partial writes?**

No. MVCC guarantees readers see only fully committed writes. An in-progress
write is invisible to all readers until it commits under a single `commit_ts`.

**Q: What happens if my writer panics mid-write?**

`WriteTxn` rolls back via its `Drop` implementation, so in-progress staged
changes are discarded. The WAL ensures the database file itself remains
consistent — the in-progress write was never committed. The engine mutex is
released normally.

**Q: Is there a way to do multi-document transactions?**

Not in v1. Each `insert_one`, `update_one`, etc. is its own atomic unit.
Multi-document transaction support is out of scope for v1.

---

## Quick Reference

| Want to… | Use |
|----------|-----|
| Share database across threads | `client.clone()` (it's already `Arc`-backed) |
| Serialize writes | mqlite does this automatically |
| Read without blocking writes | `read_only(true)` or just `find_one` |
| Wait for writer lock | `busy_timeout(Duration::from_secs(N))` |
| Custom retry logic | `busy_handler(|attempts| ...)` |
| Use in async code | `tokio::task::spawn_blocking` |
| Multi-process read scaling | One writer + N read-only opens |
