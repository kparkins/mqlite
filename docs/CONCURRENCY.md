# mqlite Concurrency Guide

mqlite uses a **Single-Writer, Multiple-Reader (SWMR)** concurrency model —
the same model used by SQLite in WAL mode. Understanding it is essential for
writing correct multi-threaded and multi-process applications.

---

## The SWMR Model

```
┌─────────────────────────────────────────────────────┐
│                  .mqlite file                        │
│                                                     │
│  Readers (unlimited) ──────────────────────────►   │
│  Reader 1: consistent snapshot @ T₁                │
│  Reader 2: consistent snapshot @ T₁                │
│  Reader N: consistent snapshot @ T₁                │
│                                                     │
│  Writer (one at a time) ───────────────────────►   │
│  Writer: exclusive write lock, advances T to T₂    │
└─────────────────────────────────────────────────────┘
```

**Key properties:**

| Property | Behavior |
|----------|----------|
| Writers | One writer at a time (exclusive) |
| Readers | Unlimited concurrent readers |
| Reader/writer isolation | Readers never block writers; writers never block readers |
| Reader consistency | Each reader sees a consistent snapshot at the moment it started |
| Writer consistency | Writes are serialized and see all previously committed writes |

---

## Writer Exclusivity: Two Cooperating Locks

Two locks work together to enforce the "one writer at a time" rule:

### 1. OS Advisory Lock (cross-process)

At `Client::open()` time, mqlite acquires an OS-level advisory lock on the
`.mqlite` file:

- **Unix**: `fcntl(F_SETLK)` exclusive lock
- **Windows**: `LockFileEx` exclusive lock

This prevents two separate **processes** from writing simultaneously. The lock
is held for the lifetime of the `Client` handle and released when the last
clone is dropped.

> **Important:** POSIX advisory locks are **per-process**, not per-thread. Two
> threads in the same process would both succeed at `fcntl`, which is why the
> second lock is needed.

### 2. In-Process `Mutex` (cross-thread)

An `Arc<Mutex<()>>` serializes writer threads within a single process. All
write operations (`insert_one`, `update_one`, `delete_one`, etc.) acquire this
mutex before writing.

**Locking order** (always in this order to avoid deadlocks):

```
OS advisory lock (acquired at open) → writer Mutex (acquired per write)
```

---

## Reader MVCC: Snapshot Isolation

Readers in mqlite use **Multi-Version Concurrency Control (MVCC)**:

- When a read operation begins, it sees the database state at that instant (a
  "snapshot").
- Concurrent writes by other threads do not affect an in-progress read.
- A read that started before a write completes will **not** see the new data —
  it sees the pre-write snapshot.
- The next read after a write completes will see the new data.

This means reads are always **consistent** (no torn reads, no phantom reads)
but may be **stale** if the application requires seeing the latest write.

**Example of snapshot behavior:**

```rust
use mqlite::{Client, doc};
use std::thread;

let client = Client::open_in_memory()?;
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

Because only one writer can run at a time, the key to good multi-threaded write
performance is **serializing writes efficiently** at the application level.
Two patterns work well:

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

**Q: Do I need to manually checkpoint the WAL?**

No. mqlite auto-checkpoints the WAL after every `wal_auto_checkpoint` pages
(default: 1000 pages). On clean close, a full checkpoint is performed and the
WAL file is removed.

**Q: Can readers see partial writes?**

No. MVCC guarantees readers see only fully committed writes. An in-progress
write is invisible to all readers until it completes.

**Q: What happens if my writer panics mid-write?**

The writer `Mutex` is poisoned. Subsequent write attempts will return a
`Mutex::lock()` poison error wrapped in `Error::Internal`. The WAL ensures
the database file itself remains consistent — the in-progress write was never
committed.

**Q: Is there a way to do multi-document transactions?**

Not in Phase 1. Each `insert_one`, `update_one`, etc. is its own atomic unit.
Multi-document transaction support is planned for Phase 2.

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
