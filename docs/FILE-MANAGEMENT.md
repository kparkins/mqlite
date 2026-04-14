# mqlite File Management Guide

How to safely copy, back up, monitor, and recover mqlite database files.

---

## The "Single File" Promise

mqlite is advertised as a **single-file database**. The fine print:

> A mqlite database is a single file **after a clean close**.
> During normal operation, two additional files may be present.

```
myapp.mqlite          ← Main database file (always present)
myapp.mqlite-wal      ← Write-ahead log (present during write activity)
myapp.mqlite-shm      ← WAL shared-memory index (present while any handle is open)
```

| File | When present | Role |
|------|-------------|------|
| `myapp.mqlite` | Always | Persistent B-tree pages. The "real" database. |
| `myapp.mqlite-wal` | After any write, until checkpointed | Uncommitted and unmerged write pages. |
| `myapp.mqlite-shm` | While a `Database` handle is open | In-memory WAL index, memory-mapped from this file. Deleted on clean close. |

**During normal operation** all three files form a single logical unit. Never
copy or move them individually — always treat the `.mqlite`, `-wal`, and `-shm`
files as a group.

---

## Clean Close

`Database::close()` performs a **blocking checkpoint + flush**:

1. All committed WAL pages are merged into `myapp.mqlite`.
2. The WAL file is removed.
3. The SHM file is removed.
4. The OS advisory lock is released.

After `close()` returns, `myapp.mqlite` is the sole file on disk and can be
copied safely.

```rust
use mqlite::Database;

fn write_and_close() -> mqlite::Result<()> {
    let db = Database::open("myapp.mqlite")?;
    let col = db.collection::<mqlite::Document>("events");
    col.insert_one(&mqlite::doc! { "type": "shutdown" })?;

    // Blocking flush + checkpoint. After this line, myapp.mqlite is the sole file.
    db.close()?;
    Ok(())
}
```

> **`Drop` vs `close()`:** Dropping a `Database` handle is non-blocking. It
> releases the OS lock but does **not** checkpoint the WAL. The WAL and SHM
> files remain on disk. This is safe — the next `open()` replays the WAL
> automatically — but it means `Drop` does not produce a single-file state.
> Call `close()` explicitly when you need one.

---

## Cold Backup (Safe Copy)

A cold backup copies the database while it is **not open** by any process.

```
Step 1: Ensure the database is closed (call db.close() in your app, or
        verify no process holds a file lock).
Step 2: Copy only the main file.

$ cp myapp.mqlite backup-$(date +%Y%m%d).mqlite
```

In Rust:

```rust
use mqlite::Database;
use std::fs;

fn cold_backup(src: &str, dst: &str) -> mqlite::Result<()> {
    // Open, force a clean close, then copy.
    let db = Database::open(src)?;
    db.close()?; // checkpoint + remove WAL/SHM

    fs::copy(src, dst)?;
    println!("Backup written to {dst}");
    Ok(())
}
```

**Important:** Verify that `src-wal` and `src-shm` do **not** exist after
`close()`. If they do, a crash occurred between the checkpoint and the file
removal — open and close the database again before copying.

```rust
use std::path::Path;

fn assert_single_file(path: &str) {
    assert!(!Path::new(&format!("{path}-wal")).exists(), "WAL still present");
    assert!(!Path::new(&format!("{path}-shm")).exists(), "SHM still present");
}
```

---

## Checkpoint-Then-Copy

If closing the database is not an option (e.g., the app is running), you can
checkpoint the WAL into the main file without closing:

```rust
use mqlite::Database;
use std::fs;

fn checkpoint_backup(db: &Database, src: &str, dst: &str) -> mqlite::Result<()> {
    // Flush all committed WAL pages to the main file.
    // After this, myapp.mqlite contains all committed data.
    db.checkpoint()?;

    // Safe to copy IF no concurrent writers are active.
    // For single-process apps (the common case), this is always true here
    // because checkpoint holds the writer lock while it runs.
    fs::copy(src, dst)?;
    println!("Checkpoint backup written to {dst}");
    Ok(())
}
```

**When is this safe?**
- ✅ Single-process apps: `checkpoint()` serializes with all writers in the
  same process, so the copy happens when no writes are in flight.
- ⚠️ Multi-process setups: A second process could start writing between
  `checkpoint()` and the `fs::copy`. In this case the copy may include a
  partial WAL. Use a hot backup (see below) or a cold backup instead.

---

## Hot Backup (Phase 2)

`Database::backup(dest)` will produce a consistent snapshot of the database
**while it is running**, without blocking writers. This is planned for Phase 2.

```rust
// Phase 2 — not yet available in Phase 1:
// db.backup("backup.mqlite")?;
```

In Phase 1, use the checkpoint-then-copy approach for running databases, or
cold backup (close then copy) when a brief pause is acceptable.

---

## Size Monitoring

```rust
use mqlite::Database;
use std::fs;

fn report_db_size(path: &str) {
    let main_bytes = fs::metadata(path)
        .map(|m| m.len())
        .unwrap_or(0);

    let wal_path = format!("{path}-wal");
    let wal_bytes = fs::metadata(&wal_path)
        .map(|m| m.len())
        .unwrap_or(0);

    println!("Main file : {:.1} MB", main_bytes as f64 / 1_048_576.0);
    println!("WAL file  : {:.1} MB", wal_bytes as f64 / 1_048_576.0);
    println!("Total     : {:.1} MB", (main_bytes + wal_bytes) as f64 / 1_048_576.0);
}
```

**WAL size behaviour:**
- The WAL grows with each write and shrinks (to zero) after a checkpoint.
- Auto-checkpoint triggers when the WAL reaches `wal_auto_checkpoint` pages
  (default: 1,000 pages ≈ 4 MB) or `wal_max_size` bytes (default: 100 MB).
- A large WAL does not affect read correctness but does slow reads (the reader
  must scan the WAL for page versions).

**Main file size behaviour:**
- The main file **never shrinks automatically** in Phase 1. Deleted documents
  free internal B-tree pages, but those pages are reused for future writes
  rather than returned to the OS. A `compact()` / `vacuum()` operation is
  planned for Phase 2.

---

## After a Crash: Automatic WAL Recovery

If the process is killed or the host crashes while mqlite is open, the WAL
and SHM files remain on disk. The next `Database::open()` automatically:

1. Detects the leftover WAL.
2. Replays committed transactions from the WAL into memory.
3. Discards any partially-written transaction at the tail.
4. Resumes normal operation.

No manual intervention is required.

```rust
// After a crash, just open normally — recovery is automatic.
let db = Database::open("myapp.mqlite")?;
// All committed data is available.
```

The SHM file is re-created if it is absent. Its absence does not indicate data
loss.

> **Committed vs uncommitted at crash time:**
> Transactions that were committed (i.e., the write operation returned `Ok`)
> before the crash are replayed from the WAL and are fully durable.
> Transactions that were in-flight at crash time (the operation had not
> returned `Ok`) are discarded.

---

## Read-Only Access After Failure

If the main file is corrupt but the WAL is intact, or if you need to inspect
a database without risking further damage, open it in read-only mode:

```rust
use mqlite::{Database, OpenOptions};

fn forensic_open(path: &str) -> mqlite::Result<Database> {
    Database::open_with_options(
        path,
        OpenOptions::new().read_only(true),
    )
}
```

In read-only mode:
- WAL replay is skipped (reads from the last checkpointed state in the main file).
- No write operations are permitted.
- No OS exclusive lock is acquired (multiple read-only opens can coexist).
- The SHM file is not created.

This is useful for forensic inspection or for opening a database on a
read-only filesystem (e.g., a mounted backup volume or a CD-ROM image).

---

## Network Filesystems

**mqlite does not support network filesystems** (NFS, SMB/CIFS, SSHFS, etc.).

The WAL protocol relies on:
1. **Atomic file renames** (used during checkpoints).
2. **Reliable `fcntl`/`LockFileEx` advisory locks** (used for writer exclusivity).
3. **Memory-mapped I/O** on the SHM file.

Network filesystems frequently fail to provide all three guarantees. The result
is database corruption that may not be detected immediately.

Use mqlite only on **local block storage**: local SSDs, NVMe, SD cards (with
caveats — see [IoT Deployment Guide](IOT-DEPLOYMENT.md)), or loop-mounted images.

---

## Multi-Process Safety Rules

mqlite uses OS advisory locks to coordinate between processes:

1. **One writer at a time** (across all processes on the same machine).
2. **Unlimited concurrent readers** (any number of processes).
3. **One `.mqlite` file per process**: opening the same file in two processes
   is supported; opening it twice in the same process is also supported
   (the second open shares the same lock).

```rust
// Process A (writer):
let db_a = Database::open("shared.mqlite")?;
db_a.collection::<mqlite::Document>("log").insert_one(&mqlite::doc! { "msg": "hello" })?;
// db_a holds the exclusive write lock while inserting.

// Process B (reader) — can open concurrently:
let db_b = Database::open_with_options(
    "shared.mqlite",
    mqlite::OpenOptions::new().read_only(true),
)?;
// db_b sees a consistent snapshot; it never blocks db_a.
```

For multiple writer processes competing for the same file, configure a
`busy_timeout` so they back off gracefully:

```rust
use mqlite::{Database, OpenOptions};
use std::time::Duration;

let db = Database::open_with_options(
    "shared.mqlite",
    OpenOptions::new().busy_timeout(Duration::from_secs(5)),
)?;
```

See [CONCURRENCY.md](CONCURRENCY.md) for the full concurrency model.
