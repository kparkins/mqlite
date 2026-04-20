# mqlite File Management Guide

How to safely copy, back up, monitor, and recover mqlite database files.

---

## The "Single File" Promise

mqlite is advertised as a **single-file database**. The fine print:

> A mqlite database is a single file **after a clean close**.
> During normal operation, one additional file may be present.

```
myapp.mqlite          ← Main database file (always present)
myapp.mqlite-journal  ← Write journal (present during write activity)
```

| File | When present | Role |
|------|-------------|------|
| `myapp.mqlite` | Always | Persistent B-tree pages. The "real" database. |
| `myapp.mqlite-journal` | After any write, until checkpointed | Uncommitted and unmerged write pages. |

The page-offset lookup index used by readers lives **only in memory** — it is
rebuilt from a journal scan on every open. There is no on-disk sidecar for it.

**During normal operation** the two files form a single logical unit. Never
copy or move them individually — always treat the `.mqlite` and `-journal`
files as a group.

---

## Clean Close

`Client::close()` performs a **blocking checkpoint + flush**:

1. All committed journal pages are merged into `myapp.mqlite`.
2. The journal file is removed.
3. The OS advisory lock is released.

After `close()` returns, `myapp.mqlite` is the sole file on disk and can be
copied safely.

```rust
use mqlite::Client;

fn write_and_close() -> mqlite::Result<()> {
    let client = Client::open("myapp.mqlite")?;
    let col = client.database("myapp").collection::<mqlite::Document>("events");
    col.insert_one(&mqlite::doc! { "type": "shutdown" })?;

    // Blocking flush + checkpoint. After this line, myapp.mqlite is the sole file.
    client.close()?;
    Ok(())
}
```

> **`Drop` vs `close()`:** Dropping a `Client` handle is non-blocking. It
> releases the OS lock but does **not** checkpoint the journal. The journal
> file remains on disk. This is safe — the next `open()` replays the journal
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
use mqlite::Client;
use std::fs;

fn cold_backup(src: &str, dst: &str) -> mqlite::Result<()> {
    // Open, force a clean close, then copy.
    let client = Client::open(src)?;
    client.close()?; // checkpoint + remove journal

    fs::copy(src, dst)?;
    println!("Backup written to {dst}");
    Ok(())
}
```

**Important:** Verify that `src-journal` does **not** exist after `close()`.
If it does, a crash occurred between the checkpoint and the file removal —
open and close the database again before copying.

```rust
use std::path::Path;

fn assert_single_file(path: &str) {
    assert!(!Path::new(&format!("{path}-journal")).exists(), "journal still present");
}
```

---

## Checkpoint-Then-Copy

If closing the database is not an option (e.g., the app is running), you can
checkpoint the journal into the main file without closing:

```rust
use mqlite::{Client, Database};
use std::fs;

fn checkpoint_backup(client: &Client, src: &str, dst: &str) -> mqlite::Result<()> {
    // Flush all committed journal pages to the main file.
    // After this, myapp.mqlite contains all committed data.
    client.checkpoint()?;

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
  partial journal. Use a hot backup (see below) or a cold backup instead.

---

## Hot Backup

`client.backup(dest)` produces a consistent copy of the database **while it
is running**.

```rust
use mqlite::Client;

fn hot_backup(client: &Client, dst: &str) -> mqlite::Result<()> {
    // Acquires the writer lock, checkpoints, then copies the file.
    // Writers are briefly paused during the copy; readers continue unaffected.
    client.backup(dst)?;
    println!("Hot backup written to {dst}");
    Ok(())
}
```

Implementation notes:
- Acquires the in-process writer lock before copying, so the backup sees a
  fully consistent committed state.
- Checkpoints all dirty pages to the main file before copying.
- Reads through the existing lock file descriptor to avoid releasing the
  OS advisory lock (POSIX footgun prevention).
- Destination file is created with `0600` permissions on Unix.
- Backup to the same file as the source is rejected with an error.
- Backup of an in-memory database is not supported (returns an error).

---

## Size Monitoring

```rust
use mqlite::Database;
use std::fs;

fn report_db_size(path: &str) {
    let main_bytes = fs::metadata(path)
        .map(|m| m.len())
        .unwrap_or(0);

    let journal_path = format!("{path}-journal");
    let journal_bytes = fs::metadata(&journal_path)
        .map(|m| m.len())
        .unwrap_or(0);

    println!("Main file    : {:.1} MB", main_bytes as f64 / 1_048_576.0);
    println!("Journal file : {:.1} MB", journal_bytes as f64 / 1_048_576.0);
    println!("Total        : {:.1} MB", (main_bytes + journal_bytes) as f64 / 1_048_576.0);
}
```

**Journal size behaviour:**
- The journal grows with each write and shrinks (to zero) after a checkpoint.
- Auto-checkpoint triggers when the journal reaches `journal_auto_checkpoint`
  pages (default: 1,000 pages ≈ 4 MB) or `journal_max_size` bytes (default: 100 MB).
- A large journal does not affect read correctness but does slow reads (the
  reader must scan the journal for page versions).

**Main file size behaviour:**
- The main file **never shrinks automatically**. Deleted documents
  free internal B-tree pages, but those pages are reused for future writes
  rather than returned to the OS.

---

## After a Crash: Automatic WAL Recovery

If the process is killed or the host crashes while mqlite is open, the WAL
file remains on disk. The next `Client::open()` automatically:

1. Detects the leftover WAL.
2. Replays committed transactions from the WAL into memory.
3. Discards any partially-written transaction at the tail.
4. Resumes normal operation.

No manual intervention is required.

```rust
// After a crash, just open normally — recovery is automatic.
let client = mqlite::Client::open("myapp.mqlite")?;
// All committed data is available.
```

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
use mqlite::{Client, OpenOptions};

fn forensic_open(path: &str) -> mqlite::Result<mqlite::Database> {
    let client = Client::open_with_options(
        path,
        OpenOptions::new().read_only(true),
    )?;
    Ok(client.database("mydb"))
}
```

In read-only mode:
- WAL replay is skipped (reads from the last checkpointed state in the main file).
- No write operations are permitted.
- No OS exclusive lock is acquired (multiple read-only opens can coexist).

This is useful for forensic inspection or for opening a database on a
read-only filesystem (e.g., a mounted backup volume or a CD-ROM image).

---

## Network Filesystems

**mqlite does not support network filesystems** (NFS, SMB/CIFS, SSHFS, etc.).

The WAL protocol relies on:
1. **Atomic file renames** (used during checkpoints).
2. **Reliable `fcntl`/`LockFileEx` advisory locks** (used for writer exclusivity).
3. **Durable `fsync` semantics** for the WAL file.

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
let client_a = mqlite::Client::open("shared.mqlite")?;
client_a.database("mydb").collection::<mqlite::Document>("log")
    .insert_one(&mqlite::doc! { "msg": "hello" })?;
// client_a holds the exclusive write lock while inserting.

// Process B (reader) — can open concurrently:
let client_b = mqlite::Client::open_with_options(
    "shared.mqlite",
    mqlite::OpenOptions::new().read_only(true),
)?;
// client_b sees a consistent snapshot; it never blocks client_a.
```

For multiple writer processes competing for the same file, configure a
`busy_timeout` so they back off gracefully:

```rust
use mqlite::{Client, OpenOptions};
use std::time::Duration;

let client = Client::open_with_options(
    "shared.mqlite",
    OpenOptions::new().busy_timeout(Duration::from_secs(5)),
)?;
```

See [CONCURRENCY.md](CONCURRENCY.md) for the full concurrency model.
