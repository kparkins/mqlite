# mqlite File Management Guide

How to safely copy, back up, monitor, and recover mqlite database files.

---

## The "Single File" Promise

mqlite is advertised as a **single-file database**. The fine print:

> A mqlite database is a single file after a successful checkpoint.
> During normal operation, one additional file may be present.

```
myapp.mqlite          Main database file (always present)
myapp.mqlite-journal  Write journal (present during write activity)
```

| File | When present | Role |
|------|-------------|------|
| `myapp.mqlite` | Always | Persistent B-tree pages. The "real" database. |
| `myapp.mqlite-journal` | After any write, until checkpointed | Uncommitted and unmerged write pages. |

The page-offset lookup index used by readers lives **only in memory** - it is
rebuilt from a journal scan on every open. There is no on-disk sidecar for it.

**During normal operation** the two files form a single logical unit. Never
copy or move them individually - always treat the `.mqlite` and `-journal`
files as a group.

---

## Clean Close

`Client::close()` performs a **blocking checkpoint + flush** and returns any
error to the caller:

1. All committed journal pages are merged into `myapp.mqlite`.
2. The journal file is removed.
3. The OS advisory lock is released.

After `close()` returns `Ok(())`, `myapp.mqlite` is the sole file on disk and
can be copied safely.

```rust
use mqlite::Client;

fn write_and_close() -> mqlite::Result<()> {
    let client = Client::open("myapp.mqlite")?;
    let col = client.database("myapp").collection::<mqlite::Document>("events");
    col.insert_one(&mqlite::doc! { "type": "shutdown" })?;

    // Blocking flush + checkpoint. After Ok, myapp.mqlite is the sole file.
    client.close()?;
    Ok(())
}
```

> **`Drop` vs `close()`:** Dropping the last `Client` handle currently attempts
> the same checkpoint path, but `Drop` cannot report checkpoint errors. If the
> process exits, crashes, or the checkpoint cannot complete, the journal file
> may remain on disk. This is safe because the next `open()` recovers the
> journal automatically. Call `close()` explicitly when you need a reported
> single-file handoff.

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
If it does, a crash or checkpoint failure left recovery work behind. Open and
close the database again before copying.

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
    // For single-process apps using one shared Client, checkpoint drains
    // writers before it publishes the checkpointed state.
    fs::copy(src, dst)?;
    println!("Checkpoint backup written to {dst}");
    Ok(())
}
```

**When is this safe?**
- Single-process apps: `checkpoint()` serializes with writers admitted through
  the same engine, so the copy happens after those writes are drained.
- Multi-process setups: a second process could start writing between
  `checkpoint()` and the `fs::copy`. Use a hot backup or a cold backup instead.

---

## Hot Backup

`client.backup(dest)` produces a consistent copy of the database **while it
is running**.

```rust
use mqlite::Client;

fn hot_backup(client: &Client, dst: &str) -> mqlite::Result<()> {
    // Uses the existing database lock fd, checkpoints, then copies the file.
    // Writers are briefly paused during the copy; readers continue unaffected.
    client.backup(dst)?;
    println!("Hot backup written to {dst}");
    Ok(())
}
```

Implementation notes:
- Drains in-process writers before copying, so the backup sees a fully
  consistent committed state for this client.
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
- The journal grows with writes and shrinks to an empty or removed state after
  a checkpoint.
- `Client::checkpoint()`, `Database::checkpoint()`, `backup()`, `close()`, and
  the last-handle drop path can checkpoint the journal. Do not rely on a
  background checkpoint thread.
- A large journal does not affect read correctness but can slow open-time
  recovery because the journal scan has more records to validate.

**Main file size behaviour:**
- The main file **never shrinks automatically**. Deleted documents
  free internal B-tree pages, but those pages are reused for future writes
  rather than returned to the OS.

---

## After a Crash: Automatic Journal Recovery

If the process is killed or the host crashes while mqlite is open, the
journal file remains on disk. The next `Client::open()` automatically:

1. Detects the leftover journal.
2. Scans byte-LSN log records forward, accepting only complete
   CRC-valid records and stopping at the first torn or invalid record.
3. Truncates the journal to the valid end LSN.
4. Replays accepted records by persisted `publish_seq` (skipping any whose
   `end_lsn <= checkpoint_applied_lsn`) and resumes normal operation.

No manual intervention is required.

```rust
// After a crash, just open normally. Recovery is automatic.
let client = mqlite::Client::open("myapp.mqlite")?;
// All committed data is available.
```

> **Committed vs uncommitted at crash time:**
> Transactions whose log record was fully written and (for `FullSync`) durable
> before the crash are replayed and are fully durable. Transactions that
> reserved a slot but failed before `mark_written` are not replayed; under
> `Interval` or `None`, writes that finished `mark_written` but had not been
> fsynced may not survive the crash.

---

## Read-Only Access After Failure

If the main file is corrupt but the journal is intact, or if you need to
inspect a database without risking further damage, open it in read-only mode:

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
- No write operations are permitted after open.
- No OS exclusive writer lock is acquired, so multiple read-only opens can
  coexist.
- The current open path still validates and recovers an existing journal before
  it exposes the database. Use a clean checkpointed file when opening from a
  read-only filesystem or immutable backup volume.

This is useful for forensic inspection when the file can be opened and any
required recovery can run.

---

## Network Filesystems

**mqlite does not support network filesystems** (NFS, SMB/CIFS, SSHFS, etc.).

The journal protocol relies on:
1. **Atomic file renames** (used during checkpoints).
2. **Reliable `fcntl`/`LockFileEx` advisory locks** (used for writer exclusivity).
3. **Durable `fsync` semantics** for the journal file.

Network filesystems frequently fail to provide all three guarantees. The result
is database corruption that may not be detected immediately.

Use mqlite only on **local block storage**: local SSDs, NVMe, SD cards, or
loop-mounted images.

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

// Process B (reader) - can open concurrently:
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
