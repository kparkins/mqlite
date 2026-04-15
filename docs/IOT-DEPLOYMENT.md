# mqlite IoT and Embedded Deployment Guide

Running mqlite on Raspberry Pi, microcontrollers, and other constrained devices.

---

## Why mqlite for IoT?

| Requirement | mqlite |
|-------------|--------|
| **No server process** | ✅ Embedded — runs in-process |
| **Single binary** | ✅ Zero C dependencies — pure Rust |
| **Cross-compilation** | ✅ `cargo build --target aarch64-unknown-linux-gnu` |
| **Low memory** | ✅ Buffer pool configurable down to ~1 MB |
| **Power-loss recovery** | ✅ WAL replay on next open — automatic |
| **Single-file database** | ✅ One `.mqlite` file after clean close |
| **Offline operation** | ✅ No network required |

---

## Minimal Configuration

Default settings are tuned for server hardware (64 MB buffer pool, 100 MB WAL
cap). On a Raspberry Pi or similar device, tune these down:

```rust
use mqlite::{Client, OpenOptions, DurabilityMode};
use std::time::Duration;

fn open_for_iot(path: &str) -> mqlite::Result<Client> {
    Client::open_with_options(
        path,
        OpenOptions::new()
            // 4 MB buffer pool — suitable for Raspberry Pi and similar SBCs.
            // Minimum recommended: 1 MB. Below 512 KB, B-tree performance degrades.
            .buffer_pool_size(4 * 1024 * 1024)
            // Cap the WAL at 8 MB. Auto-checkpoint more aggressively.
            .wal_max_size(8 * 1024 * 1024)
            .wal_auto_checkpoint(256)
            // 8 concurrent readers (vs default 64). Reduces memory for
            // the reader registration table.
            .max_readers(8)
            // 3-second busy timeout — allows brief contention bursts.
            .busy_timeout(Duration::from_secs(3)),
    )
}
```

### Quick reference: configurable limits

| Option | Default | Recommended (4 MB device) | Minimum safe |
|--------|---------|--------------------------|--------------|
| `buffer_pool_size` | 64 MB | 4 MB | 512 KB |
| `wal_max_size` | 100 MB | 8 MB | 2 MB |
| `wal_auto_checkpoint` (pages) | 1,000 | 256 | 64 |
| `max_readers` | 64 | 8 | 2 |

---

## Durability vs. Flash Longevity

The default durability mode is `Interval(100ms)`: mqlite fsyncs the WAL at
most once per 100 ms. This is a good balance for most deployments.

On flash storage (SD cards, eMMC), **every fsync burns write cycles**. Two
strategies:

### Strategy A — `FullSync` mode (maximum safety, more flash wear)

Use when data loss is unacceptable (medical sensors, financial meters):

```rust
use mqlite::{Client, OpenOptions, DurabilityMode};

let client = Client::open_with_options(
    "sensor.mqlite",
    OpenOptions::new()
        .buffer_pool_size(4 * 1024 * 1024)
        .durability(DurabilityMode::FullSync),
)?;
let db = client.database("iot");
```

Each committed write triggers one fsync. On a Class 10 SD card this is
typically 5–20 ms and accelerates wear.

### Strategy B — `Interval` mode with a longer interval (reduced wear)

Use when a short data-loss window is acceptable (telemetry, log data):

```rust
use mqlite::{Client, OpenOptions, DurabilityMode};
use std::time::Duration;

let client = Client::open_with_options(
    "sensor.mqlite",
    OpenOptions::new()
        .buffer_pool_size(4 * 1024 * 1024)
        // fsync at most once per second instead of once per 100 ms.
        // A power cut can lose at most ~1 second of committed data.
        .durability(DurabilityMode::Interval(Duration::from_secs(1))),
)?;
let db = client.database("iot");
```

### Trade-off summary

| Mode | fsync frequency | Data loss window | Flash wear |
|------|----------------|-----------------|------------|
| `FullSync` | Every write | Zero | High |
| `Interval(100ms)` | ≤ every 100 ms | ≤ 100 ms | Medium |
| `Interval(1s)` | ≤ every 1 s | ≤ 1 s | Low |
| `None` | Never (manual only) | Unlimited | Very low |

> **SD card recommendation:** Use `Interval(Duration::from_secs(1))` for
> typical IoT telemetry. Reserve `FullSync` for data where every record is
> critical and the SD card is an industrial-grade part rated for high write
> endurance.

---

## Handling Disk Full (`Error::DiskFull`)

Embedded devices often have small storage. A disk-full condition returns
`Error::DiskFull` and **rolls back** the current write. The database remains
consistent; no data is lost from previously committed records.

```rust
use mqlite::{Error, doc};

fn insert_reading(db: &mqlite::Database, sensor_id: &str, value: f64) -> mqlite::Result<()> {
    let readings = db.collection::<mqlite::Document>("readings");

    match readings.insert_one(&doc! { "sensor": sensor_id, "value": value }) {
        Ok(_) => Ok(()),

        Err(Error::DiskFull { path, available_bytes, .. }) => {
            eprintln!(
                "Disk full at {:?}: only {} bytes available. Pruning old data.",
                path, available_bytes
            );

            // Delete old records to free space, then retry.
            prune_oldest_readings(db, 100)?;

            // Retry the insert.
            readings.insert_one(&doc! { "sensor": sensor_id, "value": value })?;
            Ok(())
        }

        Err(e) => Err(e),
    }
}

fn prune_oldest_readings(db: &mqlite::Database, count: u64) -> mqlite::Result<()> {
    use mqlite::options::FindOptions;

    let readings = db.collection::<mqlite::Document>("readings");
    // Find the oldest `count` records by timestamp and delete them.
    let cursor = readings.find_with_options(
        doc! {},
        FindOptions::new()
            .sort(doc! { "ts": 1_i32 })
            .limit(count as i64),
    )?;
    let ids: Vec<_> = cursor
        .map(|r| r.map(|d| d.get_object_id("_id").unwrap()))
        .collect::<mqlite::Result<_>>()?;
    for id in ids {
        readings.delete_one(doc! { "_id": id })?;
    }
    Ok(())
}
```

**Pattern:** on `DiskFull`, delete the oldest or lowest-priority records, then
retry. This is the standard ring-buffer pattern for sensor data storage.

---

## Power-Loss Recovery

mqlite uses a Write-Ahead Log (WAL). When power is cut mid-write:

1. The partially-written transaction is discarded (it was never committed).
2. On the next `Client::open()`, the WAL is replayed automatically.
3. All transactions that completed before the power cut are fully restored.

```rust
// After power loss, just open normally — WAL replay is automatic.
let client = Client::open_with_options(
    "sensor.mqlite",
    OpenOptions::new().buffer_pool_size(4 * 1024 * 1024),
)?;
// All previously committed sensor readings are available.
```

No `fsck`, no repair command, no manual intervention.

**Durability guarantee by mode:**
- `FullSync`: every committed write is in the WAL on disk before `insert_one`
  returns. Power cuts lose zero committed data.
- `Interval(d)`: commits are durable within `d` of the write returning `Ok`.
  Power cuts may lose the last `d` of committed data.
- `None`: WAL is not fsynced; all data since the last checkpoint may be lost.

> **Recommendation for IoT:** `Interval(Duration::from_secs(1))` is the
> practical sweet spot — most sensor readings are worthless after a few
> seconds anyway, and the flash wear is dramatically lower than `FullSync`.

---

## Read-Only Filesystem Recovery

If the device boots with a read-only filesystem (corrupt SD card, deliberate
read-only mount for forensics), open the database in read-only mode for
inspection:

```rust
use mqlite::{Client, OpenOptions};

fn open_readonly_for_rescue(path: &str) -> mqlite::Result<Client> {
    Client::open_with_options(
        path,
        OpenOptions::new()
            .read_only(true)
            .buffer_pool_size(2 * 1024 * 1024),
    )
}
```

In read-only mode:
- WAL replay is skipped (last checkpointed state is visible).
- No writes are allowed.
- The `.mqlite-shm` file is not created.
- No exclusive OS lock is acquired.

This lets you extract readings or audit data even when the filesystem is
mounted read-only. See [FILE-MANAGEMENT.md](FILE-MANAGEMENT.md) for more.

---

## Cross-Compilation

mqlite has **zero C dependencies** in its default configuration. Cross-compile
like any other pure-Rust crate:

```bash
# Install the target once:
rustup target add aarch64-unknown-linux-gnu

# Build for 64-bit ARM Linux (Raspberry Pi 3/4/5, NVIDIA Jetson, etc.):
cargo build --release --target aarch64-unknown-linux-gnu

# Build for 32-bit ARM Linux (Raspberry Pi 2, many industrial SBCs):
rustup target add armv7-unknown-linux-gnueabihf
cargo build --release --target armv7-unknown-linux-gnueabihf

# Build for RISC-V 64 (e.g., StarFive VisionFive):
rustup target add riscv64gc-unknown-linux-gnu
cargo build --release --target riscv64gc-unknown-linux-gnu
```

### Cross-compiling with the `wire` feature

The `wire` feature adds `tokio` (an async runtime). This requires a linker
for the target. Install `cross` for a Docker-based, zero-config cross-compiler:

```bash
cargo install cross
cross build --release --target aarch64-unknown-linux-gnu --features wire
```

### Verifying the binary has no unexpected C dependencies

```bash
# On the build host, check the target binary:
file target/aarch64-unknown-linux-gnu/release/my-sensor-app
# aarch64-unknown-linux-gnu/release/my-sensor-app: ELF 64-bit LSB pie executable, ARM aarch64

# Confirm no unexpected shared library deps (libc is expected; libpq, libssl etc. are not):
aarch64-linux-gnu-readelf -d target/aarch64-unknown-linux-gnu/release/my-sensor-app | grep NEEDED
# Should only show: libc.so.6 (and libm, libpthread on glibc targets)
```

### musl (fully static binary)

For devices with minimal or missing glibc, use a musl target:

```bash
rustup target add aarch64-unknown-linux-musl
cargo build --release --target aarch64-unknown-linux-musl
# Produces a fully static binary with no runtime dependencies.
```

> **Note:** musl targets require a musl cross-linker
> (`aarch64-linux-musl-gcc`). Install via your distro's package manager or
> use `cross`.

---

## Process Isolation

**One process per `.mqlite` file.** mqlite uses OS advisory locks to ensure
only one writer is active at a time, but locks are per-process on POSIX systems.
Two threads in the same process can both hold a `Database` handle safely — the
in-process `Mutex` serializes writes. Two processes writing the same file is
also supported (advisory locks coordinate them). Three or more processes are
supported too — each will queue behind the writer lock.

For multi-sensor architectures, use one file per sensor stream:

```
/data/sensors/temperature.mqlite   ← temperature readings process
/data/sensors/humidity.mqlite      ← humidity readings process
/data/sensors/pressure.mqlite      ← pressure readings process
```

This eliminates cross-sensor lock contention entirely.

See [FILE-MANAGEMENT.md](FILE-MANAGEMENT.md) for multi-process safety rules.

---

## Resource Limit Summary

All resource limits are configurable at open time via `OpenOptions`:

```rust
use mqlite::{Client, OpenOptions, DurabilityMode};
use std::time::Duration;

// Raspberry Pi Zero (512 MB RAM, Class 10 SD card):
let client = Client::open_with_options(
    "/data/sensor.mqlite",
    OpenOptions::new()
        .buffer_pool_size(2 * 1024 * 1024)           // 2 MB
        .wal_max_size(4 * 1024 * 1024)               // 4 MB WAL cap
        .wal_auto_checkpoint(128)                     // checkpoint every 128 pages
        .max_readers(4)                               // 4 reader slots
        .busy_timeout(Duration::from_secs(5))         // wait up to 5 s for writer lock
        .durability(DurabilityMode::Interval(
            Duration::from_secs(1)                    // fsync once per second
        )),
)?;
let db = client.database("iot");
```

```rust
// Raspberry Pi 4 (4 GB RAM, fast SD or NVMe):
let client = Client::open_with_options(
    "/data/sensor.mqlite",
    OpenOptions::new()
        .buffer_pool_size(64 * 1024 * 1024)          // 64 MB (default)
        .max_readers(16)
        .busy_timeout(Duration::from_secs(5))
        .durability(DurabilityMode::Interval(
            Duration::from_millis(100)                // default
        )),
)?;
let db = client.database("iot");
```

| Resource | `OpenOptions` method | Default | Minimum |
|----------|---------------------|---------|---------|
| Buffer pool | `.buffer_pool_size(bytes)` | 64 MB | 512 KB |
| WAL cap | `.wal_max_size(bytes)` | 100 MB | 2 MB |
| Checkpoint pages | `.wal_auto_checkpoint(pages)` | 1,000 | 64 |
| Reader slots | `.max_readers(n)` | 64 | 2 |
| Writer lock wait | `.busy_timeout(d)` | 5 s | `Duration::ZERO` |
| fsync frequency | `.durability(mode)` | Interval(100ms) | — |
