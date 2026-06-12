# mqlite

**An embedded, MongoDB-compatible document store for Rust: MongoDB's API with
SQLite's deployment model.**

mqlite gives you MongoDB query, update, and index semantics against a single
local file. There is no server, no replica set, and no network hop: your
application links the library, opens a path, and gets crash-safe, concurrent
document storage with snapshot reads.

```rust
use mqlite::{Client, doc};

let client = Client::open("myapp.mqlite")?;
let users = client.database("myapp").collection::<mqlite::Document>("users");
users.insert_one(&doc! { "name": "alice", "role": "admin" })?;
let admin = users.find_one(doc! { "role": "admin" })?;
```

Status: **pre-1.0**. The API is stable enough to use; the on-disk format is
not yet frozen (see [Release policy](#release-policy)).

## Highlights

- **MongoDB 8.0 operator semantics**: filters, updates, projections, and
  index-backed queries, with typed (serde) or untyped `Document` collections.
  See the [Compatibility Matrix](docs/COMPATIBILITY.md).
- **Single file on disk**: one `.mqlite` file after a clean checkpoint, plus
  an append-only journal during writes. Recovery is automatic on open.
- **WiredTiger-style storage engine**: MVCC snapshot reads, timestamp-ordered
  version chains, a write-ahead journal with group commit, ordered publish,
  and checkpoint-into-main-file. Multi-writer, multi-reader within a process.
- **Crash safety as a discipline**: checksummed journal records, torn-tail
  truncation, crash-cut and randomized crash-injection harnesses, and an
  embedded Jepsen suite ([results below](#jepsen)).
- **B+ tree secondary indexes**: `create_index` (unique, compound, multikey)
  with planner-driven index selection and `explain`-style introspection.
- **Optional MongoDB wire shim**: point `mongosh` or `pymongo` at a local
  port for tooling interop (`wire` feature; local development only, see
  [WIRE-SECURITY.md](docs/WIRE-SECURITY.md)).

## Quick Start

mqlite is pre-1.0 and not yet on crates.io; depend on git until the first
tagged release.

```toml
[dependencies]
mqlite = { git = "https://github.com/kparkins/mqlite" }
serde = { version = "1", features = ["derive"] }
```

**Untyped documents:**

```rust
use mqlite::{Client, doc};

fn main() -> mqlite::Result<()> {
    let client = Client::open("myapp.mqlite")?;
    let db = client.database("myapp");
    let events = db.collection::<mqlite::Document>("events");
    events.insert_one(&doc! { "action": "login", "user": "alice" })?;
    let event = events.find_one(doc! { "user": "alice" })?;
    println!("{:?}", event);
    Ok(())
}
```

**Typed structs (serde):**

```rust
use mqlite::{Client, doc};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct Config { key: String, value: String }

fn main() -> mqlite::Result<()> {
    let client = Client::open("myapp.mqlite")?;
    let db = client.database("myapp");
    let configs = db.collection::<Config>("config");
    configs.insert_one(&Config { key: "theme".into(), value: "dark".into() })?;
    let theme = configs.find_one(doc! { "key": "theme" })?;
    println!("{:?}", theme);
    Ok(())
}
```

The storage engine is synchronous; async applications should run mqlite calls
on a blocking worker pool. These snippets are mirrored by
`tests/readme_examples.rs`, so they are compile- and behavior-checked on
every test run.

## Architecture

mqlite implements WiredTiger-inspired storage semantics in a single-node,
embedded package:

- **Paged B+ trees.** Collections and indexes are B+ trees over a partitioned
  buffer pool (32 KiB leaf pages, 4 KiB internal pages). Large values spill to
  overflow chains.
- **MVCC snapshot reads.** Writers append timestamp-ordered versions to
  per-key chains; readers open a `ReadView` pinned in a registry so
  reclamation never drops a version a live reader still needs. Reads never
  block writes; writes never block reads.
- **Write-ahead journal with group commit.** Commits reserve LSN-ordered slots
  in an append-only journal; a group-commit protocol batches fsyncs. Commits
  become visible through an ordered publish sequencer, so visibility order
  always matches journal order.
- **Checkpoints.** A checkpoint materializes committed versions into the main
  file, writes a single boundary record, and truncates the journal.
  `Client::close()` checkpoints, leaving one file at rest.
- **History store.** Superseded versions needed by long-lived readers spill to
  a history store instead of pinning the working set in memory.
- **Concurrency model.** Multiple writer threads and reader threads operate
  concurrently in one process (per-namespace write lanes, latch-crabbing tree
  descent, lock-order-audited reclamation). Cross-process access is guarded by
  OS file locks. See [CONCURRENCY.md](docs/CONCURRENCY.md).

## Performance

Throughput is measured by the operation-scoped `perf_matrix` harness: the
timed window covers only public API operations (setup, document generation,
and final close/checkpoint are excluded), and each row is the median of 11
runs after a discarded warm-up. Methodology, axes, and the machine-readable
baseline sidecar live in [PERFORMANCE.md](docs/PERFORMANCE.md) and
[docs/perf-baselines/](docs/perf-baselines/).

Collected 2026-06-11 from this revision on a desktop AMD Ryzen 7 7800X3D
(8 cores / 16 threads, 32 GB, Windows 11, NVMe), batch size 100, medians of
11 runs per cell ([sidecars](docs/perf-baselines/)):

| Axis | Unit | `full-sync` | `interval-50ms` (default) | `none` |
|---|---|---:|---:|---:|
| 1 writer, 1 namespace, `insert_one` | docs/s | 65 | 12,372 | 16,353 |
| 1 writer, 1 namespace, `insert_many` | docs/s | 6,495 | 114,221 | 177,473 |
| 4 writers, 1 namespace, `insert_one` | docs/s | 259 | 4,906 | 5,355 |
| 4 writers, 1 namespace, `insert_many` | docs/s | 26,224 | 164,486 | 240,554 |
| 4 writers, 4 namespaces, `insert_one` | docs/s | 259 | 42,292 | 52,428 |
| 4 writers, 4 namespaces, `insert_many` | docs/s | 26,274 | 333,697 | 436,137 |
| point reads (`find_one` by `_id`) | ops/s | 449,169 | 444,561 | 449,887 |

The `interval-50ms` and `none` columns use the canonical 20,000 documents per
writer. The `full-sync` column uses 1,000 documents per writer: every
acknowledged commit waits for an fsync (about 15 ms on this disk), so
canonical-length runs would take hours, and throughput is rate-based either
way. The fsync-bound rows are also the most stable in the matrix, with
run-to-run envelopes under 2%; the per-row min/max spread for every cell is
recorded in the sidecars. Numbers are platform-sensitive, the fsync-bound
`full-sync` column especially so (fsync latency varies by an order of
magnitude across OS/filesystem/disk). Reproduce on your hardware with
`benches/perf/run_baselines.py`.

### Durability modes

| Mode | Guarantee | Cost |
|---|---|---|
| `FullSync` | every acknowledged commit is fsync-durable | slowest; bounded by fsync latency |
| `Interval(50ms)` *(default)* | journal readiness before publish; ready prefix synced every 50 ms | survives process crashes; an OS crash/power loss can lose the last ≤50 ms |
| `None` | no explicit sync | throughput ceiling for ephemeral data |

## Correctness

mqlite treats correctness as the primary feature. The verification surface
(as of June 2026):

- **~2,500 test executions per gate**: 1,372 unit and integration tests in
  the full configuration (internal crash/state probes enabled) plus 1,169 in
  the default build, both in a release-optimized profile with a zero-warning
  clippy gate.
- **Crash recovery harnesses**: a crash-cut matrix that severs writes at
  controlled points in the commit pipeline, randomized crash injection across
  hundreds of kill/recover cycles, and multi-writer crash-recovery suites.
- **Concurrency model checking**: loom-based exhaustive interleaving tests
  for the lock-free handoff protocols, plus lock-order audit tests that pin
  the documented lock hierarchy to the code.
- **Fuzzing**: seven cargo-fuzz targets covering BSON parsing, key encoding,
  journal record decoding, recovery, query evaluation, and the wire protocol.
- **Jepsen**: see below.

<a name="jepsen"></a>
### Jepsen

The embedded Jepsen suite (`tests/jepsen/`) drives mqlite through a minimal
localhost adapter so Jepsen's generators, nemeses, and checkers (including
Knossos) run against the real `mqlite::Client` API. Because mqlite is a
single-node embedded store, the suite uses a **restart nemesis** (repeatedly
killing and restarting the process against the same database file
mid-workload) rather than network-partition or replica-set nemeses, which
would test claims mqlite doesn't make.

Fifteen workloads, each checking a distinct safety property:

| Workload | Invariant checked |
|---|---|
| `register` | linearizability of read/write/CAS on independent registers (Knossos) |
| `set` | no acknowledged insert lost across process restarts |
| `delete-set` | acknowledged deletes never resurrect after recovery |
| `read-your-writes` | acknowledged writes immediately visible |
| `unique-index` | no duplicate values under racing inserts on a unique index |
| `secondary-index` | indexed reads match full scans under upserts/deletes + restarts |
| `compound-index` | same, for `{a: 1, b: 1}` compound indexes |
| `multikey-index` | same, for array-field (multikey) indexes |
| `index-build` | `create_index` racing live writes yields a consistent index |
| `drop-index` | racing `drop_index`/`create_index` converges to consistent reads |
| `namespace-isolation` | concurrent writes land only in their own collection |
| `count-consistency` | `count_documents` agrees with a full scan after recovery |
| `find-and-modify-claim` | no job claimed by more than one acknowledged worker |
| `long-scan-snapshot` | ordered scans observe at most one epoch under racing updates |
| `write-batch-prefix` | ordered `insert_many` with a mid-batch duplicate keeps exactly the acknowledged prefix |

**Results: in the runs collected to date, all fifteen workloads pass under
the restart nemesis** (the suite's default: the adapter process is killed
and restarted against the same database file every few seconds): no
linearizability violations, no lost acknowledged writes, no duplicate unique
keys, no index/scan divergence. These are short-to-moderate runs, not
long-duration soaks. Reproduce with:

```sh
./tests/jepsen/run.sh --workload all --nemesis restart
```

(Requires Rust, Java 21+, and the `clojure` CLI. See
[tests/jepsen/README.md](tests/jepsen/README.md) and
[VERIFICATION.md](docs/VERIFICATION.md).)

## Files on Disk

| File | When present | Meaning |
|------|-------------|---------|
| `myapp.mqlite` | always | main database file |
| `myapp.mqlite-journal` | during writes | append-only journal; replayed on next open |

"Single-file database" means a single file after a successful checkpoint.
`Client::close()` runs the checkpoint and returns any error. Dropping the last
`Client` handle also attempts a checkpoint, but cannot report failures. If a
process exits or crashes before that checkpoint finishes, the journal stays on
disk and the next `Client::open` recovers it automatically.

## Feature Flags

- `wire`: MongoDB wire-protocol (OP_MSG) shim for local tool interop.
- `tracing`: structured observability via the `tracing` crate.
- `test-hooks`, `fuzz`, `loom-tests`, `perf-counters`: internal verification
  surfaces; do not enable in production builds.

## Limitations and Non-Goals

- Single-node and embedded by design: no replication, sharding, or elections.
- One process at a time holds the database open for writing (OS file locks
  guard cross-process access).
- The wire shim has no authentication or TLS; it is for local development
  only ([WIRE-SECURITY.md](docs/WIRE-SECURITY.md)).
- Aggregation pipelines and a handful of operators are unsupported; the
  [Compatibility Matrix](docs/COMPATIBILITY.md) is the source of truth.

## Documentation

- [Compatibility Matrix](docs/COMPATIBILITY.md): operator- and command-level
  MongoDB compatibility
- [Concurrency Model](docs/CONCURRENCY.md): MWMR reads, write lanes,
  cross-process locking
- [Errors](docs/ERRORS.md): `Error` variants and MongoDB error-code mapping
- [File Management](docs/FILE-MANAGEMENT.md): backup, checkpoint, crash
  recovery
- [Performance Guide](docs/PERFORMANCE.md): benchmark axes, baseline
  sidecars, profiling
- [Verification Guide](docs/VERIFICATION.md): tests, Jepsen, correctness
  gates
- [Wire Protocol Security Advisory](docs/WIRE-SECURITY.md)

## How This Was Built

mqlite is an experiment in AI-agent-driven engineering: the overwhelming
majority of its code was written by AI coding agents, directed and reviewed
through an explicit engineering discipline. Every bug fix starts from a
failing test, storage refactors must preserve hot paths byte-for-byte and pass
multi-writer throughput A/B gates, adversarial reviewer agents audit every
change, and correctness claims are backed by the verification surface above
(crash harnesses, loom, fuzzing, Jepsen). The interesting result is not that
an AI wrote a database; it's that agent-written code can be held to this bar,
and pass it.

## Release Policy

Prior to v1.0, mqlite makes no on-disk format stability guarantees between
tagged releases: any release may change the binary layout of the database
file, journal, or catalog in ways that require recreating existing files.
From v1.0, format changes will be gated behind explicit migration paths or
version flags.

## License

Licensed under either of [Apache License 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
