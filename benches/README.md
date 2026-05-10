# Benchmark Suite

Release-profile Criterion benchmarks that establish reusable baselines for
core storage workloads. Compare results only against the same workload shape.

All benches run under `[profile.bench]` (inherits `release`: `opt-level=3`,
`lto="thin"`, `debug=true`).

---

## Invocation commands

### Same-Namespace Writers

```
cargo bench --bench writers_same_ns -- --save-baseline current
```

Workload: 1, 2, and 4 concurrent writers on a **single namespace**. Each writer
inserts 20 documents (~256 B payload) per Criterion iteration. Captures the
per-namespace lane bottleneck. Durability: `Interval(100ms)`.

---

### Different-Namespace Writers

```
cargo bench --bench writers_diff_ns -- --save-baseline current
```

Workload: 2 and 4 concurrent writers, each on a **distinct namespace**. Same
insert count and payload as `writers_same_ns`. Durability: `Interval(100ms)`.

---

### Payload Size Matrix

```
cargo bench --bench payload_sizes -- --save-baseline current
```

Workload: single writer, single namespace, 10 inserts per iteration across
three payload classes: `~256B` (230 B), `~4KiB` (4 000 B), `~32KiB` (32 000 B).
Actual byte counts are printed alongside each measurement. Durability:
`Interval(100ms)`.

---

### Durability Modes

```
cargo bench --bench durability_modes -- --save-baseline current
```

Workload: single writer, single namespace, ~256 B payload, 10 inserts per
iteration under two modes: `FullSync` (fdatasync after every commit) and
`Interval(100ms)`. Isolates fsync cost from all other variables.

---

### Secondary Index Build

```
cargo bench --bench index_build -- --save-baseline current
```

Workload: 10 000 documents (~64 B payload each) pre-seeded outside the timed
region; each iteration calls `create_index` on field `category` (non-unique,
ascending). Uses the public `create_index` API only. Durability:
`Interval(100ms)`.

---

### Root-Neutral CRUD

```
cargo bench --bench read_epoch_root_neutral -- --save-baseline current
```

Workload: 1, 2, and 4 concurrent writers on a **single already-
bootstrapped** namespace doing root-neutral CRUD (20 inserts per
writer per iteration, ~256 B payload, `Interval(100ms)`). Identical
shape to `writers_same_ns` so the two baselines are directly comparable.
Each iteration also prints
`read_epoch_publish_count` / `published_catalog_rebuild_count`
deltas and the computed rebuild-elision rate, so catalog-reuse behavior is
auditable from the bench output.

Compare against an existing baseline without overwriting:

```
cargo bench --bench read_epoch_root_neutral -- --baseline current
```

---

### Same-Collection Multi-Writer CRUD

```
cargo bench --bench same_collection_multiwriter -- --save-baseline current
```

Workload: one pre-split collection, 1, 2, 4, 8, and 16 concurrent root-neutral
update writers using disjoint `_id` key bands so the timed writes target
separate leaf ranges without measuring structural split work. Each measured
iteration updates one document per writer. Payload classes are exactly 256B,
4KiB, and 32KiB. Each case runs under both
`DurabilityMode::Interval(Duration::from_millis(100))` and
`DurabilityMode::FullSync`. The benchmark prints run metadata for
each case: writer count, namespace id, payload class and bytes, durability
mode, rustc version, CPU model, core count, OS/arch, and git commit. This
command saves the Criterion baseline under
`target/criterion/`.

Compare against an existing baseline:

```
cargo bench --bench same_collection_multiwriter -- --baseline current
```

---

### Reader Memory Pressure

```
cargo bench --bench reader_memory_pressure -- --save-baseline current
```

Workload: a fixed-size hot set of documents is pre-seeded outside the timed
region; each iteration reads the entire set while a background writer
continuously inserts to force page eviction. Exercises read-path behavior under
memory pressure. Durability: `Interval(100ms)`.

---

### Group-Commit LSN

```
cargo bench --bench group_commit_lsn -- --save-baseline current
```

Workload: multiple writers commit concurrently to measure LSN assignment and
group-commit throughput. Captures serialization cost in the commit path.
Durability: `Interval(100ms)`.

---

### Reopen Latency

```
cargo bench --bench reopen -- --save-baseline current
```

Two Criterion groups:

- **reopen-after-journal**: 200 documents seeded with `FullSync`, then
  `Client::close()` (implicit checkpoint on last handle). Each iteration
  measures `Client::open_with_options` + one `count_documents` + `close`.

- **reopen-after-emergency-checkpoint**: same workload but seeded with
  `journal_max_size(256 KiB)` and `journal_auto_checkpoint(50)` to trigger
  emergency checkpoints during seeding. Reopen measures recovery cost after
  this stress pattern.

Journal byte size is printed from `<db>.mqlite.journal` file size. Frame-level
counts (legacy page frames, ChainCommit frames) are not available from the
public API and are recorded as `n/a`; this is a known limitation.

---

## Short-run verification (CI / quick check)

```
cargo bench --bench <name> -- --sample-size 10 --measurement-time 1 --warm-up-time 1
```

## Rerun without overwriting the saved baseline

```
cargo bench --bench writers_same_ns -- --baseline current --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench writers_diff_ns -- --baseline current --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench payload_sizes -- --baseline current --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench durability_modes -- --baseline current --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench index_build -- --baseline current --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench read_epoch_root_neutral -- --baseline current --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench same_collection_multiwriter -- --baseline current --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench reader_memory_pressure -- --baseline current --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench group_commit_lsn -- --baseline current --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench reopen -- --baseline current --sample-size 10 --measurement-time 1 --warm-up-time 1
```

These commands compare against the saved `current` baseline without using
`--save-baseline current`, so they cannot overwrite the saved baseline.
