# Phase 0 Benchmark Suite

Release-profile Criterion benchmarks that establish a reusable baseline for the
Storage Upgrade Phase 0 work. Later phases claim improvement only against the
same workload shape.

All benches run under `[profile.bench]` (inherits `release`: `opt-level=3`,
`lto="thin"`, `debug=true`).

---

## Invocation commands

### US-008 — same-namespace writers

```
cargo bench --bench writers_same_ns -- --save-baseline phase0
```

Workload: 1, 2, and 4 concurrent writers on a **single namespace**. Each writer
inserts 20 documents (~256 B payload) per Criterion iteration. Captures the
per-namespace lane bottleneck. Durability: `Interval(100ms)`.

---

### US-009 — different-namespace writers

```
cargo bench --bench writers_diff_ns -- --save-baseline phase0
```

Workload: 2 and 4 concurrent writers, each on a **distinct namespace**. Same
insert count and payload as US-008. No lane sharing; remaining serialization
point is the global `commit_seq`. Durability: `Interval(100ms)`.

---

### US-010 — payload size matrix

```
cargo bench --bench payload_sizes -- --save-baseline phase0
```

Workload: single writer, single namespace, 10 inserts per iteration across
three payload classes: `~256B` (230 B), `~4KiB` (4 000 B), `~32KiB` (32 000 B).
Actual byte counts are printed alongside each measurement. Durability:
`Interval(100ms)`.

---

### US-011 — durability modes

```
cargo bench --bench durability_modes -- --save-baseline phase0
```

Workload: single writer, single namespace, ~256 B payload, 10 inserts per
iteration under two modes: `FullSync` (fdatasync after every commit) and
`Interval(100ms)`. Isolates fsync cost from all other variables.

---

### US-012 — secondary index build

```
cargo bench --bench index_build -- --save-baseline phase0
```

Workload: 10 000 documents (~64 B payload each) pre-seeded outside the timed
region; each iteration calls `create_index` on field `category` (non-unique,
ascending). Uses the public `create_index` API only. Durability:
`Interval(100ms)`.

---

### Phase 1 US-017 — root-neutral CRUD

```
cargo bench --bench read_epoch_root_neutral -- --save-baseline phase1
```

Workload: 1, 2, and 4 concurrent writers on a **single already-
bootstrapped** namespace doing root-neutral CRUD (20 inserts per
writer per iteration, ~256 B payload, `Interval(100ms)`). Identical
shape to US-008's `writers_same_ns` so the two baselines are
directly comparable. Each iteration also prints
`read_epoch_publish_count` / `published_catalog_rebuild_count`
deltas and the computed rebuild-elision rate, so Phase 1's catalog-
reuse win is auditable from the bench output.

Compare against phase0 without overwriting:

```
cargo bench --bench read_epoch_root_neutral -- --baseline phase0
```

---

### Phase 5 US-021 — same-collection multi-writer CRUD

```
cargo bench --bench phase5_multiwriter -- --save-baseline phase5
```

Workload: one pre-split collection, 1, 2, 4, 8, and 16 concurrent root-neutral
update writers using disjoint `_id` key bands so the timed writes target
separate leaf ranges without measuring structural split work. Each measured
iteration updates one document per writer. Payload classes are exactly 256B,
4KiB, and 32KiB. Each case runs under both
`DurabilityMode::Interval(Duration::from_millis(100))` and
`DurabilityMode::FullSync`. The benchmark prints Phase 0-style metadata for
each case: writer count, namespace id, payload class and bytes, durability
mode, rustc version, CPU model, core count, OS/arch, and git commit. The Phase
0 baseline materialization and pass/fail comparison are separate Phase 5
stories; this command saves the Phase 5 Criterion baseline under
`target/criterion/`.

Compare against phase0 after US-032 materializes the baseline:

```
cargo bench --bench phase5_multiwriter -- --baseline phase0
```

---

### US-013 — reopen latency

```
cargo bench --bench reopen -- --save-baseline phase0
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

## Rerun without overwriting the phase0 baseline

```
cargo bench --bench writers_same_ns -- --baseline phase0 --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench writers_diff_ns -- --baseline phase0 --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench payload_sizes -- --baseline phase0 --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench durability_modes -- --baseline phase0 --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench index_build -- --baseline phase0 --sample-size 10 --measurement-time 1 --warm-up-time 1
cargo bench --bench reopen -- --baseline phase0 --sample-size 10 --measurement-time 1 --warm-up-time 1
```

These commands compare against the canonical `phase0` baseline without using
`--save-baseline phase0`, so they cannot overwrite the saved baseline.
