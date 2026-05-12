# mqlite Performance Guide

This guide describes the supported performance measurement surfaces. Use it for
throughput baselines, regression checks, and profiling artifacts. Correctness
proofs live in [VERIFICATION.md](VERIFICATION.md); performance wins should be
reported only after the relevant correctness gate still passes.

## Measurement Contract

Use the operation-scoped `perf_matrix` binary for write-throughput and point-read
baselines. Its measured window starts after database setup, namespace creation,
thread creation, and synthetic document generation. Timed work is limited to
public API operations on prebuilt `_id` primary-key documents.

Build and inspect the available axes:

```sh
cargo build --release --bin perf_matrix
target/release/perf_matrix --list-axes
target/release/perf_matrix --list-durability
```

Run one axis:

```sh
target/release/perf_matrix \
    --axis multi_writer_single_ns_single \
    --durability interval-50ms \
    --writers 4 \
    --docs-per-writer 20000 \
    --batch-size 100
```

For sidecar collection, the baseline runner passes
`--exit-after-measurement`. That keeps the subprocess contract aligned with
`"timed_scope":"operation_only"` by skipping final `Client::drop` checkpoint
work after the metric has already been printed. Close/checkpoint latency should
be measured by a separate benchmark.

## Canonical Axes

| Axis | Writers | Namespaces | Operation |
|---|---:|---:|---|
| `single_writer_single_ns_single` | 1 | 1 | `insert_one` |
| `single_writer_single_ns_batch` | 1 | 1 | `insert_many` |
| `multi_writer_single_ns_single` | 4 default | 1 | `insert_one` |
| `multi_writer_single_ns_batch` | 4 default | 1 | `insert_many` |
| `multi_writer_multi_ns_single` | 4 default | one per writer | `insert_one` |
| `multi_writer_multi_ns_batch` | 4 default | one per writer | `insert_many` |
| `read_find_one` | 1 | 1 | point read |

The single-namespace multi-writer axes are the main contention signal. The
multi-namespace axes separate namespace-lane overhead from global journal and
publish sequencing overhead. The batched axes measure the `insert_many` path,
not repeated single inserts.

## Durability Modes

The matrix supports three durability labels:

| Label | Mode | Guarantee |
|---|---|---|
| `full-sync` | `FullSync` | fsync after every commit returns |
| `interval-50ms` | `Interval(50ms)` | default interval sync profile |
| `none` | `None` | no explicit sync durability guarantee |

`interval-50ms` is the default mqlite profile. It waits for journal readiness
before publishing, then syncs the ready journal prefix on a 50ms interval. It
is intended to survive process crashes when the OS keeps accepted writes, but
an OS crash or power loss can lose commits since the last successful interval
sync. `none` is an unsafe ceiling for throughput comparisons.

## Baseline Sidecars

Use the median sidecar runner when you need a stable artifact that can be
checked into `docs/perf-baselines/` or compared by automation:

```sh
benches/perf/run_baselines.py \
    --out docs/perf-baselines/current.json \
    --runs 11 \
    --docs-per-writer 20000 \
    --batch-size 100
```

By default, the sidecar runner emits every canonical axis for every durability
label, so row identity is `(axis, writers, durability)`. Use repeated
`--durability` flags for targeted runs:

```sh
benches/perf/run_baselines.py \
    --out /tmp/mqlite-perf-interval.json \
    --durability interval-50ms
```

Use `--quick` only for smoke testing the runner:

```sh
benches/perf/run_baselines.py --quick --out /tmp/mqlite-perf-smoke.json
```

The JSON schema is documented in
[docs/perf-baselines/SCHEMA.md](perf-baselines/SCHEMA.md) and validated by:

```sh
cargo test --test perf_baseline_schema
```

## Current Baseline Snapshot

The current checked-in snapshot is
[`docs/perf-baselines/current.json`](perf-baselines/current.json), collected on
2026-05-11 from branch label `docs-durability-matrix-full`.

Hardware: MacBook Pro `Mac15,7`, Apple M3 Pro, 12 cores, 36 GB memory.

Command:

```sh
benches/perf/run_baselines.py \
    --out docs/perf-baselines/current.json \
    --runs 11 \
    --docs-per-writer 20000 \
    --batch-size 100 \
    --read-ops 100000 \
    --read-seed-docs 20000 \
    --branch docs-durability-matrix-full
```

The runner used one discarded warm-up plus 11 measured runs per
`(axis, writers, durability)` row. Write rows use 20,000 documents per writer;
the read row uses 20,000 seed documents and 100,000 point reads. The sidecar
records `teardown_policy: exit_after_operation_measurement`, so these medians
are operation-only throughput and do not include final close/checkpoint time.

Median throughput:

| Axis | Unit | `full-sync` | `interval-50ms` | `none` |
|---|---|---:|---:|---:|
| `single_writer_single_ns_single` | docs/s | 142.34* | 14,157.96* | 16,598.84* |
| `single_writer_single_ns_batch` | docs/s | 15,188.26* | 175,227.38* | 186,384.32* |
| `multi_writer_single_ns_single` | docs/s | 582.83* | 1,408.10* | 1,478.01* |
| `multi_writer_single_ns_batch` | docs/s | 50,788.98 | 101,937.11* | 90,115.07* |
| `multi_writer_multi_ns_single` | docs/s | 542.00* | 10,103.67* | 10,126.83* |
| `multi_writer_multi_ns_batch` | docs/s | 54,399.46 | 208,252.46* | 215,870.68* |
| `read_find_one` | ops/s | 424,932.49* | 429,476.55* | 461,730.43* |

`*` means the row's `(max - min) / median` envelope exceeded 5% and should be
treated as noisy. The raw sidecar keeps `min_dps`, `max_dps`, `envelope`, and
`raw_dps` for every row.

## Specialized Criterion Benches

Criterion benches answer subsystem questions. They are not the canonical write
matrix.

```sh
cargo bench --bench payload_sizes -- --save-baseline current
cargo bench --bench durability_modes -- --save-baseline current
cargo bench --bench index_build -- --save-baseline current
cargo bench --bench read_epoch_root_neutral -- --save-baseline current
cargo bench --bench reader_memory_pressure -- --save-baseline current
cargo bench --bench group_commit_lsn -- --save-baseline current
cargo bench --bench reopen -- --save-baseline current
```

For a quick smoke run, reduce sample size and timing:

```sh
cargo bench --bench <name> -- --sample-size 10 --measurement-time 1 --warm-up-time 1
```

## Profiling

macOS `sample` post-processing lives beside the benchmark runner:

```sh
benches/perf/sample_hot.py /tmp/sample.txt > /tmp/sample-hot.md
```

Keep profiling notes separate from machine-readable baseline sidecars. If a
profile explains a regression or optimization, link it from the change summary
or the relevant baseline narrative instead of changing the JSON schema.

## Reporting Results

When reporting a performance result, include:

- Git base and current revision.
- Hardware summary.
- Exact command and axis list.
- Durability mode and write shape (`insert_one` or `insert_many`).
- Median throughput, min/max envelope, and any rejected noisy rows.
- Correctness gate that passed after the measurement.

Do not mix Criterion numbers with `perf_matrix` sidecar numbers in one ratio
unless the report explicitly names both measurement contracts.
