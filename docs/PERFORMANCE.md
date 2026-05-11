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
```

Run one axis:

```sh
target/release/perf_matrix \
    --axis multi_writer_single_ns_single \
    --writers 4 \
    --docs-per-writer 20000 \
    --batch-size 100
```

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

Use `--quick` only for smoke testing the runner:

```sh
benches/perf/run_baselines.py --quick --out /tmp/mqlite-perf-smoke.json
```

The JSON schema is documented in
[docs/perf-baselines/SCHEMA.md](perf-baselines/SCHEMA.md) and validated by:

```sh
cargo test --test perf_baseline_schema
```

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
