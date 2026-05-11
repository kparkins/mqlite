# Benchmark Suite

All benchmark and profiling entry points live under `benches/`. Examples stay
as API demonstrations; benchmark runners and benchmark scripts do not live in
`examples/` or `tools/`.

## Canonical Write Matrix

Use `perf_matrix` for write-throughput and point-read baselines:

```bash
cargo build --release --bin perf_matrix
target/release/perf_matrix --list-axes
target/release/perf_matrix \
    --axis multi_writer_single_ns_single \
    --writers 4 \
    --docs-per-writer 20000 \
    --batch-size 100
```

The required write axes are:

| Axis | Writers | Namespaces | Operation |
|---|---:|---:|---|
| `single_writer_single_ns_single` | 1 | 1 | `insert_one` |
| `single_writer_single_ns_batch` | 1 | 1 | `insert_many` |
| `multi_writer_single_ns_single` | 4 default | 1 | `insert_one` |
| `multi_writer_single_ns_batch` | 4 default | 1 | `insert_many` |
| `multi_writer_multi_ns_single` | 4 default | one per writer | `insert_one` |
| `multi_writer_multi_ns_batch` | 4 default | one per writer | `insert_many` |

Every write document carries an `_id` and exercises the primary-key path. The
timed window starts after database setup, namespace creation, thread creation,
and synthetic document generation. The JSON output includes
`"timed_scope":"operation_only"` for this contract.

Run the median sidecar driver from the same folder:

```bash
benches/perf/run_baselines.py \
    --out docs/perf-baselines/current.json \
    --runs 11 \
    --docs-per-writer 20000 \
    --batch-size 100
```

Quick smoke:

```bash
benches/perf/run_baselines.py --quick --out /tmp/mqlite-perf-smoke.json
```

macOS `sample` post-processing also lives here:

```bash
benches/perf/sample_hot.py /tmp/sample.txt > /tmp/sample-hot.md
```

## Specialized Criterion Benches

Keep these for targeted subsystem questions, not for the main write matrix:

```bash
cargo bench --bench payload_sizes -- --save-baseline current
cargo bench --bench durability_modes -- --save-baseline current
cargo bench --bench index_build -- --save-baseline current
cargo bench --bench read_epoch_root_neutral -- --save-baseline current
cargo bench --bench reader_memory_pressure -- --save-baseline current
cargo bench --bench group_commit_lsn -- --save-baseline current
cargo bench --bench reopen -- --save-baseline current
```

Short-run verification:

```bash
cargo bench --bench <name> -- --sample-size 10 --measurement-time 1 --warm-up-time 1
```
