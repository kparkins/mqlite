# mqlite Verification Guide

This guide lists the repo-native checks that prove the documentation, embedded
API, storage invariants, Jepsen workloads, and performance baselines.

## Quick Checks

Run the README examples first when changing public documentation:

```sh
cargo test --test readme_examples
```

Run the perf-baseline schema smoke test when changing performance docs or
baseline sidecars:

```sh
cargo test --test perf_baseline_schema
```

## Release-Profile Correctness Gate

The broad Rust test gate is:

```sh
cargo test --release --all-targets --features wire,test-hooks
```

Use this when changing storage, recovery, wire protocol, test-hook, or public
API behavior. It builds the default crate, examples, integration tests, and the
wire feature surface.

## Embedded Jepsen

The Jepsen suite lives under `tests/jepsen/` and exercises mqlite as an
embedded single-file document store. It starts a small localhost adapter that
uses `mqlite::Client` and `Collection<Document>` directly. It does not test
replica-set, election, majority, or partition behavior because mqlite does not
claim to be a distributed database.

Run all workloads from the repository root:

```sh
./tests/jepsen/run.sh --workload all --nemesis none
```

Useful targeted runs:

```sh
./tests/jepsen/run.sh --workload register --time-limit 20 --rate 40
./tests/jepsen/run.sh --workload unique-index --time-limit 20
./tests/jepsen/run.sh --workload write-batch-prefix --nemesis restart
```

Requirements are Rust/Cargo, Java 21 or newer, and either the `clojure` CLI or
`lein`. Jepsen artifacts are written under `tests/jepsen/store/`; database
files and adapter logs are written under `target/jepsen/`.

## Benchmark Matrix

The canonical operation-scoped performance matrix is `perf_matrix`, documented
in [benches/README.md](../benches/README.md). Build it and inspect axes with:

```sh
cargo build --release --bin perf_matrix
target/release/perf_matrix --list-axes
```

The main write and point-read sidecar driver is:

```sh
benches/perf/run_baselines.py \
    --out docs/perf-baselines/current.json \
    --runs 11 \
    --docs-per-writer 20000 \
    --batch-size 100
```

Use `--quick` for a smoke run:

```sh
benches/perf/run_baselines.py --quick --out /tmp/mqlite-perf-smoke.json
```

The baseline JSON contract is documented in
[docs/perf-baselines/SCHEMA.md](perf-baselines/SCHEMA.md) and checked by
`tests/perf_baseline_schema.rs`.

## Specialized Criterion Benches

Criterion benches are for targeted subsystem questions rather than the main
write matrix:

```sh
cargo bench --bench payload_sizes -- --save-baseline current
cargo bench --bench durability_modes -- --save-baseline current
cargo bench --bench index_build -- --save-baseline current
cargo bench --bench read_epoch_root_neutral -- --save-baseline current
cargo bench --bench reader_memory_pressure -- --save-baseline current
cargo bench --bench group_commit_lsn -- --save-baseline current
cargo bench --bench reopen -- --save-baseline current
```

For a short smoke run, reduce sample size and timing:

```sh
cargo bench --bench <name> -- --sample-size 10 --measurement-time 1 --warm-up-time 1
```
