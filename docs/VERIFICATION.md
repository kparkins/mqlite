# mqlite Verification Guide

This guide lists the repo-native checks that prove the documentation, embedded
API, storage invariants, and Jepsen workloads. Performance measurement is
covered in [PERFORMANCE.md](PERFORMANCE.md).

## Quick Checks

Run the README examples first when changing public documentation:

```sh
cargo test --test readme_examples
```

Run the perf-baseline schema smoke test when changing performance baseline
sidecars:

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

## Performance Gates

Performance measurement surfaces are documented in
[PERFORMANCE.md](PERFORMANCE.md). Pair any performance claim with the relevant
correctness gate from this guide.
