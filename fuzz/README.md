# mqlite fuzz targets

This directory holds the cargo-fuzz harness for mqlite. Phase 2 §9.2/§9.3
adds three targets exercising the LogicalTxnFrame decoder and recovery
path.

## Targets

- **`logical_txn_decode`** — feeds arbitrary bytes through
  `LogicalTxnFrame::decode` in `Scanning` context. Bar: never panic, never
  loop, never UB.
- **`logical_txn_recover`** — overwrites a fresh DB's journal sidecar
  with the fuzzed bytes and re-opens via `Client::open`. Exercises the
  full Pass 1 / Pass 2 recovery scan over arbitrary journal bodies.
- **`try_skip_logical_txn`** — verifies the cursor-rewind post-condition:
  the helper either advances by `n` (returns `Some((n, _))`) or fully
  rewinds to start (returns `None`). Any other behavior is a bug.

## Seed corpus

The 12 seeds under `corpus/logical_txn_recovery/` cover every §9.2 named
input shape — empty journal, single legacy commit, full envelope, torn
logical tail, orphan logical, duplicate commit_ts, unknown op kind,
oversized op_count, mixed sequence, gap ordinal, unresolved ns_id, and
legacy + chaincommit + logical mix.

Regenerate with:

```bash
cd fuzz && cargo run --bin generate_seeds
```

The generator is fully reproducible — every seed is a deterministic byte
sequence; no RNG.

## Local replay (no fuzzer required)

To smoke-test the targets against the seed corpus once (no actual
fuzzing, just one execution per seed):

```bash
# From the repo root:
cargo +nightly fuzz run logical_txn_decode -- -runs=0
cargo +nightly fuzz run logical_txn_recover -- -runs=0
cargo +nightly fuzz run try_skip_logical_txn -- -runs=0
```

`-runs=0` means "replay corpus, then exit". Any panic / UB / hang is a
test failure.

## CI

For PRs that touch `src/journal/`, the recommended opt-in invocation is:

```bash
cargo +nightly fuzz run logical_txn_decode -- -max_total_time=300
cargo +nightly fuzz run logical_txn_recover -- -max_total_time=300
cargo +nightly fuzz run try_skip_logical_txn -- -max_total_time=300
```

Each target runs for five minutes wall-clock. CI is opt-in (not part of
the default test suite) because cargo-fuzz requires nightly Rust and a
sanitizer-instrumented build.
