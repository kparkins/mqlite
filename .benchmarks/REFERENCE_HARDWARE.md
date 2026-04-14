# Benchmark Reference Hardware

This document specifies the canonical CI runner configuration for mqlite
performance benchmarks.  Results obtained on different hardware are not directly
comparable to the stored baseline.

## CI Runner Specification

| Property | Value |
|----------|-------|
| Provider | GitHub Actions (ubuntu-latest) |
| CPU | Intel or AMD x86-64, 2 vCPUs |
| RAM | 7 GB |
| Storage | SSD (NVMe or SATA, OS-managed) |
| OS | Ubuntu 22.04 LTS (linux/amd64) |
| Rust toolchain | stable (pinned via `dtolnay/rust-toolchain@stable`) |
| Criterion version | 0.5 |
| Profile | release (default for `cargo bench`) |
| CPU isolation | none (shared GitHub Actions runner) |

## Regression Policy

The CI benchmark job (`bench:`) runs on every pull request targeting `master`.

**Failure threshold: 200% (2×)**

Any benchmark that regresses more than 2× relative to the stored `gh-pages`
baseline will:

1. Post a PR comment showing the delta for each affected benchmark.
2. Fail the `bench` CI job, blocking merge.

The 2× threshold is intentionally conservative — it catches real regressions
(algorithmic complexity changes, accidental O(n²) paths) while tolerating
normal CI noise on shared runners (±20–30% is common).

### Consecutive-measurement requirement

Criterion's confidence intervals are used automatically.  Because criterion
runs each benchmark until the measurement is statistically stable (default
target: 5% relative width), a single noisy iteration does not trigger a false
positive.

For the CI job to report a regression, the **mean** of criterion's measurement
distribution must exceed the 2× threshold — not just a single sample.

## Baseline management

Baseline results are stored in the `gh-pages` branch under `dev/bench/` as
JSON files managed by
[`benchmark-action/github-action-benchmark`](https://github.com/benchmark-action/github-action-benchmark).

The baseline is updated automatically on every push to `master` (via the
`auto-push: true` setting in `.github/workflows/ci.yml`).

To reset the baseline (e.g. after a deliberate performance improvement):

```bash
# Delete the stored data for the affected benchmark(s) from gh-pages, then
# merge a commit that triggers a fresh baseline write.
```

## Running benchmarks locally

```bash
# Full benchmark suite (saves HTML reports to target/criterion/)
cargo bench

# Single group (faster feedback)
cargo bench -- insert_one

# Compare against a saved baseline
cargo bench --bench core -- --save-baseline my_branch
cargo bench --bench core -- --baseline my_branch
```

Local results will differ from CI due to CPU frequency scaling, turbo boost,
background load, and other factors.  Use them for directional guidance only;
the CI run is the authoritative comparison.
