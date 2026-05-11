# Perf Baseline JSON Sidecar Schema

Each baseline file in `docs/perf-baselines/` consists of a markdown
narrative `<date>-<tag>.md` plus a machine-parseable JSON sidecar
`<date>-<tag>.json`. This file documents the JSON schema. PRs that
consume the baselines (PR1 compounding-delta math, PR2 hold-time
math) parse the sidecar via `serde_json` and assert the required
fields are present and well-typed.

## Top-level object

```json
{
  "date": "YYYY-MM-DD",
  "branch": "perf/r0-baseline",
  "hardware": "Mac<model>; <N> CPUs; <RAM>",
  "build_cmd": "cargo build --release --example perf_axis",
  "axis_runs": 11,
  "duration_seconds": 15,
  "rows": [ <Row>, ... ]
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `date` | string (ISO date) | yes | Capture date, `YYYY-MM-DD`. |
| `branch` | string | yes | Git branch the capture was taken on. |
| `hardware` | string | yes | One-line hardware summary (`system_profiler SPHardwareDataType` extract). |
| `build_cmd` | string | yes | The exact `cargo build` command used. |
| `axis_runs` | integer | yes | Number of measurement runs per (axis, writers); discard run counts excluded. |
| `duration_seconds` | integer | yes | `--seconds` value passed to `perf_axis`. |
| `rows` | array of Row | yes | One record per (axis, writers) measurement. |

## `Row` object

```json
{
  "axis": "same_ns_single",
  "writers": 4,
  "median_dps": 3021.0,
  "min_dps": 2980.0,
  "max_dps": 3070.0,
  "envelope": 0.030,
  "raw_dps": [3015.0, 3021.0, 3028.0, ...]
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `axis` | string (enum) | yes | One of: `same_ns_single`, `same_ns_batch`, `same_ns_partitioned`, `multi_ns_single`, `multi_ns_batch`, `read_find_one`. |
| `writers` | integer | yes | Concurrent writer count (or `1` for read axes). |
| `median_dps` | number | yes | Median throughput across `axis_runs` measurement runs (docs/sec for write axes, ops/sec for `read_find_one`). |
| `min_dps` | number | yes | Minimum across the run set. |
| `max_dps` | number | yes | Maximum across the run set. |
| `envelope` | number | yes | `(max - min) / median`; AC requires `<= 0.05`. |
| `raw_dps` | array of number | optional | Raw per-run throughput values, length == `axis_runs`. Useful for re-deriving stats. |

## Validation

`tests/perf_baseline_schema.rs` round-trips `docs/perf-baselines/*.json`
through `serde_json::Value` and asserts that the top-level keys above
are present, that `rows` is a non-empty array, and that every row has
the required keys with the expected types.
