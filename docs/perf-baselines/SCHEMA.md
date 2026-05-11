# Performance Baseline Sidecar Schema

`docs/perf-baselines/*.json` files are checked by
`tests/perf_baseline_schema.rs`. Keep this schema stable for benchmark
comparisons and automation that consume performance sidecars.

The current producer is `benches/perf/run_baselines.py`, which shells out to
the `perf_matrix` binary and writes one JSON object. See
[PERFORMANCE.md](../PERFORMANCE.md) for the benchmark workflow.

## Top-Level Object

Required fields:

| Field | Type | Meaning |
|---|---|---|
| `date` | string | Baseline collection date, usually `YYYY-MM-DD`. |
| `branch` | string | Branch or label for the run. |
| `hardware` | string | Host hardware summary. |
| `build_cmd` | string | Build command used for the benchmark binary. |
| `axis_runs` | unsigned integer | Measurement runs per axis, excluding warm-up. |
| `rows` | array | One row per measured axis. |

The sidecar must also declare either:

- `duration_seconds`, or
- `docs_per_writer` and `batch_size`.

`run_baselines.py` currently emits fixed-count runs with `docs_per_writer` and
`batch_size`.

## Row Object

Every `rows[]` entry requires:

| Field | Type | Meaning |
|---|---|---|
| `axis` | string | Axis name from the allowed set below. |
| `writers` | unsigned integer | Writer count for the axis. |
| `median_dps` | number | Median documents or operations per second. |
| `min_dps` | number | Minimum run throughput. |
| `max_dps` | number | Maximum run throughput. |
| `envelope` | number | `(max - min) / median`; must be non-negative. |

`min_dps <= median_dps <= max_dps` must hold.

## Allowed Axes

- `single_writer_single_ns_single`
- `single_writer_single_ns_batch`
- `multi_writer_single_ns_single`
- `multi_writer_single_ns_batch`
- `multi_writer_multi_ns_single`
- `multi_writer_multi_ns_batch`
- `same_ns_single`
- `same_ns_batch`
- `same_ns_partitioned`
- `multi_ns_single`
- `multi_ns_batch`
- `read_find_one`

The first seven names are emitted by the current `perf_matrix` default matrix.
The remaining names are retained so older or targeted sidecars can still round
trip through the schema test.

## Empty Directory Behavior

The schema test passes when `docs/perf-baselines/` exists but contains no JSON
sidecars. Once a sidecar exists, every JSON file in the directory must conform
to this schema.
