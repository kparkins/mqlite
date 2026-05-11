#!/usr/bin/env python3
"""Run perf_axis median-of-N baselines and emit a JSON sidecar.

Default plan:
    - 1 warm-up run (discarded) + N=11 measurement runs per (axis, writers).
    - 15 s per run.
    - Reject (axis, writers) groups whose envelope (max-min)/median > 0.05.

Usage:
    tools/perf/run_baselines.py --out docs/perf-baselines/2026-05-10-pre-r1.json
    tools/perf/run_baselines.py --quick          # 3 runs / axis, 5 s each
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

PERF_BIN = "target/release/examples/perf_axis"

# (axis, writers, dps_field) - read_find_one reports ops_per_second.
DPS_FIELD_DEFAULT = "docs_per_second"
DPS_FIELD_READ = "ops_per_second"

# Canonical run matrix (axis, writers).
DEFAULT_MATRIX: list[tuple[str, int]] = [
    ("same_ns_single", 4),
    ("same_ns_single", 8),
    ("same_ns_batch", 4),
    ("same_ns_batch", 8),
    ("same_ns_partitioned", 4),
    ("same_ns_partitioned", 8),
    ("multi_ns_single", 4),
    ("multi_ns_single", 8),
    ("multi_ns_batch", 4),
    ("multi_ns_batch", 8),
    ("read_find_one", 1),
]


@dataclass
class RunResult:
    axis: str
    writers: int
    raw_dps: list[float]

    def median(self) -> float:
        return statistics.median(self.raw_dps)

    def envelope(self) -> float:
        m = self.median()
        return (max(self.raw_dps) - min(self.raw_dps)) / m if m else 0.0

    def to_row(self) -> dict:
        return {
            "axis": self.axis,
            "writers": self.writers,
            "median_dps": round(self.median(), 2),
            "min_dps": round(min(self.raw_dps), 2),
            "max_dps": round(max(self.raw_dps), 2),
            "envelope": round(self.envelope(), 4),
            "raw_dps": [round(v, 2) for v in self.raw_dps],
        }


def run_axis_once(axis: str, writers: int, seconds: int) -> float:
    cmd = [
        PERF_BIN,
        "--axis",
        axis,
        "--seconds",
        str(seconds),
    ]
    if axis != "read_find_one":
        cmd += ["--writers", str(writers)]
    # NOTE: process wall-time IS the workload window because
    # `examples/perf_axis::main` ends with `std::process::exit(0)` to
    # bypass `Client::drop`. The original behaviour ran a per-invocation
    # checkpoint that pushed wall-time to ~45-60s for a 15s workload and
    # made median-of-11 across 11 (axis,writers) rows infeasible (~13h
    # projected). Throughput inside the workload is unaffected — the
    # `docs_per_second` print happens before main returns and is the
    # only number we read here. DO NOT remove the bypass without
    # re-baselining ALL downstream PRs that compare against this matrix:
    # PR1/PR2/PR4 measurements MUST share the identical perf_axis
    # lifecycle for the compounding-delta math to be apples-to-apples.
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    line = proc.stdout.strip().splitlines()[-1]
    record = json.loads(line)
    field = DPS_FIELD_READ if axis == "read_find_one" else DPS_FIELD_DEFAULT
    return float(record[field])


def collect_axis(axis: str, writers: int, runs: int, seconds: int) -> RunResult:
    print(f"[run] axis={axis} writers={writers} runs={runs} seconds={seconds}", flush=True)
    # Discard warm-up.
    _ = run_axis_once(axis, writers, seconds)
    dps_values: list[float] = []
    for i in range(runs):
        dps = run_axis_once(axis, writers, seconds)
        dps_values.append(dps)
        print(f"  run {i + 1}/{runs}: {dps:.2f}", flush=True)
    return RunResult(axis=axis, writers=writers, raw_dps=dps_values)


def hardware_string() -> str:
    try:
        out = subprocess.run(
            ["system_profiler", "SPHardwareDataType"], capture_output=True, text=True
        )
        wanted = []
        for line in out.stdout.splitlines():
            for key in (
                "Model Name",
                "Chip",
                "Total Number of Cores",
                "Memory",
                "Model Identifier",
            ):
                if key in line:
                    wanted.append(line.strip())
        return "; ".join(wanted) or platform.platform()
    except FileNotFoundError:
        return platform.platform()


def main(argv: Iterable[str]) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True, help="Output JSON path")
    p.add_argument("--runs", type=int, default=11, help="Measurement runs per axis")
    p.add_argument("--seconds", type=int, default=15, help="Seconds per run")
    p.add_argument("--branch", default="perf/p0-baselines")
    p.add_argument(
        "--axis",
        action="append",
        help="Restrict to a specific (axis,writers) row. Format: axis@writers. "
        "May be passed multiple times.",
    )
    p.add_argument("--quick", action="store_true", help="3 runs, 5 sec each (smoke).")
    args = p.parse_args(list(argv))

    if args.quick:
        runs = 3
        seconds = 5
    else:
        runs = args.runs
        seconds = args.seconds

    matrix = DEFAULT_MATRIX
    if args.axis:
        wanted: list[tuple[str, int]] = []
        for spec in args.axis:
            axis, _, w = spec.partition("@")
            if not w:
                return _fail(f"--axis must be of form axis@writers, got {spec!r}")
            wanted.append((axis, int(w)))
        matrix = wanted

    started = time.time()
    rows: list[dict] = []
    rejected: list[str] = []
    for axis, writers in matrix:
        result = collect_axis(axis, writers, runs, seconds)
        env = result.envelope()
        row = result.to_row()
        if env > 0.05:
            print(
                f"  WARN: envelope {env:.4f} > 0.05 for {axis}@{writers}; record kept",
                flush=True,
            )
            rejected.append(f"{axis}@{writers}={env:.4f}")
        rows.append(row)

    sidecar = {
        "date": time.strftime("%Y-%m-%d"),
        "branch": args.branch,
        "hardware": hardware_string(),
        "build_cmd": "cargo build --release --example perf_axis",
        "axis_runs": runs,
        "duration_seconds": seconds,
        "rows": rows,
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(sidecar, indent=2) + "\n")

    elapsed = time.time() - started
    print(
        f"\nWrote {out_path} ({len(rows)} rows; total elapsed {elapsed:.1f}s).",
        flush=True,
    )
    if rejected:
        print("Rows with envelope > 0.05:")
        for r in rejected:
            print(f"  {r}")
    return 0


def _fail(msg: str) -> int:
    print(f"ERROR: {msg}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
