#!/usr/bin/env python3
"""Run perf_matrix median-of-N baselines and emit a JSON sidecar.

Default plan:
    - 1 warm-up run (discarded) + N=11 measurement runs per (axis, writers).
    - Fixed documents per writer; synthetic document generation is outside
      the measured window.
    - Reject (axis, writers) groups whose envelope (max-min)/median > 0.05.

Usage:
    benches/perf/run_baselines.py --out docs/perf-baselines/current.json
    benches/perf/run_baselines.py --quick
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

PERF_BIN = "target/release/perf_matrix"

# (axis, writers, dps_field) - read_find_one reports ops_per_second.
DPS_FIELD_DEFAULT = "docs_per_second"
DPS_FIELD_READ = "ops_per_second"

# Canonical run matrix (axis, writers).
DEFAULT_MATRIX: list[tuple[str, int]] = [
    ("single_writer_single_ns_single", 1),
    ("single_writer_single_ns_batch", 1),
    ("multi_writer_single_ns_single", 4),
    ("multi_writer_single_ns_batch", 4),
    ("multi_writer_multi_ns_single", 4),
    ("multi_writer_multi_ns_batch", 4),
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


def run_axis_once(
    axis: str,
    writers: int,
    docs_per_writer: int,
    batch_size: int,
    read_ops: int,
    read_seed_docs: int,
) -> float:
    cmd = [
        PERF_BIN,
        "--axis",
        axis,
        "--docs-per-writer",
        str(docs_per_writer),
        "--batch-size",
        str(batch_size),
        "--read-ops",
        str(read_ops),
        "--read-seed-docs",
        str(read_seed_docs),
    ]
    if axis != "read_find_one":
        cmd += ["--writers", str(writers)]
    proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    # `perf_matrix` emits one JSON record per axis on stdout. Under
    # `--features perf-counters` it can also tail-print a `{"perf_counters": ...}`
    # line; pick the record that carries the throughput field we want instead
    # of blindly taking the last line.
    field = DPS_FIELD_READ if axis == "read_find_one" else DPS_FIELD_DEFAULT
    if axis == "read_find_one_under_writers":
        field = "reader_ops_per_second"
    for line in reversed(proc.stdout.strip().splitlines()):
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if field in record:
            return float(record[field])
    raise RuntimeError(
        f"no JSON record with field {field!r} on stdout for {axis}@{writers}"
    )


def collect_axis(
    axis: str,
    writers: int,
    runs: int,
    docs_per_writer: int,
    batch_size: int,
    read_ops: int,
    read_seed_docs: int,
) -> RunResult:
    print(
        f"[run] axis={axis} writers={writers} runs={runs} "
        f"docs_per_writer={docs_per_writer} batch_size={batch_size}",
        flush=True,
    )
    # Discard warm-up.
    _ = run_axis_once(axis, writers, docs_per_writer, batch_size, read_ops, read_seed_docs)
    dps_values: list[float] = []
    for i in range(runs):
        dps = run_axis_once(axis, writers, docs_per_writer, batch_size, read_ops, read_seed_docs)
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
    p.add_argument(
        "--docs-per-writer",
        type=int,
        default=20_000,
        help="Prebuilt documents per writer for write axes",
    )
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--read-ops", type=int, default=100_000)
    p.add_argument("--read-seed-docs", type=int, default=20_000)
    p.add_argument("--branch", default="perf/p0-baselines")
    p.add_argument(
        "--axis",
        action="append",
        help="Restrict to a specific (axis,writers) row. Format: axis@writers. "
        "May be passed multiple times.",
    )
    p.add_argument(
        "--quick",
        action="store_true",
        help="3 runs with reduced fixed counts (smoke).",
    )
    args = p.parse_args(list(argv))

    if args.quick:
        runs = 3
        docs_per_writer = min(args.docs_per_writer, 500)
        read_ops = min(args.read_ops, 1_000)
        read_seed_docs = min(args.read_seed_docs, 500)
    else:
        runs = args.runs
        docs_per_writer = args.docs_per_writer
        read_ops = args.read_ops
        read_seed_docs = args.read_seed_docs

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
        result = collect_axis(
            axis,
            writers,
            runs,
            docs_per_writer,
            args.batch_size,
            read_ops,
            read_seed_docs,
        )
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
        "build_cmd": "cargo build --release --bin perf_matrix",
        "matrix_version": "perf_matrix_v1",
        "axis_runs": runs,
        "docs_per_writer": docs_per_writer,
        "batch_size": args.batch_size,
        "read_ops": read_ops,
        "read_seed_docs": read_seed_docs,
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
