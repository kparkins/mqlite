#!/usr/bin/env bash
set -euo pipefail

BASE_REF="${MQLITE_PERF_BASE_REF:-HEAD}"
MIN_RATIO="${MQLITE_PERF_MIN_RATIO:-1.5}"
RUN_CORRECTNESS="${MQLITE_PERF_RUN_CORRECTNESS:-1}"
JEPSEN_ARGS="${MQLITE_PERF_JEPSEN_ARGS:---workload all --nemesis none}"
ARTIFACT_DIR="${MQLITE_PERF_ARTIFACT_DIR:-target/perf-goal}"
CURRENT_TARGET_DIR="${MQLITE_PERF_CURRENT_TARGET_DIR:-target/perf-goal-current}"

ROOT="$(git rev-parse --show-toplevel)"
BASELINE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mqlite-perf-baseline.XXXXXX")"
BASELINE_TARGET_DIR="$(mktemp -d "${TMPDIR:-/tmp}/mqlite-perf-target.XXXXXX")"

cleanup() {
  git -C "$ROOT" worktree remove --force "$BASELINE_DIR" >/dev/null 2>&1 || true
  rm -rf "$BASELINE_TARGET_DIR"
}
trap cleanup EXIT

mkdir -p "$ARTIFACT_DIR"
git -C "$ROOT" worktree add --detach "$BASELINE_DIR" "$BASE_REF" >/dev/null
mkdir -p "$BASELINE_DIR/examples"
cp "$ROOT/examples/perf_goal.rs" "$BASELINE_DIR/examples/perf_goal.rs"

run_perf_example() {
  local repo_dir="$1"
  local target_dir="$2"
  local output_path="$3"

  (
    cd "$repo_dir"
    CARGO_TARGET_DIR="$target_dir" cargo run --quiet --release --example perf_goal
  ) | tee "$output_path"
}

BASELINE_JSON="$ARTIFACT_DIR/baseline.json"
CURRENT_JSON="$ARTIFACT_DIR/current.json"
SUMMARY_JSON="$ARTIFACT_DIR/summary.json"

run_perf_example "$BASELINE_DIR" "$BASELINE_TARGET_DIR" "$BASELINE_JSON"
run_perf_example "$ROOT" "$CURRENT_TARGET_DIR" "$CURRENT_JSON"

python3 - "$BASELINE_JSON" "$CURRENT_JSON" "$SUMMARY_JSON" "$MIN_RATIO" <<'PY'
import json
import sys

baseline_path, current_path, summary_path, min_ratio_raw = sys.argv[1:]
min_ratio = float(min_ratio_raw)
with open(baseline_path, "r", encoding="utf-8") as handle:
    baseline = json.load(handle)
with open(current_path, "r", encoding="utf-8") as handle:
    current = json.load(handle)

single_same_collection_ratio = (
    current["write_single_same_collection_4"]["docs_per_second"]
    / baseline["write_single_same_collection_4"]["docs_per_second"]
)
batch_same_collection_ratio = (
    current["write_batch_same_collection_4"]["docs_per_second"]
    / baseline["write_batch_same_collection_4"]["docs_per_second"]
)
single_multi_collection_ratio = (
    current["write_single_multi_collection_8"]["docs_per_second"]
    / baseline["write_single_multi_collection_8"]["docs_per_second"]
)
batch_multi_collection_ratio = (
    current["write_batch_multi_collection_8"]["docs_per_second"]
    / baseline["write_batch_multi_collection_8"]["docs_per_second"]
)
read_mixed_ratio = (
    current["read_mixed"]["ops_per_second"]
    / baseline["read_mixed"]["ops_per_second"]
)
write_ratio = min(
    single_same_collection_ratio,
    batch_same_collection_ratio,
    single_multi_collection_ratio,
    batch_multi_collection_ratio,
)
summary = {
    "baseline": baseline,
    "current": current,
    "min_ratio": min_ratio,
    "write_ratio": write_ratio,
    "write_single_same_collection_4_ratio": single_same_collection_ratio,
    "write_batch_same_collection_4_ratio": batch_same_collection_ratio,
    "write_single_multi_collection_8_ratio": single_multi_collection_ratio,
    "write_batch_multi_collection_8_ratio": batch_multi_collection_ratio,
    "read_mixed_ratio": read_mixed_ratio,
    "performance_pass": (
        single_same_collection_ratio >= min_ratio
        and batch_same_collection_ratio >= min_ratio
        and single_multi_collection_ratio >= min_ratio
        and batch_multi_collection_ratio >= min_ratio
        and read_mixed_ratio >= min_ratio
    ),
}
with open(summary_path, "w", encoding="utf-8") as handle:
    json.dump(summary, handle, indent=2, sort_keys=True)
    handle.write("\n")

print(json.dumps(summary, indent=2, sort_keys=True))
if not summary["performance_pass"]:
    raise SystemExit(1)
PY

if [[ "$RUN_CORRECTNESS" == "1" ]]; then
  (
    cd "$ROOT"
    env -u RUSTC_WRAPPER cargo test --release --all-targets --features wire,test-hooks
    ./tests/jepsen/run.sh $JEPSEN_ARGS
  )
fi
