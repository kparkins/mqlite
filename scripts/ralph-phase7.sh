#!/usr/bin/env bash
# ralph-phase7.sh - drive Phase 7 implementation story-by-story via ralph,
# with a fresh Codex session per story so context does not accumulate.
#
# Picks the lowest-priority ready story with passes:false from
# .omc/phase-07-prd.json, invokes ralph on just that story, waits for it to
# exit, then loops. A story is ready only when every dependency is
# passes:true. Stops when every story passes.
#
# Usage:
#   scripts/ralph-phase7.sh                 # run to completion
#   scripts/ralph-phase7.sh --dry-run       # print next story, do nothing
#   scripts/ralph-phase7.sh --max 3         # stop after 3 iterations
#   PRD=/path/to/other-prd.json scripts/ralph-phase7.sh
#
# Env overrides:
#   PRD        path to the prd.json           (default: .omc/phase-07-prd.json)
#   PLAN_PRD   Ralph PRD wrapper
#              default:
#              .omx/plans/prd-phase-07-durable-checkpoint-boundary.md
#   TEST_SPEC  Ralph test wrapper
#              default:
#              .omx/plans/test-spec-phase-07-durable-checkpoint-boundary.md
#   LOG_DIR    directory for per-story logs   (default: .omc/logs)
#   CODEX_BIN  codex CLI binary for driver    (default: codex)
#   CODEX_EFFORT codex reasoning effort       (default: xhigh)
#   CRITIC     reviewer passed to ralph       (default: claude)
#   CLAUDE_BIN claude CLI binary for critic   (default: claude)
#   POLL_SECONDS         seconds between PRD pass checks while driver runs
#                        (default: 30)
#   EXIT_GRACE_SECONDS   seconds to wait after passes:true before terminating a
#                        lingering driver process (default: 60)

set -euo pipefail

DEFAULT_PLAN_PRD=".omx/plans/prd-phase-07-durable-checkpoint-boundary.md"
DEFAULT_TEST_SPEC=".omx/plans/test-spec-phase-07-durable-checkpoint-boundary.md"
DEFAULT_SOURCE_DOC="docs/STORAGE-UPGRADE-PHASE-07-DURABLE-CHECKPOINT-BOUNDARY.md"

PRD="${PRD:-.omc/phase-07-prd.json}"
PLAN_PRD="${PLAN_PRD:-$DEFAULT_PLAN_PRD}"
TEST_SPEC="${TEST_SPEC:-$DEFAULT_TEST_SPEC}"
LOG_DIR="${LOG_DIR:-.omc/logs}"
CODEX_BIN="${CODEX_BIN:-codex}"
CODEX_EFFORT="${CODEX_EFFORT:-xhigh}"
CRITIC="${CRITIC:-claude}"
CLAUDE_BIN="${CLAUDE_BIN:-claude}"
POLL_SECONDS="${POLL_SECONDS:-30}"
EXIT_GRACE_SECONDS="${EXIT_GRACE_SECONDS:-60}"
DRY_RUN=0
MAX_ITER=0
DRIVER_PID=""

usage() {
  sed -n '1,31p' "$0"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run)
      DRY_RUN=1
      ;;
    --max)
      shift
      if [ $# -eq 0 ]; then
        echo "error: --max requires an integer argument" >&2
        exit 2
      fi
      MAX_ITER="$1"
      case "$MAX_ITER" in
        ''|*[!0-9]*)
          echo "error: --max requires an integer argument" >&2
          exit 2
          ;;
      esac
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      exit 2
      ;;
  esac
  shift
done

command -v jq >/dev/null || { echo "error: jq is required" >&2; exit 2; }
case "$POLL_SECONDS" in
  ''|*[!0-9]*)
    echo "error: POLL_SECONDS must be an integer" >&2
    exit 2
    ;;
esac
case "$EXIT_GRACE_SECONDS" in
  ''|*[!0-9]*)
    echo "error: EXIT_GRACE_SECONDS must be an integer" >&2
    exit 2
    ;;
esac
[ -f "$PRD" ] || { echo "error: PRD not found at $PRD" >&2; exit 2; }
[ -f "$PLAN_PRD" ] || {
  echo "error: PRD wrapper not found at $PLAN_PRD" >&2
  exit 2
}
[ -f "$TEST_SPEC" ] || {
  echo "error: test wrapper not found at $TEST_SPEC" >&2
  exit 2
}
PHASE_NAME="$(jq -r '.meta.phase // "Storage Upgrade Phase 7"' "$PRD")"
SOURCE_DOC="$(
  jq -r --arg fallback "$DEFAULT_SOURCE_DOC" \
    '.meta.source_doc // $fallback' "$PRD"
)"
[ -f "$SOURCE_DOC" ] || {
  echo "error: Phase 7 spec not found at $SOURCE_DOC" >&2
  exit 2
}
if [ "$DRY_RUN" = 0 ]; then
  command -v "$CODEX_BIN" >/dev/null || {
    echo "error: $CODEX_BIN not in PATH (set CODEX_BIN=... to override)" >&2
    exit 2
  }
  if [ "$CRITIC" = "claude" ]; then
    command -v "$CLAUDE_BIN" >/dev/null || {
      echo "error: $CLAUDE_BIN not in PATH (set CLAUDE_BIN=... to override)" >&2
      exit 2
    }
  fi
fi

mkdir -p "$LOG_DIR"

phase_name() {
  printf '%s\n' "$PHASE_NAME"
}

source_doc() {
  printf '%s\n' "$SOURCE_DOC"
}

next_story() {
  jq -r '
    def dep_passed($stories; $dep):
      any($stories[]; .id == $dep and .passes == true);
    .stories as $stories
    | [ $stories[]
        | select(.passes == false)
        | select(all(.dependencies[]?; dep_passed($stories; .))) ]
    | sort_by(.priority)
    | .[0]
    | if . == null then empty else "\(.id)\t\(.priority)\t\(.title)" end
  ' "$PRD"
}

open_count() {
  jq -r '[.stories[] | select(.passes == false)] | length' "$PRD"
}

blocked_stories() {
  jq -r '
    def dep_passed($stories; $dep):
      any($stories[]; .id == $dep and .passes == true);
    def unmet_deps($stories; $story):
      [ $story.dependencies[]? | select(dep_passed($stories; .) | not) ];
    .stories as $stories
    | [ $stories[]
        | select(.passes == false)
        | select((unmet_deps($stories; .) | length) > 0) ]
    | sort_by(.priority)
    | .[]
    | "\(.id)\tblocked_by=\(unmet_deps($stories; .) | join(","))\t\(.title)"
  ' "$PRD"
}

story_passes() {
  jq -r --arg id "$1" \
    '.stories[] | select(.id == $id) | .passes' "$PRD"
}

stop_driver() {
  if [ -n "$DRIVER_PID" ] && kill -0 "$DRIVER_PID" 2>/dev/null; then
    kill "$DRIVER_PID" 2>/dev/null || true
  fi
}

trap 'stop_driver; exit 130' INT TERM

run_driver() {
  "$CODEX_BIN" exec \
    -C "$PWD" \
    -c "model_reasoning_effort=\"$CODEX_EFFORT\"" \
    --dangerously-bypass-approvals-and-sandbox \
    --json \
    "$prompt" \
    >"$log" 2>&1 &

  DRIVER_PID="$!"

  while kill -0 "$DRIVER_PID" 2>/dev/null; do
    if [ "$(story_passes "$id")" = "true" ]; then
      echo "[ralph-phase7] $id is passes:true; waiting for driver exit."

      waited=0
      while kill -0 "$DRIVER_PID" 2>/dev/null; do
        if [ "$waited" -ge "$EXIT_GRACE_SECONDS" ]; then
          echo "[ralph-phase7] driver still running after passes:true;"
          echo "[ralph-phase7] terminating lingering session for $id."
          stop_driver
          wait "$DRIVER_PID" 2>/dev/null || true
          DRIVER_PID=""
          return 0
        fi
        sleep 5
        waited=$((waited + 5))
      done

      wait "$DRIVER_PID"
      rc=$?
      DRIVER_PID=""
      if [ "$rc" -ne 0 ] && [ "$(story_passes "$id")" = "true" ]; then
        echo "[ralph-phase7] driver exited rc=$rc after $id passed;"
        echo "[ralph-phase7] continuing to the next story."
        return 0
      fi
      return "$rc"
    fi
    sleep "$POLL_SECONDS"
  done

  wait "$DRIVER_PID"
  rc=$?
  DRIVER_PID=""
  if [ "$rc" -ne 0 ] && [ "$(story_passes "$id")" = "true" ]; then
    echo "[ralph-phase7] driver exited rc=$rc after $id passed;"
    echo "[ralph-phase7] continuing to the next story."
    return 0
  fi
  return "$rc"
}

iter=0
while :; do
  row="$(next_story)"
  if [ -z "$row" ]; then
    open="$(open_count)"
    if [ "$open" = "0" ]; then
      echo "[ralph-phase7] all stories pass - done."
      exit 0
    fi
    echo "[ralph-phase7] no ready stories; open stories have unmet dependencies." >&2
    blocked_stories >&2
    exit 4
  fi

  iter=$((iter + 1))
  if [ "$MAX_ITER" -gt 0 ] && [ "$iter" -gt "$MAX_ITER" ]; then
    echo "[ralph-phase7] stopped after $MAX_ITER iterations (--max)."
    echo "[ralph-phase7] Open: $(open_count)"
    exit 0
  fi

  id="$(printf '%s' "$row" | cut -f1)"
  pri="$(printf '%s' "$row" | cut -f2)"
  title="$(printf '%s' "$row" | cut -f3-)"
  ts="$(date +%Y%m%d-%H%M%S)"
  log="$LOG_DIR/${ts}-phase7-${id}.log"
  phase="$(phase_name)"
  spec="$(source_doc)"
  critic_note="Use ${CRITIC} as critic before setting passes:true in ${PRD}."
  if [ "$CRITIC" = "claude" ]; then
    critic_note="Use Claude as critic before setting passes:true in ${PRD}.
If the local Ralph path cannot route --critic=claude directly, run the
Claude critic through ${CLAUDE_BIN} -p or the ask-claude skill and treat any
blocker finding as a failed story."
  fi

  echo "[ralph-phase7] iter=$iter story=$id priority=$pri"
  echo "[ralph-phase7]   phase: $phase"
  echo "[ralph-phase7]   title: $title"
  echo "[ralph-phase7]   spec:  $spec"
  echo "[ralph-phase7]   plan:  $PLAN_PRD"
  echo "[ralph-phase7]   tests: $TEST_SPEC"
  echo "[ralph-phase7]   driver: $CODEX_BIN"
  echo "[ralph-phase7]   effort: $CODEX_EFFORT"
  echo "[ralph-phase7]   critic:   $CRITIC"
  echo "[ralph-phase7]   log:   $log"

  if [ "$DRY_RUN" = 1 ]; then
    echo "[ralph-phase7] (dry-run) would invoke ralph on $id"
    exit 0
  fi

  prompt="\$ralph --critic=${CRITIC} Implement exactly one"
  prompt="${prompt} story - ${id} - from ${PRD}.
Use ${PLAN_PRD} and ${TEST_SPEC} as the Ralph planning adapters.
Treat ${PRD} as the canonical Phase 7 execution and test contract.
Use ${spec} only as durable checkpoint commit boundary source context.
Only change what is necessary for ${id}. Do not start any other story in this run.
Honor ${PRD}.meta.guardrails; no scope reduction and no partial completion.
Do not delete tests to make them pass.
Lock behavior with focused regression tests before risky storage changes when
coverage is missing.
Intrusive test code must live in a separate file from the production code it exercises.
Run the verification required by ${id}.verifiable_output and every acceptance
criterion for ${id}; include cargo test/build/grep evidence when applicable.
Test-cycle policy (speed without quality loss):
  1. Inner loop while iterating on ${id}: build and test with the
     'release-test' profile against the named test files in ${id} only.
     Prefer 'cargo nextest run --profile release-test --tests <named>' if
     'cargo nextest --version' succeeds; otherwise fall back to
     'cargo test --profile release-test --tests <named>'. Also run
     'cargo test --profile release-test --lib <relevant_module>' for unit
     tests. Optimization level matches release; only fat LTO and serial
     codegen are dropped, so observable behavior is unchanged.
  2. Final gate, run exactly once after the named tests are green and
     immediately before flipping ${id}.passes to true: run every command
     and grep gate named by ${id}.acceptance_criteria. For US-014, the
     canonical Phase 7 release gates are exactly:
       cargo test --release
       cargo test --release --features test-hooks
     Critic review still uses the story acceptance criteria as the
     authoritative evidence.
Do not run the full canonical suite repeatedly during iteration; that is the
documented bottleneck. The release-test profile exists for the inner loop only.
${critic_note}
When ${id}.passes is true and verified, run \$cancel and exit."

  run_driver || {
      rc=$?
      echo "[ralph-phase7] codex exited rc=$rc on $id - see $log" >&2
      exit "$rc"
    }

  still_open="$(story_passes "$id")"
  if [ "$still_open" != "true" ]; then
    echo "[ralph-phase7] $id still has passes:false after run - halting." >&2
    echo "[ralph-phase7] inspect $log and fix before re-running." >&2
    exit 3
  fi

  echo "[ralph-phase7] $id marked passes:true. Remaining open: $(open_count)"
done
