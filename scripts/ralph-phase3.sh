#!/usr/bin/env bash
# ralph-phase3.sh - drive Phase 3 implementation story-by-story via ralph,
# with a fresh Codex session per story so context does not accumulate.
#
# Picks the lowest-priority story with passes:false from
# .omc/phase-03-prd.json, invokes ralph on just that story, waits for it to
# exit, then loops. Stops when every story passes.
#
# Usage:
#   scripts/ralph-phase3.sh                 # run to completion
#   scripts/ralph-phase3.sh --dry-run       # print next story, do nothing
#   scripts/ralph-phase3.sh --max 3         # stop after 3 iterations
#   PRD=/path/to/other-prd.json scripts/ralph-phase3.sh
#
# Env overrides:
#   PRD        path to the prd.json           (default: .omc/phase-03-prd.json)
#   LOG_DIR    directory for per-story logs   (default: .omc/logs)
#   CODEX_BIN  codex CLI binary               (default: codex)
#   CRITIC     reviewer passed to ralph       (default: claude)
#   CLAUDE_BIN claude CLI binary for critic   (default: claude)

set -euo pipefail

PRD="${PRD:-.omc/phase-03-prd.json}"
LOG_DIR="${LOG_DIR:-.omc/logs}"
CODEX_BIN="${CODEX_BIN:-codex}"
CRITIC="${CRITIC:-claude}"
CLAUDE_BIN="${CLAUDE_BIN:-claude}"
DRY_RUN=0
MAX_ITER=0

usage() {
  sed -n '1,24p' "$0"
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
[ -f "$PRD" ] || { echo "error: PRD not found at $PRD" >&2; exit 2; }
PHASE_NAME="$(jq -r '.meta.phase // "Storage Upgrade Phase 3"' "$PRD")"
SOURCE_DOC="$(jq -r '.meta.source_doc // "docs/STORAGE-UPGRADE-PHASE-03-ORDERED-LIVE-DELTAS.md"' "$PRD")"
[ -f "$SOURCE_DOC" ] || { echo "error: Phase 3 spec not found at $SOURCE_DOC" >&2; exit 2; }
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
    [ .stories[] | select(.passes == false) ]
    | sort_by(.priority)
    | .[0]
    | if . == null then empty else "\(.id)\t\(.priority)\t\(.title)" end
  ' "$PRD"
}

open_count() {
  jq -r '[.stories[] | select(.passes == false)] | length' "$PRD"
}

iter=0
while :; do
  row="$(next_story)"
  if [ -z "$row" ]; then
    echo "[ralph-phase3] all stories pass - done."
    exit 0
  fi

  iter=$((iter + 1))
  if [ "$MAX_ITER" -gt 0 ] && [ "$iter" -gt "$MAX_ITER" ]; then
    echo "[ralph-phase3] stopped after $MAX_ITER iterations (--max). Open: $(open_count)"
    exit 0
  fi

  id="$(printf '%s' "$row" | cut -f1)"
  pri="$(printf '%s' "$row" | cut -f2)"
  title="$(printf '%s' "$row" | cut -f3-)"
  ts="$(date +%Y%m%d-%H%M%S)"
  log="$LOG_DIR/${ts}-phase3-${id}.log"
  phase="$(phase_name)"
  spec="$(source_doc)"
  critic_note="Use ${CRITIC} as critic before setting passes:true in ${PRD}."
  if [ "$CRITIC" = "claude" ]; then
    critic_note="Use Claude as critic before setting passes:true in ${PRD}; if the local Ralph path cannot route --critic=claude directly, run the Claude critic through ${CLAUDE_BIN} or the ask-claude skill and treat any blocker finding as a failed story."
  fi

  echo "[ralph-phase3] iter=$iter story=$id priority=$pri"
  echo "[ralph-phase3]   phase: $phase"
  echo "[ralph-phase3]   title: $title"
  echo "[ralph-phase3]   spec:  $spec"
  echo "[ralph-phase3]   executor: $CODEX_BIN"
  echo "[ralph-phase3]   critic:   $CRITIC"
  echo "[ralph-phase3]   log:   $log"

  if [ "$DRY_RUN" = 1 ]; then
    echo "[ralph-phase3] (dry-run) would invoke ralph on $id"
    exit 0
  fi

  prompt="\$ralph --critic=${CRITIC} Implement exactly one story - ${id} - from ${PRD}. \
Use ${spec} as the authoritative Phase 3 storage-upgrade spec and keep the PRD acceptance criteria as the completion checklist. \
Only change what is necessary for ${id}. Do not start any other story in this run. \
Lock behavior with focused regression tests before risky storage changes when coverage is missing. \
Intrusive test code must live in a separate file from the production code it exercises. \
Run the verification required by ${id}.verifiable_output and every acceptance criterion for ${id}; include cargo test/build/grep evidence when applicable. \
${critic_note} \
When ${id}.passes is true and verified, run \$cancel and exit."

  "$CODEX_BIN" exec \
    -C "$PWD" \
    --dangerously-bypass-approvals-and-sandbox \
    --json \
    "$prompt" \
    >"$log" 2>&1 || {
      rc=$?
      echo "[ralph-phase3] codex exited rc=$rc on $id - see $log" >&2
      exit "$rc"
    }

  still_open="$(jq -r --arg id "$id" \
    '.stories[] | select(.id == $id) | .passes' "$PRD")"
  if [ "$still_open" != "true" ]; then
    echo "[ralph-phase3] $id still has passes:false after run - halting." >&2
    echo "[ralph-phase3] inspect $log and fix before re-running." >&2
    exit 3
  fi

  echo "[ralph-phase3] $id marked passes:true. Remaining open: $(open_count)"
done
