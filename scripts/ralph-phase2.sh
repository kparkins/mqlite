#!/usr/bin/env bash
# ralph-phase2.sh — drive Phase 2 implementation story-by-story via ralph,
# with a fresh Claude Code session per story so context doesn't accumulate.
#
# Picks the lowest-priority (= highest priority by number) story with
# passes:false from .omc/phase-02-prd.json, invokes ralph on just that
# story, waits for it to exit, then loops. Stops when every story passes.
#
# Usage:
#   scripts/ralph-phase2.sh                 # run to completion
#   scripts/ralph-phase2.sh --dry-run       # print next story, do nothing
#   scripts/ralph-phase2.sh --max 3         # stop after 3 iterations
#   PRD=/path/to/other-prd.json scripts/ralph-phase2.sh
#
# Env overrides:
#   PRD        path to the prd.json           (default: .omc/phase-02-prd.json)
#   LOG_DIR    directory for per-story logs   (default: .omc/logs)
#   CRITIC     reviewer passed to ralph       (default: codex)
#   CLAUDE_BIN claude CLI binary              (default: claude)

set -euo pipefail

PRD="${PRD:-.omc/phase-02-prd.json}"
LOG_DIR="${LOG_DIR:-.omc/logs}"
CRITIC="${CRITIC:-codex}"
CLAUDE_BIN="${CLAUDE_BIN:-claude}"
DRY_RUN=0
MAX_ITER=0  # 0 = unlimited

while [ $# -gt 0 ]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --max)     shift; MAX_ITER="${1:-0}" ;;
    -h|--help) sed -n '1,25p' "$0"; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

command -v jq >/dev/null || { echo "error: jq is required" >&2; exit 2; }
[ -f "$PRD" ] || { echo "error: PRD not found at $PRD" >&2; exit 2; }
if [ "$DRY_RUN" = 0 ]; then
  command -v "$CLAUDE_BIN" >/dev/null || {
    echo "error: $CLAUDE_BIN not in PATH (set CLAUDE_BIN=... to override)" >&2
    exit 2
  }
fi

mkdir -p "$LOG_DIR"

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
    echo "[ralph-phase2] all stories pass — done."
    exit 0
  fi

  iter=$((iter + 1))
  if [ "$MAX_ITER" -gt 0 ] && [ "$iter" -gt "$MAX_ITER" ]; then
    echo "[ralph-phase2] stopped after $MAX_ITER iterations (--max). Open: $(open_count)"
    exit 0
  fi

  id="$(printf '%s' "$row" | cut -f1)"
  pri="$(printf '%s' "$row" | cut -f2)"
  title="$(printf '%s' "$row" | cut -f3-)"
  ts="$(date +%Y%m%d-%H%M%S)"
  log="$LOG_DIR/${ts}-${id}.log"

  echo "[ralph-phase2] iter=$iter story=$id priority=$pri"
  echo "[ralph-phase2]   title: $title"
  echo "[ralph-phase2]   log:   $log"

  if [ "$DRY_RUN" = 1 ]; then
    echo "[ralph-phase2] (dry-run) would invoke ralph on $id"
    exit 0
  fi

  prompt="/oh-my-claudecode:ralph --critic=${CRITIC} Implement exactly one story — ${id} — from ${PRD}. \
Only change what is necessary for ${id}. Do not start any other story in this run. \
Intrusive test code must live in a separate file from the production code it exercises. \
Verify every acceptance criterion for ${id} with codex before setting passes:true in ${PRD}. \
When ${id}.passes is true and verified, run /oh-my-claudecode:cancel and exit."

  # --dangerously-skip-permissions lets ralph run unattended; drop it if
  # you prefer to approve each tool call.
  "$CLAUDE_BIN" \
    -p "$prompt" \
    --dangerously-skip-permissions \
    --output-format stream-json \
    --effort=xhigh
    --verbose \
    >"$log" 2>&1 || {
      rc=$?
      echo "[ralph-phase2] claude exited rc=$rc on $id — see $log" >&2
      exit "$rc"
    }

  # Guard against a story that didn't actually flip passes:true.
  still_open=$(jq -r --arg id "$id" \
    '.stories[] | select(.id==$id) | .passes' "$PRD")
  if [ "$still_open" != "true" ]; then
    echo "[ralph-phase2] $id still has passes:false after run — halting." >&2
    echo "[ralph-phase2] inspect $log and fix before re-running." >&2
    exit 3
  fi

  echo "[ralph-phase2] $id marked passes:true. Remaining open: $(open_count)"
done
