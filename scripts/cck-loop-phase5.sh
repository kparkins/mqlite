#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/cck-loop-phase5.sh [--max-rounds N] [--timeout SECONDS] [--dry-run]

Runs a Claude/Codex convergence loop for Phase 5 PRD/spec readiness.

Required tools:
  claude (or CLAUDE_BIN override)
  codex (or CODEX_BIN override)
  jq
  rg
  shasum

Environment overrides:
  PROMPT_FILE   Base critic prompt. Defaults to
                .omx/artifacts/cck-phase5-prd-spec-convergence-prompt.md
  ARTIFACT_DIR  Output directory. Defaults to .omx/artifacts/cck-loop-phase5
  CLAUDE_BIN    Claude CLI binary. Defaults to claude
  CODEX_BIN     Codex CLI binary. Defaults to codex

The loop stops when both reviewers emit exactly:
  VERDICT: APPROVE

Otherwise it asks Codex to patch the Phase 5 planning artifacts and repeats.
USAGE
}

MAX_ROUNDS=6
TIMEOUT_SECONDS=1800
DRY_RUN=0

PROMPT_FILE="${PROMPT_FILE:-.omx/artifacts/cck-phase5-prd-spec-convergence-prompt.md}"
ARTIFACT_DIR="${ARTIFACT_DIR:-.omx/artifacts/cck-loop-phase5}"
CLAUDE_BIN="${CLAUDE_BIN:-claude}"
CODEX_BIN="${CODEX_BIN:-codex}"
ROOT="$(pwd)"

TARGET_FILES=(
  "docs/STORAGE-UPGRADE-PHASE-05-MULTI-WRITER-CRUD.md"
  ".omc/phase-05-prd.json"
  ".omx/plans/prd-phase-05-same-collection-multi-writer-crud.md"
  ".omx/plans/test-spec-phase-05-same-collection-multi-writer-crud.md"
)

PHASE5_FATAL_STALE_PATTERN='register publish_seq \(under journal_mutex'
PHASE5_FATAL_STALE_PATTERN+='|briefly re-acquired .*metadata\(read\)'

PHASE5_REVIEW_SCAN_PATTERN='VersionState.*zero hits'
PHASE5_REVIEW_SCAN_PATTERN+='|register publish_seq \(under journal_mutex'
PHASE5_REVIEW_SCAN_PATTERN+='|briefly re-acquired .*metadata\(read\)'
PHASE5_REVIEW_SCAN_PATTERN+='|cargo test tests/mwmr_failure_edges.rs'
PHASE5_REVIEW_SCAN_PATTERN+='|cargo test --release --tests -- mwmr_p5'
PHASE5_REVIEW_SCAN_PATTERN+='|commit_seq: Mutex<()> must be deleted'

MWMR_MARKER_PATTERN='PageLatch|LatchedPinnedPage|NsWriterRegistry'
MWMR_MARKER_PATTERN+='|NsWriteTicket|NsDdlBarrierGuard|PublishSequencer'
MWMR_MARKER_PATTERN+='|WriteConflictReason|journal_mutex|GroupCommit'
MWMR_MARKER_PATTERN+='|commit_seq|lane_for|acquire_lane|metadata\.read'

while [ "$#" -gt 0 ]; do
  case "$1" in
    --max-rounds)
      MAX_ROUNDS="${2:?missing value for --max-rounds}"
      shift 2
      ;;
    --timeout)
      TIMEOUT_SECONDS="${2:?missing value for --timeout}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 127
  }
}

check_inputs() {
  need_cmd omx
  need_cmd jq
  need_cmd rg
  need_cmd shasum
  need_cmd "$CLAUDE_BIN"
  need_cmd "$CODEX_BIN"

  if [ ! -f "$PROMPT_FILE" ]; then
    echo "missing prompt file: $PROMPT_FILE" >&2
    exit 1
  fi

  for file in "${TARGET_FILES[@]}"; do
    if [ ! -f "$file" ]; then
      echo "missing target file: $file" >&2
      exit 1
    fi
  done
}

run_with_timeout() {
  local seconds="$1"
  shift

  "$@" <&0 &
  local pid=$!
  local elapsed=0

  while kill -0 "$pid" >/dev/null 2>&1; do
    if [ "$elapsed" -ge "$seconds" ]; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" >/dev/null 2>&1 || true
      return 124
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done

  wait "$pid"
}

hash_targets() {
  shasum "$PROMPT_FILE" "${TARGET_FILES[@]}" | shasum | awk '{print $1}'
}

make_convergence_snapshot() {
  local out="$1"
  local missing_deps
  missing_deps="$(
    jq -r \
      '.stories[] | select(has("dependencies") | not) |
       "- " + .id + ": missing dependencies array"' \
      .omc/phase-05-prd.json
  )"

  {
    printf '# Phase 05 Convergence Snapshot\n\n'
    printf 'Generated: %s\n\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

    printf '## Target Files\n\n'
    for file in "${TARGET_FILES[@]}"; do
      printf -- '- %s\n' "$file"
    done
    printf '\n'

    printf '## Artifact Sizes\n\n'
    wc -l "${TARGET_FILES[@]}"
    printf '\n'

    printf '## Canonical PRD Story Map\n\n'
    jq -r \
      '.stories[] |
       "- " + .id + ": " + .title +
       " | passes=" + ((.passes // false) | tostring) +
       " | deps=" + ((.dependencies // []) | join(","))' \
      .omc/phase-05-prd.json
    printf '\n'

    printf '## Stories Missing Dependency Arrays\n\n'
    if [ -n "$missing_deps" ]; then
      printf '%s\n' "$missing_deps"
    else
      printf 'No missing dependency arrays.\n'
    fi
    printf '\n'

    printf '## Approval Definition\n\n'
    jq -r '.meta.approval_definition // "missing"' .omc/phase-05-prd.json
    printf '\n'

    printf '## Phase 5 Drift Scan\n\n'
    if rg -n "$PHASE5_REVIEW_SCAN_PATTERN" "${TARGET_FILES[@]}"; then
      true
    else
      printf 'No stale high-drift Phase 5 markers matched.\n'
    fi
    printf '\n'

    printf '## True MWMR Marker Scan\n\n'
    if rg -n "$MWMR_MARKER_PATTERN" \
      "${TARGET_FILES[@]}" src tests benches 2>/dev/null; then
      true
    else
      printf 'No MWMR implementation markers matched.\n'
    fi
    printf '\n'
  } >"$out"
}

make_review_prompt() {
  local reviewer="$1"
  local out="$2"
  local convergence_snapshot="$3"

  {
    cat "$PROMPT_FILE"
    printf '\n\n'
    printf -- '---\n\n'
    printf '# Convergence Review Instructions\n\n'
    printf 'Reviewer: %s\n\n' "$reviewer"
    printf 'This is a read-only critic pass for Phase 5 planning artifacts. '
    printf 'Do not edit files. Review whether the Phase 5 PRD/spec converge on '
    printf 'true same-collection MWMR in a WiredTiger-inspired design.\n\n'

    printf 'Authoritative planning surface:\n\n'
    printf -- '- Canonical PRD: `.omc/phase-05-prd.json`\n'
    printf -- '- Source design doc: '
    printf '`docs/STORAGE-UPGRADE-PHASE-05-MULTI-WRITER-CRUD.md`\n'
    printf -- '- PRD wrapper: '
    printf '`.omx/plans/prd-phase-05-same-collection-multi-writer-crud.md`\n'
    printf -- '- Test wrapper: '
    printf '`.omx/plans/test-spec-phase-05-same-collection-multi-writer-crud.md`\n\n'

    printf 'Current source is evidence, not the contract. If the PRD/spec rely on '
    printf 'nonexistent, stale, or incorrectly scoped source ownership, reject.\n\n'
    printf 'If the correct resolution of an MWMR, publish-ordering, latch, '
    printf 'journal, or barrier issue is ambiguous, consult the WiredTiger '
    printf 'implementation model and reject until the intended behavior is '
    printf 'made explicit in the Phase 5 planning artifacts.\n\n'
    printf 'If a PRD story tries to cover more than one implementation or '
    printf 'verification step, or can be broken into smaller independently '
    printf 'testable units, propose splitting it into more granular stories.\n\n'

    printf 'Primary review dimensions:\n\n'
    printf '1. Canonical PRD completeness: every load-bearing MWMR invariant, owner, '
    printf 'story dependency, and final gate belongs in `.omc/phase-05-prd.json`.\n'
    printf '2. True MWMR semantics: ordinary same-collection CRUD must not serialize '
    printf 'through namespace lanes or metadata read guards for the CRUD body.\n'
    printf '3. Page/local conflict model: page latches, pending/aborted/committed '
    printf 'states, overlap conflict detection, and SMO/reconcile interaction are '
    printf 'specified tightly enough to implement without guessing.\n'
    printf '4. Durability and publish ordering: journal append, group commit, publish '
    printf 'sequencing, reader visibility, and recovery/reinitialization '
    printf 'are coherent.\n'
    printf '5. DDL/index barriers: create/drop namespace and index build/commit remain '
    printf 'safe while ordinary CRUD overlaps.\n'
    printf '6. Execution readiness: wrappers are thin adapters, dependency arrays are '
    printf 'present, final verification commands are concrete, and contradictions are '
    printf 'absent.\n\n'

    printf 'Convergence targets:\n\n'
    printf -- '- canonical-prd: patch `.omc/phase-05-prd.json` first '
    printf 'when contract is missing or wrong\n'
    printf -- '- phase-doc: use the long doc for rationale only; it must '
    printf 'not contradict JSON\n'
    printf -- '- markdown-wrapper: wrappers must stay thin and non-authoritative\n'
    printf -- '- test-spec: test wrapper routes to JSON-owned verification, '
    printf 'not duplicate rules\n'
    printf -- '- source-ownership: named source/test/bench owners must '
    printf 'exist or be explicit new work\n'
    printf -- '- defer-out-of-phase: reject speculative future phases not '
    printf 'required for true Phase 5 MWMR\n'
    printf -- '- reject-finding: each BLOCKER/MAJOR must name the exact '
    printf 'artifact and required change\n\n'

    printf 'Return format:\n\n'
    printf 'Convergence summary:\n'
    printf -- '- canonical-prd: <aligned|drift> - <one sentence>\n'
    printf -- '- phase-doc: <aligned|drift> - <one sentence>\n'
    printf -- '- markdown-wrapper: <aligned|drift> - <one sentence>\n'
    printf -- '- test-spec: <aligned|drift> - <one sentence>\n'
    printf -- '- source-ownership: <aligned|drift> - <one sentence>\n\n'

    printf 'Findings:\n'
    printf -- '- BLOCKER: <artifact/path> - <specific contradiction or '
    printf 'missing contract>\n'
    printf -- '- MAJOR: <artifact/path> - <specific execution-readiness issue>\n\n'

    printf 'If no blockers or majors remain, end with exactly:\n'
    printf 'VERDICT: APPROVE\n\n'
    printf 'Otherwise end with exactly:\n'
    printf 'VERDICT: REJECT\n\n'

    printf 'Do not approve with open BLOCKER or MAJOR findings.\n\n'
    printf -- '---\n\n'
    printf '# Convergence Snapshot\n\n'
    cat "$convergence_snapshot"
  } >"$out"
}

run_claude_review() {
  local prompt_file="$1"
  local out="$2"
  local log="${out%.md}.log"

  if run_with_timeout "$TIMEOUT_SECONDS" \
    "$CLAUDE_BIN" -p \
    --permission-mode bypassPermissions \
    --add-dir "$ROOT" \
    --allowedTools Read,Grep,Glob \
    <"$prompt_file" >"$out" 2>"$log"; then
    return 0
  fi

  local rc=$?
  echo "claude review failed with exit code $rc" >>"$out"
  return "$rc"
}

run_codex_review() {
  local prompt_file="$1"
  local out="$2"
  local log="${out%.md}.log"

  if run_with_timeout "$TIMEOUT_SECONDS" \
    "$CODEX_BIN" exec \
    -C "$ROOT" \
    --sandbox read-only \
    -c 'approval_policy="never"' \
    --ephemeral \
    --output-last-message "$out" \
    - <"$prompt_file" >"$log" 2>&1; then
    return 0
  fi

  local rc=$?
  echo "codex review failed with exit code $rc" >>"$out"
  return "$rc"
}

approved() {
  local file="$1"
  grep -Eiq '^VERDICT:[[:space:]]*APPROVE[[:space:]]*$' "$file"
}

rejected() {
  local file="$1"
  grep -Eiq '^VERDICT:[[:space:]]*REJECT[[:space:]]*$' "$file"
}

summarize_verdict() {
  local name="$1"
  local file="$2"

  if approved "$file"; then
    echo "$name: APPROVE"
  elif rejected "$file"; then
    echo "$name: REJECT"
  else
    echo "$name: UNKNOWN"
  fi
}

make_patch_prompt() {
  local round_dir="$1"
  local out="$2"
  local target_hash_before="$3"
  local convergence_snapshot="$4"

  {
    printf '# Phase 5 CCK Patcher\n\n'
    printf 'You are patching only Phase 5 planning artifacts to converge the '
    printf 'PRD/spec on true same-collection MWMR in a WiredTiger-inspired design.\n\n'

    printf 'Editable artifacts:\n\n'
    for file in "${TARGET_FILES[@]}"; do
      printf -- '- `%s`\n' "$file"
    done
    printf '\n'

    printf 'Do not edit Rust source, tests, benches, scripts, or unrelated docs from '
    printf 'this patcher pass. If a reviewer finding requires source implementation, '
    printf 'make the planning contract explicit instead.\n\n'

    printf 'Patch policy:\n\n'
    printf '1. Treat `.omc/phase-05-prd.json` as canonical. Patch it first when '
    printf 'behavior, source ownership, dependencies, or tests are missing.\n'
    printf '2. Keep `.omx/plans/*phase-05*` wrappers thin. They must route to the '
    printf 'canonical JSON and must not add independent requirements.\n'
    printf '3. Keep the long Phase 5 doc as rationale/background. It must align with '
    printf 'the JSON, not override it.\n'
    printf '4. Preserve the goal: ordinary CRUD on the same collection overlaps on '
    printf 'disjoint page/key sets. Do not weaken Phase 5 into namespace-lane '
    printf 'serialization.\n'
    printf '5. Prefer small, execution-ready contract changes over future-state '
    printf 'architecture essays.\n\n'
    printf 'If a reviewer finding leaves the correct fix ambiguous, consult the '
    printf 'WiredTiger implementation model for the relevant MWMR, latch, '
    printf 'publish-ordering, journal, or barrier behavior, then encode the '
    printf 'chosen Phase 5 contract explicitly in the planning artifacts.\n\n'
    printf 'If a PRD story tries to cover more than one implementation or '
    printf 'verification step, or can be broken into smaller independently '
    printf 'testable units, split it into more granular stories with explicit '
    printf 'dependencies and acceptance criteria.\n\n'

    printf 'Known high-risk Phase 5 convergence areas to fix if present:\n\n'
    printf -- '- stale source baselines such as `VersionState` described '
    printf 'as absent after the PRD introduces it\n'
    printf -- '- `publish_seq` or equivalent visibility publication '
    printf 'ordered under the wrong mutex\n'
    printf -- '- metadata read guards held across ordinary CRUD bodies, '
    printf 'journal append, or publish\n'
    printf -- '- missing PageLatch/LatchedPinnedPage conflict and '
    printf 'latch-order ownership\n'
    printf -- '- missing Pending/Aborted/Committed delta-install state transitions\n'
    printf -- '- DDL/index barriers that cannot drain or close in-flight '
    printf 'writers safely\n'
    printf -- '- group-commit requirements that serialize all writers or '
    printf 'break commit-order visibility\n'
    printf -- '- final gates with impossible, stale, or unnamed test commands\n\n'

    printf 'Required validation after patching:\n\n'
    printf '```sh\n'
    printf 'jq empty .omc/phase-05-prd.json\n'
    printf 'bash -n scripts/cck-loop-phase5.sh\n'
    printf 'rg -n %q %s\n' \
      "$PHASE5_FATAL_STALE_PATTERN" "${TARGET_FILES[*]}"
    printf '```\n\n'
    printf 'The final `rg` command should return no matches. If you edit the storage '
    printf 'upgrade docs, also run `scripts/verify_phase_citations.py --strict`.\n\n'

    printf 'Before hash: %s\n\n' "$target_hash_before"
    printf 'After patching, end with exactly one of:\n\n'
    printf 'PATCHER_STATUS: PATCHED\n'
    printf 'PATCHER_STATUS: NO_CHANGES\n\n'

    printf -- '---\n\n'
    printf '# Claude Review\n\n'
    cat "$round_dir/claude-review.md"
    printf '\n\n# Codex Review\n\n'
    cat "$round_dir/codex-review.md"
    printf '\n\n# Convergence Snapshot\n\n'
    cat "$convergence_snapshot"
  } >"$out"
}

run_patcher() {
  local prompt_file="$1"
  local out="$2"
  local log="${out%.md}.log"

  if run_with_timeout "$TIMEOUT_SECONDS" \
    "$CODEX_BIN" exec \
    -C "$ROOT" \
    --dangerously-bypass-approvals-and-sandbox \
    --ephemeral \
    --output-last-message "$out" \
    - <"$prompt_file" >"$log" 2>&1; then
    return 0
  fi

  local rc=$?
  echo "patcher failed with exit code $rc" >>"$out"
  return "$rc"
}

patcher_changed() {
  local before="$1"
  local after="$2"
  local patcher_out="$3"

  [ "$before" != "$after" ] &&
    grep -Eiq '^PATCHER_STATUS:[[:space:]]*PATCHED[[:space:]]*$' \
      "$patcher_out"
}

validate_artifacts() {
  jq empty .omc/phase-05-prd.json

  if rg -n "$PHASE5_FATAL_STALE_PATTERN" "${TARGET_FILES[@]}" >/dev/null; then
    echo "stale Phase 5 drift marker still present:" >&2
    rg -n "$PHASE5_FATAL_STALE_PATTERN" "${TARGET_FILES[@]}" >&2
    return 1
  fi

  return 0
}

main() {
  check_inputs

  if [ "$DRY_RUN" -eq 1 ]; then
    echo "cck-loop-phase5 dry run"
    echo "prompt: $PROMPT_FILE"
    echo "artifact dir: $ARTIFACT_DIR"
    echo "max rounds: $MAX_ROUNDS"
    echo "timeout seconds: $TIMEOUT_SECONDS"
    echo "target hash: $(hash_targets)"
    printf 'targets:\n'
    printf '  %s\n' "${TARGET_FILES[@]}"
    exit 0
  fi

  mkdir -p "$ARTIFACT_DIR"

  local round=1
  while [ "$round" -le "$MAX_ROUNDS" ]; do
    local stamp
    stamp="$(date -u '+%Y%m%d-%H%M%S')"
    local round_dir="$ARTIFACT_DIR/${stamp}-round-${round}"
    mkdir -p "$round_dir"

    local convergence_snapshot="$round_dir/convergence-snapshot.md"
    make_convergence_snapshot "$convergence_snapshot"

    local claude_prompt="$round_dir/reviewer-prompt-claude.md"
    local codex_prompt="$round_dir/reviewer-prompt-codex.md"
    make_review_prompt "claude" "$claude_prompt" "$convergence_snapshot"
    make_review_prompt "codex" "$codex_prompt" "$convergence_snapshot"

    echo "round $round: running Claude review"
    if ! run_claude_review "$claude_prompt" "$round_dir/claude-review.md"; then
      echo "claude review failed; inspect $round_dir/claude-review.md" >&2
      echo "Artifacts: $round_dir" >&2
      exit 4
    fi

    echo "round $round: running Codex review"
    if ! run_codex_review "$codex_prompt" "$round_dir/codex-review.md"; then
      echo "codex review failed; inspect $round_dir/codex-review.md" >&2
      echo "Artifacts: $round_dir" >&2
      exit 4
    fi

    summarize_verdict "claude" "$round_dir/claude-review.md" \
      | tee "$round_dir/verdicts.txt"
    summarize_verdict "codex" "$round_dir/codex-review.md" \
      | tee -a "$round_dir/verdicts.txt"

    if approved "$round_dir/claude-review.md" &&
      approved "$round_dir/codex-review.md"; then
      validate_artifacts
      echo "Phase 5 CCK convergence approved in round $round"
      echo "Artifacts: $round_dir"
      exit 0
    fi

    if [ "$round" -eq "$MAX_ROUNDS" ]; then
      echo "max rounds reached without dual approval" >&2
      echo "Artifacts: $round_dir" >&2
      exit 1
    fi

    local before_hash
    before_hash="$(hash_targets)"
    local patch_prompt="$round_dir/patcher-prompt.md"
    make_patch_prompt \
      "$round_dir" "$patch_prompt" "$before_hash" "$convergence_snapshot"

    echo "round $round: asking Codex patcher to converge artifacts"
    if ! run_patcher "$patch_prompt" "$round_dir/patcher.md"; then
      echo "patcher failed; inspect $round_dir/patcher.md" >&2
      echo "Artifacts: $round_dir" >&2
      exit 6
    fi

    local after_hash
    after_hash="$(hash_targets)"
    validate_artifacts

    if ! patcher_changed "$before_hash" "$after_hash" "$round_dir/patcher.md"; then
      echo "patcher made no detectable changes; stopping" >&2
      echo "Artifacts: $round_dir" >&2
      exit 1
    fi

    round=$((round + 1))
  done
}

main "$@"
