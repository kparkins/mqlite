#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PHASE="08"
PRD=".omc/phase-08-prd.json"
DESIGN_DOC="docs/STORAGE-UPGRADE-PHASE-08-WT-JOURNAL-GROUP-COMMIT.md"
PRD_ADAPTER=".omx/plans/prd-phase-08-wt-journal-group-commit.md"
TEST_ADAPTER=".omx/plans/test-spec-phase-08-wt-journal-group-commit.md"
ARTIFACT_ROOT=".omc/artifacts/ralph-phase8"

ROUND_LIMIT="${RALPH_ROUND_LIMIT:-}"
CODEX_EFFORT="${CODEX_EFFORT:-${CODEX_REASONING_EFFORT:-xhigh}}"
CLAUDE_SETTING_SOURCES="${CLAUDE_SETTING_SOURCES:-project,local}"
FEEDBACK_LINES="${RALPH_FEEDBACK_LINES:-120}"
WATCHDOG_POLL_SECONDS="${RALPH_WATCHDOG_POLL_SECONDS:-15}"
EXECUTOR_IDLE_TIMEOUT_SECONDS="${RALPH_EXECUTOR_IDLE_TIMEOUT_SECONDS:-2700}"
EXECUTOR_TIMEOUT_SECONDS="${RALPH_EXECUTOR_TIMEOUT_SECONDS:-7200}"
CLAUDE_IDLE_TIMEOUT_SECONDS="${RALPH_CLAUDE_IDLE_TIMEOUT_SECONDS:-900}"
CLAUDE_TIMEOUT_SECONDS="${RALPH_CLAUDE_TIMEOUT_SECONDS:-1800}"
STORY_ID="${RALPH_STORY_ID:-}"
RUN_ONCE=0

usage() {
    cat <<'USAGE'
Usage: scripts/ralph-phase8.sh [--story US-001] [--once] [--effort EFFORT]

Runs the Phase 8 story-by-story Ralph loop:
  1. Select the lowest-priority unpassed story whose dependencies have passed.
  2. Ask Codex to implement or fix only that story.
  3. Run that story's canonical verification commands from .omc/phase-08-prd.json.
  4. Ask Claude to review the story implementation.
  5. Mark passes:true only when Codex is ready, verification passes, and Claude
     returns VERDICT: APPROVE.

Environment:
  RALPH_STORY_ID      Same as --story.
  CODEX_MODEL         Optional model passed to codex exec with --model.
  CODEX_EFFORT        Codex model_reasoning_effort override. Default: xhigh.
  CLAUDE_SETTING_SOURCES  Claude setting sources for critic. Default: project,local.
  RALPH_ROUND_LIMIT   Optional emergency round limit. Default: unlimited.
  RALPH_FEEDBACK_LINES  Lines of failed output fed into the next round. Default: 120.
  RALPH_EXECUTOR_IDLE_TIMEOUT_SECONDS  Executor no-output timeout. Default: 2700.
  RALPH_EXECUTOR_TIMEOUT_SECONDS       Executor total timeout. Default: 7200.
  RALPH_CLAUDE_IDLE_TIMEOUT_SECONDS    Critic no-output timeout. Default: 900.
  RALPH_CLAUDE_TIMEOUT_SECONDS         Critic total timeout. Default: 1800.
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --story)
            STORY_ID="${2:?missing story id}"
            shift 2
            ;;
        --once)
            RUN_ONCE=1
            shift
            ;;
        --effort)
            CODEX_EFFORT="${2:?missing effort}"
            shift 2
            ;;
        --round-limit)
            ROUND_LIMIT="${2:?missing round limit}"
            shift 2
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
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "missing required command: $1" >&2
        exit 127
    fi
}

need_cmd jq
need_cmd codex
need_cmd claude

mkdir -p "$ARTIFACT_ROOT"

timestamp() {
    date -u +"%Y%m%dT%H%M%SZ"
}

story_json() {
    local story_id="$1"
    jq --arg id "$story_id" '.stories[] | select(.id == $id)' "$PRD"
}

story_exists() {
    local story_id="$1"
    jq -e --arg id "$story_id" 'any(.stories[]; .id == $id)' "$PRD" >/dev/null
}

story_ready() {
    local story_id="$1"
    jq -e --arg id "$story_id" '
      .stories as $stories
      | (reduce $stories[] as $s ({}; .[$s.id] = $s.passes)) as $passed
      | $stories[]
      | select(.id == $id)
      | all(.dependencies[]?; $passed[.] == true)
    ' "$PRD" >/dev/null
}

all_done() {
    jq -e 'all(.stories[]; .passes == true)' "$PRD" >/dev/null
}

next_story() {
    jq -r '
      .stories as $stories
      | (reduce $stories[] as $s ({}; .[$s.id] = $s.passes)) as $passed
      | $stories
      | sort_by(.priority)[]
      | select(.passes != true)
      | select(all(.dependencies[]?; $passed[.] == true))
      | .id
    ' "$PRD" | head -n 1
}

blocked_stories() {
    jq -r '
      .stories as $stories
      | (reduce $stories[] as $s ({}; .[$s.id] = $s.passes)) as $passed
      | $stories
      | sort_by(.priority)[]
      | select(.passes != true)
      | select(all(.dependencies[]?; $passed[.] == true) | not)
      | "\(.id) blocked by \([.dependencies[]? | select($passed[.] != true)] | join(","))"
    ' "$PRD"
}

set_story_passes() {
    local story_id="$1"
    local value="$2"
    local tmp
    tmp="$(mktemp "${PRD}.tmp.XXXXXX")"
    jq --arg id "$story_id" --argjson value "$value" \
        '(.stories[] | select(.id == $id) | .passes) = $value' \
        "$PRD" > "$tmp"
    mv "$tmp" "$PRD"
    jq empty "$PRD"
}

capture_passed_ids() {
    local out="$1"
    jq -r '.stories[] | select(.passes == true) | .id' "$PRD" > "$out"
}

file_mtime_seconds() {
    local path="$1"
    if [[ -e "$path" ]]; then
        stat -f '%m' "$path"
    else
        date +%s
    fi
}

kill_process_tree() {
    local pid="$1"
    pkill -TERM -P "$pid" >/dev/null 2>&1 || true
    kill -TERM "$pid" >/dev/null 2>&1 || true
    sleep 2
    pkill -KILL -P "$pid" >/dev/null 2>&1 || true
    kill -KILL "$pid" >/dev/null 2>&1 || true
}

run_with_watchdog() {
    local label="$1"
    local prompt="$2"
    local out="$3"
    local idle_timeout="$4"
    local total_timeout="$5"
    shift 5

    local start last_mtime now pid status
    start="$(date +%s)"

    "$@" < "$prompt" > "$out" 2>&1 &
    pid="$!"

    while kill -0 "$pid" >/dev/null 2>&1; do
        sleep "$WATCHDOG_POLL_SECONDS"
        now="$(date +%s)"
        last_mtime="$(file_mtime_seconds "$out")"

        if [[ "$total_timeout" -gt 0 && $((now - start)) -gt "$total_timeout" ]]; then
            {
                printf '\nRALPH_WATCHDOG_STATUS: TIMEOUT\n'
                printf '%s exceeded total timeout after %s seconds.\n' "$label" "$total_timeout"
            } >> "$out"
            kill_process_tree "$pid"
            wait "$pid" >/dev/null 2>&1 || true
            return 124
        fi

        if [[ "$idle_timeout" -gt 0 && $((now - last_mtime)) -gt "$idle_timeout" ]]; then
            {
                printf '\nRALPH_WATCHDOG_STATUS: IDLE_TIMEOUT\n'
                printf '%s produced no output for %s seconds.\n' "$label" "$idle_timeout"
            } >> "$out"
            kill_process_tree "$pid"
            wait "$pid" >/dev/null 2>&1 || true
            return 124
        fi
    done

    wait "$pid"
    status="$?"
    return "$status"
}

reset_unapproved_passes() {
    local previously_passed="$1"
    local approved_story="${2:-}"
    local current_passed
    current_passed="$(mktemp "$ARTIFACT_ROOT/current-passed.XXXXXX")"
    jq -r '.stories[] | select(.passes == true) | .id' "$PRD" > "$current_passed"

    local story_id
    while IFS= read -r story_id; do
        [[ -z "$story_id" ]] && continue
        if [[ "$story_id" == "$approved_story" ]]; then
            continue
        fi
        if ! rg -qxF "$story_id" "$previously_passed"; then
            printf 'Resetting unapproved passes:true on %s\n' "$story_id"
            set_story_passes "$story_id" false
        fi
    done < "$current_passed"

    rm -f "$current_passed"
}

run_codex() {
    local prompt="$1"
    local out="$2"
    local args=(codex exec --cd "$ROOT" --sandbox danger-full-access)
    if [[ -n "${CODEX_MODEL:-}" ]]; then
        args+=(--model "$CODEX_MODEL")
    fi
    if [[ -n "$CODEX_EFFORT" ]]; then
        args+=(-c "model_reasoning_effort=\"$CODEX_EFFORT\"")
    fi
    args+=(-)
    run_with_watchdog \
        "Codex executor" \
        "$prompt" \
        "$out" \
        "$EXECUTOR_IDLE_TIMEOUT_SECONDS" \
        "$EXECUTOR_TIMEOUT_SECONDS" \
        "${args[@]}"
}

run_claude() {
    local prompt="$1"
    local out="$2"
    local prompt_text
    prompt_text="$(cat "$prompt")"
    run_with_watchdog \
        "Claude critic" \
        "$prompt" \
        "$out" \
        "$CLAUDE_IDLE_TIMEOUT_SECONDS" \
        "$CLAUDE_TIMEOUT_SECONDS" \
        claude --setting-sources "$CLAUDE_SETTING_SOURCES" -p "$prompt_text"
}

make_executor_prompt() {
    local story_id="$1"
    local round="$2"
    local story_dir="$3"
    local feedback_file="${4:-}"
    local prompt="$story_dir/executor-round-${round}.prompt.md"

    {
        printf '# Ralph Executor Prompt — Phase 8 %s Round %s\n\n' "$story_id" "$round"
        printf 'You are the Codex executor for one mqlite Phase 8 PRD story.\n\n'
        printf 'Working directory: `%s`.\n\n' "$ROOT"
        printf 'Canonical files:\n'
        printf -- '- `%s`\n' "$PRD"
        printf -- '- `%s`\n' "$DESIGN_DOC"
        printf -- '- `%s`\n' "$PRD_ADAPTER"
        printf -- '- `%s`\n\n' "$TEST_ADAPTER"
        printf 'Story JSON:\n\n```json\n'
        story_json "$story_id"
        printf '\n```\n\n'
        if [[ -n "$feedback_file" && -s "$feedback_file" ]]; then
            printf 'Previous verification or Claude review feedback to resolve fully:\n\n```text\n'
            sed -n "1,${FEEDBACK_LINES}p" "$feedback_file"
            printf '\n```\n\n'
        fi
        cat <<'PROMPT'
Execution contract:
- Implement or fix only the current story and its directly required tests.
- Preserve the Phase 8 PRD contract exactly. If the implementation exposes a PRD flaw, patch the PRD narrowly and explain why.
- Do not mark this story `passes:true`; the runner owns that after verification and Claude approval.
- Do not mark any other story `passes:true`.
- Run the narrow checks you need while working. The runner will run the canonical story verification commands after you finish.
- If Claude previously rejected the story, resolve every finding before claiming readiness.
- If you cannot complete the story, stop with `EXECUTOR_STATUS: BLOCKED` and a concrete blocker.

Final response must contain exactly one status line:

`EXECUTOR_STATUS: READY_FOR_REVIEW`

or

`EXECUTOR_STATUS: BLOCKED`
PROMPT
    } > "$prompt"

    printf '%s\n' "$prompt"
}

make_verification_log() {
    local story_id="$1"
    local story_dir="$2"
    local round="$3"
    local log="$story_dir/verification-round-${round}.log"
    local commands_file="$story_dir/verification-commands.txt"

    jq -r --arg id "$story_id" \
        '.stories[] | select(.id == $id) | .verification[]' \
        "$PRD" > "$commands_file"

    {
        printf 'Verification for %s round %s\n' "$story_id" "$round"
        printf 'Started: %s\n\n' "$(timestamp)"
    } > "$log"

    local index=0
    local cmd
    while IFS= read -r cmd; do
        index=$((index + 1))
        {
            printf '\n===== command %s =====\n' "$index"
            printf '%s\n\n' "$cmd"
        } >> "$log"
        if ! bash -lc "$cmd" >> "$log" 2>&1; then
            {
                printf '\nVERIFICATION_STATUS: FAIL\n'
                printf 'Failed command %s: %s\n' "$index" "$cmd"
            } >> "$log"
            return 1
        fi
    done < "$commands_file"

    printf '\nVERIFICATION_STATUS: PASS\n' >> "$log"
    printf '%s\n' "$log"
}

make_claude_prompt() {
    local story_id="$1"
    local round="$2"
    local story_dir="$3"
    local verification_log="$4"
    local prompt="$story_dir/claude-review-round-${round}.prompt.md"
    local diff_file="$story_dir/git-diff-round-${round}.patch"
    local status_file="$story_dir/git-status-round-${round}.txt"

    git status --short > "$status_file"
    git diff -- . ":(exclude)$ARTIFACT_ROOT" > "$diff_file"

    {
        printf '# Claude Critic Prompt — Phase 8 %s Round %s\n\n' "$story_id" "$round"
        printf 'You are the Claude critic for a Ralph story implementation.\n\n'
        printf 'Working directory: `%s`.\n\n' "$ROOT"
        printf 'Review only story `%s` against the canonical Phase 8 PRD and design.\n\n' "$story_id"
        printf 'Canonical files to read:\n'
        printf -- '- `%s`\n' "$PRD"
        printf -- '- `%s`\n' "$DESIGN_DOC"
        printf -- '- `%s`\n' "$PRD_ADAPTER"
        printf -- '- `%s`\n\n' "$TEST_ADAPTER"
        printf 'Story JSON:\n\n```json\n'
        story_json "$story_id"
        printf '\n```\n\n'
        printf 'Verification log: `%s`\n' "$verification_log"
        printf 'Git status snapshot: `%s`\n' "$status_file"
        printf 'Git diff snapshot: `%s`\n\n' "$diff_file"
        cat <<'PROMPT'
Review contract:
- Check correctness, missing acceptance criteria, missing tests, hidden PRD drift, and unsafe shortcuts.
- Treat the story as not complete if any acceptance criterion is unproven.
- Treat the story as not complete if any verification command failed or was skipped.
- Treat the story as not complete if the implementation marked `passes:true` before this review.
- If the story is overloaded or cannot be proven independently, reject and request a split or PRD repair.
- For ambiguous journal/group-commit/MWMR behavior, use the WiredTiger-style contract in the Phase 8 design doc as the tie-breaker.

Output format is mandatory:

VERDICT: APPROVE

Only use APPROVE when all issues are resolved and the story may be marked
`passes:true`.

Otherwise:

VERDICT: REJECT

Then list findings grouped by BLOCKER / MAJOR / MINOR with exact required fixes.
Do not wrap the verdict line in Markdown backticks.
PROMPT
    } > "$prompt"

    printf '%s\n' "$prompt"
}

approval_from_claude() {
    local review="$1"
    rg -q '^`?VERDICT:[[:space:]]*APPROVE[[:space:]]*`?$' "$review"
}

claude_returned_verdict() {
    local review="$1"
    rg -q '^`?VERDICT:[[:space:]]*(APPROVE|REJECT)[[:space:]]*`?$' "$review"
}

executor_ready() {
    local output="$1"
    rg -q '^EXECUTOR_STATUS:[[:space:]]*READY_FOR_REVIEW[[:space:]]*$' "$output"
}

write_feedback_from_tail() {
    local source="$1"
    local dest="$2"
    {
        printf 'Previous round did not pass. Resolve this before the next review.\n\n'
        printf 'Source: %s\n\n' "$source"
        tail -n "$FEEDBACK_LINES" "$source"
    } > "$dest"
}

next_round_number() {
    local story_dir="$1"
    local max_round=0
    local file name round

    for file in "$story_dir"/executor-round-*.out "$story_dir"/executor-round-*.prompt.md; do
        [[ -e "$file" ]] || continue
        name="${file##*/}"
        round="${name#executor-round-}"
        round="${round%%.*}"
        case "$round" in
            ''|*[!0-9]*)
                continue
                ;;
        esac
        if [[ "$round" -gt "$max_round" ]]; then
            max_round="$round"
        fi
    done

    printf '%s\n' "$((max_round + 1))"
}

resume_feedback_file() {
    local story_dir="$1"
    local next_round="$2"
    local previous_round="$((next_round - 1))"
    local feedback="$story_dir/feedback-resume-round-${previous_round}.txt"
    local source=""

    if [[ "$previous_round" -lt 1 ]]; then
        return 0
    fi

    if [[ -s "$story_dir/feedback-round-${previous_round}.txt" ]]; then
        printf '%s\n' "$story_dir/feedback-round-${previous_round}.txt"
        return 0
    fi

    if [[ -s "$story_dir/claude-review-round-${previous_round}.out" ]] &&
        ! approval_from_claude "$story_dir/claude-review-round-${previous_round}.out"; then
        source="$story_dir/claude-review-round-${previous_round}.out"
    elif [[ -s "$story_dir/verification-round-${previous_round}.log" ]] &&
        ! rg -q '^VERIFICATION_STATUS:[[:space:]]*PASS[[:space:]]*$' \
            "$story_dir/verification-round-${previous_round}.log"; then
        source="$story_dir/verification-round-${previous_round}.log"
    elif [[ -s "$story_dir/executor-round-${previous_round}.out" ]] &&
        ! executor_ready "$story_dir/executor-round-${previous_round}.out"; then
        source="$story_dir/executor-round-${previous_round}.out"
    fi

    if [[ -n "$source" ]]; then
        write_feedback_from_tail "$source" "$feedback"
        printf '%s\n' "$feedback"
    fi
}

run_story() {
    local story_id="$1"
    local story_dir="$ARTIFACT_ROOT/$story_id"
    mkdir -p "$story_dir"

    printf '\n===== Phase 8 Ralph story %s =====\n' "$story_id"
    story_json "$story_id" > "$story_dir/story.json"

    local feedback=""
    local round
    round="$(next_round_number "$story_dir")"
    if [[ "$round" -gt 1 ]]; then
        feedback="$(resume_feedback_file "$story_dir" "$round")"
        printf 'Resuming %s at round %s.\n' "$story_id" "$round"
    fi
    while :; do
        if [[ -n "$ROUND_LIMIT" && "$round" -gt "$ROUND_LIMIT" ]]; then
            echo "round limit $ROUND_LIMIT reached for $story_id; leaving passes:false" >&2
            set_story_passes "$story_id" false
            return 1
        fi

        printf '\n--- %s round %s: executor ---\n' "$story_id" "$round"
        local executor_prompt executor_out
        executor_prompt="$(make_executor_prompt "$story_id" "$round" "$story_dir" "$feedback")"
        executor_out="$story_dir/executor-round-${round}.out"
        local passed_before
        passed_before="$story_dir/passed-before-round-${round}.txt"
        capture_passed_ids "$passed_before"

        if ! run_codex "$executor_prompt" "$executor_out"; then
            echo "Codex executor failed for $story_id round $round. See $executor_out" >&2
            return 1
        fi

        # The runner, not the executor, owns new passes:true transitions.
        reset_unapproved_passes "$passed_before"
        set_story_passes "$story_id" false

        if ! executor_ready "$executor_out"; then
            feedback="$story_dir/feedback-round-${round}.txt"
            write_feedback_from_tail "$executor_out" "$feedback"
            printf 'Executor did not declare READY_FOR_REVIEW; looping.\n'
            round=$((round + 1))
            continue
        fi

        printf '\n--- %s round %s: verification ---\n' "$story_id" "$round"
        local verification_log
        if ! verification_log="$(make_verification_log "$story_id" "$story_dir" "$round")"; then
            verification_log="$story_dir/verification-round-${round}.log"
            feedback="$story_dir/feedback-round-${round}.txt"
            write_feedback_from_tail "$verification_log" "$feedback"
            printf 'Verification failed; looping.\n'
            round=$((round + 1))
            continue
        fi

        printf '\n--- %s round %s: Claude critic ---\n' "$story_id" "$round"
        local claude_prompt claude_out
        claude_prompt="$(make_claude_prompt "$story_id" "$round" "$story_dir" "$verification_log")"
        claude_out="$story_dir/claude-review-round-${round}.out"

        if ! run_claude "$claude_prompt" "$claude_out"; then
            echo "Claude critic failed for $story_id round $round. See $claude_out" >&2
            return 1
        fi

        if ! claude_returned_verdict "$claude_out"; then
            echo "Claude critic returned no valid verdict for $story_id round $round. See $claude_out" >&2
            return 1
        fi

        if approval_from_claude "$claude_out"; then
            set_story_passes "$story_id" true
            printf 'STORY_APPROVED: %s\n' "$story_id" | tee "$story_dir/approved.txt"
            return 0
        fi

        set_story_passes "$story_id" false
        feedback="$story_dir/feedback-round-${round}.txt"
        write_feedback_from_tail "$claude_out" "$feedback"
        printf 'Claude rejected story; looping.\n'
        round=$((round + 1))
    done
}

main() {
    jq empty "$PRD"

    if [[ -n "$STORY_ID" ]]; then
        if ! story_exists "$STORY_ID"; then
            echo "unknown story id: $STORY_ID" >&2
            exit 2
        fi
        if ! story_ready "$STORY_ID"; then
            echo "story dependencies are not passed: $STORY_ID" >&2
            blocked_stories >&2 || true
            exit 2
        fi
        run_story "$STORY_ID"
        exit $?
    fi

    while ! all_done; do
        local story_id
        story_id="$(next_story)"
        if [[ -z "$story_id" || "$story_id" == "null" ]]; then
            echo "no runnable story found; unresolved dependency graph:" >&2
            blocked_stories >&2 || true
            exit 2
        fi

        run_story "$story_id"

        if (( RUN_ONCE == 1 )); then
            break
        fi
    done

    if all_done; then
        printf '\nPHASE8_RALPH_STATUS: COMPLETE\n'
    else
        printf '\nPHASE8_RALPH_STATUS: PARTIAL\n'
    fi
}

main
