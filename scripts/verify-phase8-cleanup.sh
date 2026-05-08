#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

fail() {
  printf 'phase8 cleanup verification failed: %s\n' "$1" >&2
  exit 1
}

commit_path_pattern='journal_mutex|lock_journal_mutex|flush_under_journal_mutex|sync_journal_under_journal_mutex|begin_txn|rollback_txn|append_logical_txn|append_chain_commit|truncate_to\('
commit_path_files=(
  src/storage/paged_engine.rs
  src/storage/paged_engine/state.rs
  src/storage/paged_engine/snapshot_ops.rs
)

matches="$(rg -n "$commit_path_pattern" "${commit_path_files[@]}" || true)"
if [[ -n "$matches" ]]; then
  printf '%s\n' "$matches" >&2
  fail "legacy journal rollback/mutex symbols remain in production commit paths"
fi

if rg -n 'journal_mutex' docs/CONCURRENCY.md >/tmp/phase8-doc-stale.$$; then
  cat /tmp/phase8-doc-stale.$$ >&2
  rm -f /tmp/phase8-doc-stale.$$
  fail "CONCURRENCY.md still documents journal_mutex as a live writer lock"
fi
rm -f /tmp/phase8-doc-stale.$$

legacy_defs="$(rg -n 'fn (begin_txn|rollback_txn|append_logical_txn|append_chain_commit(_end_lsn)?|truncate_to)\b' \
  src/storage/handle.rs src/journal/mod.rs src/mvcc/transaction.rs || true)"
unmarked_defs=""
while IFS=: read -r file line rest; do
  [[ -n "${file:-}" ]] || continue
  context="$(sed -n "${line},$((line + 3))p" "$file")"
  if ! printf '%s\n' "$context" | rg -q 'allow-phase8-legacy-audit'; then
    unmarked_defs+="${file}:${line}:${rest}"$'\n'
  fi
done <<< "$legacy_defs"
if [[ -n "$unmarked_defs" ]]; then
  printf '%s' "$unmarked_defs" >&2
  fail "retired journal APIs must carry allow-phase8-legacy-audit markers"
fi

if [[ -e src/storage/paged_engine/group_commit.rs ]]; then
  fail "retired ticket group_commit.rs production module still exists"
fi

group_commit_matches="$(rg -n 'GroupCommitManager' src/storage src/journal || true)"
if [[ -n "$group_commit_matches" ]]; then
  printf '%s\n' "$group_commit_matches" >&2
  fail "ticket GroupCommitManager symbol remains under production source"
fi

if ! rg -q 'truncate_tail_to_valid_end_lsn' src/journal/recovery.rs; then
  fail "recovery tail truncation helper is not named truncate_tail_to_valid_end_lsn"
fi

bad_tail_helper="$(rg -n 'fn truncate_tail_to\(' src/journal/recovery.rs || true)"
if [[ -n "$bad_tail_helper" ]]; then
  printf '%s\n' "$bad_tail_helper" >&2
  fail "ambiguous recovery tail truncation helper name remains"
fi

printf 'phase8 cleanup verification passed\n'
