#!/usr/bin/env bash
# Pre-commit hook: verify file:line citation integrity in phase docs + cck.md.
#
# Wire up:
#   ln -sf ../../scripts/pre-commit-citations.sh .git/hooks/pre-commit
#   # or append the exec line to an existing .git/hooks/pre-commit
#
# Pass --strict to fail the commit on any drift (default: warn only).
set -euo pipefail
REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || dirname "$(dirname "$0")")"
exec python3 "$REPO_ROOT/scripts/verify_phase_citations.py" "$@"
