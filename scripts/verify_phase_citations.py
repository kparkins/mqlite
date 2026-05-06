#!/usr/bin/env python3
"""Verify that phase-doc file:line citations still point at live source ranges.

The check is intentionally lightweight: it scans phase docs for `src/...` and
`tests/...` citations, verifies that each target exists and that the cited line
range is still in bounds, then optionally verifies a nearby backtick identifier.
Pass `--strict` to fail on any drift.
"""

import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_GLOB = "docs/STORAGE-UPGRADE-PHASE-*.md"
EXTRA_FILES = ["cck.md"]

# Also match bare citations without a preceding identifier on the same pass,
# so we can count all citations even when no identifier is nearby.
BARE_CITE_RE = re.compile(
    r"`?(src/[^\s`:,\"')]+|tests/[^\s`:,\"')]+):(\d+(?:-\d+)?)`?",
)


def parse_range(spec: str) -> Tuple[int, int]:
    if "-" in spec:
        lo, hi = spec.split("-", 1)
        return int(lo), int(hi)
    n = int(spec)
    return n, n


def lines_of(path: Path) -> Optional[List[str]]:
    try:
        return path.read_text(errors="replace").splitlines()
    except OSError:
        return None


def identifier_in_range(file_lines: List[str], lo: int, hi: int, ident: str) -> bool:
    bare = ident.split("::")[-1].rstrip("()")
    full = ident.rstrip("()")
    for line in file_lines[lo - 1 : hi]:
        if bare in line or full in line:
            return True
    return False


def collect_docs(repo: Path) -> List[Path]:
    paths = sorted(repo.glob(DOCS_GLOB))
    for name in EXTRA_FILES:
        path = repo / name
        if path.exists():
            paths.append(path)
    return paths


def check_doc(doc_path: Path, repo: Path, strict: bool) -> Tuple[int, int]:
    text = doc_path.read_text(errors="replace")
    total = 0
    drifts = 0

    for match in BARE_CITE_RE.finditer(text):
        total += 1
        cite_file_str = match.group(1)
        line_spec = match.group(2)
        cite_file = repo / cite_file_str

        if not cite_file.exists():
            print(
                f"  WARN  missing-file  {doc_path.name}: "
                f"{cite_file_str}:{line_spec} -- file not found"
            )
            drifts += 1
            continue

        file_lines = lines_of(cite_file)
        if file_lines is None:
            continue
        lo, hi = parse_range(line_spec)
        total_lines = len(file_lines)

        if lo > total_lines:
            print(
                f"  WARN  range-beyond-eof  {doc_path.name}: "
                f"{cite_file_str}:{line_spec} "
                f"(file has {total_lines} lines)"
            )
            drifts += 1
            continue

        hi = min(hi, total_lines)

        # Find the identifier that precedes this citation in the source text.
        pos = match.start()
        preceding = text[max(0, pos - 200) : pos]
        id_match = re.search(r"`([A-Za-z_:][A-Za-z0-9_:<>()]*)`\s*$", preceding)
        if id_match is None:
            continue

        ident = id_match.group(1)
        if not identifier_in_range(file_lines, lo, hi, ident):
            print(
                f"  WARN  symbol-drift  {doc_path.name}: "
                f"`{ident}` cited at {cite_file_str}:{line_spec} "
                f"-- identifier not found in that range"
            )
            drifts += 1

    return total, drifts


def main() -> int:
    strict = "--strict" in sys.argv
    repo = REPO_ROOT
    docs = collect_docs(repo)

    if not docs:
        print("No phase docs found.")
        return 0

    grand_total = 0
    grand_drifts = 0

    for doc in docs:
        total, drifts = check_doc(doc, repo, strict)
        grand_total += total
        grand_drifts += drifts

    print(
        f"\nCitation check complete: "
        f"{grand_total} citations scanned, "
        f"{grand_drifts} drift(s) detected."
    )

    if strict and grand_drifts > 0:
        print("Exiting non-zero (--strict mode).")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
