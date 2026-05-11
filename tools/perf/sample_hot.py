#!/usr/bin/env python3
"""Extract hot self-time frames from a macOS `sample` call graph.

macOS `sample` format:
    `    + NNN funcname (in mod) + offset [addr]` — depth 1 (1 space after +)
    `    +   NNN funcname ...` — depth 2 (3 spaces)
    `    +     NNN funcname ...` — depth 3 (5 spaces)
    `    +         ! NNN funcname ...` — leaf at this depth (! = end of stack)
    `    +         ! : NNN funcname ...` — additional leaf siblings under same parent

A frame is a self-time frame if it's marked with `!`, OR if it has no children
(next line at the same or shallower depth). The count on a `!` line IS the
self-time for that frame.

Usage: sample_hot.py <pr1-sample.txt> [N]
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

# Match: `    + NNN ...` or `    +   NNN ...` or `    +         ! NNN ...`
# The "spaces between + and digit" encodes depth (1 space = depth 1, 3 spaces = 2, etc.)
LINE_RE = re.compile(r"^(\s*)\+(\s+)(?:!\s*)?(?::\s*)?(\d+)\s+(.+?)\s*$")

SYNC_HINTS = (
    "lock",
    "Mutex",
    "RwLock",
    "parking_lot",
    "park",
    "futex",
    "pthread_mutex",
    "psynch",
    "ulock_wait",
    "Condvar",
    "spin_loop",
)


def is_leaf_marker(line: str) -> bool:
    """Lines containing `+         ! NNN ...` are leaf frames."""
    return "+ " in line and "! " in line and re.search(r"\+\s+!\s+", line) is not None


def short_name(label: str) -> str:
    s = label.split("  (in ")[0]
    s = s.split("  ;")[0]
    s = re.sub(r"\s+\+\s+\d+\s*\[?[0x]?[0-9a-f]*\]?$", "", s)
    s = re.sub(r"\s+\d+\s*\[?[0x]?[0-9a-f]*\]?$", "", s)
    s = re.sub(r"\s+\[?[0x]?[0-9a-f]*\]?\s*\S*\s*$", "", s)
    return s.strip()


def is_sync(label: str) -> bool:
    return any(h in label for h in SYNC_HINTS)


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    path = Path(sys.argv[1])
    n = int(sys.argv[2]) if len(sys.argv) > 2 else 30

    self_time: Counter[str] = Counter()
    in_call_graph = False
    with path.open() as fp:
        for line in fp:
            if not in_call_graph:
                if line.startswith("Call graph:"):
                    in_call_graph = True
                continue
            if line.startswith("Total number") or line.startswith("Binary Images"):
                in_call_graph = False
                continue
            # We only care about leaf-marked lines (`!` after the `+`).
            if not is_leaf_marker(line):
                continue
            m = LINE_RE.match(line)
            if not m:
                continue
            _indent, _between, count_str, label = m.groups()
            self_time[short_name(label)] += int(count_str)

    total = sum(self_time.values())
    if total == 0:
        print("(no leaf samples found)")
        return

    print(f"# macOS sample — {total} leaf self-time samples\n")
    print(f"## Top {n} self-time frames (overall)\n")
    print(f"{'pct':>6}  {'count':>8}  {'sync?':>6}  symbol")
    print("-" * 100)
    for sym, c in self_time.most_common(n):
        pct = 100.0 * c / total
        sync = "[SYNC]" if is_sync(sym) else ""
        print(f"{pct:5.2f}%  {c:8d}  {sync:>6}  {sym}")

    sync_total = sum(c for s, c in self_time.items() if is_sync(s))
    mqlite_total = sum(c for s, c in self_time.items() if "mqlite" in s)
    print(
        f"\n## Sync primitives: {sync_total} samples "
        f"({100.0 * sync_total / total:.2f}% of leaf samples)"
    )
    print(
        f"## mqlite-prefixed: {mqlite_total} samples "
        f"({100.0 * mqlite_total / total:.2f}% of leaf samples)"
    )

    print("\n## Top 25 mqlite leaf frames\n")
    print(f"{'pct':>6}  {'count':>8}  symbol")
    print("-" * 100)
    n_shown = 0
    for sym, c in self_time.most_common():
        if "mqlite" not in sym:
            continue
        pct = 100.0 * c / total
        print(f"{pct:5.2f}%  {c:8d}  {sym}")
        n_shown += 1
        if n_shown >= 25:
            break


if __name__ == "__main__":
    main()
