#!/usr/bin/env python3
"""Extract hot self-time frames from a macOS `sample` call graph.

macOS `sample` format (call-graph mode):
    `    +         ! NNN funcname (in mod) + offset [addr]`
    `    +         ! : NNN funcname ...`           (sibling leaf)
    `    +         ! : | + ! NNN funcname ...`     (deeply nested leaf)
    `    +         ! : | + NNN funcname ...`       (intermediate frame, NOT a leaf)

A line is a self-time leaf iff a `!` appears immediately before the
count digits (i.e. the rightmost match of `! <digits> <symbol>`). The
count on that `!` row is treated as the self-time at that frame.
Intermediate inclusive-count rows (e.g. `+ : | + NNN funcname`) lack
the `!` directly before the count and are skipped.

Usage: sample_hot.py <pr1-sample.txt> [N]
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

# Match the rightmost `!  N  symbol` segment in a line. The trailing
# context terminates the symbol at the binary path (`(in <bin>)`) or
# end-of-line so `findall` returns one match per leaf row.
LEAF_RE = re.compile(r"!\s+(\d+)\s+(\S.+?)(?:\s+\(in\s|\s*$)")

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
            # We only care about leaf-marked lines: ones with a `!` directly
            # before the count digits. Take the rightmost match in case the
            # line has multiple `!` segments along the structural prefix.
            matches = LEAF_RE.findall(line)
            if not matches:
                continue
            count_str, label = matches[-1]
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
