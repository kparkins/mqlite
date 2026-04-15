# Sequencing Review: phase1-reconciliation.md

**Reviewer:** nux  
**Date:** 2026-04-14  
**Verdict:** PASS WITH NOTES

---

## Summary

The core storage layer ordering is architecturally sound. No circular dependencies
exist. Phase 0 correctly precedes Phase 1. Parallelization opportunities are
correctly identified. However, two dependency errors in the graph would cause
failures if used as-is to dispatch work: `3.1` is under-specified and `4.1` is
misplaced.

---

## Dependency Graph Analysis

### Circular Dependencies

**None found.** The graph is a clean DAG:

```
0.1 → 0.2 → 1.1 → 1.2 → 1.3 → 1.4
                 ↘ 1.5 → 1.6
             ↘ 2.1
```

### Storage Layer Ordering

**Correct.** Buffer pool → B+ tree → Catalog → Secondary indexes.

| Layer | Rationale |
|-------|-----------|
| Buffer pool (1.1) first | All page I/O flows through it; every other layer depends on it |
| B+ tree (1.2) second | Uses buffer pool for page ops; documents are B+ tree leaves |
| Catalog (1.3) third | IS a B+ tree (at fixed root); needs 1.2 wired before it can be built |
| Secondary indexes (1.4) fourth | Uses catalog to track metadata; uses B+ tree for index data |

WAL (1.5) branching from buffer pool (1.1) is correct: WAL integration is a
flush-path concern inside the buffer pool, independent of what's stored in pages.

### Phase 0 → Phase 1 Ordering

**Correct.** 0.1 (Client/Database/Collection object model) → 0.2 (StorageEngine
trait) → Phase 1 implementation. Phase 0 establishes the stable contracts before
any storage wiring begins.

### Parallelization Opportunities

The plan correctly identifies:
- 1.5 (WAL) can start in parallel with 1.3/1.4 — both need 1.1; 1.5 modifies
  buffer pool internals while 1.3/1.4 build on the B+ tree API. No file
  conflicts if discipline is maintained.
- 2.1 (wire multi-db) can proceed in parallel with all of Phase 1 — depends
  only on the Client type (Phase 0).

One missed parallel opportunity: 3.4 (benchmarks, after 1.4) can run in parallel
with 1.5, 1.6, and 2.1. Minor; not a sequencing error.

---

## Issues Found

### Issue 1 — SIGNIFICANT: 3.1 dependency is under-specified

**Graph shows:** `1.2 → 3.1`  
**Problem:** 3.1's own description reads:

> `Client::open(path)` → insert 10k docs **across 3 databases** → **create indexes**
> → close → reopen → verify all docs, **indexes**, query results.

"3 databases" requires catalog (1.3). "Create indexes" requires secondary indexes
(1.4). The test as written cannot be implemented after only 1.2.

**Fix:** `3.1` should depend on `1.4` (not `1.2`). Since 1.4 transitively requires
1.3 and 1.2, this captures all needed dependencies.

```
# Current (wrong)
1.2 → 3.1

# Corrected
1.4 → 3.1
```

If 3.1 is intentionally scoped to a simpler "B+ tree persistence only" test
(no multi-db, no indexes), the test description must be revised to match. As
written, the description and the dependency are inconsistent.

### Issue 2 — SIGNIFICANT: 4.1 is misplaced in the graph

**Graph shows:** 4.1 as a child of `0.2`, implying it can start after `0.2` is
done. The inline comment says `-- after 1.4`. These contradict each other.

**Problem:** 4.1 removes `EngineState`, `Vec<Document>`, BSON blob persistence,
`open_in_memory()`, and all in-memory special cases. None of this can be deleted
until the replacement (the complete paged storage stack through 1.4) is done and
functional. Deleting it after 0.2 would remove the only working engine.

**Fix:** Move 4.1 to depend on `1.4`:

```
# Current (wrong) — graph places 4.1 under 0.2
0.2 → 4.1

# Corrected
1.4 → 4.1
```

The inline comment `-- after 1.4` is correct; the graph placement is wrong.

### Issue 3 — MINOR: 0.1 ↔ 0.2 dependency direction

**Graph shows:** `0.1 → 0.2` (implement Client/Database/Collection types first,
then define StorageEngine trait).

**Observation:** `ClientInner` holds `Box<dyn StorageEngine>`. The Client types
in 0.1 compile only after the `StorageEngine` trait in 0.2 is declared. Strictly,
0.2 should precede 0.1 (or they must be co-developed).

The plan takes an API-first stance (sketch the public API, then extract the
internal contract), which is a valid design approach. In practice, both 0.1 and
0.2 are one concurrent work unit — the trait and the types reference each other.

**Recommendation:** Either reverse the arrow (`0.2 → 0.1`) or annotate these as
co-developed. If dispatched as separate beads, the 0.1 bead should declare a
placeholder `StorageEngine` trait to unblock compilation.

### Issue 4 — MINOR: 2.1 dependency precision

**Graph shows:** `0.2 → 2.1`  
**Observation:** 2.1's core requirement is "wire server takes `&Client` not
`&Database`", which is a dependency on the `Client` type from `0.1`, not the
`StorageEngine` trait from `0.2`.

The dependency is transitive-correct (0.1 precedes 0.2, so 0.2→2.1 implies
0.1 is done), but the more precise edge would be `0.1 → 2.1`.

No correctness risk from the current graph; just a precision note.

---

## Sequencing by Phase

| Step | Can start when... | Graph says | Correct? |
|------|-------------------|------------|----------|
| 0.1 | Immediately | (root) | ✓ |
| 0.2 | After 0.1 | After 0.1 | ✓ (minor note: arguably should be first) |
| 1.1 | After 0.2 | After 0.2 | ✓ |
| 1.2 | After 1.1 | After 1.1 | ✓ |
| 1.3 | After 1.2 | After 1.2 | ✓ |
| 1.4 | After 1.3 | After 1.3 | ✓ |
| 1.5 | After 1.1 | After 1.1 | ✓ (parallel with 1.2–1.4) |
| 1.6 | After 1.5 | After 1.5 | ✓ |
| 2.1 | After 0.1/0.2 | After 0.2 | ✓ (minor precision note) |
| 3.1 | After 1.4 | **After 1.2** | ❌ under-specified |
| 3.2 | After 1.5 | After 1.5 | ✓ |
| 3.3 | After 1.6 | After 1.6 | ✓ |
| 3.4 | After 1.4 | (text only) | ✓ (not in graph, minor) |
| 3.5 | After 2.1 | After 2.1 | ✓ |
| 4.1 | After 1.4 | **After 0.2** | ❌ misplaced |
| 4.2 | After 4.1 | After 4.1 | ✓ |

---

## Corrected Dependency Graph

```
0.1 (Client/Database/Collection types)
 |
 +-> 0.2 (StorageEngine trait)          ← note: co-develop or reverse arrow
      |
      +-> 1.1 (buffer pool + allocator + file I/O)
      |    |
      |    +-> 1.2 (B+ tree document storage)
      |    |    |
      |    |    +-> 1.3 (catalog)
      |    |         |
      |    |         +-> 1.4 (secondary indexes)
      |    |              |
      |    |              +-> 3.1 (persistence test)   ← moved from 1.2
      |    |              |
      |    |              +-> 4.1 (remove dead code)   ← moved from 0.2
      |    |                   |
      |    |                   +-> 4.2 (warnings cleanup)
      |    |
      |    +-> 1.5 (WAL integration)      ← parallel with 1.2–1.4
      |         |
      |         +-> 1.6 (SWMR)
      |         |    |
      |         |    +-> 3.3 (SWMR test)
      |         |
      |         +-> 3.2 (crash recovery test)
      |
      +-> 2.1 (wire protocol multi-db)   ← parallel with Phase 1
           |
           +-> 3.5 (wire integration test)

3.4 (benchmarks) ← after 1.4 (parallel with 1.5, 1.6, 2.1)
```

---

## Verdict: PASS WITH NOTES

The plan is architecturally sound. The storage layer ordering is correct. No
circular dependencies. Phase 0 → Phase 1 sequencing is correct. Parallelization
opportunities are well-identified.

Two dependency errors (3.1 under-specified, 4.1 misplaced) need correction before
dispatching beads, as a polecat working 3.1 after 1.2 would discover mid-stream
that catalog and indexes are required. The 4.1 misplacement would allow dead code
removal before the replacement engine exists.

**Required fixes before dispatch:**
1. Change 3.1's parent dependency from `1.2` to `1.4`
2. Change 4.1's parent dependency from `0.2` to `1.4`
