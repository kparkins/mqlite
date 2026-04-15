# Phase 1 Reconciliation Plan: Consolidated Review

**Synthesizer:** mqlite polecat furiosa  
**Date:** 2026-04-14  
**Issue:** hq-pny6  
**Plan:** `docs/specs/phase1-reconciliation.md`  

---

## Overall Verdict: GO WITH FIXES

The plan is architecturally sound. The root cause diagnosis is correct — the
disconnected storage modules and BSON-blob engine are the problem, and the
phased integration plan is the right solution. The dependency graph is largely
correct (two errors require fixing before dispatch). The five review legs
returned 4x PASS WITH NOTES and 1x FAIL (scope), but the mayor has issued an
explicit override on all three scope-creep objections. No leg returned a
blocking NO-GO.

**Work may proceed after must-fix items below are applied to the plan.**

---

## Mayor Override (Scope Review)

The scope reviewer (slit, hq-mhyt) issued a FAIL verdict on three items:

1. **Removal of `open_in_memory()`** — contradicts PRD R8, DoD item 9, Story 2  
2. **Addition of `Client` type** — not in PRD, changes public API shape  
3. **Multi-database namespace support** — not in PRD, scoped addition  

**Mayor override (hq-pny6 notes):** These are INTENTIONAL design decisions
that supersede the original PRD:

- **(1) No in-memory mode** — one code path, tests use tempfiles. Simplicity
  over the special-case `PageManagerMode::InMemory` branch.
- **(2) Client type is required** to match MongoDB driver hierarchy
  (`Client→Database→Collection`). The PRD was wrong about the object model.
- **(3) Multi-database namespacing is required** for wire protocol correctness
  (`$db` field). The single-db constraint was an implementation limitation,
  not a deliberate product choice.

The scope-creep FAIL is **treated as GO** for all three items.

---

## Leg Verdicts

| Review | Reviewer | Bead | Verdict | Summary |
|--------|----------|------|---------|---------|
| Completeness | furiosa | hq-t9rj | PASS WITH NOTES | 4 must-fix (backup(), compat suite, in-memory [overridden], docs bead) |
| Sequencing | nux | hq-3t20 | PASS WITH NOTES | 2 dependency graph errors (3.1 and 4.1 misplaced) |
| Risk | rictus | hq-dia9 | PASS WITH NOTES | 5 critical integration interfaces underspecified |
| Scope Discipline | slit | hq-mhyt | ~~FAIL~~ → **GO** | Mayor override on all 3 critical items |
| Testability | furiosa | hq-9i91 | PASS WITH NOTES | 3.5 wire CI plan absent; 3.1 dep error; 3.2 sync primitive missing |

---

## Consolidated Must-Fix Items

These must be applied to `docs/specs/phase1-reconciliation.md` before Phase 0
beads are dispatched.

### MF-1: Fix dependency graph — 3.1 depends on 1.4, not 1.2

**Source:** Sequencing review (Issue 1), Testability review (3.1 section)

The persistence test (3.1) exercises 3 databases (requires catalog/1.3) and
creates indexes (requires secondary indexes/1.4). The current graph shows
`1.2 → 3.1`, which is wrong — 3.1 cannot be implemented until 1.4 is done.

**Fix:** Change `3.1`'s parent from `1.2` to `1.4`.

### MF-2: Fix dependency graph — 4.1 depends on 1.4, not 0.2

**Source:** Sequencing review (Issue 2)

Dead code removal (4.1) deletes the BSON-blob engine and `Vec<Document>`.
This cannot happen until the replacement storage stack (1.4) is complete and
functional. The current graph places 4.1 under 0.2, which would allow dead
code removal before the replacement engine exists.

**Fix:** Change `4.1`'s parent from `0.2` to `1.4`.

### MF-3: Add integration design requirement before Phase 1 coding

**Source:** Risk review (RISK-01, RISK-02, RISK-03)

Three interface mismatches between standalone modules are underspecified and
will block code mid-stream:

- **RISK-01:** `BTreePageStore` ↔ `BufferPool` are different shapes — need a
  `BufferPoolPageStore` adapter struct defined before 1.2 starts.
- **RISK-02:** `WalManager` is not a `PageIo` implementation — the dirty-page
  routing through WAL must be specified before 1.5 starts.
- **RISK-03:** `PageAllocator<'a>` borrow model is incompatible with concurrent
  use — needs lifetime redesign before 1.1 integration begins.

**Fix:** Add a note to Phase 1 (before 1.1) requiring an interface design
document covering `BufferPoolPageStore`, the WAL write transaction protocol,
and the `AllocatorHandle` lifetime model before implementation begins.

### MF-4: Phase 3.5 requires a CI sub-plan

**Source:** Testability review (3.5 section), Completeness review (C-07)

Phase 3.5 states "CI-runnable" as the goal but "currently requires manual
execution" as the status. No concrete plan exists for: pymongo suite location,
pip install step in CI, port management, or server teardown on failure.

**Fix:** Expand 3.5 to include a concrete CI sub-plan, or explicitly mark it
as "manual only" and create a separate bead for CI automation.

### MF-5: Phase 3.2 needs a synchronization primitive

**Source:** Testability review (3.2 section)

The crash recovery test forks a child, inserts data in FullSync mode, and kills
the child. Without a synchronization mechanism between parent and child (e.g., a
pipe write after fsync completes), the parent may kill the child before any fsync
occurs — verifying nothing.

**Fix:** Add a requirement to 3.2 specifying the synchronization mechanism: the
child must signal the parent after at least one commit is fsynced before the
parent sends SIGKILL.

### MF-6: Add documentation bead to Phase 4

**Source:** Completeness review (C-09)

PRD DoD item 10 requires: API reference, migration guide from MongoDB driver,
known limitations, security advisory for wire protocol. No plan phase covers
this. 16 beads cover all implementation work but documentation is absent.

**Fix:** Add a Phase 4.3 bead for documentation.

### MF-7: Add `backup(dest)` bead

**Source:** Completeness review (C-01)

PRD G1 acceptance criteria includes `Database::backup(dest)` producing a
consistent hot copy. No plan phase covers this.

**Fix:** Add a Phase 4.4 bead for `backup(dest)` implementation.

---

## Should-Fix Items

These are not blocking but should be addressed:

| ID | Source | Description |
|----|--------|-------------|
| SF-1 | Completeness C-02 | Native API compatibility test suite vs MongoDB 8.0 (PRD DoD item 3, R12) — no bead |
| SF-2 | Completeness C-03 | `insert_many` ordered/unordered semantics (PRD R10) not called out |
| SF-3 | Completeness C-04 | `find_one_and_replace`, `find_one_and_delete` not called out |
| SF-4 | Completeness C-05 | Projection support (field inclusion/exclusion) not mentioned |
| SF-5 | Completeness C-08 | Benchmark regression detection in CI not planned |
| SF-6 | Completeness C-10 | `stats()`, `explain()` methods (PRD R11) missing |
| SF-7 | Completeness C-11 | File format magic bytes/versioning not explicit in 1.1 |
| SF-8 | Risk RISK-04 | `StorageEngine` trait has no transaction boundary — `find()` returning `Vec<Document>` precludes snapshot cursors |
| SF-9 | Risk RISK-05 | SWMR snapshot isolation: how reader `pin()` routes through WAL is underspecified |
| SF-10 | Risk RISK-08 | Performance targets need hardware assumptions (SSD vs HDD) and contingency |
| SF-11 | Risk RISK-11 | Phase 1 estimate is optimistic (6 beads); revised estimate 8-10 with integration adapters |
| SF-12 | Testability | Add phase gate tests for 1.1, 1.2, 1.3 (close/reopen at each layer) |
| SF-13 | Testability | Wrap existing Vec engine in `StorageEngine` trait as Phase 0 deliverable |
| SF-14 | Testability | Clarify 3.4 benchmarks: CI gates vs design goals |
| SF-15 | Testability | 3.3 snapshot isolation assertions underspecified; WriterBusy scenario missing |
| SF-16 | Sequencing | 0.1 ↔ 0.2 are co-dependent; consider co-developing or reversing arrow |
| SF-17 | Risk RISK-09 | 2.1 wire refactor scope is larger than one bullet; audit command handlers in `wire/server.rs` |
| SF-18 | Risk RISK-10 | Catalog namespace key format change needed for multi-db; secondary index lifecycle undefined |

---

## What the Plan Gets Right

- **Correct diagnosis:** BSON-blob `Vec<Document>` engine is the root cause.
- **Correct architecture:** `Client → Database → Collection<T>` with `StorageEngine` trait.
- **Correct storage stack ordering:** buffer pool → B+ tree → catalog → indexes.
- **Real standalone modules:** B+ tree, WAL, buffer pool, catalog each have tests and correct algorithms.
- **Right approach:** Clean integration rewrite, not patching the broken engine incrementally.
- **No async runtime:** Sync-first API matches PRD G3.
- **Parallelism opportunities:** 1.5 parallel with 1.3/1.4; 2.1 parallel with Phase 1.

---

## Corrected Dependency Graph

```
0.1 (Client/Database/Collection types)
 |
 +-> 0.2 (StorageEngine trait)
      |
      +-> 1.1 (buffer pool + allocator + file I/O)
      |    |
      |    +-> 1.2 (B+ tree document storage)
      |    |    |
      |    |    +-> 1.3 (catalog)
      |    |         |
      |    |         +-> 1.4 (secondary indexes)
      |    |              |
      |    |              +-> 3.1 (persistence test)   ← FIXED: was under 1.2
      |    |              |
      |    |              +-> 3.4 (benchmarks)
      |    |              |
      |    |              +-> 4.1 (remove dead code)   ← FIXED: was under 0.2
      |    |                   |
      |    |                   +-> 4.2 (warnings cleanup)
      |    |                   |
      |    |                   +-> 4.3 (documentation)  ← NEW
      |    |                   |
      |    |                   +-> 4.4 (backup(dest))   ← NEW
      |    |
      |    +-> 1.5 (WAL integration)     ← parallel with 1.2-1.4
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
```
