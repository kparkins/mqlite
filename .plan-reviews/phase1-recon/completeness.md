# Plan Review: Completeness

**Plan**: `docs/specs/phase1-reconciliation.md`
**PRD**: `.prd-reviews/mqlite-embedded-mongodb/prd-draft.md`
**Date**: 2026-04-14
**Reviewer**: polecat furiosa
**Issue**: hq-t9rj

---

## Verdict: PASS WITH NOTES

The plan is architecturally sound. The core storage stack (Phases 0–1) comprehensively addresses the root cause of mqlite's broken state: the disconnected B+ tree, WAL, buffer pool, and catalog modules are now properly ordered into a sequential integration plan, and the object model mismatch (missing `Client` type, single-database constraint) is fixed in Phase 0. The dependency graph is correct and reflects real build order constraints.

**However, four PRD requirements have no corresponding plan step.** Two of these (R8 in-memory mode, G1 `backup()`) represent direct contradictions or omissions that would cause a technical DoD failure if left unaddressed. The remaining two (documentation, native API compatibility test suite) are explicit DoD checklist items in the PRD.

---

## PRD Goals vs. Plan Coverage

### G1 — Single-file storage

| PRD Acceptance Criterion | Plan Coverage | Status |
|--------------------------|---------------|--------|
| Only `.mqlite` file after clean close | Phase 1.5 (WAL integration: `close()` checkpoints and deletes WAL+SHM) | ✓ Covered |
| `checkpoint()` forces WAL merge | Phase 1.5 (`checkpoint()` replays WAL to main file, truncates WAL) | ✓ Covered |
| `backup(dest)` produces consistent hot copy | **Not mentioned in any phase** | ✗ Missing |
| Copying cleanly-closed file produces valid DB | Implicit in 1.5 (clean close = single file) | ✓ Covered |

**Gap C-01 (must-fix):** `Database::backup(dest)` is an explicit PRD G1 acceptance criterion ("produces a consistent copy even while the database is open"). No plan phase covers implementing a hot backup method. Phase 1.5 mentions `checkpoint()` but backup is a different operation requiring snapshot semantics.

---

### G2 — MongoDB API compatibility

| PRD Acceptance Criterion | Plan Coverage | Status |
|--------------------------|---------------|--------|
| In-scope query operators correct vs MongoDB 8.0 | Phase 1.2 (eval_filter in COLLSCAN), 1.4 (IndexScan B+ tree rewrite) — filter.rs reused | ✓ Covered (scoped) |
| In-scope update operators correct | update_operators.rs reused; wired through StorageEngine.update() in 0.2 | ✓ Covered |
| Unsupported operators return correct error code | error.rs marked reusable; no bead explicitly confirms codes still map | ⚠ Assumed |
| Compatibility test suite vs MongoDB 8.0 | **Not in any Phase 3 bead** | ✗ Missing |
| BSON comparison ordering in indexes | key_encoding.rs marked reusable; used in 1.2/1.4 | ✓ Covered |
| `find_one_and_update`, `find_one_and_replace`, `find_one_and_delete` | PRD DoD item 2 lists all three; not mentioned in plan | ⚠ Uncalled out |
| Projection support | PRD G2 lists field inclusion/exclusion, `_id: 0`; not mentioned in any phase | ⚠ Uncalled out |
| `insert_many` ordered/unordered semantics (R10) | **Not mentioned in any phase** | ✗ Missing |

**Gap C-02 (must-fix):** No plan phase creates a compatibility test suite that runs the same operations against MongoDB 8.0 and mqlite. PRD DoD item 3 and R12 both require this. Phase 3.5 covers the pymongo wire protocol test, but there is no equivalent for the native API layer.

**Gap C-03 (should-fix):** `insert_many` ordered/unordered semantics (PRD R10) has no plan coverage. This is a non-trivial behavior distinction (ordered: stop at first error with partial inserted_ids; unordered: attempt all, collect all errors).

**Gap C-04 (should-fix):** `find_one_and_replace`, `find_one_and_delete` are in PRD DoD item 2 but not called out in any plan phase. They are presumably inside Phase 0.2 (StorageEngine trait) and Phase 1 wiring, but the scope isn't stated.

**Gap C-05 (should-fix):** Projection support (field inclusion/exclusion, `_id: 0`) is in PRD G2 in-scope list but absent from all plan steps. No bead covers implementing or testing it.

---

### G3 — Zero-server operation

| PRD Acceptance Criterion | Plan Coverage | Status |
|--------------------------|---------------|--------|
| Compiles with no async runtime | Phase 0.1 (sync-first API) | ✓ Covered |
| `Database::open()` and CRUD without background threads | Phase 0.1, 0.2 | ✓ Covered |
| `Database` is Send + Sync + Clone | Phase 0.1 (`Arc<ClientInner>`) | ✓ Covered |
| `Database::open_in_memory()` (PRD R8) | **Phase 4.1 explicitly deletes it** | ✗ Conflict |

**Gap C-06 (must-fix — critical conflict):** The plan's success criteria item 13 states "No in-memory mode — one code path, always file-backed, tests use tempfiles." Phase 4.1 explicitly deletes `open_in_memory()`. But PRD R8 is a committed requirement: "`Database::open_in_memory()` creates a database with no file backing." PRD Story 2 depends on it ("swap the connection to an in-memory mqlite instance"). PRD DoD item 9 requires it ("In-memory mode: `Database::open_in_memory()` works with the same API").

This is a direct plan/PRD contradiction. The plan must either:
- Restore `open_in_memory()` as a real phase (backed by `PageManagerMode::InMemory` per the PRD's Rough Approach), or
- Explicitly document that R8 is being descoped and obtain sign-off before implementation begins.

Without resolution, Phase 4.1 will break a committed PRD requirement.

---

### G4 — Crash recovery

| PRD Acceptance Criterion | Plan Coverage | Status |
|--------------------------|---------------|--------|
| kill -9 → reopen → committed data present (FullSync) | Phase 1.5 (WAL), 3.2 (crash recovery test) | ✓ Covered |
| Interval mode 100ms loss window | Phase 1.5 (DurabilityMode::Interval) | ✓ Covered |
| No partial documents after crash | Phase 1.5 (WAL frames with CRC32C) | ✓ Covered |
| CRC32C checksums detect torn pages | Phase 1.5 mentions WAL integration; CRC32C not called out explicitly | ⚠ Assumed |
| Automatic WAL replay on recovery | Phase 1.5 (`open()` replays WAL if present) | ✓ Covered |

G4 is well-covered. Minor note: CRC32C checksums are a PRD acceptance criterion but are not called out as an explicit deliverable in 1.5 or anywhere in Phase 3.

---

### G5 — Concurrent read access

| PRD Acceptance Criterion | Plan Coverage | Status |
|--------------------------|---------------|--------|
| 10 readers + 1 writer, no errors | Phase 1.6 (SWMR), 3.3 (SWMR test) | ✓ Covered |
| Two OS processes, same file, no corruption | Phase 1.6 (fcntl file locking) | ✓ Covered |
| Second writer → WriterBusy on timeout | Phase 1.6 (busy_timeout) | ✓ Covered |
| Snapshot isolation for cursors | Phase 1.6 (WAL position tracking) | ✓ Covered |
| Up to 64 concurrent readers | Phase 1.6 (SHM tracks reader positions) | ✓ Covered |

G5 is comprehensively covered by Phase 1.6 and 3.3.

---

### G6 — Wire protocol shim

| PRD Acceptance Criterion | Plan Coverage | Status |
|--------------------------|---------------|--------|
| 18 Phase 1 commands work | Existing server.rs has 18 commands; Phase 2.1 fixes multi-db routing | ✓ Covered |
| `mongosh` connects, show dbs, CRUD | Phase 2.1 (multi-db routing), 3.5 (wire integration test) | ✓ Covered |
| pymongo test suite passes | Phase 3.5 (CI-runnable pymongo test) | ✓ Covered |
| listDatabases returns all databases | Phase 2.1 explicitly covers this | ✓ Covered |
| `use mydb` in mongosh works | Phase 2.1 | ✓ Covered |
| Localhost-only binding by default | Existing behavior preserved; no phase changes this | ✓ Covered |

Phase 2.1 is appropriately scoped for the multi-db wire fix. 3.5 covers integration validation.

**Gap C-07 (should-fix):** Phase 3.5 says the pymongo test suite "currently requires manual execution" and needs to become CI-runnable. The plan states the goal ("CI-runnable") but doesn't describe what infrastructure change enables this. A sentence clarifying what needs to change (e.g., `WireProtocol::bind(&client, ...)` replaces `WireProtocol::bind(&db, ...)`) would strengthen the bead's exit criteria.

---

### G7 — Reasonable performance

| PRD Acceptance Criterion | Plan Coverage | Status |
|--------------------------|---------------|--------|
| Benchmark suite covering all G7 operations | Phase 3.4 (rewrite benches/core.rs) | ✓ Covered |
| Point lookup < 10 us (cached) | Phase 3.4 targets table matches PRD | ✓ Covered |
| Bulk insert 10K docs < 500 ms | Phase 3.4 targets table matches PRD | ✓ Covered |
| Regression detection in CI | Not mentioned in plan | ⚠ Absent |

**Gap C-08 (should-fix):** PRD G7 acceptance criteria requires "No operation regresses more than 2x between releases (regression detection in CI)." Phase 3.4 adds the benchmark suite but doesn't mention CI integration for regression detection.

---

## Missing PRD Requirements (No Plan Phase)

### Documentation (PRD DoD item 10) — must-fix

PRD Phase 1 DoD item 10: "API reference, migration guide from MongoDB driver, known limitations, security advisory for wire protocol."

**Gap C-09 (must-fix):** There is no documentation bead in any phase. The 16-bead plan has no Step for producing the required Tier 1 documentation. This is not a "nice to have" — it is a named DoD checklist item. A Phase 4.3 or Phase 3.6 bead should be added.

---

### Observability (PRD R11) — should-fix

PRD R11 requires: `Database::stats()`, `Collection::stats()`, `Cursor::explain()`.

**Gap C-10 (should-fix):** No plan phase covers implementing observability methods. These are in the public API (referenced in the design docs) but absent from all 16 plan steps. They could be bundled into Phase 0.1 (object model) or added as a separate Phase 4.3.

---

### File Format Versioning (PRD R9) — should-fix

PRD R9: "The `.mqlite` file starts with magic bytes `MQLT` followed by a uint32 format version."

**Gap C-11 (should-fix):** Phase 1.1 (buffer pool + page allocator + file I/O) does not explicitly call out file format magic bytes or version field. This is a small addition to 1.1 scope but matters for forward compatibility and the PRD's explicit requirement.

---

### Error Model Validation (PRD R1) — observation

`error.rs` is correctly marked as reusable. However, after the engine rewrite, the new `StorageEngine::insert()` path will need to surface the correct error codes (11000 for dup key, 121 for validation failure, etc.) through the new trait boundary. No phase explicitly lists "verify MongoDB error codes work through PagedEngine → StorageEngine → Collection<T>."

This is likely covered implicitly by 3.1 (end-to-end persistence test uses real docs that can trigger dup key errors), but the scope isn't stated. Adding a sentence to 3.1 noting error code verification would close this gap.

---

## Bead Count Verification

The plan states 16 beads. Counting explicit numbered steps:

| Phase | Steps | Beads |
|-------|-------|-------|
| 0 | 0.1, 0.2 | 2 |
| 1 | 1.1, 1.2, 1.3, 1.4, 1.5, 1.6 | 6 |
| 2 | 2.1 | 1 |
| 3 | 3.1, 3.2, 3.3, 3.4, 3.5 | 5 |
| 4 | 4.1, 4.2 | 2 |
| **Total** | | **16** |

Count is correct. The missing items (backup, docs, observability) are additional beads not yet in the plan, not miscounts.

---

## Summary of Gaps

### Must-Fix

| ID | Description | PRD Reference | Affected Plan Phase |
|----|-------------|---------------|---------------------|
| C-01 | `backup(dest)` method missing | G1 acceptance criteria | No phase covers this |
| C-02 | No native API compatibility test suite (vs MongoDB 8.0) | DoD item 3, R12 | Phase 3 has no such bead |
| C-06 | In-memory mode: plan deletes it, PRD requires it (R8, Story 2, DoD item 9) | R8, DoD #9 | Phase 4.1 creates direct conflict |
| C-09 | No documentation bead | DoD item 10 | Phases 0–4 have no doc step |

### Should-Fix

| ID | Description | PRD Reference | Suggested Phase |
|----|-------------|---------------|-----------------|
| C-03 | `insert_many` ordered/unordered semantics | R10 | Phase 0.2 or new bead |
| C-04 | `find_one_and_replace`, `find_one_and_delete` not called out | DoD item 2 | Phase 0.2 (add to scope) |
| C-05 | Projection support not mentioned | G2 | Phase 1.2 (add to scope) |
| C-07 | Phase 3.5 exit criteria unclear (what enables CI-runnable) | G6 | Phase 3.5 (clarify) |
| C-08 | Benchmark regression detection in CI not planned | G7 | Phase 3.4 or 4.2 |
| C-10 | Observability: stats(), explain() missing | R11 | New bead Phase 4.3 |
| C-11 | File format magic bytes/versioning not explicit in 1.1 | R9 | Phase 1.1 scope |

### Observations

| ID | Description |
|----|-------------|
| O-01 | CRC32C checksums are a PRD G4 acceptance criterion but Phase 1.5 doesn't call them out by name. Likely implemented as part of WAL but worth an explicit exit criterion. |
| O-02 | Error code routing through new StorageEngine → Collection<T> boundary not explicitly verified in any phase. Adding one sentence to Phase 3.1 would close this. |
| O-03 | The dependency graph (Section 4) correctly reflects the build order. Parallelization notes are accurate: 1.5 (WAL) can proceed after 1.1 in parallel with 1.3/1.4. |
| O-04 | "key_encoding.rs: Yes, standalone, well-tested" in Section 5 is correct. Its integration into 1.2 (encode _id) and 1.4 (compound key encoding) is called out inline. No separate bead needed. |
| O-05 | Phase 4.2 (14 compiler warnings) — consider making "zero warnings" an explicit gate rather than a task. Warnings tend to accumulate if the gate isn't enforced in CI. |
