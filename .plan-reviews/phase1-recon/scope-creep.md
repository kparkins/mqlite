# Scope Review: Phase 1 Reconciliation Plan

**Review Date:** 2026-04-14
**Reviewer:** mqlite polecat slit
**Document Reviewed:** `docs/specs/phase1-reconciliation.md`
**Reference PRD:** `.prd-reviews/mqlite-embedded-mongodb/prd-draft.md`

---

## Verdict: FAIL ❌

The plan has two critical conflicts with the PRD and several significant scope additions not authorized by the PRD. These must be resolved before work begins.

---

## Critical Issues (Contradictions with PRD)

### CRITICAL-1: `open_in_memory()` is explicitly required by the PRD — the plan removes it

The reconciliation plan section 4.1 says:
> "Delete `EngineState`, `Vec<Document>`, `open_in_memory()`, `NoopFileLock`, and all in-memory special cases."

And success criteria 13 says:
> "No in-memory mode — one code path, always file-backed, tests use tempfiles."

**This directly contradicts:**
- PRD R8: "Database::open_in_memory() creates a database with no file backing. No durability, no WAL, no auxiliary files. Same API, same concurrency model."
- PRD Phase 1 DoD item 9: "In-memory mode: `Database::open_in_memory()` works with the same API."
- PRD Story 2: The entire "Test fixture database" story depends on `Database::open_in_memory()`.
- PRD Phase 1 success criterion explicitly lists in-memory mode as a delivery gate.

**Impact:** Removing in-memory mode is a PRD regression, not a reconciliation task. The PRD chose this feature deliberately for test ergonomics (Story 2). This decision cannot be reversed by a polecat — it needs mayor sign-off.

**Fix options (requires mayor decision):**
- Option A: Revert — keep `open_in_memory()` as PRD requires. Tests can use tempfiles OR in-memory (developer choice). In-memory mode bypasses WAL/file I/O (already planned in PRD rough approach: "PageManagerMode::InMemory allocates pages from Vec<Vec<u8>>").
- Option B: Get PRD amended — if in-memory mode is intentionally dropped, update PRD R8, DoD item 9, and Story 2 before the plan executes.

---

### CRITICAL-2: The plan changes the public API shape away from the PRD's explicit design

The reconciliation plan (Phase 0.1) introduces a `Client` type and three-level hierarchy:
```rust
// Plan's proposed API
let client = Client::open("myapp.mqlite")?;
let db = client.database("mydb");
let coll = db.collection::<Document>("restaurants");
```

**The PRD explicitly chose a two-level hierarchy:**
- PRD G3: "Database and Collection<T> are Send + Sync + Clone (cheap, Arc<Inner> internally)"
- PRD rough approach: "`Database::open()`, `Collection<T>` with serde support, all CRUD methods"
- PRD Story 1: Developer "opens a `.mqlite` file" — implies `Database::open()`
- PRD Story 2: "swap the connection to an in-memory mqlite instance (`Database::open_in_memory()`)"
- PRD G3 acceptance: "`Database::open()`, CRUD operations, and index management work without any background threads"

The PRD never mentions a `Client` type. The API the PRD specifies is `Database -> Collection<T>`, not `Client -> Database -> Collection<T>`.

**The reconciliation plan's justification** ("pymongo uses a Client type, so we need one too") is solving a wire protocol problem through an API surface change. The wire protocol issue (pymongo `client["mydb"]` gets `Unauthorized`) is a wire server routing bug — fix it by accepting any `$db` value and routing to the single embedded database. The native API shape doesn't need to change to fix the wire protocol.

**Impact:** Introducing `Client` breaks API compatibility and adds multi-database namespace support throughout the storage layer. The PRD is `Database::open("myapp.mqlite")` — one file, one database, simple. This change adds a new type, new routing logic, and contradicts the existing public API described in the PRD.

**Fix options (requires mayor decision):**
- Option A: Keep the PRD's `Database::open()` shape. Fix the wire server bug separately: accept any `$db` name in OP_MSG commands instead of enforcing filename-stem matching. `listDatabases` returns one entry (the filename stem). This fixes the pymongo/mongosh issue without an API overhaul.
- Option B: Get PRD amended — if the three-level hierarchy is the right long-term call, update the PRD's G3, Stories, and API docs before executing.

---

### CRITICAL-3: Multi-database namespace support is scope not in the PRD

Directly tied to CRITICAL-2. The reconciliation plan's Phase 0.1 and 0.2 add:
- Multiple named databases per `.mqlite` file
- Namespace routing (`"db.collection"` keys throughout storage)
- `list_database_names()` extracting distinct prefixes
- Wire protocol routing by `$db` field

The PRD never mentions multiple databases in one file. The PRD concept is: one `.mqlite` file = one database. PRD G1 says "One `.mqlite` file per database when cleanly closed." G1 does not say one `.mqlite` file hosts multiple databases.

The reconciliation plan's own success criteria 2, 3, and 4 (which require multiple named databases) **are not found in the PRD** — they were invented by the reconciliation plan author.

**Impact:** Multi-db namespace support threads through every storage operation (`insert(&self, ns: &str, ...)`, catalog schema, index management). It's not a free add — it changes the data model. If the PRD's `Database::open()` is a single-database abstraction, this is an unauthorized expansion.

---

## Major Issues (Scope Additions)

### MAJOR-1: `StorageEngine` trait — premature abstraction for this milestone

Phase 0.2 defines a `Box<dyn StorageEngine>` trait. The plan says:
> "A different engine (LSM, mmap, etc.) can be swapped in later by implementing the same trait."

This is future-proofing. Phase 1 ships exactly one engine (`PagedEngine`). The PRD's 5-layer architecture describes a concrete storage stack, not a pluggable trait. There is no PRD requirement for a storage engine abstraction.

**Recommendation:** Ship `PagedEngine` concretely in Phase 1. Extract a trait when (if) a second engine is actually needed. The vtable overhead and design complexity aren't free.

**Can be deferred:** File a follow-up bead. Not a blocker if kept as a concrete type for Phase 1.

---

### MAJOR-2: Phase 3.5 — CI-automated wire protocol integration test

The plan notes:
> "CI-runnable: start wire server, run pymongo test suite, validate. Currently requires manual execution."

The PRD G6 acceptance criteria say "A non-trivial pymongo (4.x) test suite passes" — it doesn't say CI-automated. Making wire tests CI-runnable adds infrastructure work (starting a background process in CI, port management, CI timeout handling) that is distinct from writing the tests themselves.

**Recommendation:** Defer CI automation to a follow-up bead. Manual execution satisfies PRD G6. File a separate bead for "Automate pymongo wire protocol test suite in CI."

**If timeline is halved, cut this entirely.**

---

## Minor Issues

### MINOR-1: Plan's 3.4 benchmark suite references wrong performance target

The plan's benchmark table shows "Single doc insert (FullSync)" target as `< 2 ms`. The PRD G7 shows the same target. Consistent — not an issue.

However, the plan's table omits PRD G7's `< 100 us` target for `Single doc insert (Interval mode)`. Make sure benchmarks include this.

---

### MINOR-2: SWMR (Phase 1.6) is required but high-risk

SWMR with WAL-based snapshot isolation is the most complex item in the plan. The PRD G5 explicitly requires it — it's not scope creep. But its complexity is high.

**Risk:** If SWMR slips, it blocks the entire Phase 1 DoD (item 7). A simpler intermediate (parking_lot::RwLock with readers-don't-block-writers) could satisfy G5's functional test while SWMR is polished, if needed as a contingency.

This is a risk flag, not a scope flag.

---

## Minimum Set for a Working Database (per PRD)

The minimum set that satisfies all PRD requirements:

| Item | Required | Rationale |
|------|----------|-----------|
| `Database::open()` (two-level API) | Yes | PRD G3, all stories |
| `Database::open_in_memory()` | Yes | PRD R8, DoD item 9 |
| Buffer pool + page allocator (1.1) | Yes | PRD G1, R4 |
| B+ tree document storage (1.2) | Yes | PRD G1, G7 |
| Catalog (1.3) | Yes | PRD G1 (persistence) |
| Secondary indexes (1.4) | Yes | PRD R3, G7 indexed scan target |
| WAL integration (1.5) | Yes | PRD G4 |
| SWMR concurrency (1.6) | Yes | PRD G5 |
| Wire protocol multi-db routing | **No** | Fix wire server bug instead |
| E2E persistence test (3.1) | Yes | PRD R12, DoD item 1 |
| Crash recovery test (3.2) | Yes | PRD R12, DoD item 6 |
| SWMR concurrency test (3.3) | Yes | PRD G5 acceptance criteria |
| Benchmark suite (3.4) | Yes | PRD G7, DoD item 8 |
| CI-automated wire test (3.5) | No | PRD doesn't require CI automation |
| Dead code removal (4.1) | Yes | Follow-on to storage work |
| Warnings cleanup (4.2) | Yes | PRD DoD item "Zero compiler warnings" |

---

## What Gets Cut if Timeline is Halved

By PRD priority (G1-G7 all required for DoD):

**Keep (PRD hard requirements):**
- Storage stack (1.1–1.5): required for G1, G4
- Secondary indexes (1.4): required for G7 indexed scan target
- SWMR (1.6): required for G5
- In-memory mode: required for R8, DoD item 9
- Wire protocol (2.1): required for G6, but fix single-db routing bug, not multi-db

**Cut (not in PRD or can be manual):**
- `Client/Database/Collection` three-level refactor (Phase 0.1): Keep PRD's two-level shape
- `StorageEngine` trait (Phase 0.2): Use concrete `PagedEngine`
- Multi-database namespace support: Not in PRD
- CI-automated wire tests (3.5): Keep manual, file a bead
- Phase 4.2 warnings cleanup: Defer to immediately post-MVP (keeps PRD compliance)

---

## Summary Table

| Issue | Category | Severity | Action Required |
|-------|----------|----------|-----------------|
| Removes `open_in_memory()` | Contradicts PRD | CRITICAL | Mayor decision before proceeding |
| Adds `Client` type (Phase 0.1) | API change not in PRD | CRITICAL | Mayor decision before proceeding |
| Multi-database per file (0.1, 0.2) | Scope addition not in PRD | CRITICAL | Mayor decision before proceeding |
| StorageEngine trait (0.2) | Premature abstraction | MAJOR | Defer to follow-up bead |
| CI-automated wire test (3.5) | Infrastructure scope add | MAJOR | Defer to follow-up bead |
| Missing `< 100 us` insert benchmark | Gap in test coverage | MINOR | Fix in 3.4 spec |
| SWMR complexity risk | Risk flag (not scope) | MINOR | Monitor; have contingency plan |

---

## Recommended Next Steps

1. **Mayor must decide** on CRITICAL-1, CRITICAL-2, and CRITICAL-3 before any Phase 0 work begins. These are PRD-level decisions, not implementation choices.
2. If the PRD `Database::open()` API is kept: fix the wire server's `$db` routing bug separately (simpler patch, no API change).
3. If in-memory mode is kept: re-include `open_in_memory()` in the build plan (PRD rough approach already planned PageManagerMode::InMemory).
4. File a follow-up bead for StorageEngine trait abstraction (not MVP).
5. File a follow-up bead for CI-automated wire tests (not MVP).
