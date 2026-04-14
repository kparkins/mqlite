# PRD Alignment Round 3: User Stories + Open Questions

**PRD**: `.prd-reviews/mqlite-embedded-mongodb/prd-draft.md`
**Design**: `.designs/vtmo/` (api.md, data.md, integration.md, scale.md, security.md, ux.md)
**Date**: 2026-04-14
**Reviewer**: polecat furiosa (automated)

---

## Part A: User Story Coverage

Walk through every USER STORY in the PRD and verify the design docs provide concrete implementation paths for each scenario.

### Story 1: Embedded Application Storage

> A Rust CLI tool needs to store structured configuration and user data. Developer adds `mqlite` as a cargo dependency, opens a `.mqlite` file, and does `insert_one`, `find`, `update_one` with familiar MQL semantics. No server to start, no Docker compose, no connection strings.

**Design Coverage Analysis:**

| Requirement | Design Doc | Status | Notes |
|-------------|-----------|--------|-------|
| `cargo add mqlite` with minimal deps | integration.md lines 262-321 | **COVERED** | Default features = none, base crate ~10 deps |
| `Database::open("path.mqlite")` entry point | api.md lines 73-74, ux.md lines 91-92 | **COVERED** | Zero-config open with sensible defaults |
| `insert_one`, `find`, `update_one` methods | api.md lines 140-176 | **COVERED** | Full CRUD API surface |
| MQL semantics (familiar query operators) | integration.md lines 466-514 | **COVERED** | All Phase 1 operators listed |
| No server, no Docker, no connection strings | api.md line 6, ux.md lines 67-135 | **COVERED** | Sync-first, embedded-only |
| File appears in working directory | ux.md lines 108-110 | **COVERED** | "The database file appears in the current directory" |
| Serde integration for typed docs | api.md lines 59, 82-83 | **COVERED** | `Collection<T>` with `T: Serialize + DeserializeOwned` |

**Verdict: FULLY COVERED**

The embedded app developer journey is comprehensively addressed. The getting-started experience (ux.md Journey 1, lines 67-135) demonstrates <10 lines from `cargo add` to first query.

---

### Story 2: Test Fixture Database

> A team uses MongoDB in production. For unit tests, they swap the connection to an in-memory mqlite instance (`Database::open_in_memory()`). Tests run in milliseconds with the same query logic, no MongoDB container needed. When a test uses an unsupported operator, mqlite returns an explicit error (not silent success), alerting the team to a compatibility gap.

**Design Coverage Analysis:**

| Requirement | Design Doc | Status | Notes |
|-------------|-----------|--------|-------|
| `Database::open_in_memory()` | api.md line 79, ux.md lines 137-192 | **COVERED** | Explicit in-memory API |
| Tests run in milliseconds | ux.md line 168 | **COVERED** | "sub-millisecond for small operations. No disk I/O" |
| Same query logic as production | integration.md lines 534-559 | **COVERED** | Compatibility test suite against MongoDB 8.0 |
| No MongoDB container needed | ux.md line 167 | **COVERED** | "No temp directories, no cleanup" |
| Unsupported operators return explicit errors | api.md lines 316, 399-407, ux.md lines 189-191 | **COVERED** | Error::UnsupportedOperator with operator name and suggestions |
| No silent success on unsupported operations | api.md line 467-468 | **COVERED** | "Silent success on an unsupported operator is a critical bug" |
| Drop releases all memory | ux.md line 169 | **COVERED** | "Drop on the Database handle releases all memory" |
| Query behavior matches file-backed mode | ux.md line 170 | **COVERED** | Explicitly stated as requirement |

**Gap Identified:**

- **Deterministic ObjectId generation for snapshot testing**: ux.md line 192 mentions "Consider a `Database::open_in_memory_with_seed(42)` that produces deterministic ObjectIds for snapshot testing" but this is phrased as a suggestion, not a committed design decision.
  - **Classification**: nice-to-have — Deterministic ObjectId generation is useful for snapshot testing but not critical for Phase 1. The current `open_in_memory()` API satisfies Story 2's core requirements.

**Verdict: FULLY COVERED** (with one nice-to-have gap)

---

### Story 3: MongoDB Driver Interop via Wire Protocol

> A developer wants to inspect their mqlite database using `mongosh` or Compass. They enable the wire protocol shim on a local port (`--features wire`), connect with standard MongoDB tools using `directConnection=true`, and browse/query their data.

**Design Coverage Analysis:**

| Requirement | Design Doc | Status | Notes |
|-------------|-----------|--------|-------|
| Wire protocol shim feature-gated | api.md line 7, integration.md lines 271-274 | **COVERED** | `wire` feature flag |
| mongosh connects and works | integration.md lines 159-175 | **COVERED** | Full command compatibility table |
| `directConnection=true` documented | integration.md lines 18, 192-197, 230 | **COVERED** | Explicit requirement with code example |
| Browse/query data with standard tools | ux.md lines 194-231 | **COVERED** | Journey 3 fully describes the UX |
| Unsupported commands return clear errors | api.md lines 386-407, integration.md lines 173-175 | **COVERED** | Error code 59 for aggregate/count/distinct |
| Localhost-only binding by default | api.md line 7, security.md line 129 | **COVERED** | Default bind to 127.0.0.1 |

**Gap Identified:**

- **Compass support**: Story 3 mentions "Compass" but the design docs only test against mongosh and pymongo. Compass may have additional command requirements.
  - **Classification**: nice-to-have — Compass is a GUI tool that uses the same wire protocol as mongosh. If mongosh works, Compass should work for basic browsing. Explicit Compass testing can be Phase 1.1.

- **CLI tool for serving**: ux.md lines 214-219 show a `mqlite serve` CLI example, and ux.md Open Question #5 asks "Is a CLI tool in scope for Phase 1?" This is unresolved.
  - **Classification**: should-fix — Resolve Open Question #5. Either commit to a Phase 1 CLI or explicitly defer it. The wire protocol shim is useful even without a CLI (can be started programmatically), but a CLI significantly improves the Story 3 experience.

**Verdict: MOSTLY COVERED** (with one should-fix gap)

---

### Story 4: Edge/IoT Data Collection

> A sensor gateway collects readings into a local mqlite database with `Interval(1s)` durability (acceptable 1s loss window for sensor data). When connectivity is available, a sync process reads documents and pushes them to a cloud MongoDB instance. The query model is identical on both sides.

**Design Coverage Analysis:**

| Requirement | Design Doc | Status | Notes |
|-------------|-----------|--------|-------|
| `Interval(Duration)` durability mode | api.md lines 124-131 | **COVERED** | `DurabilityMode::Interval(Duration)` |
| Configurable loss window | api.md line 128 | **COVERED** | Interval parameter controls loss window |
| Local data collection | ux.md lines 232-278 | **COVERED** | Journey 4 fully describes edge/IoT use case |
| Sync process reads documents | api.md lines 146-148 | **COVERED** | `find()` with filter |
| Query model identical to MongoDB | integration.md lines 433-451 | **COVERED** | Migration table shows 1:1 mapping |
| Resource-constrained deployment | ux.md lines 269-278, integration.md lines 333-344 | **COVERED** | Small buffer pool, cross-compilation targets |
| Crash recovery after power cuts | data.md lines 300-306, ux.md line 271 | **COVERED** | WAL recovery automatic |
| Disk-full handling | scale.md lines 263-270, ux.md lines 352-363 | **COVERED** | Error::DiskFull with graceful degradation |
| Read-only filesystem recovery | ux.md line 277 | **PARTIAL** | Mentioned but not detailed in api.md |

**Gap Identified:**

- **Read-only mode API**: ux.md line 277 mentions "Database::open_read_only('readings.mqlite') should work without attempting WAL replay or writes" but api.md OpenOptions (lines 113-122) only has `read_only: Option<bool>` without specifying behavior for recovery scenarios.
  - **Classification**: should-fix — Add documentation in api.md clarifying that `read_only: true` skips WAL modifications and allows opening databases on read-only filesystems. This is critical for forensic access in IoT failure scenarios.

**Verdict: MOSTLY COVERED** (with one should-fix gap)

---

### Story 5: Data Migration Tool

> A migration utility reads from one mqlite file and writes to another, applying transformations. For safe copies of an active database, the utility calls `db.backup(dest)` for a hot backup, or `db.checkpoint()` followed by file copy for a cold backup.

**Design Coverage Analysis:**

| Requirement | Design Doc | Status | Notes |
|-------------|-----------|--------|-------|
| Read from one file, write to another | api.md lines 73-74 | **COVERED** | Multiple `Database::open()` calls |
| Apply transformations | api.md lines 140-176 | **COVERED** | Full CRUD API for reads and writes |
| `db.backup(dest)` for hot backup | api.md line 97, ux.md line 133 | **COVERED** | `Database::backup()` method |
| `db.checkpoint()` for safe cold copy | api.md line 94, ux.md lines 133-134 | **COVERED** | `Database::checkpoint()` method |
| Safe copy of active database | integration.md lines 422-430 | **COVERED** | File management table with backup methods |

**Verdict: FULLY COVERED**

---

## Part B: User Story Coverage Summary

| Story | Status | Gaps |
|-------|--------|------|
| 1: Embedded Application | **FULLY COVERED** | None |
| 2: Test Fixture Database | **FULLY COVERED** | 1 nice-to-have (deterministic ObjectId) |
| 3: Wire Protocol Interop | **MOSTLY COVERED** | 1 should-fix (CLI tool resolution), 1 nice-to-have (Compass) |
| 4: Edge/IoT Data Collection | **MOSTLY COVERED** | 1 should-fix (read-only mode documentation) |
| 5: Data Migration Tool | **FULLY COVERED** | None |

---

## Part C: PRD Open Questions Resolution

The PRD lists 4 remaining open questions (reduced from 11). Verify each has a resolution in the design docs.

### OQ1: WAL checkpointing — synchronous or asynchronous?

**PRD Statement**: "Synchronous is simpler (inline after write crosses threshold). Asynchronous is better for write latency. Recommend synchronous for Phase 1."

**Design Resolution**: scale.md Open Question #1 (line 341) recommends "synchronous for Phase 1, async as Phase 2 optimization." Also scale.md line 59 describes "Non-blocking checkpoint (runs between write operations)."

**Status**: **RESOLVED** — Synchronous checkpointing for Phase 1.

---

### OQ2: Should `Database` implement `Drop` with WAL flush?

**PRD Statement**: "Recommend: `Drop` does non-blocking close. Explicit `db.close()` for blocking flush. Document the difference."

**Design Resolution**: api.md Open Question #2 (line 485) states "Drop does non-blocking close. Explicit `db.close()` method for blocking flush." data.md lines 319-327 describe the clean close protocol. ux.md line 111 mentions "Closing happens on `Drop`. No explicit `.close()` required (but available for explicit flush control)."

**Status**: **RESOLVED** — Drop does non-blocking close; `close()` is available for explicit control.

---

### OQ3: Should OP_COMPRESSED be Phase 1?

**PRD Statement**: "For localhost-only debugging, compression overhead may exceed benefit. Recommend: defer to Phase 1.1."

**Design Resolution**: integration.md lines 96-103 explicitly defer OP_COMPRESSED to Phase 1.1, citing that `zstd` requires C bindings (violates pure-Rust constraint) and localhost debugging doesn't benefit from compression.

**Status**: **RESOLVED** — OP_COMPRESSED deferred to Phase 1.1.

---

### OQ4: Should compound indexes be Phase 1 MVP or Phase 1.1?

**PRD Statement**: "They add significant complexity. Single-field + auto `_id` may be sufficient for initial release. Current answer: Phase 1 (per design spec), but can be descoped if it jeopardizes delivery."

**Design Resolution**: data.md lines 215-227 include compound indexes in Phase 1 with full design (concatenated key encoding, prefix queries, per-field sort direction).

**Status**: **RESOLVED** — Compound indexes are Phase 1.

---

## Part D: Design Doc Open Questions Resolution

Each design doc contains open questions. These must be resolved for implementation to proceed without ambiguity.

### api.md Open Questions

| # | Question | Resolution | Classification |
|---|----------|-----------|----------------|
| 1 | Should `find_one_and_update` return pre- or post-modification document by default? | Match MongoDB behavior: default to pre-modification. Add `FindOneAndUpdateOptions { return_document: ReturnDocument::Before \| After }`. | **RESOLVED** — Document in api.md that default is `Before` to match MongoDB. |
| 2 | Should `Database` implement `Drop` with WAL flush? | Drop does non-blocking close. Explicit `db.close()` for blocking flush. | **RESOLVED** — Already stated in api.md line 485. |
| 3 | How does `insert_many` handle partial failures with `ordered: true`? | (Already marked resolved in api.md line 487) | **RESOLVED** — `InsertManyOptions { ordered: bool }` added. |
| 4 | Should the wire protocol support OP_COMPRESSED? | Defer to Phase 1.1. | **RESOLVED** — See OQ3 above. |
| 5 | What is the cursor timeout for idle cursors via wire protocol? | 10 minutes, matching MongoDB. | **RESOLVED** — PRD R5 line 286 already specifies "Wire protocol cursor idle timeout: 10 minutes". |
| 6 | Should `update_one`/`update_many` support upsert? | (Already marked resolved in api.md line 493) | **RESOLVED** — `UpdateOptions { upsert: bool }` added. |
| 7 | How does `serverStatus` report for an embedded database? | Return: uptime, connection count (wire protocol), storage stats (file size, WAL size, buffer pool usage), operation counters. | **RESOLVED** — This is sufficiently detailed for implementation. |

### ux.md Open Questions

| # | Question | Resolution | Classification |
|---|----------|-----------|----------------|
| 1 | Should `Collection<T>` require `T: Serialize + DeserializeOwned`, or should there be typed and untyped collection handles? | Follow MongoDB Rust driver pattern: `Collection<T>` where `T` defaults to `Document`. | **RESOLVED** — api.md already specifies `Collection<T: Serialize + DeserializeOwned>` with `Collection<Document>` as untyped default. |
| 2 | What is the default busy timeout? | 5 seconds. This is explicitly stated in api.md line 118 and ux.md line 522. | **RESOLVED** |
| 3 | Should `Database` implement `Clone`? | Yes. api.md lines 106-108 explicitly state `Database` is `Send + Sync + Clone` with `Arc<Inner>` internally. | **RESOLVED** |
| 4 | How should the wire protocol shim report its server version? | Report `maxWireVersion: 21` (MongoDB 8.0) but strip unsupported capabilities. Include `mqlite.version` field. Do NOT report a MongoDB version string. | **RESOLVED** — integration.md lines 107-149 specify this. |
| 5 | Is a CLI tool (`mqlite` binary) in scope for Phase 1? | **UNRESOLVED** | **must-fix** — See resolution below. |
| 6 | Should `find()` return an `Iterator` or a custom `Cursor` type that implements `Iterator`? | `Cursor<T>` that implements `Iterator`. api.md lines 236-246 specify this. | **RESOLVED** |
| 7 | What is the behavior of `Drop` on `Database`? | Non-blocking close. WAL recoverable on next open. See api.md OQ#2. | **RESOLVED** |
| 8 | Should mqlite provide a `#[cfg(test)]` helper module? | Nice-to-have for Phase 1.1. The core `open_in_memory()` API suffices for basic test fixtures. | **DEFERRED** |

**Resolution for ux.md OQ#5 (CLI tool):**

The PRD and design docs position the wire protocol as a debugging/interop feature. A CLI tool (`mqlite serve <file>`) would significantly improve the Story 3 experience but is not strictly required — the wire protocol can be started programmatically.

**Decision**: Defer the CLI tool to Phase 1.1. Phase 1 ships with programmatic `WireProtocol::bind()` API only. Add a note in ux.md that the CLI is planned for Phase 1.1.

### integration.md Open Questions

| # | Question | Resolution | Classification |
|---|----------|-----------|----------------|
| 1 | Should mqlite handle session IDs in commands gracefully? | Silently ignore `lsid` fields. Log at debug level. Do NOT error. | **RESOLVED** — This is the recommended approach and must be documented. |
| 2 | How should mqlite handle `readConcern` and `writeConcern` in commands? | Accept and ignore. Log at debug level. | **RESOLVED** — Same pattern as session IDs. |
| 3 | Should the wire protocol support `explain` command? | Defer to Phase 1.1. The native API `cursor.explain()` is sufficient for Phase 1. | **DEFERRED** |
| 4 | What pymongo version is the compatibility target? | pymongo 4.x. | **RESOLVED** — Explicitly stated in integration.md line 594. |
| 5 | Should mqlite provide a compatibility test harness as a developer tool? | Nice-to-have for contributors. Can be built incrementally as the test suite grows. | **DEFERRED** |
| 6 | How does mqlite handle `$db` field in OP_MSG? | Validate and error on mismatch. mqlite is single-database; `$db` must match the opened database name. | **RESOLVED** — This is the recommended approach. |
| 7 | Should the wire protocol support cursor pinning for getMore? | Yes, enforce that getMore must be sent on the same connection that created the cursor. | **RESOLVED** — Required for correctness. |
| 8 | What is the testing strategy for cross-platform file format compatibility? | CI job creates file on one platform, reads on another. Ensure little-endian consistency. | **RESOLVED** — Documented approach. |

### data.md Open Questions

| # | Question | Resolution | Classification |
|---|----------|-----------|----------------|
| 1 | Should the primary data store use a clustered index or a heap? | Clustered index (documents stored in `_id` order). This matches MongoDB behavior and optimizes the common case. | **RESOLVED** — data.md already recommends clustered (line 418). |
| 2 | What is the free page reclamation strategy? | Immediate free list return for simplicity. VACUUM for file size reduction. | **RESOLVED** — data.md line 420 recommends this. |
| 3 | Should overflow pages be the same size as leaf pages (32KB)? | Yes, uniform 32KB overflow pages. | **RESOLVED** — data.md line 422 recommends this. |
| 4 | How are concurrent index builds handled? | Blocking index builds for Phase 1. Background index builds are Phase 2. | **RESOLVED** — Acceptable for embedded use case. Document in api.md. |
| 5 | What happens when the file grows beyond available disk? | `Error::DiskFull` is returned. WAL handles partial writes gracefully. | **RESOLVED** — See scale.md disk-full behavior. |
| 6 | Should compound index key encoding handle null/missing fields? | Yes, index with null key. Match MongoDB behavior. | **RESOLVED** — Required for compatibility. |

---

## Part E: Summary of Findings

### Must-Fix Items

| # | Finding | Source | Suggested Fix |
|---|---------|--------|---------------|
| 1 | CLI tool scope unresolved | ux.md OQ#5 | Add note to ux.md and integration.md that `mqlite` CLI is deferred to Phase 1.1. Phase 1 ships with programmatic `WireProtocol::bind()` only. |
| 2 | Read-only mode behavior undocumented | Story 4 gap | Add documentation in api.md clarifying that `read_only: true` skips WAL modifications and allows opening on read-only filesystems. |
| 3 | `find_one_and_update` default return not explicit | api.md OQ#1 | Add note in api.md that default is `ReturnDocument::Before` to match MongoDB. |
| 4 | Session/concern handling not documented | integration.md OQ#1-2 | Add explicit note in api.md/integration.md that `lsid`, `readConcern`, `writeConcern` are silently ignored with debug-level logging. |
| 5 | Blocking index builds not documented | data.md OQ#4 | Add note in api.md that `create_index` blocks writes until completion. Background builds are Phase 2. |

### Should-Fix Items

| # | Finding | Source | Suggested Fix |
|---|---------|--------|---------------|
| 6 | Compass testing not explicitly planned | Story 3 gap | Add Compass to integration testing targets in integration.md. |
| 7 | `$db` field validation behavior undocumented | integration.md OQ#6 | Add explicit handling in wire protocol command dispatch: validate `$db` matches, error on mismatch with clear message. |
| 8 | Cursor pinning for getMore undocumented | integration.md OQ#7 | Add note in api.md wire protocol section about connection-pinned cursors. |

### Nice-to-Have Items

| # | Finding | Source | Suggested Fix |
|---|---------|--------|---------------|
| 9 | Deterministic ObjectId for snapshot testing | Story 2 gap | Consider `Database::open_in_memory_with_seed(seed)` for Phase 1.1. |
| 10 | `#[cfg(test)]` helper module | ux.md OQ#8 | Consider `mqlite::test` module with fixture helpers for Phase 1.1. |
| 11 | Wire protocol `explain` command | integration.md OQ#3 | Defer to Phase 1.1. |
| 12 | Compatibility test harness binary | integration.md OQ#5 | Defer to Phase 1.1. |

---

## Part F: Changes Applied to Design Docs

All must-fix items above must be applied as edits to the design documents. The following changes are required:

1. **ux.md**: Add resolution to Open Question #5 — CLI tool deferred to Phase 1.1.
2. **api.md**: Add read-only mode documentation in OpenOptions section.
3. **api.md**: Add explicit note that `find_one_and_update` defaults to `ReturnDocument::Before`.
4. **api.md/integration.md**: Add section on graceful handling of session IDs and concerns.
5. **api.md**: Add note that `create_index` blocks writes until completion (Phase 2: background builds).
6. **integration.md**: Add Compass to testing targets.
7. **integration.md**: Add `$db` field validation behavior.
8. **api.md**: Add cursor pinning documentation for wire protocol.

---

## Part G: Overall Alignment Assessment

| Dimension | Round 1 Status | Round 2 Status | Round 3 Status |
|-----------|---------------|---------------|----------------|
| Requirements | 1 must-fix, 9 should-fix | — | — |
| Goals | — | — | — |
| Constraints | — | 1 must-fix, 6 should-fix | — |
| Non-Goals | — | 0 must-fix, 3 should-fix | — |
| User Stories | — | — | 5 must-fix, 3 should-fix, 4 nice-to-have |
| Open Questions | — | — | All PRD OQs resolved; Design OQs mostly resolved |

**Overall Assessment**: The design docs comprehensively cover all PRD requirements, goals, constraints, non-goals, and user stories. The remaining gaps are documentation clarifications and Phase 1.1 deferrals, not missing functionality. The design is **implementation-ready** after the must-fix documentation updates are applied.
