# PRD Alignment Round 1: Requirements + Goals

**PRD**: `.prd-reviews/mqlite-embedded-mongodb/prd-draft.md`
**Design**: `.designs/vtmo/` (api.md, data.md, integration.md, scale.md, security.md, ux.md)
**Date**: 2026-04-14
**Reviewer**: polecat furiosa (automated)

---

## Part A: Requirements Coverage

Walk through every REQUIREMENT in the PRD and verify the plan has a concrete task/phase that addresses it.

### R1: Error model

- **COVERED**: R1 → `api.md` Error Taxonomy section (lines 228-272)
  - All MongoDB-compatible error codes listed (11000, 121, 27, 26, 48, 59, 22, 10334)
  - mqlite-specific errors defined (WriterBusy, CorruptDatabase, DiskFull, UnsupportedOperator)
  - `Error::code()` and `Error::code_name()` methods defined
  - Integration analysis (`integration.md` lines 436-449) has error code verification table

### R2: Document validation

- **COVERED**: R2 → `data.md` Document Validation section (lines 326-346)
  - Well-formedness (BSON parsing via `bson` crate)
  - Size limit (16MB)
  - Nesting depth (max 100 levels)
  - `_id` auto-generation with MongoDB-compatible ObjectId format
  - `_id` immutability enforcement
- **PARTIAL**: Data model adds two extra limits not in PRD: max 10,000 fields per document, max 1,024 bytes field name length. These are reasonable defensive additions but should be noted.
  - **Classification**: should-fix — Document the additional limits in PRD or note them as mqlite-specific extensions. The PRD should acknowledge these or the design should mark them as configurable/relaxable.

### R3: Index support

- **COVERED**: R3 → `data.md` Index Architecture section (lines 179-223)
  - Auto `_id` index (clustered, lines 183-189)
  - Single-field indexes (secondary indexes, lines 193-198)
  - Compound indexes (lines 213-223)
  - Multikey indexes (lines 201-209)
  - Unique indexes — mentioned in `api.md` IndexOptions (line 222: `unique: Option<bool>`)
  - Sparse indexes — mentioned in `api.md` IndexOptions (line 223: `sparse: Option<bool>`)
- **GAP**: The design docs describe the index architecture well but do not explicitly mention the "Out of scope" index types (TTL, text, geospatial, partial, hashed) or how they will be rejected. The unsupported-operation error handling in `api.md` covers this implicitly.
  - **Classification**: should-fix — Add explicit mention in api.md or data.md that createIndex requests for unsupported index types (TTL, text, geospatial, partial, hashed) should return appropriate errors.

### R4: Storage architecture

- **COVERED**: R4 → `data.md` comprehensively
  - Variable page B+ tree: 4KB internal, 32KB leaf, 32KB overflow (lines 86-135)
  - BSON stored as-is (line 13)
  - Catalog design (lines 226-270)
  - Buffer pool with CLOCK-sweep (lines 349-377)
  - WAL design (lines 272-323)
  - `bson` crate usage and re-export (integration.md lines 209-253)
  - ObjectId generation (data.md lines 338-346)

### R5: Resource limits

- **COVERED**: R5 → `scale.md` Resource Limits section (lines 149-162)
  - All limits from PRD table are present with matching defaults/ranges
  - Configuration via `OpenOptions` (api.md lines 113-132)

### R6: Disk-full behavior

- **COVERED**: R6 → `scale.md` Disk-Full Behavior (lines 263-270), `api.md` Error::DiskFull variant (line 255), `ux.md` Disk Full error UX (lines 352-363)
  - All 4 PRD requirements addressed: return DiskFull error, database remains readable, application can retry, checkpoint can reclaim WAL space

### R7: Platform targets

- **COVERED**: R7 → `integration.md` Cross-Compilation Targets (lines 296-306)
  - Linux x86_64: P0
  - macOS ARM64: P0
  - Linux aarch64: P0
  - Windows and WASM noted as P1/P2

### R8: In-memory mode

- **COVERED**: R8 → `api.md` Database::open_in_memory() (line 79), `ux.md` Journey 2 (lines 137-192)
  - Same API, same concurrency model, suitable for testing

### R9: File format versioning

- **COVERED**: R9 → `data.md` File Header (lines 62-83)
  - Magic bytes "MQLT" (0x4D514C54) at offset 0
  - Format version uint32 at offset 4
- **GAP**: The PRD states "File format version changes constitute semver-breaking changes." The design docs do not explicitly state this policy.
  - **Classification**: should-fix — Add file format versioning policy to data.md stating that backward-incompatible format changes require mqlite major version bump.

### R10: insert_many ordered/unordered semantics

- **PARTIAL**: R10 → `api.md` mentions `insert_many` method (line 142) but does NOT explicitly address ordered/unordered semantics
  - api.md Open Question #3 (line 416) asks about partial failure handling for `ordered: true`
  - integration.md wire protocol insert command notes "Bulk insert with ordered/unordered semantics" (line 286)
  - **Classification**: must-fix — The api.md should explicitly define `InsertManyOptions { ordered: bool }` (default true) and document the behavior for both modes, matching PRD R10. The open question should be resolved (MongoDB behavior is well-defined; match it).

### R11: Observability (Phase 1 minimum)

- **COVERED**: R11 → `api.md` Database::stats() (line 103), Collection::stats() (line 172), Cursor::explain() (line 199)
  - `ux.md` Observability UX (lines 389-421) provides detailed examples
- **GAP**: PRD requires optional `tracing` feature flag. Design mentions it in `integration.md` Crate Structure (line 269, 279) but doesn't detail what spans/events are emitted.
  - **Classification**: should-fix — Add a brief section in api.md or integration.md specifying which operations emit tracing spans (queries, WAL writes, checkpoint, etc.).

### R12: Test strategy

- **COVERED**: R12 → `integration.md` Testing Integration section (lines 309-360)
  - Unit tests per layer: mentioned in test pyramid
  - Compatibility test suite: lines 334-341
  - Crash testing: Jepsen-style (lines 343-349)
  - Property-based testing: mentioned but not detailed
  - Fuzz testing: lines 352-360
- **GAP**: Property-based testing for B+ tree invariants is mentioned in the test pyramid but lacks detail on what properties to test.
  - **Classification**: should-fix — Add explicit property list for B+ tree testing (ordering invariant, balance, parent-child consistency, sibling pointers) either in data.md or integration.md.

---

## Part B: Goals Alignment

Walk through every GOAL in the PRD and verify the plan achieves it.

### G1: Single-file storage

- **ALIGNED**: G1 → `data.md` WAL Design (lines 272-323)
  - Three-file model during operation (.mqlite, .mqlite-wal, .mqlite-shm) collapsing to single file on clean close
  - Clean close protocol (data.md lines 316-323)
  - Database::checkpoint() (api.md line 94)
  - Database::backup() (api.md line 97)
  - `ux.md` File Management UX (lines 131-135)
  - **Acceptance criteria coverage**:
    - Open + Drop with no writes → single file: Covered by clean close protocol
    - Write + close → single file: Covered by WAL checkpoint on close
    - Copy single file = valid database: Covered by design (BSON stored as-is, self-contained)
    - checkpoint() forces WAL merge: Covered (api.md line 94)
    - backup(dest) produces consistent copy: Covered (api.md line 97)

### G2: MongoDB API compatibility (MongoDB 8.0 target)

- **ALIGNED**: G2 → `api.md` CRUD methods (lines 140-173), `integration.md` full compatibility strategy
  - All in-scope query operators listed in PRD are addressed by the query engine design (referenced in api.md integration points)
  - All in-scope update operators addressed
  - Unsupported operation behavior: api.md lines 329-349 (proper error codes)
  - Compatibility test suite: integration.md lines 334-341
- **PARTIAL**: BSON type comparison ordering for indexes
  - data.md Key Encoding section (lines 137-177) defines the encoding but the ordering doesn't exactly match PRD G2's stated order: "MinKey < Null < Numbers < Symbol < String < Object < Array < BinData < ObjectId < Boolean < Date < Timestamp < RegExp < MaxKey"
  - data.md type tag order: MinKey(0x00) < Null(0x05) < Numbers(0x10) < Symbol(0x15) < String(0x20) < Object(0x30) < Array(0x40) < BinData(0x50) < ObjectId(0x60) < Boolean(0x70) < Date(0x80) < Timestamp(0x85) < RegExp(0x90) < MaxKey(0xFF)
  - This appears correct and matches MongoDB's ordering. **COVERED**.
- **GAP**: The plan doesn't have an explicit task/phase for implementing each query/update operator. The operator set is defined but there's no per-operator implementation plan or test matrix in the design docs.
  - **Classification**: should-fix — Add a per-operator implementation checklist or test matrix to integration.md showing each operator's implementation status target.

### G3: Zero-server operation

- **ALIGNED**: G3 → `api.md` fully addresses this
  - Sync-first API (lines 13-14, 390-391)
  - Database::open("path.mqlite") entry point (lines 73-74)
  - Zero async runtime dependency for base crate (integration.md lines 259-268)
  - Wire protocol behind `wire` feature flag with own tokio runtime
  - **Thread safety**: Database and Collection are Send + Sync + Clone (api.md lines 379-386), Cursor is Send but not Sync
  - **Acceptance criteria coverage**: All three criteria clearly addressed

### G4: Crash recovery

- **ALIGNED**: G4 → `data.md` WAL Design + `scale.md` Power Loss Handling
  - WAL-based durability (data.md lines 272-323)
  - DurabilityMode enum: FullSync, Interval, None (api.md lines 124-131)
  - CRC32C checksums per frame (data.md line 292)
  - Auto-recovery on open (data.md lines 300-301)
  - Power loss handling (scale.md lines 256-261)
  - **Acceptance criteria coverage**:
    - FullSync kill -9 survival: Covered by WAL design
    - Interval mode + wait + kill: Covered
    - Crash during write → clean recovery: Covered by CRC32C and commit markers
    - Auto WAL replay: Covered (data.md line 301)
    - CRC32C detects torn pages: Covered (scale.md line 260)

### G5: Concurrent read access

- **ALIGNED**: G5 → `scale.md` Concurrency Model (lines 62-122)
  - SWMR model fully described
  - Snapshot isolation at cursor open time (scale.md lines 100-104)
  - Writer contention with configurable timeout (scale.md lines 108-113, api.md line 282)
  - Multi-process access via POSIX fcntl (scale.md line 58)
  - Reader limit configurable (default 64, max 256)
  - **Acceptance criteria coverage**: All four criteria addressed

### G6: Wire protocol shim

- **ALIGNED**: G6 → `api.md` Wire Protocol section (lines 274-375), `integration.md` full wire protocol design
  - All 18 Phase 1 commands listed (api.md lines 278-298)
  - OP_MSG framing (integration.md lines 65-93)
  - Handshake response with maxWireVersion: 21 (integration.md lines 107-133)
  - localhost-only binding by default (security.md)
  - **Acceptance criteria coverage**:
    - mongosh connects and runs CRUD: integration.md lines 155-171
    - pymongo test suite: integration.md lines 173-193
    - Unsupported commands return code 59: api.md lines 329-338
    - Localhost-only default: security.md line 129
- **GAP**: PRD says wire protocol reports `mqlite.version` in hello response. api.md handshake response (lines 302-318) includes this. However, the `listDatabases` command (mentioned in PRD) needs clarification — mqlite is single-DB per file. integration.md line 285 says "Returns the single database name" which addresses this.
  - **COVERED** on closer inspection.

### G7: Reasonable performance

- **ALIGNED**: G7 → `scale.md` Performance Targets (lines 126-138)
  - All operation targets from PRD table are present with matching or better values
  - Benchmark suite requirement noted
  - Regression detection mentioned
  - **Acceptance criteria coverage**:
    - Benchmark suite exists: Addressed
    - Targets met on reference hardware: Targets specified
    - No 2x regression between releases: Not explicitly addressed in design
  - **Classification**: should-fix — Add regression detection strategy (CI benchmark comparison) to integration.md or scale.md.

---

## Part C: PRD Sections Coverage

### Problem Statement
- **COVERED** — Design docs address all target audiences (Rust app devs, CLI authors, edge/IoT, test/dev, migration) via ux.md user journeys.

### Non-Goals
- **COVERED** — Wire protocol unsupported command handling (code 59) covers rejection of non-goal features. Security.md explicitly addresses no-auth, no-encryption-at-rest as Phase 1 decisions.

### User Stories
- **COVERED** — ux.md (lines 67-278) maps all 5 PRD stories to detailed user journeys.

### Constraints
- **COVERED** — All technical constraints (Rust-only, SWMR, file format stability, page-based storage, BSON) are reflected in the design. Architecture (5-layer stack) is described across design docs. Resource and business constraints addressed.

### Open Questions
- **PARTIAL** — PRD has 4 open questions:
  1. WAL checkpointing sync/async: Addressed in scale.md Open Question 1 (recommends sync for Phase 1)
  2. Database Drop with WAL flush: Addressed in api.md Open Question 2
  3. OP_COMPRESSED Phase 1: Addressed in integration.md (deferred to Phase 1.1)
  4. Compound indexes Phase 1 vs 1.1: Addressed in data.md (included in Phase 1)

### Phase 1 Definition of Done
- **Checklist coverage**:
  1. Storage engine: **COVERED** (data.md, scale.md)
  2. CRUD: **COVERED** (api.md lines 140-173)
  3. Query operators: **COVERED** (api.md references, integration.md compatibility testing)
  4. Indexes: **COVERED** (data.md index architecture)
  5. Wire protocol: **COVERED** (api.md, integration.md)
  6. Crash recovery: **COVERED** (data.md WAL, scale.md)
  7. Concurrency: **COVERED** (scale.md)
  8. Performance: **COVERED** (scale.md)
  9. In-memory mode: **COVERED** (api.md, ux.md)
  10. Documentation: **PARTIAL** — ux.md lists documentation tiers but there's no explicit task for creating each doc
    - **Classification**: should-fix — Add documentation deliverables to the plan as explicit tasks.

---

## Summary of Findings

### Must-Fix Items

| # | Finding | PRD Section | Plan Gap | Suggested Fix |
|---|---------|------------|----------|---------------|
| 1 | `insert_many` ordered/unordered semantics not explicitly designed | R10 | api.md lacks InsertManyOptions | Add `InsertManyOptions { ordered: bool }` to api.md options section. Resolve Open Question #3 by matching MongoDB 8.0 behavior (stop on first error in ordered mode, report all errors in unordered mode). |

### Should-Fix Items

| # | Finding | PRD Section | Plan Gap | Suggested Fix |
|---|---------|------------|----------|---------------|
| 2 | Additional document validation limits not in PRD | R2 | data.md adds field count (10K) and field name length (1024) limits | Note these as mqlite-specific extensions in design doc, or add to PRD. |
| 3 | Unsupported index types not explicitly rejected | R3 | No error handling for TTL/text/geo/partial/hashed createIndex | Add explicit note about rejecting unsupported index types with appropriate error. |
| 4 | File format versioning policy not stated | R9 | data.md doesn't state semver policy | Add note that format-breaking changes require mqlite major version bump. |
| 5 | Tracing spans not detailed | R11 | No list of what operations emit tracing events | Add brief tracing event spec. |
| 6 | Property-based test properties not listed | R12 | B+ tree property tests lack specifics | List properties: ordering, balance, parent-child, siblings. |
| 7 | Per-operator implementation checklist missing | G2 | No operator-by-operator plan | Add operator implementation matrix. |
| 8 | Performance regression detection not planned | G7 | No CI benchmark comparison | Add regression detection strategy. |
| 9 | Documentation deliverables not tasked | Phase 1 DoD #10 | No explicit doc tasks | Add documentation tasks to plan. |
| 10 | `upsert` support not addressed | api.md OQ #6 | Open question unresolved | Resolve: upsert should be Phase 1 (common pattern, required for real-world test suites). Add `UpdateOptions { upsert: bool }` to api.md. |

---

## Changes Applied to Design Docs

All must-fix and should-fix items above have been applied as edits to the design documents. See git diff for exact changes.
