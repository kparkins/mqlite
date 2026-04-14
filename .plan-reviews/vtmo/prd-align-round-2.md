# PRD Alignment Round 2: Constraints + Non-Goals

**PRD**: `.prd-reviews/mqlite-embedded-mongodb/prd-draft.md`
**Design**: `.designs/vtmo/` (api.md, data.md, integration.md, scale.md, security.md, ux.md)
**Date**: 2026-04-14
**Reviewer**: polecat furiosa (automated)

---

## Part A: Constraints Coverage

Walk through every CONSTRAINT in the PRD and verify the plan has concrete design decisions that honor it.

### C1: Rust-only implementation (no C/C++ dependencies)

- **COVERED**: C1 → `integration.md` Dependency Budget (lines 308-316), OP_COMPRESSED Decision (lines 96-103)
  - Base crate dependencies: `bson`, `thiserror`, `crc32c` — all pure Rust
  - `tokio` only behind `wire` feature flag
  - OP_COMPRESSED deferred specifically because `zstd` requires C bindings (integration.md line 101)
  - integration.md constraint #6 (line 556): "Pure Rust constraint limits compression options"
  - security.md constraint #4 (line 203): "Pure Rust constraint — Limits crypto library choices (no OpenSSL)"
- **GAP**: The PRD specifies that "Transitive pure-Rust dependencies that optionally have C backends (e.g., `flate2`) must use their Rust backend." The design docs do not mention enforcing `default-features = false` or feature-gating C backends in transitive dependencies.
  - **Classification**: should-fix — Add a note in integration.md specifying that `Cargo.toml` must use feature flags to prevent transitive C dependencies (e.g., `flate2` must use `rust_backend` feature, `crc32c` must use pure Rust implementation). Consider adding a CI check that validates no C compilation units appear in the dependency tree.

### C2: Single-writer / multiple-reader concurrency

- **COVERED**: C2 → `scale.md` Concurrency Model (lines 62-122), entire Option 2 analysis
  - SWMR model fully described with read/write/checkpoint paths
  - Multi-process via POSIX fcntl (scale.md line 58)
  - Writer contention with configurable timeout (scale.md lines 108-113)
  - api.md confirms sync-first with blocking writer (lines 16, 447)
  - data.md WAL design (lines 276-327) implements the SWMR primitives
  - security.md attack surface #11 (line 179) addresses multi-process file corruption risk

### C3: File format stability (versioned from day one)

- **COVERED**: C3 → `data.md` File Format Versioning Policy (lines 61-63), File Header (lines 65-87)
  - Magic bytes "MQLT" at offset 0
  - Format version uint32 at offset 4
  - Explicit policy: "Backward-incompatible file format changes constitute semver-breaking changes"
  - Forward compatibility returns clear error (data.md line 63)
  - Reserved space in header (72 bytes at offset 72) for future use — good practice for format stability
- **Note**: Round 1 identified this policy was missing; it has since been added. **RESOLVED**.

### C4: Page-based storage (variable-page B+ trees)

- **COVERED**: C4 → `data.md` comprehensively
  - 4KB internal nodes (lines 89-101)
  - 32KB leaf nodes (lines 103-128)
  - 32KB overflow pages (lines 129-139)
  - Two free lists for the two size classes (data.md lines 36-40, file header offsets 36-40)
  - Buffer pool with separate pools for 4KB and 32KB pages (data.md lines 375-381)

### C5: BSON document model (official `bson` crate, 16MB max)

- **COVERED**: C5 → `data.md` lines 13-15, `integration.md` BSON Ecosystem Integration (lines 208-253)
  - Official `bson` crate used and re-exported (integration.md lines 210-226)
  - 16MB max document size (data.md line 334, api.md line 304)
  - Version pinning strategy (integration.md lines 229-234)
  - Re-export avoids user-side version conflicts (integration.md line 234)
  - api.md constraint #8 (line 461): bson re-export pins the version

### C6: Architecture (5-layer stack)

- **COVERED**: C6 → Design docs collectively implement all 5 layers:
  1. Wire Protocol Shim → `api.md` lines 331-432, `integration.md` lines 63-93
  2. Native Rust API → `api.md` lines 63-177, `ux.md` module organization (lines 447-469)
  3. Query Engine → `integration.md` MQL Operator Matrix (lines 448-501)
  4. Storage Engine → `data.md` B+ tree + WAL + buffer pool, `scale.md` concurrency
  5. File → `data.md` File Format Specification (lines 59-139)
- **GAP**: The PRD states the wire protocol "reports as standalone with mqlite version." The design docs handle this (api.md lines 371-374, integration.md lines 136-149) but the hello response in integration.md (lines 107-124) omits the `mqlite.version` field, while api.md (lines 372-374) includes it.
  - **Classification**: should-fix — The handshake response in integration.md should include the `mqlite` field to match api.md's response and the PRD constraint.

### C7: Greenfield Rust project

- **COVERED**: Implicit — all design docs describe building from scratch. No references to existing codebases or forking. The `ux.md` crate naming section (lines 441-443) confirms the crate is new.

### C8: Build from storage engine up (no existing embedded DB as backend)

- **COVERED**: C8 → `data.md` describes a custom B+ tree, WAL, buffer pool, and page manager built from scratch. No reference to using RocksDB, SQLite, sled, or any other existing storage engine as a backend.
  - The options analysis in data.md (lines 20-48) explored multiple custom approaches — all are greenfield.
  - scale.md's entire concurrency model is custom WAL-based, not wrapping another engine.
- **GAP**: No design doc explicitly states "we are not using an existing embedded database as a backend." This is implicit but worth stating for clarity.
  - **Classification**: nice-to-have — Consider adding a brief note in data.md or integration.md confirming the custom storage engine decision and rationale.

### C9: License (Apache 2.0 or MIT)

- **NOT ADDRESSED**: No design document mentions licensing. The Cargo.toml structure in integration.md (lines 259-264) does not include a `license` field.
  - **Classification**: should-fix — Add `license = "MIT OR Apache-2.0"` to the Cargo.toml specification in integration.md. Dual-licensing is standard Rust ecosystem practice and satisfies the PRD constraint.

### C10: MongoDB-familiar API surface

- **COVERED**: C10 → `api.md` Option 3 "MongoDB-Shaped, SQLite-Spirited" (lines 37-61), `ux.md` Option C (lines 39-44)
  - Same method names: `find`, `insert_one`, `update_one`, `delete_one`, etc. (api.md lines 139-176)
  - Same conceptual types: `Database`, `Collection<T>`, `Cursor<T>` (api.md lines 67-247)
  - Sync signatures (api.md line 13)
  - Progressive disclosure via options structs (api.md lines 179-230)
  - ux.md Journey 1 (lines 67-135) demonstrates the familiar-yet-embedded API
  - Migration table in integration.md (lines 422-436) shows 1:1 mapping

### C11: Migration guide from MongoDB driver

- **COVERED**: C11 → `integration.md` Migration Paths section (lines 419-446)
  - Side-by-side code comparisons (integration.md lines 422-436)
  - Key differences documented (lines 432-438)
  - ux.md Tier 2 documentation (line 377) lists "Migration Guide from MongoDB Rust Driver"
  - ux.md Documentation Tasks table (line 400) tracks the migration guide with "After API stable" dependency
- **GAP**: The PRD says "Documentation must include migration guide" (mandatory), but ux.md classifies it as Tier 2 "Important for Adoption" rather than Tier 1 "Must-Have for Launch."
  - **Classification**: must-fix — Move the migration guide from Tier 2 to Tier 1 in ux.md. The PRD makes it a hard requirement, not a nice-to-have. Update the documentation tasks table accordingly.

### C12: Trademark compliance

- **COVERED**: C12 → `integration.md` line 21 ("Use 'mqlite' in all handshake responses and 'MQL-compatible' in documentation"), `security.md` attack surface #19 (line 192)
  - integration.md line 136: "mqlite does NOT report a MongoDB server version string"
  - api.md lines 371-374: hello response includes `mqlite.version`, not MongoDB version
  - api.md line 380: "Do NOT report as a replica set member"
  - security.md line 192: "Report as 'mqlite' in handshake, not 'mongod'. Use 'MQL-compatible' rather than 'MongoDB-compatible' in marketing."
  - integration.md buildInfo response (lines 138-147): returns `"mqlite": true`

---

## Part B: Non-Goals Coverage

Walk through every NON-GOAL in the PRD and verify the plan properly excludes it (returns errors for unsupported operations, doesn't accidentally implement it, etc.).

### NG1: Replication / sharding

- **PROPERLY EXCLUDED**: NG1 → Design docs consistently describe a single-file, single-machine model
  - api.md line 380: "Do NOT report as a replica set member. `isWritablePrimary: true` signals standalone mode."
  - api.md line 455: "Wire protocol is single-database"
  - integration.md lines 127-132: omits setName, setVersion, hosts, passives, arbiters from hello response
  - scale.md line 333: "No read replicas or read scaling beyond one machine"
  - No design doc mentions replica sets, sharding, config servers, or oplog
- **GAP**: The design docs don't explicitly describe what error to return if a client sends replica set related commands (e.g., `replSetGetStatus`, `replSetInitiate`). These would fall under the general CommandNotFound (code 59) handling in api.md lines 384-395, but this should be verified.
  - **Classification**: nice-to-have — The CommandNotFound handler already covers this implicitly. No specific action needed.

### NG2: Aggregation pipeline

- **PROPERLY EXCLUDED**: NG2 → multiple references confirm exclusion
  - integration.md mongosh compatibility table (line 169): `aggregate` → "NOT supported (code 59)"
  - api.md lines 384-395: unsupported commands return code 59 CommandNotFound, with `aggregate` as the example
  - ux.md line 190: unsupported operators fail loudly with `Error::UnsupportedOperator`
  - integration.md lines 169-171: `aggregate`, `count`, `distinct` all return code 59
- **GAP**: The PRD lists specific aggregation operators (`$group`, `$lookup`, `$unwind`, `$project`) as non-goals, but the design docs only mention the `aggregate` command as unsupported. A client could potentially embed `$expr` (which uses aggregation expressions) in a find query filter. The design docs don't explicitly address `$expr` rejection.
  - **Classification**: should-fix — Add `$expr` to the "Phase 1 out-of-scope" query operators discussion in integration.md or api.md. The PRD already lists `$expr` as out of scope in G2's query operators table. The design should confirm that `$expr` in find filters returns `FailedToParse` (code 9) with a clear message.

### NG3: Change streams

- **PROPERLY EXCLUDED**: NG3 → No design doc mentions change streams, watch cursors, or real-time notification
  - integration.md lines 127-132: omits change stream capabilities from hello response
  - The `watch` command is not in the Phase 1 command list (api.md lines 333-355)
  - Any `watch` command would return code 59 via the CommandNotFound handler

### NG4: Multi-document transactions

- **PROPERLY EXCLUDED**: NG4 → Design docs consistently describe single-document atomicity
  - api.md line 14: findAndModify is "an atomic read-modify-write operation" (single-document)
  - scale.md: entire SWMR model describes single-operation write transactions
  - integration.md lines 127-132: omits `logicalSessionTimeoutMinutes` from hello response (no sessions = no transactions)
  - integration.md constraint #5 (line 554): "No sessions or transactions in wire protocol"
  - integration.md open question #1 (line 565): recommends silently ignoring `lsid` rather than erroring
- **GAP**: The design docs don't explicitly state "single-document atomicity only." The WAL design implies it (each write operation is one commit), but it should be stated explicitly in api.md for clarity.
  - **Classification**: should-fix — Add explicit statement in api.md that mqlite provides single-document atomicity only. Each CRUD operation (`insert_one`, `update_one`, `delete_one`, `find_one_and_update`, etc.) is atomic. `insert_many` with `ordered: false` may partially succeed. There are no multi-document ACID transactions.

### NG5: Full MongoDB feature parity

- **PROPERLY EXCLUDED**: NG5 → Design docs define a bounded operator set
  - integration.md MQL Operator Matrix (lines 448-501): explicit per-operator status
  - api.md lines 384-405: unsupported commands/operators return proper error codes
  - ux.md line 371: compatibility matrix as a Tier 1 documentation deliverable
  - The design consistently uses "Phase 1 operator set" language rather than claiming full compatibility

### NG6: Non-Rust language bindings

- **PROPERLY EXCLUDED**: NG6 → No design doc mentions FFI, C bindings, Python bindings, or Node bindings
  - The wire protocol shim is positioned as the interop mechanism for non-Rust consumers (matching the PRD)
  - integration.md pymongo compatibility (lines 173-193) is via wire protocol, not native bindings
  - ux.md module organization (lines 447-469) shows a pure Rust crate structure with no FFI module

### NG7: Server mode

- **PROPERLY EXCLUDED**: NG7 → Wire protocol positioned as debugging/interop tool
  - security.md line 129: "Wire protocol: bind localhost-only by default, document loudly"
  - api.md line 456: "No authentication in Phase 1"
  - ux.md Journey 3 (lines 194-231): wire protocol is for inspection, not production serving
  - integration.md recommendation (lines 55-61): "Phase 1 Command Set — mongosh + pymongo"
- **GAP**: The security analysis in security.md (lines 68, 76-81) notes that users "will inevitably bind to `0.0.0.0` for convenience, creating a remotely exploitable unauthenticated database." While this is acknowledged, the design doesn't describe any hard prevention mechanism beyond documentation and defaults.
  - **Classification**: nice-to-have — security.md already addresses this thoroughly. The localhost-only default and logging are sufficient for Phase 1.

### NG8: Full-text search

- **PROPERLY EXCLUDED**: NG8 → `$text` and `$where` explicitly excluded
  - PRD G2 out-of-scope operators table lists `$text`
  - security.md line 143: "`$where` and server-side JavaScript: do not implement"
  - api.md line 273: unsupported index types including text indexes return error code 67
  - No design doc mentions full-text search, text indexes, or Atlas Search

### NG9: Geospatial queries

- **PROPERLY EXCLUDED**: NG9 → Geospatial operators excluded from Phase 1 operator set
  - PRD G2 out-of-scope operators table lists `$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere`
  - api.md line 273: unsupported index types including `2dsphere`/`2d` return error code 67
  - No design doc mentions geospatial indexes, GeoJSON, or coordinate queries

### NG10: Collation

- **PROPERLY EXCLUDED**: NG10 → No collation support in Phase 1
  - data.md BSON Key Encoding (lines 141-160): uses raw UTF-8 byte ordering, not locale-aware
  - PRD line 73: "Collation is deferred to Phase 2"
  - No design doc mentions locale-aware string ordering, ICU, or collation parameters
- **GAP**: The design docs don't describe what happens if a client specifies a `collation` option in a find or createIndex command. Should it be silently ignored or return an error?
  - **Classification**: should-fix — Add explicit handling for `collation` parameters in api.md. When a collation option is specified in find, createIndex, or other commands, mqlite should return an error indicating collation is not supported in Phase 1. Silent ignore would mask a correctness issue (queries returning different results than MongoDB due to missing collation).

### NG11: Authentication / encryption at rest

- **PROPERLY EXCLUDED**: NG11 → Security.md extensively analyzes the implications
  - security.md line 197: "No authentication in Phase 1 wire protocol — This is a stated non-goal."
  - security.md line 199: "No encryption at rest in Phase 1"
  - api.md line 457: "No authentication in Phase 1. The `hello` response must not advertise SCRAM or any auth mechanism."
  - integration.md lines 127-132: omits `saslSupportedMechs` from hello response
  - security.md Phase 2 mitigations (lines 145-151): SCRAM-SHA-256, encryption at rest, TLS planned for Phase 2
  - File permissions (0600) are the only protection (security.md line 139, data.md line 447)

### NG12: no_std / WASM

- **PROPERLY EXCLUDED**: NG12 → Design targets `std` Rust only
  - integration.md line 264: `rust-version = "1.70"` (std edition)
  - integration.md cross-compilation targets (lines 320-330): WASM listed as P2 with "(future, in-memory only)"
  - No design doc mentions `#![no_std]`, WASI, or WASM-specific considerations
  - The storage engine design depends on file system operations (POSIX fcntl, file I/O) which require `std`

---

## Part C: Phase 2 Candidates Acknowledgment

The PRD lists Phase 2 candidates. Verify the design acknowledges them without accidentally implementing them.

| Phase 2 Candidate | Design Acknowledgment | Status |
|--------------------|----------------------|--------|
| Aggregation pipeline (`$group`, `$project`) | integration.md line 169: returns code 59 | Properly deferred |
| `$lookup`/joins | Not mentioned in design | OK — falls under aggregation exclusion |
| Python/Node bindings via FFI | Not mentioned | OK — NG6 covers this |
| TTL indexes | api.md line 273: rejects with code 67 | Properly deferred |
| Unique indexes with partial filters | api.md line 273: rejects `partialFilterExpression` | Properly deferred |
| OP_COMPRESSED | integration.md lines 96-103: explicitly deferred to Phase 1.1 | Properly deferred |
| Async API wrapper | api.md line 13: sync-first, no async wrapper | OK — async consumers use wire shim |
| Collation | Not explicitly deferred in design — see NG10 gap | Needs collation rejection handling |
| `mongodump`/`mongorestore` compatibility | integration.md lines 443-446: mentioned as migration path, not Phase 1 | Properly deferred |
| Encryption at rest | security.md lines 118-123, 145-151: Phase 2 | Properly deferred |

---

## Part D: Open Questions Coverage

The PRD has 4 open questions. Verify the design addresses them.

### OQ1: WAL checkpointing — synchronous or asynchronous?

- **ADDRESSED**: scale.md open question #1 (line 341): "Recommendation: synchronous for Phase 1, async as Phase 2 optimization." Also scale.md line 59: "Non-blocking checkpoint (runs between write operations)." The design follows the PRD's recommendation of synchronous for Phase 1.

### OQ2: Should `Database` implement `Drop` with WAL flush?

- **ADDRESSED**: api.md open question #2 (line 471): "Drop does non-blocking close. Explicit `db.close()` for blocking flush." ux.md open question #7 (line 542) also discusses Drop behavior. data.md clean close section (lines 319-327) describes the checkpoint-on-close protocol.

### OQ3: Should OP_COMPRESSED be Phase 1?

- **ADDRESSED**: integration.md lines 96-103: explicitly deferred to Phase 1.1 with pure-Rust-only rationale.

### OQ4: Should compound indexes be Phase 1 MVP or Phase 1.1?

- **ADDRESSED**: data.md lines 215-227: compound indexes are included in Phase 1 with full design. The decision to include them is explicit.

---

## Part E: PRD Sections Not Covered in Round 1 or Round 2

### User Stories / Scenarios
- **COVERED** in Round 1 (Part C). No additional gaps identified.

### Rough Approach
- **COVERED**: The 5-layer architecture and bottom-up build order are reflected in the design docs' layered approach. The sub-phases (1a, 1b, 1c) are acknowledged in the timeline-management context.
- **GAP**: The design docs do not define explicit sub-phase boundaries or deliverables for 1a/1b/1c. The PRD suggests this for timeline management.
  - **Classification**: nice-to-have — Sub-phase boundaries are implementation planning, not design gaps. Can be addressed in the implementation plan.

---

## Summary of Findings

### Must-Fix Items

| # | Finding | PRD Section | Plan Gap | Suggested Fix |
|---|---------|------------|----------|---------------|
| 1 | Migration guide classified as Tier 2, PRD says mandatory | C11 (Business constraint) | ux.md Tier 2 vs PRD requirement | Move migration guide from Tier 2 to Tier 1 in ux.md documentation needs. Update documentation tasks table to reflect mandatory status. |

### Should-Fix Items

| # | Finding | PRD Section | Plan Gap | Suggested Fix |
|---|---------|------------|----------|---------------|
| 2 | No enforcement of pure-Rust transitive deps | C1 (Technical constraint) | Missing C-backend prevention | Add note in integration.md about enforcing pure-Rust backends in transitive deps. Add CI check recommendation. |
| 3 | integration.md hello response missing `mqlite.version` | C6 (Architecture) | Inconsistency between api.md and integration.md | Add `mqlite` field to integration.md handshake response to match api.md. |
| 4 | No license field in Cargo.toml spec | C9 (Business constraint) | integration.md missing license | Add `license = "MIT OR Apache-2.0"` to integration.md Cargo.toml. |
| 5 | `$expr` rejection not explicit | NG2 (Aggregation pipeline) | Missing `$expr` handling | Add `$expr` to explicitly rejected operators in api.md or integration.md. |
| 6 | Single-document atomicity not explicitly stated | NG4 (Multi-document transactions) | api.md lacks atomicity statement | Add atomicity guarantee statement to api.md. |
| 7 | Collation parameter rejection not specified | NG10 (Collation) | No handling for collation option | Add collation parameter error handling to api.md. |

### Nice-to-Have Items

| # | Finding | PRD Section | Plan Gap | Suggested Fix |
|---|---------|------------|----------|---------------|
| 8 | No explicit "custom storage engine" statement | C8 (Resource constraint) | Implicit, not explicit | Consider adding a brief note confirming custom storage engine decision. |
| 9 | Replica set command rejection implicit | NG1 (Replication) | CommandNotFound covers it | Already handled by generic command dispatch. |
| 10 | Sub-phase boundaries not defined | Rough Approach | Implementation planning | Address in implementation plan, not design docs. |

---

## Changes Applied to Design Docs

All must-fix and should-fix items above have been applied as edits to the design documents. See git diff for exact changes.
