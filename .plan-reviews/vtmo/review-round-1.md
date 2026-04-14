# Plan Self-Review Round 1: Completeness + Sequencing

**Design**: `.designs/vtmo/` (api.md, data.md, integration.md, scale.md, security.md, ux.md)
**PRD**: `.prd-reviews/mqlite-embedded-mongodb/prd-draft.md`
**Prior reviews**: `.plan-reviews/vtmo/prd-align-round-{1,2,3}.md`
**Date**: 2026-04-14
**Reviewer**: polecat furiosa

---

## Part A: Completeness Review

Checking for missing infrastructure setup, missing test tasks, missing documentation tasks, missing error handling, implicit dependencies, and tasks too coarse-grained.

### A1: Missing Infrastructure Setup

---

**FINDING C-01**: No CI/CD setup task in the implementation plan.

- **Severity**: must-fix
- **Detail**: integration.md describes CI jobs that must exist: pure-Rust dependency check, cross-compilation targets, benchmark comparison, fuzz testing corpus. The Rough Approach's 7 build steps are silent on when CI is set up. Without CI, purity violations and cross-compile regressions go undetected.
- **Suggested addition**: Add "CI/CD infrastructure setup" as a Phase 0 task before Step 1:
  - Cargo workspace configuration with feature flags
  - GitHub Actions / CI runner with P0 cross-compile targets (x86_64-linux, aarch64-linux, aarch64-darwin)
  - `cargo audit` job for dependency vulnerability scanning
  - Pure-Rust enforcement job (no C deps in direct dependency tree)
  - Benchmark comparison job (`.benchmarks/` baseline storage, 2x regression detection)
  - Fuzz testing corpus tracking

---

**FINDING C-02**: No initial project scaffolding task.

- **Severity**: should-fix
- **Detail**: The plan dives into "Step 1: File format and page manager" with no prior task establishing the Cargo workspace structure (crate layout, feature flags, module organization), minimum Rust version (1.70, per integration.md), or code quality baselines (`#![deny(clippy::unwrap_used)]`, Clippy configuration).
- **Suggested addition**: Add "Project scaffolding" to Phase 0:
  - Create `mqlite` crate with feature flags `wire` and `tracing`
  - Establish module structure per ux.md: `database`, `collection`, `cursor`, `error`, `options`, `index`, `results`, `bson_compat`, `wire/` (feature-gated)
  - Configure Clippy: deny `unwrap_used`, `expect_used` in library code
  - Set `rust-version = "1.70"` in Cargo.toml
  - License: `MIT OR Apache-2.0`

---

**FINDING C-03**: No cross-compilation verification task.

- **Severity**: should-fix
- **Detail**: integration.md lists aarch64-linux and aarch64-darwin as P0 cross-compilation targets. These must be verified at the start, not discovered as issues late in Phase 1b. File locking (`fcntl`) and memory-mapped file handling behave differently across targets.
- **Suggested addition**: Add cross-compile verification to Phase 0 CI setup. Confirm `cargo build --target aarch64-unknown-linux-gnu` succeeds with no C dependencies before starting storage engine implementation.

---

### A2: Missing Test Tasks

---

**FINDING C-04**: Test infrastructure not called out as explicit task.

- **Severity**: must-fix
- **Detail**: R12 defines the full test strategy (property-based, fuzz, crash, compatibility tests) but the Rough Approach doesn't include a task to SET UP this infrastructure. Consequence: test infrastructure gets set up ad-hoc during implementation, or skipped until the end. The B+ tree property tests (R12, data.md section) must be written AS the B+ tree is implemented.
- **Suggested addition**: Add "Test harness setup" to Phase 0:
  - Add `proptest` (or `quickcheck`) for B+ tree invariant testing
  - Add `cargo-fuzz` with initial fuzz targets for BSON parser and wire protocol parser
  - Add `criterion` for benchmarks
  - Define the property-based test framework for B+ tree invariants (8 properties from data.md)

---

**FINDING C-05**: Crash testing harness not tasked.

- **Severity**: should-fix
- **Detail**: G4 acceptance criteria require automated crash testing (kill -9 at random points, verify recovery). integration.md describes the harness (fork child, send SIGKILL, validate). But the Rough Approach has no task for building this harness. Without it, WAL correctness can't be verified.
- **Suggested addition**: Add "Crash test harness" to Phase 1a (storage engine phase), implementing the fork+SIGKILL crash injection framework described in integration.md.

---

**FINDING C-06**: Compatibility test suite setup not tasked.

- **Severity**: should-fix
- **Detail**: integration.md describes running identical operations against MongoDB 8.0 and mqlite to compare results. But acquiring the MongoDB CRUD spec test YAML files, running a MongoDB 8.0 instance (likely via Docker), and curating the applicable pymongo test subset aren't called out as tasks.
- **Suggested addition**: Add "Compatibility test suite setup" to Phase 1c:
  - Clone MongoDB CRUD spec test repository
  - Docker Compose file for MongoDB 8.0 reference instance
  - Curated pymongo 4.x test subset covering Phase 1 commands

---

**FINDING C-07**: `$push` modifier combinations lack dedicated test note.

- **Severity**: should-fix
- **Detail**: integration.md lists `$push` with modifiers `$each`, `$position`, `$sort`, `$slice`. These combinations are complex and frequently misimplemented. No test note calls out modifier combination testing as a specific responsibility.
- **Suggested addition**: Add note in integration.md update operator table that `$push` modifier combinations (`$each` + `$sort`, `$each` + `$position` + `$slice`) require dedicated compatibility tests against MongoDB 8.0 reference.

---

### A3: Missing API Surface

---

**FINDING C-08**: `Database::close()` method missing from API spec.

- **Severity**: must-fix
- **Detail**: api.md Open Question #2 resolves to "Drop does non-blocking close. Explicit `db.close()` for blocking flush." But the `Database Handle` section in api.md doesn't include `pub fn close(&self) -> Result<()>`. The method is referenced in ux.md and the OQ resolution but never formally specified.
- **Suggested fix**: Add to `Database Handle` in api.md:
  ```rust
  /// Flush WAL, checkpoint, and close. Blocks until complete.
  /// Drop performs a non-blocking close; use this for explicit flush guarantees.
  pub fn close(self) -> Result<()>;
  ```

---

**FINDING C-09**: `wal_max_size` missing from `OpenOptions`.

- **Severity**: must-fix
- **Detail**: scale.md and PRD R5 both define "WAL max size (forced checkpoint): 100MB, configurable". But `OpenOptions` in api.md only includes `wal_auto_checkpoint: Option<u32>` (page count threshold). The absolute size limit (`wal_max_size`) is missing from the API spec.
- **Suggested fix**: Add to `OpenOptions` in api.md:
  ```rust
  wal_max_size: Option<u64>,  // Default: 100MB. WAL size forcing a checkpoint regardless of page count.
  ```

---

**FINDING C-10**: `InsertManyResult` definition is inconsistent.

- **Severity**: must-fix
- **Detail**: api.md has two conflicting definitions:
  - **Result Types section** (end of api.md): `pub struct InsertManyResult { pub inserted_ids: HashMap<usize, Bson> }` — no `errors` field
  - **Resolved OQ#3 / InsertManyOptions section** (middle of api.md): defines `InsertManyResult` with `pub inserted_ids: HashMap<usize, Bson>` AND `pub errors: Vec<BulkWriteError>`
  
  The Results Types section is the authoritative API spec and should include `errors`.
- **Suggested fix**: Update the Result Types section to:
  ```rust
  pub struct InsertManyResult {
      pub inserted_ids: HashMap<usize, Bson>,
      pub errors: Vec<BulkWriteError>,   // Non-empty only on partial failure
  }
  ```

---

**FINDING C-11**: `FindOneAndUpdateOptions` struct not formally defined.

- **Severity**: must-fix
- **Detail**: api.md OQ#1 resolves to: "Default is `ReturnDocument::Before`. Configurable via `FindOneAndUpdateOptions { return_document: ReturnDocument::Before | After }`." But no struct definition appears in the API section. The `find_one_and_update`, `find_one_and_replace`, and `find_one_and_delete` methods have `_with_options` variants nowhere defined.
- **Suggested fix**: Add to api.md (Options section):
  ```rust
  pub enum ReturnDocument { Before, After }

  pub struct FindOneAndUpdateOptions {
      /// Default: ReturnDocument::Before (matches MongoDB findAndModify behavior)
      pub return_document: Option<ReturnDocument>,
      /// If true and no document matches, insert the filter document.
      pub upsert: Option<bool>,
  }

  pub struct FindOneAndDeleteOptions {
      pub sort: Option<Document>,   // Which document to delete if filter matches multiple
  }

  pub struct FindOneAndReplaceOptions {
      pub return_document: Option<ReturnDocument>,
      pub upsert: Option<bool>,
  }
  ```
  And update Collection methods to include `_with_options` variants.

---

**FINDING C-12**: Carry-over from round 3 must-fix #1 — `read_only` mode behavior undocumented.

- **Severity**: must-fix
- **Detail**: `OpenOptions` has `read_only: Option<bool>` but api.md provides no documentation of what this mode does. ux.md line 277 mentions it, but the authoritative API spec is api.md.
- **Suggested fix**: Add note to `OpenOptions` in api.md:
  ```rust
  // Note on read_only mode:
  // When `read_only: true`:
  // - WAL replay is SKIPPED (database state is as of last checkpoint)
  // - No writes are attempted, even for recovery
  // - Safe for opening on read-only filesystems (IoT forensic access)
  // - If WAL exists with uncommitted changes, they are NOT visible
  // - SHM file is not created or modified
  ```

---

**FINDING C-13**: Carry-over from round 3 must-fix #3 — `find_one_and_update` default return undocumented.

- **Severity**: must-fix
- **Detail**: api.md OQ#1 resolved to "Default is `ReturnDocument::Before`" but this resolution appears only in the Open Questions section, not in the method signature or docstring for `find_one_and_update`.
- **Suggested fix**: Add doc comment to `find_one_and_update` in api.md:
  ```rust
  /// Returns the document as it appeared BEFORE the update (pre-modification).
  /// To return the post-modification document, use find_one_and_update_with_options
  /// with FindOneAndUpdateOptions { return_document: ReturnDocument::After }.
  pub fn find_one_and_update(&self, filter: Document, update: Document) -> Result<Option<T>>;
  ```

---

**FINDING C-14**: Carry-over from round 3 must-fix #4 — session/concern handling undocumented.

- **Severity**: must-fix
- **Detail**: integration.md OQ#1-2 resolved to: silently ignore `lsid`, `readConcern`, `writeConcern` with DEBUG-level logging. This behavior affects wire protocol compatibility with pymongo 4.x which sends these by default. The resolution exists in integration.md OQ section but not in the wire protocol command dispatch description.
- **Suggested fix**: Add explicit section in integration.md wire protocol architecture:
  > **Silently ignored fields**: The wire protocol command handlers silently ignore `lsid` (logical session ID), `readConcern`, and `writeConcern` fields in all commands. pymongo 4.x sends these by default. Log at DEBUG level: `"Ignoring lsid/readConcern/writeConcern (not supported in mqlite)"`. Do NOT return an error.

---

**FINDING C-15**: Carry-over from round 3 must-fix #5 — blocking index builds undocumented.

- **Severity**: must-fix
- **Detail**: data.md OQ#4 resolved to: "Blocking index builds for Phase 1." But this behavior isn't documented in the `create_index` method spec in api.md. Users need to know that calling `create_index` on a large collection will block all writes for potentially several seconds.
- **Suggested fix**: Add doc comment in api.md:
  ```rust
  /// BLOCKING: Acquires the writer lock and holds it until the index is fully built.
  /// For a 100K-document collection, this may take several seconds.
  /// Background index builds are planned for Phase 2.
  pub fn create_index(&self, model: IndexModel) -> Result<String>;
  ```

---

**FINDING C-16**: Security Phase 1 hardening tasks absent from implementation plan.

- **Severity**: must-fix
- **Detail**: security.md identifies 7 "must-do" Phase 1 mitigations (BSON hardening, file permissions 0600, regex safety, checksummed pages, resource limits, `$where`/`$function` rejection, wire protocol localhost-only). These are absent from the Rough Approach's 7 implementation steps.
- **Suggested addition**: Add "Security hardening" as a sub-task in Phase 1a (storage, enforcing resource limits, checksums) and Phase 1b (BSON validation depth/size limits at parse boundary, file permission setting, `$where`/`$function` rejection):
  - `Database::open()` sets file permissions to 0600
  - BSON parsing enforces depth ≤ 100, size ≤ 16MB, field count ≤ 10,000
  - `$regex` uses Rust `regex` crate only (no PCRE); per-query timeout
  - `$where` and `$function` return explicit `CommandNotFound` (never implement)
  - Wire protocol binds 127.0.0.1 by default; emit startup warning banner

---

**FINDING C-17**: Documentation tasks absent from Rough Approach.

- **Severity**: must-fix
- **Detail**: Phase 1 DoD item #10 requires documentation completion. ux.md has a detailed Tier 1 documentation plan (6 documents, all required for launch). The Rough Approach has no documentation steps.
- **Suggested addition**: Add documentation tasks to Phase 1b and 1c:
  - **Phase 1b (with API layer)**: Begin API reference documentation on docs.rs. Draft README/Quick Start.
  - **Phase 1c (with wire protocol)**: Write Wire Protocol Security Advisory. Finalize Compatibility Matrix. Finalize Migration Guide. Complete Concurrency Guide. Complete Error Guide.
  - **Launch gate**: All Tier 1 docs complete before Phase 1 is declared done.

---

**FINDING C-18**: Buffer pool implementation too coarse-grained.

- **Severity**: should-fix
- **Detail**: Step 3 bundles "Buffer pool with CLOCK-sweep eviction" and "B+ tree" together. The buffer pool is a significant implementation component (frame allocation, pin/unpin, dirty tracking, CLOCK sweep, mixed 4KB/32KB pools). It's also a dependency of the WAL reader path (Step 2). Treating it as part of Step 3 is too coarse.
- **Suggested addition**: Split Step 3 into:
  - **Step 3a**: Buffer pool (CLOCK-sweep, mixed page sizes, pin/unpin, dirty flush)
  - **Step 3b**: B+ tree using buffer pool (split/merge, sibling pointers, overflow pages)

---

### A4: Summary of Completeness Findings

| ID | Finding | Severity |
|----|---------|----------|
| C-01 | No CI/CD setup task | must-fix |
| C-04 | Test infrastructure not tasked | must-fix |
| C-08 | `Database::close()` missing from API | must-fix |
| C-09 | `wal_max_size` missing from OpenOptions | must-fix |
| C-10 | `InsertManyResult` inconsistent | must-fix |
| C-11 | `FindOneAndUpdateOptions` not defined | must-fix |
| C-12 | `read_only` mode behavior undocumented | must-fix |
| C-13 | `find_one_and_update` default return undocumented | must-fix |
| C-14 | Session/concern handling undocumented | must-fix |
| C-15 | Blocking index builds undocumented | must-fix |
| C-16 | Security hardening tasks absent | must-fix |
| C-17 | Documentation tasks absent from plan | must-fix |
| C-02 | No project scaffolding task | should-fix |
| C-03 | No cross-compilation verification task | should-fix |
| C-05 | Crash testing harness not tasked | should-fix |
| C-06 | Compatibility test suite setup not tasked | should-fix |
| C-07 | `$push` modifier combinations lack test note | should-fix |
| C-18 | Buffer pool implementation too coarse-grained | should-fix |

---

## Part B: Sequencing Review

Checking for ordering problems, hidden dependencies, unnecessary serial dependencies, parallelism opportunities, and circular dependencies.

### B1: Ordering Problems

---

**FINDING S-01**: Test infrastructure must precede implementation, not run concurrently or after.

- **Severity**: must-fix
- **Detail**: The Rough Approach starts with Step 1 (file format) with no prior step establishing test infrastructure. The B+ tree property tests must be written concurrent with B+ tree implementation. Setting up proptest/criterion after Step 3 means the B+ tree was built without continuous property verification.
- **Suggested reorder**: Add Step 0 "Project scaffolding and test harness setup" before Step 1. Step 0 must complete before implementation begins.

---

**FINDING S-02**: BSON key encoding must precede B+ tree implementation.

- **Severity**: must-fix
- **Detail**: data.md states BSON comparison ordering "is non-retrofittable" — if wrong, every index is corrupt. Currently, BSON key encoding is bundled with Step 3 (B+ tree). Risk: B+ tree is built and partially tested before key encoding is verified correct. Edge cases (NaN, -0.0, Decimal128, cross-type comparison) discovered late require B+ tree rework.
- **Suggested reorder**: Split current Step 3 into:
  - **Step 3a**: Implement and unit-test BSON comparison key encoding. Test all type combinations (MinKey/MaxKey, Null, Numbers, String, Object, Array, BinData, ObjectId, Boolean, Date, Timestamp, RegExp). Must be complete and verified before Step 3b.
  - **Step 3b**: Buffer pool (CLOCK-sweep, mixed 4KB/32KB page sizes)
  - **Step 3c**: B+ tree using encoding from 3a and buffer pool from 3b

---

**FINDING S-03**: Buffer pool is needed by WAL reader, not just B+ tree.

- **Severity**: should-fix
- **Detail**: The WAL read path (Step 2) needs a page cache for recently-accessed WAL frames (performance) and main file pages (WAL fallback reads). Step 2's WAL implementation must either operate without any buffer pool (suboptimal, requiring rework in Step 3) or Step 3's buffer pool must be moved earlier.
- **Suggested reorder**: Move buffer pool to Step 2.5 (between WAL and B+ tree), making it available to both the WAL reader and B+ tree.

---

**FINDING S-04**: In-memory mode affects storage engine, not just API layer.

- **Severity**: must-fix
- **Detail**: R8 defines `Database::open_in_memory()`. This is listed in the Rough Approach only as part of Step 6 (Native API). But in-memory mode requires storage engine support: the page manager (Step 1) must allocate pages from RAM instead of a file; the WAL (Step 2) must be skippable; the SHM file (Step 2) must not be created. If in-memory mode is only considered at Step 6, it requires rework of Steps 1 and 2.
- **Suggested fix**: Add to Step 1: "The page manager must support both file-backed and memory-backed modes. In-memory mode allocates pages from a Vec<Vec<u8>> instead of the file. This flag propagates through all storage engine layers." Add to Step 2: "WAL and SHM are skipped entirely in in-memory mode."

---

**FINDING S-05**: Error taxonomy should be established before implementation layers, not discovered per-layer.

- **Severity**: should-fix
- **Detail**: Each storage layer (Step 1-7) returns errors. Without a pre-defined taxonomy, each layer invents its own error format and later reconciliation is needed. api.md's Error taxonomy is comprehensive, but it's not called out as a preliminary step.
- **Suggested reorder**: Add to Step 0: "Define error taxonomy from api.md (Error enum, MongoDB error codes, code() method). Implement in `src/error.rs`. All subsequent layers use this shared error type."

---

**FINDING S-06**: SHM memory layout must be clarified relative to buffer pool.

- **Severity**: should-fix
- **Detail**: The SHM file (Step 2) contains a WAL index hash table and reader slot tracking. It's unclear whether this uses the buffer pool's memory management or a separate fixed-size in-process data structure. If it uses the buffer pool, buffer pool (Step 3a) must precede WAL (Step 2).
- **Suggested fix**: Clarify in data.md/scale.md WAL section: "The SHM WAL index is a fixed-size in-process memory region (not managed by the data buffer pool). It is mmap'd from the SHM file. This is separate from the data buffer pool that caches B+ tree pages."

---

### B2: Hidden Dependencies

---

**FINDING S-07**: Catalog schema and index metadata format must be defined before Step 4.

- **Severity**: should-fix
- **Detail**: The catalog stores index metadata that the query planner (Step 5) reads. If the catalog schema evolves during Step 4 development, Step 5 needs to be updated. The interface between catalog (Step 4) and query planner (Step 5) should be designed upfront as a data contract.
- **Suggested fix**: Add design artifact requirement: "Before starting Step 4, produce a catalog schema document defining collection entry format, index entry format, and all metadata fields. This schema is the contract between Step 4 (catalog implementation) and Step 5 (query planner)."

---

**FINDING S-08**: Compatibility tests split by path (native API vs. wire protocol).

- **Severity**: should-fix
- **Detail**: integration.md's compatibility test suite runs tests against both native API and wire protocol paths. Currently, the plan implies compatibility testing only after wire protocol (Step 7). But BSON round-trip, query operator correctness, and error code verification can all run against the native API (Step 6) — several phases earlier.
- **Suggested reorder**: Split compatibility testing:
  - After Step 6 (native API complete): Run native API compatibility tests (BSON round-trip, operator correctness, error codes, insert_many semantics)
  - After Step 7 (wire protocol complete): Run wire protocol compatibility tests (mongosh, pymongo test suite, full 18-command surface)

---

### B3: Parallelism Opportunities

---

**FINDING S-09**: Error types and public API types can be scaffolded before storage engine.

- **Severity**: should-fix
- **Detail**: `mqlite::Error`, `Result<T, Error>`, `Database`, `Collection<T>`, `Cursor<T>`, `FindOptions`, `UpdateOptions`, `IndexModel`, `InsertOneResult`, etc. can be defined as stub/placeholder types before any storage implementation exists. This enables test code and documentation to be drafted earlier, and makes the API surface visible for review.
- **Suggested opportunity**: In Step 0, scaffold all public types as `pub struct Database { /* TODO */ }` etc. This is parallelizable with storage engine implementation.

---

**FINDING S-10**: Wire protocol frame parser is independent of storage engine.

- **Severity**: should-fix (low priority)
- **Detail**: OP_MSG parsing (framing, section kinds, checksum validation, message size limits) has zero dependency on the storage engine. It can be implemented and fuzz-tested while Steps 1-5 are underway.
- **Suggested opportunity**: Note in Rough Approach that the OP_MSG frame parser can be built in parallel with storage engine work. Command handlers (which call the native API) must wait for Step 6.

---

**FINDING S-11**: Performance benchmarks should be measured progressively, not just at launch.

- **Severity**: should-fix
- **Detail**: G7 and integration.md describe benchmark CI with 2x regression detection. But if benchmarks are only collected after Phase 1c is complete, there's no baseline to compare against during development. Regressions introduced early won't be detected until launch.
- **Suggested fix**: Add benchmark checkpoints: after Phase 1a (storage engine raw I/O), after Phase 1b (query throughput), after Phase 1c (wire protocol overhead). Store baselines in `.benchmarks/` from each phase checkpoint.

---

### B4: Circular Dependency Check

No circular dependencies found in the 7-step bottom-up build order. Each step has clear prerequisites:
- Step 1 depends on: Rust stdlib, `bson` crate, `crc32c` crate
- Step 2 depends on: Step 1
- Step 3 depends on: Step 1 (BSON encoding), Step 2 (WAL for crash recovery)
- Step 4 depends on: Step 3
- Step 5 depends on: Step 4
- Step 6 depends on: Step 5
- Step 7 depends on: Step 6

No cycles. ✓

---

### B5: Summary of Sequencing Findings

| ID | Finding | Severity |
|----|---------|----------|
| S-01 | Test infrastructure must precede implementation | must-fix |
| S-02 | BSON key encoding must precede B+ tree | must-fix |
| S-04 | In-memory mode affects Steps 1-2, not just Step 6 | must-fix |
| S-03 | Buffer pool needed by WAL (Step 2), not just B+ tree | should-fix |
| S-05 | Error taxonomy should be established in Step 0 | should-fix |
| S-06 | SHM vs. buffer pool memory layout unclear | should-fix |
| S-07 | Catalog schema must be defined before Step 4 | should-fix |
| S-08 | Compatibility tests can split: native API (Step 6) vs. wire (Step 7) | should-fix |
| S-09 | Public API types can be scaffolded in Step 0 | should-fix |
| S-10 | Wire protocol parser is independent of storage engine | should-fix |
| S-11 | Benchmark baselines should be collected progressively | should-fix |

---

## Part C: Revised Rough Approach

The following revised implementation sequence incorporates all must-fix items:

### Step 0: Project scaffolding and test infrastructure (NEW)

Before any implementation:
1. Create Cargo workspace: `mqlite` crate with feature flags `wire`, `tracing`
2. Configure Cargo.toml: `rust-version = "1.70"`, `license = "MIT OR Apache-2.0"`
3. Module structure per ux.md
4. Clippy: `#![deny(clippy::unwrap_used, clippy::expect_used)]` in library code
5. Set up CI: cross-compile (x86_64-linux, aarch64-linux, aarch64-darwin), `cargo audit`, pure-Rust dependency check
6. Add `proptest`, `criterion`, `cargo-fuzz` to dev dependencies
7. Define initial fuzz targets: BSON parser, OP_MSG frame parser
8. Implement error taxonomy from api.md in `src/error.rs` (all variants, error codes, `code()` method)
9. Scaffold public API types as stubs: `Database`, `Collection<T>`, `Cursor<T>`, result types, options types

### Step 1: File format and page manager

Same as before, plus:
- **In-memory mode support**: Page manager accepts `PageManagerMode::File(path)` or `PageManagerMode::InMemory`. In-memory allocates pages from `Vec<Vec<u8>>`. All subsequent layers use this abstraction.
- **File creation permissions**: Set 0600 on new `.mqlite` files.
- **Security**: `O_NOFOLLOW` on file open to prevent symlink attacks.

### Step 2: WAL implementation

Same as before, plus:
- **In-memory mode**: WAL and SHM are completely bypassed in in-memory mode. Writes go directly to the page manager's in-memory store.
- **SHM clarification**: The SHM WAL index is a fixed-size mmap'd region, separate from the data buffer pool. Its memory layout is defined here, not in Step 3.

### Step 3a: BSON key encoding (NEW — extracted from Step 3)

Before any B+ tree implementation:
1. Implement MongoDB BSON comparison ordering key encoding for all 14 type categories
2. Unit tests for all type tags, edge cases: NaN, -0.0, Decimal128, cross-type comparison
3. Compound index concatenation with per-field sort direction (XOR 0xFF for descending)
4. Property test: for any two BSON values A < B (by MongoDB ordering), `encode(A) < encode(B)` by `memcmp`
5. **Must be fully verified before Step 3b begins**

### Step 3b: Buffer pool (NEW — extracted from Step 3)

1. CLOCK-sweep eviction algorithm
2. Separate pools for 4KB (internal nodes) and 32KB (leaf/overflow) pages
3. Pin/unpin with dirty tracking
4. Available to both WAL reader path (Step 2) and B+ tree (Step 3c)

### Step 3c: B+ tree storage engine (was Step 3)

Uses encoding from 3a and buffer pool from 3b. Property-based tests for all 8 invariants from data.md during implementation.

### Steps 4-7: Unchanged in order

Step 4 (Catalog + indexes), Step 5 (Query engine), Step 6 (Native API), Step 7 (Wire protocol).

### Documentation and testing additions

- **After Step 6**: Run native API compatibility tests. Begin API reference, README, Migration Guide.
- **After Step 7**: Run wire protocol compatibility tests (mongosh, pymongo). Complete all Tier 1 documentation. Wire Protocol Security Advisory.
- **CI benchmarks**: Add baseline checkpoints after Steps 1a, 1b, 1c.

---

## Part D: Changes Applied to Design Docs

The following must-fix changes are applied to api.md and integration.md:

### api.md changes

1. **Added `close()` method** to Database Handle
2. **Added `wal_max_size` field** to OpenOptions
3. **Updated `InsertManyResult`** in Result Types section to include `errors`
4. **Added `FindOneAndUpdateOptions`, `FindOneAndDeleteOptions`, `FindOneAndReplaceOptions`**
5. **Updated `read_only` note** in OpenOptions to document WAL skip behavior
6. **Added doc comment** to `find_one_and_update` about `ReturnDocument::Before` default
7. **Added doc comment** to `create_index` about blocking behavior

### integration.md changes

1. **Added silently-ignored fields section** (lsid, readConcern, writeConcern) to wire protocol command dispatch
2. **Added compatibility test split** (native API vs. wire protocol)

---

## Part E: Overall Quality Assessment

| Dimension | Status | Notes |
|-----------|--------|-------|
| PRD alignment | ✓ COMPLETE | All PRD requirements, goals, constraints, and stories covered (per round 3) |
| API surface completeness | PARTIAL | Several missing methods/structs found (C-08 through C-11); fixed in this round |
| Implementation plan completeness | PARTIAL | Missing CI, test infrastructure, documentation tasks; fixed in this round |
| Sequencing correctness | PARTIAL | BSON key encoding and in-memory mode must be front-loaded; fixed in this round |
| Security completeness | PARTIAL | Phase 1 hardening tasks not in Rough Approach; noted as must-fix |
| Documentation plan | PARTIAL | Tier 1 docs not linked to implementation phases; fixed in this round |

**Verdict**: The design docs are comprehensive at the architectural level. The gaps are primarily in implementation plan details (when/how to set up infrastructure, missing API surface details, sequencing of prerequisites). After applying the fixes in this round, the plan is implementation-ready.
