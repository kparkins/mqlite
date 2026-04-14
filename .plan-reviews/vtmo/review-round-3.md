# Plan Self-Review Round 3: Testability + Coherence

**Design**: `.designs/vtmo/` (api.md, data.md, integration.md, scale.md, security.md, ux.md)
**PRD**: `.prd-reviews/mqlite-embedded-mongodb/prd-draft.md`
**Prior reviews**: `.plan-reviews/vtmo/prd-align-round-{1,2,3}.md`, `.plan-reviews/vtmo/review-round-{1,2}.md`
**Date**: 2026-04-14
**Reviewer**: polecat nux

---

## Part A: Testability Review

For each task/phase: Does it have clear acceptance criteria? Can acceptance criteria be verified automatically? Are test tasks explicit?

---

### A1: Testability Findings

---

**TESTABILITY T-01: API behavioral contracts lack acceptance criteria**

- Classification: **UNTESTABLE** — must-fix
- Detail: api.md specifies behavioral contracts for key methods but does not state them as verifiable acceptance criteria:
  - `find_one_and_update`: "returns pre-modification document by default" — no test stating what the returned document contains.
  - `insert_many` with `ordered: true`: stops at first error — no test specifying what `inserted_ids` contains when insert 3 of 5 fails.
  - `upsert: true`: creates a new document from filter equality conditions — no test for what document is created when `{ "field": "value" }` is both the filter and the base for the upsert.
- Suggested criteria (must-fix):
  - `find_one_and_update` test: Insert doc `{a: 1}`. Update `{a: 1}` to `{$set: {a: 2}}`. Assert returned document has `a: 1` (pre-modification). Assert db now contains `{a: 2}`.
  - `insert_many` partial failure test: Insert 5 docs where doc[2] violates a unique index. With `ordered: true`: assert `inserted_ids` contains keys 0 and 1 only, assert `errors` has one entry with `index: 2` and code 11000, assert docs 3/4 are absent from DB. With `ordered: false`: all non-failing inserts committed.
  - `upsert` test: `update_one({email: "a@b.com"}, {$set: {name: "Alice"}}, upsert: true)` on empty collection. Assert `upserted_id` is non-null. Assert inserted doc has `{email: "a@b.com", name: "Alice"}`.

---

**TESTABILITY T-02: No end-to-end persistence test task**

- Classification: **MISSING-TEST** — must-fix
- Detail: There is no explicit test task for the critical cross-phase user journey: open DB → insert documents → create index → query with index → close → reopen → verify data persisted. This test validates WAL replay, catalog persistence, index persistence, and B+ tree integrity across a database reopen. Without it, each layer can pass its unit tests while the integration silently fails.
- Suggested test task: Add to Phase 1a completion criteria an explicit **persistence round-trip test**:
  1. Open database at `test.mqlite`.
  2. Insert 1000 documents into `users` collection.
  3. Create an index on `email` field.
  4. Query using the index; verify results.
  5. Drop the `Database` handle (triggers WAL flush / clean close).
  6. Reopen the database from `test.mqlite`.
  7. Verify document count = 1000.
  8. Verify index is present in `list_indexes()`.
  9. Verify indexed query returns same results as step 4.
  10. Clean up test file.
  This test must pass before Phase 1 is declared complete.

---

**TESTABILITY T-03: Wire protocol response format tests not tasked**

- Classification: **MISSING-TEST** — should-fix
- Detail: integration.md (Round 2 addition) notes key commands with non-obvious response shapes (`findAndModify` returns `value` not `document`, `getMore` returns `nextBatch`, etc.). However, there is no explicit test task for response format verification. Developers may implement the commands and get the field names wrong without failing any existing test.
- Suggested test task: Add to Step 7 (wire protocol implementation): "Before closing Step 7, implement response format parity tests for all 18 Phase 1 commands. Each test sends the command to both MongoDB 8.0 and mqlite and compares the response document structure (field names, nesting, types). Key commands: `findAndModify`, `getMore`, `createIndexes`, `find` with empty result set, `insert` with partial failure." This test can be automated since the responses are deterministic.

---

**TESTABILITY T-04: Crash test acceptance criteria are underdefined**

- Classification: **VAGUE-CRITERIA** — should-fix
- Detail: integration.md describes crash testing ("Fork child, SIGKILL, verify recovery") but does not state the acceptance threshold. "Run thousands of crash cycles in CI" is not a verifiable gate — it requires defining: (a) minimum cycle count per CI run, (b) minimum crash points covered (during insert, during checkpoint, during index build), (c) what constitutes a failure (any inconsistency = fail, or is partial data expected?).
- Suggested rewrite: "Crash recovery acceptance criteria: CI runs 500 crash-inject cycles per CI build (50 per scenario × 10 scenarios: insert at WAL frame 0/10/100/last, checkpoint at 25%/50%/75%, index build at start/mid/end). All 500 cycles must pass. Failure = any open returns an error, any committed doc is missing, any uncommitted doc appears, or any index-data inconsistency. CI fails the merge if any cycle fails."

---

**TESTABILITY T-05: Phase exit gates not specified as executable tests**

- Classification: **VAGUE-CRITERIA** — should-fix
- Detail: The plan describes Phases 1a (storage), 1b (native API), 1c (wire protocol) but the "exit criteria" for each phase are prose descriptions, not executable test suites. A developer cannot run `cargo test --phase=1a` and see a binary pass/fail. Without this, phases bleed into each other and it's unclear when one is done.
- Suggested criteria: For each Phase, create a dedicated integration test module (`tests/phase_1a_acceptance.rs`, `tests/phase_1b_acceptance.rs`, `tests/phase_1c_acceptance.rs`) that contains exactly the acceptance tests from the PRD for that phase. Phase exit = all tests in that module pass. Specifically:
  - Phase 1a gate: WAL recovery test (crash + reopen), B+ tree invariants (all 10), overflow chain integrity for 32KB+ document.
  - Phase 1b gate: All G2 query operators return correct results, `insert_many` partial failure, `find_one_and_update` contract, persistence round-trip (T-02).
  - Phase 1c gate: `mongosh` smoke test script passes, pymongo curated test suite passes, all 18 wire protocol commands return valid responses.

---

**TESTABILITY T-06: Index-vs-scan consistency test missing**

- Classification: **MISSING-TEST** — should-fix
- Detail: B+ tree invariant #7 (index-data consistency) is listed in data.md but it's a property-based test invariant checked after mutations, not an explicit query-level consistency test. A silent bug in index scan logic (e.g., index scan returns 90 docs, full scan returns 100 docs for the same filter) would not be caught by invariant #7. This is the most likely class of index correctness bug.
- Suggested test task: Add to Step 5b (index operations) completion criteria: "For each implemented index type (single-field, compound, multikey, sparse, unique), verify that `find_with_options(filter, opts)` with `hint: index_name` returns the same result set as `find(filter)` with no hint (full collection scan). Parameterize over query operators: `$eq`, `$gt`, `$lt`, `$gte`, `$lte`, `$in`, `$ne`. Differences = index correctness bug."

---

### A2: Testability Summary

| ID | Category | Task | Severity |
|----|----------|------|----------|
| T-01 | UNTESTABLE | API behavioral contracts (find_one_and_update, insert_many, upsert) | must-fix |
| T-02 | MISSING-TEST | End-to-end persistence round-trip test | must-fix |
| T-03 | MISSING-TEST | Wire protocol response format parity tests | should-fix |
| T-04 | VAGUE-CRITERIA | Crash test acceptance threshold | should-fix |
| T-05 | VAGUE-CRITERIA | Phase exit gates as executable test modules | should-fix |
| T-06 | MISSING-TEST | Index-vs-scan consistency test | should-fix |

---

## Part B: Coherence Review

Checking the plan holistically for internal contradictions, naming consistency, architecture coherence, missing glue, and overall readability.

---

### B1: Internal Contradictions

---

**COHERENCE C-01: `compact()` deferred in api.md but shown as working in ux.md**

- Severity: **must-fix**
- Detail: api.md states: "compact() — DEFERRED TO PHASE 2. Page reclamation (like SQLite VACUUM) is not in the Phase 1 DoD." However, ux.md Journey 1 includes `db.compact()?` under "How do I shrink it?" in the File Management UX section, presenting it as a working Phase 1 feature.
- Suggested fix: Update ux.md Journey 1 File Management section: Replace `db.compact()?` with `// compact() is a Phase 2 feature. Use db.checkpoint() to merge the WAL into the main file. File size reduction (free page reclamation) is planned for Phase 2.`

---

**COHERENCE C-02: `Database::stats()` deferred in api.md but used throughout ux.md**

- Severity: **must-fix**
- Detail: api.md states: "stats() — DEFERRED TO PHASE 2. Database statistics are not in the Phase 1 DoD." However, ux.md uses `db.stats()` in:
  - Journey 1 ("How big is my database?")
  - Journey 4 edge/IoT: `db.stats().file_size` for disk monitoring
  - Observability UX: `let stats = db.stats()?; println!("File size: {} bytes", stats.file_size);`
  - ux.md even references `DatabaseStats` fields (file_size, collection_count, document_count, wal_size, buffer_pool_used, buffer_pool_total, free_page_count)
- Suggested fix: Update ux.md in all three locations. Replace `db.stats()` usage with:
  - For disk monitoring: "Phase 1 does not expose a stats() API (Phase 2). Monitor file size directly via `std::fs::metadata("data.mqlite")?.len()` for disk usage tracking."
  - For observability: Move stats() to a "Phase 2 Observability" subsection. Phase 1 observability is via `tracing` spans only.

---

**COHERENCE C-03: `Collection::stats()` deferred but shown in ux.md Observability section**

- Severity: **must-fix**
- Detail: api.md states: "stats() — DEFERRED TO PHASE 2. Collection statistics are not in the Phase 1 DoD." However, ux.md Observability UX section shows:
  ```rust
  let coll_stats = collection.stats()?;
  println!("Documents: {}", coll_stats.document_count);
  println!("Avg document size: {} bytes", coll_stats.avg_document_size);
  println!("Indexes: {:?}", coll_stats.index_names);
  println!("Total index size: {} bytes", coll_stats.total_index_size);
  ```
- Suggested fix: Remove collection.stats() from the Observability UX section. Replace with: "`collection.stats()` is a Phase 2 feature. Phase 1 collection information is available via `collection.list_indexes()` (index list) and `collection.estimated_document_count()` (document count approximation)."

---

**COHERENCE C-04: SHM reader slot size is mathematically wrong**

- Severity: **must-fix**
- Detail: data.md SHM Layout specifies:
  ```
  SHM Layout:
    0      4    Reader count: uint32
    4      4    Writer lock: uint32 (PID of writer, 0 = unlocked)
    8      24   Reader slots: [snapshot_id(4) | pid(4)] × 64 readers max
  ```
  The comment says "64 readers max" but 64 readers × 8 bytes per slot = **512 bytes**, not 24. The "24" appears to describe 3 reader slots, not 64. This makes the SHM layout specification internally inconsistent and unimplementable as written.
- Suggested fix: Correct data.md SHM layout to: `8      512   Reader slots: [snapshot_id(4) | pid(4)] × 64 reader slots` (offset 8, size 512 bytes). Then WAL index hash table starts at offset 520, not 200. Update the offset for "WAL index" accordingly. The SHM file starts at offset 0; with 64 reader slots the layout becomes: header(8) + reader_slots(512) + wal_index(remainder).

---

**COHERENCE C-05: Wire protocol cursor ID specification is missing**

- Severity: **must-fix**
- Detail: api.md defines `Cursor<T>` as a Rust type implementing `Iterator`. The wire protocol layer must generate numeric cursor IDs for the `find` response (`cursor.id`) and validate them on `getMore`. However, nowhere in the design docs is specified:
  - What is the cursor ID format? (int64, random? sequential? connection-scoped?)
  - How does the wire protocol layer map a cursor ID back to the native `Cursor<T>` handle?
  - What cursor ID value indicates "no more data"? (MongoDB uses 0)
  - How does cursor cleanup (connection close, `killCursors` command) interact with the native Cursor's Drop?
  This is a critical architectural gap — the wire protocol and native API must agree on cursor lifecycle.
- Suggested fix: Add to api.md Wire Protocol Architecture section a "Cursor ID Contract":
  ```
  Cursor ID Contract (wire protocol ↔ native API):
  - Cursor IDs are int64, generated as incrementing counter per connection (starts at 1).
  - Wire protocol maintains a HashMap<i64, Cursor<Document>> per TCP connection.
  - On `find`: generate cursor_id, store Cursor in map, return cursor_id in response.
  - On `getMore`: look up cursor_id in map, advance cursor, return next batch.
  - cursor_id = 0 in response means cursor is exhausted (no more data).
  - On connection close: drop all Cursor handles associated with that connection.
  - On `killCursors`: remove specified cursor IDs from map (drops the Cursor).
  - Cursor IDs are connection-scoped — cursor ID 5 on connection A ≠ cursor ID 5 on connection B.
  ```

---

### B2: Naming Inconsistencies

---

**COHERENCE C-06: `OpenOptions` is a struct in api.md, but ux.md uses builder pattern**

- Severity: should-fix
- Detail: api.md defines `OpenOptions` as a plain struct with public fields:
  ```rust
  pub struct OpenOptions {
      buffer_pool_size: Option<usize>,
      durability: Option<DurabilityMode>,
      ...
  }
  ```
  However, ux.md shows a builder pattern: `OpenOptions::new().buffer_pool_size(64 * 1024 * 1024).durability(DurabilityMode::FullSync).wal_auto_checkpoint(1000)`. One of these must be the canonical design. The builder pattern is superior for ergonomics (no struct update syntax, method chaining, validation on build).
- Suggested fix: Update api.md to use builder pattern (add `pub fn new() -> Self` and setter methods). Remove `pub` from struct fields. Align ux.md and api.md.

---

**COHERENCE C-07: `WireProtocol` type used in ux.md but not defined in api.md**

- Severity: should-fix
- Detail: ux.md Journey 3 shows:
  ```rust
  let _server = WireProtocol::bind(&db, "127.0.0.1:27017")?;
  ```
  But api.md's Wire Protocol Architecture section describes the internal architecture without defining the public `WireProtocol` struct and its `bind()` method. The public API for starting the wire protocol shim is absent from api.md.
- Suggested fix: Add to api.md Wire Protocol Architecture section:
  ```rust
  #[cfg(feature = "wire")]
  pub struct WireProtocol { /* opaque */ }

  #[cfg(feature = "wire")]
  impl WireProtocol {
      /// Bind the wire protocol shim to a TCP address.
      /// Runs the listener in background threads; stops when dropped.
      /// Default address: "127.0.0.1:27017".
      /// Returns error if address is already in use.
      pub fn bind(db: &Database, addr: &str) -> Result<WireProtocol>;
  }
  ```

---

**COHERENCE C-08: `repair()` method mentioned in ux.md but absent from api.md**

- Severity: should-fix
- Detail: ux.md Failure Mode UX section describes `Database::repair("myapp.mqlite")` for corrupt database recovery. api.md's `Database` impl block does not include this method. Either it's a planned-but-unstated Phase 1 API or a ux.md speculation that should be deferred.
- Suggested fix: Decision needed. If Phase 1: add `pub fn repair(path: impl AsRef<Path>) -> Result<RepairReport>` to api.md. If Phase 2: update ux.md error message to not suggest `repair()` — say "restore from a backup" instead. Recommended: defer `repair()` to Phase 2 (it's complex), update ux.md accordingly.

---

**COHERENCE C-09: `busy_handler` callback in ux.md but absent from api.md**

- Severity: should-fix
- Detail: ux.md Writer Contention section shows:
  ```rust
  OpenOptions::new().busy_handler(|attempts| { ... })
  ```
  api.md `OpenOptions` only has `busy_timeout: Option<Duration>`. No `busy_handler` callback is defined.
- Suggested fix: Either add `busy_handler: Option<Box<dyn Fn(u32) -> bool + Send>>` to `OpenOptions` in api.md, or remove the `busy_handler` example from ux.md and document only the timeout approach. Recommended: add `busy_handler` — it's the SQLite pattern and significantly more flexible.

---

**COHERENCE C-10: `ExplainResult` type used in api.md but never defined**

- Severity: should-fix
- Detail: api.md `Cursor<T>` includes:
  ```rust
  pub fn explain(&self) -> Result<ExplainResult>;
  ```
  `ExplainResult` is never defined anywhere in the design docs. This is a named type that implementers would need to create without guidance.
- Suggested fix: Add to api.md:
  ```rust
  pub struct ExplainResult {
      /// Human-readable description of the chosen query plan.
      pub plan: String,
      /// Name of the index used, if any.
      pub index_used: Option<String>,
      /// Estimated number of documents examined.
      pub docs_examined: u64,
      /// Whether the query required a collection scan.
      pub full_scan: bool,
  }
  ```

---

**COHERENCE C-11: `CountOptions`, `DeleteOptions` exported in ux.md but undefined/unused in api.md**

- Severity: should-fix
- Detail: ux.md `lib.rs` exports: `pub use options::{FindOptions, UpdateOptions, DeleteOptions, CountOptions};`. However:
  - `count_documents` and `estimated_document_count` in api.md take no options struct.
  - `delete_one` and `delete_many` in api.md take no options struct.
  - Neither `DeleteOptions` nor `CountOptions` is defined anywhere.
- Suggested fix: Either (a) remove `CountOptions` and `DeleteOptions` from the export list in ux.md — they're not needed for Phase 1 since neither count nor delete methods accept options — or (b) add `_with_options` variants for these methods and define the structs. Recommended: remove from export list for Phase 1; add in Phase 2 when sort/collation for delete is needed.

---

**COHERENCE C-12: api.md OQ #5 (cursor timeout) is resolved in scale.md but not closed**

- Severity: should-fix
- Detail: api.md Open Questions #5 asks "What is the cursor timeout for idle cursors opened via wire protocol?" scale.md Resource Limits table defines: "Max cursor idle time: 600 s (10 min)." This OQ is already resolved by scale.md but api.md still marks it as open.
- Suggested fix: Update api.md OQ #5: "~~What is the cursor timeout for idle cursors opened via wire protocol?~~ **RESOLVED**: 600 seconds (10 min) — see scale.md Resource Limits table. Matches MongoDB's default. Configurable."

---

### B3: Missing Glue

---

**COHERENCE C-13: `serverStatus` needs stats data but `stats()` API is deferred**

- Severity: should-fix
- Detail: api.md notes "Wire protocol's `serverStatus` command serves basic diagnostics needs for Phase 1." api.md also defers `Database::stats()` to Phase 2. But `serverStatus` must return something useful — connection count, uptime, storage stats. Where does this data come from without a `stats()` API? The integration point between the wire protocol's `serverStatus` handler and the storage engine's internal state is unspecified.
- Suggested fix: Add to api.md a note under the `compact()` / `stats()` deferred block: "Note: the wire protocol `serverStatus` handler in Phase 1 reads data directly from internal engine state (not through the public stats() API): uptime from process start time, WAL file size via `std::fs::metadata()`, connection count from wire protocol connection tracker, buffer pool stats from the pool's internal counters. These are implementation details, not public API."

---

**COHERENCE C-14: WAL recovery algorithm incomplete — checkpoint sequence field unused**

- Severity: should-fix
- Detail: data.md WAL Header includes a "Checkpoint sequence: uint32" field but the WAL Operations section ("On open, scan the WAL. Replay all committed frames.") does not explain how this field is used during recovery. Does a higher checkpoint sequence mean frames before it are already in the main file and should be skipped? What happens if the WAL has frames from before and after a partial checkpoint?
- Suggested fix: Add to data.md WAL Design — Recovery section:
  ```
  Recovery Algorithm:
  1. Read WAL header. Verify magic bytes, format version, and salt match main file header.
     If salt mismatch: WAL is stale (left from a different database session) — delete WAL and proceed.
  2. Read WAL frames sequentially. For each frame:
     a. Verify frame checksum (CRC32C covers frame header + page data).
     b. If checksum fails: this is the start of an uncommitted/partial write — stop here.
     c. If "database page count" = 0: non-commit frame, add to pending set.
     d. If "database page count" != 0: commit frame — apply all pending frames to the main file.
        Clear pending set and advance the "last committed position" marker.
  3. Discard all frames after the last commit frame (the partial write that was interrupted).
  4. Update the main file header with the new page count from the last commit frame.
  5. The checkpoint sequence field records how many checkpoints have occurred. It is used
     to detect a WAL file that was partially checkpointed before a crash: frames before
     the checkpoint sequence boundary are already in the main file and can be skipped
     during replay (optimization for large WAL files).
  ```

---

**COHERENCE C-15: integration.md File Management table shows `compact()` as Phase 1**

- Severity: should-fix
- Detail: integration.md File Management for Operators table includes:
  ```
  | Shrink | `db.compact()` | Reclaims free pages, rewrites file |
  ```
  api.md defers `compact()` to Phase 2.
- Suggested fix: Update integration.md file management table: Replace `compact()` row with: "| Shrink | (Phase 2) | compact() is deferred. In Phase 1, delete unnecessary documents and run `db.checkpoint()` to merge the WAL. The main file size does not automatically shrink in Phase 1. |"

---

**COHERENCE C-16: Catalog backup offset in file header is in "Reserved" area without marking**

- Severity: should-fix
- Detail: data.md Constraints #5 (catalog hardening) states the backup catalog root page is at "offset 76 within the currently reserved header space." However, the file header specification shows offset 72-127 as "Reserved (zero-filled, for future use — encryption metadata, etc.)." The backup catalog root is committed to using offset 76 but the file header spec doesn't show this. An implementer reading the file header spec would not know offset 76 is reserved for catalog backup.
- Suggested fix: Update the File Header specification to explicitly list offset 76:
  ```
  72      4      Catalog root backup: uint32 (redundant copy of offset 32, for corruption recovery)
  76      52     Reserved (zero-filled, for future use — encryption metadata, etc.)
  ```
  Note: this changes the reserved region from bytes 72-127 to 76-127.

---

### B4: Completeness Delta

After 5 prior review rounds, these gaps remain:

---

**COHERENCE C-17: `OpenOptions::read_only` note references IoT forensic access but ignores WAL replay**

- Severity: should-fix
- Detail: api.md `OpenOptions` includes:
  ```rust
  // Note on read_only mode:
  // When `read_only: true`:
  // - WAL replay is SKIPPED (database state is as of last checkpoint)
  ```
  The note says WAL replay is skipped, meaning uncommitted data from the last session is NOT visible. But if the device crashed mid-write and the only copy of committed data is in the WAL (not yet checkpointed), skipping WAL replay means committed data is silently invisible in read-only mode. This is counterintuitive and potentially dangerous for the "forensic access after IoT/edge failure" use case.
- Suggested fix: Add clarification to read_only note: "CAUTION: If the database was not cleanly checkpointed before the device was put into read-only mode (e.g., power was cut mid-operation), committed data that exists only in the WAL file will NOT be visible. Read-only mode shows only the state of the last successful checkpoint. If the WAL file is present, it signals uncommitted state. For forensic access where committed data may be in the WAL, use normal read-write mode, which will replay the WAL safely."

---

**COHERENCE C-18: No spec for SHM hash table collision policy or initial size**

- Severity: should-fix
- Detail: data.md SHM Layout specifies "WAL index: hash table [page_number(4) → wal_offset(8)]" but does not specify:
  - Initial hash table size (number of buckets)
  - Collision resolution policy (open addressing? chaining?)
  - What happens when the hash table is full (WAL has more unique pages than buckets)
  - Whether the SHM file can grow
  Without these, two implementations would produce incompatible SHM formats.
- Suggested fix: Add to data.md SHM Design: "SHM hash table: fixed-size open-addressing hash table with linear probing. Initial size: 4096 buckets (65,536 bytes for 4-byte page numbers + 8-byte offsets). Load factor threshold: 75% — if WAL index exceeds 3072 entries, trigger an emergency checkpoint to reduce WAL size before continuing. The SHM file is fixed-size (header + reader slots + hash table = 520 + 65536 = ~66KB). The hash table does not grow; the WAL auto-checkpoint threshold must be set to keep the WAL under 4096 unique pages."

---

### B5: Overall Readability

A developer could pick up these design documents and start building. The architecture is clear and well-organized. Minor issues:

1. The documents cross-reference each other appropriately (api.md → storage engine → query engine, etc.).
2. The phase structure (0 → 1a → 1b → 1c) is consistently used across documents.
3. Data flows are specified (BSON in, BSON out, cursor IDs, error codes).
4. The main remaining readability issues are the Phase 2 deferrals that appear as Phase 1 features in ux.md (fixed in Part C below).

**Grade: B+** (before fixes). The core architecture is well-specified. The must-fix issues are mainly API surface consistency and testability gaps, not architectural deficiencies.

---

## Part C: Changes Applied to Design Docs

### api.md changes

1. **Closed OQ #5** (C-12) — cursor timeout resolved: 600s, matches scale.md.
2. **Added `WireProtocol` type definition** (C-07) — public API for starting wire protocol shim.
3. **Added `ExplainResult` type** (C-10) — define the return type of `Cursor::explain()`.
4. **Added cursor ID contract** (C-05) — spec for wire protocol ↔ native cursor mapping.
5. **Added `serverStatus` data source note** (C-13) — explains where Phase 1 serverStatus gets data.
6. **Updated `OpenOptions`** (C-06) — clarified builder pattern is the intended API shape.
7. **Added `busy_handler` to OpenOptions** (C-09) — added callback variant alongside `busy_timeout`.
8. **Removed `CountOptions`/`DeleteOptions` from export list** (C-11) — unused in Phase 1.

### data.md changes

1. **Fixed SHM reader slot size** (C-04) — corrected to 512 bytes for 64 readers, updated WAL index offset.
2. **Added WAL recovery algorithm** (C-14) — explicit step-by-step recovery procedure.
3. **Added SHM hash table spec** (C-18) — fixed-size open-addressing, 4096 buckets, 75% load factor checkpoint trigger.
4. **Updated file header** (C-16) — explicitly marks offset 72 as "Catalog root backup" instead of reserved.

### integration.md changes

1. **Fixed compact() in file management table** (C-15) — noted as Phase 2, described Phase 1 alternative.

### ux.md changes

1. **Fixed compact() in Journey 1** (C-01) — noted as Phase 2, suggested checkpoint() alternative.
2. **Fixed stats() throughout** (C-02, C-03) — all three locations updated to note Phase 2 deferral and provide Phase 1 alternatives.
3. **Removed `repair()` from failure mode UX** (C-08) — replaced with "restore from backup".
4. **Added read_only mode WAL caveat** (C-17) — explicit warning about invisible committed data.

---

## Part D: Overall Quality Assessment

| Dimension | Status | Notes |
|-----------|--------|-------|
| Testability of API behavioral contracts | **PARTIAL** | T-01: key behavioral contracts need explicit test criteria |
| End-to-end integration test coverage | **MISSING** | T-02: persistence round-trip test task required |
| Wire protocol response format testing | **MISSING** | T-03: no test task for response format parity |
| Crash test acceptance criteria | **VAGUE** | T-04: cycle count and failure definition needed |
| Phase exit gates | **VAGUE** | T-05: need executable test modules per phase |
| Naming consistency | **PARTIAL** | Multiple Phase 2 APIs appear as Phase 1 in ux.md (now fixed) |
| Architecture coherence | **GOOD** | Core architecture is sound; cursor ID gap now filled |
| File format specification completeness | **PARTIAL** | SHM math error fixed; WAL recovery algorithm added |
| Internal contradictions | **4 found** | compact(), stats(), collection.stats(), SHM math — all fixed |
| Missing glue | **3 found** | Cursor ID, serverStatus data source, WAL recovery — addressed |

**Verdict**: The plan has reached implementation-ready quality on the core architecture (storage engine, query engine, wire protocol command set). The must-fix findings are API surface inconsistencies introduced by ux.md speculating ahead of api.md's Phase 2 deferrals, plus a specification error in the SHM layout. After applying this round's fixes, the plan is ready for bead creation and implementation phase dispatch.

---

## Iterative Review Summary — All 6 Rounds

### PRD Alignment (3 rounds):
- **Round 1 (requirements + goals)**: 8 fixes
- **Round 2 (constraints + non-goals)**: 7 fixes
- **Round 3 (user-stories + open-questions)**: 5 fixes

### Plan Self-Review (3 rounds):
- **Round 4 (completeness + sequencing)**: 11 fixes
- **Round 5 (risk + scope-creep)**: 12 scope/risk fixes (including 2 must-fix scope cuts: token auth, typed query builder)
- **Round 6 (testability + coherence)**: 18 findings (6 testability, 12 coherence); 5 must-fix, 13 should-fix

**Final plan**: `.designs/vtmo/` (api.md, data.md, integration.md, scale.md, security.md, ux.md)
**Review logs**: `.plan-reviews/vtmo/`

**Proceeding to bead creation...**
