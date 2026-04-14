# Plan Self-Review Round 2: Risk + Scope-Creep

**Design**: `.designs/vtmo/` (api.md, data.md, integration.md, scale.md, security.md, ux.md)
**PRD**: `.prd-reviews/mqlite-embedded-mongodb/prd-draft.md`
**Prior review**: `.plan-reviews/vtmo/review-round-1.md`
**Date**: 2026-04-14
**Reviewer**: polecat furiosa

---

## Part A: Risk Review

Identifying technical risks, dependency risks, knowledge risks, rollback risks, and missing spikes.

---

### A1: Technical Risks

---

**RISK R-01: WAL correctness — unproven design with no spike**

- Impact: **HIGH**
- Likelihood: **MEDIUM**
- Mitigation: **must-fix**
- Detail: The WAL implementation (Step 2) is the most correctness-critical component in the plan. SQLite's WAL implementation took years to stabilize. The design describes a page-level redo log with CRC32C checksums, SHM WAL index hash table, POSIX `fcntl` file locking, reader snapshot tracking, and non-blocking checkpoint. Getting any of these wrong produces silent data corruption. The current plan has no spike or prototype to validate the WAL design before committing to full implementation. The first end-to-end validation happens at the end of Phase 1a — too late to change the fundamental design.
- Suggested action: Add a **WAL correctness spike** to Step 0 (or early Phase 1a):
  - Implement minimal WAL: file header with salt, single writer path (append frame + commit marker), crash recovery (replay frames), and CRC32C validation.
  - Test: fork child, write 100 documents, SIGKILL child, reopen — verify all committed docs present.
  - Do NOT proceed to full Step 2 implementation until spike proves the design works on all P0 platforms (x86_64-linux, aarch64-linux, aarch64-darwin).

---

**RISK R-02: B+ tree split/merge correctness at page size boundary**

- Impact: **HIGH**
- Likelihood: **MEDIUM**
- Mitigation: **must-fix**
- Detail: The 4KB/32KB variable page size creates a unique split/merge scenario: when a 32KB leaf is split, the split produces a new 32KB sibling and may require promoting a key into a 4KB internal node. The sibling pointer maintenance (each leaf has prev/next pointers for range scans) must be updated atomically across the split. In a WAL-based system, this means the WAL frame for the split must include all affected pages. A bug here means range scans return incomplete results — silently.
- Suggested action: The round 1 review already separated Step 3a (BSON encoding) from 3b/3c. Add to Step 3c an explicit requirement: **all 8 property-based B+ tree invariants must pass before Step 3c is declared complete**. Add two explicit invariants to the existing list in data.md:
  - "All leaf sibling pointers form a valid doubly-linked list across all leaves in key order"
  - "After any split, the parent's key range correctly covers all descendants"

---

**RISK R-03: pymongo handshake compatibility — no early validation**

- Impact: **HIGH**
- Likelihood: **MEDIUM**
- Mitigation: **must-fix**
- Detail: pymongo 4.x has specific handshake requirements: it sends `lsid`, `readConcern`, `writeConcern` in every command; it checks specific fields in the `hello` response (expects `isWritablePrimary`, not `ismaster`); it may negotiate OP_COMPRESSED even when mqlite doesn't support it. The `maxWireVersion: 21` advertisement implies MongoDB 8.0 capabilities — pymongo may attempt operations that mqlite doesn't support (e.g., causal consistency, retryable writes) based on this advertisement. Currently, the first pymongo validation happens in Phase 1c (Step 7) — after the full 18-command surface is built. If the handshake fails, the acceptance criteria for Phase 1c cannot be met without significant wire protocol rework.
- Suggested action: Add a **pymongo connectivity spike** before starting Step 7 implementation:
  - Implement only: hello/isMaster response, ping, buildInfo — the minimum to get pymongo to connect.
  - Run: `MongoClient("mongodb://localhost:27017/?directConnection=true")` + `.admin.command("ping")`.
  - Confirm: no "unsupported capability" errors, no unexpected driver-side fallback behavior.
  - Document findings before proceeding to the other 15 commands.

---

**RISK R-04: Multi-process SHM locking — macOS-specific fcntl behavior**

- Impact: **HIGH**
- Likelihood: **LOW** (but HIGH impact if triggered)
- Mitigation: **should-fix**
- Detail: POSIX `fcntl(F_SETLK)` semantics differ between Linux and macOS in specific ways:
  - On macOS, `fcntl` locks are inherited by child processes across `fork()`, unlike Linux.
  - Lock release behavior when the holding thread exits (not the process) differs.
  - macOS has a 10K advisory lock limit that can cause silent `fcntl` failures under heavy multi-process load.
  SQLite has explicit macOS-specific workarounds in its VFS layer for these issues. The current plan doesn't address these differences.
- Suggested action: Add to Step 2 design notes: "Verify POSIX fcntl behavior on both Linux and macOS during implementation. SQLite's `os_unix.c` POSIX VFS layer documents known platform differences — review it before implementing the locking protocol. Specifically test: (1) multi-process access with 2 OS processes, (2) lock release on thread death, (3) lock behavior after fork."

---

**RISK R-05: `bson` crate version coupling in public API**

- Impact: **HIGH**
- Likelihood: **MEDIUM**
- Mitigation: **must-fix**
- Detail: api.md plans to re-export `bson` types (`Document`, `Bson`, `ObjectId`, `doc!`) from mqlite's public API. This makes the `bson` crate version part of mqlite's semver contract. The `bson` crate has had a major breaking version bump before (v1 → v2). If `bson` has another major version, every mqlite upgrade is a breaking change for users, regardless of whether mqlite's own API changed. This creates long-term maintenance risk and may slow down `bson` adoption of security fixes.
- Suggested action: Establish a clear **bson version coupling policy** in api.md:
  - Option A (simpler, current approach): Re-export as planned. Document explicitly: "Upgrading the `bson` crate major version is a mqlite semver-breaking change." Pin `bson` with a `>=X.Y, <Z` bound and commit to tracking `bson` major versions promptly.
  - Option B (more work): Wrap `bson` types in newtype wrappers for the public API surface. Only expose the raw `bson` types where absolutely necessary.
  - **Recommended**: Option A with explicit versioning policy documented in the API design. The coupling is unavoidable for the MongoDB-familiarity goal; making it explicit is the right mitigation.

---

**RISK R-06: Overflow page chain correctness for large documents**

- Impact: **HIGH**
- Likelihood: **MEDIUM**
- Mitigation: **should-fix**
- Detail: Documents exceeding ~31KB (a 32KB leaf page minus header) are stored in overflow page chains. The current design (data.md) specifies overflow pages are 32KB and linked, but does not specify: (1) the exact overflow page header format, (2) how partial updates to large documents are handled (must the entire chain be rewritten on any field update?), (3) how the WAL handles a chain of 500 overflow pages for a 16MB document (15+ WAL frames). A bug in overflow chain management means all large documents (>31KB) are silently corrupted.
- Suggested action: Add to data.md an **overflow page format specification**:
  ```
  Overflow Page Format (32KB):
  Offset  Size   Field
  0       1      Page type: 0x05 (overflow)
  1       3      Reserved
  4       4      Next overflow page: uint32 (0 = last in chain)
  8       4      Data length in this page: uint32
  12      4      Page checksum: CRC32C
  16      32752  Payload data (remainder of document)
  ```
  And update semantics: "On document update, the entire overflow chain is overwritten in the WAL (full page images for all affected overflow pages). Partial overflow chain writes are not supported in Phase 1."

---

**RISK R-07: Catalog corruption renders entire database unreadable**

- Impact: **HIGH**
- Likelihood: **LOW**
- Mitigation: **should-fix**
- Detail: The catalog B+ tree maps all collection names to root pages and index metadata. A corruption of the catalog root page (e.g., torn write during power loss) makes every collection and every index inaccessible. The design mentions checksummed pages but doesn't specify catalog-specific redundancy. Unlike collection data (where a corrupt page affects only a portion of the data), catalog corruption is total.
- Suggested action: Add catalog hardening to data.md:
  - Store catalog root page number in TWO locations in the file header (primary + backup offset).
  - On catalog update, write the new catalog root to WAL. Only update the file header backup location after checkpoint.
  - Add a consistency check in `Database::open()`: verify catalog root page checksum before trusting its contents.

---

**RISK R-08: Decimal128 ordering in BSON key encoding**

- Impact: **MEDIUM**
- Likelihood: **MEDIUM**
- Mitigation: **should-fix**
- Detail: The BSON type comparison ordering includes Decimal128 (IEEE 754 decimal128). Step 3a requires encoding all 14 type categories into byte-comparable keys. Decimal128's ordering relative to other numeric types (int32, int64, double) is specified by MongoDB but complex: Decimal128 NaN values have specific ordering, and comparison between Decimal128 and integer types requires careful handling. The `bson` crate's Decimal128 support is limited — it provides a raw byte representation but minimal arithmetic. Getting the byte-comparable encoding wrong for Decimal128 means indexes on fields with Decimal128 values are silently incorrect.
- Suggested action: Add to Step 3a: "Before implementing Decimal128 key encoding, spike the `bson` crate's Decimal128 support: can we extract the sign, exponent, and coefficient for comparison ordering? If not, fall back to string-based ordering (which still produces a consistent total order, just not numerically correct for mixed-type comparisons). Document the choice and test against MongoDB 8.0's Decimal128 ordering explicitly."

---

### A2: Dependency Risks

---

**RISK R-09: No rollback plan if WAL design proves unworkable**

- Impact: **HIGH**
- Likelihood: **LOW**
- Mitigation: **should-fix**
- Detail: If the SHM-based WAL index hash table design proves unworkable (e.g., the hash table grows too large for the SHM file size, or POSIX fcntl behavior is irreconcilable across platforms), there's no documented fallback design. The team would need to redesign from scratch mid-Phase 1a.
- Suggested action: Document a simplified WAL fallback design in scale.md:
  - **Fallback**: Instead of SHM hash table for WAL frame index, readers scan the WAL file linearly to find the latest committed version of each page. This is O(WAL frames) per page read but requires no SHM complexity. Acceptable for Phase 1 if WAL is capped at a small size (e.g., 10MB = ~300 frames) with aggressive checkpointing.
  - Define the decision trigger: "If SHM-based WAL index is not working correctly on both Linux and macOS after 2 weeks of implementation, switch to linear WAL scan for Phase 1."

---

**RISK R-10: Wire protocol response format compliance — 18 command surface**

- Impact: **MEDIUM**
- Likelihood: **HIGH**
- Mitigation: **should-fix**
- Detail: Each of the 18 Phase 1 commands has a specific response document format that pymongo and mongosh parse and check. For example: `find` returns a cursor with specific `firstBatch`/`id`/`ns` fields; `insert` returns `n` and optional `writeErrors`; `findAndModify` returns `value` (not `document`) and `lastErrorObject`. Getting any response field name or type wrong causes driver failures. The current design docs don't specify the exact response format for each command — they describe the commands but not their wire protocol response shapes.
- Suggested action: Add to integration.md a "Command Response Formats" section referencing the MongoDB driver wire protocol specifications for each command. Key commands with non-obvious response formats: `findAndModify` (returns `value` field), `getMore` (returns `nextBatch`), `createIndexes` (returns `numIndexesAfter`), `findOne` via `find` (returns empty result set vs. `null`).

---

### A3: Knowledge Risks

---

**RISK R-11: WAL implementation requires specialist expertise**

- Impact: **HIGH**
- Likelihood: **MEDIUM**
- Mitigation: **should-fix**
- Detail: Implementing a correct WAL with POSIX multi-process file locking, SHM coordination, and crash recovery is a specialized domain. It requires deep understanding of: `fsync` behavior differences across OS and filesystem types (ext4, APFS, ZFS), `fcntl` lock semantic differences (Linux vs. macOS vs. BSD), WAL recovery correctness (which frames to replay, which to discard), and checkpoint correctness under concurrent readers. This expertise is rarely available without prior exposure to SQLite or similar systems.
- Suggested action: Add to Step 2 implementation notes: "Study SQLite's `os_unix.c` WAL implementation before writing any WAL code. Key sections: the WAL lock protocol, the SHM file format, and the checkpoint algorithm. SQLite's WAL code is well-commented and the canonical reference for this class of problem."

---

### A4: Risk Summary

| ID | Risk | Impact | Likelihood | Mitigation |
|----|------|--------|------------|------------|
| R-01 | WAL correctness spike missing | HIGH | MEDIUM | **must-fix** |
| R-02 | B+ tree split/merge property test gate | HIGH | MEDIUM | **must-fix** |
| R-03 | pymongo handshake compatibility spike | HIGH | MEDIUM | **must-fix** |
| R-05 | `bson` crate version coupling policy | HIGH | MEDIUM | **must-fix** |
| R-04 | macOS fcntl behavior differences | HIGH | LOW | should-fix |
| R-06 | Overflow page chain format unspecified | HIGH | MEDIUM | should-fix |
| R-07 | Catalog corruption = total data loss | HIGH | LOW | should-fix |
| R-08 | Decimal128 key encoding spike | MEDIUM | MEDIUM | should-fix |
| R-09 | No WAL design fallback plan | HIGH | LOW | should-fix |
| R-10 | Wire protocol response format compliance | MEDIUM | HIGH | should-fix |
| R-11 | WAL expertise requirement | HIGH | MEDIUM | should-fix |

---

## Part B: Scope-Creep Review

Checking for gold-plating, premature optimization, over-engineering, and deferrable items.

---

### B1: Items to CUT

---

**SCOPE S-01: `--require-token` wire protocol auth flag (security.md)**

- Classification: **CUT** — must-fix
- Detail: security.md Phase 1 recommendation #2 reads: "Wire protocol: add a `--require-token` flag (Option 3 partial). A simple shared-secret token passed in the connection handshake metadata." This is not in the PRD. The PRD explicitly lists "Authentication / encryption at rest" as a Phase 1 non-goal (Non-Goals section). Adding an informal token auth mechanism creates two problems: (1) it implements an authentication mechanism that isn't MongoDB-standard (no driver supports it), meaning drivers can't use it; (2) it competes with Phase 2's plan for SCRAM-SHA-256. The token mechanism would need to be removed or replaced in Phase 2, creating compatibility churn.
- Suggested action: **Remove** the `--require-token` recommendation from security.md. Replace with: "Phase 1 wire protocol is intentionally unauthenticated. Mitigation is localhost-only binding and documentation. Authentication is a Phase 2 feature (SCRAM-SHA-256)."

---

**SCOPE S-02: Typed MQL query builder API (security.md)**

- Classification: **CUT** — must-fix
- Detail: security.md threat #7 (MQL injection, HIGH severity) recommends: "Provide typed query builder API that prevents operator injection by construction." A typed query builder is a completely different and significantly larger API surface than what the PRD specifies. The PRD describes a BSON document-based filter API matching MongoDB's pattern: `collection.find(doc! { "age": { "$gt": 18 } })`. A typed builder API would look different and require substantial design work. The injection risk is a valid concern, but the PRD's API design is already specified — a typed builder is scope creep.
- Suggested action: **Remove** the typed query builder recommendation from security.md. Replace with: "MQL injection risk is documented in this analysis. Mitigations are: (1) clear documentation of safe API usage patterns, (2) validation of operator keys in filter documents (reject `$`-prefixed keys in value positions where operators are not expected). No typed builder API in Phase 1."

---

**SCOPE S-03: `exhaustAllowed` OP_MSG flag handling (integration.md)**

- Classification: **CUT** — should-fix
- Detail: integration.md's OP_MSG parsing plan includes parsing `bit 16: exhaustAllowed` from OP_MSG flagBits. `exhaustAllowed` enables "exhaust cursor" mode where the server streams all cursor results without client issuing getMore for each batch. This is a complex optimization feature not required by mongosh or pymongo 4.x for Phase 1 acceptance. The PRD acceptance criteria for G6 are: mongosh basic CRUD works, pymongo test suite passes. Exhaust cursor mode is not tested in either.
- Suggested action: Add note to integration.md OP_MSG parsing section: "Parse and ignore `exhaustAllowed` flag (bit 16) in Phase 1. Always respond with normal cursor batch semantics regardless of this flag. Exhaust cursor mode is Phase 2."

---

### B2: Items to SIMPLIFY

---

**SCOPE S-04: Progressive benchmark baselines after each sub-phase**

- Classification: **SIMPLIFY** — should-fix
- Detail: Round 1 review added a recommendation (S-11) for benchmark baselines at each sub-phase (1a, 1b, 1c) stored in `.benchmarks/`. The PRD G7 requires: "A benchmark suite exists that measures all operations in the table above. Phase 1 release meets the stated targets on reference hardware." This requires a benchmark suite at Phase 1 completion — not progressive tracking during development. Implementing progressive benchmark CI adds infrastructure overhead (storing baselines between CI runs, comparing across phases, tracking regressions during active development) without clear PRD justification.
- Suggested action: Simplify to: benchmark suite exists and passes at Phase 1 DoD. Defer progressive CI regression tracking to after Phase 1 ships (where regressions between releases are the actual risk).

---

**SCOPE S-05: OP_MSG checksum generation on outbound responses**

- Classification: **SIMPLIFY** — should-fix
- Detail: integration.md says "Validate checksum if present; generate checksum on responses if client sent one." Generating CRC32C checksums on outbound responses adds CPU overhead to every wire protocol response. For localhost debugging purposes (Phase 1's stated use case), this overhead serves no security purpose. MongoDB drivers accept responses without checksums — the `checksumPresent` flag in response flagBits is optional. The incoming-checksum validation is worthwhile (catch malformed client messages); the outgoing checksum generation is not.
- Suggested action: Phase 1 policy: validate incoming checksums if `flagChecksumPresent` is set in the client's flagBits. **Always omit checksums on outbound responses** (set flagBits to 0). Document this in integration.md.

---

**SCOPE S-06: SHM max reader count of 256 (scale.md)**

- Classification: **SIMPLIFY** — should-fix
- Detail: scale.md specifies "Up to 64 concurrent readers (configurable, max 256)." The PRD says the same. Implementing max 256 requires a larger SHM hash table and reader slot array than max 64. The SHM file size, hash table slot count, and reader slot size are all affected. For Phase 1, 64 concurrent readers is the stated default and is sufficient for all described use cases (embedded apps, CLI tools, test pipelines). Max 256 can be implemented in Phase 2 if users actually need it.
- Suggested action: Set Phase 1 hard limit: `max_readers` capped at 64. Remove the "max 256" claim from scale.md Phase 1 documentation. Phase 2 can raise the cap with a file format change to the SHM structure.

---

**SCOPE S-07: Cargo workspace from Step 0 (PRD Rough Approach)**

- Classification: **SIMPLIFY** — should-fix
- Detail: Step 0 in the PRD calls for "Cargo workspace: `mqlite` crate, feature flags `wire` and `tracing`." For Phase 1 with a single `mqlite` crate, a Cargo workspace adds a top-level `Cargo.toml` plus a `mqlite/Cargo.toml` with no benefit. Workspaces are valuable when there are multiple crates sharing a dependency graph. When there is one crate, the workspace is pure overhead.
- Suggested action: Phase 1: single `Cargo.toml` at the crate root. Convert to workspace when subcrates are actually needed (e.g., `mqlite-ffi`, `mqlite-cli` in Phase 2). Remove workspace requirement from Step 0.

---

**SCOPE S-08: `cargo-fuzz` setup before parsers exist (PRD Rough Approach Step 0)**

- Classification: **SIMPLIFY** — should-fix
- Detail: Step 0 includes "Fuzz targets scaffolded: BSON parser, OP_MSG frame parser." Setting up `cargo-fuzz` requires nightly Rust or specific CI configuration. Scaffolding fuzz targets before the parsers exist means the targets are stubs that test nothing. Setting up fuzz infrastructure early adds CI complexity without value.
- Suggested action: Move fuzz target setup: BSON parser fuzz target to Step 1 (when the BSON parser exists); OP_MSG frame parser fuzz target to Step 7 (when the wire protocol exists). Remove from Step 0.

---

### B3: Items to DEFER

---

**SCOPE S-09: MongoDB Compass compatibility**

- Classification: **DEFER** — should-fix
- Detail: integration.md mentions "Phase 1 targets mongosh compatibility; Compass compatibility is a stretch goal." This is correctly scoped but needs to be explicitly removed from any Phase 1 acceptance criteria. Compass uses additional commands for schema analysis and explain plan visualization beyond what the PRD's G6 acceptance criteria require.
- Suggested action: Remove Compass from Phase 1 acceptance criteria. Add to Phase 2 backlog.

---

**SCOPE S-10: `Database::compact()` method (api.md)**

- Classification: **DEFER** — should-fix
- Detail: api.md includes `pub fn compact(&self) -> Result<()>` (analogous to SQLite's VACUUM). This method is not in the PRD Phase 1 DoD. Compaction (reclaiming free pages by rewriting the entire database) is a significant operation that requires: reading all live pages, rewriting them to a new file, atomically replacing the old file, and handling WAL interactions. This is entirely separable from Phase 1's core functionality.
- Suggested action: Remove `compact()` from the Phase 1 API surface in api.md. Add to Phase 2 backlog. The `checkpoint()` method (which IS in the PRD G1 acceptance criteria) satisfies the "safe to copy" use case.

---

**SCOPE S-11: `Database::stats()` and `CollectionStats` (api.md)**

- Classification: **DEFER** — should-fix
- Detail: api.md includes `pub fn stats(&self) -> Result<DatabaseStats>` and `pub fn stats(&self) -> Result<CollectionStats>` on Collection. These are not in the PRD Phase 1 DoD. While useful for diagnostics, implementing stats requires tracking and maintaining counters throughout the storage engine that add overhead to every write path.
- Suggested action: Remove `stats()` from Phase 1 API surface. Add to Phase 2 backlog. The wire protocol's `serverStatus` command can serve basic diagnostics needs for Phase 1.

---

**SCOPE S-12: Kind 1 (document sequence) OP_MSG sections**

- Classification: **DEFER** (pending verification) — should-fix
- Detail: integration.md plans to parse Kind 1 (document sequence) sections in OP_MSG. Kind 1 is used by drivers for bulk operations (insert, update, delete). However, drivers can be configured to use Kind 0 (body) for all operations. If pymongo 4.x and mongosh 2.x use Kind 0 for all Phase 1 commands, Kind 1 parsing is unnecessary complexity.
- Suggested action: During the pymongo connectivity spike (R-03), verify which section kinds pymongo uses for the 18 Phase 1 commands. If Kind 0 only: defer Kind 1 to Phase 2. If Kind 1 is required: implement it. Add this as a tracked decision point before Step 7 begins.

---

### B4: Scope-Creep Summary

| ID | Item | Classification | Priority |
|----|------|----------------|----------|
| S-01 | `--require-token` wire protocol auth | CUT | must-fix |
| S-02 | Typed MQL query builder API | CUT | must-fix |
| S-03 | `exhaustAllowed` OP_MSG flag | CUT | should-fix |
| S-04 | Progressive benchmark baselines | SIMPLIFY | should-fix |
| S-05 | OP_MSG checksum outbound generation | SIMPLIFY | should-fix |
| S-06 | SHM max reader count 64 vs 256 | SIMPLIFY | should-fix |
| S-07 | Cargo workspace from Step 0 | SIMPLIFY | should-fix |
| S-08 | `cargo-fuzz` before parsers exist | SIMPLIFY | should-fix |
| S-09 | MongoDB Compass compatibility | DEFER | should-fix |
| S-10 | `Database::compact()` | DEFER | should-fix |
| S-11 | `Database::stats()` / `CollectionStats` | DEFER | should-fix |
| S-12 | OP_MSG Kind 1 document sequence | DEFER (verify) | should-fix |

---

## Part C: Changes Applied to Design Docs

### security.md changes

1. **Removed `--require-token` recommendation** (S-01) — Phase 1 wire protocol is intentionally unauthenticated. Token auth removed.
2. **Removed typed query builder recommendation** (S-02) — Not in PRD. Replaced with documentation-based mitigation.

### integration.md changes

1. **Added `exhaustAllowed` clarification** (S-03) — Phase 1 ignores this flag.
2. **Added pymongo connectivity spike** (R-03) — Before Step 7 full implementation.
3. **Added OP_MSG checksum policy** (S-05) — Validate incoming, omit outgoing.
4. **Added response format reference note** (R-10) — findAndModify, getMore response shapes.
5. **Added Kind 1 decision point** (S-12) — Verify during pymongo spike before implementing.

### data.md changes

1. **Added overflow page format spec** (R-06) — Explicit page header for overflow chains.
2. **Added two B+ tree property test invariants** (R-02) — Sibling chain validity and split key coverage.
3. **Added catalog redundancy strategy** (R-07) — Dual-write catalog root page.

### scale.md changes

1. **Clarified max reader limit** (S-06) — Phase 1 hard limit: 64 readers. Phase 2 can expand.
2. **Added WAL fallback design** (R-09) — Linear WAL scan fallback if SHM proves unworkable.
3. **Added macOS fcntl note** (R-04) — Reference SQLite os_unix.c for platform differences.

### api.md changes

1. **Removed `compact()`** (S-10) — Deferred to Phase 2.
2. **Removed `stats()`** (S-11) — Deferred to Phase 2.
3. **Added bson version coupling policy** (R-05) — Explicit versioning policy documented.

### PRD (prd-draft.md) Rough Approach changes

1. **Removed Cargo workspace from Step 0** (S-07) — Single crate for Phase 1.
2. **Moved cargo-fuzz setup** (S-08) — BSON fuzz to Step 1, OP_MSG fuzz to Step 7.
3. **Added WAL correctness spike to Phase 1a** (R-01).
4. **Added pymongo connectivity spike before Step 7** (R-03).
5. **Removed progressive benchmark baselines** (S-04) — Single benchmark gate at Phase 1 DoD.

---

## Part D: Overall Quality Assessment

| Dimension | Status | Notes |
|-----------|--------|-------|
| Technical risk coverage | **PARTIAL** | WAL correctness and pymongo compatibility are unproven — spikes added |
| Scope discipline | **PARTIAL** | 2 must-fix scope cuts (token auth, typed builder) + 10 should-fix simplifications |
| Dependency risks | **LOW** | bson crate coupling is the primary concern; now documented |
| Knowledge risks | **PARTIAL** | WAL expertise requirement noted; SQLite reference added |
| Rollback plans | **PARTIAL** | WAL design fallback added; overflow page chain format specified |
| Missing spikes | **PARTIAL** | WAL spike + pymongo spike added; Decimal128 spike noted |

**Verdict**: The design is architecturally sound. The primary risks are implementation-phase risks (WAL correctness, B+ tree edge cases) rather than design-phase risks. The scope additions from security.md are the most concerning finding — they add unapproved features that conflict with PRD non-goals. After applying this round's fixes, the plan is ready for testability and coherence review (Round 3).
