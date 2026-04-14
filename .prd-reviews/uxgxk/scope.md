# Scope Analysis

## Summary

The PRD defines an ambitious but conceptually clean project: a SQLite-equivalent for MongoDB. The non-goals section is strong and covers the obvious exclusions (replication, aggregation pipeline, change streams, multi-doc transactions). However, the "Phase 1" framing implies a phased roadmap that doesn't actually exist in the document — there's no Phase 2/3 definition, which means scope boundaries are aspirational rather than contractual. The biggest scope risks are (1) the wire protocol shim expanding from "debugging tool" to "compatibility layer," (2) the MQL operator surface area being underspecified (which 80% of MQL?), and (3) the storage engine complexity being treated as a single line item when it's arguably a separate project.

The MVP question is the critical one: does mqlite need wire protocol compatibility at all in Phase 1, or is the native Rust API the actual product? The wire protocol shim is positioned as "optional, lower priority" but has its own user story and is listed as a goal — that ambiguity will cause prioritization fights.

## Findings

### Critical Gaps / Questions

- **No concrete MQL operator cut line.** The PRD says "80% of MQL that covers 95% of embedded use cases" but never defines which operators are in Phase 1. The query engine section lists broad categories ($eq, $gt, $and, $or, $exists, $in, $regex, etc.) but these are illustrative, not exhaustive. Is `$elemMatch` in or out? `$size`? `$slice` in projections? Every operator not explicitly listed will be a scope negotiation.
  - *Why this matters:* MQL operator compatibility is the core value proposition. Vagueness here means every operator becomes a debate.
  - *Question:* Can we produce a definitive in/out table for every MQL query operator, update operator, and projection operator for Phase 1?

- **Wire protocol shim scope is contradictory.** Goal #6 calls it a goal. The non-goals say "not for running as a production server replacement." The rough approach calls it "optional, lower priority." User Story 3 presents it as a real feature (mongosh + Compass connectivity). Which is it — a debugging convenience or a compatibility feature?
  - *Why this matters:* Wire protocol compatibility is a fractal of complexity. Supporting `mongosh` "basic operations" requires implementing a substantial command surface (isMaster, buildInfo, getCmdLineOpts, getParameter, serverStatus, ping, plus the actual CRUD commands). "Basic operations" is not a specification.
  - *Question:* Should the wire protocol shim be explicitly moved to Phase 2, or should we define a minimal command set (exact list) that constitutes "done" for Phase 1?

- **Storage engine is a project unto itself.** The PRD treats "B+ tree storage engine with WAL, buffer pool, variable page sizes, overflow pages, and crash recovery" as step 2-3 of a 7-step plan. This is closer to 60-70% of the total engineering effort. The PRD doesn't acknowledge this complexity weighting, which means timelines will be wildly off.
  - *Why this matters:* If the storage engine takes 6 months and the query layer takes 2, the "Phase 1" framing misleads about delivery timeline.
  - *Question:* Should the storage engine be its own project with its own milestones, or does "Phase 1" need sub-phases (1a: storage engine, 1b: query + API, 1c: wire protocol)?

- **No success criteria or acceptance tests.** The PRD defines features but not what "done" looks like. What does "MongoDB API compatibility" mean in measurable terms? Is there a test suite? A compatibility matrix? A target percentage of the MongoDB CRUD spec that must pass?
  - *Why this matters:* Without acceptance criteria, Phase 1 is never "done" — there's always one more operator, one more edge case.
  - *Question:* Can we define a concrete compatibility test suite (e.g., subset of MongoDB's own jstestfuzz or CRUD spec tests) that constitutes Phase 1 completion?

### Important Considerations

- **In-memory mode is scope creep hiding in plain sight.** The last bullet under "Key technical decisions" asks "whether to support in-memory mode from the start." User Story 2 (test fixtures) strongly implies this is needed. If it's needed, it's a requirement, not a decision — put it in scope or explicitly defer it.
  - *Suggested action:* Either add in-memory mode as a Phase 1 requirement (it's needed for the test-fixture story) or explicitly cut User Story 2 from Phase 1.

- **"Familiar to MongoDB Rust driver users" is an underspecified constraint.** The API should "mirror the MongoDB Rust driver's API shape where practical." The MongoDB Rust driver has a large API surface: sessions, client options, read/write concerns, codec options, BSON serialization integration, connection pooling semantics. How much of this API shape is "practical" to mirror?
  - *Suggested action:* List the specific MongoDB Rust driver types/traits that mqlite will mirror, and which it won't.

- **BSON 16MB document limit implies overflow page complexity.** The PRD inherits MongoDB's 16MB doc limit. With 32KB leaf pages, a max-size document spans ~500 pages. The overflow page mechanism for this is non-trivial and will affect every layer from storage through query. This constraint should be evaluated: does an embedded database actually need 16MB documents, or would a 1MB or 4MB limit dramatically simplify the storage engine?
  - *Suggested action:* Justify the 16MB limit for embedded use cases or reduce it.

- **No phasing for index types.** The PRD lists "auto _id, single-field, compound" indexes. Are all three required for Phase 1 MVP? Compound indexes require significantly more complexity in the query planner (index intersection, prefix matching, sort optimization). Single-field + auto _id might be sufficient for MVP.
  - *Suggested action:* Consider deferring compound indexes to Phase 1b or Phase 2.

- **The "competitive with SQLite" performance goal is vague.** What workload? What dataset size? What's the measurement? SQLite is extraordinarily optimized after 20+ years. "Competitive" against a mature C codebase from a greenfield Rust project is either impossible or meaningless depending on interpretation.
  - *Suggested action:* Replace with concrete benchmarks: e.g., "insert 100K 1KB documents in <X seconds," "point query by _id in <Y microseconds."

### Observations

- **Day-after-launch requests will be:** (1) aggregation pipeline (especially `$group` and `$project`), (2) `$lookup` / joins, (3) Python/Node bindings, (4) async Rust API, (5) `mongodump`/`mongorestore` compatibility, (6) TTL indexes, (7) unique indexes. The PRD should acknowledge these as Phase 2 candidates so they have a home.

- **Natural phase seams are clear.** Phase 1: storage + native CRUD API. Phase 2: wire protocol + language bindings. Phase 3: aggregation pipeline + advanced indexes. The PRD roughly implies this but should make it explicit.

- **The "pure Rust, no C/C++ dependencies" constraint could conflict with performance goals.** BSON parsing, compression (for OP_COMPRESSED), and cryptographic operations (if auth is ever added) all have faster C implementations. This constraint should be flagged as potentially revisitable, not absolute.

- **File format versioning is mentioned but not specified.** "Versioned from day one" is correct, but the format version strategy (magic bytes, header layout, migration path) needs design before implementation starts — changing it later is extremely painful for an on-disk format.

- **No mention of database size limits for Phase 1.** Open Question #5 asks about max database size but doesn't propose a practical limit. For Phase 1, a 4GB or even 1GB tested-and-guaranteed limit would be perfectly fine and would simplify page addressing.

- **Missing: error semantics.** When mqlite encounters a query operator it doesn't support, does it error, silently ignore, or return partial results? MongoDB has specific error codes and messages. Compatibility here matters for the "migrate between embedded and server MongoDB" story.

- **Missing: concurrent access across processes.** The SWMR model is described but it's unclear if this is in-process only (multiple threads) or cross-process (multiple OS processes accessing the same file, like SQLite supports). This is a fundamental architecture decision that affects file locking strategy.

## Confidence Assessment

**Medium-High.** The PRD covers the big picture well and has a solid non-goals section. The main gaps are at the precision layer: exactly which operators, exactly which commands, exactly what "done" means. These are the gaps that cause scope creep in practice — not missing features, but underspecified boundaries around the features that are listed. The storage engine complexity underestimation is the highest risk to timeline accuracy.
