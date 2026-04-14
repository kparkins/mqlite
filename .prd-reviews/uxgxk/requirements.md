# Requirements Completeness

## Summary

This PRD establishes a clear vision and architectural direction for mqlite, but it is significantly underspecified in terms of success criteria, acceptance conditions, and non-functional requirements. The document reads more as a design brief than a testable specification. Almost every goal uses qualitative language ("reasonable performance", "core MQL", "familiar API") without defining measurable thresholds. A QA engineer would struggle to write acceptance tests from this document because "done" is never concretely defined for any capability.

The most critical gaps are: (1) no performance benchmarks or targets, (2) no definition of what "MongoDB API compatibility" means in testable terms (which exact commands, which edge cases, which error codes), (3) no specification of failure modes and error handling behavior, and (4) no crash recovery acceptance criteria despite crash recovery being listed as a primary goal.

## Findings

### Critical Gaps / Questions

- **No success criteria for "MongoDB API compatibility"**
  The PRD says "Support core MQL query and update operators so existing MongoDB mental models transfer directly" but never defines a compatibility test suite or acceptance matrix. Which MongoDB server version is the compatibility target? (4.4? 5.0? 7.0?) Are results expected to be byte-identical BSON? What about field ordering in returned documents, cursor ID semantics, error code compatibility? Without a target version and a concrete compatibility matrix, two engineers will implement two different things and both will claim success.
  - *Why this matters:* This is the project's core differentiator. Ambiguity here creates scope creep or under-delivery.
  - *Suggested question:* "Which MongoDB server version(s) define the compatibility target, and is there a specific subset of the MongoDB CRUD spec or test corpus we should pass?"

- **No performance targets or benchmarks**
  Goal 7 says "Competitive with SQLite for similar workloads" but defines no numbers. What is the target for: single-document insert latency? Point query by `_id`? Range scan throughput? Bulk insert rate? Index creation time? "Competitive" is not a testable criterion. Does competitive mean within 2x? 10x?
  - *Why this matters:* Without targets, there's no way to know when performance work is "done" or to catch regressions.
  - *Suggested question:* "Can we define specific latency/throughput targets? E.g., single-document insert < Xus, point query by _id < Yus, bulk insert of 100K docs < Zs on reference hardware?"

- **No crash recovery acceptance criteria**
  Goal 4 says "WAL-based durability guarantees so the database survives process crashes and power loss without corruption" but Open Question 10 reveals the durability level is undefined. Is a committed write guaranteed durable after `write()` returns? After `fsync()`? Only after WAL flush? What is the acceptable data loss window? This is a safety-critical property — it must be specified, not left open.
  - *Why this matters:* Different durability guarantees require fundamentally different implementations. "Crash safe" without a precise definition is untestable.
  - *Suggested question:* "What is the durability contract? (a) Durable after WAL flush (write may be lost if crash between API return and flush), (b) Durable after fsync per commit (slow but safe), (c) Configurable? What is the maximum acceptable data loss window?"

- **No error handling specification**
  The PRD describes only happy paths. There is no mention of: what errors the API returns, what happens on disk full, corrupt file detection and recovery, behavior on concurrent write attempts (does the second writer block, fail, or queue?), what happens when a document exceeds 16MB, how index constraint violations are reported, or what errors the wire protocol shim returns to clients.
  - *Why this matters:* Error behavior is often >50% of implementation complexity. Leaving it unspecified guarantees inconsistency across the codebase.
  - *Suggested question:* "Can we define the error taxonomy? At minimum: (1) What errors does the writer return on conflict? (2) What happens on disk full? (3) How are corrupt files detected/reported? (4) Do we mirror MongoDB error codes or define our own?"

- **No definition of "done" for Phase 1**
  The PRD lists a build order but no delivery milestones, exit criteria, or minimum viable feature set. Is Phase 1 done when all 7 layers compile? When a specific test suite passes? When the wire protocol shim can serve `mongosh`? When a benchmark target is hit? There is no acceptance gate.
  - *Why this matters:* Without a definition of done, Phase 1 can stretch indefinitely or ship prematurely.
  - *Suggested question:* "What is the minimal acceptance test for Phase 1 completion? E.g., 'insert 1M documents, query by indexed field, survive kill -9, connect with mongosh and run basic CRUD.'"

- **SWMR concurrency semantics unspecified**
  Goal 5 says "Multiple readers can operate simultaneously alongside a single writer" but does not specify: isolation level (snapshot? read-committed?), whether readers see in-progress writes, what happens when a reader's snapshot becomes stale, or maximum reader count. The SQLite analogy is helpful but SQLite's WAL mode has specific documented semantics (e.g., readers see the database as of the last completed write transaction) — mqlite needs equivalent precision.
  - *Why this matters:* Concurrency semantics affect correctness of every multi-threaded consumer.
  - *Suggested question:* "What isolation level do readers get? Snapshot isolation at the time of cursor open? Do long-running readers block WAL checkpointing (as in SQLite)?"

### Important Considerations

- **Wire protocol compatibility depth is undefined**
  The PRD says the shim should support "enough for `mongosh` basic operations" but doesn't enumerate which commands. `mongosh` issues `hello`, `buildInfo`, `getCmdLineOpts`, `getLog`, `listDatabases`, `listCollections`, plus CRUD commands, and various metadata queries on startup alone. What subset is required? What should the shim return for unsupported commands — an error, or a stub response?
  - *Suggested question:* "Can we define an explicit allowlist of wire protocol commands for Phase 1?"

- **No data migration / upgrade path requirements**
  The PRD mentions file format versioning ("versioned from day one") but doesn't specify: what happens when a newer version of mqlite opens an older file? Is automatic migration required? Is there a migration tool? What about downgrade (newer file opened by older library)?
  - *Suggested question:* "When file format changes between versions, is in-place automatic upgrade required, or is a separate migration tool acceptable?"

- **No size/scale limits defined**
  Open Question 5 asks about max database size but there are no stated limits for: max number of collections, max number of indexes per collection, max number of concurrent readers, max document nesting depth, max field name length, or max number of documents per collection. These affect storage engine design decisions.
  - *Suggested question:* "What are the practical scale targets for Phase 1? E.g., database files up to X GB, collections up to Y documents, Z indexes per collection."

- **No API ergonomic requirements**
  The PRD says the API should "feel immediately familiar to anyone who has used the MongoDB Rust driver" but doesn't specify: should it be a trait-based interface (for mockability)? Should `Database::open()` take a path or a config struct? Should errors use `thiserror` or a custom enum? Is `serde` integration required for typed document mapping? These API design decisions are high-impact for adoption and are hard to change later.
  - *Suggested question:* "Is there a specific subset of the `mongodb` Rust driver's API surface that should be mirrored? Should we support serde-based typed document access from day one?"

- **No observability or diagnostics requirements**
  There is no mention of: logging (what, where, at what level), metrics exposure (open file handles, cache hit rates, WAL size), `EXPLAIN`-equivalent for query plans, database statistics commands, or file integrity checking tools. For a storage engine, these are essential for debugging production issues.
  - *Suggested question:* "What diagnostics are needed in Phase 1? At minimum: query plan explanation, database stats (doc count, file size, index info), and WAL health?"

- **No thread-safety / `Send`+`Sync` requirements specified**
  The concurrency model says SWMR but doesn't state whether the `Database` handle is `Send + Sync`, whether collection handles can be shared across threads, or whether the API is designed for use with `tokio::spawn` or `std::thread::spawn`. This is foundational for a Rust library.
  - *Suggested question:* "Must `Database` and collection handles be `Send + Sync`? Is the expected usage pattern one handle shared across threads, or handle-per-thread?"

### Observations

- **Open Questions are load-bearing design decisions, not optional**
  Several of the 11 open questions (BSON choice, mmap vs explicit I/O, async vs sync API, ObjectId generation) aren't deferrable — they affect the storage format, public API surface, and compatibility contract. The PRD should distinguish which open questions are pre-implementation blockers vs. which can be decided during implementation.

- **No mention of `Drop` / cleanup semantics**
  For an embedded Rust database, the behavior on `Drop` is critical. Does dropping the `Database` handle flush the WAL? Is it safe to `std::mem::forget` a handle? What happens if the process calls `std::process::exit()` without dropping? These are Rust-specific requirements that matter for correctness.

- **Test strategy is absent**
  There is no mention of how the project will be tested: unit tests per layer? Integration tests against MongoDB's own test suite? Fuzz testing for the BSON parser and storage engine? Property-based testing for B+ tree invariants? A storage engine without a test strategy is a time bomb.

- **"Phase 1" implies later phases but they're not outlined**
  The non-goals list items like "aggregation pipeline" and "change streams" as out of scope for Phase 1, but there's no Phase 2 roadmap. This matters because Phase 1 architectural decisions may preclude or complicate later phases. At minimum, the PRD should state whether the Phase 1 architecture must be extensible toward these features.

- **No mention of `no_std` or WASM compatibility**
  Given the IoT/edge use case, it's worth stating explicitly whether `no_std` or WASM compilation is a requirement or non-goal. This affects fundamental choices (allocator, I/O, threading).

- **Backup/copy semantics need clarification**
  Story 5 says "just copy the file when the writer is idle" — but what does "idle" mean precisely? Is there an API to quiesce the database for safe copy? Can you copy while readers are active? What about copying while the WAL has unflushed data (the WAL is presumably in the same file or a sibling file)?

## Confidence Assessment

**Low-Medium** — The PRD effectively communicates the product vision, target audience, and high-level architecture. However, it lacks the specificity needed to drive implementation without significant additional decision-making. Nearly every goal would benefit from a measurable acceptance criterion. The 11 open questions include several that are architectural prerequisites, not deferred details. A QA engineer could not write a meaningful acceptance test suite from this document as-is. The PRD needs a "Definition of Done" section with concrete, testable criteria for each goal before implementation should begin.
