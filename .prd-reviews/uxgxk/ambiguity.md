# Ambiguity Analysis

## Summary

The PRD paints a clear high-level vision — "SQLite for MongoDB" — but leaves significant implementation-level ambiguity that would cause two engineers to build meaningfully different systems. The most critical gaps cluster around three areas: (1) undefined behavioral semantics for MQL compatibility, (2) vague performance and durability targets, and (3) unclear API contract boundaries. Many of these are acknowledged in the "Open Questions" section, which is honest, but the sheer number of open questions (11) touching core architecture decisions suggests the PRD is still in a pre-implementable state for several layers of the stack.

The document also has a pattern of referencing external systems ("modeled on SQLite's WAL mode", "WiredTiger-style variable page B+ trees", "mirror the MongoDB Rust driver's API shape") without specifying which properties of those systems are being adopted and which are being intentionally diverged from. This "similar to X" framing is a reliable source of implementation disagreements.

## Findings

### Critical Gaps / Questions

- **"Core MQL" is not defined precisely.** The PRD says it targets "the 80% of MQL that covers 95% of embedded use cases" (line 38) but never enumerates the exact boundary. Section 5 of the rough approach lists specific operators (`$eq/$gt/$lt`, `$and/$or/$not`, `$exists/$type`, `$in/$all/$elemMatch`, `$regex`, `$set/$unset/$inc/$push/$pull`) but uses "etc" in several places. Two engineers would draw the line differently on whether `$rename`, `$addToSet`, `$pop`, `$min`, `$max`, `$mul`, `$currentDate`, `$setOnInsert`, or `$each` modifiers are in scope.
  - **Why this matters:** Operator support is the primary compatibility surface. Ambiguity here means features either get silently dropped or scope-crept in.
  - **Suggested question:** Can you provide an exhaustive list of query and update operators that are in-scope for Phase 1? For each, are partial semantics acceptable (e.g., `$elemMatch` projection vs. query form)?

- **"Reasonable performance" has no measurable definition.** Goal 7 says "competitive with SQLite for similar workloads" (line 28). What constitutes "competitive"? Within 2x? 10x? Same order of magnitude? Which SQLite workloads — WAL mode, default journal mode? What document sizes? What's the benchmark methodology?
  - **Why this matters:** Without a concrete target, there's no way to know when performance work is "done" or when to stop optimizing vs. ship. A 5x regression vs. SQLite could be called "competitive" or "unacceptable" depending on who you ask.
  - **Suggested question:** Can you define 3-5 benchmark scenarios with quantitative targets (e.g., "insert 100K 1KB documents in < X seconds", "point-query by _id in < Y microseconds")?

- **Crash recovery guarantees are explicitly unresolved.** Open Question 10 asks "exactly what durability level?" but this is a foundational architectural decision that affects the WAL design, the API surface (do we expose sync modes?), and the user contract. "Crash recovery" (Goal 4) says "survives process crashes and power loss without corruption" — but does that mean every committed write is durable, or only writes that were explicitly fsync'd?
  - **Why this matters:** The answer changes the WAL implementation, default performance characteristics, and what users can rely on. SQLite's WAL mode itself has nuanced durability semantics (PRAGMA synchronous levels) that are not trivially replicated.
  - **Suggested question:** What is the default durability guarantee? (a) Write is durable after API call returns (fsync per commit), (b) write is durable after WAL flush (group commit / periodic), or (c) configurable? What happens to writes that are in the WAL but not yet checkpointed during a power failure?

- **"MongoDB API compatibility" scope is contradictory.** Goal 2 says "existing MongoDB mental models (and ideally code) transfer directly" (line 23). Story 2 says "same query logic" for test fixtures. But the non-goals exclude aggregation pipeline, change streams, multi-doc transactions, full-text search, and geospatial. If a team's production code uses `$lookup` in an aggregation, their test code won't work. The PRD both promises compatibility and extensively carves it out, but doesn't clarify what happens when unsupported operations are attempted — silent failure? Error? Partial result?
  - **Why this matters:** The error behavior for unsupported operations is a critical UX decision. It determines whether mqlite fails fast (safe) or silently drops features (dangerous for test fixtures that are supposed to verify production behavior).
  - **Suggested question:** What is the behavior when a client issues an unsupported MQL operation (e.g., aggregation pipeline via wire protocol, or an unsupported operator via native API)? Explicit error with code? Silent ignore? Which MongoDB error codes should we mirror?

- **Async vs. sync API is unresolved and architectural.** Open Question 11 asks about async vs. sync, but this is a pervasive architectural decision, not a local one. It affects: the wire protocol shim (which needs async for TCP), the native API surface, internal buffer pool implementation, WAL checkpoint scheduling, and what the `Database::open()` signature looks like.
  - **Why this matters:** Choosing wrong means rewriting the entire API layer. This should be decided before any implementation begins.
  - **Suggested question:** Given that the primary target is embedded Rust applications, should the native API be sync-only with the wire protocol shim internally spawning an async runtime? Or should the entire stack be async?

- **"Single file" has undefined boundaries.** Goal 1 says "one `.mqlite` file per database" (line 22), and the constraint says the file format is versioned (line 66). But the WAL is traditionally a separate file (SQLite uses `database.db-wal` and `database.db-shm`). Is the WAL inside the single file, or are auxiliary files allowed? Story 5 says "just copy the file" for snapshots — this only works if WAL and shared memory are inside the single file or if the copy operation is coordinated.
  - **Why this matters:** "Single file" is a headline feature. If the actual on-disk representation is 2-3 files (main + WAL + shared memory), the marketing claim needs adjustment, OR the WAL design needs to embed everything in one file (which has significant complexity implications).
  - **Suggested question:** Does "single file" mean literally one file on disk at all times, or can there be transient auxiliary files (WAL, lock files) similar to SQLite? If auxiliary files exist, how does the "just copy the file" story work?

### Important Considerations

- **"Should mirror the MongoDB Rust driver's API shape where practical" (line 117) is vague.** What does "where practical" mean? Which driver version? The MongoDB Rust driver has `Client`, `Database`, `Collection<T>` generics, `ClientSession` for transactions, typed vs. untyped document modes, `find().await` returning a cursor that implements `Stream`, etc. Some of these (sessions, typed generics) are deeply entangled with features mqlite won't support.
  - **Suggested question:** Can you list the specific MongoDB Rust driver methods/types that should have mqlite equivalents, and which should be intentionally omitted?

- **Compound index semantics are underspecified.** Line 114 mentions "single-field and compound index support" but doesn't define: max number of fields in a compound index, whether index key ordering (ascending/descending) matters, whether partial indexes or sparse indexes are in scope, or how compound indexes interact with the heuristic planner.
  - **Suggested question:** What compound index features are in scope? Just basic multi-field indexes with fixed ascending order, or full MongoDB-style compound indexes with per-field direction?

- **"Heuristic planner" is ambiguous.** The PRD mentions a "heuristic query planner" (line 73, 115) but doesn't define the heuristics. Does it pick the best single index? Consider index intersection? Use any statistics? Handle covered queries (index-only plans)? The open question (9) acknowledges this but leaves it unresolved.
  - **Suggested question:** For Phase 1, is the planner just "pick the most selective single index, fall back to collection scan" or something more sophisticated?

- **BSON type handling and edge cases are unspecified.** MongoDB has complex BSON type comparison ordering, type coercion rules, and specific handling of `undefined`, `MinKey`/`MaxKey`, `Decimal128`, `Binary`, etc. The PRD doesn't specify which BSON types are fully supported.
  - **Suggested question:** Should mqlite support the full BSON type taxonomy, or a subset? Specifically: Decimal128, Binary subtypes, JavaScript/JavaScriptWithScope, DBPointer (deprecated), Symbol (deprecated)?

- **"No C/C++ dependencies" (line 64) vs. potential BSON crate choice.** The official `bson` crate is pure Rust, so this constraint may be satisfiable. But "Rust-safe FFI at most" is vague — safe FFI to what? If we use no C dependencies, why mention FFI at all?
  - **Suggested question:** Is the "Rust-safe FFI" clause intended to allow specific known dependencies, or is it a general escape hatch? Can you enumerate any non-Rust dependencies you'd consider acceptable?

- **Wire protocol shim scope is unclear.** Line 119 says "enough command support for `mongosh` basic operations" but `mongosh` issues many commands during connection setup (`buildInfo`, `getCmdLineOpts`, `atlasVersion`, `getLog`, `hostInfo`, `serverStatus`, topology discovery). Which commands must be implemented vs. can return empty/stub responses?
  - **Suggested question:** Can you define a minimum set of wire protocol commands that must return real data vs. commands that can return stubs/errors? Is there a target `mongosh` version?

- **Collection namespace rules are undefined.** MongoDB has specific rules: database names can't contain `.`, `/`, `\`, NUL, and are limited to 64 characters; collection names can't start with `system.`, can't contain NUL. Does mqlite inherit all of these rules or define its own?

- **What does `insert_many` look like on error?** MongoDB's `insert_many` has `ordered: true/false` semantics — does a failure on document 5 of 10 roll back 1-4 (ordered), or continue inserting 6-10 (unordered)? This is a significant behavioral spec question.

### Observations

- **The "Open Questions" section is unusually large (11 items) for a PRD heading to implementation.** Several of these (BSON library choice, async vs. sync, WAL checkpointing) are foundational. This suggests the PRD may benefit from a design spike phase where these are resolved before committing to the implementation sequence.

- **Phase 1 is implicitly defined but never given a timeline or success metric.** There's no definition of "Phase 1 is done when..." other than "all 7 layers work." A minimum viable subset (e.g., "insert_one + find_one with equality filter on _id, single-file, crash-safe") would make the first milestone more concrete.

- **Story 4 (edge/IoT sync) implies a read-side API for bulk export that isn't in the API spec.** "A sync process reads documents and pushes them to a cloud MongoDB instance" requires either cursor-based iteration over all documents (specified) or a change-tracking mechanism (explicitly a non-goal). How does the sync process know which documents are new?

- **Story 5 ("just copy the file when the writer is idle") has an unsafe assumption.** Copying a file while a reader holds a snapshot could produce an inconsistent copy if the WAL is a separate file. If truly single-file, this is safe only when no WAL tail exists. The PRD should specify whether a safe-copy / backup API is needed.

- **The constraint "backward compatibility within a major version" (line 66) needs its own micro-spec.** What is a "major version" for a pre-1.0 project? Does every 0.x bump count? SemVer says pre-1.0 has no compatibility guarantees — does mqlite override this?

- **"Reports as standalone mongod" (line 72) raises version identity questions.** What MongoDB version does the wire protocol report? This affects driver behavior — drivers adjust their feature usage based on reported server version. Reporting as a recent version but missing aggregation/transactions will confuse drivers.

## Confidence Assessment

**Medium-Low.** The PRD establishes a strong product vision and the architectural layering is well-conceived, but the number of unresolved foundational questions (durability semantics, async model, single-file definition, MQL operator boundary, error behavior) means that two teams could read this PRD and build substantially different systems. The PRD is ready for design spikes but not yet ready for heads-down implementation across the full stack. The bottom layers (file format, WAL, B+ tree) are specified well enough to begin, but the upper layers (query engine operator boundary, API surface, wire protocol command coverage) need sharper specifications.
