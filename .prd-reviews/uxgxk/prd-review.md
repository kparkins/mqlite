# PRD Review: mqlite — Embedded, File-Based, MongoDB-Compatible Database

## Executive Summary

The mqlite PRD presents a compelling and well-structured vision — "SQLite for MongoDB" — with a sound architectural layering and reasonable non-goals. However, the PRD is not yet implementation-ready. Six independent review dimensions converged on the same core problem: the document defines *what* to build but not *how it behaves* at the precision needed to write code or tests. The most critical risks are (1) undefined MQL operator boundaries that will cause endless scope debates, (2) unresolved durability/crash-recovery semantics that are foundational to the storage engine design, (3) a contradictory "single file" claim vs. WAL architecture, and (4) no measurable definition of "done" for Phase 1. The bottom layers (file format, WAL, B+ tree) are close to implementable after resolving a few key questions, but the upper layers (query engine boundary, API surface, wire protocol) need sharper specifications before coding begins.

## Before You Build: Critical Questions

These must be answered before implementation starts. They affect foundational architecture decisions that are expensive or impossible to change later.

### MQL Compatibility Boundary

**Q1: What is the exact set of MQL operators in scope for Phase 1?**
- Why this matters: The PRD says "80% of MQL for 95% of use cases" but never enumerates the boundary. Two engineers would draw the line differently on `$rename`, `$addToSet`, `$pop`, `$min/$max`, `$mul`, `$setOnInsert`, `$size`, `$slice`, projection operators, etc. Every unlisted operator becomes a scope negotiation.
- Found by: ambiguity, scope, requirements
- Suggested action: Produce a definitive in/out table for every query operator, update operator, and projection operator. For each, specify whether partial semantics are acceptable (e.g., `$elemMatch` in query position vs. projection position).

**Q2: What is the behavior when an unsupported MQL operation is encountered?**
- Why this matters: If a team uses mqlite as a test double (Story 2) and their production code uses `$lookup` or unsupported operators, silent failure is dangerous. Explicit errors are safe but break the "code transfers directly" promise.
- Found by: ambiguity, scope, gaps
- Suggested action: Define the error behavior — explicit error with a specific code, or something else? Should mqlite mirror MongoDB error codes for compatibility?

**Q3: Which MongoDB server version defines the compatibility target?**
- Why this matters: MongoDB behavior differs across versions (4.4, 5.0, 7.0). Without a target, there's no way to validate correctness or write compatibility tests.
- Found by: requirements
- Suggested action: Pick a target version and identify a subset of MongoDB's CRUD spec tests or jstestfuzz corpus as the Phase 1 acceptance criteria.

### Durability and Crash Recovery

**Q4: What is the durability contract?**
- Why this matters: This is foundational to the WAL design, API surface, and user trust. The PRD says "survives crashes and power loss" but Open Question 10 reveals the semantics are undefined. Different answers require fundamentally different implementations.
- Found by: ambiguity, requirements, gaps
- Suggested options: (a) Durable after API call returns (fsync per commit — safe, slow), (b) Durable after WAL flush with configurable flush interval (fast, small loss window), (c) Configurable with a default. Pick one.

**Q5: What is the crash-testing strategy?**
- Why this matters: WAL bugs manifest as silent corruption after crashes, not in normal testing. A greenfield WAL needs property-based crash testing from day one, not as an afterthought.
- Found by: feasibility
- Suggested action: Commit to a specific approach — deterministic simulation, fault injection, or a jepsen-style harness — and make it a first-class work item.

### Storage Architecture

**Q6: Does "single file" mean literally one file at all times, or one file when cleanly closed?**
- Why this matters: SQLite's WAL mode uses 2-3 files (main + `-wal` + `-shm`). If mqlite must be truly single-file during operation, the WAL must be embedded in the main file — significantly more complex. If auxiliary files are allowed, Story 5 ("just copy the file") needs qualification.
- Found by: ambiguity, feasibility
- Suggested action: Clarify whether transient auxiliary files (WAL, lock, shared memory) are acceptable. If so, document copy/backup semantics.

**Q7: Is the variable page size B+ tree (4KB internal / 32KB leaf) justified, or would uniform pages work?**
- Why this matters: Variable page sizes require a custom allocator that's essentially a mini filesystem, and every bug in it risks silent data corruption. Uniform 4KB pages with overflow for large values is well-understood and significantly simpler.
- Found by: feasibility
- Suggested action: Provide benchmarks or rationale for the variable scheme. If speculative, consider starting with uniform 4KB pages.

**Q8: Is multi-process access to the same file a supported use case?**
- Why this matters: The SWMR model is described but it's unclear if this is in-process only (threads) or cross-process (like SQLite). Cross-process requires file locking (POSIX fcntl or equivalent). Without it, two processes opening the same file will corrupt it silently.
- Found by: gaps

### API Design

**Q9: Async or sync for the native Rust API?**
- Why this matters: This is pervasive — it affects the wire protocol shim, the API surface, internal buffer pool, WAL checkpoint scheduling, and every user's integration pattern. The wrong choice means rewriting the API layer.
- Found by: ambiguity, feasibility
- Suggested answer: Sync-first (embedded single-writer has no I/O multiplexing benefit from async; wire protocol shim can use spawn_blocking internally). This avoids forcing Tokio as a dependency on all users.

**Q10: What is the writer contention behavior?**
- Why this matters: When a second writer tries to acquire the lock — does it block, fail immediately, or timeout? This is a critical API design question that affects every application's architecture. SQLite's `SQLITE_BUSY` and busy-handler callback are core to its usability.
- Found by: gaps, requirements

### Scope and Completion

**Q11: What is the concrete definition of "done" for Phase 1?**
- Why this matters: Without acceptance criteria, Phase 1 is never done — there's always one more operator, one more edge case. The PRD lists a build order but no exit criteria.
- Found by: scope, requirements
- Suggested action: Define a minimal acceptance test — e.g., "insert 1M documents, query by indexed field, survive kill -9, connect with mongosh and run basic CRUD, pass X% of MongoDB CRUD spec tests."

**Q12: Should the wire protocol shim be Phase 1 or Phase 2?**
- Why this matters: It's simultaneously listed as a goal, called "optional, lower priority," given its own user story, and excluded as a "production server." This contradiction will cause prioritization fights. Supporting `mongosh` requires implementing a substantial command surface.
- Found by: ambiguity, scope, stakeholders
- Suggested action: Either defer to Phase 2, or define an exact command allowlist that constitutes "done."

## Important But Non-Blocking

These should be answered, but implementation of lower layers can start while they're resolved.

- **Error taxonomy**: What error types does the API expose? What are recovery expectations for transient vs. fatal vs. data-dependent errors? (gaps, requirements)
- **Document validation**: Does mqlite validate BSON well-formedness on insert? Reject documents exceeding 16MB? Enforce `_id` uniqueness? (gaps)
- **Resource limits**: Max buffer pool memory, max open cursors, max concurrent readers, max WAL size before forced checkpoint. Embedded/IoT deployments can't tolerate unbounded growth. (gaps)
- **Compound index scope**: Compound indexes add significant complexity (concatenated key encoding, prefix queries, per-field sort direction). Consider deferring to Phase 1.1 — single-field + auto `_id` may be sufficient for MVP. (scope, feasibility)
- **Multikey indexes**: `$elemMatch` and `$all` are listed as in-scope operators, but without multikey indexes, array queries require collection scans. Clarify whether multikey indexes are Phase 1. (feasibility)
- **16MB document limit justification**: With 32KB leaf pages, a max-size document spans ~500 pages. For embedded use cases, would 1MB or 4MB simplify the storage engine considerably? (scope, feasibility)
- **Sort and collation**: The PRD lists query operators but never mentions `.sort()`, collation, or locale-aware ordering. Nearly every real query involves ordering. (gaps)
- **In-memory mode**: Story 2 (test fixtures) strongly implies this is needed. If needed, it's a requirement — put it in scope or explicitly cut Story 2. (scope, gaps)
- **Disk-full behavior**: What happens on ENOSPC during write, WAL append, or checkpoint? Does the database remain readable? Can it recover when space is freed? (gaps)
- **Backup API**: "Just copy the file when idle" only works for cold copies. What about hot backups of actively-written databases? SQLite has a backup API for this. (gaps)
- **Thread-safety contract**: Must `Database` and `Collection` handles be `Send + Sync`? What about cursors? Rust's type system enforces some of this, but the intended contract should be specified. (requirements, gaps)
- **Observability hooks**: No metrics, logging, or diagnostic APIs mentioned. Debugging production issues without visibility into buffer pool hit rate, WAL size, query plans, or index usage will be painful. (gaps, stakeholders, requirements)
- **MongoDB Rust driver API mirror scope**: "Where practical" is vague. List the specific types/methods that should have mqlite equivalents, and which should be omitted. (ambiguity, scope)
- **Platform targets**: The edge/IoT use case implies ARM cross-compilation. The test fixture use case implies CI environments. State target platforms explicitly (Linux x86_64, macOS ARM64, Windows, ARM Linux, WASM?). (stakeholders)

## Observations and Suggestions

- **The storage engine is 60-70% of the total effort**, but the PRD treats it as steps 2-3 of a 7-step plan. Consider breaking Phase 1 into sub-phases: 1a (storage engine), 1b (query + API), 1c (wire protocol). This would make timeline estimation more realistic.
- **The 11 open questions include several that are architectural prerequisites**, not deferrable decisions. Specifically: BSON library choice, async vs. sync, WAL checkpointing, and ObjectId generation should be resolved before implementation begins. Suggest a design spike phase to close these.
- **MongoDB trademark/legal review is needed.** Reporting as "standalone mongod" in the wire protocol, marketing as "MongoDB-compatible," and mirroring the driver API could trigger trademark scrutiny (cf. FerretDB, DocumentDB precedents). (stakeholders)
- **File format versioning needs a micro-spec before coding.** Magic bytes, header layout, version migration path — changing this later is extremely painful for an on-disk format. (scope, gaps)
- **Security posture for the wire protocol shim should be explicit.** Even if Phase 1 is unauthenticated, document that it binds localhost-only by default and warn about exposure risks. (stakeholders, gaps)
- **BSON comparison ordering must be designed into the B+ tree from the start.** MongoDB's type comparison rules (MinKey < Null < Numbers < String < ...) affect index key sorting. Retrofitting this is a storage-engine-level change. (feasibility)
- **Use the official `bson` crate.** It's maintained by MongoDB Inc., handles all types correctly, and the dependency cost is modest. Rolling a custom BSON layer would be a multi-month detour. (feasibility)
- **ObjectId generation should use MongoDB-compatible ObjectIds**, not UUIDs. The entire compatibility story depends on it, and the wire protocol would break with UUID-shaped `_id` values. (feasibility, ambiguity)
- **Day-after-launch requests will be**: aggregation pipeline (`$group`, `$project`), `$lookup`/joins, Python/Node bindings, `mongodump`/`mongorestore` compatibility, TTL indexes, unique indexes. Acknowledge these as Phase 2 candidates so they have a home. (scope)
- **The "test fixture" and "production embedded" personas have conflicting needs** (speed/disposability vs. durability/crash recovery). The PRD should acknowledge this tension and specify whether defaults favor one persona. (stakeholders)
- **A test strategy is absent.** No mention of unit tests per layer, integration tests against MongoDB's test suite, fuzz testing, or property-based testing for B+ tree invariants. A storage engine without a test strategy is a time bomb. (requirements)

## Confidence Assessment

| Dimension | Score | Notes |
|-----------|-------|-------|
| Requirements completeness | Low | Goals are qualitative, not testable. No acceptance criteria. |
| Technical feasibility | Medium-High | Buildable, but variable page B+ tree and WAL correctness are high-risk. |
| Scope clarity | Medium | Good non-goals, but MQL boundary and wire protocol scope are ambiguous. |
| Ambiguity level | Medium-Low | Many statements open to multiple interpretations. 11 open questions. |
| Stakeholder coverage | Medium-Low | Primary user clear; security, ops, legal, non-Rust consumers underrepresented. |
| Overall readiness | Medium-Low | Strong vision, sound architecture. Not yet implementable across the full stack. Lower layers can start after Q4-Q8 are answered. |

## Next Steps

- [ ] Human answers critical questions above (Q1-Q12)
- [ ] Update PRD with answers and sharpen specifications
- [ ] Resolve the 11 existing open questions (several overlap with Qs above)
- [ ] Design spike for storage engine (page size, WAL, crash testing strategy)
- [ ] Define Phase 1 acceptance test suite
- [ ] Pour `design` convoy to generate implementation plan
