# Technical Feasibility

## Summary

mqlite is an ambitious but fundamentally buildable project. The core premise — an embedded B+ tree storage engine with a WAL, exposed through MongoDB-compatible query semantics — has clear precedents (SQLite, sled, redb) and no theoretical showstoppers. The hardest problems cluster around three areas: (1) getting the WAL + B+ tree + variable page sizes right without data corruption, which is deep systems work that's difficult to test exhaustively; (2) achieving faithful-enough MQL semantics that "MongoDB compatible" doesn't become a persistent source of subtle bugs for users; and (3) the variable page size B+ tree design (4KB internal / 32KB leaf / overflow), which is a non-standard architecture that adds significant complexity compared to uniform page sizes. None of these are impossible, but each one can easily 2-3x the estimated effort if underestimated.

The PRD is well-scoped for a Phase 1. The non-goals are reasonable. The main feasibility risks are not "can this be built?" but rather "can this be built correctly?" — storage engines have a very high correctness bar, and the gap between a working prototype and a crash-safe, corruption-free database is enormous.

## Findings

### Critical Gaps / Questions

- **Variable page size B+ tree is a research-level design decision.** The PRD specifies 4KB internal nodes, 32KB leaf pages, and overflow pages. This is *not* a standard B+ tree — most databases use uniform page sizes (SQLite: 4KB, WiredTiger: 4KB default). Variable page sizes mean the page manager must handle allocation of different-sized regions within a single file, fragmentation, free-space management for multiple size classes, and WAL replay for mixed-size pages. This is substantially harder than a uniform page design.
  - **Why this matters:** A uniform 4KB page design with overflow for large values is well-understood. The proposed variable scheme requires a custom allocator that is essentially a mini filesystem, and every bug in it can cause silent data corruption.
  - **Question:** What is the specific benefit of 32KB leaf pages over 4KB leaf pages with overflow? Has this been benchmarked or is it speculative? Would a simpler uniform page size with overflow-only-for-large-documents work?

- **WAL implementation correctness is the highest-risk component.** The PRD references "SQLite's WAL mode" but SQLite's WAL has 20+ years of hardening, fuzz testing, and adversarial crash testing (the `crashsim` test harness). A greenfield WAL implementation in Rust needs an equivalent testing strategy from day one.
  - **Why this matters:** A WAL bug doesn't show up in normal testing — it manifests as silent corruption after a crash. Without property-based crash testing (e.g., model checking with `loom` or fault injection), you'll ship corruption bugs.
  - **Question:** What is the crash-testing strategy? Will the project use deterministic simulation testing, fault injection, or a `jepsen`-style harness? This should be a first-class work item, not an afterthought.

- **BSON comparison semantics are deceptively complex and under-specified.** MongoDB has very specific type comparison ordering (MinKey < Null < Numbers < String < Object < Array < BinData < ObjectId < Boolean < Date < Timestamp < RegExp < MaxKey). The B+ tree index needs to sort keys according to this ordering. The PRD mentions BSON but doesn't address the comparison semantics.
  - **Why this matters:** If index key ordering doesn't match MongoDB's type comparison rules, indexed queries will return wrong results. This is subtle and hard to retrofit.
  - **Question:** Will the B+ tree key comparator implement MongoDB's full BSON comparison order? This is a prerequisite for correct indexing and needs to be designed into the storage engine from the start, not bolted on at the query layer.

- **"Rust-only implementation: No C/C++ dependencies" conflicts with practical BSON and compression choices.** The PRD says "pure Rust or Rust-safe FFI at most" but also references OP_COMPRESSED (which implies snappy/zlib/zstd compression) and may want to use the official `bson` crate (which has optional C dependencies for performance).
  - **Why this matters:** Pure-Rust compression libraries exist (e.g., `snap`, `flate2` with Rust backend, `zstd` with Rust bindings) but some have performance gaps vs. C implementations. The constraint needs clarification — does "no C dependencies" mean "no C in the dependency tree" or "no C that we write ourselves"?
  - **Question:** How strict is the "no C/C++ dependencies" constraint? Does it extend to transitive dependencies (e.g., `zstd-sys`)? This affects choices for compression, BSON, and potentially crypto if auth is ever added.

- **Single-file constraint + WAL = two files (at minimum).** SQLite's WAL mode uses two files: the main database file and a `-wal` file (plus a `-shm` file for shared memory). The PRD says "single `.mqlite` file" but a WAL-based design almost certainly requires a separate WAL file, at least while the WAL is active.
  - **Why this matters:** This is either a documentation issue (the "single file" is the quiescent state, with a WAL file present during operation) or a design constraint that requires embedding the WAL inside the main file (much more complex, non-standard).
  - **Question:** Does "single file" mean one file when the database is cleanly closed, with a WAL file present during operation (like SQLite)? Or must it literally be one file at all times? The latter significantly complicates the WAL design.

### Important Considerations

- **Compound index implementation is a significant additional scope item.** The PRD lists compound indexes but doesn't elaborate. Compound indexes require: concatenated key encoding that preserves per-field sort order, support for prefix queries, and correct handling of mixed ascending/descending fields. This is not trivial and is probably 2-3 weeks of work on its own.
  - Consider deferring compound indexes to Phase 1.1 and shipping with single-field indexes only.

- **Heuristic query planner vs. actually useful index selection.** The PRD says "heuristic planner" but MQL queries can involve `$and`/`$or` with multiple fields, nested conditions, and array operators. Even a "simple" planner needs to handle: single-field index selection for equality, range index selection for `$gt`/`$lt`, index intersection for compound queries, and full-collection-scan fallback.
  - The planner is low risk but high effort if "heuristic" actually means "does something useful for compound conditions." Define the planner's scope explicitly: does it handle `$or` with multiple index scans? Does it consider index covering?

- **The `$elemMatch` array operator interacts badly with indexing.** MongoDB's multikey indexes are complex — a single document with an array field generates multiple index entries. The PRD lists `$elemMatch` and `$all` as in-scope operators but doesn't mention multikey indexes.
  - **Why this matters:** Without multikey indexes, array queries require a collection scan. With multikey indexes, the storage engine must handle one-to-many document-to-index-key mappings, and the query planner must understand multikey bounds.
  - Clarify whether multikey indexes are in scope for Phase 1.

- **The wire protocol shim's "reports as standalone mongod" may trigger driver behaviors that mqlite can't support.** MongoDB drivers negotiate capabilities via the `hello` handshake. If mqlite reports capabilities it doesn't actually support (e.g., sessions, read concern levels, write concern `w: "majority"`), drivers may send unsupported commands or assume guarantees that don't hold.
  - The shim should carefully limit the capabilities it advertises, and the PRD should specify which driver versions/behaviors are targeted.

- **`fsync` semantics vary significantly across platforms and filesystems.** The PRD mentions "synchronous/fsync-per-commit modes" as an open question. On Linux, `fdatasync` doesn't flush file metadata; on macOS, `fsync` doesn't guarantee disk-level durability without `fcntl(F_FULLFSYNC)`. On some filesystems, `fsync` on the file doesn't flush the directory entry.
  - This is well-trodden ground (SQLite handles it) but it's a source of real corruption bugs if not handled per-platform from the start. The project should enumerate target platforms and their durability primitives early.

- **Max document size of 16MB + overflow pages implies a single insert can touch many pages.** A 16MB document at 32KB per leaf page requires ~500 overflow pages. This has implications for WAL size during writes and checkpoint performance.
  - Consider whether 16MB is necessary for Phase 1 embedded use cases, or if a smaller limit (e.g., 4MB) would simplify the storage engine considerably.

### Observations

- **The `bson` crate from the official MongoDB Rust driver is the pragmatic choice.** It's maintained by MongoDB, Inc., handles all BSON types correctly, and the dependency cost is modest. Rolling a custom BSON layer is tempting for control but would be a multi-month effort to match edge-case handling.

- **In-memory mode should be trivial if the page manager abstracts I/O.** The PRD mentions this as a "key decision" but if the storage engine is built on a page manager trait with `read_page`/`write_page`, an in-memory implementation is straightforward. Design for it from the start (trait-based I/O) but don't build it until needed.

- **The async vs. sync API question has a clear answer for Phase 1: sync.** An embedded single-writer database has no I/O multiplexing benefit from async. The wire protocol shim (if async for connection handling) can call sync storage methods via `spawn_blocking`. Async-first would add Tokio as a required dependency for all users, which conflicts with the "zero-dependency" ethos.

- **The build order in the PRD (bottom-up, storage first) is correct.** This is the only order that works for a storage engine — you cannot test the query engine without the storage layer. The risk is that upper layers reveal storage engine API deficiencies late. Mitigate with early integration tests at each layer boundary.

- **ObjectId generation (Open Question #8) has a simple answer: use MongoDB-compatible ObjectIds.** The whole point is MongoDB compatibility. UUIDs would break user expectations and make migration harder. The `bson` crate already provides ObjectId generation.

## Confidence Assessment

**Medium-High.** The PRD is well-structured, the non-goals are clearly stated, and the build order is sound. The main feasibility risks are in the implementation details of the storage engine (variable page sizes, WAL correctness, BSON sort order) rather than in the overall architecture. The gaps identified above are answerable questions, not fundamental blockers. The project is buildable by a team with storage engine experience, but the variable page B+ tree and WAL correctness require careful design work before coding begins. If the team lacks prior experience with crash-safe storage engines, estimate 2-3x the naive timeline for the storage layer specifically.
