# Missing Requirements

## Summary

The PRD defines a clear vision and architecture for an embedded MongoDB-compatible database, but it leaves several production-critical requirements unaddressed. The most significant gaps center on security (no authentication or authorization model for the native API), data lifecycle management (no migration, upgrade, or backup/restore story), and operational observability (no metrics, logging, or debugging guidance for applications embedding the library). Concurrent access semantics are described at a high level (SWMR) but the precise failure modes, error surfaces, and guarantees visible to callers are unspecified.

Additionally, the PRD lacks requirements around file integrity verification, resource limits, graceful degradation under disk pressure, and the developer experience for error handling. These are the kinds of gaps that won't block initial development but will cause production incidents, support escalations, and painful retrofits if not addressed before the API surface stabilizes.

## Findings

### Critical Gaps / Questions

- **No authentication or authorization model for the native API.** The PRD asks about wire protocol auth (Open Question #6) but says nothing about the embedded Rust API. If mqlite is used in multi-tenant applications or shared-process environments, there is no mechanism to restrict which code paths can access which collections, drop databases, or modify indexes. Even SQLite has an authorizer callback. *Question: Should the native API support an authorization hook or callback that embedders can use to enforce access control?*

- **No file locking or multi-process access semantics.** The PRD describes SWMR concurrency within a process (threads), but never specifies what happens when two separate OS processes open the same `.mqlite` file. SQLite uses file locks (POSIX `fcntl` or Windows locks) to coordinate multi-process access. Without this, silent data corruption is a likely production incident. *Question: Is multi-process access to the same file a supported use case? If so, what locking protocol will be used? If not, how is it prevented or detected?*

- **No error model or error handling requirements.** The PRD specifies operations (insert, find, update, delete) but never defines what errors look like. What happens on disk full? Corrupted page? WAL overflow? BSON validation failure? Duplicate `_id`? The native API's error surface is one of the most important parts of the contract for embedders. *Question: What error types will the API expose, and what are the recovery expectations for each class of error (transient vs. fatal vs. data-dependent)?*

- **No data migration or schema evolution story.** Documents are schemaless, but the file format itself has a schema (page layout, metadata structures, index format). The PRD requires file format versioning (constraint) but never specifies how upgrades happen. What does a user do when they have a v1 file and install a library version that writes v2? *Question: Will mqlite support in-place file format upgrades, or will users need an explicit migration tool? What is the backward/forward compatibility contract?*

- **No document validation requirements.** MongoDB supports JSON Schema validation on collections. The PRD doesn't mention whether mqlite will validate documents on insert/update, enforce any shape constraints, or even validate that inserted BSON is well-formed beyond having an `_id`. *Question: Will mqlite enforce any document validation? At minimum, will it reject malformed BSON, or is the caller responsible for well-formedness?*

- **No concurrent access failure semantics.** The PRD says "single writer / multiple readers" but doesn't specify: What happens when a second writer tries to acquire the lock? Block? Fail immediately? Timeout? Is the timeout configurable? This is a critical API design question that affects every application's architecture. SQLite's `SQLITE_BUSY` and busy-handler callback are core to its usability. *Question: What is the writer contention behavior? Block, fail, timeout? Is it configurable?*

### Important Considerations

- **No resource limits or budgeting.** The PRD doesn't address maximum memory consumption for the buffer pool, maximum number of open cursors, maximum number of concurrent readers, or maximum WAL size before forced checkpoint. Embedded databases are often deployed in resource-constrained environments (IoT, CLI tools) where unbounded memory growth is unacceptable. *The next engineer will need to know: what are the configurable resource knobs and their defaults?*

- **No disk-full or low-disk-space behavior.** What happens when a write (or WAL append, or checkpoint) fails due to ENOSPC? Does the database remain readable? Can it recover when space is freed? SQLite has specific behaviors here that users rely on. This is a top-3 ops question at launch.

- **No backup/restore or hot-copy semantics.** The PRD mentions "just copy the file when the writer is idle" (Story 5), but this only works for cold copies. What about backing up a database that is actively being written to? SQLite has a backup API for this. Without one, users will copy files mid-write and get corrupted backups.

- **No observability or instrumentation requirements.** Embedded databases need logging, metrics, or tracing hooks for the embedding application. How does an app know the buffer pool hit rate, WAL size, checkpoint frequency, slow queries, or index usage? Without this, debugging production issues in applications using mqlite will be extremely painful.

- **No `_id` uniqueness enforcement specification.** The PRD mentions auto `_id` index creation but doesn't explicitly state that `_id` uniqueness is enforced on insert/update, or what happens on violation (error? silent overwrite?). This is fundamental to MongoDB compatibility.

- **No behavior spec for the in-memory mode mentioned in the rough approach.** The "Key technical decisions" section mentions supporting in-memory mode but this is absent from goals, constraints, and user stories. If supported, does it have the same API? Same concurrency model? Different durability guarantees (obviously)? This needs to be explicitly scoped in or out.

- **No maximum collection or index count limits.** What happens when someone creates thousands of collections in a single file? Thousands of indexes on a collection? The catalog layer needs bounds or at minimum documented behavior under stress.

- **No specification for sort behavior or collation.** MQL queries frequently use `.sort()`. The PRD lists query operators but never mentions sorting, collation (case sensitivity, locale-aware ordering), or how sort interacts with indexes. This is a significant functional gap since nearly every real query involves ordering.

- **No cursor lifecycle or resource cleanup requirements.** The PRD mentions "cursor-based result iteration" but doesn't specify: How long do cursors live? What resources do they hold (reader snapshots, buffer pool pages)? What happens if a caller drops a cursor without closing it? Can open cursors block WAL checkpointing (as in SQLite)?

- **No specification for `null`, missing fields, or type comparison ordering.** MongoDB has specific rules for comparing values of different BSON types and for how `null`/missing fields behave in queries and sorts. These are subtle but critical for compatibility. The PRD should specify whether MongoDB's type comparison order is followed.

### Observations

- **Wire protocol shim security surface is unaddressed.** Even if "Phase 1 is unauthenticated," the PRD should specify: Will it bind to localhost only? Can the bind address be configured? What is the risk if someone exposes it on 0.0.0.0? A single sentence establishing the security posture would prevent a class of user-deployed vulnerabilities.

- **No mention of BSON size validation.** MongoDB enforces a 16MB document limit. The PRD mentions this as a constraint but doesn't specify where enforcement happens (insert time? storage layer? both?) or what error is returned.

- **No fsync/durability configuration.** Open Question #10 asks about durability levels but the PRD doesn't establish a default or enumerate the options. For an embedded database, the tradeoff between `fsync`-per-write (safe, slow) and periodic-fsync (fast, small window of loss) is a core configuration decision that affects every user.

- **No test compatibility target.** The PRD mentions MongoDB API compatibility but doesn't reference MongoDB's own test suite or specify a subset of MongoDB's behavior that mqlite aims to pass. A concrete compatibility target (e.g., "pass MongoDB's CRUD spec tests for the supported operators") would make "done" measurable.

- **No thread-safety contract for API objects.** Can a `Database` handle be shared across threads? Can `Collection` handles? Cursors? Rust's type system (Send/Sync) will enforce some of this, but the intended contract should be specified so the API can be designed accordingly.

- **No guidance on temp file usage.** Will mqlite create temporary files during operation (WAL, journal, shared-memory file like SQLite's `-shm`)? If so, the PRD should specify naming, location, and cleanup behavior. Users deploying to read-only filesystems or containers with limited tmpfs will need to know.

- **ObjectId compatibility affects wire protocol correctness.** Open Question #8 suggests UUID might be acceptable for `_id`, but if the wire protocol shim reports as a mongod, drivers and tools will expect ObjectId-shaped `_id` values. Using UUIDs would break driver assumptions and tool compatibility. This should be a firm decision, not an open question.

## Confidence Assessment

**Medium-Low.** The PRD covers the "what" (features, architecture, build order) well but is thin on the "how it behaves" in production. The critical gaps around multi-process locking, error model, writer contention semantics, and data migration are the kind of requirements that, if left unspecified, result in incompatible implementations or production-breaking behavior that is expensive to change post-1.0. The open questions section shows awareness of some uncertainties, but several of the gaps identified above (file locking, error model, resource limits, backup) aren't even mentioned as open questions yet.
