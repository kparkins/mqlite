# Stakeholder Analysis

## Summary

The PRD identifies a clear primary audience — Rust application developers who want embedded MongoDB-compatible storage — but significantly underrepresents several stakeholder groups who will be affected by this project. The most notable gaps are around operators who manage applications using mqlite in production, the broader MongoDB ecosystem community (driver authors, tooling maintainers), and security-conscious teams evaluating mqlite for sensitive data. The PRD also conflates "embedded library users" and "wire protocol consumers" as a single audience when their needs diverge sharply on topics like authentication, observability, and failure semantics.

Additionally, the document is silent on who builds and maintains mqlite itself beyond Phase 1. Open-source database projects have unusually high support burdens — file format compatibility, data corruption incidents, and migration tooling are support-intensive areas that need to be staffed or explicitly scoped out.

## Findings

### Critical Gaps / Questions

- **Security-conscious users and security reviewers are absent.** The PRD mentions Apache 2.0/MIT licensing and wire protocol auth as an open question (#6), but never addresses data-at-rest encryption, access control for the `.mqlite` file, or threat modeling for the wire protocol shim. Any team evaluating mqlite for PII, health data, or financial records will ask about encryption before adoption. The wire protocol shim, even marked "optional/debugging," creates a network-accessible surface with zero auth by default — this is a security incident waiting to happen.
  - *Why this matters:* A database that stores user data without addressing encryption or access control will be rejected by security review at any regulated organization.
  - *Suggested question:* What is the threat model for mqlite? Specifically: (a) is data-at-rest encryption in scope for Phase 1 or Phase 2, (b) does the wire protocol shim bind to localhost only by default, and (c) who is responsible for documenting security limitations?

- **MongoDB Inc. and trademark/licensing stakeholders are not mentioned.** The PRD proposes reporting as a "standalone mongod" in the wire protocol handshake, mirroring the MongoDB Rust driver's API surface, and using MongoDB's BSON format. MongoDB's SSPL license and trademark policy are relevant here. Projects that claim MongoDB compatibility (e.g., FerretDB, DocumentDB) have navigated legal and trademark scrutiny.
  - *Why this matters:* Calling the wire protocol response "standalone mongod" and mimicking the driver API could trigger trademark concerns. This could block distribution or force a rename post-launch.
  - *Suggested question:* Has legal reviewed the MongoDB trademark implications of (a) the wire protocol handshake identifying as mongod, (b) marketing the project as "MongoDB-compatible," and (c) mirroring the MongoDB Rust driver API?

- **Data loss / corruption incident responders are unaccounted for.** The PRD discusses crash recovery and WAL guarantees, but says nothing about what happens when corruption *does* occur. Who handles support? What diagnostic tools exist? What's the recovery path? Database projects are judged by how they handle failure, not just by how they prevent it.
  - *Why this matters:* When a user reports "my .mqlite file is corrupted," there must be a playbook: diagnostic tooling, a recovery command, and clear documentation of durability guarantees and their limits.
  - *Suggested question:* What is the plan for database corruption diagnostics and recovery tooling? Is a `mqlite check` / `mqlite repair` command in scope for Phase 1?

### Important Considerations

- **Non-Rust language consumers are deferred but will arrive immediately.** The PRD lists "Non-Rust language bindings" as a Phase 1 non-goal, but Story 2 (test fixture database) and Story 3 (mongosh/Compass interop) practically guarantee that Python, Node.js, and Go developers will attempt to use mqlite via the wire protocol shim as a test double. The shim becomes a de facto API for non-Rust users, which contradicts its stated purpose as a debugging tool.
  - *Why this matters:* The wire protocol shim will attract non-Rust users who treat it as a lightweight test MongoDB, placing production-level expectations on a component designed for debugging. This creates support burden and API stability pressure on an "optional" component.
  - *Suggested question:* Should the wire protocol shim be explicitly documented as unsupported for production use by non-Rust consumers, or should its scope be expanded to acknowledge this use case?

- **Library/framework integrators need embedding contracts.** The PRD addresses application developers but not framework/library authors who might embed mqlite as a dependency (e.g., an ORM, a sync engine, a local-first framework). These integrators need: API stability guarantees, minimum supported Rust version (MSRV) policy, file format versioning strategy, and guidance on concurrent access from multiple processes.
  - *Why this matters:* A library that embeds mqlite as a transitive dependency locks its entire user base into mqlite's file format and API stability. Without explicit contracts, any breaking change cascades through the ecosystem.
  - *Suggested question:* What is the MSRV policy and semver commitment? Will file format changes be treated as breaking changes (requiring major version bumps)?

- **Operations/observability teams are invisible.** Even in embedded mode, applications using mqlite need operational insight: database file size, page utilization, WAL size, query performance, index usage statistics. The PRD mentions no metrics, logging, or diagnostic APIs. Developers debugging performance issues in production will have no visibility into what mqlite is doing.
  - *Why this matters:* Without observability hooks, every performance issue becomes "is it mqlite or my code?" with no way to answer the question.
  - *Suggested question:* Is a statistics/metrics API (e.g., `db.stats()`, index hit rates, WAL checkpoint progress) in scope for Phase 1?

- **CI/CD and build system teams are affected.** The "pure Rust, no C/C++ dependencies" constraint has implications for build pipelines, cross-compilation targets, and WASM portability. These teams need to know: what platforms are supported, does it cross-compile to ARM/WASM, and what are the build-time dependencies?
  - *Why this matters:* The edge/IoT use case (Story 4) implies ARM cross-compilation. The test fixture use case (Story 2) implies CI environments with constrained resources. Neither is addressed.
  - *Suggested question:* What are the target platforms for Phase 1? Specifically: Linux x86_64, macOS ARM64, Windows, ARM Linux, WASM?

### Observations

- **The "test fixture" persona (Story 2) has conflicting needs with the "production embedded" persona (Story 1).** Test users want speed, minimal resource usage, and disposability. Production embedded users want durability, crash recovery, and data integrity. These pull in opposite directions on decisions like WAL flush policy, fsync frequency, and in-memory mode. The PRD should acknowledge this tension rather than presenting both as naturally served by the same defaults.

- **The migration/interop story (Stories 3-5) implicitly assumes behavioral compatibility, not just API compatibility.** Users migrating from MongoDB will expect identical sort orders, type coercion rules, and edge-case behavior (e.g., how `$in` handles nested arrays, how `$regex` interacts with indexes). The PRD says "MongoDB API compatibility" but doesn't define the compatibility contract. This will generate a long tail of "it works differently than MongoDB" bug reports.

- **The open-source community as a stakeholder is absent.** If this is Apache 2.0/MIT licensed, external contributors will file issues, submit PRs, and request features. The PRD has no mention of contribution guidelines, governance, or how community feedback flows into prioritization. This is a project-sustainability concern.

- **Documentation consumers are unmentioned beyond "migration guide."** A database engine needs extensive documentation: file format specification (for forensic recovery and third-party tooling), API reference, performance characteristics, known limitations, comparison with MongoDB semantics. Who writes this? When?

- **The "sync to cloud MongoDB" use case (Story 4) implies a sync protocol stakeholder.** If mqlite is used for offline/edge collection with sync to MongoDB, someone needs to build the sync tooling. That team needs: change tracking capabilities (even if change streams are a non-goal), conflict resolution semantics, and a way to read the WAL or equivalent. This is a hidden dependency on features not in scope.

## Confidence Assessment

**Medium-Low.** The PRD clearly identifies its primary user (Rust application developer with MongoDB familiarity) but is thin on secondary stakeholders who will drive significant support, legal, and design decisions. The security, legal/trademark, and operational observability gaps are particularly concerning because they tend to become blocking issues late in development when they're expensive to address. The wire protocol shim's dual identity (debugging tool vs. de facto non-Rust API) is a stakeholder conflict that will cause scope pressure if not resolved early.
