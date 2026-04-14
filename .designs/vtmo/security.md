# Security Analysis

## Summary

mqlite presents two fundamentally different security profiles depending on deployment mode: as an embedded library, it inherits the process's trust boundary and its primary threats are malicious input data (crafted BSON, ReDoS patterns, malformed files) and resource exhaustion; as a wire-protocol-accessible service, it exposes an unauthenticated network listener that accepts arbitrary MongoDB commands, making it vulnerable to every class of network attack. Phase 1's decision to ship the wire protocol shim without authentication is the single highest-severity security concern — it converts mqlite from a library with local-only attack surface into a network service with zero access control.

The Rust implementation provides strong baseline memory safety guarantees that eliminate entire vulnerability classes (buffer overflows, use-after-free, double-free) that plague C/C++ database engines. However, Rust's safety guarantees have boundaries: `unsafe` blocks in the storage engine (page manipulation, mmap if used, FFI to compression libraries), BSON parsing of untrusted input, and the regex engine's susceptibility to catastrophic backtracking all represent attack surface that Rust's type system does not protect. The single-file storage model introduces file-level threats (crafted headers, symlink attacks, race conditions during backup) that must be addressed at the design level. This analysis enumerates 16 attack surface categories, rates each by severity and likelihood, and recommends concrete mitigations for Phase 1.

## Threat Model

### Threat Actors

| Actor | Motivation | Capability | Relevant Modes |
|-------|-----------|------------|----------------|
| **Malicious local user** | Data theft, corruption, DoS | File system access, can craft .mqlite files | Embedded + Wire |
| **Untrusted network client** | RCE, data exfiltration, DoS | Can connect to wire protocol listener, send arbitrary OP_MSG | Wire protocol only |
| **Malicious document author** | DoS, data corruption, injection | Controls document content inserted via application | Embedded + Wire |
| **Supply chain attacker** | RCE, backdoor | Compromised dependency (bson crate, compression lib) | Build time |
| **Malicious file provider** | Code execution, data corruption | Provides a crafted .mqlite file that the application opens | Embedded |
| **Co-tenant process** | Data access, corruption | Shares filesystem, may attempt concurrent file access | Embedded |

### Trust Boundaries

```
┌─────────────────────────────────────────────────────────────┐
│                    APPLICATION PROCESS                       │
│                                                             │
│  ┌──────────────┐    ┌──────────────────────────────────┐   │
│  │  Application  │    │          mqlite library           │   │
│  │    Code       │───>│  Native API (trusted boundary)   │   │
│  │  (trusted)    │    │  ┌────────────┐  ┌────────────┐  │   │
│  └──────────────┘    │  │Query Engine│  │  Storage   │  │   │
│                      │  └────────────┘  │  Engine    │  │   │
│                      │                  └─────┬──────┘  │   │
│                      └────────────────────────┼─────────┘   │
│                                               │             │
│  ┌──────────────────────────────────┐         │             │
│  │    Wire Protocol Shim            │         │             │
│  │  (UNTRUSTED BOUNDARY — network)  │◄── TCP  │             │
│  └──────────────────────────────────┘         │             │
└───────────────────────────────────────────────┼─────────────┘
                                                │
                                    ┌───────────▼───────────┐
                                    │   .mqlite file        │
                                    │   (filesystem)        │
                                    └───────────────────────┘
```

**Trust boundary 1 — Native API**: The caller is in-process and trusted. Input validation focuses on correctness (well-formed BSON, valid operators) rather than adversarial defense. However, if the application passes user-controlled input directly into query filters or regex patterns, injection is possible.

**Trust boundary 2 — Wire Protocol Shim**: The caller is a network client and is UNTRUSTED. Every byte received must be treated as adversarial. This is the highest-risk boundary. In Phase 1, there is no authentication — any client that can reach the port has full read/write access.

**Trust boundary 3 — File System**: The .mqlite file may have been crafted by an attacker (e.g., downloaded, received via email, opened from untrusted source). The storage engine must validate file integrity and not trust header values, page pointers, or embedded metadata without verification.

### Attack Vectors

1. **Network → Wire Protocol**: Unauthenticated command execution, BSON bombs, connection flooding
2. **Application → Native API**: MQL injection via user-controlled filter values, regex injection
3. **File System → Storage Engine**: Crafted .mqlite files, symlink attacks, TOCTOU races
4. **Dependencies → Build**: Compromised crate supply chain
5. **Concurrent Access → Storage**: Writer starvation, lock contention, multi-process corruption

## Analysis

### Key Considerations

- **Rust's memory safety is the strongest single security property.** Buffer overflows, use-after-free, and data races (the top 3 vulnerability classes in C/C++ databases) are eliminated at compile time outside of `unsafe` blocks. This is a major advantage over SQLite, WiredTiger, and every C-based storage engine.
- **The wire protocol shim without authentication is a critical risk.** Even binding to localhost, any local process can connect. In containerized environments, "localhost" may be shared across containers. Phase 1 must document this prominently and ideally support a "no listen" compile-time flag.
- **BSON parsing of untrusted input is the primary code-level attack surface.** The `bson` crate handles most edge cases, but mqlite must enforce depth limits, size limits, and type validation on top of it.
- **The `$regex` operator is the highest-risk query feature.** Rust's `regex` crate uses a finite automaton and is ReDoS-resistant by design, but if mqlite supports PCRE-compatible features (backreferences, lookahead) via a different engine, ReDoS becomes possible.
- **Single-file storage means the file IS the database — file permissions ARE access control.** There is no separate auth layer for the embedded case. The `.mqlite` file's OS permissions are the only thing between an attacker and full data access.
- **Phase 1 has no encryption at rest.** Data in the `.mqlite` file is plaintext. This is a blocking concern for any deployment handling PII, health data, or financial records.

### Options Explored

#### Option 1: Wire Protocol — Localhost-Only, Unauthenticated (Phase 1 Baseline)

- **Description**: The wire protocol shim binds to `127.0.0.1` only, with no authentication. This is the simplest approach and matches the PRD's "debugging/interop" positioning.
- **Pros**: Minimal implementation complexity. Sufficient for `mongosh` debugging. Matches SQLite's model (no built-in auth).
- **Cons**: Any local process can read/write all data. Containerized deployments may expose localhost. Users will inevitably bind to `0.0.0.0` for convenience, creating a remotely exploitable unauthenticated database. No audit trail of who accessed what.
- **Effort**: Low (default behavior)

#### Option 2: Wire Protocol — SCRAM-SHA-256 Authentication (Phase 2)

- **Description**: Implement MongoDB's SCRAM-SHA-256 authentication mechanism in the wire protocol handshake. Clients must authenticate before executing commands.
- **Pros**: Standard MongoDB authentication. Drivers support it natively. Prevents unauthorized access even on misconfigured networks.
- **Cons**: Significant implementation effort (SCRAM state machine, credential storage, user management commands). Increases wire protocol surface area. Overkill for "debugging tool" positioning.
- **Effort**: High

#### Option 3: Wire Protocol — Unix Socket Only + Optional Token Auth

- **Description**: Default to Unix domain socket (no TCP). Optionally accept a shared secret token in the handshake for TCP mode.
- **Pros**: Unix socket eliminates network exposure entirely. Token auth is simple to implement. Covers the debugging use case securely.
- **Cons**: Windows compatibility requires named pipes or TCP fallback. Token auth is not MongoDB-standard (drivers won't support it natively). Adds a custom auth mechanism to maintain.
- **Effort**: Medium

#### Option 4: BSON Hardening — Defense-in-Depth Parsing

- **Description**: Layer mqlite-specific validation on top of the `bson` crate: max nesting depth (e.g., 100 levels), max document size enforcement at parse time, max field name length, max array element count, and type allowlisting for index keys.
- **Pros**: Prevents BSON bombs, deeply-nested documents that exhaust stack, and oversized documents that exhaust memory. Low implementation cost.
- **Cons**: May reject documents that MongoDB would accept (compatibility tradeoff). Needs careful threshold selection.
- **Effort**: Low

#### Option 5: Regex Safety — Use Rust `regex` Crate Exclusively

- **Description**: Implement `$regex` using only Rust's `regex` crate (finite automaton, guaranteed linear time). Do not support PCRE features (backreferences, lookahead/lookbehind). Document the incompatibility with MongoDB's PCRE-based `$regex`.
- **Pros**: Eliminates ReDoS entirely. The `regex` crate is one of the best-audited Rust libraries. Linear-time guarantee means no query can become a CPU bomb.
- **Cons**: MongoDB's `$regex` supports PCRE features that Rust's `regex` does not (backreferences, lookahead). Some user patterns will fail that work in MongoDB. This is a compatibility gap.
- **Effort**: Low (use the existing crate as-is)

#### Option 6: File Format Integrity — Checksummed Pages

- **Description**: Every page in the .mqlite file includes a CRC32 or xxHash checksum. The storage engine validates checksums on read. A header magic number and version field prevent opening non-mqlite files.
- **Pros**: Detects corruption from disk errors, partial writes, and malicious file modification. Prevents parsing garbage data as valid pages. Standard practice in production databases.
- **Cons**: Adds ~4 bytes per page overhead. Checksum computation has CPU cost (negligible for CRC32). Does not prevent a sophisticated attacker who can recompute checksums.
- **Effort**: Low-Medium

#### Option 7: Encryption at Rest — Full Database Encryption

- **Description**: Encrypt the .mqlite file using AES-256-GCM with a user-provided key. The WAL and all pages are encrypted. Key derivation from passphrase via Argon2.
- **Pros**: Protects data from physical theft, backups being compromised, and unauthorized file access. Feature parity with SQLCipher.
- **Cons**: Significant implementation complexity. Key management is the user's problem. Performance impact (~10-20% for AES-NI hardware). Cannot use mmap with encrypted pages. Pure-Rust AES implementations exist but are slower than hardware-accelerated C.
- **Effort**: High

### Recommendation

**Phase 1 mitigations (must-do):**

1. **Wire protocol: bind localhost-only by default, document loudly.** The default bind address MUST be `127.0.0.1`. Binding to `0.0.0.0` should require explicit opt-in with a warning in logs. The README, API docs, and wire protocol startup output should all warn: "Wire protocol is unauthenticated. Do not expose to untrusted networks."

2. **Wire protocol: add a `--require-token` flag** (Option 3 partial). A simple shared-secret token passed in the connection handshake metadata. Not MongoDB-standard, but prevents casual unauthorized access. Low effort, high value.

3. **BSON hardening (Option 4): enforce at parse boundary.** Max nesting depth = 100. Max document size = 16MB (enforced at BSON parse, not just storage). Max field count per document = 10,000. Max field name length = 1024 bytes. Reject documents exceeding these limits with a clear error.

4. **Regex: use Rust `regex` crate only (Option 5).** Document the PCRE incompatibility. Add a per-query timeout (e.g., 5 seconds default) as defense-in-depth even though the `regex` crate is linear-time.

5. **File format: checksummed pages (Option 6).** CRC32C for every page. Magic bytes and version in the file header. Validate on every page read. Reject files that fail checksum validation with a clear "file corrupted" error.

6. **File permissions: set restrictive defaults.** `Database::open()` should create new files with mode `0600` (owner read/write only). Document that file permissions are the access control mechanism for embedded mode.

7. **Resource limits: enforce bounded allocation.** Max concurrent readers (configurable, default 64). Max WAL size before forced checkpoint (configurable, default 100MB). Max buffer pool memory (configurable, default 64MB). Max result set size per cursor batch.

8. **`$where` and server-side JavaScript: do not implement.** These are the #1 injection vector in MongoDB. Phase 1 non-goal. Return an explicit "unsupported" error.

**Phase 2 mitigations (plan for but don't build):**

- SCRAM-SHA-256 authentication for wire protocol
- Encryption at rest (AES-256-GCM with key derivation)
- TLS for wire protocol connections
- Audit logging (who accessed what, when)
- File integrity verification tool (`mqlite check`)

## Attack Surface Map

### Critical Severity

| # | Surface | Threat | Severity | Likelihood | Mitigation |
|---|---------|--------|----------|------------|------------|
| 1 | **Wire protocol: unauthenticated listener** | Any local process (or remote, if misconfigured) can read/write all data, drop collections, create indexes. Full database compromise. | **Critical** | **High** (users will misconfigure bind address) | Localhost-only default. Token auth. Prominent documentation. Startup warning banner. |
| 2 | **Wire protocol: no TLS** | All data transmitted in plaintext over TCP. BSON documents, query patterns, and results are visible to network sniffers. | **Critical** | **Medium** (mostly local/debug use, but some will deploy to networks) | Document as plaintext. Phase 2: TLS support. |

### High Severity

| # | Surface | Threat | Severity | Likelihood | Mitigation |
|---|---------|--------|----------|------------|------------|
| 3 | **BSON parsing: malformed documents** | Crafted BSON with invalid lengths, recursive nesting, or oversized fields can cause memory exhaustion, stack overflow, or parser panics. | **High** | **Medium** | Enforce depth limit (100), size limit (16MB), field count limit. Fuzz test the BSON intake path. |
| 4 | **`$regex`: ReDoS patterns** | User-controlled regex patterns with catastrophic backtracking can pin a CPU core indefinitely, blocking the single writer. | **High** | **High** (regex from user input is common) | Use Rust `regex` crate (linear-time guarantee). Add per-query timeout. Do NOT use PCRE. |
| 5 | **Crafted .mqlite files** | A malicious file with corrupted page pointers, invalid B+ tree structure, or forged checksums can cause the storage engine to read arbitrary file offsets, corrupt memory, or enter infinite loops. | **High** | **Medium** (relevant for apps that open user-provided files) | Checksummed pages. Validate page pointers against file bounds. Validate B+ tree invariants on traversal. Magic bytes + version check. |
| 6 | **No encryption at rest** | The .mqlite file is plaintext on disk. Filesystem access = full data access. Backups, cloud storage, lost devices all expose data. | **High** | **High** (every deployment without FDE) | Document limitation. File permissions 0600. Phase 2: encryption at rest. |
| 7 | **MQL operator injection** | If application passes user input directly into filter documents (e.g., `{"field": user_input}` where user_input is `{"$gt": ""}`), attackers can inject query operators to bypass intended filters. | **High** | **High** (extremely common pattern in web apps) | Document safe API usage. Provide typed query builder API that prevents operator injection by construction. Validate that filter values don't contain unexpected operator keys when string values are expected. |

### Medium Severity

| # | Surface | Threat | Severity | Likelihood | Mitigation |
|---|---------|--------|----------|------------|------------|
| 8 | **Denial of service: large document writes** | Inserting many 16MB documents rapidly fills disk and WAL. A single malicious writer can exhaust storage. | **Medium** | **Medium** | Configurable max document size. WAL size limit with forced checkpoint. Disk space monitoring hooks. |
| 9 | **Denial of service: unbounded cursor results** | A `find({})` on a large collection with no limit returns the entire dataset, consuming memory proportional to collection size. | **Medium** | **Medium** | Default batch size limit (101 documents, matching MongoDB default). Max cursor memory budget. Cursor timeout for idle cursors. |
| 10 | **Writer starvation** | A flood of reader connections or long-running read transactions prevent WAL checkpointing, causing the WAL to grow unboundedly. The single writer may be starved if checkpoint is blocked. | **Medium** | **Low-Medium** | Max WAL size with forced checkpoint. Max reader snapshot age. Configurable checkpoint policy. |
| 11 | **Concurrency: multi-process file corruption** | Two OS processes opening the same .mqlite file without proper file locking will corrupt the database silently. | **Medium** | **Medium** (common in deployment, especially with cron jobs) | POSIX `fcntl` / Windows file locking. Detect and reject concurrent access from other processes. Document multi-process semantics. |
| 12 | **Symlink attacks** | Attacker creates a symlink at the expected .mqlite path pointing to a sensitive system file (e.g., `/etc/passwd`). `Database::open()` follows the symlink and overwrites the target. | **Medium** | **Low** | Use `O_NOFOLLOW` on open. Resolve path before write. Document symlink behavior. |
| 13 | **Temp file leakage** | WAL file, shared memory file, or other transient files created during operation may contain sensitive data and persist after crash. | **Medium** | **Medium** | Predictable naming (`.mqlite-wal`, `.mqlite-shm`). Cleanup on normal close. Document residual files after crash. Use restrictive permissions (0600). |
| 14 | **Wire protocol: command injection** | Malformed OP_MSG with invalid section kinds, incorrect checksums, or oversized payloads could exploit parsing bugs in the wire protocol layer. | **Medium** | **Medium** | Strict OP_MSG parsing with size limits. Validate section kinds. Reject oversized messages (configurable, default 48MB matching MongoDB). Fuzz test the wire protocol parser. |
| 15 | **Backup race conditions** | Copying the .mqlite file while the database is active (writer is running) produces an inconsistent copy if WAL is a separate file. Even with a single file, a copy during checkpoint produces a torn read. | **Medium** | **High** (users will try this) | Provide a backup API that acquires a read snapshot and copies consistently. Document that naive `cp` during writes is unsafe. Offer a `PRAGMA`-like checkpoint-and-lock for safe cold copy. |

### Low Severity

| # | Surface | Threat | Severity | Likelihood | Mitigation |
|---|---------|--------|----------|------------|------------|
| 16 | **Supply chain: dependency compromise** | The `bson` crate, compression libraries (`snap`, `flate2`, `zstd`), or other dependencies could be compromised to introduce backdoors or vulnerabilities. | **Low** | **Low** | Pin dependency versions. Use `cargo-audit` in CI. Minimize dependency count. Prefer well-maintained, widely-used crates. Review `unsafe` blocks in dependencies. |
| 17 | **Resource exhaustion: file descriptors** | Each reader holds a file descriptor. Unbounded readers exhaust the process's FD limit, causing failures in the application (not just mqlite). | **Low** | **Low** | Configurable max reader count. Document FD usage. Gracefully reject new readers when limit is reached. |
| 18 | **Resource exhaustion: memory** | Buffer pool, cursor state, and WAL pages consume memory. Without limits, a heavily-loaded mqlite instance can cause OOM in the host process. | **Low** | **Medium** | Configurable buffer pool cap. LRU eviction. Max cursor count. Max WAL size. Document memory usage characteristics. |
| 19 | **MongoDB trademark/legal** | Reporting as "standalone mongod" in wire protocol and marketing as "MongoDB-compatible" may trigger MongoDB Inc. trademark enforcement. | **Low** (security) | **Medium** (legal) | Report as "mqlite" in handshake, not "mongod". Use "MQL-compatible" rather than "MongoDB-compatible" in marketing. Legal review before public release. Precedent: FerretDB navigated this successfully. |
| 20 | **Wire protocol: version mismatch** | Reporting a MongoDB server version that implies capabilities mqlite lacks (sessions, transactions, read concern) causes drivers to send unsupported commands, leading to confusing errors or silent data loss. | **Low** | **Medium** | Report minimum viable version. Strip unsupported capabilities from `hello` response. Return explicit "not supported" errors for unimplemented commands. |

## Constraints Identified

1. **No authentication in Phase 1 wire protocol** — This is a stated non-goal. The mitigation (localhost-only, documentation, optional token) must be robust because the vulnerability is by design.

2. **No encryption at rest in Phase 1** — Blocks adoption for regulated data. Must be clearly documented as a limitation. File permissions are the only protection.

3. **Single-writer model** — Limits DoS impact (only one writer at a time) but also means the writer is a single point of failure for availability. A blocked writer blocks all writes.

4. **Pure Rust constraint** — Limits crypto library choices (no OpenSSL). Pure-Rust AES (e.g., `aes-gcm` crate) is available but slower than hardware-accelerated C. Affects Phase 2 encryption performance.

5. **MongoDB wire protocol compatibility** — The handshake must report enough capability to satisfy drivers without claiming capabilities that don't exist. This is a narrow design space.

6. **BSON format is externally defined** — mqlite must accept any valid BSON, including types it may not fully support (Decimal128, JavaScript, etc.). The attack surface of BSON parsing is inherited, not designed.

7. **File format must be stable within major versions** — Security fixes to the file format (e.g., adding checksums, changing page layout) must be backward-compatible or provide migration tooling.

## Open Questions

1. **Does the wire protocol shim support TLS in Phase 2?** If so, the socket layer needs to be designed for TLS from the start (async I/O, certificate management). If not, the wire protocol should be documented as "debug-only, never expose to networks."

2. **What is the `unsafe` budget for the storage engine?** Page manipulation, buffer pool management, and potential mmap usage may require `unsafe`. Should there be a policy (e.g., "all unsafe must be in a dedicated `raw` module, reviewed by two engineers, and covered by Miri tests")?

3. **Should mqlite validate BSON on insert or trust the caller?** In embedded mode, the caller is trusted — validation adds overhead. In wire protocol mode, the caller is untrusted — validation is mandatory. Should there be a "trusted mode" (skip validation) vs. "untrusted mode" (full validation)?

4. **Is multi-process file access a supported use case?** If yes, file locking is a security-critical component (incorrect locking = silent corruption). If no, how do we detect and prevent it?

5. **What compression libraries will be used for OP_COMPRESSED?** Each library (snappy, zlib, zstd) has its own vulnerability history. The choice affects supply chain risk. Pure-Rust implementations exist for all three but vary in maturity.

6. **Should the file format include HMAC-based integrity (not just CRC32)?** CRC32 detects accidental corruption but not intentional tampering. HMAC requires a key, which ties into the encryption-at-rest story. For Phase 1, CRC32 is likely sufficient.

7. **What is the maximum nesting depth for BSON documents?** MongoDB uses 100. This directly affects stack usage during recursive operations (query matching, BSON traversal). The limit should be enforced at parse time.

8. **How will mqlite handle MongoDB's server-side JavaScript features ($where, $function, mapReduce)?** These are the highest-risk MongoDB features. Recommendation: do not implement, return explicit "unsupported" errors. But this needs to be a firm decision, not an implicit omission.

## Integration Points

### → Storage Engine Design
- Page checksums must be designed into the page format from day one (cannot be retrofitted without file format change)
- File header must include magic bytes, version, and integrity metadata
- Page pointer validation bounds-checking affects B+ tree traversal hot path — must be efficient
- `O_NOFOLLOW` and file permission semantics affect `Database::open()` implementation
- File locking strategy (fcntl vs. flock vs. lockfile) affects multi-process safety

### → Query Engine Design
- BSON validation depth and size limits affect every insert/update path
- `$regex` engine choice (Rust `regex` vs. PCRE) affects compatibility and ReDoS risk
- Query timeout mechanism needs integration with the cursor/execution layer
- MQL operator injection prevention may influence the native API design (typed builders vs. raw BSON)

### → Wire Protocol Design
- Default bind address (localhost-only) is a network configuration decision
- Token auth (if implemented) adds to the handshake state machine
- OP_MSG size limits and section validation are parse-level concerns
- The `hello` response capability advertisement directly affects driver behavior
- Connection limits and rate limiting prevent connection flooding DoS

### → Native API Design
- `Database::open()` must handle file permissions, symlink prevention, and locking
- Error types must distinguish security-relevant failures (auth failure, permission denied, corrupted file) from operational errors
- Resource limit configuration (buffer pool, max readers, WAL size) is exposed via API
- A future encryption-at-rest feature will add a key parameter to `open()`

### → File Format Specification
- Magic bytes, version field, and checksum algorithm are security-relevant format decisions
- Reserving header space for future encryption metadata avoids format-breaking changes
- Page checksum offset and algorithm must be specified in the format spec

### → Performance Design
- BSON validation overhead on the insert hot path must be measured
- Page checksum computation overhead on the read hot path must be measured
- Regex query timeout adds complexity to the query execution loop
- Connection rate limiting and max-reader enforcement affect concurrency throughput
