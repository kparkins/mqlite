# Testability Review: Phase 1 Reconciliation Plan

**Reviewer:** furiosa  
**Date:** 2026-04-14  
**Spec:** `docs/specs/phase1-reconciliation.md`  
**Verdict:** PASS WITH NOTES

---

## Summary

The plan is architecturally sound. The five Phase 3 test beads (3.1–3.5) map
onto the major success criteria from Section 7. However, there are meaningful
gaps that will make it hard to verify each phase before proceeding to the next,
and two of the five test beads have CI-readiness problems that need resolution
before implementation begins.

---

## Phase-by-Phase Acceptance Criteria Assessment

### Phase 0: Architecture & API Surface

**0.1 — Client/Database/Collection object model**  
Spec describes the target API shape with code examples. Success Criteria §7 #1
and #2 cover the end-state, but there is no intermediate gate: "0.1 is done
when test X passes." There is no planned test that exercises the public API
before the storage layer exists.

**Gap:** No acceptance test for the API surface in isolation. A simple
integration test (compile against the new types, call the public API with a
tempfile backend) would confirm the shape is correct before Phase 1 begins.
Without one, a misshapen API won't be discovered until Phase 3.

**0.2 — StorageEngine trait**  
The trait interface is fully specified. However, there is no planned
mock/stub implementation to exercise the contract before `PagedEngine` is
built. Phase 0 ends with a trait definition; Phase 1 starts implementing it.
If the trait turns out to be wrong (missing method, wrong return type), it's
discovered late.

**Gap:** A `VecEngine` stub (the current in-memory implementation rewrapped
behind the trait) would allow 0.1 and 0.2 to be tested end-to-end before a
single line of Phase 1 storage code is written.

---

### Phase 1: Storage Stack

**1.1 — Buffer pool + page allocator + file I/O**  
Behaviors described: `fetch_page`, `alloc_page`, `mark_dirty`. No dedicated
acceptance test. The existing `buffer_pool.rs` tests are standalone (in-memory
allocator); they do not exercise the wired-to-file path.

**Gap:** Missing test: alloc a page, write data, close the file, reopen, read
back the page. This is the earliest point where "data survives close/reopen"
can be verified.

**1.2 — B+ tree as document storage**  
Behaviors described: insert, point lookup, COLLSCAN, overflow pages. No
dedicated acceptance test at this layer.

**Gap:** Missing test: insert N docs via the B+ tree + buffer pool, close,
reopen, find by `_id`. This is testable before catalog exists.

**1.3 — Catalog**  
Behaviors described: create/drop/list namespaces, survives open. No acceptance
test.

**Gap:** Missing test: create two namespaces, close, reopen, verify both
namespaces and their data are present.

**1.4 — Secondary indexes**  
Behaviors described: build index on existing data, maintain on write, range
scan. No acceptance test.

**Gap:** Missing test: insert docs, create index, query via index, verify O(n)
vs O(log n) plan selection, verify index maintained on update/delete.

**1.5 — WAL integration**  
Behaviors described: write-ahead property, DurabilityMode variants, checkpoint,
open-with-replay. No acceptance test at this layer that is distinct from 3.2.

**Gap:** Missing test: write in FullSync mode, verify WAL file exists before
checkpoint, checkpoint, verify WAL file is removed.

**1.6 — SWMR concurrency**  
Behaviors described: snapshot isolation, readers never block writers, SHM
reader tracking. Corresponding test is 3.3.

**Assessment:** PASS — 1.6 and 3.3 are well-paired, provided 3.3 is
sufficiently specified (see below).

---

### Phase 2: Wire Protocol Fix

**2.1 — Multi-database wire protocol**  
Behaviors described: `$db` routing, `listDatabases`, `use mydb`, error path
removal. Corresponding test is 3.5.

**Gap:** There is no intermediate smoke test between 2.1 implementation and
the full pymongo suite in 3.5. At minimum, a unit test that exercises
multi-database namespace routing in isolation would make failures easier to
diagnose.

---

## Phase 3 Test Bead Assessment

### 3.1 — End-to-end persistence test

**Specification:** `Client::open(path)` → insert 10k docs across 3 databases
→ create indexes → close → reopen → verify all docs, indexes, query results.

**Dependency in spec:** Shown as depending on 1.2 (B+ tree storage) only.

**Issue:** The test exercises 3 databases, which requires the catalog (1.3).
The dependency graph shows 3.1 → 1.2, but 3.1 actually needs 1.3 too. This
inconsistency in the dependency graph could cause premature test execution.

**CI-runnable:** Yes. Tempfile-backed, no external dependencies.

**Verdict:** PASS WITH NOTES — dependency graph needs correction (add 1.3).

---

### 3.2 — Crash recovery through public API

**Specification:** insert in FullSync mode → fork → kill -9 child → reopen
in parent → verify committed data present.

**CI-runnable assessment:**

On Linux (GitHub Actions ubuntu runners): `fork()` and `SIGKILL` work.
The test pattern is standard for WAL testing.

On macOS: `fork()` is restricted under the macOS sandbox; some CI
configurations disallow it. If macOS runners are used, this test may need
a `#[cfg(target_os = "linux")]` annotation or a docker-based CI runner.

**Specification gaps:**

1. **What counts as "committed"?** In `FullSync` mode, a commit is durable
   after fsync. The test must ensure the parent kills the child *after* at
   least one fsync completes. Without synchronization (e.g., a write to a
   pipe from child to parent saying "first batch fsynced"), the test may kill
   the child before any fsync, verifying nothing.

2. **What is the child doing?** The spec says "insert in FullSync mode" but
   doesn't specify: does the child insert one batch and then loop? does it
   insert indefinitely? The test needs a defined "committed" boundary that the
   parent can wait for.

3. **What about uncommitted data?** The spec says "verify committed data
   present" but doesn't say "verify uncommitted data is absent." A crash
   recovery test should also verify that partial writes do not corrupt the
   database.

**Verdict:** PASS WITH NOTES — needs synchronization primitive between
parent and child to establish "committed" boundary; macOS CI caveat applies.

---

### 3.3 — SWMR concurrency test

**Specification:** 10 reader threads + 1 writer thread. Writer inserts 10k
docs. Readers do concurrent finds. Verify: no errors, no data races, correct
snapshot isolation, WriterBusy on timeout.

**CI-runnable:** Yes. Thread-based, no external dependencies.

**Specification gaps:**

1. **"Correct snapshot isolation" is underspecified.** What exactly should
   readers observe? The test should assert: a reader that opens a cursor at
   time T sees only docs committed before T, not docs committed after T. This
   requires structured assertions, not just "no errors."

2. **"WriterBusy on timeout" needs a scenario.** How does the test induce
   writer contention with a timeout? One concrete approach: hold a write lock
   in one thread, attempt a write from another with a short busy_timeout,
   verify `WriterBusy` is returned. This scenario should be explicitly
   described.

3. **"No data races" is unverifiable without Miri or TSan.** Running the
   test under `cargo test` doesn't guarantee absence of data races. The test
   should be annotated to run under `RUSTFLAGS="-Z sanitizer=thread"` (nightly)
   or at least under `ThreadSanitizer` in CI.

**Verdict:** PASS WITH NOTES — needs more precise snapshot isolation
assertions and an explicit WriterBusy scenario.

---

### 3.4 — Benchmarks on real storage

**Targets:**

| Operation | Target |
|-----------|--------|
| Point lookup by _id (cached) | < 10 µs |
| Point lookup by _id (uncached) | < 1 ms |
| Indexed range scan (100 docs) | < 5 ms |
| Single doc insert (FullSync) | < 2 ms |
| Bulk insert 10k (Interval) | < 500 ms |

**Realism assessment:**

- **< 10 µs cached lookup:** In-memory B+ tree traversal for a small dataset.
  Achievable on modern hardware.
- **< 1 ms uncached lookup:** Requires one random read from disk. NVMe SSDs
  typical read latency: 50–200 µs. HDDs: 5–10 ms. This target is realistic
  for NVMe/SSD but will fail on HDD and on slow CI runners.
- **< 5 ms range scan (100 docs):** Depends heavily on doc size and whether
  pages are cached. With a warm cache, achievable. Cold: requires multiple
  sequential reads.
- **< 2 ms FullSync insert:** Requires fsync to complete in < 2 ms.
  NVMe SSD: achievable. HDD: not achievable (~10 ms fsync). GitHub Actions
  runners use SSD-backed storage, so this is likely achievable in CI but
  still variable.
- **< 500 ms bulk 10k (Interval):** Achievable on any modern hardware.

**Critical gap — are these pass/fail CI gates or informational?**

The spec does not say. If they are pass/fail gates in CI, three problems arise:

1. GitHub Actions shared runners have variable CPU/IO performance. Benchmarks
   that take 1.5 ms locally can take 8 ms on a loaded CI runner.
2. The uncached lookup and FullSync insert targets are hardware-dependent.
3. Flaky benchmark gates are worse than no benchmark gates — they erode trust
   in CI.

**Recommendation:** Benchmarks should be informational measurements committed
to `benches/core.rs`, not CI gates. The targets are useful as design goals and
for regression detection on dedicated hardware, but should not block PRs.

**Verdict:** PASS WITH NOTES — targets are plausible on modern SSD hardware,
but the spec must clarify whether these are CI gates or design goals. If CI
gates, expect flakiness.

---

### 3.5 — Wire protocol integration test

**Specification:** "CI-runnable: start wire server, run pymongo test suite,
validate. Currently requires manual execution."

**This is the most critical gap in the test plan.**

The spec simultaneously asserts "CI-runnable" as the goal and "currently
requires manual execution" as the status. This is not a plan for making it
CI-runnable — it is a statement that the work is not done.

**What's missing:**

1. **The pymongo test suite is not defined.** Does it already exist
   (`tests/wire_integration/` or similar)? Does it need to be written? If it
   needs to be written, that's a bead that isn't in the plan.

2. **CI environment setup is not described.** pymongo must be installed on CI
   runners. This requires either a `requirements.txt` + pip install step in
   the CI config, or a Docker image with pymongo. Neither is mentioned.

3. **Port conflict management.** Starting a wire server on port 27017 in CI
   requires either a fixed port + conflict avoidance, or a random-port test
   harness. Not mentioned.

4. **Shutdown/teardown.** The test must reliably stop the wire server after
   the test suite runs, even on failure.

Without a concrete plan for each of these, 3.5 cannot be considered a
CI-runnable test bead — it is manual validation with a label change.

**Verdict:** FAIL — "CI-runnable" goal is stated but the plan to achieve it is
absent. This bead needs a sub-plan (pymongo suite location, CI setup steps,
port management, teardown) before it can be executed.

---

## Missing from the Test Plan

### 1. Phase gate tests for each Phase 1 sub-step

There are no tests that confirm Phase 1.1, 1.2, 1.3, and 1.4 each work in
isolation before the next sub-step begins. If a bug is introduced in 1.2,
it won't be discovered until 3.1 runs (after all of Phase 1 is complete).
Early detection is much cheaper.

**Recommendation:** Add a test per Phase 1 sub-step (see gaps above). These
can be simple integration tests in `tests/storage/`.

### 2. Mock StorageEngine for Phase 0 validation

Without a `VecEngine` stub (the existing Vec-backed engine behind the new
trait), there is no way to test that the public API (Phase 0) is correct
before Phase 1 storage code exists.

**Recommendation:** Wrap the existing `engine.rs` Vec implementation in the
`StorageEngine` trait interface as Phase 0's first deliverable. This unblocks
validation of the entire public API (0.1) before Phase 1 begins.

### 3. Existing test suite migration plan

The existing `proptest` suites for `btree.rs` and `wal/` are standalone. The
plan notes these modules will likely need "interface changes for buffer pool
integration." If the existing test suite breaks during integration, it should
be fixed, not deleted.

**Gap:** No mention of running `cargo test` as a gate during Phase 1 sub-steps,
or of which existing tests should still pass at each milestone.

**Recommendation:** Add a CI gate: existing standalone unit tests must continue
to pass after each Phase 1 commit. Tests that require interface changes should
be updated, not removed.

### 4. API compatibility test for MongoDB driver shape

G2 (MongoDB API compatibility) is the core product goal. The plan tests wire
compatibility (3.5) and query/update operator correctness (implicitly, via
existing tests). But there is no test that explicitly verifies the Rust API
shape matches the official MongoDB Rust driver.

**Recommendation:** Add a compilation test (possibly in `tests/api_compat.rs`)
that mirrors the official MongoDB driver's usage patterns from the spec's code
examples. This fails at compile time if the API shape diverges.

### 5. Multi-database isolation test

Phase 2 introduces multi-database support. The success criteria (§7 #2, #3)
require that `client["db1"]` and `client["db2"]` are isolated. The only test
covering this is 3.5 (wire integration), which is not CI-ready.

**Recommendation:** Add a Rust-level integration test: insert docs in `db1`,
query from `db2`, verify isolation. This is independent of pymongo and
CI-runnable immediately.

### 6. DurabilityMode::None and DurabilityMode::Interval behavior tests

3.2 tests FullSync. But DurabilityMode has three variants. None and Interval
are untested in the plan.

**Recommendation:** Add tests:
- `DurabilityMode::None`: verify writes complete without blocking on fsync
  (performance, not correctness)
- `DurabilityMode::Interval`: verify writes are visible after the interval
  period without explicit close

---

## Dependency Graph Issue

The dependency graph in §4 shows:

```
1.2 (B+ tree document storage)
 |
 +-> 3.1 (persistence test)
```

But 3.1's specification says "insert 10k docs across **3 databases**," which
requires the catalog (1.3). The correct dependency is:

```
1.3 (catalog)
 |
 +-> 3.1 (persistence test)
```

This should be corrected before beads are created for Phase 3, or 3.1 will be
marked ready before 1.3 exists.

---

## Verdict: PASS WITH NOTES

The plan's architecture is correct and the test strategy covers the major
success criteria. The five Phase 3 test beads represent the right tests.
However, implementation should not begin until the following are resolved:

**Blockers (must fix before starting):**

1. **3.5 wire integration test** needs a concrete CI plan (pymongo suite
   location, pip install step, port management, teardown) or be marked
   explicitly as "manual only" with a separate bead to make it CI-runnable.

2. **3.1 dependency graph** needs correction: 3.1 depends on 1.3, not just 1.2.

**Strong recommendations (should fix):**

3. Add phase gate tests for 1.1, 1.2, 1.3 in isolation (close/reopen tests
   at each layer).

4. Wrap existing Vec engine in `StorageEngine` trait as Phase 0 deliverable
   to enable early API validation.

5. Specify 3.2 synchronization: how does the parent know the child has fsynced
   at least one commit before being killed?

6. Specify 3.3 more precisely: what does "correct snapshot isolation" assert,
   and what scenario triggers WriterBusy?

7. Clarify 3.4 benchmark targets: CI gates vs. design goals.

**Nice to have:**

8. Add API compatibility compilation test (`tests/api_compat.rs`).

9. Add multi-database isolation Rust test independent of pymongo.

10. Add DurabilityMode::None and Interval test coverage.
