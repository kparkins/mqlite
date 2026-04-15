# Phase 1 Reconciliation Plan: Risk Review

**Plan**: `docs/specs/phase1-reconciliation.md`
**Date**: 2026-04-14
**Reviewer**: polecat rictus
**Bead**: hq-dia9

---

## Verdict: PASS WITH NOTES

The plan correctly diagnoses the problem, selects the right architecture, and has a
sound dependency graph. The standalone modules are real — they have tests, correct
algorithms, and well-defined interfaces. The phasing is logical.

**The risk is not in the diagnosis. The risk is in the integration.**

Six integration interfaces between standalone modules are underspecified. These are not
implementation details — they are design decisions that will block code if not resolved
before work starts. Three of them are structural enough that if resolved incorrectly they
require rework.

The estimates are optimistic. The "Likely reusable" tag in Section 5 suggests that each
standalone module is ~1 bead of wiring. In practice, each layer needs its interface
adapted to the next layer's model, and the adaptation is where the hard design work lives.

---

## Risk Findings

### RISK-01 — BTreePageStore ↔ BufferPool interface is undefined

**Severity**: high  
**Phase**: 1.2 (B+ tree document storage)

The B+ tree uses a `BTreePageStore` trait (generic parameter `S`, owns the store,
calls `read_internal`, `write_internal`, `alloc_internal`, etc.).  The buffer pool
provides a `pin(page_number, PageSize) -> PinnedPage` API protected by internal
`Mutex<Partition>`.

These two interfaces are different shapes:

| BTreePageStore | BufferPool |
|---|---|
| Takes ownership of store (generic param) | Shared behind `Arc` |
| `write_internal(page, buf)` → immediate write | `pin(page)` → mutate in place → drop to unpin |
| `alloc_internal()` → page number | Allocation delegated to `PageAllocator` |
| Reads return `Box<[u8; SIZE]>` | Reads return `PinnedPage<'_>` (pinned, auto-unpinned on drop) |

Step 1.2 says "Insert: encode key, insert into B+ tree via buffer pool" but doesn't
define the bridge struct.  The bridge needs to:

1. Implement `BTreePageStore` using `Arc<BufferPool>` + `PageAllocator`
2. Manage the pin/unpin lifecycle across B+ tree calls (a single B+ tree insert can
   pin many pages in a single recursive operation)
3. Keep the header in sync with allocator state after every alloc

**If not resolved before coding 1.2**: The natural implementation attempts to put
`BufferPool` behind `BTreePageStore`, discovers the lifetime/ownership mismatch, and
rewrites the B+ tree interface or the buffer pool's pinning model.

**Suggested resolution**: Define a `BufferPoolPageStore` adapter struct before
implementing 1.2.  It holds `Arc<BufferPool>` + `Arc<Mutex<PageAllocator>>`.  The
adapter must handle `alloc_*` by locking the allocator and writing the updated header
back through the buffer pool.

---

### RISK-02 — WAL ↔ BufferPool flush integration has no defined interface

**Severity**: high  
**Phase**: 1.5 (WAL integration)

Step 1.5 says "Dirty pages written to WAL before main file (write-ahead)" and "Wire
WAL into buffer pool's flush path."  But `BufferPool::flush()` currently calls
`PageIo::write_page()` directly.  `WalManager` is not a `PageIo` implementation — it
appends frames at a write cursor, not at arbitrary page offsets.

The WAL manager API:
- `append_non_commit(page_number, data)` — appends one frame (write path)
- `commit(page_count)` — appends commit frame
- `read_page(page_number)` — reads most recent WAL frame for a page (read path)

For write-ahead to work, the flow must be:

```
dirty_pages → WAL::append_non_commit(x N) → WAL::commit → flush to main on checkpoint
```

But the buffer pool's flush iterates dirty frames and calls `PageIo::write_page`.
To route through the WAL, either:

1. The `PageIo` passed to `BufferPool::new` is a `WalPageIo` that wraps `WalManager`, or
2. `BufferPool::flush()` needs a new path that accepts a WAL handle

Option (1) has a problem: `WalManager` is not `Sync` (has `File` and mutable write
cursor state), but `PageIo: Send + Sync`.  Wrapping in `Mutex` makes it `Sync` but
adds lock contention on every page flush.

Option (2) requires an API change to `BufferPool`.

**If not resolved before coding 1.5**: Buffer pool and WAL are wired together in an
ad-hoc way that makes SWMR (1.6) difficult to retrofit.

**Suggested resolution**: Define the write path contract explicitly before 1.5 starts.
The likely correct answer is a `TransactionContext` that holds:
- A `Vec<(page_number, data)>` of dirty pages for the current transaction
- On commit: calls `WalManager::append_non_commit` for each, then `commit()`
- The buffer pool is NOT responsible for WAL writes — the engine layer drives it

This matches how SQLite's WAL integration works (pager writes to WAL, not to main file,
and WAL is checkpointed separately).

---

### RISK-03 — PageAllocator lifetime model is incompatible with concurrent use

**Severity**: high  
**Phase**: 1.1 (buffer pool + allocator + file I/O)

`PageAllocator<'a>` holds `header: &'a mut FileHeader`.  This is a mutable borrow
design: one allocator at a time, and it must be dropped before the header is used
elsewhere.

The plan says "alloc_page(size_class) -> page_no — free list or extend file."  But in
the integration:
- The buffer pool lives behind `Arc` and is shared across threads
- The header lives in... where?  `DatabaseInner`?  The buffer pool?

If the header lives in `DatabaseInner` under a `Mutex`, then every alloc call requires
locking `DatabaseInner`, getting a `&mut FileHeader`, creating a `PageAllocator`, calling
`allocate_*`, writing the dirty header back to page 0 through the buffer pool, and
dropping everything.  This works for single-writer but the borrow lifetime prevents
any lookahead or batched allocation.

**Specific problem**: `alloc_leaf` calls `io.write_page` to zero out the free-list link
page.  The `io` used is a `&dyn PageIo`.  If this `PageIo` is the buffer pool itself
(pinning the page to zero it), and the header is already borrowed mutably via the
allocator... the borrow checker will reject both being live at once unless the buffer pool
is accessed through an `Arc` (which it is, but `pin()` returns a `PinnedPage<'_>` that
borrows the pool).  These lifetimes need careful structuring.

**If not resolved before coding 1.1**: The allocator API must be redesigned mid-stream
to use owned (not borrowed) header access, which changes its interface and any code
that built on the borrow model.

**Suggested resolution**: Design `AllocatorHandle` as a cloneable struct that holds
`Arc<Mutex<AllocatorState>>` where `AllocatorState` owns the `FileHeader`.  Free the
allocator from the borrow model before the buffer pool integration begins.

---

### RISK-04 — StorageEngine trait has no transaction boundary

**Severity**: medium  
**Phase**: 0.2 (StorageEngine trait)

The proposed trait:

```rust
fn insert(&self, ns: &str, doc: Document) -> Result<Bson>;
fn update(&self, ns: &str, filter: &Document, update: &Document, opts: &UpdateOptions) -> Result<UpdateResult>;
```

Each call is a complete operation.  But inside `PagedEngine`, an `insert` must:
1. Insert the document into the primary B+ tree
2. Check unique constraints on all secondary indexes
3. Insert into each secondary index B+ tree
4. Append all dirty pages to the WAL
5. Commit the WAL frame

Steps 1–5 must be atomic.  If a crash occurs after step 2 but before step 5, the
secondary index is inconsistent with the primary.

The `StorageEngine` trait as designed forces the entire transaction into one call with
no way to add a `begin`/`commit` pair later without breaking the API.  This is fine if
the implementation guarantees internal atomicity (which it must), but the trait doesn't
express this, and the WAL commit point isn't clear from the trait alone.

Additionally: `find(&self, ...)` returning `Vec<Document>` precludes cursor-based
lazy iteration.  For large collections this means loading everything into memory.
The MongoDB API uses cursors; the existing `Cursor<T>` type would be abandoned.

**If not resolved before coding**: The trait gets implemented, then SWMR requires
snapshot-isolation reads (snapshots are WAL-position-bound, not operation-bound),
and the `find` return type makes snapshot cursors impossible to express.

**Suggested resolution**: Add transaction lifecycle to the trait:

```rust
fn begin_write(&self) -> Result<WriteTransaction>;
fn begin_read(&self) -> Result<ReadTransaction>;
```

Or at minimum, mark the current find returning `Vec<Document>` as a Phase 1
compromise with a note that cursor-returning find is Phase 2.

---

### RISK-05 — SWMR snapshot isolation implementation is underspecified

**Severity**: medium  
**Phase**: 1.6 (SWMR concurrency)

The plan says "Readers: see snapshot at WAL position when cursor was opened."

The existing `ShmIndex` tracks reader min/max WAL positions using a lock table.  But:

1. **How does a reader acquire its WAL snapshot position?**  It must read the current
   WAL write cursor at cursor-open time and register that position in SHM.  But the
   WAL write cursor changes as the writer appends.  If the reader opens a cursor
   between two WAL appends (during a multi-frame transaction), it might see a partial
   write.  Only committed WAL positions are valid snapshot points.

2. **The SHM crash recovery concern**: The plan says SHM is in-memory (or recreated on
   crash).  The existing `ShmIndex` is an in-memory structure.  On crash and reopen,
   all reader positions are lost.  This is correct (all readers are dead after a
   crash), but the WAL replay must know the "high water mark" of committed frames
   independent of SHM.  The WAL commit frame encodes `page_count` — this is the
   replay boundary.  This is correct in the current implementation.

3. **Writer + reader lock ordering**: If a writer acquires the write lock, then a
   reader tries to pin a page that is in the WAL but not the main file, the reader
   must read from the WAL.  If the writer is mid-transaction (frames appended but not
   yet committed), the reader must see the last committed WAL frame, not the
   in-progress write.  The buffer pool's `pin()` calls `read_page()` on the `PageIo`
   — if this `PageIo` doesn't know about the WAL, readers bypass WAL and see stale
   data.

The plan resolves this by saying "Writer: exclusive lock, writes to WAL" and "Readers:
see snapshot at WAL position" but the mechanism for routing reader `pin()` calls
through the WAL is exactly the same underspecified integration as RISK-02.

**If not resolved before coding 1.6**: SWMR is retro-fitted onto a write path that
doesn't support it, requiring buffer pool API changes.

---

### RISK-06 — B+ tree root page update atomicity is not guaranteed

**Severity**: medium  
**Phase**: 1.2, 1.5

`BTree::insert()` may cause a root split.  After a split, `btree.root_page` changes.
This new root page number must be persisted to the catalog.  The catalog is itself a
B+ tree (whose root is stored in the file header at offset 32).

The update sequence for "insert doc + tree split" must be:

```
1. Write new pages to WAL (non-commit frames)
2. Write updated catalog entry (new data_root_page) to WAL
3. Write updated file header (if catalog root also changed) to WAL
4. Commit frame
```

If step 4 is omitted before a crash, step 1-3 are all uncommitted and get rolled back.
That's correct.  But if step 4 is committed without step 2 (new root not in catalog),
the collection is permanently inaccessible.

The plan says "callers must persist the new root page number" but doesn't define the
caller's protocol or the write ordering relative to the WAL commit.

**Suggested resolution**: Add an explicit note in 1.2 and 1.5 that after every B+ tree
write operation, the caller (the engine) must check `btree.root_page` and update the
catalog entry if it changed, all within the same WAL transaction before commit.

---

### RISK-07 — Secondary index maintenance atomicity across crash

**Severity**: medium  
**Phase**: 1.4 (secondary indexes)

`update_index_on_update()` calls:
1. `update_index_on_delete()` — removes old index entry
2. `update_index_on_insert()` — adds new index entry

If a crash occurs between 1 and 2, the index is inconsistent (old entry removed, new
entry not added).

This is correctly handled if both operations are in the same WAL transaction (all WAL
frames appended before the commit frame).  But the plan doesn't explicitly state that
all primary + secondary index writes for a single document operation must be in a
single WAL transaction.

**Suggested resolution**: Add a requirement to 1.4 that all index maintenance
operations for a single `insert`/`update`/`delete` must be committed atomically: all
B+ tree writes (primary + all secondaries) appended as non-commit WAL frames, then a
single commit frame.

---

### RISK-08 — Performance targets have no pre-implementation validation

**Severity**: medium  
**Phase**: 3.4 (benchmarks)

G7 targets:

| Operation | Target |
|-----------|--------|
| Point lookup by _id (cached) | < 10 µs |
| Point lookup by _id (uncached) | < 1 ms |
| Indexed range scan (100 docs) | < 5 ms |
| Single doc insert (FullSync) | < 2 ms |
| Bulk insert 10k (Interval) | < 500 ms |

These are reasonable targets for a B+ tree with buffer pool, but they have not been
validated against the specific page sizes and tree depth expected in practice.

**Concerns**:

- A 32 KB leaf page holds ~1000 keys for typical document _id sizes.  At 10M docs,
  the tree is ~3-4 levels deep.  Cached: 3-4 pin() calls.  If each pin() acquires
  a `Mutex<Partition>` lock, the overhead per lookup is ~4 mutex acquires.  On modern
  hardware this is achievable in < 10 µs but only if the mutex contention is low.

- FullSync insert at < 2 ms requires `fsync()` to complete in < 2 ms.  On SSD this
  is typically 0.5–1.5 ms.  On NVMe < 0.5 ms.  But on network-attached storage or
  consumer HDD, fsync takes 4–10 ms.  The target needs clarification: is it for SSD
  only?  The `REFERENCE_HARDWARE.md` should be consulted.

- Bulk insert 10k in < 500 ms at Interval durability = < 50 µs per insert average.
  With the buffer pool absorbing writes and periodic fsync, this is likely achievable
  but depends heavily on the WAL frame write implementation efficiency.

**Risk**: If the B+ tree + buffer pool implementation doesn't hit G7 targets in Phase
3.4, there's no remediation path defined.  Should the page size be reduced?  Should
the buffer pool partitioning be tuned?  Should keys be cached separately?

**Suggested addition**: Add a performance feasibility note to Phase 3.4 citing the
hardware assumptions (from `.benchmarks/REFERENCE_HARDWARE.md`) and a contingency
(e.g., "if < 10 µs cached lookup is not achievable, investigate inline key cache or
lock-free pin path").

---

### RISK-09 — Wire server refactor scope is underspecified

**Severity**: medium  
**Phase**: 2.1 (multi-database wire protocol)

"Wire server takes `&Client` not `&Database`" is one bullet in the plan.  The wire
server (`wire/server.rs`) is a 700+ line async tokio module with:

- Per-connection cursor state (ConnectionCursors, idle eviction)
- 18 command handlers accessing `DatabaseInner` directly
- OP_QUERY / OP_MSG dual-opcode handshake
- `$db` field parsing (exists but currently hardcoded to one database)

The refactor from `&Database` to `&Client` requires:
1. Defining the `Client` type (Phase 0.1)
2. Updating every command handler to call `client.database(db_name)` for the `$db`
   field in the incoming command
3. Removing `db_name` derivation from filename stem
4. Updating `listDatabases` to enumerate all namespaces

This is medium scope, not one bullet.  But it cannot start until Phase 0.1 (Client
type) is done, and Phase 0.1 depends on Phase 0.2 (StorageEngine trait).  Since the
plan says Phase 2 can proceed in parallel with Phase 1, this is accurate — 2.1 can
start as soon as 0.1/0.2 are done, even if Phase 1 storage integration is incomplete.

**Risk is low if 0.1/0.2 are clean.**  The main concern is that 2.1 currently treats
"wire server uses `Database` object" as one line, when it's actually a substantial
command-handler audit.  The bead estimate for 2.1 ("Medium, 1 bead") may be light.

---

### RISK-10 — Module reusability is optimistic for catalog and secondary_index

**Severity**: low  
**Phase**: 1.3, 1.4

**Catalog**: Currently uses `collection_key(name: &str)` with a single-level namespace
(`0x01 || name`).  Multi-db support requires namespaced keys (`0x01 || "db.coll"`).
The `CollectionEntry.name` field stores bare collection names.  For multi-db, it needs
to store the full namespace or have a separate `db` field.  This is a schema change
to the catalog's on-disk format — not a wiring problem, but also not "likely reusable
without changes."

**Secondary indexes**: `build_index_keys()` takes a `Document` and outputs keys.
The function is correct and reusable.  But `update_index_on_insert()` and friends take
a `BTreePageStore` directly, which means the caller must provide the right store for
the right index.  Managing N index stores per collection, each being a separate B+ tree
backed by the buffer pool, is a bookkeeping problem not addressed in the plan.

**Specifically**: who holds the secondary index B+ tree instances?  They're opened
by root page (from the catalog).  But the `BTree<S>` struct owns `S`.  If `S` is a
`BufferPoolPageStore`, each secondary index is a `BTree<BufferPoolPageStore>`.  These
must be stored somewhere — per-namespace in a `HashMap` in the engine?  Opened lazily?
Kept in the catalog entry?  The plan says "each secondary index builds a B+ tree" but
doesn't define the lifecycle of these tree handles.

---

### RISK-11 — Effort calibration

**Severity**: low  
**Phase**: all

Section 6 estimates:
- Phase 0: 2 beads (Medium) — API surface + trait
- Phase 1: 6 beads (Large) — storage stack
- Phase 2: 1 bead (Medium) — wire fix
- Phase 3: 5 beads (Medium) — testing
- Phase 4: 2 beads (Small) — cleanup

Risks that add to Phase 1 scope:
- RISK-01 (BTreePageStore bridge) adds ~1 bead of design + implementation
- RISK-02 (WAL/buffer pool integration) adds ~1 bead
- RISK-03 (allocator lifecycle) may require an API change to `PageAllocator` (0.5 beads)
- RISK-05 (SWMR) is currently underweighted — "replace Mutex with WAL-based snapshot
  isolation" is likely 1-2 beads on its own

Revised estimate for Phase 1: 8–10 beads (not 6).

This is not a blocker but affects sprint planning.

---

### RISK-12 — No in-memory mode migration path specified

**Severity**: low  
**Phase**: 4.1 (remove dead code)

`Database::open_in_memory()` is currently a public API function with tests in
`compat_tests.rs`:

```rust
fn in_memory_creates_no_files()
```

The plan says "No in-memory mode. One code path. Tests use tempfiles."  But removing
a public API function is a breaking change.  The `compat_tests.rs` references it,
and if external users depend on it, removing it breaks semver.

Since the version is `0.1.0` (pre-release), this is acceptable.  But the removal
should be explicitly listed in 4.1 along with updating `compat_tests.rs`.

---

## Assessment of Module Reusability

The plan's Section 5 assessment is mostly correct with these additions:

| Module | Plan says | Reviewer says |
|--------|-----------|---------------|
| storage/btree.rs | Likely | **Likely with adapter** — interface needs `BufferPoolPageStore` bridge (RISK-01) |
| storage/buffer_pool.rs | Likely | **Likely with API change** — flush path needs WAL routing (RISK-02) |
| storage/allocator.rs | Likely | **Likely with lifecycle change** — borrow model needs rethinking (RISK-03) |
| wal/ | Likely | **Likely** — WAL frame format and recovery are correct; integration path unclear (RISK-02, RISK-05) |
| storage/catalog.rs | Likely | **Likely with schema change** — namespace key format must change for multi-db (RISK-10) |
| storage/secondary_index.rs | Likely | **Likely with lifecycle design** — tree instance lifecycle undefined (RISK-10) |
| key_encoding.rs | Yes | **Yes** — standalone, correct |
| query/filter.rs | Yes | **Yes** |
| query/planner.rs | Partial | **Partial** — IndexScan execution needs complete rewrite (plan correctly notes this) |
| update_operators.rs | Yes | **Yes** |
| wire/protocol.rs | Yes | **Yes** |
| wire/server.rs | Partial | **Partial** — scope larger than one bullet implies (RISK-09) |

---

## What the Plan Gets Right

1. **Correct problem diagnosis**: The BSON-blob persistence and `Vec<Document>` engine are
   correctly identified as the root cause.  The audit table in Section 1 is accurate.

2. **Correct architecture**: `Client → Database → Collection` with `StorageEngine` trait
   and `PagedEngine` implementation is the right model.  Matching MongoDB's driver
   hierarchy is necessary for the test-double story (PRD G2).

3. **Correct dependency graph**: The ordering in Section 4 is correct.  0.1 → 0.2 → 1.1
   → 1.2 → 1.3 → 1.4 is the right build order.  WAL (1.5) can proceed in parallel with
   catalog/secondary index (1.3/1.4) once buffer pool (1.1) is done.

4. **Standalone modules are real**: The crash recovery tests (500 cycles, 10 scenarios)
   are a genuine confidence signal for the WAL.  The B+ tree probtests validate insert/
   delete/range scan correctness.  This is not vaporware — the building blocks exist.

5. **Clean-slate approach**: Rewriting the integration glue (engine.rs, database.rs,
   collection.rs) while keeping the validated standalone modules is the right call.
   Trying to patch the BSON-blob engine incrementally would be harder than the rewrite.

6. **No in-memory mode**: The plan's "one code path, tempfiles for tests" is the right
   call.  The current `open_in_memory()` introduces special-case branches (NoopFileLock,
   no WAL, no B+ tree) that make it impossible to test the real persistence path.

---

## Recommendations Before Implementation Begins

**Required before Phase 1 coding starts:**

1. **Write an interface design document for the BTreePageStore ↔ BufferPool bridge**
   before implementing 1.2.  Define `BufferPoolPageStore` and its lifecycle.
   (Addresses RISK-01, RISK-03)

2. **Define the write transaction protocol** before implementing 1.5.  Specify how
   dirty pages are routed through the WAL: who calls `append_non_commit`, when
   `commit` is called, and how the buffer pool flush is coordinated.
   (Addresses RISK-02, RISK-04, RISK-07)

3. **Add a note to Phase 0.2** that the `StorageEngine` trait's `find` returning
   `Vec<Document>` is a Phase 1 compromise, and that SWMR cursors will need a
   different return type in Phase 2.
   (Addresses RISK-04, RISK-05)

**Should-fix before first bead is dispatched:**

4. **Revise Phase 1 estimate** from 6 beads to 8–10 beads with the integration adapter
   work made explicit.  (Addresses RISK-11)

5. **Expand Phase 2.1** description to list the command handlers that need `$db` routing.
   A brief audit of `wire/server.rs` command dispatch would surface the scope.
   (Addresses RISK-09)
