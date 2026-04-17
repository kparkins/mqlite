# True MVCC Design

## Problem: SQLite-style WAL used as a concurrency mechanism

mqlite currently uses a SQLite-style WAL (`src/wal/`) for two unrelated jobs:

1. **Crash recovery** — append page frames, replay on open, checkpoint to main file. This is correct and should be preserved, renamed to *journal* (see below).
2. **Snapshot isolation** — readers hold `RwLock<BpBackend>` (shared) for the duration of every scan. Writers hold it exclusively. This means a long `find()` blocks all writes until it finishes.

The second job is the problem. The WAL was never designed to provide snapshot isolation — that design belongs to a proper MVCC layer. As a result:

- Readers block writers (and vice versa) for the full duration of every scan.
- There is exactly one version of every document: the latest committed version.
- Multi-statement transactions with snapshot isolation are impossible.
- Long reads under heavy write workloads cause unbounded write latency.

MongoDB's WiredTiger storage engine solves this with per-document in-memory version chains. That is the target.

---

## Rename: WAL → Journal

The WAL already does exactly one thing correctly: crash recovery. WiredTiger calls this the **journal**. Before implementing MVCC, rename the module to reflect its true role:

| Old | New |
|-----|-----|
| `src/wal/` | `src/journal/` |
| `src/wal/mod.rs` | `src/journal/mod.rs` |
| `src/wal/wal_file.rs` | `src/journal/log_file.rs` |
| `src/wal/shm.rs` | `src/journal/shm.rs` |
| `WalManager` | `JournalManager` |
| `WalLayeredSource` | `JournalLayeredSource` |
| `-wal` file suffix | `-journal` file suffix |
| `-shm` file suffix | `-shm` file suffix (keep) |

The journal's behavior is unchanged: append page frames on write, replay on open, checkpoint periodically, truncate after checkpoint. MVCC does not interact with the journal at all — the journal handles crash recovery; MVCC handles concurrent read visibility.

---

## Solution: WiredTiger-style In-Memory Version Chains

### Core concept

Every write to a document creates a new **version** in memory, attached to the buffer pool page frame that contains the document's B-tree key. The old version is not discarded — it remains in the chain with a `stop_ts` set. Readers find the version matching their `read_ts` by walking the chain. Old versions are discarded lazily during page eviction (reconciliation).

### Components required

#### 1. Timestamp Oracle (`src/mvcc/timestamp.rs`)

A global monotonically increasing `AtomicU64`. Every committed write transaction increments it and records its `commit_ts`. Every read transaction records `read_ts = current_ts` at the moment it starts.

```
commit_ts: atomically assigned at commit, never reused
read_ts:   snapshot of commit_ts at the moment the read view opens
```

#### 2. Version Entry (`src/mvcc/version.rs`)

```
VersionEntry {
    start_ts:  u64,        // commit_ts of the write that created this version
    stop_ts:   u64,        // commit_ts of the write that superseded it (u64::MAX = current)
    txn_id:    u64,        // id of the writing transaction (for read-your-own-writes)
    data:      Vec<u8>,    // serialized BSON document (or tombstone marker for deletes)
    is_tombstone: bool,    // true if this version represents a deletion
}
```

A version is **visible** to a reader with `read_ts = T` when:
```
start_ts <= T  AND  T < stop_ts
```

#### 3. Version Chain (`src/mvcc/version.rs`)

A per-key linked list of `VersionEntry` values, ordered newest-first. The head is always the current (latest committed) version. The tail is the oldest version still retained.

In WiredTiger this is called the *update list* (`WT_UPDATE`). In mqlite it will be a `VecDeque<VersionEntry>` or linked list stored alongside the buffer pool page frame — not inside the B-tree cell itself.

#### 4. Page Version Map (`src/storage/buffer_pool.rs`)

Each cached page frame in `BufferPool` gains a companion structure:

```
PageFrame {
    data:          [u8; PAGE_SIZE],      // on-disk page content (as today)
    dirty:         bool,
    version_chains: HashMap<Vec<u8>, VecDeque<VersionEntry>>,  // key → chain
}
```

`version_chains` maps a B-tree cell key (the encoded `_id`) to its version chain. The on-disk `data` always reflects the **oldest retained committed version** (the reconciled on-disk image). In-memory chains hold versions newer than what is on disk.

#### 5. ReadView (`src/mvcc/read_view.rs`)

A `ReadView` is opened at the start of any read operation. It records:

```
ReadView {
    read_ts:        u64,   // snapshot timestamp — see versions where start_ts <= read_ts
    txn_id:         u64,   // own transaction id (for read-your-own-writes)
}
```

`ReadView` is lightweight — just two integers. It is passed into every B-tree read operation in place of the current implicit "read latest" behavior. No lock is held after the `ReadView` is created.

#### 6. Transaction Handle (`src/mvcc/transaction.rs`)

A write transaction holds:

```
WriteTxn {
    txn_id:     u64,
    read_ts:    u64,       // snapshot for reads within this transaction
    commit_ts:  u64,       // assigned at commit time
    pending:    Vec<PendingWrite>,  // buffered writes not yet committed
}
```

Uncommitted writes are kept in `pending`. On commit, `commit_ts` is assigned from the oracle, all pending `VersionEntry` values have `start_ts = commit_ts`, and the old version's `stop_ts` is set to `commit_ts`. On rollback, pending entries are discarded.

#### 7. Visibility Check in B-tree reads (`src/storage/btree.rs`)

`BTree::get` and `BTree::range_scan` currently return the raw cell value for a key. Under MVCC, after locating a key in the B-tree, the engine must:

1. Check the page frame's `version_chains` for this key.
2. If a chain exists, walk it newest-first to find the first entry where `start_ts <= read_ts < stop_ts`.
3. If no chain entry is visible, use the on-disk cell value (which represents the oldest retained version).
4. If the visible version is a tombstone, treat the key as deleted.

This visibility check must be injected via a `ReadView` argument added to all read methods on `BTree<S>`.

#### 8. Reconciliation during eviction (`src/storage/buffer_pool.rs`)

When the buffer pool evicts a dirty page, it must **reconcile** the version chain before writing the page to disk:

1. Determine `oldest_required_ts = min(read_ts across all open ReadViews)`.
2. For each key in the page's `version_chains`:
   - Find the oldest version with `stop_ts > oldest_required_ts` — this becomes the new on-disk value.
   - Discard all versions with `stop_ts <= oldest_required_ts` — no active reader can see them.
   - If the remaining chain has exactly one entry and it matches the on-disk value, clear the chain.
3. Write the reconciled page to the journal (crash recovery) and to the main file on checkpoint.

This is exactly WiredTiger's reconciliation step. It is the only place old versions are discarded.

#### 9. History Store (`src/storage/history_store.rs`)

When a page's version chain grows too long (e.g., more than a configurable threshold of versions, or chain memory exceeds a limit), older entries are evicted from the in-memory chain into a dedicated **history store B-tree**.

The history store is a single shared B-tree with keys of the form:
```
(collection_ns_id: u32)(encoded_doc_id: bytes)(start_ts_big_endian: u64)
```

This ordering allows efficient range lookup: to find the version of document `D` visible at `read_ts = T`, scan backwards from `(ns, D, T)` to find the latest entry with `start_ts <= T`.

When a `ReadView` cannot find a version in the in-memory chain (because it was evicted to the history store), it probes the history store. This is an extra B-tree lookup but only occurs for long-lived reads under heavy write workloads.

The history store is also subject to GC: entries with `stop_ts <= oldest_required_ts` are deleted during reconciliation or a background GC pass.

#### 10. Active ReadView Registry (`src/mvcc/read_view.rs`)

A global `Arc<Mutex<BTreeMap<u64, u64>>>` maps `txn_id → read_ts` for all open `ReadView` instances. This is the mechanism for computing `oldest_required_ts` during reconciliation:

```
oldest_required_ts = min(read_ts for all open ReadViews)
```

Opening a `ReadView` inserts into the registry. Dropping a `ReadView` removes it. The registry is read during every eviction/reconciliation cycle.

---

## Required Changes Summary

> **Status as of 2026-04-16**: All phases below shipped across commits T0…T9
> on the MVCC branch. See `docs/adr/0001-mvcc.md` for the accepted ADR, and
> `.omc/plans/mvcc-wiredtiger.md` for the task-by-task acceptance evidence.

### Phase 1 — Foundation (shipped T0/T1/T2)

1. **WAL → Journal rename** — `src/journal/` is the sole survivor of the
   old `src/wal/` tree; the `-wal` sidecar is renamed on open for
   backwards compatibility with pre-T0 database files.

2. **`src/mvcc/` module** ships with:
   - `timestamp.rs` — HLC oracle `Ts { physical_ms, logical }`,
     floored on open to the successor of the last durable commit.
   - `version.rs` — `VersionEntry`, `OverflowRef` (RAII decref),
     `VersionData::{Inline, Overflow}`.
   - `read_view.rs` — `ReadView` with `poisoned` / `pin_ops_in_flight`
     atomics, `ReadViewRegistry` keyed by `txn_id` holding `Weak<ReadView>`
     slots, `ChainSnapshot` (S13 atomic-handoff pin protocol).
   - `transaction.rs` — `WriteTxn::begin` drains the deferred-free queue
     before allocating; staged primary + sec-index writes commit under a
     single `commit_ts`.
   - `deferred_free.rs` — writer-drained queue of overflow first-pages
     whose refcount hit zero on the reader path.

3. **`version_chains` on buffer-pool frames** — each `PageFrame` owns its
   per-user-key version chains; reconciliation on eviction walks them
   against `ReadViewRegistry::oldest_required_ts()`.

### Phase 2 — Read Path (shipped T3/T3.5/T3.75/T5')

4. **`ReadView` threaded through BTree reads** — `get` / `range_scan` walk
   the chain for visibility before falling back to the history store.
   The split-migration and merge-path guards landed at
   `src/storage/btree.rs:1281,1308` (T3.5).

5. **RwLock read path removed from `PagedEngine`** (T5'). `find`,
   `find_one`, `count`, `list_indexes`, `aggregate` open a `ReadView`,
   read lock-free, and drop the view. Writers acquire only the writer
   serialization mutex (position 6).

### Phase 3 — Write Path (shipped T5'/T6)

6. **`WriteTxn` on `BpBackend::with_txn`** — staged primary + sec-index
   writes plus refcount deltas for overflow pins are atomically stamped
   with the same `commit_ts` (no half-committed witness).

7. **Reconciliation on eviction** — `BufferPool::reconcile_frame_at`
   drops versions older than `oldest_required_ts` and pushes cold
   entries into the history store before the page flushes to the
   journal.

### Phase 4 — History Store & GC (shipped T7/T8)

8. **`src/storage/history_store.rs`** — a dedicated B-tree keyed by
   `(ns_id, kind, user_key, start_ts_BE)`. Readers probe it on chain
   miss; kind-tagging keeps primary and sec-index entries disjoint.

9. **GC pass** — runs from `checkpoint()` against
   `ReadViewRegistry::oldest_required_ts()`; deletes entries whose
   `stop_ts <= oldest_required_ts`.

### Phase 5 — Barriers, counters, ADR (shipped T8/T9)

10. **12 mandatory + 5 diagnostic MVCC counters** wired through the
    metrics surface (`mvcc.reconcile.*`, `mvcc.history_store.*`,
    `mvcc.read_view.*`, `overflow.pages_in_use`, …). Four deferred
    sampling sites are listed in ADR 0001 §6.

11. **`drop_collection` force-expire barrier** (T9) — acquires writer
    serialization (6), force-expires every live `ReadView` via the
    registry (5), waits for `pin_ops_in_flight == 0`, then invokes
    `free_subtree` under the same writer serialization. Post-barrier,
    pre-opened views surface `Error::ReadViewExpired` on next use.

12. **Feature flag removed.** `feature = "mvcc"` no longer exists —
    MVCC is unconditional. Verified by `rg 'feature = "mvcc"'` → 0
    matches in `Cargo.toml`, `src/`, and `tests/`.

13. **ADR 0001 accepted** — see `docs/adr/0001-mvcc.md` for decision
    record, lock order, alternatives, and post-v1 deferred items.

---

## Key Invariants

- `start_ts` of the head of any chain is always the `commit_ts` of the most recent committed write to that document.
- `stop_ts` of the previous head is always equal to `start_ts` of the new head.
- `oldest_required_ts` is always ≤ the `read_ts` of any open `ReadView`.
- The on-disk page image always reflects the oldest version that any current or future reader might need (i.e., `stop_ts > oldest_required_ts_at_last_reconciliation`).
- The journal (formerly WAL) records the reconciled on-disk state for crash recovery only. MVCC version chains are in-memory only and rebuilt from the journal on crash recovery (the on-disk state is always a valid single-version snapshot of the database).
- On crash recovery, the journal replays to produce the latest committed on-disk state. All in-memory version chains start empty. This is correct because crash recovery produces a consistent point-in-time snapshot.
