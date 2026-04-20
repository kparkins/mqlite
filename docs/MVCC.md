# MVCC Architecture

## WiredTiger-style In-Memory Version Chains

### Core concept

Every write to a document creates a new **version** in memory, attached to the buffer pool page frame that contains the document's B-tree key. The old version is not discarded — it remains in the chain with a `stop_ts` set. Readers find the version matching their `read_ts` by walking the chain. Old versions are discarded lazily during page eviction (reconciliation).

### Components

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

## Key Invariants

- `start_ts` of the head of any chain is always the `commit_ts` of the most recent committed write to that document.
- `stop_ts` of the previous head is always equal to `start_ts` of the new head.
- `oldest_required_ts` is always ≤ the `read_ts` of any open `ReadView`.
- The on-disk page image always reflects the oldest version that any current or future reader might need (i.e., `stop_ts > oldest_required_ts_at_last_reconciliation`).
- The journal records the reconciled on-disk state for crash recovery only. MVCC version chains are in-memory only and rebuilt from the journal on crash recovery (the on-disk state is always a valid single-version snapshot of the database).
- On crash recovery, the journal replays to produce the latest committed on-disk state. All in-memory version chains start empty. This is correct because crash recovery produces a consistent point-in-time snapshot.
