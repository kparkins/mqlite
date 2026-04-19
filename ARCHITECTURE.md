# mqlite Architecture

WiredTiger-style MVCC over a paged B-tree store with a dedicated history tier.

## 1. Block Diagram

```
                                 ┌─────────────────────────────────┐
                                 │           Client API            │
                                 │  insert/update/delete  find/    │
                                 │  count  drop_collection         │
                                 └───────────────┬─────────────────┘
                                                 │
                                                 v
┌───────────────────────────────────────────────────────────────────────────┐
│                              PagedEngine                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │          inner: Mutex<BpBackend>   (position 6 — writer mutex)      │  │
│  │  ┌─────────────────────┐  ┌──────────────────────────────────────┐  │  │
│  │  │  active_txn:        │  │   txn_counter: AtomicU64             │  │  │
│  │  │    Option<WriteTxn> │  │                                      │  │  │
│  │  └─────────────────────┘  └──────────────────────────────────────┘  │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└───────┬────────────────────┬─────────────────────────────────┬────────────┘
        │                    │                                 │
        v                    v                                 v
┌─────────────────┐ ┌─────────────────────────────┐ ┌──────────────────────┐
│   MVCC Core     │ │     BufferPoolHandle        │ │   Storage B-trees    │
│  src/mvcc/*     │ │                             │ │                      │
│ ┌─────────────┐ │ │ ┌─────────────────────────┐ │ │ ┌──────────────────┐ │
│ │TimestampOr. │ │ │ │  main pool              │ │ │ │  BTree<S>        │ │
│ │HLC 12B      │ │ │ │   inner_32k (pos 3)     │ │ │ │  (one generic,   │ │
│ │(ms+u32 log) │ │ │ │   inner_4k  (pos 4)     │ │ │ │   primary AND    │ │
│ └─────────────┘ │ │ └─────────────────────────┘ │ │ │   sec-index)     │ │
│ ┌─────────────┐ │ │ ┌─────────────────────────┐ │ │ └──────────────────┘ │
│ │ReadViewReg. │ │ │ │  history_pool (dedic.)  │ │ │ ┌──────────────────┐ │
│ │(position 5) │ │ │ │  pos 1 — outermost      │ │ │ │  HistoryStore    │ │
│ │BTreeMap<    │ │ │ │  debug_assert guard     │ │ │ │  key =           │ │
│ │  u64,       │ │ │ │  (no re-entry)          │ │ │ │   ns_BE|kind|    │ │
│ │  Weak<RV>>  │ │ │ └─────────────────────────┘ │ │ │   key|ts_BE      │ │
│ └─────────────┘ │ │ ┌─────────────────────────┐ │ │ └──────────────────┘ │
│ ┌─────────────┐ │ │ │  AllocatorHandle        │ │ └──────────────────────┘
│ │VersionEntry │ │ │ │   state Mutex (pos 2)   │ │
│ │ start/stop  │ │ │ │   DeferredFreeQueue     │ │
│ │ txn_id      │ │ │ │     (pos 1.5)           │ │
│ │ data:       │ │ │ └─────────────────────────┘ │
│ │  Inline |   │ │ │ ┌─────────────────────────┐ │
│ │  Overflow(  │ │ │ │  JournalManager         │ │
│ │   OverflowR)│ │ │ │   ChainCommit frames    │ │
│ └─────────────┘ │ │ │   + CRC32 disambig.     │ │
│ ┌─────────────┐ │ │ │   oracle.set_min on     │ │
│ │ChainSnapshot│ │ │ │    reopen               │ │
│ │(CoW deep    │ │ │ └─────────────────────────┘ │
│ │ clone; CAS  │ │ └─────────────────────────────┘
│ │ incref each │ │
│ │ OverflowRef)│ │        ┌───────────────────────────────────────────┐
│ └─────────────┘ │        │       Per-Frame in BufferPool             │
│ ┌─────────────┐ │        │  ┌───────────────┐  ┌───────────────────┐ │
│ │17 metrics   │ │        │  │ data:         │  │ version_chains:   │ │
│ │counters     │ │        │  │ [u8; PAGE]    │  │ HashMap<Key,      │ │
│ └─────────────┘ │        │  │ (baseline)    │  │  Arc<VecDeque<    │ │
└─────────────────┘        │  └───────────────┘  │    VersionEntry>>>│ │
                           │                     └───────────────────┘ │
                           └───────────────────────────────────────────┘
```

### Lock Order (outer → inner)

| Position | Resource | Notes |
|---|---|---|
| 1 | history-store partition | outermost |
| 1.5 | `DeferredFreeQueue::pending` | brief, RAII push on refcount → 0 |
| 2 | `AllocatorHandle::state` | alloc/free/refcount-header-write |
| 3 | 32 KB main partition (`inner_32k`) | |
| 4 | 4 KB main partition (`inner_4k`) | |
| 5 | `ReadViewRegistry` | must be snapshotted BEFORE partition mutex in reconcile |
| 6 | writer serialization (`PagedEngine::inner` Mutex) | one writer at a time |

Readers acquire NONE of {AllocatorHandle::state, main partition, history-store partition} for pure reads. The only lock any reader path touches is `DeferredFreeQueue::pending` — briefly, when `OverflowRef::Drop` decrefs to 0 and pushes a `u32` before releasing.

## 2. Read Path (`find`)

```
User                PagedEngine         Registry      Oracle       BTree          BufferPool         ChainSnap         HistoryStore
 │                      │                   │            │            │                │                 │                 │
 │── find(filter) ─────>│                   │            │            │                │                 │                 │
 │                      │─ inner.lock() ──> (brief; tree lookup only)                                                      │
 │                      │─ now() ─────────────────────> read_ts                                                            │
 │                      │─ register(txn_id, Weak<RV>) ──>│            │                │                 │                 │
 │                      │─ inner.unlock() ──>            │            │                │                 │                 │
 │                      │                                             │                │                 │                 │
 │                      │── range_scan_mvcc(view, history_probe) ────>│                │                 │                 │
 │                      │                                             │─ read_leaf ───>│                 │                 │
 │                      │                                             │                │─ new(chains,v) >│                 │
 │                      │                                             │                │                 │ poison pre-chk  │
 │                      │                                             │                │                 │ fetch_add pin   │
 │                      │                                             │                │                 │ deep clone +    │
 │                      │                                             │                │                 │   CAS incref    │
 │                      │                                             │                │                 │   each Overflow │
 │                      │                                             │                │                 │ poison post-chk │
 │                      │                                             │                │<─ ChainSnapshot ┤                 │
 │                      │                                             │                │                 │                 │
 │                      │  ┌── for each key in leaf ──────────────────┴────────────────────────────────────────────────────┤
 │                      │  │                                          │─ visible_at(k,v) ─────────────> │                  │
 │                      │  │   ┌── chain hit ── return VersionEntry <──────────────────────────────────┤                   │
 │                      │  │   └── chain miss ─> probe_primary(ns,k,read_ts) ──────────────────────────────────────────>   │
 │                      │  │                     (HistoryStoreGuard thread-local)                                           │
 │                      │  │   ┌── history hit <──────────────────────────────────────────────────────────────────────────┤
 │                      │  │   └── history miss ─> fall back to on-disk cell (baseline)                                    │
 │                      │  └──────────────────────────────────────────────────────────────────────────────────────────────┤
 │<── cursor(rows) ─────┤                                                                                                   │
 │                                                                                                                          │
 │  (later, ReadView drops)                                                                                                 │
 │                      │─ Drop ──> unregister from Registry                                                                │
 │                      │          ── Arc<VecDeque> drops ── RAII decref each OverflowRef                                   │
 │                      │          ── any ref → 0 enqueues to DeferredFreeQueue (drained by writer)                         │
```

## 3. Write Path (`insert` / `update` / `delete`)

```
User           PagedEngine        WriteTxn       Allocator      BTree           sec-idx          Oracle        BufferPool       Journal
 │                 │                  │              │             │                │                │              │              │
 │─ insert(doc) ─> │                  │              │             │                │                │              │              │
 │                 │─ inner.lock() ──────────────────────────────────────────────────────────────────────────── (pos 6)
 │                 │─ begin(txn_id) ─>│              │             │                │                │              │              │
 │                 │                  │─ drain_free_queue ──> (pos 1.5 → pos 2)
 │                 │<─── txn ─────────┤              │             │                │                │              │              │
 │                 │                                                                                                                │
 │                 │   [dual-write: durable cell + staged chain entry]                                                              │
 │                 │─ tree.insert(key, bson) ───────────────────────>│                │                │              │              │
 │                 │─ stage_primary_insert(ns, key, bytes) ──> WriteTxn.pending_primary                                              │
 │                 │─ check unique + pending_sec_index conflict ─────────────────────>│                │              │              │
 │                 │─ stage_sec_index_insert(root, secKey, id) ──> WriteTxn.pending_sec_index                                        │
 │                 │                                                                                                                │
 │                 │─ commit(oracle, handle) ──>│                                                                                   │
 │                 │                             │─ allocate_commit_ts ────────────────────────────── > commit_ts (HLC)             │
 │                 │                             │                                                                                  │
 │                 │  ┌── drain pending_primary ─┤                                                                                  │
 │                 │  │                           │─ find_leaf(ns, key) ────────────────────────────────────> owning leaf page      │
 │                 │  │                           │─ take_chain(page) ──────────────────────────────────────> Arc<VecDeque>         │
 │                 │  │                           │─ Arc::make_mut (CoW)                                                             │
 │                 │  │                           │─ prev_head.stop_ts = commit_ts                                                   │
 │                 │  │                           │─ push_front VersionEntry{start=commit_ts, stop=MAX}                              │
 │                 │  │                           │─ put_chain ──────────────────────────────────────────────>  (same page)         │
 │                 │  └───────────────────────────┤                                                                                  │
 │                 │  ┌── drain pending_sec_index ┤                                                                                  │
 │                 │  │                           │─ install index entry ─────────────────────────> sec-idx BTree                   │
 │                 │  └───────────────────────────┤                                                                                  │
 │                 │                             │─ append_chain_commit ──────────────────────────────────────────────────────── > │
 │                 │                             │  { commit_ts, refcount_deltas, page_writes, CRC32 }                               │
 │                 │                             │<─────────── fsync ─────────────────────────────────────────────────────────── ── │
 │                 │<─ Ok ────────────────────────┤                                                                                  │
 │                 │─ inner.unlock() ────────────────────────────────────────────────────────────────────────── (release pos 6)
 │<── Ok ──────────┤                                                                                                                 │
```

## 4. Reconciliation on Eviction

```
Checkpoint/pin-miss     BufferPool         Registry              Partition(3/4)        Allocator(2 + 1.5)      HistoryStore
     │                      │                  │                      │                     │                       │
     │─ pin_with_reconcile ─>│                  │                      │                     │                       │
     │                      │─ oldest_required_ts() ───> ort                                                          │
     │                      │                                          │                     │                       │
     │                      │  ** ort SNAPSHOTTED BEFORE partition lock (order 5 → 3) **                             │
     │                      │                                          │                     │                       │
     │                      │─ lock partition ────────────────────────>│                     │                       │
     │                      │                                                                                         │
     │                      │  ┌── for each (key, chain) in frame.version_chains ───────────────────────────────────┤
     │                      │  │   Arc::make_mut(chain)                                                               │
     │                      │  │   chain.retain(|e| e.stop_ts > ort)                                                  │
     │                      │  │     -> dropped VersionEntry runs OverflowRef::Drop (RAII decref)                     │
     │                      │  │     -> any ref→0 enqueued onto DeferredFreeQueue (pos 1.5)                           │
     │                      │  │   if chain.len==1 && matches(on_disk) { remove key }                                 │
     │                      │  │   (optionally push aged VersionEntry into HistoryStore under guard)                  │
     │                      │  └──────────────────────────────────────────────────────────────────────────────────── ┤
     │                      │                                                                                         │
     │                      │─ release partition lock ─────────────────>  (drop pos 3/4 BEFORE pos 2)                 │
     │                      │                                                                                         │
     │                      │─ drain_free_queue(io, queue) ───────────────────────────────>│                          │
     │                      │                                                              │ acquire pos 1.5          │
     │                      │                                                              │ acquire pos 2 (state)    │
     │                      │                                                              │ for each enqueued page:  │
     │                      │                                                              │   load refcount Acquire  │
     │                      │                                                              │   if 0 → free_overflow   │
     │                      │                                                              │   else → re-enqueue      │
     │                      │                                                              │ release pos 2, pos 1.5   │
     │                      │<───────────────────────────────────────────────────────────── ┤                          │
     │                      │─ metrics: reconcile.entries_dropped_total += N                                           │
     │                      │           overflow.pages_freed_total += K                                                │
     │                      │           deferred_free_queue_depth (gauge)                                              │
     │<─ Ok ────────────────┤                                                                                          │
```

## 5. `drop_collection` Barrier

The only global stop-the-world path.

```
User              PagedEngine         inner(pos6)        Registry(pos5)         Views(all)        free_subtree
 │                    │                   │                   │                      │                 │
 │ drop_collection ─> │                   │                   │                      │                 │
 │                    │─ lock ───────────>│                   │                      │                 │
 │                    │                                       │                      │                 │
 │                    │─ force_expire_all() ──────────────── >│                      │                 │
 │                    │                                       │─ snapshot Weak<RV>s  │                 │
 │                    │                                       │   (under registry    │                 │
 │                    │                                       │    mutex, release    │                 │
 │                    │                                       │    BEFORE upgrade)   │                 │
 │                    │                                       │                      │                 │
 │                    │                                       │─ for each Weak → upgrade → view.force_expire()
 │                    │                                       │                      │─ poisoned.store(true, Release)
 │                    │                                       │                      │─ wait_pin_drain (spin 128 → yield, warn on stall)
 │                    │                                       │                      │  (blocks until pin_ops_in_flight == 0)
 │                    │                                       │                      │                 │
 │                    │─ free_subtree(ns) ─────────────────────────────────────────────────────────── > │
 │                    │   (runs under writer mutex still held, chains drop via RAII,                   │
 │                    │    overflow pages decref/enqueue, drain in same pass)                          │
 │                    │<── Ok ─────────────────────────────────────────────────────────────────────── ┤
 │                    │─ unlock ─────────>│                                                             │
 │<── Ok ─────────────┤                                                                                 │
```

Post-barrier: pre-existing ReadViews are poisoned.
- `ChainSnapshot::new(view)` → poison pre-check → returns empty snapshot (no rows).
- Any caller that opts in to `view.check_active()` → `Err(ReadViewExpired)`.
- New reads open a fresh view and see `NamespaceNotFound`.

## 6. One-Line Mental Model

```
READ   = ReadView pins read_ts → chain walk (CoW snapshot) → fallback to history store → fallback to on-disk cell
WRITE  = WriteTxn holds writer mutex → stage primary + sec-index + overflow deltas → one ChainCommit frame @ one commit_ts → fsync
GC     = on eviction, reconcile uses oldest_required_ts to drop dead entries + decref overflow (RAII) → writer drains free queue
DROP   = writer mutex + force_expire_all views + pin_ops drain + free_subtree under same lock
```

## 7. Why the History Store Exists

Version chains live in-memory on buffer-pool frames. The buffer pool is finite. Chains grow on every write. Evictions reclaim frames.

Without a history store, two bad options:

1. **Discard the chain on eviction.** A long reader holding an old `read_ts` loses the version it needs — snapshot isolation silently breaks.
2. **Refuse to evict frames with chains.** One long reader pins the entire buffer pool indefinitely — OOM.

The history store is the third option: when reconciliation evicts a frame with chain entries still visible to some active reader (`stop_ts > oldest_required_ts`), it **spills them to an on-disk B-tree** keyed by `(ns, kind, doc_id, start_ts)`. The frame is then freely evictable. On chain-miss, the reader probes the history store by descending range-scan from `(ns, kind, doc_id, view.read_ts)`.

Dedicated buffer pool partition (position 1, outermost) prevents recursive eviction: history-store work never reaches back into main-pool locks.

GC runs at checkpoint, walks the history store, deletes entries with `stop_ts ≤ oldest_required_ts`, RAII-decrefs any overflow pages they referenced.

| Without history store | With history store |
|---|---|
| Long reader blocks GC → OOM | GC runs freely, long reader still satisfied |
| Chain entries stuck in RAM for hours | Chain entries age out to disk, RAM bounded |
| Snapshot isolation best-effort | Snapshot isolation durable across evictions |

## 8. Component Map (Source Locations)

| Component | File |
|---|---|
| `TimestampOracle` (HLC 12B) | `src/mvcc/timestamp.rs` |
| `VersionEntry`, `VersionData`, `OverflowRef` | `src/mvcc/version.rs` |
| `ReadView`, `ReadViewRegistry`, `ChainSnapshot` | `src/mvcc/read_view.rs` |
| `WriteTxn` + staging APIs | `src/mvcc/transaction.rs` |
| 17 metrics counters | `src/mvcc/metrics.rs` |
| `BufferPool` + `reconcile` + `pin_with_reconcile` | `src/storage/buffer_pool.rs` |
| `AllocatorHandle`, `DeferredFreeQueue`, refcount atomics | `src/storage/allocator.rs` |
| `BufferPoolHandle` (main + history pools) | `src/storage/handle.rs` |
| `BTree<S: BTreePageStore>` (primary + sec-index, single generic) | `src/storage/btree.rs` |
| `HistoryStore` (kind-tagged keys) | `src/storage/history_store.rs` |
| `HistoryProbe` trait wiring | `src/storage/btree.rs` + `src/storage/paged_engine.rs` |
| `PagedEngine`, `with_txn`, `drop_collection` barrier | `src/storage/paged_engine.rs` |
| `JournalManager`, `ChainCommitFrame`, oracle recovery | `src/journal/mod.rs`, `src/journal/log_file.rs` |

See `docs/adr/0001-mvcc.md` for the accepted design decisions and `.omc/plans/mvcc-wiredtiger.md` for the full plan with per-task acceptance criteria.
