# PR4 cross-key audit — `Mutex<HistoryStore<BufferPoolPageStore>>` sharding

**Author:** worker-history
**Date:** 2026-05-10
**Status:** parking-lot reference (PR4 implementation deferred per spike #6)
**Plan:** `.omc/plans/mwmr-page-latch.md` rev 4, PR4
**Verdict:** **PROCEED-when-justified** — sharding is structurally safe; current profile evidence does not justify the implementation cost. Concrete re-eval triggers in §6.

## 0. Context — why this is a parking-lot reference

The MWMR page-latch plan rev 4 enumerated PR4 as the optional final step, conditional on a cross-key audit of `HistoryStore<S>`. PR0 baselines triggered the plan's own decision-gate escalation (`R = same_ns_partitioned@4 / multi_ns_single@4 = 0.38`, below the 0.5 floor); the user picked option (b), a spike (#6) to identify the actual upstream bottleneck. The spike's verdict: **`Mutex<HistoryStore>` is cold on the current profile** — `probe_visible_version` self-time is 0.04% and `_pthread_mutex_firstfit_lock_slow` does not appear in the top 30 spike samples. The PR4 hypothesis (history-store mutex is a meaningful contention source) is not supported by current evidence.

Implementation is therefore deferred. The audit work is preserved here as a parking-lot reference so a future profile that **does** show history-store contention can pick this up without redoing the cross-key analysis. Concrete re-eval triggers are listed in §6.

**Sources reviewed during the audit:**
- `src/storage/history_store.rs` lines 396–746 (full method surface)
- `src/storage/paged_engine/state.rs:192,617–652` (declaration + init)
- `src/storage/paged_engine/visibility.rs:20–116` (SecondaryHistoryProbe)
- `src/storage/paged_engine/snapshot_ops.rs:67–152, 531–540` (PrimaryHistoryProbe + gc_pass site)
- `src/storage/reconcile/driver.rs:11, 460–493` (commit_history_spills)
- `src/storage/paged_engine/tests/logical_replay_frontier.rs:29–31` (asserts recovery_apply has no spill calls — closes the "recovery touches HistoryStore" question)

---

## 1. Method surface on `HistoryStore<S: BTreePageStore>`

Enumerated from `src/storage/history_store.rs`, grouped by callability outside the type itself.

### Public-to-the-crate, runtime
| Method | Signature shape | Touches keys |
|---|---|---|
| `probe_primary` | `(&self, collection_id, doc_id, read_ts) -> Result<Option<VersionEntry>>` | exactly one prefix `(TreeIdent::Primary, doc_id)` |
| `probe_sec_index` | `(&self, collection_id, index_id, sec_key, read_ts) -> Result<Option<VersionEntry>>` | exactly one prefix `(TreeIdent::Secondary{index_id}, sec_key)` |
| `commit_spill_txn` | `(&mut self, txn: HistorySpillTxn) -> Result<()>` | **N keys per call** — loops over `txn.staged` |
| `commit_spill_txn_durable` (impl on `BufferPoolPageStore` only) | `(&mut self, txn) -> Result<()>` | **N keys + 1 header update + 1 flush** |
| `gc_pass` | `(&mut self, ort: Ts) -> Result<GcResult>` | **full-tree scan, all keys** |

### Public-to-the-crate, init-time only
| Method | Notes |
|---|---|
| `create_empty_root` | called once on fresh-DB open from `state.rs:619`; returns `(Self, root_page)` |
| `open` | called once on reopen from `state.rs:629` |
| `with_overflow_allocator` | one-shot builder, called once at open |

### Test-only
- `create` (gated `#[cfg(test)]`)
- `decode_history_key` (gated `#[cfg(test)]`)

### `HistorySpillTxn` static helpers
- `HistoryStore::<S>::spill_primary(txn, ident, key, entry, counter)` — stages one write into a `HistorySpillTxn`. **Pure staging — no state mutation on `HistoryStore`.**
- `HistoryStore::<S>::spill_sec_index(...)` — same shape.

### Private helpers (called only from public methods above)
- `probe_visible_entry` — single-prefix range scan
- `apply_spill` — single-key insert with idempotent duplicate check (`if existing == value { return Ok(false) }`)

---

## 2. Cross-key invariant analysis (the audit question)

For each method that touches more than one key, document (a) the consistency requirement and (b) whether `fxhash(prefix) % N` sharding breaks it.

### 2.1 `commit_spill_txn` — N-write batch
**Consistency requirement today:** **None** that depends on cross-key atomicity.

Code (`history_store.rs:501-519`):
```rust
for write in txn.staged {
    ... apply_spill(...)?;
    if inserted { forget_history_record_overflow_ref(entry); }
}
```

The loop returns `Err` on the **first** failed `apply_spill`. **Earlier writes in the same batch remain inserted** (already durable in the in-memory B-tree state). There is no rollback. A caller observing `Err` from `commit_spill_txn` cannot assume "no writes happened".

Per-key idempotency (`apply_spill` in `history_store.rs:521-540`) provides the only durable invariant:
- A retry with the same `(ident, key, entry, counter)` and the same encoded value short-circuits to `Ok(false)`.
- A retry with a different value bytes returns `Error::DuplicateKey`.

**Sharding effect:** partitioning a batch into per-shard sub-batches and committing each under its own mutex is observationally identical to today's "first-error early-return" semantics. Per-shard order may differ (shard 5 may fail before shard 2 in a different sequence than today), so the *identity* of the first-failed-key is non-deterministic — but no caller relies on a specific failure-identity ordering.

**Verdict:** ✓ **shard-safe.** No invariant broken.

### 2.2 `commit_spill_txn_durable` — N-write batch + header persist + flush
**Consistency requirement today:** *history-before-leaf* — the staged history writes must be durable before the caller (`reconcile/driver.rs:commit_history_spills`) proceeds with leaf install.

Code (`history_store.rs:641-654`):
```rust
self.commit_spill_txn(txn)?;
let root_page = self.tree.root_page;
let root_level = self.tree.root_level;
handle.allocator().update_header(|h| {
    h.history_store_root_page = root_page;
    h.history_store_root_level = root_level;
})?;
handle.flush()
```

The flush is **one** `BufferPoolHandle::flush` call covering whatever pages are dirty in the dedicated history-pool partition.

**Sharding effect:**
- Sharded layout requires N separate `BTree<S>` (one per shard, see §4 below) → N separate `(root_page, root_level)` pairs in `FileHeader`.
- Per-shard sub-batches each call `commit_spill_txn` against their own shard's tree → each shard updates its own `(root_page, root_level)` pair in the header.
- A **single global flush** at end-of-batch satisfies the history-before-leaf invariant for the union (the dedicated history pool is one partition; flushing it once persists every shard's mutated pages atomically per-page but not as a bundle).
- Failure mode is unchanged: if shard 5 fails partway, the caller (`commit_history_spills`) returns `Err` and the leaf install is skipped — same shape as today's first-error early-return.

**Header-update concurrency:** `handle.allocator().update_header(...)` is internally synchronized by the allocator (header is a single page mutated under its own lock). Multiple shards updating disjoint header slots commute (each writes `h.history_store_root_pages[shard_idx]` only). ✓ Safe.

**Verdict:** ✓ **shard-safe.** No new failure mode; per-shard durability composes.

### 2.3 `gc_pass` — full-tree scan
**Consistency requirement today:** **None across keys.** Each victim deletion + overflow-refcount transfer is independent. The `record_history_store_gc_pass()` metric ticks once per call.

Code (`history_store.rs:689-745`): `tree.range_scan(None, None)` → build victim list → per-key `tree.delete` → per-entry overflow `OverflowRef::from_existing_refcount` (RAII drop decrements refcount on `AllocatorHandle`).

**Sharding effect:** fan out to all N shards, sequentially under each shard's mutex. Per-shard semantics identical. Aggregate `GcResult` is a sum across shards. Metric ticks once total (the wrapper records, not each shard). Overflow refcount lives on the **single** `AllocatorHandle` — refcount transfers across shards interleave safely under the allocator's existing concurrency contract.

**Cross-shard ordering:** none required. gc_pass deletions are append-tombstone-style (entries with `stop_ts <= ort` are removed); no cross-shard observation.

**Verdict:** ✓ **shard-safe**, but fan-out is sequential under N mutexes (cost analysis in §3).

### 2.4 Recovery / checkpoint paths — closure check
- `recovery_apply.rs` does **not** call any `HistoryStore` mutator (asserted by `tests/logical_replay_frontier.rs:29-31`). Recovery seats the history-store root pages from `FileHeader` at open time only (`state.rs:617-638`). Sharded variant: seat N roots from N header slots. ✓ Safe.
- `checkpoint` (snapshot_ops.rs:531-540) calls `gc_pass` once per checkpoint pass. Sharded variant fans out as described in §2.3. ✓ Safe.

### 2.5 Probe paths — single-key
- `probe_primary` and `probe_sec_index` both delegate to `probe_visible_entry`, which range-scans **bounded by a single `(TreeIdent, key_bytes)` prefix**. Shard key derivation must use this prefix (NOT the full key, because spills with specific `(start_ts, counter)` must land in the same shard as probes that don't know `start_ts`). Concretely: `fxhash(collection_id_be || tree_kind || index_id_be || key_len_be || key_bytes) % N`.

**Verdict:** ✓ **shard-safe.** Single-key paths are the headline win whenever contention manifests.

### 2.6 Overflow-buffer paths
The history value layout has two `data_kind` variants: `Inline` (bytes inline) and `Overflow` (`first_page` + `total_length` referencing an overflow chain owned by the `AllocatorHandle`). `OverflowRef` RAII wraps the refcount; `AllocatorHandle::overflow_refcount` and the deferred-free queue are global to the engine. Sharding `HistoryStore` does **not** shard the allocator. Cross-shard `OverflowRef` operations (e.g., a sec-index spill from shard A and a primary spill from shard B both holding overflow refs to the same chain — possible if `forget_history_record_overflow_ref` is bypassed somehow, though it isn't on the spill path) compose under the allocator's existing concurrency contract.

**Verdict:** ✓ **shard-safe.** Overflow refcounting is unaffected by `HistoryStore` sharding.

---

## 3. Cross-shard fanout cost evaluation

| Method | Today | Sharded (N=16) | Cost ratio |
|---|---|---|---|
| `probe_primary` / `probe_sec_index` | 1 mutex acquire | 1 mutex acquire (hash → shard) | **1.0×** (no change in lock count; contention drops by ~1/N when probes hit different shards) |
| `commit_spill_txn(_durable)` per leaf reconcile | 1 mutex + N writes | up to S unique shards × (mutex + per-shard writes) where S ≤ min(N, |batch|); one global flush | **S× lock acquires.** Folded-leaf batches typically span O(1)-O(few) keys, so S is small in practice. Worst case S = N = 16 lock acquires per checkpoint leaf. |
| `gc_pass` per checkpoint | 1 mutex + 1 full scan | N mutex acquires + N scans | **N× lock acquires.** Mitigated by the fact that gc_pass is per-checkpoint (low frequency) and per-shard scan is 1/N the size on average. |

**Probes** are the contention hotspot **in any future profile that justifies PR4** (`_pthread_mutex_firstfit_lock_slow on PrimaryHistoryProbe` per the plan §4 table). Sharding **directly attacks** that line: with uniform `fxhash` distribution over keys, contention drops by ≈1/N for the single-mutex-acquire probe path.

**Spill** lock-acquire amplification (S× vs 1×) is bounded by the per-leaf staging size, which is small (O(1) versions per key after Phase 3 reconcile pruning). The amplification is **constant per leaf** with very small S in practice.

**gc_pass** N× amplification is real but per-checkpoint (O(seconds-minutes) cadence). See §3.1 for the concrete upper bound.

### 3.1 `gc_pass` fanout cost — concrete upper bound

**Per-checkpoint additive overhead from sharding `gc_pass`:**

| Component | Today (1 tree) | Sharded (N=16) | Delta |
|---|---|---|---|
| Mutex acquires (uncontended `std::sync::Mutex` on macOS Apple Silicon: ~30–100 ns each) | 1 | 16 | **+15 acquires ≈ +0.45–1.5 µs** |
| `BTree::range_scan(None, None)` setup overhead per scan (root-page pin, cursor allocation: ~1 µs each, conservative) | 1 | 16 | **+15 setups ≈ +15 µs** |
| Aggregate scan body (cell decode + victim build + per-key delete): scales linearly with total entries scanned, **invariant under sharding** because total entries = sum of per-shard entries | T | T | **0** (work is preserved, just split N ways) |
| Metric tick `record_history_store_gc_pass` | 1 | 1 (in wrapper, not per-shard) | 0 |

**Upper-bound additive cost: ≈ 16 µs per checkpoint pass.**

At checkpoint cadence (seconds-to-minutes apart in steady state), 16 µs/pass is in the noise — it does **not** move steady-state throughput on `same_ns_single` or affect checkpoint p99 latency in any measurable way.

**Verdict:** fanout cost does **not** dominate. Per-key contention savings on the probe path are the headline win and are not offset by the spill/gc_pass amplification.

---

## 4. Structural constraint — why sharding requires N separate B-trees

The plan's sketch:
```rust
pub struct ShardedHistoryStore<S> {
    shards: [Mutex<HistoryStore<S>>; N],
}
```
implies **N separate `HistoryStore` instances**. Each `HistoryStore` owns a `BTree<S>` rooted at its own `root_page`. Why this is the only viable layout:

### 4.1 Could shards share one B-tree (option-a-shared-tree)? — REJECTED

Option-a is "N shard mutexes, one shared B-tree". This was the natural first design and it was rejected because of a **`&mut self` constraint on `BTree<S>`** that forecloses concurrent multi-mutex access to a shared tree. Documented here so future readers can see why N separate trees was the only viable path.

**The constraint, stated precisely (verified against `src/storage/btree/`):**

`BTree<S: BTreePageStore>` exposes the only mutating entry points for the history-store key space:
- `insert(&mut self, key, value)` — used by `apply_spill`
- `delete(&mut self, key)` — used by `gc_pass` victim removal

Both take `&mut self`. Their bodies:
- update `self.root_page: u32` and `self.root_level: u8` on root splits/merges
- traverse-then-mutate page chains, where the traversal cursor and the mutation point share the same `&mut` borrow lifetime
- coordinate parent-pointer fix-ups during splits across multiple buffer-pool pages (the splitter writes both old-leaf and new-leaf and rewrites the parent's child pointer; this is one logical mutation spanning ≥3 pages)

Rust's borrow checker forbids two threads from holding `&mut tree` simultaneously, even if they intend to mutate disjoint keys. The shard-mutex layout `[Mutex<HistoryStore<S>>; N]` over a *shared* `BTree<S>` would require either:
- **(i)** Wrapping the shared tree in `Mutex<BTree<S>>` and acquiring it inside every shard's critical section. This collapses sharding back to one global mutex — defeats the entire PR. ✗
- **(ii)** Replacing `&mut self` on `BTree::insert/delete` with `&self` and moving the `root_page`/`root_level` to `AtomicU32`/`AtomicU8`. This is fine for the *metadata*, but **does not solve splits**: a concurrent split needs to atomically rewrite `(old_leaf, new_leaf, parent.child_pointer)` across three buffer-pool pages. That requires a per-tree latch coupling protocol — i.e., a B-tree concurrency redesign (Bw-tree or B-link tree). The plan explicitly defers that as `mwmr-bw-tree-leaves` (Option C, §2). ✗
- **(iii)** Accepting that two shards can never insert into the same B-tree concurrently, defeating sharding. ✗

None of these is acceptable inside PR4's scope.

**Conclusion:** sharding requires **N separate `BTree<S>` instances**, each with its own `root_page` and `root_level`. This is the source of the `FileHeader` format change in §5.

The B-tree concurrency redesign that *would* enable option-a is filed as `mwmr-bw-tree-leaves` (a multi-thousand-LOC chain-format + WAL contract change spanning recovery and checkpoint, per plan §2 Option C). Out of scope for PR4 by design.

---

## 5. Implementation surface area (when justified)

| Surface | Today | Sharded | Δ LOC (rough) |
|---|---|---|---|
| `FileHeader` (`src/storage/header.rs`) | one `(history_store_root_page: u32, history_store_root_level: u8)` | N pairs (or one `[u32; N]` + one `[u8; N]` block) | +~80 LOC for serdes + offset shift |
| `state.rs` init (`MetadataState::new`) | seat one root | seat N roots, one `BufferPoolPageStore::new_history` per shard | +~80 LOC |
| `paged_engine.rs:457-458` (catalog/history page-id set for replay bound) | inserts one history root | inserts N roots | +~10 LOC |
| `snapshot_ops.rs::primary_history_probe` | wraps `&shared.history_store` | wraps `&shared.history_store_shards` + selects shard from key | +~30 LOC for `PrimaryHistoryProbe::probe_visible_version` shard routing |
| `visibility.rs::SecondaryHistoryProbe::probe_visible_version` | locks one mutex, calls `probe_sec_index` | hashes `(ident, key)`, selects shard, locks one mutex | +~25 LOC |
| `reconcile/driver.rs::commit_history_spills` | one `HistorySpillTxn` → one `commit_spill_txn_durable` | partition staged writes into per-shard `HistorySpillTxn`s, commit each, single global flush | +~80 LOC |
| `snapshot_ops.rs` checkpoint gc_pass | one `gc_pass(ort)` | fan out to N shards, sum `GcResult`s, tick metric once | +~30 LOC |
| New `src/storage/history_store_shards.rs` | n/a | `ShardedHistoryStore<S>` wrapper, `shard_for(key)`, `commit_spill_txn_durable_partitioned`, fanout helpers | +~150 LOC |
| Tests | history_store tests stay; add cross-shard recovery ordering test, shard-routing test | | +~80 LOC |
| **TOTAL ESTIMATE** | | | **~565 LOC ± 30%** |

The plan rev-4 estimate of 450 LOC ± 50% covers this honest 500–700 LOC range at its upper band. Pre-release status (per AGENTS.md project memory) makes the format change cheap. `HistoryStore<S>` itself stays largely unchanged — `HistoryStore<S>` remains the per-shard primitive; the wrapper composes them.

### 5.1 Pre-merge concerns to surface during the eventual PR4 implementation

These are NOT blockers to the verdict but should be addressed during implementation:

1. **Shard count `N=16` is a guess.** The plan defers a microbench inside PR4. Recommend committing the audit with `N=16` as a TUNABLE constant and noting the microbench-revisit obligation.
2. **`tools/perf/sample_hot.py` and `examples/perf_axis.rs`** — PR4 needs a sample profile capture confirming `_pthread_mutex_firstfit_lock_slow on PrimaryHistoryProbe` drops by ≥50% (per AC). This is straightforward.
3. **Cross-shard recovery ordering test** is required (per AC). Test strategy: create N spills covering keys that hash to different shards, force a process exit between commit_spill_txn and flush in a way that leaves one shard's write durable and another's not (using `test-hooks`). On recovery, assert the live engine reads back the durable shard's writes only and re-applies the lost ones from the journal tail (logical_replay_frontier path stays intact). Concrete failpoints to look at: `hidden_accessors::us026_fail_if_armed`.
4. **Catalog page-id set in `paged_engine.rs:457-458`** — used by `check_recovery_replay_pool_bound`. Sharded variant must insert all N history roots; missing any one would let recovery's pool-bound check ignore that shard's pages. Easy to get wrong.
5. **`HistoryStoreGuard` thread-local depth sentinel** at `history_store.rs:82-105` — the non-recursion sentinel must remain a SINGLE depth counter (not per-shard), because the protection is "any history-store entry, anywhere in the engine". Sharded variant: each shard's public method increments the same `HISTORY_STORE_DEPTH`. ✓ trivial.

---

## 6. Re-evaluation triggers (code-grounded)

This audit's **PROCEED-when-justified** verdict is bounded above by current profile evidence (spike #6: history-store mutex is cold). Implementation is unblocked when **either** of the following triggers fires on a future post-PR2 profile run:

### Trigger A — Probe-path mutex appears in profile top-30
On any post-PR2 `examples/perf_axis --axis same_ns_single --seconds 30 --writers 4` `sample` capture, post-processed via `tools/perf/sample_hot.py`:
- **Symbol:** `_pthread_mutex_firstfit_lock_slow` (or its parking_lot equivalent if the codebase migrates) appearing on the call stack of `PrimaryHistoryProbe::probe_visible_version` (`snapshot_ops.rs:141-152`) **or** `SecondaryHistoryProbe::probe_visible_version` (`visibility.rs:104-116`).
- **Threshold:** appears in the top-30 self-time entries of the `sample_hot.py` output for the run.
- **Action:** unblock PR4 implementation; this audit is the cross-key precondition.

### Trigger B — `probe_visible_version` self-time crosses 5% of write-path
On the same post-PR2 profile capture:
- **Metric:** combined self-time of `probe_visible_version` (both impls) as a fraction of `run_write_commit_envelope` (`paged_engine.rs:591`) self-time.
- **Threshold:** ≥ 5%.
- **Rationale:** spike #6 measured `probe_visible_version` at 0.04% of profile self-time on `same_ns_single@4`; a 100× rise to 5% indicates the cold lock has become a real contention source on whatever workload PR1/PR2 leaves behind.
- **Action:** unblock PR4 implementation.

### Cancel-only trigger (not re-eval) — Bw-tree path lands first
If `mwmr-bw-tree-leaves` ships before either trigger fires, PR4 is **superseded** rather than deferred. A B-link/Bw-tree leaf design would make option-a-shared-tree (one tree, latch-coupling protocol) viable, and the cleaner path is the latch-free leaf rather than mutex-sharded forest of N trees. Document this here so a future implementer can check `mwmr-bw-tree-leaves` status before picking up PR4.

### What does NOT trigger re-eval
- `same_ns_single` throughput stagnation alone: PR0/PR1/PR2 own that headline. PR4 only ships if the residual hot symbol after PR2 is the history-store mutex specifically.
- General write-path contention: PR1 (`flip_pending_for_txn` selective CoW) and PR2 (`live_delta_payload_exceeds_leaf_budget` deletion + Phase A/B install) are the in-flight contention plays. PR4 attacks a different lock entirely.
- Read-path slowdown without probe-path symbols: cold-read fallthrough to `HistoryStore` is rare in steady state; a slow read path is more likely buffer-pool latch wait (PR1/PR2 territory) than history-store mutex contention.

---

## 7. Verdict

**VERDICT: PROCEED-when-justified.**

Sharding `Mutex<HistoryStore<BufferPoolPageStore>>` is **structurally safe**. No method on `HistoryStore<S>` has a cross-key atomic invariant that sharding would break. The four candidate concerns:
- `commit_spill_txn` is **already non-atomic on per-write failure** — sharded fanout preserves this exact semantic;
- `commit_spill_txn_durable`'s history-before-leaf invariant is per-shard durability + single-global-flush — composes cleanly;
- `gc_pass` is per-key independent — fanout is mechanical, ~16 µs/pass overhead under N=16;
- Recovery does **not** call any `HistoryStore` mutator, so the recovery story is "seat N roots from N header slots" — straightforward.

**Implementation is gated on profile evidence.** Spike #6 found the history-store mutex cold (`probe_visible_version` 0.04%, no `firstfit_lock_slow` in top-30). Until trigger A or trigger B fires on a future profile, the ~565 LOC investment is unjustified. When a trigger fires, this audit is the cross-key precondition — the implementer can branch and start work without redoing the analysis.

---

## 8. Files PR4 implementation would touch (when justified)

- `src/storage/header.rs` — N-root format
- `src/storage/paged_engine/state.rs` — N-root seating in `MetadataState::new`; field decl change
- `src/storage/paged_engine/snapshot_ops.rs` — `PrimaryHistoryProbe` shard routing; checkpoint gc_pass fanout
- `src/storage/paged_engine/visibility.rs` — `SecondaryHistoryProbe` shard routing
- `src/storage/reconcile/driver.rs` — `commit_history_spills` partitioning
- `src/storage/paged_engine.rs` — recovery-replay pool-bound page-id set
- `src/storage/history_store_shards.rs` (NEW)
- `tests/history_store_shard_recovery_ordering.rs` (NEW)

`src/storage/history_store.rs` itself stays largely unchanged — `HistoryStore<S>` remains the per-shard primitive. The wrapper composes them.
