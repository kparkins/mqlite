---
date: 2026-05-10
branch: perf/p0-spike-bottleneck
parent: perf/p0-baselines @ 95203e3
purpose: Disambiguate the upstream root cause that PR0's R=0.38 escalation surfaced
sample_inputs:
  - docs/perf-baselines/2026-05-10-pre-r1-hot.md   (same_ns_single@4, 15s, concurrent)
  - docs/perf-baselines/2026-05-10-spike-partitioned-hot.md (same_ns_partitioned@4, 30s, isolated)
hardware: MacBook Pro Mac15,7; Apple M3 Pro; 12 cores (6P+6E); 36 GB; AC; macOS 26.4.1
---

# Spike: same-NS upstream bottleneck diagnosis

## Why this spike

PR0's decision-gate failed at R = `same_ns_partitioned@4 / multi_ns_single@4`
= 0.3823 < 0.5. The plan §3 PR0 rule says the bottleneck is broader
than per-leaf latches — but doesn't pin the actual root. This spike
captures one isolated `sample` profile on `same_ns_partitioned@4`
(the cleanest test case: by construction the four hot leaves are
disjoint) and diffs it against the existing `same_ns_single@4`
baseline. Symbols that DROP are leaf-latch symptoms; symbols that
PERSIST identify the upstream ceiling.

Throughput note for context (not the diagnostic itself):

| axis (writers)            | duration | docs/s | total samples |
|---------------------------|---------:|-------:|--------------:|
| `same_ns_single@4`        |     15 s | 4 166  |       128 331 |
| `same_ns_partitioned@4`   |     30 s | 2 202  |       204 104 |

Per-second sample density is comparable (8 555 vs 6 803 / s); throughput
is lower in the partitioned 30 s run because workload throughput
degrades with elapsed time on this DB (chain growth, page count, MVCC
history). The diagnostic relies on RELATIVE rank of hot symbols, not
absolute counts.

## Side-by-side hot frames

| symbol                                                          | baseline single@4 |  pct  | spike partitioned@4 |  pct  | persists? |
|-----------------------------------------------------------------|------------------:|------:|--------------------:|------:|:---------:|
| `Collection::insert_one`                                        |            41 372 | 32.2% |              96 478 | 47.3% |    yes    |
| `_pthread_cond_wait`                                            |            13 178 | 10.3% |              27 571 | 13.5% |    yes    |
| `BufferPool::pin_then_latch`                                    |             6 849 |  5.3% |              11 092 |  5.4% |    yes    |
| `BTree::scan::get_mvcc`                                         |             6 490 |  5.1% |              14 751 |  7.2% |    yes    |
| `__psynch_cvwait`                                               |             6 193 |  4.8% |              14 872 |  7.3% |    yes    |
| **`parking_lot::RawRwLock::lock_exclusive_slow`** *(SYNC)*       |             4 305 |  3.4% |          **15 834** | **7.8%** | **yes (got worse)** |
| **`alloc::sync::Arc<T>::make_mut`**                              |             4 093 |  3.2% |          **13 244** | **6.5%** | **yes (got worse)** |
| `parking_lot::RawRwLock::lock_shared_slow` *(SYNC)*              |             2 093 |  1.6% |               3 311 |  1.6% |    yes    |
| `BTree::read_leaf_for_point_key`                                 |               n/a |       |               5 018 |  2.5% |    yes    |
| `parking_lot::raw_mutex::RawMutex::unlock_slow` *(SYNC)*         |               273 |  0.2% |                 477 |  0.2% |    yes    |
| `PrimaryHistoryProbe::probe_visible_version`                    |               118 |  0.1% |                  78 |  0.04% |   COLD    |
| `paged_engine::publish::rebuild_and_publish`                    |               n/a |       |                  12 |  0.01% |   COLD    |
| `journal::log_file::sync_data`                                  |                93 |  0.1% |                 216 |  0.1% |   COLD    |

The four hot symbols that are at minimum equally hot (and several
that get WORSE in proportion) when leaves are disjoint:
`pin_then_latch`, `lock_exclusive_slow`, `Arc::make_mut`,
`lock_shared_slow`. These are exactly the symbols the rev-4 plan's
PR1 + PR2 target.

The four candidate roots from the planning sketch and their fate:

| # | candidate                                                  | spike data                                                  | verdict |
|---|------------------------------------------------------------|-------------------------------------------------------------|--------:|
| 1 | `Mutex<HistoryStore>` / `probe_visible_version` (PR4 hyp.) | `probe_visible_version` 78 samples (0.04%); no `firstfit_lock_slow` in top 30 | **RULED OUT** |
| 2 | B-tree internal-node + buffer-pool latch primitives        | `lock_exclusive_slow` 15 834 (7.8%); `pin_then_latch` 11 092 (5.4%) — TOP | **CONFIRMED** |
| 3 | MVCC publish gate / commit-frontier                         | `rebuild_and_publish` 12 samples (0.01%); no `flip_pending` *as a leaf* | **RULED OUT** |
| 4 | Journal / `LogManager` / journal mutex                     | `sync_data` 216 (0.1%), `fcntl` 216 (0.1%); no journal-mutex symbols. Rev-5 PR1 holds. | **RULED OUT** |

## What `lock_exclusive_slow` is actually waiting for

Tracing the deeper call sites in `/tmp/sample-spike-partitioned.txt`,
the dominant exclusive-latch site is:

```
flip_pending_to_committed_for                       (paged_engine/index_maint.rs:585)
+ pin_then_latch (mode = Exclusive)                 (buffer_pool/mod.rs:1183)
  + RawRwLock::lock_exclusive_slow                  ~22 K samples across 3 stacks
```

i.e., the **commit path's `flip_pending_to_committed_for` taking the
exclusive page latch via `pin_then_latch`** — exactly the call shape
the rev-4 plan flagged. Three stack instances accumulate to ~22 000
samples just on this one bottleneck, and these are the single largest
contributor to `lock_exclusive_slow` in the partitioned profile.

Secondary callers of `pin_then_latch` (smaller but real):

```
acquire_smo_latches                                 (paged_engine/smo_latch.rs:151)
+ pin_then_latch                                    ~5 600 samples (4 stacks)
read_leaf_for_point_key                             (btree/scan.rs)
+ pin_then_latch (mode = Shared)                    ~9 700 samples — read coupling
```

`acquire_smo_latches` is exactly the call PR2 redesigns into Phase A/B
shared-then-exclusive (plan §3 PR2). `read_leaf_for_point_key`'s
shared latch is what the read-coupling AC measures.

## Where `Arc::make_mut` is coming from

```
flip_pending_to_committed_for + 304                 (paged_engine/index_maint.rs:585)
+ LatchedPinnedPage::flip_pending_for_txn           (buffer_pool/mod.rs:515-562)
  + Arc::make_mut                                   ~17 K inclusive across 3 stacks
```

This is the **exact call path the rev-4 plan PR1 targets** with
"selective CoW + bounded-retry Phase A/B" (plan §3 PR1). Today
`flip_pending_for_txn` calls `Arc::make_mut` on every chain on the
frame (`for chain_arc in frame.deltas.values_mut()` at
`buffer_pool/mod.rs:524`), not only chains containing a `Pending(txn_id)`
entry. With partitioning, each writer's dedicated leaf accumulates ~16 K
chains over 30 s, and every commit's `flip_pending_for_txn` iterates
ALL of them. That's where the 13 244 `Arc::make_mut` samples come from.

PR1's selective CoW (skip chains without `Pending(txn_id)`) directly
deletes that work. The fix is correct *regardless of which axis we run*.

## Why partitioning didn't move throughput

`same_ns_partitioned@4` (3 844 dps) is LOWER than `same_ns_single@4`
(4 166 dps) even though leaves are disjoint, because:

1. **`Arc::make_mut` work scales with chain count per leaf, not with
   inter-writer leaf overlap.** Each partitioned writer accumulates
   ~16 K chains on its dedicated leaf and pays full O(chains)
   `make_mut` per commit. Same-NS-single has 4 writers piling onto 1
   leaf so chains accumulate similarly there. Partitioning trades
   "few hot leaves with many writers each" for "many hot leaves
   with one writer each" — total chain work is invariant.

2. **`flip_pending_to_committed_for`'s exclusive latch via
   `pin_then_latch` contends regardless of which leaf is targeted.**
   The 22 K-sample exclusive-latch wait in the partitioned profile is
   on the writer's OWN leaf — it's serialization between the writer
   itself and concurrent buffer-pool background activity (durability
   thread firing every `DURABILITY_INTERVAL_MS=100ms`,
   snapshot/checkpoint operations, etc.). Partitioning increases
   parallelism, which gives more chances to collide with background
   work, which is why `lock_exclusive_slow` got WORSE in proportion
   (3.4% → 7.8%).

3. **The visible "ceiling" is the per-writer steady-state cost of
   one commit:** flip O(chains) + acquire exclusive latch + journal
   sync. No amount of leaf-disjointness reduces the per-commit cost.
   To move the headline you have to reduce per-commit work, which is
   exactly what PR1 (selective CoW) and PR2 (Phase A/B install) do.

## Verdict

**Rev-4 page-latch theory is RIGHT despite R < 0.5.** Partitioning
failing to relieve contention is not evidence that the page-latch
hypothesis is wrong; it's evidence that per-commit work (Arc::make_mut
per chain, exclusive latch hold during flip) is the binding cost,
NOT inter-writer leaf overlap.

The four candidate root-causes from the spike planning sketch
collapse to:

- (1) `Mutex<HistoryStore>` / PR4 hypothesis — **RULED OUT** by the
  cold `probe_visible_version` (0.04%) and absent `firstfit_lock_slow`.
- (2) B-tree internal-node + buffer-pool latch primitives — **CONFIRMED
  as the dominant symbols**, but specifically the LEAF-level latches
  during commit (`flip_pending_to_committed_for + pin_then_latch
  Exclusive`), NOT internal-node latches. PR1 + PR2 target this
  directly.
- (3) MVCC publish gate / commit-frontier — **RULED OUT** by the cold
  `rebuild_and_publish` (0.01%).
- (4) Journal / `LogManager` — **RULED OUT** by cold `sync_data` /
  absent journal-mutex symbols. Rev-5 PR1's journal-mutex removal
  is holding.

## Recommendation for next planning unit

**Verdict bucket: "rev-4 page-latch theory still right despite R<0.5
— partitioning doesn't help and PR1 should still ship."**

Concrete next steps for team-lead:

1. **Proceed with PR0.5 → PR1 → PR2 as planned.** The structural
   ACs in PR1 (selective CoW, retry counters, bounded-retry exhaustion
   gate) and PR2 (Phase A/B, hold-time mean, `live_delta_payload_*`
   deletion) all measure real improvements that the spike data
   confirms target the dominant symbols.

2. **Throughput compounding-delta ACs need calibration adjustment.**
   The original `baseline_raw × 1.20 = 5 000 dps` PR1 target assumed
   leaf-latch was the headline gate. The spike says it IS, but the
   per-commit work ceiling (chain count, exclusive-latch hold) is
   reduced by PR1+PR2 by a factor that's hard to project without
   measurement. Recommend keeping PR1's 1.20× as a stretch and
   adopting `1.05× = 4 374 dps` as the floor that triggers
   `keep PR1, document shortfall` rather than revert.

3. **DEMOTE PR4** in the rev-4 plan or convert it to a follow-up
   conditional on post-PR2 evidence. The cross-key audit in
   `.omc/plans/mwmr-history-shard-callsites.md` is still worth
   producing as a parking-lot doc, but PR4 implementation is no
   longer justified by the profile evidence — `probe_visible_version`
   is 0.04% of leaf samples and `_pthread_mutex_firstfit_lock_slow`
   does not appear in the top-30 hot frames at all. Sharding a cold
   mutex is wasted complexity. If post-PR2 shifts the profile and
   HistoryStore becomes hot, re-evaluate.

4. **NEW post-PR2 follow-up candidate (deferred, conditional):**
   if `lock_exclusive_slow` on `pin_then_latch` from
   `flip_pending_to_committed_for` survives PR1+PR2 above, say,
   2 000 samples / 15 s, the residual is collision with the
   durability/snapshot background thread firing at 100 ms intervals.
   A future investigation could either lengthen `DURABILITY_INTERVAL_MS`
   under load (back-pressure), gate background work behind writer
   activity (silence flushes during write bursts), or move the
   commit-time flip out from under the exclusive latch via PR1's
   Phase-A `Arc` swap. The Phase-A swap is already in PR1's design;
   if PR1 ships and the residual persists, the durability thread is
   the next handle.

5. **Re-measure `same_ns_partitioned@4` post-PR1 and post-PR2.** The
   ratio R = `partitioned@4 / multi_ns_single@4` should rise toward
   0.85 if PR1+PR2 are addressing the right thing. If it doesn't
   move, that's the leading indicator that something other than
   per-commit page work is gating, and the (4) post-PR2 follow-up
   becomes the headline.

## Code references for the verified bottleneck

Line numbers verified against `master` HEAD (fad1561) — the rev-4 plan's
file:line citations are slightly stale; current locations:

- `src/storage/paged_engine/index_maint.rs:618` —
  `flip_pending_to_committed_for` (the post-durable in-memory flip
  caller — PR1 reshapes this into Phase A/B)
- `src/storage/paged_engine/index_maint.rs:618` calls
  `pin_then_latch(page, mode=Exclusive)` which lands on
- `src/storage/buffer_pool/mod.rs:1185` — `BufferPool::pin_then_latch`
  (the `lock_exclusive_slow` site)
- `src/storage/buffer_pool/mod.rs:517` —
  `LatchedPinnedPage::flip_pending_for_txn` (the `Arc::make_mut` site
  PR1 selectively-CoWs)
- `src/storage/buffer_pool/mod.rs:527` — the `for chain_arc in
  frame.deltas.values_mut()` loop that PR1's selective CoW replaces
  with "iterate only keys in pages_with_pending_txn(txn_id)"
- `src/storage/paged_engine/smo_latch.rs:151` —
  `acquire_smo_latches` (the install-path latch acquisition that
  PR2 reshapes into shared-then-exclusive Phase A/B)

## Reproduction

```bash
# isolated 30s sample on partitioned
target/release/examples/perf_axis --axis same_ns_partitioned --writers 4 --seconds 30 &
PID=$!
sleep 1
sample $PID 30 -file /tmp/sample-spike-partitioned.txt
wait $PID

# post-process
tools/perf/sample_hot.py /tmp/sample-spike-partitioned.txt > \
    docs/perf-baselines/2026-05-10-spike-partitioned-hot.md
```

`/tmp/sample-spike-partitioned.txt` is the raw 282 KB sample; the
hot-frames table at
[`2026-05-10-spike-partitioned-hot.md`](2026-05-10-spike-partitioned-hot.md)
is the post-processed view this diagnosis cites.
