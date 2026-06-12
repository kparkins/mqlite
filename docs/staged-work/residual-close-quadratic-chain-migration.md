# Residual O(n²) close-time checkpoint: split-time chain re-migration

Status: **documented, not fixed**. This is the SECOND quadratic term in
close-time checkpoint, ~60× smaller than the original BUG-CLOSE bug (see the
BUG-CLOSE entry in `.omc/plans/deep-refactor-2026-06-10.md`). The contract
tests `tests/close_checkpoint_bounded.rs` /
`tests/close_checkpoint_bounded_secondary.rs` pass ~9× under their 20 s bound
at 20 k docs, so this term is a future scaling target, NOT a live regression. A
fix is **structural** (it reworks the materialize-fold ↔ chain-migration
interaction, a correctness-load-bearing hot path) and therefore must land
behind its own failing-first test + critic treatment; it is out of scope for
the cheap-fix rule that gated the chain-free-reads BUG-CLOSE fix.

## (a) Where it lives

`src/storage/paged_engine/checkpoint_materialize.rs`:
`materialize_primary_deltas_for_checkpoint` and
`materialize_ready_secondary_deltas_for_checkpoint`. Each folds the visible
resident deltas of a dirty tree into base pages by replaying them as single-key
`BTree::insert` / `replace_existing` / `delete` ops through the chain-free
structural store (`new_structural_store_chain_free`). Every time one of those
inserts fills a leaf, `BTree::split_leaf` (`src/storage/btree/insert.rs`) calls
`partition_chains_for_split` (`src/storage/btree/chain_migration.rs`), which
**drains the entire still-resident MVCC delta map of the splitting leaf** via
`with_all_chains_under_latch` (`std::mem::take` of the frame's `BTreeMap`) and
**re-homes each chain** onto its post-split destination page via a per-chain
`with_chain_under_latch` (a `pin_then_latch` + frame mutate).

## (b) The mechanism: why it is O(n²)

Ordinary CRUD never splits a base leaf; it accumulates every committed version
as a resident delta chain on a base-empty leaf (this is the deferred-split
design the original BUG-CLOSE clone amplified). So at checkpoint, ONE base leaf
carries the resident chains for ~all `n` keys of its tree.

The materialize loop folds the `n` deltas in **sorted key order** (they come
out of `visible_delta_entries`, a left-to-right leaf walk). It inserts them into
the chain-free structural tree one at a time. As the leftmost active leaf fills
(~110 cells of 256 B payload per 32 KB leaf) it splits, and
`partition_chains_for_split` drains **every chain still resident on that leaf**,
which (because chains only leave a leaf when that leaf itself splits) is still
proportional to the keys not yet pushed past it. Each chain is therefore
drained-and-re-homed once per split of every leaf it transits before reaching
its final leaf, and the sorted-order fold makes the chains pile onto the
leftmost active leaf, so early chains transit ~`n / 110` splits.

Summed over all splits the total drain + re-home work is

```
Σ (entries drained per split) ≈ n²/110   (drain ops + equal re-home ops)
```

The chain-free-reads fix killed the per-READ clone but left the per-SPLIT
**drain + re-home** in place, and that drain is over the same full resident
chain map.

## (c) Counter evidence (test-hooks probe)

Probe module `src/storage/close_quadratic_probe.rs`; two-scale `#[ignore]`d
harness `src/storage/paged_engine/tests/close_quadratic_probe_harness.rs`
(`close_quadratic_probe_harness::residual_close_quadratic_counter_growth*`).
Counters are reset immediately before `drop(client)` and snapshotted
immediately after, so they capture only the close-time checkpoint window. Scale
is doubled (10 k → 20 k), so ratio ~2.0 ⇒ linear, ~4.0 ⇒ quadratic.

PRIMARY close (single namespace), close wall 0.74 s → 2.43 s:

| counter                 | @10 000   | @20 000   | ratio | verdict     |
|-------------------------|-----------|-----------|-------|-------------|
| materialize_delta_ops   | 10 000    | 20 000    | 2.00  | linear      |
| descent_internal_reads  | 23 292    | 63 292    | 2.72  | n·log n     |
| leaf_splits             | 207       | 415       | 2.00  | linear      |
| leaf_cells_parsed       | 1 444 758 | 2 894 726 | 2.00  | linear      |
| chain_drain_calls       | 207       | 415       | 2.00  | linear      |
| **chain_drain_entries** | 1 046 592 | 4 176 560 | **3.99** | **QUADRATIC** |
| **chain_rehome_ops**    | 1 046 592 | 4 176 560 | **3.99** | **QUADRATIC** |

SECONDARY close (one `{seq:1}` index), close wall 1.21 s → 3.50 s:

| counter                 | @10 000    | @20 000    | ratio | verdict     |
|-------------------------|------------|------------|-------|-------------|
| materialize_delta_ops   | 20 000     | 40 000     | 2.00  | linear      |
| descent_internal_reads  | 32 780     | 82 780     | 2.53  | n·log n     |
| leaf_splits             | 245        | 492        | 2.01  | linear      |
| leaf_cells_parsed       | 5 212 310  | 10 495 614 | 2.01  | linear      |
| chain_drain_calls       | 245        | 492        | 2.01  | linear      |
| **chain_drain_entries** | 1 246 624  | 4 967 504  | **3.98** | **QUADRATIC** |
| **chain_rehome_ops**    | 1 246 624  | 4 967 504  | **3.98** | **QUADRATIC** |

The split COUNT is linear (`chain_drain_calls` ~2×) but the entries drained PER
split grow linearly with `n` (avg ≈ 5 057 → 10 063 primary), so the product is
quadratic. Every other instrumented site is linear or n·log n; this is the
sole quadratic term.

## (d) Why a naive "skip the drain" is WRONG (the load-bearing constraint)

Unlike the BUG-CLOSE clone (which the rebuild always discarded, pure dead
work), the split-time chain migration is **correctness-load-bearing**:

1. The re-homed chains are real committed-but-uncheckpointed versions. A held
   `ReadView` older than checkpoint may still need them (exactly the BUG-7
   snapshot-isolation hazard that `stage_checkpoint_pre_mutation`
   (`src/storage/paged_engine/snapshot_ops/checkpoint.rs`) spills/relieves
   around). They must end up on the correct post-split leaf so the subsequent
   reconcile/relief/residual passes and `clear_materialized_chains` find them
   on the right page.
2. A materialize failure that migrated chains MUST poison-and-reopen
   (`StructuralPageBatch::migrated_chains` → `CheckpointPostMutationFailure`);
   the migration is not staged copy-on-write, so an abort that freed the
   destination page would silently lose the chains.

So the chains cannot simply be dropped or left on the source leaf; they must be
routed to their final leaf. The inefficiency is routing each chain **repeatedly**
(once per transited split), not routing them at all.

## (e) Fix sketch (structural; needs failing test first)

Any of the following removes the re-migration without losing the routing
guarantee; each is its own benchmarked, failing-test-first change with critic
treatment because it touches the checkpoint-materialize ↔ chain-migration hot
interaction:

- **Drain once, route to final leaf.** Drain the source base leaf's full chain
  map up front (one `std::mem::take`), bulk-build the rebuilt leaf level from the
  sorted deltas so the final key→leaf partition is known, then re-home each
  chain directly to its final leaf exactly once. Total chain work → O(n).
  Requires teaching the materialize fold to produce the final leaf layout before
  placing chains (a sorted-bulk-load instead of repeated single-key
  insert-and-split).
- **Defer migration to a single post-fold pass.** Let `split_leaf` skip
  `partition_chains_for_split` when driven by the chain-free structural store
  (a flag analogous to `chain_free_reads`), then after the whole tree is rebuilt
  walk the source leaf's resident chains once and route each to the leaf the new
  separators assign it. Must preserve the `migrated_chains` poison-on-abort
  signal for the single post-fold pass.
- **Sorted-bulk-load the leaf level.** Replace the per-delta `insert` loop with a
  bottom-up bulk loader (deltas are already sorted), packing full leaves and
  building internals in one O(n) pass, then attach chains to final leaves once.

All three need a failing perf/counter assertion (e.g. a `chain_rehome_ops`
bound in the `#[ignore]`d harness, or a wall-clock bound at a larger scale than
the 20 s contract test) demonstrating the quadratic BEFORE the change, plus the
MWMR A/B discipline since they touch checkpoint structure.

## (f) Trigger threshold

Fit `t ≈ 0.0038 s × (docs/1k)²` (close-window wall, single namespace, 256 B
payload, this box). Observed close wall: 20 k → 2.4 s, 40 k → 6.8 s, 80 k →
24.4 s. Extrapolation: ~64 min at 1 M docs/namespace; `chain_rehome_ops` at 1 M
≈ 4.18 M × (1000/20)² ≈ 1.0 × 10¹⁰ pin+latch ops. The current contract bound
(20 k docs < 20 s) is ~9× clear. **Schedule the fix when any single-namespace
checkpoint is expected to exceed ~200 k-400 k uncheckpointed docs** (close
wall ~0.15 s → ~0.6 s and climbing), or sooner if a workload bulk-loads a
namespace and closes without an intervening checkpoint to drain the resident
deltas.

## (g) Probe lifecycle

The probe counters in `src/storage/close_quadratic_probe.rs` are
`cfg(any(test, feature = "test-hooks"))`-only (zero release impact; the
`record_*` calls at the probed sites carry the same cfg) and are retained per
the precedent of `spill_flush_observations` so the quadratic stays
re-measurable. The `#[ignore]`d harness documents how to read them
(reset-before-close, snapshot-after-close, divide at two scales). Re-run after
any candidate fix: a successful fix drops `chain_drain_entries` /
`chain_rehome_ops` to a ~2.0 ratio.
