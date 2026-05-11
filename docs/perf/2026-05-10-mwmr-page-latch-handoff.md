# MWMR Page-Latch Plan — Execution Handoff

**Date:** 2026-05-10 → 2026-05-11
**Plan:** `.omc/plans/mwmr-page-latch.md` (rev 4, status: approved 2026-05-09)
**State:** `master` is 18 commits ahead of `origin/master` at `12a77d7`.
The PR2 abort-cache follow-up fix and this handoff doc are local working-tree
changes. NOT pushed to origin.

## TL;DR

Original goal: lift `same_ns_single@4` from 3,021 dps (historical) to ≥5,000 dps sustained, stretch ≥7,500.

Result: **4,166 → 7,793 dps = 1.871× raw lift on real hardware (M3 Pro)**. Both floor (5K, exceeded by 56%) and stretch (7.5K, exceeded by ~4%) goals hit.

All five plan PRs executed: PR0 baselines + spike, PR0.5 unification, PR1 selective CoW, PR2 running-sum cache. PR4 demoted to audit-only doc per spike verdict.

2026-05-11 follow-up: review found a PR2 correctness bug in the shared
Phase B swap path. `Pending -> Committed` swaps are byte-neutral, but
`Pending -> Aborted` swaps must subtract live-head bytes from the
`live_delta_payload_bytes` cache. The working tree now fixes
`try_swap_chains_if_unchanged` to apply per-key before/after byte deltas and
adds the regression test `phase_b_abort_swap_updates_cache`.

## Result table (median-of-11, 15s/run, M3 Pro)

| Axis | PR0 baseline | Post-PR1 | Post-PR2 | Total lift |
|---|---:|---:|---:|---:|
| `same_ns_single@4` | 4,166 | 7,373 | **7,793** | **1.871×** |
| `same_ns_partitioned@4` | 3,845 | 6,303 | 6,227 | 1.620× |
| `same_ns_batch@4` | 26,580 | 39,870 | 46,210 | 1.738× |
| `multi_ns_single@4` | 10,058 | 15,901 | 16,023 | 1.594× |
| `multi_ns_batch@8` | 126,701 | 149,591 | 163,902 | 1.294× |
| `read_find_one@1` | 951,550 | 919,261 | 946,708 | 0.995× |

## Causal evidence (Amdahl chain)

The work is defensible on physics, not just numbers:

- **Spike (R=0.38)** proved per-leaf contention is NOT the bottleneck — partitioning by `_id` across 4 writers in one B-tree achieves nothing (`same_ns_partitioned@4 = 3,845 < same_ns_single@4 = 4,166`). The hot symbols (`pin_then_latch`, `lock_exclusive_slow`, `Arc::make_mut`) PERSIST AND GET WORSE under partitioning, which means the per-commit cost is the binding constraint.
- **PR1 surgical metric**: `Arc::make_mut` in `flip_pending_to_committed_for` dropped from 4,093 samples to **11** (99.7% reduction). Selective-CoW invariant test directly proves only chains with `Pending(txn_id)` entries get cloned.
- **PR1 Amdahl reframe (AC #3 miss)**: `lock_exclusive_slow` went UP 17% wall-time / 36% per-insert because writers complete inserts 1.77× faster, contending for the exclusive Phase B latch more often per second. The "fail" was a measurement-domain confusion, not a real regression. The structural goal of moving `Arc::make_mut` out from under exclusive latch was achieved.
- **PR2 surgical metric (Amdahl falsification)**: The scanner micro-time (`live_delta_payload_exceeds_leaf_budget`) dropped from 693 ns to **23 ns** (96.7%). The macro install-section drop (1016 → 383 ns = 633 ns) is fully accounted for by the micro drop (670 ns). **Causal proof: scanner WAS the binding cost.**

## Commits landed on master (FF + 2 merge commits)

```
12a77d7 PR1 sidecar: append same-NS @8 rows + sort matrix
3437d07 Merge perf/p4-history-audit-doc into master
270b548 Merge perf/p0-spike-bottleneck into master
9cf7877 PR2 sidecar: full 11-row matrix + perf_axis tempdir cleanup
9be964f PR2: running-sum cache + delete legacy whole-frame scanner
835f883 PR1 sidecar: AC#3 falsification clause + broad-spectrum callout
e27a03b PR1: selective CoW + Phase A/B split + bounded retry → EngineFatal
07daef1 PR0.5 commit 3/3: delete legacy chain mutators + invariant test
4c0ccc9 PR0.5 commit 2/3: route every chain mutation through the latched API
ed93a59 PR0.5 commit 1/3: spike with_chain_under_latch API + 3 callsites
b0daeb0 docs(perf): relocate history-store shard audit out of gitignored .omc/
06284fc perf(plan): add PR4 history-shard cross-key audit (parking-lot ref)
6f6e343 perf(spike): diagnose R=0.38 escalation
95203e3 perf(tooling): lock in apples-to-apples runner contract for PR1+
536efe5 perf(baselines): record 2026-05-10 pre-r1 median-of-11 matrix + R=0.38 FAIL
de38604 perf(tooling): skip Client::drop in perf_axis to keep runner wall-time tractable
4910546 perf(baselines): capture 2026-05-10 pre-r1 sample profile
9ef0553 perf(tooling): add PR0 baseline infrastructure (schema, runner, --writers flag)
fad1561 perf(tooling): add perf_axis tight-loop profiling harness  ← pre-plan baseline
```

**18 commits ahead of `origin/master`. Local only. NOT pushed.** The
abort-cache fix is not in that count yet; it is still a working-tree follow-up.

## Files added/touched (since pre-plan baseline `fad1561`)

- Committed chain: 60 files changed, +5,378 / -551 LOC net
- Working-tree follow-up: `src/storage/buffer_pool/mod.rs`,
  `src/storage/buffer_pool/tests/running_sum_cache_invariant.rs`, and this
  handoff doc
- Benchmark tools consolidated under `benches/perf/`
- New tests: `tests/perf_baseline_schema.rs`, `src/storage/buffer_pool/tests/running_sum_cache_invariant.rs`, `src/storage/paged_engine/tests/flip_committed_concurrent_observers.rs`
- New module: `src/storage/buffer_pool/metrics_perf.rs`
- New cargo feature: `perf-counters = ["dep:hdrhistogram"]`
- New optional dep: `hdrhistogram = "7"` (gated by `perf-counters`)

## Documentation artifacts (under `docs/`)

| Path | Purpose |
|---|---|
| `docs/perf-baselines/2026-05-10-pre-r1.{md,json}` | PR0 median-of-11 baseline + sample hot frames |
| `docs/perf-baselines/2026-05-10-pre-r1-hot.md` | PR0 sample profile top frames |
| `docs/perf-baselines/2026-05-10-spike-bottleneck-diagnosis.md` | Spike #6 diagnosis (R=0.38 verdict) |
| `docs/perf-baselines/2026-05-10-spike-partitioned-hot.md` | Spike sample on `same_ns_partitioned@4` |
| `docs/perf-baselines/2026-05-10-post-pr1.{md,json}` | PR1 verification matrix + AC#3 reframe |
| `docs/perf-baselines/2026-05-10-post-pr2.{md,json}` | PR2 final 11-row matrix + Amdahl confirmation |
| `docs/perf-baselines/SCHEMA.md` | JSON sidecar schema |
| `docs/perf/2026-05-10-history-store-shard-audit.md` | PR4 cross-key audit (parking-lot reference) |
| `docs/perf/2026-05-10-mwmr-page-latch-handoff.md` | This updated local handoff document |
| `.omc/plans/mwmr-page-latch.md` | The plan itself (rev 4, gitignored) |

## What's NOT done (intentional / deferred)

### 2026-05-11 follow-up fix

- **Fixed in working tree:** `try_swap_chains_if_unchanged` now maintains
  `live_delta_payload_bytes` by applying each prepared swap's
  `chain_live_head_bytes(before) -> chain_live_head_bytes(after)` delta.
  This preserves the fast commit path while making abort cleanup correct.
- **New regression:** `phase_b_abort_swap_updates_cache` installs pending
  insert heads, swaps them to `Aborted`, and asserts the cache drops to 0 and
  still equals a fresh recompute.
- **Targeted validation passed:**
  - `env -u RUSTC_WRAPPER cargo test phase_b_abort_swap_updates_cache -- --nocapture`
  - `env -u RUSTC_WRAPPER cargo test --lib running_sum_cache_invariant -- --nocapture`
  - `env -u RUSTC_WRAPPER cargo test --features test-hooks --test phase8_journal_group_commit pre_reservation_failure_writes_no_complete_record -- --nocapture`
- **Still recommended before push:** run the full release gate again after
  committing the follow-up.

### Deferred follow-ups (NEW planning units, post-PR2)

1. **Durability-thread interaction** with the per-write exclusive latch (spike's deferred follow-up #5). PR2 collapsed the only fragment the running-sum cache could touch; remaining per-insert headroom is in B-tree pathing, latch acquire/release, and journal append. The durability thread firing every 100ms creates exclusive-latch contention regardless of PR1+PR2 fixes. This is the next planning unit if more lift is wanted.

2. **PR4 HistoryStore sharding** (parked). Re-eval triggers documented in `docs/perf/2026-05-10-history-store-shard-audit.md` §6:
   - `_pthread_mutex_firstfit_lock_slow` on `PrimaryHistoryProbe` reaching top-30 self-time on a future `sample_hot.py` output, OR
   - combined `probe_visible_version` self-time crossing 5% of `run_write_commit_envelope` self-time (vs spike's 0.04%)
   - Cancel-only trigger: `mwmr-bw-tree-leaves` ships first → PR4 superseded.

3. **R = `partitioned/multi_ns` ceiling** stayed flat at ~0.38–0.40. PR1+PR2 lifted both legs proportionally; the ratio shape didn't change. Breaking R > 0.5 requires attacking the still-shared per-namespace serialization (likely durability-thread interaction).

4. **`mwmr-pin-fastpath`** — was originally PR3 in plan rev 4. Dropped during ralplan iteration after Architect+Critic flagged it as a sympathy-fix risk (`parking_lot::try_lock_*` falls through to `lock_*` on contention which goes to back of queue). Filed as separate exploratory follow-up.

5. **`mwmr-bw-tree-leaves`** — long-term latch-free leaf design. Considered and invalidated for the rev-4 timeline (WAL/recovery/checkpoint blast-radius too large). Re-open trigger: post-PR2 evidence that B-tree internal-node latches dominate.

### Plan rev 4 ADR consequences

- Pre-release status (per `project_prerelease_status.md` memory): format/wire-protocol changes were free; no backwards-compat shims introduced.
- Principle 3 enforced: every PR ended with strictly fewer write code paths.
- One Architect/Critic note left in the plan body about line-number drift (rev 4 referenced `flip_pending_to_committed_for` at `:585`; spike re-anchored to `:618` then PR1 shifted to `:622`). Line numbers in the plan are now stale post-merge — not worth re-anchoring since the plan is closed.

## How to verify the merged state

```bash
# Build
cargo build --release --tests

# Test (PR2 baseline was 1078 passing; follow-up adds one unit test)
cargo test --release --tests

# Focused PR2 abort-cache regression/fix
env -u RUSTC_WRAPPER cargo test --lib running_sum_cache_invariant -- --nocapture
env -u RUSTC_WRAPPER cargo test --features test-hooks --test phase8_journal_group_commit \
    pre_reservation_failure_writes_no_complete_record -- --nocapture

# Current consolidated benchmark smoke
cargo build --release --bin perf_matrix
target/release/perf_matrix --list-axes
target/release/perf_matrix \
    --axis multi_writer_single_ns_single \
    --writers 4 \
    --docs-per-writer 20000 \
    --batch-size 100

# Reproduce current fixed-count median sidecar
benches/perf/run_baselines.py \
    --out /tmp/verify-post-pr2.json \
    --branch master \
    --runs 11 \
    --axis multi_writer_single_ns_single@4

# Historical PR2 numbers were captured with the deleted perf_axis duration
# runner. New captures use fixed prebuilt documents and should not be compared
# directly without rebaselining.

# Reproduce perf-counter validation on the consolidated harness
cargo build --release --features perf-counters --bin perf_matrix
target/release/perf_matrix \
    --axis multi_writer_single_ns_single \
    --writers 4 \
    --docs-per-writer 20000
# Expected JSON output:
#   install_phase_b_mean_hold_ns: ~383 (vs PR1 baseline 1016)
#   live_delta_check_mean_hold_ns: ~23 (vs PR1 baseline 693)
#   flip_retry_rate: 0.000000
#   flip_retry_exhausted: 0
```

## Push / next steps decision points

User has these choices:

1. **Commit the follow-up fix, rerun the full release gate, then push.** Master is 18 commits ahead before the follow-up commit. The merge commits at `270b548` (spike) and `3437d07` (audit) preserve branch context for archeology.
2. **Open PRs on GitHub** (one per branch: PR0, PR0.5, PR1, PR2 chain + spike + audit). More review-friendly but redundant since work is already merged locally.
3. **Squash-merge for cleaner main history** before push. Each PR becomes one commit on master. Loses per-commit granularity (especially valuable in PR0.5's 3-commit structure) but is more conventional.
4. **Hold local indefinitely.** Continue experimentation; merge to remote when ready.

Recommendation: **(1) commit the follow-up fix, run the full release gate, then push**. The per-commit history captures real engineering judgment (the spike protocol, the AC#3 reframe, the metric ambiguity stop) that's worth preserving. The two merge commits make it clear which work was the perf chain vs sidecar diagnosis vs audit doc, and the follow-up commit should make the abort-cache correction explicit.

## Branch cleanup

After deciding push strategy, the work branches can be deleted (still locally available since fully merged into master):

```bash
git branch -d perf/p0-baselines perf/p0-spike-bottleneck \
              perf/p05-unify-chain-mutators \
              perf/p1-flip-hoist-cow perf/p2-running-sum-cache \
              perf/p4-history-audit-doc

# Their worktrees can also be removed:
git worktree remove .claude/worktrees/perf-worker-pr0
git worktree remove .claude/worktrees/perf-pr05
git worktree remove .claude/worktrees/perf-pr1
git worktree remove .claude/worktrees/perf-pr2
git worktree remove .claude/worktrees/perf-spike
```

Branches `perf/r1-find-one-limit` and `perf/w1-unify-journal-allocation` are pre-plan branches whose work is also already merged; same cleanup applies.

## Process notes (for future plan executions)

What worked:
- **Design-proposal-before-code** for PR1 and PR2 caught two real spec ambiguities (post-durable EngineFatal contract; Phase B metric definition) before any wasted implementation.
- **Spike before commit** when AC #3 missed (PR2's metric ambiguity stop) prevented committing measurements that didn't answer the question.
- **Median-of-11 with 5% noise envelope** caught real variance issues; the runner's `(max-min)/median` rejection was load-bearing for AC #6 verification.
- **`with_chain_under_latch` unification (PR0.5)** was the single most important architectural change — it created a choke point that PR1's Phase A/B and PR2's running-sum cache both depend on.

What slipped:
- Worker initially went silent for 80 min on PR2 implementation (cadence drift). Recovered after status-ping. Lesson: enforce 15-20 min ping cadence on long PRs.
- Worker patched `perf_axis` (option-c `std::process::exit(0)`) without re-asking when new wall-time data emerged. Recoverable but a real protocol slip — fixed by reframing the ask.
- The `std::process::exit(0)` runner bypass leaked TempDir cleanup, accumulating 54 GB across multi-day sessions. Fixed in the PR2 sidecar follow-up commit (`9cf7877`).
- PR2 overgeneralized the Phase B cache invariant from `Pending -> Committed`
  to the shared abort path. `Pending -> Aborted` changes live-head byte
  contribution and needed an explicit regression plus cache delta update.

What was preserved:
- All architectural decisions code-grounded with file:line citations
- All AC measurements include both numbers (target + measured) and verdict
- All "stop and report" protocol decisions documented inline
- All deferred follow-ups have explicit re-eval triggers, not vague "if it gets worse"
