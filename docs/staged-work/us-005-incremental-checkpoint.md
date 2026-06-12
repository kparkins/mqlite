# US-005: Incremental per-page checkpoint (staged producer)

Status: **quarantined**. Compiled only under `cfg(test)` (so the test fixtures
that exercise the recovery consumer stay live) and under the
`us005-incremental-checkpoint` Cargo feature (off by default). Production builds
pay zero cost and carry none of this surface.

Each quarantined item carries a one-line `QUARANTINED dormant US-005 producer`
marker comment that points back to this document.

## (a) What production checkpoints actually do

The live checkpoint path (`src/storage/paged_engine/snapshot_ops/checkpoint.rs`,
~:560-621) **materializes folded leaf images directly into the main database
file** and emits a **single inline `CheckpointBoundary` record** at the end of
the checkpoint. It does **not** emit per-page `CheckpointPageFrame` records into
the journal. Production therefore never calls the per-page producer chain below;
it consumes only `consume_checkpoint_batch_id()` to stamp the boundary's batch id.

## (b) What the quarantined surface is

The quarantined producer is the **WiredTiger-style incremental per-page
checkpoint PRODUCER**: instead of writing folded leaves straight to the main
file, it would append each dirty page as a `CheckpointPageFrame` log record
inside a fenced checkpoint batch (`begin_checkpoint_batch` →
`append_checkpoint_page_frame`* → `append_checkpoint_commit_boundary`), then let
a driver replay/materialize those frames. That driver was **never built**, so
the producer has no live caller and was staged ahead of it.

Quarantined items (gated, not deleted):

- `src/storage/handle.rs`: `journal_page_size`, `validate_dirty_subset`,
  `next_checkpoint_batch_id`, `flush_journal_durable`,
  `validate_checkpoint_flush_set`.
- `src/journal/mod.rs`: `next_checkpoint_batch_id` (method),
  `begin_checkpoint_batch`, `abort_empty_checkpoint_batch`,
  `append_checkpoint_page_frame`; plus `append_checkpoint_frame` and
  `append_checkpoint_commit_boundary` under the broader
  `cfg(any(test, feature = "test-hooks", feature = "us005-incremental-checkpoint"))`.
- `src/journal/checkpoint_batch.rs`: `BoundaryAppended`,
  `CheckpointBatchCursor`, `CheckpointFlushSet` (+`::new`), and
  `CheckpointBatchId::as_u64`.
- `src/storage/reconcile/driver.rs`: `CheckpointReconcilePlan::checkpoint_flush_set`.

Always-live (NOT gated): production checkpoint depends on them:

- `JournalManager::consume_checkpoint_batch_id` (`src/journal/mod.rs`) and
  `BufferPoolHandle::consume_checkpoint_batch_id` (`src/storage/handle.rs`).
- The `next_checkpoint_batch_id` **field** on `JournalManager`
  (`src/journal/mod.rs`).
- The `CheckpointBatchId` **struct** itself (see the field note below).

## (c) The recovery CONSUMER is live and tested

Recovery of `CheckpointPageFrame` / `CheckpointBoundary` records is **live**
(`src/journal/recovery.rs`): boundary collection, page-frame partitioning, batch
draining into the main file, and the orphan-page-frame truncation clamp
(`partition.orphan_truncate_lsn` vs `state.max_kept_record_end_lsn`) all run in
production. The `journal`, `recovery`, `checkpoint`, and `bug_recovery` test
suites pass against this consumer using the test fixtures
(`append_checkpoint_frame` / `append_checkpoint_commit_boundary`) to fabricate
its inputs. A future driver therefore needs only **producer + driver wiring**;
the durable-replay side already exists and is covered.

## (d) UNFIXED BLOCKER: concurrency wedge in `abort_empty_checkpoint_batch`

`abort_empty_checkpoint_batch` (`src/journal/mod.rs`) only clears
`checkpoint_batch_active` when its guard
`self.log_manager.next_lsn() == cursor.clean_start_offset` holds. But ordinary
CRUD reservations **bypass the outer `JournalManager` mutex by design**
(`reserve_log_record_on`, `&self`; see `src/journal/mod.rs` reserve path around
the `reserve_log_record` / `reserve_log_record_on` methods). If a concurrent
CRUD reservation advances the LSN frontier between `begin_checkpoint_batch` and
`abort_empty_checkpoint_batch`, the guard fails, the abort becomes a no-op, and
`checkpoint_batch_active` is **wedged forever**; every later
`begin_checkpoint_batch` then errors `"checkpoint batch already active"`.

Any future wiring work **must first land a failing concurrency test** that
reproduces this wedge (per the repo's failing-test-first rule) before changing
the abort/guard logic.

## (e) Why the test fixtures stay compiled under `cfg(test)`

`append_checkpoint_frame` and `append_checkpoint_commit_boundary` intentionally
remain compiled under `cfg(test)` (and `test-hooks`): they **fabricate the live
recovery consumer's inputs** (well-formed batches + boundaries) so the recovery
suites can prove replay/clamp behaviour without the never-built driver. They are
not dead; they are the consumer's test harness.

## Note: `checkpoint_batch_active` field left uncfg'd

The adjudication asked to gate the `checkpoint_batch_active` field with the
quarantine cfg *if it compiles cleanly in all configs*. It does **not**: the
field is initialized by the LIVE constructors `JournalManager::open_or_create`
and `JournalManager::recover_existing` (`src/journal/recovery.rs`), and its type
tuple `(CheckpointBatchId, JournalOffset)` forces `CheckpointBatchId` to remain
compiled in every config. Gating the field would require gating those
production constructors, which is impossible. The field is therefore left
uncfg'd; in default builds it is written `None` by the constructors and read by
no quarantined code, so it is benign. Only `CheckpointBatchId::as_u64` (called
solely by the quarantined producers) is gated, not the struct.
