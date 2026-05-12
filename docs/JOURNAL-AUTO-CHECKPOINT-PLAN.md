# Journal Auto-Checkpoint Plan

`OpenOptions` already exposes `journal_auto_checkpoint` with a default of 1000
pages and `journal_max_size` with a default of 100 MB. The open path does not
yet route those values into `PagedEngine`, so mqlite currently relies on
explicit `checkpoint()`, `close()`, last-handle drop, and emergency paths.

## Goal

Wire a page-count checkpoint trigger that is SQLite-WAL-like in shape without
changing the durability mode contract. Durability continues to be controlled by
`DurabilityMode`; auto-checkpointing controls how much committed journal tail is
folded back into the main file.

## Implementation Plan

1. Route `journal_auto_checkpoint` and `journal_max_size` from `OpenOptions`
   through `Client::open_with_options` into `PagedEngine`.
2. Track a cheap post-commit journal growth signal in pages, not documents.
   The trigger should use journal or dirty-frame page counts so batch size and
   payload size affect it naturally.
3. After publish, check whether either threshold is exceeded. If not, return on
   the write path without additional work.
4. When a threshold is exceeded, request one checkpoint owner. Avoid letting
   every writer that observes the threshold run checkpoint work.
5. Run checkpoint through the existing checkpoint gate so new writers are
   blocked and active writers drain consistently.
6. Preserve the existing checkpoint-incomplete behavior. If a checkpoint cannot
   advance because of pinned or non-installable state, surface or record that
   failure without corrupting the engine.
7. Add tests for threshold routing, single-owner behavior, disabled or tiny
   thresholds, max-size fallback, and recovery after a threshold checkpoint.
8. Add performance coverage for interval durability with and without
   auto-checkpoint pressure.

## Acceptance Criteria

- `OpenOptions::journal_auto_checkpoint(1000)` changes runtime behavior.
- A threshold crossing causes at most one checkpoint attempt per observed
  frontier.
- Checkpoint attempts use the same correctness path as explicit
  `Client::checkpoint()`.
- `journal_auto_checkpoint(0)` is either rejected or documented as disabled.
- Benchmarks report the auto-checkpoint threshold alongside durability.
