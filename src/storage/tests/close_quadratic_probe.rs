//! #18 RESIDUAL-n² PROBE — test-only close-time checkpoint counters.
//!
//! After the O(n²) close bug (BUG-CLOSE: `ChainSnapshot::new` deep-cloning the
//! resident delta map per structural rebuild read) was killed via chain-free
//! reads, a SECOND, ~60× smaller quadratic term remained in close-time
//! checkpoint (fits `0.0038s × (docs/1k)²`). These counters instrument the
//! candidate quadratic sites in the checkpoint materialize → B-tree rebuild →
//! leaf-split → chain-migration path so a two-scale harness can compute growth
//! ratios: a linear counter ~doubles when docs double, the quadratic culprit
//! ~quadruples.
//!
//! Thread-local (like `spill_flush_observations`): the close-time checkpoint
//! runs entirely on the thread that drops the last `Client` handle, so one
//! test's counts can never bleed into a concurrently running test.
//!
//! USAGE (see `tests/close_quadratic_probe_harness.rs`, `#[ignore]`d):
//!   1. bulk-insert N docs,
//!   2. `reset_all()` immediately before `drop(client)`,
//!   3. read the counters immediately after the drop returns,
//!   4. run at two scales and divide.
//!
//! These counters are `cfg(any(test, feature = "test-hooks"))` only — zero
//! release impact (the `record_*` calls at the probed sites are behind the same
//! cfg).

#![cfg(any(test, feature = "test-hooks"))]
// The reset/get/snapshot reader surface is consumed only by the cfg(test)
// harness (`close_quadratic_probe_harness`); under `feature = "test-hooks"`
// alone (no cfg(test)) only the `record_*` writers at the probed sites are
// reachable. Mirror `spill_flush_observations`: compile the full surface under
// test-hooks for parity and silence the reader-only dead-code lint.
#![allow(
    dead_code,
    reason = "reader surface read only by cfg(test) suites; compiled under test-hooks for parity"
)]

use std::cell::Cell;

thread_local! {
    /// `apply_*_checkpoint_delta` invocations (one per visible delta folded).
    /// Expected LINEAR (~n): the materialize loop iterates collected deltas once.
    static MATERIALIZE_DELTA_OPS: Cell<u64> = const { Cell::new(0) };

    /// Internal-node reads during root-to-leaf descents in the rebuild
    /// (`insert_subtree` / `find_leaf`). Expected ~n·log(n): linear ops × depth.
    static DESCENT_INTERNAL_READS: Cell<u64> = const { Cell::new(0) };

    /// `split_leaf` invocations during the rebuild. Expected ~linear in pages
    /// produced (≈ n / cells-per-leaf).
    static LEAF_SPLITS: Cell<u64> = const { Cell::new(0) };

    /// Total cells parsed across every `LeafNode::parse` in the rebuild.
    /// QUADRATIC if the rebuild re-parses a growing leaf on every fold before
    /// it splits (each parse is O(cells-on-leaf)).
    static LEAF_CELLS_PARSED: Cell<u64> = const { Cell::new(0) };

    /// Total chains DRAINED across every chain-migration helper
    /// (`partition_chains_for_split` / `move_all_leaf_chains` /
    /// `redistribute_leaf_chains`) — i.e. sum over all
    /// `with_all_chains_under_latch` drains of the BTreeMap entry count.
    /// PRIME QUADRATIC SUSPECT: every leaf split during the primary/secondary
    /// rebuild drains the FULL still-resident chain map of the source leaf
    /// (chain-free reads skip chains on READ but not on split-time DRAIN), so
    /// repeated splits of an accumulating leaf re-drain O(remaining) chains.
    static CHAIN_DRAIN_ENTRIES: Cell<u64> = const { Cell::new(0) };

    /// Total chains RE-HOMED across every chain-migration helper (one
    /// `with_chain_under_latch` per drained chain). Tracks `CHAIN_DRAIN_ENTRIES`
    /// but counts the per-chain re-home (pin + frame mutate) cost separately.
    static CHAIN_REHOME_OPS: Cell<u64> = const { Cell::new(0) };

    /// Calls to `with_all_chains_under_latch` (number of drain operations, not
    /// entries). Expected ~ number of structural mutations touching chains.
    static CHAIN_DRAIN_CALLS: Cell<u64> = const { Cell::new(0) };
}

macro_rules! probe_counter {
    ($cell:ident, $reset:ident, $get:ident, $record:ident) => {
        pub(crate) fn $reset() {
            $cell.with(|c| c.set(0));
        }
        pub(crate) fn $get() -> u64 {
            $cell.with(|c| c.get())
        }
        pub(crate) fn $record(n: u64) {
            $cell.with(|c| c.set(c.get() + n));
        }
    };
}

probe_counter!(
    MATERIALIZE_DELTA_OPS,
    reset_materialize_delta_ops,
    materialize_delta_ops,
    record_materialize_delta_ops
);
probe_counter!(
    DESCENT_INTERNAL_READS,
    reset_descent_internal_reads,
    descent_internal_reads,
    record_descent_internal_reads
);
probe_counter!(LEAF_SPLITS, reset_leaf_splits, leaf_splits, record_leaf_splits);
probe_counter!(
    LEAF_CELLS_PARSED,
    reset_leaf_cells_parsed,
    leaf_cells_parsed,
    record_leaf_cells_parsed
);
probe_counter!(
    CHAIN_DRAIN_ENTRIES,
    reset_chain_drain_entries,
    chain_drain_entries,
    record_chain_drain_entries
);
probe_counter!(
    CHAIN_REHOME_OPS,
    reset_chain_rehome_ops,
    chain_rehome_ops,
    record_chain_rehome_ops
);
probe_counter!(
    CHAIN_DRAIN_CALLS,
    reset_chain_drain_calls,
    chain_drain_calls,
    record_chain_drain_calls
);

/// Reset every probe counter (call immediately before the timed close).
pub(crate) fn reset_all() {
    reset_materialize_delta_ops();
    reset_descent_internal_reads();
    reset_leaf_splits();
    reset_leaf_cells_parsed();
    reset_chain_drain_entries();
    reset_chain_rehome_ops();
    reset_chain_drain_calls();
}

/// Snapshot of every probe counter for a single scale.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ProbeSnapshot {
    pub(crate) materialize_delta_ops: u64,
    pub(crate) descent_internal_reads: u64,
    pub(crate) leaf_splits: u64,
    pub(crate) leaf_cells_parsed: u64,
    pub(crate) chain_drain_entries: u64,
    pub(crate) chain_rehome_ops: u64,
    pub(crate) chain_drain_calls: u64,
}

/// Snapshot every probe counter (call immediately after the timed close).
pub(crate) fn snapshot() -> ProbeSnapshot {
    ProbeSnapshot {
        materialize_delta_ops: materialize_delta_ops(),
        descent_internal_reads: descent_internal_reads(),
        leaf_splits: leaf_splits(),
        leaf_cells_parsed: leaf_cells_parsed(),
        chain_drain_entries: chain_drain_entries(),
        chain_rehome_ops: chain_rehome_ops(),
        chain_drain_calls: chain_drain_calls(),
    }
}
