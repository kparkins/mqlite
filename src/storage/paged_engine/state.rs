//! Shared + metadata state for the PagedEngine.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;

use crate::error::{Error, Result};
use crate::journal::log_file::{LogicalOpKind, LogicalTxnFrame};
use crate::journal::ParsedLogicalFrames;
use crate::mvcc::metrics::{
    record_logical_txn_pass2_resolved_op, record_logical_txn_pass2_unresolved_op,
};
use crate::mvcc::timestamp::TimestampOracle;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::{open_with_fallback as catalog_open_with_fallback, Catalog};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::history_store::HistoryStore;
use crate::storage::root_snapshot::ReadEpoch;

use super::catalog_ops::catalog_lock;
use super::publish::build_published_catalog;

// ---------------------------------------------------------------------------
// SharedState — fields shared by read path (no mutex) and writer (mutex held)
// ---------------------------------------------------------------------------

/// State shared by the read path (no mutex) and the writer inside
/// `Mutex<BpBackend>`.
pub(crate) struct SharedState {
    pub handle: Arc<BufferPoolHandle>,
    pub history_store: std::sync::Mutex<HistoryStore<BufferPoolPageStore>>,
    pub oracle: TimestampOracle,
    /// Atomically published read epoch for the mutex-free read path.
    /// Phase 1 §10.1: readers load a single `Arc<ReadEpoch>` and observe
    /// both `visible_ts` and `catalog` through the same guard.
    pub published: ArcSwap<ReadEpoch>,
    /// Monotonic transaction identifier source shared by readers and writers.
    pub txn_counter: AtomicU64,
    /// Monotonically-increasing generation counter for the published
    /// catalog. Advanced under `metadata.write()` alongside every
    /// `published_catalog_rebuild`; readers load with `Ordering::Acquire`
    /// WITHOUT holding `metadata.read()`. Phase 1 §10.1 / Phase 5
    /// §10.17.1 / §10.21 CV-5.
    pub catalog_gen: AtomicU64,
    /// §10.8 #19 publish-pause rendezvous hook. Per-engine (NOT
    /// process-global) so parallel tests using independent engines
    /// cannot consume each other's barriers. Under `#[cfg(test)]`
    /// only — production builds carry neither the `Mutex` nor the
    /// `Arc<Barrier>` (§11 #10: no new `Mutex` / `Arc` on commit path).
    #[cfg(test)]
    pub publish_pause_hook: std::sync::Mutex<Option<std::sync::Arc<std::sync::Barrier>>>,
}

impl SharedState {
    /// Centralized read-path load of the published epoch. In `#[cfg(test)]`
    /// builds this bumps `EPOCH_LOAD_COUNT` so `ReadOpScope` can detect
    /// any read operation that performs more than one load (Phase 1 §10.5 / US-008).
    ///
    /// NOTE: `publish_commit` (the write path's canonical helper, §10.2)
    /// invokes `self.published.load_full()` directly to observe the prior
    /// epoch for the strict-monotonicity debug_assert and for
    /// `Arc::clone` on epoch-only publishes. That load does NOT go
    /// through `load_published` and does NOT increment the read-path
    /// counter — Phase 1 §10.5 explicitly scopes the single-load gate
    /// to the read path.
    pub(crate) fn load_published(&self) -> arc_swap::Guard<Arc<ReadEpoch>> {
        #[cfg(test)]
        {
            EPOCH_LOAD_COUNT.with(|c| c.set(c.get() + 1));
        }
        self.published.load()
    }
}

// ---------------------------------------------------------------------------
// Test-only EPOCH_LOAD_COUNT + ReadOpScope (Phase 1 §10.5, US-008)
// ---------------------------------------------------------------------------

#[cfg(test)]
thread_local! {
    pub(crate) static EPOCH_LOAD_COUNT: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Test-only RAII guard that enforces the Phase 1 §10.5 single-load
/// discipline: every read-path entry point performs at most `limit`
/// calls to `SharedState::load_published`. Constructed at the top of
/// the test that drives a read; on `Drop` it asserts the observed
/// delta does not exceed the limit. Compound operations that
/// deliberately re-load (documented and rare) use `ReadOpScope::new(2)`
/// with an inline comment.
///
/// Gated under `#[cfg(test)]` so release builds carry no runtime cost.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct ReadOpScope {
    start: u32,
    limit: u32,
}

#[cfg(test)]
impl ReadOpScope {
    /// Begin a scope that tolerates up to `limit` epoch loads. Snapshots
    /// the thread-local `EPOCH_LOAD_COUNT` at construction.
    pub(crate) fn new(limit: u32) -> Self {
        let start = EPOCH_LOAD_COUNT.with(|c| c.get());
        Self { start, limit }
    }
}

#[cfg(test)]
impl Drop for ReadOpScope {
    fn drop(&mut self) {
        let end = EPOCH_LOAD_COUNT.with(|c| c.get());
        let delta = end.saturating_sub(self.start);
        assert!(
            delta <= self.limit,
            "operation performed {} epoch loads, limit {}",
            delta,
            self.limit
        );
    }
}

// ---------------------------------------------------------------------------
// MetadataState — catalog wrapped in metadata RwLock
// ---------------------------------------------------------------------------

/// Per-engine catalog state protected by an `RwLock`. DDL ops take the
/// write guard to gain exclusive access; CRUD writers take the read
/// guard (shared with other CRUD writers) and mutate the catalog via
/// the interior `Mutex<Catalog>`.
///
/// Lock order: `metadata` RwLock -> `ns_lanes` mutex -> `commit_seq`
/// mutex -> `catalog` Mutex. DO NOT grab `metadata.write()` while
/// holding the catalog mutex — that would invert the order relative to
/// a reader that already holds `metadata.read()` and is waiting for the
/// catalog mutex.
pub(crate) struct MetadataState {
    /// Catalog B+ tree for collection/index metadata.
    ///
    /// Wrapped in `Mutex` so CRUD writers can mutate under
    /// `metadata.read()` without upgrading to `write()`. DDL paths
    /// still take `metadata.write()` for coarse-grain CRUD-vs-DDL
    /// exclusion; they also briefly acquire this mutex, which is
    /// uncontended while no CRUD writer holds `metadata.read()`.
    pub catalog: std::sync::Mutex<Catalog<BufferPoolPageStore>>,
}

/// Phase 2 §5.2 Pass 2 — validate `ParsedLogicalFrames` against the live
/// catalog without mutating any durable state.
///
/// Per-op resolution taxonomy:
///   - `PrimaryInsert|PrimaryUpdate|PrimaryDelete` → `ns_id` must resolve
///     via `Catalog::find_collection_by_id`; a miss ticks the unresolved
///     counter.
///   - `SecondaryInsert|SecondaryDelete` → `index_id` must resolve via
///     `Catalog::find_index_by_id`; a miss ticks the unresolved counter.
///
/// Per-frame invariant: op ordinals MUST be dense `0..op_count-1` with
/// no gaps or duplicates. A violation is a Phase 2 invariant error
/// (Pass 1 should have already enforced this via the decoder, so
/// reaching this arm implies recovery-plus-catalog corruption).
///
/// Contract: the `&Catalog` receiver is the only durable-state access.
/// No mutation of the catalog tree, buffer pool, journal, HLC oracle,
/// or history store — the only observable side-effect is the Phase 2
/// `logical_txn_pass2_{resolved,unresolved}_ops_total` counters.
fn validate_parsed_logical_frames_against_catalog<S>(
    catalog: &Catalog<S>,
    parsed: &ParsedLogicalFrames,
) -> Result<()>
where
    S: crate::storage::btree::BTreePageStore,
{
    for (_offset, frame) in &parsed.frames {
        validate_frame_ordinals_dense(frame)?;
        for op in &frame.ops {
            match &op.kind {
                LogicalOpKind::PrimaryInsert { ns_id, .. }
                | LogicalOpKind::PrimaryUpdate { ns_id, .. }
                | LogicalOpKind::PrimaryDelete { ns_id, .. } => {
                    if catalog.find_collection_by_id(*ns_id)?.is_some() {
                        record_logical_txn_pass2_resolved_op();
                    } else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            target: "mqlite",
                            ns_id = *ns_id,
                            commit_ts = ?frame.commit_ts,
                            "Pass 2: unresolved ns_id (Phase 2 tolerance — log-and-proceed; \
                             Phase 4 §8.13 hard-errors this)"
                        );
                        record_logical_txn_pass2_unresolved_op();
                    }
                }
                LogicalOpKind::SecondaryInsert { index_id, .. }
                | LogicalOpKind::SecondaryDelete { index_id, .. } => {
                    if catalog.find_index_by_id(*index_id)?.is_some() {
                        record_logical_txn_pass2_resolved_op();
                    } else {
                        #[cfg(feature = "tracing")]
                        tracing::warn!(
                            target: "mqlite",
                            index_id = *index_id,
                            commit_ts = ?frame.commit_ts,
                            "Pass 2: unresolved index_id (Phase 2 tolerance — \
                             log-and-proceed; Phase 4 §8.13 hard-errors this)"
                        );
                        record_logical_txn_pass2_unresolved_op();
                    }
                }
            }
        }
    }
    Ok(())
}

/// §3.4 invariant: op_ordinal values form a dense sequence
/// `0..ops.len()-1` with no gaps and no duplicates. Pass 1 should
/// already have enforced this via `LogicalTxnFrame::decode`; we re-check
/// here because Pass 2 is the last gate before published-state open.
fn validate_frame_ordinals_dense(frame: &LogicalTxnFrame) -> Result<()> {
    let n = frame.ops.len();
    let mut seen = vec![false; n];
    for op in &frame.ops {
        let ord = op.op_ordinal as usize;
        if ord >= n {
            return Err(Error::Internal(format!(
                "Pass 2: op_ordinal {} out of range 0..{} (commit_ts {:?})",
                op.op_ordinal, n, frame.commit_ts
            )));
        }
        if seen[ord] {
            return Err(Error::Internal(format!(
                "Pass 2: duplicate op_ordinal {} (commit_ts {:?})",
                op.op_ordinal, frame.commit_ts
            )));
        }
        seen[ord] = true;
    }
    Ok(())
}

impl MetadataState {
    /// Create the initial MetadataState + SharedState from an existing
    /// (or fresh) buffer pool handle.
    pub(super) fn new(
        handle: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
    ) -> Result<(Self, Arc<SharedState>)> {
        let store = BufferPoolPageStore::new(Arc::clone(&handle));
        let (backup_root, header_next_namespace_id, header_next_index_id, history_root_page) =
            handle.allocator().with_header(|h| {
                (
                    h.catalog_root_backup,
                    h.next_namespace_id as i64,
                    h.next_index_id as i64,
                    h.history_store_root_page,
                )
            })?;
        // Phase 1 §10.7 — propagate the persisted `next_*` counters to the
        // in-memory catalog. Fresh DB uses the defaults (1) from
        // `Catalog::create`.
        let (catalog, used_backup) = catalog_open_with_fallback(
            store,
            catalog_root_page,
            catalog_root_level,
            backup_root,
            catalog_root_level,
            header_next_namespace_id,
            header_next_index_id,
            |_page| true,
        )?;
        let _ = used_backup; // noted for tracing/logging if needed

        // Phase 2 §5.2 — Pass 2 post-open validation of logical frames.
        // Runs exactly once immediately after `catalog_open_with_fallback`
        // and before any user-visible state is published. Phase 2
        // tolerance: unresolved ids are log-and-proceed. Must not mutate
        // the catalog, buffer pool, journal, HLC floor, or any tree —
        // the `&Catalog` signature alone pins the no-mutation contract
        // (see `validate_parsed_logical_frames_against_catalog`).
        let parsed_logical = handle.take_parsed_logical_frames();
        validate_parsed_logical_frames_against_catalog(&catalog, &parsed_logical)?;
        // T7 — journal-tail HLC oracle recovery: floor the oracle above
        // every durable ChainCommit from the previous lifetime. Missing
        // `successor()` (saturated `Ts::MAX`) is a hard error per plan.
        let oracle = TimestampOracle::new();
        if let Some(max_ts) = handle.recovered_max_commit_ts()? {
            match max_ts.successor() {
                Some(next) => oracle.set_min(next),
                None => return Err(Error::TimestampExhausted),
            }
        }
        // Phase 1 §10.7 — on fresh DB, allocate an empty root via
        // `HistoryStore::create_empty_root` and persist the page id to
        // the header. On reopen, the persisted root page is recorded
        // but the tree itself is rebuilt fresh each lifetime (Phase 4's
        // §8.11 is what introduces durable history-store contents; Phase 1
        // only pins the root-page field in the header so Phase 4 does
        // not need a format bump). This matches the pre-Phase-1 semantic:
        // reconciliation repopulates the history tree lazily after open.
        let (history_store_inner, persisted_history_root) =
            HistoryStore::create_empty_root(BufferPoolPageStore::new_history(Arc::clone(&handle)))?;

        // Phase 1 §10.6 — build the initial `ReadEpoch` from the live
        // catalog at `oracle.now()`. Fresh DB: `oracle.now()` is
        // `Ts { 0, 0 }`. Reopen: oracle is already floored above
        // `max_commit_ts.successor()`.
        let initial_catalog = Arc::new(build_published_catalog(&catalog)?);
        let initial_epoch = ReadEpoch {
            visible_ts: oracle.now(),
            catalog: initial_catalog,
        };

        let shared = Arc::new(SharedState {
            handle,
            history_store: std::sync::Mutex::new(history_store_inner),
            oracle,
            published: ArcSwap::from_pointee(initial_epoch),
            txn_counter: AtomicU64::new(1),
            catalog_gen: AtomicU64::new(0),
            #[cfg(test)]
            publish_pause_hook: std::sync::Mutex::new(None),
        });

        let md = Self {
            catalog: std::sync::Mutex::new(catalog),
        };
        // For a new database, persist the freshly-allocated catalog root
        // AND the history-store root page to the file header immediately
        // (will be written to disk on flush). Reopen case: header values
        // already match; we still persist the history-store root if it
        // was zero and just freshly created.
        if catalog_root_page == 0 || history_root_page == 0 {
            let cat = catalog_lock(&md);
            let root_page = cat.root_page();
            let root_level = cat.root_level();
            drop(cat);
            shared.handle.allocator().update_header(|h| {
                if catalog_root_page == 0 {
                    h.catalog_root_page = root_page;
                    h.catalog_root_level = root_level;
                    h.catalog_root_backup = root_page;
                }
                if history_root_page == 0 {
                    h.history_store_root_page = persisted_history_root;
                }
            })?;
        }
        // Seed `catalog_gen` with 1 so every subsequent rebuild sees a
        // monotonically strictly greater value — readers can distinguish
        // "not yet published" (0) from "fresh epoch" (>= 1).
        shared.catalog_gen.store(1, Ordering::Release);
        Ok((md, shared))
    }
}

// ---------------------------------------------------------------------------
// OwnedLaneGuard — parking_lot ArcMutexGuard; owns both lock + Arc so the
// guard can be returned from functions with no lifetime restriction.
// ---------------------------------------------------------------------------

/// An owned guard for a per-namespace lane mutex.
///
/// `parking_lot::ArcMutexGuard` holds the `Arc` internally, so it is
/// `Send` and has no lifetime parameter — no `unsafe` needed.
pub(super) type OwnedLaneGuard = parking_lot::ArcMutexGuard<parking_lot::RawMutex, ()>;

/// Resolve the per-namespace lane mutex, creating one if needed.
pub(super) fn lane_for(engine: &super::PagedEngine, ns: &str) -> Arc<parking_lot::Mutex<()>> {
    if let Some(entry) = engine.ns_lanes.get(ns) {
        return Arc::clone(entry.value());
    }
    engine
        .ns_lanes
        .entry(ns.to_string())
        .or_insert_with(|| Arc::new(parking_lot::Mutex::new(())))
        .clone()
}

/// Acquire the namespace lane with busy-timeout / busy-handler semantics.
///
/// Uses `parking_lot::Mutex` which never poisons, so all poisoned-error
/// branches from the previous `std::sync::Mutex` implementation are gone.
pub(super) fn acquire_lane(
    engine: &super::PagedEngine,
    lane: Arc<parking_lot::Mutex<()>>,
) -> Result<OwnedLaneGuard> {
    // Fast path — uncontended case; no syscall overhead.
    if let Some(g) = lane.try_lock_arc() {
        return Ok(g);
    }

    let timeout = engine.busy_timeout;

    // busy_handler path: spin with 1 ms sleeps until handler gives up.
    if let Some(handler) = &engine.busy_handler {
        let mut attempts: u32 = 0;
        loop {
            std::thread::sleep(Duration::from_millis(1));
            if let Some(g) = lane.try_lock_arc() {
                return Ok(g);
            }
            if !handler.0(attempts) {
                return Err(Error::WriterBusy);
            }
            attempts = attempts.saturating_add(1);
        }
    }

    // Timed wait path — use parking_lot's built-in timeout.
    if timeout.is_zero() {
        return Err(Error::WriterBusy);
    }
    match lane.try_lock_arc_for(timeout) {
        Some(g) => Ok(g),
        None => Err(Error::WriterBusy),
    }
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod state_tests;
