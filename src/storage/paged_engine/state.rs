//! Shared + metadata state for the PagedEngine.

use std::collections::HashMap;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::atomic::AtomicU8;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use dashmap::DashMap;

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
use crate::storage::reconcile::plan::{DirtyReason, LeafState, TreeIdent};
use crate::storage::root_snapshot::PublishedEpoch;

use super::catalog_ops::catalog_lock;
use super::publish::build_published_catalog;
use super::recovery_apply::{
    apply_parsed_logical_frames, check_recovery_replay_pool_bound,
    install_recovered_published_epoch,
};

// ---------------------------------------------------------------------------
// SharedState — fields shared by read path (no mutex) and writer (mutex held)
// ---------------------------------------------------------------------------

/// State shared by the read path (no mutex) and the writer inside
/// `Mutex<BpBackend>`.
pub(crate) struct SharedState {
    pub handle: Arc<BufferPoolHandle>,
    pub history_store: std::sync::Mutex<HistoryStore<BufferPoolPageStore>>,
    /// Phase 4 dirty-leaf index keyed by stable tree identity.
    pub dirty_leaves: DashMap<TreeIdent, HashMap<u32, LeafState>>,
    pub oracle: TimestampOracle,
    /// Atomically published read epoch for the mutex-free read path.
    /// Readers load one `Arc<PublishedEpoch>` and observe the full
    /// visibility tuple through the same guard.
    pub published: ArcSwap<PublishedEpoch>,
    /// Engine-fatal poison flag for post-durable unrecoverable live-state
    /// failures. Once set, new operations return [`Error::EngineFatal`]
    /// until the database is reopened.
    pub engine_poisoned: AtomicBool,
    /// Monotonic transaction identifier source shared by readers and writers.
    pub txn_counter: AtomicU64,
    /// §10.8 #19 publish-pause rendezvous hook. Per-engine (NOT
    /// process-global) so parallel tests using independent engines
    /// cannot consume each other's barriers. Under `#[cfg(test)]`
    /// only — production builds carry neither the `Mutex` nor the
    /// `Arc<Barrier>` (§11 #10: no new `Mutex` / `Arc` on commit path).
    #[cfg(test)]
    pub publish_pause_hook: std::sync::Mutex<Option<std::sync::Arc<std::sync::Barrier>>>,
    /// Test-only counter for the post-open recovery epoch store. This is
    /// per-engine so integration tests do not race on a global metric.
    #[cfg(any(test, feature = "test-hooks"))]
    pub recovery_open_published_store_count: AtomicU64,
    /// Test-only S9 primary-install fault injector for US-019.
    #[cfg(any(test, feature = "test-hooks"))]
    pub us019_primary_install_failures: AtomicU8,
    /// Test-only S9 primary-install attempt counter for US-019.
    #[cfg(any(test, feature = "test-hooks"))]
    pub us019_primary_install_attempts: AtomicU64,
    /// Test-only namespace-keyed write-body entry rendezvous hooks.
    #[cfg(any(test, feature = "test-hooks"))]
    pub write_body_entry_hooks: std::sync::Mutex<
        std::collections::HashMap<
            String,
            std::collections::VecDeque<super::test_accessors::WriteBodyEntryHook>,
        >,
    >,
    /// Monotonic ids for test-only write-body entry hooks.
    #[cfg(any(test, feature = "test-hooks"))]
    pub write_body_entry_hook_next_id: AtomicU64,
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
    pub(crate) fn load_published(&self) -> Arc<PublishedEpoch> {
        #[cfg(test)]
        {
            EPOCH_LOAD_COUNT.with(|c| c.set(c.get() + 1));
        }
        self.published.load_full()
    }

    /// Record that a leaf has resident versions eligible for checkpoint
    /// reconciliation.
    pub(crate) fn mark_leaf_dirty(&self, ident: TreeIdent, page_id: u32, reason: DirtyReason) {
        let leaf_state = LeafState {
            dirty_reason: reason,
        };
        self.dirty_leaves
            .entry(ident)
            .or_default()
            .insert(page_id, leaf_state);
    }

    /// Remove dirty-leaf state for one tree that is leaving the catalog.
    pub(crate) fn clear_dirty_tree(&self, ident: &TreeIdent) {
        self.dirty_leaves.remove(ident);
    }

    /// Remove dirty-leaf state for every tree owned by a dropped collection.
    pub(crate) fn clear_dirty_collection(&self, collection_id: i64) {
        let idents: Vec<TreeIdent> = self
            .dirty_leaves
            .iter()
            .filter_map(|entry| {
                (entry.key().collection_id == collection_id).then(|| entry.key().clone())
            })
            .collect();
        for ident in idents {
            self.clear_dirty_tree(&ident);
        }
    }

    /// Return [`Error::EngineFatal`] if this live engine has been poisoned.
    pub(crate) fn check_engine_not_poisoned(&self) -> Result<()> {
        if self.engine_poisoned.load(Ordering::Acquire) {
            return Err(Error::EngineFatal);
        }
        Ok(())
    }

    /// Poison the live engine after a post-durable unrecoverable failure.
    pub(crate) fn poison_engine(&self) {
        self.engine_poisoned.store(true, Ordering::Release);
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
/// CRUD lock order: `ns_lanes` mutex -> `metadata.read()` -> `commit_seq`
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

/// Read guard over [`MetadataState`] used by future writer-visibility plumbing.
#[allow(
    dead_code,
    reason = "Phase 5 writer visibility will hold metadata.read() across identity resolution"
)]
pub(in crate::storage::paged_engine) type MetadataReadGuard<'a> =
    std::sync::RwLockReadGuard<'a, MetadataState>;

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
        let (
            backup_root,
            header_next_namespace_id,
            header_next_index_id,
            history_root_page,
            history_root_level,
        ) = handle.allocator().with_header(|h| {
            (
                h.catalog_root_backup,
                h.next_namespace_id as i64,
                h.next_index_id as i64,
                h.history_store_root_page,
                h.history_store_root_level,
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
        // tolerance: unresolved ids are log-and-proceed. The validation
        // pass itself does not mutate durable state.
        let parsed_logical = handle.take_parsed_logical_frames();
        validate_parsed_logical_frames_against_catalog(&catalog, &parsed_logical)?;
        check_recovery_replay_pool_bound(&handle, &catalog, &parsed_logical)?;
        // T7 — journal-tail HLC oracle recovery: floor the oracle above
        // every durable ChainCommit from the previous lifetime. Missing
        // `successor()` (saturated `Ts::MAX`) is a hard error per plan.
        let oracle = TimestampOracle::new();
        let recovered_max_commit_ts = handle.recovered_max_commit_ts()?;
        if let Some(max_ts) = recovered_max_commit_ts {
            match max_ts.successor() {
                Some(next) => oracle.set_min(next),
                None => return Err(Error::TimestampExhausted),
            }
        }
        // Phase 4 US-011 — on fresh DB, allocate an empty root and persist
        // both the root page and level. On reopen, use the header-persisted
        // `(root_page, root_level)` so history entries survive a restart.
        let history_allocator = Arc::new(handle.allocator().clone());
        let (history_store_inner, persisted_history_root, persisted_history_level) =
            if history_root_page == 0 {
                let (history, root_page) = HistoryStore::create_empty_root(
                    BufferPoolPageStore::new_history(Arc::clone(&handle)),
                )?;
                (
                    history.with_overflow_allocator(Arc::clone(&history_allocator)),
                    root_page,
                    0,
                )
            } else {
                (
                    HistoryStore::open(
                        BufferPoolPageStore::new_history(Arc::clone(&handle)),
                        history_root_page,
                        history_root_level,
                    )
                    .with_overflow_allocator(Arc::clone(&history_allocator)),
                    history_root_page,
                    history_root_level,
                )
            };

        // Pre-replay epoch. Readers cannot reach this engine until open
        // returns; keeping both timestamps at Ts::MIN ensures a failed replay
        // does not publish partially-applied committed deltas.
        let initial_catalog = Arc::new(build_published_catalog(&catalog)?);
        let initial_epoch = PublishedEpoch {
            visible_ts: crate::mvcc::Ts::default(),
            catalog: initial_catalog,
            catalog_generation: 1,
            sequencer_frontier: crate::mvcc::Ts::default(),
        };

        let shared = Arc::new(SharedState {
            handle,
            history_store: std::sync::Mutex::new(history_store_inner),
            dirty_leaves: DashMap::new(),
            oracle,
            published: ArcSwap::from_pointee(initial_epoch),
            engine_poisoned: AtomicBool::new(false),
            txn_counter: AtomicU64::new(1),
            #[cfg(test)]
            publish_pause_hook: std::sync::Mutex::new(None),
            #[cfg(any(test, feature = "test-hooks"))]
            recovery_open_published_store_count: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            us019_primary_install_failures: AtomicU8::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            us019_primary_install_attempts: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            write_body_entry_hooks: std::sync::Mutex::new(std::collections::HashMap::new()),
            #[cfg(any(test, feature = "test-hooks"))]
            write_body_entry_hook_next_id: AtomicU64::new(1),
        });

        let md = Self {
            catalog: std::sync::Mutex::new(catalog),
        };
        apply_parsed_logical_frames(&shared, &md, &parsed_logical)?;
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
                    h.history_store_root_level = persisted_history_level;
                }
            })?;
        }
        install_recovered_published_epoch(&shared, &md, recovered_max_commit_ts)?;
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

#[cfg(test)]
#[path = "state_us001_tests.rs"]
mod state_us001_tests;

#[cfg(test)]
#[path = "state_us002_tests.rs"]
mod state_us002_tests;
