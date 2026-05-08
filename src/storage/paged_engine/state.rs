//! Shared + metadata state for the PagedEngine.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
#[cfg(any(test, feature = "test-hooks"))]
use std::sync::atomic::AtomicU8;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::{Condvar, Mutex as PlMutex};

use crate::error::{EngineFatalReason, Error, Result};
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
use crate::storage::reconcile::driver::{DirtyReason, LeafState, TreeIdent};
use crate::storage::root_snapshot::PublishedEpoch;

use super::catalog_ops::catalog_lock;
use super::publish::build_published_catalog;
use super::publish_sequencer::PublishSequencer;
use super::recovery_apply::{
    apply_parsed_logical_frames, check_recovery_replay_pool_bound,
    install_recovered_published_epoch,
};
use super::writer_registry::NsWriterRegistry;

// ---------------------------------------------------------------------------
// SharedState — fields shared by read path (no mutex) and writer (mutex held)
// ---------------------------------------------------------------------------

/// Engine-wide writer-admission gate used by checkpoint freeze windows.
///
/// The namespace writer registry remains per-collection. This gate sits
/// before that namespace admission point so a checkpoint can close all new
/// writer admission, wait for already-admitted writers to publish or abort,
/// then run the mutation-free planning / freeze window without relying on
/// metadata-write exclusion.
pub(crate) struct CheckpointAdmissionGate {
    inner: PlMutex<CheckpointAdmissionInner>,
    cvar: Condvar,
}

#[derive(Default)]
struct CheckpointAdmissionInner {
    admits: u64,
    releases: u64,
    close_count: u32,
    poisoned_reason: Option<EngineFatalReason>,
}

impl CheckpointAdmissionGate {
    /// Construct an open checkpoint admission gate.
    pub(crate) fn new() -> Self {
        Self {
            inner: PlMutex::new(CheckpointAdmissionInner::default()),
            cvar: Condvar::new(),
        }
    }

    /// Close writer admission and wait until every admitted writer exits.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WriterBusy`] when already-admitted writers do not
    /// drain within `timeout`. Returns [`Error::EngineFatal`] when the live
    /// engine is already poisoned.
    pub(crate) fn close_and_drain_all(
        self: &Arc<Self>,
        timeout: Duration,
    ) -> Result<CheckpointAdmissionGuard> {
        let guard = CheckpointAdmissionGuard {
            gate: Arc::clone(self),
        };
        let start = Instant::now();
        let mut inner = self.inner.lock();
        inner.close_count = inner.close_count.saturating_add(1);
        loop {
            if let Some(reason) = inner.poisoned_reason.clone() {
                return Err(Error::EngineFatal { reason });
            }
            if inner.admits == inner.releases {
                return Ok(guard);
            }
            let remaining = timeout.checked_sub(start.elapsed()).unwrap_or_default();
            if remaining.is_zero() {
                return Err(Error::WriterBusy);
            }
            let wait = self.cvar.wait_for(&mut inner, remaining);
            if wait.timed_out() && inner.admits != inner.releases {
                return Err(Error::WriterBusy);
            }
        }
    }

    /// Admit one writer unless a checkpoint has closed admission.
    ///
    /// # Errors
    ///
    /// Returns [`Error::EngineFatal`] when the live engine has been poisoned.
    pub(crate) fn admit_writer(self: &Arc<Self>) -> Result<CheckpointWriterAdmission> {
        let mut inner = self.inner.lock();
        loop {
            if let Some(reason) = inner.poisoned_reason.clone() {
                return Err(Error::EngineFatal { reason });
            }
            if inner.close_count == 0 {
                inner.admits = inner.admits.saturating_add(1);
                return Ok(CheckpointWriterAdmission {
                    gate: Arc::clone(self),
                });
            }
            self.cvar.wait(&mut inner);
        }
    }

    fn reopen(&self) {
        let mut inner = self.inner.lock();
        if inner.poisoned_reason.is_none() {
            inner.close_count = inner.close_count.saturating_sub(1);
            if inner.close_count == 0 {
                self.cvar.notify_all();
            }
        }
    }

    fn poison(&self, reason: EngineFatalReason) {
        let mut inner = self.inner.lock();
        inner.close_count = inner.close_count.saturating_add(1);
        if inner.poisoned_reason.is_none() {
            inner.poisoned_reason = Some(reason);
        }
        self.cvar.notify_all();
    }
}

/// RAII guard returned by [`CheckpointAdmissionGate::close_and_drain_all`].
#[must_use = "CheckpointAdmissionGuard reopens checkpoint writer admission on drop"]
pub(crate) struct CheckpointAdmissionGuard {
    gate: Arc<CheckpointAdmissionGate>,
}

impl Drop for CheckpointAdmissionGuard {
    fn drop(&mut self) {
        self.gate.reopen();
    }
}

/// Writer admission token released after the writer publishes or aborts.
#[must_use = "dropping CheckpointWriterAdmission releases the admitted writer"]
pub(crate) struct CheckpointWriterAdmission {
    gate: Arc<CheckpointAdmissionGate>,
}

impl Drop for CheckpointWriterAdmission {
    fn drop(&mut self) {
        let mut inner = self.gate.inner.lock();
        inner.releases = inner.releases.saturating_add(1);
        if inner.admits == inner.releases {
            self.gate.cvar.notify_all();
        }
    }
}

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
    /// Engine-fatal poison reason for post-durable unrecoverable live
    /// state failures. `Some(reason)` once set; preserves the first
    /// reason for diagnosis. New operations return
    /// [`Error::EngineFatal`] with this reason until the database is
    /// reopened.
    pub engine_poisoned: PlMutex<Option<EngineFatalReason>>,
    /// Publish-slot sequencer used for ordered live publish under the
    /// dense-slot protocol. It owns commit timestamp registration,
    /// `mark_ready` closures, and the `published_frontier` AtomicTs.
    pub(crate) publish_sequencer: Arc<PublishSequencer>,
    /// Per-collection DDL/probe admission lanes keyed by durable
    /// `CollectionEntry.id`. Ordinary CRUD is fenced by `metadata.read()`
    /// and does not admit through this registry.
    pub(crate) ns_writers: Arc<NsWriterRegistry>,
    /// Engine-wide checkpoint admission gate. Checkpoint closes this
    /// before taking the freeze window; CRUD writers enter here before
    /// namespace admission and drop the token after publish or abort.
    pub(crate) checkpoint_admission: Arc<CheckpointAdmissionGate>,
    /// Stale-SMO classification retry cap.
    pub(crate) smo_classification_retry_cap: u32,
    /// Monotonic transaction identifier source shared by readers and writers.
    pub txn_counter: AtomicU64,
    /// DDL reservation counter. Mutated ONLY by DDL paths
    /// (`create_namespace`, `drop_namespace`, `create_index_*`, `drop_index`,
    /// `bootstrap_namespace`) under `metadata.write()` via
    /// `fetch_add(1, AcqRel) + 1`. The reserved value is the
    /// `PublishedEpoch.catalog_generation` stamped by the DDL's publish
    /// closure. Ordinary CRUD MUST NOT load or mutate this field.
    /// Initialized to the live `PublishedEpoch.catalog_generation` so a
    /// later DDL's first reservation produces a strictly larger value.
    pub(crate) next_catalog_gen: AtomicU64,
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
    /// Test-only US-009 event order counter.
    #[cfg(any(test, feature = "test-hooks"))]
    pub us009_event_order_counter: AtomicU64,
    /// Test-only order at which Pending entries flipped to Committed.
    #[cfg(any(test, feature = "test-hooks"))]
    pub us009_committed_flip_order: AtomicU64,
    /// Test-only order at which the CRUD publish step became ready.
    #[cfg(any(test, feature = "test-hooks"))]
    pub us009_publish_ready_order: AtomicU64,
    /// Test-only one-shot failure after committed flip and before publish.
    #[cfg(any(test, feature = "test-hooks"))]
    pub us009_fail_after_committed_flip: AtomicU8,
    /// Test-only US-026 one-shot post-register cleanup failpoint.
    #[cfg(any(test, feature = "test-hooks"))]
    pub us026_post_register_failpoint: AtomicU8,
    /// Test-only Phase 8 failure injected after log reservation and before
    /// dirty pages can be stamped with the reserved record's end LSN.
    #[cfg(any(test, feature = "test-hooks"))]
    pub phase8_fail_next_dirty_lsn_stamp: AtomicU8,
    /// Test-only Phase 8 failure injected after dirty pages are stamped with
    /// the reserved record's end LSN and before the record bytes are written.
    #[cfg(any(test, feature = "test-hooks"))]
    pub phase8_fail_next_after_dirty_lsn_stamp: AtomicU8,
    /// Test-only Phase 8 failure injected after the commit record is durable
    /// and before Pending heads flip to Committed.
    #[cfg(any(test, feature = "test-hooks"))]
    pub phase8_fail_next_after_durable_before_flip: AtomicU8,
    /// Test-only namespace-keyed write-body entry rendezvous hooks.
    #[cfg(any(test, feature = "test-hooks"))]
    pub write_body_entry_hooks: std::sync::Mutex<
        std::collections::HashMap<
            String,
            std::collections::VecDeque<super::hidden_accessors::WriteBodyEntryHook>,
        >,
    >,
    /// Test-only one-shot pause after Pending install and before log
    /// reservation.
    #[cfg(any(test, feature = "test-hooks"))]
    pub phase8_before_reservation_hook:
        std::sync::Mutex<Option<super::hidden_accessors::Phase8BeforeReservationHook>>,
    /// Test-only create-index build-scan rendezvous hooks.
    #[cfg(any(test, feature = "test-hooks"))]
    pub create_index_build_hooks: std::sync::Mutex<
        std::collections::HashMap<
            (String, String),
            std::collections::VecDeque<super::hidden_accessors::CreateIndexBuildHook>,
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

    /// Load a coherent `(PublishedEpoch, PublishSequencer.published_frontier)`
    /// pair (§10.19 C-1, US-037). The publisher stores the new epoch
    /// first and the live frontier second; readers MUST retry the pair
    /// when `published_frontier < epoch.visible_ts`, otherwise a foreign
    /// `Pending` entry whose `start_ts == epoch.visible_ts` could be
    /// evaluated against a stale frontier and incorrectly hidden.
    ///
    /// `ReadView::sequencer_frontier()` keeps the live-load semantics
    /// the PRD requires; this helper closes the inter-store window at
    /// view-open time so subsequent live loads are guaranteed
    /// `>= epoch.visible_ts` (the sequencer frontier is monotonic).
    ///
    /// Returns immediately for the steady state `frontier >= visible_ts`;
    /// otherwise spins with `spin_loop`. Bounded in practice by the two
    /// adjacent atomic stores in `publish_commit` /
    /// `PublishSequencer::mark_ready`.
    pub(crate) fn load_published_coherent(&self) -> Arc<PublishedEpoch> {
        loop {
            let epoch = self.load_published();
            if self
                .publish_sequencer
                .published_frontier
                .load(std::sync::atomic::Ordering::Acquire)
                >= epoch.visible_ts
            {
                return epoch;
            }
            std::hint::spin_loop();
        }
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

    /// Remove dirty-leaf state for pages that a checkpoint successfully folded.
    pub(crate) fn clear_dirty_pages(&self, ident: &TreeIdent, pages: &[u32]) {
        if pages.is_empty() {
            return;
        }
        let remove_tree = if let Some(mut dirty) = self.dirty_leaves.get_mut(ident) {
            for page in pages {
                dirty.remove(page);
            }
            dirty.is_empty()
        } else {
            false
        };
        if remove_tree {
            self.dirty_leaves.remove(ident);
        }
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

    /// Return [`Error::EngineFatal`] if this live engine has been
    /// poisoned. The first poison reason is preserved for diagnosis;
    /// subsequent poison attempts do not overwrite it.
    pub(crate) fn check_engine_not_poisoned(&self) -> Result<()> {
        if let Some(reason) = self.engine_poisoned.lock().clone() {
            return Err(Error::EngineFatal { reason });
        }
        Ok(())
    }

    /// Poison the live engine after a post-durable unrecoverable
    /// failure. Preserves the first reason if called more than once;
    /// later attempts notify the sequencer but do not overwrite the
    /// stored reason (§10.19.0 C-2 / US-036).
    pub(crate) fn poison_engine(&self, reason: EngineFatalReason) {
        let mut guard = self.engine_poisoned.lock();
        if guard.is_none() {
            *guard = Some(reason);
        }
        let reason = guard.clone().expect("engine poison reason recorded");
        drop(guard);
        self.checkpoint_admission.poison(reason);
    }
}

/// Poison the live engine after a post-durable commit failure and
/// return [`Error::EngineFatal`] with `reason`.
///
/// §10.19.0 C-2 / US-036 escalation helper:
///
/// 1. Records `reason` in `SharedState.engine_poisoned`. Preserves the
///    first reason if called more than once for diagnosis.
/// 2. Calls the sequencer's poison hook so every blocked successor
///    waiting in `register` / `wait_until_predecessors_complete` /
///    `mark_ready` wakes and returns `Error::EngineFatal` instead of
///    publishing its own slot or marking a durable slot `Aborted`.
/// 3. Returns the constructed `Error::EngineFatal { reason }` so the
///    caller can `?`-propagate without re-reading the poison state.
///
/// Callers MUST NOT use this helper before the durable journal commit
/// completes; pre-durable failures route through the cleanup matrix
/// owned by US-026 / US-009 (mark Pending → Aborted, mark sequencer
/// slot aborted, drop ticket).
pub(crate) fn poison_after_durable_commit(
    shared: &SharedState,
    reason: EngineFatalReason,
) -> Error {
    shared.poison_engine(reason.clone());
    shared.publish_sequencer.poison(reason.clone());
    Error::EngineFatal { reason }
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

/// Per-engine catalog state guarded by `PagedEngine::metadata`. DDL ops take
/// the write guard to gain exclusive access; CRUD writers take the read guard
/// (shared with other CRUD writers) and mutate the catalog via the interior
/// `Mutex<Catalog>`.
///
/// CRUD order: `metadata.read()` is held across the private write body,
/// resident-chain mutation, durable log envelope, and ordered publish. Page
/// latches protect resident-chain mutation, the Phase 8 log manager owns
/// byte-LSN reservation/durability, and `PublishSequencer` publishes slots in
/// order. DO NOT grab
/// `metadata.write()` while holding the catalog mutex; that would invert the
/// order relative to a reader that already holds `metadata.read()` and is
/// waiting for the catalog mutex.
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

fn validate_phase7_case_c_candidates(parsed: &ParsedLogicalFrames) -> Result<()> {
    if let Some(candidate) = parsed.case_c_candidates.first() {
        return Err(Error::Recovery {
            detail: format!(
                "chain_commit_offset={}: ChainCommit without matching \
                 LogicalTxnFrame commit_ts={:?}",
                candidate.chain_commit_offset, candidate.commit_ts
            ),
        });
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
        smo_classification_retry_cap: u32,
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
        let (catalog, _) = catalog_open_with_fallback(
            store,
            catalog_root_page,
            catalog_root_level,
            backup_root,
            catalog_root_level,
            header_next_namespace_id,
            header_next_index_id,
            |_page| true,
        )?;

        // Phase 2 §5.2 — Pass 2 post-open validation of logical frames.
        // Runs exactly once immediately after `catalog_open_with_fallback`
        // and before any user-visible state is published. Phase 2
        // tolerance: unresolved ids are log-and-proceed. The validation
        // pass itself does not mutate durable state.
        let parsed_logical = handle.take_parsed_logical_frames();
        validate_phase7_case_c_candidates(&parsed_logical)?;
        validate_parsed_logical_frames_against_catalog(&catalog, &parsed_logical)?;
        check_recovery_replay_pool_bound(&handle, &catalog, &parsed_logical)?;
        // T7 — journal-tail HLC oracle recovery: floor the oracle above
        // every durable ChainCommit from the previous lifetime. Missing
        // `successor()` (saturated `Ts::MAX`) is a hard error per plan.
        let oracle = TimestampOracle::new();
        let recovered_max_commit_ts = handle.recovered_max_commit_ts()?;
        let recovered_max_publish_seq = handle.recovered_max_publish_seq()?;
        let next_publish_seq = match recovered_max_publish_seq {
            Some(seq) => seq.checked_add(1).ok_or_else(|| {
                Error::Internal("recovered publish_seq floor overflows u64".into())
            })?,
            None => 1,
        };
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
        };

        let shared = Arc::new(SharedState {
            handle,
            history_store: std::sync::Mutex::new(history_store_inner),
            dirty_leaves: DashMap::new(),
            oracle,
            published: ArcSwap::from_pointee(initial_epoch),
            engine_poisoned: PlMutex::new(None),
            // Phase 8 recovery initializes the lock-free
            // `published_frontier` with the recovered HLC floor and starts
            // the live `publish_seq` window above the highest accepted
            // non-control record. A fresh DB uses the default floors.
            publish_sequencer: PublishSequencer::new_from_recovered_floors(
                recovered_max_commit_ts.unwrap_or_default(),
                next_publish_seq,
            ),
            ns_writers: Arc::new(NsWriterRegistry::new()),
            checkpoint_admission: Arc::new(CheckpointAdmissionGate::new()),
            smo_classification_retry_cap,
            txn_counter: AtomicU64::new(1),
            // §10.17.1 — start the DDL reservation counter at the live
            // `PublishedEpoch.catalog_generation` (1 on fresh open). The
            // first DDL `fetch_add(1) + 1` reserves a strictly larger
            // generation. Reopen recovery bumps this to the recovered
            // published value via `install_recovered_published_epoch`.
            next_catalog_gen: AtomicU64::new(1),
            #[cfg(test)]
            publish_pause_hook: std::sync::Mutex::new(None),
            #[cfg(any(test, feature = "test-hooks"))]
            recovery_open_published_store_count: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            us019_primary_install_failures: AtomicU8::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            us019_primary_install_attempts: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            us009_event_order_counter: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            us009_committed_flip_order: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            us009_publish_ready_order: AtomicU64::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            us009_fail_after_committed_flip: AtomicU8::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            us026_post_register_failpoint: AtomicU8::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            phase8_fail_next_dirty_lsn_stamp: AtomicU8::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            phase8_fail_next_after_dirty_lsn_stamp: AtomicU8::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            phase8_fail_next_after_durable_before_flip: AtomicU8::new(0),
            #[cfg(any(test, feature = "test-hooks"))]
            write_body_entry_hooks: std::sync::Mutex::new(std::collections::HashMap::new()),
            #[cfg(any(test, feature = "test-hooks"))]
            phase8_before_reservation_hook: std::sync::Mutex::new(None),
            #[cfg(any(test, feature = "test-hooks"))]
            create_index_build_hooks: std::sync::Mutex::new(std::collections::HashMap::new()),
            #[cfg(any(test, feature = "test-hooks"))]
            write_body_entry_hook_next_id: AtomicU64::new(1),
        });
        let weak_shared = Arc::downgrade(&shared);
        shared
            .handle
            .allocator()
            .install_freeze_violation_poisoner(move || {
                if let Some(shared) = weak_shared.upgrade() {
                    shared.poison_engine(EngineFatalReason::CheckpointPostMutationFailure);
                }
            })?;

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

#[cfg(test)]
#[path = "tests/state_recovery.rs"]
mod state_recovery;

#[cfg(test)]
#[path = "tests/dirty_leaf_state.rs"]
mod dirty_leaf_state;

#[cfg(test)]
#[path = "tests/dirty_leaf_marking.rs"]
mod dirty_leaf_marking;
