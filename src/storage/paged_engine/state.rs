//! Shared + metadata state for the PagedEngine.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use parking_lot::Mutex as PlMutex;

use crate::error::{EngineFatalReason, Error, Result};
use crate::journal::ParsedLogicalFrames;
use crate::mvcc::timestamp::TimestampOracle;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::{open_with_fallback as catalog_open_with_fallback, Catalog};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::history_store::HistoryStore;
use crate::storage::reconcile::driver::{DirtyReason, LeafState, TreeIdent};
use crate::storage::root_snapshot::PublishedEpoch;
use crate::storage::structural_page_batch::{StructuralBatchStore, StructuralPageBatch};

use super::publish::build_published_catalog;
use super::publish_sequencer::PublishSequencer;
use super::recovery_apply::{
    apply_parsed_logical_frames, check_recovery_replay_pool_bound,
    install_recovered_published_epoch, validate_parsed_logical_frames_against_catalog,
};
use super::writer_registry::NsWriterRegistry;

#[cfg(test)]
#[path = "tests/read_op_scope.rs"]
mod read_op_scope;
#[cfg(not(any(test, feature = "test-hooks")))]
#[path = "state_no_test_hooks.rs"]
mod state_test_hooks;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/state_test_hooks.rs"]
mod state_test_hooks;

#[cfg(test)]
pub(crate) use read_op_scope::ReadOpScope;
#[cfg(test)]
use read_op_scope::EPOCH_LOAD_COUNT;
use state_test_hooks::SharedStateTestHooks;

// ---------------------------------------------------------------------------
// SharedState — fields shared by read path (no mutex) and writer (mutex held)
// ---------------------------------------------------------------------------

// The checkpoint writer-admission gate moved to `checkpoint_gate.rs`
// (plan item R12). Re-exported here so every existing import/usage path
// through `state` keeps resolving unchanged — callers reach the gate via
// `SharedState.checkpoint_admission` and these re-exported type names.
// `CheckpointAdmissionGate` is named directly below (field type +
// constructor); the two guard tokens are returned by the gate's
// `pub(crate)` methods and re-exported alongside it so the full gate
// surface stays reachable through `state` for current and future callers.
#[allow(
    unused_imports,
    reason = "guard/admission tokens flow through method return types; re-exported so the gate surface resolves through `state`"
)]
pub(crate) use super::checkpoint_gate::{
    CheckpointAdmissionGate, CheckpointAdmissionGuard, CheckpointWriterAdmission,
};

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
    #[allow(
        dead_code,
        reason = "normal builds carry an empty hook-state stub; test-hooks builds read the field"
    )]
    pub test_hooks: SharedStateTestHooks,
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

    /// Create a new [`BufferPoolPageStore`] backed by `self.handle`.
    pub(super) fn new_btree_store(&self) -> BufferPoolPageStore {
        BufferPoolPageStore::new(Arc::clone(&self.handle))
    }

    /// Create a structural writer-side page store borrowing the given batch.
    pub(super) fn new_structural_store<'a>(
        &self,
        batch: &'a mut StructuralPageBatch,
    ) -> StructuralBatchStore<'a> {
        batch.store(self.new_btree_store())
    }

    /// Create a structural writer-side page store with chain-free leaf reads.
    ///
    /// WHY: checkpoint materialize rebuilds the B+ tree from `(key, value)`
    /// pairs already harvested via `visible_delta_entries`. The rebuild ops
    /// (`insert` / `replace_existing` / `delete`) parse only base + staged page
    /// bytes and discard the chain snapshot `read_leaf` returns. Cloning the
    /// resident chains on every such read is pure dead work, O(n) per read × n
    /// reads, which made close O(n²) (measured 4.4s × (docs/4k)²). This store
    /// routes those reads through the image-only path.
    pub(super) fn new_structural_store_chain_free<'a>(
        &self,
        batch: &'a mut StructuralPageBatch,
    ) -> StructuralBatchStore<'a> {
        batch.store(self.new_btree_store()).with_chain_free_reads()
    }

    /// Poison the live engine after a post-durable unrecoverable
    /// failure. Preserves the first reason if called more than once;
    /// later attempts notify the sequencer but do not overwrite the
    /// stored reason (§10.19.0 C-2 / US-036).
    pub(crate) fn poison_engine(&self, reason: EngineFatalReason) {
        let mut guard = self.engine_poisoned.lock();
        let reason = match &*guard {
            Some(existing) => existing.clone(),
            None => {
                *guard = Some(reason.clone());
                reason
            }
        };
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
/// Ordinary pre-durable failures route through the cleanup matrix owned
/// by US-026 / US-009 (mark Pending → Aborted, mark sequencer slot
/// aborted, drop ticket) and MUST NOT use this helper — the engine can
/// still abort cleanly. The one sanctioned pre-durable caller is the
/// cleanup matrix itself when its Pending → Aborted flip FAILS
/// (`cleanup_registered_pre_durable_failure`): the chain is wedged in
/// `Pending`, and unwinding normally would let frontier passage surface
/// the never-durable write as committed (a dirty read), so the only safe
/// exit is this poison.
pub(crate) fn poison_after_durable_commit(
    shared: &SharedState,
    reason: EngineFatalReason,
) -> Error {
    shared.poison_engine(reason.clone());
    shared.publish_sequencer.poison(reason.clone());
    Error::EngineFatal { reason }
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

impl MetadataState {
    #[allow(
        clippy::expect_used,
        reason = "catalog poisoning is an invariant breach; existing behavior is to panic"
    )]
    pub(super) fn catalog_lock(&self) -> std::sync::MutexGuard<'_, Catalog<BufferPoolPageStore>> {
        self.catalog.lock().expect("catalog poisoned")
    }

    /// Create the initial MetadataState + SharedState from an existing
    /// (or fresh) buffer pool handle.
    ///
    /// Open is split into named phases (plan item R12) that run in the same
    /// order and propagate errors in the same order as before the split:
    /// [`Self::open_catalog_validated`] (open + Pass-2 gates),
    /// [`Self::recover_oracle_and_sequencer_floors`] (HLC + publish-seq
    /// floors), [`Self::open_or_create_history_store`], assemble
    /// `SharedState`, replay deltas, [`Self::persist_fresh_roots`], then
    /// install the recovered published epoch.
    pub(super) fn new(
        handle: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
        smo_classification_retry_cap: u32,
    ) -> Result<(Self, Arc<SharedState>)> {
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

        // Phase A — open the catalog and run the Pass-2 post-open gates.
        let (catalog, parsed_logical) = Self::open_catalog_validated(
            &handle,
            catalog_root_page,
            catalog_root_level,
            backup_root,
            header_next_namespace_id,
            header_next_index_id,
        )?;

        // Phase B — recover the HLC oracle floor and publish-seq window.
        let (oracle, recovered_max_commit_ts, next_publish_seq) =
            Self::recover_oracle_and_sequencer_floors(&handle)?;

        // Phase C — open or create the history store.
        let (history_store_inner, persisted_history_root, persisted_history_level) =
            Self::open_or_create_history_store(&handle, history_root_page, history_root_level)?;

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
            // Journal recovery initializes the lock-free `published_frontier`
            // with the recovered HLC floor and starts the live `publish_seq`
            // window above the highest accepted non-control record. A fresh
            // DB uses the default floors.
            publish_sequencer: PublishSequencer::new_with_recovery_state(
                recovered_max_commit_ts.unwrap_or_default(),
                next_publish_seq,
            ),
            ns_writers: Arc::new(NsWriterRegistry::new()),
            checkpoint_admission: Arc::new(CheckpointAdmissionGate::new()),
            smo_classification_retry_cap,
            txn_counter: AtomicU64::new(1),
            // DDL-reservation counter. It must start at or below the live
            // published `catalog_generation` so the first DDL's
            // `fetch_add(1) + 1` reserves a strictly larger generation than
            // any reader has already cached; otherwise a stale-identity gate
            // could miss a catalog change. Both a fresh open and a reopen
            // start at 1: `install_recovered_published_epoch` republishes the
            // recovered catalog with `catalog_generation` reset to 1 rather
            // than restoring the pre-crash counter, so this seed stays in
            // lockstep with it. (Generations are not persisted, so recovery
            // cannot resume the old sequence — it restarts a fresh one.)
            next_catalog_gen: AtomicU64::new(1),
            test_hooks: SharedStateTestHooks::new(),
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

        // Phase D — persist freshly-allocated roots to the file header.
        Self::persist_fresh_roots(
            &shared,
            &md,
            catalog_root_page,
            history_root_page,
            persisted_history_root,
            persisted_history_level,
        )?;
        install_recovered_published_epoch(&shared, &md, recovered_max_commit_ts)?;
        Ok((md, shared))
    }

    /// Phase A — open the catalog (with backup fallback) and run the Pass-2
    /// post-open validation gates before any user-visible state publishes.
    ///
    /// Runs exactly once immediately after `catalog_open_with_fallback` and
    /// before any user-visible state is published. Phase 2 tolerance:
    /// unresolved ids are log-and-proceed. The validation pass itself does
    /// not mutate durable state. Returns the opened catalog and the consumed
    /// `ParsedLogicalFrames` for the later replay.
    fn open_catalog_validated(
        handle: &Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
        backup_root: u32,
        header_next_namespace_id: i64,
        header_next_index_id: i64,
    ) -> Result<(Catalog<BufferPoolPageStore>, ParsedLogicalFrames)> {
        let store = BufferPoolPageStore::new(Arc::clone(handle));
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
        // Recovery "case C" (an unpaired durable ChainCommit) is structurally
        // impossible in the Phase 8 wire format — a CrudCommit record carries
        // the logical and chain frames together — so no unpaired-chain
        // rejection runs here. See the `ParsedLogicalFrames` doc comment in
        // `journal/recovery.rs`.
        let parsed_logical = handle.take_parsed_logical_frames();
        validate_parsed_logical_frames_against_catalog(&catalog, &parsed_logical)?;
        check_recovery_replay_pool_bound(handle, &catalog, &parsed_logical)?;
        Ok((catalog, parsed_logical))
    }

    /// Phase B — recover the HLC oracle floor and the publish-seq window.
    ///
    /// T7 — journal-tail HLC oracle recovery: floor the oracle above every
    /// durable ChainCommit from the previous lifetime. Missing `successor()`
    /// (saturated `Ts::MAX`) is a hard error per plan. Returns the seeded
    /// oracle, the recovered max commit ts, and the next publish-seq.
    fn recover_oracle_and_sequencer_floors(
        handle: &Arc<BufferPoolHandle>,
    ) -> Result<(TimestampOracle, Option<crate::mvcc::Ts>, u64)> {
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
        Ok((oracle, recovered_max_commit_ts, next_publish_seq))
    }

    /// Phase C — open the persisted history store or create a fresh root.
    ///
    /// Phase 4 US-011 — on fresh DB, allocate an empty root and persist both
    /// the root page and level. On reopen, use the header-persisted
    /// `(root_page, root_level)` so history entries survive a restart.
    /// Returns the configured store plus the `(root_page, root_level)` to be
    /// persisted by [`Self::persist_fresh_roots`].
    fn open_or_create_history_store(
        handle: &Arc<BufferPoolHandle>,
        history_root_page: u32,
        history_root_level: u8,
    ) -> Result<(HistoryStore<BufferPoolPageStore>, u32, u8)> {
        let history_allocator = Arc::new(handle.allocator().clone());
        if history_root_page == 0 {
            let (history, root_page) = HistoryStore::create_empty_root(
                BufferPoolPageStore::new_history(Arc::clone(handle)),
            )?;
            Ok((
                history.with_overflow_allocator(Arc::clone(&history_allocator)),
                root_page,
                0,
            ))
        } else {
            Ok((
                HistoryStore::open(
                    BufferPoolPageStore::new_history(Arc::clone(handle)),
                    history_root_page,
                    history_root_level,
                )
                .with_overflow_allocator(Arc::clone(&history_allocator)),
                history_root_page,
                history_root_level,
            ))
        }
    }

    /// Phase D — persist freshly-allocated catalog/history roots to the file
    /// header (written to disk on flush).
    ///
    /// For a new database, persist the freshly-allocated catalog root AND the
    /// history-store root page to the file header immediately. Reopen case:
    /// header values already match; we still persist the history-store root
    /// if it was zero and just freshly created.
    fn persist_fresh_roots(
        shared: &SharedState,
        md: &Self,
        catalog_root_page: u32,
        history_root_page: u32,
        persisted_history_root: u32,
        persisted_history_level: u8,
    ) -> Result<()> {
        if catalog_root_page == 0 || history_root_page == 0 {
            let cat = md.catalog_lock();
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
        Ok(())
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
