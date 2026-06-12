//! `ReadView` — the snapshot primitive every reader holds.
//!
//! Each open reader holds a `ReadView` pinning its `read_ts`; the
//! version-chain walker uses `read_ts` to pick the visible entry. The
//! `ReadViewRegistry` tracks live `ReadView`s so the writer can compute
//! `oldest_required_ts`.
//!
//! The `poisoned` flag and `pin_ops_in_flight` counter together support
//! force-expiry: `force_expire` flips `poisoned`, then spins until
//! `pin_ops_in_flight` reaches 0, so no concurrent pin walk can be
//! mid-flight when pages are released.
//!
//! Production-path atomics use the cfg(loom) shim pattern so loom
//! harnesses can permute them.
//
// LOCK-ORDER:
// Database-wide total order. Any path acquiring two or
// more of these MUST acquire in this order and release in reverse:
//
// 1.   history-store partition mutex (outermost)
// 1.5. PageLifetimeQueue::pending mutex
//      — brief; acquired by OverflowRef::Drop on 0-refcount transition to push
//      a u32 first_page, by drain_free_queue on writer path to drain.
// 2.   AllocatorHandle::state mutex
// 3.   32 KB main partition mutex (BufferPool::inner_32k)
//      Used only to find/pin/unpin a frame. It is released before acquiring
//      PageLatch, so partition mutex and PageLatch are never nested.
// 3a.  PageLatch on a resident leaf frame
//      — Shared or Exclusive mode. Acquired AFTER pin_page has released
//      the partition mutex. A thread holding multiple page latches must
//      acquire them in ascending page-id order.
// 3b.  4 KB main partition mutex (BufferPool::inner_4k)
// 4.   [unused — historical 4 KB slot kept for table stability]
// 5.   ReadViewRegistry mutex (Arc<Mutex<BTreeMap<u64, u64>>>)
// 6.   PublishSequencer mutex
//      — held for register_with_oracle slot allocation, mark_ready /
//      mark_aborted transitions, and dense window advancement. Publish
//      closures must not acquire metadata, PageLatch, or journal_mutex.
// 7.   NsWriterRegistry admission mutex (per-ns)
//      — held only during admit/release; brief. Takes after metadata.read()
//      on CRUD, after metadata.write() on DDL. Waits on cvar while
//      close_and_drain is active.
// 8.   catalog Mutex (inside MetadataState)
//      — innermost. No further locks acquired under it.
//
// `metadata` RwLock is NOT in this numbered table because it is an
// orthogonal DDL-vs-CRUD fence: CRUD `read()` is held only for id capture
// plus the brief NsWriterRegistry admit; DDL `write()` is held for the DDL
// body and drain. The RwLock itself has no interaction with the numbered
// positions after CRUD drops it before the body.
//
// Readers still DO NOT acquire `AllocatorHandle::state` for pure reads.
// The reader-path OverflowRef::Drop still acquires PageLifetimeQueue::pending
// briefly when decref brings count to 0. ReadViewRegistry::oldest_required_ts()
// MUST be snapshotted BEFORE any partition mutex or page latch in any
// reconciliation path.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use crate::mvcc::metrics;
use crate::mvcc::registry::ReadViewRegistry;

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU32};

#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU32};

use crate::mvcc::timestamp::Ts;
use crate::storage::paged_engine::publish_sequencer::PublishSequencer;
use crate::storage::root_snapshot::{PublishedCatalog, PublishedEpoch};

/// A snapshot handle for an active reader.
///
/// Constructed by `ReadViewRegistry::open()` with `read_ts` taken from
/// the timestamp oracle. The visibility rule:
///
/// - Committed entry `E` is visible iff `E.start_ts <= read_ts < E.stop_ts`.
/// - Pending entry is visible to its own `txn_id`; foreign pending entries
///   are gated by the pinned sequencer frontier and timestamp window.
///
/// `poisoned` is set by force-expiry before touching any owned pins;
/// `pin_ops_in_flight` lets the force-expiry path wait for concurrent
/// pin walks to complete before releasing pages.
pub struct ReadView {
    /// Snapshot timestamp for visibility checks.
    pub read_ts: Ts,
    /// Transaction identifier — also serves as the txn_id used to resolve
    /// visibility of this reader's own pending entries when the reader
    /// doubles as a writer.
    pub txn_id: u64,
    /// Set by `force_expire`. Any subsequent pin-walk observes this via an
    /// Acquire load at the pre-check and again post-increment of
    /// `pin_ops_in_flight`; if poisoned, it bails without walking pins.
    pub poisoned: AtomicBool,
    /// Active pin-walk count. Incremented on entry to
    /// `pin_overflows`-style code and decremented on exit. `force_expire`
    /// spins until this reaches 0 before the caller is allowed to proceed
    /// with page-release.
    pub pin_ops_in_flight: AtomicU32,
    /// Registry back-pointer. When `Some`, `Drop` unregisters `txn_id` from
    /// the registry so `oldest_required_ts()` no longer considers this
    /// view. `None` for standalone `new_frontier_pinned_for_tests` /
    /// `new_for_epoch` callers — primarily tests that exercise snapshot
    /// visibility without a registry.
    registry: Option<Arc<ReadViewRegistry>>,
    /// Published visibility tuple pinned for this view's lifetime.
    epoch: Arc<PublishedEpoch>,
    /// Live sequencer frontier provider captured from `SharedState` when
    /// the view is opened. The view loads the live `published_frontier`
    /// for foreign-Pending visibility checks (see `version_visible_to`)
    /// rather than caching a stale snapshot inside `PublishedEpoch`, so a
    /// long-lived reader sees commits that the sequencer publishes after
    /// the view was opened.
    publish_sequencer: Arc<PublishSequencer>,
}

impl std::fmt::Debug for ReadView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadView")
            .field("read_ts", &self.read_ts)
            .field("txn_id", &self.txn_id)
            .field("poisoned", &self.poisoned.load(Ordering::Acquire))
            .field(
                "pin_ops_in_flight",
                &self.pin_ops_in_flight.load(Ordering::Acquire),
            )
            .field("catalog_generation", &self.epoch.catalog_generation)
            .field("sequencer_frontier", &self.sequencer_frontier())
            .finish()
    }
}

impl ReadView {
    /// Construct a fresh, registry-less `ReadView` whose foreign-Pending
    /// visibility is **frontier-pinned at `Ts::default()`**.
    ///
    /// # Warning — test-only semantics
    ///
    /// This constructor builds a brand-new `PublishSequencer` pinned at
    /// `Ts::default()`, so [`version_visible_to`](crate::mvcc::chain_snapshot)
    /// treats every *foreign* `Pending` entry as **not yet published** — the
    /// live-frontier proof that production readers rely on is permanently
    /// "frozen at zero" for this view. It is therefore ONLY correct for tests
    /// and internal snapshot fixtures that do not exercise foreign-Pending
    /// visibility. Production reader paths MUST route through
    /// [`Self::new_for_epoch`] / [`Self::open_for_epoch`] with a
    /// `PublishSequencer` captured from `SharedState`. Tests that need an
    /// explicit frontier value should use [`Self::new_with_frontier`].
    #[must_use]
    pub fn new_frontier_pinned_for_tests(read_ts: Ts, txn_id: u64) -> Self {
        Self::new_for_epoch(
            standalone_epoch(read_ts),
            txn_id,
            PublishSequencer::new_with_published_frontier(Ts::default()),
        )
    }

    /// Test-only standalone constructor with an explicit sequencer
    /// frontier. Builds a fresh `PublishSequencer` pinned at `frontier`
    /// so the live-frontier accessor returns it. Production paths must
    /// route through `new_for_epoch` / `open_for_epoch` with a sequencer
    /// captured from `SharedState`.
    #[cfg(any(test, feature = "test-hooks"))]
    #[must_use]
    pub fn new_with_frontier(read_ts: Ts, txn_id: u64, frontier: Ts) -> Self {
        Self::new_for_epoch(
            standalone_epoch(read_ts),
            txn_id,
            PublishSequencer::new_with_published_frontier(frontier),
        )
    }

    /// Construct a `ReadView` over an already-loaded published epoch.
    ///
    /// Reader paths load `SharedState.published` once, clone that epoch
    /// into the view, and read all visibility metadata from this pinned
    /// value for the rest of the snapshot lifetime. The view also pins
    /// `Arc<PublishSequencer>` so foreign-Pending visibility loads the
    /// live `published_frontier` instead of a stale snapshot.
    #[must_use]
    pub(crate) fn new_for_epoch(
        epoch: Arc<PublishedEpoch>,
        txn_id: u64,
        publish_sequencer: Arc<PublishSequencer>,
    ) -> Self {
        Self {
            read_ts: epoch.visible_ts,
            txn_id,
            poisoned: AtomicBool::new(false),
            pin_ops_in_flight: AtomicU32::new(0),
            registry: None,
            epoch,
            publish_sequencer,
        }
    }

    /// Open a registry-tracked `ReadView` whose foreign-Pending visibility is
    /// **frontier-pinned at `Ts::default()`**. The view registers itself with
    /// `registry` for the lifetime of the returned `Arc`; the last `Arc::drop`
    /// unregisters the view's `txn_id`, bounding the duration that `read_ts`
    /// pins the `oldest_required_ts()` horizon. The registry slot also tracks
    /// a `Weak<ReadView>` so `force_expire_all` can iterate live views.
    ///
    /// # Warning — test-only semantics
    ///
    /// Like [`Self::new_frontier_pinned_for_tests`], this builds a brand-new
    /// `PublishSequencer` pinned at `Ts::default()`, so foreign-Pending
    /// visibility is permanently frozen at zero for this view. It is correct
    /// only for tests that exercise the registry / `oldest_required_ts`
    /// horizon without depending on live foreign-Pending visibility.
    /// Production reader paths MUST use [`Self::open_for_epoch`] with a
    /// `PublishSequencer` captured from `SharedState`.
    #[must_use]
    pub fn open_frontier_pinned_for_tests(
        registry: Arc<ReadViewRegistry>,
        read_ts: Ts,
        txn_id: u64,
    ) -> Arc<Self> {
        Self::open_for_epoch(
            registry,
            standalone_epoch(read_ts),
            txn_id,
            PublishSequencer::new_with_published_frontier(Ts::default()),
        )
    }

    /// Open a registry-tracked view over an already-loaded published epoch.
    #[must_use]
    pub(crate) fn open_for_epoch(
        registry: Arc<ReadViewRegistry>,
        epoch: Arc<PublishedEpoch>,
        txn_id: u64,
        publish_sequencer: Arc<PublishSequencer>,
    ) -> Arc<Self> {
        let read_ts = epoch.visible_ts;
        let view = Arc::new(Self {
            read_ts,
            txn_id,
            poisoned: AtomicBool::new(false),
            pin_ops_in_flight: AtomicU32::new(0),
            registry: Some(Arc::clone(&registry)),
            epoch,
            publish_sequencer,
        });
        registry.register(txn_id, read_ts, Arc::downgrade(&view));
        view
    }

    /// Test-only standalone variant of
    /// [`Self::open_for_epoch_conservative_then_refine`] with a
    /// frontier-pinned standalone epoch (no `SharedState` sequencer), used to
    /// exercise the conservative-then-refine registry handshake in isolation.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn open_for_epoch_conservative_then_refine_for_tests(
        registry: Arc<ReadViewRegistry>,
        read_ts: Ts,
        txn_id: u64,
    ) -> Arc<Self> {
        // Mirror the production ordering in
        // `snapshot_ops::read_exec::open_snapshot_read_view`: pin the
        // conservative floor FIRST (with a placeholder back-pointer), then
        // build the view over the epoch, then attach + refine.
        registry.register(txn_id, Ts::default(), std::sync::Weak::new());
        Self::open_for_epoch_conservative_then_refine(
            registry,
            standalone_epoch(read_ts),
            txn_id,
            PublishSequencer::new_with_published_frontier(Ts::default()),
        )
    }

    /// Build a registry-tracked view over an already-loaded `epoch` and a
    /// registry slot that has ALREADY been conservatively pinned at
    /// `Ts::default()` for `txn_id` (ITEM 1, option a).
    ///
    /// Ordering invariant — the conservative pin precedes the epoch load.
    /// The caller MUST have already inserted `txn_id → Ts::default()` into
    /// `registry` (via [`ReadViewRegistry::register`] with a placeholder
    /// `Weak::new()`) BEFORE loading `epoch`. From that pin onward the reader
    /// is a member of every `oldest_required_ts()` snapshot at the lowest
    /// possible floor, so no reconcile/eviction prune can run between the pin
    /// and now and drop a resident superseded version visible at any
    /// `ts >= Ts::default()` — which is every version this reader could need.
    /// (The prune drops superseded committed versions whose
    /// `stop_ts <= oldest_required_ts()` without spilling them to the history
    /// store; a `Ts::default()` floor protects all of them.)
    ///
    /// This constructor builds the `Arc<ReadView>`, then in a single registry
    /// lock publishes the real `Weak<ReadView>` back-pointer and refines the
    /// pinned slot up from `Ts::default()` to the view's real `read_ts` (the
    /// view's own `read_ts` field is always the real snapshot ts — only the
    /// registry slot starts conservative), so the reader does not over-pin the
    /// horizon for its whole lifetime. A concurrent prune observes either the
    /// conservative floor or the refined `read_ts`; neither lets it drop a
    /// version this reader needs.
    ///
    /// Net registry-mutex ops for the open are exactly two — the caller's
    /// conservative `register` plus this `attach_view_and_refine` — matching
    /// the prior shape; the fix only MOVES the pin to precede the load.
    #[must_use]
    pub(crate) fn open_for_epoch_conservative_then_refine(
        registry: Arc<ReadViewRegistry>,
        epoch: Arc<PublishedEpoch>,
        txn_id: u64,
        publish_sequencer: Arc<PublishSequencer>,
    ) -> Arc<Self> {
        let read_ts = epoch.visible_ts;
        let view = Arc::new(Self {
            read_ts,
            txn_id,
            poisoned: AtomicBool::new(false),
            pin_ops_in_flight: AtomicU32::new(0),
            registry: Some(Arc::clone(&registry)),
            epoch,
            publish_sequencer,
        });
        // The conservative pin already happened before the epoch load. Now
        // that the view (and its real back-pointer) exist, publish the Weak
        // and refine the slot up to the real read_ts in one lock.
        registry.attach_view_and_refine(txn_id, read_ts, Arc::downgrade(&view));
        view
    }

    /// Snapshot timestamp pinned by the published epoch.
    #[must_use]
    pub(crate) fn visible_ts(&self) -> Ts {
        self.epoch.visible_ts
    }

    /// Published epoch pinned by this view.
    #[must_use]
    pub(crate) fn published_epoch(&self) -> &Arc<PublishedEpoch> {
        &self.epoch
    }

    /// Live sequencer frontier published by `PublishSequencer`. Loads
    /// `publish_sequencer.published_frontier` with `Acquire`; never reads
    /// a cached `PublishedEpoch` field, so the value reflects commits
    /// published after this view was opened.
    #[must_use]
    pub(crate) fn sequencer_frontier(&self) -> Ts {
        self.publish_sequencer
            .published_frontier
            .load(Ordering::Acquire)
    }

    /// True iff this view has been force-expired. Readers MUST check this
    /// before acting on a cached `ReadView`; the engine returns
    /// `Error::ReadViewExpired` on any subsequent operation.
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// Return `Err(Error::ReadViewExpired)` if this view has been
    /// force-expired, else `Ok(())`. Called at the top of reader paths
    /// that want to surface the expiry as a user-visible error.
    pub fn check_active(&self) -> crate::error::Result<()> {
        if self.is_poisoned() {
            Err(crate::error::Error::ReadViewExpired)
        } else {
            Ok(())
        }
    }

    /// Force-expire this view. Does NOT walk any owned snapshots.
    /// This method has two responsibilities:
    ///
    /// 1. Flip `poisoned = true` (Release) so any subsequent
    ///    `ChainSnapshot::new` short-circuits at its pre-check without
    ///    performing a refcount bump.
    /// 2. Spin until `pin_ops_in_flight == 0` so any pin-walk that
    ///    happened to run `fetch_add(1)` before it observed the poison
    ///    store has finished its critical section; such a pin-walk
    ///    observes poison on its post-bump recheck and rolls back via
    ///    RAII Drop.
    ///
    /// Snapshots that completed construction before the poison store
    /// remain valid Rust values held by the reader; their default drop
    /// glue runs `OverflowRef::Drop` (atomic decref) on each entry when
    /// the reader releases the snapshot. Force-expiry does not skip any
    /// refcount decrement that the natural drop would have performed.
    ///
    /// Ticks `mvcc.read_views_force_expired_total += 1` once per call.
    pub fn force_expire(&self) {
        // Step 1: poison BEFORE any wait. New pin ops see poisoned=true
        // on their pre-check and return early without CAS.
        self.poisoned.store(true, Ordering::Release);

        // Step 2: spin until no pin op is mid-flight.
        self.wait_pin_drain();

        metrics::record_read_view_force_expired();
    }

    /// Spin-wait for `pin_ops_in_flight` to drain to zero.
    ///
    /// First `SPIN_BUDGET` iterations: `spin_loop()` (fast path for
    /// microsecond races). After the spin budget: `yield_now()` so the
    /// scheduler can run the pin-walker. After `TIMEOUT_MS` ms: emit a
    /// warning tracing event and tick
    /// `mvcc.force_expire_spin_stalls_total`; keep yielding.
    fn wait_pin_drain(&self) {
        const SPIN_BUDGET: u32 = 128;
        const TIMEOUT_MS: u64 = 10;

        for _ in 0..SPIN_BUDGET {
            if self.pin_ops_in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            std::hint::spin_loop();
        }

        let start = Instant::now();
        let mut stalled = false;
        loop {
            if self.pin_ops_in_flight.load(Ordering::Acquire) == 0 {
                return;
            }
            if !stalled && start.elapsed().as_millis() as u64 > TIMEOUT_MS {
                metrics::record_force_expire_spin_stall();
                #[cfg(feature = "tracing")]
                tracing::warn!("force_expire spinning > {}ms", TIMEOUT_MS);
                stalled = true;
            }
            std::thread::yield_now();
        }
    }
}

/// Test-only handle that owns an `Arc<PublishSequencer>` so integration
/// tests can advance the live `published_frontier` after a `ReadView`
/// is opened against the same sequencer, exercising the contract that a
/// view reads the live frontier rather than a frontier snapshot.
///
/// Always compiled — the canonical `cargo test --release --test
/// mwmr_timestamp_frontier read_view_uses_live_publish_sequencer_frontier`
/// gate runs without enabling the `test-hooks` feature, so this thin
/// wrapper must be visible at the public API boundary.
pub struct TestFrontierHandle {
    sequencer: Arc<PublishSequencer>,
}

impl TestFrontierHandle {
    /// Construct a fresh sequencer pinned at `initial_frontier`.
    #[must_use]
    pub fn new(initial_frontier: Ts) -> Self {
        Self {
            sequencer: PublishSequencer::new_with_published_frontier(initial_frontier),
        }
    }

    /// Advance the live `published_frontier` to `ts`. Mirrors what
    /// `PublishSequencer::mark_ready` does after the publish closure
    /// stores the new epoch.
    pub fn advance(&self, ts: Ts) {
        self.sequencer
            .published_frontier
            .store(ts, Ordering::Release);
    }

    /// Open a `ReadView` against this handle's sequencer. The view
    /// loads the live frontier through `ReadView::sequencer_frontier()`
    /// so subsequent `advance` calls are observed by the same view.
    #[must_use]
    pub fn read_view(&self, read_ts: Ts, txn_id: u64) -> ReadView {
        ReadView::new_for_epoch(
            standalone_epoch(read_ts),
            txn_id,
            Arc::clone(&self.sequencer),
        )
    }
}

fn standalone_epoch(read_ts: Ts) -> Arc<PublishedEpoch> {
    Arc::new(PublishedEpoch {
        visible_ts: read_ts,
        catalog: Arc::new(PublishedCatalog {
            namespaces: HashMap::new(),
            namespace_id_by_name: HashMap::new(),
            index_owner_by_id: HashMap::new(),
        }),
        catalog_generation: 0,
    })
}

impl Drop for ReadView {
    fn drop(&mut self) {
        if let Some(reg) = &self.registry {
            reg.unregister(self.txn_id);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(not(loom))]
#[path = "tests/read_view.rs"]
mod tests;
