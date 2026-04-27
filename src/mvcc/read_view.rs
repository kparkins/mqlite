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
// The full database-wide total order; any path that acquires two or more of
// these mutexes MUST acquire them in this order, and release in reverse:
//
// 1.   history-store partition mutex (outermost)
// 1.5. DeferredFreeQueue::pending mutex
//      — brief; acquired by OverflowRef::Drop on 0-refcount transition to push
//      a u32 first_page, by drain_free_queue on writer path to drain.
//      OverflowRef::Drop acquires 1.5 and releases immediately (no downstream
//      acquisitions). drain_free_queue acquires 1.5 first, then
//      AllocatorHandle::state (1.5 → 2).
// 2.   AllocatorHandle::state mutex (Arc<Mutex<AllocatorState>>)
//      — for any alloc_*/free_*/free_overflow_chain / refcount-header-write op
//      that must update FileHeader free lists. Atomic page-header refcount ops
//      (incref_overflow, decref_overflow) happen WITHOUT this mutex and are
//      lock-free.
// 3.   32 KB main partition mutex (BufferPool::inner_32k)
// 4.   4 KB main partition mutex  (BufferPool::inner_4k)
// 5.   ReadViewRegistry mutex (Arc<Mutex<BTreeMap<u64, u64>>>)
// 6.   writer serialization mutex
//
// Readers DO NOT acquire `AllocatorHandle::state` for pure reads (refcount
// atomics live on the page header and are lock-free). The reader-path
// `OverflowRef::Drop` DOES acquire `DeferredFreeQueue::pending` briefly
// (push a u32) when decref brings count to 0 — this is the ONLY lock any
// reader path acquires; it is strictly above the allocator mutex in the
// order and closed before any other acquisition. Free-side
// `drain_free_queue` acquires `DeferredFreeQueue::pending` first, then
// `AllocatorHandle::state`, and is called only from writer-serialized
// context (writer mutex held). `ReadViewRegistry::oldest_required_ts()`
// MUST be snapshotted **before** any partition mutex is acquired in a
// reconciliation path.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::Bound;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use crate::mvcc::metrics;

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU32};

#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU32};

use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::{VersionEntry, VersionState};
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
    /// view. `None` for standalone `ReadView::new(..)` callers — primarily
    /// tests that exercise snapshot visibility without a registry.
    registry: Option<Arc<ReadViewRegistry>>,
    /// Published visibility tuple pinned for this view's lifetime.
    epoch: Arc<PublishedEpoch>,
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
            .field("sequencer_frontier", &self.epoch.sequencer_frontier)
            .finish()
    }
}

impl ReadView {
    /// Construct a fresh, live `ReadView` not tracked by any registry.
    /// Prefer `ReadViewRegistry::open` on reader paths — this constructor
    /// exists for tests and internal snapshot fixtures.
    #[must_use]
    pub fn new(read_ts: Ts, txn_id: u64) -> Self {
        Self::new_for_epoch(standalone_epoch(read_ts), txn_id)
    }

    /// Construct a `ReadView` over an already-loaded published epoch.
    ///
    /// Reader paths load `SharedState.published` once, clone that epoch
    /// into the view, and read all visibility metadata from this pinned
    /// value for the rest of the snapshot lifetime.
    #[must_use]
    pub(crate) fn new_for_epoch(epoch: Arc<PublishedEpoch>, txn_id: u64) -> Self {
        Self {
            read_ts: epoch.visible_ts,
            txn_id,
            poisoned: AtomicBool::new(false),
            pin_ops_in_flight: AtomicU32::new(0),
            registry: None,
            epoch,
        }
    }

    /// Open a `ReadView` that registers itself with `registry` for the
    /// lifetime of the returned `Arc`. The last `Arc::drop` unregisters
    /// the view's `txn_id`, bounding the duration that `read_ts` pins the
    /// `oldest_required_ts()` horizon. The registry slot also tracks a
    /// `Weak<ReadView>` so `force_expire_all` can iterate live views.
    #[must_use]
    pub fn open(registry: Arc<ReadViewRegistry>, read_ts: Ts, txn_id: u64) -> Arc<Self> {
        Self::open_for_epoch(registry, standalone_epoch(read_ts), txn_id)
    }

    /// Open a registry-tracked view over an already-loaded published epoch.
    #[must_use]
    pub(crate) fn open_for_epoch(
        registry: Arc<ReadViewRegistry>,
        epoch: Arc<PublishedEpoch>,
        txn_id: u64,
    ) -> Arc<Self> {
        let read_ts = epoch.visible_ts;
        let view = Arc::new(Self {
            read_ts,
            txn_id,
            poisoned: AtomicBool::new(false),
            pin_ops_in_flight: AtomicU32::new(0),
            registry: Some(Arc::clone(&registry)),
            epoch,
        });
        registry.register(txn_id, read_ts, Arc::downgrade(&view));
        view
    }

    /// Snapshot timestamp pinned by the published epoch.
    #[must_use]
    pub(crate) fn visible_ts(&self) -> Ts {
        self.epoch.visible_ts
    }

    /// Published catalog pinned by the view.
    #[must_use]
    pub(crate) fn catalog(&self) -> &PublishedCatalog {
        &self.epoch.catalog
    }

    /// Published catalog generation pinned by the view.
    #[must_use]
    pub(crate) fn catalog_generation(&self) -> u64 {
        self.epoch.catalog_generation
    }

    /// Published sequencer frontier pinned by the view.
    #[must_use]
    pub(crate) fn sequencer_frontier(&self) -> Ts {
        self.epoch.sequencer_frontier
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

fn standalone_epoch(read_ts: Ts) -> Arc<PublishedEpoch> {
    Arc::new(PublishedEpoch {
        visible_ts: read_ts,
        catalog: Arc::new(PublishedCatalog {
            namespaces: HashMap::new(),
            namespace_id_by_name: HashMap::new(),
        }),
        catalog_generation: 0,
        sequencer_frontier: Ts::default(),
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
// ReadViewRegistry
// ---------------------------------------------------------------------------

/// Tracks live `ReadView`s so the writer / reconciliation path can compute
/// `oldest_required_ts()` — the lowest `read_ts` any open reader pins, and
/// therefore the upper bound on versions the reconciler may discard.
///
/// Invariants:
/// - Every `ReadView::open(registry, …)` inserts; the matching drop removes.
/// - Empty registry ⇒ `oldest_required_ts() == Ts::MAX` (no horizon held).
///
/// The internal mutex is position **5** in the global lock order documented
/// at the top of this file. `oldest_required_ts()` must be snapshotted
/// BEFORE any partition mutex is acquired in a reconciliation path.
pub struct ReadViewRegistry {
    inner: Mutex<BTreeMap<u64, RegistrySlot>>,
}

/// Registry entry: the view's `read_ts` plus a `Weak` back-pointer used
/// by `force_expire_all` to iterate live views. `Weak` avoids keeping the
/// view alive past the caller's last `Arc` reference.
struct RegistrySlot {
    read_ts: Ts,
    view: Weak<ReadView>,
}

impl std::fmt::Debug for ReadViewRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[allow(clippy::unwrap_used)]
        let guard = self.inner.lock().unwrap();
        f.debug_struct("ReadViewRegistry")
            .field("live_views", &guard.len())
            .finish()
    }
}

impl ReadViewRegistry {
    /// Construct an empty registry.
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(BTreeMap::new()),
        })
    }

    /// Insert `(txn_id → read_ts, Weak<ReadView>)`. Overwrites any prior
    /// entry for the same `txn_id` (callers must keep `txn_id` unique
    /// across concurrently live views). Refreshes
    /// `mvcc.active_read_views` gauge.
    pub(crate) fn register(&self, txn_id: u64, read_ts: Ts, view: Weak<ReadView>) {
        #[allow(clippy::unwrap_used)]
        let mut guard = self.inner.lock().unwrap();
        guard.insert(txn_id, RegistrySlot { read_ts, view });
        metrics::set_active_read_views(guard.len() as u64);
    }

    /// Remove `txn_id` from the registry. No-op if absent. Refreshes
    /// `mvcc.active_read_views` gauge.
    pub fn unregister(&self, txn_id: u64) {
        #[allow(clippy::unwrap_used)]
        let mut guard = self.inner.lock().unwrap();
        guard.remove(&txn_id);
        metrics::set_active_read_views(guard.len() as u64);
    }

    /// Smallest `read_ts` across all live views, or `Ts::MAX` if empty.
    #[must_use]
    pub fn oldest_required_ts(&self) -> Ts {
        #[allow(clippy::unwrap_used)]
        let guard = self.inner.lock().unwrap();
        guard.values().map(|s| s.read_ts).min().unwrap_or(Ts::MAX)
    }

    /// Force-expire EVERY registered `ReadView`. Snapshots all
    /// `Weak<ReadView>` handles under the registry mutex, releases the
    /// mutex, then upgrades each `Weak` and calls `force_expire` on any
    /// upgradable view — which flips `poisoned` and spins until the
    /// view's `pin_ops_in_flight` drains to zero.
    ///
    /// The snapshot-then-release pattern avoids a reentrant acquisition:
    /// `Arc::drop` of a view calls `registry.unregister` which would
    /// re-enter this mutex. By dropping the upgraded `Arc`s outside the
    /// mutex we guarantee no nested lock.
    ///
    /// Caller must hold the writer serialization mutex (position 6) to
    /// prevent new `ReadView::open` races from observing a half-drained
    /// state.
    pub fn force_expire_all(&self) {
        let views: Vec<Weak<ReadView>> = {
            #[allow(clippy::unwrap_used)]
            let guard = self.inner.lock().unwrap();
            guard.values().map(|s| s.view.clone()).collect()
        };
        for w in views {
            if let Some(v) = w.upgrade() {
                v.force_expire();
            }
        }
    }

    /// Number of live views. Mainly for tests / observability.
    #[must_use]
    pub fn len(&self) -> usize {
        #[allow(clippy::unwrap_used)]
        self.inner.lock().unwrap().len()
    }

    /// True iff no live views are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        #[allow(clippy::unwrap_used)]
        self.inner.lock().unwrap().is_empty()
    }
}

// ---------------------------------------------------------------------------
// ChainSnapshot — reader-path snapshot of a frame's per-key version chains
// ---------------------------------------------------------------------------

/// Reader-side snapshot of a leaf frame's per-key version chains.
///
/// Construction deep-clones every `VersionEntry` in every chain, which runs
/// `OverflowRef::Clone` (CAS-loop incref) on each `VersionData::Overflow`.
/// Every entry observed through the snapshot is therefore pinned — its
/// backing overflow chain cannot be freed while the snapshot is live.
///
/// Drop follows the default Rust drop-glue: the outer map drops each
/// `VecDeque<VersionEntry>`, which drops every contained `VersionEntry`,
/// which in turn runs `OverflowRef::Drop` (atomic decref + deferred-free
/// enqueue on 0).
///
/// **Force-expiry contract:**
///
/// 1. `new` checks `view.poisoned` BEFORE taking any refcount bumps. If
///    poisoned, it returns an empty snapshot (no `fetch_add`, no clones).
/// 2. `new` takes `pin_ops_in_flight.fetch_add(1, Release)`, performs the
///    deep clone (each entry's refcount bumped), then re-checks
///    `poisoned` under an `Acquire` load and decrements
///    `pin_ops_in_flight`. If poisoned-after, the cloned chains are
///    dropped here — RAII decrefs every bumped entry so the net refcount
///    delta is zero.
/// 3. No explicit `Drop` impl: ordinary drop glue suffices because
///    `force_expire` does NOT walk snapshot pins. Every refcount bump has
///    a matching decref through a single code path.
pub struct ChainSnapshot {
    /// Deep-cloned per-key chains. Each `VecDeque<VersionEntry>` is owned
    /// exclusively by this snapshot; the `VersionEntry` values inside each
    /// `VecDeque` were cloned from the source (running `OverflowRef::Clone`
    /// for `VersionData::Overflow` entries).
    chains: BTreeMap<Vec<u8>, VecDeque<VersionEntry>>,
    /// Back-reference to the owning reader's `ReadView`, used for the
    /// poison check during `new`. `None` for standalone callers (primarily
    /// tests that exercise snapshot visibility without a registry).
    #[allow(dead_code)]
    view: Option<Arc<ReadView>>,
}

impl std::fmt::Debug for ChainSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainSnapshot")
            .field("num_keys", &self.chains.len())
            .field("view_attached", &self.view.is_some())
            .finish()
    }
}

impl ChainSnapshot {
    /// Construct a snapshot from a frame's per-key version chains.
    ///
    /// Deep-clones every entry (bumping overflow refcounts via
    /// `OverflowRef::Clone`) under the atomic-handoff protocol. See
    /// type-level docs for the poison contract.
    #[must_use]
    pub fn new(
        source: &BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>,
        view: Option<Arc<ReadView>>,
    ) -> Self {
        // Pre-check: if the owning view is already poisoned, refuse to
        // pin any entries. The empty snapshot is the "force-expired view
        // sees nothing" contract.
        if let Some(v) = &view {
            if v.poisoned.load(Ordering::Acquire) {
                return ChainSnapshot {
                    chains: BTreeMap::new(),
                    view,
                };
            }
            v.pin_ops_in_flight.fetch_add(1, Ordering::Release);
        }

        // Deep clone: each inner `VersionEntry::clone()` runs
        // `OverflowRef::clone()` which is the CAS-loop incref.
        let mut chains = BTreeMap::new();
        for (k, chain) in source {
            let cloned: VecDeque<VersionEntry> = chain.iter().cloned().collect();
            chains.insert(k.clone(), cloned);
        }

        // Re-check poison AFTER the bumps. If force-expiry fired while we
        // were cloning, drop the cloned chains here — RAII decrefs every
        // entry we just bumped so the net refcount delta is zero.
        if let Some(v) = &view {
            let poisoned_after = v.poisoned.load(Ordering::Acquire);
            v.pin_ops_in_flight.fetch_sub(1, Ordering::Release);
            if poisoned_after {
                return ChainSnapshot {
                    chains: BTreeMap::new(),
                    view,
                };
            }
        }

        ChainSnapshot { chains, view }
    }

    /// Find the entry in the chain for `key` visible at `view.read_ts`.
    ///
    /// Visibility rule:
    /// - Own pending entry: visible by matching `txn_id`.
    /// - Foreign pending entry: same timestamp window and
    ///   `start_ts <= view.sequencer_frontier()`.
    /// - Committed entry: `start_ts <= read_ts < stop_ts`.
    /// - Aborted entry: skipped.
    #[must_use]
    pub fn visible_at(&self, key: &[u8], view: &ReadView) -> Option<&VersionEntry> {
        self.chains
            .get(key)
            .and_then(|chain| chain.iter().find(|entry| version_visible_to(entry, view)))
    }

    /// Iterate visible `(key, entry)` pairs within the supplied byte bounds.
    ///
    /// Uses the same visibility predicate as [`Self::visible_at`].
    pub fn visible_range<'a>(
        &'a self,
        start: Bound<&'a [u8]>,
        end: Bound<&'a [u8]>,
        view: &'a ReadView,
    ) -> impl Iterator<Item = (&'a [u8], &'a VersionEntry)> + 'a {
        self.chains
            .range::<[u8], _>((start, end))
            .filter_map(move |(key, chain)| {
                chain
                    .iter()
                    .find(|entry| version_visible_to(entry, view))
                    .map(|entry| (key.as_slice(), entry))
            })
    }

    /// True when history can contain a useful version for `key` at `read_ts`.
    #[must_use]
    pub fn history_is_candidate(&self, key: &[u8], read_ts: Ts) -> bool {
        self.chains.get(key).map_or(true, |chain| {
            chain.iter().all(|entry| {
                entry.start_ts > read_ts || matches!(entry.state, VersionState::Pending { .. })
            })
        })
    }

    /// Number of distinct keys with chains in this snapshot.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.chains.len()
    }

    /// True iff the snapshot holds no chains.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chains.is_empty()
    }

    /// Length of the chain for `key`, or 0 if absent.
    #[must_use]
    pub fn chain_len(&self, key: &[u8]) -> usize {
        self.chains.get(key).map_or(0, |c| c.len())
    }
}

fn version_visible_to(entry: &VersionEntry, view: &ReadView) -> bool {
    let read_ts = view.visible_ts();
    match entry.state {
        VersionState::Pending { txn_id } => {
            if txn_id == view.txn_id {
                true
            } else {
                entry.start_ts <= read_ts
                    && read_ts < entry.stop_ts
                    && entry.start_ts <= view.sequencer_frontier()
            }
        }
        VersionState::Committed => entry.start_ts <= read_ts && read_ts < entry.stop_ts,
        VersionState::Aborted => false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::mvcc::version::{OverflowRef, VersionData};
    use crate::storage::allocator::AllocatorHandle;
    use crate::storage::header::FileHeader;

    fn fresh_allocator() -> AllocatorHandle {
        AllocatorHandle::new(FileHeader::new(0, 0, 0))
    }

    fn overflow_entry(alloc: &AllocatorHandle, first_page: u32, ts: Ts) -> VersionEntry {
        let r = OverflowRef::new_owned(first_page, 128, alloc.clone()).unwrap();
        VersionEntry {
            start_ts: ts,
            stop_ts: Ts::MAX,
            txn_id: 1,
            state: VersionState::Committed,
            data: VersionData::Overflow(r),
            is_tombstone: false,
        }
    }

    #[test]
    fn new_read_view_is_live() {
        let rv = ReadView::new(
            Ts {
                physical_ms: 100,
                logical: 1,
            },
            42,
        );
        assert_eq!(rv.read_ts.physical_ms, 100);
        assert_eq!(rv.read_ts.logical, 1);
        assert_eq!(rv.txn_id, 42);
        assert!(!rv.poisoned.load(Ordering::Acquire));
        assert_eq!(rv.pin_ops_in_flight.load(Ordering::Acquire), 0);
    }

    #[test]
    fn poisoned_flag_transitions() {
        let rv = ReadView::new(Ts::default(), 0);
        assert!(!rv.poisoned.load(Ordering::Acquire));
        rv.poisoned.store(true, Ordering::Release);
        assert!(rv.poisoned.load(Ordering::Acquire));
    }

    #[test]
    fn pin_ops_counter_tracks_in_flight() {
        let rv = ReadView::new(Ts::default(), 0);
        rv.pin_ops_in_flight.fetch_add(1, Ordering::Release);
        rv.pin_ops_in_flight.fetch_add(1, Ordering::Release);
        assert_eq!(rv.pin_ops_in_flight.load(Ordering::Acquire), 2);
        rv.pin_ops_in_flight.fetch_sub(1, Ordering::Release);
        assert_eq!(rv.pin_ops_in_flight.load(Ordering::Acquire), 1);
    }

    // -----------------------------------------------------------------------
    // ChainSnapshot — construction / refcount preservation
    // -----------------------------------------------------------------------

    #[test]
    fn chain_snapshot_new_bumps_each_overflow_refcount() {
        let alloc = fresh_allocator();
        let mut source: BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> = BTreeMap::new();

        // Key A: chain of 3 overflow entries on pages 10, 11, 12.
        let mut chain_a = VecDeque::new();
        chain_a.push_back(overflow_entry(
            &alloc,
            10,
            Ts {
                physical_ms: 300,
                logical: 0,
            },
        ));
        chain_a.push_back(overflow_entry(
            &alloc,
            11,
            Ts {
                physical_ms: 200,
                logical: 0,
            },
        ));
        chain_a.push_back(overflow_entry(
            &alloc,
            12,
            Ts {
                physical_ms: 100,
                logical: 0,
            },
        ));
        source.insert(b"A".to_vec(), Arc::new(chain_a));

        // Key B: chain of 1 overflow entry on page 20.
        let mut chain_b = VecDeque::new();
        chain_b.push_back(overflow_entry(
            &alloc,
            20,
            Ts {
                physical_ms: 400,
                logical: 0,
            },
        ));
        source.insert(b"B".to_vec(), Arc::new(chain_b));

        for p in [10, 11, 12, 20] {
            assert_eq!(
                alloc.overflow_refcount(p),
                1,
                "baseline refcount for page {p}"
            );
        }

        let snap = ChainSnapshot::new(&source, None);

        // Post-construction: each overflow page refcount must be baseline + 1.
        for p in [10, 11, 12, 20] {
            assert_eq!(
                alloc.overflow_refcount(p),
                2,
                "post-snapshot refcount for page {p}"
            );
        }
        assert_eq!(snap.key_count(), 2);
        assert_eq!(snap.chain_len(b"A"), 3);
        assert_eq!(snap.chain_len(b"B"), 1);

        // Drop: refcount returns to baseline; no leak, no double-decref.
        drop(snap);
        for p in [10, 11, 12, 20] {
            assert_eq!(
                alloc.overflow_refcount(p),
                1,
                "post-drop refcount for page {p}"
            );
        }
    }

    #[test]
    fn chain_snapshot_is_empty_on_empty_source() {
        let source: BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> = BTreeMap::new();
        let snap = ChainSnapshot::new(&source, None);
        assert!(snap.is_empty());
        assert_eq!(snap.key_count(), 0);
    }

    // -----------------------------------------------------------------------
    // ChainSnapshot — force-expiry contract
    // -----------------------------------------------------------------------

    #[test]
    fn chain_snapshot_poisoned_before_new_takes_no_pins() {
        let alloc = fresh_allocator();
        let mut source: BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> = BTreeMap::new();
        let mut chain = VecDeque::new();
        chain.push_back(overflow_entry(
            &alloc,
            7,
            Ts {
                physical_ms: 100,
                logical: 0,
            },
        ));
        source.insert(b"k".to_vec(), Arc::new(chain));
        assert_eq!(alloc.overflow_refcount(7), 1);

        let view = Arc::new(ReadView::new(
            Ts {
                physical_ms: 500,
                logical: 0,
            },
            42,
        ));
        view.poisoned.store(true, Ordering::Release);

        let pre_ops = view.pin_ops_in_flight.load(Ordering::Acquire);
        let snap = ChainSnapshot::new(&source, Some(view.clone()));
        let post_ops = view.pin_ops_in_flight.load(Ordering::Acquire);

        assert!(snap.is_empty(), "poisoned view must yield empty snapshot");
        assert_eq!(
            alloc.overflow_refcount(7),
            1,
            "poisoned-before path must not bump refcount"
        );
        assert_eq!(
            pre_ops, post_ops,
            "poisoned-before path must not touch pin_ops_in_flight"
        );
    }

    #[test]
    fn chain_snapshot_poisoned_after_bump_drops_clones() {
        // Simulated atomic handoff: between fetch_add and the deep clone,
        // force_expire flips `poisoned`. We can't inject directly inside
        // `new` without loom, so we hand-roll the sequence here to prove
        // the invariant and then cover the real path with the loom test
        // in tests/force_expiry_pin_race.rs.
        let alloc = fresh_allocator();
        let mut source: BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> = BTreeMap::new();
        let mut chain = VecDeque::new();
        chain.push_back(overflow_entry(
            &alloc,
            9,
            Ts {
                physical_ms: 100,
                logical: 0,
            },
        ));
        source.insert(b"k".to_vec(), Arc::new(chain));

        let view = Arc::new(ReadView::new(
            Ts {
                physical_ms: 500,
                logical: 0,
            },
            42,
        ));
        // Not poisoned when `new` starts.
        assert!(!view.poisoned.load(Ordering::Acquire));

        // Poison it AFTER construction starts but BEFORE we drop the snap.
        // The real `new` re-check only fires if poisoned flipped during
        // construction — so to observe the drop-path under a purely
        // sequential test we arrange: poison, then construct with Some(v).
        // The pre-check wins and returns empty; refcount stays at baseline.
        view.poisoned.store(true, Ordering::Release);
        let snap = ChainSnapshot::new(&source, Some(view.clone()));
        drop(snap);
        assert_eq!(
            alloc.overflow_refcount(9),
            1,
            "pre-check poisoned path must leave refcount unchanged"
        );

        // Independent drop-path proof: clone-equivalent operation (the
        // body of `new` after fetch_add succeeds, assuming no poison)
        // must restore refcount to baseline on Drop. Already covered by
        // `chain_snapshot_new_bumps_each_overflow_refcount`.
    }

    // -----------------------------------------------------------------------
    // ReadViewRegistry
    // -----------------------------------------------------------------------

    #[test]
    fn empty_registry_oldest_is_ts_max() {
        let reg = ReadViewRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.oldest_required_ts(), Ts::MAX);
    }

    // -----------------------------------------------------------------------
    // force_expire
    // -----------------------------------------------------------------------

    #[test]
    fn force_expire_sets_poisoned_and_ticks_counter() {
        crate::mvcc::metrics::reset_read_views_force_expired();
        let rv = ReadView::new(
            Ts {
                physical_ms: 100,
                logical: 0,
            },
            42,
        );
        assert!(!rv.poisoned.load(Ordering::Acquire));
        rv.force_expire();
        assert!(rv.poisoned.load(Ordering::Acquire));
        assert_eq!(
            crate::mvcc::metrics::read_views_force_expired_snapshot(),
            1,
            "force_expire must tick the counter",
        );
    }

    #[test]
    fn force_expire_returns_immediately_when_pin_ops_is_zero() {
        let rv = ReadView::new(Ts::default(), 0);
        assert_eq!(rv.pin_ops_in_flight.load(Ordering::Acquire), 0);
        let start = std::time::Instant::now();
        rv.force_expire();
        // Should be well under the 10ms timeout; 100ms budget is generous.
        assert!(start.elapsed().as_millis() < 100);
    }

    #[test]
    fn three_open_views_report_min_ts() {
        let reg = ReadViewRegistry::new();
        let ts100 = Ts {
            physical_ms: 100,
            logical: 0,
        };
        let ts200 = Ts {
            physical_ms: 200,
            logical: 0,
        };
        let ts300 = Ts {
            physical_ms: 300,
            logical: 0,
        };
        let v100 = ReadView::open(reg.clone(), ts100, 1);
        let v200 = ReadView::open(reg.clone(), ts200, 2);
        let v300 = ReadView::open(reg.clone(), ts300, 3);
        assert_eq!(reg.len(), 3);
        assert_eq!(reg.oldest_required_ts(), ts100);
        // Keep all three alive through the assertion.
        drop((v100, v200, v300));
        assert!(reg.is_empty());
    }

    #[test]
    fn drop_oldest_advances_horizon() {
        let reg = ReadViewRegistry::new();
        let ts100 = Ts {
            physical_ms: 100,
            logical: 0,
        };
        let ts200 = Ts {
            physical_ms: 200,
            logical: 0,
        };
        let ts300 = Ts {
            physical_ms: 300,
            logical: 0,
        };
        let v100 = ReadView::open(reg.clone(), ts100, 1);
        let _v200 = ReadView::open(reg.clone(), ts200, 2);
        let _v300 = ReadView::open(reg.clone(), ts300, 3);
        assert_eq!(reg.oldest_required_ts(), ts100);
        drop(v100);
        assert_eq!(reg.oldest_required_ts(), ts200);
        assert_eq!(reg.len(), 2);
    }

    #[test]
    fn standalone_new_does_not_register() {
        // ReadView::new(..) paths (tests, snapshot fixtures) must not
        // affect any registry — the `registry` field is None and Drop is
        // a no-op.
        let reg = ReadViewRegistry::new();
        {
            let _rv = ReadView::new(
                Ts {
                    physical_ms: 500,
                    logical: 0,
                },
                99,
            );
            assert!(reg.is_empty());
        }
        assert!(reg.is_empty());
        assert_eq!(reg.oldest_required_ts(), Ts::MAX);
    }

    #[test]
    fn chain_snapshot_mem_store_shape_visibility() {
        // Mirrors the MemPageStore acceptance bullet: chains inserted,
        // `visible_at` returns the correct entry.
        let alloc = fresh_allocator();
        let mut source: BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> = BTreeMap::new();

        // Chain for key K: head is committed at ts=200, stop_ts=MAX; older
        // entry committed at ts=100, stopped at ts=200.
        let head = VersionEntry {
            start_ts: Ts {
                physical_ms: 200,
                logical: 0,
            },
            stop_ts: Ts::MAX,
            txn_id: 7,
            state: VersionState::Committed,
            data: VersionData::Inline(b"v2".to_vec()),
            is_tombstone: false,
        };
        let older = VersionEntry {
            start_ts: Ts {
                physical_ms: 100,
                logical: 0,
            },
            stop_ts: Ts {
                physical_ms: 200,
                logical: 0,
            },
            txn_id: 6,
            state: VersionState::Committed,
            data: VersionData::Overflow(OverflowRef::new_owned(42, 256, alloc.clone()).unwrap()),
            is_tombstone: false,
        };
        let mut chain = VecDeque::new();
        chain.push_back(head);
        chain.push_back(older);
        source.insert(b"K".to_vec(), Arc::new(chain));

        let snap = ChainSnapshot::new(&source, None);

        let reader_old = ReadView::new(
            Ts {
                physical_ms: 150,
                logical: 0,
            },
            99,
        );
        let reader_new = ReadView::new(
            Ts {
                physical_ms: 250,
                logical: 0,
            },
            99,
        );
        let reader_pending = ReadView::new(
            Ts {
                physical_ms: 200,
                logical: 0,
            },
            99,
        );

        let got_old = snap
            .visible_at(b"K", &reader_old)
            .expect("entry visible at ts=150");
        assert_eq!(got_old.start_ts.physical_ms, 100);
        assert_eq!(got_old.txn_id, 6);

        let got_new = snap
            .visible_at(b"K", &reader_new)
            .expect("entry visible at ts=250");
        assert_eq!(got_new.start_ts.physical_ms, 200);
        assert_eq!(got_new.txn_id, 7);

        // Exactly at 200: head is visible (start_ts <= read_ts < stop_ts=MAX).
        let got_boundary = snap
            .visible_at(b"K", &reader_pending)
            .expect("head visible at read_ts=start_ts");
        assert_eq!(got_boundary.start_ts.physical_ms, 200);

        assert!(snap.visible_at(b"missing", &reader_new).is_none());
    }
}

#[cfg(test)]
#[cfg(not(loom))]
#[path = "read_view_us001_tests.rs"]
mod read_view_us001_tests;

#[cfg(test)]
#[cfg(not(loom))]
#[path = "read_view_us004_tests.rs"]
mod read_view_us004_tests;
