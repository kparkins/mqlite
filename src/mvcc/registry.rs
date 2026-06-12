//! `ReadViewRegistry` — tracks live `ReadView`s so the writer / reconciliation
//! path can compute `oldest_required_ts()`.
//!
//! The registry mutex is position **5** in the database-wide lock order
//! documented at the top of [`crate::mvcc::read_view`].

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use crate::mvcc::metrics;
use crate::mvcc::read_view::ReadView;
use crate::mvcc::timestamp::Ts;

/// Tracks live `ReadView`s so the writer / reconciliation path can compute
/// `oldest_required_ts()` — the lowest `read_ts` any open reader pins, and
/// therefore the upper bound on versions the reconciler may discard.
///
/// Invariants:
/// - Every `ReadView::open_for_epoch(registry, …)` (or the test-only
///   `open_frontier_pinned_for_tests`) inserts; the matching drop removes.
/// - Empty registry ⇒ `oldest_required_ts() == Ts::MAX` (no horizon held).
///
/// The internal mutex is position **5** in the global lock order documented
/// at the top of `read_view.rs`. `oldest_required_ts()` must be snapshotted
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

    /// Raise a registered view's pinned `read_ts` from a conservative floor
    /// up to its real snapshot timestamp.
    ///
    /// ITEM 1 — load-to-register prune race. `open_snapshot_read_view`
    /// registers a new reader at the conservative floor `Ts::default()`
    /// BEFORE it loads the published epoch, so the reader instantly pins the
    /// entire reclaim horizon and no concurrent reconcile/eviction prune can
    /// drop a resident superseded version the reader still needs. Once
    /// registered (and so visible to every subsequent `oldest_required_ts()`
    /// snapshot), the reader calls this to raise its slot to the real
    /// `read_ts`, releasing the over-broad pin so it does not stall
    /// reclamation for its whole lifetime.
    ///
    /// `new_read_ts` MUST be `>= the value the slot was registered with`
    /// (the refine only ever raises the floor); the call is a no-op if the
    /// `txn_id` is no longer registered (the view was dropped concurrently).
    pub(crate) fn refine_read_ts(&self, txn_id: u64, new_read_ts: Ts) {
        #[allow(clippy::unwrap_used)]
        let mut guard = self.inner.lock().unwrap();
        if let Some(slot) = guard.get_mut(&txn_id) {
            debug_assert!(
                new_read_ts >= slot.read_ts,
                "refine_read_ts must only raise the pinned floor (was {:?}, new {:?})",
                slot.read_ts,
                new_read_ts
            );
            slot.read_ts = new_read_ts;
        }
    }

    /// Attach the now-constructed `Weak<ReadView>` to a pre-pinned slot and
    /// refine its `read_ts` up to the real snapshot timestamp, in a single
    /// registry-mutex acquisition.
    ///
    /// ITEM 1 — load-to-register prune race (option a). The pin must precede
    /// the epoch load, so `open_snapshot_read_view` first inserts a
    /// conservative `Ts::default()` slot with a placeholder `Weak::new()`
    /// (see [`Self::register`]) BEFORE the view — and therefore the epoch the
    /// view pins — exists. Once the view is built, this call publishes the
    /// real back-pointer (so `force_expire_all` can find the view) and raises
    /// the slot to the real `read_ts`, in one lock so a concurrent
    /// reconcile/eviction prune observes either the conservative floor or the
    /// refined `read_ts` — never an inconsistent middle state, and never a
    /// floor above a version this reader needs.
    ///
    /// Net registry-mutex ops for an open are exactly two — the conservative
    /// `register` plus this attach-and-refine — matching the prior
    /// `register` + `refine_read_ts` shape; this only MOVES the pin earlier.
    ///
    /// `new_read_ts` MUST be `>=` the slot's current floor (the refine only
    /// raises it). No-op if the `txn_id` is no longer registered.
    pub(crate) fn attach_view_and_refine(
        &self,
        txn_id: u64,
        new_read_ts: Ts,
        view: Weak<ReadView>,
    ) {
        #[allow(clippy::unwrap_used)]
        let mut guard = self.inner.lock().unwrap();
        if let Some(slot) = guard.get_mut(&txn_id) {
            debug_assert!(
                new_read_ts >= slot.read_ts,
                "attach_view_and_refine must only raise the pinned floor (was {:?}, new {:?})",
                slot.read_ts,
                new_read_ts
            );
            slot.read_ts = new_read_ts;
            slot.view = view;
        }
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
    /// The sole caller is the `drop_namespace` DDL barrier, which holds
    /// the engine's `metadata.write()` fence and the dropped namespace's
    /// writer-admission gate before invoking this. That exclusivity — not
    /// the publish sequencer mutex — is what prevents a new
    /// `ReadView::open_for_epoch` on the dropped namespace from racing in and
    /// observing a half-drained state; readers that opened a view BEFORE
    /// the barrier are force-expired here, and any view opened against the
    /// stale pre-drop epoch afterward is handled by deferred page
    /// retirement rather than by this method.
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
