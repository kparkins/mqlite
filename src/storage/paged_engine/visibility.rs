//! Writer-side visibility context for Phase 3 uniqueness plumbing.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::read_view::ReadView;
use crate::mvcc::transaction::WriteTxn;
use crate::storage::btree::{BTreePageStore, HistoryProbe};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::history_store::HistoryStore;
use crate::storage::root_snapshot::NamespaceId;

use super::snapshot_ops::{primary_history_probe, PrimaryHistoryProbe};
use super::state::{MetadataReadGuard, SharedState};

/// Writer-side visibility context built once per `run_write_existing` call.
///
/// The context pins one published epoch for the full Phase 3 write lifetime.
/// Downstream uniqueness and install helpers receive shared references to this
/// value instead of constructing their own read views.
pub(crate) struct WriteVisibility<'a> {
    pub(in crate::storage::paged_engine) read_view: Arc<ReadView>,
    pub(in crate::storage::paged_engine) ns_id: NamespaceId,
    pub(in crate::storage::paged_engine) primary_history:
        PrimaryHistoryProbe<'a, BufferPoolPageStore>,
    history_store: &'a std::sync::Mutex<HistoryStore<BufferPoolPageStore>>,
}

impl<'a> WriteVisibility<'a> {
    /// Build a writer visibility context for `ns`.
    ///
    /// The constructor performs exactly one published-epoch load through
    /// [`SharedState::load_published`], resolves the namespace against that
    /// epoch, and opens a [`ReadView`] over the same pinned epoch.
    ///
    /// # Arguments
    ///
    /// * `shared` - engine state that owns the published epoch and read-view
    ///   registry.
    /// * `ns` - namespace being written.
    ///
    /// # Returns
    ///
    /// A visibility context pinned to one published epoch.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CollectionNotFound`] when `ns` is absent from the
    /// current published catalog.
    pub(crate) fn new(shared: &'a SharedState, ns: &str) -> Result<Self> {
        #[cfg(test)]
        super::us008_tests::record_write_visibility_new();

        let epoch = shared.load_published();
        let snapshot = epoch
            .catalog
            .get_by_name(ns)
            .ok_or_else(|| Error::CollectionNotFound {
                name: ns.to_owned(),
            })?;
        let txn_id = shared
            .txn_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let read_view = ReadView::open_for_epoch(
            Arc::clone(shared.handle.read_view_registry()),
            Arc::clone(&epoch),
            txn_id,
        );
        let primary_history = primary_history_probe(shared, snapshot.id);
        Ok(Self {
            read_view,
            ns_id: snapshot.id,
            primary_history,
            history_store: &shared.history_store,
        })
    }

    /// Build a secondary-index history probe for one index in this namespace.
    #[must_use]
    pub(in crate::storage::paged_engine) fn secondary_history_probe(
        &self,
        index_id: i64,
    ) -> SecondaryHistoryProbe<'a, BufferPoolPageStore> {
        SecondaryHistoryProbe::new(self.history_store, self.ns_id, index_id)
    }

    /// Construct the Phase 5 writer visibility context.
    ///
    /// Phase 5 invariants for the eventual implementation:
    ///   1. `md_read` is a single `metadata.read()` guard held across identity
    ///      resolution and `ReadView` construction.
    ///   2. The `ReadView` is opened at the captured published epoch's
    ///      `visible_ts`.
    ///   3. The published epoch consumed here is the same one used for identity
    ///      resolution in the surrounding writer body.
    ///   4. The returned context is dropped before commit timestamp allocation
    ///      at §10.16 S4.
    ///
    /// Phase 3 does not call this constructor; it uses [`WriteVisibility::new`]
    /// and holds that context across S2-S12.
    ///
    /// # Arguments
    ///
    /// * `shared` - engine state used by the eventual Phase 5 implementation.
    /// * `md_read` - single metadata read guard for identity resolution.
    /// * `txn` - writer transaction whose identity will bind the view.
    ///
    /// # Returns
    ///
    /// The Phase 5 writer visibility context.
    ///
    /// # Panics
    ///
    /// Always panics in Phase 3 because the metadata-guard protocol belongs to
    /// Phase 5.
    #[must_use]
    #[allow(
        dead_code,
        clippy::panic,
        reason = "Phase 5 placeholder; current writer path must use WriteVisibility::new"
    )]
    pub(crate) fn for_writer(
        shared: &'a SharedState,
        md_read: MetadataReadGuard<'a>,
        txn: &WriteTxn,
    ) -> Self {
        let _ = (shared, md_read, txn);
        unimplemented!(
            "Phase 5 metadata-guard protocol; Phase 3 uses WriteVisibility::new \
             which is held S2-S12"
        )
    }
}

/// Secondary-index history probe placeholder for Phase 4 spill support.
pub(in crate::storage::paged_engine) struct SecondaryHistoryProbe<'a, S: BTreePageStore> {
    store: &'a std::sync::Mutex<HistoryStore<S>>,
    collection_id: i64,
    index_id: i64,
}

impl<'a, S: BTreePageStore> SecondaryHistoryProbe<'a, S> {
    /// Create a secondary history probe for one secondary-index kind tag.
    ///
    /// # Arguments
    ///
    /// * `store` - history-store mutex.
    /// * `collection_id` - durable collection identifier.
    /// * `index_id` - durable secondary index identifier.
    ///
    /// # Returns
    ///
    /// A history probe for one secondary index.
    #[must_use]
    pub(in crate::storage::paged_engine) fn new(
        store: &'a std::sync::Mutex<HistoryStore<S>>,
        collection_id: i64,
        index_id: i64,
    ) -> Self {
        Self {
            store,
            collection_id,
            index_id,
        }
    }
}

impl<S: BTreePageStore> HistoryProbe for SecondaryHistoryProbe<'_, S> {
    fn probe(
        &self,
        key: &[u8],
        read_ts: crate::mvcc::timestamp::Ts,
    ) -> Result<Option<crate::mvcc::version::VersionEntry>> {
        let guard = self
            .store
            .lock()
            .map_err(|_| Error::Internal("history_store mutex poisoned".into()))?;
        guard.probe_sec_index(self.collection_id, self.index_id, key, read_ts)
    }
}
