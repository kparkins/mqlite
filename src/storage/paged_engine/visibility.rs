//! Writer-side MVCC visibility context and history probes.

use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::mvcc::read_view::ReadView;
use crate::storage::btree::{BTreePageStore, HistoryProbe};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::history_store::HistoryStore;
use crate::storage::root_snapshot::NamespaceId;

use super::snapshot_ops::{primary_history_probe, PrimaryHistoryProbe};
use super::state::SharedState;

/// Writer-side visibility context built once per `run_write_commit_envelope` call.
///
/// The context pins one published epoch for the full Phase 3 write lifetime.
/// Downstream uniqueness and install helpers receive shared references to this
/// value instead of constructing their own read views.
pub(crate) struct WriteVisibility<'a> {
    pub(in crate::storage::paged_engine) read_view: Arc<ReadView>,
    pub(in crate::storage::paged_engine) ns_id: NamespaceId,
    pub(in crate::storage::paged_engine) primary_history:
        PrimaryHistoryProbe<'a, BufferPoolPageStore>,
    history_store: &'a Mutex<HistoryStore<BufferPoolPageStore>>,
}

impl<'a> WriteVisibility<'a> {
    /// Build a writer visibility context for `ns`.
    ///
    /// The constructor performs exactly one coherent published-epoch
    /// load through [`SharedState::load_published_coherent`] (US-037
    /// §10.19 C-1), resolves the namespace against that epoch, and
    /// opens a [`ReadView`] over the same pinned epoch.
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
        super::write_visibility_epoch::record_write_visibility_new();

        // §10.19 C-1 / US-037: load the (epoch, sequencer-frontier) pair
        // coherently so foreign-Pending visibility evaluated through this
        // view's `sequencer_frontier()` cannot see a frontier behind the
        // epoch's `visible_ts`.
        let epoch = shared.load_published_coherent();
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
            Arc::clone(&shared.publish_sequencer),
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
        SecondaryHistoryProbe {
            store: self.history_store,
            collection_id: self.ns_id,
            index_id,
        }
    }
}

/// Secondary-index history probe for one namespace/index pair.
pub(in crate::storage::paged_engine) struct SecondaryHistoryProbe<'a, S: BTreePageStore> {
    store: &'a Mutex<HistoryStore<S>>,
    collection_id: i64,
    index_id: i64,
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
