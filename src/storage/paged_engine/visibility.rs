//! Writer-side visibility context for Phase 3 uniqueness plumbing.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::read_view::ReadView;
use crate::mvcc::transaction::WriteTxn;
use crate::storage::btree::{BTreePageStore, HistoryProbe};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::history_store::{HistoryStore, KIND_SEC_INDEX_BASE};
use crate::storage::root_snapshot::NamespaceId;

use super::snapshot_ops::{primary_history_probe, PrimaryHistoryProbe};
use super::state::{MetadataReadGuard, SharedState};

/// Writer-side visibility context built once per `run_write_existing` call.
///
/// The context pins one published epoch for the full Phase 3 write lifetime.
/// Downstream uniqueness and install helpers receive shared references to this
/// value instead of constructing their own read views.
#[allow(dead_code)]
pub(crate) struct WriteVisibility<'a> {
    pub(in crate::storage::paged_engine) read_view: Arc<ReadView>,
    pub(in crate::storage::paged_engine) ns_id: NamespaceId,
    pub(in crate::storage::paged_engine) primary_history:
        PrimaryHistoryProbe<'a, BufferPoolPageStore>,
    pub(in crate::storage::paged_engine) secondary_history:
        Option<SecondaryHistoryProbe<'a, BufferPoolPageStore>>,
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
        let primary_history = primary_history_probe(shared, ns);
        Ok(Self {
            read_view,
            ns_id: snapshot.id,
            primary_history,
            secondary_history: None,
        })
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
    #[allow(dead_code)]
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
#[allow(dead_code)]
pub(in crate::storage::paged_engine) struct SecondaryHistoryProbe<'a, S: BTreePageStore> {
    store: &'a std::sync::Mutex<HistoryStore<S>>,
    ns_id: u32,
    kind_tag: u8,
}

#[allow(dead_code)]
impl<'a, S: BTreePageStore> SecondaryHistoryProbe<'a, S> {
    /// Create a secondary history probe for one secondary-index kind tag.
    ///
    /// Phase 3 keeps [`WriteVisibility::secondary_history`] as `None`; this
    /// constructor exists so the field has the Phase 4-ready type.
    ///
    /// # Arguments
    ///
    /// * `store` - history-store mutex.
    /// * `ns_id` - history-store namespace partition.
    /// * `secondary_ordinal` - ordinal added to the secondary kind-tag base.
    ///
    /// # Returns
    ///
    /// A history probe for one secondary index.
    #[must_use]
    pub(in crate::storage::paged_engine) fn new(
        store: &'a std::sync::Mutex<HistoryStore<S>>,
        ns_id: u32,
        secondary_ordinal: u8,
    ) -> Self {
        Self {
            store,
            ns_id,
            kind_tag: KIND_SEC_INDEX_BASE.saturating_add(secondary_ordinal),
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
        guard.probe_sec_index(self.ns_id, key, self.kind_tag, read_ts)
    }
}
