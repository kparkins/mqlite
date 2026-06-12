//! Published read-side state: `PublishedEpoch` and `PublishedCatalog`.
//!
//! Readers load a single `Arc<PublishedEpoch>` atomically via
//! `ArcSwap::load()` and observe visibility metadata plus the catalog
//! through the same guard.
//! Writers publish a new epoch on commit by `ArcSwap::store()`.
//!
//! See `docs/STORAGE-UPGRADE-PHASE-01-READ-EPOCH.md` §3.1 and §10.1 for the
//! invariants; in particular:
//!
//! - single atomic load per read operation
//! - root-neutral reuse is *physical* reuse — the new epoch carries the
//!   same `Arc<PublishedCatalog>` allocation as the prior epoch
//! - the payload is intentionally narrow: only fields the read path
//!   consumes today

use std::collections::HashMap;
use std::sync::Arc;

use bson::Document;

use crate::mvcc::timestamp::Ts;
use crate::storage::catalog::IndexState;

/// Durable namespace identifier (§10.7). Allocated from the file header
/// `next_namespace_id` counter. Stable across splits and renames.
pub(crate) type NamespaceId = i64;

/// Durable index identifier (§10.7). Allocated from the file header
/// `next_index_id` counter. Stable across splits.
pub(crate) type IndexId = i64;

/// Root-of-tree metadata for one namespace.
///
/// The `id` field is the durable identity allocated by §10.7.
#[derive(Clone)]
pub(crate) struct NamespaceSnapshot {
    pub id: NamespaceId,
    pub data_root_page: u32,
    pub data_root_level: u8,
    pub indexes: Vec<PublishedIndex>,
}

/// Stable read-path fields of an `IndexEntry` as of the published catalog.
#[derive(Clone)]
pub(crate) struct PublishedIndex {
    pub id: IndexId,
    pub name: String,
    pub root_page: u32,
    pub root_level: u8,
    pub key_pattern: Document,
    pub unique: bool,
    pub sparse: bool,
    /// Partial-index filter expression (MongoDB `partialFilterExpression`).
    /// `None` for ordinary indexes. The write path gates entry maintenance on
    /// this filter; the planner only selects the index when the query filter
    /// syntactically covers it.
    pub partial_filter_expression: Option<Document>,
    /// TTL expiry in seconds (MongoDB `expireAfterSeconds`). `None` for non-TTL
    /// indexes. The TTL sweep reads this from the published snapshot to find
    /// candidate documents to expire.
    pub expire_after_seconds: Option<i64>,
    /// Lifecycle state. Query planning must skip any index whose state
    /// is not `Ready` — the contents may be incomplete.
    pub state: IndexState,
}

/// Reader-visible catalog. Narrow by design: only fields the read path
/// actually consumes today.
///
/// Fields explicitly excluded from the published payload: `document_count`,
/// `avg_doc_size` on collections; `multikey`, `entry_count` on indexes.
/// These stay on the live `Catalog`. See §3.6 / §10.11 of
/// `docs/STORAGE-UPGRADE-PHASE-01-READ-EPOCH.md`.
#[derive(Clone)]
pub(crate) struct PublishedCatalog {
    /// Id-keyed primary map. Every lookup is at worst two map hits (O(1)).
    pub namespaces: HashMap<NamespaceId, NamespaceSnapshot>,
    /// Name → id sidecar map. Name-based lookups resolve through
    /// `namespace_id_by_name` → `namespaces`.
    pub namespace_id_by_name: HashMap<String, NamespaceId>,
    /// Index id → owning namespace id sidecar for lock-free secondary
    /// reconcile dirty marking.
    pub index_owner_by_id: HashMap<IndexId, NamespaceId>,
}

impl PublishedCatalog {
    /// Look up a namespace snapshot by name. Two map hits (O(1)).
    pub(crate) fn get_by_name(&self, name: &str) -> Option<&NamespaceSnapshot> {
        self.namespace_id_by_name
            .get(name)
            .and_then(|id| self.namespaces.get(id))
    }
}

/// Atomically published read epoch. The outer object is what `ArcSwap`
/// swaps; all fields must be observed through a single guard.
///
/// US-037 removed the duplicated `sequencer_frontier` snapshot from this
/// type; readers now load the live frontier from `PublishSequencer`
/// through `ReadView::sequencer_frontier()` (§10.19 C-1).
pub(crate) struct PublishedEpoch {
    pub visible_ts: Ts,
    pub catalog: Arc<PublishedCatalog>,
    pub catalog_generation: u64,
}

#[cfg(test)]
#[path = "tests/root_snapshot.rs"]
mod tests;
