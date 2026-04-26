//! Published read-side state: `ReadEpoch` and `PublishedCatalog`.
//!
//! Readers load a single `Arc<ReadEpoch>` atomically via `ArcSwap::load()`
//! and observe both `visible_ts` (used to open a `ReadView`) and `catalog`
//! (used to resolve namespace + index roots) through the same guard.
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
/// The `id` field is the durable identity allocated by §10.7; `name` is
/// kept alongside for logging and MongoDB-compatible lookups but is not a
/// stable key.
#[derive(Clone)]
pub(crate) struct NamespaceSnapshot {
    // `id` / `name` are part of the §10.1 published contract surface
    // (consumed by `id_for_name` + `get_by_id`); suppress dead_code so
    // the Phase 5 sequencer work can pick them up without churn.
    #[allow(dead_code)]
    pub id: NamespaceId,
    #[allow(dead_code)]
    pub name: String,
    pub data_root_page: u32,
    pub data_root_level: u8,
    pub indexes: Vec<PublishedIndex>,
}

/// Stable fields of an `IndexEntry` as of the published catalog. The `id`
/// field is the durable identity allocated by §10.7.
#[derive(Clone)]
pub(crate) struct PublishedIndex {
    // `id` is part of the §10.1 published contract surface; suppress
    // dead_code until Phase 5 consumers land.
    #[allow(dead_code)]
    pub id: IndexId,
    pub name: String,
    pub root_page: u32,
    pub root_level: u8,
    pub key_pattern: Document,
    pub unique: bool,
    pub sparse: bool,
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
}

impl PublishedCatalog {
    /// Look up a namespace snapshot by name. Two map hits (O(1)).
    pub(crate) fn get_by_name(&self, name: &str) -> Option<&NamespaceSnapshot> {
        self.namespace_id_by_name
            .get(name)
            .and_then(|id| self.namespaces.get(id))
    }

    /// Look up a namespace snapshot by durable id.
    #[allow(dead_code)]
    pub(crate) fn get_by_id(&self, id: NamespaceId) -> Option<&NamespaceSnapshot> {
        self.namespaces.get(&id)
    }

    /// Resolve a name to its durable namespace id. One-line accessor over
    /// `namespace_id_by_name`; Phase 4/5 consumers depend on this method
    /// being present from Phase 1 onward.
    #[allow(dead_code)]
    pub(crate) fn id_for_name(&self, name: &str) -> Option<NamespaceId> {
        self.namespace_id_by_name.get(name).copied()
    }
}

/// Atomically published read epoch. The outer object is what `ArcSwap`
/// swaps; both fields must be observed through a single guard.
pub(crate) struct ReadEpoch {
    pub visible_ts: Ts,
    pub catalog: Arc<PublishedCatalog>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_catalog() -> PublishedCatalog {
        let mut namespaces = HashMap::new();
        let mut by_name = HashMap::new();
        for (id, name) in [(1i64, "db.a"), (2i64, "db.b"), (7i64, "db.gap")] {
            namespaces.insert(
                id,
                NamespaceSnapshot {
                    id,
                    name: name.to_owned(),
                    data_root_page: (id as u32) * 100,
                    data_root_level: 0,
                    indexes: Vec::new(),
                },
            );
            by_name.insert(name.to_owned(), id);
        }
        PublishedCatalog {
            namespaces,
            namespace_id_by_name: by_name,
        }
    }

    /// §10.1 / §11 #11 — `id_for_name` resolves known names to their
    /// durable id via the name sidecar.
    #[test]
    fn id_for_name_returns_sidecar_id_for_known_name() {
        let cat = mk_catalog();
        assert_eq!(cat.id_for_name("db.a"), Some(1));
        assert_eq!(cat.id_for_name("db.b"), Some(2));
        assert_eq!(cat.id_for_name("db.gap"), Some(7));
    }

    /// §11 #11 — `id_for_name` returns `None` for unknown names.
    #[test]
    fn id_for_name_returns_none_for_unknown_name() {
        let cat = mk_catalog();
        assert_eq!(cat.id_for_name("db.missing"), None);
        assert_eq!(cat.id_for_name(""), None);
    }

    /// `id_for_name` is consistent with `get_by_name` / `get_by_id`.
    #[test]
    fn id_for_name_round_trips_through_namespaces_map() {
        let cat = mk_catalog();
        let id = cat.id_for_name("db.a").expect("db.a resolves");
        let ns = cat.get_by_id(id).expect("id in namespaces map");
        assert_eq!(ns.name, "db.a");
        assert_eq!(ns.id, id);

        let ns2 = cat.get_by_name("db.a").expect("name in sidecar");
        assert_eq!(ns2.id, id);
    }
}
