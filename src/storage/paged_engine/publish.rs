//! Publish-decision contract: `PublishDirty`, `build_published_catalog`,
//! and the canonical `publish_commit` helper.
//!
//! Phase 1 draws an explicit line between "reader-visible published
//! metadata changed" (`published_catalog_dirty`) and "the on-disk
//! catalog tree or file header changed" (`catalog_header_dirty`). The
//! decision is set **where the mutation is known**, not guessed at
//! publish time. See §4 and §10.2 of
//! `docs/STORAGE-UPGRADE-PHASE-01-READ-EPOCH.md`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::timestamp::Ts;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::Catalog;
use crate::storage::root_snapshot::{
    NamespaceSnapshot, PublishedCatalog, PublishedIndex, ReadEpoch,
};

use super::state::SharedState;

// ---------------------------------------------------------------------------
// PublishDirty
// ---------------------------------------------------------------------------

/// Dirty-flag pair threaded from mutation sites through commit into the
/// publish step.
///
/// Root-neutral CRUD leaves both flags clear; DDL and root-moving CRUD
/// set at least one. `published_catalog_dirty` forces a fresh
/// `Arc<PublishedCatalog>` to be built; `catalog_header_dirty` triggers
/// `sync_catalog_root_overlay` independently of the publish step (§4.4).
#[derive(Default, Copy, Clone, Debug)]
pub(crate) struct PublishDirty {
    /// Reader-visible published metadata changed. Forces a new
    /// `Arc<PublishedCatalog>` at publish time.
    pub published_catalog_dirty: bool,
    /// The on-disk catalog tree or file header changed, but the
    /// reader-visible contract did not. Triggers
    /// `sync_catalog_root_overlay` without rebuilding `PublishedCatalog`.
    pub catalog_header_dirty: bool,
}

impl PublishDirty {
    /// Mark the reader-visible catalog dirty (a new `PublishedCatalog`
    /// must be built at publish time).
    pub(crate) fn mark_published(&mut self) {
        self.published_catalog_dirty = true;
    }

    /// Mark the on-disk catalog header dirty (`sync_catalog_root_overlay`
    /// must run, but the published catalog Arc may be reused).
    pub(crate) fn mark_header(&mut self) {
        self.catalog_header_dirty = true;
    }

    /// Merge two dirty states with bitwise-OR semantics on both fields.
    #[allow(dead_code)]
    pub(crate) fn merge(&mut self, other: PublishDirty) {
        self.published_catalog_dirty |= other.published_catalog_dirty;
        self.catalog_header_dirty |= other.catalog_header_dirty;
    }
}

// ---------------------------------------------------------------------------
// build_published_catalog — full rebuild from a live Catalog
// ---------------------------------------------------------------------------

/// Build a fresh `PublishedCatalog` from the current catalog contents.
///
/// Populates both the id-keyed namespaces map and the name → id sidecar
/// from the `id` field on each `CollectionEntry` / `IndexEntry`. Only
/// the fields that the read path consumes today are copied; non-published
/// fields (`document_count`, `avg_doc_size`, `multikey`, `entry_count`)
/// stay on the live catalog.
pub(crate) fn build_published_catalog(
    catalog: &Catalog<BufferPoolPageStore>,
) -> Result<PublishedCatalog> {
    let collections = catalog.list_collections()?;
    let mut namespaces: HashMap<i64, NamespaceSnapshot> = HashMap::with_capacity(collections.len());
    let mut namespace_id_by_name: HashMap<String, i64> = HashMap::with_capacity(collections.len());
    for coll in collections {
        let indexes = catalog.list_indexes(&coll.name)?;
        let mut idxs = Vec::with_capacity(indexes.len());
        for idx in indexes {
            idxs.push(PublishedIndex {
                id: idx.id,
                name: idx.name.clone(),
                root_page: idx.root_page,
                root_level: idx.root_level,
                key_pattern: idx.key_pattern.clone(),
                unique: idx.unique,
                sparse: idx.sparse,
                state: idx.state,
            });
        }
        namespace_id_by_name.insert(coll.name.clone(), coll.id);
        namespaces.insert(
            coll.id,
            NamespaceSnapshot {
                id: coll.id,
                name: coll.name.clone(),
                data_root_page: coll.data_root_page,
                data_root_level: coll.data_root_level,
                indexes: idxs,
            },
        );
    }
    Ok(PublishedCatalog {
        namespaces,
        namespace_id_by_name,
    })
}

// ---------------------------------------------------------------------------
// publish_commit — canonical publish helper
// ---------------------------------------------------------------------------

/// Canonical publish helper. Either builds a fresh `Arc<PublishedCatalog>`
/// (when `dirty.published_catalog_dirty`) or clones the previous Arc
/// (root-neutral), wraps it in a new `ReadEpoch` with the caller-provided
/// `visible_ts`, and atomically stores it on `shared.published`.
///
/// `visible_ts` selection rule (§6.3), enforced at every call site:
///   - if the txn had primary writes: pass the allocated `commit_ts`
///   - otherwise (metadata-only DDL): pass `shared.oracle.commit()?`
///     — NEVER `oracle.now()`; that peek can return equal Ts across two
///     sub-millisecond calls, violating strict visible-ts monotonicity.
pub(crate) fn publish_commit(
    shared: &SharedState,
    catalog: &Catalog<BufferPoolPageStore>,
    visible_ts: Ts,
    dirty: PublishDirty,
) -> Result<Arc<ReadEpoch>> {
    let prev = shared.published.load_full();
    debug_assert!(
        visible_ts > prev.visible_ts,
        "visible_ts must be strictly monotonic; caller must use commit_ts or oracle.commit()"
    );
    let catalog_arc = if dirty.published_catalog_dirty {
        Arc::new(build_published_catalog(catalog)?)
    } else {
        Arc::clone(&prev.catalog)
    };
    let new_epoch = Arc::new(ReadEpoch {
        visible_ts,
        catalog: catalog_arc,
    });
    shared.published.store(Arc::clone(&new_epoch));
    // Phase 1 §10.1: advance `catalog_gen` on every rebuild publish.
    // Callers run under the commit_seq envelope (which serializes
    // publishes) — a plain `fetch_add(1, Release)` is sufficient.
    // Readers load with `Acquire` without holding `metadata.read()`;
    // the Phase 5 §10.17.1 / §10.21 sequencer's catalog-revalidation
    // path depends on this counter being strictly monotonic across
    // rebuilds and unchanged across epoch-only (root-neutral) publishes.
    if dirty.published_catalog_dirty {
        shared
            .catalog_gen
            .fetch_add(1, std::sync::atomic::Ordering::Release);
    }
    // Phase 1 §10.10 counters (US-012). Ticked after the atomic
    // publish so readers observing them cannot see a state where a
    // publish "has happened" according to the counter but not the
    // `ArcSwap`.
    crate::mvcc::metrics::record_read_epoch_publish();
    if dirty.published_catalog_dirty {
        crate::mvcc::metrics::record_published_catalog_rebuild();
    }
    if dirty.catalog_header_dirty {
        crate::mvcc::metrics::record_catalog_header_sync();
    }
    if !dirty.published_catalog_dirty && !dirty.catalog_header_dirty {
        crate::mvcc::metrics::record_root_neutral_commit();
    }
    Ok(new_epoch)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_dirty_default_is_clear() {
        let d = PublishDirty::default();
        assert!(!d.published_catalog_dirty);
        assert!(!d.catalog_header_dirty);
    }

    #[test]
    fn mark_published_sets_only_published_bit() {
        let mut d = PublishDirty::default();
        d.mark_published();
        assert!(d.published_catalog_dirty);
        assert!(!d.catalog_header_dirty);
    }

    #[test]
    fn mark_header_sets_only_header_bit() {
        let mut d = PublishDirty::default();
        d.mark_header();
        assert!(!d.published_catalog_dirty);
        assert!(d.catalog_header_dirty);
    }

    #[test]
    fn merge_is_bitwise_or() {
        let mut d = PublishDirty {
            published_catalog_dirty: false,
            catalog_header_dirty: true,
        };
        let other = PublishDirty {
            published_catalog_dirty: true,
            catalog_header_dirty: false,
        };
        d.merge(other);
        assert!(d.published_catalog_dirty);
        assert!(d.catalog_header_dirty);
    }

    #[test]
    fn merge_preserves_already_set_bits() {
        let mut d = PublishDirty {
            published_catalog_dirty: true,
            catalog_header_dirty: true,
        };
        let other = PublishDirty::default();
        d.merge(other);
        assert!(d.published_catalog_dirty);
        assert!(d.catalog_header_dirty);
    }
}
