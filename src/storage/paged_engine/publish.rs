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
    NamespaceSnapshot, PublishedCatalog, PublishedEpoch, PublishedIndex,
};
use crate::storage::structural_page_batch::StructuralPageBatch;

use super::state::{MetadataState, SharedState};

// ---------------------------------------------------------------------------
// PublishDirty
// ---------------------------------------------------------------------------

/// Dirty-flag pair threaded from mutation sites through commit into the
/// publish step.
///
/// Root-neutral CRUD leaves both flags clear; DDL and root-moving CRUD
/// set at least one. `published_catalog_dirty` forces a fresh
/// `Arc<PublishedCatalog>` to be built; `catalog_header_dirty` tracks a
/// catalog-root header owner update independently of the publish step (§4.4).
#[derive(Default, Copy, Clone, Debug)]
pub(crate) struct PublishDirty {
    /// Reader-visible published metadata changed. Forces a new
    /// `Arc<PublishedCatalog>` at publish time.
    pub published_catalog_dirty: bool,
    /// The on-disk catalog tree or file header changed, but the
    /// reader-visible contract did not. Tracks a catalog-root header owner
    /// update without rebuilding `PublishedCatalog`.
    pub catalog_header_dirty: bool,
}

impl PublishDirty {
    /// Mark the reader-visible catalog dirty (a new `PublishedCatalog`
    /// must be built at publish time).
    pub(crate) fn mark_published(&mut self) {
        self.published_catalog_dirty = true;
    }

    /// Mark the on-disk catalog header dirty (the catalog-root header owner
    /// must run, but the published catalog Arc may be reused).
    pub(crate) fn mark_header(&mut self) {
        self.catalog_header_dirty = true;
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
    let mut index_owner_by_id: HashMap<i64, i64> = HashMap::new();
    for coll in collections {
        let indexes = catalog.list_indexes(&coll.name)?;
        let mut idxs = Vec::with_capacity(indexes.len());
        for idx in indexes {
            index_owner_by_id.insert(idx.id, coll.id);
            idxs.push(PublishedIndex {
                #[cfg(test)]
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
        index_owner_by_id,
    })
}

// ---------------------------------------------------------------------------
// publish_commit — canonical publish helper
// ---------------------------------------------------------------------------

/// Canonical publish helper. Either builds a fresh `Arc<PublishedCatalog>`
/// (when `dirty.published_catalog_dirty`) or clones the previous Arc
/// (root-neutral), wraps it in a new `PublishedEpoch` with the
/// caller-provided `visible_ts`, and atomically stores it on
/// `shared.published`.
///
/// `visible_ts` selection rule (§6.3), enforced at every call site:
///   - if the txn had primary writes: pass the allocated `commit_ts`
///   - otherwise (metadata-only DDL): pass `shared.oracle.commit()?`
///     — NEVER `oracle.now()`; that peek can return equal Ts across two
///     sub-millisecond calls, violating strict visible-ts monotonicity.
///
/// `reserved_catalog_gen` (Phase 5 §10.17.1, US-006):
///   - DDL paths reserve `SharedState.next_catalog_gen.fetch_add(1, AcqRel) + 1`
///     under `metadata.write()` BEFORE the durable envelope and pass
///     `Some(reserved)` here. The published epoch's
///     `catalog_generation` is stamped with that exact reservation.
///   - Ordinary CRUD paths pass `None`. The published epoch's
///     `catalog_generation` is stamped with the prior published value
///     (the CRUD publish closure NEVER advances the DDL identity
///     counter, even when the reader-visible Arc is rebuilt because a
///     data root moved). §10.21 CV-5 forbids the CRUD publish closure
///     from loading `next_catalog_gen` or returning `WriteConflict`.
pub(crate) fn publish_commit(
    shared: &SharedState,
    catalog: &Catalog<BufferPoolPageStore>,
    visible_ts: Ts,
    dirty: PublishDirty,
    reserved_catalog_gen: Option<u64>,
) -> Result<Arc<PublishedEpoch>> {
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
    let catalog_generation = match reserved_catalog_gen {
        // DDL: stamp the reserved generation. The reservation was the
        // result of `next_catalog_gen.fetch_add(1, AcqRel) + 1` inside
        // `metadata.write()`, so it is strictly greater than the prior
        // published generation.
        Some(reserved) => {
            debug_assert!(
                reserved > prev.catalog_generation,
                "DDL-reserved catalog_generation must advance beyond prior published"
            );
            reserved
        }
        // CRUD: inherit the prior published generation. The Arc may be
        // rebuilt (data_root_page moved, etc.) but the DDL identity
        // counter is unchanged so the §10.17.1 captured-identity gate
        // does not trip on concurrent CRUDs.
        None => prev.catalog_generation,
    };
    let new_epoch = Arc::new(PublishedEpoch {
        visible_ts,
        catalog: catalog_arc,
        catalog_generation,
    });
    #[cfg(any(test, feature = "test-hooks"))]
    super::hidden_accessors::phase3_abort_if_armed(
        super::hidden_accessors::Phase3CommitFailpoint::DuringPublishBeforeStore,
    );
    shared.published.store(Arc::clone(&new_epoch));
    // §10.19 C-1 / US-037: store the new epoch first, then the live
    // sequencer frontier with `Release`. This is the legacy
    // (Phase 3/4) producer of `published_frontier`; the Phase 5
    // sequencer-driven path will route the same store through
    // `PublishSequencer::mark_ready` (US-012). `AtomicTs::store` is
    // single-producer; both paths are mutually exclusive per commit and
    // each is serialized by its own mutex (the legacy path runs under
    // the writer serialization, the sequencer path runs under the
    // sequencer mutex), so the seqlock invariant is preserved during
    // the transitional state when only the legacy path stores.
    shared
        .publish_sequencer
        .published_frontier
        .store(visible_ts, std::sync::atomic::Ordering::Release);
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
// Thin wrappers used by mutation sites
// ---------------------------------------------------------------------------

/// Take the CRUD / DDL catalog lock, invoke `publish_commit` with the
/// threaded `PublishDirty`, and record the legacy published-snapshot rebuild
/// counter when a fresh `Arc<PublishedCatalog>` was actually built.
///
/// `reserved_catalog_gen` (Phase 5 §10.17.1, US-006):
///   - DDL callers pass `Some(reserved)` where `reserved` was returned
///     by `SharedState.next_catalog_gen.fetch_add(1, AcqRel) + 1` under
///     `metadata.write()` BEFORE this publish. The published epoch's
///     `catalog_generation` is stamped with that exact reservation.
///   - CRUD callers pass `None`. The published epoch's
///     `catalog_generation` inherits the prior published value, never
///     advancing through a CRUD publish (§10.21 CV-5).
pub(super) fn rebuild_and_publish(
    shared: &SharedState,
    md: &MetadataState,
    publish_ts: crate::mvcc::timestamp::Ts,
    dirty: PublishDirty,
    reserved_catalog_gen: Option<u64>,
) -> Result<()> {
    let cat = md.catalog_lock();
    publish_commit(shared, &cat, publish_ts, dirty, reserved_catalog_gen)?;
    if dirty.published_catalog_dirty {
        crate::mvcc::metrics::record_published_snapshot_rebuild();
    }
    Ok(())
}

/// Update catalog-root header fields through the structural header owner.
pub(super) fn sync_catalog_root_structural(
    shared: &SharedState,
    md: &MetadataState,
    batch: &mut StructuralPageBatch,
) -> Result<()> {
    let (root_page, root_level, next_namespace_id, next_index_id) = {
        let cat = md.catalog_lock();
        (
            cat.root_page(),
            cat.root_level(),
            cat.next_namespace_id() as u64,
            cat.next_index_id() as u64,
        )
    };
    batch.update_header(&shared.handle, |header| {
        header.catalog_root_page = root_page;
        header.catalog_root_level = root_level;
        header.catalog_root_backup = root_page;
        header.next_namespace_id = next_namespace_id;
        header.next_index_id = next_index_id;
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/publish.rs"]
mod tests;
