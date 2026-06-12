//! R6 regression tests: `drop_namespace` mutates the live metadata
//! `Catalog` (`drop_collection`) BEFORE the durable catalog commit. A
//! non-fatal commit failure used to only `mark_aborted` the publish slot,
//! leaving the metadata catalog (entry gone) disagreeing with the published
//! epoch (entry still visible). A retried `drop_namespace` then saw
//! `get_collection == None` and returned `Ok(())` WITHOUT committing or
//! publishing anything — the namespace ghosted in the published epoch
//! forever. The drop path now captures the `CollectionEntry` and its
//! `IndexEntry` records before `drop_collection` and re-inserts them under
//! the still-held `metadata.write()` on every non-fatal body/commit
//! failure, symmetric with the create-side undo.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use bson::doc;

use super::*;
use crate::storage::buffer_pool::{default_sizes, BufferPool, PageSize};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "test.bug6drop";

/// Walk the on-header 32 KiB free list (pool-coherent reads) and report
/// whether `page` is currently on it.
fn free_list_32k_contains(engine: &PagedEngine, page: u32) -> bool {
    let mut head = engine
        .shared
        .handle
        .allocator()
        .with_header(|h| h.free_list_head_32k)
        .expect("read free list head");
    let mut steps = 0u32;
    while head != 0 && steps < 10_000 {
        if head == page {
            return true;
        }
        let pin = engine
            .shared
            .handle
            .fetch_page(head, PageSize::Large32k)
            .expect("pin free-list link page");
        let d = pin.data();
        head = u32::from_le_bytes([d[0], d[1], d[2], d[3]]);
        steps += 1;
    }
    false
}

fn buffered_engine_with_busy(busy: Duration) -> PagedEngine {
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let header = FileHeader::new_now();
    let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
    PagedEngine::new_buffered_with_busy(handle, 0, 0, busy, None, 3, DurabilityMode::default())
        .expect("create buffered engine")
}

/// Arm a one-shot non-fatal failure at the catalog log-record reservation,
/// run `drop_namespace`, and assert:
/// (a) the drop surfaces the injected error,
/// (b) the metadata catalog and the published epoch still AGREE the
///     namespace (and its index) exist,
/// (c) a retried `drop_namespace` actually completes — it publishes the
///     removal instead of early-returning `Ok(())` on a ghost entry.
#[test]
fn failed_drop_namespace_commit_must_not_tear_catalog_state_and_retry_completes() {
    let engine = buffered_engine_with_busy(Duration::from_millis(200));
    engine.create_namespace(NS).expect("create namespace");
    engine
        .insert(NS, doc! { "_id": 1, "value": "kept" })
        .expect("insert document");
    let model = IndexModel::builder().keys(doc! { "value": 1 }).build();
    engine
        .create_index(NS, &model)
        .expect("create secondary index");

    super::hidden_accessors::arm_catalog_commit_reserve_failure(&engine.shared);
    let err = engine
        .drop_namespace(NS)
        .expect_err("armed catalog-commit reservation failpoint must fail drop_namespace");
    assert!(
        matches!(err, Error::Internal(_)),
        "expected the injected non-fatal reservation failure, got {err:?}"
    );

    // (b) catalog and published epoch must agree the namespace still exists.
    let published_ns = engine.shared.load_published();
    let published_snap = published_ns.catalog.get_by_name(NS);
    assert!(
        published_snap.is_some(),
        "published epoch must still expose '{NS}' after a non-fatal failed drop"
    );
    let published_index_count = published_snap.map(|s| s.indexes.len()).unwrap_or(0);
    let (metadata_entry, metadata_indexes) = {
        let cat = engine.metadata_state.catalog_lock();
        (
            cat.get_collection(NS).expect("metadata catalog read"),
            cat.list_indexes(NS).expect("metadata index read"),
        )
    };
    assert!(
        metadata_entry.is_some(),
        "aborted drop_namespace left the metadata catalog without '{NS}' while \
         the published epoch still exposes it (torn state)"
    );
    assert_eq!(
        metadata_indexes.len(),
        published_index_count,
        "metadata catalog and published epoch must agree on '{NS}' index entries"
    );
    // The restored entry must be the SAME namespace identity (id + data root),
    // not a freshly allocated lookalike.
    let restored = metadata_entry.expect("restored collection entry");
    let published_id = published_snap.map(|s| s.id).expect("published ns id");
    assert_eq!(
        restored.id, published_id,
        "restored CollectionEntry must keep the published namespace id"
    );
    drop(published_ns);

    // The pre-drop data must still be readable through the agreed state.
    assert_eq!(
        engine.count(NS, &doc! {}).expect("count after failed drop"),
        1,
        "documents must remain visible after a non-fatal failed drop"
    );

    // (c) a retried drop_namespace must complete and publish the removal.
    engine
        .drop_namespace(NS)
        .expect("retried drop_namespace must complete");
    assert!(
        engine
            .shared
            .load_published()
            .catalog
            .get_by_name(NS)
            .is_none(),
        "retried drop_namespace must publish the namespace removal"
    );
    assert!(
        engine
            .metadata_state
            .catalog_lock()
            .get_collection(NS)
            .expect("metadata catalog read after retry")
            .is_none(),
        "retried drop_namespace must remove the metadata catalog entry"
    );
}

/// F37: `Catalog::drop_collection` is multi-step — the per-index record
/// deletes precede the collection record delete. The undo marker
/// (`catalog_dropped`) used to be set only AFTER the whole `drop_collection`
/// succeeded, so a mid-loop failure left index records ghost-deleted with
/// the restore SKIPPED: the metadata catalog disagreed with the published
/// epoch at index granularity (the R6 tear), and a retried drop saw torn
/// index state. The marker must cover the entire multi-step mutation.
#[test]
fn mid_drop_collection_index_failure_restores_all_index_records_and_retry_completes() {
    const NS_F37: &str = "test.bug6drop.f37";
    let engine = buffered_engine_with_busy(Duration::from_millis(200));
    engine.create_namespace(NS_F37).expect("create namespace");
    engine
        .insert(NS_F37, doc! { "_id": 1, "value": "kept", "other": 7 })
        .expect("insert document");
    for keys in [doc! { "value": 1 }, doc! { "other": 1 }] {
        let model = IndexModel::builder().keys(keys).build();
        engine
            .create_index(NS_F37, &model)
            .expect("create secondary index");
    }
    let captured_indexes: BTreeSet<(i64, String, u32, u8)> = {
        let cat = engine.metadata_state.catalog_lock();
        cat.list_indexes(NS_F37)
            .expect("list indexes before drop")
            .into_iter()
            .map(|e| (e.id, e.name, e.root_page, e.root_level))
            .collect()
    };
    assert_eq!(
        captured_indexes.len(),
        2,
        "setup must register two secondary indexes"
    );

    // Fail drop_collection after the FIRST index-record delete, before the
    // collection-record delete.
    crate::storage::catalog::drop_collection_failpoint::arm(NS_F37);
    let err = engine
        .drop_namespace(NS_F37)
        .expect_err("armed mid-loop drop_collection failpoint must fail drop_namespace");
    assert!(
        matches!(err, Error::Internal(_)),
        "expected the injected non-fatal mid-loop failure, got {err:?}"
    );

    // The restore must have re-inserted the ghost-deleted index record(s):
    // metadata catalog still lists the namespace AND all of its indexes,
    // with the captured identities (id, name, root) intact.
    let (metadata_entry, restored_indexes) = {
        let cat = engine.metadata_state.catalog_lock();
        (
            cat.get_collection(NS_F37).expect("metadata catalog read"),
            cat.list_indexes(NS_F37).expect("metadata index read"),
        )
    };
    assert!(
        metadata_entry.is_some(),
        "mid-loop failed drop_namespace must leave the collection record present"
    );
    let restored: BTreeSet<(i64, String, u32, u8)> = restored_indexes
        .into_iter()
        .map(|e| (e.id, e.name, e.root_page, e.root_level))
        .collect();
    assert_eq!(
        restored, captured_indexes,
        "a mid-drop_collection failure ghost-deleted index records: the undo \
         marker must cover the per-index delete loop so the restore re-inserts \
         every captured index entry"
    );
    // Published epoch agreement (index granularity).
    let published = engine.shared.load_published();
    let published_index_count = published
        .catalog
        .get_by_name(NS_F37)
        .map(|s| s.indexes.len())
        .unwrap_or(0);
    assert_eq!(
        published_index_count,
        captured_indexes.len(),
        "published epoch and metadata catalog must agree on '{NS_F37}' indexes"
    );
    drop(published);

    // A retried drop must complete and publish the removal.
    engine
        .drop_namespace(NS_F37)
        .expect("retried drop_namespace must complete");
    assert!(
        engine
            .shared
            .load_published()
            .catalog
            .get_by_name(NS_F37)
            .is_none(),
        "retried drop_namespace must publish the namespace removal"
    );
    assert!(
        engine
            .metadata_state
            .catalog_lock()
            .get_collection(NS_F37)
            .expect("metadata catalog read after retry")
            .is_none(),
        "retried drop_namespace must remove the metadata catalog entry"
    );
    assert!(
        engine
            .metadata_state
            .catalog_lock()
            .list_indexes(NS_F37)
            .expect("metadata index read after retry")
            .is_empty(),
        "retried drop_namespace must remove every index record"
    );
}

/// F10: the restore goes create-then-overwrite, and used to push the
/// scratch root onto the to-free list BEFORE `update_collection` ran — and
/// regardless of its outcome. If the overwrite fails, the surviving catalog
/// entry keeps pointing at the scratch root while the restore frees it:
/// the next open/insert descends a freed (recyclable) page — strictly worse
/// than no restore. The scratch root may be freed ONLY when the overwrite
/// landed (`Ok(true)`); on failure the entry must keep its scratch root —
/// a valid, still-allocated empty leaf, i.e. a safe empty collection.
#[test]
fn restore_with_failed_update_must_not_free_the_surviving_entrys_root() {
    const NS_F10: &str = "test.bug6drop.f10";
    let engine = buffered_engine_with_busy(Duration::from_millis(200));
    engine.create_namespace(NS_F10).expect("create namespace");
    engine
        .insert(NS_F10, doc! { "_id": 1, "value": "kept" })
        .expect("insert document");
    let original_root = {
        let cat = engine.metadata_state.catalog_lock();
        cat.get_collection(NS_F10)
            .expect("metadata catalog read")
            .expect("collection entry before drop")
            .data_root_page
    };

    // Fail the drop AFTER `drop_collection` fully succeeded (so the restore
    // re-creates the collection record), and fail the restore's
    // `update_collection` overwrite of the captured entry.
    super::hidden_accessors::arm_catalog_commit_reserve_failure(&engine.shared);
    super::ns_ddl::restore_update_failpoint::arm(NS_F10);
    let err = engine
        .drop_namespace(NS_F10)
        .expect_err("armed catalog-commit reservation failpoint must fail drop_namespace");
    assert!(
        matches!(err, Error::Internal(_)),
        "expected the injected non-fatal reservation failure, got {err:?}"
    );

    // The restore re-created the record but the overwrite failed: the entry
    // survives pointing at the scratch root.
    let restored = engine
        .metadata_state
        .catalog_lock()
        .get_collection(NS_F10)
        .expect("metadata catalog read after failed drop")
        .expect("restore must re-insert the collection record");
    assert_ne!(
        restored.data_root_page, original_root,
        "failpoint sanity: the captured-entry overwrite must have been \
         skipped, leaving the entry on its scratch root"
    );
    assert_eq!(restored.id, {
        let published = engine.shared.load_published();
        published
            .catalog
            .get_by_name(NS_F10)
            .expect("published epoch still exposes the namespace")
            .id
    });

    // THE F10 INVARIANT: the surviving entry's data_root_page must still be
    // a valid allocated page — never on the free list (recyclable) while
    // the catalog references it.
    assert!(
        !free_list_32k_contains(&engine, restored.data_root_page),
        "the surviving catalog entry's data_root_page {} is on the 32 KiB \
         free list — the restore freed a scratch root the entry still \
         references; the next open/insert would descend a reused/zeroed page",
        restored.data_root_page
    );
}
