use std::sync::Arc;

use bson::doc;

use super::*;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::reconcile::driver::{DirtyReason, TreeIdent, TreeKind};
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "test.us002";

fn buffered_engine() -> PagedEngine {
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
    PagedEngine::new_buffered(handle, 0, 0).expect("create buffered engine")
}

fn create_ready_email_index(engine: &PagedEngine) {
    engine.create_namespace(NS).unwrap();
    engine
        .create_index(NS, &IndexModel::builder().keys(doc! { "email": 1 }).build())
        .unwrap();
}

fn primary_ident(engine: &PagedEngine) -> TreeIdent {
    let epoch = engine.shared.load_published();
    let ns_snap = epoch.catalog.get_by_name(NS).expect("namespace snapshot");
    TreeIdent {
        collection_id: ns_snap.id,
        kind: TreeKind::Primary,
    }
}

fn secondary_ident(engine: &PagedEngine) -> TreeIdent {
    let epoch = engine.shared.load_published();
    let ns_snap = epoch.catalog.get_by_name(NS).expect("namespace snapshot");
    let index = ns_snap
        .indexes
        .iter()
        .find(|idx| idx.name == "email_1")
        .expect("ready email index");
    let owner = epoch
        .catalog
        .index_owner_by_id
        .get(&index.id)
        .copied()
        .expect("published index owner sidecar");
    assert_eq!(owner, ns_snap.id);
    TreeIdent {
        collection_id: owner,
        kind: TreeKind::Secondary { index_id: index.id },
    }
}

fn assert_dirty_leaf(engine: &PagedEngine, ident: TreeIdent, reason: DirtyReason) {
    let dirty = engine
        .shared
        .dirty_leaves
        .get(&ident)
        .expect("dirty tree entry");
    assert!(
        dirty.keys().all(|page_id| *page_id > 0),
        "dirty leaves must be keyed by real page ids"
    );
    assert!(
        dirty
            .values()
            .any(|leaf_state| leaf_state.dirty_reason == reason),
        "dirty tree entry must retain the expected reason"
    );
}

fn assert_dirty_tree_removed(engine: &PagedEngine, ident: &TreeIdent) {
    assert!(
        !engine.shared.dirty_leaves.contains_key(ident),
        "dropped tree dirty entries must be removed"
    );
}

#[test]
fn published_catalog_rebuild_adds_index_owner_sidecar() {
    let engine = buffered_engine();
    create_ready_email_index(&engine);

    let epoch = engine.shared.load_published();
    let ns_snap = epoch.catalog.get_by_name(NS).expect("namespace snapshot");
    let index = ns_snap.indexes.first().expect("ready index");

    assert_eq!(
        epoch.catalog.index_owner_by_id.get(&index.id).copied(),
        Some(ns_snap.id)
    );
    assert_eq!(
        epoch
            .catalog
            .index_owner_by_id
            .get(&(index.id + 1))
            .copied(),
        None
    );
}

#[test]
fn crud_installs_mark_primary_and_secondary_dirty_leaves() {
    let engine = buffered_engine();
    create_ready_email_index(&engine);

    engine.shared.dirty_leaves.clear();
    engine
        .insert(NS, doc! { "_id": 1, "email": "a@example.com" })
        .unwrap();
    assert_dirty_leaf(&engine, primary_ident(&engine), DirtyReason::PrimaryWrite);
    assert_dirty_leaf(
        &engine,
        secondary_ident(&engine),
        DirtyReason::SecondaryWrite,
    );

    engine.shared.dirty_leaves.clear();
    engine
        .update(
            NS,
            &doc! { "_id": 1 },
            &doc! { "$set": { "email": "b@example.com" } },
            &UpdateOptions::default(),
            false,
        )
        .unwrap();
    assert_dirty_leaf(&engine, primary_ident(&engine), DirtyReason::PrimaryWrite);
    assert_dirty_leaf(
        &engine,
        secondary_ident(&engine),
        DirtyReason::SecondaryWrite,
    );

    engine.shared.dirty_leaves.clear();
    engine
        .delete(NS, &doc! { "_id": 1 }, false)
        .expect("delete indexed document");
    assert_dirty_leaf(&engine, primary_ident(&engine), DirtyReason::PrimaryWrite);
    assert_dirty_leaf(
        &engine,
        secondary_ident(&engine),
        DirtyReason::SecondaryWrite,
    );
}

#[test]
fn drop_namespace_clears_dirty_entries_for_dropped_trees() {
    let engine = buffered_engine();
    create_ready_email_index(&engine);
    engine
        .insert(NS, doc! { "_id": 1, "email": "drop-ns@example.com" })
        .expect("insert indexed document");
    let primary = primary_ident(&engine);
    let secondary = secondary_ident(&engine);
    assert_dirty_leaf(&engine, primary.clone(), DirtyReason::PrimaryWrite);
    assert_dirty_leaf(&engine, secondary.clone(), DirtyReason::SecondaryWrite);

    engine.drop_namespace(NS).expect("drop namespace");

    assert_dirty_tree_removed(&engine, &primary);
    assert_dirty_tree_removed(&engine, &secondary);
}

#[test]
fn drop_index_clears_dirty_entries_for_dropped_secondary_tree_only() {
    let engine = buffered_engine();
    create_ready_email_index(&engine);
    engine
        .insert(NS, doc! { "_id": 1, "email": "drop-index@example.com" })
        .expect("insert indexed document");
    let primary = primary_ident(&engine);
    let secondary = secondary_ident(&engine);
    assert_dirty_leaf(&engine, primary.clone(), DirtyReason::PrimaryWrite);
    assert_dirty_leaf(&engine, secondary.clone(), DirtyReason::SecondaryWrite);

    engine.drop_index(NS, "email_1").expect("drop index");

    assert_dirty_leaf(&engine, primary, DirtyReason::PrimaryWrite);
    assert_dirty_tree_removed(&engine, &secondary);
}
