use super::*;

use std::sync::Arc;

use bson::doc;

use crate::error::{EngineFatalReason, Error, Result, WriteConflictReason};
use crate::index::IndexModel;
use crate::options::FindOptions;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::catalog::{IndexEntry, IndexState};
use crate::storage::engine::StorageEngine;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::paged_engine::hidden_accessors::Us026PostRegisterFailpoint;
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "test.us018c.docs";
const TAG_INDEX: &str = "tag_1";

fn buffered_engine() -> Result<PagedEngine> {
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
    PagedEngine::new_buffered(handle, 0, 0)
}

fn insert_docs(engine: &PagedEngine, start: i32, end: i32) {
    for id in start..end {
        engine
            .insert(
                NS,
                doc! {
                    "_id": id,
                    "tag": format!("tag-{}", id % 5),
                    "payload": format!("payload-{id:04}"),
                },
            )
            .expect("insert doc");
    }
}

fn tag_index_model() -> IndexModel {
    IndexModel::builder().keys(doc! { "tag": 1 }).build()
}

fn tag_index_entry(engine: &PagedEngine) -> Result<IndexEntry> {
    let _md = engine
        .metadata
        .read()
        .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
    super::catalog_ops::catalog_lock(&engine.metadata_state)
        .get_index(NS, TAG_INDEX)?
        .ok_or_else(|| Error::Internal("tag index missing".into()))
}

fn assert_tag_index_covers(engine: &PagedEngine, expected: usize) {
    let (docs, explain) = engine
        .find(NS, &doc! { "tag": "tag-3" }, &FindOptions::default())
        .expect("find tag");
    assert_eq!(
        explain.index_used.as_deref(),
        Some(TAG_INDEX),
        "resume must promote the rebuilt Building index to planner-visible Ready"
    );
    assert!(
        !explain.full_scan,
        "query must not use COLLSCAN after resume"
    );
    assert_eq!(docs.len(), expected);
}

#[test]
fn test_failed_build_catalog_update_restores_original_entry() {
    let engine = buffered_engine().expect("engine");
    engine.create_namespace(NS).expect("create namespace");
    engine
        .insert(NS, doc! { "_id": 1, "tag": ["tag-3"], "payload": "array" })
        .expect("insert multikey doc");

    let outcome = engine
        .create_index_reserve(NS, &tag_index_model(), TAG_INDEX)
        .expect("reserve Building index");
    assert!(matches!(
        outcome,
        super::index_maint::ReserveOutcome::Reserved(_)
    ));
    let original = tag_index_entry(&engine).expect("original Building entry");
    assert!(!original.multikey);

    engine.test_fail_after_build_catalog_update_once();
    engine
        .create_index_build(NS, TAG_INDEX)
        .expect_err("injected post-catalog-update failure");

    let after = tag_index_entry(&engine).expect("rolled-back Building entry");
    assert_eq!(
        after, original,
        "failed build must not leave a live catalog root/multikey update"
    );
}

#[test]
fn building_index_name_does_not_report_idempotent_success() {
    let engine = buffered_engine().expect("engine");
    engine.create_namespace(NS).expect("create namespace");

    let outcome = engine
        .create_index_reserve(NS, &tag_index_model(), TAG_INDEX)
        .expect("reserve Building index");
    assert!(matches!(
        outcome,
        super::index_maint::ReserveOutcome::Reserved(_)
    ));

    let err = engine
        .create_index(NS, &tag_index_model())
        .expect_err("Building index must not report create_index success");
    assert!(matches!(
        err,
        Error::WriteConflict {
            reason: WriteConflictReason::CatalogGenerationChanged
        }
    ));
    assert_eq!(
        tag_index_entry(&engine).expect("Building entry").state,
        IndexState::Building
    );
}

#[test]
fn ready_index_create_is_noop_without_new_catalog_publish() {
    let engine = buffered_engine().expect("engine");
    engine.create_namespace(NS).expect("create namespace");
    engine
        .create_index(NS, &tag_index_model())
        .expect("create index");
    let published_after_create = engine.shared.load_published();

    let name = engine
        .create_index(NS, &tag_index_model())
        .expect("idempotent create_index");

    assert_eq!(name, TAG_INDEX);
    assert_eq!(
        engine.shared.load_published().catalog_generation,
        published_after_create.catalog_generation,
        "idempotent Ready create_index must not publish a new catalog generation"
    );
}

#[test]
fn build_flush_failure_after_structural_commit_poisons_engine() {
    let engine = buffered_engine().expect("engine");
    engine.create_namespace(NS).expect("create namespace");
    engine
        .insert(NS, doc! { "_id": 1, "tag": ["tag-3"], "payload": "array" })
        .expect("insert multikey doc");

    let outcome = engine
        .create_index_reserve(NS, &tag_index_model(), TAG_INDEX)
        .expect("reserve Building index");
    assert!(matches!(
        outcome,
        super::index_maint::ReserveOutcome::Reserved(_)
    ));

    engine.test_us026_arm_post_register_failpoint(Us026PostRegisterFailpoint::Flush);
    let err = engine
        .create_index_build(NS, TAG_INDEX)
        .expect_err("post-structural-commit flush failure must poison the engine");
    assert!(matches!(
        err,
        Error::EngineFatal {
            reason: EngineFatalReason::PostDurableDdlPublishFailure
        }
    ));

    let rejected = engine
        .insert(NS, doc! { "_id": 2, "tag": "tag-3" })
        .expect_err("poisoned engine rejects later writes");
    assert!(matches!(
        rejected,
        Error::EngineFatal {
            reason: EngineFatalReason::PostDurableDdlPublishFailure
        }
    ));
}

#[test]
fn test_class_b_b_dual_writes_during_build_survive_reopen_and_merge() {
    let engine = buffered_engine().expect("engine");
    engine.create_namespace(NS).expect("create namespace");
    insert_docs(&engine, 0, 40);

    let outcome = engine
        .create_index_reserve(NS, &tag_index_model(), TAG_INDEX)
        .expect("reserve Building index");
    assert!(matches!(
        outcome,
        super::index_maint::ReserveOutcome::Reserved(_)
    ));
    engine
        .create_index_build(NS, TAG_INDEX)
        .expect("initial partial build");

    insert_docs(&engine, 40, 60);

    engine
        .resume_building_indexes_after_open()
        .expect("resume Building index after simulated reopen");
    assert_tag_index_covers(&engine, 12);
}
