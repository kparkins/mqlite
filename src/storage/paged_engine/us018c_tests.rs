use super::*;

use std::sync::Arc;

use bson::doc;

use crate::error::Result;
use crate::index::IndexModel;
use crate::options::FindOptions;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::engine::StorageEngine;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
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
fn test_class_b_b_dual_writes_during_build_survive_reopen_and_merge() {
    let engine = buffered_engine().expect("engine");
    engine.create_namespace(NS).expect("create namespace");
    insert_docs(&engine, 0, 40);

    let outcome = engine
        .create_index_reserve(NS, &tag_index_model(), TAG_INDEX)
        .expect("reserve Building index");
    assert!(matches!(
        outcome,
        super::index_maint::ReserveOutcome::Reserved
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
