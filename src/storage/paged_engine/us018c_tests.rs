use super::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use bson::doc;

use crate::error::{Error, Result};
use crate::index::IndexModel;
use crate::options::FindOptions;
use crate::storage::buffer_pool::{default_sizes, BufferPool, PageSize, PageSource};
use crate::storage::engine::StorageEngine;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;

const NS: &str = "test.us018c.docs";
const TAG_INDEX: &str = "tag_1";

#[derive(Default)]
struct MockIo {
    pages: StdMutex<HashMap<u32, Vec<u8>>>,
}

struct ArcIo(Arc<MockIo>);

impl PageSource for ArcIo {
    fn read_page(&self, page: u32, _size: PageSize, buf: &mut [u8]) -> Result<()> {
        let pages = self
            .0
            .pages
            .lock()
            .map_err(|_| Error::Internal("mock io pages mutex poisoned".into()))?;
        if let Some(data) = pages.get(&page) {
            let n = buf.len().min(data.len());
            buf[..n].copy_from_slice(&data[..n]);
            if n < buf.len() {
                buf[n..].fill(0);
            }
        } else {
            buf.fill(0);
        }
        Ok(())
    }

    fn write_page(&self, page: u32, _size: PageSize, buf: &[u8]) -> Result<()> {
        self.0
            .pages
            .lock()
            .map_err(|_| Error::Internal("mock io pages mutex poisoned".into()))?
            .insert(page, buf.to_vec());
        Ok(())
    }
}

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
