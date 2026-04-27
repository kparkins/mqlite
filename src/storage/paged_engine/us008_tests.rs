use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use bson::doc;

use super::state::ReadOpScope;
use super::visibility::WriteVisibility;
use super::*;
use crate::error::{Error, Result};
use crate::index::IndexModel;
use crate::options::IndexOptions;
use crate::storage::buffer_pool::{default_sizes, BufferPool, PageSize, PageSource};
use crate::storage::engine::StorageEngine;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;

thread_local! {
    static WRITE_VISIBILITY_NEW_CALLS: Cell<u64> = const { Cell::new(0) };
}

/// Record a `WriteVisibility::new` call on the current test thread.
pub(super) fn record_write_visibility_new() {
    WRITE_VISIBILITY_NEW_CALLS.with(|calls| calls.set(calls.get() + 1));
}

fn reset_write_visibility_new_calls() {
    WRITE_VISIBILITY_NEW_CALLS.with(|calls| calls.set(0));
}

fn write_visibility_new_calls() -> u64 {
    WRITE_VISIBILITY_NEW_CALLS.with(Cell::get)
}

#[derive(Default)]
struct MockIo {
    pages: StdMutex<HashMap<u32, Vec<u8>>>,
}

struct ArcIo(Arc<MockIo>);

impl PageSource for ArcIo {
    fn read_page(&self, page: u32, _size: PageSize, buf: &mut [u8]) -> Result<()> {
        let pages = self.0.pages.lock().unwrap();
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
        self.0.pages.lock().unwrap().insert(page, buf.to_vec());
        Ok(())
    }
}

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

#[test]
fn test_run_write_existing_constructs_single_write_visibility() {
    let engine = buffered_engine();
    engine.create_namespace("test.us008.single").unwrap();

    reset_write_visibility_new_calls();
    engine
        .insert("test.us008.single", doc! { "_id": 1, "v": "a" })
        .unwrap();

    assert_eq!(write_visibility_new_calls(), 1);
}

#[test]
fn test_phase3_secondary_history_is_none_and_correct() {
    let engine = buffered_engine();
    let ns = "test.us008.secondary";
    engine.create_namespace(ns).unwrap();

    let vis = WriteVisibility::new(&engine.shared, ns).unwrap();
    let epoch = engine.shared.load_published();
    let ns_snap = epoch
        .catalog
        .get_by_name(ns)
        .expect("namespace is published");
    assert_eq!(vis.ns_id, ns_snap.id);
    assert_eq!(vis.read_view.visible_ts(), epoch.visible_ts);
    assert!(vis.secondary_history.is_none());
    drop(vis);

    let model = IndexModel::builder()
        .keys(doc! { "email": 1 })
        .options(IndexOptions::new().unique(true))
        .build();
    engine.create_index(ns, &model).unwrap();
    engine
        .insert(ns, doc! { "_id": 1, "email": "a@example.com" })
        .unwrap();
    let err = engine
        .insert(ns, doc! { "_id": 2, "email": "a@example.com" })
        .expect_err("duplicate secondary key should be rejected");
    assert!(matches!(err, Error::DuplicateKey { .. }));
}

#[test]
fn test_write_path_single_epoch_load() {
    let engine = buffered_engine();
    engine.create_namespace("test.us008.load").unwrap();

    let _scope = ReadOpScope::new(1);
    engine
        .insert("test.us008.load", doc! { "_id": 1, "v": "a" })
        .unwrap();
}
