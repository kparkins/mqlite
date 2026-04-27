use super::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use bson::{doc, Bson};

use crate::error::{Error, Result};
use crate::index::IndexModel;
use crate::keys::{encode_compound_key, encode_key};
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::options::{FindOptions, IndexOptions};
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool, PageSize, PageSource};
use crate::storage::catalog::{IndexEntry, IndexState};
use crate::storage::engine::StorageEngine;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;

const READY_NS: &str = "test.us011.ready";
const BUILDING_NS: &str = "test.us011.building";
const BUILDING_UNIQUE_NS: &str = "test.us011.building_unique";
const EMAIL_INDEX: &str = "email_1";

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

fn email_index_model(unique: bool) -> IndexModel {
    let mut builder = IndexModel::builder().keys(doc! { "email": 1 });
    if unique {
        builder = builder.options(IndexOptions::new().unique(true));
    }
    builder.build()
}

fn create_ready_email_index(engine: &PagedEngine, ns: &str) -> Result<IndexEntry> {
    engine.create_namespace(ns)?;
    engine.create_index(ns, &email_index_model(false))?;
    index_entry(engine, ns)
}

fn create_building_email_index(engine: &PagedEngine, ns: &str, unique: bool) -> Result<IndexEntry> {
    engine.create_namespace(ns)?;
    let outcome = engine.create_index_reserve(ns, &email_index_model(unique), EMAIL_INDEX)?;
    if !matches!(outcome, super::index_maint::ReserveOutcome::Reserved) {
        return Err(Error::Internal(
            "expected new Building index reservation".into(),
        ));
    }
    let entry = index_entry(engine, ns)?;
    assert_eq!(entry.state, IndexState::Building);
    Ok(entry)
}

fn index_entry(engine: &PagedEngine, ns: &str) -> Result<IndexEntry> {
    let md = engine
        .metadata
        .read()
        .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
    let entry = super::catalog_ops::catalog_lock(&md)
        .get_index(ns, EMAIL_INDEX)?
        .ok_or_else(|| Error::Internal("email index missing".into()))?;
    Ok(entry)
}

fn collection_entry(
    engine: &PagedEngine,
    ns: &str,
) -> Result<crate::storage::catalog::CollectionEntry> {
    let md = engine
        .metadata
        .read()
        .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
    let entry = super::catalog_ops::catalog_lock(&md)
        .get_collection(ns)?
        .ok_or_else(|| Error::Internal("collection missing".into()))?;
    Ok(entry)
}

fn secondary_key(email: &str, id: &Bson) -> Vec<u8> {
    let email = Bson::String(email.to_owned());
    encode_compound_key(&[(&email, true), (id, true)])
}

fn take_chain(
    engine: &PagedEngine,
    root_page: u32,
    root_level: u8,
    key: &[u8],
) -> Result<(Option<Vec<u8>>, Vec<VersionEntry>)> {
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&engine.shared.handle)),
        root_page,
        root_level,
    );
    let base = match tree.search(key)? {
        Some(crate::storage::btree::CellValue::Inline(bytes)) => Some(bytes),
        Some(crate::storage::btree::CellValue::Overflow { .. }) => {
            return Err(Error::Internal(
                "test keys should not spill index payloads".into(),
            ));
        }
        None => None,
    };
    let leaf = tree.find_leaf(key)?;
    let chain = engine
        .shared
        .handle
        .pool()
        .take_chain(leaf, key)?
        .ok_or_else(|| Error::Internal("delta chain missing".into()))?;
    let entries: Vec<VersionEntry> = chain.iter().cloned().collect();
    engine
        .shared
        .handle
        .pool()
        .put_chain(leaf, key.to_vec(), chain)?;
    Ok((base, entries))
}

fn assert_inline(entry: &VersionEntry, expected: &[u8]) -> Result<()> {
    match &entry.data {
        VersionData::Inline(bytes) => {
            assert_eq!(bytes, expected);
            Ok(())
        }
        VersionData::Overflow(_) => Err(Error::Internal("expected inline version data".into())),
    }
}

#[test]
fn test_install_pending_sec_index_creates_delta_head() -> Result<()> {
    let engine = buffered_engine()?;
    let index = create_ready_email_index(&engine, READY_NS)?;
    let id = Bson::Int32(1);
    let key = secondary_key("a@example.test", &id);

    engine.insert(READY_NS, doc! { "_id": 1, "email": "a@example.test" })?;
    let (base_after_insert, insert_chain) =
        take_chain(&engine, index.root_page, index.root_level, &key)?;
    assert!(base_after_insert.is_none());
    assert_eq!(insert_chain.len(), 1);
    assert_eq!(insert_chain[0].stop_ts, Ts::MAX);
    assert!(!insert_chain[0].is_tombstone);
    assert!(matches!(insert_chain[0].state, VersionState::Committed));

    engine.delete(READY_NS, &doc! { "_id": 1 }, false)?;
    let (base_after_delete, delete_chain) =
        take_chain(&engine, index.root_page, index.root_level, &key)?;
    assert!(base_after_delete.is_none());
    assert_eq!(delete_chain.len(), 2);
    assert_eq!(delete_chain[0].stop_ts, Ts::MAX);
    assert_eq!(delete_chain[1].stop_ts, delete_chain[0].start_ts);
    assert!(delete_chain[0].is_tombstone);
    assert_inline(&delete_chain[0], &[])?;
    Ok(())
}

#[test]
fn test_building_index_receives_delta_only_writes() -> Result<()> {
    let engine = buffered_engine()?;
    let index = create_building_email_index(&engine, BUILDING_NS, false)?;
    let id = Bson::Int32(1);
    let key = secondary_key("building@example.test", &id);

    engine.insert(
        BUILDING_NS,
        doc! { "_id": 1, "email": "building@example.test" },
    )?;

    let (base, chain) = take_chain(&engine, index.root_page, index.root_level, &key)?;
    assert!(base.is_none());
    assert_eq!(chain.len(), 1);
    assert!(!chain[0].is_tombstone);

    let (docs, explain) = engine.find(
        BUILDING_NS,
        &doc! { "email": "building@example.test" },
        &FindOptions::new(),
    )?;
    assert!(explain.index_used.is_none());
    assert_eq!(docs.len(), 1);
    let actual_id = docs[0]
        .get_i32("_id")
        .map_err(|e| Error::Internal(format!("missing _id: {e}")))?;
    assert_eq!(actual_id, 1);
    Ok(())
}

#[test]
fn test_building_index_unique_precheck_fires() -> Result<()> {
    let engine = buffered_engine()?;
    create_building_email_index(&engine, BUILDING_UNIQUE_NS, true)?;

    engine.insert(
        BUILDING_UNIQUE_NS,
        doc! { "_id": 1, "email": "dupe@example.test" },
    )?;
    let err = match engine.insert(
        BUILDING_UNIQUE_NS,
        doc! { "_id": 2, "email": "dupe@example.test" },
    ) {
        Ok(_) => {
            return Err(Error::Internal(
                "building unique index accepted duplicate write".into(),
            ))
        }
        Err(err) => err,
    };
    assert!(matches!(err, Error::DuplicateKey { .. }));
    Ok(())
}

#[test]
fn test_primary_and_secondary_share_single_commit_ts() -> Result<()> {
    let engine = buffered_engine()?;
    let index = create_ready_email_index(&engine, READY_NS)?;
    let id = Bson::Int32(37);
    let doc_key = encode_key(&id);
    let sec_key = secondary_key("shared-ts@example.test", &id);

    engine.insert(
        READY_NS,
        doc! { "_id": 37, "email": "shared-ts@example.test" },
    )?;

    let coll = collection_entry(&engine, READY_NS)?;
    let (_, primary_chain) =
        take_chain(&engine, coll.data_root_page, coll.data_root_level, &doc_key)?;
    let (_, secondary_chain) = take_chain(&engine, index.root_page, index.root_level, &sec_key)?;

    assert_eq!(primary_chain.len(), 1);
    assert_eq!(secondary_chain.len(), 1);
    assert_eq!(primary_chain[0].start_ts, secondary_chain[0].start_ts);
    Ok(())
}
