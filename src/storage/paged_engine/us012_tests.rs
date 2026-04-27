use super::*;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex as StdMutex;

use bson::{doc, Bson};

use crate::error::{Error, Result};
use crate::keys::encode_key;
use crate::mvcc::{VersionEntry, VersionState};
use crate::options::FindOptions;
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool, PageSize, PageSource};
use crate::storage::catalog::CollectionEntry;
use crate::storage::engine::StorageEngine;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;

const LIVE_READER_NS: &str = "test.us012.live_reader";
const SPIN_LIMIT: usize = 10_000;

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

fn collection_entry(engine: &PagedEngine, ns: &str) -> Result<CollectionEntry> {
    let md = engine
        .metadata
        .read()
        .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
    let entry = super::catalog_ops::catalog_lock(&md)
        .get_collection(ns)?
        .ok_or_else(|| Error::Internal("collection missing".into()))?;
    Ok(entry)
}

fn primary_chain_for_id(
    engine: &PagedEngine,
    coll: &CollectionEntry,
    id: &Bson,
) -> Result<Vec<VersionEntry>> {
    let key = encode_key(id);
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&engine.shared.handle)),
        coll.data_root_page,
        coll.data_root_level,
    );
    let leaf = tree.find_leaf(&key)?;
    let chain = engine
        .shared
        .handle
        .pool()
        .take_chain(leaf, &key)?
        .ok_or_else(|| Error::Internal("primary delta chain missing".into()))?;
    let entries: Vec<VersionEntry> = chain.iter().cloned().collect();
    engine.shared.handle.pool().put_chain(leaf, key, chain)?;
    Ok(entries)
}

#[test]
fn test_durable_logical_frame_exists_before_resident_install_live_reader() -> Result<()> {
    use super::test_accessors::install_publish_pause;

    let engine = Arc::new(buffered_engine()?);
    engine.create_namespace(LIVE_READER_NS)?;
    engine.insert(LIVE_READER_NS, doc! { "_id": 0i32, "seed": true })?;

    let coll = collection_entry(&engine, LIVE_READER_NS)?;
    let gate = Arc::new(Barrier::new(2));
    let _guard = install_publish_pause(&engine.shared, Arc::clone(&gate));

    let writer_engine = Arc::clone(&engine);
    let writer = std::thread::spawn(move || {
        writer_engine
            .insert(LIVE_READER_NS, doc! { "_id": 42i32, "phase": "paused" })
            .expect("writer insert");
    });

    let id = Bson::Int32(42);
    let paused_chain = (0..SPIN_LIMIT)
        .find_map(|_| {
            let observed = primary_chain_for_id(&engine, &coll, &id).ok();
            if observed.is_none() {
                std::thread::yield_now();
            }
            observed
        })
        .ok_or_else(|| Error::Internal("writer did not install a pending primary head".into()))?;

    assert_eq!(paused_chain.len(), 1);
    assert!(matches!(
        paused_chain[0].state,
        VersionState::Pending { .. }
    ));

    let (pre_publish_docs, _) =
        engine.find(LIVE_READER_NS, &doc! { "_id": 42i32 }, &FindOptions::new())?;
    assert!(
        pre_publish_docs.is_empty(),
        "pre-publish readers must not see the resident Pending head"
    );

    gate.wait();
    writer.join().expect("writer thread panicked");

    let (post_publish_docs, _) =
        engine.find(LIVE_READER_NS, &doc! { "_id": 42i32 }, &FindOptions::new())?;
    assert_eq!(post_publish_docs.len(), 1);

    let committed_chain = primary_chain_for_id(&engine, &coll, &id)?;
    assert!(matches!(committed_chain[0].state, VersionState::Committed));
    Ok(())
}
