use super::*;

use std::collections::VecDeque;
use std::sync::Arc;

use bson::{doc, Bson};

use crate::error::Result;
use crate::keys::encode_key;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::engine::StorageEngine;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::reconcile::driver::reconcile_tree_dirty_set;
use crate::storage::reconcile::plan::{DirtyReason, TreeIdent, TreeKind};
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "test.us015";
const LARGE_INLINE_BYTES: usize = crate::storage::page::PAGE_SIZE_LEAF as usize;

fn buffered_engine() -> (PagedEngine, Arc<MockIo>) {
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
    let engine = PagedEngine::new_buffered(handle, 0, 0).expect("create buffered engine");
    (engine, io)
}

fn primary_ident(engine: &PagedEngine) -> TreeIdent {
    let epoch = engine.shared.load_published();
    let ns_snap = epoch.catalog.get_by_name(NS).expect("namespace snapshot");
    TreeIdent {
        collection_id: ns_snap.id,
        kind: TreeKind::Primary,
    }
}

fn primary_leaf_for_id(engine: &PagedEngine, id: &Bson) -> Result<(Vec<u8>, u32)> {
    let key = encode_key(id);
    let epoch = engine.shared.load_published();
    let ns_snap = epoch.catalog.get_by_name(NS).expect("namespace snapshot");
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&engine.shared.handle)),
        ns_snap.data_root_page,
        ns_snap.data_root_level,
    );
    let leaf = tree.find_leaf(&key)?;
    Ok((key, leaf))
}

fn assert_dirty_leaf(engine: &PagedEngine, ident: &TreeIdent, leaf: u32) {
    let dirty = engine
        .shared
        .dirty_leaves
        .get(ident)
        .expect("dirty tree entry");
    assert!(dirty.contains_key(&leaf), "expected dirty leaf {leaf}");
}

fn assert_no_dirty_leaf(engine: &PagedEngine, ident: &TreeIdent, leaf: u32) {
    if let Some(dirty) = engine.shared.dirty_leaves.get(ident) {
        assert!(!dirty.contains_key(&leaf), "leaf {leaf} stayed dirty");
    }
}

fn committed_inline(start_ts: Ts, payload: Vec<u8>) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts: Ts::MAX,
        txn_id: 15,
        state: VersionState::Committed,
        data: VersionData::Inline(payload),
        is_tombstone: false,
    }
}

#[test]
fn checkpoint_reconciles_dirty_primary_leaf_then_flushes() -> Result<()> {
    let (engine, io) = buffered_engine();
    engine.create_namespace(NS)?;
    engine.insert(NS, doc! { "_id": 1, "value": "before-checkpoint" })?;
    let ident = primary_ident(&engine);
    let (key, leaf) = primary_leaf_for_id(&engine, &Bson::Int32(1))?;
    assert_dirty_leaf(&engine, &ident, leaf);
    assert!(!engine.shared.handle.pool().chains_empty(leaf)?);

    engine.checkpoint()?;

    assert_no_dirty_leaf(&engine, &ident, leaf);
    assert!(engine.shared.handle.pool().chains_empty(leaf)?);
    let pages = io
        .pages
        .lock()
        .map_err(|_| Error::Internal("mock io pages mutex poisoned".into()))?;
    assert!(
        pages.contains_key(&leaf),
        "checkpoint must flush folded leaf"
    );
    drop(pages);
    assert!(engine.find_one(NS, &doc! { "_id": 1 })?.is_some());
    assert!(engine
        .shared
        .handle
        .pool()
        .take_chain(leaf, &key)?
        .is_none());
    Ok(())
}

#[test]
fn checkpoint_no_op_when_no_dirty_leaves() -> Result<()> {
    let (engine, _io) = buffered_engine();

    engine.checkpoint()?;

    assert!(engine.shared.dirty_leaves.is_empty());
    Ok(())
}

#[test]
fn checkpoint_keeps_not_installable_leaf_dirty_and_reports_stats() -> Result<()> {
    let (engine, _io) = buffered_engine();
    engine.create_namespace(NS)?;
    engine.insert(NS, doc! { "_id": 7, "value": "large-delta" })?;
    let ident = primary_ident(&engine);
    let (key, leaf) = primary_leaf_for_id(&engine, &Bson::Int32(7))?;
    let checkpoint_ts = engine.shared.load_published().visible_ts;
    let oversized = vec![0xA5; LARGE_INLINE_BYTES];
    engine.shared.handle.pool().put_chain(
        leaf,
        key.clone(),
        Arc::new(VecDeque::from([committed_inline(checkpoint_ts, oversized)])),
    )?;
    engine.shared.dirty_leaves.clear();
    engine
        .shared
        .mark_leaf_dirty(ident.clone(), leaf, DirtyReason::PrimaryWrite);

    let stats = {
        let md = engine
            .metadata
            .write()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        reconcile_tree_dirty_set(
            &engine,
            &md,
            ident.clone(),
            &[leaf],
            checkpoint_ts,
            engine
                .shared
                .handle
                .read_view_registry()
                .oldest_required_ts(),
            false,
        )?
    };
    assert_eq!(stats.dirty_leaves, 1);
    assert_eq!(stats.installed, 0);
    assert_eq!(stats.not_installable, 1);

    let err = engine
        .checkpoint()
        .expect_err("US-003 blocks checkpoint before not-installable residue mutates");
    assert!(matches!(err, Error::CheckpointIncomplete { .. }));

    assert_dirty_leaf(&engine, &ident, leaf);
    let chain = engine
        .shared
        .handle
        .pool()
        .take_chain(leaf, &key)?
        .expect("non-installable chain must remain attached");
    assert_eq!(chain.len(), 1);
    engine.shared.handle.pool().put_chain(leaf, key, chain)?;
    Ok(())
}
