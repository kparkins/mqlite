use super::*;

use std::sync::Arc;

use bson::{doc, Bson};

use crate::error::Result;
use crate::index::IndexModel;
use crate::keys::{encode_compound_key, encode_key};
use crate::mvcc::transaction::Ns;
use crate::mvcc::{
    PrimaryOp, PrimaryWrite, SecIndexOp, SecIndexWrite, Ts, VersionData, VersionEntry, VersionState,
};
use crate::options::{FindOptions, IndexOptions};
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool, LatchMode};
use crate::storage::catalog::{IndexEntry, IndexState};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};
use crate::storage::history_store::{HistorySpillTxn, HistoryStore};
use crate::storage::reconcile::driver::{TreeIdent, TreeKind};
use crate::storage::test_support::{ArcIo, MockIo};

const READY_NS: &str = "test.us011.ready";
const BUILDING_NS: &str = "test.us011.building";
const BUILDING_UNIQUE_NS: &str = "test.us011.building_unique";
const EMAIL_INDEX: &str = "email_1";

fn buffered_engine() -> Result<PagedEngine> {
    let io = Arc::new(MockIo::default());
    buffered_engine_from_io(io, FileHeader::new_now(), 0, 0)
}

fn buffered_engine_from_io(
    io: Arc<MockIo>,
    header: FileHeader,
    catalog_root_page: u32,
    catalog_root_level: u8,
) -> Result<PagedEngine> {
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
    PagedEngine::new_buffered(handle, catalog_root_page, catalog_root_level)
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
    if !matches!(outcome, super::index_maint::ReserveOutcome::Reserved(_)) {
        return Err(Error::Internal(
            "expected new Building index reservation".into(),
        ));
    }
    let entry = index_entry(engine, ns)?;
    assert_eq!(entry.state, IndexState::Building);
    Ok(entry)
}

fn index_entry(engine: &PagedEngine, ns: &str) -> Result<IndexEntry> {
    let _md = engine
        .metadata
        .read()
        .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
    let entry = engine
        .metadata_state
        .catalog_lock()
        .get_index(ns, EMAIL_INDEX)?
        .ok_or_else(|| Error::Internal("email index missing".into()))?;
    Ok(entry)
}

fn collection_entry(
    engine: &PagedEngine,
    ns: &str,
) -> Result<crate::storage::catalog::CollectionEntry> {
    let _md = engine
        .metadata
        .read()
        .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
    let entry = engine
        .metadata_state
        .catalog_lock()
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
    let entries = engine.shared.handle.pool().with_chain_under_latch(
        leaf,
        key,
        LatchMode::Exclusive,
        |slot| {
            let chain = slot
                .as_ref()
                .ok_or_else(|| Error::Internal("delta chain missing".into()))?;
            let entries: Vec<VersionEntry> = chain.iter().cloned().collect();
            // Slot stays as-is — we only inspect the chain.
            Ok::<_, Error>(entries)
        },
    )??;
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

fn doc_bytes(doc: &bson::Document) -> Result<Vec<u8>> {
    bson::to_vec(doc).map_err(Error::BsonSerialization)
}

fn persisted_header(io: &Arc<MockIo>) -> Result<FileHeader> {
    let pages = io
        .pages
        .lock()
        .map_err(|_| Error::Internal("mock io pages mutex poisoned".into()))?;
    let page = pages
        .get(&0)
        .ok_or_else(|| Error::Internal("page 0 was not flushed".into()))?;
    let mut buf = [0u8; HEADER_PAGE_SIZE];
    buf.copy_from_slice(&page[..HEADER_PAGE_SIZE]);
    FileHeader::from_bytes(&buf)
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
        &FindOptions::default(),
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

#[test]
fn test_replayed_same_txn_pending_install_does_not_duplicate_delta_heads() -> Result<()> {
    const TXN_ID: u64 = 7;
    let engine = buffered_engine()?;
    let index = create_ready_email_index(&engine, READY_NS)?;
    let coll = collection_entry(&engine, READY_NS)?;
    let commit_ts = Ts {
        physical_ms: 50_000,
        logical: 0,
    };
    let id = Bson::Int32(91);
    let doc = doc! { "_id": 91, "email": "retry-idempotent@example.test" };
    let primary_key = encode_key(&id);
    let secondary_key = secondary_key("retry-idempotent@example.test", &id);
    let vis = super::visibility::WriteVisibility::new(&engine.shared, READY_NS)?;

    let primary = PrimaryWrite {
        ns_id: coll.id,
        ns: Ns::from(READY_NS),
        root_page: coll.data_root_page,
        root_level: coll.data_root_level,
        key: primary_key.clone(),
        expected_head: None,
        op: PrimaryOp::Insert {
            data: doc_bytes(&doc)?,
        },
    };
    let secondary = SecIndexWrite {
        index_id: index.id,
        index_root_page: index.root_page,
        index_root_level: index.root_level,
        unique_directions: None,
        key: secondary_key.clone(),
        expected_head: None,
        op: SecIndexOp::Insert {
            id_bytes: bson::to_vec(&doc! { "_id": 91 }).map_err(Error::BsonSerialization)?,
        },
    };

    super::index_maint::install_pending_primary(
        &engine.shared,
        &engine.metadata_state,
        vec![primary.clone()],
        &vis,
        commit_ts,
        TXN_ID,
    )?;
    super::index_maint::install_pending_primary(
        &engine.shared,
        &engine.metadata_state,
        vec![primary],
        &vis,
        commit_ts,
        TXN_ID,
    )?;
    super::index_maint::install_pending_sec_index(
        &engine.shared,
        &engine.metadata_state,
        vec![secondary.clone()],
        &vis,
        commit_ts,
        TXN_ID,
    )?;
    super::index_maint::install_pending_sec_index(
        &engine.shared,
        &engine.metadata_state,
        vec![secondary],
        &vis,
        commit_ts,
        TXN_ID,
    )?;

    let (_, primary_chain) = take_chain(
        &engine,
        coll.data_root_page,
        coll.data_root_level,
        &primary_key,
    )?;
    let (_, secondary_chain) =
        take_chain(&engine, index.root_page, index.root_level, &secondary_key)?;

    assert_eq!(primary_chain.len(), 1);
    assert_eq!(secondary_chain.len(), 1);
    assert!(matches!(
        primary_chain[0].state,
        VersionState::Pending { txn_id: TXN_ID }
    ));
    assert!(matches!(
        secondary_chain[0].state,
        VersionState::Pending { txn_id: TXN_ID }
    ));
    Ok(())
}

#[test]
fn test_history_store_reopens_from_header_persisted_root() -> Result<()> {
    let io = Arc::new(MockIo::default());
    let engine = buffered_engine_from_io(Arc::clone(&io), FileHeader::new_now(), 0, 0)?;
    let ident = TreeIdent {
        collection_id: 99,
        kind: TreeKind::Primary,
    };
    let entry = VersionEntry {
        start_ts: Ts {
            physical_ms: 10,
            logical: 0,
        },
        stop_ts: Ts {
            physical_ms: 40,
            logical: 0,
        },
        txn_id: 7,
        state: VersionState::Committed,
        data: VersionData::Inline(b"reopened-history".to_vec()),
        is_tombstone: false,
    };
    let mut spill_txn = HistorySpillTxn::new();
    HistoryStore::<BufferPoolPageStore>::spill_primary(
        &mut spill_txn,
        ident,
        b"doc-99",
        &entry,
        0,
    )?;
    {
        let mut history = engine
            .shared
            .history_store
            .lock()
            .map_err(|_| Error::Internal("history_store mutex poisoned".into()))?;
        history.commit_spill_txn_durable(spill_txn)?;
    }
    engine.shared.handle.flush()?;

    let persisted = persisted_header(&io)?;
    assert_ne!(persisted.history_store_root_page, 0);
    assert_eq!(persisted.history_store_root_level, 0);
    let catalog_root_page = persisted.catalog_root_page;
    let catalog_root_level = persisted.catalog_root_level;
    drop(engine);

    let reopened = buffered_engine_from_io(
        Arc::clone(&io),
        persisted,
        catalog_root_page,
        catalog_root_level,
    )?;
    let visible = reopened
        .shared
        .history_store
        .lock()
        .map_err(|_| Error::Internal("history_store mutex poisoned".into()))?
        .probe_primary(
            99,
            b"doc-99",
            Ts {
                physical_ms: 20,
                logical: 0,
            },
        )?
        .ok_or_else(|| Error::Internal("history entry did not survive reopen".into()))?;

    assert_inline(&visible, b"reopened-history")?;
    Ok(())
}
