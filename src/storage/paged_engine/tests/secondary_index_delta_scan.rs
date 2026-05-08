use super::*;

use std::collections::VecDeque;
use std::sync::Arc;

use bson::{doc, Bson, Document};

use crate::keys::{encode_compound_key, encode_key};
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::btree::{BTree, BTreePageStore};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::root_snapshot::{NamespaceSnapshot, PublishedIndex};
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "test.us007";
const EMAIL_INDEX: &str = "email_1";
const TXN_ID: u64 = 7_007;

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

fn namespace_snapshot(engine: &PagedEngine) -> NamespaceSnapshot {
    let epoch = engine.shared.load_published();
    epoch
        .catalog
        .get_by_name(NS)
        .expect("namespace snapshot exists")
        .clone()
}

fn ready_email_index(ns_snap: &NamespaceSnapshot) -> PublishedIndex {
    ns_snap
        .indexes
        .iter()
        .find(|idx| idx.name == EMAIL_INDEX)
        .expect("ready email index exists")
        .clone()
}

fn visible_ts(engine: &PagedEngine) -> Ts {
    engine.shared.load_published().visible_ts
}

fn committed_inline(ts: Ts, bytes: Vec<u8>, is_tombstone: bool) -> VersionEntry {
    VersionEntry {
        start_ts: ts,
        stop_ts: Ts::MAX,
        txn_id: TXN_ID,
        state: VersionState::Committed,
        data: VersionData::Inline(bytes),
        is_tombstone,
    }
}

fn install_primary_delta(
    engine: &PagedEngine,
    ns_snap: &NamespaceSnapshot,
    id: &Bson,
    doc: &Document,
    ts: Ts,
) {
    let key = encode_key(id);
    let bytes = bson::to_vec(doc).expect("serialize primary document");
    let mut tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&engine.shared.handle)),
        ns_snap.data_root_page,
        ns_snap.data_root_level,
    );
    let leaf = tree.find_leaf(&key).expect("primary leaf exists");
    let mut chain = VecDeque::new();
    chain.push_back(committed_inline(ts, bytes, false));
    tree.store
        .put_chain(leaf, key, Arc::new(chain))
        .expect("install primary delta chain");
}

fn secondary_key(email: &str, id: &Bson) -> Vec<u8> {
    let email = Bson::String(email.to_owned());
    encode_compound_key(&[(&email, true), (id, true)])
}

fn secondary_value(id: &Bson) -> Vec<u8> {
    bson::to_vec(&doc! { "_id": id.clone() }).expect("serialize index value")
}

fn install_secondary_delta(
    engine: &PagedEngine,
    index: &PublishedIndex,
    email: &str,
    id: &Bson,
    entry: VersionEntry,
) {
    let key = secondary_key(email, id);
    let mut tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&engine.shared.handle)),
        index.root_page,
        index.root_level,
    );
    let leaf = tree.find_leaf(&key).expect("secondary leaf exists");
    let mut chain = VecDeque::new();
    chain.push_back(entry);
    tree.store
        .put_chain(leaf, key, Arc::new(chain))
        .expect("install secondary delta chain");
}

fn install_delta_only_indexed_doc(engine: &PagedEngine, email: &str, id: i32) {
    let ns_snap = namespace_snapshot(engine);
    let index = ready_email_index(&ns_snap);
    let id_bson = Bson::Int32(id);
    let ts = visible_ts(engine);
    let doc = doc! { "_id": id, "email": email, "marker": "delta-only" };

    install_primary_delta(engine, &ns_snap, &id_bson, &doc, ts);
    install_secondary_delta(
        engine,
        &index,
        email,
        &id_bson,
        committed_inline(ts, secondary_value(&id_bson), false),
    );
}

#[test]
fn test_execute_index_scan_ready_sees_delta_only_secondary() {
    use super::state::ReadOpScope;

    let engine = buffered_engine();
    create_ready_email_index(&engine);
    install_delta_only_indexed_doc(&engine, "delta@example.test", 1);

    let _scope = ReadOpScope::new(1);
    let (docs, explain) = engine
        .find(
            NS,
            &doc! { "email": "delta@example.test" },
            &FindOptions::default(),
        )
        .unwrap();

    assert_eq!(explain.index_used.as_deref(), Some(EMAIL_INDEX));
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].get_i32("_id").unwrap(), 1);
}

#[test]
fn test_execute_index_scan_ready_hides_delta_only_secondary_tombstone() {
    let engine = buffered_engine();
    create_ready_email_index(&engine);

    let ns_snap = namespace_snapshot(&engine);
    let index = ready_email_index(&ns_snap);
    let id_bson = Bson::Int32(2);
    let ts = visible_ts(&engine);
    let doc = doc! { "_id": 2, "email": "deleted@example.test" };

    install_primary_delta(&engine, &ns_snap, &id_bson, &doc, ts);
    install_secondary_delta(
        &engine,
        &index,
        "deleted@example.test",
        &id_bson,
        committed_inline(ts, Vec::new(), true),
    );

    let (docs, explain) = engine
        .find(
            NS,
            &doc! { "email": "deleted@example.test" },
            &FindOptions::default(),
        )
        .unwrap();

    assert_eq!(explain.index_used.as_deref(), Some(EMAIL_INDEX));
    assert!(docs.is_empty());
}

#[test]
fn test_index_scan_and_collscan_agree_on_delta_only_entry() {
    let engine = buffered_engine();
    create_ready_email_index(&engine);
    install_delta_only_indexed_doc(&engine, "agree@example.test", 3);

    let filter = doc! { "email": "agree@example.test" };
    let (index_docs, explain) = engine.find(NS, &filter, &FindOptions::default()).unwrap();

    let epoch = engine.shared.load_published();
    let ns_snap = epoch
        .catalog
        .get_by_name(NS)
        .expect("namespace snapshot exists");
    let (_, collscan_pairs) = super::snapshot_ops::execute_snapshot_pairs_from_snap(
        &engine.shared,
        NS,
        ns_snap,
        &filter,
        Arc::clone(&epoch),
        false,
    )
    .unwrap();
    let collscan_docs: Vec<Document> = collscan_pairs.into_iter().map(|(_, doc)| doc).collect();

    assert_eq!(explain.index_used.as_deref(), Some(EMAIL_INDEX));
    assert_eq!(index_docs, collscan_docs);
    assert_eq!(index_docs.len(), 1);
}
