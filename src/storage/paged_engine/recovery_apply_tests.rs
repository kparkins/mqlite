use std::fs;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bson::Bson;

use super::super::{catalog_ops, PagedEngine};
use super::apply_parsed_logical_frames;
use crate::client::Client;
use crate::journal::log_file::{
    LogicalOp, LogicalOpKind, LogicalTxnFrame, OverflowRefWire, LOGICAL_TXN_FORMAT_VERSION,
};
use crate::journal::JournalManager;
use crate::journal::ParsedLogicalFrames;
use crate::keys::encode_key;
use crate::mvcc::read_view::ReadView;
use crate::mvcc::timestamp::Ts;
use crate::mvcc::{VersionEntry, VersionState};
use crate::options::OpenOptions as DbOpenOptions;
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::catalog::CollectionEntry;
use crate::storage::engine::StorageEngine;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "test.us018b";
const COMMIT_TS: Ts = Ts {
    physical_ms: 9_000_000_000_000,
    logical: 0,
};
const OP_ORDINAL: u32 = 7;
const OPEN_FAILURE_COMMIT_TS: Ts = Ts {
    physical_ms: 9_000_000_000_001,
    logical: 0,
};

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

fn collection(engine: &PagedEngine) -> CollectionEntry {
    let md = engine.metadata.read().expect("metadata read");
    let entry = catalog_ops::catalog_lock(&md)
        .get_collection(NS)
        .expect("read catalog")
        .expect("collection exists");
    entry
}

fn primary_chain_for_key(
    engine: &PagedEngine,
    coll: &CollectionEntry,
    key: &[u8],
) -> Vec<VersionEntry> {
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&engine.shared.handle)),
        coll.data_root_page,
        coll.data_root_level,
    );
    let leaf = tree.find_leaf(key).expect("find leaf");
    let chain = engine
        .shared
        .handle
        .pool()
        .take_chain(leaf, key)
        .expect("take chain")
        .expect("chain exists");
    let entries: Vec<VersionEntry> = chain.iter().cloned().collect();
    engine
        .shared
        .handle
        .pool()
        .put_chain(leaf, key.to_vec(), chain)
        .expect("restore chain");
    entries
}

fn parsed_frame(ops: Vec<LogicalOp>) -> ParsedLogicalFrames {
    ParsedLogicalFrames {
        frames: vec![(
            100,
            LogicalTxnFrame {
                salt1: 0,
                salt2: 0,
                commit_ts: COMMIT_TS,
                diagnostic_txn_id: 42,
                format_version: LOGICAL_TXN_FORMAT_VERSION,
                flags: 0,
                ops,
            },
        )],
        ..Default::default()
    }
}

fn primary_insert(ns_id: i64, id: i32, op_ordinal: u32) -> LogicalOp {
    LogicalOp {
        op_ordinal,
        kind: LogicalOpKind::PrimaryInsert {
            ns_id,
            key: encode_key(&Bson::Int32(id)),
            value: format!("doc-{id}").into_bytes(),
            overflow: None,
        },
    }
}

fn journal_path(db_path: &Path) -> std::path::PathBuf {
    let mut journal = db_path.as_os_str().to_owned();
    journal.push("-journal");
    std::path::PathBuf::from(journal)
}

fn read_main_header(db_path: &Path) -> FileHeader {
    use std::io::{Read, Seek, SeekFrom};

    let mut main_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(db_path)
        .expect("open main file");
    let mut buf = [0u8; HEADER_PAGE_SIZE];
    main_file.seek(SeekFrom::Start(0)).expect("seek header");
    main_file.read_exact(&mut buf).expect("read header");
    FileHeader::from_bytes(&buf).expect("decode header")
}

fn append_bad_overflow_replay_frame(db_path: &Path) {
    let mut main_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(db_path)
        .expect("open main file");
    let header = read_main_header(db_path);
    let mut mgr =
        JournalManager::open_or_create(db_path, &header, &mut main_file).expect("open journal");
    let (salt1, salt2) = mgr.salts();
    let bad_op = LogicalOp {
        op_ordinal: 1,
        kind: LogicalOpKind::PrimaryInsert {
            ns_id: 1,
            key: encode_key(&Bson::Int32(11)),
            value: Vec::new(),
            overflow: Some(OverflowRefWire {
                first_page: 123,
                total_len: 456,
            }),
        },
    };
    let frame = LogicalTxnFrame {
        salt1,
        salt2,
        commit_ts: OPEN_FAILURE_COMMIT_TS,
        diagnostic_txn_id: 42,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![primary_insert(1, 10, 0), bad_op],
    };

    mgr.append_logical_txn(frame).expect("append logical");
    mgr.append_chain_commit(OPEN_FAILURE_COMMIT_TS, vec![], vec![])
        .expect("append chain commit");
    mgr.sync_journal().expect("sync journal");
}

#[test]
fn test_replay_applier_is_idempotent_on_double_apply() {
    let engine = buffered_engine();
    engine.create_namespace(NS).expect("create namespace");
    engine
        .shared
        .recovery_open_published_store_count
        .store(0, Ordering::Relaxed);

    let coll = collection(&engine);
    let key = encode_key(&Bson::Int32(1));
    let parsed = parsed_frame(vec![primary_insert(coll.id, 1, OP_ORDINAL)]);

    {
        let md = engine.metadata.read().expect("metadata read");
        apply_parsed_logical_frames(&engine.shared, &md, &parsed).expect("first apply");
    }
    let after_first = primary_chain_for_key(&engine, &coll, &key);

    {
        let md = engine.metadata.read().expect("metadata read");
        apply_parsed_logical_frames(&engine.shared, &md, &parsed).expect("second apply");
    }
    let after_second = primary_chain_for_key(&engine, &coll, &key);

    assert_eq!(
        after_second.len(),
        after_first.len(),
        "second replay must not grow chain depth"
    );
    assert_eq!(after_second[0].start_ts, COMMIT_TS);
    assert_eq!(after_second[0].txn_id, u64::from(OP_ORDINAL));
    assert!(matches!(after_second[0].state, VersionState::Committed));
    assert_eq!(
        engine
            .shared
            .recovery_open_published_store_count
            .load(Ordering::Relaxed),
        0,
        "the applier itself must not publish"
    );
}

#[test]
fn test_replay_applier_failure_leaves_no_reader_visible_partial_state() {
    let engine = buffered_engine();
    engine.create_namespace(NS).expect("create namespace");
    engine
        .shared
        .recovery_open_published_store_count
        .store(0, Ordering::Relaxed);

    let coll = collection(&engine);
    let visible_before_replay = engine.shared.load_published();
    let pre_replay_view = ReadView::open_for_epoch(
        Arc::clone(engine.shared.handle.read_view_registry()),
        visible_before_replay,
        99,
    );
    let good_key = encode_key(&Bson::Int32(1));
    let bad_key = encode_key(&Bson::Int32(2));
    let mut bad_op = primary_insert(coll.id, 2, 1);
    bad_op.kind = LogicalOpKind::PrimaryInsert {
        ns_id: coll.id,
        key: bad_key,
        value: Vec::new(),
        overflow: Some(OverflowRefWire {
            first_page: 123,
            total_len: 456,
        }),
    };
    let parsed = parsed_frame(vec![primary_insert(coll.id, 1, 0), bad_op]);

    let err = {
        let md = engine.metadata.read().expect("metadata read");
        apply_parsed_logical_frames(&engine.shared, &md, &parsed)
            .expect_err("overflow replay should fail")
    };
    assert!(
        format!("{err:?}").contains("overflow payloads"),
        "expected overflow failure, got {err:?}"
    );

    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&engine.shared.handle)),
        coll.data_root_page,
        coll.data_root_level,
    );
    let leaf = tree.find_leaf(&good_key).expect("find leaf");
    let snapshot = engine
        .shared
        .handle
        .pool()
        .snapshot_chains(leaf, Some(Arc::clone(&pre_replay_view)))
        .expect("snapshot chains")
        .expect("resident leaf");
    assert!(
        snapshot.visible_at(&good_key, &pre_replay_view).is_none(),
        "a reader pinned before replay must not see the partial committed delta"
    );
    assert_eq!(
        engine
            .shared
            .recovery_open_published_store_count
            .load(Ordering::Relaxed),
        0,
        "failed replay must not perform the end-of-open publish"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("open_failure.mqlite");
    {
        let client =
            Client::open_with_options(&db_path, DbOpenOptions::new()).expect("open setup client");
        client
            .database("d")
            .create_collection("c")
            .expect("create setup collection");
        client.close().expect("checkpoint setup catalog");
    }
    append_bad_overflow_replay_frame(&db_path);
    let journal_path = journal_path(&db_path);
    let main_before = fs::read(&db_path).expect("read main before failed open");
    let journal_before = fs::read(&journal_path).expect("read journal before failed open");

    let open_err = match Client::open_with_options(&db_path, DbOpenOptions::new()) {
        Ok(_) => panic!("open must fail when recovery replay hits unsupported overflow"),
        Err(err) => err,
    };
    assert!(
        format!("{open_err:?}").contains("overflow payloads"),
        "expected open-surface overflow replay failure, got {open_err:?}"
    );
    assert_eq!(
        fs::read(&db_path).expect("read main after failed open"),
        main_before,
        "failed replay open must not mutate the main database file"
    );
    assert_eq!(
        fs::read(&journal_path).expect("read journal after failed open"),
        journal_before,
        "failed replay open must not mutate the journal file"
    );
}
