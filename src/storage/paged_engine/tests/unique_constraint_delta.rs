use std::cell::Cell;
use std::collections::VecDeque;
use std::sync::Arc;

use bson::{doc, Bson, Document};

use super::doc_helpers::check_unique_constraints_mvcc;
use crate::error::{Error, Result};
use crate::keys::encode_key;
use crate::mvcc::transaction::Ns;
use crate::mvcc::{PrimaryOp, PrimaryWrite, ReadView, Ts, VersionData, VersionEntry, VersionState};
use crate::storage::btree::{BTree, BTreePageStore, MemPageStore};

const NS_ID: i64 = 1;
const TXN_ID: u64 = 7;
const READER_TXN_ID: u64 = 99;
const READ_TS: Ts = Ts {
    physical_ms: 200,
    logical: 0,
};
const VERSION_START_TS: Ts = Ts {
    physical_ms: 100,
    logical: 0,
};

thread_local! {
    static INSTALL_PENDING_PRIMARY_CALLS: Cell<u64> = const { Cell::new(0) };
}

/// Record an `install_pending_primary` call on the current test build.
pub(super) fn record_install_pending_primary_call() {
    INSTALL_PENDING_PRIMARY_CALLS.with(|calls| calls.set(calls.get() + 1));
}

fn reset_install_pending_primary_calls() {
    INSTALL_PENDING_PRIMARY_CALLS.with(|calls| calls.set(0));
}

fn install_pending_primary_calls() -> u64 {
    INSTALL_PENDING_PRIMARY_CALLS.with(Cell::get)
}

fn unique_specs() -> Vec<(String, Vec<String>, bool)> {
    vec![("email_unique".to_owned(), vec!["email".to_owned()], false)]
}

fn test_doc(id: i32, email: &str) -> Document {
    doc! { "_id": id, "email": email }
}

fn doc_bytes(doc: &Document) -> Result<Vec<u8>> {
    bson::to_vec(doc).map_err(Error::BsonSerialization)
}

fn live_entry(doc: &Document) -> Result<VersionEntry> {
    Ok(VersionEntry {
        start_ts: VERSION_START_TS,
        stop_ts: Ts::MAX,
        txn_id: TXN_ID,
        state: VersionState::Committed,
        data: VersionData::Inline(doc_bytes(doc)?),
        is_tombstone: false,
    })
}

fn install_chain(tree: &mut BTree<MemPageStore>, doc: &Document) -> Result<()> {
    let id = doc.get("_id").unwrap_or(&Bson::Null);
    let key = encode_key(id);
    let leaf = tree.find_leaf(&key)?;
    let chain = VecDeque::from([live_entry(doc)?]);
    tree.store.put_chain(leaf, key, Arc::new(chain))
}

fn pending_insert(ns: &str, doc: &Document) -> Result<PrimaryWrite> {
    Ok(PrimaryWrite {
        ns_id: NS_ID,
        ns: Ns::from(ns),
        root_page: 0,
        root_level: 0,
        key: encode_key(doc.get("_id").unwrap_or(&Bson::Null)),
        expected_head: None,
        op: PrimaryOp::Insert {
            data: doc_bytes(doc)?,
        },
    })
}

fn read_view() -> ReadView {
    ReadView::new(READ_TS, READER_TXN_ID)
}

fn assert_duplicate(result: Result<()>) {
    assert!(matches!(result, Err(Error::DuplicateKey { .. })));
}

fn assert_duplicate_detail(result: Result<()>) {
    assert_eq!(
        result.unwrap_err().to_string(),
        "duplicate key error: E11000 duplicate key error — unique index 'email_unique': \
         dup key {email: Some(String(\"a@example.com\"))}"
    );
}

#[test]
fn test_primary_unique_detects_delta_only_conflict() -> Result<()> {
    let ns = "test.us009.delta";
    let mut tree = BTree::create(MemPageStore::new())?;
    install_chain(&mut tree, &test_doc(1, "a@example.com"))?;

    let result = check_unique_constraints_mvcc(
        &tree,
        &unique_specs(),
        &test_doc(2, "a@example.com"),
        &read_view(),
        None,
        &[],
        ns,
    );

    assert_duplicate_detail(result);
    Ok(())
}

#[test]
fn test_primary_unique_detects_same_txn_staged_conflict() -> Result<()> {
    let ns = "test.us009.staged";
    let tree = BTree::create(MemPageStore::new())?;
    let pending = [pending_insert(ns, &test_doc(1, "a@example.com"))?];

    let result = check_unique_constraints_mvcc(
        &tree,
        &unique_specs(),
        &test_doc(2, "a@example.com"),
        &read_view(),
        None,
        &pending,
        ns,
    );

    assert_duplicate_detail(result);
    Ok(())
}

#[test]
fn test_primary_unique_same_txn_conflict_detected_at_stage_not_install() -> Result<()> {
    let ns = "test.us009.stage";
    let tree = BTree::create(MemPageStore::new())?;
    let pending = [pending_insert(ns, &test_doc(1, "a@example.com"))?];

    reset_install_pending_primary_calls();
    let result = check_unique_constraints_mvcc(
        &tree,
        &unique_specs(),
        &test_doc(2, "a@example.com"),
        &read_view(),
        None,
        &pending,
        ns,
    );

    assert_duplicate(result);
    assert_eq!(install_pending_primary_calls(), 0);
    Ok(())
}

#[test]
fn test_primary_unique_allows_same_id_self_update() -> Result<()> {
    let ns = "test.us009.self";
    let tree = BTree::create(MemPageStore::new())?;
    let pending = [pending_insert(ns, &test_doc(1, "a@example.com"))?];

    check_unique_constraints_mvcc(
        &tree,
        &unique_specs(),
        &test_doc(1, "a@example.com"),
        &read_view(),
        None,
        &pending,
        ns,
    )
}
