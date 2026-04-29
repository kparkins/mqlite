//! US-013 history-spill ownership-transfer regression tests.

use super::*;

use std::mem::ManuallyDrop;
use std::sync::Arc;

use crate::storage::btree::MemPageStore;
use crate::storage::header::FileHeader;
use crate::storage::reconcile::plan::{TreeIdent, TreeKind};

const COLLECTION_ID: i64 = 13;
const FIRST_PAGE: u32 = 1300;
const TOTAL_LENGTH: u64 = 8192;
const SPILL_COUNTER: u32 = 0;

fn ts(ms: u64) -> Ts {
    Ts {
        physical_ms: ms,
        logical: 0,
    }
}

fn primary_ident() -> TreeIdent {
    TreeIdent {
        collection_id: COLLECTION_ID,
        kind: TreeKind::Primary,
    }
}

fn overflow_entry(first_page: u32, alloc: AllocatorHandle) -> VersionEntry {
    VersionEntry {
        start_ts: ts(10),
        stop_ts: ts(20),
        txn_id: 42,
        state: VersionState::Committed,
        data: VersionData::Overflow(
            OverflowRef::new_owned(first_page, TOTAL_LENGTH, alloc).unwrap(),
        ),
        is_tombstone: false,
    }
}

#[test]
fn history_spill_transfers_overflow_ref_before_live_invalidation() {
    let alloc = AllocatorHandle::new(FileHeader::new(0, 0, 0));
    let mut history = HistoryStore::create(MemPageStore::new())
        .unwrap()
        .with_overflow_allocator(Arc::new(alloc.clone()));
    let entry = overflow_entry(FIRST_PAGE, alloc.clone());
    assert_eq!(alloc.overflow_refcount(FIRST_PAGE), 1);

    let mut first_txn = HistorySpillTxn::new();
    HistoryStore::<MemPageStore>::spill_primary(
        &mut first_txn,
        primary_ident(),
        b"doc",
        &entry,
        SPILL_COUNTER,
    )
    .unwrap();
    assert_eq!(
        alloc.overflow_refcount(FIRST_PAGE),
        2,
        "staging must incref the history-side ownership before live invalidation"
    );

    history.commit_spill_txn(first_txn).unwrap();
    assert_eq!(
        alloc.overflow_refcount(FIRST_PAGE),
        2,
        "new history record must retain its transferred overflow ref"
    );

    let mut retry_txn = HistorySpillTxn::new();
    HistoryStore::<MemPageStore>::spill_primary(
        &mut retry_txn,
        primary_ident(),
        b"doc",
        &entry,
        SPILL_COUNTER,
    )
    .unwrap();
    assert_eq!(alloc.overflow_refcount(FIRST_PAGE), 3);
    history.commit_spill_txn(retry_txn).unwrap();
    assert_eq!(
        alloc.overflow_refcount(FIRST_PAGE),
        2,
        "byte-identical retry must release its temporary transfer ref"
    );

    drop(entry);
    assert_eq!(
        alloc.overflow_refcount(FIRST_PAGE),
        1,
        "history record must keep the overflow chain live after live entry drops"
    );

    let visible = history
        .probe_primary(COLLECTION_ID, b"doc", ts(15))
        .unwrap()
        .expect("history entry should remain probe-visible");
    match visible.data {
        VersionData::Overflow(oref) => {
            assert_eq!(oref.first_page(), FIRST_PAGE);
            assert_eq!(oref.total_length(), TOTAL_LENGTH);
            assert_eq!(alloc.overflow_refcount(FIRST_PAGE), 2);
            drop(oref);
        }
        VersionData::Inline(_) => panic!("expected overflow history payload"),
    }
    assert_eq!(alloc.overflow_refcount(FIRST_PAGE), 1);
}

#[test]
fn history_spill_refuses_dropped_overflow_ref_without_resurrecting() {
    let alloc = AllocatorHandle::new(FileHeader::new(0, 0, 0));
    let entry = ManuallyDrop::new(overflow_entry(FIRST_PAGE + 1, alloc.clone()));
    alloc.set_overflow_refcount_for_test(FIRST_PAGE + 1, 0);
    let mut txn = HistorySpillTxn::new();

    let err = HistoryStore::<MemPageStore>::spill_primary(
        &mut txn,
        primary_ident(),
        b"doc",
        &entry,
        SPILL_COUNTER,
    )
    .unwrap_err();

    assert!(
        matches!(err, Error::Internal(message) if message.contains("dropped before history spill"))
    );
    assert_eq!(txn.len(), 0);
    assert_eq!(alloc.overflow_refcount(FIRST_PAGE + 1), 0);
    assert_eq!(alloc.page_lifetime_queue().depth(), 0);
}
