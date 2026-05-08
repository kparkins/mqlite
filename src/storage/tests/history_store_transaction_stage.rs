//! US-011 staged history-spill API regression tests.

use super::*;

use crate::storage::btree::MemPageStore;
use crate::storage::reconcile::driver::{TreeIdent, TreeKind};

const COLLECTION_ID: i64 = 11;
const SECONDARY_INDEX_ID: i64 = 29;

fn ts(ms: u64, logical: u32) -> Ts {
    Ts {
        physical_ms: ms,
        logical,
    }
}

fn primary_ident() -> TreeIdent {
    TreeIdent {
        collection_id: COLLECTION_ID,
        kind: TreeKind::Primary,
    }
}

fn secondary_ident() -> TreeIdent {
    TreeIdent {
        collection_id: COLLECTION_ID,
        kind: TreeKind::Secondary {
            index_id: SECONDARY_INDEX_ID,
        },
    }
}

fn inline_entry(start: Ts, stop: Ts, payload: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: start,
        stop_ts: stop,
        txn_id: 42,
        state: VersionState::Committed,
        data: VersionData::Inline(payload.to_vec()),
        is_tombstone: false,
    }
}

fn inline_payload(entry: VersionEntry) -> Vec<u8> {
    match entry.data {
        VersionData::Inline(bytes) => bytes,
        VersionData::Overflow(_) => panic!("expected inline payload"),
    }
}

#[test]
fn spill_primary_stages_until_history_spill_txn_commits() {
    let mut history = HistoryStore::create(MemPageStore::new()).unwrap();
    let entry = inline_entry(ts(10, 0), ts(20, 0), b"staged-primary");
    let mut txn = HistorySpillTxn::new();

    HistoryStore::<MemPageStore>::spill_primary(&mut txn, primary_ident(), b"doc-1", &entry, 0)
        .unwrap();

    assert_eq!(txn.len(), 1);
    assert!(
        history
            .probe_primary(COLLECTION_ID, b"doc-1", ts(15, 0))
            .unwrap()
            .is_none(),
        "staged writes must not touch the B-tree before commit"
    );

    history.commit_spill_txn(txn).unwrap();
    let visible = history
        .probe_primary(COLLECTION_ID, b"doc-1", ts(15, 0))
        .unwrap()
        .expect("committed spill is visible to cold-read probe");
    assert_eq!(inline_payload(visible), b"staged-primary");
}

#[test]
fn spill_sec_index_stages_secondary_tree_identity() {
    let mut history = HistoryStore::create(MemPageStore::new()).unwrap();
    let entry = inline_entry(ts(30, 0), ts(60, 0), b"staged-secondary");
    let mut txn = HistorySpillTxn::new();

    HistoryStore::<MemPageStore>::spill_sec_index(
        &mut txn,
        secondary_ident(),
        b"email\0doc-1",
        &entry,
        2,
    )
    .unwrap();
    history.commit_spill_txn(txn).unwrap();

    assert!(
        history
            .probe_primary(COLLECTION_ID, b"email\0doc-1", ts(40, 0))
            .unwrap()
            .is_none(),
        "secondary spills must not alias the primary tree"
    );
    let visible = history
        .probe_sec_index(
            COLLECTION_ID,
            SECONDARY_INDEX_ID,
            b"email\0doc-1",
            ts(40, 0),
        )
        .unwrap()
        .expect("secondary spill is visible through secondary probe");
    assert_eq!(inline_payload(visible), b"staged-secondary");
}
