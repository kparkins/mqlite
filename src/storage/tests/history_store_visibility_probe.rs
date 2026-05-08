//! US-012 focused history-probe tests.

use super::*;
use crate::storage::btree::MemPageStore;
use crate::storage::reconcile::driver::{TreeIdent, TreeKind};

const COLLECTION_ID: i64 = 12;

fn ts(ms: u64) -> Ts {
    Ts {
        physical_ms: ms,
        logical: 0,
    }
}

fn primary_ident(collection_id: i64) -> TreeIdent {
    TreeIdent {
        collection_id,
        kind: TreeKind::Primary,
    }
}

fn inline_entry(start: Ts, stop: Ts, payload: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: start,
        stop_ts: stop,
        txn_id: 1,
        state: VersionState::Committed,
        data: VersionData::Inline(payload.to_vec()),
        is_tombstone: false,
    }
}

fn tombstone(start: Ts, stop: Ts) -> VersionEntry {
    VersionEntry {
        start_ts: start,
        stop_ts: stop,
        txn_id: 2,
        state: VersionState::Committed,
        data: VersionData::Inline(Vec::new()),
        is_tombstone: true,
    }
}

fn spill_primary(
    history: &mut HistoryStore<MemPageStore>,
    key: &[u8],
    entry: &VersionEntry,
    counter: u32,
) -> Result<()> {
    let mut txn = HistorySpillTxn::new();
    HistoryStore::<MemPageStore>::spill_primary(
        &mut txn,
        primary_ident(COLLECTION_ID),
        key,
        entry,
        counter,
    )?;
    history.commit_spill_txn(txn)
}

fn inline_payload(entry: VersionEntry) -> Vec<u8> {
    match entry.data {
        VersionData::Inline(bytes) => bytes,
        VersionData::Overflow(_) => panic!("expected inline payload"),
    }
}

#[test]
fn test_history_probe_full_visibility() -> Result<()> {
    let mut history = HistoryStore::create(MemPageStore::new())?;
    let key = b"doc-1";

    spill_primary(&mut history, key, &inline_entry(ts(10), ts(20), b"v10"), 0)?;
    spill_primary(&mut history, key, &inline_entry(ts(20), ts(40), b"v20"), 0)?;

    let visible = history
        .probe_primary(COLLECTION_ID, key, ts(39))?
        .expect("read before stop_ts should see the entry");
    assert_eq!(inline_payload(visible), b"v20");

    assert!(
        history.probe_primary(COLLECTION_ID, key, ts(40))?.is_none(),
        "read_ts equal to stop_ts must not see the stopped entry"
    );
    assert!(
        history.probe_primary(COLLECTION_ID, key, ts(9))?.is_none(),
        "read_ts before start_ts must not see the entry"
    );
    Ok(())
}

#[test]
fn test_history_probe_returns_primary_tombstone() -> Result<()> {
    let mut history = HistoryStore::create(MemPageStore::new())?;
    let key = b"doc-2";

    spill_primary(
        &mut history,
        key,
        &inline_entry(ts(10), ts(20), b"before-delete"),
        0,
    )?;
    spill_primary(&mut history, key, &tombstone(ts(20), Ts::MAX), 0)?;

    let visible = history
        .probe_primary(COLLECTION_ID, key, ts(25))?
        .expect("primary tombstone should be returned to readers");
    assert!(visible.is_tombstone);
    assert_eq!(visible.start_ts, ts(20));
    Ok(())
}
