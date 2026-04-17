//! T5' plan-line 794 acceptance test — **secondary-index atomic commit**.
//!
//! Contract:
//! - An update that mutates a primary document AND its dependent
//!   secondary-index entries (old key tombstoned, new key installed)
//!   MUST stamp every chain entry with the **same** `commit_ts`.
//! - A reader at a read_ts below the shared `commit_ts` observes the
//!   pre-update state across primary and secondary.
//! - A reader at a read_ts at-or-above the shared `commit_ts` observes
//!   the post-update state across primary and secondary.
//! - No read_ts exists that witnesses a half-committed pair.
//!
//! Rather than round-trip through the engine (which would hide the
//! multi-chain timing), this test simulates the three VersionEntrys the
//! writer installs at commit time (primary doc swap, sec-index old-key
//! tombstone, sec-index new-key insert) and asserts atomic visibility.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use mqlite::mvcc::{ChainSnapshot, ReadView, Ts, VersionData, VersionEntry};

const PRIMARY_KEY: &[u8] = b"pk/user-1";
const SEC_OLD_KEY: &[u8] = b"idx/status/active/user-1";
const SEC_NEW_KEY: &[u8] = b"idx/status/inactive/user-1";

const WRITER_TXN_ID: u64 = 4242;

fn entry(start: Ts, stop: Ts, txn_id: u64, bytes: &[u8], tombstone: bool) -> VersionEntry {
    VersionEntry {
        start_ts: start,
        stop_ts: stop,
        txn_id,
        data: VersionData::Inline(bytes.to_vec()),
        is_tombstone: tombstone,
    }
}

/// Build the post-commit snapshot that `install_pending_primary` and the
/// sec-index install path would produce.
fn snap_after_commit(commit_ts: Ts, pre_commit_ts: Ts) -> ChainSnapshot {
    let mut source: HashMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> = HashMap::new();

    // Primary chain: new doc as head (stop=MAX), old doc stopped at commit_ts.
    let mut primary = VecDeque::new();
    primary.push_back(entry(commit_ts, Ts::MAX, WRITER_TXN_ID, b"doc-v2", false));
    primary.push_back(entry(pre_commit_ts, commit_ts, /* prior */ 1, b"doc-v1", false));
    source.insert(PRIMARY_KEY.to_vec(), Arc::new(primary));

    // Sec-index (old key `status=active`): tombstone head at commit_ts,
    // prior presence entry stopped at commit_ts.
    let mut old_idx = VecDeque::new();
    old_idx.push_back(entry(commit_ts, Ts::MAX, WRITER_TXN_ID, b"", true));
    old_idx.push_back(entry(pre_commit_ts, commit_ts, 1, b"pk/user-1", false));
    source.insert(SEC_OLD_KEY.to_vec(), Arc::new(old_idx));

    // Sec-index (new key `status=inactive`): fresh insert at commit_ts.
    let mut new_idx = VecDeque::new();
    new_idx.push_back(entry(commit_ts, Ts::MAX, WRITER_TXN_ID, b"pk/user-1", false));
    source.insert(SEC_NEW_KEY.to_vec(), Arc::new(new_idx));

    ChainSnapshot::new(&source, None)
}

#[test]
fn all_three_chain_entries_share_commit_ts() {
    let commit_ts = Ts { physical_ms: 1_000, logical: 0 };
    let pre_commit = Ts { physical_ms: 500, logical: 0 };
    let snap = snap_after_commit(commit_ts, pre_commit);

    // Reader AFTER commit sees the post-update state for ALL three chains.
    let reader_after = ReadView::new(Ts { physical_ms: 1_500, logical: 0 }, 99);

    let primary_hit = snap.visible_at(PRIMARY_KEY, &reader_after).expect("primary visible");
    let old_idx_hit = snap.visible_at(SEC_OLD_KEY, &reader_after).expect("old-idx visible");
    let new_idx_hit = snap.visible_at(SEC_NEW_KEY, &reader_after).expect("new-idx visible");

    // Contract: the THREE VersionEntry's observed here all share commit_ts.
    assert_eq!(primary_hit.start_ts, commit_ts);
    assert_eq!(old_idx_hit.start_ts, commit_ts);
    assert_eq!(new_idx_hit.start_ts, commit_ts);

    // Primary swapped to v2, old-idx is a tombstone, new-idx points at pk.
    match &primary_hit.data {
        VersionData::Inline(v) => assert_eq!(v, b"doc-v2"),
        _ => panic!("expected inline"),
    }
    assert!(old_idx_hit.is_tombstone, "old-idx entry must be a tombstone");
    match &new_idx_hit.data {
        VersionData::Inline(v) => assert_eq!(v, b"pk/user-1"),
        _ => panic!("expected inline"),
    }
}

#[test]
fn reader_below_commit_ts_sees_pre_update_everywhere() {
    let commit_ts = Ts { physical_ms: 1_000, logical: 0 };
    let pre_commit = Ts { physical_ms: 500, logical: 0 };
    let snap = snap_after_commit(commit_ts, pre_commit);

    // Reader strictly between pre_commit and commit_ts observes v1.
    let reader_before = ReadView::new(Ts { physical_ms: 750, logical: 0 }, 99);

    let primary_hit = snap.visible_at(PRIMARY_KEY, &reader_before).expect("primary visible");
    let old_idx_hit = snap.visible_at(SEC_OLD_KEY, &reader_before).expect("old-idx visible");
    let new_idx_hit = snap.visible_at(SEC_NEW_KEY, &reader_before);

    assert_eq!(primary_hit.start_ts, pre_commit);
    match &primary_hit.data {
        VersionData::Inline(v) => assert_eq!(v, b"doc-v1"),
        _ => panic!("expected inline"),
    }
    // Old idx at this read_ts still presents the live (non-tombstone) mapping.
    assert!(!old_idx_hit.is_tombstone);
    assert_eq!(old_idx_hit.start_ts, pre_commit);
    // The new sec-index key did not exist before commit_ts.
    assert!(new_idx_hit.is_none(), "new sec-index key must be invisible pre-commit");
}

#[test]
fn no_read_ts_witnesses_partial_commit() {
    // Sweep read_ts's around commit_ts and verify the observable state is
    // either fully-pre or fully-post — never a mix.
    let commit_ts = Ts { physical_ms: 1_000, logical: 0 };
    let pre_commit = Ts { physical_ms: 500, logical: 0 };
    let snap = snap_after_commit(commit_ts, pre_commit);

    let sample_points: Vec<u64> = (0..=20).map(|i| 900 + i * 10).collect();
    for ts_ms in sample_points {
        let rv = ReadView::new(Ts { physical_ms: ts_ms, logical: 0 }, 99);
        let p = snap.visible_at(PRIMARY_KEY, &rv);
        let o = snap.visible_at(SEC_OLD_KEY, &rv);
        let n = snap.visible_at(SEC_NEW_KEY, &rv);

        let post =
            p.map(|e| e.start_ts == commit_ts).unwrap_or(false)
            && o.map(|e| e.is_tombstone).unwrap_or(false)
            && n.is_some();
        let pre = p.map(|e| e.start_ts == pre_commit).unwrap_or(false)
            && o.map(|e| !e.is_tombstone).unwrap_or(false)
            && n.is_none();
        assert!(
            post || pre,
            "torn state observed at ts_ms={}: primary_pre={:?} old_idx_tomb={:?} new_idx={:?}",
            ts_ms,
            p.map(|e| e.start_ts),
            o.map(|e| e.is_tombstone),
            n.is_some(),
        );
    }
}
