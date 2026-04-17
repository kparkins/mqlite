//! T5' plan-line 801 acceptance test — **multikey tombstone batch
//! atomicity**.
//!
//! Contract:
//! - Updating a document whose indexed field is an array (multikey
//!   index) removes one element from the array; every orphaned
//!   sec-index entry receives a tombstone at commit time.
//! - ALL tombstones in the batch MUST share the single `commit_ts` used
//!   by the primary update.
//! - Unaffected sec-index keys retain their pre-update `start_ts`.
//! - Readers observe all-or-nothing across the primary update and the
//!   orphan tombstones.
//!
//! Scenario: a document with `tags: ["red","green","blue"]` is updated
//! to `tags: ["red","blue"]`. The sec-index chain for key `tags=green`
//! gains a tombstone head; `tags=red` and `tags=blue` remain untouched.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use mqlite::mvcc::{ChainSnapshot, ReadView, Ts, VersionData, VersionEntry};

const PRIMARY_KEY: &[u8] = b"pk/item-1";
const IDX_RED: &[u8] = b"idx/tags/red/item-1";
const IDX_GREEN: &[u8] = b"idx/tags/green/item-1";
const IDX_BLUE: &[u8] = b"idx/tags/blue/item-1";

const WRITER_TXN_ID: u64 = 77;

fn entry(start: Ts, stop: Ts, txn_id: u64, bytes: &[u8], tombstone: bool) -> VersionEntry {
    VersionEntry {
        start_ts: start,
        stop_ts: stop,
        txn_id,
        data: VersionData::Inline(bytes.to_vec()),
        is_tombstone: tombstone,
    }
}

fn snap_after_multikey_update(commit_ts: Ts, pre_commit: Ts) -> ChainSnapshot {
    let mut source: HashMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> = HashMap::new();

    // Primary chain: new bytes at commit_ts, prior stopped at commit_ts.
    let mut pri = VecDeque::new();
    pri.push_back(entry(commit_ts, Ts::MAX, WRITER_TXN_ID, b"doc-v2", false));
    pri.push_back(entry(pre_commit, commit_ts, 1, b"doc-v1", false));
    source.insert(PRIMARY_KEY.to_vec(), Arc::new(pri));

    // Sec-index key `tags=red`: unchanged, only the pre_commit entry.
    let mut red = VecDeque::new();
    red.push_back(entry(pre_commit, Ts::MAX, 1, b"pk/item-1", false));
    source.insert(IDX_RED.to_vec(), Arc::new(red));

    // Sec-index key `tags=green`: ORPHAN — tombstoned at commit_ts.
    let mut green = VecDeque::new();
    green.push_back(entry(commit_ts, Ts::MAX, WRITER_TXN_ID, b"", true));
    green.push_back(entry(pre_commit, commit_ts, 1, b"pk/item-1", false));
    source.insert(IDX_GREEN.to_vec(), Arc::new(green));

    // Sec-index key `tags=blue`: unchanged, like red.
    let mut blue = VecDeque::new();
    blue.push_back(entry(pre_commit, Ts::MAX, 1, b"pk/item-1", false));
    source.insert(IDX_BLUE.to_vec(), Arc::new(blue));

    ChainSnapshot::new(&source, None)
}

#[test]
fn orphan_tombstone_shares_commit_ts_with_primary() {
    let commit_ts = Ts { physical_ms: 900, logical: 0 };
    let pre_commit = Ts { physical_ms: 400, logical: 0 };
    let snap = snap_after_multikey_update(commit_ts, pre_commit);

    let reader_after = ReadView::new(Ts { physical_ms: 1_500, logical: 0 }, 99);

    let pri = snap.visible_at(PRIMARY_KEY, &reader_after).expect("primary");
    let green = snap.visible_at(IDX_GREEN, &reader_after).expect("green idx");
    assert_eq!(
        pri.start_ts, commit_ts,
        "primary update must be stamped with commit_ts"
    );
    assert!(green.is_tombstone, "green idx must be a tombstone");
    assert_eq!(
        green.start_ts, commit_ts,
        "green tombstone must share commit_ts with primary"
    );
}

#[test]
fn unaffected_sec_keys_retain_precommit_ts() {
    let commit_ts = Ts { physical_ms: 900, logical: 0 };
    let pre_commit = Ts { physical_ms: 400, logical: 0 };
    let snap = snap_after_multikey_update(commit_ts, pre_commit);

    let reader_after = ReadView::new(Ts { physical_ms: 1_500, logical: 0 }, 99);

    for (label, key) in [("red", IDX_RED), ("blue", IDX_BLUE)] {
        let e = snap
            .visible_at(key, &reader_after)
            .unwrap_or_else(|| panic!("{label} idx must be visible"));
        assert!(
            !e.is_tombstone,
            "{label} idx must not be tombstoned by an unrelated array-element removal"
        );
        assert_eq!(
            e.start_ts, pre_commit,
            "{label} idx must retain pre-commit start_ts (commit did not touch it)"
        );
    }
}

#[test]
fn no_reader_witnesses_torn_multikey_state() {
    // Across a grid of read_ts's bracketing commit_ts, readers must see
    // a coherent snapshot: either (primary-v1 + green live) or (primary-v2
    // + green tombstone). Red and blue remain live across the span.
    let commit_ts = Ts { physical_ms: 900, logical: 0 };
    let pre_commit = Ts { physical_ms: 400, logical: 0 };
    let snap = snap_after_multikey_update(commit_ts, pre_commit);

    for ts_ms in [500u64, 700, 899, 900, 901, 1_000, 2_000] {
        let rv = ReadView::new(Ts { physical_ms: ts_ms, logical: 0 }, 99);
        let pri = snap.visible_at(PRIMARY_KEY, &rv).expect("primary always visible");
        let green = snap.visible_at(IDX_GREEN, &rv).expect("green entry always visible");
        let is_post = pri.start_ts == commit_ts;
        if is_post {
            assert!(
                green.is_tombstone,
                "post-commit reader at ts_ms={ts_ms} must see green tombstoned"
            );
        } else {
            assert!(
                !green.is_tombstone,
                "pre-commit reader at ts_ms={ts_ms} must see green live"
            );
        }
        // Red + blue always live (unaffected by the update).
        let red = snap.visible_at(IDX_RED, &rv).expect("red visible");
        let blue = snap.visible_at(IDX_BLUE, &rv).expect("blue visible");
        assert!(!red.is_tombstone);
        assert!(!blue.is_tombstone);
    }
}
