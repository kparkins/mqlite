use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::sync::Arc;

use crate::mvcc::version::{OverflowRef, VersionData, VersionEntry, VersionState};
use crate::storage::allocator::AllocatorHandle;
use crate::storage::header::FileHeader;

use super::ChainSnapshot;
use crate::mvcc::read_view::ReadView;

fn ts(physical_ms: u64) -> crate::mvcc::Ts {
    crate::mvcc::Ts {
        physical_ms,
        logical: 0,
    }
}

fn entry(start_ts: crate::mvcc::Ts, value: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts: crate::mvcc::Ts::MAX,
        txn_id: 1,
        state: VersionState::Committed,
        data: VersionData::Inline(value.to_vec()),
        is_tombstone: false,
    }
}

fn source_from_keys(keys: &[&[u8]]) -> BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> {
    let mut source = BTreeMap::new();
    for key in keys {
        let mut chain = VecDeque::new();
        chain.push_back(entry(ts(10), key));
        source.insert((*key).to_vec(), Arc::new(chain));
    }
    source
}

#[test]
fn test_chain_snapshot_visible_range_in_order() {
    let source = source_from_keys(&[
        b"k09", b"k02", b"k07", b"k00", b"k05", b"k01", b"k08", b"k03", b"k06", b"k04",
    ]);
    let snap = ChainSnapshot::new(&source, None);
    let view = ReadView::new_frontier_pinned_for_tests(ts(20), 99);

    let keys = snap
        .visible_range(Bound::Unbounded, Bound::Unbounded, &view)
        .map(|(key, _)| key.to_vec())
        .collect::<Vec<_>>();

    assert_eq!(
        keys,
        vec![
            b"k00".to_vec(),
            b"k01".to_vec(),
            b"k02".to_vec(),
            b"k03".to_vec(),
            b"k04".to_vec(),
            b"k05".to_vec(),
            b"k06".to_vec(),
            b"k07".to_vec(),
            b"k08".to_vec(),
            b"k09".to_vec(),
        ]
    );
}

#[test]
fn test_chain_snapshot_visible_range_bounded() {
    let source = source_from_keys(&[b"a", b"b", b"c", b"d", b"e", b"f"]);
    let snap = ChainSnapshot::new(&source, None);
    let view = ReadView::new_frontier_pinned_for_tests(ts(20), 99);

    let included_start_excluded_end = snap
        .visible_range(
            Bound::Included(b"b".as_slice()),
            Bound::Excluded(b"e".as_slice()),
            &view,
        )
        .map(|(key, _)| key.to_vec())
        .collect::<Vec<_>>();
    assert_eq!(
        included_start_excluded_end,
        vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
    );

    let excluded_start_included_end = snap
        .visible_range(
            Bound::Excluded(b"b".as_slice()),
            Bound::Included(b"e".as_slice()),
            &view,
        )
        .map(|(key, _)| key.to_vec())
        .collect::<Vec<_>>();
    assert_eq!(
        excluded_start_included_end,
        vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]
    );
}

#[test]
fn test_chain_snapshot_refcount_preserved_after_btreemap_switch() {
    let alloc = AllocatorHandle::new(FileHeader::new(0, 0, 0));
    let mut source = BTreeMap::new();
    let mut chain = VecDeque::new();
    chain.push_back(VersionEntry {
        start_ts: ts(10),
        stop_ts: crate::mvcc::Ts::MAX,
        txn_id: 1,
        state: VersionState::Committed,
        data: VersionData::Overflow(OverflowRef::new_owned(32, 128, alloc.clone()).unwrap()),
        is_tombstone: false,
    });
    source.insert(b"overflow".to_vec(), Arc::new(chain));

    assert_eq!(alloc.overflow_refcount(32), 1);
    let snap = ChainSnapshot::new(&source, None);
    assert_eq!(alloc.overflow_refcount(32), 2);
    drop(snap);
    assert_eq!(alloc.overflow_refcount(32), 1);
}

#[test]
fn history_is_candidate_matches_us004_predicate() {
    let source = source_from_keys(&[b"older", b"newer"]);
    let snap = ChainSnapshot::new(&source, None);

    assert!(snap.history_is_candidate(b"missing", ts(20)));
    assert!(!snap.history_is_candidate(b"older", ts(20)));
    assert!(snap.history_is_candidate(b"newer", ts(5)));
}
