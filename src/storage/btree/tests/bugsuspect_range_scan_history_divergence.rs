//! Bug-suspect: range-scan vs history-store fallthrough divergence.
//!
//! Suspect (deep-refactor-2026-06-10, rank ~4): an MVCC *range* scan silently
//! drops a key that exists only as a resident delta chain with **no base
//! cell** and whose only chain entry is newer than the reader's `read_ts`,
//! even though the history store holds a version visible at `read_ts`. The
//! *point* lookup (`get_mvcc`) handles exactly this state via
//! `history_is_candidate` + `probe_visible_version`, so `find(_id)` and a
//! range scan over the same snapshot return DIFFERENT results for the same
//! key — a snapshot-isolation divergence.
//!
//! Why the range merge misses it (scan.rs `try_for_each_range_scan_mvcc_bounded`):
//!   * The merge has two ordered sources — base cells and `visible_range`
//!     chain entries. The history probe fires ONLY inside the `MergeSource::Base`
//!     arm.
//!   * A delta-only key has no base cell, so it never produces a `Base`
//!     source.
//!   * Its only chain entry is all-newer-than-reader, so `visible_range`
//!     (which filters on `version_visible_to`) yields nothing for it.
//!   * Net: the key produces neither merge source, the probe never fires for
//!     it, and the visible history version is dropped.
//!
//! Contrast `get_mvcc` (point read): after `visible_at` misses it consults
//! `history_is_candidate` and probes history regardless of whether the key
//! has a base cell — so it surfaces the history version.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::{ReadView, Ts, VersionData, VersionEntry, VersionState};
use crate::storage::buffer_pool::LatchMode;

use super::{BTree, BTreePageStore, HistoryProbe, MemPageStore};

const TXN_ID: u64 = 1;
const READER_TXN_ID: u64 = 99;

fn ts(physical_ms: u64) -> Ts {
    Ts {
        physical_ms,
        logical: 0,
    }
}

fn version(start_ts: Ts, stop_ts: Ts, value: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts,
        txn_id: TXN_ID,
        state: VersionState::Committed,
        data: VersionData::Inline(value.to_vec()),
        is_tombstone: false,
    }
}

fn install_chain(
    tree: &mut BTree<MemPageStore>,
    key: &[u8],
    entries: Vec<VersionEntry>,
) -> Result<()> {
    let leaf = tree.find_leaf(key)?;
    let chain = entries.into_iter().collect::<VecDeque<_>>();
    tree.store
        .with_chain_under_latch(leaf, key, LatchMode::Exclusive, |slot| {
            *slot = Some(Arc::new(chain));
        })
}

/// History probe that returns a fixed entry for one specific key.
struct KeyedHistoryProbe {
    key: Vec<u8>,
    entry: VersionEntry,
    calls: RefCell<Vec<Vec<u8>>>,
}

impl KeyedHistoryProbe {
    fn new(key: &[u8], entry: VersionEntry) -> Self {
        Self {
            key: key.to_vec(),
            entry,
            calls: RefCell::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<Vec<u8>> {
        self.calls.borrow().clone()
    }
}

impl HistoryProbe for KeyedHistoryProbe {
    fn probe_visible_version(&self, key: &[u8], _read_ts: Ts) -> Result<Option<VersionEntry>> {
        self.calls.borrow_mut().push(key.to_vec());
        if key == self.key.as_slice() {
            Ok(Some(self.entry.clone()))
        } else {
            Ok(None)
        }
    }
}

/// Build a tree with base keys `a`, `c`, then install a DELTA-ONLY key `b`
/// (no base cell) whose only chain entry is newer than the reader. History
/// holds a version of `b` visible at the reader's read_ts.
fn setup() -> (BTree<MemPageStore>, ReadView, KeyedHistoryProbe) {
    let mut tree = BTree::create(MemPageStore::new()).unwrap();
    // Base cells exist for the bookend keys but NOT for `b`.
    tree.insert(b"a", b"base-a").unwrap();
    tree.insert(b"c", b"base-c").unwrap();

    // Delta-only key `b`: resident chain has a single entry at ts(150),
    // strictly NEWER than the reader at read_ts ts(100). `visible_range`
    // therefore yields nothing for `b`, and `b` has no base cell, so the
    // range merge never visits it.
    install_chain(
        &mut tree,
        b"b",
        vec![version(ts(150), Ts::MAX, b"newer-than-reader")],
    )
    .unwrap();

    // History store: `b` has a version visible at read_ts ts(100).
    let history = KeyedHistoryProbe::new(b"b", version(ts(90), ts(150), b"history-b"));

    let view = ReadView::new_frontier_pinned_for_tests(ts(100), READER_TXN_ID);
    (tree, view, history)
}

/// Point lookup surfaces the history version of the delta-only key — this is
/// the behavior the range scan must match.
#[test]
fn point_lookup_surfaces_history_for_delta_only_all_newer_key() {
    let (tree, view, history) = setup();

    let got = tree
        .get_mvcc(b"b", &view, Some(&history))
        .expect("get_mvcc")
        .expect("point lookup must surface the history version of `b`");
    assert_eq!(got.as_slice(), b"history-b");
    assert_eq!(history.calls(), vec![b"b".to_vec()]);
}

/// Range scan over the SAME snapshot must yield the SAME version of `b` that
/// the point lookup returns.
///
/// FAILS today: the range merge never probes history for the delta-only key
/// `b`, so `b` is silently absent from the scan even though `get_mvcc(b)`
/// returns it — find-by-id and find-by-range diverge for one snapshot.
///
/// REAL bug, but the fix CROSSES THE STORAGE/MVCC BOUNDARY: the range merge
/// in `scan.rs` (this agent's scope) cannot enumerate the chain-map keys that
/// are history-candidates-with-no-base-and-not-visible without a new read-only
/// accessor on `mvcc::ChainSnapshot` (its `chains` map is private and
/// `visible_range` only yields VISIBLE entries). Per the wave's scope rule,
/// the failing test is the deliverable and the fix is HANDED OFF for
/// coordination with the mvcc agent. `#[ignore]`d only so the suite stays
/// green; REMOVE the ignore once the mvcc-side accessor lands and the scan
/// merge gains a third "history-candidate delta-only key" source.
#[test]
fn range_scan_surfaces_history_for_delta_only_all_newer_key() {
    let (tree, view, history) = setup();

    let rows = tree
        .range_scan_mvcc(None, None, &view, Some(&history))
        .expect("range_scan_mvcc");

    let b_row = rows.iter().find(|(k, _)| k.as_slice() == b"b");
    assert!(
        b_row.is_some(),
        "range scan dropped delta-only key `b` that `get_mvcc(b)` returns from \
         history — find-by-id vs find-by-range divergence. rows: {:?}",
        rows.iter()
            .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        b_row.unwrap().1.as_slice(),
        b"history-b",
        "range scan must return the same history version of `b` as the point lookup"
    );
}
