use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::btree::reconcile::{encode_folded_leaf, FoldedLeafCell, FoldedLeafLinks};
use crate::storage::page::PAGE_SIZE_LEAF;
use crate::storage::reconcile::plan::{TreeIdent, TreeKind};
use crate::storage::reconcile::synth::{synthesize_page, NotInstallable};

const COLLECTION_ID: i64 = 42;
const TXN_ID: u64 = 7;
const CHECKPOINT_TS: Ts = Ts {
    physical_ms: 30,
    logical: 0,
};
const ORT: Ts = Ts {
    physical_ms: 15,
    logical: 0,
};

fn ts(physical_ms: u64) -> Ts {
    Ts {
        physical_ms,
        logical: 0,
    }
}

fn primary_ident() -> TreeIdent {
    TreeIdent {
        collection_id: COLLECTION_ID,
        kind: TreeKind::Primary,
    }
}

fn inline_committed(start: u64, stop: u64, payload: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: ts(start),
        stop_ts: ts(stop),
        txn_id: TXN_ID,
        state: VersionState::Committed,
        data: VersionData::Inline(payload.to_vec()),
        is_tombstone: false,
    }
}

fn inline_committed_open(start: u64, payload: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: ts(start),
        stop_ts: Ts::MAX,
        txn_id: TXN_ID,
        state: VersionState::Committed,
        data: VersionData::Inline(payload.to_vec()),
        is_tombstone: false,
    }
}

fn tombstone_committed(start: u64, stop: u64) -> VersionEntry {
    VersionEntry {
        start_ts: ts(start),
        stop_ts: ts(stop),
        txn_id: TXN_ID,
        state: VersionState::Committed,
        data: VersionData::Inline(Vec::new()),
        is_tombstone: true,
    }
}

fn inline_pending(start: u64, payload: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: ts(start),
        stop_ts: Ts::MAX,
        txn_id: TXN_ID,
        state: VersionState::Pending { txn_id: TXN_ID },
        data: VersionData::Inline(payload.to_vec()),
        is_tombstone: false,
    }
}

fn chain(entries: impl IntoIterator<Item = VersionEntry>) -> Arc<VecDeque<VersionEntry>> {
    Arc::new(entries.into_iter().collect())
}

fn inline_payload(entry: &VersionEntry) -> &[u8] {
    match &entry.data {
        VersionData::Inline(bytes) => bytes,
        VersionData::Overflow(_) => panic!("expected inline test payload"),
    }
}

#[test]
fn synthesize_page_applies_full_timestamp_decision_rule() {
    let links = FoldedLeafLinks {
        next_leaf_page: 99,
        prev_leaf_page: 12,
    };
    let base = encode_folded_leaf(
        &[
            FoldedLeafCell::inline(b"delete".to_vec(), b"old-delete-base".to_vec()),
            FoldedLeafCell::inline(b"future-only".to_vec(), b"future-base".to_vec()),
            FoldedLeafCell::inline(b"untouched".to_vec(), b"untouched-base".to_vec()),
            FoldedLeafCell::inline(b"winner".to_vec(), b"old-winner-base".to_vec()),
        ],
        links,
    )
    .unwrap();

    let mut chains = BTreeMap::new();
    chains.insert(
        b"winner".to_vec(),
        chain([
            inline_pending(60, b"pending"),
            inline_committed_open(40, b"future"),
            inline_committed(20, 35, b"winner-at-checkpoint"),
            inline_committed(10, 25, b"reader-visible-aged"),
            inline_committed(5, 10, b"obsolete"),
        ]),
    );
    chains.insert(
        b"delete".to_vec(),
        chain([
            tombstone_committed(22, 35),
            inline_committed(8, 22, b"delete-reader-visible-aged"),
        ]),
    );
    chains.insert(
        b"future-only".to_vec(),
        chain([inline_committed_open(50, b"future-only-retained")]),
    );

    let synthesized = synthesize_page(&base, &chains, CHECKPOINT_TS, ORT, primary_ident()).unwrap();

    let expected_base = encode_folded_leaf(
        &[
            FoldedLeafCell::inline(b"future-only".to_vec(), b"future-base".to_vec()),
            FoldedLeafCell::inline(b"untouched".to_vec(), b"untouched-base".to_vec()),
            FoldedLeafCell::inline(b"winner".to_vec(), b"winner-at-checkpoint".to_vec()),
        ],
        links,
    )
    .unwrap();
    assert_eq!(synthesized.new_base, expected_base);

    let spilled: BTreeMap<_, _> = synthesized
        .history_spill
        .iter()
        .map(|entry| (entry.key.as_slice(), inline_payload(&entry.entry)))
        .collect();
    assert_eq!(
        spilled.get(b"winner".as_slice()).copied(),
        Some(b"reader-visible-aged".as_slice())
    );
    assert_eq!(
        spilled.get(b"delete".as_slice()).copied(),
        Some(b"delete-reader-visible-aged".as_slice())
    );
    assert_eq!(synthesized.history_spill.len(), 2);
    assert!(synthesized
        .history_spill
        .iter()
        .all(|entry| entry.ident == primary_ident()));
    assert!(synthesized
        .history_spill
        .iter()
        .all(|entry| entry.counter == 0));

    let winner_retained = synthesized
        .retained_chains
        .get(b"winner".as_slice())
        .unwrap();
    assert_eq!(winner_retained.len(), 2);
    assert_eq!(inline_payload(&winner_retained[0]), b"pending");
    assert_eq!(inline_payload(&winner_retained[1]), b"future");
    assert_eq!(
        synthesized.retained_chains[b"future-only".as_slice()].len(),
        1
    );
    assert!(!synthesized
        .retained_chains
        .contains_key(b"delete".as_slice()));
}

#[test]
fn synthesize_page_keeps_history_spill_counter_stable_across_retry_horizon_shift() {
    let base = encode_folded_leaf(&[], FoldedLeafLinks::default()).unwrap();
    let mut chains = BTreeMap::new();
    chains.insert(
        b"retry".to_vec(),
        chain([
            inline_committed_open(30, b"winner"),
            inline_committed(20, 24, b"newer-retry-obsolete"),
            inline_committed(10, 100, b"older-still-visible"),
        ]),
    );

    let first = synthesize_page(&base, &chains, ts(30), ts(15), primary_ident()).unwrap();
    let retry = synthesize_page(&base, &chains, ts(30), ts(19), primary_ident()).unwrap();

    let first_counter = first
        .history_spill
        .iter()
        .find(|entry| inline_payload(&entry.entry) == b"older-still-visible")
        .expect("first pass spills the still-visible version")
        .counter;
    let retry_counter = retry
        .history_spill
        .iter()
        .find(|entry| inline_payload(&entry.entry) == b"older-still-visible")
        .expect("retry still spills the still-visible version")
        .counter;

    assert_eq!(first_counter, retry_counter);
}

#[test]
fn synthesize_page_rejects_checkpoint_winners_over_leaf_budget() {
    let base = encode_folded_leaf(&[], FoldedLeafLinks::default()).unwrap();
    let mut chains = BTreeMap::new();
    chains.insert(
        b"large".to_vec(),
        chain([inline_committed_open(
            20,
            &vec![0xA5; PAGE_SIZE_LEAF as usize],
        )]),
    );

    let result = synthesize_page(&base, &chains, CHECKPOINT_TS, ORT, primary_ident());

    assert!(matches!(
        result,
        Err(NotInstallable::VisibleWinnerExceedsPageBudget)
    ));
}

#[test]
fn synthesize_page_rejects_retained_payload_over_leaf_budget() {
    let base = encode_folded_leaf(&[], FoldedLeafLinks::default()).unwrap();
    let mut chains = BTreeMap::new();
    chains.insert(
        b"retained-large".to_vec(),
        chain([
            inline_committed_open(40, &vec![0xB6; PAGE_SIZE_LEAF as usize]),
            inline_committed_open(20, b"winner"),
        ]),
    );

    let result = synthesize_page(&base, &chains, CHECKPOINT_TS, ORT, primary_ident());

    assert!(matches!(
        result,
        Err(NotInstallable::FoldedLeafExceedsPageByteBudget)
    ));
}
