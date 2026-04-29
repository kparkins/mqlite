use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::{VersionData, VersionEntry, VersionState};
use crate::storage::page::{LEAF_FLAG_HAS_OVERFLOW, LEAF_HEADER_SIZE, PAGE_SIZE_LEAF};

use super::node::LeafNode;
use super::reconcile::{
    encode_folded_leaf, predict_encoded_leaf_size, FoldedLeafCell, FoldedLeafLinks,
};
use super::CellValue;

const TXN_ID: u64 = 7;
const TS_10: Ts = Ts {
    physical_ms: 10,
    logical: 0,
};
const TS_20: Ts = Ts {
    physical_ms: 20,
    logical: 0,
};
const TS_30: Ts = Ts {
    physical_ms: 30,
    logical: 0,
};

fn inline_entry(start_ts: Ts, stop_ts: Ts, bytes: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts,
        txn_id: TXN_ID,
        state: VersionState::Committed,
        data: VersionData::Inline(bytes.to_vec()),
        is_tombstone: false,
    }
}

#[test]
fn predict_encoded_leaf_size_counts_leaf_and_retained_chain_bytes() {
    let winners = vec![
        FoldedLeafCell::inline(b"k1".to_vec(), b"value".to_vec()),
        FoldedLeafCell::overflow(b"k2".to_vec(), 42, 128),
    ];
    let mut retained_chains = BTreeMap::new();
    retained_chains.insert(
        b"k3".to_vec(),
        Arc::new(VecDeque::from([
            inline_entry(TS_10, TS_20, b"older"),
            inline_entry(TS_20, TS_30, b"newer"),
        ])),
    );

    let inline_cell = 2 + b"k1".len() + 1 + 4 + b"value".len();
    let overflow_cell = 2 + b"k2".len() + 1 + 8;
    let retained_chain_key = 2 + b"k3".len();
    let retained_chain_entry_count = 4;
    let retained_entry_fixed = 12 + 12 + 8 + 1 + 1 + 1;
    let retained_inline_payload = 4 + b"older".len() + 4 + b"newer".len();

    assert_eq!(
        predict_encoded_leaf_size(&winners, &retained_chains),
        LEAF_HEADER_SIZE
            + 2 * winners.len()
            + inline_cell
            + overflow_cell
            + retained_chain_key
            + retained_chain_entry_count
            + 2 * retained_entry_fixed
            + retained_inline_payload
    );
}

#[test]
fn predict_encoded_leaf_size_matches_encoded_leaf_without_retained_chains() {
    let winners = vec![
        FoldedLeafCell::inline(b"a".to_vec(), b"one".to_vec()),
        FoldedLeafCell::inline(b"b".to_vec(), b"two".to_vec()),
    ];
    let encoded = encode_folded_leaf(&winners, FoldedLeafLinks::default()).unwrap();
    let parsed = LeafNode::parse(&encoded).unwrap();

    assert_eq!(
        predict_encoded_leaf_size(&winners, &BTreeMap::new()),
        parsed.used_bytes()
    );
}

#[test]
fn encode_folded_leaf_round_trips_cells_and_sibling_links() {
    let winners = vec![
        FoldedLeafCell::inline(b"a".to_vec(), b"one".to_vec()),
        FoldedLeafCell::overflow(b"b".to_vec(), 91, 4096),
    ];
    let links = FoldedLeafLinks {
        next_leaf_page: 12,
        prev_leaf_page: 8,
    };

    let encoded = encode_folded_leaf(&winners, links).unwrap();
    let parsed = LeafNode::parse(&encoded).unwrap();

    assert_eq!(encoded.len(), PAGE_SIZE_LEAF as usize);
    assert_eq!(
        parsed.flags & LEAF_FLAG_HAS_OVERFLOW,
        LEAF_FLAG_HAS_OVERFLOW
    );
    assert_eq!(parsed.next_leaf_page, links.next_leaf_page);
    assert_eq!(parsed.prev_leaf_page, links.prev_leaf_page);
    assert_eq!(parsed.cells.len(), 2);
    assert_eq!(parsed.cells[0].key, b"a");
    assert_eq!(parsed.cells[1].key, b"b");
    assert!(matches!(parsed.cells[0].value, CellValue::Inline(_)));
    assert!(matches!(parsed.cells[1].value, CellValue::Overflow { .. }));
}
