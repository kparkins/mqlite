//! US-009 focused tests for the Phase 4 durable history-store key schema.

use super::*;
use crate::storage::btree::MemPageStore;
use crate::storage::reconcile::plan::{TreeIdent, TreeKind};

const COLLECTION_ID: i64 = 7;
const SECONDARY_INDEX_ID: i64 = 23;
const DUPLICATE_KEY_COUNTER: u32 = 3;

fn ts(ms: u64, logical: u32) -> Ts {
    Ts {
        physical_ms: ms,
        logical,
    }
}

fn primary_ident(collection_id: i64) -> TreeIdent {
    TreeIdent {
        collection_id,
        kind: TreeKind::Primary,
    }
}

fn secondary_ident(collection_id: i64, index_id: i64) -> TreeIdent {
    TreeIdent {
        collection_id,
        kind: TreeKind::Secondary { index_id },
    }
}

fn inline_entry(start: Ts, stop: Ts, payload: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: start,
        stop_ts: stop,
        txn_id: 99,
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

trait HistoryStoreTestSpillExt {
    fn spill_primary(
        &mut self,
        collection_id: i64,
        key_bytes: &[u8],
        entry: &VersionEntry,
        counter: u32,
    ) -> Result<()>;

    fn spill_sec_index(
        &mut self,
        collection_id: i64,
        index_id: i64,
        key_bytes: &[u8],
        entry: &VersionEntry,
        counter: u32,
    ) -> Result<()>;
}

impl HistoryStoreTestSpillExt for HistoryStore<MemPageStore> {
    fn spill_primary(
        &mut self,
        collection_id: i64,
        key_bytes: &[u8],
        entry: &VersionEntry,
        counter: u32,
    ) -> Result<()> {
        let mut txn = HistorySpillTxn::new();
        HistoryStore::<MemPageStore>::spill_primary(
            &mut txn,
            primary_ident(collection_id),
            key_bytes,
            entry,
            counter,
        )?;
        self.commit_spill_txn(txn)
    }

    fn spill_sec_index(
        &mut self,
        collection_id: i64,
        index_id: i64,
        key_bytes: &[u8],
        entry: &VersionEntry,
        counter: u32,
    ) -> Result<()> {
        let mut txn = HistorySpillTxn::new();
        HistoryStore::<MemPageStore>::spill_sec_index(
            &mut txn,
            secondary_ident(collection_id, index_id),
            key_bytes,
            entry,
            counter,
        )?;
        self.commit_spill_txn(txn)
    }
}

#[test]
fn us009_key_schema_is_length_delimited_and_big_endian() {
    let ident = secondary_ident(COLLECTION_ID, SECONDARY_INDEX_ID);
    let start_ts = ts(0x0102_0304_0506_0708, 0x090a_0b0c);
    let counter = 0x0d0e_0f10;
    let key_bytes = b"a\0bc";

    let encoded = encode_history_key(&ident, key_bytes, start_ts, counter);

    assert_eq!(encoded.len(), 8 + 1 + 8 + 4 + key_bytes.len() + 12 + 4);
    assert_eq!(&encoded[0..8], &COLLECTION_ID.to_be_bytes());
    assert_eq!(encoded[8], HISTORY_TREE_KIND_SECONDARY);
    assert_eq!(&encoded[9..17], &SECONDARY_INDEX_ID.to_be_bytes());
    assert_eq!(&encoded[17..21], &(key_bytes.len() as u32).to_be_bytes());
    assert_eq!(&encoded[21..25], key_bytes);
    assert_eq!(&encoded[25..37], &start_ts.to_be_bytes());
    assert_eq!(&encoded[37..41], &counter.to_be_bytes());

    let (decoded_ident, decoded_key, decoded_start_ts, decoded_counter) =
        decode_history_key(&encoded).expect("valid US-009 key");
    assert_eq!(decoded_ident, ident);
    assert_eq!(decoded_key, key_bytes);
    assert_eq!(decoded_start_ts, start_ts);
    assert_eq!(decoded_counter, counter);
}

#[test]
fn us009_probe_uses_length_delimited_prefix_and_full_visibility_window() {
    let mut history = HistoryStore::create(MemPageStore::new()).unwrap();
    let ident = primary_ident(COLLECTION_ID);

    history
        .spill_primary(
            ident.collection_id,
            b"a",
            &inline_entry(ts(10, 0), ts(15, 0), b"expired"),
            0,
        )
        .unwrap();
    history
        .spill_primary(
            ident.collection_id,
            b"a\0tail",
            &inline_entry(ts(12, 0), ts(30, 0), b"prefix-neighbor"),
            0,
        )
        .unwrap();

    assert!(
        history
            .probe_primary(ident.collection_id, b"a", ts(17, 0))
            .unwrap()
            .is_none(),
        "newest start_ts <= read_ts is not visible once read_ts reaches stop_ts"
    );

    let neighbor = history
        .probe_primary(ident.collection_id, b"a\0tail", ts(17, 0))
        .unwrap()
        .expect("length-delimited neighbor remains independently visible");
    assert_eq!(inline_payload(neighbor), b"prefix-neighbor");
}

#[test]
fn us009_probe_upper_bound_includes_all_counters_at_read_ts() {
    let mut history = HistoryStore::create(MemPageStore::new()).unwrap();
    let ident = secondary_ident(COLLECTION_ID, SECONDARY_INDEX_ID);

    history
        .spill_sec_index(
            ident.collection_id,
            SECONDARY_INDEX_ID,
            b"k",
            &inline_entry(ts(20, 0), ts(40, 0), b"counter-0"),
            0,
        )
        .unwrap();
    history
        .spill_sec_index(
            ident.collection_id,
            SECONDARY_INDEX_ID,
            b"k",
            &inline_entry(ts(20, 0), ts(40, 0), b"counter-1"),
            1,
        )
        .unwrap();

    let visible = history
        .probe_sec_index(ident.collection_id, SECONDARY_INDEX_ID, b"k", ts(20, 0))
        .unwrap()
        .expect("read_ts upper bound must include counter u32::MAX records");
    assert_eq!(inline_payload(visible), b"counter-1");
}

#[test]
fn us009_duplicate_spill_is_idempotent_only_for_exact_value_copy() {
    let mut history = HistoryStore::create(MemPageStore::new()).unwrap();
    let ident = primary_ident(COLLECTION_ID);
    let first = inline_entry(ts(30, 0), ts(50, 0), b"stable");
    let divergent = inline_entry(ts(30, 0), ts(50, 0), b"changed");

    history
        .spill_primary(ident.collection_id, b"dup", &first, DUPLICATE_KEY_COUNTER)
        .unwrap();
    history
        .spill_primary(ident.collection_id, b"dup", &first, DUPLICATE_KEY_COUNTER)
        .expect("byte-identical duplicate spill is an idempotent no-op");

    let err = history
        .spill_primary(
            ident.collection_id,
            b"dup",
            &divergent,
            DUPLICATE_KEY_COUNTER,
        )
        .expect_err("same durable key with different bytes must fail");
    assert!(matches!(err, Error::DuplicateKey { .. }));
}
