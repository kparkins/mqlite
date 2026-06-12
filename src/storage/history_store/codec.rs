//! History-store key/value codecs.
//!
//! Owns the on-disk encoding for history-store B-tree keys and the
//! `VersionEntry` value layout, plus the shared header decode used by both the
//! full `VersionEntry` deserializer and the GC sweep's metadata-only reparse.
//!
//! ## Key schema (Phase 4 — Format Lock)
//!
//! ```text
//! key = collection_id(i64 BE)
//!     | tree_kind(u8)
//!     | index_id(i64 BE)
//!     | key_len(u32 BE)
//!     | key_bytes
//!     | start_ts(Ts BE 12B)
//!     | counter(u32 BE)
//! ```
//!
//! ## Value layout
//!
//! ```text
//! value = start_ts(12 LE)
//!       | stop_ts(12 LE)
//!       | txn_id(8 LE)
//!       | is_tombstone(1 B)
//!       | data_kind(1 B)   // 0 = Inline, 1 = Overflow
//!       | payload…
//! ```
//!
//! Inline payload: `len: u32 LE` || bytes.
//! Overflow payload: `first_page: u32 LE` || `total_length: u64 LE`.
//! Overflow rehydration requires a caller-supplied allocator handle.

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::{OverflowRef, VersionData, VersionEntry, VersionState};
use crate::storage::allocator::AllocatorHandle;
use crate::storage::reconcile::driver::{TreeIdent, TreeKind};

/// History key tree-kind tag for primary collection data.
pub(crate) const HISTORY_TREE_KIND_PRIMARY: u8 = 0x00;

/// History key tree-kind tag for secondary index data.
pub(crate) const HISTORY_TREE_KIND_SECONDARY: u8 = 0x01;

pub(super) const HISTORY_PRIMARY_INDEX_ID: i64 = 0;
pub(super) const HISTORY_KEY_FIXED_PREFIX_LEN: usize = 8 + 1 + 8 + 4;
pub(super) const HISTORY_KEY_TS_LEN: usize = 12;
pub(super) const HISTORY_KEY_COUNTER_LEN: usize = 4;

const DATA_KIND_INLINE: u8 = 0;
pub(super) const DATA_KIND_OVERFLOW: u8 = 1;

/// Length of the fixed value header (start_ts | stop_ts | txn_id |
/// is_tombstone | data_kind).
const VALUE_HEADER_LEN: usize = 12 + 12 + 8 + 1 + 1;

// ---------------------------------------------------------------------------
// Little-endian slice helpers
// ---------------------------------------------------------------------------

pub(super) fn ts_from_le_slice(bytes: &[u8]) -> Ts {
    let mut out = [0u8; 12];
    out.copy_from_slice(bytes);
    Ts::from_le_bytes(out)
}

fn u32_from_le_slice(bytes: &[u8]) -> u32 {
    let mut out = [0u8; 4];
    out.copy_from_slice(bytes);
    u32::from_le_bytes(out)
}

fn u64_from_le_slice(bytes: &[u8]) -> u64 {
    let mut out = [0u8; 8];
    out.copy_from_slice(bytes);
    u64::from_le_bytes(out)
}

// ---------------------------------------------------------------------------
// Key encoding / decoding
// ---------------------------------------------------------------------------

/// Encode a history-store key per the Phase 4 schema.
///
/// Layout:
/// `(collection_id BE 8)(tree_kind 1)(index_id BE 8)(key_len BE 4)`
/// `(key_bytes)(start_ts BE 12)(counter BE 4)`.
pub(crate) fn encode_history_key(
    ident: &TreeIdent,
    key_bytes: &[u8],
    start_ts: Ts,
    counter: u32,
) -> Vec<u8> {
    let (tree_kind, index_id) = history_tree_parts(ident);
    let mut out = Vec::with_capacity(
        HISTORY_KEY_FIXED_PREFIX_LEN
            + key_bytes.len()
            + HISTORY_KEY_TS_LEN
            + HISTORY_KEY_COUNTER_LEN,
    );
    out.extend_from_slice(&ident.collection_id.to_be_bytes());
    out.push(tree_kind);
    out.extend_from_slice(&index_id.to_be_bytes());
    out.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(key_bytes);
    out.extend_from_slice(&start_ts.to_be_bytes());
    out.extend_from_slice(&counter.to_be_bytes());
    out
}

/// Inverse of [`encode_history_key`]. Returns `None` when `bytes` is too
/// short to carry a valid header/footer.
#[cfg(test)]
pub(crate) fn decode_history_key(bytes: &[u8]) -> Option<(TreeIdent, &[u8], Ts, u32)> {
    if bytes.len() < HISTORY_KEY_FIXED_PREFIX_LEN + HISTORY_KEY_TS_LEN + HISTORY_KEY_COUNTER_LEN {
        return None;
    }
    let collection_id = i64::from_be_bytes(bytes[0..8].try_into().ok()?);
    let tree_kind = bytes[8];
    let index_id = i64::from_be_bytes(bytes[9..17].try_into().ok()?);
    let key_len = u32::from_be_bytes(bytes[17..21].try_into().ok()?) as usize;
    let key_start = HISTORY_KEY_FIXED_PREFIX_LEN;
    let key_end = key_start.checked_add(key_len)?;
    let ts_end = key_end.checked_add(HISTORY_KEY_TS_LEN)?;
    let counter_end = ts_end.checked_add(HISTORY_KEY_COUNTER_LEN)?;
    if bytes.len() != counter_end {
        return None;
    }
    let ident = history_ident_from_parts(collection_id, tree_kind, index_id)?;
    let key_bytes = &bytes[key_start..key_end];
    let mut ts_buf = [0u8; 12];
    ts_buf.copy_from_slice(&bytes[key_end..ts_end]);
    let start_ts = Ts::from_be_bytes(ts_buf);
    let counter = u32::from_be_bytes(bytes[ts_end..counter_end].try_into().ok()?);
    Some((ident, key_bytes, start_ts, counter))
}

pub(super) fn history_tree_parts(ident: &TreeIdent) -> (u8, i64) {
    match ident.kind {
        TreeKind::Primary => (HISTORY_TREE_KIND_PRIMARY, HISTORY_PRIMARY_INDEX_ID),
        TreeKind::Secondary { index_id } => (HISTORY_TREE_KIND_SECONDARY, index_id),
    }
}

#[cfg(test)]
fn history_ident_from_parts(collection_id: i64, tree_kind: u8, index_id: i64) -> Option<TreeIdent> {
    let kind = match tree_kind {
        HISTORY_TREE_KIND_PRIMARY if index_id == HISTORY_PRIMARY_INDEX_ID => TreeKind::Primary,
        HISTORY_TREE_KIND_SECONDARY => TreeKind::Secondary { index_id },
        _ => return None,
    };
    Some(TreeIdent {
        collection_id,
        kind,
    })
}

/// Build the prefix that every entry for `(TreeIdent, key_bytes)` shares.
pub(super) fn probe_prefix(ident: &TreeIdent, key_bytes: &[u8]) -> Vec<u8> {
    let (tree_kind, index_id) = history_tree_parts(ident);
    let mut out = Vec::with_capacity(HISTORY_KEY_FIXED_PREFIX_LEN + key_bytes.len());
    out.extend_from_slice(&ident.collection_id.to_be_bytes());
    out.push(tree_kind);
    out.extend_from_slice(&index_id.to_be_bytes());
    out.extend_from_slice(&(key_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(key_bytes);
    out
}

// ---------------------------------------------------------------------------
// VersionEntry value serialization
// ---------------------------------------------------------------------------

/// Parsed fixed header of a history-store value (everything before the
/// data-kind payload).
///
/// Decoded once and shared by both [`decode_version_entry_value`] (which then
/// continues into the payload) and [`HistoryStore::gc_pass`](super::HistoryStore::gc_pass)
/// (which only needs `stop_ts` and `data_kind`). Centralizing the offsets here
/// guarantees the two readers can never drift apart.
pub(super) struct ValueHeader {
    pub start_ts: Ts,
    pub stop_ts: Ts,
    pub txn_id: u64,
    pub is_tombstone: bool,
    pub data_kind: u8,
}

/// Decode the fixed value header (offsets 0..34): `start_ts(12) | stop_ts(12)
/// | txn_id(8) | is_tombstone(1) | data_kind(1)`.
///
/// # Errors
///
/// Returns [`Error::Internal`] when `bytes` is shorter than the fixed header.
pub(super) fn decode_value_header(bytes: &[u8]) -> Result<ValueHeader> {
    if bytes.len() < VALUE_HEADER_LEN {
        return Err(Error::Internal(
            "history_store: value buffer truncated before fixed header".into(),
        ));
    }
    Ok(ValueHeader {
        start_ts: ts_from_le_slice(&bytes[0..12]),
        stop_ts: ts_from_le_slice(&bytes[12..24]),
        txn_id: u64_from_le_slice(&bytes[24..32]),
        is_tombstone: bytes[32] != 0,
        data_kind: bytes[33],
    })
}

/// Decode the overflow payload `(first_page, total_length)` that follows an
/// `Overflow` value header (offsets 34..46).
///
/// # Errors
///
/// Returns [`Error::Internal`] when `bytes` is too short to carry the payload.
pub(super) fn decode_overflow_payload(bytes: &[u8]) -> Result<(u32, u64)> {
    if bytes.len() < VALUE_HEADER_LEN + 4 + 8 {
        return Err(Error::Internal(
            "history_store: overflow value truncated".into(),
        ));
    }
    let first_page = u32_from_le_slice(&bytes[34..38]);
    let total_length = u64_from_le_slice(&bytes[38..46]);
    Ok((first_page, total_length))
}

/// Serialize a `VersionEntry` to the history-store value layout.
pub(crate) fn encode_version_entry_value(entry: &VersionEntry) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + 12 + 8 + 1 + 1 + 16);
    out.extend_from_slice(&entry.start_ts.to_le_bytes());
    out.extend_from_slice(&entry.stop_ts.to_le_bytes());
    out.extend_from_slice(&entry.txn_id.to_le_bytes());
    out.push(u8::from(entry.is_tombstone));
    match &entry.data {
        VersionData::Inline(bytes) => {
            out.push(DATA_KIND_INLINE);
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        VersionData::Overflow(oref) => {
            out.push(DATA_KIND_OVERFLOW);
            out.extend_from_slice(&oref.first_page().to_le_bytes());
            out.extend_from_slice(&oref.total_length().to_le_bytes());
        }
    }
    out
}

/// Deserialize a `VersionEntry` from the history-store value layout.
///
/// Overflow entries require an `allocator` so `OverflowRef::new_owned` can
/// bump the refcount. Passing `None` rehydrates overflow payloads as an
/// error — callers on a pure probe path that only need metadata (or that
/// reject overflow entries in tests) can opt out of the allocator bump.
pub(crate) fn decode_version_entry_value(
    bytes: &[u8],
    allocator: Option<&AllocatorHandle>,
) -> Result<VersionEntry> {
    let header = decode_value_header(bytes)?;
    let data = match header.data_kind {
        DATA_KIND_INLINE => {
            if bytes.len() < 34 + 4 {
                return Err(Error::Internal(
                    "history_store: inline value missing length prefix".into(),
                ));
            }
            let len = u32_from_le_slice(&bytes[34..38]) as usize;
            let start = 38usize;
            let end = start
                .checked_add(len)
                .ok_or_else(|| Error::Internal("history_store: inline length overflow".into()))?;
            if bytes.len() < end {
                return Err(Error::Internal(
                    "history_store: inline payload truncated".into(),
                ));
            }
            VersionData::Inline(bytes[start..end].to_vec())
        }
        DATA_KIND_OVERFLOW => {
            let (first_page, total_length) = decode_overflow_payload(bytes)?;
            let alloc = allocator.ok_or_else(|| {
                Error::Internal("history_store: overflow entry requires allocator handle".into())
            })?;
            VersionData::Overflow(OverflowRef::new_owned(
                first_page,
                total_length,
                alloc.clone(),
            )?)
        }
        other => {
            return Err(Error::Internal(format!(
                "history_store: unknown data_kind {other}"
            )));
        }
    };
    Ok(VersionEntry {
        start_ts: header.start_ts,
        stop_ts: header.stop_ts,
        txn_id: header.txn_id,
        state: VersionState::Committed,
        data,
        is_tombstone: header.is_tombstone,
    })
}
