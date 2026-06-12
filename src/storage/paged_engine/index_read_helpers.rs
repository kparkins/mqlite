//! Read-side secondary-index helpers (extracted from index_maint.rs).
//!
//! These free functions support snapshot reads: decoding the `_id` stored in
//! an index entry and building the B+ tree scan bounds for an index condition.

use std::sync::Arc;

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::keys::{encode_compound_key, COMPOUND_SEP};
use crate::query::planner::IndexCondition;
use crate::storage::btree::{BTree, CellValue};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::handle::BufferPoolHandle;

/// Retrieve the serialised `_id` value stored in an index tree entry.
///
/// Returns `Ok(Some(id))` for a well-formed entry (including entries whose
/// stored `_id` is `Bson::Null`, which is a valid MongoDB document identity).
/// Returns `Ok(None)` when the payload is empty or the `_id` key is absent —
/// both indicate a corrupt or missing index entry that the caller must treat
/// as an error rather than silently skipping.
pub(super) fn index_entry_id_free(
    handle: &Arc<BufferPoolHandle>,
    cv: CellValue,
) -> Result<Option<Bson>> {
    let bytes = match cv {
        CellValue::Inline(b) => b,
        CellValue::Overflow {
            first_page,
            total_length,
        } => {
            let tmp_store = BufferPoolPageStore::new(Arc::clone(handle));
            let tmp_tree = BTree::open(tmp_store, 1, 0);
            tmp_tree.read_overflow(first_page, total_length)?
        }
    };
    if bytes.is_empty() {
        return Ok(None);
    }
    let doc: Document = bson::from_slice(&bytes).map_err(Error::BsonDeserialization)?;
    match doc.get("_id") {
        Some(id) => Ok(Some(id.clone())),
        None => Ok(None),
    }
}

/// Build the [start, end] range for a secondary index B+ tree scan.
pub(super) fn index_bounds_free(
    condition: &IndexCondition,
    ascending: bool,
) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    fn prefix(val: &Bson, asc: bool) -> Vec<u8> {
        let mut p = encode_compound_key(&[(val, asc)]);
        p.push(COMPOUND_SEP);
        p
    }
    fn prefix_next(val: &Bson, asc: bool) -> Vec<u8> {
        let mut p = prefix(val, asc);
        if let Some(last) = p.last_mut() {
            *last += 1;
        }
        p
    }
    match condition {
        IndexCondition::Eq(v) => (Some(prefix(v, ascending)), Some(prefix_next(v, ascending))),
        IndexCondition::Any => (None, None),
        IndexCondition::In(_) => (None, None),
        IndexCondition::Range { gt, gte, lt, lte } => {
            if ascending {
                let start = match (gte.as_ref(), gt.as_ref()) {
                    (Some(v), _) => Some(prefix(v, true)),
                    (None, Some(v)) => Some(prefix_next(v, true)),
                    _ => None,
                };
                let end = match (lte.as_ref(), lt.as_ref()) {
                    (Some(v), _) => Some(prefix_next(v, true)),
                    (None, Some(v)) => Some(prefix(v, true)),
                    _ => None,
                };
                (start, end)
            } else {
                let start = match (lte.as_ref(), lt.as_ref()) {
                    (Some(v), _) => Some(prefix(v, false)),
                    (None, Some(v)) => Some(prefix_next(v, false)),
                    _ => None,
                };
                let end = match (gte.as_ref(), gt.as_ref()) {
                    (Some(v), _) => Some(prefix_next(v, false)),
                    (None, Some(v)) => Some(prefix(v, false)),
                    _ => None,
                };
                (start, end)
            }
        }
    }
}
