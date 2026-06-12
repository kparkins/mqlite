//! B+ tree doc-storage helpers (generic over S: BTreePageStore).

use std::ops::Bound;

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::keys::encode_key;
use crate::mvcc::read_view::ReadView;
use crate::mvcc::transaction::PrimaryWrite;
use crate::query::eval_filter;
use crate::storage::btree::{BTree, BTreePageStore, HistoryProbe};
use crate::validation::validate_document;

use super::doc_helpers::{check_unique_constraints_mvcc, ensure_id};
use super::visibility::WriteVisibility;

/// Validate and prepare `doc` for a logical primary insert.
///
/// `unique_specs` are `(name, fields, sparse)` tuples for unique secondary
/// indexes; violated constraints return [`Error::DuplicateKey`] before the
/// transaction is modified.
///
/// Returns `(id_bson, encoded_key, bson_bytes)` so callers can stage the MVCC
/// primary-chain entry without writing row bytes through a structural B-tree
/// batch.
pub(super) fn prepare_insert_document<S: BTreePageStore>(
    tree: &BTree<S>,
    doc: &mut Document,
    unique_specs: &[(String, Vec<String>, bool)],
    vis: &WriteVisibility<'_>,
    pending: &[PrimaryWrite],
    ns: &str,
) -> Result<(Bson, Vec<u8>, Vec<u8>)> {
    validate_document(doc)?;
    let id_bson = ensure_id(doc);
    // Check declared unique constraints before touching the tree.
    let history = Some(&vis.primary_history as &dyn HistoryProbe);
    check_unique_constraints_mvcc(
        tree,
        unique_specs,
        doc,
        vis.read_view.as_ref(),
        history,
        pending,
        ns,
    )?;
    let key = encode_key(&id_bson);
    if tree
        .get_mvcc(&key, vis.read_view.as_ref(), history)?
        .is_some()
    {
        return Err(Error::DuplicateKey {
            detail: "document with _id already exists".to_string(),
        });
    }
    let bson_bytes = bson::to_vec(doc).map_err(Error::BsonSerialization)?;
    Ok((id_bson, key, bson_bytes))
}

/// MVCC-aware collection scan. For each key visible at `view.read_ts` (or
/// the on-disk cell when no chain entry is present), decode the value as
/// BSON and retain rows that satisfy `filter`. The optional `history`
/// probe is consulted when neither the chain nor a newer version is visible,
/// so readers can still see entries evicted from memory chains into the
/// history store.
pub(super) fn btree_collscan<S: BTreePageStore>(
    tree: &BTree<S>,
    filter: &Document,
    view: &ReadView,
    history: Option<&dyn HistoryProbe>,
    match_limit: Option<usize>,
) -> Result<Vec<(Vec<u8>, Document)>> {
    let mut result = Vec::new();
    tree.try_for_each_range_scan_mvcc_bounded(
        Bound::Unbounded,
        Bound::Unbounded,
        view,
        history,
        |key, bson_bytes| {
            let doc: Document =
                bson::from_slice(&bson_bytes).map_err(Error::BsonDeserialization)?;
            if eval_filter(&doc, filter)? {
                result.push((key, doc));
                if match_limit.is_some_and(|limit| result.len() >= limit) {
                    return Ok(false);
                }
            }
            Ok(true)
        },
    )?;
    Ok(result)
}
