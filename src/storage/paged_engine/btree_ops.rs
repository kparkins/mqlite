//! B+ tree doc-storage helpers (generic over S: BTreePageStore).

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::key_encoding::encode_key;
use crate::mvcc::read_view::ReadView;
use crate::query::eval_filter;
use crate::storage::btree::{BTree, BTreePageStore};
use crate::validation::validate_document;

use super::doc_helpers::{check_unique_constraints, ensure_id};

/// Insert `doc` into `tree`, auto-assigning `_id` if absent.
///
/// `unique_specs` are `(name, fields, sparse)` tuples for unique secondary
/// indexes; violated constraints return [`Error::DuplicateKey`] before the
/// tree is modified.
///
/// Returns `(id_bson, encoded_key, bson_bytes, tree_root_page)` so callers
/// can stage the MVCC primary-chain entry via `WriteTxn::stage_primary_insert`
/// after the on-disk cell lands. `tree_root_page` is sampled AFTER the insert
/// so any root split is reflected.
pub(super) fn btree_insert_doc<S: BTreePageStore>(
    tree: &mut BTree<S>,
    doc: &mut Document,
    unique_specs: &[(String, Vec<String>, bool)],
) -> Result<(Bson, Vec<u8>, Vec<u8>, u32)> {
    validate_document(doc)?;
    let id_bson = ensure_id(doc);
    // Check secondary unique constraints before touching the tree.
    check_unique_constraints(tree, unique_specs, doc)?;
    let key = encode_key(&id_bson);
    let bson_bytes = bson::to_vec(doc).map_err(Error::BsonSerialization)?;
    tree.insert(&key, &bson_bytes).map_err(|e| match e {
        Error::DuplicateKey { .. } => Error::DuplicateKey {
            detail: "document with _id already exists".to_string(),
        },
        other => other,
    })?;
    let tree_root = tree.root_page;
    Ok((id_bson, key, bson_bytes, tree_root))
}

/// MVCC-aware collection scan. For each key visible at `view.read_ts` (or
/// the on-disk cell when no chain entry is present), decode the value as
/// BSON and retain rows that satisfy `filter`. The optional `history`
/// probe (plan §T7) is consulted when neither the chain nor a newer
/// version is visible, so readers can still see entries evicted from
/// memory chains into the history store.
pub(super) fn btree_collscan<S: BTreePageStore>(
    tree: &BTree<S>,
    filter: &Document,
    view: &ReadView,
    history: Option<&dyn crate::storage::btree::HistoryProbe>,
) -> Result<Vec<(Vec<u8>, Document)>> {
    let pairs = tree.range_scan_mvcc(None, None, view, history)?;
    let mut result = Vec::with_capacity(pairs.len());
    for (key, bson_bytes) in pairs {
        let doc: Document = bson::from_slice(&bson_bytes).map_err(Error::BsonDeserialization)?;
        if eval_filter(&doc, filter)? {
            result.push((key, doc));
        }
    }
    Ok(result)
}
