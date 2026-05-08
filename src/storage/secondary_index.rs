//! Secondary index implementation: single-field, compound, and multikey indexes.
//!
//! ## Architecture
//!
//! Each secondary index is stored as a separate B+ tree where:
//!
//! - **Key**: `encode_compound_key(field_values... | _id)` — the secondary field
//!   values followed by the document's `_id`, all encoded as a memcmp-sortable
//!   compound key.  Including `_id` guarantees uniqueness within the tree even
//!   for non-unique indexes.
//! - **Value**: BSON-serialized `{"_id": doc_id}` — enables the index scan to
//!   retrieve the document's `_id` without parsing the compound key.
//!
//! ## Key formats
//!
//! | Index type | Key |
//! |------------|-----|
//! | Single-field ASC | `encode(field) \| 0x01 \| encode(_id)` |
//! | Single-field DESC | `~encode(field) \| 0x01 \| encode(_id)` |
//! | Compound `{a:1, b:-1}` | `encode(a) \| 0x01 \| ~encode(b) \| 0x01 \| encode(_id)` |
//!
//! (Descending fields have their bytes bitwise-inverted by [`encode_compound_key`].)
//!
//! ## Multikey indexes
//!
//! When any indexed field contains an array, one index entry is generated per
//! array element.  The `multikey` flag in [`IndexEntry`] is set on the first
//! encounter and never cleared.  Duplicate array elements (e.g. `["a", "a"]`)
//! produce identical keys; the second insert is silently skipped.
//!
//! Compound indexes spanning two array fields ("parallel arrays") are rejected
//! with [`Error::Internal`].
//!
//! ## Unique constraint enforcement
//!
//! For unique indexes, before any insert we perform a range scan over
//! `[secondary_prefix, secondary_prefix_end)` where `secondary_prefix` is the
//! encoded field values followed by the compound separator (`0x01`).  Any
//! existing entry with a different `_id` constitutes a violation and returns
//! [`Error::DuplicateKey`] (code 11000).

use std::collections::HashSet;
use std::ops::Bound;

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::keys::{encode_compound_key, COMPOUND_SEP};
use crate::mvcc::read_view::ReadView;
use crate::mvcc::transaction::{SecIndexOp, SecIndexWrite, WriteTxn};
use crate::storage::btree::{BTree, BTreePageStore, CellValue, HistoryProbe};
use crate::storage::catalog::IndexEntry;

// ---------------------------------------------------------------------------
// Index name generation
// ---------------------------------------------------------------------------

/// Generate a standard index name from a key pattern document.
///
/// Mirrors MongoDB's naming convention:
/// - `{email: 1}` → `"email_1"`
/// - `{name: 1, age: -1}` → `"name_1_age_-1"`
pub(crate) fn generate_index_name(keys: &Document) -> String {
    keys.iter()
        .map(|(field, dir)| {
            let suffix = match dir {
                Bson::Int32(-1) | Bson::Int64(-1) => "-1",
                _ => "1",
            };
            format!("{field}_{suffix}")
        })
        .collect::<Vec<_>>()
        .join("_")
}

// ---------------------------------------------------------------------------
// Dotted-path field extraction
// ---------------------------------------------------------------------------

/// Extract a BSON value from a document using a (possibly dotted) field path.
///
/// Supports nested fields: `"address.city"` traverses into embedded documents.
/// Returns `None` if any segment is missing or an intermediate value is not a
/// document.
pub(crate) fn extract_field_value<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut segments = path.splitn(2, '.');
    let head = segments.next()?;
    let current = doc.get(head)?;

    match segments.next() {
        None => Some(current),
        Some(rest) => match current {
            Bson::Document(nested) => extract_field_value(nested, rest),
            _ => None,
        },
    }
}

// ---------------------------------------------------------------------------
// Index key construction
// ---------------------------------------------------------------------------

/// Build the B+ tree key(s) for a document given an index key pattern and `_id`.
///
/// Returns `(keys, is_multikey)` where:
/// - `keys` is the list of index entry keys to insert (may be empty for sparse
///   indexes when key fields are absent, or have multiple entries for multikey
///   indexes).
/// - `is_multikey` is `true` when any key field contained an array.
///
/// # Errors
///
/// - `Error::Internal` if the index is compound and two fields are both arrays
///   ("parallel arrays" are not supported).
pub(crate) fn build_index_keys(
    doc: &Document,
    key_pattern: &Document,
    doc_id: &Bson,
    sparse: bool,
) -> Result<(Vec<Vec<u8>>, bool)> {
    // Collect (value, ascending) for each key field.
    let fields: Vec<(Option<&Bson>, bool)> = key_pattern
        .iter()
        .map(|(field, dir)| {
            let ascending = !matches!(dir, Bson::Int32(-1) | Bson::Int64(-1));
            let val = extract_field_value(doc, field);
            (val, ascending)
        })
        .collect();

    // Sparse index: skip if any indexed field is absent.
    if sparse && fields.iter().any(|(v, _)| v.is_none()) {
        return Ok((vec![], false));
    }

    let null_bson = Bson::Null;

    // Detect multikey (array) fields.
    let mut array_field_idx: Option<usize> = None;
    for (i, (val, _)) in fields.iter().enumerate() {
        if matches!(val, Some(Bson::Array(_))) {
            if array_field_idx.is_some() {
                // Compound index with two array fields is a MongoDB error.
                return Err(Error::Internal(
                    "cannot index parallel arrays for compound index".into(),
                ));
            }
            array_field_idx = Some(i);
        }
    }

    if let Some(arr_idx) = array_field_idx {
        // Multikey: generate one entry per array element. The invariant set
        // above guarantees that `fields[arr_idx].0` is `Some(Bson::Array(_))`.
        let arr = match fields[arr_idx].0 {
            Some(Bson::Array(a)) => a,
            _ => {
                return Err(Error::Internal(
                    "secondary_index: array_field_idx invariant broken".into(),
                ))
            }
        };

        let mut keys = Vec::with_capacity(arr.len());
        for elem in arr {
            let mut entry: Vec<(&Bson, bool)> = fields
                .iter()
                .enumerate()
                .map(|(i, (val, ascending))| {
                    if i == arr_idx {
                        (elem, *ascending)
                    } else {
                        (val.unwrap_or(&null_bson), *ascending)
                    }
                })
                .collect();
            entry.push((doc_id, true));
            keys.push(encode_compound_key(&entry));
        }
        Ok((keys, true))
    } else {
        // Single entry (including non-array single-field or compound).
        let mut entry: Vec<(&Bson, bool)> = fields
            .iter()
            .map(|(val, ascending)| (val.unwrap_or(&null_bson), *ascending))
            .collect();
        entry.push((doc_id, true));
        Ok((vec![encode_compound_key(&entry)], false))
    }
}

/// Build the exclusive end key for a unique-constraint range scan.
///
/// The prefix is `encode_compound_key(field_values) + [COMPOUND_SEP]`.
/// The end is `encode_compound_key(field_values) + [COMPOUND_SEP + 1]`.
/// Since `COMPOUND_SEP = 0x01 < 0xFF`, the increment never overflows.
fn unique_range(field_values: &[(&Bson, bool)]) -> (Vec<u8>, Vec<u8>) {
    let mut start = encode_compound_key(field_values);
    start.push(COMPOUND_SEP);
    let mut end = start.clone();
    if let Some(last) = end.last_mut() {
        *last += 1; // COMPOUND_SEP 0x01 -> 0x02, safe
    }
    (start, end)
}

// ---------------------------------------------------------------------------
// Unique constraint check
// ---------------------------------------------------------------------------

/// Verify that inserting `doc_id` with the given secondary field values would
/// not violate a unique constraint.
///
/// Performs a range scan over all existing entries sharing the same secondary
/// key prefix.  If any such entry exists with a *different* `_id`, returns
/// `Error::DuplicateKey` (code 11000).
///
/// # Parameters
///
/// - `index_tree`: the secondary index B+ tree to scan.
/// - `key_pattern`: original key-pattern document (used in the error message).
/// - `field_values`: the encoded secondary field values (before appending `_id`).
/// - `doc_id`: the `_id` of the document being inserted.
pub(crate) fn check_unique_constraint_base_only<S: BTreePageStore>(
    index_tree: &BTree<S>,
    key_pattern: &Document,
    field_values: &[(&Bson, bool)],
    doc_id: &Bson,
) -> Result<()> {
    let (start, end) = unique_range(field_values);
    let existing = index_tree.range_scan(Some(&start), Some(&end))?;

    if existing.is_empty() {
        return Ok(());
    }

    // Build the key that *this* document would produce — the only acceptable
    // match (would mean we're updating a document in-place).
    let my_key = {
        let mut entry = field_values.to_vec();
        entry.push((doc_id, true));
        encode_compound_key(&entry)
    };

    for (existing_key, _) in &existing {
        if existing_key != &my_key {
            return Err(Error::DuplicateKey {
                detail: format!(
                    "E11000 duplicate key error — unique index violation on key pattern {key_pattern:?}"
                ),
            });
        }
    }
    Ok(())
}

/// Verify that inserting `doc_id` with the given secondary field values would
/// not violate a unique constraint at this writer's MVCC snapshot.
///
/// Scans the committed secondary-index state through the MVCC merge path and
/// compacts this transaction's staged secondary writes before checking pending
/// inserts. A staged delete masks both an earlier staged insert for the same
/// key and a committed entry found by the scan.
#[allow(
    clippy::too_many_arguments,
    reason = "US-010 pins the Phase 3 helper signature for caller verification"
)]
pub(crate) fn check_unique_constraint_mvcc<S: BTreePageStore>(
    index_tree: &BTree<S>,
    key_pattern: &Document,
    field_values: &[(&Bson, bool)],
    doc_id: &Bson,
    view: &ReadView,
    history: Option<&dyn HistoryProbe>,
    pending: &[SecIndexWrite],
    index_root_page: u32,
) -> Result<()> {
    let (start, end) = unique_range(field_values);
    let existing = index_tree.range_scan_mvcc_bounded(
        Bound::Included(start.as_slice()),
        Bound::Excluded(end.as_slice()),
        view,
        history,
    )?;
    let my_key = {
        let mut entry = field_values.to_vec();
        entry.push((doc_id, true));
        encode_compound_key(&entry)
    };

    let mut pending_deletes: HashSet<Vec<u8>> = HashSet::new();
    let mut pending_inserts: HashSet<Vec<u8>> = HashSet::new();
    for write in pending
        .iter()
        .filter(|write| write.index_root_page == index_root_page)
    {
        if write.key.as_slice() < start.as_slice() || write.key.as_slice() >= end.as_slice() {
            continue;
        }
        match &write.op {
            SecIndexOp::Insert { .. } => {
                pending_deletes.remove(&write.key);
                pending_inserts.insert(write.key.clone());
            }
            SecIndexOp::Delete => {
                pending_inserts.remove(&write.key);
                pending_deletes.insert(write.key.clone());
            }
        }
    }

    for (existing_key, _) in &existing {
        if pending_deletes.contains(existing_key) {
            continue;
        }
        if existing_key != &my_key {
            return Err(Error::DuplicateKey {
                detail: format!(
                    "E11000 duplicate key error — unique index violation on key pattern {key_pattern:?}"
                ),
            });
        }
    }

    for pending_key in &pending_inserts {
        if pending_key != &my_key {
            return Err(Error::DuplicateKey {
                detail: format!(
                    "E11000 duplicate key error — unique index violation on key pattern {key_pattern:?}"
                ),
            });
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Core index maintenance operations
// ---------------------------------------------------------------------------

/// Stage index-insert entries for a newly inserted document (MVCC T5').
///
/// Runtime path: keys are computed, unique-constraint pre-check runs
/// against the durable `index_tree`, and the resulting writes are staged into
/// the active `WriteTxn`. The `install_pending_sec_index` pass at commit time
/// drains the buffer and performs the actual `BTree::insert` on each key.
///
/// Returns `true` if the document triggered multikey behaviour (i.e. the index
/// metadata's `multikey` flag should be set to `true`). For **sparse** indexes,
/// documents missing the indexed field(s) are silently skipped.
///
/// # Errors
///
/// - `Error::DuplicateKey` if `index_entry.unique` and the secondary key
///   already exists for a different document (either in the durable tree or
///   already staged by this same txn).
pub(crate) fn update_index_on_insert<S: BTreePageStore>(
    doc: &Document,
    doc_id: &Bson,
    index_tree: &BTree<S>,
    index_entry: &IndexEntry,
    view: &ReadView,
    history: Option<&dyn HistoryProbe>,
    txn: &mut WriteTxn,
) -> Result<bool> {
    let (keys, is_multikey) =
        build_index_keys(doc, &index_entry.key_pattern, doc_id, index_entry.sparse)?;

    if keys.is_empty() {
        // Sparse: nothing to index for this document.
        return Ok(false);
    }

    // Unique-constraint check (single-entry path only; multikey unique
    // semantics are not yet enforced).
    if index_entry.unique && !is_multikey {
        let null_bson = Bson::Null;
        let field_values: Vec<(&Bson, bool)> = index_entry
            .key_pattern
            .iter()
            .map(|(field, dir)| {
                let ascending = !matches!(dir, Bson::Int32(-1) | Bson::Int64(-1));
                let val = extract_field_value(doc, field).unwrap_or(&null_bson);
                (val, ascending)
            })
            .collect();
        check_unique_constraint_mvcc(
            index_tree,
            &index_entry.key_pattern,
            &field_values,
            doc_id,
            view,
            history,
            txn.pending_sec_index.as_slice(),
            index_entry.root_page,
        )?;
    }

    // Serialize _id once; reused for every key (multikey may have several).
    let id_bytes =
        bson::to_vec(&bson::doc! { "_id": doc_id.clone() }).map_err(Error::BsonSerialization)?;

    // Multikey arrays can emit duplicate compound keys (e.g. `["a", "a"]`);
    // the legacy direct-write path silently swallowed these via `Err(Dup)`
    // on the second insert. In the staged model we dedupe up front so the
    // commit-time install doesn't see duplicates from the same doc.
    let staged_keys: Vec<Vec<u8>> = if is_multikey {
        let mut seen = HashSet::with_capacity(keys.len());
        keys.into_iter()
            .filter(|k| seen.insert(k.clone()))
            .collect()
    } else {
        keys
    };

    for key in staged_keys {
        txn.stage_sec_index_insert(index_entry.id, index_entry.root_page, key, id_bytes.clone());
    }

    Ok(is_multikey)
}

/// Stage index-delete entries for a deleted document (MVCC T5').
///
/// Idempotent at install time: a delete of an already-absent key is silently
/// swallowed by the commit-time install pass.
pub(crate) fn update_index_on_delete(
    doc: &Document,
    doc_id: &Bson,
    index_entry: &IndexEntry,
    txn: &mut WriteTxn,
) -> Result<()> {
    let (keys, _) = build_index_keys(doc, &index_entry.key_pattern, doc_id, index_entry.sparse)?;

    for key in keys {
        txn.stage_sec_index_delete(index_entry.id, index_entry.root_page, key);
    }
    Ok(())
}

/// Stage index-update entries when a document is replaced (MVCC T5').
///
/// Stages the old document's keys for deletion, then the new document's
/// keys for insertion. The commit-time install pass runs them in order so
/// the net effect is an overwrite; when `old_key == new_key` the delete +
/// insert pair reduces to the new value.
#[allow(
    clippy::too_many_arguments,
    reason = "US-010 threads MVCC uniqueness visibility through the existing update API"
)]
pub(crate) fn update_index_on_update<S: BTreePageStore>(
    old_doc: &Document,
    new_doc: &Document,
    old_id: &Bson,
    new_id: &Bson,
    index_tree: &BTree<S>,
    index_entry: &IndexEntry,
    view: &ReadView,
    history: Option<&dyn HistoryProbe>,
    txn: &mut WriteTxn,
) -> Result<bool> {
    update_index_on_delete(old_doc, old_id, index_entry, txn)?;
    update_index_on_insert(new_doc, new_id, index_tree, index_entry, view, history, txn)
}

/// Direct-insert variant used by `build_index` (one-shot index build during
/// `create_index`). Unlike `update_index_on_insert`, this mutates the tree
/// in place rather than staging through a `WriteTxn` — a full-collection
/// build would otherwise accumulate every document's key in
/// `pending_sec_index` for one monolithic install, which is wasteful when
/// the tree is empty and no concurrent readers exist.
fn update_index_on_insert_direct<S: BTreePageStore>(
    doc: &Document,
    doc_id: &Bson,
    index_tree: &mut BTree<S>,
    index_entry: &IndexEntry,
) -> Result<bool> {
    let (keys, is_multikey) =
        build_index_keys(doc, &index_entry.key_pattern, doc_id, index_entry.sparse)?;

    if keys.is_empty() {
        return Ok(false);
    }

    if index_entry.unique && !is_multikey {
        let null_bson = Bson::Null;
        let field_values: Vec<(&Bson, bool)> = index_entry
            .key_pattern
            .iter()
            .map(|(field, dir)| {
                let ascending = !matches!(dir, Bson::Int32(-1) | Bson::Int64(-1));
                let val = extract_field_value(doc, field).unwrap_or(&null_bson);
                (val, ascending)
            })
            .collect();
        check_unique_constraint_base_only(
            index_tree,
            &index_entry.key_pattern,
            &field_values,
            doc_id,
        )?;
    }

    let id_bytes =
        bson::to_vec(&bson::doc! { "_id": doc_id.clone() }).map_err(Error::BsonSerialization)?;

    for key in &keys {
        match index_tree.insert(key, &id_bytes) {
            Ok(()) => {}
            Err(Error::DuplicateKey { .. }) if is_multikey => {}
            Err(e) => return Err(e),
        }
    }

    Ok(is_multikey)
}

/// Build (or rebuild) a secondary index by scanning all documents in the
/// collection's primary data tree.
///
/// `data_tree` is the clustered (`_id`) B+ tree:
/// - key: `encode_key(_id)` (any BSON type)
/// - value: raw BSON document bytes
///
/// Every document is scanned and a corresponding entry is inserted into
/// `index_tree`.  This is a **blocking** build — the caller must hold a writer
/// lock for the entire duration (background builds are not yet implemented).
///
/// Returns `true` if any document triggered multikey behaviour; the caller
/// should persist this flag to the catalog's [`IndexEntry`].
///
/// # Errors
///
/// Propagates storage errors.  `Error::DuplicateKey` is returned if a unique
/// index would be violated by existing data.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn build_index<S1, S2>(
    data_tree: &BTree<S1>,
    index_tree: &mut BTree<S2>,
    index_entry: &IndexEntry,
) -> Result<bool>
where
    S1: BTreePageStore,
    S2: BTreePageStore,
{
    let all_entries = data_tree.range_scan(None, None)?;
    let mut any_multikey = false;

    for (_, cell_value) in &all_entries {
        let doc_bytes = match cell_value {
            CellValue::Inline(b) => b.clone(),
            CellValue::Overflow {
                first_page,
                total_length,
            } => data_tree.read_overflow(*first_page, *total_length)?,
        };

        let doc: Document = bson::from_slice(&doc_bytes).map_err(Error::BsonDeserialization)?;

        let doc_id = doc.get("_id").ok_or_else(|| {
            Error::Internal("document missing '_id' field during index build".into())
        })?;

        let is_multikey = update_index_on_insert_direct(&doc, doc_id, index_tree, index_entry)?;
        if is_multikey {
            any_multikey = true;
        }
    }

    Ok(any_multikey)
}

/// Build (or rebuild) a secondary index from MVCC-visible primary rows.
///
/// Ordinary CRUD now keeps logical row authority in resident delta chains, so
/// DDL-style index builds must scan the primary tree through the same MVCC
/// merge path used by readers. Base-only cells remain sufficient for legacy
/// callers of [`build_index`], while this helper is used by the paged engine's
/// online `create_index` path.
pub(crate) fn build_index_mvcc<S1, S2>(
    data_tree: &BTree<S1>,
    index_tree: &mut BTree<S2>,
    index_entry: &IndexEntry,
    view: &ReadView,
    history: Option<&dyn HistoryProbe>,
) -> Result<bool>
where
    S1: BTreePageStore,
    S2: BTreePageStore,
{
    let all_entries = data_tree.range_scan_mvcc(None, None, view, history)?;
    let mut any_multikey = false;

    for (_, doc_bytes) in all_entries {
        let doc: Document = bson::from_slice(&doc_bytes).map_err(Error::BsonDeserialization)?;
        let doc_id = doc.get("_id").ok_or_else(|| {
            Error::Internal("document missing '_id' field during index build".into())
        })?;

        let is_multikey = update_index_on_insert_direct(&doc, doc_id, index_tree, index_entry)?;
        if is_multikey {
            any_multikey = true;
        }
    }

    Ok(any_multikey)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/secondary_index.rs"]
mod tests;
