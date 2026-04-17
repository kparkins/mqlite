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

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::key_encoding::{encode_compound_key, COMPOUND_SEP};
use crate::mvcc::transaction::{SecIndexOp, WriteTxn};
use crate::storage::btree::{BTree, BTreePageStore, CellValue};
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
    let parts: Vec<String> = keys
        .iter()
        .map(|(field, dir)| {
            let suffix = match dir {
                Bson::Int32(-1) | Bson::Int64(-1) => "-1",
                _ => "1",
            };
            format!("{field}_{suffix}")
        })
        .collect();
    parts.join("_")
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
    // Collect (field_name, value, ascending) for each key field.
    let fields: Vec<(String, Option<&Bson>, bool)> = key_pattern
        .iter()
        .map(|(field, dir)| {
            let ascending = !matches!(dir, Bson::Int32(-1) | Bson::Int64(-1));
            let val = extract_field_value(doc, field);
            (field.clone(), val, ascending)
        })
        .collect();

    // Sparse index: skip if any indexed field is absent.
    if sparse && fields.iter().any(|(_, v, _)| v.is_none()) {
        return Ok((vec![], false));
    }

    let null_bson = Bson::Null;

    // Detect multikey (array) fields.
    let mut array_field_idx: Option<usize> = None;
    for (i, (_, val, _)) in fields.iter().enumerate() {
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
        // Multikey: generate one entry per array element.
        let arr = match fields[arr_idx].1.unwrap() {
            Bson::Array(a) => a,
            _ => unreachable!(),
        };

        let mut keys = Vec::with_capacity(arr.len());
        for elem in arr {
            let mut entry: Vec<(&Bson, bool)> = fields
                .iter()
                .enumerate()
                .map(|(i, (_, val, ascending))| {
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
            .map(|(_, val, ascending)| (val.unwrap_or(&null_bson), *ascending))
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
    *end.last_mut().expect("non-empty prefix") += 1; // 0x01 → 0x02, safe
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
pub(crate) fn check_unique_constraint<S: BTreePageStore>(
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
                    "E11000 duplicate key error — unique index violation on key pattern {:?}",
                    key_pattern
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
/// Runtime runtime path: keys are computed, unique-constraint pre-check runs
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
    txn: &mut WriteTxn,
) -> Result<bool> {
    let (keys, is_multikey) =
        build_index_keys(doc, &index_entry.key_pattern, doc_id, index_entry.sparse)?;

    if keys.is_empty() {
        // Sparse: nothing to index for this document.
        return Ok(false);
    }

    // Unique-constraint check (single-entry path only; multikey semantics
    // are complex and deferred to Phase 2 for production hardening).
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
        check_unique_constraint(index_tree, &index_entry.key_pattern, &field_values, doc_id)?;
        // In-txn conflict: a prior `stage_sec_index_insert` on the same
        // index whose key falls within this doc's unique prefix range
        // claims the same unique slot. Secondary keys are
        // `compound_field_vals | COMPOUND_SEP | _id`, so compare by
        // prefix (field values only), not full key equality — the `_id`
        // suffix differs when two distinct docs collide on a unique field.
        let (range_start, range_end) = unique_range(&field_values);
        for pending in &txn.pending_sec_index {
            if pending.index_root_page == index_entry.root_page
                && matches!(pending.op, SecIndexOp::Insert { .. })
                && pending.key.as_slice() >= range_start.as_slice()
                && pending.key.as_slice() < range_end.as_slice()
            {
                return Err(Error::DuplicateKey {
                    detail: format!(
                        "unique index '{}' — in-txn conflict",
                        index_entry.name
                    ),
                });
            }
        }
    }

    // Serialize _id once; reused for every key (multikey may have several).
    let id_bytes = bson::to_vec(&bson::doc! { "_id": doc_id.clone() })
        .map_err(Error::BsonSerialization)?;

    // Multikey arrays can emit duplicate compound keys (e.g. `["a", "a"]`);
    // the legacy direct-write path silently swallowed these via `Err(Dup)`
    // on the second insert. In the staged model we dedupe up front so the
    // commit-time install doesn't see duplicates from the same doc.
    let staged_keys: Vec<Vec<u8>> = if is_multikey {
        let mut seen = std::collections::HashSet::new();
        keys.into_iter().filter(|k| seen.insert(k.clone())).collect()
    } else {
        keys
    };

    for key in staged_keys {
        txn.stage_sec_index_insert(index_entry.root_page, key, id_bytes.clone());
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
    let (keys, _) =
        build_index_keys(doc, &index_entry.key_pattern, doc_id, index_entry.sparse)?;

    for key in keys {
        txn.stage_sec_index_delete(index_entry.root_page, key);
    }
    Ok(())
}

/// Stage index-update entries when a document is replaced (MVCC T5').
///
/// Stages the old document's keys for deletion, then the new document's
/// keys for insertion. The commit-time install pass runs them in order so
/// the net effect is an overwrite; when `old_key == new_key` the delete +
/// insert pair reduces to the new value.
pub(crate) fn update_index_on_update<S: BTreePageStore>(
    old_doc: &Document,
    new_doc: &Document,
    old_id: &Bson,
    new_id: &Bson,
    index_tree: &BTree<S>,
    index_entry: &IndexEntry,
    txn: &mut WriteTxn,
) -> Result<bool> {
    update_index_on_delete(old_doc, old_id, index_entry, txn)?;
    update_index_on_insert(new_doc, new_id, index_tree, index_entry, txn)
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
        check_unique_constraint(index_tree, &index_entry.key_pattern, &field_values, doc_id)?;
    }

    let id_bytes = bson::to_vec(&bson::doc! { "_id": doc_id.clone() })
        .map_err(Error::BsonSerialization)?;

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
/// lock for the entire duration (background builds are Phase 2).
///
/// Returns `true` if any document triggered multikey behaviour; the caller
/// should persist this flag to the catalog's [`IndexEntry`].
///
/// # Errors
///
/// Propagates storage errors.  `Error::DuplicateKey` is returned if a unique
/// index would be violated by existing data.
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

        let is_multikey =
            update_index_on_insert_direct(&doc, doc_id, index_tree, index_entry)?;
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
mod tests {
    use super::*;
    use crate::storage::btree::{BTree, MemPageStore};
    use crate::storage::catalog::IndexEntry;
    use bson::{doc, oid::ObjectId, Bson};

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn fresh_tree() -> BTree<MemPageStore> {
        BTree::create(MemPageStore::new()).expect("create tree")
    }

    /// Drain a `WriteTxn`'s pending sec-index writes into `tree`, mirroring
    /// the commit-time `install_pending_sec_index` pass. Lets tests keep
    /// observable tree-state assertions after staging.
    fn install_pending<S: BTreePageStore>(
        txn: &mut WriteTxn,
        tree: &mut BTree<S>,
    ) -> Result<()> {
        let writes = std::mem::take(&mut txn.pending_sec_index);
        for w in writes {
            match w.op {
                SecIndexOp::Insert { id_bytes } => {
                    tree.insert(&w.key, &id_bytes)?;
                }
                SecIndexOp::Delete => {
                    let _ = tree.delete(&w.key)?;
                }
            }
        }
        Ok(())
    }

    /// Stage + install helper for insert tests.
    fn stage_insert<S: BTreePageStore>(
        doc: &Document,
        doc_id: &Bson,
        tree: &mut BTree<S>,
        entry: &IndexEntry,
    ) -> Result<bool> {
        let mut txn = WriteTxn::new(0);
        let r = update_index_on_insert(doc, doc_id, tree, entry, &mut txn)?;
        install_pending(&mut txn, tree)?;
        Ok(r)
    }

    /// Stage + install helper for delete tests.
    fn stage_delete<S: BTreePageStore>(
        doc: &Document,
        doc_id: &Bson,
        tree: &mut BTree<S>,
        entry: &IndexEntry,
    ) -> Result<()> {
        let mut txn = WriteTxn::new(0);
        update_index_on_delete(doc, doc_id, entry, &mut txn)?;
        install_pending(&mut txn, tree)
    }

    /// Stage + install helper for update tests.
    fn stage_update<S: BTreePageStore>(
        old_doc: &Document,
        new_doc: &Document,
        old_id: &Bson,
        new_id: &Bson,
        tree: &mut BTree<S>,
        entry: &IndexEntry,
    ) -> Result<bool> {
        let mut txn = WriteTxn::new(0);
        let r = update_index_on_update(old_doc, new_doc, old_id, new_id, tree, entry, &mut txn)?;
        install_pending(&mut txn, tree)?;
        Ok(r)
    }

    fn make_index_entry(key_pattern: Document, unique: bool, sparse: bool) -> IndexEntry {
        IndexEntry {
            name: generate_index_name(&key_pattern),
            collection: "test".into(),
            root_page: 1,
            root_level: 0,
            key_pattern,
            unique,
            sparse,
            multikey: false,
            entry_count: 0,
        }
    }

    fn oid_bson() -> Bson {
        Bson::ObjectId(ObjectId::new())
    }

    // -----------------------------------------------------------------------
    // generate_index_name
    // -----------------------------------------------------------------------

    #[test]
    fn index_name_single_ascending() {
        assert_eq!(generate_index_name(&doc! { "email": 1 }), "email_1");
    }

    #[test]
    fn index_name_single_descending() {
        assert_eq!(generate_index_name(&doc! { "score": -1 }), "score_-1");
    }

    #[test]
    fn index_name_compound() {
        assert_eq!(
            generate_index_name(&doc! { "name": 1, "age": -1 }),
            "name_1_age_-1"
        );
    }

    // -----------------------------------------------------------------------
    // extract_field_value
    // -----------------------------------------------------------------------

    #[test]
    fn extract_top_level_field() {
        let doc = doc! { "email": "alice@example.com" };
        let val = extract_field_value(&doc, "email").unwrap();
        assert_eq!(val, &Bson::String("alice@example.com".into()));
    }

    #[test]
    fn extract_nested_field() {
        let doc = doc! { "address": { "city": "Vancouver" } };
        let val = extract_field_value(&doc, "address.city").unwrap();
        assert_eq!(val, &Bson::String("Vancouver".into()));
    }

    #[test]
    fn extract_missing_field_returns_none() {
        let doc = doc! { "name": "Alice" };
        assert!(extract_field_value(&doc, "email").is_none());
        assert!(extract_field_value(&doc, "address.city").is_none());
    }

    #[test]
    fn extract_missing_nested_segment_returns_none() {
        let doc = doc! { "address": "123 Main St" }; // not a document
        assert!(extract_field_value(&doc, "address.city").is_none());
    }

    // -----------------------------------------------------------------------
    // build_index_keys — single field
    // -----------------------------------------------------------------------

    #[test]
    fn single_field_key_ascending() {
        let id = Bson::Int32(42);
        let doc = doc! { "email": "alice@example.com" };
        let (keys, multikey) = build_index_keys(&doc, &doc! { "email": 1 }, &id, false).unwrap();
        assert_eq!(keys.len(), 1);
        assert!(!multikey);
    }

    #[test]
    fn single_field_key_missing_field_uses_null() {
        let id = oid_bson();
        let doc = doc! { "name": "Alice" }; // no "score"
        let (keys, _) = build_index_keys(
            &doc,
            &doc! { "score": 1 },
            &id,
            false, // non-sparse
        )
        .unwrap();
        assert_eq!(keys.len(), 1, "non-sparse: one null entry expected");
    }

    #[test]
    fn sparse_index_skips_missing_field() {
        let id = oid_bson();
        let doc = doc! { "name": "Alice" }; // no "score"
        let (keys, _) = build_index_keys(
            &doc,
            &doc! { "score": 1 },
            &id,
            true, // sparse
        )
        .unwrap();
        assert!(keys.is_empty(), "sparse: document should be skipped");
    }

    // -----------------------------------------------------------------------
    // build_index_keys — compound
    // -----------------------------------------------------------------------

    #[test]
    fn compound_key_sort_order() {
        let id = Bson::Int32(1);

        let doc_a = doc! { "name": "Alice", "age": 30 };
        let doc_b = doc! { "name": "Alice", "age": 40 };
        let doc_c = doc! { "name": "Bob",   "age": 20 };

        let pattern = doc! { "name": 1, "age": 1 };

        let (ka, _) = build_index_keys(&doc_a, &pattern, &id, false).unwrap();
        let (kb, _) = build_index_keys(&doc_b, &pattern, &id, false).unwrap();
        let (kc, _) = build_index_keys(&doc_c, &pattern, &id, false).unwrap();

        assert!(
            ka[0] < kb[0],
            "Alice/30 should sort before Alice/40 (same name, lower age)"
        );
        assert!(
            kb[0] < kc[0],
            "Alice/40 should sort before Bob/20 (name: Alice < Bob)"
        );
    }

    #[test]
    fn compound_descending_reverses_age_order() {
        let id = Bson::Int32(1);
        let doc_a = doc! { "name": "Alice", "age": 30 };
        let doc_b = doc! { "name": "Alice", "age": 40 };

        // age: -1 means higher age should sort FIRST (lower key bytes).
        let pattern = doc! { "name": 1, "age": -1 };

        let (ka, _) = build_index_keys(&doc_a, &pattern, &id, false).unwrap();
        let (kb, _) = build_index_keys(&doc_b, &pattern, &id, false).unwrap();

        assert!(
            kb[0] < ka[0],
            "age DESC: age=40 should produce a lower key than age=30"
        );
    }

    // -----------------------------------------------------------------------
    // build_index_keys — multikey (array field)
    // -----------------------------------------------------------------------

    #[test]
    fn multikey_one_entry_per_element() {
        let id = Bson::Int32(1);
        let doc = doc! { "tags": ["rust", "db", "bson"] };
        let (keys, multikey) = build_index_keys(&doc, &doc! { "tags": 1 }, &id, false).unwrap();
        assert_eq!(keys.len(), 3, "three tags → three index entries");
        assert!(multikey);
    }

    #[test]
    fn multikey_duplicate_array_elements() {
        let id = Bson::Int32(1);
        let doc = doc! { "tags": ["rust", "rust"] }; // duplicate
        let (keys, multikey) = build_index_keys(&doc, &doc! { "tags": 1 }, &id, false).unwrap();
        // Two identical keys are produced; deduplication happens at insert time.
        assert_eq!(keys.len(), 2);
        assert!(multikey);
        assert_eq!(keys[0], keys[1]); // same encoded bytes
    }

    #[test]
    fn compound_parallel_arrays_error() {
        let id = Bson::Int32(1);
        let doc = doc! { "a": [1, 2], "b": [3, 4] };
        let result = build_index_keys(&doc, &doc! { "a": 1, "b": 1 }, &id, false);
        assert!(
            matches!(result, Err(Error::Internal(_))),
            "parallel arrays in compound index should be an error"
        );
    }

    // -----------------------------------------------------------------------
    // update_index_on_insert
    // -----------------------------------------------------------------------

    #[test]
    fn insert_single_field_entry() {
        let mut tree = fresh_tree();
        let id = oid_bson();
        let doc = doc! { "_id": id.clone(), "email": "a@test.com" };
        let entry = make_index_entry(doc! { "email": 1 }, false, false);

        let multikey = stage_insert(&doc, &id, &mut tree, &entry).unwrap();
        assert!(!multikey);

        // Verify the entry exists in the tree.
        let (keys, _) = build_index_keys(&doc, &entry.key_pattern, &id, false).unwrap();
        let found = tree.search(&keys[0]).unwrap();
        assert!(found.is_some(), "index entry should exist after insert");
    }

    #[test]
    fn insert_multikey_dedup() {
        let mut tree = fresh_tree();
        let id = Bson::Int32(1);
        let doc = doc! { "_id": id.clone(), "tags": ["rust", "rust"] };
        let entry = make_index_entry(doc! { "tags": 1 }, false, false);

        // Should succeed even though two identical keys are produced.
        let multikey = stage_insert(&doc, &id, &mut tree, &entry).unwrap();
        assert!(multikey);
    }

    #[test]
    fn insert_sparse_skips_missing() {
        let mut tree = fresh_tree();
        let id = oid_bson();
        let doc = doc! { "_id": id.clone(), "name": "Alice" }; // no "score"
        let entry = make_index_entry(doc! { "score": 1 }, false, true /* sparse */);

        let multikey = stage_insert(&doc, &id, &mut tree, &entry).unwrap();
        assert!(!multikey);

        // Tree should be empty.
        let all = tree.range_scan(None, None).unwrap();
        assert!(all.is_empty());
    }

    // -----------------------------------------------------------------------
    // Unique constraint
    // -----------------------------------------------------------------------

    #[test]
    fn unique_insert_second_same_field_errors() {
        let mut tree = fresh_tree();
        let id1 = Bson::ObjectId(ObjectId::new());
        let id2 = Bson::ObjectId(ObjectId::new());

        let doc1 = doc! { "_id": id1.clone(), "email": "dup@test.com" };
        let doc2 = doc! { "_id": id2.clone(), "email": "dup@test.com" };

        let entry = make_index_entry(doc! { "email": 1 }, true /* unique */, false);

        stage_insert(&doc1, &id1, &mut tree, &entry).unwrap();
        let result = stage_insert(&doc2, &id2, &mut tree, &entry);

        assert!(
            matches!(result, Err(Error::DuplicateKey { .. })),
            "unique index: second document with same email should fail"
        );
    }

    #[test]
    fn unique_insert_different_field_succeeds() {
        let mut tree = fresh_tree();
        let id1 = Bson::ObjectId(ObjectId::new());
        let id2 = Bson::ObjectId(ObjectId::new());

        let doc1 = doc! { "_id": id1.clone(), "email": "a@test.com" };
        let doc2 = doc! { "_id": id2.clone(), "email": "b@test.com" };

        let entry = make_index_entry(doc! { "email": 1 }, true, false);

        stage_insert(&doc1, &id1, &mut tree, &entry).unwrap();
        stage_insert(&doc2, &id2, &mut tree, &entry).unwrap(); // must succeed
    }

    #[test]
    fn unique_insert_same_id_idempotent() {
        // Inserting the same (field_value, _id) pair twice should only fail
        // on the BTree DuplicateKey at the tree level, not at the unique check.
        let mut tree = fresh_tree();
        let id = Bson::ObjectId(ObjectId::new());
        let doc = doc! { "_id": id.clone(), "email": "a@test.com" };
        let entry = make_index_entry(doc! { "email": 1 }, true, false);

        stage_insert(&doc, &id, &mut tree, &entry).unwrap();
        // Second insert (same _id): unique check passes, BTree DuplicateKey error.
        let result = stage_insert(&doc, &id, &mut tree, &entry);
        // BTree-level duplicate, not a unique-constraint violation.
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // MVCC T5' — staging-specific behaviours (sub-step 5)
    // -----------------------------------------------------------------------

    /// In-txn conflict: two stage_insert calls with the same unique key in
    /// the same txn must be caught by the `pending_sec_index` scan in
    /// `update_index_on_insert` — the durable tree is still empty at that
    /// point so `check_unique_constraint` would otherwise pass.
    #[test]
    fn staged_unique_in_txn_conflict() {
        let tree = fresh_tree();
        let entry = make_index_entry(doc! { "email": 1 }, true /* unique */, false);
        let id1 = Bson::ObjectId(ObjectId::new());
        let id2 = Bson::ObjectId(ObjectId::new());
        let doc1 = doc! { "_id": id1.clone(), "email": "dup@test.com" };
        let doc2 = doc! { "_id": id2.clone(), "email": "dup@test.com" };

        let mut txn = WriteTxn::new(0);
        update_index_on_insert(&doc1, &id1, &tree, &entry, &mut txn)
            .expect("first insert stages cleanly");
        let result = update_index_on_insert(&doc2, &id2, &tree, &entry, &mut txn);
        assert!(
            matches!(result, Err(Error::DuplicateKey { .. })),
            "second staged insert with same unique key must fail as in-txn conflict"
        );
    }

    /// Multikey dedupe happens at stage time via `HashSet`: duplicate array
    /// elements (e.g. `tags: ["rust", "rust"]`) produce a single
    /// `SecIndexWrite` in `pending_sec_index`, not two.
    #[test]
    fn staged_multikey_dedupe_single_pending_entry() {
        let tree = fresh_tree();
        let entry = make_index_entry(doc! { "tags": 1 }, false, false);
        let id = Bson::Int32(1);
        let doc = doc! { "_id": id.clone(), "tags": ["rust", "rust", "db"] };

        let mut txn = WriteTxn::new(0);
        let is_multikey = update_index_on_insert(&doc, &id, &tree, &entry, &mut txn)
            .expect("stage multikey insert");
        assert!(is_multikey);
        assert_eq!(
            txn.pending_sec_index.len(),
            2,
            "three array elems with one dup should yield two staged writes (rust, db)",
        );
        // All staged ops are Inserts targeting this index's root_page.
        for w in &txn.pending_sec_index {
            assert_eq!(w.index_root_page, entry.root_page);
            assert!(matches!(w.op, SecIndexOp::Insert { .. }));
        }
    }

    // -----------------------------------------------------------------------
    // update_index_on_delete
    // -----------------------------------------------------------------------

    #[test]
    fn delete_removes_entry() {
        let mut tree = fresh_tree();
        let id = oid_bson();
        let doc = doc! { "_id": id.clone(), "email": "del@test.com" };
        let entry = make_index_entry(doc! { "email": 1 }, false, false);

        stage_insert(&doc, &id, &mut tree, &entry).unwrap();
        stage_delete(&doc, &id, &mut tree, &entry).unwrap();

        let all = tree.range_scan(None, None).unwrap();
        assert!(all.is_empty(), "entry should be removed after delete");
    }

    #[test]
    fn delete_multikey_removes_all_array_entries() {
        let mut tree = fresh_tree();
        let id = Bson::Int32(5);
        let doc = doc! { "_id": id.clone(), "tags": ["a", "b", "c"] };
        let entry = make_index_entry(doc! { "tags": 1 }, false, false);

        stage_insert(&doc, &id, &mut tree, &entry).unwrap();

        let before = tree.range_scan(None, None).unwrap();
        assert_eq!(before.len(), 3);

        stage_delete(&doc, &id, &mut tree, &entry).unwrap();

        let after = tree.range_scan(None, None).unwrap();
        assert!(after.is_empty(), "all multikey entries should be removed");
    }

    // -----------------------------------------------------------------------
    // update_index_on_update
    // -----------------------------------------------------------------------

    #[test]
    fn update_replaces_old_entry_with_new() {
        let mut tree = fresh_tree();
        let id = oid_bson();
        let old_doc = doc! { "_id": id.clone(), "email": "old@test.com" };
        let new_doc = doc! { "_id": id.clone(), "email": "new@test.com" };
        let entry = make_index_entry(doc! { "email": 1 }, false, false);

        stage_insert(&old_doc, &id, &mut tree, &entry).unwrap();
        stage_update(&old_doc, &new_doc, &id, &id, &mut tree, &entry).unwrap();

        let all = tree.range_scan(None, None).unwrap();
        assert_eq!(all.len(), 1, "exactly one entry after update");

        // Verify the entry is for "new@test.com", not "old@test.com".
        let (old_keys, _) = build_index_keys(&old_doc, &entry.key_pattern, &id, false).unwrap();
        let (new_keys, _) = build_index_keys(&new_doc, &entry.key_pattern, &id, false).unwrap();

        assert!(
            tree.search(&old_keys[0]).unwrap().is_none(),
            "old entry should be gone"
        );
        assert!(
            tree.search(&new_keys[0]).unwrap().is_some(),
            "new entry should be present"
        );
    }

    // -----------------------------------------------------------------------
    // build_index (full scan)
    // -----------------------------------------------------------------------

    #[test]
    fn build_index_populates_from_data_tree() {
        use crate::key_encoding::encode_key;

        let mut data_tree = fresh_tree();
        let mut idx_tree = fresh_tree();

        // Insert 3 documents into the data tree (simulates primary storage).
        let docs = vec![
            (
                Bson::ObjectId(ObjectId::new()),
                doc! { "email": "alice@test.com", "score": 10 },
            ),
            (
                Bson::ObjectId(ObjectId::new()),
                doc! { "email": "bob@test.com",   "score": 20 },
            ),
            (
                Bson::ObjectId(ObjectId::new()),
                doc! { "email": "carol@test.com", "score": 15 },
            ),
        ];

        for (id, mut doc) in docs.clone() {
            doc.insert("_id", id.clone());
            let key = encode_key(&id);
            let value = bson::to_vec(&doc).unwrap();
            data_tree.insert(&key, &value).unwrap();
        }

        let index_entry = make_index_entry(doc! { "email": 1 }, false, false);
        let any_multikey = build_index(&data_tree, &mut idx_tree, &index_entry).unwrap();
        assert!(!any_multikey);

        let all_idx = idx_tree.range_scan(None, None).unwrap();
        assert_eq!(all_idx.len(), 3, "three documents → three index entries");
    }

    #[test]
    fn build_index_detects_multikey() {
        use crate::key_encoding::encode_key;

        let mut data_tree = fresh_tree();
        let mut idx_tree = fresh_tree();

        let id = Bson::ObjectId(ObjectId::new());
        let mut doc = doc! { "tags": ["rust", "db"] };
        doc.insert("_id", id.clone());

        let key = encode_key(&id);
        let value = bson::to_vec(&doc).unwrap();
        data_tree.insert(&key, &value).unwrap();

        let index_entry = make_index_entry(doc! { "tags": 1 }, false, false);
        let any_multikey = build_index(&data_tree, &mut idx_tree, &index_entry).unwrap();
        assert!(
            any_multikey,
            "document with array field should trigger multikey"
        );

        let all_idx = idx_tree.range_scan(None, None).unwrap();
        assert_eq!(all_idx.len(), 2, "two tags → two index entries");
    }

    #[test]
    fn build_index_unique_detects_duplicate() {
        use crate::key_encoding::encode_key;

        let mut data_tree = fresh_tree();
        let mut idx_tree = fresh_tree();

        let id1 = Bson::ObjectId(ObjectId::new());
        let id2 = Bson::ObjectId(ObjectId::new());

        let mut doc1 = doc! { "email": "dup@test.com" };
        doc1.insert("_id", id1.clone());
        let mut doc2 = doc! { "email": "dup@test.com" };
        doc2.insert("_id", id2.clone());

        data_tree
            .insert(&encode_key(&id1), &bson::to_vec(&doc1).unwrap())
            .unwrap();
        data_tree
            .insert(&encode_key(&id2), &bson::to_vec(&doc2).unwrap())
            .unwrap();

        let index_entry = make_index_entry(doc! { "email": 1 }, true /* unique */, false);
        let result = build_index(&data_tree, &mut idx_tree, &index_entry);
        assert!(
            matches!(result, Err(Error::DuplicateKey { .. })),
            "build_index on data with duplicate emails must error on unique index"
        );
    }

    // -----------------------------------------------------------------------
    // Prefix query semantics (validates unique-range logic)
    // -----------------------------------------------------------------------

    #[test]
    fn index_supports_prefix_query() {
        // Verify that a range scan with a prefix of the secondary key returns
        // only entries for that prefix.
        let mut tree = fresh_tree();
        let entry = make_index_entry(doc! { "score": 1 }, false, false);

        let id1 = Bson::Int32(1);
        let id2 = Bson::Int32(2);
        let id3 = Bson::Int32(3);

        let doc1 = doc! { "_id": id1.clone(), "score": 100 };
        let doc2 = doc! { "_id": id2.clone(), "score": 100 };
        let doc3 = doc! { "_id": id3.clone(), "score": 200 };

        stage_insert(&doc1, &id1, &mut tree, &entry).unwrap();
        stage_insert(&doc2, &id2, &mut tree, &entry).unwrap();
        stage_insert(&doc3, &id3, &mut tree, &entry).unwrap();

        // Use unique_range to scan all entries for score=100.
        let score_100 = Bson::Int32(100);
        let fv = [(&score_100, true)];
        let (start, end) = {
            use crate::key_encoding::encode_compound_key;
            let mut s = encode_compound_key(&fv);
            s.push(COMPOUND_SEP);
            let mut e = s.clone();
            *e.last_mut().unwrap() += 1;
            (s, e)
        };

        let results = tree.range_scan(Some(&start), Some(&end)).unwrap();
        assert_eq!(results.len(), 2, "two docs with score=100 should be found");
    }
}
