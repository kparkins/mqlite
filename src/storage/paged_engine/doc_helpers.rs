//! Pure document helpers shared across `doc_ops`: id assignment, validation,
//! projection, sorting, and unique-constraint checks.

use std::time::{SystemTime, UNIX_EPOCH};

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::keys::encode_key;
use crate::mvcc::read_view::ReadView;
use crate::mvcc::transaction::{PrimaryOp, PrimaryWrite};
use crate::query::get_nested_field;
use crate::storage::btree::{BTree, BTreePageStore, HistoryProbe};
use crate::storage::oid::ObjectIdGenerator;

/// Return current Unix milliseconds.
pub(in crate::storage::paged_engine) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Ensure a document has an `_id` field.  Auto-assigns an [`ObjectId`] if absent.
pub(in crate::storage::paged_engine) fn ensure_id(doc: &mut Document) -> Bson {
    if let Some(id) = doc.get("_id") {
        id.clone()
    } else {
        let oid = Bson::ObjectId(ObjectIdGenerator::generate());
        doc.insert("_id", oid.clone());
        oid
    }
}

/// Validate that an index key pattern does not request an unsupported index type.
///
/// Rejects `text`, `2d`, `2dsphere`, and `hashed` indexes (not yet implemented).
pub(in crate::storage::paged_engine) fn validate_index_keys(keys: &Document) -> Result<()> {
    const SUGGESTION: &str = "Supported: single-field, compound, unique, sparse, and multikey \
         indexes. Text, geospatial, hashed, TTL, and partial indexes are \
         planned for a future release.";

    for (_field, value) in keys {
        if let Bson::String(t) = value {
            if matches!(t.as_str(), "text" | "2d" | "2dsphere" | "hashed") {
                return Err(Error::UnsupportedIndexOption {
                    option: t.to_owned(),
                    suggestion: SUGGESTION.to_owned(),
                });
            }
        }
    }
    Ok(())
}

/// Check unique index constraints against MVCC-visible rows and staged writes.
///
/// `unique_specs` is a list of `(index_name, fields, sparse)` for each unique
/// index. If any visible or same-transaction document matches the new doc on
/// all indexed fields with a different `_id`, returns [`Error::DuplicateKey`].
pub(in crate::storage::paged_engine) fn check_unique_constraints_mvcc<S: BTreePageStore>(
    tree: &BTree<S>,
    unique_specs: &[(String, Vec<String>, bool)],
    new_doc: &Document,
    view: &ReadView,
    history: Option<&dyn HistoryProbe>,
    pending: &[PrimaryWrite],
    ns: &str,
) -> Result<()> {
    if unique_specs.is_empty() {
        return Ok(());
    }

    let null_encoded = encode_key(&Bson::Null);
    let new_id = new_doc.get("_id").unwrap_or(&Bson::Null);

    for (idx_name, fields, sparse) in unique_specs {
        let encode_fields = |out: &mut Vec<Vec<u8>>, doc: &Document| {
            out.clear();
            out.extend(
                fields
                    .iter()
                    .map(|f| encode_key(doc.get(f.as_str()).unwrap_or(&Bson::Null))),
            );
        };
        let duplicate_key = || Error::DuplicateKey {
            detail: format!(
                "E11000 duplicate key error — unique index '{}': dup key {{{}}}",
                idx_name,
                fields
                    .iter()
                    .map(|f| format!("{}: {:?}", f, new_doc.get(f.as_str())))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        };

        let mut new_encoded = Vec::with_capacity(fields.len());
        encode_fields(&mut new_encoded, new_doc);

        if *sparse && new_encoded.iter().all(|v| v == &null_encoded) {
            continue;
        }

        let pairs = tree.range_scan_mvcc(None, None, view, history)?;
        let mut existing_encoded = Vec::with_capacity(fields.len());
        for (_, bson_bytes) in pairs {
            let existing: Document =
                bson::from_slice(&bson_bytes).map_err(Error::BsonDeserialization)?;

            encode_fields(&mut existing_encoded, &existing);
            if new_encoded == existing_encoded
                && existing.get("_id").unwrap_or(&Bson::Null) != new_id
            {
                return Err(duplicate_key());
            }
        }

        for staged in pending.iter().filter(|staged| staged.ns.as_str() == ns) {
            let data = match &staged.op {
                PrimaryOp::Insert { data } | PrimaryOp::Update { data } => data,
                PrimaryOp::Delete => continue,
            };
            let existing: Document = bson::from_slice(data).map_err(Error::BsonDeserialization)?;

            encode_fields(&mut existing_encoded, &existing);
            if new_encoded == existing_encoded
                && existing.get("_id").unwrap_or(&Bson::Null) != new_id
            {
                return Err(duplicate_key());
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Sort / projection helpers (replicated from engine.rs for local use)
// ---------------------------------------------------------------------------

pub(in crate::storage::paged_engine) fn compare_docs(
    a: &Document,
    b: &Document,
    sort: &Document,
) -> std::cmp::Ordering {
    for (field, dir) in sort {
        let ascending = !matches!(dir, Bson::Int32(-1) | Bson::Int64(-1));
        let av = get_nested_field(a, field).cloned().unwrap_or(Bson::Null);
        let bv = get_nested_field(b, field).cloned().unwrap_or(Bson::Null);
        let ord = encode_key(&av).cmp(&encode_key(&bv));
        if ord == std::cmp::Ordering::Equal {
            continue;
        }
        return if ascending { ord } else { ord.reverse() };
    }
    std::cmp::Ordering::Equal
}

pub(in crate::storage::paged_engine) fn apply_projection_to_doc(
    mut doc: Document,
    proj: &Document,
) -> Document {
    let is_inclusion = proj
        .iter()
        .filter(|(k, _)| k.as_str() != "_id")
        .any(|(_, v)| !matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)));

    let explicit_id_excl = proj
        .get("_id")
        .is_some_and(|v| matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)));

    if is_inclusion {
        let mut result = Document::new();
        if !explicit_id_excl {
            if let Some(id) = doc.get("_id") {
                result.insert("_id", id.clone());
            }
        }
        for (k, v) in proj {
            if k == "_id" {
                continue;
            }
            if !matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)) {
                if let Some(val) = doc.get(k) {
                    result.insert(k, val.clone());
                }
            }
        }
        result
    } else {
        for (k, _) in proj {
            doc.remove(k);
        }
        doc
    }
}
