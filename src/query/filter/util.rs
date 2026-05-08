//! Helpers shared across the filter evaluator: dotted-path field access,
//! BSON comparison via [`encode_key`], argument-validation helpers, and the
//! `BadValue` error constructor.

use std::cmp::Ordering;

use bson::{Bson, Document};
use serde::de::Error as SerdeDeError;

use crate::error::{Error, Result};
use crate::keys::encode_key;

// ---------------------------------------------------------------------------
// Nested field access (dot notation)
// ---------------------------------------------------------------------------

/// Retrieve the value at a dotted path in a document.
///
/// Supports:
/// - Simple field: `"name"` → `doc.get("name")`
/// - Nested: `"a.b.c"` → traverse embedded documents
/// - Array index: `"arr.0"` → first element of array `arr`
///
/// Returns `None` if the path does not exist.
pub(crate) fn get_nested_field<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut parts = path.splitn(2, '.');
    let head = parts.next()?;
    let tail = parts.next();

    let current = doc.get(head)?;

    match tail {
        None => Some(current),
        Some(rest) => match current {
            Bson::Document(sub_doc) => get_nested_field(sub_doc, rest),
            Bson::Array(arr) => {
                // Try numeric index first.
                if let Ok(idx) = rest.parse::<usize>() {
                    arr.get(idx)
                } else {
                    // Non-numeric key in an array position — no match.
                    None
                }
            }
            _ => None,
        },
    }
}

// ---------------------------------------------------------------------------
// BSON comparison helpers
// ---------------------------------------------------------------------------

/// Compare two BSON values using MongoDB's canonical ordering.
///
/// Uses [`encode_key`] to produce `memcmp`-sortable byte sequences.
pub(super) fn compare_bson(a: &Bson, b: &Bson) -> Ordering {
    let ka = encode_key(a);
    let kb = encode_key(b);
    ka.cmp(&kb)
}

/// Return true if two BSON values are equal under MongoDB's ordering.
pub(super) fn bson_eq(a: &Bson, b: &Bson) -> bool {
    encode_key(a) == encode_key(b)
}

// ---------------------------------------------------------------------------
// Argument validation helpers
// ---------------------------------------------------------------------------

pub(super) fn require_array<'a>(op: &str, val: &'a Bson) -> Result<&'a [Bson]> {
    match val {
        Bson::Array(arr) => Ok(arr),
        _ => Err(bad_value(&format!(
            "{op} must be an array, got: {}",
            bson_type_name(val)
        ))),
    }
}

pub(super) fn require_document<'a>(ctx: &str, val: &'a Bson) -> Result<&'a Document> {
    match val {
        Bson::Document(doc) => Ok(doc),
        _ => Err(bad_value(&format!(
            "{ctx} must be a document, got: {}",
            bson_type_name(val)
        ))),
    }
}

/// Convert a BSON value to `i64`, accepting only whole-number values.
///
/// `Double` values are accepted only if they are exact integers (e.g., `2.0`).
/// Used by `$size` which requires a non-negative integer argument.
pub(super) fn bson_to_i64_strict(op: &str, val: &Bson) -> Result<i64> {
    match val {
        Bson::Int32(n) => Ok(*n as i64),
        Bson::Int64(n) => Ok(*n),
        Bson::Double(f) => {
            let i = *f as i64;
            if i as f64 == *f {
                Ok(i)
            } else {
                Err(bad_value(&format!(
                    "{op} requires a whole-number value, got {f}"
                )))
            }
        }
        _ => Err(bad_value(&format!(
            "{op} must be a number, got: {}",
            bson_type_name(val)
        ))),
    }
}

pub(super) fn bson_to_bool(op: &str, val: &Bson) -> Result<bool> {
    match val {
        Bson::Boolean(b) => Ok(*b),
        Bson::Int32(n) => Ok(*n != 0),
        Bson::Int64(n) => Ok(*n != 0),
        Bson::Double(n) => Ok(*n != 0.0),
        _ => Err(bad_value(&format!(
            "{op} must be a boolean, got: {}",
            bson_type_name(val)
        ))),
    }
}

pub(super) fn bson_type_name(val: &Bson) -> &'static str {
    match val {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Binary(_) => "binData",
        Bson::Undefined => "undefined",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::DbPointer(_) => "dbPointer",
        Bson::JavaScriptCode(_) => "javascript",
        Bson::Symbol(_) => "symbol",
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope",
        Bson::Int32(_) => "int",
        Bson::Timestamp(_) => "timestamp",
        Bson::Int64(_) => "long",
        Bson::Decimal128(_) => "decimal",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
    }
}

pub(super) fn bad_value(msg: &str) -> Error {
    // Repurpose BsonDeserialization to carry BadValue messages.
    // MongoDB error code 2 (BadValue). Requires the serde::de::Error trait.
    Error::BsonDeserialization(bson::de::Error::custom(format!("BadValue: {msg}")))
}
