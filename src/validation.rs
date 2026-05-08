//! BSON document validation.
//!
//! Enforces structural limits on documents at the insert boundary (before any write to storage).
//! These limits are mandatory security mitigations:
//!
//! | Limit | Value | Rationale |
//! |-------|-------|-----------|
//! | Max document size | 16,777,216 bytes (16MB) | Matches MongoDB; prevents memory exhaustion |
//! | Max nesting depth | 100 levels | Prevents stack overflow in recursive operations |
//! | Max field count | 10,000 | DoS prevention (not a MongoDB limit) |
//! | Max field name length | 1,024 bytes | Defense-in-depth (MongoDB allows ~16MB) |
//! | Field name null bytes | Forbidden | BSON spec; prevents index corruption |
//!
//! When a violation is detected the function returns one of:
//! - [`Error::DocumentTooLarge`] (code 10334) — document exceeds 16MB after BSON serialization
//! - [`Error::DocumentValidationFailure`] (code 121) — any other structural constraint

use bson::{Bson, Document};

use crate::error::{Error, Result};

/// Maximum allowed BSON-serialized document size in bytes (16MB, matching MongoDB).
pub const MAX_DOCUMENT_SIZE: usize = 16_777_216;

/// Maximum nesting depth for BSON documents and arrays.
/// Prevents stack overflow during recursive operations (query matching, BSON traversal).
pub const MAX_NESTING_DEPTH: u32 = 100;

/// Maximum total number of fields across the entire document tree.
/// This is a mqlite-specific limit for DoS prevention; MongoDB has no equivalent.
pub const MAX_FIELD_COUNT: usize = 10_000;

/// Maximum byte length of any single BSON field name.
/// mqlite enforces a stricter 1,024-byte limit (MongoDB technically allows up to document size).
pub const MAX_FIELD_NAME_LEN: usize = 1_024;

/// Validate a BSON document before inserting it into a collection.
///
/// Checks document size, nesting depth, field count, and field name constraints.
/// Returns `Ok(())` if the document passes all checks, or an error describing
/// the first violation encountered.
///
/// # Validation order
///
/// 1. **Structural walk** (nesting depth, field count, field names) — runs first because
///    documents with null bytes in field names will cause `bson::to_vec` to fail with a
///    generic `BsonSerialization` error; we want to return `DocumentValidationFailure`
///    (code 121) for these instead.
/// 2. **Size check** — runs after structural validation using `bson::to_vec`, so it only
///    fires on structurally valid documents.
///
/// # Errors
///
/// - [`Error::DocumentValidationFailure`] (code 121) if a structural constraint is violated
/// - [`Error::DocumentTooLarge`] (code 10334) if the BSON-serialized size exceeds 16MB
/// - [`Error::BsonSerialization`] if BSON serialization itself fails for another reason
pub fn validate_document(doc: &Document) -> Result<()> {
    let mut field_count = 0;
    validate_doc_recursive(doc, 0, &mut field_count)?;

    let bytes = bson::to_vec(doc).map_err(Error::BsonSerialization)?;
    if bytes.len() > MAX_DOCUMENT_SIZE {
        return Err(Error::DocumentTooLarge {
            size: bytes.len(),
            max: MAX_DOCUMENT_SIZE,
        });
    }

    Ok(())
}

/// Recursively validate a [`Document`] node.
///
/// `depth` is the 0-based nesting depth of this document (0 = root).
/// `field_count` accumulates the total number of fields seen so far across the whole tree.
fn validate_doc_recursive(doc: &Document, depth: u32, field_count: &mut usize) -> Result<()> {
    if depth > MAX_NESTING_DEPTH {
        return Err(Error::DocumentValidationFailure {
            detail: format!("maximum nesting depth ({MAX_NESTING_DEPTH}) exceeded"),
        });
    }

    for (key, value) in doc {
        if key.len() > MAX_FIELD_NAME_LEN || key.contains('\0') {
            return Err(Error::DocumentValidationFailure {
                detail: "field name too long or contains null byte".into(),
            });
        }

        *field_count += 1;
        if *field_count > MAX_FIELD_COUNT {
            return Err(Error::DocumentValidationFailure {
                detail: format!("field count exceeds maximum ({MAX_FIELD_COUNT})"),
            });
        }

        validate_value_recursive(value, depth + 1, field_count)?;
    }

    Ok(())
}

/// Recursively validate a [`Bson`] value, descending into nested documents and arrays.
fn validate_value_recursive(value: &Bson, depth: u32, field_count: &mut usize) -> Result<()> {
    match value {
        Bson::Document(nested) => validate_doc_recursive(nested, depth, field_count)?,
        Bson::Array(arr) => {
            for elem in arr {
                validate_value_recursive(elem, depth + 1, field_count)?;
            }
        }
        // All other BSON types are leaf nodes — no further recursion needed.
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/validation.rs"]
mod tests;
