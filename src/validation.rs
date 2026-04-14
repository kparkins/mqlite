//! BSON document validation.
//!
//! Enforces structural limits on documents at the insert boundary (before any write to storage).
//! These limits are mandatory security mitigations (mqlite security.md Phase 1, mitigation #3):
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

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

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
    // Step 1: structural walk — must run before bson::to_vec to catch null-byte
    // field names with the correct error type.
    let mut field_count: usize = 0;
    validate_doc_recursive(doc, 0, &mut field_count)?;

    // Step 2: size check using canonical BSON serialization.
    let bytes = bson::to_vec(doc).map_err(Error::BsonSerialization)?;
    if bytes.len() > MAX_DOCUMENT_SIZE {
        return Err(Error::DocumentTooLarge {
            size: bytes.len(),
            max: MAX_DOCUMENT_SIZE,
        });
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Recursively validate a [`Document`] node.
///
/// `depth` is the 0-based nesting depth of this document (0 = root).
/// `field_count` accumulates the total number of fields seen so far across the whole tree.
fn validate_doc_recursive(doc: &Document, depth: u32, field_count: &mut usize) -> Result<()> {
    if depth > MAX_NESTING_DEPTH {
        return Err(Error::DocumentValidationFailure {
            detail: format!(
                "maximum nesting depth ({MAX_NESTING_DEPTH}) exceeded"
            ),
        });
    }

    for (key, value) in doc.iter() {
        // Field name validation
        validate_field_name(key)?;

        // Running field count
        *field_count += 1;
        if *field_count > MAX_FIELD_COUNT {
            return Err(Error::DocumentValidationFailure {
                detail: format!(
                    "field count exceeds maximum ({MAX_FIELD_COUNT})"
                ),
            });
        }

        // Recurse into nested structures
        validate_value_recursive(value, depth + 1, field_count)?;
    }

    Ok(())
}

/// Recursively validate a [`Bson`] value, descending into nested documents and arrays.
fn validate_value_recursive(value: &Bson, depth: u32, field_count: &mut usize) -> Result<()> {
    match value {
        Bson::Document(nested) => {
            validate_doc_recursive(nested, depth, field_count)?;
        }
        Bson::Array(arr) => {
            for elem in arr.iter() {
                validate_value_recursive(elem, depth + 1, field_count)?;
            }
        }
        // All other BSON types are leaf nodes — no further recursion needed.
        _ => {}
    }
    Ok(())
}

/// Validate a single BSON field name.
///
/// Rules:
/// - Length must not exceed [`MAX_FIELD_NAME_LEN`] bytes.
/// - Must not contain null bytes (`\0`), which are forbidden by the BSON spec
///   and can corrupt index keys.
fn validate_field_name(name: &str) -> Result<()> {
    if name.len() > MAX_FIELD_NAME_LEN {
        return Err(Error::DocumentValidationFailure {
            detail: "field name too long or contains null byte".into(),
        });
    }
    if name.contains('\0') {
        return Err(Error::DocumentValidationFailure {
            detail: "field name too long or contains null byte".into(),
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use bson::{doc, Bson, Document};

    use crate::error::Error;

    use super::*;

    // ---- Size limit --------------------------------------------------------
    //
    // We use Bson::String values rather than Bson::Binary to avoid triggering the
    // bson crate's own binary-length limit (~16MB) which would return a
    // BsonSerialization error before our DocumentTooLarge check fires.
    //
    // BSON string field overhead (key "d"):
    //   4 (doc size) + 1 (type) + 1 ("d") + 1 (null) + 4 (str len) + content + 1 (null) + 1 (doc term)
    //   = content + 13 bytes
    //
    // So content = MAX_DOCUMENT_SIZE - 13 produces exactly MAX_DOCUMENT_SIZE bytes serialized.

    /// A document just at the 16MB serialized-size limit must pass.
    #[test]
    fn document_at_size_limit_passes() {
        // content = MAX_DOCUMENT_SIZE - 13 → serialized = MAX_DOCUMENT_SIZE exactly.
        let content_len = MAX_DOCUMENT_SIZE - 13;
        let big_doc = doc! { "d": "x".repeat(content_len) };
        assert!(validate_document(&big_doc).is_ok(), "document at limit should pass");
    }

    /// A document that exceeds 16MB must be rejected with `DocumentTooLarge`.
    #[test]
    fn document_exceeds_size_limit_fails() {
        // content = MAX_DOCUMENT_SIZE - 12 → serialized = MAX_DOCUMENT_SIZE + 1 bytes.
        let content_len = MAX_DOCUMENT_SIZE - 12;
        let big_doc = doc! { "d": "x".repeat(content_len) };
        let err = validate_document(&big_doc).unwrap_err();
        assert!(
            matches!(err, Error::DocumentTooLarge { .. }),
            "expected DocumentTooLarge, got: {err:?}"
        );
    }

    // ---- Nesting depth -----------------------------------------------------

    /// A document nested exactly at the limit (100 levels) must pass.
    ///
    /// The root document is depth 0; after 100 levels of nesting, the leaf
    /// is at depth 100 which equals MAX_NESTING_DEPTH exactly — this triggers
    /// the `>` check only at depth 101, so depth 100 should pass.
    #[test]
    fn document_at_nesting_limit_passes() {
        let doc = build_nested_doc(MAX_NESTING_DEPTH as usize);
        assert!(validate_document(&doc).is_ok(), "nesting at limit should pass");
    }

    /// A document nested 101 levels deep must fail with `DocumentValidationFailure`.
    #[test]
    fn document_exceeds_nesting_limit_fails() {
        let doc = build_nested_doc(MAX_NESTING_DEPTH as usize + 1);
        let err = validate_document(&doc).unwrap_err();
        assert!(
            matches!(err, Error::DocumentValidationFailure { ref detail } if detail.contains("nesting depth")),
            "expected DocumentValidationFailure (nesting), got: {err:?}"
        );
    }

    // ---- Field count -------------------------------------------------------

    /// A document with exactly 10,000 fields must pass.
    #[test]
    fn document_at_field_count_limit_passes() {
        let mut doc = Document::new();
        for i in 0..MAX_FIELD_COUNT {
            doc.insert(format!("f{i}"), Bson::Int32(0));
        }
        assert!(validate_document(&doc).is_ok(), "10000 fields should pass");
    }

    /// A document with 10,001 fields must fail with `DocumentValidationFailure`.
    #[test]
    fn document_exceeds_field_count_fails() {
        let mut doc = Document::new();
        for i in 0..=MAX_FIELD_COUNT {
            doc.insert(format!("f{i}"), Bson::Int32(0));
        }
        let err = validate_document(&doc).unwrap_err();
        assert!(
            matches!(err, Error::DocumentValidationFailure { ref detail } if detail.contains("field count")),
            "expected DocumentValidationFailure (field count), got: {err:?}"
        );
    }

    // ---- Field name length -------------------------------------------------

    /// A field name at the 1,024-byte limit must pass.
    #[test]
    fn field_name_at_limit_passes() {
        let name = "a".repeat(MAX_FIELD_NAME_LEN);
        let doc = doc! { name: 1 };
        assert!(validate_document(&doc).is_ok(), "field name at limit should pass");
    }

    /// A field name of 1,025 bytes must fail.
    #[test]
    fn field_name_too_long_fails() {
        let name = "a".repeat(MAX_FIELD_NAME_LEN + 1);
        let doc = doc! { name: 1 };
        let err = validate_document(&doc).unwrap_err();
        assert!(
            matches!(err, Error::DocumentValidationFailure { ref detail } if detail.contains("field name")),
            "expected DocumentValidationFailure (field name), got: {err:?}"
        );
    }

    // ---- Null bytes in field names -----------------------------------------

    /// A field name containing a null byte must fail.
    #[test]
    fn field_name_with_null_byte_fails() {
        let mut doc = Document::new();
        doc.insert("key\0embedded", Bson::Int32(1));
        let err = validate_document(&doc).unwrap_err();
        assert!(
            matches!(err, Error::DocumentValidationFailure { ref detail } if detail.contains("null byte")),
            "expected DocumentValidationFailure (null byte), got: {err:?}"
        );
    }

    /// A field name that is just a null byte must fail.
    #[test]
    fn field_name_only_null_byte_fails() {
        let mut doc = Document::new();
        doc.insert("\0", Bson::Int32(1));
        let err = validate_document(&doc).unwrap_err();
        assert!(
            matches!(err, Error::DocumentValidationFailure { ref detail } if detail.contains("null byte")),
            "expected DocumentValidationFailure (null byte), got: {err:?}"
        );
    }

    // ---- Error codes -------------------------------------------------------

    /// `DocumentTooLarge` should return MongoDB error code 10334.
    #[test]
    fn document_too_large_has_correct_code() {
        // content = MAX_DOCUMENT_SIZE - 12 → serialized size = MAX_DOCUMENT_SIZE + 1 bytes.
        let content_len = MAX_DOCUMENT_SIZE - 12;
        let big_doc = doc! { "d": "x".repeat(content_len) };
        let err = validate_document(&big_doc).unwrap_err();
        assert_eq!(err.code(), Some(10334));
    }

    /// `DocumentValidationFailure` should return MongoDB error code 121.
    #[test]
    fn document_validation_failure_has_correct_code() {
        let mut doc = Document::new();
        doc.insert("\0", Bson::Int32(1));
        let err = validate_document(&doc).unwrap_err();
        assert_eq!(err.code(), Some(121));
    }

    // ---- Helpers -----------------------------------------------------------

    /// Build a document nested `depth` levels deep: `{ "n": { "n": { ... } } }`.
    ///
    /// At depth 0 the document is `{ "n": 1 }` (a leaf).
    /// At depth 1 it is `{ "n": { "n": 1 } }`, etc.
    fn build_nested_doc(depth: usize) -> Document {
        if depth == 0 {
            return doc! { "n": 1i32 };
        }
        let inner = build_nested_doc(depth - 1);
        doc! { "n": Bson::Document(inner) }
    }
}
