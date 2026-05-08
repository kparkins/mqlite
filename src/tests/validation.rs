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
    assert!(
        validate_document(&big_doc).is_ok(),
        "document at limit should pass"
    );
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
    assert!(
        validate_document(&doc).is_ok(),
        "nesting at limit should pass"
    );
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
    assert!(
        validate_document(&doc).is_ok(),
        "field name at limit should pass"
    );
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
