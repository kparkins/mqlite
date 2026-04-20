// ---------------------------------------------------------------------------
// Error helpers for wire command handlers
// ---------------------------------------------------------------------------

use bson::{doc, Document};

/// Build a `BadValue` (code 2) error response document.
pub(super) fn err_bad_value(msg: impl Into<String>) -> Document {
    doc! {
        "ok": 0.0_f64,
        "errmsg": msg.into(),
        "code": crate::error::codes::BAD_VALUE,
        "codeName": "BadValue",
    }
}

/// Build a `collation not supported` error response (code 2, BadValue).
pub(super) fn err_collation_unsupported() -> Document {
    err_bad_value("collation is not supported in this version of mqlite")
}

/// Convert a mqlite `Error` into a top-level command error document.
pub(super) fn err_from_mqlite(e: crate::error::Error) -> Document {
    let code = e.code().unwrap_or(crate::error::codes::INTERNAL_ERROR);
    doc! {
        "ok": 0.0_f64,
        "errmsg": e.to_string(),
        "code": code,
        "codeName": mqlite_code_name(code),
    }
}

/// Map a MongoDB error code to its canonical `codeName` string.
pub(super) fn mqlite_code_name(code: i32) -> &'static str {
    match code {
        crate::error::codes::DUPLICATE_KEY => "DuplicateKey",
        crate::error::codes::NAMESPACE_NOT_FOUND => "NamespaceNotFound",
        crate::error::codes::CURSOR_NOT_FOUND => "CursorNotFound",
        crate::error::codes::BAD_VALUE => "BadValue",
        crate::error::codes::UNSUPPORTED_OPERATOR => "FailedToParse",
        crate::error::codes::CANNOT_CREATE_INDEX => "CannotCreateIndex",
        _ => "InternalError",
    }
}

/// Convert a mqlite `Error` to a write-error `(code, message)` pair for
/// embedding inside a `writeErrors` array.
pub(super) fn write_err_from_mqlite(e: &crate::error::Error) -> (i32, String) {
    let code = e.code().unwrap_or(crate::error::codes::INTERNAL_ERROR);
    (code, e.to_string())
}
