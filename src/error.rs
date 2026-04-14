//! Error types for mqlite operations.
//!
//! # Error Taxonomy
//!
//! mqlite errors fall into two categories:
//!
//! ## MongoDB-Compatible Errors
//!
//! These map directly to MongoDB error codes and are returned in the same
//! scenarios as the corresponding MongoDB server errors. Wire-protocol clients
//! receive the numeric code and code name in the `{ ok: 0, code: N, codeName: "…" }`
//! response document.
//!
//! | Code  | Name                        | Trigger                                              |
//! |-------|-----------------------------|------------------------------------------------------|
//! | 11000 | DuplicateKey                | Unique-index key violation on insert or update       |
//! | 121   | DocumentValidationFailure   | Document rejected by collection validator            |
//! | 27    | IndexNotFound               | Named index does not exist                           |
//! | 26    | NamespaceNotFound           | Collection (namespace) does not exist                |
//! | 48    | NamespaceExists             | Collection (namespace) already exists                |
//! | 59    | CommandNotFound             | Unknown or unsupported command                       |
//! | 22    | InvalidBSON                 | Malformed BSON data                                  |
//! | 10334 | BSONObjectTooLarge          | Document exceeds 16 MiB BSON size limit              |
//! | 1     | InternalError               | Unexpected internal state                            |
//!
//! ## mqlite-Specific Errors
//!
//! These have no MongoDB equivalent and carry richer diagnostic information
//! tuned for the embedded-database use case.
//!
//! | Variant               | Description                                                |
//! |-----------------------|------------------------------------------------------------|
//! | WriterBusy            | Single-writer lock is held by another operation            |
//! | CorruptDatabase       | On-disk data is structurally invalid                       |
//! | DiskFull              | Write failed because the filesystem has no space left      |
//! | UnsupportedOperator   | MQL query operator not implemented in this version         |
//! | UnsupportedIndexOption| Index option not implemented in this version               |
//! | Io                    | OS-level I/O error                                         |
//! | BsonSerialization     | Rust-to-BSON serialization failed                          |
//! | BsonDeserialization   | BSON-to-Rust deserialization failed                        |

use std::time::Duration;
use thiserror::Error;

/// MongoDB-compatible numeric error codes.
///
/// All codes match MongoDB 8.0. Wire-protocol clients receive these codes in
/// the `code` field of the server error response document.
pub mod codes {
    /// Unexpected internal state (MongoDB generic fallback).
    pub const INTERNAL_ERROR: i32 = 1;

    /// Malformed BSON data.
    pub const INVALID_BSON: i32 = 22;

    /// Collection (namespace) does not exist.
    pub const NAMESPACE_NOT_FOUND: i32 = 26;

    /// Named index does not exist.
    pub const INDEX_NOT_FOUND: i32 = 27;

    /// Cursor expired or was already closed.
    pub const CURSOR_NOT_FOUND: i32 = 43;

    /// Collection (namespace) already exists.
    pub const NAMESPACE_EXISTS: i32 = 48;

    /// Unknown or unsupported command.
    pub const COMMAND_NOT_FOUND: i32 = 59;

    /// Document rejected by collection validator.
    pub const DOCUMENT_VALIDATION_FAILURE: i32 = 121;

    /// Document exceeds 16 MiB BSON size limit.
    pub const DOCUMENT_TOO_LARGE: i32 = 10334;

    /// Unique-index key violation.
    pub const DUPLICATE_KEY: i32 = 11000;
}

/// The primary error type for mqlite operations.
///
/// Implements [`std::error::Error`] via [`thiserror`]. Every variant produces
/// a human-readable, actionable message. MongoDB-compatible variants additionally
/// expose a numeric code via [`Error::code`] and a code name via
/// [`Error::code_name`], enabling wire-protocol serialisation.
///
/// # Example
///
/// ```
/// use mqlite::{Error, Result};
///
/// fn check(e: &Error) {
///     if let Some(code) = e.code() {
///         eprintln!("MongoDB error code {}: {}", code, e.code_name().unwrap_or("—"));
///     }
/// }
/// ```
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    // ── MongoDB-compatible variants ──────────────────────────────────────────────
    //
    // Ordered by ascending error code for easy cross-reference with the table
    // in the module-level doc comment.

    /// Malformed or structurally invalid BSON data. (MongoDB error code 22)
    ///
    /// Returned when raw bytes cannot be parsed as valid BSON, when a document
    /// contains invalid field names (e.g. keys with embedded NUL bytes), or when
    /// the encoded size marker does not match the actual byte length.
    #[error(
        "InvalidBSON: {detail}\n\
         Ensure the BSON document is well-formed and within the 16 MiB size limit.\n\
         Tip: use bson::to_document / bson::from_document for round-trip safety."
    )]
    InvalidBson {
        /// Human-readable description of what was invalid.
        detail: String,
    },

    /// The collection (namespace) does not exist. (MongoDB error code 26)
    ///
    /// Equivalent to MongoDB's `NamespaceNotFound`. Raised when a collection
    /// is referenced by name but has not been created yet.
    #[error(
        "NamespaceNotFound: collection \"{ns}\" does not exist.\n\
         Use db.create_collection(\"{ns}\") to create it explicitly, or insert \
         a document to create it implicitly.\n\
         Use db.list_collection_names() to enumerate existing collections."
    )]
    NamespaceNotFound {
        /// Fully-qualified namespace (e.g. `"mydb.users"`), or bare collection name.
        ns: String,
    },

    /// The named index does not exist on the collection. (MongoDB error code 27)
    ///
    /// Raised when `drop_index` or `create_index` (in replace mode) targets an
    /// index that has not been created.
    #[error(
        "IndexNotFound: index \"{name}\" does not exist.\n\
         Use collection.list_indexes() to see all indexes on the collection.\n\
         Indexes are identified by their name, not their key specification."
    )]
    IndexNotFound {
        /// The index name that was not found.
        name: String,
    },

    /// Cursor expired or was already exhausted / closed. (MongoDB error code 43)
    ///
    /// Cursors are single-use in mqlite. A cursor that has been fully iterated
    /// or explicitly closed cannot be used again.
    #[error(
        "CursorNotFound: cursor id {id} not found or already closed.\n\
         Cursors are single-use and are invalidated after full iteration or explicit close.\n\
         Create a new cursor by calling find() again."
    )]
    CursorNotFound {
        /// The cursor identifier that was not found.
        id: i64,
    },

    /// The collection (namespace) already exists. (MongoDB error code 48)
    ///
    /// Returned by `create_collection` when the collection was previously created.
    #[error(
        "NamespaceExists: collection \"{ns}\" already exists.\n\
         Use db.collection(\"{ns}\") to access the existing collection.\n\
         Pass `CreateCollectionOptions::if_not_exists(true)` to suppress this error."
    )]
    NamespaceExists {
        /// The namespace (collection name) that already exists.
        ns: String,
    },

    /// The command is not recognised by mqlite. (MongoDB error code 59)
    ///
    /// Wire-protocol clients receive this when they send a command that mqlite
    /// does not implement. See the compatibility matrix in the crate docs.
    #[error(
        "CommandNotFound: no such command \"{command}\".\n\
         mqlite implements a subset of MongoDB commands.\n\
         Supported commands: find, insert, update, delete, aggregate (basic),\n\
         \t\t\tcreateIndexes, dropIndexes, listIndexes,\n\
         \t\t\tcreate (collection), drop, listCollections, ping.\n\
         See: https://docs.rs/mqlite/latest/mqlite/compatibility"
    )]
    CommandNotFound {
        /// The unrecognised command name (e.g. `"mapReduce"`).
        command: String,
    },

    /// Document rejected by the collection's validator. (MongoDB error code 121)
    ///
    /// Raised when a document being inserted or updated fails the JSON Schema
    /// or query-expression validator attached to the collection.
    #[error(
        "DocumentValidationFailure: {detail}\n\
         Check the collection's validator expression or remove invalid fields.\n\
         Use db.get_collection_info() to inspect the current validator."
    )]
    DocumentValidationFailure {
        /// Human-readable description of the validation failure.
        detail: String,
    },

    /// Document exceeds the 16 MiB BSON size limit. (MongoDB error code 10334)
    ///
    /// MongoDB's BSON specification caps documents at 16,777,216 bytes. mqlite
    /// enforces the same limit for compatibility.
    #[error(
        "BSONObjectTooLarge: document size {size} bytes exceeds maximum {max} bytes.\n\
         Split large documents into smaller sub-documents, or store bulk binary \
         data in a separate collection / external object store."
    )]
    DocumentTooLarge {
        /// Actual encoded size of the document in bytes.
        size: usize,
        /// Maximum allowed size in bytes (16,777,216 for MongoDB compatibility).
        max: usize,
    },

    /// Unique-index constraint violation. (MongoDB error code 11000)
    ///
    /// A document was inserted or an existing document was updated such that two
    /// documents in the same collection would share the same value for a field
    /// (or compound field tuple) covered by a unique index.
    #[error(
        "E11000 DuplicateKey: collection {collection} — index key \"{key}\" already exists.\n\
         Ensure the document's key field is unique before inserting.\n\
         Use update with upsert, or query before inserting to avoid this error."
    )]
    DuplicateKey {
        /// The collection where the violation occurred.
        collection: String,
        /// The index key expression that was violated (e.g. `"_id_"` or `"email_1"`).
        key: String,
    },

    // ── mqlite-specific variants ─────────────────────────────────────────────────

    /// The single-writer lock is held by another in-flight write operation.
    ///
    /// mqlite uses a single-writer model: at most one write transaction may be
    /// active at a time. This error is returned when a second write is attempted
    /// while the first is still in progress and the configured `busy_timeout` has
    /// elapsed (or was set to `Duration::ZERO` for immediate failure).
    #[error(
        "WriterBusy: the writer lock has been held for {held_for:?}.\n\
         mqlite uses a single-writer model — only one write operation can execute at a time.\n\
         To resolve:\n\
         \t• Ensure the previous write completed (await the result) before starting a new one.\n\
         \t• Serialize concurrent writes through a channel, mutex, or task queue.\n\
         \t• Increase the busy timeout: OpenOptions::new().busy_timeout(Duration::from_secs(10))\n\
         \t• Provide a retry callback: OpenOptions::new().busy_handler(|attempts| attempts < 5)"
    )]
    WriterBusy {
        /// How long the writer lock has been held by the incumbent operation.
        held_for: Duration,
    },

    /// The database file is corrupt or structurally invalid.
    ///
    /// Raised when the storage engine detects an integrity violation that prevents
    /// normal operation: a truncated file header, a page checksum mismatch, a
    /// WAL segment with an invalid sequence number, etc.
    ///
    /// When `recoverable` is `true`, the last successfully committed checkpoint
    /// is intact and the database can be opened in read-only mode.
    ///
    /// When `recoverable` is `false`, structural damage extends to committed data
    /// and the database should be restored from a backup.
    #[error(
        "CorruptDatabase at {path:?}: {detail}\n\
         Recoverable: {recoverable}\n\
         Recovery options:\n\
         \t• If recoverable=true: open with OpenOptions::new().read_only(true) to \
         access the last successfully checkpointed state.\n\
         \t• If recoverable=false: restore from a backup. Database::repair() is \
         planned for Phase 2.\n\
         \t• In either case, file a bug report with the error detail and database path."
    )]
    CorruptDatabase {
        /// Filesystem path to the database file.
        path: std::path::PathBuf,
        /// Human-readable description of the corruption.
        detail: String,
        /// If `true`, the last checkpoint is intact and read-only access is possible.
        recoverable: bool,
    },

    /// A write could not be completed because the filesystem has no space left.
    ///
    /// mqlite writes atomically via WAL: data is written to the WAL segment first,
    /// then checkpointed. A `DiskFull` error means the WAL write failed; any
    /// previously committed data remains intact.
    #[error(
        "DiskFull at {path:?}: required {required_bytes} bytes, \
         only {available_bytes} bytes available.\n\
         Previously committed data is intact — only the current write was lost.\n\
         To resolve:\n\
         \t• Free disk space (remove old backups, compact WAL with checkpoint()).\n\
         \t• Move the database file to a volume with more capacity.\n\
         \t• Reduce document sizes or implement a data-expiry policy."
    )]
    DiskFull {
        /// Filesystem path where the write was attempted.
        path: std::path::PathBuf,
        /// Number of bytes required by the failed write.
        required_bytes: u64,
        /// Number of bytes available on the filesystem at the time of failure.
        available_bytes: u64,
    },

    /// An MQL query operator is not supported by mqlite.
    ///
    /// Raised when a filter document contains a `$`-prefixed operator that
    /// mqlite's query engine does not implement.
    #[error(
        "UnsupportedOperator(\"{operator}\"): {suggestion}\n\
         Phase 1 supported operators:\n\
         \t• Comparison:  $eq, $gt, $gte, $lt, $lte, $ne, $in, $nin\n\
         \t• Logical:     $and, $or, $not, $nor\n\
         \t• Element:     $exists, $type\n\
         \t• Array:       $all, $elemMatch\n\
         \t• String:      $regex\n\
         See: https://docs.rs/mqlite/latest/mqlite/compatibility"
    )]
    UnsupportedOperator {
        /// The operator name, including the leading `$` (e.g. `"$where"`).
        operator: String,
        /// A concrete suggestion for an alternative approach, or empty string if none.
        suggestion: String,
    },

    /// An index option is not supported by mqlite.
    ///
    /// Raised when `create_index` receives an [`IndexOptions`] field that
    /// mqlite does not implement in this version.
    ///
    /// [`IndexOptions`]: crate::options::IndexOptions
    #[error(
        "UnsupportedIndexOption(\"{option}\"): {suggestion}\n\
         Phase 1 supported index options: unique, sparse, name.\n\
         See: https://docs.rs/mqlite/latest/mqlite/compatibility"
    )]
    UnsupportedIndexOption {
        /// The option name (e.g. `"expireAfterSeconds"`, `"weights"`).
        option: String,
        /// A concrete suggestion for an alternative approach, or empty string if none.
        suggestion: String,
    },

    /// An OS-level I/O error.
    ///
    /// Transparent wrapper around [`std::io::Error`]. Raised for file open
    /// failures, permission errors, read/write errors, and similar OS-level faults.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// BSON serialization of a Rust value failed.
    ///
    /// Transparent wrapper around [`bson::ser::Error`]. Raised when a value
    /// implementing [`serde::Serialize`] cannot be converted to a BSON document
    /// (e.g. a map with non-string keys, an integer too large for BSON's i64).
    #[error("BSON serialization error: {0}")]
    BsonSerialization(#[from] bson::ser::Error),

    /// BSON deserialization of bytes into a Rust value failed.
    ///
    /// Transparent wrapper around [`bson::de::Error`]. Raised when a stored
    /// BSON document cannot be deserialised into the expected Rust type (e.g.
    /// a type mismatch or missing required field).
    #[error("BSON deserialization error: {0}")]
    BsonDeserialization(#[from] bson::de::Error),

    /// An internal invariant was violated.
    ///
    /// This variant should never be triggered by correct usage of the public API.
    /// If you see it, it is a bug in mqlite — please file an issue including the
    /// full error message and the code path that produced it.
    #[error("Internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Return the MongoDB-compatible numeric error code for this error, if one exists.
    ///
    /// Returns `Some(code)` for MongoDB-compatible error variants, and `None` for
    /// mqlite-specific errors that have no MongoDB equivalent.
    ///
    /// Wire-protocol clients receive this value in the `code` field of the error
    /// response document.
    ///
    /// # Example
    ///
    /// ```
    /// use mqlite::Error;
    ///
    /// let e = Error::DuplicateKey {
    ///     collection: "users".into(),
    ///     key: "email_1".into(),
    /// };
    /// assert_eq!(e.code(), Some(11000));
    /// ```
    pub fn code(&self) -> Option<i32> {
        match self {
            Error::InvalidBson { .. } => Some(codes::INVALID_BSON),
            Error::NamespaceNotFound { .. } => Some(codes::NAMESPACE_NOT_FOUND),
            Error::IndexNotFound { .. } => Some(codes::INDEX_NOT_FOUND),
            Error::CursorNotFound { .. } => Some(codes::CURSOR_NOT_FOUND),
            Error::NamespaceExists { .. } => Some(codes::NAMESPACE_EXISTS),
            Error::CommandNotFound { .. } => Some(codes::COMMAND_NOT_FOUND),
            Error::DocumentValidationFailure { .. } => Some(codes::DOCUMENT_VALIDATION_FAILURE),
            Error::DocumentTooLarge { .. } => Some(codes::DOCUMENT_TOO_LARGE),
            Error::DuplicateKey { .. } => Some(codes::DUPLICATE_KEY),
            Error::Internal(_) => Some(codes::INTERNAL_ERROR),
            // mqlite-specific errors — no MongoDB code
            Error::WriterBusy { .. }
            | Error::CorruptDatabase { .. }
            | Error::DiskFull { .. }
            | Error::UnsupportedOperator { .. }
            | Error::UnsupportedIndexOption { .. }
            | Error::Io(_)
            | Error::BsonSerialization(_)
            | Error::BsonDeserialization(_) => None,
        }
    }

    /// Return the MongoDB-compatible error code name for this error, if one exists.
    ///
    /// Returns `Some(name)` for MongoDB-compatible error variants, and `None` for
    /// mqlite-specific errors that have no MongoDB equivalent.
    ///
    /// Wire-protocol clients receive this value in the `codeName` field of the
    /// error response document.
    ///
    /// # Example
    ///
    /// ```
    /// use mqlite::Error;
    ///
    /// let e = Error::DuplicateKey {
    ///     collection: "users".into(),
    ///     key: "email_1".into(),
    /// };
    /// assert_eq!(e.code_name(), Some("DuplicateKey"));
    /// ```
    pub fn code_name(&self) -> Option<&'static str> {
        match self {
            Error::InvalidBson { .. } => Some("InvalidBSON"),
            Error::NamespaceNotFound { .. } => Some("NamespaceNotFound"),
            Error::IndexNotFound { .. } => Some("IndexNotFound"),
            Error::CursorNotFound { .. } => Some("CursorNotFound"),
            Error::NamespaceExists { .. } => Some("NamespaceExists"),
            Error::CommandNotFound { .. } => Some("CommandNotFound"),
            Error::DocumentValidationFailure { .. } => Some("DocumentValidationFailure"),
            Error::DocumentTooLarge { .. } => Some("BSONObjectTooLarge"),
            Error::DuplicateKey { .. } => Some("DuplicateKey"),
            Error::Internal(_) => Some("InternalError"),
            // mqlite-specific errors — no MongoDB code name
            Error::WriterBusy { .. }
            | Error::CorruptDatabase { .. }
            | Error::DiskFull { .. }
            | Error::UnsupportedOperator { .. }
            | Error::UnsupportedIndexOption { .. }
            | Error::Io(_)
            | Error::BsonSerialization(_)
            | Error::BsonDeserialization(_) => None,
        }
    }
}

/// Convenience `Result` type alias for mqlite operations.
///
/// All public API functions return `Result<T>` which expands to
/// `std::result::Result<T, mqlite::Error>`.
///
/// # Example
///
/// ```no_run
/// fn do_something() -> mqlite::Result<()> {
///     Ok(())
/// }
/// ```
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_key_code_and_name() {
        let e = Error::DuplicateKey {
            collection: "users".into(),
            key: "email_1".into(),
        };
        assert_eq!(e.code(), Some(11000));
        assert_eq!(e.code_name(), Some("DuplicateKey"));
    }

    #[test]
    fn document_validation_failure_code() {
        let e = Error::DocumentValidationFailure {
            detail: "field 'age' must be >= 0".into(),
        };
        assert_eq!(e.code(), Some(121));
        assert_eq!(e.code_name(), Some("DocumentValidationFailure"));
    }

    #[test]
    fn index_not_found_code() {
        let e = Error::IndexNotFound {
            name: "email_1".into(),
        };
        assert_eq!(e.code(), Some(27));
        assert_eq!(e.code_name(), Some("IndexNotFound"));
    }

    #[test]
    fn namespace_not_found_code() {
        let e = Error::NamespaceNotFound {
            ns: "mydb.users".into(),
        };
        assert_eq!(e.code(), Some(26));
        assert_eq!(e.code_name(), Some("NamespaceNotFound"));
    }

    #[test]
    fn namespace_exists_code() {
        let e = Error::NamespaceExists {
            ns: "mydb.users".into(),
        };
        assert_eq!(e.code(), Some(48));
        assert_eq!(e.code_name(), Some("NamespaceExists"));
    }

    #[test]
    fn command_not_found_code() {
        let e = Error::CommandNotFound {
            command: "mapReduce".into(),
        };
        assert_eq!(e.code(), Some(59));
        assert_eq!(e.code_name(), Some("CommandNotFound"));
    }

    #[test]
    fn invalid_bson_code() {
        let e = Error::InvalidBson {
            detail: "unexpected end of input".into(),
        };
        assert_eq!(e.code(), Some(22));
        assert_eq!(e.code_name(), Some("InvalidBSON"));
    }

    #[test]
    fn document_too_large_code() {
        let e = Error::DocumentTooLarge {
            size: 20_000_000,
            max: 16_777_216,
        };
        assert_eq!(e.code(), Some(10334));
        assert_eq!(e.code_name(), Some("BSONObjectTooLarge"));
    }

    #[test]
    fn internal_error_code() {
        let e = Error::Internal("something went wrong".into());
        assert_eq!(e.code(), Some(1));
        assert_eq!(e.code_name(), Some("InternalError"));
    }

    #[test]
    fn mqlite_specific_errors_have_no_code() {
        let errors: &[Error] = &[
            Error::WriterBusy {
                held_for: Duration::from_millis(500),
            },
            Error::CorruptDatabase {
                path: "/tmp/db.mqlite".into(),
                detail: "checksum mismatch on page 3".into(),
                recoverable: true,
            },
            Error::DiskFull {
                path: "/tmp/db.mqlite".into(),
                required_bytes: 4096,
                available_bytes: 0,
            },
            Error::UnsupportedOperator {
                operator: "$where".into(),
                suggestion: "use $expr with supported operators instead".into(),
            },
            Error::UnsupportedIndexOption {
                option: "expireAfterSeconds".into(),
                suggestion: "implement TTL expiry in application logic".into(),
            },
        ];
        for e in errors {
            assert_eq!(e.code(), None, "expected no code for {:?}", e);
            assert_eq!(e.code_name(), None, "expected no code_name for {:?}", e);
        }
    }

    #[test]
    fn writer_busy_message_contains_duration() {
        let e = Error::WriterBusy {
            held_for: Duration::from_secs(3),
        };
        let msg = e.to_string();
        assert!(
            msg.contains("3s") || msg.contains("3000ms"),
            "expected duration in message, got: {msg}"
        );
        assert!(msg.contains("single-writer"), "expected single-writer hint in: {msg}");
    }

    #[test]
    fn corrupt_database_recoverable_flag_in_message() {
        let e = Error::CorruptDatabase {
            path: "/data/mydb.mqlite".into(),
            detail: "WAL sequence gap".into(),
            recoverable: false,
        };
        let msg = e.to_string();
        assert!(msg.contains("false"), "recoverable flag should appear in message: {msg}");
        assert!(msg.contains("backup"), "recovery hint should mention backup: {msg}");
    }

    #[test]
    fn disk_full_message_contains_byte_counts() {
        let e = Error::DiskFull {
            path: "/data/mydb.mqlite".into(),
            required_bytes: 8192,
            available_bytes: 512,
        };
        let msg = e.to_string();
        assert!(msg.contains("8192"), "required bytes should appear in message: {msg}");
        assert!(msg.contains("512"), "available bytes should appear in message: {msg}");
    }

    #[test]
    fn unsupported_operator_message_contains_suggestion() {
        let e = Error::UnsupportedOperator {
            operator: "$where".into(),
            suggestion: "use $expr instead".into(),
        };
        let msg = e.to_string();
        assert!(msg.contains("$where"), "operator should appear in message: {msg}");
        assert!(msg.contains("$expr"), "suggestion should appear in message: {msg}");
    }

    #[test]
    fn result_type_alias_compiles() {
        fn ok() -> Result<i32> {
            Ok(42)
        }
        fn err() -> Result<i32> {
            Err(Error::Internal("test".into()))
        }
        assert_eq!(ok().unwrap(), 42);
        assert!(err().is_err());
    }
}
