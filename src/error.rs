use thiserror::Error;

/// MongoDB-compatible error codes.
pub mod codes {
    /// Received a wire protocol message that is malformed or not supported.
    pub const ILLEGAL_OP: i32 = 48;
    /// Generic internal server error.
    pub const INTERNAL_ERROR: i32 = 1;
    /// A provided value is invalid for the given field or operation.
    pub const BAD_VALUE: i32 = 2;
    /// A write violated a unique index constraint.
    pub const DUPLICATE_KEY: i32 = 11000;
    /// The referenced cursor has expired or does not exist.
    pub const CURSOR_NOT_FOUND: i32 = 43;
    /// The referenced collection or database does not exist.
    pub const NAMESPACE_NOT_FOUND: i32 = 26;
    /// The database file format is not supported.
    pub const UNSUPPORTED_FORMAT: i32 = 115;
    /// An MQL operator is not supported by mqlite.
    /// Matches MongoDB error code 9 (FailedToParse / unknown operator).
    pub const UNSUPPORTED_OPERATOR: i32 = 9;
    /// An index type or option is not supported.
    /// Matches MongoDB error code 67 (CannotCreateIndex).
    pub const CANNOT_CREATE_INDEX: i32 = 67;
    /// A document failed schema validation or structural constraints.
    /// Matches MongoDB error code 121.
    pub const DOCUMENT_VALIDATION_FAILURE: i32 = 121;
    /// A document exceeds the maximum allowed size (16MB).
    /// Matches MongoDB error code 10334.
    pub const DOCUMENT_TOO_LARGE: i32 = 10334;
}

/// The primary error type for mqlite operations.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// An I/O error occurred at the OS level.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// BSON serialization failed.
    #[error("BSON serialization error: {0}")]
    BsonSerialization(#[from] bson::ser::Error),

    /// BSON deserialization failed.
    #[error("BSON deserialization error: {0}")]
    BsonDeserialization(#[from] bson::de::Error),

    /// Another write is in progress. Writer lock is contended.
    #[error(
        "WriterBusy — another write operation is in progress.\n\
         The database uses a single-writer model. Only one write operation \
         can execute at a time.\n\
         To resolve:\n  \
         - Ensure previous write completed before starting a new one\n  \
         - Serialize writes through a channel or mutex\n  \
         - Configure a busy timeout: OpenOptions::new().busy_timeout(Duration::from_secs(5))"
    )]
    WriterBusy,

    /// An MQL operator is not supported by mqlite.
    #[error(
        "UnsupportedOperator(\"{operator}\") — this operator is not supported in mqlite.\n\
         Phase 1 supports: $eq, $gt, $gte, $lt, $lte, $ne, $in, $nin,\n\
         \t\t\t$and, $or, $not, $nor, $exists, $type,\n\
         \t\t\t$all, $elemMatch, $regex\n\
         See: https://docs.rs/mqlite/latest/mqlite/compatibility"
    )]
    UnsupportedOperator {
        /// The name of the unsupported operator (e.g. `"$where"`).
        operator: String,
    },

    /// A command is not supported by mqlite's wire protocol shim.
    #[error("Unsupported command: {command}")]
    UnsupportedCommand {
        /// The name of the unsupported command (e.g. `"aggregate"`).
        command: String,
    },

    /// The database file is corrupt or structurally invalid.
    #[error(
        "CorruptDatabase at {path:?}: {detail}\n\
         Recoverable: {recoverable}\n\
         Note: Database::repair() is planned for Phase 2. In Phase 1, restore from a backup \
         or open in read_only mode to access the last successfully checkpointed state."
    )]
    CorruptDatabase {
        /// Path to the corrupt database file.
        path: std::path::PathBuf,
        /// Human-readable description of the corruption.
        detail: String,
        /// Whether the database can be partially recovered.
        recoverable: bool,
    },

    /// The disk is full; the write could not be completed.
    #[error(
        "DiskFull at {path:?}: required {required_bytes} bytes, \
         only {available_bytes} available.\n\
         {suggestion}"
    )]
    DiskFull {
        /// Path to the database file.
        path: std::path::PathBuf,
        /// Number of bytes required for the failed write.
        required_bytes: u64,
        /// Number of bytes available on the device.
        available_bytes: u64,
        /// Suggested remediation steps.
        suggestion: String,
    },

    /// Duplicate key violation (MongoDB error code 11000).
    #[error("Duplicate key error: {detail}")]
    DuplicateKey {
        /// Description of which key and value caused the violation.
        detail: String,
    },

    /// The requested collection does not exist.
    #[error("Collection not found: {name}")]
    CollectionNotFound {
        /// Name of the collection that was not found.
        name: String,
    },

    /// The cursor was not found (expired or already closed).
    #[error("Cursor not found: {id}")]
    CursorNotFound {
        /// The cursor ID that was not found.
        id: i64,
    },

    /// An internal error occurred that should never happen in correct usage.
    #[error("Internal error: {0}")]
    Internal(String),

    /// A wire protocol message is malformed, exceeds size limits, or uses an
    /// unsupported opcode (e.g. OP_COMPRESSED / opcode 2012).
    /// MongoDB error code 48 (IllegalOperation).
    #[error("Invalid wire message: {detail}")]
    InvalidWireMessage {
        /// Human-readable description of the problem.
        detail: String,
    },

    /// A document failed structural validation (nesting depth, field count, or field name
    /// constraints). MongoDB error code 121.
    #[error("Document failed validation: {detail}")]
    DocumentValidationFailure {
        /// Human-readable description of which constraint was violated.
        detail: String,
    },

    /// A document exceeds the maximum allowed BSON-serialized size of 16,777,216 bytes.
    /// MongoDB error code 10334.
    #[error(
        "Document too large: {size} bytes exceeds maximum {max} bytes \
         (BSON-serialized document size)"
    )]
    DocumentTooLarge {
        /// Serialized size of the document in bytes.
        size: usize,
        /// Maximum allowed size in bytes (16,777,216 for mqlite).
        max: usize,
    },

    /// The database path points to a symlink, which mqlite refuses to follow for
    /// security reasons (symlink attack prevention).
    #[error("Refusing to open symlink at path: {path:?}")]
    SymlinkRejected {
        /// The path that was detected as a symlink.
        path: std::path::PathBuf,
    },

    /// An index type or option is not supported by mqlite.
    /// MongoDB error code 67 (CannotCreateIndex).
    ///
    /// Returned by `create_index` when the caller requests a TTL, text,
    /// geospatial, partial, or hashed index — none of which are supported in
    /// Phase 1.
    #[error(
        "UnsupportedIndexOption(\"{option}\"): {suggestion}\n\
         Supported index types: single-field, compound, unique, sparse, multikey."
    )]
    UnsupportedIndexOption {
        /// The name of the unsupported option (e.g. `"expireAfterSeconds"`,
        /// `"text"`, `"2dsphere"`).
        option: String,
        /// Human-readable suggestion listing supported alternatives.
        suggestion: String,
    },

    /// The journal file's magic bytes or format version does not match what this build supports.
    /// Typically produced when opening a pre-T1 database whose stale `-wal` sidecar is still on
    /// disk, or a T1+ journal from a future format version.
    #[error(
        "UnsupportedJournalFormat: found magic {found:?}, expected {expected:?}.\n\
         The journal sidecar on disk does not match what this build of mqlite supports. \
         This typically means the database was created by an older or newer mqlite version."
    )]
    UnsupportedJournalFormat {
        /// Magic bytes found in the on-disk journal (or legacy `-wal`) sidecar.
        found: [u8; 4],
        /// Magic bytes this build expects (`MQJL`).
        expected: [u8; 4],
    },

    /// The HLC logical counter saturated at `u32::MAX` for the current
    /// millisecond and the wall clock has not advanced past it.
    ///
    /// Only reachable under pathological load (more than `u32::MAX` commits
    /// in the same millisecond) or a stuck clock.
    #[error(
        "TimestampExhausted — the HLC logical counter reached u32::MAX for \
         the current millisecond. Wait for the wall clock to advance or \
         reduce the commit rate."
    )]
    TimestampExhausted,
}

impl Error {
    /// Return the MongoDB-compatible error code for this error, if one applies.
    pub fn code(&self) -> Option<i32> {
        match self {
            Error::DuplicateKey { .. } => Some(codes::DUPLICATE_KEY),
            Error::CollectionNotFound { .. } => Some(codes::NAMESPACE_NOT_FOUND),
            Error::CursorNotFound { .. } => Some(codes::CURSOR_NOT_FOUND),
            Error::Internal(_) => Some(codes::INTERNAL_ERROR),
            Error::DocumentValidationFailure { .. } => Some(codes::DOCUMENT_VALIDATION_FAILURE),
            Error::DocumentTooLarge { .. } => Some(codes::DOCUMENT_TOO_LARGE),
            Error::SymlinkRejected { .. } => Some(codes::BAD_VALUE),
            Error::InvalidWireMessage { .. } => Some(codes::ILLEGAL_OP),
            Error::UnsupportedOperator { .. } => Some(codes::UNSUPPORTED_OPERATOR),
            Error::UnsupportedIndexOption { .. } => Some(codes::CANNOT_CREATE_INDEX),
            _ => None,
        }
    }
}

/// Convenience Result type alias for mqlite operations.
pub type Result<T> = std::result::Result<T, Error>;
