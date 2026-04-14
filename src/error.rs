use thiserror::Error;

/// MongoDB-compatible error codes.
pub mod codes {
    pub const INTERNAL_ERROR: i32 = 1;
    pub const BAD_VALUE: i32 = 2;
    pub const DUPLICATE_KEY: i32 = 11000;
    pub const CURSOR_NOT_FOUND: i32 = 43;
    pub const NAMESPACE_NOT_FOUND: i32 = 26;
    pub const UNSUPPORTED_FORMAT: i32 = 115;
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
    UnsupportedOperator { operator: String },

    /// A command is not supported by mqlite's wire protocol shim.
    #[error("Unsupported command: {command}")]
    UnsupportedCommand { command: String },

    /// The database file is corrupt or structurally invalid.
    #[error(
        "CorruptDatabase at {path:?}: {detail}\n\
         Recoverable: {recoverable}\n\
         Note: Database::repair() is planned for Phase 2. In Phase 1, restore from a backup \
         or open in read_only mode to access the last successfully checkpointed state."
    )]
    CorruptDatabase {
        path: std::path::PathBuf,
        detail: String,
        recoverable: bool,
    },

    /// The disk is full; the write could not be completed.
    #[error(
        "DiskFull at {path:?}: required {required_bytes} bytes, \
         only {available_bytes} available.\n\
         {suggestion}"
    )]
    DiskFull {
        path: std::path::PathBuf,
        required_bytes: u64,
        available_bytes: u64,
        suggestion: String,
    },

    /// Duplicate key violation (MongoDB error code 11000).
    #[error("Duplicate key error: {detail}")]
    DuplicateKey { detail: String },

    /// The requested collection does not exist.
    #[error("Collection not found: {name}")]
    CollectionNotFound { name: String },

    /// The cursor was not found (expired or already closed).
    #[error("Cursor not found: {id}")]
    CursorNotFound { id: i64 },

    /// An internal error occurred that should never happen in correct usage.
    #[error("Internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Return the MongoDB-compatible error code for this error, if one applies.
    pub fn code(&self) -> Option<i32> {
        match self {
            Error::DuplicateKey { .. } => Some(codes::DUPLICATE_KEY),
            Error::CollectionNotFound { .. } => Some(codes::NAMESPACE_NOT_FOUND),
            Error::CursorNotFound { .. } => Some(codes::CURSOR_NOT_FOUND),
            Error::Internal(_) => Some(codes::INTERNAL_ERROR),
            _ => None,
        }
    }
}

/// Convenience Result type alias for mqlite operations.
pub type Result<T> = std::result::Result<T, Error>;
