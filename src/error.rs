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

/// Why an [`Error::WriteConflict`] was raised.
///
/// Phase 5 §10.3.3 first-committer-wins detection points produce these
/// reasons; §10.17.1, §10.24, and §10.25 each contribute additional
/// variants (`CatalogGenerationChanged`, `StructuralContention`,
/// `UniqueConflict`). The discriminants are part of the public contract
/// and are matched directly by US-002 tests.
#[derive(Debug, Clone)]
pub enum WriteConflictReason {
    /// The writer's `ReadView` predates a concurrent committed head on the
    /// same key. Detected at the read-then-modify precheck (§10.3.1 A) or
    /// at delta-install commit when the expected concrete head no longer
    /// matches (§10.3.1 C).
    StaleSnapshot,
    /// Two readers on the same page-local latch requested upgrade; one
    /// loses. Retry is immediate and does not require a new `ReadView`
    /// (§10.3.1 B).
    UpgradeRace,
    /// Two writers installed deltas on the same primary key; the
    /// first-committer wins. Caller should open a fresh `ReadView` before
    /// retrying (§10.3.1 C, §10.20).
    SameKeyConflict {
        /// Up to the first 32 bytes of the conflicting key, for
        /// diagnostics. Not load-bearing.
        key_preview: Vec<u8>,
    },
    /// The captured catalog generation no longer matches the published
    /// epoch when the writer revalidated before its install / journal
    /// envelope. Pre-durable; rolls back only in-memory staging
    /// (§10.17.1).
    CatalogGenerationChanged,
    /// Multi-leaf install could not acquire all required exclusive page
    /// latches in ascending `page_id` order. Partial acquisition is
    /// released and the caller may retry (§10.24).
    StructuralContention,
    /// A unique-index install observed another live (non-Aborted,
    /// `stop_ts == Ts::MAX`) entry whose compound-key prefix excluding
    /// the trailing `_id` equals this writer's prefix; the
    /// first-committer wins (§10.25).
    UniqueConflict {
        /// Up to the first 32 bytes of the conflicting unique-key prefix,
        /// for diagnostics. Not load-bearing.
        key_prefix_preview: Vec<u8>,
    },
}

/// Why a live buffer-pool pin could not find an evictable frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PoolExhaustedReason {
    /// Every frame in the target pool partition is pinned.
    AllFramesPinned,
    /// Every available eviction candidate carries resident deltas that
    /// cannot be dropped without first reconciling them.
    DeltaBearingFrames,
}

impl std::fmt::Display for PoolExhaustedReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AllFramesPinned => f.write_str("all frames pinned"),
            Self::DeltaBearingFrames => f.write_str("delta-bearing frames"),
        }
    }
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

    /// A concurrent writer committed a conflicting change. Caller decides
    /// whether to retry. Distinct from `WriterBusy`, which signals namespace
    /// or lane contention with no logical conflict.
    ///
    /// Reasons are enumerated in [`WriteConflictReason`]; the §10.3.3 text
    /// is part of the contract and is matched by the US-002 discriminant
    /// test.
    #[error(
        "WriteConflict — another writer committed a conflicting change.\n\
         Reason: {reason:?}\n\
         This is a first-committer-wins engine; the caller may re-run the \
         transaction against a fresh ReadView."
    )]
    WriteConflict {
        /// Why the conflict occurred. See [`WriteConflictReason`].
        reason: WriteConflictReason,
    },

    /// An MQL operator is not supported by mqlite.
    #[error(
        "UnsupportedOperator(\"{operator}\") — this operator is not supported in mqlite.\n\
         Supported operators: $eq, $gt, $gte, $lt, $lte, $ne, $in, $nin,\n\
         \t\t\t$and, $or, $not, $nor, $exists, $type,\n\
         \t\t\t$all, $elemMatch, $regex\n\
         See: https://docs.rs/mqlite/latest/mqlite/compatibility"
    )]
    UnsupportedOperator {
        /// The name of the unsupported operator (e.g. `"$where"`).
        operator: String,
    },

    /// A command is not supported by mqlite's wire protocol shim.
    #[error("unsupported command: {command}")]
    UnsupportedCommand {
        /// The name of the unsupported command (e.g. `"aggregate"`).
        command: String,
    },

    /// The database file is corrupt or structurally invalid.
    #[error(
        "CorruptDatabase at {path:?}: {detail}\n\
         Recoverable: {recoverable}\n\
         Note: Restore from a backup or open in read_only mode to access the last \
         successfully checkpointed state."
    )]
    #[non_exhaustive]
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
    #[non_exhaustive]
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
    #[error("duplicate key error: {detail}")]
    DuplicateKey {
        /// Description of which key and value caused the violation.
        detail: String,
    },

    /// The requested collection does not exist.
    #[error("collection not found: {name}")]
    CollectionNotFound {
        /// Name of the collection that was not found.
        name: String,
    },

    /// The cursor was not found (expired or already closed).
    #[error("cursor not found: {id}")]
    CursorNotFound {
        /// The cursor ID that was not found.
        id: i64,
    },

    /// An internal error occurred that should never happen in correct usage.
    #[error("internal error: {0}")]
    Internal(String),

    /// A wire protocol message is malformed, exceeds size limits, or uses an
    /// unsupported opcode (e.g. OP_COMPRESSED / opcode 2012).
    /// MongoDB error code 48 (IllegalOperation).
    #[error("invalid wire message: {detail}")]
    InvalidWireMessage {
        /// Human-readable description of the problem.
        detail: String,
    },

    /// A document failed structural validation (nesting depth, field count, or field name
    /// constraints). MongoDB error code 121.
    #[error("document failed validation: {detail}")]
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
    #[error("refusing to open symlink at path: {path:?}")]
    SymlinkRejected {
        /// The path that was detected as a symlink.
        path: std::path::PathBuf,
    },

    /// An index type or option is not supported by mqlite.
    /// MongoDB error code 67 (CannotCreateIndex).
    ///
    /// Returned by `create_index` when the caller requests a TTL, text,
    /// geospatial, partial, or hashed index.
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

    /// A caller supplied an invalid engine configuration value.
    #[error("invalid config {field}: {detail}")]
    InvalidConfig {
        /// Configuration field that failed validation.
        field: &'static str,
        /// Human-readable validation failure.
        detail: String,
    },

    /// The journal file's magic bytes or format version does not match what this build supports.
    /// Produced when the journal sidecar on disk was created by an incompatible mqlite version.
    #[error(
        "UnsupportedJournalFormat: found magic {found:?}, expected {expected:?}.\n\
         The journal sidecar on disk does not match what this build of mqlite supports. \
         This typically means the database was created by an older or newer mqlite version."
    )]
    #[non_exhaustive]
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

    /// An overflow-page refcount would exceed `u32::MAX`.
    ///
    /// Returned by `incref_overflow` when the CAS-loop observes the refcount
    /// already at `u32::MAX`. The atomic value is left unchanged. This only
    /// arises under pathological pin counts (≥ 4 billion live OverflowRefs
    /// on one chain) and indicates a pin leak.
    #[error(
        "RefcountOverflow — overflow-page refcount would exceed u32::MAX. \
         This indicates a pin leak; investigate long-lived ReadViews or \
         OverflowRef retention."
    )]
    RefcountOverflow,

    /// The `ReadView` has been force-expired by the engine.
    ///
    /// Returned when a reader tries to use a `ReadView` whose `poisoned`
    /// flag was set by `ReadViewRegistry::force_expire_all` — e.g. during
    /// a `drop_collection` barrier. The caller must open a new
    /// `ReadView` to continue reading.
    #[error(
        "ReadViewExpired — this ReadView was force-expired by the engine \
         (drop_collection barrier or forced expiry). Open a new ReadView \
         to continue reading."
    )]
    ReadViewExpired,

    /// A shared-state mutex was poisoned by a panicking thread.
    #[error("state poisoned: {component}")]
    StatePoisoned {
        /// Name of the component whose mutex was poisoned (e.g. `"history_store"`).
        component: &'static str,
    },

    /// A catalog field could not be parsed from BSON.
    #[error("catalog parse error at {field}: {source}")]
    CatalogParse {
        /// The BSON field name that failed to parse.
        field: &'static str,
        /// The underlying BSON deserialization error.
        #[source]
        source: bson::de::Error,
    },

    /// An update operator caused a type mismatch.
    #[error("update operator {operator} type mismatch: expected {expected}, got {got}")]
    UpdateOperatorTypeMismatch {
        /// The operator name (e.g. `"$inc"`).
        operator: &'static str,
        /// The expected BSON type name.
        expected: &'static str,
        /// The actual BSON type name encountered.
        got: &'static str,
    },

    /// A Phase 2 journal frame would exceed the hard byte cap on write.
    ///
    /// Returned by the `LogicalTxnFrame` encoder when the computed
    /// `total_frame_bytes` exceeds `LOGICAL_TXN_MAX_FRAME_SIZE`; the encoder
    /// bails before any byte is appended so the journal stays well-formed.
    #[error(
        "JournalFrameTooLarge: logical_frame_bytes={logical_frame_bytes} \
         exceeds max_bytes={max_bytes}"
    )]
    JournalFrameTooLarge {
        /// Computed encoded size of the offending frame in bytes.
        logical_frame_bytes: usize,
        /// Hard byte cap imposed by the journal format.
        max_bytes: usize,
    },

    /// Internal-only eviction refusal for a delta-bearing frame.
    ///
    /// Not produced by public engine APIs; those still surface
    /// `Error::PoolExhausted` when every eviction candidate is blocked.
    #[error("buffer-pool eviction blocked for page {page}: {reason}")]
    BufferPoolEvictionBlocked {
        /// Page number for the rejected eviction candidate.
        page: u32,
        /// Stable reason the frame must remain resident.
        reason: &'static str,
    },

    /// Reopen logical replay would exceed the configured buffer-pool size.
    #[error(
        "recovery pool exhausted: logical replay would exceed \
         BufferPool::config.max_pool_bytes; increase max_pool_bytes or perform \
         a forced reconcile on the previous open before closing"
    )]
    RecoveryPoolExhausted,

    /// A live CRUD or reader path could not find an evictable buffer-pool
    /// frame. Checkpoint frontier pressure is reported separately as
    /// [`Error::CheckpointIncomplete`].
    #[error(
        "pool exhausted: {reason}; close or expire readers or pins, wait for \
         checkpoint relief, or increase buffer_pool_size"
    )]
    PoolExhausted {
        /// Why the pool could not make room for the requested page.
        reason: PoolExhaustedReason,
    },

    /// Open-time recovery found durable evidence that cannot be replayed
    /// safely.
    #[error("recovery error: {detail}")]
    Recovery {
        /// Stable operator-facing recovery detail.
        detail: String,
    },

    /// A checkpoint cannot advance the durable frontier without losing
    /// checkpoint-visible resident state.
    #[error(
        "checkpoint incomplete: first_blocking_page={first_blocking_page}, reason={reason}; \
         close or expire long readers or pins, enable overflow spill if blocking, \
         raise pool or cap limits, then retry checkpoint"
    )]
    CheckpointIncomplete {
        /// First dirty leaf that blocked checkpoint planning.
        first_blocking_page: u32,
        /// Why the dirty leaf could not be included in this checkpoint.
        reason: CheckpointIncompleteReason,
    },

    /// The engine reached a post-durable state that requires reopening.
    ///
    /// Phase 5 §10.19.0 C-2 / US-036 — once the `journal_mutex` durability
    /// envelope or FullSync cohort fsync completes, an in-memory failure
    /// during Pending→Committed flip or `mark_ready` cannot be represented
    /// as `Aborted` because durable bytes already exist on disk. The
    /// engine is poisoned, refuses new operations, and must be reopened.
    #[error(
        "engine fatal: post-durable in-memory state could not be repaired \
         ({reason:?}); the engine is poisoned, refuses new operations, \
         and must be reopened"
    )]
    EngineFatal {
        /// Why the post-durable failure was unrecoverable. See
        /// [`EngineFatalReason`].
        reason: EngineFatalReason,
    },
}

/// Why an [`Error::EngineFatal`] was raised.
///
/// Distinguishes post-durable in-memory failures that escalate to engine
/// poison. The discriminants are part of the public contract and cover every
/// failure boundary that cannot be represented as a normal abort: log-slot
/// reservation, CRUD publish, Pending→Committed flip, DDL publish, and
/// checkpoint post-mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EngineFatalReason {
    /// A Phase 8 log writer failed after reserving a byte-LSN range and before
    /// marking the record written. The live process must not skip the gap or
    /// let later records become durable; reopen recovery owns truncation to the
    /// last valid prefix.
    PostReservationLogWriteFailure,
    /// A post-durable failure during the ordinary CRUD `mark_ready`
    /// publish closure or its surrounding post-durable scope. The
    /// `journal_mutex` envelope has already completed when this is
    /// raised (§10.19.0 C-2, §10.21).
    PostDurablePublishFailure,
    /// A post-durable failure flipping `VersionState::Pending` →
    /// `Committed` via `flip_pending_to_committed_for`. The durable
    /// journal commit has completed; in-memory state cannot be
    /// reconciled (§10.20.1, §10.21).
    PostDurablePendingFlipFailure,
    /// A post-durable failure during a DDL publish closure (create or
    /// drop index, drop namespace, create-index cleanup). The durable
    /// DDL envelope has already completed when this is raised
    /// (§10.8.1, §10.8.2, §10.8.3).
    PostDurableDdlPublishFailure,
    /// A checkpoint failed after its mutation phase began. The live
    /// engine is poisoned; close and reopen to recover from the last
    /// durable checkpoint boundary.
    CheckpointPostMutationFailure,
    /// A *pre-durable* commit cleanup could not flip its resident
    /// `VersionState::Pending` heads to `Aborted`
    /// (`flip_pending_to_aborted_for` failed: bounded-retry exhaustion under
    /// chain contention or a pin failure). The commit was never durable, but
    /// the leaked Pending heads remain resident; the publish slot must NOT be
    /// aborted, because advancing the published frontier past an unflipped
    /// slot would let foreign readers treat the Pending-below-frontier head as
    /// committed (a dirty read of never-committed data). Poisoning instead is
    /// safe: the txn is not durable, so reopen-recovery discards it wholesale,
    /// the resident Pending heads die with the process, and no in-flight
    /// reader ever sees the slot pass the frontier.
    PreDurableAbortFlipFailure,
}

/// Why [`Error::CheckpointIncomplete`] blocked a checkpoint.
///
/// Phase 7 US-010 keeps these reasons distinct from live
/// [`Error::PoolExhausted`] so checkpoint callers can tell whether retrying
/// requires pool relief, history-cap changes, or reachability repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CheckpointIncompleteReason {
    /// The dirty leaf was pinned by another reader or writer, so in-place
    /// checkpoint install would require forbidden frame CoW.
    FrameCoWRefused,
    /// A checkpoint-visible overflow value needs spill ownership transfer, but
    /// the overflow-spill path is not wired for that page.
    OverflowSpillNotWired,
    /// Checkpoint-visible winners alone exceed the folded leaf page budget.
    VisibleWinnerExceedsPageBudget,
    /// Retained tombstone predecessors or sidecar chains keep the folded leaf
    /// over budget until readers or pins move forward.
    TombstonePredecessorPressure,
    /// Buffer-pool pressure prevented the checkpoint from making the dirty
    /// leaf resident or keeping it resident before mutation.
    PoolExhausted(PoolExhaustedReason),
    /// A history-store spill key already exists with different bytes.
    HistoryDuplicateConflict,
    /// The per-key history duplicate disambiguator reached its cap.
    HistoryDuplicateCapExceeded,
    /// Dirty-leaf reachability no longer matches the catalog or resident tree.
    ReachabilityRepairRequired,
}

impl std::fmt::Display for CheckpointIncompleteReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FrameCoWRefused => f.write_str("frame CoW refused"),
            Self::OverflowSpillNotWired => f.write_str("overflow spill not wired"),
            Self::VisibleWinnerExceedsPageBudget => {
                f.write_str("visible winner exceeds page budget")
            }
            Self::TombstonePredecessorPressure => f.write_str("tombstone predecessor pressure"),
            Self::PoolExhausted(reason) => write!(f, "pool exhausted: {reason}"),
            Self::HistoryDuplicateConflict => f.write_str("history duplicate conflict"),
            Self::HistoryDuplicateCapExceeded => f.write_str("history duplicate cap exceeded"),
            Self::ReachabilityRepairRequired => f.write_str("reachability repair required"),
        }
    }
}

impl Error {
    /// Return the MongoDB-compatible error code for this error, if one applies.
    #[must_use]
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
            Error::InvalidConfig { .. } => Some(codes::BAD_VALUE),
            _ => None,
        }
    }
}

/// Convenience Result type alias for mqlite operations.
pub type Result<T> = std::result::Result<T, Error>;
