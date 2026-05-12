//! Journal file format — header, checkpoint page records, and I/O helpers.
//!
//! ## Journal File Layout
//!
//! ```text
//! [Journal Header — 32 bytes]
//! [Record 0 Header — 24 bytes][Record 0 Page Data — 4KB or 32KB]
//! [Record 1 Header — 24 bytes][Record 1 Page Data — 4KB or 32KB]
//! ...
//! ```
//!
//! ## Journal Header (32 bytes)
//!
//! ```text
//! Offset  Size  Field
//!   0      4    Magic: "MQJL" (0x4D514A4C)
//!   4      4    Format version: u32 LE (3)
//!   8      4    Page size internal: u32 LE (4096)
//!  12      4    Page size leaf: u32 LE (32768)
//!  16      4    Salt 1: u32 LE (must match main file header)
//!  20      4    Salt 2: u32 LE (must match main file header)
//!  24      4    Checkpoint sequence: u32 LE
//!  28      4    Header checksum: CRC32C of bytes 0–27
//! ```
//!
//! ## Checkpoint Page Record Header (24 bytes)
//!
//! ```text
//! Offset  Size  Field
//!   0      4    Page number: u32 LE
//!   4      4    DB page count after commit: u32 LE (0 = non-commit frame)
//!   8      4    Salt 1: u32 LE
//!  12      4    Salt 2: u32 LE
//!  16      4    Page size: u32 LE (4096 or 32768)
//!  20      4    Frame checksum: CRC32C of bytes 0–19 + page data
//! ```
//!
//! Followed immediately by `page_size` bytes of page data.
//!
//! ## Checksums
//!
//! All checksums use CRC32C.  The frame checksum covers the first 20 bytes
//! of the frame header (the `checksum` field itself is excluded) followed by
//! the entire page data.  This allows the recovery algorithm to verify each
//! frame independently.

#![allow(clippy::expect_used)]

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};
use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

// ---------------------------------------------------------------------------
// Positioned log I/O
// ---------------------------------------------------------------------------

/// Positioned write and sync operations used by the Phase 8 log manager.
pub(crate) trait PositionedLogIo: Send + Sync {
    /// Write some bytes from `data` at absolute byte offset `offset`.
    ///
    /// Returning a short byte count is allowed; callers are responsible for
    /// retrying until the full buffer is written or an error is returned.
    ///
    /// # Errors
    ///
    /// Returns any OS or test-injected write error.
    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<usize>;

    /// Sync log file data to stable storage.
    ///
    /// # Errors
    ///
    /// Returns any OS or test-injected sync error.
    fn sync_data(&self) -> io::Result<()>;
}

impl PositionedLogIo for File {
    fn write_at(&self, offset: u64, data: &[u8]) -> io::Result<usize> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            FileExt::write_at(self, data, offset)
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            self.seek_write(data, offset)
        }

        #[cfg(not(any(unix, windows)))]
        {
            let _ = (offset, data);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "positioned log writes are unsupported on this platform",
            ))
        }
    }

    fn sync_data(&self) -> io::Result<()> {
        File::sync_data(self)
    }
}

/// File wrapper that writes log bytes at explicit offsets.
pub(crate) struct PositionedLogFile {
    io: Box<dyn PositionedLogIo>,
}

impl PositionedLogFile {
    /// Create a positioned log writer from a file handle.
    pub(crate) fn new(file: File) -> Self {
        Self { io: Box::new(file) }
    }

    /// Create a positioned log writer from a test or adapter implementation.
    pub(crate) fn from_io(io: Box<dyn PositionedLogIo>) -> Self {
        Self { io }
    }

    /// Write all bytes from `data` at absolute byte offset `offset`.
    ///
    /// Short positioned writes are retried. A zero-length progress report is
    /// converted to [`io::ErrorKind::WriteZero`] so callers never mark a
    /// partially written slot complete.
    ///
    /// # Errors
    ///
    /// Returns any write error from the underlying positioned writer or
    /// [`io::ErrorKind::WriteZero`] if the writer made no progress.
    pub(crate) fn write_all_at(&self, mut offset: u64, mut data: &[u8]) -> io::Result<()> {
        while !data.is_empty() {
            let written = self.io.write_at(offset, data)?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "positioned log write made no progress",
                ));
            }
            offset = offset.checked_add(written as u64).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "positioned log offset overflow",
                )
            })?;
            data = &data[written..];
        }
        Ok(())
    }

    /// Sync log file data to stable storage.
    ///
    /// # Errors
    ///
    /// Returns any sync error from the underlying writer.
    pub(crate) fn sync_data(&self) -> io::Result<()> {
        self.io.sync_data()
    }
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic bytes identifying a valid `.mqlite-journal` journal file.
pub(crate) const JOURNAL_MAGIC: [u8; 4] = *b"MQJL";

/// Journal format version (increment on backward-incompatible changes).
pub(crate) const JOURNAL_FORMAT_VERSION: u32 = 3;

/// Pre-release journal format versions this build may safely discard.
pub(crate) const RETIRED_PRE_RELEASE_JOURNAL_FORMAT_VERSIONS: &[u32] = &[1, 2];

/// Total size of the journal file header in bytes.
pub(crate) const JOURNAL_HEADER_SIZE: usize = 32;

// ---------------------------------------------------------------------------
// Phase 8 LogRecord codec
// ---------------------------------------------------------------------------

/// Magic value for a Phase 8 redo-log record.
///
/// The little-endian byte sequence is `MQL8`.
pub(crate) const LOG_RECORD_MAGIC: u32 = 0x384C_514D;

/// Phase 8 outer-record format version.
pub(crate) const LOG_RECORD_FORMAT_VERSION: u16 = 1;

/// Fixed byte size of a Phase 8 `LogRecord` header.
pub(crate) const LOG_RECORD_HEADER_LEN: usize = 72;

/// Hard cap on one encoded Phase 8 log record.
pub(crate) const MAX_LOG_RECORD_BYTES: usize = 64 * 1024 * 1024;

/// Offset of the `magic` field in the fixed log-record header.
pub(crate) const LOG_RECORD_MAGIC_OFFSET: usize = 0;
/// Offset of the `format_version` field in the fixed log-record header.
pub(crate) const LOG_RECORD_FORMAT_VERSION_OFFSET: usize = 4;
/// Offset of the `header_len` field in the fixed log-record header.
pub(crate) const LOG_RECORD_HEADER_LEN_OFFSET: usize = 6;
/// Offset of the `record_kind` field in the fixed log-record header.
pub(crate) const LOG_RECORD_KIND_OFFSET: usize = 8;
/// Offset of the `flags` field in the fixed log-record header.
pub(crate) const LOG_RECORD_FLAGS_OFFSET: usize = 10;
/// Offset of the `total_len` field in the fixed log-record header.
pub(crate) const LOG_RECORD_TOTAL_LEN_OFFSET: usize = 12;
/// Offset of the `start_lsn` field in the fixed log-record header.
pub(crate) const LOG_RECORD_START_LSN_OFFSET: usize = 16;
/// Offset of the `end_lsn` field in the fixed log-record header.
pub(crate) const LOG_RECORD_END_LSN_OFFSET: usize = 24;
/// Offset of the `txn_id` field in the fixed log-record header.
pub(crate) const LOG_RECORD_TXN_ID_OFFSET: usize = 32;
/// Offset of the `publish_seq` field in the fixed log-record header.
pub(crate) const LOG_RECORD_PUBLISH_SEQ_OFFSET: usize = 40;
/// Offset of the `commit_ts.physical_ms` field in the fixed log-record header.
pub(crate) const LOG_RECORD_COMMIT_TS_PHYSICAL_OFFSET: usize = 48;
/// Offset of the `commit_ts.logical` field in the fixed log-record header.
pub(crate) const LOG_RECORD_COMMIT_TS_LOGICAL_OFFSET: usize = 56;
/// Offset of the `payload_len` field in the fixed log-record header.
pub(crate) const LOG_RECORD_PAYLOAD_LEN_OFFSET: usize = 60;
/// Offset of the `header_crc32c` field in the fixed log-record header.
pub(crate) const LOG_RECORD_HEADER_CRC32C_OFFSET: usize = 64;
/// Offset of the `payload_crc32c` field in the fixed log-record header.
pub(crate) const LOG_RECORD_PAYLOAD_CRC32C_OFFSET: usize = 68;

const LOG_RECORD_KIND_CRUD_COMMIT: u16 = 1;
const LOG_RECORD_KIND_CATALOG_COMMIT: u16 = 2;
const LOG_RECORD_KIND_CHECKPOINT_BOUNDARY: u16 = 3;
const LOG_RECORD_KIND_CHECKPOINT_PAGE_FRAME: u16 = 4;

const LOG_RECORD_FLAG_HAS_LOGICAL_PAYLOAD: u16 = 0x0001;
const LOG_RECORD_FLAG_HAS_CHAIN_PAYLOAD: u16 = 0x0002;
const LOG_RECORD_FLAG_HAS_CATALOG_PAYLOAD: u16 = 0x0004;
const LOG_RECORD_FLAG_CHECKPOINT_BOUNDARY: u16 = 0x0008;
const LOG_RECORD_FLAG_HAS_CHECKPOINT_PAGE: u16 = 0x0010;
const LOG_RECORD_KNOWN_FLAGS: u16 = LOG_RECORD_FLAG_HAS_LOGICAL_PAYLOAD
    | LOG_RECORD_FLAG_HAS_CHAIN_PAYLOAD
    | LOG_RECORD_FLAG_HAS_CATALOG_PAYLOAD
    | LOG_RECORD_FLAG_CHECKPOINT_BOUNDARY
    | LOG_RECORD_FLAG_HAS_CHECKPOINT_PAGE;

/// Legal Phase 8 outer-record kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LogRecordKind {
    /// CRUD commit record.
    CrudCommit,
    /// Catalog or DDL commit record.
    CatalogCommit,
    /// Checkpoint control record.
    CheckpointBoundary,
    /// Per-page checkpoint payload that belongs to an in-flight checkpoint
    /// batch. Recovery accumulates these by `batch_id` and flushes them when
    /// the matching `CheckpointBoundary` record is replayed.
    CheckpointPageFrame,
}

impl LogRecordKind {
    fn from_wire(value: u16) -> Result<Self> {
        match value {
            LOG_RECORD_KIND_CRUD_COMMIT => Ok(Self::CrudCommit),
            LOG_RECORD_KIND_CATALOG_COMMIT => Ok(Self::CatalogCommit),
            LOG_RECORD_KIND_CHECKPOINT_BOUNDARY => Ok(Self::CheckpointBoundary),
            LOG_RECORD_KIND_CHECKPOINT_PAGE_FRAME => Ok(Self::CheckpointPageFrame),
            _ => Err(invalid_log_record(format!(
                "Phase 8 LogRecord unknown record_kind {value}"
            ))),
        }
    }

    fn wire_value(self) -> u16 {
        match self {
            Self::CrudCommit => LOG_RECORD_KIND_CRUD_COMMIT,
            Self::CatalogCommit => LOG_RECORD_KIND_CATALOG_COMMIT,
            Self::CheckpointBoundary => LOG_RECORD_KIND_CHECKPOINT_BOUNDARY,
            Self::CheckpointPageFrame => LOG_RECORD_KIND_CHECKPOINT_PAGE_FRAME,
        }
    }

    fn required_flags(self) -> LogRecordFlags {
        match self {
            Self::CrudCommit => LogRecordFlags(
                LOG_RECORD_FLAG_HAS_LOGICAL_PAYLOAD | LOG_RECORD_FLAG_HAS_CHAIN_PAYLOAD,
            ),
            Self::CatalogCommit => LogRecordFlags(LOG_RECORD_FLAG_HAS_CATALOG_PAYLOAD),
            Self::CheckpointBoundary => LogRecordFlags(LOG_RECORD_FLAG_CHECKPOINT_BOUNDARY),
            Self::CheckpointPageFrame => LogRecordFlags(LOG_RECORD_FLAG_HAS_CHECKPOINT_PAGE),
        }
    }
}

/// Phase 8 log-record flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LogRecordFlags(u16);

impl LogRecordFlags {
    /// `CrudCommit` carries the logical transaction payload bytes.
    pub(crate) const HAS_LOGICAL_PAYLOAD: Self = Self(LOG_RECORD_FLAG_HAS_LOGICAL_PAYLOAD);
    /// `CrudCommit` carries chain/refcount/page-write payload bytes.
    pub(crate) const HAS_CHAIN_PAYLOAD: Self = Self(LOG_RECORD_FLAG_HAS_CHAIN_PAYLOAD);
    /// `CatalogCommit` carries catalog payload bytes.
    pub(crate) const HAS_CATALOG_PAYLOAD: Self = Self(LOG_RECORD_FLAG_HAS_CATALOG_PAYLOAD);
    /// `CheckpointBoundary` carries checkpoint-frontier payload bytes.
    pub(crate) const CHECKPOINT_BOUNDARY: Self = Self(LOG_RECORD_FLAG_CHECKPOINT_BOUNDARY);
    /// `CheckpointPageFrame` carries one in-flight checkpoint page payload.
    pub(crate) const HAS_CHECKPOINT_PAGE: Self = Self(LOG_RECORD_FLAG_HAS_CHECKPOINT_PAGE);

    fn from_bits(bits: u16) -> Result<Self> {
        let flags = Self(bits);
        flags.reject_unknown_bits()?;
        Ok(flags)
    }

    /// Return the raw wire bits.
    pub(crate) fn bits(self) -> u16 {
        self.0
    }

    fn reject_unknown_bits(self) -> Result<()> {
        let unknown = self.0 & !LOG_RECORD_KNOWN_FLAGS;
        if unknown != 0 {
            return Err(invalid_log_record(format!(
                "Phase 8 LogRecord unknown flag bits {unknown:#06x}"
            )));
        }
        Ok(())
    }

    fn validate_for_kind(self, kind: LogRecordKind) -> Result<()> {
        self.reject_unknown_bits()?;
        let required = kind.required_flags();
        if self != required {
            return Err(invalid_log_record(format!(
                "Phase 8 LogRecord invalid flags {:#06x} for {:?}",
                self.0, kind
            )));
        }
        Ok(())
    }
}

/// Typed Phase 8 log-record payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogRecordPayload {
    /// CRUD commit payload: existing logical bytes plus chain/refcount/page-write bytes.
    CrudCommit {
        /// Encoded logical transaction bytes.
        logical_payload: Vec<u8>,
        /// Encoded chain/refcount/page-write bytes.
        chain_payload: Vec<u8>,
    },
    /// Catalog commit payload bytes.
    CatalogCommit(Vec<u8>),
    /// Checkpoint boundary control payload bytes.
    CheckpointBoundary(Vec<u8>),
    /// In-flight checkpoint page payload bytes.
    CheckpointPageFrame(Vec<u8>),
}

impl LogRecordPayload {
    fn kind(&self) -> LogRecordKind {
        match self {
            Self::CrudCommit { .. } => LogRecordKind::CrudCommit,
            Self::CatalogCommit(_) => LogRecordKind::CatalogCommit,
            Self::CheckpointBoundary(_) => LogRecordKind::CheckpointBoundary,
            Self::CheckpointPageFrame(_) => LogRecordKind::CheckpointPageFrame,
        }
    }

    fn flags(&self) -> LogRecordFlags {
        self.kind().required_flags()
    }

    fn encoded_len(&self) -> Result<usize> {
        match self {
            Self::CrudCommit {
                logical_payload,
                chain_payload,
            } => 8usize
                .checked_add(logical_payload.len())
                .and_then(|n| n.checked_add(chain_payload.len()))
                .ok_or_else(|| log_record_too_large(usize::MAX)),
            Self::CatalogCommit(payload)
            | Self::CheckpointBoundary(payload)
            | Self::CheckpointPageFrame(payload) => Ok(payload.len()),
        }
    }

    fn encode_into(&self, out: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::CrudCommit {
                logical_payload,
                chain_payload,
            } => {
                out.extend_from_slice(&len_to_u32(logical_payload.len())?.to_le_bytes());
                out.extend_from_slice(&len_to_u32(chain_payload.len())?.to_le_bytes());
                out.extend_from_slice(logical_payload);
                out.extend_from_slice(chain_payload);
            }
            Self::CatalogCommit(payload)
            | Self::CheckpointBoundary(payload)
            | Self::CheckpointPageFrame(payload) => {
                out.extend_from_slice(payload);
            }
        }
        Ok(())
    }

    fn decode(kind: LogRecordKind, payload: &[u8]) -> Result<Self> {
        match kind {
            LogRecordKind::CrudCommit => {
                if payload.len() < 8 {
                    return Err(invalid_log_record(
                        "Phase 8 CrudCommit payload is shorter than split header",
                    ));
                }
                let logical_len =
                    u32::from_le_bytes(payload[0..4].try_into().expect("4 bytes")) as usize;
                let chain_len =
                    u32::from_le_bytes(payload[4..8].try_into().expect("4 bytes")) as usize;
                let logical_end = 8usize.checked_add(logical_len).ok_or_else(|| {
                    invalid_log_record("Phase 8 CrudCommit logical length overflow")
                })?;
                let chain_end = logical_end.checked_add(chain_len).ok_or_else(|| {
                    invalid_log_record("Phase 8 CrudCommit chain length overflow")
                })?;
                if chain_end != payload.len() {
                    return Err(invalid_log_record(
                        "Phase 8 CrudCommit payload split does not match payload_len",
                    ));
                }
                Ok(Self::CrudCommit {
                    logical_payload: payload[8..logical_end].to_vec(),
                    chain_payload: payload[logical_end..chain_end].to_vec(),
                })
            }
            LogRecordKind::CatalogCommit => Ok(Self::CatalogCommit(payload.to_vec())),
            LogRecordKind::CheckpointBoundary => Ok(Self::CheckpointBoundary(payload.to_vec())),
            LogRecordKind::CheckpointPageFrame => Ok(Self::CheckpointPageFrame(payload.to_vec())),
        }
    }
}

/// Phase 8 log-record draft whose length is known before LSN reservation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogRecordDraft {
    kind: LogRecordKind,
    flags: LogRecordFlags,
    txn_id: u64,
    publish_seq: u64,
    commit_ts: Ts,
    payload: LogRecordPayload,
}

impl LogRecordDraft {
    /// Build a CRUD commit draft from existing logical and chain payload bytes.
    pub(crate) fn crud(
        txn_id: u64,
        publish_seq: u64,
        commit_ts: Ts,
        logical_payload: Vec<u8>,
        chain_payload: Vec<u8>,
    ) -> Self {
        Self::from_payload(
            txn_id,
            publish_seq,
            commit_ts,
            LogRecordPayload::CrudCommit {
                logical_payload,
                chain_payload,
            },
        )
    }

    /// Build a catalog commit draft.
    pub(crate) fn catalog(txn_id: u64, publish_seq: u64, commit_ts: Ts, payload: Vec<u8>) -> Self {
        Self::from_payload(
            txn_id,
            publish_seq,
            commit_ts,
            LogRecordPayload::CatalogCommit(payload),
        )
    }

    /// Build a checkpoint-boundary control draft.
    pub(crate) fn checkpoint_boundary(txn_id: u64, commit_ts: Ts, payload: Vec<u8>) -> Self {
        Self::from_payload(
            txn_id,
            0,
            commit_ts,
            LogRecordPayload::CheckpointBoundary(payload),
        )
    }

    /// Build a checkpoint per-page draft.
    pub(crate) fn checkpoint_page_frame(commit_ts: Ts, payload: Vec<u8>) -> Self {
        Self::from_payload(
            0,
            0,
            commit_ts,
            LogRecordPayload::CheckpointPageFrame(payload),
        )
    }

    fn from_payload(
        txn_id: u64,
        publish_seq: u64,
        commit_ts: Ts,
        payload: LogRecordPayload,
    ) -> Self {
        Self {
            kind: payload.kind(),
            flags: payload.flags(),
            txn_id,
            publish_seq,
            commit_ts,
            payload,
        }
    }

    /// Compute the final encoded length before LSN reservation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::JournalFrameTooLarge`] if the draft exceeds
    /// [`MAX_LOG_RECORD_BYTES`], or [`Error::CorruptDatabase`] if the draft
    /// violates Phase 8 kind/flag/publish-sequence rules.
    pub(crate) fn encoded_len(&self) -> Result<usize> {
        self.validate_semantics()?;
        let payload_len = self.payload.encoded_len()?;
        let total_len = LOG_RECORD_HEADER_LEN
            .checked_add(payload_len)
            .ok_or_else(|| log_record_too_large(usize::MAX))?;
        if total_len > MAX_LOG_RECORD_BYTES {
            return Err(log_record_too_large(total_len));
        }
        Ok(total_len)
    }

    /// Finalize the draft for a reserved LSN range.
    ///
    /// # Errors
    ///
    /// Returns [`Error::JournalFrameTooLarge`] if the draft exceeds
    /// [`MAX_LOG_RECORD_BYTES`], or [`Error::CorruptDatabase`] if the LSN range
    /// overflows or the draft violates Phase 8 semantics.
    pub(crate) fn finalize(self, start_lsn: u64) -> Result<FinalizedLogRecord> {
        let total_len = self.encoded_len()?;
        let end_lsn = start_lsn
            .checked_add(total_len as u64)
            .ok_or_else(|| invalid_log_record("Phase 8 LogRecord end_lsn overflow"))?;

        let payload_len = total_len - LOG_RECORD_HEADER_LEN;
        let mut payload = Vec::with_capacity(payload_len);
        self.payload.encode_into(&mut payload)?;
        debug_assert_eq!(payload.len(), payload_len);

        let payload_crc32c = crc32c::crc32c(&payload);
        let header = LogRecordHeader {
            kind: self.kind,
            flags: self.flags,
            total_len,
            start_lsn,
            end_lsn,
            txn_id: self.txn_id,
            publish_seq: self.publish_seq,
            commit_ts: self.commit_ts,
            payload_len,
            payload_crc32c,
        };

        let mut bytes = vec![0u8; total_len];
        header.write_with_header_crc(0, &mut bytes[..LOG_RECORD_HEADER_LEN])?;
        let header_crc32c = crc32c::crc32c(&bytes[..LOG_RECORD_HEADER_LEN]);
        bytes[LOG_RECORD_HEADER_CRC32C_OFFSET..LOG_RECORD_HEADER_CRC32C_OFFSET + 4]
            .copy_from_slice(&header_crc32c.to_le_bytes());
        bytes[LOG_RECORD_HEADER_LEN..].copy_from_slice(&payload);

        Ok(FinalizedLogRecord {
            start_lsn,
            end_lsn,
            bytes,
        })
    }

    fn validate_semantics(&self) -> Result<()> {
        self.flags.validate_for_kind(self.kind)?;
        if self.payload.kind() != self.kind {
            return Err(invalid_log_record(
                "Phase 8 LogRecord payload kind does not match record_kind",
            ));
        }
        match self.kind {
            LogRecordKind::CrudCommit | LogRecordKind::CatalogCommit if self.publish_seq == 0 => {
                Err(invalid_log_record(
                    "Phase 8 LogRecord publish_seq 0 is reserved for CheckpointBoundary",
                ))
            }
            LogRecordKind::CheckpointBoundary | LogRecordKind::CheckpointPageFrame
                if self.publish_seq != 0 =>
            {
                Err(invalid_log_record(
                    "Phase 8 Checkpoint records must use publish_seq 0",
                ))
            }
            _ => Ok(()),
        }
    }
}

/// Finalized Phase 8 log record bytes ready for `write_reserved`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FinalizedLogRecord {
    start_lsn: u64,
    end_lsn: u64,
    bytes: Vec<u8>,
}

impl FinalizedLogRecord {
    /// Return the reserved start LSN.
    pub(crate) fn start_lsn(&self) -> u64 {
        self.start_lsn
    }

    /// Return the exclusive end LSN.
    pub(crate) fn end_lsn(&self) -> u64 {
        self.end_lsn
    }

    /// Return the exact finalized bytes to write at `start_lsn`.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Decoded Phase 8 outer log record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogRecord {
    /// Inclusive byte-offset LSN where the record starts.
    pub(crate) start_lsn: u64,
    /// Exclusive byte-offset LSN where the record ends.
    pub(crate) end_lsn: u64,
    /// Diagnostic transaction id carried by the writer.
    pub(crate) txn_id: u64,
    /// Durable publish sequence for replay ordering.
    pub(crate) publish_seq: u64,
    /// Commit timestamp carried by the writer.
    pub(crate) commit_ts: Ts,
    /// Legal record kind.
    pub(crate) kind: LogRecordKind,
    /// Legal flags for `kind`.
    pub(crate) flags: LogRecordFlags,
    /// Encoded payload byte length.
    pub(crate) payload_len: usize,
    /// Encoded total record byte length.
    pub(crate) total_len: usize,
    /// Stored header CRC32C.
    pub(crate) header_crc32c: u32,
    /// Stored payload CRC32C.
    pub(crate) payload_crc32c: u32,
    /// Decoded typed payload.
    pub(crate) payload: LogRecordPayload,
}

impl LogRecord {
    /// Decode and validate the leading Phase 8 log record in `buf`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CorruptDatabase`] for malformed, truncated, corrupt,
    /// unsupported, or semantically invalid record bytes.
    pub(crate) fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < LOG_RECORD_HEADER_LEN {
            return Err(invalid_log_record(
                "Phase 8 LogRecord truncated fixed header",
            ));
        }

        let magic = read_u32(buf, LOG_RECORD_MAGIC_OFFSET);
        if magic != LOG_RECORD_MAGIC {
            return Err(invalid_log_record(format!(
                "Phase 8 LogRecord bad magic {magic:#010x}"
            )));
        }

        let format_version = read_u16(buf, LOG_RECORD_FORMAT_VERSION_OFFSET);
        if format_version != LOG_RECORD_FORMAT_VERSION {
            return Err(invalid_log_record(format!(
                "Phase 8 LogRecord bad format_version {format_version}"
            )));
        }

        let header_len = read_u16(buf, LOG_RECORD_HEADER_LEN_OFFSET) as usize;
        if header_len != LOG_RECORD_HEADER_LEN {
            return Err(invalid_log_record(format!(
                "Phase 8 LogRecord header_len {header_len} does not match {LOG_RECORD_HEADER_LEN}"
            )));
        }

        let kind = LogRecordKind::from_wire(read_u16(buf, LOG_RECORD_KIND_OFFSET))?;
        let flags = LogRecordFlags::from_bits(read_u16(buf, LOG_RECORD_FLAGS_OFFSET))?;
        flags.validate_for_kind(kind)?;

        let total_len = read_u32(buf, LOG_RECORD_TOTAL_LEN_OFFSET) as usize;
        if total_len > MAX_LOG_RECORD_BYTES {
            return Err(log_record_too_large(total_len));
        }
        let start_lsn = read_u64(buf, LOG_RECORD_START_LSN_OFFSET);
        let end_lsn = read_u64(buf, LOG_RECORD_END_LSN_OFFSET);
        let txn_id = read_u64(buf, LOG_RECORD_TXN_ID_OFFSET);
        let publish_seq = read_u64(buf, LOG_RECORD_PUBLISH_SEQ_OFFSET);
        let commit_ts = Ts {
            physical_ms: read_u64(buf, LOG_RECORD_COMMIT_TS_PHYSICAL_OFFSET),
            logical: read_u32(buf, LOG_RECORD_COMMIT_TS_LOGICAL_OFFSET),
        };
        let payload_len = read_u32(buf, LOG_RECORD_PAYLOAD_LEN_OFFSET) as usize;
        let header_crc32c = read_u32(buf, LOG_RECORD_HEADER_CRC32C_OFFSET);
        let payload_crc32c = read_u32(buf, LOG_RECORD_PAYLOAD_CRC32C_OFFSET);

        let expected_total_len = header_len
            .checked_add(payload_len)
            .ok_or_else(|| invalid_log_record("Phase 8 LogRecord total_len overflow"))?;
        if total_len != expected_total_len {
            return Err(invalid_log_record(format!(
                "Phase 8 LogRecord total_len {total_len} does not equal header_len + payload_len {expected_total_len}"
            )));
        }
        if buf.len() < total_len {
            return Err(invalid_log_record("Phase 8 LogRecord truncated payload"));
        }
        let expected_end_lsn = start_lsn
            .checked_add(total_len as u64)
            .ok_or_else(|| invalid_log_record("Phase 8 LogRecord end_lsn overflow"))?;
        if end_lsn != expected_end_lsn {
            return Err(invalid_log_record(format!(
                "Phase 8 LogRecord end_lsn {end_lsn} does not equal start_lsn + total_len {expected_end_lsn}"
            )));
        }

        let mut header_for_crc = [0u8; LOG_RECORD_HEADER_LEN];
        header_for_crc.copy_from_slice(&buf[..LOG_RECORD_HEADER_LEN]);
        header_for_crc[LOG_RECORD_HEADER_CRC32C_OFFSET..LOG_RECORD_HEADER_CRC32C_OFFSET + 4]
            .copy_from_slice(&0u32.to_le_bytes());
        let computed_header_crc32c = crc32c::crc32c(&header_for_crc);
        if header_crc32c != computed_header_crc32c {
            return Err(invalid_log_record(format!(
                "Phase 8 LogRecord header_crc32c mismatch: stored 0x{header_crc32c:08X}, computed 0x{computed_header_crc32c:08X}"
            )));
        }

        let payload_bytes = &buf[LOG_RECORD_HEADER_LEN..total_len];
        let computed_payload_crc32c = crc32c::crc32c(payload_bytes);
        if payload_crc32c != computed_payload_crc32c {
            return Err(invalid_log_record(format!(
                "Phase 8 LogRecord payload_crc32c mismatch: stored 0x{payload_crc32c:08X}, computed 0x{computed_payload_crc32c:08X}"
            )));
        }

        match kind {
            LogRecordKind::CrudCommit | LogRecordKind::CatalogCommit if publish_seq == 0 => {
                return Err(invalid_log_record(
                    "Phase 8 LogRecord publish_seq 0 is reserved for CheckpointBoundary",
                ));
            }
            LogRecordKind::CheckpointBoundary | LogRecordKind::CheckpointPageFrame
                if publish_seq != 0 =>
            {
                return Err(invalid_log_record(
                    "Phase 8 Checkpoint records must use publish_seq 0",
                ));
            }
            _ => {}
        }

        let payload = LogRecordPayload::decode(kind, payload_bytes)?;
        Ok(Self {
            start_lsn,
            end_lsn,
            txn_id,
            publish_seq,
            commit_ts,
            kind,
            flags,
            payload_len,
            total_len,
            header_crc32c,
            payload_crc32c,
            payload,
        })
    }
}

const CATALOG_COMMIT_PAYLOAD_MAGIC: [u8; 4] = *b"MQCC";
const CATALOG_COMMIT_PAYLOAD_VERSION: u16 = 1;
const CHECKPOINT_BOUNDARY_PAYLOAD_MAGIC: [u8; 4] = *b"MQCB";
const CHECKPOINT_BOUNDARY_PAYLOAD_VERSION: u16 = 1;

/// Typed catalog/DDL operation carried by a Phase 8 `CatalogCommit`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CatalogCommitKind {
    /// Namespace create or implicit collection bootstrap.
    NamespaceCreate,
    /// Namespace drop.
    NamespaceDrop,
    /// Reserve a Building index entry.
    IndexReserve,
    /// Persist index build page/catalog changes before Ready publish.
    IndexBuild,
    /// Promote a Building index to Ready.
    IndexBuildCommit,
    /// Remove a failed Building index.
    IndexCleanup,
    /// Drop a Ready index.
    IndexDrop,
}

impl CatalogCommitKind {
    fn wire_value(self) -> u8 {
        match self {
            Self::NamespaceCreate => 1,
            Self::NamespaceDrop => 2,
            Self::IndexReserve => 3,
            Self::IndexBuild => 4,
            Self::IndexBuildCommit => 5,
            Self::IndexCleanup => 6,
            Self::IndexDrop => 7,
        }
    }

    fn from_wire(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::NamespaceCreate),
            2 => Ok(Self::NamespaceDrop),
            3 => Ok(Self::IndexReserve),
            4 => Ok(Self::IndexBuild),
            5 => Ok(Self::IndexBuildCommit),
            6 => Ok(Self::IndexCleanup),
            7 => Ok(Self::IndexDrop),
            _ => Err(invalid_log_record(format!(
                "Phase 8 CatalogCommit unknown variant {value}"
            ))),
        }
    }
}

/// One page image included in a typed catalog commit payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogCommitPage {
    /// Page number in the main database file.
    pub(crate) page_number: u32,
    /// Physical page size of `data`.
    pub(crate) page_size: JournalPageSize,
    /// Full page bytes to write during recovery replay.
    pub(crate) data: Vec<u8>,
}

/// Typed payload carried by `LogRecordKind::CatalogCommit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogCommitPayload {
    /// Catalog lifecycle operation represented by this record.
    pub(crate) kind: CatalogCommitKind,
    /// Published catalog generation before the operation.
    pub(crate) catalog_generation_before: u64,
    /// Published catalog generation after the operation.
    pub(crate) catalog_generation_after: u64,
    /// Header image that must be durable with the structural pages.
    pub(crate) header: FileHeader,
    /// Structural catalog/data/index page images staged by the DDL batch.
    pub(crate) pages: Vec<CatalogCommitPage>,
}

impl CatalogCommitPayload {
    /// Encode this catalog commit payload.
    ///
    /// # Errors
    ///
    /// Returns an error if the page count or any page image is invalid for the
    /// Phase 8 wire format.
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        out.extend_from_slice(&CATALOG_COMMIT_PAYLOAD_MAGIC);
        out.extend_from_slice(&CATALOG_COMMIT_PAYLOAD_VERSION.to_le_bytes());
        out.push(self.kind.wire_value());
        out.push(0);
        out.extend_from_slice(&self.catalog_generation_before.to_le_bytes());
        out.extend_from_slice(&self.catalog_generation_after.to_le_bytes());
        out.extend_from_slice(&len_to_u32(HEADER_PAGE_SIZE)?.to_le_bytes());
        out.extend_from_slice(&self.header.to_bytes());
        out.extend_from_slice(&len_to_u32(self.pages.len())?.to_le_bytes());
        for page in &self.pages {
            if page.data.len() != page.page_size.bytes() {
                return Err(invalid_log_record(format!(
                    "Phase 8 CatalogCommit page {} has {} bytes for {:?}",
                    page.page_number,
                    page.data.len(),
                    page.page_size
                )));
            }
            out.extend_from_slice(&page.page_number.to_le_bytes());
            out.extend_from_slice(&page.page_size.as_u32().to_le_bytes());
            out.extend_from_slice(&len_to_u32(page.data.len())?.to_le_bytes());
            out.extend_from_slice(&page.data);
        }
        Ok(out)
    }

    /// Decode and validate a catalog commit payload.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CorruptDatabase`] if the payload is malformed or uses
    /// an unsupported catalog variant.
    pub(crate) fn decode(buf: &[u8]) -> Result<Self> {
        const PREFIX_LEN: usize = 28;
        if buf.len() < PREFIX_LEN {
            return Err(invalid_log_record(
                "Phase 8 CatalogCommit payload truncated fixed header",
            ));
        }
        if buf[0..4] != CATALOG_COMMIT_PAYLOAD_MAGIC {
            return Err(invalid_log_record("Phase 8 CatalogCommit bad magic"));
        }
        let version = read_u16(buf, 4);
        if version != CATALOG_COMMIT_PAYLOAD_VERSION {
            return Err(invalid_log_record(format!(
                "Phase 8 CatalogCommit bad version {version}"
            )));
        }
        let kind = CatalogCommitKind::from_wire(buf[6])?;
        if buf[7] != 0 {
            return Err(invalid_log_record(
                "Phase 8 CatalogCommit reserved byte must be zero",
            ));
        }
        let catalog_generation_before = read_u64(buf, 8);
        let catalog_generation_after = read_u64(buf, 16);
        let header_len = read_u32(buf, 24) as usize;
        if header_len != HEADER_PAGE_SIZE {
            return Err(invalid_log_record(format!(
                "Phase 8 CatalogCommit header length {header_len} is invalid"
            )));
        }
        let header_start = PREFIX_LEN;
        let header_end = header_start
            .checked_add(header_len)
            .ok_or_else(|| invalid_log_record("Phase 8 CatalogCommit header length overflow"))?;
        if buf.len() < header_end + 4 {
            return Err(invalid_log_record(
                "Phase 8 CatalogCommit payload truncated header image",
            ));
        }
        let header_bytes: &[u8; HEADER_PAGE_SIZE] = buf[header_start..header_end]
            .try_into()
            .expect("validated header len");
        let header = FileHeader::from_bytes(header_bytes)?;
        let mut cursor = header_end;
        let page_count = read_u32(buf, cursor) as usize;
        cursor += 4;
        let mut pages = Vec::with_capacity(page_count);
        for _ in 0..page_count {
            if buf.len().saturating_sub(cursor) < 12 {
                return Err(invalid_log_record(
                    "Phase 8 CatalogCommit payload truncated page header",
                ));
            }
            let page_number = read_u32(buf, cursor);
            cursor += 4;
            let page_size = JournalPageSize::from_u32(read_u32(buf, cursor))?;
            cursor += 4;
            let data_len = read_u32(buf, cursor) as usize;
            cursor += 4;
            if data_len != page_size.bytes() {
                return Err(invalid_log_record(format!(
                    "Phase 8 CatalogCommit page {page_number} data length {data_len} \
                     does not match {:?}",
                    page_size
                )));
            }
            let data_end = cursor.checked_add(data_len).ok_or_else(|| {
                invalid_log_record("Phase 8 CatalogCommit page data length overflow")
            })?;
            if data_end > buf.len() {
                return Err(invalid_log_record(
                    "Phase 8 CatalogCommit payload truncated page data",
                ));
            }
            pages.push(CatalogCommitPage {
                page_number,
                page_size,
                data: buf[cursor..data_end].to_vec(),
            });
            cursor = data_end;
        }
        if cursor != buf.len() {
            return Err(invalid_log_record(
                "Phase 8 CatalogCommit payload has trailing bytes",
            ));
        }
        Ok(Self {
            kind,
            catalog_generation_before,
            catalog_generation_after,
            header,
            pages,
        })
    }
}

/// Typed payload carried by `LogRecordKind::CheckpointBoundary`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointBoundaryPayload {
    /// Highest byte-LSN durably materialized into the main file.
    pub(crate) checkpoint_applied_lsn: u64,
    /// Identifier of the checkpoint batch closed by this boundary.
    /// Recovery uses this to drain accumulated `CheckpointPageFrame` records.
    pub(crate) batch_id: u64,
    /// Header image written and fsynced before the boundary record.
    pub(crate) header: FileHeader,
}

impl CheckpointBoundaryPayload {
    /// Encode this checkpoint boundary payload.
    ///
    /// # Errors
    ///
    /// Returns an error if the header length cannot be encoded.
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        if self.header.checkpoint_applied_lsn != self.checkpoint_applied_lsn {
            return Err(invalid_log_record(
                "Phase 8 CheckpointBoundary header checkpoint_applied_lsn mismatch",
            ));
        }
        let mut out = Vec::new();
        out.extend_from_slice(&CHECKPOINT_BOUNDARY_PAYLOAD_MAGIC);
        out.extend_from_slice(&CHECKPOINT_BOUNDARY_PAYLOAD_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes());
        out.extend_from_slice(&self.checkpoint_applied_lsn.to_le_bytes());
        out.extend_from_slice(&self.batch_id.to_le_bytes());
        out.extend_from_slice(&len_to_u32(HEADER_PAGE_SIZE)?.to_le_bytes());
        out.extend_from_slice(&self.header.to_bytes());
        Ok(out)
    }

    /// Decode and validate a checkpoint boundary payload.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CorruptDatabase`] if the payload is malformed.
    pub(crate) fn decode(buf: &[u8]) -> Result<Self> {
        const PREFIX_LEN: usize = 28;
        if buf.len() < PREFIX_LEN {
            return Err(invalid_log_record(
                "Phase 8 CheckpointBoundary payload truncated fixed header",
            ));
        }
        if buf[0..4] != CHECKPOINT_BOUNDARY_PAYLOAD_MAGIC {
            return Err(invalid_log_record("Phase 8 CheckpointBoundary bad magic"));
        }
        let version = read_u16(buf, 4);
        if version != CHECKPOINT_BOUNDARY_PAYLOAD_VERSION {
            return Err(invalid_log_record(format!(
                "Phase 8 CheckpointBoundary bad version {version}"
            )));
        }
        if read_u16(buf, 6) != 0 {
            return Err(invalid_log_record(
                "Phase 8 CheckpointBoundary reserved field must be zero",
            ));
        }
        let checkpoint_applied_lsn = read_u64(buf, 8);
        let batch_id = read_u64(buf, 16);
        let header_len = read_u32(buf, 24) as usize;
        if header_len != HEADER_PAGE_SIZE {
            return Err(invalid_log_record(format!(
                "Phase 8 CheckpointBoundary header length {header_len} is invalid"
            )));
        }
        if buf.len() != PREFIX_LEN + HEADER_PAGE_SIZE {
            return Err(invalid_log_record(
                "Phase 8 CheckpointBoundary payload length mismatch",
            ));
        }
        let header_bytes: &[u8; HEADER_PAGE_SIZE] =
            buf[PREFIX_LEN..].try_into().expect("validated header len");
        let header = FileHeader::from_bytes(header_bytes)?;
        if header.checkpoint_applied_lsn != checkpoint_applied_lsn {
            return Err(invalid_log_record(
                "Phase 8 CheckpointBoundary header checkpoint_applied_lsn mismatch",
            ));
        }
        Ok(Self {
            checkpoint_applied_lsn,
            batch_id,
            header,
        })
    }
}

/// Pool that produced a checkpoint per-page record.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum CheckpointPagePool {
    /// Main data/catalog buffer pool.
    Main,
    /// Dedicated history-store buffer pool.
    History,
}

impl CheckpointPagePool {
    fn wire_value(self) -> u8 {
        match self {
            Self::Main => 0,
            Self::History => 1,
        }
    }

    fn from_wire(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Main),
            1 => Ok(Self::History),
            _ => Err(invalid_log_record(format!(
                "Phase 8 CheckpointPageFrame unknown pool {value}"
            ))),
        }
    }
}

const CHECKPOINT_PAGE_FRAME_PAYLOAD_MAGIC: [u8; 4] = *b"MQCP";
const CHECKPOINT_PAGE_FRAME_PAYLOAD_VERSION: u16 = 1;

/// Typed payload carried by `LogRecordKind::CheckpointPageFrame`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointPageFramePayload {
    /// Identifier of the checkpoint batch this page belongs to.
    pub(crate) batch_id: u64,
    /// Pool that owns the page (main or history).
    pub(crate) pool: CheckpointPagePool,
    /// Page number in the main database file.
    pub(crate) page_number: u32,
    /// Physical page size of `data`.
    pub(crate) page_size: JournalPageSize,
    /// Full page bytes captured from the dirty frame snapshot.
    pub(crate) data: Vec<u8>,
}

impl CheckpointPageFramePayload {
    /// Encode the payload bytes for `LogRecordPayload::CheckpointPageFrame`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CorruptDatabase`] if `data` does not match `page_size`.
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        if self.data.len() != self.page_size.bytes() {
            return Err(invalid_log_record(format!(
                "Phase 8 CheckpointPageFrame page {} data length {} does not match {:?}",
                self.page_number,
                self.data.len(),
                self.page_size
            )));
        }
        let mut out = Vec::with_capacity(28 + self.data.len());
        out.extend_from_slice(&CHECKPOINT_PAGE_FRAME_PAYLOAD_MAGIC);
        out.extend_from_slice(&CHECKPOINT_PAGE_FRAME_PAYLOAD_VERSION.to_le_bytes());
        out.push(self.pool.wire_value());
        out.push(0);
        out.extend_from_slice(&self.batch_id.to_le_bytes());
        out.extend_from_slice(&self.page_number.to_le_bytes());
        out.extend_from_slice(&self.page_size.as_u32().to_le_bytes());
        out.extend_from_slice(&len_to_u32(self.data.len())?.to_le_bytes());
        out.extend_from_slice(&self.data);
        Ok(out)
    }

    /// Decode and validate a `CheckpointPageFrame` payload.
    ///
    /// # Errors
    ///
    /// Returns [`Error::CorruptDatabase`] if the payload is malformed.
    pub(crate) fn decode(buf: &[u8]) -> Result<Self> {
        const PREFIX_LEN: usize = 24;
        if buf.len() < PREFIX_LEN {
            return Err(invalid_log_record(
                "Phase 8 CheckpointPageFrame payload truncated fixed header",
            ));
        }
        if buf[0..4] != CHECKPOINT_PAGE_FRAME_PAYLOAD_MAGIC {
            return Err(invalid_log_record("Phase 8 CheckpointPageFrame bad magic"));
        }
        let version = read_u16(buf, 4);
        if version != CHECKPOINT_PAGE_FRAME_PAYLOAD_VERSION {
            return Err(invalid_log_record(format!(
                "Phase 8 CheckpointPageFrame bad version {version}"
            )));
        }
        let pool = CheckpointPagePool::from_wire(buf[6])?;
        if buf[7] != 0 {
            return Err(invalid_log_record(
                "Phase 8 CheckpointPageFrame reserved byte must be zero",
            ));
        }
        let batch_id = read_u64(buf, 8);
        let page_number = read_u32(buf, 16);
        let page_size = JournalPageSize::from_u32(read_u32(buf, 20))?;
        let data_len = read_u32(buf, PREFIX_LEN) as usize;
        let data_start = PREFIX_LEN + 4;
        if data_len != page_size.bytes() {
            return Err(invalid_log_record(format!(
                "Phase 8 CheckpointPageFrame page {page_number} data length {data_len} \
                 does not match {:?}",
                page_size
            )));
        }
        if buf.len() != data_start + data_len {
            return Err(invalid_log_record(
                "Phase 8 CheckpointPageFrame payload has wrong trailing length",
            ));
        }
        let data = buf[data_start..].to_vec();
        Ok(Self {
            batch_id,
            pool,
            page_number,
            page_size,
            data,
        })
    }
}

struct LogRecordHeader {
    kind: LogRecordKind,
    flags: LogRecordFlags,
    total_len: usize,
    start_lsn: u64,
    end_lsn: u64,
    txn_id: u64,
    publish_seq: u64,
    commit_ts: Ts,
    payload_len: usize,
    payload_crc32c: u32,
}

impl LogRecordHeader {
    fn write_with_header_crc(&self, header_crc32c: u32, out: &mut [u8]) -> Result<()> {
        debug_assert_eq!(out.len(), LOG_RECORD_HEADER_LEN);
        out[LOG_RECORD_MAGIC_OFFSET..LOG_RECORD_MAGIC_OFFSET + 4]
            .copy_from_slice(&LOG_RECORD_MAGIC.to_le_bytes());
        out[LOG_RECORD_FORMAT_VERSION_OFFSET..LOG_RECORD_FORMAT_VERSION_OFFSET + 2]
            .copy_from_slice(&LOG_RECORD_FORMAT_VERSION.to_le_bytes());
        out[LOG_RECORD_HEADER_LEN_OFFSET..LOG_RECORD_HEADER_LEN_OFFSET + 2]
            .copy_from_slice(&len_to_u16(LOG_RECORD_HEADER_LEN)?.to_le_bytes());
        out[LOG_RECORD_KIND_OFFSET..LOG_RECORD_KIND_OFFSET + 2]
            .copy_from_slice(&self.kind.wire_value().to_le_bytes());
        out[LOG_RECORD_FLAGS_OFFSET..LOG_RECORD_FLAGS_OFFSET + 2]
            .copy_from_slice(&self.flags.bits().to_le_bytes());
        out[LOG_RECORD_TOTAL_LEN_OFFSET..LOG_RECORD_TOTAL_LEN_OFFSET + 4]
            .copy_from_slice(&len_to_u32(self.total_len)?.to_le_bytes());
        out[LOG_RECORD_START_LSN_OFFSET..LOG_RECORD_START_LSN_OFFSET + 8]
            .copy_from_slice(&self.start_lsn.to_le_bytes());
        out[LOG_RECORD_END_LSN_OFFSET..LOG_RECORD_END_LSN_OFFSET + 8]
            .copy_from_slice(&self.end_lsn.to_le_bytes());
        out[LOG_RECORD_TXN_ID_OFFSET..LOG_RECORD_TXN_ID_OFFSET + 8]
            .copy_from_slice(&self.txn_id.to_le_bytes());
        out[LOG_RECORD_PUBLISH_SEQ_OFFSET..LOG_RECORD_PUBLISH_SEQ_OFFSET + 8]
            .copy_from_slice(&self.publish_seq.to_le_bytes());
        out[LOG_RECORD_COMMIT_TS_PHYSICAL_OFFSET..LOG_RECORD_COMMIT_TS_PHYSICAL_OFFSET + 8]
            .copy_from_slice(&self.commit_ts.physical_ms.to_le_bytes());
        out[LOG_RECORD_COMMIT_TS_LOGICAL_OFFSET..LOG_RECORD_COMMIT_TS_LOGICAL_OFFSET + 4]
            .copy_from_slice(&self.commit_ts.logical.to_le_bytes());
        out[LOG_RECORD_PAYLOAD_LEN_OFFSET..LOG_RECORD_PAYLOAD_LEN_OFFSET + 4]
            .copy_from_slice(&len_to_u32(self.payload_len)?.to_le_bytes());
        out[LOG_RECORD_HEADER_CRC32C_OFFSET..LOG_RECORD_HEADER_CRC32C_OFFSET + 4]
            .copy_from_slice(&header_crc32c.to_le_bytes());
        out[LOG_RECORD_PAYLOAD_CRC32C_OFFSET..LOG_RECORD_PAYLOAD_CRC32C_OFFSET + 4]
            .copy_from_slice(&self.payload_crc32c.to_le_bytes());
        Ok(())
    }
}

fn read_u16(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(buf[offset..offset + 2].try_into().expect("2 bytes"))
}

fn read_u32(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(buf[offset..offset + 4].try_into().expect("4 bytes"))
}

fn read_u64(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(buf[offset..offset + 8].try_into().expect("8 bytes"))
}

fn len_to_u16(len: usize) -> Result<u16> {
    u16::try_from(len).map_err(|_| invalid_log_record("Phase 8 LogRecord length exceeds u16"))
}

fn len_to_u32(len: usize) -> Result<u32> {
    u32::try_from(len).map_err(|_| invalid_log_record("Phase 8 LogRecord length exceeds u32"))
}

fn log_record_too_large(total_len: usize) -> Error {
    Error::JournalFrameTooLarge {
        logical_frame_bytes: total_len,
        max_bytes: MAX_LOG_RECORD_BYTES,
    }
}

fn invalid_log_record(detail: impl Into<String>) -> Error {
    Error::CorruptDatabase {
        path: std::path::PathBuf::new(),
        detail: detail.into(),
        recoverable: true,
    }
}

// ---------------------------------------------------------------------------
// Frame kinds
// ---------------------------------------------------------------------------
//
// Frame-kind bytes identify MVCC `ChainCommit` and logical-transaction frames.
// Retired page-write records keep their original page-frame layout and do not
// carry a frame-kind byte at a known position. Byte layout for `ChainCommit`:
//
//   offset  size  field
//    0       1    frame_kind: u8 (0x02 = CHAIN_COMMIT)
//    1       3    reserved: [u8; 3] (MUST be 0)
//    4       4    total_frame_bytes: u32 LE (length prefix)
//    8       4    salt1: u32 LE
//   12       4    salt2: u32 LE
//   16      12    commit_ts: Ts-LE (physical_ms u64 LE || logical u32 LE)
//   28       4    refcount_delta_count: u32 LE
//   32       N    refcount_deltas: [(page: u32, delta: i32)] × count
//   32+N     4    page_write_count: u32 LE
//   36+N     M    page_writes[]
//   36+N+M   4    checksum_crc32: u32 LE (covers bytes 0..36+N+M)

/// Frame-kind discriminant for MVCC chain-commit frames.
#[allow(dead_code)]
pub(crate) const FRAME_KIND_CHAIN_COMMIT: u8 = 0x02;

/// Byte offset of the fixed-size `ChainCommit` header prefix end
/// (through `refcount_delta_count`, exclusive of the variable-length tail).
#[allow(dead_code)]
pub(crate) const CHAIN_COMMIT_FIXED_HEADER_LEN: usize = 32;

/// Hard cap on `ChainCommit.total_frame_bytes` used during recovery to reject
/// nonsense lengths before any allocation.
#[allow(dead_code)]
pub(crate) const CHAIN_COMMIT_MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

/// Frame-kind discriminant for Phase 2 logical-transaction frames (§3, §4).
///
/// Reserved for the `LogicalTxnFrame` wire format added in Phase 2; the frame
/// is parsed and validated by recovery but never mutates durable state while
/// Phase 2 remains the active phase (see §3.3 authority window).
#[allow(dead_code)]
pub(crate) const FRAME_KIND_LOGICAL_TXN: u8 = 0x03;

/// Journal/main-file page id newtype (§3.11).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(dead_code)]
pub(crate) struct PageId(pub u32);

/// Byte offset into the journal file — return type of the append helpers.
#[allow(dead_code)]
pub(crate) type JournalOffset = u64;

/// Byte length of the fixed-size prefix of a `LogicalTxnFrame` header,
/// ending immediately before the per-op body (§4.1).
#[allow(dead_code)]
pub(crate) const LOGICAL_TXN_FIXED_HEADER_LEN: usize = 48;

/// Hard cap on `LogicalTxnFrame.total_frame_bytes` (§3.5).
///
/// Inherited from the `ChainCommit` cap; encoders must reject oversize frames
/// before any byte is appended to the journal.
#[allow(dead_code)]
pub(crate) const LOGICAL_TXN_MAX_FRAME_SIZE: usize = 64 * 1024 * 1024;

/// Minimum well-formed `LogicalTxnFrame.total_frame_bytes`: the 48-byte
/// fixed header plus the trailing 4-byte CRC32C (§4.1).
#[allow(dead_code)]
pub(crate) const LOGICAL_TXN_MIN_FRAME_SIZE: usize = LOGICAL_TXN_FIXED_HEADER_LEN + 4;

/// Hard cap on `LogicalTxnFrame.op_count` used during decode to reject
/// nonsense counts before any allocation.
#[allow(dead_code)]
pub(crate) const LOGICAL_TXN_MAX_OP_COUNT: usize = 1_000_000;

/// Hard cap on per-op key length in bytes (§4.6).
#[allow(dead_code)]
pub(crate) const LOGICAL_TXN_MAX_KEY_BYTES: usize = 16 * 1024;

/// Hard cap on per-op inline value length in bytes (§4.6).
///
/// Values exceeding this cap are spilled through the existing overflow-page
/// mechanism; the logical frame carries only an `OverflowRefWire` in that
/// case (§4.2).
#[allow(dead_code)]
pub(crate) const LOGICAL_TXN_MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;

/// Current `LogicalTxnFrame` format-version discriminant (§4.1).
#[allow(dead_code)]
pub(crate) const LOGICAL_TXN_FORMAT_VERSION: u16 = 1;

// ---------------------------------------------------------------------------
// LogicalTxnFrame types (Phase 2 §4.2)
// ---------------------------------------------------------------------------

/// Reference to an overflow chain carrying an inline-too-large value.
///
/// Emitted inside a `PrimaryInsert`/`PrimaryUpdate` op when the serialized
/// value exceeds [`LOGICAL_TXN_MAX_VALUE_BYTES`]; the logical frame carries
/// the overflow anchor, not the payload bytes (§4.2).
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct OverflowRefWire {
    /// Anchor page of the overflow chain.
    pub first_page: u32,
    /// Total byte length of the spilled value.
    pub total_len: u64,
}

/// Logical-op body discriminant.
///
/// Exactly five variants per §4.2 / §4.4 — `PrimaryInsert`, `PrimaryUpdate`,
/// `PrimaryDelete`, `SecondaryInsert`, `SecondaryDelete`. The encoded opcode
/// bytes (0x01 / 0x02 / 0x03 / 0x11 / 0x12) are emitted by the US-004
/// encoder; this type only models the in-memory shape.
///
/// `ns_id` and `index_id` are `i64` to match the durable BSON encoding of
/// `CollectionEntry.id` / `IndexEntry.id` resolved at stage time (§3.1a) —
/// never from data_root_page or root_page.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum LogicalOpKind {
    /// Primary-tree insert (key not previously present).
    PrimaryInsert {
        ns_id: i64,
        key: Vec<u8>,
        value: Vec<u8>,
        overflow: Option<OverflowRefWire>,
    },
    /// Primary-tree update (in-place overwrite of an existing key).
    PrimaryUpdate {
        ns_id: i64,
        key: Vec<u8>,
        value: Vec<u8>,
        overflow: Option<OverflowRefWire>,
    },
    /// Primary-tree delete (by key).
    PrimaryDelete { ns_id: i64, key: Vec<u8> },
    /// Secondary-index insert — `id_bytes` is the primary-key postfix.
    SecondaryInsert {
        index_id: i64,
        key: Vec<u8>,
        id_bytes: Vec<u8>,
    },
    /// Secondary-index delete (by composite key).
    SecondaryDelete { index_id: i64, key: Vec<u8> },
}

/// Single logical op inside a [`LogicalTxnFrame`].
///
/// `op_ordinal` is a dense `[0..op_count)` counter assigned at emit time in
/// staging order (§3.6); decoders enforce dense ordinals at parse time.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct LogicalOp {
    /// Dense ordinal within the containing frame.
    pub op_ordinal: u32,
    /// Op body.
    pub kind: LogicalOpKind,
}

/// Logical transaction frame (Phase 2 §3, §4).
///
/// Emitted once per successful write-txn commit between `allocate_commit_ts`
/// and `ChainCommit` (§3.7). No `refcount_deltas` field — refcount
/// bookkeeping remains `ChainCommitFrame`'s job per §3.4 / §3.9.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct LogicalTxnFrame {
    /// Database-lifetime salt 1; verified during recovery.
    pub salt1: u32,
    /// Database-lifetime salt 2; verified during recovery.
    pub salt2: u32,
    /// Commit timestamp from [`WriteTxn::allocate_commit_ts`]. NEVER folded
    /// into `recovered_max_commit_ts` — only `ChainCommit` advances the HLC
    /// floor (§3.10).
    pub commit_ts: Ts,
    /// Diagnostic-only transaction id; not used for recovery identity.
    pub diagnostic_txn_id: u64,
    /// Wire-format version; must equal [`LOGICAL_TXN_FORMAT_VERSION`].
    pub format_version: u16,
    /// Flag word; reserved and MUST be zero in format version 1 (§4.6).
    pub flags: u16,
    /// Op list in staging order — secondary writes first, primary writes
    /// second per §3.6 emit-side convention.
    pub ops: Vec<LogicalOp>,
}

/// Decoder-context tag discriminating scanner-positioned parses from
/// mid-stream parses (§4.6).
///
/// `Scanning` rewinds on every failure row in the §4.6 disposition table;
/// `MidStream` returns `Error::CorruptDatabase` for the content-error rows
/// and `Ok(None)` only for salt mismatch (different database lifetime —
/// never corruption). The `follower` flag marks a confirmed follower frame
/// whose content error must surface as a decode error rather than a
/// tail-truncation rewind.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum DecodeCtx {
    /// Parser is reading forward from an uncommitted cursor and any failure
    /// row must rewind + return `Ok(None)`.
    Scanning,
    /// Parser has already confirmed a follower frame is present and should
    /// surface content errors rather than silently rewind.
    MidStream { follower: bool },
}

// ---------------------------------------------------------------------------
// LogicalTxnFrame encoder (Phase 2 §4.5)
// ---------------------------------------------------------------------------

/// Per-op wire-format opcode bytes from §4.4.
const OP_KIND_PRIMARY_INSERT: u8 = 0x01;
const OP_KIND_PRIMARY_UPDATE: u8 = 0x02;
const OP_KIND_PRIMARY_DELETE: u8 = 0x03;
const OP_KIND_SECONDARY_INSERT: u8 = 0x11;
const OP_KIND_SECONDARY_DELETE: u8 = 0x12;

/// Fixed 8-byte per-op prefix: op_kind(1) + reserved(3) + op_ordinal(4).
const LOGICAL_OP_PREFIX_LEN: usize = 8;

/// Byte length of the serialized `OverflowRefWire` tail on primary
/// insert/update ops (first_page: u32 + total_len: u64).
const OVERFLOW_REF_WIRE_LEN: usize = 12;

impl LogicalOpKind {
    /// Opcode byte written at offset 0 of each op body per §4.4.
    fn opcode(&self) -> u8 {
        match self {
            LogicalOpKind::PrimaryInsert { .. } => OP_KIND_PRIMARY_INSERT,
            LogicalOpKind::PrimaryUpdate { .. } => OP_KIND_PRIMARY_UPDATE,
            LogicalOpKind::PrimaryDelete { .. } => OP_KIND_PRIMARY_DELETE,
            LogicalOpKind::SecondaryInsert { .. } => OP_KIND_SECONDARY_INSERT,
            LogicalOpKind::SecondaryDelete { .. } => OP_KIND_SECONDARY_DELETE,
        }
    }

    /// Encoded length of the op-kind-specific body (excludes the shared
    /// 8-byte prefix). Per §4.4 each body begins with the 8-byte ns_id or
    /// index_id, then the variable-length key/value/id_bytes fields.
    fn body_len(&self) -> usize {
        match self {
            LogicalOpKind::PrimaryInsert {
                key,
                value,
                overflow,
                ..
            }
            | LogicalOpKind::PrimaryUpdate {
                key,
                value,
                overflow,
                ..
            } => {
                let overflow_len = if overflow.is_some() {
                    OVERFLOW_REF_WIRE_LEN
                } else {
                    0
                };
                8 + 4 + key.len() + 4 + value.len() + 1 + overflow_len
            }
            LogicalOpKind::PrimaryDelete { key, .. } => 8 + 4 + key.len(),
            LogicalOpKind::SecondaryInsert { key, id_bytes, .. } => {
                8 + 4 + key.len() + 4 + id_bytes.len()
            }
            LogicalOpKind::SecondaryDelete { key, .. } => 8 + 4 + key.len(),
        }
    }

    /// Write the op-kind-specific body bytes (no shared prefix) per §4.4.
    fn encode_body_into(&self, buf: &mut Vec<u8>) {
        match self {
            LogicalOpKind::PrimaryInsert {
                ns_id,
                key,
                value,
                overflow,
            }
            | LogicalOpKind::PrimaryUpdate {
                ns_id,
                key,
                value,
                overflow,
            } => {
                buf.extend_from_slice(&ns_id.to_le_bytes());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
                match overflow {
                    None => buf.push(0),
                    Some(r) => {
                        buf.push(1);
                        buf.extend_from_slice(&r.first_page.to_le_bytes());
                        buf.extend_from_slice(&r.total_len.to_le_bytes());
                    }
                }
            }
            LogicalOpKind::PrimaryDelete { ns_id, key } => {
                buf.extend_from_slice(&ns_id.to_le_bytes());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
            }
            LogicalOpKind::SecondaryInsert {
                index_id,
                key,
                id_bytes,
            } => {
                buf.extend_from_slice(&index_id.to_le_bytes());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(&(id_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(id_bytes);
            }
            LogicalOpKind::SecondaryDelete { index_id, key } => {
                buf.extend_from_slice(&index_id.to_le_bytes());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
            }
        }
    }
}

impl LogicalOp {
    /// Total encoded byte length (shared 8-byte prefix + body).
    fn encoded_len(&self) -> usize {
        LOGICAL_OP_PREFIX_LEN + self.kind.body_len()
    }

    /// Append the wire-format bytes for this op to `buf` per §4.4.
    fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.push(self.kind.opcode());
        buf.extend_from_slice(&[0u8; 3]); // reserved
        buf.extend_from_slice(&self.op_ordinal.to_le_bytes());
        self.kind.encode_body_into(buf);
    }
}

impl LogicalTxnFrame {
    fn validate_encode_limits(&self, total: usize) -> Result<()> {
        if self.ops.len() > LOGICAL_TXN_MAX_OP_COUNT {
            return Err(Error::JournalFrameTooLarge {
                logical_frame_bytes: total,
                max_bytes: LOGICAL_TXN_MAX_FRAME_SIZE,
            });
        }

        for op in &self.ops {
            match &op.kind {
                LogicalOpKind::PrimaryInsert { key, value, .. }
                | LogicalOpKind::PrimaryUpdate { key, value, .. } => {
                    Self::validate_inline_len(key.len(), LOGICAL_TXN_MAX_KEY_BYTES)?;
                    Self::validate_inline_len(value.len(), LOGICAL_TXN_MAX_VALUE_BYTES)?;
                }
                LogicalOpKind::PrimaryDelete { key, .. }
                | LogicalOpKind::SecondaryDelete { key, .. } => {
                    Self::validate_inline_len(key.len(), LOGICAL_TXN_MAX_KEY_BYTES)?;
                }
                LogicalOpKind::SecondaryInsert { key, id_bytes, .. } => {
                    Self::validate_inline_len(key.len(), LOGICAL_TXN_MAX_KEY_BYTES)?;
                    Self::validate_inline_len(id_bytes.len(), LOGICAL_TXN_MAX_VALUE_BYTES)?;
                }
            }
        }

        Ok(())
    }

    fn validate_inline_len(len: usize, max: usize) -> Result<()> {
        if len > max {
            return Err(Error::JournalFrameTooLarge {
                logical_frame_bytes: len,
                max_bytes: max,
            });
        }
        Ok(())
    }

    /// Compute the total encoded byte size (`total_frame_bytes`) per §4.5.
    fn total_frame_bytes(&self) -> usize {
        let ops_len: usize = self.ops.iter().map(LogicalOp::encoded_len).sum();
        // Fixed header + ops + trailing CRC32C.
        LOGICAL_TXN_FIXED_HEADER_LEN + ops_len + 4
    }

    /// Encode this frame to the §4.1 byte layout.
    ///
    /// Returns `Err(Error::JournalFrameTooLarge)` before any byte is
    /// appended when the computed `total_frame_bytes` exceeds
    /// `LOGICAL_TXN_MAX_FRAME_SIZE`. On success the returned vector is
    /// exactly `total_frame_bytes` bytes long and its last 4 bytes are the
    /// CRC32C of the preceding body per §4.1.
    #[allow(dead_code)]
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let total = self.total_frame_bytes();
        if total > LOGICAL_TXN_MAX_FRAME_SIZE {
            return Err(Error::JournalFrameTooLarge {
                logical_frame_bytes: total,
                max_bytes: LOGICAL_TXN_MAX_FRAME_SIZE,
            });
        }
        self.validate_encode_limits(total)?;
        // Safe: total ≤ LOGICAL_TXN_MAX_FRAME_SIZE (64 MiB) fits in u32.
        let total_u32 = total as u32;
        let op_count_u32 = self.ops.len() as u32;

        let mut buf = Vec::with_capacity(total);
        buf.push(FRAME_KIND_LOGICAL_TXN);
        buf.extend_from_slice(&[0u8; 3]); // reserved_a
        buf.extend_from_slice(&total_u32.to_le_bytes());
        buf.extend_from_slice(&self.salt1.to_le_bytes());
        buf.extend_from_slice(&self.salt2.to_le_bytes());
        buf.extend_from_slice(&self.commit_ts.to_le_bytes()); // 12 B
        buf.extend_from_slice(&self.diagnostic_txn_id.to_le_bytes());
        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.extend_from_slice(&self.flags.to_le_bytes());
        buf.extend_from_slice(&op_count_u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // reserved_b (§3.9)

        debug_assert_eq!(buf.len(), LOGICAL_TXN_FIXED_HEADER_LEN);
        for op in &self.ops {
            op.encode_into(&mut buf);
        }

        debug_assert_eq!(buf.len(), total - 4, "CRC not yet appended");
        let cs = crc32c::crc32c(&buf);
        buf.extend_from_slice(&cs.to_le_bytes());
        debug_assert_eq!(buf.len(), total);

        Ok(buf)
    }

    /// Decode a `LogicalTxnFrame` from the leading bytes of `buf` per §4.5.
    ///
    /// The decoder honors the full §4.6 disposition table, split by caller
    /// context:
    ///
    /// - [`DecodeCtx::Scanning`] returns `Ok(None)` on every failure row
    ///   (truncation, wrong `frame_kind`, reserved-non-zero, out-of-range
    ///   length, salt or CRC mismatch, unknown `format_version`, non-zero
    ///   `flags`, over-cap `op_count`, non-zero `reserved_b`, over-cap
    ///   key/value, unknown `op_kind`, `op_ordinal >= op_count`, non-dense
    ///   ordinals, invalid `overflow_present`, EOF mid-body).
    /// - [`DecodeCtx::MidStream`] returns `Err(Error::CorruptDatabase)` for
    ///   every content-error row that cannot be explained by tail
    ///   truncation, and `Ok(None)` only for the tail-like rows
    ///   (truncated-mid-header, `frame_kind` mismatch, salt mismatch, EOF
    ///   mid-body). `recoverable = false` applies only to the unknown
    ///   `format_version` row; every other content error is
    ///   `recoverable = true`.
    ///
    /// No panic and no unbounded allocation on any adversarial input —
    /// every size field is bounds-checked before any `Vec::with_capacity`
    /// per §4.6.
    ///
    /// # Arguments
    /// - `buf`: candidate frame bytes; MAY be longer than the frame.
    /// - `expected_salt1`: journal-header salt 1 for this database lifetime.
    /// - `expected_salt2`: journal-header salt 2 for this database lifetime.
    /// - `ctx`: disposition tag per §4.6.
    ///
    /// # Returns
    /// - `Ok(Some(frame))` when the full frame is valid.
    /// - `Ok(None)` per §4.6 for tail-like rows or any Scanning-context
    ///   failure.
    /// - `Err(Error::CorruptDatabase { .. })` per §4.6 in MidStream
    ///   context only.
    #[allow(dead_code)]
    pub(crate) fn decode(
        buf: &[u8],
        expected_salt1: u32,
        expected_salt2: u32,
        ctx: DecodeCtx,
    ) -> Result<Option<Self>> {
        // 1. Fixed-header bounds. Tail-like; always Ok(None).
        if buf.len() < LOGICAL_TXN_FIXED_HEADER_LEN {
            return Ok(None);
        }

        // 2. frame_kind discriminant. Dispatch mismatch; always Ok(None).
        if buf[0] != FRAME_KIND_LOGICAL_TXN {
            return Ok(None);
        }

        // 3. reserved_a at bytes 1..4 MUST be zero.
        if buf[1] != 0 || buf[2] != 0 || buf[3] != 0 {
            return dispose(
                &ctx,
                true,
                "LogicalTxnFrame reserved_a non-zero".to_string(),
            );
        }

        // 4. total_frame_bytes bound check before any allocation.
        let total = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes")) as usize;
        if !(LOGICAL_TXN_MIN_FRAME_SIZE..=LOGICAL_TXN_MAX_FRAME_SIZE).contains(&total) {
            return dispose(
                &ctx,
                true,
                format!("LogicalTxnFrame total_frame_bytes {total} out of range"),
            );
        }

        // 5. EOF mid-body: buffer shorter than declared length. Always Ok(None).
        if buf.len() < total {
            return Ok(None);
        }

        // 6. Salt mismatch — Ok(None) in EVERY context per §4.6.
        let salt1 = u32::from_le_bytes(buf[8..12].try_into().expect("4 bytes"));
        let salt2 = u32::from_le_bytes(buf[12..16].try_into().expect("4 bytes"));
        if salt1 != expected_salt1 || salt2 != expected_salt2 {
            return Ok(None);
        }

        // 7. CRC32C over bytes [0 .. total - 4).
        let body_end = total - 4;
        let stored_cs = u32::from_le_bytes(buf[body_end..total].try_into().expect("4 bytes"));
        let computed_cs = crc32c::crc32c(&buf[..body_end]);
        if stored_cs != computed_cs {
            return dispose(&ctx, true, "LogicalTxnFrame CRC32C mismatch".to_string());
        }

        // 8. Parse fixed-header content after CRC gate.
        let commit_ts = Ts::from_le_bytes(buf[16..28].try_into().expect("12 bytes"));
        let diagnostic_txn_id = u64::from_le_bytes(buf[28..36].try_into().expect("8 bytes"));
        let format_version = u16::from_le_bytes(buf[36..38].try_into().expect("2 bytes"));
        let flags = u16::from_le_bytes(buf[38..40].try_into().expect("2 bytes"));
        let op_count = u32::from_le_bytes(buf[40..44].try_into().expect("4 bytes"));
        let reserved_b = u32::from_le_bytes(buf[44..48].try_into().expect("4 bytes"));

        // 9. format_version — only unknown is recoverable:false.
        if format_version != LOGICAL_TXN_FORMAT_VERSION {
            return dispose(
                &ctx,
                false,
                format!("LogicalTxnFrame unknown format_version {format_version}"),
            );
        }

        // 10. flags MUST be zero in format version 1.
        if flags != 0 {
            return dispose(
                &ctx,
                true,
                format!("LogicalTxnFrame flags {flags:#x} non-zero"),
            );
        }

        // 11. op_count bound check before any allocation.
        let op_count_usize = op_count as usize;
        if op_count_usize > LOGICAL_TXN_MAX_OP_COUNT {
            return dispose(
                &ctx,
                true,
                format!(
                    "LogicalTxnFrame op_count {op_count} exceeds \
                     LOGICAL_TXN_MAX_OP_COUNT {LOGICAL_TXN_MAX_OP_COUNT}"
                ),
            );
        }

        // 12. reserved_b (bytes 44..48) MUST be zero per §3.9.
        if reserved_b != 0 {
            return dispose(
                &ctx,
                true,
                format!("LogicalTxnFrame reserved_b {reserved_b:#x} non-zero"),
            );
        }

        // 13. Walk ops against the CRC-verified body, enforcing §4.6
        //     per-op disposition + dense ordinals.
        let mut cursor = LOGICAL_TXN_FIXED_HEADER_LEN;
        let ops = match parse_ops(&buf[..body_end], &mut cursor, op_count_usize, &ctx)? {
            Some(ops) => ops,
            None => return Ok(None),
        };
        if cursor != body_end {
            // Ops consumed less than the declared body — malformed.
            // Tail-safe: return Ok(None) in both contexts (matches
            // ChainCommit's trailing-bytes behavior).
            return Ok(None);
        }

        Ok(Some(LogicalTxnFrame {
            salt1,
            salt2,
            commit_ts,
            diagnostic_txn_id,
            format_version,
            flags,
            ops,
        }))
    }
}

/// Walk the per-op table inside the CRC-verified logical-frame body.
///
/// Enforces dense ordinals in `0..op_count` (no gaps, no duplicates) per
/// §4.6. Bounds-checks every length field before any allocation.
///
/// # Arguments
/// - `body`: slice ending at `body_end` (CRC excluded).
/// - `cursor`: in/out byte offset within `body`.
/// - `op_count`: already bounded to `LOGICAL_TXN_MAX_OP_COUNT`.
/// - `ctx`: §4.6 disposition tag.
///
/// # Returns
/// - `Ok(Some(ops))` when every op parses cleanly with dense ordinals.
/// - `Ok(None)` on EOF mid-body (tail-like failure).
/// - `Err(Error::CorruptDatabase)` in MidStream context for content errors.
fn parse_ops(
    body: &[u8],
    cursor: &mut usize,
    op_count: usize,
    ctx: &DecodeCtx,
) -> Result<Option<Vec<LogicalOp>>> {
    // Safe: op_count ≤ LOGICAL_TXN_MAX_OP_COUNT (checked by caller).
    let mut ops = Vec::with_capacity(op_count);
    let mut seen_ordinals = vec![false; op_count];

    for _ in 0..op_count {
        // Shared 8-byte prefix: op_kind(1) + reserved(3) + op_ordinal(4).
        if body.len().saturating_sub(*cursor) < LOGICAL_OP_PREFIX_LEN {
            return Ok(None);
        }
        let op_kind = body[*cursor];
        // Per §4.4 table row 1..4: op-prefix reserved bytes MUST be zero.
        // §4.6 disposition applies (Scanning Ok(None); MidStream
        // Err(recoverable:true)).
        if body[*cursor + 1] != 0 || body[*cursor + 2] != 0 || body[*cursor + 3] != 0 {
            return dispose(
                ctx,
                true,
                "LogicalTxnFrame op-prefix reserved bytes non-zero".to_string(),
            );
        }
        let op_ordinal =
            u32::from_le_bytes(body[*cursor + 4..*cursor + 8].try_into().expect("4 bytes"));
        *cursor += LOGICAL_OP_PREFIX_LEN;

        // Ordinal range + denseness per §4.6.
        if (op_ordinal as usize) >= op_count {
            return dispose(
                ctx,
                true,
                format!("LogicalTxnFrame op_ordinal {op_ordinal} >= op_count {op_count}"),
            );
        }
        if seen_ordinals[op_ordinal as usize] {
            return dispose(
                ctx,
                true,
                format!("LogicalTxnFrame non-dense op_ordinal {op_ordinal} duplicated"),
            );
        }
        seen_ordinals[op_ordinal as usize] = true;

        // Op-kind-specific body.
        let kind_opt = match op_kind {
            OP_KIND_PRIMARY_INSERT => {
                parse_primary_write_body(body, cursor, ctx, /*is_insert=*/ true)?
            }
            OP_KIND_PRIMARY_UPDATE => {
                parse_primary_write_body(body, cursor, ctx, /*is_insert=*/ false)?
            }
            OP_KIND_PRIMARY_DELETE => parse_primary_delete_body(body, cursor, ctx)?,
            OP_KIND_SECONDARY_INSERT => parse_secondary_insert_body(body, cursor, ctx)?,
            OP_KIND_SECONDARY_DELETE => parse_secondary_delete_body(body, cursor, ctx)?,
            _ => {
                return dispose(
                    ctx,
                    true,
                    format!("LogicalTxnFrame unknown op_kind {op_kind:#04x}"),
                );
            }
        };
        let kind = match kind_opt {
            Some(k) => k,
            None => return Ok(None),
        };

        ops.push(LogicalOp { op_ordinal, kind });
    }

    Ok(Some(ops))
}

/// Parse the `(ns_id, key, value, overflow)` body for `PrimaryInsert` or
/// `PrimaryUpdate`. Returns `Ok(None)` on EOF, `Err` in MidStream on
/// content errors.
fn parse_primary_write_body(
    body: &[u8],
    cursor: &mut usize,
    ctx: &DecodeCtx,
    is_insert: bool,
) -> Result<Option<LogicalOpKind>> {
    let Some(ns_id) = read_i64_le(body, cursor) else {
        return Ok(None);
    };
    let Some(key) = read_length_prefixed(body, cursor, LOGICAL_TXN_MAX_KEY_BYTES, ctx, "key")?
    else {
        return Ok(None);
    };
    let Some(value) =
        read_length_prefixed(body, cursor, LOGICAL_TXN_MAX_VALUE_BYTES, ctx, "value")?
    else {
        return Ok(None);
    };
    if body.len().saturating_sub(*cursor) < 1 {
        return Ok(None);
    }
    let present = body[*cursor];
    *cursor += 1;
    let overflow = match present {
        0 => None,
        1 => {
            if body.len().saturating_sub(*cursor) < OVERFLOW_REF_WIRE_LEN {
                return Ok(None);
            }
            let first_page =
                u32::from_le_bytes(body[*cursor..*cursor + 4].try_into().expect("4 bytes"));
            let total_len =
                u64::from_le_bytes(body[*cursor + 4..*cursor + 12].try_into().expect("8 bytes"));
            *cursor += OVERFLOW_REF_WIRE_LEN;
            Some(OverflowRefWire {
                first_page,
                total_len,
            })
        }
        _ => {
            return dispose(
                ctx,
                true,
                format!(
                    "LogicalTxnFrame overflow_present byte {present:#04x} \
                     not in {{0, 1}}"
                ),
            );
        }
    };
    Ok(Some(if is_insert {
        LogicalOpKind::PrimaryInsert {
            ns_id,
            key,
            value,
            overflow,
        }
    } else {
        LogicalOpKind::PrimaryUpdate {
            ns_id,
            key,
            value,
            overflow,
        }
    }))
}

/// Parse the `(ns_id, key)` body for `PrimaryDelete`.
fn parse_primary_delete_body(
    body: &[u8],
    cursor: &mut usize,
    ctx: &DecodeCtx,
) -> Result<Option<LogicalOpKind>> {
    let Some(ns_id) = read_i64_le(body, cursor) else {
        return Ok(None);
    };
    let Some(key) = read_length_prefixed(body, cursor, LOGICAL_TXN_MAX_KEY_BYTES, ctx, "key")?
    else {
        return Ok(None);
    };
    Ok(Some(LogicalOpKind::PrimaryDelete { ns_id, key }))
}

/// Parse the `(index_id, key, id_bytes)` body for `SecondaryInsert`.
fn parse_secondary_insert_body(
    body: &[u8],
    cursor: &mut usize,
    ctx: &DecodeCtx,
) -> Result<Option<LogicalOpKind>> {
    let Some(index_id) = read_i64_le(body, cursor) else {
        return Ok(None);
    };
    let Some(key) = read_length_prefixed(body, cursor, LOGICAL_TXN_MAX_KEY_BYTES, ctx, "key")?
    else {
        return Ok(None);
    };
    // §4.4.3 row `id_len`: bounded by LOGICAL_TXN_MAX_VALUE_BYTES (16 MiB),
    // not the key cap — `id_bytes` carries the full `{"_id": ...}` BSON
    // payload from `SecIndexOp::Insert { id_bytes }`.
    let Some(id_bytes) =
        read_length_prefixed(body, cursor, LOGICAL_TXN_MAX_VALUE_BYTES, ctx, "id_bytes")?
    else {
        return Ok(None);
    };
    Ok(Some(LogicalOpKind::SecondaryInsert {
        index_id,
        key,
        id_bytes,
    }))
}

/// Parse the `(index_id, key)` body for `SecondaryDelete`.
fn parse_secondary_delete_body(
    body: &[u8],
    cursor: &mut usize,
    ctx: &DecodeCtx,
) -> Result<Option<LogicalOpKind>> {
    let Some(index_id) = read_i64_le(body, cursor) else {
        return Ok(None);
    };
    let Some(key) = read_length_prefixed(body, cursor, LOGICAL_TXN_MAX_KEY_BYTES, ctx, "key")?
    else {
        return Ok(None);
    };
    Ok(Some(LogicalOpKind::SecondaryDelete { index_id, key }))
}

/// Read an `i64` little-endian from `body[*cursor..]`, advancing the
/// cursor. Returns `None` on EOF.
fn read_i64_le(body: &[u8], cursor: &mut usize) -> Option<i64> {
    if body.len().saturating_sub(*cursor) < 8 {
        return None;
    }
    let v = i64::from_le_bytes(body[*cursor..*cursor + 8].try_into().expect("8 bytes"));
    *cursor += 8;
    Some(v)
}

/// Read a `u32`-LE length-prefixed byte slice, bounds-checking the length
/// field against `max_len` before any allocation per §4.6.
///
/// Returns:
/// - `Ok(Some(vec))` on success.
/// - `Ok(None)` on EOF mid-body (tail failure).
/// - `Err(Error::CorruptDatabase)` in MidStream when `len > max_len`.
fn read_length_prefixed(
    body: &[u8],
    cursor: &mut usize,
    max_len: usize,
    ctx: &DecodeCtx,
    what: &str,
) -> Result<Option<Vec<u8>>> {
    if body.len().saturating_sub(*cursor) < 4 {
        return Ok(None);
    }
    let len = u32::from_le_bytes(body[*cursor..*cursor + 4].try_into().expect("4 bytes")) as usize;
    *cursor += 4;
    if len > max_len {
        return dispose(
            ctx,
            true,
            format!("LogicalTxnFrame {what} length {len} exceeds max {max_len}"),
        );
    }
    if body.len().saturating_sub(*cursor) < len {
        return Ok(None);
    }
    // Safe: len ≤ max_len (bounded above).
    let data = body[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(Some(data))
}

/// §4.6 disposition helper: Scanning rewinds (Ok(None)); MidStream surfaces
/// `Err(Error::CorruptDatabase { recoverable, .. })`.
fn dispose<T>(ctx: &DecodeCtx, recoverable: bool, detail: String) -> Result<Option<T>> {
    match ctx {
        DecodeCtx::Scanning => Ok(None),
        DecodeCtx::MidStream { .. } => Err(Error::CorruptDatabase {
            path: std::path::PathBuf::new(),
            detail,
            recoverable,
        }),
    }
}

// ---------------------------------------------------------------------------
// JournalHeader
// ---------------------------------------------------------------------------

/// Parsed representation of the 32-byte journal file header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JournalHeader {
    /// Must equal [`JOURNAL_MAGIC`].
    pub magic: [u8; 4],
    /// Format version; must equal [`JOURNAL_FORMAT_VERSION`].
    pub format_version: u32,
    /// Internal page size (bytes); must match the main file.
    pub page_size_internal: u32,
    /// Leaf page size (bytes); must match the main file.
    pub page_size_leaf: u32,
    /// Salt 1 from the main file header.  Used for stale-journal detection.
    pub salt1: u32,
    /// Salt 2 from the main file header.  Used for stale-journal detection.
    pub salt2: u32,
    /// Number of complete checkpoints performed on this journal file.  Used as
    /// an optimisation hint to skip already-checkpointed frames on recovery.
    pub checkpoint_seq: u32,
    // checksum at offset 28 — computed/verified on read/write; not stored here
}

impl JournalHeader {
    /// Create a fresh journal header bound to the given salt values.
    pub(crate) fn new(salt1: u32, salt2: u32) -> Self {
        Self {
            magic: JOURNAL_MAGIC,
            format_version: JOURNAL_FORMAT_VERSION,
            page_size_internal: PAGE_SIZE_INTERNAL,
            page_size_leaf: PAGE_SIZE_LEAF,
            salt1,
            salt2,
            checkpoint_seq: 0,
        }
    }

    /// Serialize to a 32-byte buffer, computing the CRC32C checksum.
    pub(crate) fn to_bytes(&self) -> [u8; JOURNAL_HEADER_SIZE] {
        let mut buf = [0u8; JOURNAL_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.magic);
        buf[4..8].copy_from_slice(&self.format_version.to_le_bytes());
        buf[8..12].copy_from_slice(&self.page_size_internal.to_le_bytes());
        buf[12..16].copy_from_slice(&self.page_size_leaf.to_le_bytes());
        buf[16..20].copy_from_slice(&self.salt1.to_le_bytes());
        buf[20..24].copy_from_slice(&self.salt2.to_le_bytes());
        buf[24..28].copy_from_slice(&self.checkpoint_seq.to_le_bytes());
        // Checksum covers bytes 0–27 (the checksum field itself is excluded)
        let checksum = crc32c::crc32c(&buf[..28]);
        buf[28..32].copy_from_slice(&checksum.to_le_bytes());
        buf
    }

    /// Deserialize and verify from a 32-byte buffer.
    ///
    /// Returns `Err(Error::UnsupportedJournalFormat)` when the magic bytes or
    /// format version do not match this build.  Checksum failures return
    /// `Err(Error::CorruptDatabase)`.
    pub(crate) fn from_bytes(buf: &[u8; JOURNAL_HEADER_SIZE]) -> Result<Self> {
        // 1. Magic
        let magic: [u8; 4] = buf[0..4].try_into().expect("4 bytes");
        if magic != JOURNAL_MAGIC {
            return Err(Error::UnsupportedJournalFormat {
                found: magic,
                expected: JOURNAL_MAGIC,
            });
        }

        // 2. Format version
        let format_version = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes"));
        if format_version != JOURNAL_FORMAT_VERSION {
            return Err(Error::UnsupportedJournalFormat {
                found: magic,
                expected: JOURNAL_MAGIC,
            });
        }

        // 3. Checksum
        let stored_checksum = u32::from_le_bytes(buf[28..32].try_into().expect("4 bytes"));
        let computed_checksum = crc32c::crc32c(&buf[..28]);
        if stored_checksum != computed_checksum {
            return Err(Error::CorruptDatabase {
                path: std::path::PathBuf::new(),
                detail: format!(
                    "journal header checksum mismatch: stored 0x{stored_checksum:08X}, \
                     computed 0x{computed_checksum:08X}"
                ),
                recoverable: true,
            });
        }

        Ok(Self {
            magic,
            format_version,
            page_size_internal: u32::from_le_bytes(buf[8..12].try_into().expect("4 bytes")),
            page_size_leaf: u32::from_le_bytes(buf[12..16].try_into().expect("4 bytes")),
            salt1: u32::from_le_bytes(buf[16..20].try_into().expect("4 bytes")),
            salt2: u32::from_le_bytes(buf[20..24].try_into().expect("4 bytes")),
            checkpoint_seq: u32::from_le_bytes(buf[24..28].try_into().expect("4 bytes")),
        })
    }
}

/// Page size indicator used inside a journal frame header.
///
/// This tells the recovery algorithm how many bytes of page data follow the
/// frame header, avoiding any ambiguity about the page-size allocation scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JournalPageSize {
    /// 4 KiB — internal (branch) B+ tree node.
    Small4k,
    /// 32 KiB — leaf node, overflow page, or file header.
    Large32k,
}

impl JournalPageSize {
    /// Return the page size in bytes.
    pub(crate) fn bytes(self) -> usize {
        match self {
            JournalPageSize::Small4k => PAGE_SIZE_INTERNAL as usize,
            JournalPageSize::Large32k => PAGE_SIZE_LEAF as usize,
        }
    }

    /// Encode as the u32 stored in the frame header.
    pub(crate) fn as_u32(self) -> u32 {
        self.bytes() as u32
    }

    /// Decode from the u32 stored in the frame header.
    pub(crate) fn from_u32(v: u32) -> Result<Self> {
        match v {
            PAGE_SIZE_INTERNAL => Ok(JournalPageSize::Small4k),
            PAGE_SIZE_LEAF => Ok(JournalPageSize::Large32k),
            _ => Err(Error::CorruptDatabase {
                path: std::path::PathBuf::new(),
                detail: format!("journal frame: unknown page_size field {v}"),
                recoverable: false,
            }),
        }
    }
}

// Low-level journal I/O helpers
// ---------------------------------------------------------------------------

/// Seek `f` to the position just after the journal header, ready to read the
/// first frame.
pub(crate) fn seek_to_first_frame<F: Seek>(f: &mut F) -> io::Result<()> {
    f.seek(SeekFrom::Start(JOURNAL_HEADER_SIZE as u64))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// ChainCommit frame
// ---------------------------------------------------------------------------

/// Page-write entry carried inside a `ChainCommit` frame.
///
/// Byte layout: `(page: u32 LE, page_size: u8, reserved: [u8; 3],
/// data: [u8; page_size_bytes])`. `page_size == 0` selects
/// [`PAGE_SIZE_INTERNAL`]; `page_size == 1` selects [`PAGE_SIZE_LEAF`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct ChainPageWrite {
    pub page: u32,
    pub page_size: JournalPageSize,
    pub data: Vec<u8>,
}

impl ChainPageWrite {
    /// Total encoded byte size (8 B header + payload).
    fn encoded_len(&self) -> usize {
        8 + self.page_size.bytes()
    }
}

/// Chain-commit frame — one emitted per `WriteTxn::commit()`.
///
/// Byte layout:
///
/// ```text
///  0       1    frame_kind: u8 (0x02 = CHAIN_COMMIT)
///  1       3    reserved: [u8; 3] (MUST be 0)
///  4       4    total_frame_bytes: u32 LE
///  8       4    salt1: u32 LE
/// 12       4    salt2: u32 LE
/// 16      12    commit_ts: Ts-LE
/// 28       4    refcount_delta_count: u32 LE
/// 32       N    refcount_deltas[]: (page: u32 LE, delta: i32 LE) × count
/// 32+N     4    page_write_count: u32 LE
/// 36+N     M    page_writes[]
/// 36+N+M   4    checksum_crc32: u32 LE (covers 0..36+N+M)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct ChainCommitFrame {
    pub salt1: u32,
    pub salt2: u32,
    pub commit_ts: Ts,
    pub refcount_deltas: Vec<(u32, i32)>,
    pub page_writes: Vec<ChainPageWrite>,
}

impl ChainCommitFrame {
    /// Compute the total encoded byte size (`total_frame_bytes`).
    #[allow(dead_code)]
    pub(crate) fn total_frame_bytes(&self) -> usize {
        let deltas_n = 8 * self.refcount_deltas.len();
        let writes_m: usize = self.page_writes.iter().map(|w| w.encoded_len()).sum();
        36 + deltas_n + writes_m + 4
    }

    /// Encode to bytes. Fails only on arithmetic overflow of the
    /// length prefix (≥ `CHAIN_COMMIT_MAX_FRAME_SIZE`).
    #[allow(dead_code)]
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let total = self.total_frame_bytes();
        if total > CHAIN_COMMIT_MAX_FRAME_SIZE {
            return Err(Error::Internal(format!(
                "ChainCommit frame {total} B exceeds MAX_FRAME_SIZE {CHAIN_COMMIT_MAX_FRAME_SIZE}"
            )));
        }
        let total_u32 = u32::try_from(total)
            .map_err(|_| Error::Internal("ChainCommit frame length overflows u32".into()))?;

        let mut buf = Vec::with_capacity(total);
        buf.push(FRAME_KIND_CHAIN_COMMIT);
        buf.extend_from_slice(&[0u8; 3]); // reserved
        buf.extend_from_slice(&total_u32.to_le_bytes());
        buf.extend_from_slice(&self.salt1.to_le_bytes());
        buf.extend_from_slice(&self.salt2.to_le_bytes());
        buf.extend_from_slice(&self.commit_ts.to_le_bytes());

        let delta_count = u32::try_from(self.refcount_deltas.len())
            .map_err(|_| Error::Internal("ChainCommit refcount_delta_count exceeds u32".into()))?;
        buf.extend_from_slice(&delta_count.to_le_bytes());
        for (page, delta) in &self.refcount_deltas {
            buf.extend_from_slice(&page.to_le_bytes());
            buf.extend_from_slice(&delta.to_le_bytes());
        }

        let write_count = u32::try_from(self.page_writes.len())
            .map_err(|_| Error::Internal("ChainCommit page_write_count exceeds u32".into()))?;
        buf.extend_from_slice(&write_count.to_le_bytes());
        for pw in &self.page_writes {
            debug_assert_eq!(
                pw.data.len(),
                pw.page_size.bytes(),
                "page_write data length must match page_size"
            );
            buf.extend_from_slice(&pw.page.to_le_bytes());
            buf.push(match pw.page_size {
                JournalPageSize::Small4k => 0,
                JournalPageSize::Large32k => 1,
            });
            buf.extend_from_slice(&[0u8; 3]); // reserved
            buf.extend_from_slice(&pw.data);
        }

        debug_assert_eq!(buf.len(), total - 4, "checksum not yet appended");
        let cs = crc32c::crc32c(&buf);
        buf.extend_from_slice(&cs.to_le_bytes());
        debug_assert_eq!(buf.len(), total);

        Ok(buf)
    }

    /// Decode from bytes. Returns `Ok(None)` when the buffer is
    /// truncated, salt-mismatched, kind-wrong, or checksum-invalid —
    /// the recovery caller treats every such outcome as frame-not-present.
    /// Returns `Err` only on programmer error (callers pass an
    /// absurdly tiny slice) — which is never emitted in recovery.
    #[allow(dead_code)]
    pub(crate) fn decode(
        buf: &[u8],
        expected_salt1: u32,
        expected_salt2: u32,
    ) -> Result<Option<Self>> {
        // 1. Need at least the 32-byte fixed header to read counts.
        if buf.len() < CHAIN_COMMIT_FIXED_HEADER_LEN {
            return Ok(None);
        }

        // 2. Frame kind discriminant.
        if buf[0] != FRAME_KIND_CHAIN_COMMIT {
            return Ok(None);
        }

        // 3. Length prefix. Validate before trusting any count field.
        let total_frame_bytes = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes")) as usize;
        if total_frame_bytes > CHAIN_COMMIT_MAX_FRAME_SIZE {
            return Ok(None);
        }
        let refcount_delta_count =
            u32::from_le_bytes(buf[28..32].try_into().expect("4 bytes")) as usize;
        let min_required = 36usize
            .saturating_add(8usize.saturating_mul(refcount_delta_count))
            .saturating_add(4);
        if total_frame_bytes < min_required {
            return Ok(None);
        }
        if buf.len() < total_frame_bytes {
            return Ok(None); // truncated
        }

        // 4. Salts.
        let salt1 = u32::from_le_bytes(buf[8..12].try_into().expect("4 bytes"));
        let salt2 = u32::from_le_bytes(buf[12..16].try_into().expect("4 bytes"));
        if salt1 != expected_salt1 || salt2 != expected_salt2 {
            return Ok(None);
        }

        // 5. CRC over bytes 0..total-4 against trailing 4 bytes.
        let body_end = total_frame_bytes - 4;
        let stored_cs = u32::from_le_bytes(
            buf[body_end..total_frame_bytes]
                .try_into()
                .expect("4 bytes"),
        );
        let computed_cs = crc32c::crc32c(&buf[..body_end]);
        if stored_cs != computed_cs {
            return Ok(None);
        }

        // 6. Parse body.
        let commit_ts = Ts::from_le_bytes(buf[16..28].try_into().expect("12 bytes"));

        let mut cursor = 32usize;
        let mut refcount_deltas = Vec::with_capacity(refcount_delta_count);
        for _ in 0..refcount_delta_count {
            if cursor + 8 > body_end {
                return Ok(None);
            }
            let page = u32::from_le_bytes(buf[cursor..cursor + 4].try_into().expect("4 bytes"));
            let delta =
                i32::from_le_bytes(buf[cursor + 4..cursor + 8].try_into().expect("4 bytes"));
            refcount_deltas.push((page, delta));
            cursor += 8;
        }

        if cursor + 4 > body_end {
            return Ok(None);
        }
        let page_write_count =
            u32::from_le_bytes(buf[cursor..cursor + 4].try_into().expect("4 bytes")) as usize;
        cursor += 4;

        let remaining = body_end.saturating_sub(cursor);
        let min_page_write_bytes = 8 + JournalPageSize::Small4k.bytes();
        if page_write_count > remaining / min_page_write_bytes {
            return Ok(None);
        }
        let mut page_writes = Vec::with_capacity(page_write_count);
        for _ in 0..page_write_count {
            if cursor + 8 > body_end {
                return Ok(None);
            }
            let page = u32::from_le_bytes(buf[cursor..cursor + 4].try_into().expect("4 bytes"));
            let page_size_marker = buf[cursor + 4];
            // reserved: buf[cursor+5..cursor+8]
            let page_size = match page_size_marker {
                0 => JournalPageSize::Small4k,
                1 => JournalPageSize::Large32k,
                _ => return Ok(None),
            };
            cursor += 8;
            let data_len = page_size.bytes();
            if cursor + data_len > body_end {
                return Ok(None);
            }
            let data = buf[cursor..cursor + data_len].to_vec();
            cursor += data_len;
            page_writes.push(ChainPageWrite {
                page,
                page_size,
                data,
            });
        }

        // Final tail consistency: cursor must equal body_end.
        if cursor != body_end {
            return Ok(None);
        }

        Ok(Some(Self {
            salt1,
            salt2,
            commit_ts,
            refcount_deltas,
            page_writes,
        }))
    }
}

// ---------------------------------------------------------------------------
// ChainCommit cursor reader
// ---------------------------------------------------------------------------

/// Peek at the current position and read a `ChainCommitFrame` if one is
/// present, returning the number of bytes consumed.
///
/// ## Cursor semantics
///
/// - On `Ok(Some((n, commit_ts, start_offset)))`: the reader is positioned
///   at the next frame, `n` is the number of bytes the `ChainCommit`
///   consumed, and `commit_ts` is the decoded commit timestamp (carried out
///   so recovery can fold it into `TimestampOracle::set_min`).
///   `start_offset` is the journal offset where the frame started.
/// - On `Ok(None)`: the reader is restored to its original position.
/// - On `Err`: the reader position is undefined; the caller should treat
///   the scan as aborted.
pub(crate) fn read_chain_commit_at_cursor<R: Read + Seek>(
    r: &mut R,
    expected_salt1: u32,
    expected_salt2: u32,
) -> Result<Option<(u64, Ts, u64)>> {
    let start = r.stream_position().map_err(Error::Io)?;

    // Read the fixed 32-byte header prefix first. Cheaper rejects happen
    // in this prefix before we commit to reading a full variable-length
    // frame.
    let mut header = [0u8; CHAIN_COMMIT_FIXED_HEADER_LEN];
    match r.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
            return Ok(None);
        }
        Err(e) => return Err(Error::Io(e)),
    }

    // Quick reject: frame_kind discriminant and reserved-zero bytes.
    if header[0] != FRAME_KIND_CHAIN_COMMIT || header[1] != 0 || header[2] != 0 || header[3] != 0 {
        r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
        return Ok(None);
    }

    let total_frame_bytes = u32::from_le_bytes(header[4..8].try_into().expect("4 bytes")) as usize;
    // §A.2 minimum is 40 bytes (32 header + 4 write_count + 4 CRC).
    if !(40..=CHAIN_COMMIT_MAX_FRAME_SIZE).contains(&total_frame_bytes) {
        r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
        return Ok(None);
    }

    // Read the rest into a contiguous buffer for the full decode+CRC check.
    let mut full = vec![0u8; total_frame_bytes];
    full[..CHAIN_COMMIT_FIXED_HEADER_LEN].copy_from_slice(&header);
    match r.read_exact(&mut full[CHAIN_COMMIT_FIXED_HEADER_LEN..]) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
            return Ok(None);
        }
        Err(e) => return Err(Error::Io(e)),
    }

    match ChainCommitFrame::decode(&full, expected_salt1, expected_salt2)? {
        Some(frame) => Ok(Some((total_frame_bytes as u64, frame.commit_ts, start))),
        None => {
            r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
            Ok(None)
        }
    }
}

// ---------------------------------------------------------------------------
// try_skip_logical_txn
// ---------------------------------------------------------------------------

/// Disposition returned by [`try_skip_logical_txn_disposition`] (§7).
///
/// Distinguishes "this is not a logical frame" from "this IS a torn logical
/// frame" so the recovery loop can tick the
/// `logical_txn_torn_frames_total` counter (§7) on the latter without
/// otherwise changing scan behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum LogicalScan {
    /// Bytes are NOT a logical frame attempt; try the next record kind.
    NotLogical,
    /// Structural signature matches but decode failed (CRC mismatch or
    /// truncated mid-body) — bytes look like a partially-written logical
    /// frame.
    Torn,
    /// A valid logical frame was read and decoded.
    Valid(u64, LogicalTxnFrame),
}

/// Peek at the current position and skip over a [`LogicalTxnFrame`] with the
/// full [`LogicalScan`] disposition (§7).
///
/// Distinguishes three cases:
///
/// - `NotLogical`: byte 0 isn't `FRAME_KIND_LOGICAL_TXN`, or reserved bytes
///   are non-zero, or `total_frame_bytes` is out of range, or salts don't
///   match (different database lifetime). Cursor is rewound.
/// - `Torn`: structural signature (kind + reserved + length + salts) matches
///   but the frame body is truncated or fails CRC. Cursor is rewound.
/// - `Valid`: full decode succeeded. Cursor advances to `start + n`.
///
/// `try_skip_logical_txn` wraps this helper and collapses `Torn` /
/// `NotLogical` into `Ok(None)` for the existing call-site contract.
#[allow(dead_code)]
pub(crate) fn try_skip_logical_txn_disposition<R: Read + Seek>(
    r: &mut R,
    expected_salt1: u32,
    expected_salt2: u32,
) -> Result<LogicalScan> {
    let start = r.stream_position().map_err(Error::Io)?;

    // Read the fixed 48-byte header probe first. Cheaper rejects happen on
    // this prefix before committing to reading a full variable-length frame.
    let mut header = [0u8; LOGICAL_TXN_FIXED_HEADER_LEN];
    match r.read_exact(&mut header) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
            return Ok(LogicalScan::NotLogical);
        }
        Err(e) => return Err(Error::Io(e)),
    }

    // Structural signature: kind + reserved + length range + salts.
    let kind_match = header[0] == FRAME_KIND_LOGICAL_TXN;
    let reserved_zero = header[1] == 0 && header[2] == 0 && header[3] == 0;
    let total_frame_bytes = u32::from_le_bytes(header[4..8].try_into().expect("4 bytes")) as usize;
    let length_in_range =
        (LOGICAL_TXN_MIN_FRAME_SIZE..=LOGICAL_TXN_MAX_FRAME_SIZE).contains(&total_frame_bytes);
    let salt1 = u32::from_le_bytes(header[8..12].try_into().expect("4 bytes"));
    let salt2 = u32::from_le_bytes(header[12..16].try_into().expect("4 bytes"));
    let salts_match = salt1 == expected_salt1 && salt2 == expected_salt2;

    if !(kind_match && reserved_zero && length_in_range && salts_match) {
        r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
        return Ok(LogicalScan::NotLogical);
    }

    // Read the remainder of the frame into a contiguous buffer for the full
    // decode + CRC check. Truncation here means the bytes match the
    // structural signature but the body is incomplete — torn.
    let mut full = vec![0u8; total_frame_bytes];
    full[..LOGICAL_TXN_FIXED_HEADER_LEN].copy_from_slice(&header);
    let body_complete = match r.read_exact(&mut full[LOGICAL_TXN_FIXED_HEADER_LEN..]) {
        Ok(()) => true,
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => false,
        Err(e) => return Err(Error::Io(e)),
    };
    if body_complete {
        if let Some(frame) =
            LogicalTxnFrame::decode(&full, expected_salt1, expected_salt2, DecodeCtx::Scanning)?
        {
            return Ok(LogicalScan::Valid(total_frame_bytes as u64, frame));
        }
    }

    // The bytes carry the logical structural signature but did not decode.
    // Unsupported old page-record bytes are not parsed as a compatibility
    // format here; recovery treats this as a torn logical tail.
    r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
    Ok(LogicalScan::Torn)
}

/// Peek at the current position and skip over a [`LogicalTxnFrame`] if one is
/// present, returning the number of bytes consumed and the decoded frame.
///
/// Probe the current cursor for a [`LogicalTxnFrame`]. Always uses
/// [`DecodeCtx::Scanning`]: every §4.6 disposition-table failure row rewinds
/// the reader and returns `Ok(None)`.
///
/// # Arguments
/// - `r`: reader positioned at the candidate frame start.
/// - `expected_salt1`: journal-header salt 1 for this database lifetime.
/// - `expected_salt2`: journal-header salt 2 for this database lifetime.
///
/// # Cursor semantics
///
/// - On `Ok(Some((n, frame)))`: the reader is positioned at `start + n`, `n`
///   is the number of bytes consumed by the logical frame, and `frame` is
///   the decoded value for downstream Pass 1 collection.
/// - On `Ok(None)`: the reader is restored to its original position.
/// - On `Err`: the reader position is undefined; the caller should treat
///   the scan as aborted.
#[allow(dead_code)]
pub(crate) fn try_skip_logical_txn<R: Read + Seek>(
    r: &mut R,
    expected_salt1: u32,
    expected_salt2: u32,
) -> Result<Option<(u64, LogicalTxnFrame)>> {
    match try_skip_logical_txn_disposition(r, expected_salt1, expected_salt2)? {
        LogicalScan::Valid(n, frame) => Ok(Some((n, frame))),
        LogicalScan::Torn | LogicalScan::NotLogical => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/log_file_codec.rs"]
mod tests;
