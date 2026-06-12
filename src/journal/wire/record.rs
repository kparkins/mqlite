//! Phase 8 `LogRecord` 72-byte header encode/decode with dual CRC32C validation.
//!
//! ## Wire layout (72 bytes)
//!
//! ```text
//! Offset  Size  Field
//!   0      4    magic: u32 LE (0x384C514D = "MQL8")
//!   4      2    format_version: u16 LE (1)
//!   6      2    header_len: u16 LE (72)
//!   8      2    record_kind: u16 LE
//!  10      2    flags: u16 LE
//!  12      4    total_len: u32 LE (header + payload)
//!  16      8    start_lsn: u64 LE
//!  24      8    end_lsn: u64 LE
//!  32      8    txn_id: u64 LE
//!  40      8    publish_seq: u64 LE
//!  48      8    commit_ts.physical_ms: u64 LE
//!  56      4    commit_ts.logical: u32 LE
//!  60      4    payload_len: u32 LE
//!  64      4    header_crc32c: u32 LE  (CRC of bytes 0–71 with this field zeroed)
//!  68      4    payload_crc32c: u32 LE (CRC of payload bytes)
//! ```
//!
//! Two CRC32C fields guard against both header corruption and payload corruption
//! independently.

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;

use super::{
    invalid_log_record, len_to_u16, len_to_u32, log_record_too_large, read_u16, read_u32,
    read_u64, LOG_RECORD_COMMIT_TS_LOGICAL_OFFSET, LOG_RECORD_COMMIT_TS_PHYSICAL_OFFSET,
    LOG_RECORD_END_LSN_OFFSET, LOG_RECORD_FLAGS_OFFSET, LOG_RECORD_FORMAT_VERSION,
    LOG_RECORD_FORMAT_VERSION_OFFSET, LOG_RECORD_HEADER_CRC32C_OFFSET, LOG_RECORD_HEADER_LEN,
    LOG_RECORD_HEADER_LEN_OFFSET, LOG_RECORD_KIND_OFFSET, LOG_RECORD_MAGIC,
    LOG_RECORD_MAGIC_OFFSET, LOG_RECORD_PAYLOAD_CRC32C_OFFSET, LOG_RECORD_PAYLOAD_LEN_OFFSET,
    LOG_RECORD_PUBLISH_SEQ_OFFSET, LOG_RECORD_START_LSN_OFFSET, LOG_RECORD_TOTAL_LEN_OFFSET,
    MAX_LOG_RECORD_BYTES, LOG_RECORD_TXN_ID_OFFSET,
};

// ---------------------------------------------------------------------------
// Private wire-format discriminants
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// LogRecordKind
// ---------------------------------------------------------------------------

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
    pub(super) fn from_wire(value: u16) -> Result<Self> {
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

    pub(super) fn wire_value(self) -> u16 {
        match self {
            Self::CrudCommit => LOG_RECORD_KIND_CRUD_COMMIT,
            Self::CatalogCommit => LOG_RECORD_KIND_CATALOG_COMMIT,
            Self::CheckpointBoundary => LOG_RECORD_KIND_CHECKPOINT_BOUNDARY,
            Self::CheckpointPageFrame => LOG_RECORD_KIND_CHECKPOINT_PAGE_FRAME,
        }
    }

    pub(super) fn required_flags(self) -> LogRecordFlags {
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

// ---------------------------------------------------------------------------
// LogRecordFlags
// ---------------------------------------------------------------------------

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

    pub(super) fn from_bits(bits: u16) -> Result<Self> {
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

    pub(super) fn validate_for_kind(self, kind: LogRecordKind) -> Result<()> {
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

// ---------------------------------------------------------------------------
// LogRecordPayload
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// LogRecordDraft
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// FinalizedLogRecord
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// LogRecordDecodeError
// ---------------------------------------------------------------------------

/// Classified [`LogRecord::decode_classified`] failure.
///
/// The decode rows split into two recovery dispositions (F24, the outer-layer
/// sibling of the §4.6 inner-frame split):
///
/// - Rows at or before the dual-CRC32C gates (bad magic / version / lengths /
///   kind / flags / either CRC mismatch) fire on bytes that have NOT been
///   proven writer-committed — a torn tail produces exactly these failures,
///   so the recovery scan may stop and truncate at such a record.
/// - Rows AFTER both CRC gates passed (publish_seq kind rules, payload
///   decode such as the CrudCommit split-header consistency check) fire
///   inside a fully-written, dual-CRC-valid record: content corruption.
///   Flattening these into a torn tail would physically truncate every later
///   committed record off the journal (the BUG-4 failure class).
#[derive(Debug)]
pub(crate) enum LogRecordDecodeError {
    /// Failure at or before the CRC32C gates — indistinguishable from a torn
    /// tail.
    TornEligible(Error),
    /// Semantic failure after both CRC32C gates passed — detected corruption
    /// inside a fully-written record.
    PostCrcCorrupt(Error),
}

impl LogRecordDecodeError {
    /// Drop the classification, keeping the underlying error.
    pub(crate) fn into_error(self) -> Error {
        match self {
            Self::TornEligible(error) | Self::PostCrcCorrupt(error) => error,
        }
    }
}

/// Wrap a torn-tail-eligible decode row.
fn torn(error: Error) -> LogRecordDecodeError {
    LogRecordDecodeError::TornEligible(error)
}

// ---------------------------------------------------------------------------
// LogRecord (decoded)
// ---------------------------------------------------------------------------

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
        Self::decode_classified(buf).map_err(LogRecordDecodeError::into_error)
    }

    /// Decode like [`Self::decode`], but classify failures by whether the
    /// failing row fired before or after the dual-CRC32C gates (see
    /// [`LogRecordDecodeError`]). The kind / flags / length / end_lsn rows
    /// are checked before the header CRC, so a torn tail can hit them with
    /// arbitrary garbage; they classify as torn-eligible.
    ///
    /// # Errors
    ///
    /// Returns [`LogRecordDecodeError::TornEligible`] for rows at or before
    /// the CRC gates, [`LogRecordDecodeError::PostCrcCorrupt`] for semantic
    /// rows inside a dual-CRC-valid record.
    pub(crate) fn decode_classified(
        buf: &[u8],
    ) -> std::result::Result<Self, LogRecordDecodeError> {
        if buf.len() < LOG_RECORD_HEADER_LEN {
            return Err(torn(invalid_log_record(
                "Phase 8 LogRecord truncated fixed header",
            )));
        }

        let magic = read_u32(buf, LOG_RECORD_MAGIC_OFFSET);
        if magic != LOG_RECORD_MAGIC {
            return Err(torn(invalid_log_record(format!(
                "Phase 8 LogRecord bad magic {magic:#010x}"
            ))));
        }

        let format_version = read_u16(buf, LOG_RECORD_FORMAT_VERSION_OFFSET);
        if format_version != LOG_RECORD_FORMAT_VERSION {
            return Err(torn(invalid_log_record(format!(
                "Phase 8 LogRecord bad format_version {format_version}"
            ))));
        }

        let header_len = read_u16(buf, LOG_RECORD_HEADER_LEN_OFFSET) as usize;
        if header_len != LOG_RECORD_HEADER_LEN {
            return Err(torn(invalid_log_record(format!(
                "Phase 8 LogRecord header_len {header_len} does not match {LOG_RECORD_HEADER_LEN}"
            ))));
        }

        let kind = LogRecordKind::from_wire(read_u16(buf, LOG_RECORD_KIND_OFFSET)).map_err(torn)?;
        let flags =
            LogRecordFlags::from_bits(read_u16(buf, LOG_RECORD_FLAGS_OFFSET)).map_err(torn)?;
        flags.validate_for_kind(kind).map_err(torn)?;

        let total_len = read_u32(buf, LOG_RECORD_TOTAL_LEN_OFFSET) as usize;
        if total_len > MAX_LOG_RECORD_BYTES {
            return Err(torn(log_record_too_large(total_len)));
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
            .ok_or_else(|| torn(invalid_log_record("Phase 8 LogRecord total_len overflow")))?;
        if total_len != expected_total_len {
            return Err(torn(invalid_log_record(format!(
                "Phase 8 LogRecord total_len {total_len} does not equal header_len + payload_len {expected_total_len}"
            ))));
        }
        if buf.len() < total_len {
            return Err(torn(invalid_log_record(
                "Phase 8 LogRecord truncated payload",
            )));
        }
        let expected_end_lsn = start_lsn
            .checked_add(total_len as u64)
            .ok_or_else(|| torn(invalid_log_record("Phase 8 LogRecord end_lsn overflow")))?;
        if end_lsn != expected_end_lsn {
            return Err(torn(invalid_log_record(format!(
                "Phase 8 LogRecord end_lsn {end_lsn} does not equal start_lsn + total_len {expected_end_lsn}"
            ))));
        }

        let mut header_for_crc = [0u8; LOG_RECORD_HEADER_LEN];
        header_for_crc.copy_from_slice(&buf[..LOG_RECORD_HEADER_LEN]);
        header_for_crc[LOG_RECORD_HEADER_CRC32C_OFFSET..LOG_RECORD_HEADER_CRC32C_OFFSET + 4]
            .copy_from_slice(&0u32.to_le_bytes());
        let computed_header_crc32c = crc32c::crc32c(&header_for_crc);
        if header_crc32c != computed_header_crc32c {
            return Err(torn(invalid_log_record(format!(
                "Phase 8 LogRecord header_crc32c mismatch: stored 0x{header_crc32c:08X}, computed 0x{computed_header_crc32c:08X}"
            ))));
        }

        let payload_bytes = &buf[LOG_RECORD_HEADER_LEN..total_len];
        let computed_payload_crc32c = crc32c::crc32c(payload_bytes);
        if payload_crc32c != computed_payload_crc32c {
            return Err(torn(invalid_log_record(format!(
                "Phase 8 LogRecord payload_crc32c mismatch: stored 0x{payload_crc32c:08X}, computed 0x{computed_payload_crc32c:08X}"
            ))));
        }

        // Both CRC32C gates passed: every row below fires inside a
        // fully-written record and classifies as content corruption.
        match kind {
            LogRecordKind::CrudCommit | LogRecordKind::CatalogCommit if publish_seq == 0 => {
                return Err(LogRecordDecodeError::PostCrcCorrupt(invalid_log_record(
                    "Phase 8 LogRecord publish_seq 0 is reserved for CheckpointBoundary",
                )));
            }
            LogRecordKind::CheckpointBoundary | LogRecordKind::CheckpointPageFrame
                if publish_seq != 0 =>
            {
                return Err(LogRecordDecodeError::PostCrcCorrupt(invalid_log_record(
                    "Phase 8 Checkpoint records must use publish_seq 0",
                )));
            }
            _ => {}
        }

        let payload = LogRecordPayload::decode(kind, payload_bytes)
            .map_err(LogRecordDecodeError::PostCrcCorrupt)?;
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

// ---------------------------------------------------------------------------
// LogRecordHeader (private serialization helper)
// ---------------------------------------------------------------------------

pub(super) struct LogRecordHeader {
    pub(super) kind: LogRecordKind,
    pub(super) flags: LogRecordFlags,
    pub(super) total_len: usize,
    pub(super) start_lsn: u64,
    pub(super) end_lsn: u64,
    pub(super) txn_id: u64,
    pub(super) publish_seq: u64,
    pub(super) commit_ts: Ts,
    pub(super) payload_len: usize,
    pub(super) payload_crc32c: u32,
}

impl LogRecordHeader {
    pub(super) fn write_with_header_crc(&self, header_crc32c: u32, out: &mut [u8]) -> Result<()> {
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
