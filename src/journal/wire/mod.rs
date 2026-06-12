//! Wire-format codec for the mqlite journal (Phase 8 on-disk layout).
//!
//! ## On-disk format overview
//!
//! ```text
//! Journal file
//! ┌─────────────────────────────┐
//! │ JournalHeader   — 32 bytes  │  Magic "MQJL", version, page sizes,
//! │                             │  salts, checkpoint seq, CRC32C
//! ├─────────────────────────────┤
//! │ LogRecord 0                 │  72-byte fixed header + variable payload
//! │   ├─ header (72 B)         │  Magic "MQL8", kind, flags, LSNs, CRC32C×2
//! │   └─ payload (variable)    │  Kind-specific encoded bytes
//! ├─────────────────────────────┤
//! │ LogRecord 1 …               │
//! └─────────────────────────────┘
//! ```
//!
//! ## Record kinds
//!
//! | Kind                 | Payload module | Description                          |
//! |----------------------|----------------|--------------------------------------|
//! | `CrudCommit`         | `record`       | Logical + chain payload bytes        |
//! | `CatalogCommit`      | `payloads`     | DDL catalog payload                  |
//! | `CheckpointBoundary` | `payloads`     | Checkpoint frontier + header image   |
//! | `CheckpointPageFrame`| `payloads`     | One in-flight checkpoint page        |
//!
//! Legacy ChainCommit and LogicalTxnFrame byte streams are carried as the
//! opaque `chain_payload` and `logical_payload` blobs inside `CrudCommit`
//! records. Their codecs live in [`payloads`] and [`logical`] respectively.
//!
//! ## Checksums
//!
//! All checksums use CRC32C. Each `LogRecord` carries two checksums:
//! - `header_crc32c` — CRC32C of the 72-byte header with the checksum field
//!   zeroed out.
//! - `payload_crc32c` — CRC32C of the raw payload bytes.
//!
//! The `JournalHeader` checksum covers bytes 0–27 (the checksum field excluded).

pub(crate) mod header;
pub(crate) mod logical;
pub(crate) mod payloads;
pub(crate) mod record;

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Wire-format re-exports — the single facade home for every journal wire type.
//
// The `log_file.rs` re-export shim was deleted in R4; consumers that used to
// reach these through the old `journal::log_file::*` path now reach them
// through `journal::wire::*`. The owning submodules (`header`, `record`,
// `payloads`, `logical`) are the canonical definition sites; this block only
// re-surfaces them at the module root so call sites stay path-stable.
// ---------------------------------------------------------------------------

pub(crate) use self::header::{
    JournalHeader, JOURNAL_FORMAT_VERSION, JOURNAL_HEADER_SIZE, JOURNAL_MAGIC,
    RETIRED_PRE_RELEASE_JOURNAL_FORMAT_VERSIONS,
};
#[allow(unused_imports)]
pub(crate) use self::logical::{
    build_logical_txn_frame, try_skip_logical_txn, try_skip_logical_txn_disposition, DecodeCtx,
    LogicalOp, LogicalOpKind, LogicalScan, LogicalTxnFrame, OverflowRefWire, LOGICAL_OP_PREFIX_LEN,
};
#[allow(unused_imports)]
pub(crate) use self::payloads::{
    read_chain_commit_at_cursor, CatalogCommitKind, CatalogCommitPage, CatalogCommitPayload,
    ChainCommitFrame, ChainPageWrite, CheckpointBoundaryPayload, CheckpointPageFramePayload,
    CheckpointPagePool,
};
#[allow(unused_imports)]
pub(crate) use self::record::{
    FinalizedLogRecord, LogRecord, LogRecordDraft, LogRecordFlags, LogRecordKind, LogRecordPayload,
};

// ---------------------------------------------------------------------------
// Phase 8 LogRecord constants
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

// ---------------------------------------------------------------------------
// Frame-kind constants (used by logical and payloads modules)
// ---------------------------------------------------------------------------

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
// Page size indicator
// ---------------------------------------------------------------------------

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
        use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};
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
        use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};
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

// ---------------------------------------------------------------------------
// Shared little-endian read helpers (used by record, logical, payloads)
// ---------------------------------------------------------------------------

/// Read a `u16` little-endian from `buf` at `offset`.
pub(super) fn read_u16(buf: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(buf[offset..offset + 2].try_into().expect("2 bytes"))
}

/// Read a `u32` little-endian from `buf` at `offset`.
pub(super) fn read_u32(buf: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(buf[offset..offset + 4].try_into().expect("4 bytes"))
}

/// Read a `u64` little-endian from `buf` at `offset`.
pub(super) fn read_u64(buf: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(buf[offset..offset + 8].try_into().expect("8 bytes"))
}

/// Convert a `usize` to `u16`, returning an error on overflow.
pub(super) fn len_to_u16(len: usize) -> Result<u16> {
    u16::try_from(len).map_err(|_| invalid_log_record("Phase 8 LogRecord length exceeds u16"))
}

/// Convert a `usize` to `u32`, returning an error on overflow.
pub(super) fn len_to_u32(len: usize) -> Result<u32> {
    u32::try_from(len).map_err(|_| invalid_log_record("Phase 8 LogRecord length exceeds u32"))
}

/// Construct a `JournalFrameTooLarge` error for an oversized record.
pub(super) fn log_record_too_large(total_len: usize) -> Error {
    Error::JournalFrameTooLarge {
        logical_frame_bytes: total_len,
        max_bytes: MAX_LOG_RECORD_BYTES,
    }
}

/// Construct a recoverable `CorruptDatabase` error for a malformed log record.
pub(super) fn invalid_log_record(detail: impl Into<String>) -> Error {
    Error::CorruptDatabase {
        path: std::path::PathBuf::new(),
        detail: detail.into(),
        recoverable: true,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/log_file_codec.rs"]
mod log_file_codec;
