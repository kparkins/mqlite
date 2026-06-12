//! Payload codecs for `CatalogCommitPayload`, `CheckpointBoundaryPayload`,
//! `CheckpointPageFramePayload`, and `ChainCommitFrame`.
//!
//! Each type owns its own magic tag and version byte so recovery can validate
//! each payload independently of the outer [`LogRecord`](super::record::LogRecord) header.
//!
//! ## Magic tags
//!
//! | Type                        | Magic  | Version |
//! |-----------------------------|--------|---------|
//! | `CatalogCommitPayload`      | `MQCC` | 1       |
//! | `CheckpointBoundaryPayload` | `MQCB` | 1       |
//! | `CheckpointPageFramePayload`| `MQCP` | 1       |
//! | `ChainCommitFrame`          | kind byte `0x02`, no payload magic |

use std::io::{self, Read, Seek, SeekFrom};

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};

use super::{
    invalid_log_record, len_to_u32, read_u16, read_u32, read_u64, JournalPageSize,
    CHAIN_COMMIT_FIXED_HEADER_LEN, CHAIN_COMMIT_MAX_FRAME_SIZE, FRAME_KIND_CHAIN_COMMIT,
};

// ---------------------------------------------------------------------------
// Payload magic / version constants
// ---------------------------------------------------------------------------

const CATALOG_COMMIT_PAYLOAD_MAGIC: [u8; 4] = *b"MQCC";
const CATALOG_COMMIT_PAYLOAD_VERSION: u16 = 1;
const CHECKPOINT_BOUNDARY_PAYLOAD_MAGIC: [u8; 4] = *b"MQCB";
const CHECKPOINT_BOUNDARY_PAYLOAD_VERSION: u16 = 1;
const CHECKPOINT_PAGE_FRAME_PAYLOAD_MAGIC: [u8; 4] = *b"MQCP";
const CHECKPOINT_PAGE_FRAME_PAYLOAD_VERSION: u16 = 1;

// ---------------------------------------------------------------------------
// CatalogCommitKind
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// CatalogCommitPage
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// CatalogCommitPayload
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// CheckpointBoundaryPayload
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// CheckpointPagePool
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// CheckpointPageFramePayload
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// ChainPageWrite
// ---------------------------------------------------------------------------

/// Page-write entry carried inside a `ChainCommit` frame.
///
/// Byte layout: `(page: u32 LE, page_size: u8, reserved: [u8; 3],
/// data: [u8; page_size_bytes])`. `page_size == 0` selects
/// [`PAGE_SIZE_INTERNAL`](crate::storage::page::PAGE_SIZE_INTERNAL);
/// `page_size == 1` selects
/// [`PAGE_SIZE_LEAF`](crate::storage::page::PAGE_SIZE_LEAF).
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct ChainPageWrite {
    /// Page number in the main database file.
    pub page: u32,
    /// Physical size of this page.
    pub page_size: JournalPageSize,
    /// Full page bytes captured from the dirty buffer.
    pub data: Vec<u8>,
}

impl ChainPageWrite {
    /// Total encoded byte size (8 B header + payload).
    fn encoded_len(&self) -> usize {
        8 + self.page_size.bytes()
    }
}

// ---------------------------------------------------------------------------
// ChainCommitFrame
// ---------------------------------------------------------------------------

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
    /// Database-lifetime salt 1; verified during recovery.
    pub salt1: u32,
    /// Database-lifetime salt 2; verified during recovery.
    pub salt2: u32,
    /// Commit timestamp from the writing transaction.
    pub commit_ts: Ts,
    /// Reference-count adjustments for pages modified by this commit.
    pub refcount_deltas: Vec<(u32, i32)>,
    /// Full page images written by this commit.
    pub page_writes: Vec<ChainPageWrite>,
}

impl ChainCommitFrame {
    /// Build and encode a `ChainCommit` payload from a transaction's drained
    /// commit data. This is the wire-side builder the MVCC commit path calls
    /// instead of constructing the frame literal itself — keeping wire-frame
    /// construction on the journal side. Returns the encoded payload bytes.
    pub(crate) fn build_payload(
        salt1: u32,
        salt2: u32,
        commit_ts: Ts,
        refcount_deltas: Vec<(u32, i32)>,
        page_writes: Vec<ChainPageWrite>,
    ) -> Result<Vec<u8>> {
        Self {
            salt1,
            salt2,
            commit_ts,
            refcount_deltas,
            page_writes,
        }
        .encode()
    }

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
// read_chain_commit_at_cursor
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
