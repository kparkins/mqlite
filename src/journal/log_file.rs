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
//!   4      4    Format version: u32 LE (2)
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

use std::io::{self, Read, Seek, SeekFrom, Write};

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;
use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic bytes identifying a valid `.mqlite-journal` journal file.
pub(crate) const JOURNAL_MAGIC: [u8; 4] = *b"MQJL";

/// Journal format version (increment on backward-incompatible changes).
pub(crate) const JOURNAL_FORMAT_VERSION: u32 = 2;

/// Pre-release journal format versions this build may safely discard.
pub(crate) const RETIRED_PRE_RELEASE_JOURNAL_FORMAT_VERSIONS: &[u32] = &[1];

/// Total size of the journal file header in bytes.
pub(crate) const JOURNAL_HEADER_SIZE: usize = 32;

/// Total size of a journal frame header in bytes (before page data).
pub(crate) const JOURNAL_FRAME_HEADER_SIZE: usize = 24;

/// Total size of a page-0 checkpoint boundary record.
pub(crate) const PAGE0_BOUNDARY_RECORD_WIRE_SIZE: usize =
    JOURNAL_FRAME_HEADER_SIZE + PAGE_SIZE_INTERNAL as usize;

// ---------------------------------------------------------------------------
// FrameKind
// ---------------------------------------------------------------------------
//
// The journal distinguishes legacy page-write commit frames from MVCC
// `ChainCommit` frames used for version-chain installations. Byte layout
// for `ChainCommit`:
//
//   offset  size  field
//    0       1    frame_kind: u8 (0x02 = CHAIN_COMMIT; 0x01 = legacy commit)
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

/// Discriminant byte at offset 0 of a legacy page-write commit frame.
///
/// Retired page-write records do not carry this byte at a known position. The
/// `ChainCommit` discriminant remains distinct from ordinary page identifiers.
#[allow(dead_code)]
pub(crate) const FRAME_KIND_LEGACY_COMMIT: u8 = 0x01;

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
// Checkpoint page-frame codecs
// ---------------------------------------------------------------------------

fn page_record_checksum(header_prefix: &[u8; 20], page_data: &[u8]) -> u32 {
    let mut digest = crc32c::crc32c(header_prefix);
    digest = crc32c::crc32c_append(digest, page_data);
    digest
}

pub(super) fn write_page_frame_record<W: Write>(
    w: &mut W,
    page_number: u32,
    db_page_count: u32,
    salt1: u32,
    salt2: u32,
    page_size: JournalPageSize,
    page_data: &[u8],
) -> io::Result<()> {
    debug_assert_eq!(page_data.len(), page_size.bytes());

    let mut buf = [0u8; JOURNAL_FRAME_HEADER_SIZE];
    buf[0..4].copy_from_slice(&page_number.to_le_bytes());
    buf[4..8].copy_from_slice(&db_page_count.to_le_bytes());
    buf[8..12].copy_from_slice(&salt1.to_le_bytes());
    buf[12..16].copy_from_slice(&salt2.to_le_bytes());
    buf[16..20].copy_from_slice(&page_size.as_u32().to_le_bytes());
    let prefix: [u8; 20] = buf[..20].try_into().expect("20 bytes");
    let checksum = page_record_checksum(&prefix, page_data);
    buf[20..24].copy_from_slice(&checksum.to_le_bytes());

    w.write_all(&buf)?;
    w.write_all(page_data)?;
    Ok(())
}

/// Checkpoint-batch page-frame codec for checkpoint-owned dirty pages.
///
/// This named codec owns checkpoint step-8 non-commit frames. It reuses the
/// page-frame byte layout exactly:
///
/// ```text
///  0       4    page_number: u32 LE
///  4       4    db_page_count: u32 LE (= 0 for checkpoint batch pages)
///  8       4    salt1: u32 LE
/// 12       4    salt2: u32 LE
/// 16       4    page_size: u32 LE
/// 20       4    checksum_crc32: u32 LE over bytes 0..20 + page data
/// 24       N    page data
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CheckpointBatchPageRecord {
    /// Page id captured by the checkpoint flush set.
    pub page_number: u32,
    /// Database-lifetime salt 1.
    pub salt1: u32,
    /// Database-lifetime salt 2.
    pub salt2: u32,
    /// Size of the page payload.
    pub page_size: JournalPageSize,
}

impl CheckpointBatchPageRecord {
    /// Compute the shared page-frame CRC32C.
    pub(crate) fn compute_checksum(header_prefix: &[u8; 20], page_data: &[u8]) -> u32 {
        page_record_checksum(header_prefix, page_data)
    }

    /// Write a checkpoint-owned non-commit page frame.
    ///
    /// # Errors
    ///
    /// Returns any I/O error raised by the target writer.
    pub(crate) fn write<W: Write>(&self, w: &mut W, page_data: &[u8]) -> io::Result<()> {
        write_page_frame_record(
            w,
            self.page_number,
            0,
            self.salt1,
            self.salt2,
            self.page_size,
            page_data,
        )
    }

    /// Read a checkpoint-owned non-commit page frame.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the reader.
    pub(crate) fn read<R: Read + Seek>(
        r: &mut R,
        expected_salt1: u32,
        expected_salt2: u32,
    ) -> Result<Option<Self>> {
        let Some((record, _page_data)) = Self::read_with_data(r, expected_salt1, expected_salt2)?
        else {
            return Ok(None);
        };
        Ok(Some(record))
    }

    /// Read a checkpoint-owned non-commit page frame and return its payload.
    ///
    /// The reader is rewound when the cursor does not contain a checkpoint
    /// batch record so callers can try a page-0 boundary at the same offset.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the reader.
    pub(crate) fn read_with_data<R: Read + Seek>(
        r: &mut R,
        expected_salt1: u32,
        expected_salt2: u32,
    ) -> Result<Option<(Self, Vec<u8>)>> {
        let start = r.stream_position().map_err(Error::Io)?;
        let mut buf = [0u8; JOURNAL_FRAME_HEADER_SIZE];
        match r.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
                return Ok(None);
            }
            Err(e) => return Err(Error::Io(e)),
        }

        let page_number = u32::from_le_bytes(buf[0..4].try_into().expect("4 bytes"));
        let db_page_count = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes"));
        let salt1 = u32::from_le_bytes(buf[8..12].try_into().expect("4 bytes"));
        let salt2 = u32::from_le_bytes(buf[12..16].try_into().expect("4 bytes"));
        let page_size_u32 = u32::from_le_bytes(buf[16..20].try_into().expect("4 bytes"));
        let stored_checksum = u32::from_le_bytes(buf[20..24].try_into().expect("4 bytes"));

        if db_page_count != 0 || salt1 != expected_salt1 || salt2 != expected_salt2 {
            r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
            return Ok(None);
        }
        let page_size = match JournalPageSize::from_u32(page_size_u32) {
            Ok(page_size) => page_size,
            Err(_) => {
                r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
                return Ok(None);
            }
        };
        let mut page_data = vec![0u8; page_size.bytes()];
        match r.read_exact(&mut page_data) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
                return Ok(None);
            }
            Err(e) => return Err(Error::Io(e)),
        }
        let prefix: [u8; 20] = buf[..20].try_into().expect("20 bytes");
        if page_record_checksum(&prefix, &page_data) != stored_checksum {
            r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
            return Ok(None);
        }

        Ok(Some((
            Self {
                page_number,
                salt1,
                salt2,
                page_size,
            },
            page_data,
        )))
    }

    /// Return the total serialized byte size for this page frame.
    pub(crate) fn total_size(&self) -> usize {
        JOURNAL_FRAME_HEADER_SIZE + self.page_size.bytes()
    }
}

/// Page-0 commit-boundary codec for the durable checkpoint frontier.
///
/// This named codec owns checkpoint step-9 page-0 boundary frames. It reuses
/// the same page-frame layout as ordinary committed page frames, with these
/// lock-in invariants:
///
/// ```text
///  0       4    page_number: u32 LE (= 0)
///  4       4    db_page_count: u32 LE (= staged FileHeader.total_page_count)
///  8       4    salt1: u32 LE
/// 12       4    salt2: u32 LE
/// 16       4    page_size: u32 LE (= 4096)
/// 20       4    checksum_crc32: u32 LE over bytes 0..20 + page-0 bytes
/// 24    4096    staged FileHeader bytes, including last_checkpoint_ts
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Page0BoundaryRecord {
    salt1: u32,
    salt2: u32,
    header: crate::storage::header::FileHeader,
}

impl Page0BoundaryRecord {
    /// Build a page-0 boundary from a staged checkpoint header.
    pub(crate) fn new(salt1: u32, salt2: u32, header: crate::storage::header::FileHeader) -> Self {
        Self {
            salt1,
            salt2,
            header,
        }
    }

    /// Return the decoded staged header.
    pub(crate) fn header(&self) -> &crate::storage::header::FileHeader {
        &self.header
    }

    /// Return the database page count covered by the boundary.
    pub(crate) fn db_page_count(&self) -> u32 {
        self.header.total_page_count
    }

    /// Return the checkpoint timestamp published by the staged header.
    pub(crate) fn checkpoint_ts(&self) -> Ts {
        self.header.last_checkpoint_ts
    }

    /// Write the page-0 boundary as a committed page-frame record.
    ///
    /// # Errors
    ///
    /// Returns any I/O error raised by the target writer.
    pub(crate) fn write<W: Write>(&self, w: &mut W) -> io::Result<()> {
        write_page_frame_record(
            w,
            0,
            self.header.total_page_count,
            self.salt1,
            self.salt2,
            JournalPageSize::Small4k,
            &self.header.to_bytes(),
        )
    }

    /// Probe the current cursor for a page-0 boundary and rewind on absence.
    ///
    /// # Errors
    ///
    /// Returns I/O errors from the reader or file-header parse errors for
    /// bytes that otherwise match the boundary page-frame shape.
    pub(crate) fn read<R: Read + Seek>(
        r: &mut R,
        expected_salt1: u32,
        expected_salt2: u32,
    ) -> Result<Option<Self>> {
        let start = r.stream_position().map_err(Error::Io)?;
        let mut buf = [0u8; JOURNAL_FRAME_HEADER_SIZE];
        match r.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
                return Ok(None);
            }
            Err(e) => return Err(Error::Io(e)),
        }

        let page_number = u32::from_le_bytes(buf[0..4].try_into().expect("4 bytes"));
        let db_page_count = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes"));
        let salt1 = u32::from_le_bytes(buf[8..12].try_into().expect("4 bytes"));
        let salt2 = u32::from_le_bytes(buf[12..16].try_into().expect("4 bytes"));
        let page_size_u32 = u32::from_le_bytes(buf[16..20].try_into().expect("4 bytes"));
        let stored_checksum = u32::from_le_bytes(buf[20..24].try_into().expect("4 bytes"));

        if page_number != 0
            || db_page_count == 0
            || salt1 != expected_salt1
            || salt2 != expected_salt2
            || page_size_u32 != JournalPageSize::Small4k.as_u32()
        {
            r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
            return Ok(None);
        }
        let mut page_data = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        match r.read_exact(&mut page_data) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
                return Ok(None);
            }
            Err(e) => return Err(Error::Io(e)),
        }
        let prefix: [u8; 20] = buf[..20].try_into().expect("20 bytes");
        if page_record_checksum(&prefix, &page_data) != stored_checksum {
            r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
            return Ok(None);
        }

        match Self::from_page_frame_parts(
            page_number,
            db_page_count,
            salt1,
            salt2,
            JournalPageSize::Small4k,
            &page_data,
        )? {
            Some(boundary) => Ok(Some(boundary)),
            None => {
                r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
                Ok(None)
            }
        }
    }

    /// Decode a page-0 boundary from already-read page-frame parts.
    ///
    /// # Errors
    ///
    /// Returns file-header parse errors when the page-frame shape says this
    /// is a boundary but the page-0 bytes are not a valid staged header.
    pub(crate) fn from_page_frame_parts(
        page_number: u32,
        db_page_count: u32,
        salt1: u32,
        salt2: u32,
        page_size: JournalPageSize,
        page_data: &[u8],
    ) -> Result<Option<Self>> {
        if page_number != 0 || db_page_count == 0 || page_size != JournalPageSize::Small4k {
            return Ok(None);
        }
        if page_data.len() != crate::storage::header::HEADER_PAGE_SIZE {
            return Ok(None);
        }
        let page: &[u8; crate::storage::header::HEADER_PAGE_SIZE] =
            page_data.try_into().expect("header page size checked");
        let header = crate::storage::header::FileHeader::from_bytes(page)?;
        if header.last_checkpoint_ts == Ts::default() {
            return Ok(None);
        }
        if header.total_page_count != db_page_count {
            return Err(Error::CorruptDatabase {
                path: std::path::PathBuf::new(),
                detail: format!(
                    "page-0 checkpoint boundary db_page_count {db_page_count} \
                     does not match staged header total_page_count {}",
                    header.total_page_count
                ),
                recoverable: false,
            });
        }
        Ok(Some(Self {
            salt1,
            salt2,
            header,
        }))
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
#[path = "log_file_tests.rs"]
mod tests;
