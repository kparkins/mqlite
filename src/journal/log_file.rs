//! Journal file format — header, frame header, and I/O helpers.
//!
//! ## Journal File Layout
//!
//! ```text
//! [Journal Header — 32 bytes]
//! [Frame 0 Header — 24 bytes][Frame 0 Page Data — 4KB or 32KB]
//! [Frame 1 Header — 24 bytes][Frame 1 Page Data — 4KB or 32KB]
//! ...
//! ```
//!
//! ## Journal Header (32 bytes)
//!
//! ```text
//! Offset  Size  Field
//!   0      4    Magic: "MQJL" (0x4D514A4C)
//!   4      4    Format version: u32 LE (1)
//!   8      4    Page size internal: u32 LE (4096)
//!  12      4    Page size leaf: u32 LE (32768)
//!  16      4    Salt 1: u32 LE (must match main file header)
//!  20      4    Salt 2: u32 LE (must match main file header)
//!  24      4    Checkpoint sequence: u32 LE
//!  28      4    Header checksum: CRC32C of bytes 0–27
//! ```
//!
//! ## Journal Frame Header (24 bytes)
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
pub(crate) const JOURNAL_FORMAT_VERSION: u32 = 1;

/// Total size of the journal file header in bytes.
pub(crate) const JOURNAL_HEADER_SIZE: usize = 32;

/// Total size of a journal frame header in bytes (before page data).
pub(crate) const JOURNAL_FRAME_HEADER_SIZE: usize = 24;

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
/// Legacy page-write frames do not carry this byte at a known position — they
/// are identified by position within the journal and by the length/salt
/// fields of `JournalFrameHeader`. The `ChainCommit` discriminant is chosen
/// to be distinct from any plausible high-order byte of a `page_number` field
/// in the legacy frame format so a mixed journal can be recovered.
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

/// Frame-kind discriminant for Phase 2 checkpoint-commit-boundary frames
/// (§3.11).
///
/// Emitted by the checkpointer to mark the `[commit_ts_lo, commit_ts_hi]`
/// range covered by a completed checkpoint so recovery can drop pre-boundary
/// logical frames.
#[allow(dead_code)]
pub(crate) const FRAME_KIND_CHECKPOINT_COMMIT_BOUNDARY: u8 = 0x04;

/// Fixed byte length of a `CheckpointCommitBoundaryFrame` (§3.11).
///
/// Header (1 + 3 + 4) + salts (4 + 4) + checkpoint_epoch (8) + Ts lo (12) +
/// Ts hi (12) + overflow_cutoff_page (4) + trailing CRC32C (4) = 56 bytes.
#[allow(dead_code)]
pub(crate) const CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE: usize = 56;

// ---------------------------------------------------------------------------
// Phase 2 checkpoint-boundary newtypes (§3.11)
// ---------------------------------------------------------------------------

/// Monotonically increasing epoch counter allocated at each checkpoint.
///
/// Zero is reserved (§3.11); the encoder rejects zero before any byte is
/// produced so a torn zero-epoch frame can never appear on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(dead_code)]
pub(crate) struct CheckpointEpoch(pub u64);

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
// CheckpointCommitBoundaryFrame (§3.11)
// ---------------------------------------------------------------------------

/// Checkpoint-commit-boundary frame — emitted by the checkpointer (§3.11).
///
/// Records the `[commit_ts_lo, commit_ts_hi]` range fully reconciled to the
/// main file, plus the highest overflow page reconciled as part of the
/// checkpoint. Recovery (US-017, Phase 2 §3.11) uses the highest
/// `covers_commit_ts_hi` it observes to discard pre-boundary logical frames
/// before hand-off to Pass 2.
///
/// Fixed 56-byte on-disk layout:
///
/// ```text
///  0       1    frame_kind: u8 (0x04 = CHECKPOINT_COMMIT_BOUNDARY)
///  1       3    reserved_a: [u8; 3] (MUST be 0)
///  4       4    total_frame_bytes: u32 LE (= 56)
///  8       4    salt1: u32 LE
/// 12       4    salt2: u32 LE
/// 16       8    checkpoint_epoch: u64 LE (non-zero — zero is reserved)
/// 24      12    covers_commit_ts_lo: Ts-LE
/// 36      12    covers_commit_ts_hi: Ts-LE
/// 48       4    overflow_cutoff_page: u32 LE
/// 52       4    checksum_crc32: u32 LE (covers 0..52)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct CheckpointCommitBoundaryFrame {
    /// Database-lifetime salt 1; verified during recovery.
    pub salt1: u32,
    /// Database-lifetime salt 2; verified during recovery.
    pub salt2: u32,
    /// Monotonically increasing checkpoint epoch; zero is reserved (§3.11).
    pub checkpoint_epoch: u64,
    /// Lowest `commit_ts` covered by this boundary (inclusive).
    pub covers_commit_ts_lo: Ts,
    /// Highest `commit_ts` covered by this boundary (inclusive).
    pub covers_commit_ts_hi: Ts,
    /// Highest overflow page id reconciled by this checkpoint.
    pub overflow_cutoff_page: u32,
}

impl CheckpointCommitBoundaryFrame {
    /// Encode to the fixed 56-byte §3.11 layout.
    ///
    /// Returns `Err(Error::Internal)` when `checkpoint_epoch == 0` (zero is
    /// reserved per §3.11) before any byte is produced.
    #[allow(dead_code)]
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        if self.checkpoint_epoch == 0 {
            return Err(Error::Internal(
                "CheckpointCommitBoundaryFrame: checkpoint_epoch 0 is reserved (§3.11)".into(),
            ));
        }
        let total = CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE;
        let total_u32 = total as u32;

        let mut buf = Vec::with_capacity(total);
        buf.push(FRAME_KIND_CHECKPOINT_COMMIT_BOUNDARY);
        buf.extend_from_slice(&[0u8; 3]); // reserved_a
        buf.extend_from_slice(&total_u32.to_le_bytes());
        buf.extend_from_slice(&self.salt1.to_le_bytes());
        buf.extend_from_slice(&self.salt2.to_le_bytes());
        buf.extend_from_slice(&self.checkpoint_epoch.to_le_bytes());
        buf.extend_from_slice(&self.covers_commit_ts_lo.to_le_bytes());
        buf.extend_from_slice(&self.covers_commit_ts_hi.to_le_bytes());
        buf.extend_from_slice(&self.overflow_cutoff_page.to_le_bytes());

        debug_assert_eq!(buf.len(), total - 4, "CRC not yet appended");
        let cs = crc32c::crc32c(&buf);
        buf.extend_from_slice(&cs.to_le_bytes());
        debug_assert_eq!(buf.len(), total);

        Ok(buf)
    }

    /// Decode from bytes. Returns `Ok(None)` on any tail/salt/CRC/kind
    /// mismatch — a torn boundary frame is treated as absent (§3.11 point 4).
    #[allow(dead_code)]
    pub(crate) fn decode(
        buf: &[u8],
        expected_salt1: u32,
        expected_salt2: u32,
    ) -> Result<Option<Self>> {
        // 1. Need the full fixed-size frame.
        if buf.len() < CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE {
            return Ok(None);
        }

        // 2. frame_kind discriminant.
        if buf[0] != FRAME_KIND_CHECKPOINT_COMMIT_BOUNDARY {
            return Ok(None);
        }

        // 3. reserved_a MUST be zero.
        if buf[1] != 0 || buf[2] != 0 || buf[3] != 0 {
            return Ok(None);
        }

        // 4. Length prefix — fixed, must equal the frame size.
        let total = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes")) as usize;
        if total != CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE {
            return Ok(None);
        }

        // 5. Salts.
        let salt1 = u32::from_le_bytes(buf[8..12].try_into().expect("4 bytes"));
        let salt2 = u32::from_le_bytes(buf[12..16].try_into().expect("4 bytes"));
        if salt1 != expected_salt1 || salt2 != expected_salt2 {
            return Ok(None);
        }

        // 6. CRC32C over bytes [0 .. total - 4).
        let body_end = total - 4;
        let stored_cs = u32::from_le_bytes(buf[body_end..total].try_into().expect("4 bytes"));
        let computed_cs = crc32c::crc32c(&buf[..body_end]);
        if stored_cs != computed_cs {
            return Ok(None);
        }

        // 7. Parse body after CRC gate.
        let checkpoint_epoch = u64::from_le_bytes(buf[16..24].try_into().expect("8 bytes"));
        // Zero is reserved (§3.11); a CRC-valid frame with epoch 0 is treated
        // as absent so recovery skips it and resumes from the previous valid
        // boundary (§3.11 point 4).
        if checkpoint_epoch == 0 {
            return Ok(None);
        }
        let covers_commit_ts_lo = Ts::from_le_bytes(buf[24..36].try_into().expect("12 bytes"));
        let covers_commit_ts_hi = Ts::from_le_bytes(buf[36..48].try_into().expect("12 bytes"));
        let overflow_cutoff_page = u32::from_le_bytes(buf[48..52].try_into().expect("4 bytes"));

        Ok(Some(Self {
            salt1,
            salt2,
            checkpoint_epoch,
            covers_commit_ts_lo,
            covers_commit_ts_hi,
            overflow_cutoff_page,
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

// ---------------------------------------------------------------------------
// JournalFrameHeader
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

/// Parsed representation of a journal frame header (24 bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JournalFrameHeader {
    /// Page number this frame contains an image of.
    pub page_number: u32,
    /// Non-zero → commit frame: total database page count after this commit.
    /// Zero → non-commit (intermediate write within a transaction).
    pub db_page_count: u32,
    /// Salt 1 from the journal file header (verified on read).
    pub salt1: u32,
    /// Salt 2 from the journal file header (verified on read).
    pub salt2: u32,
    /// Page size for this frame's data segment.
    pub page_size: JournalPageSize,
    // checksum at offset 20 — computed/verified on read/write; not stored here
}

impl JournalFrameHeader {
    /// Compute the CRC32C checksum for a frame.
    ///
    /// Covers the first 20 bytes of the frame header (excluding the checksum
    /// field itself) followed by the entire page data.
    pub(crate) fn compute_checksum(header_prefix: &[u8; 20], page_data: &[u8]) -> u32 {
        let mut digest = crc32c::crc32c(header_prefix);
        digest = crc32c::crc32c_append(digest, page_data);
        digest
    }

    /// Serialize the header and write it plus page data to `w`.
    ///
    /// Returns the byte offset **before** writing (i.e., where this frame
    /// starts in the journal file), assuming `w` is positioned at the write cursor.
    pub(crate) fn write<W: Write>(&self, w: &mut W, page_data: &[u8]) -> io::Result<()> {
        debug_assert_eq!(page_data.len(), self.page_size.bytes());

        let mut buf = [0u8; JOURNAL_FRAME_HEADER_SIZE];
        buf[0..4].copy_from_slice(&self.page_number.to_le_bytes());
        buf[4..8].copy_from_slice(&self.db_page_count.to_le_bytes());
        buf[8..12].copy_from_slice(&self.salt1.to_le_bytes());
        buf[12..16].copy_from_slice(&self.salt2.to_le_bytes());
        buf[16..20].copy_from_slice(&self.page_size.as_u32().to_le_bytes());
        // Compute checksum over bytes 0–19 + page data
        let prefix: [u8; 20] = buf[..20].try_into().expect("20 bytes");
        let checksum = Self::compute_checksum(&prefix, page_data);
        buf[20..24].copy_from_slice(&checksum.to_le_bytes());

        w.write_all(&buf)?;
        w.write_all(page_data)?;
        Ok(())
    }

    /// Read and validate a frame header from `r`.
    ///
    /// `expected_salt1` and `expected_salt2` are the salt values from the journal
    /// file header.  Salt mismatch is treated as a checksum failure (stops
    /// recovery at this frame).
    ///
    /// Returns `None` when a checksum failure is detected (indicating the end
    /// of committed journal data).  Returns `Some(header)` on success.  On I/O
    /// error, returns `Err`.
    pub(crate) fn read<R: Read>(
        r: &mut R,
        expected_salt1: u32,
        expected_salt2: u32,
    ) -> Result<Option<Self>> {
        let mut buf = [0u8; JOURNAL_FRAME_HEADER_SIZE];
        match r.read_exact(&mut buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(Error::Io(e)),
        }

        let page_number = u32::from_le_bytes(buf[0..4].try_into().expect("4 bytes"));
        let db_page_count = u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes"));
        let salt1 = u32::from_le_bytes(buf[8..12].try_into().expect("4 bytes"));
        let salt2 = u32::from_le_bytes(buf[12..16].try_into().expect("4 bytes"));
        let page_size_u32 = u32::from_le_bytes(buf[16..20].try_into().expect("4 bytes"));
        let stored_checksum = u32::from_le_bytes(buf[20..24].try_into().expect("4 bytes"));

        // Salt mismatch → treat as bad checksum (stop recovery)
        if salt1 != expected_salt1 || salt2 != expected_salt2 {
            return Ok(None);
        }

        // Phase 2: invalid `page_size` indicates these bytes are NOT a
        // valid legacy frame. With mixed-format journals (logical +
        // ChainCommit + boundary + legacy), unknown page_size most
        // commonly means a torn typed frame whose `try_skip_*` helper
        // returned `None`. Return `Ok(None)` to halt the linear scan
        // gracefully rather than propagating a hard `CorruptDatabase`
        // — the surviving bytes are not durable per §3.7/§3.11 anyway.
        let page_size = match JournalPageSize::from_u32(page_size_u32) {
            Ok(ps) => ps,
            Err(_) => return Ok(None),
        };

        // Read page data
        let mut page_data = vec![0u8; page_size.bytes()];
        match r.read_exact(&mut page_data) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(Error::Io(e)),
        }

        // Verify checksum
        let prefix: [u8; 20] = buf[..20].try_into().expect("20 bytes");
        let computed = Self::compute_checksum(&prefix, &page_data);
        if computed != stored_checksum {
            return Ok(None); // Bad checksum — stop here
        }

        Ok(Some(Self {
            page_number,
            db_page_count,
            salt1,
            salt2,
            page_size,
        }))
    }

    /// Return the total byte size of this frame (header + page data).
    pub(crate) fn total_size(&self) -> usize {
        JOURNAL_FRAME_HEADER_SIZE + self.page_size.bytes()
    }
}

// ---------------------------------------------------------------------------
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
// try_skip_chain_commit
// ---------------------------------------------------------------------------

/// Peek at the current position and skip over a `ChainCommitFrame` if one is
/// present, returning the number of bytes consumed.
///
/// `JournalFrameHeader` and `ChainCommitFrame` cohabit the same append-only
/// log. Every scanner that iterates frames linearly must call this helper
/// before falling through to `JournalFrameHeader::read`; a `ChainCommit`
/// frame interpreted as a legacy header errors out on the `page_size` field
/// (invalid) and corrupts any `truncate_to` / recovery scan.
///
/// ## Disambiguation
///
/// A legacy frame with `page_number == 2` has an identical first 4 bytes
/// (`[2, 0, 0, 0]`) to a `ChainCommit` header prefix. This helper performs
/// the full `ChainCommitFrame::decode` CRC check to tell them apart. Matching
/// CRCs for a 32+ byte header on random legacy data is astronomically
/// unlikely (~1 in 2^32).
///
/// ## Cursor semantics
///
/// - On `Ok(Some((n, commit_ts)))`: the reader is positioned at the next
///   frame, `n` is the number of bytes the `ChainCommit` consumed, and
///   `commit_ts` is the decoded commit timestamp (carried out so recovery
///   can fold it into `TimestampOracle::set_min`).
/// - On `Ok(None)`: the reader is restored to its original position. The
///   caller proceeds to `JournalFrameHeader::read` as normal.
/// - On `Err`: the reader position is undefined; the caller should treat
///   the scan as aborted.
pub(crate) fn try_skip_chain_commit<R: Read + Seek>(
    r: &mut R,
    expected_salt1: u32,
    expected_salt2: u32,
) -> Result<Option<(u64, Ts)>> {
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
        Some(frame) => Ok(Some((total_frame_bytes as u64, frame.commit_ts))),
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
/// Mirrors [`BoundaryScan`]: distinguishes "this is not a logical frame" from
/// "this IS a torn logical frame" so the recovery loop can tick the
/// `logical_txn_torn_frames_total` counter (§7) on the latter without
/// otherwise changing scan behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum LogicalScan {
    /// Bytes are NOT a logical frame attempt — try the next helper or fall
    /// through to legacy.
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

    // Logical decode rejected (truncated body or CRC mismatch). The
    // structural signature aliases a legacy frame whose `page_number` LSB
    // is `FRAME_KIND_LOGICAL_TXN` (=0x03), high bytes 0, salts at the
    // expected offsets, and `db_page_count` ∈
    // [`LOGICAL_TXN_MIN_FRAME_SIZE`, `LOGICAL_TXN_MAX_FRAME_SIZE`]. Probe
    // the same offset as a legacy frame and verify its CRC; if it
    // validates the bytes are unambiguously a real legacy frame and the
    // helper must report `NotLogical` so the legacy parser handles them.
    // CRC32C collision on a 4 KB / 32 KB buffer is ~2^-32, so this fallback
    // is effectively unambiguous in practice.
    r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
    let mut legacy_hdr = [0u8; JOURNAL_FRAME_HEADER_SIZE];
    let legacy_hdr_complete = match r.read_exact(&mut legacy_hdr) {
        Ok(()) => true,
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => false,
        Err(e) => return Err(Error::Io(e)),
    };
    if legacy_hdr_complete {
        let legacy_page_size_u32 =
            u32::from_le_bytes(legacy_hdr[16..20].try_into().expect("4 bytes"));
        let legacy_page_size = match legacy_page_size_u32 {
            PAGE_SIZE_INTERNAL => Some(PAGE_SIZE_INTERNAL as usize),
            PAGE_SIZE_LEAF => Some(PAGE_SIZE_LEAF as usize),
            _ => None,
        };
        if let Some(page_bytes) = legacy_page_size {
            let mut legacy_page_data = vec![0u8; page_bytes];
            let legacy_data_complete = match r.read_exact(&mut legacy_page_data) {
                Ok(()) => true,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => false,
                Err(e) => return Err(Error::Io(e)),
            };
            if legacy_data_complete {
                let legacy_stored_crc =
                    u32::from_le_bytes(legacy_hdr[20..24].try_into().expect("4 bytes"));
                let legacy_prefix: [u8; 20] = legacy_hdr[..20].try_into().expect("20 bytes");
                let legacy_computed_crc =
                    JournalFrameHeader::compute_checksum(&legacy_prefix, &legacy_page_data);
                if legacy_stored_crc == legacy_computed_crc {
                    r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
                    return Ok(LogicalScan::NotLogical);
                }
            }
        }
    }

    // Neither logical decode nor legacy CRC validated → genuine torn
    // logical frame.
    r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
    Ok(LogicalScan::Torn)
}

/// Peek at the current position and skip over a [`LogicalTxnFrame`] if one is
/// present, returning the number of bytes consumed and the decoded frame.
///
/// Mirrors [`try_skip_chain_commit`]: every linear journal scanner must call
/// this helper before falling through to [`JournalFrameHeader::read`] so a
/// logical-transaction frame is not misinterpreted as a legacy page-write
/// frame (§6.1). Always uses [`DecodeCtx::Scanning`] — every §4.6
/// disposition-table failure row rewinds the reader and returns `Ok(None)`.
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
/// - On `Ok(None)`: the reader is restored to its original position. The
///   caller proceeds to the next skip helper or to
///   [`JournalFrameHeader::read`] as normal.
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
// try_skip_checkpoint_commit_boundary
// ---------------------------------------------------------------------------

/// Disposition returned by [`try_skip_checkpoint_commit_boundary`] (§3.11).
///
/// Distinguishes three cases that share the same cursor contract but demand
/// different scanner behavior:
///
/// - `NotBoundary` — no boundary at this offset; caller proceeds to the next
///   skip helper. Reader position is restored.
/// - `Torn` — the byte at this offset IS the boundary kind byte `0x04` but
///   the frame is truncated or fails CRC. Per §3.11 point 4 the scan must
///   HALT here; falling through to legacy parsing would risk interpreting
///   boundary bytes as a legacy page-write header (the leading `0x04` maps
///   to legacy `page_number = 4`, `total_frame_bytes` lands in legacy
///   `db_page_count`, and the `checkpoint_epoch` low bytes would land in
///   the legacy `page_size` field — which surfaces as a hard
///   `CorruptDatabase` via `JournalPageSize::from_u32`). Reader position
///   is restored.
/// - `Valid(n, frame)` — a fully-validated boundary frame was consumed;
///   reader advanced by `n` bytes.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum BoundaryScan {
    /// Not a boundary frame at this offset.
    NotBoundary,
    /// Boundary kind byte present but the frame is torn — CRC mismatch,
    /// truncated prefix, or any other decode failure after the discriminant.
    Torn,
    /// A valid boundary frame was read and decoded.
    Valid(u64, CheckpointCommitBoundaryFrame),
}

/// Peek at the current position and skip over a
/// [`CheckpointCommitBoundaryFrame`] if one is present (§3.11).
///
/// Mirrors [`try_skip_chain_commit`] / [`try_skip_logical_txn`] but returns
/// the [`BoundaryScan`] tri-state so the caller can distinguish "this is not
/// a boundary, try the next helper" from "this IS a torn boundary — halt
/// the scan" (§3.11 point 4).
///
/// # Disambiguation
///
/// The boundary frame's first 16 bytes (kind + reserved + length + salts)
/// CAN ALIAS a valid legacy page-write frame: a legacy frame with
/// `page_number == 4`, `db_page_count == CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE`
/// (56), and matching salts has the exact same structural signature. CRC32C
/// then disambiguates definitively:
///
/// 1. Quick-reject on the structural signature (kind + reserved + length +
///    salts). If it does not match, the bytes are unambiguously NOT a
///    boundary attempt → `NotBoundary`.
/// 2. Otherwise read the full 56-byte boundary frame and verify CRC. If
///    valid → `Valid`.
/// 3. Boundary CRC failed: try the same bytes as a legacy frame (24-byte
///    header + page payload + CRC). If the legacy CRC validates the bytes
///    are a real legacy frame that happens to alias the boundary header
///    layout → `NotBoundary` (legacy parser handles the bytes).
/// 4. Neither boundary nor legacy CRC validates → genuine torn boundary
///    per §3.11 point 4 → `Torn`.
///
/// In the fallback (boundary CRC failed, then probing as legacy), only
/// ONE CRC event needs to validate accidentally for the helper to
/// misclassify torn boundary bytes as a legacy frame. CRC32C collision
/// on a single 4 KB / 32 KB buffer is ~2^-32, so the probe is
/// effectively unambiguous in practice but not 2^-64.
///
/// # Cursor semantics
///
/// - `Valid(n, frame)`: reader is at `start + n`.
/// - `Torn` / `NotBoundary`: reader is restored to `start`.
/// - `Err`: reader position is undefined; caller should abort.
#[allow(dead_code)]
pub(crate) fn try_skip_checkpoint_commit_boundary<R: Read + Seek>(
    r: &mut R,
    expected_salt1: u32,
    expected_salt2: u32,
) -> Result<BoundaryScan> {
    let start = r.stream_position().map_err(Error::Io)?;

    // Step 1: peek the 16-byte structural signature (kind + reserved +
    // length + salts). If any field doesn't match the boundary layout
    // we report `NotBoundary`.
    let mut sig = [0u8; 16];
    match r.read_exact(&mut sig) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
            r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
            return Ok(BoundaryScan::NotBoundary);
        }
        Err(e) => return Err(Error::Io(e)),
    }
    let kind_match = sig[0] == FRAME_KIND_CHECKPOINT_COMMIT_BOUNDARY;
    let reserved_zero = sig[1] == 0 && sig[2] == 0 && sig[3] == 0;
    let length = u32::from_le_bytes(sig[4..8].try_into().expect("4 bytes")) as usize;
    let length_match = length == CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE;
    let fs1 = u32::from_le_bytes(sig[8..12].try_into().expect("4 bytes"));
    let fs2 = u32::from_le_bytes(sig[12..16].try_into().expect("4 bytes"));
    let salts_match = fs1 == expected_salt1 && fs2 == expected_salt2;

    if !(kind_match && reserved_zero && length_match && salts_match) {
        r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
        return Ok(BoundaryScan::NotBoundary);
    }

    // Step 2: bytes are structurally a boundary attempt. Read the full
    // 56-byte payload and try to decode as a boundary.
    r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
    let mut bbuf = [0u8; CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE];
    let boundary_buf_complete = match r.read_exact(&mut bbuf) {
        Ok(()) => true,
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => false,
        Err(e) => return Err(Error::Io(e)),
    };
    if boundary_buf_complete {
        if let Some(frame) =
            CheckpointCommitBoundaryFrame::decode(&bbuf, expected_salt1, expected_salt2)?
        {
            return Ok(BoundaryScan::Valid(
                CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE as u64,
                frame,
            ));
        }
    }

    // Step 3: boundary decode failed (CRC mismatch or truncated). Probe
    // the same offset as a legacy frame to disambiguate "torn boundary"
    // from "valid legacy frame whose header aliases the boundary
    // signature" (e.g. page_number=4, db_page_count=56). If the legacy
    // CRC validates → NotBoundary; let the legacy parser handle it.
    r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
    let mut legacy_hdr = [0u8; JOURNAL_FRAME_HEADER_SIZE];
    let legacy_hdr_complete = match r.read_exact(&mut legacy_hdr) {
        Ok(()) => true,
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => false,
        Err(e) => return Err(Error::Io(e)),
    };
    if legacy_hdr_complete {
        let legacy_page_size_u32 =
            u32::from_le_bytes(legacy_hdr[16..20].try_into().expect("4 bytes"));
        let legacy_page_size = match legacy_page_size_u32 {
            PAGE_SIZE_INTERNAL => Some(PAGE_SIZE_INTERNAL as usize),
            PAGE_SIZE_LEAF => Some(PAGE_SIZE_LEAF as usize),
            _ => None,
        };
        if let Some(page_bytes) = legacy_page_size {
            let mut legacy_page_data = vec![0u8; page_bytes];
            let legacy_data_complete = match r.read_exact(&mut legacy_page_data) {
                Ok(()) => true,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => false,
                Err(e) => return Err(Error::Io(e)),
            };
            if legacy_data_complete {
                let legacy_stored_crc =
                    u32::from_le_bytes(legacy_hdr[20..24].try_into().expect("4 bytes"));
                let legacy_prefix: [u8; 20] = legacy_hdr[..20].try_into().expect("20 bytes");
                let legacy_computed_crc =
                    JournalFrameHeader::compute_checksum(&legacy_prefix, &legacy_page_data);
                if legacy_stored_crc == legacy_computed_crc {
                    // Bytes are a valid legacy frame that happens to
                    // alias the boundary signature (e.g. page_number=4,
                    // db_page_count=56). Defer to the legacy parser.
                    r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
                    return Ok(BoundaryScan::NotBoundary);
                }
            }
        }
    }

    // Neither boundary nor legacy CRC validated → genuine torn boundary.
    r.seek(SeekFrom::Start(start)).map_err(Error::Io)?;
    Ok(BoundaryScan::Torn)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "log_file_tests.rs"]
mod tests;
