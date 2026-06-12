//! `LogicalTxnFrame` encode/decode, `parse_ops`, per-op body parsers, and the
//! §4.6 disposition-table handling.
//!
//! ## Wire layout (§4.1)
//!
//! ```text
//! Offset  Size  Field
//!   0      1    frame_kind: u8 (0x03 = FRAME_KIND_LOGICAL_TXN)
//!   1      3    reserved_a: [u8; 3] (MUST be 0)
//!   4      4    total_frame_bytes: u32 LE
//!   8      4    salt1: u32 LE
//!  12      4    salt2: u32 LE
//!  16     12    commit_ts: Ts-LE (physical_ms u64 LE || logical u32 LE)
//!  28      8    diagnostic_txn_id: u64 LE
//!  36      2    format_version: u16 LE (1)
//!  38      2    flags: u16 LE (MUST be 0 in version 1)
//!  40      4    op_count: u32 LE
//!  44      4    reserved_b: u32 LE (MUST be 0)
//!  48      …    op bodies × op_count
//!  end-4   4    CRC32C of bytes 0..end-4
//! ```
//!
//! ## §4.6 disposition table
//!
//! The [`DecodeCtx`] tag controls how each failure row is surfaced:
//! - `Scanning` — every failure returns `Ok(None)`; the recovery loop rewinds.
//! - `MidStream` — content errors return `Err(CorruptDatabase)`; only
//!   tail-like rows (truncation, kind mismatch, salt mismatch) return `Ok(None)`.

use std::io::{self, Read, Seek, SeekFrom};

use crate::error::{Error, Result};
use crate::mvcc::timestamp::Ts;

use super::{
    FRAME_KIND_LOGICAL_TXN, LOGICAL_TXN_FIXED_HEADER_LEN,
    LOGICAL_TXN_FORMAT_VERSION, LOGICAL_TXN_MAX_FRAME_SIZE, LOGICAL_TXN_MAX_KEY_BYTES,
    LOGICAL_TXN_MAX_OP_COUNT, LOGICAL_TXN_MAX_VALUE_BYTES, LOGICAL_TXN_MIN_FRAME_SIZE,
};

use crate::mvcc::transaction::{PrimaryOp, PrimaryWrite, SecIndexOp, SecIndexWrite};

// ---------------------------------------------------------------------------
// Frame construction (wire-side builder for the commit path)
// ---------------------------------------------------------------------------

/// Build a [`LogicalTxnFrame`] from staged `sec_writes` + `primary_writes`
/// in the canonical emit order (secondaries first, primaries second) with a
/// dense `0..N` `op_ordinal` counter, so recovery replays the operations in
/// the same order they were emitted. Keeps frame-construction pure — no
/// `Cell` consumption, no journal I/O — so it can be exercised directly from
/// unit tests without also driving `append_logical_txn`.
pub(crate) fn build_logical_txn_frame(
    txn_id: u64,
    commit_ts: Ts,
    salt1: u32,
    salt2: u32,
    primary_writes: &[PrimaryWrite],
    sec_writes: &[SecIndexWrite],
) -> LogicalTxnFrame {
    let total = sec_writes.len().saturating_add(primary_writes.len());
    let mut ops: Vec<LogicalOp> = Vec::with_capacity(total);
    let mut ordinal: u32 = 0;

    for sw in sec_writes {
        let kind = match &sw.op {
            SecIndexOp::Insert { id_bytes } => LogicalOpKind::SecondaryInsert {
                index_id: sw.index_id,
                key: sw.key.clone(),
                id_bytes: id_bytes.clone(),
            },
            SecIndexOp::Delete => LogicalOpKind::SecondaryDelete {
                index_id: sw.index_id,
                key: sw.key.clone(),
            },
        };
        ops.push(LogicalOp {
            op_ordinal: ordinal,
            kind,
        });
        ordinal = ordinal.saturating_add(1);
    }

    for pw in primary_writes {
        let kind = match &pw.op {
            PrimaryOp::Insert { data } => LogicalOpKind::PrimaryInsert {
                ns_id: pw.ns_id,
                key: pw.key.clone(),
                value: data.clone(),
                overflow: None,
            },
            PrimaryOp::Update { data } => LogicalOpKind::PrimaryUpdate {
                ns_id: pw.ns_id,
                key: pw.key.clone(),
                value: data.clone(),
                overflow: None,
            },
            PrimaryOp::Delete => LogicalOpKind::PrimaryDelete {
                ns_id: pw.ns_id,
                key: pw.key.clone(),
            },
        };
        ops.push(LogicalOp {
            op_ordinal: ordinal,
            kind,
        });
        ordinal = ordinal.saturating_add(1);
    }

    LogicalTxnFrame {
        salt1,
        salt2,
        commit_ts,
        diagnostic_txn_id: txn_id,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops,
    }
}

// ---------------------------------------------------------------------------
// Per-op wire-format opcodes (§4.4)
// ---------------------------------------------------------------------------

const OP_KIND_PRIMARY_INSERT: u8 = 0x01;
const OP_KIND_PRIMARY_UPDATE: u8 = 0x02;
const OP_KIND_PRIMARY_DELETE: u8 = 0x03;
const OP_KIND_SECONDARY_INSERT: u8 = 0x11;
const OP_KIND_SECONDARY_DELETE: u8 = 0x12;

/// Fixed 8-byte per-op prefix: op_kind(1) + reserved(3) + op_ordinal(4).
pub(crate) const LOGICAL_OP_PREFIX_LEN: usize = 8;

/// Byte length of the serialized `OverflowRefWire` tail on primary
/// insert/update ops (first_page: u32 + total_len: u64).
const OVERFLOW_REF_WIRE_LEN: usize = 12;

// ---------------------------------------------------------------------------
// OverflowRefWire
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

// ---------------------------------------------------------------------------
// LogicalOpKind
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// LogicalOp
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// DecodeCtx
// ---------------------------------------------------------------------------

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
// LogicalTxnFrame
// ---------------------------------------------------------------------------

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
    pub(crate) fn total_frame_bytes(&self) -> usize {
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

// ---------------------------------------------------------------------------
// parse_ops and per-op body parsers
// ---------------------------------------------------------------------------

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
pub(super) fn parse_ops(
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
pub(super) fn dispose<T>(ctx: &DecodeCtx, recoverable: bool, detail: String) -> Result<Option<T>> {
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
// LogicalScan / try_skip_logical_txn
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
