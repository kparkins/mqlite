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
// FrameKind (MVCC T2+)
// ---------------------------------------------------------------------------
//
// Post-T2 the journal distinguishes legacy page-write commit frames from the
// MVCC `ChainCommit` frame introduced for version-chain installations. Byte
// layout for `ChainCommit` is pinned in Format Lock Appendix §A.2 of the
// MVCC plan:
//
//   offset  size  field
//    0       1    frame_kind: u8 (0x02 = CHAIN_COMMIT; 0x01 = legacy commit)
//    1       3    reserved: [u8; 3] (MUST be 0)
//    4       4    total_frame_bytes: u32 LE (length prefix — MAJOR-2 fix)
//    8       4    salt1: u32 LE
//   12       4    salt2: u32 LE
//   16      12    commit_ts: Ts-LE (physical_ms u64 LE || logical u32 LE)
//   28       4    refcount_delta_count: u32 LE
//   32       N    refcount_deltas: [(page: u32, delta: i32)] × count
//   32+N     4    page_write_count: u32 LE
//   36+N     M    page_writes[]
//   36+N+M   4    checksum_crc32: u32 LE (covers bytes 0..36+N+M)
//
// T2 pins the discriminants and the fixed-size header offsets so later tasks
// (T3/T5'/T6) can plumb the frame through the writer/recovery paths without
// re-opening the format lock.

/// Discriminant byte at offset 0 of any frame introduced post-T2.
///
/// Legacy page-write frames emitted before T2 do not carry this byte — they
/// are identified by position within the journal and by the length/salt
/// fields of `JournalFrameHeader`. The `ChainCommit` byte is chosen to be
/// distinct from any plausible high-order byte of a `page_number` field
/// in the legacy frame format so a mixed journal can be recovered.
#[allow(dead_code)]
pub(crate) const FRAME_KIND_LEGACY_COMMIT: u8 = 0x01;

/// Frame-kind discriminant for MVCC chain-commit frames (Format Lock §A.2).
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

        let page_size = JournalPageSize::from_u32(page_size_u32)?;

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
// ChainCommit frame (MVCC T3 — Format Lock §A.2)
// ---------------------------------------------------------------------------

/// Page-write entry carried inside a `ChainCommit` frame.
///
/// Byte layout (§A.2): `(page: u32 LE, page_size: u8, reserved: [u8; 3],
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

/// MVCC chain-commit frame — one emitted per `WriteTxn::commit()`.
///
/// Byte layout per Format Lock Appendix §A.2:
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

        let delta_count = u32::try_from(self.refcount_deltas.len()).map_err(|_| {
            Error::Internal("ChainCommit refcount_delta_count exceeds u32".into())
        })?;
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
    /// the recovery caller treats every such outcome as frame-not-present
    /// per §A.2. Returns `Err` only on programmer error (callers pass an
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
        let total_frame_bytes =
            u32::from_le_bytes(buf[4..8].try_into().expect("4 bytes")) as usize;
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
            let page =
                u32::from_le_bytes(buf[cursor..cursor + 4].try_into().expect("4 bytes"));
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
            let page =
                u32::from_le_bytes(buf[cursor..cursor + 4].try_into().expect("4 bytes"));
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_header() -> JournalHeader {
        JournalHeader::new(0xDEAD_BEEF, 0xCAFE_BABE)
    }

    #[test]
    fn journal_header_roundtrip() {
        let h = sample_header();
        let bytes = h.to_bytes();
        let decoded = JournalHeader::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.magic, JOURNAL_MAGIC);
        assert_eq!(decoded.format_version, JOURNAL_FORMAT_VERSION);
        assert_eq!(decoded.salt1, 0xDEAD_BEEF);
        assert_eq!(decoded.salt2, 0xCAFE_BABE);
        assert_eq!(decoded.checkpoint_seq, 0);
    }

    #[test]
    fn journal_header_bad_magic_rejected() {
        let mut bytes = sample_header().to_bytes();
        bytes[0] = b'X';
        // Recompute checksum so only magic is wrong
        let checksum = crc32c::crc32c(&bytes[..28]);
        bytes[28..32].copy_from_slice(&checksum.to_le_bytes());
        assert!(JournalHeader::from_bytes(&bytes).is_err());
    }

    #[test]
    fn journal_header_checksum_failure_rejected() {
        let mut bytes = sample_header().to_bytes();
        bytes[10] ^= 0xFF; // corrupt within checksum range
        assert!(JournalHeader::from_bytes(&bytes).is_err());
    }

    #[test]
    fn frame_header_size_constant() {
        assert_eq!(JOURNAL_FRAME_HEADER_SIZE, 24);
        assert_eq!(JOURNAL_HEADER_SIZE, 32);
    }

    #[test]
    fn frame_roundtrip_4k() {
        let frame = JournalFrameHeader {
            page_number: 42,
            db_page_count: 100,
            salt1: 0xDEAD,
            salt2: 0xBEEF,
            page_size: JournalPageSize::Small4k,
        };
        let page_data = vec![0xABu8; PAGE_SIZE_INTERNAL as usize];
        let mut buf = Vec::new();
        frame.write(&mut buf, &page_data).unwrap();
        assert_eq!(
            buf.len(),
            JOURNAL_FRAME_HEADER_SIZE + PAGE_SIZE_INTERNAL as usize
        );

        // Read back
        let mut cursor = std::io::Cursor::new(&buf);
        let decoded = JournalFrameHeader::read(&mut cursor, 0xDEAD, 0xBEEF)
            .unwrap()
            .expect("should parse");
        assert_eq!(decoded.page_number, 42);
        assert_eq!(decoded.db_page_count, 100);
        assert_eq!(decoded.page_size, JournalPageSize::Small4k);
    }

    #[test]
    fn frame_roundtrip_32k() {
        let frame = JournalFrameHeader {
            page_number: 7,
            db_page_count: 0,
            salt1: 1,
            salt2: 2,
            page_size: JournalPageSize::Large32k,
        };
        let page_data = vec![0x5Au8; PAGE_SIZE_LEAF as usize];
        let mut buf = Vec::new();
        frame.write(&mut buf, &page_data).unwrap();
        assert_eq!(buf.len(), JOURNAL_FRAME_HEADER_SIZE + PAGE_SIZE_LEAF as usize);

        let mut cursor = std::io::Cursor::new(&buf);
        let decoded = JournalFrameHeader::read(&mut cursor, 1, 2)
            .unwrap()
            .expect("should parse");
        assert_eq!(decoded.page_number, 7);
        assert_eq!(decoded.db_page_count, 0); // non-commit
    }

    #[test]
    fn frame_bad_checksum_returns_none() {
        let frame = JournalFrameHeader {
            page_number: 1,
            db_page_count: 10,
            salt1: 1,
            salt2: 2,
            page_size: JournalPageSize::Small4k,
        };
        let page_data = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        let mut buf = Vec::new();
        frame.write(&mut buf, &page_data).unwrap();
        // Corrupt a byte in the page data
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;

        let mut cursor = std::io::Cursor::new(&buf);
        let result = JournalFrameHeader::read(&mut cursor, 1, 2).unwrap();
        assert!(result.is_none(), "bad checksum must return None");
    }

    #[test]
    fn frame_salt_mismatch_returns_none() {
        let frame = JournalFrameHeader {
            page_number: 1,
            db_page_count: 10,
            salt1: 1,
            salt2: 2,
            page_size: JournalPageSize::Small4k,
        };
        let page_data = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        let mut buf = Vec::new();
        frame.write(&mut buf, &page_data).unwrap();

        // Read with wrong salts
        let mut cursor = std::io::Cursor::new(&buf);
        let result = JournalFrameHeader::read(&mut cursor, 99, 99).unwrap();
        assert!(result.is_none(), "salt mismatch must return None");
    }

    #[test]
    fn journal_page_size_roundtrip() {
        assert_eq!(JournalPageSize::from_u32(4096).unwrap(), JournalPageSize::Small4k);
        assert_eq!(JournalPageSize::from_u32(32768).unwrap(), JournalPageSize::Large32k);
        assert!(JournalPageSize::from_u32(9999).is_err());
    }

    // -----------------------------------------------------------------
    // ChainCommit frame tests (MVCC T3 / Format Lock §A.2)
    // -----------------------------------------------------------------

    fn sample_chain_commit() -> ChainCommitFrame {
        ChainCommitFrame {
            salt1: 0xDEAD_BEEF,
            salt2: 0xCAFE_BABE,
            commit_ts: Ts {
                physical_ms: 0x0011_2233_4455_6677,
                logical: 0x89AB_CDEF,
            },
            refcount_deltas: vec![(10, 1), (20, -1), (u32::MAX - 1, 42)],
            page_writes: vec![
                ChainPageWrite {
                    page: 100,
                    page_size: JournalPageSize::Small4k,
                    data: vec![0xAAu8; PAGE_SIZE_INTERNAL as usize],
                },
                ChainPageWrite {
                    page: 200,
                    page_size: JournalPageSize::Large32k,
                    data: vec![0x5Au8; PAGE_SIZE_LEAF as usize],
                },
            ],
        }
    }

    #[test]
    fn chain_commit_roundtrip() {
        let frame = sample_chain_commit();
        let bytes = frame.encode().unwrap();
        assert_eq!(bytes.len(), frame.total_frame_bytes());
        let decoded = ChainCommitFrame::decode(&bytes, frame.salt1, frame.salt2)
            .unwrap()
            .expect("round-trip must decode");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn chain_commit_empty_payload_roundtrip() {
        let frame = ChainCommitFrame {
            salt1: 1,
            salt2: 2,
            commit_ts: Ts { physical_ms: 0, logical: 0 },
            refcount_deltas: vec![],
            page_writes: vec![],
        };
        let bytes = frame.encode().unwrap();
        // 32-byte fixed header + 4-byte page_write_count + 4-byte CRC = 40.
        assert_eq!(bytes.len(), 40);
        assert_eq!(frame.total_frame_bytes(), 40);
        let decoded = ChainCommitFrame::decode(&bytes, 1, 2).unwrap().expect("decode");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn chain_commit_total_frame_bytes_bound_min() {
        let frame = ChainCommitFrame {
            salt1: 1,
            salt2: 2,
            commit_ts: Ts::PENDING,
            refcount_deltas: vec![],
            page_writes: vec![],
        };
        // §A.2 minimum: 32-byte fixed header + 4-byte page_write_count + 4-byte CRC.
        assert_eq!(frame.total_frame_bytes(), 40);
    }

    #[test]
    fn chain_commit_truncation_returns_none() {
        let frame = sample_chain_commit();
        let bytes = frame.encode().unwrap();
        // Every prefix shorter than total must decode as None, not panic.
        for n in 0..bytes.len() {
            let res = ChainCommitFrame::decode(&bytes[..n], frame.salt1, frame.salt2).unwrap();
            assert!(res.is_none(), "prefix of length {n} should be truncated");
        }
        // Full-length must succeed.
        let full = ChainCommitFrame::decode(&bytes, frame.salt1, frame.salt2)
            .unwrap()
            .expect("full decode");
        assert_eq!(full, frame);
    }

    #[test]
    fn chain_commit_salt_mismatch_returns_none() {
        let frame = sample_chain_commit();
        let bytes = frame.encode().unwrap();
        assert!(ChainCommitFrame::decode(&bytes, 0, frame.salt2).unwrap().is_none());
        assert!(ChainCommitFrame::decode(&bytes, frame.salt1, 0).unwrap().is_none());
    }

    #[test]
    fn chain_commit_corrupt_checksum_returns_none() {
        let frame = sample_chain_commit();
        let mut bytes = frame.encode().unwrap();
        // Flip a byte in the body — CRC must now reject.
        bytes[40] ^= 0xFF;
        let res = ChainCommitFrame::decode(&bytes, frame.salt1, frame.salt2).unwrap();
        assert!(res.is_none(), "corrupt body must fail CRC and return None");
    }

    #[test]
    fn chain_commit_bad_frame_kind_returns_none() {
        let frame = sample_chain_commit();
        let mut bytes = frame.encode().unwrap();
        bytes[0] = 0xFF;
        assert!(
            ChainCommitFrame::decode(&bytes, frame.salt1, frame.salt2).unwrap().is_none(),
            "wrong frame_kind must return None, not parse as ChainCommit"
        );
    }

    #[test]
    fn chain_commit_bogus_length_prefix_returns_none() {
        let frame = sample_chain_commit();
        let mut bytes = frame.encode().unwrap();
        // total_frame_bytes field at offset 4..8 — set beyond MAX to hit the bound.
        let bogus = (CHAIN_COMMIT_MAX_FRAME_SIZE as u64 + 1) as u32;
        bytes[4..8].copy_from_slice(&bogus.to_le_bytes());
        assert!(
            ChainCommitFrame::decode(&bytes, frame.salt1, frame.salt2).unwrap().is_none(),
            "length prefix above MAX must reject before reading any count"
        );
    }

    #[test]
    fn chain_commit_inflated_delta_count_returns_none() {
        // An attacker-crafted frame whose refcount_delta_count claims more
        // deltas than the length prefix can accommodate must be rejected
        // before any out-of-bounds indexing.
        let frame = ChainCommitFrame {
            salt1: 1,
            salt2: 2,
            commit_ts: Ts::PENDING,
            refcount_deltas: vec![],
            page_writes: vec![],
        };
        let mut bytes = frame.encode().unwrap();
        // Poke refcount_delta_count = 1000 at offset 28..32 without resizing.
        bytes[28..32].copy_from_slice(&1000u32.to_le_bytes());
        let res = ChainCommitFrame::decode(&bytes, 1, 2).unwrap();
        assert!(res.is_none(), "count exceeding length prefix must return None");
    }
}
