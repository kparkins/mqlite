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
}
