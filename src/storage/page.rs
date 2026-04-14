//! Page type definitions, header layouts, and CRC32C checksum helpers.
//!
//! ## Page types
//!
//! | Constant | Value | Size | Description |
//! |----------|-------|------|-------------|
//! | [`PAGE_TYPE_INTERNAL`] | 0x01 | 4 KB | B+ tree internal (branch) node |
//! | [`PAGE_TYPE_LEAF`]     | 0x02 | 32 KB | B+ tree leaf node |
//! | [`PAGE_TYPE_OVERFLOW`] | 0x05 | 32 KB | Overflow page for large documents |
//!
//! ## Checksum policy
//!
//! Every page stores a CRC32C checksum in a 4-byte field at **offset 4–7**.
//! The checksum covers bytes **0–3** and bytes **8 onward** (the checksum field
//! itself is excluded). Callers must recompute and store the checksum before
//! writing a page to disk, and must verify it after reading.

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Page type bytes
// ---------------------------------------------------------------------------

/// Internal (branch) B+ tree node page type byte.
pub(crate) const PAGE_TYPE_INTERNAL: u8 = 0x01;

/// Leaf B+ tree node page type byte.
pub(crate) const PAGE_TYPE_LEAF: u8 = 0x02;

/// Overflow page type byte (used for documents that exceed a single leaf page).
pub(crate) const PAGE_TYPE_OVERFLOW: u8 = 0x05;

// ---------------------------------------------------------------------------
// Page sizes
// ---------------------------------------------------------------------------

/// Internal node page size in bytes (4 KiB).
pub(crate) const PAGE_SIZE_INTERNAL: u32 = 4096;

/// Leaf node page size in bytes (32 KiB).
pub(crate) const PAGE_SIZE_LEAF: u32 = 32768;

// ---------------------------------------------------------------------------
// Header sizes
// ---------------------------------------------------------------------------

/// Size of the internal node header in bytes.
pub(crate) const INTERNAL_HEADER_SIZE: usize = 12;

/// Size of the leaf node header in bytes.
pub(crate) const LEAF_HEADER_SIZE: usize = 20;

/// Size of the overflow page header in bytes.
pub(crate) const OVERFLOW_HEADER_SIZE: usize = 16;

// ---------------------------------------------------------------------------
// Leaf page flags
// ---------------------------------------------------------------------------

/// Leaf flag: at least one cell in this leaf uses an overflow pointer.
pub(crate) const LEAF_FLAG_HAS_OVERFLOW: u8 = 0x01;

// ---------------------------------------------------------------------------
// Cell value type markers
// ---------------------------------------------------------------------------

/// Cell value type: the value is an inline length-prefixed BSON document.
pub(crate) const VALUE_TYPE_INLINE: u8 = 0x01;

/// Cell value type: the value continues on an overflow page chain.
/// The cell contains `(page_number: u32, total_length: u32)`.
pub(crate) const VALUE_TYPE_OVERFLOW: u8 = 0x02;

// ---------------------------------------------------------------------------
// Internal page header  (12 bytes)
// ---------------------------------------------------------------------------

/// Structured header of a 4 KB internal (branch) B+ tree node.
///
/// ## On-disk layout (12 bytes at start of page)
///
/// ```text
/// Offset  Size  Field
///  0       1    page_type: u8 (must be 0x01)
///  1       1    level: u8 (distance from leaf level; leaves = 0)
///  2       2    key_count: u16 LE (number of separator keys)
///  4       4    checksum: u32 LE (CRC32C over bytes 0–3 and 8 onward)
///  8       4    rightmost_child: u32 LE (page number of right-most child)
/// 12        …   key entries: [key_len(2 LE) | key(var) | child_page(4 LE)]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InternalPageHeader {
    /// Must be [`PAGE_TYPE_INTERNAL`] (0x01).
    pub page_type: u8,
    /// Distance from the leaf level. Leaf pages have level 0; the root of a
    /// 3-level tree has level 2.
    pub level: u8,
    /// Number of separator keys stored in this node.
    /// A node with `n` keys has `n + 1` children (the extra child is
    /// `rightmost_child`).
    pub key_count: u16,
    /// CRC32C checksum. Covers bytes 0–3 and 8 onward (excludes the 4-byte
    /// checksum field at offset 4–7).
    pub checksum: u32,
    /// Page number of the right-most child — the child to the right of all
    /// separator keys.
    pub rightmost_child: u32,
}

impl InternalPageHeader {
    /// Parse the header from the first [`INTERNAL_HEADER_SIZE`] bytes of a page.
    pub(crate) fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < INTERNAL_HEADER_SIZE {
            return Err(Error::Internal(format!(
                "internal page buffer is {} bytes, need at least {INTERNAL_HEADER_SIZE}",
                buf.len()
            )));
        }
        Ok(Self {
            page_type: buf[0],
            level: buf[1],
            key_count: u16::from_le_bytes([buf[2], buf[3]]),
            checksum: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            rightmost_child: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
        })
    }

    /// Serialize the header into the first [`INTERNAL_HEADER_SIZE`] bytes of `buf`.
    pub(crate) fn write_to(&self, buf: &mut [u8]) {
        buf[0] = self.page_type;
        buf[1] = self.level;
        buf[2..4].copy_from_slice(&self.key_count.to_le_bytes());
        buf[4..8].copy_from_slice(&self.checksum.to_le_bytes());
        buf[8..12].copy_from_slice(&self.rightmost_child.to_le_bytes());
    }

    /// Return an error if the page type byte is not [`PAGE_TYPE_INTERNAL`].
    pub(crate) fn validate_type(&self) -> Result<()> {
        if self.page_type != PAGE_TYPE_INTERNAL {
            return Err(Error::Internal(format!(
                "expected internal page type 0x01, found 0x{:02X}",
                self.page_type
            )));
        }
        Ok(())
    }
}

/// Compute the CRC32C checksum for an internal page.
///
/// Covers bytes 0–3 and bytes 8 onward (skips the 4-byte checksum field at
/// offset 4–7).
pub(crate) fn internal_page_checksum(page: &[u8; PAGE_SIZE_INTERNAL as usize]) -> u32 {
    let digest = crc32c::crc32c(&page[..4]);
    crc32c::crc32c_append(digest, &page[8..])
}

/// Verify the CRC32C checksum stored in an internal page.
///
/// Returns `Err` if the stored checksum at offset 4–7 does not match the
/// checksum computed from the page contents.
pub(crate) fn verify_internal_page_checksum(
    page: &[u8; PAGE_SIZE_INTERNAL as usize],
) -> Result<()> {
    let header = InternalPageHeader::from_bytes(page)?;
    let computed = internal_page_checksum(page);
    if header.checksum != computed {
        return Err(Error::Internal(format!(
            "internal page checksum mismatch: stored 0x{:08X}, computed 0x{:08X}",
            header.checksum, computed
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Leaf page header  (20 bytes)
// ---------------------------------------------------------------------------

/// Structured header of a 32 KB leaf B+ tree node.
///
/// ## On-disk layout (20 bytes at start of page)
///
/// ```text
/// Offset  Size  Field
///  0       1    page_type: u8 (must be 0x02)
///  1       1    flags: u8 (bit 0 = LEAF_FLAG_HAS_OVERFLOW)
///  2       2    entry_count: u16 LE
///  4       4    checksum: u32 LE (CRC32C over bytes 0–3 and 8 onward)
///  8       4    next_leaf_page: u32 LE (right sibling page number, 0 = none)
/// 12       4    prev_leaf_page: u32 LE (left sibling page number, 0 = none)
/// 16       2    free_space_offset: u16 LE (byte offset where free space begins)
/// 18       2    cell_ptr_offset: u16 LE (byte offset of cell pointer array)
/// 20        …   cell pointer array: [u16 LE offset] × entry_count
/// ```
///
/// Cells grow from the end of the page toward the cell pointer array.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LeafPageHeader {
    /// Must be [`PAGE_TYPE_LEAF`] (0x02).
    pub page_type: u8,
    /// Page flags. Bit 0: [`LEAF_FLAG_HAS_OVERFLOW`] — at least one entry uses
    /// an overflow pointer.
    pub flags: u8,
    /// Number of cells (key–value entries) in this leaf.
    pub entry_count: u16,
    /// CRC32C checksum. Covers bytes 0–3 and 8 onward.
    pub checksum: u32,
    /// Page number of the right sibling leaf (doubly-linked list for range
    /// scans), or 0 if this is the rightmost leaf.
    pub next_leaf_page: u32,
    /// Page number of the left sibling leaf, or 0 if leftmost.
    pub prev_leaf_page: u32,
    /// Byte offset (from page start) where the free region begins.
    pub free_space_offset: u16,
    /// Byte offset (from page start) of the cell pointer array.
    pub cell_ptr_offset: u16,
}

impl LeafPageHeader {
    /// Parse the header from the first [`LEAF_HEADER_SIZE`] bytes of a page.
    pub(crate) fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < LEAF_HEADER_SIZE {
            return Err(Error::Internal(format!(
                "leaf page buffer is {} bytes, need at least {LEAF_HEADER_SIZE}",
                buf.len()
            )));
        }
        Ok(Self {
            page_type: buf[0],
            flags: buf[1],
            entry_count: u16::from_le_bytes([buf[2], buf[3]]),
            checksum: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            next_leaf_page: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
            prev_leaf_page: u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
            free_space_offset: u16::from_le_bytes([buf[16], buf[17]]),
            cell_ptr_offset: u16::from_le_bytes([buf[18], buf[19]]),
        })
    }

    /// Serialize the header into the first [`LEAF_HEADER_SIZE`] bytes of `buf`.
    pub(crate) fn write_to(&self, buf: &mut [u8]) {
        buf[0] = self.page_type;
        buf[1] = self.flags;
        buf[2..4].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[4..8].copy_from_slice(&self.checksum.to_le_bytes());
        buf[8..12].copy_from_slice(&self.next_leaf_page.to_le_bytes());
        buf[12..16].copy_from_slice(&self.prev_leaf_page.to_le_bytes());
        buf[16..18].copy_from_slice(&self.free_space_offset.to_le_bytes());
        buf[18..20].copy_from_slice(&self.cell_ptr_offset.to_le_bytes());
    }

    /// Return an error if the page type byte is not [`PAGE_TYPE_LEAF`].
    pub(crate) fn validate_type(&self) -> Result<()> {
        if self.page_type != PAGE_TYPE_LEAF {
            return Err(Error::Internal(format!(
                "expected leaf page type 0x02, found 0x{:02X}",
                self.page_type
            )));
        }
        Ok(())
    }

    /// Returns `true` if the [`LEAF_FLAG_HAS_OVERFLOW`] flag is set.
    pub(crate) fn has_overflow(&self) -> bool {
        self.flags & LEAF_FLAG_HAS_OVERFLOW != 0
    }
}

/// Compute the CRC32C checksum for a leaf page.
///
/// Covers bytes 0–3 and bytes 8 onward (skips the checksum field at offset
/// 4–7).
pub(crate) fn leaf_page_checksum(page: &[u8; PAGE_SIZE_LEAF as usize]) -> u32 {
    let digest = crc32c::crc32c(&page[..4]);
    crc32c::crc32c_append(digest, &page[8..])
}

/// Verify the CRC32C checksum stored in a leaf page.
pub(crate) fn verify_leaf_page_checksum(page: &[u8; PAGE_SIZE_LEAF as usize]) -> Result<()> {
    let header = LeafPageHeader::from_bytes(page)?;
    let computed = leaf_page_checksum(page);
    if header.checksum != computed {
        return Err(Error::Internal(format!(
            "leaf page checksum mismatch: stored 0x{:08X}, computed 0x{:08X}",
            header.checksum, computed
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Overflow page header  (16 bytes)
// ---------------------------------------------------------------------------

/// Structured header of a 32 KB overflow page.
///
/// Overflow pages are used when a document's BSON serialization exceeds the
/// usable space in a single leaf cell. The leaf cell stores a pointer to the
/// first overflow page; subsequent fragments are linked via `next_overflow_page`.
///
/// ## On-disk layout (16 bytes at start of page)
///
/// ```text
/// Offset  Size  Field
///  0       1    page_type: u8 (must be 0x05)
///  1       3    reserved: [u8; 3] (zero-filled)
///  4       4    checksum: u32 LE (CRC32C over bytes 0–3 and 8 onward)
///  8       4    next_overflow_page: u32 LE (0 = last page in chain)
/// 12       4    data_length: u32 LE (bytes of payload in this page)
/// 16        …   payload (continuation of BSON document)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OverflowPageHeader {
    /// Must be [`PAGE_TYPE_OVERFLOW`] (0x05).
    pub page_type: u8,
    /// CRC32C checksum. Covers bytes 0–3 and 8 onward.
    pub checksum: u32,
    /// Page number of the next overflow page in the chain, or 0 if this is the
    /// last page.
    pub next_overflow_page: u32,
    /// Number of valid payload bytes in this page (≤ `PAGE_SIZE_LEAF - OVERFLOW_HEADER_SIZE`).
    pub data_length: u32,
}

impl OverflowPageHeader {
    /// Parse the header from the first [`OVERFLOW_HEADER_SIZE`] bytes of a page.
    pub(crate) fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < OVERFLOW_HEADER_SIZE {
            return Err(Error::Internal(format!(
                "overflow page buffer is {} bytes, need at least {OVERFLOW_HEADER_SIZE}",
                buf.len()
            )));
        }
        Ok(Self {
            page_type: buf[0],
            // buf[1..4] are reserved; ignored on read
            checksum: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            next_overflow_page: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
            data_length: u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
        })
    }

    /// Serialize the header into the first [`OVERFLOW_HEADER_SIZE`] bytes of `buf`.
    pub(crate) fn write_to(&self, buf: &mut [u8]) {
        buf[0] = self.page_type;
        buf[1] = 0; // reserved
        buf[2] = 0;
        buf[3] = 0;
        buf[4..8].copy_from_slice(&self.checksum.to_le_bytes());
        buf[8..12].copy_from_slice(&self.next_overflow_page.to_le_bytes());
        buf[12..16].copy_from_slice(&self.data_length.to_le_bytes());
    }

    /// Return an error if the page type byte is not [`PAGE_TYPE_OVERFLOW`].
    pub(crate) fn validate_type(&self) -> Result<()> {
        if self.page_type != PAGE_TYPE_OVERFLOW {
            return Err(Error::Internal(format!(
                "expected overflow page type 0x05, found 0x{:02X}",
                self.page_type
            )));
        }
        Ok(())
    }
}

/// Compute the CRC32C checksum for an overflow page.
///
/// Covers bytes 0–3 and bytes 8 onward (skips the checksum field at offset
/// 4–7).
pub(crate) fn overflow_page_checksum(page: &[u8; PAGE_SIZE_LEAF as usize]) -> u32 {
    let digest = crc32c::crc32c(&page[..4]);
    crc32c::crc32c_append(digest, &page[8..])
}

/// Verify the CRC32C checksum stored in an overflow page.
pub(crate) fn verify_overflow_page_checksum(page: &[u8; PAGE_SIZE_LEAF as usize]) -> Result<()> {
    let header = OverflowPageHeader::from_bytes(page)?;
    let computed = overflow_page_checksum(page);
    if header.checksum != computed {
        return Err(Error::Internal(format!(
            "overflow page checksum mismatch: stored 0x{:08X}, computed 0x{:08X}",
            header.checksum, computed
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Internal page tests
    // -----------------------------------------------------------------------

    fn make_internal_page() -> [u8; PAGE_SIZE_INTERNAL as usize] {
        let mut buf = [0u8; PAGE_SIZE_INTERNAL as usize];
        let mut hdr = InternalPageHeader {
            page_type: PAGE_TYPE_INTERNAL,
            level: 2,
            key_count: 3,
            checksum: 0,
            rightmost_child: 42,
        };
        hdr.write_to(&mut buf);
        let cs = internal_page_checksum(&buf);
        buf[4..8].copy_from_slice(&cs.to_le_bytes());
        // Re-parse and store the valid checksum in the header struct too
        hdr.checksum = cs;
        hdr.write_to(&mut buf);
        buf
    }

    #[test]
    fn internal_page_roundtrip() {
        let page = make_internal_page();
        let hdr = InternalPageHeader::from_bytes(&page).unwrap();
        assert_eq!(hdr.page_type, PAGE_TYPE_INTERNAL);
        assert_eq!(hdr.level, 2);
        assert_eq!(hdr.key_count, 3);
        assert_eq!(hdr.rightmost_child, 42);
    }

    #[test]
    fn internal_page_type_validation() {
        let page = make_internal_page();
        let hdr = InternalPageHeader::from_bytes(&page).unwrap();
        hdr.validate_type().expect("type should be valid");
    }

    #[test]
    fn internal_page_bad_type_rejected() {
        let page = make_internal_page();
        let mut bad_hdr = InternalPageHeader::from_bytes(&page).unwrap();
        bad_hdr.page_type = PAGE_TYPE_LEAF; // wrong type
        assert!(bad_hdr.validate_type().is_err());
    }

    #[test]
    fn internal_page_checksum_valid() {
        let page = make_internal_page();
        verify_internal_page_checksum(&page).expect("checksum should be valid");
    }

    #[test]
    fn internal_page_checksum_detects_corruption() {
        let mut page = make_internal_page();
        page[100] ^= 0xFF; // flip bits in the key data area
        assert!(
            verify_internal_page_checksum(&page).is_err(),
            "should detect corruption"
        );
    }

    #[test]
    fn internal_page_checksum_excludes_checksum_field() {
        // Corruption exactly at the checksum field (offset 4–7) should be
        // detected because the stored value no longer matches recomputed value.
        let mut page = make_internal_page();
        page[4] ^= 0xFF;
        assert!(verify_internal_page_checksum(&page).is_err());
    }

    // -----------------------------------------------------------------------
    // Leaf page tests
    // -----------------------------------------------------------------------

    fn make_leaf_page() -> [u8; PAGE_SIZE_LEAF as usize] {
        let mut buf = [0u8; PAGE_SIZE_LEAF as usize];
        let mut hdr = LeafPageHeader {
            page_type: PAGE_TYPE_LEAF,
            flags: LEAF_FLAG_HAS_OVERFLOW,
            entry_count: 7,
            checksum: 0,
            next_leaf_page: 100,
            prev_leaf_page: 50,
            free_space_offset: LEAF_HEADER_SIZE as u16,
            cell_ptr_offset: LEAF_HEADER_SIZE as u16,
        };
        hdr.write_to(&mut buf);
        let cs = leaf_page_checksum(&buf);
        buf[4..8].copy_from_slice(&cs.to_le_bytes());
        hdr.checksum = cs;
        hdr.write_to(&mut buf);
        buf
    }

    #[test]
    fn leaf_page_roundtrip() {
        let page = make_leaf_page();
        let hdr = LeafPageHeader::from_bytes(&page).unwrap();
        assert_eq!(hdr.page_type, PAGE_TYPE_LEAF);
        assert_eq!(hdr.flags, LEAF_FLAG_HAS_OVERFLOW);
        assert_eq!(hdr.entry_count, 7);
        assert_eq!(hdr.next_leaf_page, 100);
        assert_eq!(hdr.prev_leaf_page, 50);
        assert_eq!(hdr.free_space_offset, LEAF_HEADER_SIZE as u16);
        assert_eq!(hdr.cell_ptr_offset, LEAF_HEADER_SIZE as u16);
    }

    #[test]
    fn leaf_page_has_overflow_flag() {
        let page = make_leaf_page();
        let hdr = LeafPageHeader::from_bytes(&page).unwrap();
        assert!(hdr.has_overflow());
    }

    #[test]
    fn leaf_page_no_overflow_flag() {
        let mut buf = [0u8; PAGE_SIZE_LEAF as usize];
        let mut hdr = LeafPageHeader {
            page_type: PAGE_TYPE_LEAF,
            flags: 0,
            entry_count: 0,
            checksum: 0,
            next_leaf_page: 0,
            prev_leaf_page: 0,
            free_space_offset: LEAF_HEADER_SIZE as u16,
            cell_ptr_offset: LEAF_HEADER_SIZE as u16,
        };
        hdr.write_to(&mut buf);
        let cs = leaf_page_checksum(&buf);
        buf[4..8].copy_from_slice(&cs.to_le_bytes());
        hdr.checksum = cs;
        hdr.write_to(&mut buf);

        let parsed = LeafPageHeader::from_bytes(&buf).unwrap();
        assert!(!parsed.has_overflow());
    }

    #[test]
    fn leaf_page_type_validation() {
        let page = make_leaf_page();
        let hdr = LeafPageHeader::from_bytes(&page).unwrap();
        hdr.validate_type().expect("type should be valid");
    }

    #[test]
    fn leaf_page_checksum_valid() {
        let page = make_leaf_page();
        verify_leaf_page_checksum(&page).expect("checksum should be valid");
    }

    #[test]
    fn leaf_page_checksum_detects_corruption() {
        let mut page = make_leaf_page();
        page[200] ^= 0xAB;
        assert!(verify_leaf_page_checksum(&page).is_err());
    }

    // -----------------------------------------------------------------------
    // Overflow page tests
    // -----------------------------------------------------------------------

    fn make_overflow_page() -> [u8; PAGE_SIZE_LEAF as usize] {
        let mut buf = [0u8; PAGE_SIZE_LEAF as usize];
        let mut hdr = OverflowPageHeader {
            page_type: PAGE_TYPE_OVERFLOW,
            checksum: 0,
            next_overflow_page: 77,
            data_length: 64,
        };
        hdr.write_to(&mut buf);
        // Write some payload bytes
        for i in 0..64usize {
            buf[OVERFLOW_HEADER_SIZE + i] = i as u8;
        }
        let cs = overflow_page_checksum(&buf);
        buf[4..8].copy_from_slice(&cs.to_le_bytes());
        hdr.checksum = cs;
        hdr.write_to(&mut buf);
        buf
    }

    #[test]
    fn overflow_page_roundtrip() {
        let page = make_overflow_page();
        let hdr = OverflowPageHeader::from_bytes(&page).unwrap();
        assert_eq!(hdr.page_type, PAGE_TYPE_OVERFLOW);
        assert_eq!(hdr.next_overflow_page, 77);
        assert_eq!(hdr.data_length, 64);
        // Reserved bytes read as zero
        assert_eq!(page[1], 0);
        assert_eq!(page[2], 0);
        assert_eq!(page[3], 0);
    }

    #[test]
    fn overflow_page_type_validation() {
        let page = make_overflow_page();
        let hdr = OverflowPageHeader::from_bytes(&page).unwrap();
        hdr.validate_type().expect("type should be valid");
    }

    #[test]
    fn overflow_page_checksum_valid() {
        let page = make_overflow_page();
        verify_overflow_page_checksum(&page).expect("checksum should be valid");
    }

    #[test]
    fn overflow_page_checksum_detects_corruption() {
        let mut page = make_overflow_page();
        page[OVERFLOW_HEADER_SIZE + 10] ^= 0x55; // corrupt payload
        assert!(verify_overflow_page_checksum(&page).is_err());
    }

    // -----------------------------------------------------------------------
    // Constants sanity checks
    // -----------------------------------------------------------------------

    #[test]
    fn page_type_constants_are_correct() {
        assert_eq!(PAGE_TYPE_INTERNAL, 0x01);
        assert_eq!(PAGE_TYPE_LEAF, 0x02);
        assert_eq!(PAGE_TYPE_OVERFLOW, 0x05);
    }

    #[test]
    fn page_size_constants_are_correct() {
        assert_eq!(PAGE_SIZE_INTERNAL, 4096);
        assert_eq!(PAGE_SIZE_LEAF, 32768);
    }

    #[test]
    fn value_type_constants_are_correct() {
        assert_eq!(VALUE_TYPE_INLINE, 0x01);
        assert_eq!(VALUE_TYPE_OVERFLOW, 0x02);
    }
}
