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
//! Internal and leaf pages store a CRC32C checksum at **offset 4–7**. The
//! checksum covers bytes **0–3** and bytes **8 onward** (the checksum field
//! itself is excluded). Overflow pages store the checksum at **offset 8–11**;
//! their checksum also excludes the atomic refcount field at **offset 4–7**.
//! Callers must recompute and store the checksum before writing a page to disk,
//! and must verify it after reading.

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
///
/// Grew from 16 → 20 in T3 (MVCC Format Lock §A.1) to add a
/// `refcount: AtomicU32` field at offset 4. Checksum coverage explicitly
/// excludes bytes 4..8 (refcount) and bytes 8..12 (checksum field itself)
/// so atomic refcount ops do not invalidate the page's integrity guarantee.
pub(crate) const OVERFLOW_HEADER_SIZE: usize = 20;

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
#[allow(dead_code)]
pub(crate) fn verify_internal_page_checksum(
    page: &[u8; PAGE_SIZE_INTERNAL as usize],
) -> Result<()> {
    let header = InternalPageHeader::from_bytes(page)?;
    let computed = internal_page_checksum(page);
    if header.checksum != computed {
        #[cfg(feature = "tracing")]
        tracing::error!(
            target: "mqlite",
            stored = header.checksum,
            computed,
            "mqlite::corrupt_page"
        );
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
    #[allow(dead_code)]
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
#[allow(dead_code)]
pub(crate) fn verify_leaf_page_checksum(page: &[u8; PAGE_SIZE_LEAF as usize]) -> Result<()> {
    let header = LeafPageHeader::from_bytes(page)?;
    let computed = leaf_page_checksum(page);
    if header.checksum != computed {
        #[cfg(feature = "tracing")]
        tracing::error!(
            target: "mqlite",
            stored = header.checksum,
            computed,
            "mqlite::corrupt_page"
        );
        return Err(Error::Internal(format!(
            "leaf page checksum mismatch: stored 0x{:08X}, computed 0x{:08X}",
            header.checksum, computed
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Overflow page header  (20 bytes, post-T3)
// ---------------------------------------------------------------------------

/// Structured header of a 32 KB overflow page.
///
/// Overflow pages are used when a document's BSON serialization exceeds the
/// usable space in a single leaf cell. The leaf cell stores a pointer to the
/// first overflow page; subsequent fragments are linked via `next_overflow_page`.
///
/// ## On-disk layout (20 bytes at start of page — post-T3, MVCC Format Lock §A.1)
///
/// ```text
/// Offset  Size  Field
///  0       1    page_type: u8 (must be 0x05)
///  1       3    reserved: [u8; 3] (zero-filled)
///  4       4    refcount: u32 LE (atomic — see allocator::AllocatorHandle)
///  8       4    checksum: u32 LE (CRC32C; coverage excludes bytes 4..12)
/// 12       4    next_overflow_page: u32 LE (0 = last page in chain)
/// 16       4    data_length: u32 LE (bytes of payload in this page)
/// 20        …   payload (continuation of BSON document)
/// ```
///
/// **Checksum coverage** (MAJOR-3 fix): CRC32C over bytes 0..4 + 12..END.
/// EXCLUDES bytes 4..8 (refcount — mutated atomically without rewriting the
/// page) and bytes 8..12 (checksum field itself). A flip of any byte in
/// 4..8 does NOT invalidate the page's stored checksum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OverflowPageHeader {
    /// Must be [`PAGE_TYPE_OVERFLOW`] (0x05).
    pub page_type: u8,
    /// Reference count — number of live `OverflowRef` handles pinning this
    /// chain. Serialized as a plain `u32` in the on-disk struct; atomic
    /// operations go through `allocator::AllocatorHandle::incref_overflow`
    /// etc.
    pub refcount: u32,
    /// CRC32C checksum. Covers bytes 0..4 + 12..END; EXCLUDES bytes 4..12.
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
            refcount: u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]),
            checksum: u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]),
            next_overflow_page: u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]),
            data_length: u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]),
        })
    }

    /// Serialize the header into the first [`OVERFLOW_HEADER_SIZE`] bytes of `buf`.
    pub(crate) fn write_to(&self, buf: &mut [u8]) {
        buf[0] = self.page_type;
        buf[1..4].fill(0);
        buf[4..8].copy_from_slice(&self.refcount.to_le_bytes());
        buf[8..12].copy_from_slice(&self.checksum.to_le_bytes());
        buf[12..16].copy_from_slice(&self.next_overflow_page.to_le_bytes());
        buf[16..20].copy_from_slice(&self.data_length.to_le_bytes());
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
/// Post-T3 layout (Format Lock §A.1): covers bytes 0..4 (page_type +
/// reserved) and bytes 12..END (next_overflow_page + data_length + payload).
/// EXCLUDES bytes 4..8 (refcount — mutated atomically) and bytes 8..12
/// (checksum field itself).
pub(crate) fn overflow_page_checksum(page: &[u8; PAGE_SIZE_LEAF as usize]) -> u32 {
    let digest = crc32c::crc32c(&page[..4]);
    crc32c::crc32c_append(digest, &page[12..])
}

/// Verify the CRC32C checksum stored in an overflow page.
#[allow(dead_code)]
pub(crate) fn verify_overflow_page_checksum(page: &[u8; PAGE_SIZE_LEAF as usize]) -> Result<()> {
    let header = OverflowPageHeader::from_bytes(page)?;
    let computed = overflow_page_checksum(page);
    if header.checksum != computed {
        #[cfg(feature = "tracing")]
        tracing::error!(
            target: "mqlite",
            stored = header.checksum,
            computed,
            "mqlite::corrupt_page"
        );
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
#[path = "tests/page.rs"]
mod tests;
