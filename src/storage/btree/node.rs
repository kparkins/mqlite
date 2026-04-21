use crate::error::{Error, Result};
use crate::storage::page::{
    internal_page_checksum, leaf_page_checksum, InternalPageHeader, LeafPageHeader,
    INTERNAL_HEADER_SIZE, LEAF_FLAG_HAS_OVERFLOW, LEAF_HEADER_SIZE, PAGE_SIZE_INTERNAL,
    PAGE_SIZE_LEAF, PAGE_TYPE_INTERNAL, PAGE_TYPE_LEAF, VALUE_TYPE_INLINE, VALUE_TYPE_OVERFLOW,
};

// ---------------------------------------------------------------------------
// Internal node (parsed representation)
// ---------------------------------------------------------------------------

/// Parsed representation of a 4 KB internal B+ tree node.
///
/// An internal node with `entries.len()` separator keys has `entries.len() + 1`
/// child pointers: `entries[i].1` is the child to the **left** of `entries[i].0`,
/// and `rightmost_child` is the child to the right of all keys.
///
/// Navigation: to descend to the correct child for search key `K`, find the
/// smallest index `i` where `K < entries[i].0`.  If no such `i` exists, follow
/// `rightmost_child`.
#[derive(Debug, Clone)]
pub(super) struct InternalNode {
    pub(super) level: u8,
    /// `(separator_key, left_child_page)` pairs, sorted ascending by key.
    pub(super) entries: Vec<(Vec<u8>, u32)>,
    pub(super) rightmost_child: u32,
}

impl InternalNode {
    /// Parse an internal page buffer into a structured [`InternalNode`].
    pub(super) fn parse(data: &[u8]) -> Result<Self> {
        let hdr = InternalPageHeader::from_bytes(data)?;
        hdr.validate_type()?;

        let mut entries = Vec::with_capacity(hdr.key_count as usize);
        let mut pos = INTERNAL_HEADER_SIZE;

        for _ in 0..hdr.key_count {
            if pos + 2 > data.len() {
                return Err(Error::Internal(
                    "internal page truncated reading key_len".into(),
                ));
            }
            let key_len = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;

            if pos + key_len + 4 > data.len() {
                return Err(Error::Internal(
                    "internal page truncated reading key/child".into(),
                ));
            }
            let key = data[pos..pos + key_len].to_vec();
            pos += key_len;

            let child_page =
                u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
            pos += 4;

            entries.push((key, child_page));
        }

        Ok(InternalNode {
            level: hdr.level,
            entries,
            rightmost_child: hdr.rightmost_child,
        })
    }

    /// Serialize this node into a 4 KB internal page buffer.
    ///
    /// Returns `Err` if the encoded size exceeds `PAGE_SIZE_INTERNAL`.
    pub(super) fn encode(&self) -> Result<[u8; PAGE_SIZE_INTERNAL as usize]> {
        let mut buf = [0u8; PAGE_SIZE_INTERNAL as usize];

        // Write entries first to calculate size.
        let mut pos = INTERNAL_HEADER_SIZE;
        for (key, child_page) in &self.entries {
            let key_len = key.len();
            let entry_size = 2 + key_len + 4;
            if pos + entry_size > PAGE_SIZE_INTERNAL as usize {
                return Err(Error::Internal(format!(
                    "internal node too large: {} entries do not fit in {} bytes",
                    self.entries.len(),
                    PAGE_SIZE_INTERNAL
                )));
            }
            buf[pos..pos + 2].copy_from_slice(&(key_len as u16).to_le_bytes());
            pos += 2;
            buf[pos..pos + key_len].copy_from_slice(key);
            pos += key_len;
            buf[pos..pos + 4].copy_from_slice(&child_page.to_le_bytes());
            pos += 4;
        }

        // Write header.
        let hdr = InternalPageHeader {
            page_type: PAGE_TYPE_INTERNAL,
            level: self.level,
            key_count: self.entries.len() as u16,
            checksum: 0,
            rightmost_child: self.rightmost_child,
        };
        hdr.write_to(&mut buf);

        // Compute and store checksum.
        let cs = internal_page_checksum(&buf);
        buf[4..8].copy_from_slice(&cs.to_le_bytes());

        Ok(buf)
    }

    /// Returns the number of bytes this node would occupy on disk (header + entries).
    pub(super) fn encoded_size(&self) -> usize {
        INTERNAL_HEADER_SIZE
            + self
                .entries
                .iter()
                .map(|(k, _)| 2 + k.len() + 4)
                .sum::<usize>()
    }

    /// Returns `true` if another entry of `extra_key_len` bytes would still fit.
    pub(super) fn can_insert(&self, extra_key_len: usize) -> bool {
        // Each entry = 2 (key_len field) + key_len + 4 (child_page)
        let new_entry_size = 2 + extra_key_len + 4;
        self.encoded_size() + new_entry_size <= PAGE_SIZE_INTERNAL as usize
    }

    /// Find the child page to descend to for search key `key`.
    pub(super) fn find_child(&self, key: &[u8]) -> u32 {
        for (k, child_page) in &self.entries {
            if key < k.as_slice() {
                return *child_page;
            }
        }
        self.rightmost_child
    }

    /// Find the index of the child pointer for `key` (same semantics as `find_child`
    /// but returns the index for parent-update purposes).
    ///
    /// Returns an index `i` in `0..=entries.len()`:
    /// - `i < entries.len()`: child is `entries[i].1`
    /// - `i == entries.len()`: child is `rightmost_child`
    pub(super) fn find_child_idx(&self, key: &[u8]) -> usize {
        for (i, (k, _)) in self.entries.iter().enumerate() {
            if key < k.as_slice() {
                return i;
            }
        }
        self.entries.len()
    }

    /// Child page at index `idx`.
    pub(super) fn child_at(&self, idx: usize) -> u32 {
        if idx < self.entries.len() {
            self.entries[idx].1
        } else {
            self.rightmost_child
        }
    }
}

// ---------------------------------------------------------------------------
// Leaf node (parsed representation)
// ---------------------------------------------------------------------------

/// A cell value stored in a leaf page.
#[derive(Debug, Clone)]
pub(crate) enum CellValue {
    /// The value is stored inline in the leaf page as a raw BSON document.
    Inline(Vec<u8>),
    /// The value exceeds the inline threshold; this is a pointer to an overflow
    /// chain.
    Overflow {
        /// Page number of the first overflow page in the chain.
        first_page: u32,
        /// Total byte length of the BSON document across all overflow pages.
        total_length: u32,
    },
}

/// A key–value cell in a leaf page.
#[derive(Debug, Clone)]
pub(super) struct LeafCell {
    /// B+ tree key (encoded with [`crate::keys`]).
    pub(super) key: Vec<u8>,
    /// The associated value.
    pub(super) value: CellValue,
}

impl LeafCell {
    /// Returns the encoded byte size of this cell on disk.
    ///
    /// Layout: `key_len(2) | key | value_type(1) | value_data`
    pub(super) fn encoded_size(&self) -> usize {
        let value_size = match &self.value {
            CellValue::Inline(v) => 4 + v.len(), // bson_len(4) + bson_data
            CellValue::Overflow { .. } => 8,     // first_page(4) + total_length(4)
        };
        2 + self.key.len() + 1 + value_size
    }
}

/// Parsed representation of a 32 KB leaf B+ tree node.
///
/// Cells are kept sorted by key.
#[derive(Debug, Clone)]
pub(super) struct LeafNode {
    /// Leaf flags (see [`LEAF_FLAG_HAS_OVERFLOW`]).
    pub(super) flags: u8,
    /// Right sibling page number (0 = rightmost leaf).
    pub(super) next_leaf_page: u32,
    /// Left sibling page number (0 = leftmost leaf).
    pub(super) prev_leaf_page: u32,
    /// Cells sorted ascending by key.
    pub(super) cells: Vec<LeafCell>,
}

impl LeafNode {
    /// Total bytes used by `cells` when encoded into a leaf page.
    pub(super) fn used_bytes_for_cells(cells: &[LeafCell]) -> usize {
        LEAF_HEADER_SIZE
            + cells.len() * 2 // one u16 pointer per cell
            + cells.iter().map(|c| c.encoded_size()).sum::<usize>()
    }

    /// Parse a 32 KB leaf page buffer into a [`LeafNode`].
    pub(super) fn parse(data: &[u8]) -> Result<Self> {
        let hdr = LeafPageHeader::from_bytes(data)?;
        hdr.validate_type()?;

        let n = hdr.entry_count as usize;
        let cell_ptr_base = LEAF_HEADER_SIZE; // always 20

        if cell_ptr_base + n * 2 > data.len() {
            return Err(Error::Internal(
                "leaf page: cell pointer array out of bounds".into(),
            ));
        }

        let mut cells = Vec::with_capacity(n);
        for i in 0..n {
            let ptr_offset = cell_ptr_base + i * 2;
            let cell_offset = u16::from_le_bytes([data[ptr_offset], data[ptr_offset + 1]]) as usize;

            let cell = Self::parse_cell(data, cell_offset)?;
            cells.push(cell);
        }

        // Cells should already be sorted by key (cell pointers are in sorted order).
        // We trust the on-disk ordering but could verify in debug builds.

        Ok(LeafNode {
            flags: hdr.flags,
            next_leaf_page: hdr.next_leaf_page,
            prev_leaf_page: hdr.prev_leaf_page,
            cells,
        })
    }

    /// Parse a single cell starting at `offset` in the page buffer.
    pub(super) fn parse_cell(data: &[u8], offset: usize) -> Result<LeafCell> {
        if offset + 2 > data.len() {
            return Err(Error::Internal(format!(
                "leaf cell at offset {offset} is out of bounds (page len {})",
                data.len()
            )));
        }
        let key_len = u16::from_le_bytes([data[offset], data[offset + 1]]) as usize;
        let pos = offset + 2;

        if pos + key_len + 1 > data.len() {
            return Err(Error::Internal(
                "leaf cell: key or value_type out of bounds".into(),
            ));
        }
        let key = data[pos..pos + key_len].to_vec();
        let value_type = data[pos + key_len];

        let value_pos = pos + key_len + 1;
        let value = match value_type {
            VALUE_TYPE_INLINE => {
                if value_pos + 4 > data.len() {
                    return Err(Error::Internal(
                        "leaf cell: inline bson_len out of bounds".into(),
                    ));
                }
                let bson_len = u32::from_le_bytes([
                    data[value_pos],
                    data[value_pos + 1],
                    data[value_pos + 2],
                    data[value_pos + 3],
                ]) as usize;
                let data_start = value_pos + 4;
                if data_start + bson_len > data.len() {
                    return Err(Error::Internal(
                        "leaf cell: inline bson data out of bounds".into(),
                    ));
                }
                CellValue::Inline(data[data_start..data_start + bson_len].to_vec())
            }
            VALUE_TYPE_OVERFLOW => {
                if value_pos + 8 > data.len() {
                    return Err(Error::Internal(
                        "leaf cell: overflow pointer out of bounds".into(),
                    ));
                }
                let first_page = u32::from_le_bytes([
                    data[value_pos],
                    data[value_pos + 1],
                    data[value_pos + 2],
                    data[value_pos + 3],
                ]);
                let total_length = u32::from_le_bytes([
                    data[value_pos + 4],
                    data[value_pos + 5],
                    data[value_pos + 6],
                    data[value_pos + 7],
                ]);
                CellValue::Overflow {
                    first_page,
                    total_length,
                }
            }
            other => {
                return Err(Error::Internal(format!(
                    "unknown cell value type 0x{other:02X}"
                )));
            }
        };

        Ok(LeafCell { key, value })
    }

    /// Serialize this leaf node into a 32 KB page buffer.
    ///
    /// Returns `Err` if the total cell data exceeds the page capacity.
    pub(super) fn encode(&self) -> Result<[u8; PAGE_SIZE_LEAF as usize]> {
        let n = self.cells.len();
        let used = self.used_bytes();
        if used > PAGE_SIZE_LEAF as usize {
            return Err(Error::Internal(format!(
                "leaf node too large: {used} bytes exceed page size {}",
                PAGE_SIZE_LEAF
            )));
        }

        let mut buf = [0u8; PAGE_SIZE_LEAF as usize];

        // Write cells from end of page backward.
        let mut write_pos = PAGE_SIZE_LEAF as usize;
        let cell_ptr_base = LEAF_HEADER_SIZE;

        let has_overflow = self
            .cells
            .iter()
            .any(|c| matches!(c.value, CellValue::Overflow { .. }));

        for (i, cell) in self.cells.iter().enumerate() {
            let cell_size = cell.encoded_size();
            write_pos -= cell_size;

            // Record cell pointer (offset of this cell from page start).
            let ptr_offset = cell_ptr_base + i * 2;
            buf[ptr_offset..ptr_offset + 2].copy_from_slice(&(write_pos as u16).to_le_bytes());

            // Encode cell data.
            let key_len = cell.key.len();
            buf[write_pos..write_pos + 2].copy_from_slice(&(key_len as u16).to_le_bytes());
            buf[write_pos + 2..write_pos + 2 + key_len].copy_from_slice(&cell.key);

            let vp = write_pos + 2 + key_len;
            match &cell.value {
                CellValue::Inline(bson) => {
                    buf[vp] = VALUE_TYPE_INLINE;
                    let bson_len = bson.len() as u32;
                    buf[vp + 1..vp + 5].copy_from_slice(&bson_len.to_le_bytes());
                    buf[vp + 5..vp + 5 + bson.len()].copy_from_slice(bson);
                }
                CellValue::Overflow {
                    first_page,
                    total_length,
                } => {
                    buf[vp] = VALUE_TYPE_OVERFLOW;
                    buf[vp + 1..vp + 5].copy_from_slice(&first_page.to_le_bytes());
                    buf[vp + 5..vp + 9].copy_from_slice(&total_length.to_le_bytes());
                }
            }
        }

        // free_space_offset = end of cell pointer array.
        let free_space_offset = (cell_ptr_base + n * 2) as u16;

        // Write header.
        let flags = if has_overflow {
            self.flags | LEAF_FLAG_HAS_OVERFLOW
        } else {
            self.flags & !LEAF_FLAG_HAS_OVERFLOW
        };
        let hdr = LeafPageHeader {
            page_type: PAGE_TYPE_LEAF,
            flags,
            entry_count: n as u16,
            checksum: 0,
            next_leaf_page: self.next_leaf_page,
            prev_leaf_page: self.prev_leaf_page,
            free_space_offset,
            cell_ptr_offset: LEAF_HEADER_SIZE as u16,
        };
        hdr.write_to(&mut buf);

        // Compute and store checksum.
        let cs = leaf_page_checksum(&buf);
        buf[4..8].copy_from_slice(&cs.to_le_bytes());

        Ok(buf)
    }

    /// Total bytes used in the page (header + pointer array + cell data).
    pub(super) fn used_bytes(&self) -> usize {
        Self::used_bytes_for_cells(&self.cells)
    }

    /// Available free bytes in the page.
    pub(super) fn free_bytes(&self) -> usize {
        PAGE_SIZE_LEAF as usize - self.used_bytes()
    }

    /// Returns `true` if a new cell of `cell_size` bytes would fit.
    pub(super) fn can_insert(&self, cell_size: usize) -> bool {
        // Need room for the cell data AND a 2-byte cell pointer.
        self.free_bytes() >= cell_size + 2
    }

    /// Returns `true` when a non-root leaf should be rebalanced.
    pub(super) fn needs_rebalance(&self) -> bool {
        self.cells.len() < super::MIN_LEAF_CELLS || self.used_bytes() < super::MIN_LEAF_BYTES
    }

    /// Binary-search for `key`.  Returns `Ok(idx)` if found, `Err(idx)` for
    /// the insertion position.
    pub(super) fn binary_search(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        self.cells.binary_search_by(|c| c.key.as_slice().cmp(key))
    }
}

// ---------------------------------------------------------------------------
// Split result type
// ---------------------------------------------------------------------------

/// Returned when an insert causes a node to split.
pub(super) struct SplitResult {
    /// The separator key to be promoted to the parent.
    pub(super) promoted_key: Vec<u8>,
    /// Page number of the newly allocated right sibling.
    pub(super) right_page: u32,
}
