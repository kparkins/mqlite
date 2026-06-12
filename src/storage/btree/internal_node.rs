//! Parsed representation of a 4 KB internal B+ tree node.

use crate::error::{Error, Result};
use crate::storage::page::{
    internal_page_checksum, InternalPageHeader, INTERNAL_HEADER_SIZE, PAGE_SIZE_INTERNAL,
    PAGE_TYPE_INTERNAL,
};

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
