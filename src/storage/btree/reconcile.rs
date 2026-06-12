//! Reconcile-only B-tree helpers for folded leaf images.
//!
//! This module deliberately avoids the CRUD insert/delete helpers. Reconcile
//! synthesizes one replacement leaf image and leaves atomic installation to
//! `BufferPool::replace_leaf_and_chains`.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::version::{VersionData, VersionEntry, VersionState};
use crate::storage::page::LEAF_HEADER_SIZE;

use super::node::{LeafCell, LeafNode};
use super::CellValue;

pub(crate) const SLOT_POINTER_BYTES: usize = 2;
pub(crate) const CELL_KEY_LEN_BYTES: usize = 2;
pub(crate) const CELL_VALUE_TYPE_BYTES: usize = 1;
pub(crate) const CELL_INLINE_LEN_BYTES: usize = 4;
pub(crate) const CELL_OVERFLOW_REF_BYTES: usize = 8;

const RETAINED_CHAIN_KEY_LEN_BYTES: usize = 2;
const RETAINED_CHAIN_ENTRY_COUNT_BYTES: usize = 4;
const TS_BYTES: usize = 12;
const TXN_ID_BYTES: usize = 8;
const VERSION_STATE_TAG_BYTES: usize = 1;
const PENDING_STATE_TXN_ID_BYTES: usize = 8;
const TOMBSTONE_FLAG_BYTES: usize = 1;
const VERSION_DATA_KIND_BYTES: usize = 1;
const RETAINED_INLINE_LEN_BYTES: usize = 4;
const RETAINED_OVERFLOW_REF_BYTES: usize = 12;

/// Delta chains retained on a folded leaf after synthesis.
pub(crate) type RetainedChains = BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>;

/// Key/value cell that can be encoded into a folded leaf image.
#[derive(Clone, Debug)]
pub(crate) struct FoldedLeafCell {
    /// B-tree key bytes.
    pub(crate) key: Vec<u8>,
    /// Folded base value for the key.
    pub(crate) value: CellValue,
}

impl FoldedLeafCell {
    /// Build an inline folded leaf cell.
    pub(crate) fn inline(key: Vec<u8>, value: Vec<u8>) -> Self {
        Self {
            key,
            value: CellValue::Inline(value),
        }
    }

    /// Build an overflow-pointer folded leaf cell.
    pub(crate) fn overflow(key: Vec<u8>, first_page: u32, total_length: u32) -> Self {
        Self {
            key,
            value: CellValue::Overflow {
                first_page,
                total_length,
            },
        }
    }

    /// Return the encoded on-page byte width of this folded cell.
    ///
    /// The value width comes from the already-classified [`CellValue`] (a
    /// folded cell records whether it overflowed when it was folded); the
    /// outer cell arithmetic is the canonical
    /// [`super::layout::leaf_cell_encoded_size`] shared with `LeafCell` and
    /// the SMO split classifier.
    pub(crate) fn encoded_size(&self) -> usize {
        let value_bytes = match &self.value {
            CellValue::Inline(bytes) => CELL_INLINE_LEN_BYTES + bytes.len(),
            CellValue::Overflow { .. } => CELL_OVERFLOW_REF_BYTES,
        };
        super::layout::leaf_cell_encoded_size(self.key.len(), value_bytes)
    }
}

/// Sibling links to preserve when encoding a replacement folded leaf.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FoldedLeafLinks {
    /// Right sibling page number, or 0 for the rightmost leaf.
    pub(crate) next_leaf_page: u32,
    /// Left sibling page number, or 0 for the leftmost leaf.
    pub(crate) prev_leaf_page: u32,
}

/// Decoded folded leaf image for reconcile synthesis.
#[derive(Clone, Debug)]
pub(crate) struct FoldedLeafImage {
    /// Cells currently present in the folded base image.
    pub(crate) cells: Vec<FoldedLeafCell>,
    /// Sibling links carried by the folded base image.
    pub(crate) links: FoldedLeafLinks,
}

/// Predict the byte footprint of a folded leaf plus retained chain payloads.
///
/// The folded leaf portion uses the exact current leaf layout:
/// header, one `u16` slot pointer per winner, and each encoded cell. Retained
/// chains are sidecar frame payloads, so they are counted separately using a
/// self-contained per-key/per-version encoding estimate for downstream
/// synthesis budgeting.
pub(crate) fn predict_encoded_leaf_size(
    winners: &[FoldedLeafCell],
    retained_chains: &RetainedChains,
) -> usize {
    let leaf_bytes = LEAF_HEADER_SIZE
        + winners.len() * SLOT_POINTER_BYTES
        + winners
            .iter()
            .map(FoldedLeafCell::encoded_size)
            .sum::<usize>();

    leaf_bytes + retained_chains_encoded_size(retained_chains)
}

/// Encode folded winners as a complete 32 KB leaf page image.
pub(crate) fn encode_folded_leaf(
    winners: &[FoldedLeafCell],
    links: FoldedLeafLinks,
) -> Result<Vec<u8>> {
    let cells = winners
        .iter()
        .map(|cell| LeafCell {
            key: cell.key.clone(),
            value: cell.value.clone(),
        })
        .collect();
    let node = LeafNode {
        flags: 0,
        next_leaf_page: links.next_leaf_page,
        prev_leaf_page: links.prev_leaf_page,
        cells,
    };
    Ok(node.encode()?.to_vec())
}

/// Decode an existing folded leaf image into reconcile-facing cells and links.
pub(crate) fn decode_folded_leaf(image: &[u8]) -> Result<FoldedLeafImage> {
    let node = LeafNode::parse(image)?;
    let links = FoldedLeafLinks {
        next_leaf_page: node.next_leaf_page,
        prev_leaf_page: node.prev_leaf_page,
    };
    let cells = node
        .cells
        .into_iter()
        .map(|cell| FoldedLeafCell {
            key: cell.key,
            value: cell.value,
        })
        .collect();

    Ok(FoldedLeafImage { cells, links })
}

fn retained_chains_encoded_size(chains: &RetainedChains) -> usize {
    chains
        .iter()
        .map(|(key, chain)| {
            RETAINED_CHAIN_KEY_LEN_BYTES
                + key.len()
                + RETAINED_CHAIN_ENTRY_COUNT_BYTES
                + chain
                    .iter()
                    .map(retained_version_entry_encoded_size)
                    .sum::<usize>()
        })
        .sum()
}

fn retained_version_entry_encoded_size(entry: &VersionEntry) -> usize {
    TS_BYTES
        + TS_BYTES
        + TXN_ID_BYTES
        + version_state_encoded_size(entry.state)
        + TOMBSTONE_FLAG_BYTES
        + VERSION_DATA_KIND_BYTES
        + version_data_encoded_size(&entry.data)
}

fn version_state_encoded_size(state: VersionState) -> usize {
    VERSION_STATE_TAG_BYTES
        + match state {
            VersionState::Pending { .. } => PENDING_STATE_TXN_ID_BYTES,
            VersionState::Committed | VersionState::Aborted => 0,
        }
}

fn version_data_encoded_size(data: &VersionData) -> usize {
    match data {
        VersionData::Inline(bytes) => RETAINED_INLINE_LEN_BYTES + bytes.len(),
        VersionData::Overflow(_) => RETAINED_OVERFLOW_REF_BYTES,
    }
}
