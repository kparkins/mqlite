//! B+ tree leaf-cell layout constants and the canonical cell-size arithmetic.
//!
//! ## Why one canonical size function
//!
//! The encoded byte width of a leaf cell feeds split-point classification: the
//! SMO path decides whether a write is root-neutral or structural by asking
//! whether the incremented leaf still fits ([`super::classify`]), and the
//! insert/delete split choosers (`choose_leaf_redistribution_split`,
//! `pack_cells_multiway`) sum these widths to keep both halves within
//! `PAGE_SIZE_LEAF`. A one-byte divergence between the "will it fit" estimate
//! and the actual `encode()` footprint is a split-point bug: a leaf that the
//! classifier judged root-neutral could overflow on encode, aborting the
//! structural batch. So every site that needs a leaf cell's on-page size routes
//! through [`leaf_cell_encoded_size`] here.

use crate::storage::buffer_pool::PageSize;
use crate::storage::page::{OVERFLOW_HEADER_SIZE, PAGE_SIZE_LEAF};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Values larger than this (in bytes) are stored in an overflow chain.
///
/// Chosen to leave room for a reasonable key and cell-pointer overhead in
/// the 32 KB leaf page.  Documents ≤ 30 KB are stored inline.
pub(crate) const OVERFLOW_THRESHOLD: usize = 30 * 1024;

/// Usable payload bytes per overflow page.
pub(super) const OVERFLOW_PAGE_DATA: usize = PAGE_SIZE_LEAF as usize - OVERFLOW_HEADER_SIZE;

/// A leaf with fewer than this many cells after a deletion triggers a
/// merge-or-redistribute operation.
pub(super) const MIN_LEAF_CELLS: usize = 4;

/// Non-root leaves also try to stay at least half full by bytes.
///
/// Leaf cells are variable-sized, so count-only balancing can choose a merge
/// that overflows the 32 KB page even though the sibling pair could be safely
/// redistributed.
pub(super) const MIN_LEAF_BYTES: usize = PAGE_SIZE_LEAF as usize / 2;

// ---------------------------------------------------------------------------
// Canonical leaf-cell size arithmetic
// ---------------------------------------------------------------------------

/// On-page byte width of a leaf cell's value field.
///
/// Inline values store `bson_len(4) + bson_data`; overflow values store a
/// fixed `first_page(4) + total_length(4)` pointer pair. This is the single
/// place that classifies a raw value length into its encoded value-field size.
pub(super) const fn leaf_cell_value_size(value_len: usize) -> usize {
    if value_len > OVERFLOW_THRESHOLD {
        8 // first_page(4) + total_length(4)
    } else {
        4 + value_len // bson_len(4) + bson_data
    }
}

/// On-page byte width of a complete leaf cell.
///
/// Layout: `key_len(2) | key | value_type(1) | value_field`. `value_size` is
/// the already-classified value-field width (see [`leaf_cell_value_size`] for
/// the inline/overflow branch, or pass the overflow constant `8` directly for
/// a known overflow cell). This is the canonical arithmetic shared by
/// `LeafCell::encoded_size`, `FoldedLeafCell::encoded_size`, and the
/// `leaf_can_insert_value` classifier.
pub(super) const fn leaf_cell_encoded_size(key_len: usize, value_size: usize) -> usize {
    2 + key_len + 1 + value_size
}

/// Map a B-tree path step's level to its allocator page size.
///
/// Level 0 is a 32 KiB leaf; any higher level is a 4 KiB internal page. Both
/// the reader descent ([`super::scan`]) and the SMO planner
/// (`paged_engine::smo_latch`) use this to pin or latch a page at its TRUE
/// size instead of the residency heuristic in `BufferPool::detect_page_size`,
/// which defaults a non-resident page to 32 KiB and would load an evicted
/// interior (4 KiB) page into the wrong partition.
pub(crate) fn page_size_for_level(level: u8) -> PageSize {
    if level == 0 {
        PageSize::Large32k
    } else {
        PageSize::Small4k
    }
}
