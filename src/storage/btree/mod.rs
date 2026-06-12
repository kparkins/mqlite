//! B+ tree core — search, insert, split, delete (merge), range scan, overflow.
//!
//! ## Design
//!
//! - **Internal nodes**: 4 KB pages (`PAGE_SIZE_INTERNAL`).  Each node stores up to ~150
//!   separator keys with 4-byte child-page pointers.  Fan-out ~150 per level; a 3-level
//!   tree addresses 3.375 M leaf pages.
//! - **Leaf nodes**: 32 KB pages (`PAGE_SIZE_LEAF`).  Cells are stored in a slot-array
//!   layout: a sorted cell-pointer array at the front of the page and cell data packed
//!   from the page end toward the middle.
//! - **Overflow**: documents whose serialized BSON exceeds [`OVERFLOW_THRESHOLD`] bytes
//!   are stored in chained 32 KB overflow pages; the leaf cell contains only a pointer.
//! - **Sibling pointers**: leaf pages form a doubly-linked list enabling `O(1)` range
//!   scan advancement.
//!
//! ## Module layout (R14)
//!
//! - [`store`] — the [`BTreePageStore`] trait, [`LeafPageImage`], and the
//!   reader-path [`HistoryProbe`] trait.
//! - [`layout`] — leaf-cell layout constants and the canonical cell-size
//!   arithmetic shared by every fit/split-classification site.
//! - [`internal_node`] / [`leaf_node`] — parsed page representations (the
//!   [`node`] facade re-exports them under the historical path).
//! - [`overflow`] — overflow-chain read/write/free/collect plus version-data
//!   resolution.
//! - [`chain_migration`] — atomic MVCC delta-chain migration across splits,
//!   merges, and redistributions.
//! - [`classify`] — SMO leaf-shape classifiers (consumed only by `smo_latch`).
//! - `insert` / `delete` / `scan` / `chain` — the CRUD and traversal paths.
//!
//! ## Page access abstraction
//!
//! The [`BTreePageStore`] trait decouples the B+ tree logic from the concrete
//! page I/O (buffer pool + allocator). Tests mount an in-memory store from the
//! test module tree.
//!
//! ## Root tracking
//!
//! [`BTree`] owns `root_page: u32` (the page number of the current root) and
//! `root_level: u8` (0 = leaf, > 0 = internal at that level).  A root split increments
//! `root_level` and updates `root_page`; callers must persist the new root page number
//! (e.g. into the catalog or file header) if durability is required.
//!
//! ## Structural-mutation exclusivity contract
//!
//! Per-page latches alone do NOT make a structural mutation safe. A reader
//! descends the tree holding at most a short shared latch on the page it is
//! crabbing through and otherwise relies on copy-on-write page-image
//! snapshots — it never latches the whole root-to-leaf path. A structural
//! mutation (a leaf or internal split, a leaf merge / redistribute, a page
//! free, or an overflow-chain migration) rewrites pages OTHER than the one a
//! crabbing reader currently holds: it moves cells between siblings, promotes
//! or demotes separators in parents, and recycles freed page numbers. If such
//! a mutation ran concurrently with a reader holding only a single per-page
//! latch, the reader could follow a child pointer into a page that has already
//! been emptied, merged away, or repurposed — observing a torn or vanished
//! subtree.
//!
//! The invariant that prevents this: structural mutation is only safe under
//! engine-level exclusivity over the entire affected path, NOT under per-call
//! page latches. In production the write path establishes this two ways. (1)
//! Ordinary CRUD that turns out to be structural escalates from a per-leaf
//! latch to exclusive latches over every page on the root-to-leaf path (the
//! SMO-latch set in `paged_engine::smo_latch`); a write classified as
//! root-neutral takes only the target-leaf latch, so the escalation is what
//! buys structural safety. (2) Whole-tree operations establish exclusivity by
//! making the tree unreachable first: checkpoint materialization runs under
//! the engine's `metadata.write()` fence, while the index-rebuild path frees
//! an old derived tree ([`BTree::free_all_pages`]) only after the rebuild's
//! unpublished structural batch has replaced every reference to it — no
//! reader can hold a root pointer into the freed subtree, which is the same
//! guarantee by a different mechanism. Both paths take `&mut self`
//! on the [`BTree`], so the Rust borrow checker also forbids a second
//! concurrent mutator of the same tree wrapper. Calling a mutating method
//! while only a per-page latch (or no engine-level exclusivity) is held is a
//! correctness bug, not merely a performance one: it races readers that the
//! page latch was never designed to exclude.

use crate::error::Result;

use node::{InternalNode, LeafNode};

// ---------------------------------------------------------------------------
// Submodules
// ---------------------------------------------------------------------------

mod chain;
mod chain_migration;
mod classify;
mod internal_node;
mod layout;
mod leaf_node;
mod node;
mod overflow;
mod store;

#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/range_scan_latch_scope.rs"]
pub mod range_scan_latch_scope;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/reader_crabbing_observations.rs"]
pub mod reader_crabbing_observations;
pub(crate) mod reconcile;

mod delete;
mod insert;
mod scan;

use chain::{collect_subtree_pages, free_subtree};

// ---------------------------------------------------------------------------
// Re-exports — every historical `btree::` path must resolve unchanged.
// ---------------------------------------------------------------------------

pub(crate) use classify::{leaf_can_insert_value, leaf_needs_rebalance_after_delete};
pub(crate) use layout::{page_size_for_level, OVERFLOW_THRESHOLD};
pub(crate) use node::CellValue;
pub(crate) use overflow::read_overflow_chain;
pub(crate) use store::{BTreePageStore, HistoryProbe, LeafPageImage};

#[cfg(test)]
#[path = "tests/mem_page_store.rs"]
mod mem_page_store;
#[cfg(test)]
pub(crate) use mem_page_store::MemPageStore;

// ---------------------------------------------------------------------------
// Path step
// ---------------------------------------------------------------------------

/// One page on a root-to-leaf B-tree traversal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BTreePathStep {
    /// Page reached at this step.
    pub(crate) page_id: u32,
    /// Parent page from the previous step, or `None` for the root.
    pub(crate) parent_page: Option<u32>,
    /// Child slot in `parent_page`, or `None` for the root.
    pub(crate) child_slot: Option<usize>,
    /// B-tree level of `page_id` (`0` means leaf).
    pub(crate) level: u8,
}

// ---------------------------------------------------------------------------
// Leaf-range inspectors (consumed by index_maint + smo_latch)
// ---------------------------------------------------------------------------

/// Return true when the encoded leaf contains a base cell in `[start, end)`.
pub(crate) fn leaf_contains_key_in_range(
    data: &[u8],
    start: &[u8],
    end: &[u8],
    exclude_key: &[u8],
) -> Result<bool> {
    let node = LeafNode::parse(data)?;
    let mut idx = node.binary_search(start).unwrap_or_else(|insert| insert);
    while let Some(cell) = node.cells.get(idx) {
        let key = cell.key.as_slice();
        if key >= end {
            break;
        }
        if key != exclude_key {
            return Ok(true);
        }
        idx += 1;
    }
    Ok(false)
}

/// Return sibling leaf page ids that may also contain keys in `[start, end)`.
pub(crate) fn leaf_unique_prefix_sibling_pages(
    data: &[u8],
    start: &[u8],
    end: &[u8],
) -> Result<Vec<u32>> {
    let node = LeafNode::parse(data)?;
    let mut pages = Vec::with_capacity(2);
    if let Some(first) = node.cells.first() {
        if node.prev_leaf_page != 0 && start <= first.key.as_slice() {
            pages.push(node.prev_leaf_page);
        }
    } else if node.prev_leaf_page != 0 {
        pages.push(node.prev_leaf_page);
    }
    if let Some(last) = node.cells.last() {
        if node.next_leaf_page != 0 && end > last.key.as_slice() {
            pages.push(node.next_leaf_page);
        }
    } else if node.next_leaf_page != 0 {
        pages.push(node.next_leaf_page);
    }
    Ok(pages)
}

// ---------------------------------------------------------------------------
// Empty-page seed helpers
// ---------------------------------------------------------------------------
//
// Used by `StructuralBatchStore::alloc_leaf` / `alloc_internal` to seed staged
// structural bytes with a valid empty-page image immediately after a fresh page
// is returned from the base allocator. Without the seed, any subsequent
// in-batch read of the page falls through to the shared buffer-pool frame,
// which still holds zero bytes (or stale bytes if the page was recycled from
// the free list). The decoder
// rejects that as "unknown cell value type 0x00" or "expected leaf
// page type 0x02, found 0x00".
//
// The empty-leaf seed is also used for fresh pages that the caller
// will immediately repurpose as overflow pages: `write_overflow_chain`
// writes the full page (zero-init buffer + header + payload), so the
// seed bytes are replaced before any read sees them as overflow.

/// Build the bytes of a valid empty 32 KB leaf page (zero cells,
/// no sibling links, no flags).
pub(crate) fn empty_leaf_page_bytes() -> Result<[u8; crate::storage::page::PAGE_SIZE_LEAF as usize]>
{
    let node = LeafNode {
        flags: 0,
        next_leaf_page: 0,
        prev_leaf_page: 0,
        cells: Vec::new(),
    };
    node.encode()
}

/// Build the bytes of a valid empty 4 KB internal page (level 0,
/// zero entries, zero rightmost child).
pub(crate) fn empty_internal_page_bytes(
) -> Result<[u8; crate::storage::page::PAGE_SIZE_INTERNAL as usize]> {
    let node = InternalNode {
        level: 0,
        entries: Vec::new(),
        rightmost_child: 0,
    };
    node.encode()
}

// ---------------------------------------------------------------------------
// BTree
// ---------------------------------------------------------------------------

/// A B+ tree backed by a [`BTreePageStore`].
///
/// The tree owns a page store and a root pointer.  On creation, [`BTree::create`]
/// allocates the first (empty) leaf page and sets it as the root.
///
/// After a root split, `root_page` and `root_level` are updated in place.
/// Callers that need persistence must store the updated root page number (e.g. in
/// the file header or catalog) after any mutating operation.
pub(crate) struct BTree<S: BTreePageStore> {
    /// The backing page store.
    pub(crate) store: S,
    /// Page number of the current root page.
    pub(crate) root_page: u32,
    /// Tree height indicator:
    /// - `0`: root is a leaf page.
    /// - `n > 0`: root is an internal page at level `n`.
    pub(crate) root_level: u8,
}

impl<S: BTreePageStore> BTree<S> {
    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /// Create a new empty B+ tree in `store`, allocating the first leaf page as root.
    pub(crate) fn create(mut store: S) -> Result<Self> {
        let root_page = store.alloc_leaf()?;
        let buf = empty_leaf_page_bytes()?;
        store.write_leaf_structural(root_page, &buf)?;
        Ok(BTree {
            store,
            root_page,
            root_level: 0,
        })
    }

    /// Initialise a pre-allocated page as an empty leaf root and return a new tree.
    ///
    /// Use this when the page was already allocated (e.g. by the catalog's
    /// `create_collection`) but not yet written.  Writing an empty leaf header
    /// at `root_page` makes the tree ready for insertions without allocating
    /// an additional page.
    pub(crate) fn create_at(mut store: S, root_page: u32) -> Result<Self> {
        let buf = empty_leaf_page_bytes()?;
        store.write_leaf_structural(root_page, &buf)?;
        Ok(BTree {
            store,
            root_page,
            root_level: 0,
        })
    }

    /// Wrap an existing `store` with a known `root_page` and `root_level`.
    ///
    /// Use this when opening an existing tree that was persisted to a file header.
    pub(crate) fn open(store: S, root_page: u32, root_level: u8) -> Self {
        BTree {
            store,
            root_page,
            root_level,
        }
    }

    /// Free every page occupied by this B+ tree, returning them to the allocator.
    ///
    /// Traverses the complete tree and calls `free_internal` / `free_leaf` on
    /// every internal node, leaf node, and overflow chain.  After this call the
    /// `BTree` is consumed and must not be used again.
    ///
    /// Callers must have already removed the tree's root-page reference from
    /// the catalog or file header before (or after) calling this, to prevent
    /// the freed pages from being referenced again.
    pub(crate) fn free_all_pages(mut self) -> Result<()> {
        free_subtree(&mut self.store, self.root_page, self.root_level)
    }

    /// Return every page occupied by this B+ tree with its allocator page size.
    ///
    /// The list includes internal, leaf, and overflow pages. Callers that need
    /// deterministic multi-page latch ordering should sort the returned vector
    /// by page id before acquiring latches.
    pub(crate) fn collect_pages_by_size(&mut self) -> Result<Vec<(u32, crate::storage::buffer_pool::PageSize)>> {
        let mut pages = Vec::new();
        collect_subtree_pages(&self.store, self.root_page, self.root_level, &mut pages)?;
        Ok(pages)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/structural_operations.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/history_probe_scan.rs"]
mod history_probe_scan;

#[cfg(test)]
#[path = "tests/delta_point_lookup.rs"]
mod delta_point_lookup;

#[cfg(test)]
#[path = "tests/merged_range_scan.rs"]
mod merged_range_scan;

#[cfg(test)]
#[path = "tests/folded_leaf_encoding.rs"]
mod folded_leaf_encoding;

#[cfg(test)]
#[path = "tests/history_fallthrough.rs"]
mod history_fallthrough;

#[cfg(test)]
#[path = "tests/split_delta_routing.rs"]
mod split_delta_routing;

#[cfg(test)]
#[path = "tests/split_atomicity.rs"]
mod split_atomicity;

#[cfg(test)]
#[path = "tests/leaf_cell_decode.rs"]
mod leaf_cell_decode;

#[cfg(test)]
#[path = "tests/split_leaf_byte_budget.rs"]
mod split_leaf_byte_budget;

#[cfg(test)]
#[path = "tests/replace_existing_overflow_leak.rs"]
mod replace_existing_overflow_leak;

#[cfg(test)]
#[path = "tests/overflow_chain_corruption.rs"]
mod overflow_chain_corruption;

#[cfg(test)]
#[path = "tests/bugsuspect_range_scan_history_divergence.rs"]
mod bugsuspect_range_scan_history_divergence;
