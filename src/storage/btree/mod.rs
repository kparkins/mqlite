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
//! ## Page access abstraction
//!
//! The [`BTreePageStore`] trait decouples the B+ tree logic from the concrete page I/O
//! (buffer pool + allocator). Tests mount an in-memory store from the test module tree.
//!
//! ## Root tracking
//!
//! [`BTree`] owns `root_page: u32` (the page number of the current root) and
//! `root_level: u8` (0 = leaf, > 0 = internal at that level).  A root split increments
//! `root_level` and updates `root_page`; callers must persist the new root page number
//! (e.g. into the catalog or file header) if durability is required.

use std::collections::{BTreeMap, VecDeque};
use std::ops::Deref;
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::read_view::ChainSnapshot;
use crate::mvcc::version::VersionEntry;
use crate::storage::buffer_pool::{LatchMode, PageSize};

/// Reader-path history fallthrough.
///
/// Bound to a specific durable tree identity at the call site — the BTree
/// layer only sees an opaque probe object and walks `(key, read_ts)`.
/// A `None` return means "no visible history entry"; a `Some(entry)` return
/// means the probe found the newest history version visible at `read_ts`
/// (tombstones included — the caller treats tombstones as "key absent").
pub(crate) trait HistoryProbe {
    fn probe_visible_version(
        &self,
        key: &[u8],
        read_ts: crate::mvcc::timestamp::Ts,
    ) -> Result<Option<VersionEntry>>;
}
use crate::storage::page::{OVERFLOW_HEADER_SIZE, PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

/// Immutable 32 KiB leaf page image returned by reader paths.
///
/// Buffer-pool readers can hold the existing published `ArcSwap<Vec<u8>>`
/// snapshot without cloning the page bytes. Structural staged writes still
/// return owned images so mutable paths never edit shared frame snapshots in
/// place.
#[derive(Clone)]
pub(crate) enum LeafPageImage {
    Shared(Arc<Vec<u8>>),
    Owned(Box<[u8; PAGE_SIZE_LEAF as usize]>),
}

impl LeafPageImage {
    pub(crate) fn shared(data: Arc<Vec<u8>>) -> Result<Self> {
        if data.len() != PAGE_SIZE_LEAF as usize {
            return Err(crate::error::Error::Internal(format!(
                "leaf page image has {} bytes, expected {}",
                data.len(),
                PAGE_SIZE_LEAF
            )));
        }
        Ok(Self::Shared(data))
    }

    pub(crate) fn owned(data: Box<[u8; PAGE_SIZE_LEAF as usize]>) -> Self {
        Self::Owned(data)
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Shared(data) => data.as_slice(),
            Self::Owned(data) => data.as_slice(),
        }
    }
}

impl Deref for LeafPageImage {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

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
// Submodules
// ---------------------------------------------------------------------------

mod chain;
mod node;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/range_scan_latch_scope.rs"]
pub mod range_scan_latch_scope;
#[cfg(any(test, feature = "test-hooks"))]
#[path = "tests/reader_crabbing_observations.rs"]
pub mod reader_crabbing_observations;
pub(crate) mod reconcile;

use chain::{collect_subtree_pages, free_subtree};
pub(crate) use node::CellValue;
use node::{InternalNode, LeafNode};

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
// Page store abstraction
// ---------------------------------------------------------------------------

/// Abstraction for reading, writing, allocating, and freeing B+ tree pages.
///
/// Implementors can back the store with the buffer pool + page allocator for
/// production use, or with an in-memory hash map for unit tests.
pub(crate) trait BTreePageStore {
    /// Shared page guard held by reader traversal.
    type SharedReadGuard<'a>
    where
        Self: 'a;

    /// Read a 4 KB internal page into a heap-allocated buffer.
    fn read_internal(&self, page: u32) -> Result<Box<[u8; PAGE_SIZE_INTERNAL as usize]>>;

    /// Read a 32 KB leaf (or overflow) page into a heap-allocated buffer,
    /// returning an optional [`ChainSnapshot`] pinning every per-key MVCC
    /// version chain on the frame.
    ///
    /// The returned snapshot deep-clones each `VersionEntry`, running
    /// `OverflowRef::Clone` (CAS-loop incref) so every overflow page
    /// referenced from the chain is pinned for the snapshot's lifetime.
    /// Callers that do not need chain visibility can ignore the second
    /// tuple element — dropping the snapshot RAII-decrefs every bumped
    /// refcount.
    ///
    /// `None` is returned when the backing implementation has no MVCC
    /// chains for `page` (e.g. overflow pages read through the same API,
    /// or a buffer pool frame that is not currently resident).
    fn read_leaf(&self, page: u32) -> Result<(LeafPageImage, Option<ChainSnapshot>)>;

    /// Pin `page` and acquire the reader-side shared page latch.
    ///
    /// Implementations without page-local latches may return a no-op guard.
    fn pin_shared_for_read<'a>(
        &'a self,
        page: u32,
        size: PageSize,
    ) -> Result<Self::SharedReadGuard<'a>>;

    /// Read an internal page while its reader guard is still live.
    fn read_internal_guarded(
        &self,
        page: u32,
        _guard: &Self::SharedReadGuard<'_>,
    ) -> Result<Box<[u8; PAGE_SIZE_INTERNAL as usize]>> {
        self.read_internal(page)
    }

    /// Read a leaf page while its reader guard is still live.
    fn read_leaf_guarded(
        &self,
        page: u32,
        _guard: &Self::SharedReadGuard<'_>,
    ) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        self.read_leaf(page)
    }

    /// Read a point-lookup leaf while its reader guard is still live.
    fn read_leaf_for_key_guarded(
        &self,
        page: u32,
        guard: &Self::SharedReadGuard<'_>,
        key: &[u8],
    ) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        let _ = key;
        self.read_leaf_guarded(page, guard)
    }

    /// Write a 4 KB internal page.
    fn write_internal(&mut self, page: u32, data: &[u8; PAGE_SIZE_INTERNAL as usize])
        -> Result<()>;

    /// Write a 32 KB leaf (or overflow) page.
    fn write_leaf_structural(
        &mut self,
        page: u32,
        data: &[u8; PAGE_SIZE_LEAF as usize],
    ) -> Result<()>;

    /// Allocate a new 4 KB internal page.  Returns the page number.
    fn alloc_internal(&mut self) -> Result<u32>;

    /// Allocate a new 32 KB leaf page.  Returns the page number.
    fn alloc_leaf(&mut self) -> Result<u32>;

    /// Return a 4 KB internal page to the free pool.
    fn free_internal(&mut self, page: u32) -> Result<()>;

    /// Return a 32 KB leaf page to the free pool.
    fn free_leaf(&mut self, page: u32) -> Result<()>;

    // -----------------------------------------------------------------------
    // MVCC version-chain accessors (T3.5 → PR0.5)
    //
    // Leaf frames own per-key MVCC version chains. Split / merge operations
    // migrate these chains alongside the cells that own them; the `free_leaf`
    // call sites in the merge path are guarded by `chains_empty` to fail
    // loudly if migration is ever skipped.
    //
    // PR0.5 unifies every mutation behind `with_chain_under_latch` /
    // `with_all_chains_under_latch`. Both pin the leaf and acquire the
    // per-page latch before invoking the caller's closure, so concurrent
    // CRUD writers serialize on `frame.deltas` instead of racing through
    // the buffer-pool partition mutex. PR1's selective CoW and PR2's
    // running-sum cache hang off this single choke point.
    // -----------------------------------------------------------------------

    /// True iff no delta chains are attached to leaf `page`. Read-only
    /// inspector used by structural-cleanup guards (e.g. the
    /// `free_leaf`-path `chains_empty` check). Implementations may use a
    /// shared latch or no latch at all; mutation is forbidden.
    fn chains_empty(&self, page: u32) -> Result<bool>;

    /// Pin leaf `page` under `mode`, run `f` against the chain slot for
    /// `key`, and release the pin+latch.
    ///
    /// The closure receives `&mut Option<Arc<...>>` — `None` when the
    /// frame currently has no chain for `key`. After it returns, the
    /// slot is written back to the frame's `deltas` map: `Some` is
    /// inserted, `None` leaves the slot absent.
    ///
    /// `mode` must be [`LatchMode::Exclusive`] for chain mutation;
    /// shared callers should use `pin_shared_for_read` and the snapshot
    /// helpers on `LatchedPinnedPage` instead.
    fn with_chain_under_latch<R, F>(
        &mut self,
        page: u32,
        key: &[u8],
        mode: LatchMode,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut Option<Arc<VecDeque<VersionEntry>>>) -> R;

    /// Pin leaf `page` under `mode`, run `f` against the entire chain
    /// map, and release the pin+latch. Used by leaf-merge migration
    /// (drain) and overflow-page repurpose (clear).
    fn with_all_chains_under_latch<R, F>(&mut self, page: u32, mode: LatchMode, f: F) -> Result<R>
    where
        F: FnOnce(&mut BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>) -> R;
}

#[cfg(test)]
#[path = "tests/mem_page_store.rs"]
mod mem_page_store;
#[cfg(test)]
pub(crate) use mem_page_store::MemPageStore;

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
pub(crate) fn empty_leaf_page_bytes() -> Result<[u8; PAGE_SIZE_LEAF as usize]> {
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
pub(crate) fn empty_internal_page_bytes() -> Result<[u8; PAGE_SIZE_INTERNAL as usize]> {
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
    pub(crate) fn collect_pages_by_size(&mut self) -> Result<Vec<(u32, PageSize)>> {
        let mut pages = Vec::new();
        collect_subtree_pages(&self.store, self.root_page, self.root_level, &mut pages)?;
        Ok(pages)
    }
}

/// Return whether `data` has room for an inserted leaf cell.
pub(crate) fn leaf_can_insert_value(data: &[u8], key_len: usize, value_len: usize) -> Result<bool> {
    let node = LeafNode::parse(data)?;
    Ok(node.can_insert(encoded_leaf_cell_size(key_len, value_len)))
}

/// Return whether deleting `key` from `data` would make a non-root leaf rebalance.
pub(crate) fn leaf_needs_rebalance_after_delete(data: &[u8], key: &[u8]) -> Result<bool> {
    let mut node = LeafNode::parse(data)?;
    if let Ok(idx) = node.binary_search(key) {
        node.cells.remove(idx);
    }
    Ok(node.needs_rebalance())
}

fn encoded_leaf_cell_size(key_len: usize, value_len: usize) -> usize {
    let value_size = if value_len > OVERFLOW_THRESHOLD {
        8
    } else {
        4 + value_len
    };
    2 + key_len + 1 + value_size
}

mod delete;
mod insert;
mod scan;
pub(crate) use scan::read_overflow_chain;

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
