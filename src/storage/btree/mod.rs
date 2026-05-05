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
//! (buffer pool + allocator).  The in-memory [`MemPageStore`] is used for unit tests.
//!
//! ## Root tracking
//!
//! [`BTree`] owns `root_page: u32` (the page number of the current root) and
//! `root_level: u8` (0 = leaf, > 0 = internal at that level).  A root split increments
//! `root_level` and updates `root_page`; callers must persist the new root page number
//! (e.g. into the catalog or file header) if durability is required.

use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::read_view::ChainSnapshot;
use crate::mvcc::version::VersionEntry;
use crate::storage::buffer_pool::PageSize;

type VersionChainDrain = Vec<(Vec<u8>, Arc<VecDeque<VersionEntry>>)>;

/// Reader-path history fallthrough.
///
/// Bound to a specific durable tree identity at the call site — the BTree
/// layer only sees an opaque probe object and walks `(key, read_ts)`.
/// A `None` return means "no visible history entry"; a `Some(entry)` return
/// means the probe found the newest history version visible at `read_ts`
/// (tombstones included — the caller treats tombstones as "key absent").
pub(crate) trait HistoryProbe {
    fn probe(
        &self,
        key: &[u8],
        read_ts: crate::mvcc::timestamp::Ts,
    ) -> Result<Option<VersionEntry>>;
}
use crate::storage::page::{OVERFLOW_HEADER_SIZE, PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

/// Immutable 32 KiB leaf page image returned by reader paths.
///
/// Buffer-pool readers can hold the existing published `ArcSwap<Vec<u8>>`
/// snapshot without cloning the page bytes. Writer-side overlays still return
/// owned images so mutable paths never edit shared frame snapshots in place.
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
pub(crate) mod reconcile;
#[cfg(any(test, feature = "test-hooks"))]
pub mod us016_test_probe;
#[cfg(any(test, feature = "test-hooks"))]
pub mod us025_test_probe;

use chain::*;
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
    // MVCC version-chain accessors (T3.5)
    //
    // Leaf frames own per-key MVCC version chains. Split / merge operations
    // migrate these chains alongside the cells that own them; the `free_leaf`
    // call sites in the merge path are guarded by `chains_empty` to fail
    // loudly if migration is ever skipped.
    // -----------------------------------------------------------------------

    /// Remove and return the version chain for `key` on leaf `page`.
    #[allow(dead_code)]
    fn take_chain(&mut self, page: u32, key: &[u8]) -> Result<Option<Arc<VecDeque<VersionEntry>>>>;

    /// Install a delta chain for `key` on leaf `page`. Overwrites any
    /// existing chain for that key.
    fn put_chain(
        &mut self,
        page: u32,
        key: Vec<u8>,
        chain: Arc<VecDeque<VersionEntry>>,
    ) -> Result<()>;

    /// True iff no delta chains are attached to leaf `page`.
    fn chains_empty(&self, page: u32) -> Result<bool>;

    /// Clear ALL delta chains attached to leaf `page`.
    ///
    /// Used by the overflow-chain free path: overflow pages are allocated
    /// from the same 32 KB leaf pool as data leaves, so a page that was
    /// previously a data leaf may carry stale `deltas` entries
    /// when reborn as an overflow page. Clearing those entries before
    /// `free_leaf` keeps the T3.5 guard sound.
    ///
    /// No-op if the frame is not currently resident — there are no
    /// chains to clear in that case.
    fn clear_chains(&mut self, page: u32) -> Result<()>;

    /// Remove and return every delta chain currently attached to leaf
    /// `page`. Used by the leaf-merge path to migrate the residual chains
    /// for keys whose cells were already removed earlier in the same txn
    /// (e.g. by `delete_from_leaf`) onto the merged-into sibling — those
    /// tombstone-eligible chains must remain visible to MVCC readers
    /// whose ReadView predates the delete commit.
    ///
    /// Returns an empty vector if the frame is not resident.
    fn take_all_chains(&mut self, page: u32) -> Result<VersionChainDrain>;

    /// Drain every delta chain attached to one resident leaf page.
    ///
    /// This names the pool helper used by Phase 3 split routing. The existing
    /// `take_all_chains` method remains the merge/redistribute abstraction.
    fn take_all_chains_on_page(&mut self, page: u32) -> Result<VersionChainDrain> {
        self.take_all_chains(page)
    }
}

// ---------------------------------------------------------------------------
// In-memory page store (for tests)
// ---------------------------------------------------------------------------

use std::collections::{BTreeMap, HashMap};

/// A simple in-memory [`BTreePageStore`] backed by maps.
///
/// Reads of pages that were never written return zero-filled buffers.
/// Designed for unit tests; not intended for production use.
pub(crate) struct MemPageStore {
    internal_pages: HashMap<u32, Box<[u8; PAGE_SIZE_INTERNAL as usize]>>,
    leaf_pages: HashMap<u32, Box<[u8; PAGE_SIZE_LEAF as usize]>>,
    /// Per-leaf-page MVCC delta chains (T3.5). Outer key is page number,
    /// inner key is the B+ tree cell key.
    leaf_chains: HashMap<u32, BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>>,
    next_page: u32,
}

impl MemPageStore {
    /// Create an empty store with `next_page = 1` (page 0 is the file header).
    pub(crate) fn new() -> Self {
        Self {
            internal_pages: HashMap::new(),
            leaf_pages: HashMap::new(),
            leaf_chains: HashMap::new(),
            next_page: 1,
        }
    }
}

impl BTreePageStore for MemPageStore {
    type SharedReadGuard<'a>
        = ()
    where
        Self: 'a;

    fn read_internal(&self, page: u32) -> Result<Box<[u8; PAGE_SIZE_INTERNAL as usize]>> {
        Ok(self
            .internal_pages
            .get(&page)
            .cloned()
            .unwrap_or_else(|| Box::new([0u8; PAGE_SIZE_INTERNAL as usize])))
    }

    fn read_leaf(&self, page: u32) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        let buf = self
            .leaf_pages
            .get(&page)
            .cloned()
            .unwrap_or_else(|| Box::new([0u8; PAGE_SIZE_LEAF as usize]));
        let snap = self
            .leaf_chains
            .get(&page)
            .map(|src| ChainSnapshot::new(src, None));
        Ok((LeafPageImage::owned(buf), snap))
    }

    fn pin_shared_for_read<'a>(
        &'a self,
        _page: u32,
        _size: PageSize,
    ) -> Result<Self::SharedReadGuard<'a>> {
        Ok(())
    }

    fn write_internal(
        &mut self,
        page: u32,
        data: &[u8; PAGE_SIZE_INTERNAL as usize],
    ) -> Result<()> {
        self.internal_pages.insert(page, Box::new(*data));
        Ok(())
    }

    fn write_leaf_structural(
        &mut self,
        page: u32,
        data: &[u8; PAGE_SIZE_LEAF as usize],
    ) -> Result<()> {
        self.leaf_pages.insert(page, Box::new(*data));
        Ok(())
    }

    fn alloc_internal(&mut self) -> Result<u32> {
        let p = self.next_page;
        self.next_page += 1;
        Ok(p)
    }

    fn alloc_leaf(&mut self) -> Result<u32> {
        let p = self.next_page;
        self.next_page += 1;
        Ok(p)
    }

    fn free_internal(&mut self, page: u32) -> Result<()> {
        self.internal_pages.remove(&page);
        Ok(())
    }

    fn free_leaf(&mut self, page: u32) -> Result<()> {
        self.leaf_pages.remove(&page);
        Ok(())
    }

    fn take_chain(&mut self, page: u32, key: &[u8]) -> Result<Option<Arc<VecDeque<VersionEntry>>>> {
        Ok(self.leaf_chains.get_mut(&page).and_then(|m| m.remove(key)))
    }

    fn put_chain(
        &mut self,
        page: u32,
        key: Vec<u8>,
        chain: Arc<VecDeque<VersionEntry>>,
    ) -> Result<()> {
        self.leaf_chains.entry(page).or_default().insert(key, chain);
        Ok(())
    }

    fn chains_empty(&self, page: u32) -> Result<bool> {
        Ok(self.leaf_chains.get(&page).map_or(true, |m| m.is_empty()))
    }

    fn clear_chains(&mut self, page: u32) -> Result<()> {
        self.leaf_chains.remove(&page);
        Ok(())
    }

    fn take_all_chains(
        &mut self,
        page: u32,
    ) -> Result<Vec<(Vec<u8>, Arc<VecDeque<VersionEntry>>)>> {
        self.take_all_chains_on_page(page)
    }

    fn take_all_chains_on_page(
        &mut self,
        page: u32,
    ) -> Result<Vec<(Vec<u8>, Arc<VecDeque<VersionEntry>>)>> {
        Ok(self
            .leaf_chains
            .remove(&page)
            .map(|m| m.into_iter().collect())
            .unwrap_or_default())
    }
}

// ---------------------------------------------------------------------------
// Empty-page seed helpers
// ---------------------------------------------------------------------------
//
// Used by `TxnPageStore::alloc_leaf` / `alloc_internal` to seed the
// per-txn overlay with a valid empty-page image immediately after a
// fresh page is returned from the base allocator. Without the seed, any
// subsequent in-txn read of the page falls through the overlay to the
// shared buffer-pool frame, which still holds zero bytes (or stale
// bytes if the page was recycled from the free list). The decoder
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
        // Write an empty leaf page.
        let node = LeafNode {
            flags: 0,
            next_leaf_page: 0,
            prev_leaf_page: 0,
            cells: Vec::new(),
        };
        let buf = node.encode()?;
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
        let node = LeafNode {
            flags: 0,
            next_leaf_page: 0,
            prev_leaf_page: 0,
            cells: Vec::new(),
        };
        let buf = node.encode()?;
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
        collect_subtree_pages(&mut self.store, self.root_page, self.root_level, &mut pages)?;
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../btree_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../btree_us004_tests.rs"]
mod btree_us004_tests;

#[cfg(test)]
#[path = "../btree_us005_tests.rs"]
mod btree_us005_tests;

#[cfg(test)]
#[path = "../btree_us006_tests.rs"]
mod btree_us006_tests;

#[cfg(test)]
#[path = "../btree_phase4_us007_tests.rs"]
mod btree_phase4_us007_tests;

#[cfg(test)]
#[path = "../btree_us012_tests.rs"]
mod btree_us012_tests;

#[cfg(test)]
#[path = "../btree_us016_tests.rs"]
mod btree_us016_tests;

#[cfg(test)]
#[path = "../btree_us017_tests.rs"]
mod btree_us017_tests;

#[cfg(test)]
#[path = "../btree_us035_tests.rs"]
mod btree_us035_tests;
