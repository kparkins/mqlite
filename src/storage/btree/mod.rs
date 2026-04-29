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
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::read_view::ChainSnapshot;
use crate::mvcc::version::VersionEntry;

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
// Submodules
// ---------------------------------------------------------------------------

mod chain;
mod node;
pub(crate) mod reconcile;

use chain::*;
pub(crate) use node::CellValue;
use node::{InternalNode, LeafNode};

// ---------------------------------------------------------------------------
// Page store abstraction
// ---------------------------------------------------------------------------

/// Abstraction for reading, writing, allocating, and freeing B+ tree pages.
///
/// Implementors can back the store with the buffer pool + page allocator for
/// production use, or with an in-memory hash map for unit tests.
pub(crate) trait BTreePageStore {
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
    fn read_leaf(
        &self,
        page: u32,
    ) -> Result<(Box<[u8; PAGE_SIZE_LEAF as usize]>, Option<ChainSnapshot>)>;

    /// Write a 4 KB internal page.
    fn write_internal(&mut self, page: u32, data: &[u8; PAGE_SIZE_INTERNAL as usize])
        -> Result<()>;

    /// Write a 32 KB leaf (or overflow) page.
    fn write_leaf(&mut self, page: u32, data: &[u8; PAGE_SIZE_LEAF as usize]) -> Result<()>;

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
    fn read_internal(&self, page: u32) -> Result<Box<[u8; PAGE_SIZE_INTERNAL as usize]>> {
        Ok(self
            .internal_pages
            .get(&page)
            .cloned()
            .unwrap_or_else(|| Box::new([0u8; PAGE_SIZE_INTERNAL as usize])))
    }

    fn read_leaf(
        &self,
        page: u32,
    ) -> Result<(Box<[u8; PAGE_SIZE_LEAF as usize]>, Option<ChainSnapshot>)> {
        let buf = self
            .leaf_pages
            .get(&page)
            .cloned()
            .unwrap_or_else(|| Box::new([0u8; PAGE_SIZE_LEAF as usize]));
        let snap = self
            .leaf_chains
            .get(&page)
            .map(|src| ChainSnapshot::new(src, None));
        Ok((buf, snap))
    }

    fn write_internal(
        &mut self,
        page: u32,
        data: &[u8; PAGE_SIZE_INTERNAL as usize],
    ) -> Result<()> {
        self.internal_pages.insert(page, Box::new(*data));
        Ok(())
    }

    fn write_leaf(&mut self, page: u32, data: &[u8; PAGE_SIZE_LEAF as usize]) -> Result<()> {
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
        store.write_leaf(root_page, &buf)?;
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
        store.write_leaf(root_page, &buf)?;
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
