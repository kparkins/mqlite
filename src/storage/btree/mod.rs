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

use crate::error::{Error, Result};
use crate::mvcc::read_view::{ChainSnapshot, ReadView};
use crate::mvcc::version::{VersionData, VersionEntry};

/// Reader-path history fallthrough (plan §T7).
///
/// Bound to a specific `(ns_id, kind_tag)` at the call site — the BTree
/// layer only sees an opaque probe object and walks `(key, read_ts)`.
/// A `None` return means "no entry ≤ read_ts"; a `Some(entry)` return
/// means the probe found the newest visible history version (tombstones
/// included — the caller treats tombstones as "key absent").
pub(crate) trait HistoryProbe {
    fn probe(&self, key: &[u8], read_ts: crate::mvcc::timestamp::Ts)
        -> Result<Option<VersionEntry>>;
}
use crate::storage::page::{
    LEAF_HEADER_SIZE, OVERFLOW_HEADER_SIZE, PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF,
};

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

mod node;
mod chain;

pub(crate) use node::CellValue;
use node::{InternalNode, LeafCell, LeafNode, SplitResult};
use chain::*;

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
    ) -> Result<(
        Box<[u8; PAGE_SIZE_LEAF as usize]>,
        Option<ChainSnapshot>,
    )>;

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
    fn take_chain(
        &mut self,
        page: u32,
        key: &[u8],
    ) -> Result<Option<Arc<VecDeque<VersionEntry>>>>;

    /// Install a version chain for `key` on leaf `page`. Overwrites any
    /// existing chain for that key.
    fn put_chain(
        &mut self,
        page: u32,
        key: Vec<u8>,
        chain: Arc<VecDeque<VersionEntry>>,
    ) -> Result<()>;

    /// True iff no version chains are attached to leaf `page`.
    fn chains_empty(&self, page: u32) -> Result<bool>;

    /// Clear ALL version chains attached to leaf `page`.
    ///
    /// Used by the overflow-chain free path: overflow pages are allocated
    /// from the same 32 KB leaf pool as data leaves, so a page that was
    /// previously a data leaf may carry stale `version_chains` entries
    /// when reborn as an overflow page. Clearing those entries before
    /// `free_leaf` keeps the T3.5 guard sound.
    ///
    /// No-op if the frame is not currently resident — there are no
    /// chains to clear in that case.
    fn clear_chains(&mut self, page: u32) -> Result<()>;

    /// Remove and return every version chain currently attached to leaf
    /// `page`. Used by the leaf-merge path to migrate the residual chains
    /// for keys whose cells were already removed earlier in the same txn
    /// (e.g. by `delete_from_leaf`) onto the merged-into sibling — those
    /// tombstone-eligible chains must remain visible to MVCC readers
    /// whose ReadView predates the delete commit.
    ///
    /// Returns an empty vector if the frame is not resident.
    fn take_all_chains(
        &mut self,
        page: u32,
    ) -> Result<Vec<(Vec<u8>, Arc<VecDeque<VersionEntry>>)>>;
}

// ---------------------------------------------------------------------------
// In-memory page store (for tests)
// ---------------------------------------------------------------------------

use std::collections::HashMap;

/// A simple in-memory [`BTreePageStore`] backed by hash maps.
///
/// Reads of pages that were never written return zero-filled buffers.
/// Designed for unit tests; not intended for production use.
pub(crate) struct MemPageStore {
    internal_pages: HashMap<u32, Box<[u8; PAGE_SIZE_INTERNAL as usize]>>,
    leaf_pages: HashMap<u32, Box<[u8; PAGE_SIZE_LEAF as usize]>>,
    /// Per-leaf-page MVCC version chains (T3.5). Outer key is page number,
    /// inner key is the B+ tree cell key.
    leaf_chains: HashMap<u32, HashMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>>,
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
    ) -> Result<(
        Box<[u8; PAGE_SIZE_LEAF as usize]>,
        Option<ChainSnapshot>,
    )> {
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

    fn take_chain(
        &mut self,
        page: u32,
        key: &[u8],
    ) -> Result<Option<Arc<VecDeque<VersionEntry>>>> {
        Ok(self
            .leaf_chains
            .get_mut(&page)
            .and_then(|m| m.remove(key)))
    }

    fn put_chain(
        &mut self,
        page: u32,
        key: Vec<u8>,
        chain: Arc<VecDeque<VersionEntry>>,
    ) -> Result<()> {
        self.leaf_chains
            .entry(page)
            .or_default()
            .insert(key, chain);
        Ok(())
    }

    fn chains_empty(&self, page: u32) -> Result<bool> {
        Ok(self
            .leaf_chains
            .get(&page)
            .map_or(true, |m| m.is_empty()))
    }

    fn clear_chains(&mut self, page: u32) -> Result<()> {
        self.leaf_chains.remove(&page);
        Ok(())
    }

    fn take_all_chains(
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
// Empty-page seed helpers (Bug A fix — §M4a/M4b)
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

    // -----------------------------------------------------------------------
    // Search
    // -----------------------------------------------------------------------

    /// Search for `key`, returning the value if found.
    ///
    /// If the value is an overflow pointer, the raw bytes are **not** read here
    /// (the caller must call [`BTree::read_overflow`] explicitly).  Use
    /// [`BTree::get`] for a fully resolved lookup.
    pub(crate) fn search(&self, key: &[u8]) -> Result<Option<CellValue>> {
        let leaf_page = self.find_leaf(key)?;
        let (buf, _) = self.store.read_leaf(leaf_page)?;
        let node = LeafNode::parse(&buf[..])?;
        match node.binary_search(key) {
            Ok(i) => Ok(Some(node.cells[i].value.clone())),
            Err(_) => Ok(None),
        }
    }

    /// Like [`BTree::search`] but resolves overflow pointers, returning the raw
    /// BSON bytes for all cases.
    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.search(key)? {
            None => Ok(None),
            Some(CellValue::Inline(v)) => Ok(Some(v)),
            Some(CellValue::Overflow {
                first_page,
                total_length,
            }) => Ok(Some(read_overflow_chain(
                &self.store,
                first_page,
                total_length,
            )?)),
        }
    }

    /// Read a previously written overflow chain starting at `first_page`.
    pub(crate) fn read_overflow(&self, first_page: u32, total_length: u32) -> Result<Vec<u8>> {
        read_overflow_chain(&self.store, first_page, total_length)
    }

    /// MVCC-aware point lookup (T5' sub-step 3).
    ///
    /// Consults the owning leaf frame's version chain via `ChainSnapshot`
    /// first; if a [`VersionEntry`] visible to `view` exists for `key`,
    /// its payload is returned (respecting `is_tombstone`). Otherwise the
    /// on-disk cell is used — this is the dual-write intermediate state
    /// (T5' has both the in-memory chain and the on-disk cell; T6
    /// reconciliation will collapse them). Pre-MVCC keys that never got a
    /// staged write flow through the on-disk fallback.
    ///
    /// Not yet called from the engine's reader paths — those route through
    /// `range_scan_mvcc` via `btree_collscan`. Kept as a T5' acceptance
    /// deliverable and for future point-lookup fast-paths (T6+).
    #[allow(dead_code)]
    pub(crate) fn get_mvcc(
        &self,
        key: &[u8],
        view: &ReadView,
        history: Option<&dyn HistoryProbe>,
    ) -> Result<Option<Vec<u8>>> {
        let leaf_page = self.find_leaf(key)?;
        let (buf, snap) = self.store.read_leaf(leaf_page)?;
        if let Some(snap) = snap.as_ref() {
            if let Some(entry) = snap.visible_at(key, view) {
                if entry.is_tombstone {
                    return Ok(None);
                }
                return Ok(Some(match &entry.data {
                    VersionData::Inline(v) => v.clone(),
                    VersionData::Overflow(oref) => read_overflow_chain(
                        &self.store,
                        oref.first_page(),
                        oref.total_length() as u32,
                    )?,
                }));
            }
        }
        // Plan §T7: history fallthrough. The chain had no entry visible at
        // `view.read_ts` — an evicted entry in the history store might.
        if let Some(probe) = history {
            if let Some(entry) = probe.probe(key, view.read_ts)? {
                if entry.is_tombstone {
                    return Ok(None);
                }
                return Ok(Some(match &entry.data {
                    VersionData::Inline(v) => v.clone(),
                    VersionData::Overflow(oref) => read_overflow_chain(
                        &self.store,
                        oref.first_page(),
                        oref.total_length() as u32,
                    )?,
                }));
            }
        }
        // Fall back to the on-disk cell (dual-write intermediate).
        let node = LeafNode::parse(&buf[..])?;
        match node.binary_search(key) {
            Ok(i) => match &node.cells[i].value {
                CellValue::Inline(v) => Ok(Some(v.clone())),
                CellValue::Overflow {
                    first_page,
                    total_length,
                } => Ok(Some(read_overflow_chain(
                    &self.store,
                    *first_page,
                    *total_length,
                )?)),
            },
            Err(_) => Ok(None),
        }
    }

    // -----------------------------------------------------------------------
    // Insert
    // -----------------------------------------------------------------------

    /// Insert `key` → `value` into the tree.
    ///
    /// If `value.len() > OVERFLOW_THRESHOLD`, the value is written to an
    /// overflow chain automatically and the leaf cell holds a pointer.
    ///
    /// Returns `Err(Error::DuplicateKey)` if the key already exists.
    pub(crate) fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let cell_value = if value.len() > OVERFLOW_THRESHOLD {
            let first_page = write_overflow_chain(&mut self.store, value)?;
            CellValue::Overflow {
                first_page,
                total_length: value.len() as u32,
            }
        } else {
            CellValue::Inline(value.to_vec())
        };

        let split = self.insert_subtree(self.root_page, self.root_level, key, cell_value)?;

        if let Some(sr) = split {
            // The root split: allocate a new root internal node.
            let new_root = self.store.alloc_internal()?;
            let new_level = self.root_level + 1;
            let new_node = InternalNode {
                level: new_level,
                entries: vec![(sr.promoted_key, self.root_page)],
                rightmost_child: sr.right_page,
            };
            let buf = new_node.encode()?;
            self.store.write_internal(new_root, &buf)?;
            self.root_page = new_root;
            self.root_level = new_level;
        }

        Ok(())
    }

    /// Recursive insert into the subtree rooted at `page` (at `level`).
    ///
    /// Returns `Some(SplitResult)` if the node at `page` split.
    fn insert_subtree(
        &mut self,
        page: u32,
        level: u8,
        key: &[u8],
        value: CellValue,
    ) -> Result<Option<SplitResult>> {
        if level == 0 {
            return self.insert_leaf(page, key, value);
        }

        // Internal node.
        let buf = self.store.read_internal(page)?;
        let node = InternalNode::parse(&buf[..])?;
        let child_idx = node.find_child_idx(key);
        let child_page = node.child_at(child_idx);

        let child_split = self.insert_subtree(child_page, level - 1, key, value)?;

        if let Some(sr) = child_split {
            self.insert_into_internal(page, sr.promoted_key, sr.right_page)
        } else {
            Ok(None)
        }
    }

    /// Insert a key–value cell into the leaf at `page`.
    ///
    /// If the leaf is full, split it and return a [`SplitResult`].
    fn insert_leaf(
        &mut self,
        page: u32,
        key: &[u8],
        value: CellValue,
    ) -> Result<Option<SplitResult>> {
        let (buf, _) = self.store.read_leaf(page)?;
        let mut node = LeafNode::parse(&buf[..])?;

        let new_cell = LeafCell {
            key: key.to_vec(),
            value,
        };
        let cell_size = new_cell.encoded_size();

        // Check for duplicate.
        if node.binary_search(key).is_ok() {
            return Err(Error::DuplicateKey {
                detail: format!("key already exists (len={})", key.len()),
            });
        }

        if node.can_insert(cell_size) {
            // Insert and keep sorted.
            let pos = node.binary_search(key).unwrap_err();
            node.cells.insert(pos, new_cell);
            let encoded = node.encode()?;
            self.store.write_leaf(page, &encoded)?;
            Ok(None)
        } else {
            // Leaf is full: split.
            self.split_leaf(page, node, new_cell)
        }
    }

    /// Split a full leaf, inserting `new_cell`, and return the promoted key + right page.
    fn split_leaf(
        &mut self,
        left_page: u32,
        mut left_node: LeafNode,
        new_cell: LeafCell,
    ) -> Result<Option<SplitResult>> {
        // Insert new_cell into the cell list (maintaining sorted order).
        let pos = left_node.binary_search(&new_cell.key).unwrap_err();
        left_node.cells.insert(pos, new_cell);

        let total = left_node.cells.len();
        let split_at = total / 2; // right half starts here

        // Allocate right sibling.
        let right_page = self.store.alloc_leaf()?;

        // Build right node with the upper half of cells.
        let right_cells: Vec<LeafCell> = left_node.cells.drain(split_at..).collect();
        let promoted_key = right_cells[0].key.clone();

        // T3.5: Migrate version chains for the keys that are moving to the
        // right sibling. Chains stay pinned (refcount invariant preserved)
        // because Arc ownership transfers without touching the inner data.
        for cell in &right_cells {
            if let Some(chain) = self.store.take_chain(left_page, &cell.key)? {
                self.store.put_chain(right_page, cell.key.clone(), chain)?;
            }
        }

        let right_node = LeafNode {
            flags: 0,
            next_leaf_page: left_node.next_leaf_page,
            prev_leaf_page: left_page,
            cells: right_cells,
        };

        // Update left node's next pointer.
        left_node.next_leaf_page = right_page;

        // Update the old right sibling's prev pointer (if any).
        if right_node.next_leaf_page != 0 {
            let (old_next_buf, _) = self.store.read_leaf(right_node.next_leaf_page)?;
            let mut old_next = LeafNode::parse(&old_next_buf[..])?;
            old_next.prev_leaf_page = right_page;
            let enc = old_next.encode()?;
            self.store.write_leaf(right_node.next_leaf_page, &enc)?;
        }

        // Write both nodes.
        let left_enc = left_node.encode()?;
        let right_enc = right_node.encode()?;
        self.store.write_leaf(left_page, &left_enc)?;
        self.store.write_leaf(right_page, &right_enc)?;

        Ok(Some(SplitResult {
            promoted_key,
            right_page,
        }))
    }

    /// Insert a new separator key `promoted_key` into the internal node at `page`.
    ///
    /// `right_child` is the **right** sibling produced by the child split.  The
    /// **left** sibling is already referenced by the existing entry at position `p`
    /// (or by `rightmost_child` when `p == entries.len()`); we update that pointer
    /// to `right_child` and insert a new entry `(promoted_key, old_child)` where
    /// `old_child` was the former left sibling.
    ///
    /// If the internal node overflows, split it and return a [`SplitResult`].
    fn insert_into_internal(
        &mut self,
        page: u32,
        promoted_key: Vec<u8>,
        right_child: u32,
    ) -> Result<Option<SplitResult>> {
        let buf = self.store.read_internal(page)?;
        let mut node = InternalNode::parse(&buf[..])?;

        // Find insertion position: first index where entries[p].0 > promoted_key.
        let p = node
            .entries
            .partition_point(|(k, _)| k.as_slice() <= promoted_key.as_slice());

        // The child currently at position p is the left half of the split.
        // Update it to point to the right half, and remember the old (left) pointer.
        let left_child = if p < node.entries.len() {
            let old = node.entries[p].1;
            node.entries[p].1 = right_child;
            old
        } else {
            let old = node.rightmost_child;
            node.rightmost_child = right_child;
            old
        };

        if node.can_insert(promoted_key.len()) {
            node.entries.insert(p, (promoted_key, left_child));
            let encoded = node.encode()?;
            self.store.write_internal(page, &encoded)?;
            Ok(None)
        } else {
            // Internal node full: split.
            // `node` already has entries[p].1 updated to right_child.
            self.split_internal(page, node, promoted_key, left_child)
        }
    }

    /// Split a full internal node, inserting `(new_key, new_left_child)`.
    ///
    /// `node` has already been updated in `insert_into_internal` so that the
    /// child pointer for the new key's right sibling is correct.  We only need
    /// to insert `(new_key, new_left_child)` and then split at the midpoint.
    fn split_internal(
        &mut self,
        left_page: u32,
        mut left_node: InternalNode,
        new_key: Vec<u8>,
        new_left_child: u32, // left child of new_key (= left half of the child split)
    ) -> Result<Option<SplitResult>> {
        // Insert the new entry in sorted order.
        let pos = left_node
            .entries
            .partition_point(|(k, _)| k.as_slice() <= new_key.as_slice());
        left_node.entries.insert(pos, (new_key, new_left_child));

        let total = left_node.entries.len();
        let m = total / 2;

        // Promote the key at index m to the parent.
        // entries[m].1 (its left child) becomes the new rightmost of the left node.
        let (promoted_key, promoted_left_child) = left_node.entries.remove(m);

        // Drain right half (entries after m, which is now at old index m+1).
        let right_entries: Vec<(Vec<u8>, u32)> = left_node.entries.drain(m..).collect();
        let old_rightmost = left_node.rightmost_child;

        // Left node: entries[0..m-1], rightmost = promoted_left_child.
        left_node.rightmost_child = promoted_left_child;

        // Right node: entries[m+1..], rightmost = old rightmost_child.
        let right_page = self.store.alloc_internal()?;
        let right_node = InternalNode {
            level: left_node.level,
            entries: right_entries,
            rightmost_child: old_rightmost,
        };

        let left_enc = left_node.encode()?;
        let right_enc = right_node.encode()?;
        self.store.write_internal(left_page, &left_enc)?;
        self.store.write_internal(right_page, &right_enc)?;

        Ok(Some(SplitResult {
            promoted_key,
            right_page,
        }))
    }

    // -----------------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------------

    /// Delete `key` from the tree.  Returns `true` if the key existed, `false`
    /// if not found.
    ///
    /// Overflow chains are freed when a cell with an overflow pointer is deleted.
    ///
    /// After deletion, if a non-root leaf falls below the minimum occupancy by
    /// cell count or byte usage, the tree attempts to redistribute from or
    /// merge with a sibling. Parent separator keys are updated accordingly.
    pub(crate) fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let mut path: Vec<(u32, usize)> = Vec::new();
        let found = self.delete_subtree(self.root_page, self.root_level, key, &mut path)?;
        Ok(found)
    }

    /// Recursive delete.
    ///
    /// `path` accumulates `(internal_page, child_idx)` entries as we descend so that
    /// we can walk back up to fix separator keys after merges.
    fn delete_subtree(
        &mut self,
        page: u32,
        level: u8,
        key: &[u8],
        path: &mut Vec<(u32, usize)>,
    ) -> Result<bool> {
        if level == 0 {
            return self.delete_from_leaf(page, key, path);
        }

        let buf = self.store.read_internal(page)?;
        let node = InternalNode::parse(&buf[..])?;
        let child_idx = node.find_child_idx(key);
        let child_page = node.child_at(child_idx);

        path.push((page, child_idx));
        let found = self.delete_subtree(child_page, level - 1, key, path)?;
        path.pop();
        Ok(found)
    }

    /// Delete `key` from the leaf at `page`, then handle underflow.
    fn delete_from_leaf(&mut self, page: u32, key: &[u8], path: &[(u32, usize)]) -> Result<bool> {
        let (buf, _) = self.store.read_leaf(page)?;
        let mut node = LeafNode::parse(&buf[..])?;

        let idx = match node.binary_search(key) {
            Ok(i) => i,
            Err(_) => return Ok(false),
        };

        // Free overflow chain if present.
        let removed = node.cells.remove(idx);
        if let CellValue::Overflow { first_page, .. } = removed.value {
            free_overflow_chain(&mut self.store, first_page)?;
        }

        let encoded = node.encode()?;
        self.store.write_leaf(page, &encoded)?;

        // Check for underflow and potentially merge/redistribute.
        if node.needs_rebalance() && !path.is_empty() {
            self.handle_leaf_underflow(page, node, path)?;
        }

        Ok(true)
    }

    /// Handle underflow in a leaf by redistributing from or merging with a sibling.
    fn handle_leaf_underflow(
        &mut self,
        page: u32,
        node: LeafNode,
        path: &[(u32, usize)],
    ) -> Result<()> {
        // Get parent info.
        let (parent_page, child_idx) = *path.last().unwrap();
        let parent_buf = self.store.read_internal(parent_page)?;
        let parent = InternalNode::parse(&parent_buf[..])?;

        let key_count = parent.entries.len();

        // Prefer rebalancing with the left sibling. The decision is byte-aware:
        // merge only if the combined cells fit in one 32 KB leaf; otherwise
        // repartition the pair across the two existing pages.
        if child_idx > 0 {
            let left_sibling_idx = child_idx - 1;
            let left_page = parent.child_at(left_sibling_idx);
            let (left_buf, _) = self.store.read_leaf(left_page)?;
            let left_node = LeafNode::parse(&left_buf[..])?;

            if Self::can_merge_leaves(&left_node, &node) {
                return self.merge_leaf_into_left(
                    parent_page,
                    child_idx,
                    path,
                    left_page,
                    left_node,
                    page,
                    node,
                );
            }

            return self.redistribute_leaf_pair(
                left_page,
                left_node,
                page,
                node,
                parent_page,
                left_sibling_idx,
            );
        }

        // No left sibling: repair against the right sibling.
        if child_idx < key_count {
            let right_sibling_idx = child_idx + 1;
            let right_page = parent.child_at(right_sibling_idx);
            let (right_buf, _) = self.store.read_leaf(right_page)?;
            let right_node = LeafNode::parse(&right_buf[..])?;

            if Self::can_merge_leaves(&node, &right_node) {
                return self.merge_leaf_into_right(
                    parent_page,
                    path,
                    page,
                    node,
                    right_page,
                    right_node,
                );
            }

            return self.redistribute_leaf_pair(
                page,
                node,
                right_page,
                right_node,
                parent_page,
                child_idx,
            );
        }

        Err(Error::Internal(
            "leaf underflow reached a parent with no siblings".into(),
        ))
    }

    fn can_merge_leaves(left: &LeafNode, right: &LeafNode) -> bool {
        left.used_bytes() + right.used_bytes() - LEAF_HEADER_SIZE <= PAGE_SIZE_LEAF as usize
    }

    fn choose_leaf_redistribution_split(
        cells: &[LeafCell],
        original_left_len: usize,
    ) -> Option<usize> {
        let mut best: Option<((usize, usize, usize), usize)> = None;

        for split_at in 1..cells.len() {
            let left_used = LeafNode::used_bytes_for_cells(&cells[..split_at]);
            let right_used = LeafNode::used_bytes_for_cells(&cells[split_at..]);
            if left_used > PAGE_SIZE_LEAF as usize || right_used > PAGE_SIZE_LEAF as usize {
                continue;
            }

            let deficit = MIN_LEAF_BYTES.saturating_sub(left_used)
                + MIN_LEAF_BYTES.saturating_sub(right_used);
            let imbalance = left_used.abs_diff(right_used);
            let movement = split_at.abs_diff(original_left_len);
            let score = (deficit, imbalance, movement);

            match &best {
                Some((best_score, _)) if *best_score <= score => {}
                _ => best = Some((score, split_at)),
            }
        }

        best.map(|(_, split_at)| split_at)
    }

    fn move_all_leaf_chains(&mut self, from_page: u32, to_page: u32) -> Result<()> {
        for (key, chain) in self.store.take_all_chains(from_page)? {
            self.store.put_chain(to_page, key, chain)?;
        }
        Ok(())
    }

    fn redistribute_leaf_chains(
        &mut self,
        left_page: u32,
        right_page: u32,
        separator_key: &[u8],
    ) -> Result<()> {
        let mut chains = self.store.take_all_chains(left_page)?;
        chains.extend(self.store.take_all_chains(right_page)?);

        for (key, chain) in chains {
            if key.as_slice() < separator_key {
                self.store.put_chain(left_page, key, chain)?;
            } else {
                self.store.put_chain(right_page, key, chain)?;
            }
        }

        Ok(())
    }

    fn redistribute_leaf_pair(
        &mut self,
        left_page: u32,
        mut left_node: LeafNode,
        right_page: u32,
        mut right_node: LeafNode,
        parent_page: u32,
        separator_idx: usize,
    ) -> Result<()> {
        let original_left_len = left_node.cells.len();
        let mut combined = left_node.cells;
        combined.extend(right_node.cells);

        let split_at = Self::choose_leaf_redistribution_split(&combined, original_left_len)
            .ok_or_else(|| Error::Internal("leaf redistribution could not find a valid split".into()))?;

        let right_cells = combined.split_off(split_at);
        let separator_key = right_cells[0].key.clone();

        left_node.cells = combined;
        left_node.next_leaf_page = right_page;
        right_node.cells = right_cells;
        right_node.prev_leaf_page = left_page;

        self.redistribute_leaf_chains(left_page, right_page, &separator_key)?;

        let left_enc = left_node.encode()?;
        let right_enc = right_node.encode()?;
        let parent_buf = self.store.read_internal(parent_page)?;
        let mut parent = InternalNode::parse(&parent_buf[..])?;
        parent.entries[separator_idx].0 = separator_key;
        let parent_enc = parent.encode()?;

        self.store.write_leaf(left_page, &left_enc)?;
        self.store.write_leaf(right_page, &right_enc)?;
        self.store.write_internal(parent_page, &parent_enc)?;
        Ok(())
    }

    fn merge_leaf_into_left(
        &mut self,
        parent_page: u32,
        child_idx: usize,
        path: &[(u32, usize)],
        left_page: u32,
        mut left_node: LeafNode,
        page: u32,
        node: LeafNode,
    ) -> Result<()> {
        self.move_all_leaf_chains(page, left_page)?;

        left_node.cells.extend(node.cells);
        left_node.next_leaf_page = node.next_leaf_page;

        if node.next_leaf_page != 0 {
            let (next_buf, _) = self.store.read_leaf(node.next_leaf_page)?;
            let mut next_node = LeafNode::parse(&next_buf[..])?;
            next_node.prev_leaf_page = left_page;
            let enc = next_node.encode()?;
            self.store.write_leaf(node.next_leaf_page, &enc)?;
        }

        let left_enc = left_node.encode()?;
        self.store.write_leaf(left_page, &left_enc)?;
        if !self.store.chains_empty(page)? {
            return Err(Error::Internal(
                "free_leaf called with non-empty version chain".into(),
            ));
        }
        self.store.free_leaf(page)?;

        self.redirect_parent_child_pointer(parent_page, child_idx, left_page)?;
        self.remove_from_parent(parent_page, child_idx - 1, path)
    }

    fn merge_leaf_into_right(
        &mut self,
        parent_page: u32,
        path: &[(u32, usize)],
        page: u32,
        node: LeafNode,
        right_page: u32,
        mut right_node: LeafNode,
    ) -> Result<()> {
        self.move_all_leaf_chains(page, right_page)?;

        let mut merged_cells = node.cells;
        merged_cells.extend(right_node.cells);
        right_node.cells = merged_cells;
        right_node.prev_leaf_page = node.prev_leaf_page;

        if node.prev_leaf_page != 0 {
            let (prev_buf, _) = self.store.read_leaf(node.prev_leaf_page)?;
            let mut prev_node = LeafNode::parse(&prev_buf[..])?;
            prev_node.next_leaf_page = right_page;
            let enc = prev_node.encode()?;
            self.store.write_leaf(node.prev_leaf_page, &enc)?;
        }

        let right_enc = right_node.encode()?;
        self.store.write_leaf(right_page, &right_enc)?;
        if !self.store.chains_empty(page)? {
            return Err(Error::Internal(
                "free_leaf called with non-empty version chain".into(),
            ));
        }
        self.store.free_leaf(page)?;
        self.remove_from_parent(parent_page, 0, path)
    }

    /// Redirect the child pointer at `child_idx` in the internal node at
    /// `parent_page` to `new_child`, leaving every other field unchanged.
    ///
    /// Used by the left-sibling merge path to ensure the parent no longer
    /// references the just-freed leaf after the subsequent separator removal
    /// shifts slots down.
    fn redirect_parent_child_pointer(
        &mut self,
        parent_page: u32,
        child_idx: usize,
        new_child: u32,
    ) -> Result<()> {
        let buf = self.store.read_internal(parent_page)?;
        let mut parent = InternalNode::parse(&buf[..])?;
        if child_idx < parent.entries.len() {
            parent.entries[child_idx].1 = new_child;
        } else {
            parent.rightmost_child = new_child;
        }
        let enc = parent.encode()?;
        self.store.write_internal(parent_page, &enc)?;
        Ok(())
    }

    /// Remove the separator key at `separator_idx` from the internal node at
    /// `parent_page`.
    ///
    /// If that internal node is the tree root and becomes empty, collapse the
    /// root to its remaining child. Otherwise write the updated node back in
    /// place; internal-node underflow propagation is still intentionally
    /// deferred in this implementation.
    fn remove_from_parent(
        &mut self,
        parent_page: u32,
        separator_idx: usize,
        _path: &[(u32, usize)],
    ) -> Result<()> {
        let buf = self.store.read_internal(parent_page)?;
        let mut parent = InternalNode::parse(&buf[..])?;

        parent.entries.remove(separator_idx);

        if parent.entries.is_empty() {
            // The root has no more separator keys.  If it's the actual tree root,
            // we make rightmost_child the new root.
            if parent_page == self.root_page {
                self.root_page = parent.rightmost_child;
                if self.root_level > 0 {
                    self.root_level -= 1;
                }
                self.store.free_internal(parent_page)?;
                return Ok(());
            }
            // Not the root: need to propagate underflow upward.
            // For Phase 1, we accept an underfull internal node (just write it back).
            // A more complete implementation would merge internal nodes too.
            let enc = parent.encode()?;
            self.store.write_internal(parent_page, &enc)?;
        } else {
            let enc = parent.encode()?;
            self.store.write_internal(parent_page, &enc)?;
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Range scan
    // -----------------------------------------------------------------------

    /// Collect all `(key, value)` pairs in the range `[start_key, end_key]`.
    ///
    /// Both bounds are optional (use `None` for an unbounded side).  Keys are
    /// returned in ascending order following leaf sibling pointers.
    ///
    /// Overflow values are **not** resolved here; the caller receives
    /// [`CellValue::Overflow`] pointers and can call [`BTree::read_overflow`]
    /// to fetch the data.
    pub(crate) fn range_scan(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, CellValue)>> {
        let mut results = Vec::new();

        // Find the first leaf that might contain start_key.
        let first_leaf = match start_key {
            Some(k) => self.find_leaf(k)?,
            None => self.leftmost_leaf()?,
        };

        let mut cur_page = first_leaf;
        'outer: while cur_page != 0 {
            let (buf, _) = self.store.read_leaf(cur_page)?;
            let node = LeafNode::parse(&buf[..])?;

            let start_idx = match start_key {
                Some(k) => match node.binary_search(k) {
                    Ok(i) => i,
                    Err(i) => i,
                },
                None => 0,
            };

            for i in start_idx..node.cells.len() {
                let cell = &node.cells[i];
                if let Some(ek) = end_key {
                    if cell.key.as_slice() > ek {
                        break 'outer;
                    }
                }
                results.push((cell.key.clone(), cell.value.clone()));
            }

            cur_page = node.next_leaf_page;
        }

        Ok(results)
    }

    /// MVCC-aware range scan (T5' sub-step 3).
    ///
    /// Walks sibling leaves like [`BTree::range_scan`], but for each
    /// candidate cell consults the frame's `ChainSnapshot` via
    /// [`ChainSnapshot::visible_at`]: a visible [`VersionEntry`] wins
    /// (returning its resolved inline/overflow bytes, or skipping on
    /// tombstone); otherwise the on-disk cell value is yielded.
    ///
    /// Unlike the legacy `range_scan` which hands back `CellValue`
    /// placeholders for overflow payloads, this path fully resolves every
    /// row to `Vec<u8>` so chain-sourced and cell-sourced values share one
    /// shape at the call site. Keys are returned in ascending order.
    pub(crate) fn range_scan_mvcc(
        &self,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        view: &ReadView,
        history: Option<&dyn HistoryProbe>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut results: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        let first_leaf = match start_key {
            Some(k) => self.find_leaf(k)?,
            None => self.leftmost_leaf()?,
        };

        let mut cur_page = first_leaf;
        'outer: while cur_page != 0 {
            let (buf, snap) = self.store.read_leaf(cur_page)?;
            let node = LeafNode::parse(&buf[..])?;

            let start_idx = match start_key {
                Some(k) => match node.binary_search(k) {
                    Ok(i) => i,
                    Err(i) => i,
                },
                None => 0,
            };

            for i in start_idx..node.cells.len() {
                let cell = &node.cells[i];
                if let Some(ek) = end_key {
                    if cell.key.as_slice() > ek {
                        break 'outer;
                    }
                }

                // Chain-first: a visible VersionEntry wins over the on-disk
                // cell. If the entry is a tombstone, skip the key entirely.
                let chain_hit = snap
                    .as_ref()
                    .and_then(|s| s.visible_at(&cell.key, view));
                if let Some(entry) = chain_hit {
                    if entry.is_tombstone {
                        continue;
                    }
                    let bytes = match &entry.data {
                        VersionData::Inline(v) => v.clone(),
                        VersionData::Overflow(oref) => read_overflow_chain(
                            &self.store,
                            oref.first_page(),
                            oref.total_length() as u32,
                        )?,
                    };
                    results.push((cell.key.clone(), bytes));
                    continue;
                }

                // Plan §T7: history fallthrough before falling back to the
                // on-disk cell. A visible evicted entry in the history store
                // is preferred over the cell (which reflects the latest
                // committed baseline, not necessarily visible at `read_ts`).
                if let Some(probe) = history {
                    if let Some(entry) = probe.probe(&cell.key, view.read_ts)? {
                        if entry.is_tombstone {
                            continue;
                        }
                        let bytes = match &entry.data {
                            VersionData::Inline(v) => v.clone(),
                            VersionData::Overflow(oref) => read_overflow_chain(
                                &self.store,
                                oref.first_page(),
                                oref.total_length() as u32,
                            )?,
                        };
                        results.push((cell.key.clone(), bytes));
                        continue;
                    }
                }

                // Fall back to the on-disk cell.
                let bytes = match &cell.value {
                    CellValue::Inline(v) => v.clone(),
                    CellValue::Overflow {
                        first_page,
                        total_length,
                    } => read_overflow_chain(&self.store, *first_page, *total_length)?,
                };
                results.push((cell.key.clone(), bytes));
            }

            cur_page = node.next_leaf_page;
        }

        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /// Traverse from the root to the leaf page that should contain `key`.
    pub(crate) fn find_leaf(&self, key: &[u8]) -> Result<u32> {
        let mut page = self.root_page;
        let mut level = self.root_level;

        while level > 0 {
            let buf = self.store.read_internal(page)?;
            let node = InternalNode::parse(&buf[..])?;
            page = node.find_child(key);
            level -= 1;
        }

        Ok(page)
    }

    /// Follow leftmost child pointers from the root to reach the
    /// leftmost leaf page.
    fn leftmost_leaf(&self) -> Result<u32> {
        let mut page = self.root_page;
        let mut level = self.root_level;

        while level > 0 {
            let buf = self.store.read_internal(page)?;
            let node = InternalNode::parse(&buf[..])?;
            // Follow leftmost child.
            page = if node.entries.is_empty() {
                node.rightmost_child
            } else {
                node.entries[0].1
            };
            level -= 1;
        }

        Ok(page)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../btree_tests.rs"]
mod tests;
