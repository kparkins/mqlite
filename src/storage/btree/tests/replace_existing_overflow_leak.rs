//! BUG-10 repro: `replace_existing` (insert.rs) leaks overflow chains two
//! ways:
//!
//! 1. The new value's overflow chain is written *before* the key-existence
//!    binary search, so the miss arm (`return Ok(false)`) orphans the
//!    just-written chain.
//! 2. The hit arm overwrites `node.cells[idx].value` without freeing the OLD
//!    value's overflow chain (contrast `delete_from_leaf`, which calls
//!    `free_overflow_chain`).
//!
//! These chains are written with refcount 0 ("unmanaged", insert.rs) and
//! nothing sweeps orphans, so each fold in the checkpoint path permanently
//! leaks pages.
//!
//! The tests wrap [`MemPageStore`] in a `TrackingStore` that records which
//! 32 KB pages are currently allocated (alloc_leaf minus free_leaf) so leaked
//! overflow pages are observable.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;

use super::*;
use crate::error::Result;
use crate::mvcc::chain_snapshot::ChainSnapshot;
use crate::mvcc::version::VersionEntry;
use crate::storage::buffer_pool::{LatchMode, PageSize};
use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

/// Value size > `OVERFLOW_THRESHOLD`, spanning exactly two overflow pages
/// (`OVERFLOW_PAGE_DATA` = 32 748 bytes per page).
const BIG_VALUE_LEN: usize = 40_000;
const CHAIN_PAGES: usize = 2;

// ---------------------------------------------------------------------------
// TrackingStore: MemPageStore + live 32KB-page accounting
// ---------------------------------------------------------------------------

struct TrackingStore {
    inner: MemPageStore,
    live_leaf_pages: HashSet<u32>,
}

impl TrackingStore {
    fn new() -> Self {
        Self {
            inner: MemPageStore::new(),
            live_leaf_pages: HashSet::new(),
        }
    }

    fn live_count(&self) -> usize {
        self.live_leaf_pages.len()
    }
}

impl BTreePageStore for TrackingStore {
    type SharedReadGuard<'a>
        = ()
    where
        Self: 'a;

    fn read_internal(&self, page: u32) -> Result<Box<[u8; PAGE_SIZE_INTERNAL as usize]>> {
        self.inner.read_internal(page)
    }

    fn read_leaf(&self, page: u32) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        self.inner.read_leaf(page)
    }

    fn pin_shared_for_read<'a>(
        &'a self,
        page: u32,
        size: PageSize,
    ) -> Result<Self::SharedReadGuard<'a>> {
        self.inner.pin_shared_for_read(page, size)
    }

    fn write_internal(
        &mut self,
        page: u32,
        data: &[u8; PAGE_SIZE_INTERNAL as usize],
    ) -> Result<()> {
        self.inner.write_internal(page, data)
    }

    fn write_leaf_structural(
        &mut self,
        page: u32,
        data: &[u8; PAGE_SIZE_LEAF as usize],
    ) -> Result<()> {
        self.inner.write_leaf_structural(page, data)
    }

    fn alloc_internal(&mut self) -> Result<u32> {
        self.inner.alloc_internal()
    }

    fn alloc_leaf(&mut self) -> Result<u32> {
        let page = self.inner.alloc_leaf()?;
        self.live_leaf_pages.insert(page);
        Ok(page)
    }

    fn free_internal(&mut self, page: u32) -> Result<()> {
        self.inner.free_internal(page)
    }

    fn free_leaf(&mut self, page: u32) -> Result<()> {
        self.inner.free_leaf(page)?;
        self.live_leaf_pages.remove(&page);
        Ok(())
    }

    fn chains_empty(&self, page: u32) -> Result<bool> {
        self.inner.chains_empty(page)
    }

    fn with_chain_under_latch<R, F>(
        &mut self,
        page: u32,
        key: &[u8],
        mode: LatchMode,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut Option<Arc<VecDeque<VersionEntry>>>) -> R,
    {
        self.inner.with_chain_under_latch(page, key, mode, f)
    }

    fn with_all_chains_under_latch<R, F>(&mut self, page: u32, mode: LatchMode, f: F) -> Result<R>
    where
        F: FnOnce(&mut BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>) -> R,
    {
        self.inner.with_all_chains_under_latch(page, mode, f)
    }
}

// ---------------------------------------------------------------------------
// Harness sanity: tracking observes chain alloc and chain free correctly.
// ---------------------------------------------------------------------------

#[test]
fn tracking_store_observes_overflow_chain_alloc_and_free() {
    let mut tree = BTree::create(TrackingStore::new()).unwrap();
    assert_eq!(tree.store.live_count(), 1, "root leaf only");

    tree.insert(b"k", &vec![0xABu8; BIG_VALUE_LEN]).unwrap();
    assert_eq!(
        tree.store.live_count(),
        1 + CHAIN_PAGES,
        "root + {CHAIN_PAGES}-page overflow chain"
    );

    // delete_from_leaf frees the chain — the accounting must drop back.
    assert!(tree.delete(b"k").unwrap());
    assert_eq!(
        tree.store.live_count(),
        1,
        "delete must free the overflow chain"
    );
}

// ---------------------------------------------------------------------------
// BUG-10 (hit arm): replacing an overflow value never frees the old chain.
// ---------------------------------------------------------------------------

#[test]
fn replace_existing_frees_old_overflow_chain() {
    let mut tree = BTree::create(TrackingStore::new()).unwrap();

    tree.insert(b"k", &vec![0x11u8; BIG_VALUE_LEN]).unwrap();
    let baseline = tree.store.live_count();
    assert_eq!(baseline, 1 + CHAIN_PAGES, "root + old overflow chain");

    let new_value = vec![0x22u8; BIG_VALUE_LEN];
    let replaced = tree.replace_existing(b"k", &new_value).unwrap();
    assert!(replaced, "key exists, replace must report a hit");
    assert_eq!(
        tree.get(b"k").unwrap().as_deref(),
        Some(new_value.as_slice()),
        "replacement value must be readable"
    );

    // The new chain replaces the old one, so the live page count must not
    // grow: the old value's overflow pages have to be freed.
    assert_eq!(
        tree.store.live_count(),
        baseline,
        "replace_existing must free the replaced value's overflow chain \
         (old chain pages leaked)"
    );
}

// ---------------------------------------------------------------------------
// BUG-10 (miss arm): a replace miss orphans the freshly written chain.
// ---------------------------------------------------------------------------

#[test]
fn replace_existing_miss_leaves_no_orphan_overflow_pages() {
    let mut tree = BTree::create(TrackingStore::new()).unwrap();

    tree.insert(b"present", b"small inline value").unwrap();
    let baseline = tree.store.live_count();
    assert_eq!(baseline, 1, "root leaf only, no overflow pages");

    let replaced = tree
        .replace_existing(b"absent", &vec![0x33u8; BIG_VALUE_LEN])
        .unwrap();
    assert!(!replaced, "key is absent, replace must report a miss");

    // A miss changes nothing in the tree, so no page may remain allocated.
    assert_eq!(
        tree.store.live_count(),
        baseline,
        "replace_existing miss must not leak the pre-written overflow chain"
    );
}
