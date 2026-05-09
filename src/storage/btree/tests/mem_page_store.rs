//! In-memory [`BTreePageStore`] used by unit tests.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::read_view::ChainSnapshot;
use crate::mvcc::version::VersionEntry;
use crate::storage::buffer_pool::PageSize;
use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

use super::{BTreePageStore, LeafPageImage};

/// A simple in-memory [`BTreePageStore`] backed by maps.
///
/// Reads of pages that were never written return zero-filled buffers.
/// Designed for unit tests; not intended for production use.
pub(crate) struct MemPageStore {
    internal_pages: HashMap<u32, Box<[u8; PAGE_SIZE_INTERNAL as usize]>>,
    leaf_pages: HashMap<u32, Box<[u8; PAGE_SIZE_LEAF as usize]>>,
    /// Per-leaf-page MVCC delta chains. Outer key is page number, inner key
    /// is the B+ tree cell key.
    pub(crate) leaf_chains: HashMap<u32, BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>>,
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
        let page = self.next_page;
        self.next_page += 1;
        Ok(page)
    }

    fn alloc_leaf(&mut self) -> Result<u32> {
        let page = self.next_page;
        self.next_page += 1;
        Ok(page)
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
        Ok(self
            .leaf_chains
            .remove(&page)
            .map(|m| m.into_iter().collect())
            .unwrap_or_default())
    }
}
