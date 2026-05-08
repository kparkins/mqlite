//! Structural page-byte batch owners.
//!
//! Structural writers open `BTree<StructuralBatchStore>` via
//! [`StructuralBatchStore::new`] where the underlying pages live in a
//! [`BufferPoolPageStore`]. Page-byte writes made by the structural batch are
//! staged by page size and page number; reads consult staged bytes first and
//! fall back to the shared `base` store.
//!
//! On commit, staged bytes are copied onto the real shared buffer-pool frames
//! via [`BufferPoolPageStore::write_internal`] / `write_leaf_structural`. On
//! rollback, staged bytes are dropped and the shared frames are left untouched.
//!
//! - Page-byte reads and writes are structural-batch routed.
//! - Allocator calls (`alloc_internal` / `alloc_leaf` / `free_*`) route
//!   through [`AllocatorLifetimeBatch`] so rollback can undo allocations.
//! - MVCC delta-chain helpers (`take_chain` / `put_chain` /
//!   `chains_empty`) go straight through to the shared frame. They are
//!   already CoW-safe via `Arc::make_mut`: concurrent readers holding an
//!   old `Arc<VecDeque<VersionEntry>>` observe their frozen copy while the
//!   writer mutates a fresh one. Staging the chain mutation separately
//!   would create a window between `take_chain` and `put_chain` in which
//!   the shared frame is missing the chain without the structural batch
//!   being consulted; the existing CoW dance already gives us the isolation
//!   we need.
//!
//! Concurrency: a `StructuralBatchStore` and its backing
//! [`StructuralPageWrites`] are single-threaded per structural batch. The
//! staged page bytes are owned exclusively by the write path for the duration
//! of the batch and passed to helpers by `&mut` reference. Structural page
//! commits remain scoped to DDL/checkpoint paths; ordinary CRUD publishes
//! resident MVCC entries through page-local chain mutation. No `Arc`,
//! `Mutex`, or `RefCell` wrapping is needed.
//!
//! Header/catalog-root mutation is owned by [`HeaderCatalogRootBatch`]. It
//! captures the live allocator header at mutation time and selectively
//! restores catalog-root fields on pre-durable abort. Allocator/lifetime
//! mutation is owned by [`AllocatorLifetimeBatch`].

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::read_view::ChainSnapshot;
use crate::mvcc::version::VersionEntry;
use crate::storage::btree::{
    empty_internal_page_bytes, empty_leaf_page_bytes, BTreePageStore, LeafPageImage,
};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::PageSize;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF, PAGE_TYPE_OVERFLOW};

const INTERNAL_SIZE: usize = PAGE_SIZE_INTERNAL as usize;
const LEAF_SIZE: usize = PAGE_SIZE_LEAF as usize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CatalogRootHeaderState {
    root_page: u32,
    root_backup: u32,
    root_level: u8,
}

impl CatalogRootHeaderState {
    fn from_header(header: &FileHeader) -> Self {
        Self {
            root_page: header.catalog_root_page,
            root_backup: header.catalog_root_backup,
            root_level: header.catalog_root_level,
        }
    }

    fn restore_into(self, header: &mut FileHeader) {
        header.catalog_root_page = self.root_page;
        header.catalog_root_backup = self.root_backup;
        header.catalog_root_level = self.root_level;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct HeaderChange {
    before: CatalogRootHeaderState,
    after: CatalogRootHeaderState,
}

impl HeaderChange {
    fn rollback_into(self, header: &mut FileHeader) {
        if CatalogRootHeaderState::from_header(header) == self.after {
            self.before.restore_into(header);
        }
    }
}

/// Final owner for catalog-root header mutation inside structural batches.
///
/// The owner reads the live allocator header immediately before applying the
/// caller's mutation, avoiding stale page-0 snapshots. Abort restores only the
/// catalog-root fields and only when no later header owner has advanced them.
#[derive(Default)]
pub(crate) struct HeaderCatalogRootBatch {
    change: Option<HeaderChange>,
}

impl HeaderCatalogRootBatch {
    fn new() -> Self {
        Self::default()
    }

    /// Mutate the allocator-owned file header and capture rollback metadata.
    pub(crate) fn update_header<F>(&mut self, handle: &BufferPoolHandle, f: F) -> Result<()>
    where
        F: FnOnce(&mut FileHeader),
    {
        handle.allocator().update_header(|header| {
            let before = CatalogRootHeaderState::from_header(header);
            f(header);
            let after = CatalogRootHeaderState::from_header(header);
            self.capture_change_once(before, after);
        })
    }

    fn capture_change_once(
        &mut self,
        before: CatalogRootHeaderState,
        after: CatalogRootHeaderState,
    ) {
        if before != after && self.change.is_none() {
            self.change = Some(HeaderChange { before, after });
        }
    }

    fn abort(self, handle: &BufferPoolHandle) -> Result<()> {
        if let Some(change) = self.change {
            handle
                .allocator()
                .update_header(|header| change.rollback_into(header))?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Allocator/lifetime owner
// ---------------------------------------------------------------------------

/// Page allocated by a structural batch and still owned by that batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BatchAllocatedPage {
    page: u32,
    size: PageSize,
}

/// Final owner for allocator and page-lifetime work inside structural batches.
///
/// New page allocations are returned to the allocator and invalidated on abort.
/// Overflow deferred-free pages drained from [`PageLifetimeQueue`] are freed on
/// commit and requeued on abort so refcount/lifetime fencing stays distinct
/// from retired-tree work.
#[derive(Default)]
pub(crate) struct AllocatorLifetimeBatch {
    new_allocs: Vec<BatchAllocatedPage>,
    deferred_free_pages: Vec<u32>,
}

impl AllocatorLifetimeBatch {
    fn new(handle: &BufferPoolHandle) -> Self {
        Self {
            new_allocs: Vec::new(),
            deferred_free_pages: handle.allocator().drain_deferred_free_pages(),
        }
    }

    fn record_new_alloc(&mut self, page: u32, size: PageSize) {
        self.new_allocs.push(BatchAllocatedPage { page, size });
    }

    fn forget_new_alloc(&mut self, page: u32, size: PageSize) {
        self.new_allocs
            .retain(|allocated| allocated.page != page || allocated.size != size);
    }

    fn commit_lsn_fenced(self, handle: &BufferPoolHandle, last_lsn: u64) -> Result<()> {
        for page in self.deferred_free_pages {
            handle.free_page(page, PageSize::Large32k)?;
            handle.stamp_unflushable_dirty_pages_lsn(last_lsn)?;
        }
        Ok(())
    }

    fn abort(self, handle: &BufferPoolHandle) -> Result<()> {
        for allocated in self.new_allocs {
            handle.free_page(allocated.page, allocated.size)?;
            handle
                .pool()
                .invalidate_page(allocated.page, allocated.size)?;
        }
        for page in self.deferred_free_pages {
            handle.allocator().enqueue_overflow_deferred_free(page);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// StructuralPageWrites — structural staged page bytes
// ---------------------------------------------------------------------------

/// Page-byte staging shared across every `StructuralBatchStore` instance
/// opened during a single structural batch.
///
/// The `touched_*` vectors preserve insertion order so commit applies bytes
/// in a deterministic sequence (matches the order writes were produced).
#[derive(Default)]
pub(crate) struct StructuralPageWrites {
    /// 4K internal pages staged by the writer.
    staged_4k: HashMap<u32, Box<[u8; INTERNAL_SIZE]>>,
    /// 32K leaf pages staged by the writer.
    staged_32k: HashMap<u32, Box<[u8; LEAF_SIZE]>>,
    /// Page numbers in staged_4k in the order they were first written.
    touched_4k: Vec<u32>,
    /// Page numbers in staged_32k in the order they were first written.
    touched_32k: Vec<u32>,
}

impl StructuralPageWrites {
    /// Create a fresh, empty page-byte staging area.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn page_images(&self) -> Vec<StructuralPageImage> {
        let mut images = Vec::with_capacity(self.touched_4k.len() + self.touched_32k.len());
        for page_number in &self.touched_4k {
            if let Some(bytes) = self.staged_4k.get(page_number) {
                images.push(StructuralPageImage {
                    page_number: *page_number,
                    page_size: PageSize::Small4k,
                    data: bytes.to_vec(),
                });
            }
        }
        for page_number in &self.touched_32k {
            if let Some(bytes) = self.staged_32k.get(page_number) {
                images.push(StructuralPageImage {
                    page_number: *page_number,
                    page_size: PageSize::Large32k,
                    data: bytes.to_vec(),
                });
            }
        }
        images
    }

    pub(crate) fn commit_lsn_fenced(
        self,
        base: &mut BufferPoolPageStore,
        handle: &BufferPoolHandle,
        last_lsn: u64,
    ) -> Result<()> {
        self.commit_inner(base, handle, last_lsn)
    }

    fn commit_inner(
        mut self,
        base: &mut BufferPoolPageStore,
        handle: &BufferPoolHandle,
        last_lsn: u64,
    ) -> Result<()> {
        // Internal first, then leaf. Within each size, honor insertion
        // order to keep replay deterministic.
        let touched_4k = std::mem::take(&mut self.touched_4k);
        for page in touched_4k {
            if let Some(buf) = self.staged_4k.remove(&page) {
                base.write_internal(page, &buf)?;
                handle.stamp_dirty_pages_lsn(&[page], last_lsn)?;
            }
        }
        let touched_32k = std::mem::take(&mut self.touched_32k);
        for page in touched_32k {
            if let Some(buf) = self.staged_32k.remove(&page) {
                #[cfg(any(test, feature = "test-hooks"))]
                crate::storage::structural_batch_observations::record_committed_structural_leaf_bytes(
                    buf.len(),
                );
                base.write_leaf_structural(page, &buf)?;
                handle.stamp_dirty_pages_lsn(&[page], last_lsn)?;
            }
        }
        Ok(())
    }
}

/// Page image staged by a structural catalog/DDL batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StructuralPageImage {
    /// Page number in the main file.
    pub(crate) page_number: u32,
    /// Physical page size for `data`.
    pub(crate) page_size: PageSize,
    /// Full page bytes staged by the structural batch.
    pub(crate) data: Vec<u8>,
}

/// Final owner for structural page-byte batches.
///
/// Bootstrap, DDL, index build/drop, checkpoint materialization, and other
/// tree-shape changes stage their page bytes here before the durable DDL
/// boundary. Abort routes through this owner, not ordinary CRUD rollback.
pub(crate) struct StructuralPageBatch {
    writes: StructuralPageWrites,
    lifetime: AllocatorLifetimeBatch,
    header: HeaderCatalogRootBatch,
}

impl StructuralPageBatch {
    /// Create a structural batch and reserve any deferred-free pages for it.
    pub(crate) fn new(handle: &BufferPoolHandle) -> Self {
        Self {
            writes: StructuralPageWrites::new(),
            lifetime: AllocatorLifetimeBatch::new(handle),
            header: HeaderCatalogRootBatch::new(),
        }
    }

    /// Open a transaction-local page store for structural B-tree writes.
    pub(crate) fn store(&mut self, base: BufferPoolPageStore) -> StructuralBatchStore<'_> {
        StructuralBatchStore::new(base, &mut self.writes, &mut self.lifetime)
    }

    /// Return the structural page images that a typed catalog log record must
    /// carry for recovery replay.
    pub(crate) fn page_images(&self) -> Vec<StructuralPageImage> {
        self.writes.page_images()
    }

    /// Apply staged page bytes with an explicit page-LSN fence.
    pub(crate) fn commit_lsn_fenced(
        self,
        base: &mut BufferPoolPageStore,
        handle: &BufferPoolHandle,
        last_lsn: u64,
    ) -> Result<()> {
        self.writes.commit_lsn_fenced(base, handle, last_lsn)?;
        self.lifetime.commit_lsn_fenced(handle, last_lsn)
    }

    /// Abort staged structural pages and restore allocator/header state.
    pub(crate) fn abort(self, handle: &BufferPoolHandle) -> Result<()> {
        drop(self.writes);
        self.lifetime.abort(handle)?;
        self.header.abort(handle)
    }

    /// Mutate catalog-root header fields through the final header owner.
    pub(crate) fn update_header<F>(&mut self, handle: &BufferPoolHandle, f: F) -> Result<()>
    where
        F: FnOnce(&mut FileHeader),
    {
        self.header.update_header(handle, f)
    }
}

// ---------------------------------------------------------------------------
// StructuralBatchStore — BTreePageStore adapter for structural staged bytes
// ---------------------------------------------------------------------------

/// `BTreePageStore` that routes writes into a [`StructuralPageWrites`] and
/// falls back to `base` for any page not present in the structural batch.
///
/// Each writer helper that opens a BTree constructs a `StructuralBatchStore`
/// borrowing the same `&mut StructuralPageWrites` so every helper sees every
/// prior write within the transaction.
pub(crate) struct StructuralBatchStore<'a> {
    base: BufferPoolPageStore,
    writes: &'a mut StructuralPageWrites,
    lifetime: &'a mut AllocatorLifetimeBatch,
}

impl<'a> StructuralBatchStore<'a> {
    pub(crate) fn new(
        base: BufferPoolPageStore,
        writes: &'a mut StructuralPageWrites,
        lifetime: &'a mut AllocatorLifetimeBatch,
    ) -> Self {
        Self {
            base,
            writes,
            lifetime,
        }
    }
}

impl<'a> BTreePageStore for StructuralBatchStore<'a> {
    type SharedReadGuard<'g>
        = ()
    where
        Self: 'g;

    // ---- Reads: staged bytes first, fall back to base.

    fn read_internal(&self, page: u32) -> Result<Box<[u8; INTERNAL_SIZE]>> {
        if let Some(buf) = self.writes.staged_4k.get(&page) {
            return Ok(buf.clone());
        }
        self.base.read_internal(page)
    }

    fn read_leaf(&self, page: u32) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        if let Some(buf) = self.writes.staged_32k.get(&page).cloned() {
            // The chain snapshot lives on the shared frame; structural
            // page-byte staging does not duplicate chains. Pin-and-snapshot
            // against the real frame so MVCC visibility logic keeps working
            // for in-flight reads by this writer.
            let (_disk, snap) = self.base.read_leaf(page)?;
            return Ok((LeafPageImage::owned(buf), snap));
        }
        self.base.read_leaf(page)
    }

    fn pin_shared_for_read<'g>(
        &'g self,
        _page: u32,
        _size: PageSize,
    ) -> Result<Self::SharedReadGuard<'g>> {
        Ok(())
    }

    // ---- Writes: stage bytes. NEVER touch the shared frame.

    fn write_internal(&mut self, page: u32, data: &[u8; INTERNAL_SIZE]) -> Result<()> {
        let existed = self.writes.staged_4k.contains_key(&page);
        self.writes.staged_4k.insert(page, Box::new(*data));
        if !existed {
            self.writes.touched_4k.push(page);
        }
        Ok(())
    }

    fn write_leaf_structural(&mut self, page: u32, data: &[u8; LEAF_SIZE]) -> Result<()> {
        let existed = self.writes.staged_32k.contains_key(&page);
        self.writes.staged_32k.insert(page, Box::new(*data));
        if !existed {
            self.writes.touched_32k.push(page);
        }
        if data[0] == PAGE_TYPE_OVERFLOW {
            self.base
                .handle()
                .pool()
                .invalidate_page(page, PageSize::Large32k)?;
        }
        Ok(())
    }

    // ---- Allocation / free
    //
    // Every allocation made inside a structural batch is recorded in the
    // allocator/lifetime owner so abort can return the page to the free
    // list and invalidate the cached frame. Free stays straight-through,
    // with staged-byte eviction below to prevent alloc-then-free within the
    // same batch from replaying page bytes onto the freed slot at commit.

    fn alloc_internal(&mut self) -> Result<u32> {
        let page = self.base.alloc_internal()?;
        // Seed staged bytes with a valid empty-internal image.
        // A fresh allocation leaves the shared buffer-pool frame
        // zero-filled (or carrying stale bytes from a previous
        // occupant). Any subsequent in-txn read via `read_internal` must
        // see a valid page header — not zeros and not the previous
        // occupant. Commit replays the final staged bytes onto the
        // shared frame before the txn becomes visible, so later
        // in-txn writes to the same page simply overwrite the seed.
        let seed = empty_internal_page_bytes()?;
        self.writes.staged_4k.insert(page, Box::new(seed));
        self.writes.touched_4k.push(page);
        self.lifetime.record_new_alloc(page, PageSize::Small4k);
        self.base
            .handle()
            .pool()
            .invalidate_page(page, PageSize::Small4k)?;
        Ok(page)
    }

    fn alloc_leaf(&mut self) -> Result<u32> {
        let page = self.base.alloc_leaf()?;
        // Seed staged bytes with a valid empty-leaf image.
        // Same rationale as `alloc_internal` above. Overflow pages also
        // come through `alloc_leaf` because they share the 32 KB slot
        // size; that is safe because `write_overflow_chain` writes a
        // full page (zero-initialized buffer + header + payload) before
        // any read decodes the page as an overflow frame, so the
        // empty-leaf seed is always fully overwritten before it can be
        // misinterpreted.
        let seed = empty_leaf_page_bytes()?;
        self.writes.staged_32k.insert(page, Box::new(seed));
        self.writes.touched_32k.push(page);
        self.lifetime.record_new_alloc(page, PageSize::Large32k);
        Ok(page)
    }

    fn free_internal(&mut self, page: u32) -> Result<()> {
        // Also evict any staged bytes: if the same batch allocated,
        // wrote, and then freed the page, we must not replay its bytes
        // onto the freed slot at commit time.
        //
        // If the freed page was allocated in this batch, drop the allocation
        // record too. Otherwise rollback would double-free the page because
        // the in-batch `free_*` already returned it to the allocator free list.
        if self.writes.staged_4k.remove(&page).is_some() {
            self.writes.touched_4k.retain(|p| *p != page);
        }
        self.lifetime.forget_new_alloc(page, PageSize::Small4k);
        self.base.free_internal(page)
    }

    fn free_leaf(&mut self, page: u32) -> Result<()> {
        if self.writes.staged_32k.remove(&page).is_some() {
            self.writes.touched_32k.retain(|p| *p != page);
        }
        self.lifetime.forget_new_alloc(page, PageSize::Large32k);
        self.base.free_leaf(page)
    }

    // ---- MVCC chain helpers: delegate to base.
    //
    // `Arc::make_mut` CoW in the buffer pool already isolates
    // concurrent reader `ChainSnapshot` holders from the writer's
    // mutation. Staging these separately would open a window where the
    // chain is missing from the shared frame without staged structural
    // bytes carrying a replacement — that is worse, not better, for
    // correctness. See the module docstring.

    fn take_chain(&mut self, page: u32, key: &[u8]) -> Result<Option<Arc<VecDeque<VersionEntry>>>> {
        self.base.take_chain(page, key)
    }

    fn put_chain(
        &mut self,
        page: u32,
        key: Vec<u8>,
        chain: Arc<VecDeque<VersionEntry>>,
    ) -> Result<()> {
        self.base.put_chain(page, key, chain)
    }

    fn chains_empty(&self, page: u32) -> Result<bool> {
        self.base.chains_empty(page)
    }

    fn clear_chains(&mut self, page: u32) -> Result<()> {
        self.base.clear_chains(page)
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
        self.base.take_all_chains_on_page(page)
    }
}

#[cfg(test)]
#[path = "tests/structural_page_batch.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/structural_page_batch_store.rs"]
mod structural_page_batch_store;
