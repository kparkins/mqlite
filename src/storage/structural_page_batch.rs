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

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::chain_snapshot::ChainSnapshot;
use crate::mvcc::version::VersionEntry;
use crate::storage::btree::{
    empty_internal_page_bytes, empty_leaf_page_bytes, BTreePageStore, LeafPageImage,
};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::LatchMode;
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
    /// Mutate the allocator-owned file header and capture rollback metadata.
    pub(crate) fn update_header<F>(&mut self, handle: &BufferPoolHandle, f: F) -> Result<()>
    where
        F: FnOnce(&mut FileHeader),
    {
        handle.allocator().update_header(|header| {
            let before = CatalogRootHeaderState::from_header(header);
            f(header);
            let after = CatalogRootHeaderState::from_header(header);
            if before != after && self.change.is_none() {
                self.change = Some(HeaderChange { before, after });
            }
        })
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
///
/// Frees of pages NOT allocated within the batch are deferred here too:
/// `AllocatorHandle::free_*` destroys the page in place (free-list link stamp
/// over the first 4 bytes, rest zeroed, frame dirtied), while the staged leaf
/// bytes that DE-reference the page only land at commit. Freeing such a page
/// straight through would corrupt the durable base image on abort, open a
/// WAL-before-data crash window on the success path, and let the allocator
/// hand the page out again mid-batch. Deferred frees are applied at
/// `commit_lsn_fenced` (after the staged bytes) and dropped on abort.
#[derive(Default)]
pub(crate) struct AllocatorLifetimeBatch {
    new_allocs: Vec<BatchAllocatedPage>,
    deferred_free_pages: Vec<u32>,
    /// Pages freed by the batch but allocated before it. Destroyed only at
    /// commit; abort drops them so the base image keeps its pages.
    deferred_frees: Vec<BatchAllocatedPage>,
}

impl AllocatorLifetimeBatch {
    fn new(handle: &BufferPoolHandle) -> Self {
        Self {
            new_allocs: Vec::new(),
            deferred_free_pages: handle.allocator().drain_deferred_free_pages(),
            deferred_frees: Vec::new(),
        }
    }

    fn record_new_alloc(&mut self, page: u32, size: PageSize) {
        self.new_allocs.push(BatchAllocatedPage { page, size });
    }

    fn forget_new_alloc(&mut self, page: u32, size: PageSize) {
        self.new_allocs
            .retain(|allocated| allocated.page != page || allocated.size != size);
    }

    /// True iff `page` was allocated by this batch (and not freed since).
    fn allocated_in_batch(&self, page: u32, size: PageSize) -> bool {
        self.new_allocs
            .iter()
            .any(|allocated| allocated.page == page && allocated.size == size)
    }

    /// Defer freeing a page the durable base image may still reference.
    fn record_deferred_free(&mut self, page: u32, size: PageSize) {
        // N32: a duplicate deferred free would destroy the page twice at
        // commit (double free-list push of the same page id).
        debug_assert!(
            !self.deferred_frees.iter().any(|freed| freed.page == page),
            "structural batch double-deferred-free of page {page}"
        );
        self.deferred_frees.push(BatchAllocatedPage { page, size });
    }

    fn commit_lsn_fenced(self, handle: &BufferPoolHandle, last_lsn: u64) -> Result<()> {
        // In-batch frees of pre-existing pages: the staged bytes that
        // dereference them were applied just before this call, so the
        // in-place destruction now respects WAL-before-data ordering.
        let any_frees = !self.deferred_frees.is_empty() || !self.deferred_free_pages.is_empty();
        for freed in self.deferred_frees {
            handle.free_page(freed.page, freed.size)?;
        }
        for page in self.deferred_free_pages {
            handle.free_page(page, PageSize::Large32k)?;
        }
        // F29: ONE pool-wide stamp covering every free above instead of one
        // per freed page (each stamp locks all four partition mutexes in
        // both pools and full-scans every frame slot, contending the hot
        // pin path during checkpoint/DDL commits). Equivalent because the
        // stamp only transitions Unflushable -> Dirty{lsn} and Unflushable
        // frames can neither flush nor be evicted in the interim, so no
        // frame freed by an earlier iteration can lose its Unflushable
        // state before the single stamp lands — it performs exactly the
        // transitions the per-free stamps would, at the same LSN.
        if any_frees {
            handle.stamp_unflushable_dirty_pages_lsn(last_lsn)?;
        }
        Ok(())
    }

    fn abort(self, handle: &BufferPoolHandle) -> Result<()> {
        // `deferred_frees` is intentionally dropped: those pages were never
        // freed, and the durable base image still references them.
        //
        // Collect the first error instead of early-returning so the
        // lifetime-queue re-enqueue below always runs — every caller uses
        // `let _ = batch.abort(..)`, and a `?` here would silently leak the
        // drained deferred-free pages out of the queue forever.
        let mut first_err = None;
        for allocated in self.new_allocs {
            let freed = handle
                .free_page(allocated.page, allocated.size)
                .and_then(|()| {
                    handle
                        .pool()
                        .invalidate_page(allocated.page, allocated.size)
                });
            if let Err(err) = freed {
                first_err.get_or_insert(err);
            }
        }
        for page in self.deferred_free_pages {
            handle.allocator().enqueue_overflow_deferred_free(page);
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
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
    /// Set when a chain was RE-HOMED through the batch store onto a shared
    /// frame (the single-key `with_chain_under_latch` placement step every
    /// structural migration ends with). The whole-map
    /// `with_all_chains_under_latch` is intentionally NOT a trigger — its only
    /// batch caller is the overflow-page clear, which discards nothing
    /// committed (see the mutator docs).
    ///
    /// Chain moves are NOT staged copy-on-write — they mutate the shared
    /// frames directly (see the module docstring). A structural split that
    /// migrated committed resident chains onto a batch-allocated page is
    /// therefore NOT undone by [`Self::abort`]: abort frees + invalidates the
    /// destination page, so the migrated chains are lost while the durable
    /// base still routes their keys to the (now chain-less) source leaf —
    /// silent data loss for committed-but-uncheckpointed versions.
    ///
    /// The CHECKPOINT materialize path is the only production path that runs a
    /// structural batch over a tree that still carries live committed chains
    /// (its rebuild folds resident deltas over the existing primary/secondary
    /// roots). A materialize abort that migrated chains must NOT be classified
    /// `Recoverable`; the checkpoint caller queries [`Self::migrated_chains`]
    /// and escalates to a poison-and-reopen so recovery rebuilds the lost
    /// chains from the journal.
    ///
    /// DDL paths (`ns_ddl` / `index_ddl`) never set this flag: they create or
    /// free WHOLE trees — `BTree::create_at` / `create` populate FRESH pages
    /// that hold no committed chains, and drop frees/clears (retire, not
    /// migrate). No DDL structural batch opens a tree carrying live committed
    /// chains, so chain migration is unreachable there and their abort safely
    /// drops staged bytes only. [`Self::abort`] `debug_assert`s this invariant
    /// for the no-escalation callers via the dropped batch's flag.
    migrated_chains: bool,
}

impl StructuralPageBatch {
    /// Create a structural batch and reserve any deferred-free pages for it.
    pub(crate) fn new(handle: &BufferPoolHandle) -> Self {
        Self {
            writes: StructuralPageWrites::default(),
            lifetime: AllocatorLifetimeBatch::new(handle),
            header: HeaderCatalogRootBatch::default(),
            migrated_chains: false,
        }
    }

    /// Open a transaction-local page store for structural B-tree writes.
    pub(crate) fn store(&mut self, base: BufferPoolPageStore) -> StructuralBatchStore<'_> {
        StructuralBatchStore::new(
            base,
            &mut self.writes,
            &mut self.lifetime,
            &mut self.migrated_chains,
        )
    }

    /// True iff a chain mutation passed through this batch's store onto the
    /// shared frames (the abort-unsafe migration described on
    /// [`StructuralPageBatch::migrated_chains`]).
    ///
    /// The CHECKPOINT path queries this BEFORE [`Self::abort`] to decide
    /// whether a materialize failure is recoverable or must poison the engine.
    pub(crate) fn migrated_chains(&self) -> bool {
        self.migrated_chains
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
    ///
    /// Callers reaching this entry must NOT have migrated resident chains
    /// through the batch store: abort frees + invalidates batch-allocated
    /// pages, so a migrated chain on such a page is lost with no staged-byte
    /// undo. DDL batches uphold this by construction (they create/free whole
    /// trees on fresh pages — see [`Self::migrated_chains`]); the `debug_assert`
    /// catches any future DDL change that splits a chain-carrying leaf through
    /// a structural batch. The checkpoint path, which CAN migrate, uses
    /// [`Self::abort_after_chain_migration`] after escalating to poison.
    pub(crate) fn abort(self, handle: &BufferPoolHandle) -> Result<()> {
        debug_assert!(
            !self.migrated_chains,
            "structural batch abort lost migrated committed chains: a non-checkpoint \
             (DDL) structural batch migrated resident chains, which abort cannot undo. \
             Use abort_after_chain_migration on a path that escalates to poison."
        );
        self.abort_inner(handle)
    }

    /// Abort a batch that migrated resident chains, after the caller has
    /// already escalated the failure to a poison-and-reopen.
    ///
    /// Identical page/allocator/header rollback to [`Self::abort`], but WITHOUT
    /// the no-migration `debug_assert`: the checkpoint materialize path
    /// legitimately migrates chains during a leaf split and, on failure,
    /// classifies the abort as `PostMutation` poison so recovery rebuilds the
    /// lost chains from the journal. Freeing the staged/allocated pages here is
    /// still correct — the engine is being poisoned and reopened regardless.
    pub(crate) fn abort_after_chain_migration(self, handle: &BufferPoolHandle) -> Result<()> {
        self.abort_inner(handle)
    }

    fn abort_inner(self, handle: &BufferPoolHandle) -> Result<()> {
        drop(self.writes);
        // F28: run BOTH owners unconditionally and surface the first error.
        // A lifetime-abort error (surfaced since R1b) must not skip the
        // catalog-root header rollback — the header would keep pointing at
        // a freed batch-allocated root, and every caller drops the error.
        let lifetime = self.lifetime.abort(handle);
        let header = self.header.abort(handle);
        lifetime.and(header)
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
    /// Borrowed flag set when a chain mutation passes through this store to the
    /// shared frame. See [`StructuralPageBatch::migrated_chains`] — the
    /// checkpoint path uses it to escalate an abort to poison rather than
    /// silently losing migrated committed chains.
    migrated_chains: &'a mut bool,
    /// When set, `read_leaf` returns the leaf page image WITHOUT cloning the
    /// frame's resident MVCC delta map.
    ///
    /// WHY: structural rebuild (e.g. checkpoint materialize) consumes
    /// `(key, value)` pairs that were already collected once via
    /// `visible_delta_entries`. The structural rebuild ops (`insert`,
    /// `replace_existing`, `delete`) only parse base + staged page bytes and
    /// then discard the chain snapshot returned by `read_leaf`
    /// (`let (buf, _) = self.store.read_leaf(...)`). Cloning the resident
    /// chains on every such read is pure dead work, O(n) per read × n reads,
    /// which made close O(n²) (measured 4.4s × (docs/4k)²). The flag routes
    /// these reads through an image-only path that skips `snapshot_chains`.
    chain_free_reads: bool,
}

impl<'a> StructuralBatchStore<'a> {
    pub(crate) fn new(
        base: BufferPoolPageStore,
        writes: &'a mut StructuralPageWrites,
        lifetime: &'a mut AllocatorLifetimeBatch,
        migrated_chains: &'a mut bool,
    ) -> Self {
        Self {
            base,
            writes,
            lifetime,
            migrated_chains,
            chain_free_reads: false,
        }
    }

    /// Opt this store into chain-free leaf reads.
    ///
    /// WHY: see the `chain_free_reads` field doc — structural rebuild callers
    /// discard the chain snapshot `read_leaf` returns, so cloning the resident
    /// chains per read is dead work that made close O(n²). Only use this for
    /// structural rebuild paths (checkpoint materialize) where the (key,value)
    /// pairs were already harvested via `visible_delta_entries` and reads only
    /// parse base + staged page bytes.
    pub(crate) fn with_chain_free_reads(mut self) -> Self {
        self.chain_free_reads = true;
        self
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
            if self.chain_free_reads {
                // Chain-free rebuild path: the caller discards the chain
                // snapshot (`let (buf, _) = self.store.read_leaf(...)`), so we
                // return the staged image with `None` WITHOUT pinning the real
                // frame to snapshot its resident chains. Cloning those chains
                // per read is the dead work that made close O(n²).
                return Ok((LeafPageImage::owned(buf), None));
            }
            // The chain snapshot lives on the shared frame; structural
            // page-byte staging does not duplicate chains. Pin-and-snapshot
            // against the real frame so MVCC visibility logic keeps working
            // for in-flight reads by this writer.
            let (_, snap) = self.base.read_leaf(page)?;
            return Ok((LeafPageImage::owned(buf), snap));
        }
        if self.chain_free_reads {
            // Base-fallback chain-free read: copy page bytes only, skipping
            // `snapshot_chains`. Same rationale as the staged branch above.
            let image = self.base.read_leaf_image_only(page)?;
            return Ok((image, None));
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
    // list and invalidate the cached frame. Frees of batch-allocated pages
    // stay straight-through (the page is invisible outside the batch), with
    // staged-byte eviction below to prevent alloc-then-free within the same
    // batch from replaying page bytes onto the freed slot at commit. Frees
    // of pre-existing pages are deferred to the lifetime owner: the live
    // free would destroy a page the durable base image still references
    // before the batch commits (see `AllocatorLifetimeBatch`).

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
        if self.writes.staged_4k.remove(&page).is_some() {
            self.writes.touched_4k.retain(|p| *p != page);
        }
        if self.lifetime.allocated_in_batch(page, PageSize::Small4k) {
            // Batch-local page: free immediately and drop the allocation
            // record so abort does not double-free it.
            self.lifetime.forget_new_alloc(page, PageSize::Small4k);
            return self.base.free_internal(page);
        }
        self.lifetime.record_deferred_free(page, PageSize::Small4k);
        Ok(())
    }

    fn free_leaf(&mut self, page: u32) -> Result<()> {
        if self.writes.staged_32k.remove(&page).is_some() {
            self.writes.touched_32k.retain(|p| *p != page);
        }
        if self.lifetime.allocated_in_batch(page, PageSize::Large32k) {
            self.lifetime.forget_new_alloc(page, PageSize::Large32k);
            return self.base.free_leaf(page);
        }
        self.lifetime.record_deferred_free(page, PageSize::Large32k);
        Ok(())
    }

    // ---- MVCC chain helpers: delegate to base.
    //
    // `Arc::make_mut` CoW in the buffer pool already isolates
    // concurrent reader `ChainSnapshot` holders from the writer's
    // mutation. Staging these separately would open a window where the
    // chain is missing from the shared frame without staged structural
    // bytes carrying a replacement — that is worse, not better, for
    // correctness. See the module docstring.

    fn chains_empty(&self, page: u32) -> Result<bool> {
        self.base.chains_empty(page)
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
        // This is the chain RE-HOME step of every structural migration
        // (`partition_chains_for_split` / `move_all_leaf_chains` /
        // `redistribute_leaf_chains` all place a drained chain onto its new
        // owning page through here). The placement hits the shared frame, not
        // staged bytes, so abort cannot undo it — a chain re-homed onto a
        // batch-allocated page is lost when abort frees + invalidates that
        // page. Record it so the checkpoint caller can escalate an abort to
        // poison (see `StructuralPageBatch::migrated_chains`).
        //
        // The single-key `with_chain_under_latch` is the precise migration
        // signal: ordinary CRUD never routes chain writes through a structural
        // batch, and the only batch callers are the migration helpers. The
        // whole-map `with_all_chains_under_latch` below is deliberately NOT
        // flagged — its only batch caller is the overflow-page CLEAR
        // (`free_overflow_chain` wipes stale remnants before freeing a
        // repurposed page), which discards nothing committed and is idempotent
        // on abort.
        *self.migrated_chains = true;
        self.base.with_chain_under_latch(page, key, mode, f)
    }

    fn with_all_chains_under_latch<R, F>(&mut self, page: u32, mode: LatchMode, f: F) -> Result<R>
    where
        F: FnOnce(&mut BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>) -> R,
    {
        // NOT a migration signal — see `with_chain_under_latch` above. The
        // migration DRAIN (`std::mem::take`) flows through here, but the
        // matching RE-HOME (which is what abort would lose) flows through the
        // single-key path, so flagging only that path captures every real
        // committed-chain move without false-positiving the overflow clear.
        self.base.with_all_chains_under_latch(page, mode, f)
    }
}

#[cfg(test)]
#[path = "tests/structural_page_batch.rs"]
mod tests;

#[cfg(test)]
#[path = "tests/structural_page_batch_store.rs"]
mod structural_page_batch_store;

#[cfg(test)]
#[path = "tests/structural_batch_abort_free_safety.rs"]
mod structural_batch_abort_free_safety;

#[cfg(test)]
#[path = "tests/bugsuspect_storage_chain_migration_abort.rs"]
mod bugsuspect_storage_chain_migration_abort;
