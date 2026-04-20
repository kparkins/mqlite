//! Transaction-local page-byte overlay (PR 6 / plan §M4a).
//!
//! Writers open `BTree<TxnPageStore>` via [`TxnPageStore::new`] where the
//! underlying pages live in a [`BufferPoolPageStore`]. All page-byte writes
//! made by the txn go into a per-size overlay keyed by page number; reads
//! check the overlay first and fall back to the shared `base` store.
//!
//! On commit the overlay is drained and the staged bytes are copied onto
//! the real shared buffer-pool frames via
//! [`BufferPoolPageStore::write_internal`] / `write_leaf`. On rollback the
//! overlay is dropped and the shared frames are left untouched — the
//! failing writer no longer pollutes other writers' dirty state.
//!
//! Scope (PR 6, §M4a): page-byte isolation only.
//!
//! - Page-byte reads and writes are overlay-routed.
//! - Allocator calls (`alloc_internal` / `alloc_leaf` / `free_*`) go
//!   straight through to the shared allocator. PR 7 (§M4b) replaces that
//!   with txn-local allocator reservations.
//! - MVCC version-chain helpers (`take_chain` / `put_chain` /
//!   `chains_empty`) also go straight through to the shared frame. They
//!   are already CoW-safe via `Arc::make_mut`: concurrent readers holding
//!   an old `Arc<VecDeque<VersionEntry>>` observe their frozen copy while
//!   the writer mutates a fresh one. Staging the chain mutation separately
//!   would create a window between `take_chain` and `put_chain` in which
//!   the shared frame is missing the chain without the overlay being
//!   consulted; the existing CoW dance already gives us the isolation we
//!   need.
//!
//! Concurrency: a `TxnPageStore` and its backing [`TxnOverlay`] are
//! single-threaded per write-txn — the engine's outer `Mutex<BpBackend>`
//! (or per-lane mutex in PR 8+) already serializes every writer. The
//! overlay is owned exclusively by the write path for the duration of a
//! transaction and passed to helpers by `&mut` reference. No `Arc`,
//! `Mutex`, or `RefCell` wrapping is needed.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::read_view::ChainSnapshot;
use crate::mvcc::version::VersionEntry;
use crate::storage::btree::{empty_internal_page_bytes, empty_leaf_page_bytes, BTreePageStore};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::PageSize;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

const INTERNAL_SIZE: usize = PAGE_SIZE_INTERNAL as usize;
const LEAF_SIZE: usize = PAGE_SIZE_LEAF as usize;

// ---------------------------------------------------------------------------
// Reservations (PR 7 — §M4b)
// ---------------------------------------------------------------------------

/// Origin of a page reservation held by a writer txn. Determines what
/// rollback does with the page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageOrigin {
    /// Page came from a fresh `allocate_*` call (whether free-list pop or
    /// file extension). On rollback: return to allocator free list via
    /// `free_*` and invalidate the buffer-pool frame so a subsequent
    /// reallocation does not see stale cached content.
    NewAlloc,
    /// Page came from the `DeferredFreeQueue` drained at `WriteTxn::begin`
    /// time. On rollback: push back onto `DeferredFreeQueue` (NOT the
    /// allocator free list, because a concurrent reader with a live
    /// `OverflowRef` could still observe the page via refcount decrement).
    DeferredFree,
}

/// One page allocated (or drained from the deferred-free queue) during a
/// writer txn. Recorded into [`TxnOverlay::reservations`] so rollback can
/// undo the allocation and restore the pre-txn free-list state.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PageReservation {
    pub page: u32,
    pub size: PageSize,
    pub origin: PageOrigin,
}

// ---------------------------------------------------------------------------
// TxnOverlay — the shared per-txn state
// ---------------------------------------------------------------------------

/// Per-txn page-byte overlay shared across every `TxnPageStore` instance
/// opened during a single writer transaction.
///
/// The `touched_*` vectors preserve insertion order so commit applies bytes
/// in a deterministic sequence (matches the order writes were produced).
#[derive(Default)]
pub(crate) struct TxnOverlay {
    /// 4K internal pages staged by the writer.
    overlay_4k: HashMap<u32, Box<[u8; INTERNAL_SIZE]>>,
    /// 32K leaf pages staged by the writer.
    overlay_32k: HashMap<u32, Box<[u8; LEAF_SIZE]>>,
    /// Page numbers in overlay_4k in the order they were first written.
    touched_4k: Vec<u32>,
    /// Page numbers in overlay_32k in the order they were first written.
    touched_32k: Vec<u32>,

    // ---- PR 7 (§M4b) additions ------------------------------------------

    /// Every page allocated during this txn (via `alloc_internal` /
    /// `alloc_leaf` on a `TxnPageStore`) or drained from the deferred-free
    /// queue at `WriteTxn::begin`. On rollback each reservation is routed
    /// through its origin-specific undo path.
    ///
    /// Pages that are both allocated AND freed within the same txn have
    /// their reservation retained — rollback then calls `free_*` on the
    /// allocator for a page that is already on the free list, which is a
    /// legal double-free only if we evict the overlay entry at free time.
    /// The overlay eviction in `free_internal` / `free_leaf` already
    /// prevents the page bytes from being replayed at commit, so the
    /// allocator's view ends up consistent with rollback regardless.
    reservations: Vec<PageReservation>,
    /// Pre-txn snapshot of the file header. Captured lazily — only on the
    /// first call to `txn_update_header` during this txn. On rollback the
    /// header is restored to this snapshot in a single `update_header`
    /// closure call. On commit the snapshot is discarded.
    header_pre: Option<FileHeader>,
}

impl TxnOverlay {
    /// Create a fresh, empty overlay for a new writer transaction.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Apply every staged page to the shared buffer-pool frames via
    /// `base`.
    ///
    /// Called by `with_txn` at commit time while the writer still holds
    /// the engine's serialization mutex (today `PagedEngine::inner`,
    /// under PR 8 the `commit_seq` mutex). Consumes the overlay.
    ///
    /// `base` is a fresh `BufferPoolPageStore` pointing at the same
    /// `BufferPoolHandle` the txn used — passing it in rather than
    /// re-creating one inside here keeps the commit free of any handle
    /// lookup; the caller already has one handy.
    pub(crate) fn commit(
        mut self,
        base: &mut BufferPoolPageStore,
        handle: &BufferPoolHandle,
    ) -> Result<()> {
        // Internal first, then leaf. Within each size, honor insertion
        // order to keep replay deterministic.
        let touched_4k = std::mem::take(&mut self.touched_4k);
        for page in touched_4k {
            if let Some(buf) = self.overlay_4k.remove(&page) {
                base.write_internal(page, &buf)?;
            }
        }
        let touched_32k = std::mem::take(&mut self.touched_32k);
        for page in touched_32k {
            if let Some(buf) = self.overlay_32k.remove(&page) {
                base.write_leaf(page, &buf)?;
            }
        }
        // Plan §M4b: at commit time, DeferredFree reservations transition
        // from "reserved for this txn" into the allocator free list.
        // These are pages whose last `OverflowRef` dropped before the
        // txn began and were drained at `with_txn` entry into the
        // overlay (rather than directly into the free list) so a
        // rollback could return them unchanged. NewAlloc reservations
        // have no commit-time work — successful txns keep their
        // allocations. `header_pre` is discarded.
        for res in &self.reservations {
            if matches!(res.origin, PageOrigin::DeferredFree) {
                handle.free_page(res.page, res.size)?;
            }
        }
        Ok(())
    }

    /// Roll back this txn's side effects on allocator / header / buffer
    /// pool (plan §M4b).
    ///
    /// For each reservation:
    /// - `NewAlloc`: return the page to the allocator free list and
    ///   invalidate any cached frame so the next allocator user does not
    ///   see stale content cached from this txn.
    /// - `DeferredFree`: push back onto the `DeferredFreeQueue` so the
    ///   next writer drains-and-frees it after a refcount recheck.
    ///
    /// If `header_pre` is `Some`, restore the header to its pre-txn
    /// snapshot in a single `update_header` closure call.
    ///
    /// Overlay page-byte entries and the `touched_*` vectors are dropped
    /// on return (consumed by `self`) — rollback never writes them to
    /// shared frames.
    pub(crate) fn rollback(self, handle: &BufferPoolHandle) -> Result<()> {
        // Process reservations first — allocator free-list adjustments
        // must happen BEFORE the header snapshot restore, since the
        // allocator mutates the header on every `free_*` call and we
        // want the final state to match `header_pre` exactly.
        //
        // For a NewAlloc page to be returned to the free list the
        // allocator needs to run `free_*`, which itself writes into the
        // free-list head of the header. If `header_pre` was taken AFTER
        // those allocations (which is the normal ordering — allocations
        // happen in the BTree body, header mutations sync the catalog
        // root afterwards), then restoring `header_pre` is authoritative
        // anyway — the free-list head in `header_pre` reflects the
        // pre-txn state.
        //
        // Strategy: if `header_pre` is set, restore it FIRST (which
        // resets free-list pointers, total_page_count, catalog_root_page
        // etc. to pre-txn), then walk reservations to clear the
        // buffer-pool frames / requeue deferred-free pages — skipping the
        // allocator `free_*` calls because they would double-free pages
        // that `header_pre` has already returned to the free list.
        //
        // If `header_pre` is None (no `update_header` calls happened,
        // but the txn still allocated pages via the allocator), fall
        // through to calling `free_*` on each NewAlloc reservation.
        let TxnOverlay {
            reservations,
            header_pre,
            overlay_4k: _,
            overlay_32k: _,
            touched_4k: _,
            touched_32k: _,
        } = self;

        let restored_header = header_pre.is_some();
        if let Some(pre) = header_pre {
            handle.allocator().update_header(|h| *h = pre)?;
        }

        for res in reservations {
            match res.origin {
                PageOrigin::NewAlloc => {
                    if !restored_header {
                        // No header snapshot was taken; we must manually
                        // return the page to the allocator's free list.
                        // Safe because the txn never touched the header
                        // directly — only the allocator's internal
                        // free-list fields, which this `free_*` call
                        // updates consistently.
                        handle.free_page(res.page, res.size)?;
                    }
                    // Invalidate the buffer-pool frame so a subsequent
                    // allocate that reuses this page number does not see
                    // stale content cached by this txn's short-lived
                    // pin.
                    handle.pool().invalidate_page(res.page, res.size)?;
                }
                PageOrigin::DeferredFree => {
                    // Push back onto the queue. A concurrent reader may
                    // still be holding an `OverflowRef` for this page;
                    // the next writer's drain re-checks the refcount
                    // before actually freeing.
                    handle.allocator().enqueue_deferred_free(res.page);
                }
            }
        }
        Ok(())
    }

    /// Record a page reservation. Called by `TxnPageStore::alloc_*` after
    /// a successful base-allocator allocation, and by the deferred-free
    /// drain at `WriteTxn::begin`.
    pub(crate) fn push_reservation(&mut self, res: PageReservation) {
        self.reservations.push(res);
    }

    /// Capture the pre-txn header snapshot if not already set. Called by
    /// `BpBackend::txn_update_header` before the first header mutation.
    pub(crate) fn capture_header_pre_once(&mut self, header: &FileHeader) {
        if self.header_pre.is_none() {
            self.header_pre = Some(header.clone());
        }
    }

    /// True if a pre-txn header snapshot has already been captured.
    pub(crate) fn has_header_pre(&self) -> bool {
        self.header_pre.is_some()
    }
}

// ---------------------------------------------------------------------------
// TxnPageStore — BTreePageStore adapter layering overlay over base
// ---------------------------------------------------------------------------

/// `BTreePageStore` that routes writes into a [`TxnOverlay`] and
/// falls back to `base` for any page not present in the overlay.
///
/// Each writer helper that opens a BTree constructs a `TxnPageStore`
/// borrowing the same `&mut TxnOverlay` so every helper sees every
/// prior write within the transaction.
pub(crate) struct TxnPageStore<'a> {
    base: BufferPoolPageStore,
    overlay: &'a mut TxnOverlay,
}

impl<'a> TxnPageStore<'a> {
    pub(crate) fn new(base: BufferPoolPageStore, overlay: &'a mut TxnOverlay) -> Self {
        Self { base, overlay }
    }
}

impl<'a> BTreePageStore for TxnPageStore<'a> {
    // ---- Reads: overlay first, fall back to base.

    fn read_internal(&self, page: u32) -> Result<Box<[u8; INTERNAL_SIZE]>> {
        if let Some(buf) = self.overlay.overlay_4k.get(&page) {
            return Ok(buf.clone());
        }
        self.base.read_internal(page)
    }

    fn read_leaf(
        &self,
        page: u32,
    ) -> Result<(Box<[u8; LEAF_SIZE]>, Option<ChainSnapshot>)> {
        if let Some(buf) = self.overlay.overlay_32k.get(&page).cloned() {
            // The chain snapshot lives on the shared frame; a txn-local
            // byte overlay doesn't duplicate chains. Pin-and-snapshot
            // against the real frame so MVCC visibility logic keeps
            // working for in-flight reads by this writer.
            let (_disk, snap) = self.base.read_leaf(page)?;
            return Ok((buf, snap));
        }
        self.base.read_leaf(page)
    }

    // ---- Writes: stage into the overlay. NEVER touch the shared frame.

    fn write_internal(&mut self, page: u32, data: &[u8; INTERNAL_SIZE]) -> Result<()> {
        let existed = self.overlay.overlay_4k.contains_key(&page);
        self.overlay.overlay_4k.insert(page, Box::new(*data));
        if !existed {
            self.overlay.touched_4k.push(page);
        }
        Ok(())
    }

    fn write_leaf(&mut self, page: u32, data: &[u8; LEAF_SIZE]) -> Result<()> {
        let existed = self.overlay.overlay_32k.contains_key(&page);
        self.overlay.overlay_32k.insert(page, Box::new(*data));
        if !existed {
            self.overlay.touched_32k.push(page);
        }
        Ok(())
    }

    // ---- Allocation / free
    //
    // PR 7 (§M4b): every allocation made inside a txn is recorded as a
    // `NewAlloc` reservation on the overlay so rollback can return the
    // page to the allocator free list AND invalidate the cached frame.
    // Free stays straight-through, with the overlay eviction (below) to
    // prevent an alloc-then-free-in-same-txn from replaying page bytes
    // onto the freed slot at commit time.

    fn alloc_internal(&mut self) -> Result<u32> {
        let page = self.base.alloc_internal()?;
        // Seed the overlay with a valid empty-internal image (Bug A fix).
        // A fresh allocation leaves the shared buffer-pool frame
        // zero-filled (or carrying stale bytes from a previous
        // occupant). Any subsequent in-txn read via `read_internal` must
        // see a valid page header — not zeros and not the previous
        // occupant. Commit replays the final staged bytes onto the
        // shared frame before the txn becomes visible, so later
        // in-txn writes to the same page simply overwrite the seed.
        let seed = empty_internal_page_bytes()?;
        self.overlay.overlay_4k.insert(page, Box::new(seed));
        self.overlay.touched_4k.push(page);
        self.overlay.push_reservation(PageReservation {
            page,
            size: PageSize::Small4k,
            origin: PageOrigin::NewAlloc,
        });
        Ok(page)
    }

    fn alloc_leaf(&mut self) -> Result<u32> {
        let page = self.base.alloc_leaf()?;
        // Seed the overlay with a valid empty-leaf image (Bug A fix).
        // Same rationale as `alloc_internal` above. Overflow pages also
        // come through `alloc_leaf` because they share the 32 KB slot
        // size; that is safe because `write_overflow_chain` writes a
        // full page (zero-initialized buffer + header + payload) before
        // any read decodes the page as an overflow frame, so the
        // empty-leaf seed is always fully overwritten before it can be
        // misinterpreted.
        let seed = empty_leaf_page_bytes()?;
        self.overlay.overlay_32k.insert(page, Box::new(seed));
        self.overlay.touched_32k.push(page);
        self.overlay.push_reservation(PageReservation {
            page,
            size: PageSize::Large32k,
            origin: PageOrigin::NewAlloc,
        });
        Ok(page)
    }

    fn free_internal(&mut self, page: u32) -> Result<()> {
        // Also evict any overlay entry — if the same txn allocated,
        // wrote, and then freed the page, we must not replay its bytes
        // onto the freed slot at commit time.
        //
        // Plan §M4b: if the freed page was recorded as a `NewAlloc`
        // reservation (same-txn alloc-then-free), drop the reservation
        // too. Otherwise rollback would double-free the page — the
        // in-txn `free_*` already returned it to the allocator free
        // list.
        if self.overlay.overlay_4k.remove(&page).is_some() {
            self.overlay.touched_4k.retain(|p| *p != page);
        }
        self.overlay.reservations.retain(|r| {
            r.page != page
                || r.size != PageSize::Small4k
                || r.origin != PageOrigin::NewAlloc
        });
        self.base.free_internal(page)
    }

    fn free_leaf(&mut self, page: u32) -> Result<()> {
        if self.overlay.overlay_32k.remove(&page).is_some() {
            self.overlay.touched_32k.retain(|p| *p != page);
        }
        self.overlay.reservations.retain(|r| {
            r.page != page
                || r.size != PageSize::Large32k
                || r.origin != PageOrigin::NewAlloc
        });
        self.base.free_leaf(page)
    }

    // ---- MVCC chain helpers: delegate to base.
    //
    // `Arc::make_mut` CoW in the buffer pool already isolates
    // concurrent reader `ChainSnapshot` holders from the writer's
    // mutation. Staging these separately would open a window where the
    // chain is missing from the shared frame without the overlay
    // carrying a replacement — that is worse, not better, for
    // correctness. See the module docstring.

    fn take_chain(
        &mut self,
        page: u32,
        key: &[u8],
    ) -> Result<Option<Arc<VecDeque<VersionEntry>>>> {
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
        self.base.take_all_chains(page)
    }
}
