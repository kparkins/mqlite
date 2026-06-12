//! Page allocator — dual free lists for 4 KB and 32 KB pages.
//!
//! The allocator manages two singly-linked free lists embedded in the on-disk
//! file header:
//!
//! - **4 KB free list** (`free_list_head_4k`): pages used for internal B+ tree nodes.
//! - **32 KB free list** (`free_list_head_32k`): pages used for leaf nodes, overflow
//!   pages, and the file header.
//!
//! ## Module layout
//!
//! - [`free_list`] — the short-lived [`PageAllocator`](free_list::PageAllocator)
//!   borrow that walks the free-list links (read/write the next pointer) and
//!   extends the file. Owns the reusable I/O scratch buffer discipline.
//! - [`overflow`] — overflow-chain refcounting plus the page-lifetime
//!   deferred-free and dropped-tree retired-page queues, including the
//!   persisted-history refcount lifecycle documentation.
//! - this module — [`AllocatorHandle`] (the `Arc`-shared owned-state facade),
//!   the freeze guard, header access, and flush.
//!
//! ## Free-list on-disk encoding
//!
//! Each free page stores the page number of the **next** free page in the list as a
//! 4-byte little-endian `u32` at **byte offset 0** of the page.  A value of `0`
//! signals end-of-list.  All remaining bytes in the free page are zeroed.
//!
//! ## Header ownership
//!
//! [`PageAllocator`](free_list::PageAllocator) holds a mutable borrow of the
//! [`FileHeader`].  All mutations to the free-list pointers and page counts are
//! applied to the header in memory.  The **caller** is responsible for writing
//! the updated header back to page 0 after any `allocate_*` or `free_*` call so
//! that the changes are persisted.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::mvcc::deferred_free::PageLifetimeQueue;
use crate::mvcc::timestamp::Ts;
use crate::storage::buffer_pool::{PageSize, PageSource};
use crate::storage::header::FileHeader;

pub(crate) mod free_list;
mod overflow;

use free_list::PageAllocator;

// ---------------------------------------------------------------------------
// AllocatorHandle — owned-state allocator for concurrent use
// ---------------------------------------------------------------------------

/// Owned state for the [`AllocatorHandle`].
struct AllocatorState {
    header: FileHeader,
    header_dirty: bool,
    frozen: bool,
    /// Reusable page-sized I/O staging buffer for free-list link reads/writes.
    ///
    /// Lives inside the state `Mutex` so the only `PageAllocator` borrow active
    /// at a time (constructed under the lock) gets exclusive `&mut` access — no
    /// allocation/zeroing of a fresh 32 KiB `Vec` per free-list link walk under
    /// the global allocator mutex. Sized for the largest page (32 KiB).
    link_scratch: Box<[u8]>,
}

impl AllocatorState {
    /// Split-borrow the header and the link scratch buffer so a
    /// [`PageAllocator`] can take both `&mut` at once without re-borrowing
    /// `self`.
    fn allocator_parts(&mut self) -> (&mut FileHeader, &mut [u8]) {
        (&mut self.header, &mut self.link_scratch)
    }
}

type FreezeViolationPoisoner = Arc<dyn Fn() + Send + Sync + 'static>;

/// Provider returning the live `ReadViewRegistry::oldest_required_ts()`
/// used to gate `RetiredTree*` page-lifetime releases.
type RetiredPageReaderFloor = Arc<dyn Fn() -> Ts + Send + Sync + 'static>;

/// Inner state of an [`AllocatorHandle`], shared via a single `Arc`.
struct AllocatorInner {
    state: Mutex<AllocatorState>,
    /// Per-overflow-chain refcount table.
    ///
    /// Maps `first_page` → shared `AtomicU32` pin counter. Populated when
    /// the first `OverflowRef` for a chain is created. Cloned-out
    /// `Arc<AtomicU32>` handles let callers do atomic ops without holding
    /// the HashMap mutex.
    ///
    /// Atomic ops on the refcount happen OUTSIDE the allocator state mutex.
    overflow_refcounts: Mutex<HashMap<u32, Arc<AtomicU32>>>,
    /// Refcount-to-zero queue drained by the writer path.
    ///
    /// Lock-order position 1.5 (before `state` at position 2).
    page_lifetime_queue: PageLifetimeQueue,
    /// Monotonic in-memory checkpoint fence for page-lifetime drains.
    page_lifetime_checkpoint_fence: AtomicU64,
    /// Live-engine poison hook invoked when mutation is attempted while frozen.
    freeze_violation_poisoner: Mutex<Option<FreezeViolationPoisoner>>,
    /// Reader low-water provider for `RetiredTree*` lifetime releases.
    ///
    /// Installed by the engine at construction with a closure over the
    /// shared `ReadViewRegistry`. Absent (e.g. raw test handles) the drains
    /// conservatively keep retired-tree entries queued. Evaluated lazily —
    /// only when a fence-eligible retired entry exists — and ONLY by the
    /// checkpoint drain (`drain_free_queue_with_retired`); hot drains never
    /// touch it, so they never acquire the `ReadViewRegistry` mutex.
    retired_page_reader_floor: Mutex<Option<RetiredPageReaderFloor>>,
    /// Dropped-tree overflow pages whose refcount was still positive when
    /// `retire_dropped_tree_pages` walked the tree: `first_page` → the
    /// drop's `reader_fence_ts`. The final `OverflowRef` decref's enqueue
    /// consults (and consumes) this map so the page enters the lifetime
    /// queue as `RetiredTree32k` carrying the drop's reader fence instead
    /// of a fence-less `OverflowDeferredFree` a pre-drop reader could
    /// still race (the scan path reaches overflow chains through base-leaf
    /// pointers without taking a refcount).
    retired_overflow_pending: Mutex<HashMap<u32, Ts>>,
    /// Lock-free emptiness gate for `retired_overflow_pending`: the
    /// steady-state final-decref enqueue pays one atomic load instead of a
    /// map lock. See the Dekker pairing notes on
    /// [`AllocatorHandle::note_retired_overflow_pending`] and
    /// [`AllocatorHandle::enqueue_overflow_deferred_free`].
    retired_overflow_pending_count: AtomicUsize,
}

/// A `Clone`-able, `Arc`-wrapped allocator handle that owns the
/// [`FileHeader`] rather than borrowing it.
///
/// Wraps all shared state in a single `Arc<AllocatorInner>`. All
/// allocations and deallocations lock the state mutex, perform the
/// operation via a short-lived [`PageAllocator`], and release the lock.
///
/// After any allocation or free, the in-memory header is marked dirty.  Call
/// [`flush_header`](AllocatorHandle::flush_header) to persist the updated
/// header to page 0 through a `PageSource`.
#[derive(Clone)]
pub(crate) struct AllocatorHandle {
    inner: Arc<AllocatorInner>,
}

/// RAII guard for the checkpoint allocator freeze window.
#[allow(
    dead_code,
    reason = "Phase 7 US-004 lands the freeze primitive before the checkpoint driver consumes it"
)]
#[must_use = "AllocatorFreezeGuard must live until the boundary commit consumes it"]
pub(crate) struct AllocatorFreezeGuard {
    inner: Arc<AllocatorInner>,
    active: bool,
    _not_send: PhantomData<Rc<()>>,
}

impl AllocatorFreezeGuard {
    #[allow(
        dead_code,
        reason = "Phase 7 US-004 lands the freeze primitive before the checkpoint driver consumes it"
    )]
    fn release(&mut self) {
        if !self.active {
            return;
        }
        if let Ok(mut state) = self.inner.state.lock() {
            state.frozen = false;
        }
        self.active = false;
    }
}

impl Drop for AllocatorFreezeGuard {
    fn drop(&mut self) {
        self.release();
    }
}

impl AllocatorHandle {
    /// Create an `AllocatorHandle` from an existing [`FileHeader`].
    ///
    /// The header is placed in clean state (not dirty).  Call
    /// [`flush_header`](Self::flush_header) after any allocations to persist
    /// changes.
    pub(crate) fn new(header: FileHeader) -> Self {
        Self {
            inner: Arc::new(AllocatorInner {
                state: Mutex::new(AllocatorState {
                    header,
                    header_dirty: false,
                    frozen: false,
                    link_scratch: vec![0u8; PageSize::Large32k.bytes()].into_boxed_slice(),
                }),
                overflow_refcounts: Mutex::new(HashMap::new()),
                page_lifetime_queue: PageLifetimeQueue::new(),
                page_lifetime_checkpoint_fence: AtomicU64::new(0),
                freeze_violation_poisoner: Mutex::new(None),
                retired_page_reader_floor: Mutex::new(None),
                retired_overflow_pending: Mutex::new(HashMap::new()),
                retired_overflow_pending_count: AtomicUsize::new(0),
            }),
        }
    }

    /// Install the live-engine poison hook used for freeze-window violations.
    pub(crate) fn install_freeze_violation_poisoner<F>(&self, poisoner: F) -> Result<()>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let mut guard = self
            .inner
            .freeze_violation_poisoner
            .lock()
            .map_err(|_| Error::Internal("allocator poison hook mutex poisoned".into()))?;
        *guard = Some(Arc::new(poisoner));
        Ok(())
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, AllocatorState>> {
        self.inner
            .state
            .lock()
            .map_err(|_| Error::Internal("allocator mutex poisoned".into()))
    }

    fn lock_mutable_state(&self) -> Result<MutexGuard<'_, AllocatorState>> {
        let state = self.lock_state()?;
        if state.frozen {
            drop(state);
            return Err(self.freeze_violation_error());
        }
        Ok(state)
    }

    fn ensure_not_frozen(&self) -> Result<()> {
        let state = self.lock_state()?;
        if state.frozen {
            drop(state);
            return Err(self.freeze_violation_error());
        }
        Ok(())
    }

    fn freeze_violation_error(&self) -> Error {
        if let Ok(guard) = self.inner.freeze_violation_poisoner.lock() {
            if let Some(poisoner) = guard.clone() {
                poisoner();
            }
        }
        Error::Internal("allocator is frozen; mutation rejected".into())
    }

    /// Close the allocator mutation window until a boundary token is consumed.
    #[allow(
        dead_code,
        reason = "Phase 7 US-004 lands the freeze primitive before the checkpoint driver consumes it"
    )]
    pub(crate) fn freeze_guard(&self) -> Result<AllocatorFreezeGuard> {
        let mut state = self.lock_state()?;
        if state.frozen {
            drop(state);
            return Err(self.freeze_violation_error());
        }
        state.frozen = true;
        Ok(AllocatorFreezeGuard {
            inner: Arc::clone(&self.inner),
            active: true,
            _not_send: PhantomData,
        })
    }

    /// Borrow the page-lifetime queue used by `OverflowRef::drop` and drain paths.
    pub(crate) fn page_lifetime_queue(&self) -> &PageLifetimeQueue {
        &self.inner.page_lifetime_queue
    }

    /// Install the reader low-water provider used to gate `RetiredTree*`
    /// page-lifetime releases (normally a closure over
    /// `ReadViewRegistry::oldest_required_ts`).
    pub(crate) fn install_retired_page_reader_floor<F>(&self, provider: F) -> Result<()>
    where
        F: Fn() -> Ts + Send + Sync + 'static,
    {
        let mut guard = self
            .inner
            .retired_page_reader_floor
            .lock()
            .map_err(|_| Error::Internal("allocator reader-floor mutex poisoned".into()))?;
        *guard = Some(Arc::new(provider));
        Ok(())
    }

    /// Evaluate the installed reader low-water provider, if any.
    ///
    /// Returns `None` when no provider is installed (raw test handles),
    /// which drains treat as "retired-tree entries are not releasable".
    /// The provider guard is dropped BEFORE the provider runs so the
    /// provider's own locks (`ReadViewRegistry`, position 5) never nest
    /// under this leaf mutex.
    fn retired_page_reader_floor(&self) -> Option<Ts> {
        let provider = {
            let guard = self.inner.retired_page_reader_floor.lock().ok()?;
            guard.clone()?
        };
        Some(provider())
    }

    /// Advance the checkpoint fence used by page-lifetime drains.
    pub(crate) fn advance_page_lifetime_checkpoint_fence(&self) -> u64 {
        self.inner
            .page_lifetime_checkpoint_fence
            .fetch_add(1, Ordering::AcqRel)
            + 1
    }

    fn page_lifetime_checkpoint_fence(&self) -> u64 {
        self.inner
            .page_lifetime_checkpoint_fence
            .load(Ordering::Acquire)
    }

    // -----------------------------------------------------------------------
    // Allocation
    // -----------------------------------------------------------------------

    fn mutate_allocator<T>(
        &self,
        io: &dyn PageSource,
        op: impl FnOnce(&mut PageAllocator<'_>) -> Result<T>,
    ) -> Result<T> {
        let mut state = self.lock_mutable_state()?;
        let (header, scratch) = state.allocator_parts();
        let mut alloc = PageAllocator::new(header, io, scratch);
        let result = op(&mut alloc)?;
        state.header_dirty = true;
        Ok(result)
    }

    /// Allocate a 4 KB internal-node page.
    ///
    /// Updates the in-memory free list and marks the header dirty.  The
    /// caller must call [`flush_header`](Self::flush_header) (or flush the
    /// buffer pool) to persist the change to disk.
    pub(crate) fn alloc_4k(&self, io: &dyn PageSource) -> Result<u32> {
        self.mutate_allocator(io, |alloc| alloc.allocate_4k())
    }

    /// Allocate a 32 KB leaf / overflow page.
    ///
    /// Updates the in-memory free list and marks the header dirty.
    pub(crate) fn alloc_32k(&self, io: &dyn PageSource) -> Result<u32> {
        self.mutate_allocator(io, |alloc| alloc.allocate_32k())
    }

    // -----------------------------------------------------------------------
    // Deallocation
    // -----------------------------------------------------------------------

    /// Return a 4 KB page to the free list.
    ///
    /// Marks the header dirty.  The freed page's first 4 bytes are
    /// overwritten with the free-list head pointer via `io`.
    pub(crate) fn free_4k(&self, page_number: u32, io: &dyn PageSource) -> Result<()> {
        self.mutate_allocator(io, |alloc| alloc.free_4k(page_number))
    }

    /// Return a 32 KB page to the free list.
    ///
    /// Marks the header dirty.  The freed page's first 4 bytes are
    /// overwritten with the free-list head pointer via `io`.
    pub(crate) fn free_32k(&self, page_number: u32, io: &dyn PageSource) -> Result<()> {
        self.mutate_allocator(io, |alloc| alloc.free_32k(page_number))
    }

    // -----------------------------------------------------------------------
    // Header access
    // -----------------------------------------------------------------------

    /// Read the current in-memory file header.
    ///
    /// The closure receives a shared reference to the header; its return
    /// value is returned from this method.
    #[allow(dead_code)]
    pub(crate) fn with_header<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&FileHeader) -> R,
    {
        let state = self.lock_state()?;
        Ok(f(&state.header))
    }

    /// Mutate the in-memory file header and mark it dirty.
    ///
    /// Use this to update fields such as `catalog_root_page` after a B+ tree
    /// root split, without going through the allocation path.
    pub(crate) fn update_header<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&mut FileHeader),
    {
        let mut state = self.lock_mutable_state()?;
        f(&mut state.header);
        state.header_dirty = true;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Flush
    // -----------------------------------------------------------------------

    /// Write the in-memory header back to page 0 via `io` if it is dirty.
    ///
    /// Clears the dirty flag on success.  If the header is clean, this is a
    /// no-op.
    ///
    /// Typically called after all B+ tree operations in a transaction are
    /// complete, before the WAL commit frame is written.
    pub(crate) fn flush_header(&self, io: &dyn PageSource) -> Result<()> {
        let mut state = self.lock_mutable_state()?;
        if state.header_dirty {
            let bytes = state.header.to_bytes();
            io.write_page(0, PageSize::Small4k, &bytes)?;
            state.header_dirty = false;
        }
        Ok(())
    }

    /// Return `true` if the in-memory header has been modified since the
    /// last [`flush_header`](Self::flush_header) call.
    #[allow(dead_code)]
    pub(crate) fn is_header_dirty(&self) -> bool {
        self.inner.state.lock().is_ok_and(|s| s.header_dirty)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "../tests/allocator_accessors.rs"]
mod allocator_accessors;

#[cfg(test)]
#[path = "../tests/allocator.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/allocator_retired_drain.rs"]
mod retired_drain_tests;
