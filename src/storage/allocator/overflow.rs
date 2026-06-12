//! Overflow-chain refcounting + deferred-free / retired-queue machinery.
//!
//! This module owns the [`AllocatorHandle`] methods that manage the
//! per-overflow-chain refcount table, the page-lifetime deferred-free queue,
//! and the dropped-tree retired-page queue. The backing fields live on
//! `AllocatorInner` in the parent module; the access discipline notes for each
//! are documented there.
//!
//! ## Persisted-history refcount lifecycle (moved from `history_store.rs`)
//!
//! A persisted history record (a `VersionData::Overflow` entry written into the
//! history-store B-tree) **owns a logical +1 refcount** on its overflow chain's
//! `first_page`. That +1 is what keeps the overflow chain alive while the aged
//! version is only reachable from the history store. The lifecycle is:
//!
//! * On spill: `decode_version_entry_value` / `OverflowRef::new_owned` bump the
//!   refcount when the entry is rehydrated, and the insert path forgets the
//!   in-flight `OverflowRef` (`forget_history_record_overflow_ref`) so the +1
//!   stays charged to the persisted record rather than the transient handle.
//! * On GC: `HistoryStore::gc_pass` deletes the B-tree cell and transfers that
//!   persisted +1 into an ephemeral `OverflowRef::from_existing_refcount` (no
//!   bump) which then `Drop`s — decrementing here via
//!   [`AllocatorHandle::decref_overflow`] and, when the count reaches 0,
//!   enqueueing the chain for deferred free via
//!   [`AllocatorHandle::enqueue_overflow_deferred_free`].
//!
//! So every decrement of the persisted-history +1 flows through the RAII
//! `OverflowRef::Drop` → `decref_overflow` path below; the history store never
//! calls `decref_overflow` directly. (Pointer left in
//! `history_store.rs::gc_pass`.)

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::deferred_free::{PageLifetimeEntry, PageLifetimeKind};
use crate::mvcc::timestamp::Ts;
use crate::storage::allocator::free_list::PageAllocator;
use crate::storage::allocator::AllocatorHandle;
use crate::storage::buffer_pool::{PageSize, PageSource};

impl AllocatorHandle {
    // -----------------------------------------------------------------------
    // Overflow refcount
    // -----------------------------------------------------------------------
    //
    // The refcount for each overflow chain lives in an AtomicU32 that is
    // logically bound to the first page of the chain. These methods access
    // the atomic OUTSIDE the allocator state mutex — only mutations of the
    // allocator state (`state.header`) take that mutex. The per-entry atomic
    // access pattern lets Clone / Drop on OverflowRef stay lock-free on the
    // hot path.
    //
    // Drains recheck refcounts before moving pages out of reader-visible
    // overflow ownership.

    /// Look up or create the shared `AtomicU32` refcount for `first_page`.
    ///
    /// `pub(super)` so the test-only `allocator_accessors` sibling module can
    /// reuse it (`set_overflow_refcount_for_test`).
    pub(super) fn refcount_handle(&self, first_page: u32) -> Arc<AtomicU32> {
        #[allow(clippy::unwrap_used)]
        let mut table = self.inner.overflow_refcounts.lock().unwrap();
        table
            .entry(first_page)
            .or_insert_with(|| Arc::new(AtomicU32::new(0)))
            .clone()
    }

    /// Get the refcount handle if one exists; do not create.
    fn refcount_handle_opt(&self, first_page: u32) -> Option<Arc<AtomicU32>> {
        #[allow(clippy::unwrap_used)]
        let table = self.inner.overflow_refcounts.lock().unwrap();
        table.get(&first_page).cloned()
    }

    /// Saturating CAS-loop incref on the overflow-chain refcount.
    ///
    /// Returns the new (post-bump) refcount on success. Returns
    /// [`Error::RefcountOverflow`] if the observed pre-bump value is
    /// `u32::MAX`; in that case the atomic is left unchanged under every
    /// interleaving.
    ///
    /// Ordering:
    /// * Acquire on the initial load and on failed CAS attempts —
    ///   synchronizes-with prior Release decrefs for visibility of
    ///   preceding writes to the page's metadata.
    /// * Release on successful CAS store — synchronizes-with subsequent
    ///   Acquire decrefs and the `drain_free_queue` Acquire recheck.
    pub(crate) fn incref_overflow(&self, first_page: u32) -> Result<u32> {
        let atomic = self.refcount_handle(first_page);
        let mut cur = atomic.load(Ordering::Acquire);
        loop {
            if cur == u32::MAX {
                return Err(Error::RefcountOverflow);
            }
            match atomic.compare_exchange_weak(cur, cur + 1, Ordering::Release, Ordering::Acquire) {
                Ok(_) => return Ok(cur + 1),
                Err(observed) => {
                    crate::mvcc::metrics::record_overflow_refcount_cas_retry();
                    cur = observed;
                }
            }
        }
    }

    /// CAS-loop incref only when an overflow-chain refcount is still live.
    ///
    /// Returns `Ok(None)` when the refcount handle is absent or the observed
    /// refcount is 0. Unlike [`Self::incref_overflow`], this method never
    /// creates a refcount slot and never resurrects a dropped overflow chain.
    ///
    /// # Errors
    ///
    /// Returns [`Error::RefcountOverflow`] if the observed pre-bump value is
    /// `u32::MAX`; the atomic value is left unchanged.
    pub(crate) fn try_incref_live_overflow(&self, first_page: u32) -> Result<Option<u32>> {
        let Some(atomic) = self.refcount_handle_opt(first_page) else {
            return Ok(None);
        };
        let mut cur = atomic.load(Ordering::Acquire);
        loop {
            if cur == 0 {
                return Ok(None);
            }
            if cur == u32::MAX {
                return Err(Error::RefcountOverflow);
            }
            match atomic.compare_exchange_weak(cur, cur + 1, Ordering::Release, Ordering::Acquire) {
                Ok(_) => return Ok(Some(cur + 1)),
                Err(observed) => {
                    crate::mvcc::metrics::record_overflow_refcount_cas_retry();
                    cur = observed;
                }
            }
        }
    }

    /// Decref. Returns the post-decrement refcount.
    ///
    /// Ordering: Release on `fetch_sub`, synchronizing with subsequent
    /// Acquire loads by `overflow_refcount` / `drain_free_queue`.
    ///
    /// # Panics
    /// Debug-asserts that the pre-decrement count is > 0. In release
    /// builds a decref on an unknown / zeroed refcount returns 0 and has
    /// no net effect (defense-in-depth for a class of bugs that RAII is
    /// supposed to prevent).
    pub(crate) fn decref_overflow(&self, first_page: u32) -> u32 {
        let Some(atomic) = self.refcount_handle_opt(first_page) else {
            debug_assert!(
                false,
                "decref on unknown first_page {first_page} — pin accounting bug"
            );
            return 0;
        };
        let prev = atomic.fetch_sub(1, Ordering::Release);
        debug_assert!(prev > 0, "decref on already-zero refcount");
        prev.saturating_sub(1)
    }

    /// Read-only refcount probe. Uses Acquire so the reader sees all prior
    /// Release decrefs.
    pub(crate) fn overflow_refcount(&self, first_page: u32) -> u32 {
        self.refcount_handle_opt(first_page)
            .map_or(0, |a| a.load(Ordering::Acquire))
    }

    /// Refcount probe that distinguishes "no refcount slot was ever
    /// created" (`None` — plain leaf pages, never-referenced pages) from a
    /// live slot's current value (`Some(v)`, Acquire load).
    ///
    /// `retire_dropped_tree_pages` needs the distinction: a slot-less page
    /// will never see an `OverflowRef` decref, so the retire walk owns its
    /// enqueue; a slot at 0 means the final decref already ran and its
    /// enqueue (not the retire walk) owns the page's queue entry.
    pub(crate) fn overflow_refcount_slot(&self, first_page: u32) -> Option<u32> {
        self.refcount_handle_opt(first_page)
            .map(|a| a.load(Ordering::Acquire))
    }

    /// Number of overflow-chain `first_page`s whose refcount is currently
    /// `>= 1`. Backs the `mvcc.overflow.pages_in_use` gauge.
    ///
    /// Walks the refcount table under the table mutex; acceptable because
    /// this is called at checkpoint cadence, not on the hot path.
    pub(crate) fn overflow_pages_in_use(&self) -> usize {
        #[allow(clippy::unwrap_used)]
        let table = self.inner.overflow_refcounts.lock().unwrap();
        table
            .values()
            .filter(|a| a.load(Ordering::Acquire) >= 1)
            .count()
    }

    /// Enqueue a page for deferred free. Called by `OverflowRef::drop`
    /// when the decrement brings refcount to 0.
    ///
    /// If the page belongs to a dropped tree whose retire walk recorded a
    /// pending reader fence ([`Self::note_retired_overflow_pending`]), the
    /// note is consumed and the page is enqueued as `RetiredTree32k`
    /// carrying that fence — released only by the checkpoint drain once no
    /// live `ReadView` predates the drop — instead of a fence-less
    /// `OverflowDeferredFree`.
    pub(crate) fn enqueue_overflow_deferred_free(&self, first_page: u32) {
        // Dekker pairing with `note_retired_overflow_pending`: the caller's
        // refcount `fetch_sub` (Release) precedes this fence; the retire
        // walk stores the note, fences, then probes the refcount (Acquire).
        // The two SeqCst fences guarantee at least one side observes the
        // other — either the retire walk sees refcount 0 (and resolves
        // ownership under the note map's mutex) or this path sees the note
        // — so a fence-less entry can never slip through for a page whose
        // drop demanded a reader fence.
        std::sync::atomic::fence(Ordering::SeqCst);
        if let Some(reader_fence_ts) = self.take_retired_overflow_pending(first_page) {
            self.enqueue_retired_tree_page(first_page, PageSize::Large32k, reader_fence_ts);
            return;
        }
        let fence = self.page_lifetime_checkpoint_fence();
        self.inner
            .page_lifetime_queue
            .push_overflow_deferred_free(first_page, fence);
    }

    /// Record that a dropped tree's overflow page still had a positive
    /// refcount at retire time: its final decref must enqueue it as
    /// `RetiredTree32k` carrying `reader_fence_ts` (the drop's post-publish
    /// `visible_ts`).
    ///
    /// MUST be called BEFORE the retire walk probes the refcount (see the
    /// Dekker pairing note on [`Self::enqueue_overflow_deferred_free`]):
    /// note-then-probe guarantees a final decref racing the walk either
    /// consumes this note or is observed by the walk's probe.
    pub(crate) fn note_retired_overflow_pending(&self, first_page: u32, reader_fence_ts: Ts) {
        {
            #[allow(clippy::unwrap_used)]
            let mut pending = self.inner.retired_overflow_pending.lock().unwrap();
            if pending.insert(first_page, reader_fence_ts).is_none() {
                self.inner
                    .retired_overflow_pending_count
                    .fetch_add(1, Ordering::Release);
            }
        }
        std::sync::atomic::fence(Ordering::SeqCst);
    }

    /// Consume (remove and return) a pending retired-overflow note.
    ///
    /// Steady state (no drop in flight) pays a single Acquire load: the
    /// count gate skips the map mutex entirely when no note exists.
    pub(crate) fn take_retired_overflow_pending(&self, first_page: u32) -> Option<Ts> {
        if self
            .inner
            .retired_overflow_pending_count
            .load(Ordering::Acquire)
            == 0
        {
            return None;
        }
        #[allow(clippy::unwrap_used)]
        let mut pending = self.inner.retired_overflow_pending.lock().unwrap();
        let taken = pending.remove(&first_page);
        if taken.is_some() {
            self.inner
                .retired_overflow_pending_count
                .fetch_sub(1, Ordering::Release);
        }
        taken
    }

    /// Enqueue a dropped tree's retired page (either size class) for
    /// deferred free.
    ///
    /// `reader_fence_ts` is the post-publish `visible_ts` of the drop: the
    /// page is only released once a later checkpoint fence has passed AND no
    /// live `ReadView` has `read_ts < reader_fence_ts` — i.e. once no
    /// stale-epoch snapshot that could still descend the dropped tree
    /// remains registered.
    pub(crate) fn enqueue_retired_tree_page(
        &self,
        page: u32,
        size: PageSize,
        reader_fence_ts: Ts,
    ) {
        let fence = self.page_lifetime_checkpoint_fence();
        let kind = match size {
            PageSize::Small4k => PageLifetimeKind::RetiredTree4k,
            PageSize::Large32k => PageLifetimeKind::RetiredTree32k,
        };
        self.inner
            .page_lifetime_queue
            .push_retired_tree(kind, page, fence, reader_fence_ts);
    }

    /// Writer-serialized HOT-PATH drain of the deferred-free queue.
    ///
    /// Releases fence-eligible `OverflowDeferredFree` entries ONLY.
    /// `RetiredTree*` entries are checkpoint-owned: they are released
    /// exclusively by [`Self::drain_free_queue_with_retired`], whose sole
    /// production caller (`BufferPoolHandle::advance_page_lifetime_checkpoint`)
    /// passes the pool-coherent `BufferPoolPageSource`. Two invariants hang
    /// off that split:
    ///
    /// 1. **Free-list pool coherence (F8):** `free_*` writes a freed page's
    ///    next-free link through the CALLER'S io, but `allocate` reads it
    ///    back pool-coherently. Hot callers (e.g. `pin_with_reconcile`)
    ///    historically passed the pool's raw backing `PageSource`; a
    ///    retired-tree free issued there would leave the dropped tree's
    ///    still-resident frames serving stale bytes as free-list links.
    /// 2. **Hot-path cost (F39):** hot drains never scan retired entries
    ///    and never evaluate the reader floor, so a dropped 100k-page
    ///    namespace cannot tax every commit/pin with an O(depth) scan plus
    ///    a `ReadViewRegistry` mutex acquisition.
    ///
    /// Precondition: caller holds writer serialization. For each queued
    /// page, re-loads refcount with Acquire ordering and frees only if
    /// still 0. A non-zero count re-enqueues (defense-in-depth; should be
    /// unreachable under RAII correctness).
    ///
    /// Locks acquired in order: queue (1.5) → state (2). The queue is
    /// drained into a Vec before `state` is locked, so the two locks are
    /// never held simultaneously.
    ///
    /// Ticks `mvcc.overflow.pages_freed_total` per freed page and refreshes
    /// the `mvcc.deferred_free_queue_depth` gauge with the post-drain
    /// queue size (accounts for requeued entries).
    ///
    /// Returns the number of pages actually freed.
    pub(crate) fn drain_free_queue(&self, io: &dyn PageSource) -> Result<usize> {
        self.ensure_not_frozen()?;
        let entries = self
            .inner
            .page_lifetime_queue
            .take_eligible(self.page_lifetime_checkpoint_fence());
        self.free_drained_entries(entries, io)
    }

    /// Checkpoint-only drain: releases fence-eligible overflow entries AND
    /// `RetiredTree*` entries whose reader low-water has cleared.
    ///
    /// The io MUST be pool-coherent (the engine's `BufferPoolPageSource`):
    /// retired pages may still have resident frames, and the free-list link
    /// write must land in the frame (marking it dirty) so subsequent
    /// pool-coherent `allocate` link reads observe it. Writing the link
    /// through a raw backing source under a resident frame corrupts the
    /// free list on the next pop (F8).
    pub(crate) fn drain_free_queue_with_retired(&self, io: &dyn PageSource) -> Result<usize> {
        self.ensure_not_frozen()?;
        let fence = self.page_lifetime_checkpoint_fence();
        let mut entries = self.inner.page_lifetime_queue.take_eligible(fence);
        entries.extend(
            self.inner
                .page_lifetime_queue
                .take_eligible_retired(fence, || self.retired_page_reader_floor()),
        );
        self.free_drained_entries(entries, io)
    }

    /// Shared free loop for the two drains: frees each entry's page to the
    /// allocator (rechecking overflow refcounts), requeues survivors, and
    /// refreshes the depth gauge.
    fn free_drained_entries(
        &self,
        entries: Vec<PageLifetimeEntry>,
        io: &dyn PageSource,
    ) -> Result<usize> {
        if entries.is_empty() {
            crate::mvcc::metrics::set_deferred_free_queue_depth(
                self.inner.page_lifetime_queue.depth() as u64,
            );
            return Ok(0);
        }

        let mut state = self.lock_mutable_state()?;
        let mut freed = 0usize;
        let mut requeue = Vec::new();

        for entry in entries {
            let page = entry.page();
            match entry.kind() {
                PageLifetimeKind::RetiredTree4k => {
                    let (header, scratch) = state.allocator_parts();
                    let mut alloc = PageAllocator::new(header, io, scratch);
                    alloc.free_4k(page)?;
                    freed += 1;
                }
                // Retired 32 KiB pages share the overflow free path; the
                // refcount recheck is defense-in-depth (entries were
                // enqueued only at refcount 0 and `try_incref_live_overflow`
                // never resurrects a zeroed chain).
                PageLifetimeKind::OverflowDeferredFree | PageLifetimeKind::RetiredTree32k => {
                    if self.overflow_refcount(page) == 0 {
                        let (header, scratch) = state.allocator_parts();
                        let mut alloc = PageAllocator::new(header, io, scratch);
                        alloc.free_32k(page)?;
                        // Drop the refcount entry — the page is no longer live.
                        #[allow(clippy::unwrap_used)]
                        let mut table = self.inner.overflow_refcounts.lock().unwrap();
                        table.remove(&page);
                        freed += 1;
                        if entry.kind() == PageLifetimeKind::OverflowDeferredFree {
                            crate::mvcc::metrics::record_overflow_page_freed();
                        }
                    } else {
                        requeue.push(entry);
                    }
                }
            }
        }

        if freed > 0 {
            state.header_dirty = true;
        }
        drop(state);

        for entry in requeue {
            self.inner.page_lifetime_queue.push_entry(entry);
        }
        crate::mvcc::metrics::set_deferred_free_queue_depth(
            self.inner.page_lifetime_queue.depth() as u64
        );
        Ok(freed)
    }

    /// Drain the deferred-free queue but hand the 0-refcount pages to the
    /// caller as a plain `Vec<u32>` rather than freeing them to the allocator.
    ///
    /// Structural batches use this so drained pages become lifetime-owned
    /// pending frees. Commit translates each drained page into a proper
    /// `free_*` call; rollback pushes the page back onto the queue,
    /// preserving the "concurrent readers must observe refcount before free"
    /// invariant.
    ///
    /// Refcount recheck uses Acquire ordering (matches `drain_free_queue`).
    /// Entries whose refcount is still non-zero are re-enqueued.
    ///
    /// `RetiredTree*` entries are never returned here: structural batches
    /// free drained pages as 32 KiB on commit and re-enqueue them as
    /// overflow entries on abort, both of which would lose the retired
    /// entries' size class and reader low-water gate. Retired pages are
    /// only released by `drain_free_queue_with_retired` (checkpoint path,
    /// pool-coherent io); `take_eligible` never yields them.
    ///
    /// Precondition: caller holds writer serialization (same as
    /// `drain_free_queue`).
    pub(crate) fn drain_deferred_free_pages(&self) -> Vec<u32> {
        let entries = self
            .inner
            .page_lifetime_queue
            .take_eligible(self.page_lifetime_checkpoint_fence());
        if entries.is_empty() {
            crate::mvcc::metrics::set_deferred_free_queue_depth(
                self.inner.page_lifetime_queue.depth() as u64,
            );
            return Vec::new();
        }
        let mut ready = Vec::new();
        let mut requeue = Vec::new();
        for entry in entries {
            if entry.kind() != PageLifetimeKind::OverflowDeferredFree {
                requeue.push(entry);
                continue;
            }
            let page = entry.page();
            if self.overflow_refcount(page) == 0 {
                // Drop the refcount entry — the page is no longer live.
                #[allow(clippy::unwrap_used)]
                let mut table = self.inner.overflow_refcounts.lock().unwrap();
                table.remove(&page);
                ready.push(page);
            } else {
                requeue.push(entry);
            }
        }
        for entry in requeue {
            self.inner.page_lifetime_queue.push_entry(entry);
        }
        crate::mvcc::metrics::set_deferred_free_queue_depth(
            self.inner.page_lifetime_queue.depth() as u64
        );
        ready
    }
}
