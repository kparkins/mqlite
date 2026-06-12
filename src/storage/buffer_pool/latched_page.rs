//! [`LatchedPinnedPage`] — the pin-plus-latch RAII handle and the
//! resident MVCC chain-mutation / snapshot surface that runs under the
//! page-local latch, plus the off-latch chain-flip helpers.

use std::collections::{BTreeMap, VecDeque};
use std::marker::PhantomData;
use std::sync::{atomic::Ordering, Arc};

use crate::error::{Error, Result};
use crate::mvcc::chain_snapshot::ChainSnapshot;
use crate::mvcc::read_view::ReadView;
use crate::mvcc::version::{VersionEntry, VersionState};
use crate::mvcc::Ts;
use crate::storage::page::{LEAF_HEADER_SIZE, PAGE_SIZE_LEAF};

use super::chains;
use super::page_latch::{LatchMode, PageLatchExclusive, PageLatchShared};
use super::partition::Frame;
use super::{BufferPool, PageSize};

/// Internal latch hold for [`LatchedPinnedPage`]. The variant chosen on
/// construction matches the [`LatchMode`] requested by the caller; on drop
/// the embedded guard is released BEFORE the pin (the latch-before-pin
/// drop-order rule: a frame must stay pinned until its latch is released,
/// otherwise the slot could be evicted out from under a thread that still
/// holds the latch). The guards are inhabited only for their `Drop` side
/// effect (latch release); the lint allow keeps the type-level wrapper
/// expressive without a compiler warning about the unread payload.
#[allow(dead_code)]
pub(super) enum LatchHold<'pool> {
    Shared(PageLatchShared<'pool>),
    Exclusive(PageLatchExclusive<'pool>),
}

/// Wrapper around a [`LatchHold`] that ties the test-only
/// `EVENT_LATCH_RELEASE` event to the *actual* moment the underlying
/// `parking_lot` guard is dropped, so the drop-order test observes the
/// real unlock rather than a recording line that could drift from it.
///
/// `Drop` first consumes `inner`, which runs the wrapped guard's
/// destructor and physically unlocks the `PageLatch`. Only after that
/// unlock has happened does the test probe record. A future refactor
/// that reordered drop versus recording would also reorder the
/// observable side effect: callers cannot mask a regression by moving
/// recording lines without also moving the actual unlock.
pub(super) struct LatchHoldRecorder<'pool> {
    inner: Option<LatchHold<'pool>>,
}

impl<'pool> LatchHoldRecorder<'pool> {
    pub(super) fn new(hold: LatchHold<'pool>) -> Self {
        Self { inner: Some(hold) }
    }
}

impl Drop for LatchHoldRecorder<'_> {
    fn drop(&mut self) {
        // Step 1 — physically drop the inner guard. parking_lot's
        // `RwLockReadGuard` / `RwLockWriteGuard` releases its lock at
        // this `drop` call.
        drop(self.inner.take());
        // Step 2 — record the latch-release event AFTER the unlock has
        // actually happened. In production the line is a no-op.
        #[cfg(test)]
        super::latched_pinned_page_drop_order::record_drop_event(
            super::latched_pinned_page_drop_order::EVENT_LATCH_RELEASE,
        );
    }
}

/// Pin-plus-latch RAII handle.
///
/// The sole legal way to hold both a buffer-pool pin and a `PageLatch`
/// simultaneously. Construction is via [`BufferPool::pin_for_read`] or
/// [`BufferPool::pin_for_write`]; the partition mutex is acquired, the
/// pin is bumped, the partition mutex is released, and only then is the
/// page-local latch acquired. Drop reverses that order: latch first,
/// pin second — releasing the pin first would let CLOCK evict the frame
/// while the latch is still held.
///
/// `LatchedPinnedPage` is `!Send` (the `_not_send: PhantomData<*const ()>`
/// marker rejects cross-thread transfer). The handle borrows from the
/// buffer pool and the `parking_lot` guard inside [`LatchHold`] is
/// thread-pinned by the underlying `parking_lot::RwLock`.
#[allow(dead_code)]
pub(crate) struct LatchedPinnedPage<'pool> {
    /// Buffer pool reference used by `Drop` to call back into
    /// `unpin_internal`. Tied to the same lifetime as the latch hold.
    pub(super) pool: &'pool BufferPool,
    /// Frame pointer — stable while `pin_count > 0` because CLOCK
    /// eviction skips pinned frames and the partition slot vector is
    /// pre-allocated (no reallocation moves frames).
    pub(super) frame_ptr: *const Frame,
    /// Page id (page number) wrapped by this handle.
    pub(super) page_id: u32,
    /// Page-size partition that owns this frame (4 KiB or 32 KiB);
    /// recorded so `Drop` can re-enter the correct partition mutex
    /// when releasing the pin.
    pub(super) page_size: PageSize,
    /// Mode in which the page-local latch is currently held.
    pub(super) latch_mode: LatchMode,
    /// Live latch hold; taken (`None`) by `Drop` before the pin is
    /// released so the latch is dropped strictly first (releasing the pin
    /// first would expose the frame to eviction while still latched).
    /// Wrapped in [`LatchHoldRecorder`] so the test-only release event
    /// fires AFTER the underlying guard is physically unlocked.
    pub(super) latch_hold: Option<LatchHoldRecorder<'pool>>,
    /// `*const ()` marker to make the handle `!Send` — page latches are
    /// thread-pinned in production (`parking_lot::RwLock` guards keep
    /// the acquiring thread on the lock owner list).
    pub(super) _not_send: PhantomData<*const ()>,
}

impl<'pool> LatchedPinnedPage<'pool> {
    /// Page id (page number) this handle pins.
    #[allow(dead_code)]
    pub(crate) fn page_id(&self) -> u32 {
        self.page_id
    }

    /// Mode in which the page-local latch is currently held.
    #[allow(dead_code)]
    pub(crate) fn latch_mode(&self) -> LatchMode {
        self.latch_mode
    }

    /// Clone the current page-byte snapshot while this handle holds the
    /// page-local latch.
    #[allow(
        dead_code,
        reason = "public classifier uses this narrow latch read path"
    )]
    pub(crate) fn data_snapshot(&self) -> Arc<Vec<u8>> {
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted while the snapshot Arc is loaded.
        let frame = unsafe { &*self.frame_ptr };
        frame.data.load_full()
    }

    /// Copy resident delta chains while holding `LatchedPinnedPage::Shared`.
    ///
    /// This is a copies/clones only snapshot path: it never mutates the
    /// resident chain map and never acquires a buffer-pool partition mutex
    /// while the page latch is held.
    #[allow(dead_code)]
    pub(crate) fn snapshot_chains(&self, view: Option<Arc<ReadView>>) -> Result<ChainSnapshot> {
        if self.latch_mode != LatchMode::Shared {
            return Err(Error::Internal(
                "LatchedPinnedPage::snapshot_chains requires a shared page latch".into(),
            ));
        }
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The shared page latch prevents concurrent writers while
        // `ChainSnapshot::new` clones the map entries.
        let frame = unsafe { &*self.frame_ptr };
        Ok(ChainSnapshot::new(&frame.deltas, view))
    }

    /// Copy only the resident delta chain for `key` while holding the
    /// reader-side page latch.
    pub(crate) fn snapshot_chain_for_key(
        &self,
        key: &[u8],
        view: Option<Arc<ReadView>>,
    ) -> Result<ChainSnapshot> {
        if self.latch_mode != LatchMode::Shared {
            return Err(Error::Internal(
                "LatchedPinnedPage::snapshot_chain_for_key requires a shared page latch".into(),
            ));
        }
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The shared page latch prevents concurrent writers while
        // the single resident chain is cloned.
        let frame = unsafe { &*self.frame_ptr };
        Ok(ChainSnapshot::new_for_key(&frame.deltas, key, view))
    }

    /// Return the current live chain head for `key`.
    ///
    /// Aborted entries are ignored. Foreign pending entries still count as
    /// live heads for first-committer-wins checks.
    pub(crate) fn live_head(&self, key: &[u8]) -> Option<VersionEntry> {
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The page latch held by this handle serializes access to
        // the frame-local delta map for latch-aware callers.
        let frame = unsafe { &*self.frame_ptr };
        frame
            .deltas
            .get(key)
            .and_then(|chain| chain.iter().find(|entry| entry.is_live_head()).cloned())
    }

    /// Return true when this page carries a pending entry for `txn_id`.
    pub(crate) fn has_pending_txn(&self, txn_id: u64) -> bool {
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The shared or exclusive page latch serializes access to
        // the frame-local delta map while this read walks the chains.
        let frame = unsafe { &*self.frame_ptr };
        frame.deltas.values().any(|chain| {
            chain.iter().any(
                |entry| matches!(entry.state, VersionState::Pending { txn_id: id } if id == txn_id),
            )
        })
    }

    /// Return the chain for `key`, or an empty caller-owned chain.
    pub(crate) fn get_or_create_chain(&self, key: &[u8]) -> Result<Arc<VecDeque<VersionEntry>>> {
        // This helper is used by exclusive install paths, even though it only
        // reads. Requiring exclusive keeps the install classifier and
        // mutation under one latch hold.
        self.require_exclusive("get_or_create_chain")?;
        // SAFETY: see `expected_head`.
        let frame = unsafe { &*self.frame_ptr };
        Ok(frame
            .deltas
            .get(key)
            .cloned()
            .unwrap_or_else(|| Arc::new(VecDeque::new())))
    }

    /// Return true when this leaf's resident delta map has a live key in range.
    pub(crate) fn has_live_delta_key_in_range(
        &self,
        start: &[u8],
        end: &[u8],
        exclude_key: &[u8],
    ) -> Result<bool> {
        self.require_exclusive("has_live_delta_key_in_range")?;
        // SAFETY: see `expected_head`; the exclusive page latch serializes
        // delta-map access for the install-time unique-prefix scan.
        let frame = unsafe { &*self.frame_ptr };
        for (key, chain) in frame.deltas.range(start.to_vec()..end.to_vec()) {
            if key.as_slice() == exclude_key {
                continue;
            }
            if chain.iter().any(VersionEntry::is_live_head) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Read-modify-write the chain slot for `key` while holding this
    /// page's exclusive latch.
    ///
    /// The closure receives `&mut Option<Arc<...>>` — `None` when the
    /// frame currently has no chain for `key`. The closure may take,
    /// replace, or leave the slot. After it returns, the slot is written
    /// back into the frame's `deltas` map (insert if `Some`, leave
    /// removed if `None`).
    ///
    /// This is the canonical chain mutator surface: every chain mutation
    /// flows through this method (or the all-chains variant) so that the
    /// exclusive page latch is always held across the read-modify-write and
    /// the per-frame live-byte running sum stays consistent. The
    /// `pub(super)` `take_chain_locked` / `put_chain_locked` helpers in
    /// `chains.rs` exist only to back the older non-latch-aware free
    /// functions on `BufferPool`.
    pub(crate) fn with_chain<R>(
        &mut self,
        key: &[u8],
        f: impl FnOnce(&mut Option<Arc<VecDeque<VersionEntry>>>) -> R,
    ) -> Result<R> {
        self.require_exclusive("with_chain")?;
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The exclusive page latch serializes delta-map mutation.
        let frame = unsafe { &mut *self.frame_ptr.cast_mut() };
        let mut slot = frame.deltas.remove(key);
        // Leaf-budget running sum: snapshot the chain's live-head byte
        // contribution BEFORE the closure runs so we can compute the
        // per-key delta post-mutation. `before` is 0 when the chain
        // didn't exist.
        let before = slot
            .as_ref()
            .map(|chain| chains::chain_live_head_bytes(key, chain))
            .unwrap_or(0);
        let result = f(&mut slot);
        let after = slot
            .as_ref()
            .map(|chain| chains::chain_live_head_bytes(key, chain))
            .unwrap_or(0);
        if let Some(chain) = slot {
            frame.deltas.insert(key.to_vec(), chain);
        }
        // Leaf-budget running sum: signed delta accumulation via wrapping
        // arithmetic. The cache value is non-negative in absolute terms
        // over the frame's lifetime (it tracks bytes that exist on the
        // frame), but per-mutation deltas can be negative when a chain
        // shrinks via tombstone or removal.
        let cur = frame.live_delta_payload_bytes.load(Ordering::Acquire);
        let next = cur.wrapping_add(after).wrapping_sub(before);
        frame
            .live_delta_payload_bytes
            .store(next, Ordering::Release);
        Ok(result)
    }

    /// Read-modify-write the entire `deltas` map for this page while
    /// holding the exclusive latch.
    ///
    /// Used by the leaf-merge migration path to drain all chains and by
    /// the overflow-page repurpose path to clear inherited chains. The
    /// closure can mutate the map however it likes (insert / remove /
    /// drain).
    pub(crate) fn with_all_chains<R>(
        &mut self,
        f: impl FnOnce(&mut BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>) -> R,
    ) -> Result<R> {
        self.require_exclusive("with_all_chains")?;
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The exclusive page latch serializes delta-map mutation.
        let frame = unsafe { &mut *self.frame_ptr.cast_mut() };
        let result = f(&mut frame.deltas);
        // Leaf-budget running sum: arbitrary mutation through the closure
        // forces a full recompute. Used only by leaf-merge migration /
        // overflow repurpose paths — both rare events.
        let total = chains::frame_live_delta_payload_bytes(&frame.deltas);
        frame
            .live_delta_payload_bytes
            .store(total, Ordering::Release);
        Ok(result)
    }

    /// Return true when live resident deltas no longer fit one folded leaf.
    ///
    /// This is an O(1) read of the per-frame running-sum cache rather than
    /// a whole-frame scan that walks every chain and sums
    /// `chain_live_head_bytes`. The cache is kept current by every mutator
    /// that touches the frame's chains: `with_chain` / `with_all_chains` /
    /// `replace_leaf_and_chains` / `reconcile_frame_at`.
    pub(crate) fn live_delta_payload_exceeds_leaf_budget(&self) -> Result<bool> {
        self.require_exclusive("live_delta_payload_exceeds_leaf_budget")?;
        #[cfg(feature = "perf-counters")]
        let _start = std::time::Instant::now();
        // SAFETY: this handle owns a live pin, so the frame slot cannot be
        // evicted. The exclusive page latch serializes delta-map mutation;
        // the Acquire-load pairs with the Release-store in every cache
        // updater (defense-in-depth — the latch already serializes us).
        let frame = unsafe { &*self.frame_ptr };
        let cached = frame.live_delta_payload_bytes.load(Ordering::Acquire);
        let result = LEAF_HEADER_SIZE as u64 + cached > PAGE_SIZE_LEAF as u64;
        #[cfg(feature = "perf-counters")]
        {
            let elapsed_ns = _start.elapsed().as_nanos() as u64;
            chains::LIVE_DELTA_CHECK_NS_TOTAL.fetch_add(elapsed_ns, Ordering::Relaxed);
            chains::LIVE_DELTA_CHECK_CALLS.fetch_add(1, Ordering::Relaxed);
        }
        Ok(result)
    }

    /// Return the keys on this page whose chain has a `Pending(txn_id)`
    /// entry. This drives the selective copy-on-write commit path: only
    /// chains in the returned set get `Arc::make_mut` + state-flip work,
    /// instead of iterating every chain on the frame.
    ///
    /// Works under either shared or exclusive latch: the caller decides
    /// which mode is appropriate. The lock-free snapshot phase of the
    /// commit path uses `Shared`; callers that already hold `Exclusive`
    /// can also use this method.
    pub(crate) fn pending_keys_for_txn(&self, txn_id: u64) -> Vec<Vec<u8>> {
        // SAFETY: this handle owns a live pin, so the frame slot cannot
        // be evicted. The page latch (shared or exclusive) prevents
        // concurrent writers from mutating `frame.deltas` while we walk
        // it to identify pending keys.
        let frame = unsafe { &*self.frame_ptr };
        frame
            .deltas
            .iter()
            .filter_map(|(key, chain)| {
                let has_pending = chain.iter().any(|entry| {
                    matches!(entry.state, VersionState::Pending { txn_id: id } if id == txn_id)
                });
                has_pending.then(|| key.clone())
            })
            .collect()
    }

    /// Clone the `Arc` for the chain at `key`, if any. Reader-friendly
    /// snapshot used by the prepare phase of the selective copy-on-write
    /// commit: the caller drops the latch after collecting these
    /// snapshots, then operates on local clones without holding the page
    /// locked, and re-validates pointer identity under the latch before
    /// installing.
    pub(crate) fn snapshot_chain_arc(&self, key: &[u8]) -> Option<Arc<VecDeque<VersionEntry>>> {
        // SAFETY: this handle owns a live pin and the page latch (shared
        // or exclusive); both keep `frame.deltas` stable for the
        // duration of the lookup.
        let frame = unsafe { &*self.frame_ptr };
        frame.deltas.get(key).cloned()
    }

    /// Install phase of the selective copy-on-write commit: for each
    /// prepared `(key, new_arc, expected_old_arc)`, verify that
    /// `Arc::ptr_eq(frame.deltas[key], expected_old_arc)` still holds,
    /// then atomically install the new chains.
    ///
    /// Returns [`SwapOutcome::Success`] when ALL prepared entries
    /// matched and were installed. Returns [`SwapOutcome::Conflict`]
    /// when ANY entry's expected-old `Arc` no longer matches the
    /// resident chain — meaning another committer raced in between the
    /// off-latch prepare and now — in which case NO frame mutation has
    /// happened and the caller should re-snapshot and retry. The
    /// verify-all-then-install two-pass shape is what makes the swap
    /// atomic per page: a conflict on any key aborts before any key is
    /// written.
    pub(crate) fn try_swap_chains_if_unchanged(
        &mut self,
        prepared: Vec<PreparedChainSwap>,
    ) -> Result<SwapOutcome> {
        self.require_exclusive("try_swap_chains_if_unchanged")?;
        // SAFETY: pin keeps the frame resident; exclusive latch
        // serializes delta-map mutation.
        let frame = unsafe { &mut *self.frame_ptr.cast_mut() };
        // First pass: verify ALL expected-old chains still match.
        for swap in &prepared {
            match frame.deltas.get(swap.key.as_slice()) {
                Some(current) if Arc::ptr_eq(current, &swap.expected_old) => {}
                _ => return Ok(SwapOutcome::Conflict),
            }
        }
        // Second pass: install. We checked every entry first; this
        // loop cannot observe a conflict it did not see in pass one.
        //
        // Leaf-budget running-sum invariant: commit flips are usually
        // byte-neutral (`Pending` and `Committed` both count as live).
        // Abort flips are not byte-neutral because `Aborted` heads are
        // filtered out by `chain_live_head_bytes`. Keep the running sum in
        // sync by applying the same per-key byte delta as `with_chain`.
        let mut live_delta_payload_bytes = frame.live_delta_payload_bytes.load(Ordering::Acquire);
        for swap in prepared {
            let before = chains::chain_live_head_bytes(&swap.key, &swap.expected_old);
            let after = chains::chain_live_head_bytes(&swap.key, &swap.new_chain);
            live_delta_payload_bytes = live_delta_payload_bytes
                .wrapping_add(after)
                .wrapping_sub(before);
            frame.deltas.insert(swap.key, swap.new_chain);
        }
        frame
            .live_delta_payload_bytes
            .store(live_delta_payload_bytes, Ordering::Release);
        #[cfg(debug_assertions)]
        {
            let fresh = chains::frame_live_delta_payload_bytes(&frame.deltas);
            debug_assert_eq!(
                live_delta_payload_bytes, fresh,
                "Phase B swap must preserve live_delta_payload_bytes invariant"
            );
        }
        Ok(SwapOutcome::Success)
    }

    pub(super) fn require_exclusive(&self, operation: &str) -> Result<()> {
        if self.latch_mode == LatchMode::Exclusive {
            return Ok(());
        }
        Err(Error::Internal(format!(
            "LatchedPinnedPage::{operation} requires an exclusive page latch"
        )))
    }
}

/// Install-phase input for one chain swap prepared off-latch.
///
/// `expected_old` is the `Arc` clone snapshotted from the frame during
/// the off-latch prepare phase; `new_chain` is the locally copy-on-write'd
/// flipped chain. The install phase writes `new_chain` only when the
/// resident chain at `key` is still `Arc::ptr_eq` to `expected_old` —
/// proof that no other committer / aborter raced between the two phases.
#[derive(Clone)]
pub(crate) struct PreparedChainSwap {
    pub(crate) key: Vec<u8>,
    pub(crate) new_chain: Arc<VecDeque<VersionEntry>>,
    pub(crate) expected_old: Arc<VecDeque<VersionEntry>>,
}

/// Install-phase outcome for the selective copy-on-write commit.
///
/// `Conflict` is recoverable — the caller's bounded retry loop in
/// `flip_pending_to_committed_for` re-snapshots the chains and tries
/// again. `Success` means every prepared chain was installed atomically.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SwapOutcome {
    Success,
    Conflict,
}

/// Flip every `Pending(txn_id)` entry in `chain` to `Committed`
/// (when `commit_ts` is `Some`) or `Aborted` (when `None`). Returns
/// the number of entries flipped.
///
/// Operates on a chain in isolation so the selective copy-on-write
/// commit can call it on a locally-cloned chain (off-frame, with no
/// page latch held while the flip runs) and install the result later.
///
/// On abort, restores the previous live head's `stop_ts` to `Ts::MAX`
/// via `restore_previous_head_after_abort`, so aborting a pending write
/// re-exposes the version it had superseded. This function is infallible
/// (it mutates the caller's local chain clone and returns the flip count);
/// it never reads or writes the resident frame directly. The abort-restore
/// cannot clobber a concurrently-installed newer head: the caller
/// (`flip_pending_one_page`, Phase A/B) runs this on an off-latch clone and
/// installs the result through `try_swap_chains_if_unchanged`, whose
/// `Arc::ptr_eq` compare-and-swap reports `Conflict` (mutating nothing) if
/// the resident chain changed — the per-page ABA guard. Pinned by
/// `item2_abort_restore_cannot_clobber_newer_head_via_swap_cas`. Whether a
/// swallowed abort-flip *error* (the `let _ = flip_pending_to_aborted_for`
/// in `paged_engine/commit_envelope.rs`) should escalate to `engine_fatal`
/// is a decision owned by that caller, not by this primitive.
pub(crate) fn flip_pending_in_chain(
    chain: &mut VecDeque<VersionEntry>,
    txn_id: u64,
    commit_ts: Option<Ts>,
) -> usize {
    let mut flipped = 0usize;
    for idx in 0..chain.len() {
        let pending_start_ts = match chain.get(idx) {
            Some(entry)
                if matches!(
                    entry.state,
                    VersionState::Pending { txn_id: pending } if pending == txn_id
                ) =>
            {
                entry.start_ts
            }
            _ => continue,
        };
        let mut restore_after_abort = false;
        if let Some(entry) = chain.get_mut(idx) {
            match commit_ts {
                Some(ts) => {
                    entry.start_ts = ts;
                    entry.state = VersionState::Committed;
                }
                None => {
                    entry.state = VersionState::Aborted;
                    restore_after_abort = true;
                }
            }
            flipped += 1;
        }
        if restore_after_abort {
            restore_previous_head_after_abort(chain, idx, pending_start_ts);
        }
    }
    flipped
}

fn restore_previous_head_after_abort(
    chain: &mut VecDeque<VersionEntry>,
    aborted_idx: usize,
    aborted_start_ts: Ts,
) {
    if let Some(prev) = chain.iter_mut().skip(aborted_idx + 1).find(|entry| {
        !matches!(entry.state, VersionState::Aborted) && entry.stop_ts == aborted_start_ts
    }) {
        prev.stop_ts = Ts::MAX;
    }
}

impl Drop for LatchedPinnedPage<'_> {
    fn drop(&mut self) {
        // Latch-before-pin drop order — release the latch BEFORE
        // releasing the pin, so the frame stays pinned (eviction-safe)
        // until no thread holds its latch. The recorder wrapper makes the
        // latch-release event fire only after the underlying parking_lot
        // guard has unlocked, so anybody who reorders this `drop(recorder)`
        // line and the `unpin_internal` call below will see the event
        // order flip too — the test asserts that order.
        debug_assert!(
            self.latch_hold.is_some(),
            "LatchedPinnedPage::drop: latch_hold must be Some on entry; \
             releasing the pin while the latch is still held would violate \
             §10.18 rule 2 (latch-before-pin drop order)",
        );
        let recorder = self.latch_hold.take();
        // Dropping `recorder` runs `LatchHoldRecorder::drop`, which
        // physically releases the latch THEN records the event.
        drop(recorder);
        debug_assert!(
            self.latch_hold.is_none(),
            "LatchedPinnedPage::drop: latch_hold must be released before \
             the pin (§10.18 rule 2)",
        );
        // Drop must not panic; swallow the unpin error like `PinnedPage`.
        let _ = self
            .pool
            .unpin_internal(self.page_id, self.page_size, false, None);
        // Pin-release event fires AFTER `unpin_internal` returns, so
        // the recorded order matches the actual side-effect order.
        #[cfg(test)]
        super::latched_pinned_page_drop_order::record_drop_event(
            super::latched_pinned_page_drop_order::EVENT_PIN_RELEASE,
        );
    }
}

impl BufferPool {
    /// Pin a leaf page in exclusive latch mode, run `f` against its
    /// chain slot for `key`, and release the pin+latch.
    ///
    /// Canonical chain-slot mutator. Every production callsite that
    /// today reaches into `chains::take_chain` / `put_chain` should
    /// migrate to this entry point so per-page latch invariants hold.
    /// `mode` must be [`LatchMode::Exclusive`] — shared callers should
    /// use `pin_for_read_sized` + the snapshot APIs on
    /// [`LatchedPinnedPage`] instead. The mode parameter is preserved
    /// to mirror the trait signature on [`BTreePageStore`] but is
    /// validated runtime-side via `require_exclusive` inside
    /// [`LatchedPinnedPage::with_chain`].
    pub(crate) fn with_chain_under_latch<R>(
        &self,
        page: u32,
        key: &[u8],
        mode: LatchMode,
        f: impl FnOnce(&mut Option<Arc<VecDeque<VersionEntry>>>) -> R,
    ) -> Result<R> {
        let mut latched = self.pin_then_latch(page, PageSize::Large32k, mode)?;
        latched.with_chain(key, f)
    }

    /// Pin a leaf page in exclusive latch mode, run `f` against its
    /// entire chain map, and release the pin+latch.
    ///
    /// Companion to [`Self::with_chain_under_latch`] for callers that
    /// must drain or clear every chain on the page (leaf merge,
    /// overflow-page repurpose).
    pub(crate) fn with_all_chains_under_latch<R>(
        &self,
        page: u32,
        mode: LatchMode,
        f: impl FnOnce(&mut BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>) -> R,
    ) -> Result<R> {
        let mut latched = self.pin_then_latch(page, PageSize::Large32k, mode)?;
        latched.with_all_chains(f)
    }
}
