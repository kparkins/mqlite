//! Version-chain primitives — `OverflowRef` RAII handle and `VersionEntry`.
//!
//! Central invariant: every live
//! `OverflowRef` corresponds to one refcount on its `first_page`. This is
//! enforced structurally — the type is not `Copy`, not `#[derive(Clone)]`.
//! The explicit `Clone` impl bumps the refcount via the allocator's
//! saturating CAS loop; the `Drop` impl atomically decrefs and, if the
//! post-decrement count is 0, enqueues the page to the allocator's
//! page-lifetime queue. Actual free is deferred to the writer path.
//!
//! This module never calls the allocator's state mutex directly — atomic
//! refcount ops happen lock-free on the shared `AtomicU32` handles. See
//! `AllocatorHandle::incref_overflow` etc.

use crate::error::Result;
use crate::mvcc::timestamp::Ts;
use crate::storage::allocator::AllocatorHandle;

// ---------------------------------------------------------------------------
// OverflowRef — RAII refcount handle for an overflow chain
// ---------------------------------------------------------------------------

/// A reference to an overflow chain rooted at `first_page`.
///
/// Owns EXACTLY ONE refcount on `first_page` for its lifetime. The type is
/// deliberately not `Copy` and does not derive `Clone` — constructing a
/// new handle must go through the explicit `Clone` impl, which bumps the
/// refcount via the allocator's CAS-loop incref.
///
/// Safety invariant: every live `OverflowRef` (in any location — chain
/// VecDeque, `ChainSnapshot`, `WriteTxn.pending`, pins) corresponds to one
/// refcount on its `first_page`.
pub struct OverflowRef {
    first_page: u32,
    total_length: u64,
    allocator: AllocatorHandle,
}

impl OverflowRef {
    /// Construct a new `OverflowRef` for a freshly-written overflow chain.
    ///
    /// This is the single entry point that bumps the refcount from 0 → 1.
    /// All other construction goes through `Clone`. Returns
    /// `Err(Error::RefcountOverflow)` only in the pathological saturation
    /// case (unreachable when called on a newly-allocated chain).
    ///
    /// `pub(crate)` because `AllocatorHandle` is `pub(crate)` — the
    /// signature is unreachable from outside the crate regardless.
    pub(crate) fn new_owned(
        first_page: u32,
        total_length: u64,
        allocator: AllocatorHandle,
    ) -> Result<Self> {
        allocator.incref_overflow(first_page)?;
        Ok(Self {
            first_page,
            total_length,
            allocator,
        })
    }

    /// Construct an `OverflowRef` that takes logical ownership of an
    /// already-held refcount slot on `first_page` WITHOUT bumping the
    /// refcount. The caller asserts the underlying refcount is `>= 1`
    /// (typically: the entry is materialized from a persisted history-store
    /// tree cell whose insertion never dropped its producer's
    /// `OverflowRef`). On `Drop`, the standard RAII decref runs and the
    /// page is enqueued for deferred free if the post-decrement refcount
    /// is 0. This is the canonical entry point for the history-store GC
    /// path — `src/storage/history_store.rs::HistoryStore::gc_pass`.
    pub(crate) fn from_existing_refcount(
        first_page: u32,
        total_length: u64,
        allocator: AllocatorHandle,
    ) -> Self {
        debug_assert!(
            allocator.overflow_refcount(first_page) >= 1,
            "from_existing_refcount on first_page {first_page} with refcount < 1"
        );
        Self {
            first_page,
            total_length,
            allocator,
        }
    }

    /// Page number of the first page in the chain.
    #[must_use]
    pub fn first_page(&self) -> u32 {
        self.first_page
    }

    /// Total payload length across the entire chain.
    #[must_use]
    pub fn total_length(&self) -> u64 {
        self.total_length
    }

    /// Clone this overflow reference only if the underlying record is live.
    ///
    /// Returns `None` when the allocator no longer has a refcount slot for the
    /// record, or when the slot has already reached zero. In that case the
    /// method leaves the refcount unchanged so a dropped overflow chain cannot
    /// be resurrected during ownership transfer.
    #[must_use]
    pub fn try_clone(&self) -> Option<Self> {
        match self.allocator.try_incref_live_overflow(self.first_page) {
            Ok(Some(_)) => Some(Self {
                first_page: self.first_page,
                total_length: self.total_length,
                allocator: self.allocator.clone(),
            }),
            Ok(None) | Err(_) => None,
        }
    }
}

impl std::fmt::Debug for OverflowRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OverflowRef")
            .field("first_page", &self.first_page)
            .field("total_length", &self.total_length)
            .finish_non_exhaustive()
    }
}

impl Clone for OverflowRef {
    #[allow(
        clippy::panic,
        reason = "Clone is infallible; cloning a dropped overflow ref is an invariant breach"
    )]
    fn clone(&self) -> Self {
        // A live `OverflowRef` holds ≥ 1 refcount, so this incref can only
        // fail if another bug has already dropped the underlying record.
        // `Clone` is infallible by trait contract, so retain the live-path
        // behavior and panic instead of resurrecting a zero-refcount record.
        match self.try_clone() {
            Some(cloned) => cloned,
            None => panic!("OverflowRef::clone called after the underlying record was dropped"),
        }
    }
}

impl Drop for OverflowRef {
    fn drop(&mut self) {
        let post = self.allocator.decref_overflow(self.first_page);
        if post == 0 {
            self.allocator
                .enqueue_overflow_deferred_free(self.first_page);
        }
    }
}

// ---------------------------------------------------------------------------
// VersionEntry — one link in an in-memory version chain
// ---------------------------------------------------------------------------

/// Payload of a version entry: inline bytes or a refcounted overflow chain.
///
/// Cloning a `VersionData::Overflow(_)` runs the `OverflowRef::Clone`
/// incref path, preserving the refcount ↔ live-handle invariant.
#[derive(Debug)]
pub enum VersionData {
    /// Inline payload that fits in a leaf cell.
    Inline(Vec<u8>),
    /// Payload stored in a refcounted overflow chain.
    Overflow(OverflowRef),
}

impl Clone for VersionData {
    fn clone(&self) -> Self {
        match self {
            VersionData::Inline(v) => VersionData::Inline(v.clone()),
            VersionData::Overflow(r) => VersionData::Overflow(r.clone()),
        }
    }
}

impl VersionData {
    pub(crate) fn try_clone(&self) -> Option<Self> {
        match self {
            VersionData::Inline(v) => Some(VersionData::Inline(v.clone())),
            VersionData::Overflow(r) => r.try_clone().map(VersionData::Overflow),
        }
    }
}

/// Commit-state marker for a resident MVCC version.
#[derive(Clone, Copy, Debug)]
pub enum VersionState {
    /// Installed by `txn_id` but not yet published by the commit sequencer.
    Pending { txn_id: u64 },
    /// Durable commit record exists and the version is ordinarily visible
    /// according to its timestamp window.
    Committed,
    /// Writer rolled back or conflict-aborted; readers skip this version.
    Aborted,
}

/// One entry in a per-key version chain.
///
/// `stop_ts == Ts::MAX` means this entry is the current head (still
/// visible to new readers). `state` identifies pending, committed, and
/// aborted entries for visibility checks.
#[derive(Debug, Clone)]
pub struct VersionEntry {
    /// Timestamp at which this version becomes visible.
    pub start_ts: Ts,
    /// Timestamp at which this version is replaced. `Ts::MAX` for the
    /// current head.
    pub stop_ts: Ts,
    /// Transaction identifier that wrote this version. Used to resolve
    /// self-visibility for pending entries.
    pub txn_id: u64,
    /// Published state for this version.
    pub state: VersionState,
    /// Payload — inline bytes or refcounted overflow chain.
    pub data: VersionData,
    /// `true` if this entry represents a deletion.
    pub is_tombstone: bool,
}

impl VersionEntry {
    pub(crate) fn try_clone(&self) -> Option<Self> {
        Some(Self {
            start_ts: self.start_ts,
            stop_ts: self.stop_ts,
            txn_id: self.txn_id,
            state: self.state,
            data: self.data.try_clone()?,
            is_tombstone: self.is_tombstone,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::storage::header::FileHeader;

    fn fresh_allocator() -> AllocatorHandle {
        AllocatorHandle::new(FileHeader::new(0, 0, 0))
    }

    #[test]
    fn overflow_ref_new_bumps_refcount_to_one() {
        let alloc = fresh_allocator();
        let r = OverflowRef::new_owned(42, 100, alloc.clone()).unwrap();
        assert_eq!(r.first_page(), 42);
        assert_eq!(r.total_length(), 100);
        assert_eq!(alloc.overflow_refcount(42), 1);
    }

    #[test]
    fn overflow_ref_clone_bumps_refcount() {
        let alloc = fresh_allocator();
        let r = OverflowRef::new_owned(42, 100, alloc.clone()).unwrap();
        assert_eq!(alloc.overflow_refcount(42), 1);

        #[allow(
            clippy::redundant_clone,
            reason = "test asserts Clone bumps the overflow refcount"
        )]
        let r2 = r.clone();
        assert_eq!(alloc.overflow_refcount(42), 2);
        assert_eq!(r2.first_page(), 42);
    }

    #[test]
    fn overflow_ref_drop_decrefs_and_enqueues_on_zero() {
        let alloc = fresh_allocator();
        let r = OverflowRef::new_owned(42, 100, alloc.clone()).unwrap();
        drop(r);
        assert_eq!(alloc.overflow_refcount(42), 0);
        assert_eq!(
            alloc.page_lifetime_queue().depth(),
            1,
            "refcount 0 drop must enqueue for deferred free"
        );
    }

    #[test]
    fn overflow_ref_drop_does_not_enqueue_when_others_live() {
        let alloc = fresh_allocator();
        let r = OverflowRef::new_owned(42, 100, alloc.clone()).unwrap();
        let r2 = r.clone();
        assert_eq!(alloc.overflow_refcount(42), 2);

        drop(r);
        assert_eq!(alloc.overflow_refcount(42), 1);
        assert_eq!(
            alloc.page_lifetime_queue().depth(),
            0,
            "must not enqueue while a live OverflowRef remains"
        );

        drop(r2);
        assert_eq!(alloc.overflow_refcount(42), 0);
        assert_eq!(alloc.page_lifetime_queue().depth(), 1);
    }

    #[test]
    fn version_data_clone_preserves_refcount_invariant() {
        let alloc = fresh_allocator();
        let r = OverflowRef::new_owned(7, 32, alloc.clone()).unwrap();
        let vd = VersionData::Overflow(r);
        assert_eq!(alloc.overflow_refcount(7), 1);

        let vd2 = vd.clone();
        assert_eq!(alloc.overflow_refcount(7), 2);

        drop(vd);
        assert_eq!(alloc.overflow_refcount(7), 1);
        drop(vd2);
        assert_eq!(alloc.overflow_refcount(7), 0);
    }

    #[test]
    fn version_entry_clone_works() {
        let alloc = fresh_allocator();
        let r = OverflowRef::new_owned(100, 1024, alloc.clone()).unwrap();
        let entry = VersionEntry {
            start_ts: Ts {
                physical_ms: 10,
                logical: 0,
            },
            stop_ts: Ts::MAX,
            txn_id: 1,
            state: VersionState::Committed,
            data: VersionData::Overflow(r),
            is_tombstone: false,
        };
        assert_eq!(alloc.overflow_refcount(100), 1);

        #[allow(
            clippy::redundant_clone,
            reason = "test asserts VersionEntry::clone bumps the overflow refcount"
        )]
        let clone = entry.clone();
        assert_eq!(alloc.overflow_refcount(100), 2);
        assert_eq!(clone.txn_id, 1);
    }
}

#[cfg(test)]
#[cfg(not(loom))]
#[path = "version_us013_tests.rs"]
mod us013_tests;
