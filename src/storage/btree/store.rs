//! B+ tree page-store abstraction and reader-path images.
//!
//! The [`BTreePageStore`] trait decouples the B+ tree logic from the concrete
//! page I/O (buffer pool + allocator). The production adapter lives in
//! `crate::storage::btree_store::BufferPoolPageStore`; tests mount an in-memory
//! store ([`super::MemPageStore`]). Reader paths carry [`LeafPageImage`] —
//! either a shared `Arc` over the published frame snapshot or an owned copy for
//! structural staged writes — and consult MVCC history through [`HistoryProbe`].

use std::collections::{BTreeMap, VecDeque};
use std::ops::Deref;
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::chain_snapshot::ChainSnapshot;
use crate::mvcc::version::VersionEntry;
use crate::storage::buffer_pool::{LatchMode, PageSize};
use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

/// Reader-path history fallthrough.
///
/// Bound to a specific durable tree identity at the call site — the BTree
/// layer only sees an opaque probe object and walks `(key, read_ts)`.
/// A `None` return means "no visible history entry"; a `Some(entry)` return
/// means the probe found the newest history version visible at `read_ts`
/// (tombstones included — the caller treats tombstones as "key absent").
pub(crate) trait HistoryProbe {
    fn probe_visible_version(
        &self,
        key: &[u8],
        read_ts: crate::mvcc::timestamp::Ts,
    ) -> Result<Option<VersionEntry>>;
}

/// Immutable 32 KiB leaf page image returned by reader paths.
///
/// Buffer-pool readers can hold the existing published `ArcSwap<Vec<u8>>`
/// snapshot without cloning the page bytes. Structural staged writes still
/// return owned images so mutable paths never edit shared frame snapshots in
/// place.
#[derive(Clone)]
pub(crate) enum LeafPageImage {
    Shared(Arc<Vec<u8>>),
    Owned(Box<[u8; PAGE_SIZE_LEAF as usize]>),
}

impl LeafPageImage {
    pub(crate) fn shared(data: Arc<Vec<u8>>) -> Result<Self> {
        if data.len() != PAGE_SIZE_LEAF as usize {
            return Err(crate::error::Error::Internal(format!(
                "leaf page image has {} bytes, expected {}",
                data.len(),
                PAGE_SIZE_LEAF
            )));
        }
        Ok(Self::Shared(data))
    }

    pub(crate) fn owned(data: Box<[u8; PAGE_SIZE_LEAF as usize]>) -> Self {
        Self::Owned(data)
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Shared(data) => data.as_slice(),
            Self::Owned(data) => data.as_slice(),
        }
    }
}

impl Deref for LeafPageImage {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

// ---------------------------------------------------------------------------
// Page store abstraction
// ---------------------------------------------------------------------------

/// Abstraction for reading, writing, allocating, and freeing B+ tree pages.
///
/// Implementors can back the store with the buffer pool + page allocator for
/// production use, or with an in-memory hash map for unit tests.
pub(crate) trait BTreePageStore {
    /// Shared page guard held by reader traversal.
    type SharedReadGuard<'a>
    where
        Self: 'a;

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
    fn read_leaf(&self, page: u32) -> Result<(LeafPageImage, Option<ChainSnapshot>)>;

    /// Pin `page` and acquire the reader-side shared page latch.
    ///
    /// Implementations without page-local latches may return a no-op guard.
    fn pin_shared_for_read<'a>(
        &'a self,
        page: u32,
        size: PageSize,
    ) -> Result<Self::SharedReadGuard<'a>>;

    /// Read an internal page while its reader guard is still live.
    fn read_internal_guarded(
        &self,
        page: u32,
        _guard: &Self::SharedReadGuard<'_>,
    ) -> Result<Box<[u8; PAGE_SIZE_INTERNAL as usize]>> {
        self.read_internal(page)
    }

    /// Read a leaf page while its reader guard is still live.
    fn read_leaf_guarded(
        &self,
        page: u32,
        _guard: &Self::SharedReadGuard<'_>,
    ) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        self.read_leaf(page)
    }

    /// Read a point-lookup leaf while its reader guard is still live.
    fn read_leaf_for_key_guarded(
        &self,
        page: u32,
        guard: &Self::SharedReadGuard<'_>,
        key: &[u8],
    ) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        let _ = key;
        self.read_leaf_guarded(page, guard)
    }

    /// Write a 4 KB internal page.
    fn write_internal(&mut self, page: u32, data: &[u8; PAGE_SIZE_INTERNAL as usize])
        -> Result<()>;

    /// Write a 32 KB leaf (or overflow) page.
    fn write_leaf_structural(
        &mut self,
        page: u32,
        data: &[u8; PAGE_SIZE_LEAF as usize],
    ) -> Result<()>;

    /// Allocate a new 4 KB internal page.  Returns the page number.
    fn alloc_internal(&mut self) -> Result<u32>;

    /// Allocate a new 32 KB leaf page.  Returns the page number.
    fn alloc_leaf(&mut self) -> Result<u32>;

    /// Return a 4 KB internal page to the free pool.
    fn free_internal(&mut self, page: u32) -> Result<()>;

    /// Return a 32 KB leaf page to the free pool.
    fn free_leaf(&mut self, page: u32) -> Result<()>;

    // -----------------------------------------------------------------------
    // MVCC version-chain accessors
    //
    // Leaf frames own per-key MVCC version chains (the live history of each
    // key, newest first). Split / merge operations migrate these chains
    // alongside the cells that own them; the `free_leaf` call sites in the
    // merge path are guarded by `chains_empty` to fail loudly if migration is
    // ever skipped — freeing a leaf whose chains were not migrated would drop
    // versions still visible to live readers.
    //
    // Every chain mutation flows through `with_chain_under_latch` /
    // `with_all_chains_under_latch`. Both pin the leaf and acquire the
    // per-page latch before invoking the caller's closure, so concurrent CRUD
    // writers serialize on `frame.deltas` (the per-leaf delta map) instead of
    // racing each other through the buffer-pool partition mutex. Routing all
    // mutation through this single choke point is what lets the buffer pool
    // maintain a running byte-sum of live delta payload per frame and apply
    // selective copy-on-write to only the chains a committing txn touched.
    // -----------------------------------------------------------------------

    /// True iff no delta chains are attached to leaf `page`. Read-only
    /// inspector used by structural-cleanup guards (e.g. the
    /// `free_leaf`-path `chains_empty` check). Implementations may use a
    /// shared latch or no latch at all; mutation is forbidden.
    fn chains_empty(&self, page: u32) -> Result<bool>;

    /// Pin leaf `page` under `mode`, run `f` against the chain slot for
    /// `key`, and release the pin+latch.
    ///
    /// The closure receives `&mut Option<Arc<...>>` — `None` when the
    /// frame currently has no chain for `key`. After it returns, the
    /// slot is written back to the frame's `deltas` map: `Some` is
    /// inserted, `None` leaves the slot absent.
    ///
    /// `mode` must be [`LatchMode::Exclusive`] for chain mutation;
    /// shared callers should use `pin_shared_for_read` and the snapshot
    /// helpers on `LatchedPinnedPage` instead.
    fn with_chain_under_latch<R, F>(
        &mut self,
        page: u32,
        key: &[u8],
        mode: LatchMode,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut Option<Arc<VecDeque<VersionEntry>>>) -> R;

    /// Pin leaf `page` under `mode`, run `f` against the entire chain
    /// map, and release the pin+latch. Used by leaf-merge migration
    /// (drain) and overflow-page repurpose (clear).
    fn with_all_chains_under_latch<R, F>(&mut self, page: u32, mode: LatchMode, f: F) -> Result<R>
    where
        F: FnOnce(&mut BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>) -> R;
}
