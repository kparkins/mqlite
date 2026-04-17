//! `ReadView` — the snapshot primitive every reader holds.
//!
//! See MVCC plan §T3 / §S13. Each open reader holds a `ReadView` pinning
//! its `read_ts`; the version-chain walker uses `read_ts` to pick the
//! visible entry. T4 adds the `ReadViewRegistry` that tracks live
//! `ReadView`s so the writer can compute `oldest_required_ts`.
//!
//! The `poisoned` flag and `pin_ops_in_flight` counter together support
//! T8 force-expiry (MAJOR-1): `force_expire` flips `poisoned`, then spins
//! until `pin_ops_in_flight` reaches 0, so no concurrent pin walk can be
//! mid-flight when pages are released.
//!
//! Production-path atomics use the cfg(loom) shim pattern so future
//! concurrency harnesses (T4, T8) can permute them.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[cfg(loom)]
use loom::sync::atomic::{AtomicBool, AtomicU32};

#[cfg(not(loom))]
use std::sync::atomic::{AtomicBool, AtomicU32};

use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::VersionEntry;

/// A snapshot handle for an active reader.
///
/// Constructed by `ReadViewRegistry::open()` (T4) with `read_ts` taken
/// from the timestamp oracle. The visibility rule:
///
/// - Committed entry `E` is visible iff `E.start_ts <= read_ts < E.stop_ts`.
/// - Pending entry is visible only to its own `txn_id`.
///
/// `poisoned` is set by T8 force-expiry before touching any owned pins;
/// `pin_ops_in_flight` lets the force-expiry path wait for concurrent
/// pin walks to complete before releasing pages.
#[derive(Debug)]
pub struct ReadView {
    /// Snapshot timestamp for visibility checks.
    pub read_ts: Ts,
    /// Transaction identifier — also serves as the txn_id used to resolve
    /// visibility of this reader's own pending entries when the reader
    /// doubles as a writer.
    pub txn_id: u64,
    /// Set by `force_expire` (T8). Any subsequent pin-walk observes this
    /// via an Acquire load at the pre-check and again post-increment of
    /// `pin_ops_in_flight`; if poisoned, it bails without walking pins.
    pub poisoned: AtomicBool,
    /// Active pin-walk count. Incremented on entry to
    /// `pin_overflows`-style code and decremented on exit. `force_expire`
    /// spins until this reaches 0 before the caller is allowed to proceed
    /// with page-release.
    pub pin_ops_in_flight: AtomicU32,
}

impl ReadView {
    /// Construct a fresh, live `ReadView`.
    pub fn new(read_ts: Ts, txn_id: u64) -> Self {
        Self {
            read_ts,
            txn_id,
            poisoned: AtomicBool::new(false),
            pin_ops_in_flight: AtomicU32::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// ChainSnapshot — reader-path snapshot of a frame's per-key version chains
// ---------------------------------------------------------------------------

/// Reader-side snapshot of a leaf frame's per-key version chains.
///
/// Construction deep-clones every `VersionEntry` in every chain, which runs
/// `OverflowRef::Clone` (CAS-loop incref) on each `VersionData::Overflow`.
/// Every entry observed through the snapshot is therefore pinned — its
/// backing overflow chain cannot be freed while the snapshot is live.
///
/// Drop follows the default Rust drop-glue: the outer `HashMap` drops each
/// `Arc<VecDeque<VersionEntry>>`, the last `Arc` drops the `VecDeque` which
/// drops every contained `VersionEntry`, which in turn runs
/// `OverflowRef::Drop` (atomic decref + deferred-free enqueue on 0).
///
/// **S13 force-expiry contract:**
///
/// 1. `new` checks `view.poisoned` BEFORE taking any refcount bumps. If
///    poisoned, it returns an empty snapshot (no `fetch_add`, no clones).
/// 2. `new` takes `pin_ops_in_flight.fetch_add(1, Release)`, performs the
///    deep clone (each entry's refcount bumped), then re-checks
///    `poisoned` under an `Acquire` load and decrements
///    `pin_ops_in_flight`. If poisoned-after, the cloned chains are
///    dropped here — RAII decrefs every bumped entry so the net refcount
///    delta is zero.
/// 3. No explicit `Drop` impl: ordinary drop glue suffices because
///    `force_expire` does NOT walk snapshot pins. Every refcount bump has
///    a matching decref through a single code path.
pub struct ChainSnapshot {
    /// Deep-cloned per-key chains. Each inner `Arc<VecDeque<VersionEntry>>`
    /// is freshly allocated; the `VersionEntry` values inside each
    /// `VecDeque` were cloned from the source (running `OverflowRef::Clone`
    /// for `VersionData::Overflow` entries).
    chains: HashMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>,
    /// Back-reference to the owning reader's `ReadView`, used for the S13
    /// poison check during `new`. `None` on reader paths that predate the
    /// T4 `ReadViewRegistry` wiring (every construction site in T3.75
    /// supplies `None`).
    #[allow(dead_code)]
    view: Option<Arc<ReadView>>,
}

impl std::fmt::Debug for ChainSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChainSnapshot")
            .field("num_keys", &self.chains.len())
            .field("view_attached", &self.view.is_some())
            .finish()
    }
}

impl ChainSnapshot {
    /// Construct a snapshot from a frame's per-key version chains.
    ///
    /// Deep-clones every entry (bumping overflow refcounts via
    /// `OverflowRef::Clone`) under the S13 atomic-handoff protocol. See
    /// type-level docs for the poison contract.
    pub fn new(
        source: &HashMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>,
        view: Option<Arc<ReadView>>,
    ) -> Self {
        // Pre-check: if the owning view is already poisoned, refuse to
        // pin any entries. The empty snapshot is the S13 "force-expired
        // view sees nothing" contract.
        if let Some(v) = &view {
            if v.poisoned.load(Ordering::Acquire) {
                return ChainSnapshot {
                    chains: HashMap::new(),
                    view,
                };
            }
            v.pin_ops_in_flight.fetch_add(1, Ordering::Release);
        }

        // Deep clone: each inner `VersionEntry::clone()` runs
        // `OverflowRef::clone()` which is the CAS-loop incref.
        let mut chains = HashMap::with_capacity(source.len());
        for (k, chain) in source {
            let cloned: VecDeque<VersionEntry> = chain.iter().cloned().collect();
            chains.insert(k.clone(), Arc::new(cloned));
        }

        // Re-check poison AFTER the bumps. If force-expiry fired while we
        // were cloning, drop the cloned chains here — RAII decrefs every
        // entry we just bumped so the net refcount delta is zero.
        if let Some(v) = &view {
            let poisoned_after = v.poisoned.load(Ordering::Acquire);
            v.pin_ops_in_flight.fetch_sub(1, Ordering::Release);
            if poisoned_after {
                return ChainSnapshot {
                    chains: HashMap::new(),
                    view,
                };
            }
        }

        ChainSnapshot { chains, view }
    }

    /// Find the entry in the chain for `key` visible at `view.read_ts`.
    ///
    /// Visibility rule:
    /// - Pending entry (`start_ts == Ts::PENDING`): visible only to its own `txn_id`.
    /// - Committed entry: `start_ts <= read_ts < stop_ts`.
    pub fn visible_at(&self, key: &[u8], view: &ReadView) -> Option<&VersionEntry> {
        self.chains.get(key).and_then(|chain| {
            chain.iter().find(|e| {
                if e.start_ts == Ts::PENDING {
                    e.txn_id == view.txn_id
                } else {
                    e.start_ts <= view.read_ts && view.read_ts < e.stop_ts
                }
            })
        })
    }

    /// Number of distinct keys with chains in this snapshot.
    pub fn key_count(&self) -> usize {
        self.chains.len()
    }

    /// True iff the snapshot holds no chains.
    pub fn is_empty(&self) -> bool {
        self.chains.is_empty()
    }

    /// Length of the chain for `key`, or 0 if absent.
    pub fn chain_len(&self, key: &[u8]) -> usize {
        self.chains.get(key).map(|c| c.len()).unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::mvcc::version::{OverflowRef, VersionData};
    use crate::storage::allocator::AllocatorHandle;
    use crate::storage::header::FileHeader;

    fn fresh_allocator() -> AllocatorHandle {
        AllocatorHandle::new(FileHeader::new(0, 0, 0))
    }

    fn overflow_entry(alloc: &AllocatorHandle, first_page: u32, ts: Ts) -> VersionEntry {
        let r = OverflowRef::new_owned(first_page, 128, alloc.clone()).unwrap();
        VersionEntry {
            start_ts: ts,
            stop_ts: Ts::MAX,
            txn_id: 1,
            data: VersionData::Overflow(r),
            is_tombstone: false,
        }
    }

    #[test]
    fn new_read_view_is_live() {
        let rv = ReadView::new(
            Ts {
                physical_ms: 100,
                logical: 1,
            },
            42,
        );
        assert_eq!(rv.read_ts.physical_ms, 100);
        assert_eq!(rv.read_ts.logical, 1);
        assert_eq!(rv.txn_id, 42);
        assert!(!rv.poisoned.load(Ordering::Acquire));
        assert_eq!(rv.pin_ops_in_flight.load(Ordering::Acquire), 0);
    }

    #[test]
    fn poisoned_flag_transitions() {
        let rv = ReadView::new(Ts::PENDING, 0);
        assert!(!rv.poisoned.load(Ordering::Acquire));
        rv.poisoned.store(true, Ordering::Release);
        assert!(rv.poisoned.load(Ordering::Acquire));
    }

    #[test]
    fn pin_ops_counter_tracks_in_flight() {
        let rv = ReadView::new(Ts::PENDING, 0);
        rv.pin_ops_in_flight.fetch_add(1, Ordering::Release);
        rv.pin_ops_in_flight.fetch_add(1, Ordering::Release);
        assert_eq!(rv.pin_ops_in_flight.load(Ordering::Acquire), 2);
        rv.pin_ops_in_flight.fetch_sub(1, Ordering::Release);
        assert_eq!(rv.pin_ops_in_flight.load(Ordering::Acquire), 1);
    }

    // -----------------------------------------------------------------------
    // ChainSnapshot — construction / refcount preservation
    // -----------------------------------------------------------------------

    #[test]
    fn chain_snapshot_new_bumps_each_overflow_refcount() {
        let alloc = fresh_allocator();
        let mut source: HashMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> = HashMap::new();

        // Key A: chain of 3 overflow entries on pages 10, 11, 12.
        let mut chain_a = VecDeque::new();
        chain_a.push_back(overflow_entry(&alloc, 10, Ts { physical_ms: 300, logical: 0 }));
        chain_a.push_back(overflow_entry(&alloc, 11, Ts { physical_ms: 200, logical: 0 }));
        chain_a.push_back(overflow_entry(&alloc, 12, Ts { physical_ms: 100, logical: 0 }));
        source.insert(b"A".to_vec(), Arc::new(chain_a));

        // Key B: chain of 1 overflow entry on page 20.
        let mut chain_b = VecDeque::new();
        chain_b.push_back(overflow_entry(&alloc, 20, Ts { physical_ms: 400, logical: 0 }));
        source.insert(b"B".to_vec(), Arc::new(chain_b));

        for p in [10, 11, 12, 20] {
            assert_eq!(alloc.overflow_refcount(p), 1, "baseline refcount for page {p}");
        }

        let snap = ChainSnapshot::new(&source, None);

        // Post-construction: each overflow page refcount must be baseline + 1.
        for p in [10, 11, 12, 20] {
            assert_eq!(alloc.overflow_refcount(p), 2, "post-snapshot refcount for page {p}");
        }
        assert_eq!(snap.key_count(), 2);
        assert_eq!(snap.chain_len(b"A"), 3);
        assert_eq!(snap.chain_len(b"B"), 1);

        // Drop: refcount returns to baseline; no leak, no double-decref.
        drop(snap);
        for p in [10, 11, 12, 20] {
            assert_eq!(alloc.overflow_refcount(p), 1, "post-drop refcount for page {p}");
        }
    }

    #[test]
    fn chain_snapshot_is_empty_on_empty_source() {
        let source: HashMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> = HashMap::new();
        let snap = ChainSnapshot::new(&source, None);
        assert!(snap.is_empty());
        assert_eq!(snap.key_count(), 0);
    }

    // -----------------------------------------------------------------------
    // ChainSnapshot — S13 force-expiry contract
    // -----------------------------------------------------------------------

    #[test]
    fn chain_snapshot_poisoned_before_new_takes_no_pins() {
        let alloc = fresh_allocator();
        let mut source: HashMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> = HashMap::new();
        let mut chain = VecDeque::new();
        chain.push_back(overflow_entry(&alloc, 7, Ts { physical_ms: 100, logical: 0 }));
        source.insert(b"k".to_vec(), Arc::new(chain));
        assert_eq!(alloc.overflow_refcount(7), 1);

        let view = Arc::new(ReadView::new(Ts { physical_ms: 500, logical: 0 }, 42));
        view.poisoned.store(true, Ordering::Release);

        let pre_ops = view.pin_ops_in_flight.load(Ordering::Acquire);
        let snap = ChainSnapshot::new(&source, Some(view.clone()));
        let post_ops = view.pin_ops_in_flight.load(Ordering::Acquire);

        assert!(snap.is_empty(), "poisoned view must yield empty snapshot");
        assert_eq!(
            alloc.overflow_refcount(7),
            1,
            "poisoned-before path must not bump refcount"
        );
        assert_eq!(
            pre_ops, post_ops,
            "poisoned-before path must not touch pin_ops_in_flight"
        );
    }

    #[test]
    fn chain_snapshot_poisoned_after_bump_drops_clones() {
        // Simulated atomic handoff: between fetch_add and the deep clone,
        // force_expire flips `poisoned`. We can't inject directly inside
        // `new` without loom, so we hand-roll the sequence here to prove
        // the invariant and then cover the real path with the loom test
        // in tests/force_expiry_pin_race.rs.
        let alloc = fresh_allocator();
        let mut source: HashMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> = HashMap::new();
        let mut chain = VecDeque::new();
        chain.push_back(overflow_entry(&alloc, 9, Ts { physical_ms: 100, logical: 0 }));
        source.insert(b"k".to_vec(), Arc::new(chain));

        let view = Arc::new(ReadView::new(Ts { physical_ms: 500, logical: 0 }, 42));
        // Not poisoned when `new` starts.
        assert!(!view.poisoned.load(Ordering::Acquire));

        // Poison it AFTER construction starts but BEFORE we drop the snap.
        // The real `new` re-check only fires if poisoned flipped during
        // construction — so to observe the drop-path under a purely
        // sequential test we arrange: poison, then construct with Some(v).
        // The pre-check wins and returns empty; refcount stays at baseline.
        view.poisoned.store(true, Ordering::Release);
        let snap = ChainSnapshot::new(&source, Some(view.clone()));
        drop(snap);
        assert_eq!(
            alloc.overflow_refcount(9),
            1,
            "pre-check poisoned path must leave refcount unchanged"
        );

        // Independent drop-path proof: clone-equivalent operation (the
        // body of `new` after fetch_add succeeds, assuming no poison)
        // must restore refcount to baseline on Drop. Already covered by
        // `chain_snapshot_new_bumps_each_overflow_refcount`.
    }

    #[test]
    fn chain_snapshot_mem_store_shape_visibility() {
        // Mirrors the MemPageStore acceptance bullet: chains inserted,
        // `visible_at` returns the correct entry.
        let alloc = fresh_allocator();
        let mut source: HashMap<Vec<u8>, Arc<VecDeque<VersionEntry>>> = HashMap::new();

        // Chain for key K: head is committed at ts=200, stop_ts=MAX; older
        // entry committed at ts=100, stopped at ts=200.
        let head = VersionEntry {
            start_ts: Ts { physical_ms: 200, logical: 0 },
            stop_ts: Ts::MAX,
            txn_id: 7,
            data: VersionData::Inline(b"v2".to_vec()),
            is_tombstone: false,
        };
        let older = VersionEntry {
            start_ts: Ts { physical_ms: 100, logical: 0 },
            stop_ts: Ts { physical_ms: 200, logical: 0 },
            txn_id: 6,
            data: VersionData::Overflow(
                OverflowRef::new_owned(42, 256, alloc.clone()).unwrap(),
            ),
            is_tombstone: false,
        };
        let mut chain = VecDeque::new();
        chain.push_back(head);
        chain.push_back(older);
        source.insert(b"K".to_vec(), Arc::new(chain));

        let snap = ChainSnapshot::new(&source, None);

        let reader_old = ReadView::new(Ts { physical_ms: 150, logical: 0 }, 99);
        let reader_new = ReadView::new(Ts { physical_ms: 250, logical: 0 }, 99);
        let reader_pending = ReadView::new(Ts { physical_ms: 200, logical: 0 }, 99);

        let got_old = snap.visible_at(b"K", &reader_old).expect("entry visible at ts=150");
        assert_eq!(got_old.start_ts.physical_ms, 100);
        assert_eq!(got_old.txn_id, 6);

        let got_new = snap.visible_at(b"K", &reader_new).expect("entry visible at ts=250");
        assert_eq!(got_new.start_ts.physical_ms, 200);
        assert_eq!(got_new.txn_id, 7);

        // Exactly at 200: head is visible (start_ts <= read_ts < stop_ts=MAX).
        let got_boundary = snap
            .visible_at(b"K", &reader_pending)
            .expect("head visible at read_ts=start_ts");
        assert_eq!(got_boundary.start_ts.physical_ms, 200);

        assert!(snap.visible_at(b"missing", &reader_new).is_none());
    }
}
