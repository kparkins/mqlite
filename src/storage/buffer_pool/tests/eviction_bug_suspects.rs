#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]
//! Regression repros for buffer-pool eviction suspects.
//!
//! - BUG-1: a cache miss through the plain `Partition::pin_page` path
//!   (reached via `BufferPool::pin` / `BufferPool::pin_then_latch`) can
//!   CLOCK-evict a frame whose `deltas` map holds live Committed version
//!   chains, silently destroying committed-but-unreconciled MVCC data.
//!   Only `pin_page_reconciling` carries the `has_live_committed_head`
//!   guard.
//! - BUG-2: a frame whose only live heads are a txn's Pending entries is
//!   evictable during the commit envelope's install→flip window (the
//!   frame stays `PageDirtyLsn::Clean`, `stamp_last_lsn` no-ops on Clean
//!   frames, and `has_live_committed_head` excludes Pending), so the
//!   post-durable flip finds nothing to flip and the committed write is
//!   lost while the commit reports success.
//! - BUG-11: an aborted first write leaves `[state=Aborted,
//!   stop_ts=Ts::MAX]` residue that `has_live_committed_head` counts as
//!   live and `reconcile_frame_at` retains, so the frame is
//!   eviction-blocked forever.
//! - BUG-18: when the miss path's `io.read_page` for the NEW page fails
//!   after `evict_frame` already dropped the victim's `page_map` entry,
//!   the victim frame is left occupying its slot unreachable from
//!   `page_map` (a ghost), and a later pin of the ghost's page loads a
//!   second resident copy.

use super::*;

use std::collections::VecDeque;
use std::sync::Arc;

use crate::mvcc::read_view::ReadView;
use crate::mvcc::registry::ReadViewRegistry;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::allocator::AllocatorHandle;
use crate::storage::buffer_pool::partition::Partition;
use crate::storage::header::FileHeader;
use crate::storage::test_support::ZeroIo;

const DELTA_PAGE: u32 = 401;
const PRESSURE_PAGE: u32 = 403;
const DELTA_KEY: &[u8] = b"bug-suspect-key";
const TXN_ID: u64 = 1;

fn ts(physical_ms: u64) -> Ts {
    Ts {
        physical_ms,
        logical: 0,
    }
}

fn entry(state: VersionState, stop_ts: Ts) -> VersionEntry {
    VersionEntry {
        start_ts: ts(10),
        stop_ts,
        txn_id: TXN_ID,
        state,
        data: VersionData::Inline(Vec::from(&b"value"[..])),
        is_tombstone: false,
    }
}

fn committed_head() -> VersionEntry {
    entry(VersionState::Committed, Ts::MAX)
}

fn pending_head() -> VersionEntry {
    entry(VersionState::Pending { txn_id: TXN_ID }, Ts::MAX)
}

fn install_chain(pool: &BufferPool, page: u32, entry: VersionEntry) {
    pool.with_chain_under_latch(page, DELTA_KEY, LatchMode::Exclusive, |slot| {
        *slot = Some(Arc::new(VecDeque::from([entry])));
    })
    .unwrap();
}

fn one_frame_pool() -> (BufferPool, Arc<ReadViewRegistry>, AllocatorHandle) {
    let pool = BufferPool::new(PageSize::Large32k.bytes(), Box::new(ZeroIo));
    let registry = ReadViewRegistry::new();
    let allocator = AllocatorHandle::new(FileHeader::new(0, 0, 0));
    (pool, registry, allocator)
}

fn load_and_unpin(pool: &BufferPool, page: u32) {
    drop(pool.pin(page, PageSize::Large32k).unwrap());
}

/// True when the live committed version installed by `install_chain`
/// is still visible on `DELTA_PAGE`'s resident chains.
fn committed_version_survives(pool: &BufferPool) -> bool {
    let view = ReadView::new_frontier_pinned_for_tests(ts(10), TXN_ID);
    pool.snapshot_chains(DELTA_PAGE, None)
        .unwrap()
        .and_then(|snapshot| snapshot.visible_at(DELTA_KEY, &view).cloned())
        .is_some()
}

// ---------------------------------------------------------------------------
// BUG-1 — plain pin paths evict frames carrying live Committed chains
// ---------------------------------------------------------------------------

#[test]
fn bug1_committed_chain_survives_plain_pin_pressure() {
    let (pool, _registry, _allocator) = one_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);
    install_chain(&pool, DELTA_PAGE, committed_head());

    // Cache miss through the plain pin path. The horizon-free miss path
    // must refuse to evict the frame carrying the live committed chain
    // and fail the pin instead — correctness over availability.
    match pool.pin(PRESSURE_PAGE, PageSize::Large32k) {
        Err(Error::PoolExhausted {
            reason: PoolExhaustedReason::DeltaBearingFrames,
        }) => {}
        Ok(_) => panic!("plain pin pressure must not evict a live committed chain"),
        Err(other) => panic!("expected PoolExhausted(DeltaBearingFrames), got {other:?}"),
    }

    assert!(
        committed_version_survives(&pool),
        "live committed delta must survive a cache miss through the plain \
         BufferPool::pin path (committed-but-unreconciled MVCC data was \
         destroyed by CLOCK eviction)"
    );
}

#[test]
fn bug1_committed_chain_survives_pin_then_latch_pressure() {
    let (pool, _registry, _allocator) = one_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);
    install_chain(&pool, DELTA_PAGE, committed_head());

    // Cache miss through `pin_then_latch` (the CRUD write path via
    // `pin_for_write_sized`) — the same guarded `Partition::pin_page`
    // miss must refuse the delta-bearing victim and fail the pin.
    match pool.pin_for_write_sized(PRESSURE_PAGE, PageSize::Large32k) {
        Err(Error::PoolExhausted {
            reason: PoolExhaustedReason::DeltaBearingFrames,
        }) => {}
        Ok(_) => panic!("pin_then_latch pressure must not evict a live committed chain"),
        Err(other) => panic!("expected PoolExhausted(DeltaBearingFrames), got {other:?}"),
    }

    assert!(
        committed_version_survives(&pool),
        "live committed delta must survive a cache miss through \
         BufferPool::pin_then_latch (committed-but-unreconciled MVCC data \
         was destroyed by CLOCK eviction)"
    );
}

// ---------------------------------------------------------------------------
// BUG-2 — pending-only frame evicted in the install→flip window loses the
// committed write while the flip reports success
// ---------------------------------------------------------------------------

#[test]
fn bug2_pending_only_frame_survives_install_to_flip_window_pressure() {
    let (pool, registry, allocator) = one_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);

    // S7 install: the txn's write lands as a Pending head. `with_chain`
    // mutates only `deltas`, so the frame stays `PageDirtyLsn::Clean`.
    install_chain(&pool, DELTA_PAGE, pending_head());

    // S8 stamp (paged_engine.rs:989-995): the commit end LSN is stamped
    // onto the pending pages. `stamp_last_lsn` no-ops on Clean frames,
    // so the frame remains a prime eviction victim.
    pool.stamp_dirty_pages_lsn(&[DELTA_PAGE], 42).unwrap();

    // S9→S10 window: a concurrent reader misses through the GUARDED
    // reconcile path. `has_live_committed_head` excludes Pending heads,
    // so even this path evicts the pending-only frame today. A fixed
    // engine may instead block or preserve the chain — either way the
    // committed write must survive the flip below.
    let pressure =
        pool.pin_with_reconcile(PRESSURE_PAGE, PageSize::Large32k, &registry, &allocator);
    drop(pressure);

    // S10 flip: the post-durable commit flip. Mirrors
    // `flip_pending_one_page` semantics — an absent / empty pending set
    // is treated as success (index_maint.rs:791-794), so the envelope
    // reports a committed txn either way.
    let commit_ts = ts(11);
    let flipped = pool
        .with_chain_under_latch(DELTA_PAGE, DELTA_KEY, LatchMode::Exclusive, |slot| {
            match slot.as_mut() {
                Some(chain) => flip_pending_in_chain(Arc::make_mut(chain), TXN_ID, Some(commit_ts)),
                // Nothing resident to flip — flip_pending_one_page would
                // return Ok(()) here ("success").
                None => 0,
            }
        })
        .unwrap();

    assert_eq!(
        flipped, 1,
        "post-durable flip found no pending entries: the pending-only frame \
         was evicted during the install→flip window, so the durably \
         committed write is silently lost while the commit reports success"
    );
}

// ---------------------------------------------------------------------------
// BUG-11 — aborted first-write residue permanently blocks eviction
// ---------------------------------------------------------------------------

#[test]
fn bug11_aborted_first_write_residue_does_not_block_eviction() {
    let (pool, registry, allocator) = one_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);
    install_chain(&pool, DELTA_PAGE, pending_head());

    // Abort the txn's first write on this key. The abort branch of
    // `flip_pending_in_chain` sets `state = Aborted` without touching
    // `stop_ts`, and `restore_previous_head_after_abort` has no
    // predecessor to resurrect — leaving `[Aborted, stop_ts=Ts::MAX]`.
    pool.with_chain_under_latch(DELTA_PAGE, DELTA_KEY, LatchMode::Exclusive, |slot| {
        let chain = slot.as_mut().expect("chain installed above");
        let flipped = flip_pending_in_chain(Arc::make_mut(chain), TXN_ID, None);
        assert_eq!(flipped, 1, "abort flip must hit the pending head");
    })
    .unwrap();

    // The residue is dead to every reader, so reconcile-aware eviction
    // must be able to reclaim the frame. Today `has_live_committed_head`
    // counts the non-Pending stop_ts==MAX Aborted entry as live and
    // `reconcile_frame_at` retains it, so this returns
    // PoolExhausted { DeltaBearingFrames } forever.
    let result = pool.pin_with_reconcile(PRESSURE_PAGE, PageSize::Large32k, &registry, &allocator);
    assert!(
        result.is_ok(),
        "a frame whose only chain entry is aborted first-write residue \
         (state=Aborted, stop_ts=Ts::MAX) must be evictable; got {:?}",
        result.err()
    );
}

// ---------------------------------------------------------------------------
// BUG-18 — read-I/O failure on the miss path leaves a ghost frame
// ---------------------------------------------------------------------------

/// `PageSource` whose reads fail for one designated page; all other
/// reads are zero-filled and writes succeed.
struct FailingRead {
    fail_page: u32,
}

impl PageSource for FailingRead {
    fn read_page(&self, page_number: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
        if page_number == self.fail_page {
            return Err(Error::Internal("injected read_page failure".into()));
        }
        assert_eq!(buf.len(), size.bytes());
        buf.fill(0);
        Ok(())
    }

    fn write_page(&self, _page_number: u32, _size: PageSize, _buf: &[u8]) -> Result<()> {
        Ok(())
    }
}

/// Assert the page_map ↔ slot invariant: every occupied frame slot must
/// be reachable from `page_map` (see partition.rs "page_map invariant").
fn assert_no_ghost_slots(partition: &Partition) {
    for (idx, slot) in partition.frames.iter().enumerate() {
        let Some(frame) = slot else { continue };
        assert_eq!(
            partition.page_map.get(&frame.page_number).copied(),
            Some(idx),
            "occupied slot {idx} (page {}) must be reachable from page_map; \
             a failed miss-path read left a ghost frame behind",
            frame.page_number,
        );
    }
}

#[test]
fn bug18_failed_miss_read_leaves_no_ghost_frame() {
    let io = FailingRead {
        fail_page: PRESSURE_PAGE,
    };
    let mut partition = Partition::new(1, PageSize::Large32k.bytes());
    partition
        .pin_page(DELTA_PAGE, &io, PageSize::Large32k, u64::MAX)
        .unwrap();
    partition.unpin_page(DELTA_PAGE, false, None).unwrap();

    // Miss for PRESSURE_PAGE: `evict_frame` removes DELTA_PAGE from
    // `page_map`, then the read of PRESSURE_PAGE fails and the early
    // return leaves DELTA_PAGE's frame stranded in its slot.
    let result = partition.pin_page(PRESSURE_PAGE, &io, PageSize::Large32k, u64::MAX);
    assert!(result.is_err(), "injected read failure must surface");

    assert_no_ghost_slots(&partition);
}

#[test]
fn bug18_failed_miss_read_does_not_duplicate_resident_pages() {
    let io = FailingRead {
        fail_page: PRESSURE_PAGE,
    };
    let mut partition = Partition::new(3, PageSize::Large32k.bytes());
    partition
        .pin_page(DELTA_PAGE, &io, PageSize::Large32k, u64::MAX)
        .unwrap();
    partition.unpin_page(DELTA_PAGE, false, None).unwrap();
    for frame in partition.frames.iter_mut().flatten() {
        frame.ref_bit = false;
    }
    partition.clock_hand = 0;

    // Failed miss strands DELTA_PAGE's frame as a ghost (unreachable
    // from page_map but still occupying its slot).
    let result = partition.pin_page(PRESSURE_PAGE, &io, PageSize::Large32k, u64::MAX);
    assert!(result.is_err(), "injected read failure must surface");

    // A later pin of DELTA_PAGE misses (the map entry is gone) and loads
    // a SECOND copy into a different slot — divergent images/chains.
    partition
        .pin_page(DELTA_PAGE, &io, PageSize::Large32k, u64::MAX)
        .unwrap();
    let copies = partition
        .frames
        .iter()
        .flatten()
        .filter(|frame| frame.page_number == DELTA_PAGE)
        .count();
    assert_eq!(
        copies, 1,
        "page {DELTA_PAGE} must have exactly one resident copy after a \
         failed miss-path read followed by a successful re-pin"
    );
}

// ---------------------------------------------------------------------------
// ITEM 2 — foreign-pending abort-restore ABA (REFUTED, pinned here)
// ---------------------------------------------------------------------------
//
// `transaction.rs` stages writes with `expected_head: None`; the abort flip
// (`flip_pending_to_aborted_for` -> `flip_pending_one_page` with
// `commit_ts = None`) runs `restore_previous_head_after_abort`
// (latched_page.rs), which restores the prior head's `stop_ts` to `Ts::MAX`.
// The handed-off suspicion: with `expected_head: None`, can an abort restore
// clobber a NEWER committed head another writer installed between the abort's
// read and its restore (an ABA)?
//
// It cannot. The abort flip uses the SAME selective copy-on-write Phase A/B
// shape as the commit flip: Phase A snapshots the chain `Arc` and runs
// `flip_pending_in_chain` (including `restore_previous_head_after_abort`) on a
// LOCAL clone; Phase B installs via `try_swap_chains_if_unchanged`, which
// verifies `Arc::ptr_eq(resident, expected_old)` BEFORE writing and returns
// `Conflict` (mutating nothing) if any concurrent writer replaced the chain.
// The restore therefore never lands on a chain different from the one Phase A
// observed — the per-page exclusive-latch ptr_eq compare-and-swap is the ABA
// guard, and `expected_head` plays no part in the abort-restore matcher.
//
// This test pins that guard: an abort prepared off-latch must NOT clobber a
// newer committed head installed before the swap — it must report `Conflict`
// and leave the newer head intact.
#[test]
fn item2_abort_restore_cannot_clobber_newer_head_via_swap_cas() {
    use crate::storage::buffer_pool::{flip_pending_in_chain, PreparedChainSwap, SwapOutcome};

    let (pool, _registry, _allocator) = one_frame_pool();
    load_and_unpin(&pool, DELTA_PAGE);

    // Resident chain: a foreign Pending head over a prior committed version
    // it superseded (prev.stop_ts == pending.start_ts), exactly the shape
    // `restore_previous_head_after_abort` targets.
    let prev_committed = VersionEntry {
        start_ts: ts(5),
        stop_ts: ts(10), // capped by the pending head's start_ts
        txn_id: 7,
        state: VersionState::Committed,
        data: VersionData::Inline(b"prev".to_vec()),
        is_tombstone: false,
    };
    let pending = pending_head(); // start_ts == ts(10), stop_ts == Ts::MAX
    pool.with_chain_under_latch(DELTA_PAGE, DELTA_KEY, LatchMode::Exclusive, |slot| {
        *slot = Some(Arc::new(VecDeque::from([pending, prev_committed])));
    })
    .unwrap();

    // Phase A (off-latch): snapshot the chain Arc, clone, run the abort flip
    // on the clone. `expected_old` is the Arc the abort observed.
    let expected_old = {
        let latched = pool
            .pin_for_read_sized(DELTA_PAGE, PageSize::Large32k)
            .unwrap();
        latched.snapshot_chain_arc(DELTA_KEY).unwrap()
    };
    let mut aborted_clone = expected_old.clone();
    let flipped = flip_pending_in_chain(Arc::make_mut(&mut aborted_clone), TXN_ID, None);
    assert_eq!(flipped, 1, "abort flip must hit the pending head");

    // Between Phase A and Phase B, a concurrent writer commits a NEWER head
    // on the same key (the ABA): the prior pending is replaced and the
    // resident chain Arc identity changes.
    let newer_committed = VersionEntry {
        start_ts: ts(20),
        stop_ts: Ts::MAX,
        txn_id: 42,
        state: VersionState::Committed,
        data: VersionData::Inline(b"newer".to_vec()),
        is_tombstone: false,
    };
    pool.with_chain_under_latch(DELTA_PAGE, DELTA_KEY, LatchMode::Exclusive, |slot| {
        *slot = Some(Arc::new(VecDeque::from([newer_committed.clone()])));
    })
    .unwrap();

    // Phase B: install the off-latch abort result with the ptr_eq CAS. The
    // resident chain no longer matches `expected_old`, so the swap MUST
    // report Conflict and mutate nothing — the abort cannot clobber the
    // newer committed head.
    let outcome = {
        let mut latched = pool
            .pin_for_write_sized(DELTA_PAGE, PageSize::Large32k)
            .unwrap();
        latched
            .try_swap_chains_if_unchanged(vec![PreparedChainSwap {
                key: DELTA_KEY.to_vec(),
                new_chain: aborted_clone,
                expected_old,
            }])
            .unwrap()
    };
    assert_eq!(
        outcome,
        SwapOutcome::Conflict,
        "the abort's off-latch restore must not install over a chain a newer \
         committer replaced — the ptr_eq CAS must report Conflict (ABA guard)"
    );

    // The newer committed head is intact and visible; the abort clobbered
    // nothing.
    let view = ReadView::new_frontier_pinned_for_tests(ts(25), 99);
    let visible = pool
        .snapshot_chains(DELTA_PAGE, None)
        .unwrap()
        .and_then(|snap| snap.visible_at(DELTA_KEY, &view).cloned())
        .expect("newer committed head must remain visible");
    assert_eq!(
        visible.start_ts,
        ts(20),
        "the newer committer's head must survive the conflicting abort swap"
    );
}
