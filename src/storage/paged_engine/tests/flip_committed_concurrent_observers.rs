//! PR1 selective-CoW assertion.
//!
//! Validates that the Phase A/B `flip_pending_to_committed_for`
//! algorithm `Arc::make_mut`s only chains containing
//! `Pending(txn_id)`, NOT every chain on the frame (the legacy
//! `frame.deltas.values_mut()` whole-frame loop).
//!
//! Method: install N+M chains on a single leaf frame — N "pending"
//! chains carrying a `Pending(txn_id)` head and M "background"
//! committed chains. Snapshot every `Arc` before the flip, call the
//! flip, snapshot again, and compare via `Arc::ptr_eq`:
//!   - Pending chains' Arc MUST change (`!ptr_eq`) — they were CoW'd
//!     and the head was flipped to Committed.
//!   - Background chains' Arc MUST be unchanged (`ptr_eq`) — selective
//!     CoW skipped them entirely.
//!
//! Failure of either assertion means PR1's selective-CoW guarantee is
//! broken, which would either inflate `Arc::make_mut` work
//! (regression) or fail to flip pending entries (correctness bug).

use super::*;

use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::Result;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::buffer_pool::{default_sizes, BufferPool, LatchMode, PageSize};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

use super::index_maint::flip_pending_to_committed_for;

const TXN_ID: u64 = 0xC0FFEE;
const PENDING_KEYS: usize = 4;
const BACKGROUND_KEYS: usize = 8;
const PAGE_ID: u32 = 23;

fn buffered_engine() -> (PagedEngine, Arc<MockIo>) {
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let header = FileHeader::new_now();
    let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
    let engine = PagedEngine::new_buffered(handle, 0, 0).expect("create buffered engine");
    (engine, io)
}

fn pending_entry(start_ts: Ts) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts: Ts::MAX,
        txn_id: TXN_ID,
        state: VersionState::Pending { txn_id: TXN_ID },
        data: VersionData::Inline(vec![0xCA]),
        is_tombstone: false,
    }
}

fn committed_entry(start_ts: Ts, payload: u8) -> VersionEntry {
    VersionEntry {
        start_ts,
        stop_ts: Ts::MAX,
        txn_id: 1,
        state: VersionState::Committed,
        data: VersionData::Inline(vec![payload]),
        is_tombstone: false,
    }
}

fn chain_with(entries: Vec<VersionEntry>) -> Arc<VecDeque<VersionEntry>> {
    Arc::new(entries.into_iter().collect())
}

fn pending_key(i: usize) -> Vec<u8> {
    let mut k = b"P:".to_vec();
    k.extend_from_slice(&(i as u32).to_be_bytes());
    k
}

fn background_key(i: usize) -> Vec<u8> {
    let mut k = b"B:".to_vec();
    k.extend_from_slice(&(i as u32).to_be_bytes());
    k
}

#[test]
fn flip_pending_to_committed_only_touches_pending_chains() -> Result<()> {
    let (engine, _io) = buffered_engine();
    let pool = engine.shared.handle.pool();

    // Make sure the target leaf page is allocated + resident. We use a
    // direct pin to load+zero it; the page-id is arbitrary because we
    // never traverse via the B-tree — we install chains by key, route
    // by page directly.
    let _seed_pin = pool.pin(PAGE_ID, PageSize::Large32k)?;
    drop(_seed_pin);

    // Install BACKGROUND_KEYS committed-only chains plus PENDING_KEYS
    // chains carrying a Pending(TXN_ID) head. The pending entry is
    // pushed in front of an older committed entry so the chain has
    // depth 2 — flipping the head must NOT disturb the tail.
    let commit_ts = Ts {
        physical_ms: 100,
        logical: 0,
    };
    let pending_start_ts = Ts {
        physical_ms: 200,
        logical: 0,
    };
    for i in 0..BACKGROUND_KEYS {
        let key = background_key(i);
        let chain = chain_with(vec![committed_entry(commit_ts, i as u8)]);
        pool.with_chain_under_latch(PAGE_ID, &key, LatchMode::Exclusive, |slot| {
            *slot = Some(chain);
        })?;
    }
    for i in 0..PENDING_KEYS {
        let key = pending_key(i);
        let chain = chain_with(vec![
            pending_entry(pending_start_ts),
            committed_entry(commit_ts, i as u8),
        ]);
        pool.with_chain_under_latch(PAGE_ID, &key, LatchMode::Exclusive, |slot| {
            *slot = Some(chain);
        })?;
    }

    // Snapshot every chain Arc BEFORE the flip.
    let snap = |k: &[u8]| -> Arc<VecDeque<VersionEntry>> {
        pool.with_chain_under_latch(PAGE_ID, k, LatchMode::Exclusive, |slot| {
            slot.clone().expect("chain installed in setup")
        })
        .expect("snapshot under latch")
    };
    let pending_before: Vec<Arc<VecDeque<VersionEntry>>> =
        (0..PENDING_KEYS).map(|i| snap(&pending_key(i))).collect();
    let background_before: Vec<Arc<VecDeque<VersionEntry>>> =
        (0..BACKGROUND_KEYS).map(|i| snap(&background_key(i))).collect();

    // Run the flip.
    let flip_commit_ts = Ts {
        physical_ms: 300,
        logical: 0,
    };
    flip_pending_to_committed_for(&engine.shared, TXN_ID, flip_commit_ts, &[PAGE_ID])?;

    // Snapshot every chain Arc AFTER the flip.
    let pending_after: Vec<Arc<VecDeque<VersionEntry>>> =
        (0..PENDING_KEYS).map(|i| snap(&pending_key(i))).collect();
    let background_after: Vec<Arc<VecDeque<VersionEntry>>> =
        (0..BACKGROUND_KEYS).map(|i| snap(&background_key(i))).collect();

    // Selective-CoW assertion: pending chains MUST have a new Arc
    // (the Phase B swap installed the locally CoW'd flipped chain),
    // background chains MUST have the SAME Arc as before (the Phase A
    // selective-CoW pass never touched them).
    for i in 0..PENDING_KEYS {
        assert!(
            !Arc::ptr_eq(&pending_before[i], &pending_after[i]),
            "pending chain {i} should have been replaced by Phase B swap (selective CoW path)"
        );
        // And the head must now be Committed with the flip_commit_ts.
        let head = pending_after[i].front().expect("pending chain head");
        match head.state {
            VersionState::Committed => {}
            ref other => panic!("expected Committed head after flip, got {other:?}"),
        }
        assert_eq!(
            head.start_ts, flip_commit_ts,
            "head start_ts must equal the flip commit_ts"
        );
        // The older committed entry must still be present (tail
        // preserved).
        assert_eq!(pending_after[i].len(), 2, "chain depth preserved");
    }
    for i in 0..BACKGROUND_KEYS {
        assert!(
            Arc::ptr_eq(&background_before[i], &background_after[i]),
            "background chain {i} must NOT be CoW'd by selective-CoW flip — \
             this is the PR1 invariant"
        );
    }

    Ok(())
}
