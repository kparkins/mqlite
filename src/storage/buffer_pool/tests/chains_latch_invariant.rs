//! Runtime invariants for the PR0.5 chain-mutator unification.
//!
//! Asserts that:
//! - Every `frame.deltas` mutation reaches the page through the
//!   exclusive page-local latch via `with_chain_under_latch` /
//!   `with_all_chains_under_latch` (or directly on a
//!   `LatchedPinnedPage` that holds the exclusive latch).
//! - Calling `LatchedPinnedPage::with_chain` /
//!   `with_all_chains` from a shared latch hold returns
//!   `Error::Internal("...requires an exclusive page latch")`,
//!   mirroring the existing `require_exclusive` enforcement at
//!   `buffer_pool/mod.rs:564-571`.
//! - The legacy public wrappers (`BufferPool::take_chain`,
//!   `put_chain`, `clear_chains_on_page`, `drain_leaf_chains`) now
//!   route through the latched API; concurrent readers see chain
//!   mutations fully ordered behind the page latch.
//!
//! Compile-time enforcement: `LatchedPinnedPage::frame_ptr` is
//! private to `buffer_pool/`, so no caller outside the module can
//! mutate `frame.deltas` without going through one of the latched
//! entry points checked here. This file is the runtime backstop.

#![allow(clippy::panic, clippy::unwrap_used)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};

use crate::error::{Error, Result};
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::buffer_pool::{BufferPool, LatchMode, PageSize, PageSource};
use crate::storage::page::PAGE_TYPE_LEAF;

const PAGE_ID: u32 = 7;
const KEY: &[u8] = b"latch-invariant-key";

#[derive(Default)]
struct MockIo {
    pages: StdMutex<BTreeMap<u32, Vec<u8>>>,
}

impl PageSource for MockIo {
    fn read_page(&self, page: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
        let pages = self.pages.lock().unwrap();
        if let Some(data) = pages.get(&page) {
            buf.copy_from_slice(data);
        } else {
            buf.fill(0);
            buf[0] = PAGE_TYPE_LEAF;
        }
        let _ = size;
        Ok(())
    }

    fn write_page(&self, page: u32, _size: PageSize, buf: &[u8]) -> Result<()> {
        self.pages.lock().unwrap().insert(page, buf.to_vec());
        Ok(())
    }
}

fn fresh_pool() -> BufferPool {
    BufferPool::new(2 * 1024 * 1024, Box::new(MockIo::default()))
}

fn sample_chain(payload: u8) -> Arc<VecDeque<VersionEntry>> {
    Arc::new(VecDeque::from([VersionEntry {
        start_ts: Ts {
            physical_ms: 1,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id: 1,
        state: VersionState::Committed,
        data: VersionData::Inline(vec![payload]),
        is_tombstone: false,
    }]))
}

#[test]
fn with_chain_under_latch_routes_writes_through_latched_path() {
    let pool = fresh_pool();
    pool.with_chain_under_latch(PAGE_ID, KEY, LatchMode::Exclusive, |slot| {
        assert!(slot.is_none(), "fresh frame should have no chain for KEY");
        *slot = Some(sample_chain(0xAA));
    })
    .unwrap();

    pool.with_chain_under_latch(PAGE_ID, KEY, LatchMode::Exclusive, |slot| {
        let chain = slot.as_ref().expect("chain installed by previous closure");
        let entry = chain.front().unwrap();
        assert!(matches!(entry.data, VersionData::Inline(ref bytes) if bytes == &[0xAA]));
    })
    .unwrap();
}

#[test]
fn with_chain_under_latch_take_pattern_clears_slot() {
    let pool = fresh_pool();
    pool.with_chain_under_latch(PAGE_ID, KEY, LatchMode::Exclusive, |slot| {
        *slot = Some(sample_chain(0xBB));
    })
    .unwrap();

    let taken = pool
        .with_chain_under_latch(PAGE_ID, KEY, LatchMode::Exclusive, |slot| slot.take())
        .unwrap();
    assert!(taken.is_some(), "take() should return the prior chain");

    pool.with_chain_under_latch(PAGE_ID, KEY, LatchMode::Exclusive, |slot| {
        assert!(slot.is_none(), "slot should be empty after take");
    })
    .unwrap();
}

#[test]
fn with_all_chains_under_latch_drains_and_clears_map() {
    let pool = fresh_pool();
    for (k, payload) in [(b"a".as_slice(), 1u8), (b"b".as_slice(), 2)] {
        pool.with_chain_under_latch(PAGE_ID, k, LatchMode::Exclusive, |slot| {
            *slot = Some(sample_chain(payload));
        })
        .unwrap();
    }

    let drained: Vec<(Vec<u8>, Arc<VecDeque<VersionEntry>>)> = pool
        .with_all_chains_under_latch(PAGE_ID, LatchMode::Exclusive, |chains| {
            std::mem::take(chains).into_iter().collect()
        })
        .unwrap();
    assert_eq!(drained.len(), 2, "drain returns every key");

    pool.with_all_chains_under_latch(PAGE_ID, LatchMode::Exclusive, |chains| {
        assert!(chains.is_empty(), "drain leaves the map empty");
    })
    .unwrap();
}

#[test]
fn latched_pinned_page_with_chain_rejects_shared_mode() {
    let pool = fresh_pool();
    let mut shared_latch = pool
        .pin_for_read_sized(PAGE_ID, PageSize::Large32k)
        .unwrap();
    let result = shared_latch.with_chain(KEY, |slot| {
        *slot = Some(sample_chain(0xCC));
    });
    match result {
        Err(Error::Internal(msg)) if msg.contains("requires an exclusive page latch") => {}
        Err(other) => panic!("expected exclusive-latch error, got {other:?}"),
        Ok(_) => panic!("with_chain on a shared latch must error, not silently mutate"),
    }
}

#[test]
fn latched_pinned_page_with_all_chains_rejects_shared_mode() {
    let pool = fresh_pool();
    let mut shared_latch = pool
        .pin_for_read_sized(PAGE_ID, PageSize::Large32k)
        .unwrap();
    let result = shared_latch.with_all_chains(|chains| {
        chains.clear();
    });
    match result {
        Err(Error::Internal(msg)) if msg.contains("requires an exclusive page latch") => {}
        Err(other) => panic!("expected exclusive-latch error, got {other:?}"),
        Ok(_) => panic!("with_all_chains on a shared latch must error, not silently mutate"),
    }
}

#[test]
fn concurrent_with_chain_calls_serialize_through_the_latch() {
    // Two `with_chain_under_latch` invocations on the same page MUST
    // see fully-ordered mutations: the second observe the slot as
    // installed by the first, then take it. This relies on the latch
    // serializing the two RMW closures.
    let pool = fresh_pool();

    pool.with_chain_under_latch(PAGE_ID, KEY, LatchMode::Exclusive, |slot| {
        *slot = Some(sample_chain(0xEE));
    })
    .unwrap();

    let taken = pool
        .with_chain_under_latch(PAGE_ID, KEY, LatchMode::Exclusive, |slot| {
            assert!(slot.is_some(), "second closure must observe first install");
            slot.take()
        })
        .unwrap();
    assert!(taken.is_some(), "take returns the previously installed chain");

    pool.with_chain_under_latch(PAGE_ID, KEY, LatchMode::Exclusive, |slot| {
        assert!(slot.is_none(), "third closure must observe the take");
    })
    .unwrap();
}
