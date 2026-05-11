//! PR2 running-sum cache invariant proof.
//!
//! Four load-bearing tests:
//!
//! 1. **`stress_10k_mutations_keep_cache_consistent`** — runs 10 000
//!    seeded random mutations on a single page through every
//!    cache-touching mutator (`with_chain_under_latch`,
//!    `with_all_chains_under_latch`). After every mutation, the
//!    cached `live_delta_payload_bytes` is compared against a fresh
//!    recompute via `frame_live_delta_payload_bytes`. Divergence
//!    (cached != fresh) panics with the action + key + before/after
//!    cached + fresh — the test is the canonical proof that the
//!    cache stays in sync with `frame.deltas` across arbitrary
//!    mutations.
//!
//! 2. **`phase_b_swap_preserves_cache`** — installs N chains with
//!    `Pending(txn_id)` heads, snapshots the cache, runs
//!    `try_swap_chains_if_unchanged` to flip them all to
//!    `Committed`, and asserts the cache value is **unchanged**.
//!    This is the load-bearing invariant cited by the
//!    `try_swap_chains_if_unchanged` debug_assert: Pending ->
//!    Committed flips do not change `chain_live_head_bytes` because
//!    the formula filters on `state != Aborted` (both Pending and
//!    Committed satisfy) and the head's `key`/`data`/`stop_ts`/
//!    `is_tombstone` are identical before and after.
//!
//! 3. **`phase_b_abort_swap_updates_cache`** — proves the same Phase B
//!    install path subtracts bytes when Pending insert heads flip to
//!    Aborted during pre-durable cleanup. This guards the non-debug
//!    release path, where a stale cache would otherwise survive.
//!
//! 4. **`replace_leaf_and_chains_recomputes_cache`** — exercises
//!    audit Finding A: the checkpoint dirty-leaf reconcile path
//!    bypasses `with_all_chains` to swap both the page bytes and the
//!    chain map atomically. PR2 wires a fresh recompute into that
//!    path; this test asserts the cache equals
//!    `frame_live_delta_payload_bytes(&new_chains)` after the swap.
//!
//! Without these three tests, the cache could diverge from
//! `frame.deltas` undetected and silently corrupt the
//! overflow-decision in `live_delta_payload_exceeds_leaf_budget`.

#![allow(clippy::panic, clippy::unwrap_used)]

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};

use crate::error::Result;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::buffer_pool::{
    BufferPool, LatchMode, PageSize, PageSource, PreparedChainSwap, ReplaceLeafError,
    RetainedLeafChains, SwapOutcome,
};
use crate::storage::page::{PAGE_SIZE_LEAF, PAGE_TYPE_LEAF};

const PAGE_ID: u32 = 31;
const STRESS_ITERATIONS: usize = 10_000;
const STRESS_KEY_POOL: usize = 64;
const REPLACE_LEAF_EVERY_N: usize = 250;

#[derive(Default)]
struct MockIo {
    pages: StdMutex<BTreeMap<u32, Vec<u8>>>,
}

impl PageSource for MockIo {
    fn read_page(&self, page: u32, _size: PageSize, buf: &mut [u8]) -> Result<()> {
        let pages = self.pages.lock().unwrap();
        if let Some(data) = pages.get(&page) {
            buf.copy_from_slice(data);
        } else {
            buf.fill(0);
            buf[0] = PAGE_TYPE_LEAF;
        }
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

/// Tiny seeded xorshift64 RNG — avoids pulling in `rand` for a test.
struct XorShift(u64);

impl XorShift {
    fn new(seed: u64) -> Self {
        // xorshift requires a non-zero seed.
        Self(if seed == 0 { 0x9E3779B97F4A7C15 } else { seed })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn next_in(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    fn coin(&mut self, p_num: u32, p_den: u32) -> bool {
        (self.next_u64() % p_den as u64) < p_num as u64
    }
}

fn pending_entry(start_ts_ms: u64, txn_id: u64, payload: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: Ts {
            physical_ms: start_ts_ms,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id,
        state: VersionState::Pending { txn_id },
        data: VersionData::Inline(payload.to_vec()),
        is_tombstone: false,
    }
}

fn committed_entry(start_ts_ms: u64, payload: &[u8], tombstone: bool) -> VersionEntry {
    VersionEntry {
        start_ts: Ts {
            physical_ms: start_ts_ms,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id: 1,
        state: VersionState::Committed,
        data: VersionData::Inline(payload.to_vec()),
        is_tombstone: tombstone,
    }
}

fn aborted_entry(start_ts_ms: u64, payload: &[u8]) -> VersionEntry {
    VersionEntry {
        start_ts: Ts {
            physical_ms: start_ts_ms,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id: 99,
        state: VersionState::Aborted,
        data: VersionData::Inline(payload.to_vec()),
        is_tombstone: false,
    }
}

fn key_for(i: usize) -> Vec<u8> {
    let mut k = b"k:".to_vec();
    k.extend_from_slice(&(i as u32).to_be_bytes());
    k
}

fn random_chain(rng: &mut XorShift, ts_seed: u64) -> Arc<VecDeque<VersionEntry>> {
    let mut chain = VecDeque::new();
    // Start with 1-3 entries randomly mixing Committed / Pending /
    // Aborted / tombstone.
    let depth = 1 + (rng.next_in(3));
    for d in 0..depth {
        let ts = ts_seed + d as u64;
        let payload_len = rng.next_in(40) + 1;
        let payload: Vec<u8> = (0..payload_len).map(|_| (rng.next_u64() & 0xFF) as u8).collect();
        let pick = rng.next_in(5);
        let entry = match pick {
            0 => pending_entry(ts, 12345, &payload),
            1 => committed_entry(ts, &payload, false),
            2 => committed_entry(ts, &payload, true), // tombstone
            3 => aborted_entry(ts, &payload),
            _ => committed_entry(ts, &payload, false),
        };
        chain.push_front(entry);
    }
    Arc::new(chain)
}

fn assert_cache_consistent(pool: &BufferPool, action: &str, iter: usize) {
    let cached = pool
        .live_delta_payload_bytes_for_test(PAGE_ID)
        .expect("page must be resident");
    let fresh = pool
        .live_delta_payload_bytes_fresh_for_test(PAGE_ID)
        .expect("page must be resident");
    assert_eq!(
        cached, fresh,
        "running-sum cache divergence at iter={iter}, action={action}: \
         cached={cached}, fresh={fresh}",
    );
}

#[test]
fn stress_10k_mutations_keep_cache_consistent() {
    let pool = fresh_pool();
    let mut rng = XorShift::new(0x5EED);
    let mut ts_counter = 1_000u64;

    // Seed phase: install initial chains on roughly half the keys.
    for i in 0..STRESS_KEY_POOL {
        if rng.coin(1, 2) {
            let chain = random_chain(&mut rng, ts_counter);
            ts_counter += 10;
            let key = key_for(i);
            pool.with_chain_under_latch(PAGE_ID, &key, LatchMode::Exclusive, |slot| {
                *slot = Some(chain);
            })
            .unwrap();
            assert_cache_consistent(&pool, "seed", i);
        }
    }

    for iter in 0..STRESS_ITERATIONS {
        let key_idx = rng.next_in(STRESS_KEY_POOL);
        let key = key_for(key_idx);

        // Roll an action. Drain (with_all_chains) is rare to keep
        // the test long enough that the per-key delta-update path
        // dominates iteration counts. replace_leaf_and_chains is
        // separately exercised below.
        let roll = rng.next_in(100);
        let action = match roll {
            0..=39 => "insert_or_replace",
            40..=59 => "take",
            60..=84 => "mutate_in_place",
            85..=94 => "no_op_inspect",
            _ => "drain_all",
        };

        match action {
            "insert_or_replace" => {
                let chain = random_chain(&mut rng, ts_counter);
                ts_counter += 10;
                pool.with_chain_under_latch(PAGE_ID, &key, LatchMode::Exclusive, |slot| {
                    *slot = Some(chain);
                })
                .unwrap();
            }
            "take" => {
                pool.with_chain_under_latch(PAGE_ID, &key, LatchMode::Exclusive, |slot| {
                    let _ = slot.take();
                })
                .unwrap();
            }
            "mutate_in_place" => {
                let new_payload_len = rng.next_in(40) + 1;
                let new_payload: Vec<u8> = (0..new_payload_len)
                    .map(|_| (rng.next_u64() & 0xFF) as u8)
                    .collect();
                let push_or_pop = rng.next_in(3);
                let new_ts = ts_counter;
                ts_counter += 10;
                pool.with_chain_under_latch(PAGE_ID, &key, LatchMode::Exclusive, |slot| {
                    if let Some(chain_arc) = slot.as_mut() {
                        let chain_mut = Arc::make_mut(chain_arc);
                        match push_or_pop {
                            0 => chain_mut.push_front(committed_entry(new_ts, &new_payload, false)),
                            1 => {
                                let _ = chain_mut.pop_back();
                            }
                            _ => chain_mut.push_back(aborted_entry(new_ts, &new_payload)),
                        }
                        if chain_mut.is_empty() {
                            *slot = None;
                        }
                    } else {
                        // No-chain: insert a fresh one to make the
                        // step productive.
                        *slot = Some(Arc::new(VecDeque::from([committed_entry(
                            new_ts,
                            &new_payload,
                            false,
                        )])));
                    }
                })
                .unwrap();
            }
            "no_op_inspect" => {
                pool.with_chain_under_latch(PAGE_ID, &key, LatchMode::Exclusive, |_slot| {})
                    .unwrap();
            }
            "drain_all" => {
                pool.with_all_chains_under_latch(PAGE_ID, LatchMode::Exclusive, |chains| {
                    if rng.coin(1, 4) {
                        chains.clear();
                    } else {
                        // Partial drain: remove every-other key.
                        let keep: Vec<Vec<u8>> = chains.keys().cloned().collect();
                        for (i, k) in keep.into_iter().enumerate() {
                            if i % 2 == 0 {
                                chains.remove(&k);
                            }
                        }
                    }
                })
                .unwrap();
            }
            _ => unreachable!(),
        }

        assert_cache_consistent(&pool, action, iter);

        // Periodically run a `replace_leaf_and_chains` to exercise
        // audit Finding A's recompute path.
        if iter > 0 && iter % REPLACE_LEAF_EVERY_N == 0 {
            // Build a fresh chains map roughly half-populated.
            let mut new_chains: RetainedLeafChains = BTreeMap::new();
            for i in 0..STRESS_KEY_POOL {
                if rng.coin(2, 3) {
                    new_chains.insert(key_for(i), random_chain(&mut rng, ts_counter));
                    ts_counter += 10;
                }
            }
            // A valid leaf image: PAGE_TYPE_LEAF byte + zeros.
            let mut new_base = vec![0u8; PAGE_SIZE_LEAF as usize];
            new_base[0] = PAGE_TYPE_LEAF;
            // replace_leaf_and_chains needs an exclusive-latched
            // pin; the BufferPool wrapper handles latching for us.
            let mut excl = pool
                .pin_for_write_sized(PAGE_ID, PageSize::Large32k)
                .unwrap();
            match pool.replace_leaf_and_chains(&mut excl, new_base, new_chains) {
                Ok(()) => {}
                Err(ReplaceLeafError::NotResident) => {
                    panic!("page should be resident under our pin")
                }
                Err(ReplaceLeafError::NotLeaf) => panic!("constructed leaf image rejected"),
            }
            drop(excl);
            assert_cache_consistent(&pool, "replace_leaf_and_chains", iter);
        }
    }
}

#[test]
fn phase_b_swap_preserves_cache() {
    let pool = fresh_pool();

    // Install N chains, each with a Pending(TXN_ID) head over a
    // committed tail (so the chain has live head, depth 2).
    const TXN_ID: u64 = 0xC0FFEE;
    const N: usize = 8;
    let commit_ts = 100u64;
    let pending_ts = 200u64;
    for i in 0..N {
        let key = key_for(i);
        let chain = Arc::new(VecDeque::from([
            pending_entry(pending_ts, TXN_ID, &[i as u8]),
            committed_entry(commit_ts, &[i as u8], false),
        ]));
        pool.with_chain_under_latch(PAGE_ID, &key, LatchMode::Exclusive, |slot| {
            *slot = Some(chain);
        })
        .unwrap();
    }

    let cached_before = pool
        .live_delta_payload_bytes_for_test(PAGE_ID)
        .expect("page must be resident");
    // Sanity: cache equals fresh recompute.
    assert_cache_consistent(&pool, "phase_b_setup", 0);

    // Build PreparedChainSwap entries that flip Pending head to
    // Committed (preserving payload, key, stop_ts, is_tombstone).
    let mut prepared: Vec<PreparedChainSwap> = Vec::with_capacity(N);
    {
        let mut excl = pool
            .pin_for_write_sized(PAGE_ID, PageSize::Large32k)
            .unwrap();
        for i in 0..N {
            let key = key_for(i);
            let expected_old = excl
                .snapshot_chain_arc(&key)
                .expect("chain installed in setup");
            let mut new_chain = expected_old.clone();
            let new_inner = Arc::make_mut(&mut new_chain);
            if let Some(head) = new_inner.front_mut() {
                head.state = VersionState::Committed;
            }
            prepared.push(PreparedChainSwap {
                key,
                new_chain,
                expected_old,
            });
        }

        let outcome = excl.try_swap_chains_if_unchanged(prepared).unwrap();
        assert!(matches!(outcome, SwapOutcome::Success), "swap must succeed");
    }

    let cached_after = pool
        .live_delta_payload_bytes_for_test(PAGE_ID)
        .expect("page must still be resident");
    assert_eq!(
        cached_before, cached_after,
        "Phase B Pending->Committed flip must NOT change \
         live_delta_payload_bytes (before={cached_before}, \
         after={cached_after})"
    );
    // And the cache still matches a fresh recompute.
    assert_cache_consistent(&pool, "phase_b_post_flip", 0);
}

#[test]
fn phase_b_abort_swap_updates_cache() {
    let pool = fresh_pool();

    const TXN_ID: u64 = 0xAB0A7;
    const N: usize = 8;
    let pending_ts = 200u64;
    for i in 0..N {
        let key = key_for(i);
        let chain = Arc::new(VecDeque::from([pending_entry(
            pending_ts + i as u64,
            TXN_ID,
            &[i as u8, 0xAA],
        )]));
        pool.with_chain_under_latch(PAGE_ID, &key, LatchMode::Exclusive, |slot| {
            *slot = Some(chain);
        })
        .unwrap();
    }

    let cached_before = pool
        .live_delta_payload_bytes_for_test(PAGE_ID)
        .expect("page must be resident");
    assert!(
        cached_before > 0,
        "pending inserts must produce cache bytes"
    );
    assert_cache_consistent(&pool, "phase_b_abort_setup", 0);

    let mut prepared: Vec<PreparedChainSwap> = Vec::with_capacity(N);
    {
        let mut excl = pool
            .pin_for_write_sized(PAGE_ID, PageSize::Large32k)
            .unwrap();
        for i in 0..N {
            let key = key_for(i);
            let expected_old = excl
                .snapshot_chain_arc(&key)
                .expect("chain installed in setup");
            let mut new_chain = expected_old.clone();
            let new_inner = Arc::make_mut(&mut new_chain);
            if let Some(head) = new_inner.front_mut() {
                head.state = VersionState::Aborted;
            }
            prepared.push(PreparedChainSwap {
                key,
                new_chain,
                expected_old,
            });
        }

        let outcome = excl.try_swap_chains_if_unchanged(prepared).unwrap();
        assert!(matches!(outcome, SwapOutcome::Success), "swap must succeed");
    }

    let cached_after = pool
        .live_delta_payload_bytes_for_test(PAGE_ID)
        .expect("page must still be resident");
    assert_eq!(
        cached_after, 0,
        "Phase B Pending->Aborted flip must subtract pending bytes \
         from live_delta_payload_bytes (before={cached_before}, \
         after={cached_after})"
    );
    assert_cache_consistent(&pool, "phase_b_abort_post_flip", 0);
}

#[test]
fn replace_leaf_and_chains_recomputes_cache() {
    let pool = fresh_pool();

    // Seed the page with some chains so the cache is non-zero.
    for i in 0..6 {
        let key = key_for(i);
        let chain = Arc::new(VecDeque::from([committed_entry(
            10 * (i + 1) as u64,
            &[0xAA, i as u8],
            false,
        )]));
        pool.with_chain_under_latch(PAGE_ID, &key, LatchMode::Exclusive, |slot| {
            *slot = Some(chain);
        })
        .unwrap();
    }
    let cached_before = pool
        .live_delta_payload_bytes_for_test(PAGE_ID)
        .expect("page must be resident");
    assert!(cached_before > 0, "seed must produce non-zero cache");

    // Build a new chain map with a mix of pending + committed +
    // tombstone entries.
    let mut new_chains: RetainedLeafChains = BTreeMap::new();
    new_chains.insert(
        key_for(0),
        Arc::new(VecDeque::from([committed_entry(500, b"hello", false)])),
    );
    new_chains.insert(
        key_for(1),
        Arc::new(VecDeque::from([pending_entry(501, 1234, b"pending-payload")])),
    );
    new_chains.insert(
        key_for(2),
        Arc::new(VecDeque::from([committed_entry(502, b"x", true)])), // tombstone
    );

    // A valid leaf image: PAGE_TYPE_LEAF byte + zeros.
    let mut new_base = vec![0u8; PAGE_SIZE_LEAF as usize];
    new_base[0] = PAGE_TYPE_LEAF;

    {
        let mut excl = pool
            .pin_for_write_sized(PAGE_ID, PageSize::Large32k)
            .unwrap();
        pool.replace_leaf_and_chains(&mut excl, new_base, new_chains)
            .expect("replace must succeed under our exclusive pin");
    }

    // Cache must equal a fresh recompute over the new chain set.
    assert_cache_consistent(&pool, "replace_leaf_and_chains", 0);
}
