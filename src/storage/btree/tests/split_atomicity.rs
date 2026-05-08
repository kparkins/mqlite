//! Phase 3 US-017 split atomicity regression test.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::error::Result;
use crate::mvcc::{ChainSnapshot, Ts, VersionData, VersionEntry, VersionState};
use crate::storage::buffer_pool::PageSize;
use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

use super::{BTree, BTreePageStore, LeafPageImage, MemPageStore};

const SPLIT_VALUE_BYTES: usize = 6_000;
const BASE_KEYS_BEFORE_SPLIT: [u64; 4] = [10, 20, 30, 40];
const BASE_KEYS_TO_FORCE_SPLIT: [u64; 2] = [50, 60];
const ALL_BASE_CHAIN_KEYS: [u64; 2] = [20, 40];
const ALL_DELTA_ONLY_CHAIN_KEYS: [u64; 2] = [25, 55];
const MIXED_CHAIN_KEYS: [u64; 4] = [20, 25, 40, 55];
const OBSERVATION_ITERATIONS: usize = 10_000;
const WINDOW_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const UNWATCHED_PAGE: u32 = u32::MAX;
const ENTRY_MARKER: u8 = 0xA5;

type ChainArc = Arc<VecDeque<VersionEntry>>;
type ChainDrain = Vec<(Vec<u8>, ChainArc)>;
type InternalPageBytes = [u8; PAGE_SIZE_INTERNAL as usize];
type InternalPage = Box<[u8; PAGE_SIZE_INTERNAL as usize]>;
type LeafPageBytes = [u8; PAGE_SIZE_LEAF as usize];
type LeafRead = (LeafPageImage, Option<ChainSnapshot>);

#[derive(Clone, Copy)]
enum ChainDistribution {
    AllBase,
    AllDeltaOnly,
    Mixed,
}

impl ChainDistribution {
    fn keys(self) -> &'static [u64] {
        match self {
            Self::AllBase => &ALL_BASE_CHAIN_KEYS,
            Self::AllDeltaOnly => &ALL_DELTA_ONLY_CHAIN_KEYS,
            Self::Mixed => &MIXED_CHAIN_KEYS,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::AllBase => "all-base",
            Self::AllDeltaOnly => "all-delta-only",
            Self::Mixed => "mixed",
        }
    }
}

struct SplitWindowProbe {
    watched_page: AtomicU32,
    reader_attempts: AtomicUsize,
    drain_seen: AtomicBool,
}

impl SplitWindowProbe {
    fn new() -> Self {
        Self {
            watched_page: AtomicU32::new(UNWATCHED_PAGE),
            reader_attempts: AtomicUsize::new(0),
            drain_seen: AtomicBool::new(false),
        }
    }

    fn watch_page(&self, page: u32) {
        self.watched_page.store(page, Ordering::SeqCst);
    }

    fn note_reader_attempt(&self) {
        self.reader_attempts.fetch_add(1, Ordering::SeqCst);
    }

    fn after_chain_drain(&self, page: u32) {
        if self.watched_page.load(Ordering::SeqCst) != page {
            return;
        }

        self.drain_seen.store(true, Ordering::SeqCst);
        let started = Instant::now();
        while self.reader_attempts.load(Ordering::SeqCst) == 0 {
            assert!(
                started.elapsed() < WINDOW_WAIT_TIMEOUT,
                "reader did not attempt to enter the split window"
            );
            thread::yield_now();
        }
    }

    fn assert_drain_seen(&self, label: &str) {
        assert!(
            self.drain_seen.load(Ordering::SeqCst),
            "{label}: split drain hook should have been exercised"
        );
    }
}

#[derive(Clone)]
struct StoreObserver {
    inner: Arc<Mutex<MemPageStore>>,
}

impl StoreObserver {
    fn chain_locations(&self, key: &[u8]) -> Vec<u32> {
        let store = self.inner.lock().expect("store mutex poisoned");
        store
            .leaf_chains
            .iter()
            .filter_map(|(page, chains)| chains.contains_key(key).then_some(*page))
            .collect()
    }
}

struct ObservedStore {
    inner: Arc<Mutex<MemPageStore>>,
    split_window: Arc<SplitWindowProbe>,
}

impl ObservedStore {
    fn new(split_window: Arc<SplitWindowProbe>) -> (Self, StoreObserver) {
        let inner = Arc::new(Mutex::new(MemPageStore::new()));
        (
            Self {
                inner: Arc::clone(&inner),
                split_window,
            },
            StoreObserver { inner },
        )
    }

    fn with_store<T>(&self, f: impl FnOnce(&MemPageStore) -> T) -> T {
        let store = self.inner.lock().expect("store mutex poisoned");
        f(&store)
    }

    fn with_store_mut<T>(&self, f: impl FnOnce(&mut MemPageStore) -> T) -> T {
        let mut store = self.inner.lock().expect("store mutex poisoned");
        f(&mut store)
    }
}

impl BTreePageStore for ObservedStore {
    type SharedReadGuard<'a>
        = ()
    where
        Self: 'a;

    fn read_internal(&self, page: u32) -> Result<InternalPage> {
        self.with_store(|store| store.read_internal(page))
    }

    fn read_leaf(&self, page: u32) -> Result<LeafRead> {
        self.with_store(|store| store.read_leaf(page))
    }

    fn pin_shared_for_read<'a>(
        &'a self,
        _page: u32,
        _size: PageSize,
    ) -> Result<Self::SharedReadGuard<'a>> {
        Ok(())
    }

    fn write_internal(&mut self, page: u32, data: &InternalPageBytes) -> Result<()> {
        self.with_store_mut(|store| store.write_internal(page, data))
    }

    fn write_leaf_structural(&mut self, page: u32, data: &LeafPageBytes) -> Result<()> {
        self.with_store_mut(|store| store.write_leaf_structural(page, data))
    }

    fn alloc_internal(&mut self) -> Result<u32> {
        self.with_store_mut(MemPageStore::alloc_internal)
    }

    fn alloc_leaf(&mut self) -> Result<u32> {
        self.with_store_mut(MemPageStore::alloc_leaf)
    }

    fn free_internal(&mut self, page: u32) -> Result<()> {
        self.with_store_mut(|store| store.free_internal(page))
    }

    fn free_leaf(&mut self, page: u32) -> Result<()> {
        self.with_store_mut(|store| store.free_leaf(page))
    }

    fn take_chain(&mut self, page: u32, key: &[u8]) -> Result<Option<ChainArc>> {
        self.with_store_mut(|store| store.take_chain(page, key))
    }

    fn put_chain(&mut self, page: u32, key: Vec<u8>, chain: ChainArc) -> Result<()> {
        self.with_store_mut(|store| store.put_chain(page, key, chain))
    }

    fn chains_empty(&self, page: u32) -> Result<bool> {
        self.with_store(|store| store.chains_empty(page))
    }

    fn clear_chains(&mut self, page: u32) -> Result<()> {
        self.with_store_mut(|store| store.clear_chains(page))
    }

    fn take_all_chains(&mut self, page: u32) -> Result<ChainDrain> {
        self.take_all_chains_on_page(page)
    }

    fn take_all_chains_on_page(&mut self, page: u32) -> Result<ChainDrain> {
        let chains = self.with_store_mut(|store| store.take_all_chains_on_page(page))?;
        self.split_window.after_chain_drain(page);
        Ok(chains)
    }
}

fn key(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

fn encoded_keys(distribution: ChainDistribution) -> Vec<Vec<u8>> {
    distribution.keys().iter().copied().map(key).collect()
}

fn entry() -> VersionEntry {
    VersionEntry {
        start_ts: Ts {
            physical_ms: ENTRY_MARKER as u64,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id: ENTRY_MARKER as u64,
        state: VersionState::Committed,
        data: VersionData::Inline(vec![ENTRY_MARKER]),
        is_tombstone: false,
    }
}

fn chain() -> ChainArc {
    Arc::new([entry()].into_iter().collect())
}

fn insert_base_keys(tree: &mut BTree<ObservedStore>) -> Result<()> {
    let value = vec![0xCC; SPLIT_VALUE_BYTES];
    for raw_key in BASE_KEYS_BEFORE_SPLIT {
        tree.insert(&key(raw_key), &value)?;
    }
    Ok(())
}

fn spawn_reader(
    lane: Arc<Mutex<()>>,
    observer: StoreObserver,
    split_window: Arc<SplitWindowProbe>,
    keys: Vec<Vec<u8>>,
) -> thread::JoinHandle<Vec<Vec<u8>>> {
    thread::spawn(move || {
        let mut missing = Vec::new();
        for _ in 0..OBSERVATION_ITERATIONS {
            split_window.note_reader_attempt();
            let _lane_guard = lane.lock().expect("namespace lane mutex poisoned");
            for key in &keys {
                if observer.chain_locations(key).is_empty() {
                    missing.push(key.clone());
                }
            }
        }
        missing
    })
}

fn run_split_atomicity_case(distribution: ChainDistribution) -> Result<()> {
    let split_window = Arc::new(SplitWindowProbe::new());
    let (store, observer) = ObservedStore::new(Arc::clone(&split_window));
    let mut tree = BTree::create(store)?;
    insert_base_keys(&mut tree)?;

    let left_page = tree.root_page;
    split_window.watch_page(left_page);
    let watched_keys = encoded_keys(distribution);
    for watched_key in &watched_keys {
        tree.store
            .put_chain(left_page, watched_key.clone(), chain())?;
    }

    let namespace_lane = Arc::new(Mutex::new(()));
    let lane_guard = namespace_lane
        .lock()
        .expect("namespace lane mutex poisoned");
    let reader = spawn_reader(
        Arc::clone(&namespace_lane),
        observer.clone(),
        Arc::clone(&split_window),
        watched_keys.clone(),
    );

    let value = vec![0xCC; SPLIT_VALUE_BYTES];
    for raw_key in BASE_KEYS_TO_FORCE_SPLIT {
        tree.insert(&key(raw_key), &value)?;
    }
    assert_eq!(tree.root_level, 1, "test setup should split the root leaf");
    split_window.assert_drain_seen(distribution.label());

    drop(lane_guard);
    let missing = reader.join().expect("reader thread panicked");
    assert!(
        missing.is_empty(),
        "{}: reader observed a chain on neither split sibling",
        distribution.label()
    );

    for watched_key in &watched_keys {
        assert_eq!(
            observer.chain_locations(watched_key).len(),
            1,
            "{}: chain should be present on exactly one split sibling",
            distribution.label()
        );
    }
    Ok(())
}

#[test]
fn test_split_atomic_no_reader_sees_chain_on_neither_leaf() -> Result<()> {
    for distribution in [
        ChainDistribution::AllBase,
        ChainDistribution::AllDeltaOnly,
        ChainDistribution::Mixed,
    ] {
        run_split_atomicity_case(distribution)?;
    }
    Ok(())
}
