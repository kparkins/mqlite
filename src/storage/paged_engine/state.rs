//! Shared + metadata state for the PagedEngine.

use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

use crate::error::{Error, Result};
use crate::mvcc::timestamp::TimestampOracle;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::catalog::{open_with_fallback as catalog_open_with_fallback, Catalog};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::history_store::HistoryStore;
use crate::storage::root_snapshot::PublishedSnapshot;

use super::catalog_ops::build_snapshot_from_catalog;

// ---------------------------------------------------------------------------
// SharedState — fields shared by read path (no mutex) and writer (mutex held)
// ---------------------------------------------------------------------------

/// State shared by the read path (no mutex) and the writer inside
/// `Mutex<BpBackend>`. Under PR 8 this becomes the full MWMR shared
/// state; in PR 4 it's only what reads need.
pub(crate) struct SharedState {
    pub handle: Arc<BufferPoolHandle>,
    pub history_store: Arc<std::sync::Mutex<HistoryStore<BufferPoolPageStore>>>,
    pub oracle: Arc<TimestampOracle>,
    /// Atomically published snapshot for the mutex-free read path.
    pub published: ArcSwap<PublishedSnapshot>,
    /// Monotonic transaction identifier source shared by readers and writers.
    pub txn_counter: AtomicU64,
}

// ---------------------------------------------------------------------------
// MetadataState — catalog wrapped in metadata RwLock (PR 8)
// ---------------------------------------------------------------------------

/// Per-engine catalog state protected by an `RwLock`. DDL ops take the
/// write guard to gain exclusive access; CRUD writers take the read
/// guard (shared with other CRUD writers) and mutate the catalog via
/// the interior `Mutex<Catalog>`.
///
/// Lock order: `metadata` RwLock -> `ns_lanes` mutex -> `commit_seq`
/// mutex -> `catalog` Mutex. DO NOT grab `metadata.write()` while
/// holding the catalog mutex — that would invert the order relative to
/// a reader that already holds `metadata.read()` and is waiting for the
/// catalog mutex.
pub(crate) struct MetadataState {
    /// Catalog B+ tree for collection/index metadata.
    ///
    /// Wrapped in `Mutex` so CRUD writers can mutate under
    /// `metadata.read()` without upgrading to `write()`. DDL paths
    /// still take `metadata.write()` for coarse-grain CRUD-vs-DDL
    /// exclusion; they also briefly acquire this mutex, which is
    /// uncontended while no CRUD writer holds `metadata.read()`.
    pub catalog: std::sync::Mutex<Catalog<BufferPoolPageStore>>,
}

impl MetadataState {
    /// Create the initial MetadataState + SharedState from an existing
    /// (or fresh) buffer pool handle.
    pub(super) fn new(
        handle: Arc<BufferPoolHandle>,
        catalog_root_page: u32,
        catalog_root_level: u8,
    ) -> Result<(Self, Arc<SharedState>)> {
        let store = BufferPoolPageStore::new(Arc::clone(&handle));
        let backup_root = handle
            .allocator()
            .with_header(|h| h.catalog_root_backup)?;
        let (catalog, used_backup) = catalog_open_with_fallback(
            store,
            catalog_root_page,
            catalog_root_level,
            backup_root,
            catalog_root_level,
            |_page| true,
        )?;
        let _ = used_backup; // noted for tracing/logging if needed
        // T7 — journal-tail HLC oracle recovery: floor the oracle above
        // every durable ChainCommit from the previous lifetime. Missing
        // `successor()` (saturated `Ts::MAX`) is a hard error per plan.
        let oracle = Arc::new(TimestampOracle::new());
        if let Some(max_ts) = handle.recovered_max_commit_ts()? {
            match max_ts.successor() {
                Some(next) => oracle.set_min(next),
                None => return Err(Error::TimestampExhausted),
            }
        }
        // Plan §T7: construct the history store on the dedicated
        // history-routed page store. A fresh tree is built every open — the
        // previous lifetime's entries are not persisted across restart
        // because reconciliation repopulates it lazily (plan deferral 905).
        let history_store_inner = HistoryStore::create(
            BufferPoolPageStore::new_history(Arc::clone(&handle)),
        )?;

        // Build the initial published snapshot from the catalog.
        let initial_snap = build_snapshot_from_catalog(
            &catalog,
            oracle.now(),
        )?;

        let shared = Arc::new(SharedState {
            handle,
            history_store: Arc::new(std::sync::Mutex::new(history_store_inner)),
            oracle,
            published: ArcSwap::from_pointee(initial_snap),
            txn_counter: AtomicU64::new(1),
        });

        let md = Self { catalog: std::sync::Mutex::new(catalog) };
        // For a new database, persist the freshly-allocated catalog root
        // to the file header immediately (will be written to disk on flush).
        if catalog_root_page == 0 {
            let cat = md.catalog.lock().expect("catalog poisoned");
            let root_page = cat.root_page();
            let root_level = cat.root_level();
            drop(cat);
            shared.handle.allocator().update_header(|h| {
                h.catalog_root_page = root_page;
                h.catalog_root_level = root_level;
                h.catalog_root_backup = root_page;
            })?;
        }
        Ok((md, shared))
    }

}

// ---------------------------------------------------------------------------
// OwnedLaneGuard — holds Arc<Mutex<()>> + its MutexGuard together so the
// stdlib Mutex lifetime restriction is satisfied.
// ---------------------------------------------------------------------------

pub(super) struct OwnedLaneGuard {
    // The Arc keeps the Mutex alive. `_guard` is the MutexGuard that
    // references the Mutex through `lane`. We hold the Arc AFTER the
    // guard so drop order is: guard (release lock) then lane.
    pub(super) _guard: std::sync::MutexGuard<'static, ()>,
    pub(super) _lane: Arc<Mutex<()>>,
}

impl OwnedLaneGuard {
    pub(super) fn new(lane: Arc<Mutex<()>>, guard: std::sync::MutexGuard<'_, ()>) -> Self {
        // Extend the lifetime of the guard to 'static. Safe because we
        // keep `lane` alive inside `Self`, so the backing Mutex lives at
        // least as long as the guard.
        let guard_static: std::sync::MutexGuard<'static, ()> =
            unsafe { std::mem::transmute(guard) };
        OwnedLaneGuard {
            _guard: guard_static,
            _lane: lane,
        }
    }
}

/// Resolve the per-namespace lane mutex, creating one if needed.
pub(super) fn lane_for(engine: &super::PagedEngine, ns: &str) -> Arc<Mutex<()>> {
    if let Some(entry) = engine.ns_lanes.get(ns) {
        return Arc::clone(entry.value());
    }
    engine
        .ns_lanes
        .entry(ns.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

/// Acquire the namespace lane with busy-timeout / busy-handler semantics.
pub(super) fn acquire_lane(
    engine: &super::PagedEngine,
    lane: Arc<Mutex<()>>,
) -> Result<OwnedLaneGuard> {
    let lane_ptr: *const Mutex<()> = Arc::as_ptr(&lane);

    // Fast path: try without any spin first.
    match unsafe { &*lane_ptr }.try_lock() {
        Ok(g) => return Ok(OwnedLaneGuard::new(lane, g)),
        Err(std::sync::TryLockError::Poisoned(_)) => {
            return Err(Error::Internal("namespace lane mutex poisoned".into()));
        }
        Err(std::sync::TryLockError::WouldBlock) => {}
    }

    let timeout = engine.busy_timeout;
    if let Some(handler) = &engine.busy_handler {
        let mut attempts: u32 = 0;
        loop {
            std::thread::sleep(Duration::from_millis(1));
            match unsafe { &*lane_ptr }.try_lock() {
                Ok(g) => return Ok(OwnedLaneGuard::new(lane, g)),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(Error::Internal(
                        "namespace lane mutex poisoned".into(),
                    ));
                }
                Err(std::sync::TryLockError::WouldBlock) => {}
            }
            if !handler.0(attempts) {
                return Err(Error::WriterBusy);
            }
            attempts = attempts.saturating_add(1);
        }
    }

    if timeout.is_zero() {
        return Err(Error::WriterBusy);
    }

    let deadline = Instant::now() + timeout;
    let guard = match unsafe { &*lane_ptr }.lock() {
        Ok(g) => g,
        Err(_) => {
            return Err(Error::Internal("namespace lane mutex poisoned".into()));
        }
    };
    if Instant::now() >= deadline && timeout > Duration::ZERO {
        let _ = deadline;
    }
    Ok(OwnedLaneGuard::new(lane, guard))
}
