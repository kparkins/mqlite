//! Bug-suspect: catalog `update_collection` / `update_index` are
//! delete-then-insert with NO atomicity.
//!
//! Suspect (deep-refactor-2026-06-10, rank ~1, catalog.rs `update_collection`
//! ~:599 / `update_index` ~:777): both helpers do
//!
//! ```ignore
//! let existed = self.tree.delete(&key)?;   // entry removed + leaf rewritten
//! ...
//! self.tree.insert(&key, &bytes)?;          // can fail independently
//! ```
//!
//! If the re-insert fails after the delete already committed to the tree
//! (DiskFull on a split, transient I/O), the entry VANISHES from the catalog
//! for the rest of the process lifetime. On the multikey-promotion path
//! (`index_maint` runs `update_index` during ordinary inserts) this turns a
//! transient error into a permanently missing index entry: the planner then
//! treats the index as dropped and writers stop maintaining it —
//! index/document divergence.
//!
//! Test method: a `FailingMemStore` wraps `MemPageStore` behind a shared
//! `Arc<Control>` so the test arms it WITHOUT reaching into `Catalog`'s
//! private store. A clean rehearsal run learns how many leaf writes
//! `update_*` issues; a second catalog is then armed to fail the LAST leaf
//! write (the re-insert). We assert the update returns `Err` AND the entry is
//! then GONE (`get_* -> Ok(None)`).
//!
//! Verdict outcome: entry GONE after a failed update -> REAL bug. A correct
//! atomic / undo-on-failure implementation would leave the OLD entry readable.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bson::doc;

use super::{Catalog, IndexEntry};
use crate::error::{Error, Result};
use crate::index::IndexModel;
use crate::mvcc::chain_snapshot::ChainSnapshot;
use crate::mvcc::version::VersionEntry;
use crate::storage::btree::{BTreePageStore, LeafPageImage, MemPageStore};
use crate::storage::buffer_pool::{LatchMode, PageSize};
use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

/// Shared control block so the test can arm the failpoint and read the
/// leaf-write counter without owning the store the `Catalog` holds.
#[derive(Default)]
struct Control {
    leaf_writes: AtomicUsize,
    /// 1-based index of the leaf write to fail; 0 disables injection.
    fail_on: AtomicUsize,
}

impl Control {
    fn arm(&self, fail_on: usize) {
        self.fail_on.store(fail_on, Ordering::Relaxed);
    }

    fn reset(&self) {
        self.leaf_writes.store(0, Ordering::Relaxed);
        self.fail_on.store(0, Ordering::Relaxed);
    }

    fn leaf_writes(&self) -> usize {
        self.leaf_writes.load(Ordering::Relaxed)
    }
}

/// Wraps [`MemPageStore`] and returns `Err` on the Nth `write_leaf_structural`
/// call, where N is published through the shared [`Control`].
struct FailingMemStore {
    inner: MemPageStore,
    control: Arc<Control>,
}

impl FailingMemStore {
    fn new(control: Arc<Control>) -> Self {
        Self {
            inner: MemPageStore::new(),
            control,
        }
    }
}

impl BTreePageStore for FailingMemStore {
    type SharedReadGuard<'a> = ();

    fn read_internal(&self, page: u32) -> Result<Box<[u8; PAGE_SIZE_INTERNAL as usize]>> {
        self.inner.read_internal(page)
    }

    fn read_leaf(&self, page: u32) -> Result<(LeafPageImage, Option<ChainSnapshot>)> {
        self.inner.read_leaf(page)
    }

    fn pin_shared_for_read<'a>(
        &'a self,
        _page: u32,
        _size: PageSize,
    ) -> Result<Self::SharedReadGuard<'a>> {
        Ok(())
    }

    fn write_internal(
        &mut self,
        page: u32,
        data: &[u8; PAGE_SIZE_INTERNAL as usize],
    ) -> Result<()> {
        self.inner.write_internal(page, data)
    }

    fn write_leaf_structural(
        &mut self,
        page: u32,
        data: &[u8; PAGE_SIZE_LEAF as usize],
    ) -> Result<()> {
        let n = self.control.leaf_writes.fetch_add(1, Ordering::Relaxed) + 1;
        let fail_on = self.control.fail_on.load(Ordering::Relaxed);
        if fail_on != 0 && n == fail_on {
            return Err(Error::Internal(
                "injected leaf-write failure (simulated DiskFull on update re-insert)".into(),
            ));
        }
        self.inner.write_leaf_structural(page, data)
    }

    fn alloc_internal(&mut self) -> Result<u32> {
        self.inner.alloc_internal()
    }

    fn alloc_leaf(&mut self) -> Result<u32> {
        self.inner.alloc_leaf()
    }

    fn free_internal(&mut self, page: u32) -> Result<()> {
        self.inner.free_internal(page)
    }

    fn free_leaf(&mut self, page: u32) -> Result<()> {
        self.inner.free_leaf(page)
    }

    fn chains_empty(&self, page: u32) -> Result<bool> {
        self.inner.chains_empty(page)
    }

    fn with_chain_under_latch<R, F>(
        &mut self,
        page: u32,
        key: &[u8],
        mode: LatchMode,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&mut Option<Arc<VecDeque<VersionEntry>>>) -> R,
    {
        self.inner.with_chain_under_latch(page, key, mode, f)
    }

    fn with_all_chains_under_latch<R, F>(&mut self, page: u32, mode: LatchMode, f: F) -> Result<R>
    where
        F: FnOnce(&mut BTreeMap<Vec<u8>, Arc<VecDeque<VersionEntry>>>) -> R,
    {
        self.inner.with_all_chains_under_latch(page, mode, f)
    }
}

fn now() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[test]
fn update_collection_loses_entry_when_reinsert_fails() {
    // --- Rehearsal: count the leaf writes a clean update_collection issues.
    let writes = {
        let control = Arc::new(Control::default());
        let mut cat = Catalog::create(FailingMemStore::new(Arc::clone(&control)))
            .expect("create catalog");
        let id = cat.allocate_namespace_id();
        cat.create_collection("users", id, doc! {}, now())
            .expect("create collection");
        let mut updated = cat
            .get_collection("users")
            .expect("get")
            .expect("collection exists");
        updated.document_count += 1;
        control.reset();
        cat.update_collection(&updated).expect("clean update");
        control.leaf_writes()
    };
    assert!(
        writes >= 1,
        "update_collection must issue at least one leaf write"
    );

    // --- Real run: arm the LAST leaf write (the re-insert) to fail.
    let control = Arc::new(Control::default());
    let mut cat =
        Catalog::create(FailingMemStore::new(Arc::clone(&control))).expect("create catalog");
    let id = cat.allocate_namespace_id();
    cat.create_collection("users", id, doc! {}, now())
        .expect("create collection");

    let mut updated = cat
        .get_collection("users")
        .expect("get")
        .expect("collection exists before update");
    updated.document_count = 42;

    let before = cat.get_collection("users").expect("get before");
    assert!(before.is_some(), "precondition: entry present before update");

    control.reset();
    control.arm(writes);
    let res = cat.update_collection(&updated);
    assert!(
        res.is_err(),
        "update_collection must surface the injected re-insert failure"
    );

    let after = cat.get_collection("users").expect("get after failed update");
    assert!(
        after.is_some(),
        "BUG: update_collection delete-then-insert lost the `users` entry on a \
         transient re-insert failure — the collection is now permanently \
         invisible to the catalog (was Some before the failed update)"
    );
}

#[test]
fn update_index_loses_entry_when_reinsert_fails() {
    // --- Rehearsal: count clean update_index leaf writes.
    let writes = {
        let control = Arc::new(Control::default());
        let mut cat = Catalog::create(FailingMemStore::new(Arc::clone(&control)))
            .expect("create catalog");
        let id = cat.allocate_namespace_id();
        cat.create_collection("users", id, doc! {}, now())
            .expect("create collection");
        let idx_id = cat.allocate_index_id();
        let model = IndexModel::builder().keys(doc! { "email": 1 }).build();
        cat.create_index("users", idx_id, &model, "email_1")
            .expect("create index");
        let mut idx: IndexEntry = cat
            .get_index("users", "email_1")
            .expect("get index")
            .expect("index exists");
        idx.entry_count += 1;
        control.reset();
        cat.update_index(&idx).expect("clean update");
        control.leaf_writes()
    };
    assert!(writes >= 1, "update_index must issue at least one leaf write");

    // --- Real run: arm the last write (the re-insert) to fail.
    let control = Arc::new(Control::default());
    let mut cat =
        Catalog::create(FailingMemStore::new(Arc::clone(&control))).expect("create catalog");
    let id = cat.allocate_namespace_id();
    cat.create_collection("users", id, doc! {}, now())
        .expect("create collection");
    let idx_id = cat.allocate_index_id();
    let model = IndexModel::builder().keys(doc! { "email": 1 }).build();
    cat.create_index("users", idx_id, &model, "email_1")
        .expect("create index");

    let mut idx: IndexEntry = cat
        .get_index("users", "email_1")
        .expect("get index")
        .expect("index exists before failing update");
    // Simulate the multikey promotion the suspect calls out.
    idx.multikey = true;

    let before = cat.get_index("users", "email_1").expect("get before");
    assert!(before.is_some(), "precondition: index entry present");

    control.reset();
    control.arm(writes);
    let res = cat.update_index(&idx);
    assert!(
        res.is_err(),
        "update_index must surface the injected re-insert failure"
    );

    let after = cat
        .get_index("users", "email_1")
        .expect("get index after failed update");
    assert!(
        after.is_some(),
        "BUG: update_index delete-then-insert lost the `email_1` index entry on \
         a transient re-insert failure — the planner now treats the index as \
         dropped and writers stop maintaining it (was Some before the failed \
         update)"
    );
}
