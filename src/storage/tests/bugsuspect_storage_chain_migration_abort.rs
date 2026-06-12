//! Bug-suspect: chain migration during split is not abort-safe.
//!
//! Suspect (deep-refactor-2026-06-10, rank ~7, structural_page_batch.rs abort
//! vs btree/insert.rs `split_leaf` -> `chain_migration::partition_chains_for_split`):
//! `StructuralBatchStore` stages page BYTES copy-on-write, but chain moves pass
//! THROUGH to the shared frames (`with_all_chains_under_latch` /
//! `with_chain_under_latch` delegate straight to the base store). A leaf split
//! inside a structural batch therefore DRAINS the upper-half chains off the
//! original leaf (`std::mem::take`) and re-homes them onto a freshly ALLOCATED
//! right page. If the batch then ABORTS, `AllocatorLifetimeBatch::abort` frees
//! AND `invalidate_page`s every batch allocation — dropping the migrated
//! chains — while the staged parent-routing bytes are discarded, so the
//! durable base still routes those keys to the (now chain-less) original leaf.
//! Committed resident versions vanish from memory.
//!
//! The suspect ranks this LOW because the only path that splits a leaf with
//! LIVE chains through a structural batch is gated by an engine-poison
//! escalation today — but that mitigation is undocumented, and the mechanism
//! itself (abort-after-migration drops chains) is concrete and testable.
//!
//! This test drives the mechanism directly: install a committed chain on a key
//! that a split moves to a new page, perform the split through the structural
//! batch, abort, then assert the chain survives at the original tree.
//!
//! VERDICT: REAL. The chain is GONE after the abort. Production reachability:
//! `checkpoint_materialize::apply_primary_checkpoint_delta` rebuilds the tree
//! with `insert`/`replace_existing` over `new_structural_store_chain_free`; the
//! chain-free flag suppresses READ snapshots but chain MUTATIONS
//! (`partition_chains_for_split`) still pass through to the real frames, and
//! the resident chains are not cleared until the POST-commit
//! `clear_materialized_chains` step. A rebuild `insert` of a delta-only key can
//! split a leaf that still carries live committed chains; if materialize then
//! aborts, that abort is classified `CheckpointFailure::Recoverable` (NO poison
//! — see snapshot_ops/checkpoint.rs) on the documented premise that "the engine
//! stays consistent". This test shows that premise is false once a split
//! migrated chains: the live engine keeps running with the committed versions
//! dropped from memory (recoverable only by a reopen + WAL replay).
//!
//! FIX (implemented — option (b), the simpler correct one): the structural
//! batch now TRACKS whether any chain mutation passed through its store onto
//! the shared frames (`StructuralPageBatch::migrated_chains`), set by the
//! batch store's `with_chain_under_latch` / `with_all_chains_under_latch`
//! mutators. The checkpoint materialize path queries this before aborting; an
//! abort that migrated chains is escalated to `CheckpointFailure::PostMutation`
//! (poison + reopen) instead of `Recoverable`, so the dropped committed chains
//! are rebuilt from the journal rather than silently lost.
//!
//! At the pure storage layer this test exercises (no engine, so no poison to
//! observe), the chosen semantics are verified by asserting the batch SURFACES
//! the migration: `migrated_chains()` is true after the splitting insert. That
//! is the exact signal the checkpoint caller escalates on — silent loss is no
//! longer possible because the loss is now detectable at the abort boundary.
//! The plain `abort` carries a `debug_assert!(!migrated_chains)` documenting
//! the DDL invariant (DDL never migrates), so the test frees the
//! batch-allocated pages through `abort_after_chain_migration`, the entry the
//! escalating caller uses.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::btree::{BTree, BTreePageStore};
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool, LatchMode};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::structural_page_batch::StructuralPageBatch;
use crate::storage::test_support::{ArcIo, MockIo};

fn make_handle() -> Arc<BufferPoolHandle> {
    let io = MockIo::new();
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    Arc::new(BufferPoolHandle::new(pool, history_pool, FileHeader::new_now()))
}

fn committed_chain(marker: u8) -> Arc<VecDeque<VersionEntry>> {
    Arc::new(VecDeque::from([VersionEntry {
        start_ts: Ts {
            physical_ms: 10,
            logical: 0,
        },
        stop_ts: Ts::MAX,
        txn_id: 1,
        state: VersionState::Committed,
        data: VersionData::Inline(vec![marker]),
        is_tombstone: false,
    }]))
}

/// Read the committed chain marker for `key` on whichever leaf the tree at
/// `(root_page, root_level)` routes it to. Returns `None` if no chain.
fn chain_marker_for_key(
    handle: &Arc<BufferPoolHandle>,
    root_page: u32,
    root_level: u8,
    key: &[u8],
) -> Option<u8> {
    let mut store = BufferPoolPageStore::new(Arc::clone(handle));
    let tree = BTree::open(BufferPoolPageStore::new(Arc::clone(handle)), root_page, root_level);
    let leaf = tree.find_leaf(key).expect("find leaf");
    store
        .with_chain_under_latch(leaf, key, LatchMode::Exclusive, |slot| {
            slot.as_ref().and_then(|chain| match &chain.front()?.data {
                VersionData::Inline(bytes) => bytes.first().copied(),
                _ => None,
            })
        })
        .expect("read chain")
}

#[test]
fn structural_split_then_abort_surfaces_migrated_committed_chains() {
    let handle = make_handle();

    // Build a single-leaf base tree with several keys, sized so one more
    // insert splits the leaf. Large values push the leaf toward its byte
    // budget so the split is forced and the upper keys move to a NEW page.
    let big = vec![b'x'; 8_000];
    let (root_page, root_level) = {
        let mut tree = BTree::create(BufferPoolPageStore::new(Arc::clone(&handle))).unwrap();
        for k in [b"a", b"b", b"c", b"d"] {
            tree.insert(k.as_slice(), &big).unwrap();
        }
        (tree.root_page, tree.root_level)
    };
    assert_eq!(root_level, 0, "base must be a single leaf before the split");

    // Install a committed chain on the highest key `d` — the split moves `d`
    // (and its chain) to the new right page.
    {
        let mut store = BufferPoolPageStore::new(Arc::clone(&handle));
        let tree = BTree::open(BufferPoolPageStore::new(Arc::clone(&handle)), root_page, root_level);
        let leaf = tree.find_leaf(b"d").expect("find leaf for d");
        store
            .with_chain_under_latch(leaf, b"d", LatchMode::Exclusive, |slot| {
                *slot = Some(committed_chain(0xDD));
            })
            .expect("install committed chain on d");
    }
    assert_eq!(
        chain_marker_for_key(&handle, root_page, root_level, b"d"),
        Some(0xDD),
        "precondition: committed chain for `d` is installed"
    );

    // Perform a leaf-splitting insert THROUGH a structural batch.
    let mut batch = StructuralPageBatch::new(&handle);
    assert!(
        !batch.migrated_chains(),
        "precondition: no chain migration before the splitting insert"
    );
    {
        let store = batch.store(BufferPoolPageStore::new(Arc::clone(&handle)));
        let mut tree = BTree::open(store, root_page, root_level);
        // `e` with a large value overflows the leaf -> split_leaf ->
        // partition_chains_for_split migrates `d`'s chain to the new right
        // page. The chain move passes THROUGH the batch store to the shared
        // frames, which is exactly what makes the abort below unsafe.
        tree.insert(b"e", &big).expect("splitting insert");
    }

    // The chosen fix (option b): the batch SURFACES that a chain migration
    // passed through it. This is the signal the checkpoint materialize path
    // escalates on — an abort after a migration is classified PostMutation
    // (poison + reopen) instead of Recoverable, so the committed chains are
    // rebuilt from the journal rather than SILENTLY lost. If this flag were
    // not set, the checkpoint path would mis-classify the abort as Recoverable
    // and the live engine would keep running with the committed versions
    // dropped from memory — the silent data loss this test guards against.
    assert!(
        batch.migrated_chains(),
        "BUG: the structural batch did not record the chain migration that the \
         split performed through its store, so a checkpoint materialize abort \
         would mis-classify as Recoverable and silently drop the committed \
         chain for `d` (migrated onto a freed/invalidated page) while the \
         durable base still routes `d` to the original leaf."
    );

    // Free the batch-allocated pages through the migration-aware abort entry
    // — the same one the escalating checkpoint caller uses. Plain `abort`
    // carries a `debug_assert!(!migrated_chains)` documenting the DDL
    // invariant (DDL paths never migrate), so a migrating caller must route
    // here after escalating to poison.
    batch
        .abort_after_chain_migration(&handle)
        .expect("abort structural batch after migration");
}
