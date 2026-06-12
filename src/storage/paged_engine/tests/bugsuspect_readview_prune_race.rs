//! ITEM 1 — ReadView registration race (CRUD-reconcile / prune dimension).
//!
//! `open_snapshot_read_view` opens a reader's `ReadView` over the published
//! epoch. The CRUD-reconcile / eviction prune
//! (`partition::reconcile_frame_at`, and the test-only `BufferPool::reconcile`
//! mirror used here) drops resident superseded committed versions whose
//! `stop_ts <= ReadViewRegistry::oldest_required_ts()` WITHOUT spilling them to
//! the history store. That is safe only when the floor is a true lower bound on
//! every live reader's `read_ts`.
//!
//! The danger is the load-to-register window: if a reader loads the published
//! epoch BEFORE it registers, a prune in that gap sees an empty registry
//! (floor `Ts::MAX`) and drops a version the reader still needs.  F36's
//! `catalog_generation` recheck does NOT cover this — an ordinary update/insert
//! does not bump `catalog_generation`.
//!
//! The fix (ITEM 1, option a) closes the window by ORDER, not by a recheck:
//! `open_snapshot_read_view` takes the conservative registry pin
//! (`register(txn_id, Ts::default())`) FIRST, BEFORE it loads the published
//! epoch.  From the pin onward the reader is a member of every
//! `oldest_required_ts()` snapshot at the lowest possible floor, so no prune
//! can run between the pin and the load (or after) that drops a version visible
//! at any `ts >= Ts::default()` — every version this reader could need.  Once
//! the view is built the pinned slot is refined up to the real `read_ts` so the
//! reader does not over-pin the horizon for its lifetime.
//!
//! These tests pin the contract:
//!
//! - `pin_precedes_load_protects_version_against_concurrent_prune` rendezvouses
//!   a reader in the window BETWEEN the pin and the epoch load, runs a writer +
//!   reconcile-prune during the pause, resumes the reader, and asserts the
//!   reader still sees its version.  This FAILS if the pin is reordered after
//!   the load (the window reopens and the prune drops the version) — the
//!   failing evidence the fix is built against.
//! - `registered_reader_visible_version_survives_later_prune` and
//!   `conservative_register_refines_and_releases_horizon` pin the steady-state
//!   guarantee and the no-over-pin property of the refine.

use std::sync::Arc;

use bson::{doc, Bson};

use super::*;
use crate::error::Result;
use crate::keys::encode_key;
use crate::mvcc::{Ts, VersionData, VersionEntry, VersionState};
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool, LatchMode};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "test.item1.pruneRace";

fn buffered_engine() -> PagedEngine {
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
    PagedEngine::new_buffered(handle, 0, 0).expect("create buffered engine")
}

fn primary_leaf_for_id(engine: &PagedEngine, id: &Bson) -> Result<(Vec<u8>, u32)> {
    let key = encode_key(id);
    let epoch = engine.shared.load_published();
    let ns_snap = epoch.catalog.get_by_name(NS).expect("namespace snapshot");
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&engine.shared.handle)),
        ns_snap.data_root_page,
        ns_snap.data_root_level,
    );
    let leaf = tree.find_leaf(&key)?;
    Ok((key, leaf))
}

/// Install a newer committed head above the live head for `key`, capping the
/// prior head's `stop_ts` — simulating a concurrent writer superseding the
/// reader's visible version, so the old version becomes a superseded
/// RESIDENT delta (the prune target). Done by direct chain manipulation so it
/// does NOT advance the published epoch — the reader that opens afterward
/// still loads the v1 epoch and so still needs v1.
fn supersede_head(engine: &PagedEngine, leaf: u32, key: &[u8], new_start: Ts) -> Result<()> {
    engine.shared.handle.pool().with_chain_under_latch(
        leaf,
        key,
        LatchMode::Exclusive,
        |slot| {
            let mut chain = slot.take().unwrap_or_default();
            let chain_mut = Arc::make_mut(&mut chain);
            if let Some(head) = chain_mut.front_mut() {
                head.stop_ts = new_start;
            }
            chain_mut.push_front(VersionEntry {
                start_ts: new_start,
                stop_ts: Ts::MAX,
                txn_id: 99,
                state: VersionState::Committed,
                data: VersionData::Inline(b"v2".to_vec()),
                is_tombstone: false,
            });
            *slot = Some(chain);
        },
    )
}

/// THE core property (ITEM 1): the conservative pin precedes the epoch load.
///
/// A reader is paused in the window BETWEEN the conservative registry pin and
/// the published-epoch load (via the cfg-gated
/// `read_view_pin_before_epoch_load` rendezvous hook). While it is paused a
/// concurrent writer supersedes its version (v1 -> finite stop_ts, v2 head) and
/// a reconcile fires. Because the reader ALREADY pinned `Ts::default()` before
/// pausing, the reconcile floor is `Ts::default()` and the prune must retain
/// v1. The reader then resumes, loads the (still v1) epoch, and must see v1.
///
/// This is the property the bug report demanded: it FAILS if the pin is
/// reordered after the load (the window reopens, the empty-registry prune
/// drops v1, and the resumed reader sees v2/empty). The reorder experiment is
/// recorded in the module docs and the PR notes.
#[test]
fn pin_precedes_load_protects_version_against_concurrent_prune() -> Result<()> {
    let engine = buffered_engine();
    engine.create_namespace(NS)?;
    engine.insert(NS, doc! { "_id": 1, "value": "v1" })?;

    let epoch = engine.shared.load_published();
    let read_ts = epoch.visible_ts;
    let (key, leaf) = primary_leaf_for_id(&engine, &Bson::Int32(1))?;
    // v2 supersedes v1 at a ts strictly above read_ts, so v1.stop_ts > read_ts
    // and v1 is the version the reader at read_ts still needs.
    let v2_start = Ts {
        physical_ms: read_ts.physical_ms + 1_000,
        logical: 0,
    };

    let mut hook =
        super::hidden_accessors::install_read_view_pin_before_epoch_load_hook(&engine.shared);

    let view = std::thread::scope(|s| -> Result<Arc<crate::mvcc::read_view::ReadView>> {
        // Reader thread: pins conservatively, then blocks at the hook BEFORE
        // loading the epoch.
        let reader = s.spawn(|| super::snapshot_ops::open_snapshot_read_view(&engine.shared));

        // Wait until the reader has taken its conservative pin and is paused
        // before the load.
        hook.wait_until_entered()
            .expect("reader must reach the pin-before-load pause point");

        // The conservative pin is already in the registry: the reclaim floor
        // is Ts::default(), the lowest possible — NOT the empty-registry
        // Ts::MAX.
        assert_eq!(
            engine.shared.handle.read_view_registry().oldest_required_ts(),
            Ts::default(),
            "the conservative pin must precede the epoch load: while the reader \
             is paused before its load, the floor must already be Ts::default()"
        );

        // Concurrent writer supersedes v1, then a reconcile prunes. Because the
        // floor is Ts::default(), v1 (stop_ts == v2_start > Ts::default()) is
        // RETAINED.
        supersede_head(&engine, leaf, &key, v2_start)?;
        let dropped = engine.shared.handle.pool().reconcile(
            leaf,
            engine.shared.handle.read_view_registry(),
            engine.shared.handle.allocator(),
        )?;
        assert_eq!(
            dropped, 0,
            "with the conservative pin held BEFORE the load, the prune must \
             retain v1 (stop_ts > Ts::default())"
        );

        // Resume the reader; it loads the epoch, refines its slot, returns.
        hook.release().expect("release paused reader");
        reader.join().expect("reader thread panicked")
    })?;

    // The reader's snapshot is the v1 epoch, and v1 survived the prune.
    assert_eq!(view.read_ts, read_ts);
    let still_resident = engine.shared.handle.pool().with_chain_under_latch(
        leaf,
        &key,
        LatchMode::Exclusive,
        |slot| {
            slot.as_ref().is_some_and(|chain| {
                chain
                    .iter()
                    .any(|e| e.start_ts <= read_ts && read_ts < e.stop_ts)
            })
        },
    )?;
    assert!(
        still_resident,
        "the version visible at the reader's read_ts must remain resident: the \
         conservative pin taken before the epoch load protected it from the \
         concurrent prune"
    );
    drop(view);
    Ok(())
}

/// A reader registered via `open_snapshot_read_view` pins its `read_ts`, so a
/// reconcile that runs AFTER it registered must NOT drop the version visible
/// at that `read_ts` — even after a concurrent writer supersedes it. This is
/// the steady-state guarantee the conservative-register fix preserves: once
/// registered, the reader is a true member of `oldest_required_ts()`.
#[test]
fn registered_reader_visible_version_survives_later_prune() -> Result<()> {
    let engine = buffered_engine();
    engine.create_namespace(NS)?;
    engine.insert(NS, doc! { "_id": 1, "value": "v1" })?;

    let epoch = engine.shared.load_published();
    let read_ts = epoch.visible_ts;

    // Reader opens (and so registers) on the v1 epoch.
    let view = super::snapshot_ops::open_snapshot_read_view(&engine.shared)
        .expect("open_snapshot_read_view must succeed on a current epoch");
    assert_eq!(view.read_ts, read_ts);

    // The registry now pins read_ts: the floor cannot exceed it.
    assert_eq!(
        engine.shared.handle.read_view_registry().oldest_required_ts(),
        read_ts,
        "a registered reader must pin its read_ts as the reclaim floor"
    );

    // A concurrent writer supersedes _id:1 (v1 -> finite stop_ts, v2 head).
    let (key, leaf) = primary_leaf_for_id(&engine, &Bson::Int32(1))?;
    let v2_start = Ts {
        physical_ms: read_ts.physical_ms + 1_000,
        logical: 0,
    };
    supersede_head(&engine, leaf, &key, v2_start)?;

    // A reconcile now fires. Because the reader is registered at read_ts, the
    // floor is read_ts and v1 (stop_ts == v2_start > read_ts) is RETAINED.
    let dropped = engine.shared.handle.pool().reconcile(
        leaf,
        engine.shared.handle.read_view_registry(),
        engine.shared.handle.allocator(),
    )?;
    assert_eq!(
        dropped, 0,
        "the registered reader's superseded version (stop_ts > read_ts) must \
         be retained, not pruned"
    );

    // v1 is still resident and visible at read_ts.
    let still_resident = engine.shared.handle.pool().with_chain_under_latch(
        leaf,
        &key,
        LatchMode::Exclusive,
        |slot| {
            slot.as_ref().is_some_and(|chain| {
                chain
                    .iter()
                    .any(|e| e.start_ts <= read_ts && read_ts < e.stop_ts)
            })
        },
    )?;
    assert!(
        still_resident,
        "the version visible at the registered reader's read_ts must remain \
         resident after the prune"
    );
    drop(view);
    Ok(())
}

/// The reclaim horizon is not over-pinned for the reader's lifetime: the
/// conservative `Ts::default()` floor is only momentary (during
/// registration) and is refined up to the real `read_ts`, so once the view
/// drops the registry returns to the empty-horizon `Ts::MAX`.
#[test]
fn conservative_register_refines_and_releases_horizon() -> Result<()> {
    let engine = buffered_engine();
    engine.create_namespace(NS)?;
    engine.insert(NS, doc! { "_id": 1, "value": "v1" })?;

    let read_ts = engine.shared.load_published().visible_ts;

    let view =
        super::snapshot_ops::open_snapshot_read_view(&engine.shared).expect("open must succeed");
    // Refined to the real read_ts, NOT stuck at the conservative Ts::default().
    assert_eq!(
        engine.shared.handle.read_view_registry().oldest_required_ts(),
        read_ts,
        "the registered slot must be refined up from the conservative floor \
         to the reader's real read_ts"
    );
    assert_ne!(
        read_ts,
        Ts::default(),
        "precondition: a committed insert advances the visible_ts above the \
         conservative default floor, so the refine is observable"
    );

    drop(view);
    assert_eq!(
        engine.shared.handle.read_view_registry().oldest_required_ts(),
        Ts::MAX,
        "dropping the view must release the pin entirely"
    );
    Ok(())
}
