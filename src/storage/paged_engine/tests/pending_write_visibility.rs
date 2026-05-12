use super::*;

use std::sync::Arc;
use std::sync::Barrier;

use bson::{doc, Bson, Document};

use crate::error::Result;
use crate::keys::encode_key;
use crate::mvcc::{ChainSnapshot, ReadView, VersionData, VersionEntry, VersionState};
use crate::options::FindOptions;
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::catalog::CollectionEntry;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "test.us020.pending";
const TARGET_ID: i32 = 52;
const SPIN_LIMIT: usize = 10_000;

struct PendingObservation {
    snapshot: ChainSnapshot,
    pending_entry: VersionEntry,
}

fn buffered_engine() -> Result<PagedEngine> {
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
    PagedEngine::new_buffered(handle, 0, 0)
}

fn collection_entry(engine: &PagedEngine, ns: &str) -> Result<CollectionEntry> {
    let _md = engine
        .metadata
        .read()
        .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
    let entry = engine
        .metadata_state
        .catalog_lock()
        .get_collection(ns)?
        .ok_or_else(|| Error::Internal("collection missing".into()))?;
    Ok(entry)
}

fn primary_leaf_for_id(
    engine: &PagedEngine,
    coll: &CollectionEntry,
    id: &Bson,
) -> Result<(u32, Vec<u8>)> {
    let key = encode_key(id);
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&engine.shared.handle)),
        coll.data_root_page,
        coll.data_root_level,
    );
    Ok((tree.find_leaf(&key)?, key))
}

fn primary_chain_for_key(
    engine: &PagedEngine,
    leaf: u32,
    key: &[u8],
) -> Result<Option<Vec<VersionEntry>>> {
    let entries = engine.shared.handle.pool().us009_chain_entries(leaf, key)?;
    if entries.is_empty() {
        return Ok(None);
    }
    Ok(Some(entries))
}

fn wait_for_pending_snapshot(
    engine: &PagedEngine,
    leaf: u32,
    key: &[u8],
    foreign_view: Arc<ReadView>,
) -> Result<PendingObservation> {
    for _ in 0..SPIN_LIMIT {
        if let Some(entries) = primary_chain_for_key(engine, leaf, key)? {
            if let Some(entry) = entries.first() {
                if matches!(entry.state, VersionState::Pending { .. }) {
                    let snapshot = engine
                        .shared
                        .handle
                        .pool()
                        .snapshot_chains(leaf, Some(Arc::clone(&foreign_view)))?
                        .ok_or_else(|| {
                            Error::Internal("pending leaf frame is no longer resident".into())
                        })?;
                    return Ok(PendingObservation {
                        snapshot,
                        pending_entry: entry.clone(),
                    });
                }
            }
        }
        std::thread::yield_now();
    }
    Err(Error::Internal(
        "writer did not install a pending primary head".into(),
    ))
}

fn assert_inline_doc(entry: &VersionEntry, expected_id: i32) {
    match &entry.data {
        VersionData::Inline(bytes) => {
            let doc: Document = bson::from_slice(bytes).expect("pending entry stores BSON doc");
            assert_eq!(doc.get_i32("_id").ok(), Some(expected_id));
        }
        VersionData::Overflow(_) => panic!("US-020 fixture should keep the small doc inline"),
    }
}

#[test]
fn test_writer_read_sees_own_pending_before_publish() -> Result<()> {
    use super::hidden_accessors::install_publish_pause;

    let engine = Arc::new(buffered_engine()?);
    engine.create_namespace(NS)?;
    engine.insert(NS, doc! { "_id": 0i32, "seed": true })?;

    let coll = collection_entry(&engine, NS)?;
    let target = Bson::Int32(TARGET_ID);
    let (leaf, key) = primary_leaf_for_id(&engine, &coll, &target)?;
    let pre_publish_epoch = engine.shared.load_published();

    let gate = Arc::new(Barrier::new(2));
    let _guard = install_publish_pause(&engine.shared, Arc::clone(&gate));

    let writer_engine = Arc::clone(&engine);
    let writer = std::thread::spawn(move || {
        writer_engine
            .insert(NS, doc! { "_id": TARGET_ID, "phase": "pending" })
            .expect("writer insert");
    });

    let foreign_view = Arc::new(ReadView::new_for_epoch(
        Arc::clone(&pre_publish_epoch),
        u64::MAX,
        Arc::clone(&engine.shared.publish_sequencer),
    ));
    let observed = wait_for_pending_snapshot(&engine, leaf, &key, Arc::clone(&foreign_view));
    let pre_publish_find = engine.find(NS, &doc! { "_id": TARGET_ID }, &FindOptions::default());

    if writer.is_finished() {
        writer.join().expect("writer thread panicked");
        return Err(Error::Internal(
            "writer finished before publish-pause observation".into(),
        ));
    }

    // The writer is paused at the publish_pause hook after the
    // Pending-to-Committed flip and before rebuild_and_publish_locked.
    // Sample the live `published_frontier` and run every "before publish"
    // assertion BEFORE releasing the gate; once the writer publishes,
    // §10.19 C-1 advances the live frontier and the foreign-Pending
    // visibility predicate flips (US-037).
    let observed = match observed {
        Ok(observed) => observed,
        Err(error) => {
            gate.wait();
            writer.join().expect("writer thread panicked");
            return Err(error);
        }
    };
    assert_eq!(
        observed.snapshot.chain_len(&key),
        1,
        "ChainSnapshot::new must clone the foreign Pending entry and leave \
         visibility filtering to visible_at"
    );

    let pending_txn_id = match observed.pending_entry.state {
        VersionState::Pending { txn_id } => txn_id,
        VersionState::Committed | VersionState::Aborted => {
            return Err(Error::Internal("expected pending entry".into()));
        }
    };
    let pre_publish_frontier = engine
        .shared
        .publish_sequencer
        .published_frontier
        .load(std::sync::atomic::Ordering::Acquire);
    assert!(
        observed.pending_entry.start_ts > pre_publish_frontier,
        "pre-publish frontier must not have advanced past the Pending start_ts"
    );
    assert_eq!(foreign_view.visible_ts(), pre_publish_epoch.visible_ts);

    let writer_view = ReadView::new_for_epoch(
        Arc::clone(&pre_publish_epoch),
        pending_txn_id,
        Arc::clone(&engine.shared.publish_sequencer),
    );
    let writer_visible = observed
        .snapshot
        .visible_at(&key, &writer_view)
        .expect("writer must see its own Pending entry before S12 publish");
    assert_inline_doc(writer_visible, TARGET_ID);

    assert!(
        observed.snapshot.visible_at(&key, &foreign_view).is_none(),
        "foreign reader at the same read_ts must not see Pending before S12"
    );

    let (pre_publish_docs, _) = pre_publish_find?;
    assert!(
        pre_publish_docs.is_empty(),
        "engine read path must reject the foreign Pending head before publish"
    );

    let pending_start_ts = observed.pending_entry.start_ts;

    gate.wait();
    writer.join().expect("writer thread panicked");

    let post_publish_frontier = engine
        .shared
        .publish_sequencer
        .published_frontier
        .load(std::sync::atomic::Ordering::Acquire);
    assert!(
        post_publish_frontier >= pending_start_ts,
        "S12 publish must advance the live sequencer frontier past the Pending start_ts"
    );
    let (post_publish_docs, _) =
        engine.find(NS, &doc! { "_id": TARGET_ID }, &FindOptions::default())?;
    assert_eq!(post_publish_docs.len(), 1);

    Ok(())
}
