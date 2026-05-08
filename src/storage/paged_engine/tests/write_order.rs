use super::*;

use std::sync::Arc;
use std::sync::Barrier;

use bson::{doc, Bson};

use crate::error::Result;
use crate::keys::encode_key;
use crate::mvcc::{VersionEntry, VersionState};
use crate::options::FindOptions;
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::catalog::CollectionEntry;
use crate::storage::engine::StorageEngine;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

const LIVE_READER_NS: &str = "test.us012.live_reader";
const SPIN_LIMIT: usize = 10_000;

fn paged_engine_source() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/storage/paged_engine.rs");
    std::fs::read_to_string(path).expect("read paged_engine.rs")
}

fn run_write_inner_source(source: &str) -> &str {
    let start = source
        .find("fn run_write_inner")
        .expect("run_write_inner exists");
    let end = source[start..]
        .find("\n    fn register_ordinary_crud_slot")
        .expect("register_ordinary_crud_slot follows run_write_inner");
    &source[start..start + end]
}

fn assert_ordered(source: &str, markers: &[&str]) {
    let mut cursor = 0;
    for marker in markers {
        let offset = source[cursor..]
            .find(marker)
            .unwrap_or_else(|| panic!("missing marker after byte {cursor}: {marker}"));
        cursor += offset + marker.len();
    }
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
    let entry = super::catalog_ops::catalog_lock(&engine.metadata_state)
        .get_collection(ns)?
        .ok_or_else(|| Error::Internal("collection missing".into()))?;
    Ok(entry)
}

fn primary_chain_for_id(
    engine: &PagedEngine,
    coll: &CollectionEntry,
    id: &Bson,
) -> Result<Vec<VersionEntry>> {
    let key = encode_key(id);
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&engine.shared.handle)),
        coll.data_root_page,
        coll.data_root_level,
    );
    let leaf = tree.find_leaf(&key)?;
    let entries = engine
        .shared
        .handle
        .pool()
        .us009_chain_entries(leaf, &key)?;
    if entries.is_empty() {
        return Err(Error::Internal("primary delta chain missing".into()));
    }
    Ok(entries)
}

#[test]
fn test_run_write_existing_uses_authoritative_us012_sequence() {
    let source = paged_engine_source();
    let run_write_inner = run_write_inner_source(&source);

    assert_ordered(
        run_write_inner,
        &[
            "let slot = match self.register_ordinary_crud_slot()",
            "LogRecordDraft::crud(",
            "self.install_pending_sec_index_with_retry",
            "self.install_pending_primary_with_retry",
            "self.shared.handle.reserve_log_record(draft)",
            "stamp_dirty_pages_lsn(&pending_pages, commit_end_lsn)",
            "reserved.write_and_mark()",
            "self.wait_for_commit_durability(commit_end_lsn)",
            "flip_pending_to_committed_for",
            ".mark_ready(slot",
        ],
    );

    assert!(
        source.contains("publish_sequencer.register_with_oracle(&self.shared.oracle)"),
        "US-012 ordinary CRUD registration must reserve a publish slot through the oracle"
    );
}

#[test]
fn test_run_write_existing_has_no_ordinary_crud_legacy_authority() {
    let source = paged_engine_source();
    let run_write_inner = run_write_inner_source(&source);
    let retired_field = concat!("commit", "_seq");

    assert!(
        !source.contains("lane_for"),
        "US-012 removes lane_for from src/storage/paged_engine.rs"
    );
    assert!(
        !source.contains("acquire_lane"),
        "US-012 removes acquire_lane from src/storage/paged_engine.rs"
    );
    assert!(
        !source.contains(&format!("{retired_field}: Mutex"))
            && !source.contains(&format!("{retired_field}:")),
        "US-012 keeps the deleted commit-sequence field/construction absent"
    );
    for forbidden in [
        "StructuralPageWrites::new()",
        "new_txn_store",
        concat!("sync_catalog_root_", "overlay"),
        "commit_legacy_header_frame",
        "commit_structural_only",
        ".commit(&mut base_store",
        ".commit_txn(",
        "ns_writers.admit",
    ] {
        assert!(
            !run_write_inner.contains(forbidden),
            "US-003 ordinary CRUD must not contain legacy authority marker: {forbidden}"
        );
    }
}

#[test]
fn test_mark_ready_closure_is_publish_only() {
    let source = paged_engine_source();
    let mark_ready = source
        .find(".mark_ready(slot")
        .expect("mark_ready call exists");
    let closure = &source[mark_ready..];
    let closure_end = closure
        .find("record_crud_commit_")
        .expect("publish closure is followed by commit metrics");
    let closure = &closure[..closure_end];

    assert!(
        !closure.contains("flip_pending_to_committed_for"),
        "Pending-to-Committed flip must happen before mark_ready"
    );
    assert!(
        !closure.contains("lock_journal_mutex") && !closure.contains("journal_mutex"),
        "mark_ready closure must not enter the journal mutex"
    );
    assert!(
        !closure.contains("metadata.read"),
        "mark_ready closure must not acquire metadata.read()"
    );
    assert!(
        !closure.contains("WriteConflict"),
        "post-durable mark_ready closure must not return WriteConflict"
    );
}

#[test]
fn test_durable_logical_frame_exists_before_resident_install_live_reader() -> Result<()> {
    use super::hidden_accessors::install_publish_pause;

    let engine = Arc::new(buffered_engine()?);
    engine.create_namespace(LIVE_READER_NS)?;
    engine.insert(LIVE_READER_NS, doc! { "_id": 0i32, "seed": true })?;

    let coll = collection_entry(&engine, LIVE_READER_NS)?;
    let gate = Arc::new(Barrier::new(2));
    let _guard = install_publish_pause(&engine.shared, Arc::clone(&gate));

    let writer_engine = Arc::clone(&engine);
    let writer = std::thread::spawn(move || {
        writer_engine
            .insert(LIVE_READER_NS, doc! { "_id": 42i32, "phase": "paused" })
            .expect("writer insert");
    });

    let id = Bson::Int32(42);
    let paused_chain = (0..SPIN_LIMIT)
        .find_map(|_| {
            let observed = primary_chain_for_id(&engine, &coll, &id).ok();
            if observed.is_none() {
                std::thread::yield_now();
            }
            observed
        })
        .ok_or_else(|| Error::Internal("writer did not install a pending primary head".into()))?;

    assert_eq!(paused_chain.len(), 1);
    assert!(matches!(
        paused_chain[0].state,
        VersionState::Pending { .. }
    ));

    let (pre_publish_docs, _) =
        engine.find(LIVE_READER_NS, &doc! { "_id": 42i32 }, &FindOptions::new())?;
    assert!(
        pre_publish_docs.is_empty(),
        "pre-publish readers must not see the resident Pending head"
    );

    gate.wait();
    writer.join().expect("writer thread panicked");

    let (post_publish_docs, _) =
        engine.find(LIVE_READER_NS, &doc! { "_id": 42i32 }, &FindOptions::new())?;
    assert_eq!(post_publish_docs.len(), 1);

    let committed_chain = primary_chain_for_id(&engine, &coll, &id)?;
    assert!(matches!(committed_chain[0].state, VersionState::Committed));
    Ok(())
}
