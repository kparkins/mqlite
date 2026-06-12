//! BUG-7 repro: checkpoint materialization used to fold only the newest
//! checkpoint-visible value into the base page and clear ALL resident
//! chains (snapshot_ops.rs `checkpoint_after_reconcile_plan`), skipping the
//! reconcile path — the only path that spills superseded versions into the
//! history store — for materialized trees. Checkpoint admission drains
//! writers only; ReadViews stay open across checkpoint. Any held ReadView
//! older than the newest committed version on a materialized page therefore
//! silently lost the version it needs. Fixed: the checkpoint plan demotes
//! such pages to the reconcile spill path before materialization.

use std::fs::OpenOptions;
use std::sync::{Arc, Mutex};

use bson::{doc, Bson};

use super::*;
use crate::error::Result;
use crate::journal::JournalManager;
use crate::keys::encode_key;
use crate::storage::btree::BTree;
use crate::storage::btree_store::BufferPoolPageStore;
use crate::storage::buffer_pool::{default_sizes, BufferPool, PageSize, PageSource};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::reconcile::driver::spill_flush_observations;
use crate::storage::test_support::{ArcIo, MockIo};

const NS: &str = "test.bug7";

/// Which buffer pool issued a recorded backing-store page write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PoolTag {
    Main,
    History,
}

type WriteLog = Arc<Mutex<Vec<(PoolTag, u32)>>>;

/// `PageSource` wrapper that records the order of backing-store page writes
/// per pool, then delegates to the shared `MockIo`.
struct RecordingIo {
    inner: ArcIo,
    tag: PoolTag,
    log: WriteLog,
}

impl PageSource for RecordingIo {
    fn read_page(&self, page: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
        self.inner.read_page(page, size, buf)
    }

    fn write_page(&self, page: u32, size: PageSize, buf: &[u8]) -> Result<()> {
        self.log
            .lock()
            .map_err(|_| Error::Internal("write log mutex poisoned".into()))?
            .push((self.tag, page));
        self.inner.write_page(page, size, buf)
    }
}

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

/// Journal-attached engine over a shared `MockIo` page store, mirroring the
/// fixture shape in `storage/tests/history_store_spill_durability.rs`. A
/// journal must be attached so checkpoint's BUG-7 demotion path commits its
/// history spills through the production LSN-fenced
/// `commit_spill_txn_durable` -> `BufferPoolHandle::flush` sequence instead
/// of the unfenced journal-less test flush.
fn journaled_engine() -> Result<(tempfile::TempDir, PagedEngine)> {
    let dir = tempfile::tempdir().map_err(Error::Io)?;
    let db_path = dir.path().join("bug7-journal.mqlite");
    let mut main_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&db_path)
        .map_err(Error::Io)?;
    let header = FileHeader::new_now();
    let journal = Arc::new(Mutex::new(JournalManager::open_or_create(
        &db_path,
        &header,
        &mut main_file,
    )?));
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::HISTORY,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let handle = Arc::new(BufferPoolHandle::with_journal(
        pool,
        history_pool,
        header,
        journal,
        Arc::new(Mutex::new(main_file)),
    ));
    let engine = PagedEngine::new_buffered(handle, 0, 0)?;
    Ok((dir, engine))
}

/// Journal-attached engine whose backing-store writes are order-recorded
/// per pool (N2a). Same shape as [`journaled_engine`] with `RecordingIo`
/// wrapped around the shared `MockIo` for both pools.
fn journaled_recording_engine() -> Result<(tempfile::TempDir, PagedEngine, WriteLog)> {
    let dir = tempfile::tempdir().map_err(Error::Io)?;
    let db_path = dir.path().join("bug7-journal-recording.mqlite");
    let mut main_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&db_path)
        .map_err(Error::Io)?;
    let header = FileHeader::new_now();
    let journal = Arc::new(Mutex::new(JournalManager::open_or_create(
        &db_path,
        &header,
        &mut main_file,
    )?));
    let io = Arc::new(MockIo::default());
    let log: WriteLog = Arc::new(Mutex::new(Vec::new()));
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(RecordingIo {
            inner: ArcIo(Arc::clone(&io)),
            tag: PoolTag::Main,
            log: Arc::clone(&log),
        }),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::HISTORY,
        Box::new(RecordingIo {
            inner: ArcIo(io),
            tag: PoolTag::History,
            log: Arc::clone(&log),
        }),
    ));
    let handle = Arc::new(BufferPoolHandle::with_journal(
        pool,
        history_pool,
        header,
        journal,
        Arc::new(Mutex::new(main_file)),
    ));
    let engine = PagedEngine::new_buffered(handle, 0, 0)?;
    Ok((dir, engine, log))
}

/// Shared BUG-7 scenario: hold a registered snapshot ReadView at ts R,
/// commit a newer version, checkpoint, and assert the held view still sees
/// its version (from the chain or via a history-store spill).
fn assert_held_view_survives_checkpoint(engine: &PagedEngine) -> Result<()> {
    engine.create_namespace(NS)?;
    engine.insert(NS, doc! { "_id": 1, "value": "v1" })?;

    // Open and HOLD a registered snapshot ReadView at ts R (sees v1) —
    // the same view production snapshot reads use.
    let epoch = engine.shared.load_published();
    let ns_snap = epoch
        .catalog
        .get_by_name(NS)
        .expect("namespace snapshot in held epoch");
    let view =
        super::snapshot_ops::open_snapshot_read_view_for_epoch(&engine.shared, Arc::clone(&epoch))?;
    let tree = BTree::open(
        BufferPoolPageStore::new(Arc::clone(&engine.shared.handle)),
        ns_snap.data_root_page,
        ns_snap.data_root_level,
    );
    let probe = super::snapshot_ops::primary_history_probe(&engine.shared, ns_snap.id);
    let key = encode_key(&Bson::Int32(1));

    // Commit a newer version v2 at ts > R.
    engine.update(
        NS,
        &doc! { "_id": 1 },
        &doc! { "$set": { "value": "v2" } },
        &UpdateOptions::default(),
        false,
    )?;

    // Sanity: before checkpoint, MVCC chains give the held view v1.
    let before =
        super::snapshot_ops::fetch_primary_pair(&tree, key.clone(), &doc! {}, &view, Some(&probe))?
            .expect("held view must see the row before checkpoint");
    assert_eq!(
        before.1.get_str("value"),
        Ok("v1"),
        "held view must see v1 before checkpoint"
    );

    engine.checkpoint()?;

    // Snapshot isolation: the held view must STILL see v1 (from the chain
    // or via a history-store spill).
    let after = super::snapshot_ops::fetch_primary_pair(&tree, key, &doc! {}, &view, Some(&probe))?;
    let value = after
        .as_ref()
        .map(|(_, doc)| doc.get_str("value").unwrap_or("<non-string>"));
    assert_eq!(
        value,
        Some("v1"),
        "ReadView held across checkpoint lost its version: expected v1, got {value:?}"
    );
    Ok(())
}

#[test]
fn held_read_view_keeps_its_snapshot_across_checkpoint() -> Result<()> {
    let engine = buffered_engine();
    assert_held_view_survives_checkpoint(&engine)
}

/// R-bug7-journal: same scenario on a journal-attached handle, so the BUG-7
/// demotion (spill-required reconcile before materialization) exercises the
/// production LSN-fenced history-spill flush path.
#[test]
fn held_read_view_keeps_its_snapshot_across_checkpoint_with_journal() -> Result<()> {
    let (_dir, engine) = journaled_engine()?;
    assert_held_view_survives_checkpoint(&engine)
}

/// N2a: history-before-leaf must hold at the BACKING STORE, not just in
/// memory. Record the per-pool write order across the checkpoint and assert
/// every history-spill page write precedes the folded main-leaf write.
#[test]
fn history_spill_page_writes_precede_folded_leaf_write_with_journal() -> Result<()> {
    let (_dir, engine, log) = journaled_recording_engine()?;
    engine.create_namespace(NS)?;
    engine.insert(NS, doc! { "_id": 1, "value": "v1" })?;

    let epoch = engine.shared.load_published();
    let ns_snap = epoch
        .catalog
        .get_by_name(NS)
        .expect("namespace snapshot in held epoch");
    assert_eq!(ns_snap.data_root_level, 0, "single-leaf tree expected");
    let leaf = ns_snap.data_root_page;
    // Hold a registered view at ts R so checkpoint must spill v1.
    let view =
        super::snapshot_ops::open_snapshot_read_view_for_epoch(&engine.shared, Arc::clone(&epoch))?;

    engine.update(
        NS,
        &doc! { "_id": 1 },
        &doc! { "$set": { "value": "v2" } },
        &UpdateOptions::default(),
        false,
    )?;

    log.lock()
        .map_err(|_| Error::Internal("write log mutex poisoned".into()))?
        .clear();
    engine.checkpoint()?;
    drop(view);

    let events = log
        .lock()
        .map_err(|_| Error::Internal("write log mutex poisoned".into()))?
        .clone();
    // The folded image only exists after the install, which runs strictly
    // after the durable spill commit returns; the final checkpoint flush
    // then writes it. So the LAST main-leaf write in the window is the
    // folded image (earlier main-leaf writes are the journal-covered
    // pre-fold image flushed by the spill commit's own two-pool flush).
    let last_leaf_write = events
        .iter()
        .rposition(|(tag, page)| *tag == PoolTag::Main && *page == leaf)
        .expect("checkpoint must write the folded main leaf to the backing store");
    let last_history_write = events
        .iter()
        .rposition(|(tag, _)| *tag == PoolTag::History)
        .expect("BUG-7 demotion must spill history pages during this checkpoint");
    assert!(
        last_history_write < last_leaf_write,
        "folded main-leaf write at position {last_leaf_write} (events: {events:?}) did not \
         land after the last history-spill page write at position {last_history_write}: \
         history-before-leaf ordering violated on the backing store"
    );
    Ok(())
}

/// F1 regression pin: checkpoint spill batching is cross-tree and chunked.
/// N spill-required pages across >= 2 trees must commit through
/// `ceil(N / RECONCILE_CHUNK_PAGES)` durable spill flushes — here 2 pages
/// across 2 namespaces fit one chunk, so exactly ONE spill-path flush.
/// Pre-fix the R7 batching was per tree: one flush per namespace.
#[test]
fn checkpoint_spill_flushes_are_chunked_across_trees() -> Result<()> {
    let engine = buffered_engine();
    let ns_a = "test.bug7flusha";
    let ns_b = "test.bug7flushb";
    engine.create_namespace(ns_a)?;
    engine.create_namespace(ns_b)?;
    engine.insert(ns_a, doc! { "_id": 1, "value": "a1" })?;
    engine.insert(ns_b, doc! { "_id": 1, "value": "b1" })?;

    // One held view gates the global oldest-required ts: both namespaces'
    // superseded v1 versions must spill at checkpoint.
    let epoch = engine.shared.load_published();
    let view =
        super::snapshot_ops::open_snapshot_read_view_for_epoch(&engine.shared, Arc::clone(&epoch))?;

    for ns in [ns_a, ns_b] {
        engine.update(
            ns,
            &doc! { "_id": 1 },
            &doc! { "$set": { "value": "v2" } },
            &UpdateOptions::default(),
            false,
        )?;
    }

    spill_flush_observations::reset_spill_commit_flushes();
    engine.checkpoint()?;
    drop(view);

    assert_eq!(
        spill_flush_observations::spill_commit_flushes(),
        1,
        "2 spill-required pages across 2 trees fit one chunk and must take \
         exactly one durable spill flush (pre-fix: one flush per namespace)"
    );
    Ok(())
}
