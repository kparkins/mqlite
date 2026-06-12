//! BUG-3 regression probes: `commit_spill_txn_durable` durability contract.
//!
//! `HistoryStore::<BufferPoolPageStore>::commit_spill_txn_durable` documents:
//! "The subsequent handle flush writes the history pool and updated header
//! before the caller proceeds with main-leaf installation." Reconciliation
//! (`src/storage/reconcile/driver.rs`, `stage_history_spills` →
//! `commit_staged_history_spills`) relies on that ordering: it installs the
//! folded main leaves immediately after the call returns, so the aged
//! versions must already be on disk (history-before-leaf WAL ordering).
//!
//! On a journal-attached handle, `BufferPoolHandle::flush` is LSN-fenced:
//! the history pool is flushed at `handle.rs` pass 1 while the spill's frames
//! are still `Unflushable` (they were marked so on dirty unpin,
//! `partition.rs::unpin_page`) and `stamp_unflushable_dirty_pages_lsn` runs
//! only *after* that pass. BUG-3 was that pass 2 then flushed the **main**
//! pool only: the header (page 0, main pool) became durable pointing at
//! history pages that were never written. The fixed sequence re-flushes the
//! history pool after the stamp, before the final main-pool pass.
//!
//! These tests exercise exactly the production shape: journal-attached
//! handle + history-routed `BufferPoolPageStore`, one staged primary spill,
//! one `commit_spill_txn_durable` call.

use std::fs::OpenOptions;
use std::sync::Mutex;

use super::*;

use crate::journal::JournalManager;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};
use crate::storage::test_support::{ArcIo, MockIo};

struct Fixture {
    _dir: tempfile::TempDir,
    io: Arc<MockIo>,
    handle: Arc<BufferPoolHandle>,
}

/// Journal-attached handle over a shared `MockIo` backing store, mirroring
/// the fixture in `paged_engine/tests/checkpoint_flush_set.rs`. A journal
/// must be attached: journal-less test handles take the unfenced
/// `flush_journal_less_test_handle` path, which hides the LSN-fence ordering
/// under test.
fn journal_attached_fixture() -> Result<Fixture> {
    let dir = tempfile::tempdir().map_err(Error::Io)?;
    let db_path = dir.path().join("bug3-history-spill.mqlite");
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
    let io = MockIo::new();
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
    Ok(Fixture {
        _dir: dir,
        io,
        handle,
    })
}

/// Build a history-routed store on `handle`, stage one non-empty primary
/// spill, and run it through `commit_spill_txn_durable`.
fn commit_one_spill_durably(handle: &Arc<BufferPoolHandle>) -> Result<()> {
    let store = BufferPoolPageStore::new_history(Arc::clone(handle));
    let (mut history, _root_page) = HistoryStore::create_empty_root(store)?;

    let ident = TreeIdent {
        collection_id: 7,
        kind: TreeKind::Primary,
    };
    let entry = VersionEntry {
        start_ts: Ts {
            physical_ms: 10,
            logical: 0,
        },
        stop_ts: Ts {
            physical_ms: 20,
            logical: 0,
        },
        txn_id: 42,
        state: VersionState::Committed,
        data: VersionData::Inline(b"aged-version".to_vec()),
        is_tombstone: false,
    };
    let mut txn = HistorySpillTxn::new();
    HistoryStore::<BufferPoolPageStore>::spill_primary(&mut txn, ident, b"doc-1", &entry, 0)?;
    history.commit_spill_txn_durable(txn)
}

/// Read back the header that `commit_spill_txn_durable`'s flush persisted to
/// the backing store (page 0).
fn persisted_header(io: &Arc<MockIo>) -> Result<FileHeader> {
    let pages = io
        .pages
        .lock()
        .map_err(|_| Error::Internal("mock io pages mutex poisoned".into()))?;
    let page = pages.get(&0).ok_or_else(|| {
        Error::Internal("header page 0 was not written by commit_spill_txn_durable".into())
    })?;
    let mut buf = [0u8; HEADER_PAGE_SIZE];
    buf.copy_from_slice(&page[..HEADER_PAGE_SIZE]);
    FileHeader::from_bytes(&buf)
}

/// BUG-3 (a): after `commit_spill_txn_durable` returns, every history-pool
/// page it dirtied must have been written out — the caller installs the
/// folded leaf next, on the strength of this durability claim.
#[test]
fn commit_spill_txn_durable_flushes_history_pool_pages() -> Result<()> {
    let fixture = journal_attached_fixture()?;
    commit_one_spill_durably(&fixture.handle)?;

    let dirty = fixture.handle.history_pool().dirty_page_ids()?;
    assert!(
        dirty.is_empty(),
        "commit_spill_txn_durable promises 'the subsequent handle flush writes the \
         history pool and updated header', but these history pages are still \
         dirty-resident (never written): {dirty:?}"
    );
    Ok(())
}

/// BUG-3 (b): the same call makes the file header durable with the new
/// history root, so the root page it points at must also be on disk. A crash
/// after this call leaves a durable header referencing a never-written
/// history tree, and the folded main leaf can reach disk before its aged
/// versions (history-before-leaf ordering violated).
#[test]
fn commit_spill_txn_durable_writes_history_root_before_durable_header() -> Result<()> {
    let fixture = journal_attached_fixture()?;
    commit_one_spill_durably(&fixture.handle)?;

    let header = persisted_header(&fixture.io)?;
    let root_page = header.history_store_root_page;
    assert_ne!(
        root_page, 0,
        "durable header must carry the committed history root"
    );

    let root_written = fixture
        .io
        .pages
        .lock()
        .map_err(|_| Error::Internal("mock io pages mutex poisoned".into()))?
        .contains_key(&root_page);
    assert!(
        root_written,
        "durable header points at history root page {root_page}, but that page was \
         never written to the backing store — a crash here leaves the header \
         referencing a nonexistent history tree"
    );
    Ok(())
}
