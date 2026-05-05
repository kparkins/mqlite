use std::collections::BTreeSet;
use std::fs::OpenOptions;
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::journal::log_file::PageId;
use crate::journal::{CheckpointFlushSet, JournalLayeredSource, JournalManager};
use crate::mvcc::Ts;
use crate::storage::buffer_pool::{default_sizes, BufferPool, PageSize, PageSource};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

struct JournalFixture {
    _dir: tempfile::TempDir,
    handle: Arc<BufferPoolHandle>,
    journal: Arc<Mutex<JournalManager>>,
}

fn journal_fixture() -> Result<JournalFixture> {
    let dir = tempfile::tempdir().map_err(Error::Io)?;
    let db_path = dir.path().join("phase7-us005.mqlite");
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
    let backing_io = MockIo::new();
    let backing: Arc<dyn PageSource> = Arc::new(ArcIo(Arc::clone(&backing_io)));
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(JournalLayeredSource::new(
            Arc::clone(&backing),
            Arc::clone(&journal),
        )),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::HISTORY,
        Box::new(JournalLayeredSource::new(backing, Arc::clone(&journal))),
    ));
    let handle = Arc::new(BufferPoolHandle::with_journal(
        pool,
        history_pool,
        header,
        Arc::clone(&journal),
        Arc::new(Mutex::new(main_file)),
    ));
    Ok(JournalFixture {
        _dir: dir,
        handle,
        journal,
    })
}

fn dirty_page(handle: &BufferPoolHandle, page: u32, size: PageSize, fill: u8) -> Result<()> {
    let mut pinned = handle.fetch_page(page, size)?;
    pinned.data_mut().fill(fill);
    Ok(())
}

fn pages<const N: usize>(ids: [u32; N]) -> BTreeSet<PageId> {
    ids.into_iter().map(PageId).collect()
}

fn assert_internal_contains(err: Error, needle: &str) {
    assert!(
        matches!(err, Error::Internal(ref message) if message.contains(needle)),
        "expected Error::Internal containing {needle:?}, got {err:?}"
    );
}

#[test]
fn test_checkpoint_flush_set_rejects_foreign_dirty_frame_before_append() -> Result<()> {
    let fixture = journal_fixture()?;
    dirty_page(&fixture.handle, 10, PageSize::Large32k, 0x10)?;
    dirty_page(&fixture.handle, 11, PageSize::Large32k, 0x11)?;
    let write_cursor_before = fixture
        .journal
        .lock()
        .expect("journal mutex")
        .write_cursor();

    let batch_id = fixture.handle.next_checkpoint_batch_id()?;
    let flush_set =
        CheckpointFlushSet::new(batch_id, pages([10]), BTreeSet::new(), BTreeSet::new())?;
    let err = fixture
        .handle
        .flush_journal_durable(flush_set)
        .expect_err("foreign dirty frame is rejected before checkpoint append");

    assert_internal_contains(err, "foreign dirty frame 11");
    assert_eq!(
        fixture
            .journal
            .lock()
            .expect("journal mutex")
            .write_cursor(),
        write_cursor_before,
        "pre-append validation failure must not mutate the journal"
    );
    Ok(())
}

#[test]
fn test_checkpoint_flush_set_tags_only_checkpoint_batch_frames() -> Result<()> {
    let fixture = journal_fixture()?;
    dirty_page(&fixture.handle, 10, PageSize::Large32k, 0xA5)?;
    dirty_page(&fixture.handle, 11, PageSize::Large32k, 0xEF)?;

    let batch_id = fixture.handle.next_checkpoint_batch_id()?;
    let flush_set = CheckpointFlushSet::new(batch_id, pages([10]), BTreeSet::new(), pages([11]))?;
    let cursor = fixture.handle.flush_journal_durable(flush_set)?;
    assert_eq!(cursor.batch_id(), batch_id);

    let mut journal = fixture.journal.lock().expect("journal mutex");
    let pending_start = cursor.expected_pending_start();
    let mut staged_header = FileHeader::new_now();
    staged_header.last_checkpoint_ts = Ts {
        physical_ms: 1,
        logical: 0,
    };
    let boundary = journal.append_checkpoint_commit_boundary(&staged_header, cursor)?;
    assert!(
        boundary.journal_offset() >= pending_start,
        "boundary is appended after the checkpoint-owned pending range"
    );
    let checkpointed = journal
        .read_page_linear(10)?
        .expect("checkpoint-owned page was flushed to journal");
    assert_eq!(checkpointed[0], 0xA5);
    assert!(
        journal.read_page_linear(11)?.is_none(),
        "future-dirty excluded page must not be tagged or flushed"
    );
    Ok(())
}
