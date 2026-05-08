use super::*;

use std::fs::OpenOptions;
use std::sync::Arc;

use crate::error::{EngineFatalReason, Error, Result};
use crate::journal::{BoundaryAppended, JournalManager};
use crate::mvcc::deferred_free::CheckpointLifetimeDrain;
use crate::mvcc::Ts;
use crate::storage::allocator::AllocatorHandle;
use crate::storage::buffer_pool::{default_sizes, BufferPool, PageSize};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::test_support::{ArcIo, MockIo};

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

fn assert_freeze_rejection(err: Error) {
    assert!(
        matches!(err, Error::Internal(ref message) if message.contains("allocator is frozen")),
        "expected allocator freeze Error::Internal, got {err:?}"
    );
}

fn assert_checkpoint_poisoned(engine: &PagedEngine) {
    let err = engine
        .shared
        .check_engine_not_poisoned()
        .expect_err("freeze violation poisons the live engine");
    assert!(matches!(
        err,
        Error::EngineFatal {
            reason: EngineFatalReason::CheckpointPostMutationFailure
        }
    ));
}

fn boundary_with_db_page_count(db_page_count: u32) -> Result<BoundaryAppended> {
    let dir = tempfile::tempdir().map_err(Error::Io)?;
    let db_path = dir.path().join("phase7-us004.mqlite");
    let mut main_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&db_path)
        .map_err(Error::Io)?;
    let header = FileHeader::new_now();
    let mut journal = JournalManager::open_or_create(&db_path, &header, &mut main_file)?;
    let cursor = journal.begin_checkpoint_batch()?;
    let mut staged_header = header;
    staged_header.total_page_count = db_page_count;
    staged_header.last_checkpoint_ts = Ts {
        physical_ms: 1,
        logical: 0,
    };
    journal.append_checkpoint_commit_boundary(&staged_header, cursor)
}

#[test]
fn test_allocator_freeze_rejects_allocate_free_and_header_flush() -> Result<()> {
    let engine = buffered_engine()?;
    let next_page_after_unfreeze = engine
        .shared
        .handle
        .allocator()
        .with_header(|header| header.total_page_count)?;
    let freeze = engine.shared.handle.allocator().freeze_guard()?;

    let alloc_err = engine
        .shared
        .handle
        .alloc_page(PageSize::Small4k)
        .expect_err("frozen allocator rejects allocate");
    assert_freeze_rejection(alloc_err);
    assert_checkpoint_poisoned(&engine);

    let free_err = engine
        .shared
        .handle
        .free_page(1, PageSize::Small4k)
        .expect_err("frozen allocator rejects free");
    assert_freeze_rejection(free_err);

    let flush_err = engine
        .shared
        .handle
        .flush()
        .expect_err("frozen allocator rejects header flush");
    assert_freeze_rejection(flush_err);

    drop(freeze);
    assert_eq!(
        engine.shared.handle.alloc_page(PageSize::Small4k)?,
        next_page_after_unfreeze,
        "dropping the guard unfreezes allocator mutation"
    );
    Ok(())
}

#[test]
fn test_boundary_appended_token_is_single_producer_single_consumer() -> Result<()> {
    let io = ArcIo(MockIo::new());
    let allocator = AllocatorHandle::new(FileHeader::new_now());
    let mut staged_header = allocator.with_header(|header| header.clone())?;
    staged_header.total_page_count = 7;
    staged_header.catalog_root_page = 4;
    staged_header.catalog_root_level = 1;
    staged_header.catalog_root_backup = 4;

    let boundary = boundary_with_db_page_count(staged_header.total_page_count)?;
    let freeze = allocator.freeze_guard()?;
    allocator.commit_staged_header_after_boundary(
        freeze,
        staged_header.clone(),
        boundary,
        CheckpointLifetimeDrain::default(),
    )?;

    let observed = allocator.with_header(|header| header.clone())?;
    assert_eq!(observed.total_page_count, staged_header.total_page_count);
    assert_eq!(observed.catalog_root_page, staged_header.catalog_root_page);
    assert_eq!(
        observed.catalog_root_level,
        staged_header.catalog_root_level
    );
    assert!(
        !allocator.is_header_dirty(),
        "boundary-backed staged header is already durable authority"
    );
    assert_eq!(
        allocator.alloc_4k(&io)?,
        staged_header.total_page_count,
        "successful token consumption releases the freeze"
    );

    let mut mismatched_header = allocator.with_header(|header| header.clone())?;
    mismatched_header.total_page_count += 1;
    let boundary = boundary_with_db_page_count(observed.total_page_count)?;
    let freeze = allocator.freeze_guard()?;
    let err = allocator
        .commit_staged_header_after_boundary(
            freeze,
            mismatched_header,
            boundary,
            CheckpointLifetimeDrain::default(),
        )
        .expect_err("boundary page count mismatch rejects staged header");
    assert!(matches!(err, Error::Internal(message) if message.contains("boundary page count")));
    assert!(
        allocator.alloc_4k(&io).is_ok(),
        "failed token consumption still unfreezes before returning"
    );
    Ok(())
}
