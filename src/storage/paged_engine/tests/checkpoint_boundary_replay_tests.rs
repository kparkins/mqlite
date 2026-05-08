#![allow(non_snake_case)]

use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::journal::log_file::{JournalPageSize, PageId};
use crate::journal::{
    journal_path_for, CheckpointFlushSet, CheckpointPoolKind, JournalLayeredSource, JournalManager,
};
use crate::mvcc::Ts;
use crate::storage::buffer_pool::{default_sizes, BufferPool, PageSize, PageSource};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};
use crate::storage::page::PAGE_SIZE_LEAF;
use crate::storage::test_support::{ArcIo, MockIo};

const CHECKPOINT_PAGE: u32 = 9;
const CHECKPOINT_FILL: u8 = 0xA7;
const STALE_MAIN_FILL: u8 = 0x11;
const CHECKPOINT_TOTAL_PAGE_COUNT: u32 = 12;

struct BoundaryFixture {
    _dir: tempfile::TempDir,
    db_path: std::path::PathBuf,
    handle: Arc<BufferPoolHandle>,
    journal: Arc<Mutex<JournalManager>>,
    initial_header: FileHeader,
}

fn open_main_file(db_path: &std::path::Path) -> Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(db_path)
        .map_err(Error::Io)
}

fn write_header(file: &mut File, header: &FileHeader) -> Result<()> {
    file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
    file.write_all(&header.to_bytes()).map_err(Error::Io)
}

fn read_header(file: &mut File) -> Result<FileHeader> {
    let mut bytes = [0u8; HEADER_PAGE_SIZE];
    file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
    file.read_exact(&mut bytes).map_err(Error::Io)?;
    FileHeader::from_bytes(&bytes)
}

fn read_page_byte(db_path: &std::path::Path, page_number: u32) -> Result<u8> {
    let mut file = open_main_file(db_path)?;
    file.seek(SeekFrom::Start(page_number as u64 * PAGE_SIZE_LEAF as u64))
        .map_err(Error::Io)?;
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).map_err(Error::Io)?;
    Ok(byte[0])
}

fn journal_len(db_path: &std::path::Path) -> Result<u64> {
    std::fs::metadata(journal_path_for(db_path))
        .map(|metadata| metadata.len())
        .map_err(Error::Io)
}

fn pages<const N: usize>(ids: [u32; N]) -> BTreeSet<PageId> {
    ids.into_iter().map(PageId).collect()
}

fn staged_header(initial_header: &FileHeader) -> FileHeader {
    let mut header = initial_header.clone();
    header.total_page_count = CHECKPOINT_TOTAL_PAGE_COUNT;
    header.last_checkpoint_ts = Ts {
        physical_ms: 7,
        logical: 0,
    };
    header.catalog_root_page = 3;
    header.catalog_root_backup = 3;
    header.catalog_root_level = 1;
    header
}

fn fixture() -> Result<BoundaryFixture> {
    let dir = tempfile::tempdir().map_err(Error::Io)?;
    let db_path = dir.path().join("phase7-us007.mqlite");
    let initial_header = FileHeader::new_now();
    let mut main_file = open_main_file(&db_path)?;
    write_header(&mut main_file, &initial_header)?;

    let journal = Arc::new(Mutex::new(JournalManager::open_or_create(
        &db_path,
        &initial_header,
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
        initial_header.clone(),
        Arc::clone(&journal),
        Arc::new(Mutex::new(main_file)),
    ));
    Ok(BoundaryFixture {
        _dir: dir,
        db_path,
        handle,
        journal,
        initial_header,
    })
}

fn append_durable_boundary(fixture: &BoundaryFixture) -> Result<FileHeader> {
    {
        let mut page = fixture
            .handle
            .fetch_page(CHECKPOINT_PAGE, PageSize::Large32k)?;
        page.data_mut().fill(CHECKPOINT_FILL);
    }
    let checkpoint_applied_lsn = fixture.handle.current_journal_durable_lsn()?;
    fixture
        .handle
        .stamp_unflushable_dirty_pages_lsn(checkpoint_applied_lsn)?;
    let batch_id = fixture.handle.next_checkpoint_batch_id()?;
    let flush_set = CheckpointFlushSet::new(
        batch_id,
        pages([CHECKPOINT_PAGE]),
        BTreeSet::new(),
        BTreeSet::new(),
    )?;
    let cursor = fixture.handle.flush_journal_durable(flush_set)?;
    let staged_header = staged_header(&fixture.initial_header);
    let _boundary = fixture
        .journal
        .lock()
        .expect("journal mutex")
        .append_checkpoint_commit_boundary(&staged_header, cursor)?;
    Ok(staged_header)
}

fn append_durable_boundary_without_handle(
    db_path: &std::path::Path,
    initial_header: &FileHeader,
) -> Result<FileHeader> {
    let mut main_file = open_main_file(db_path)?;
    let mut journal = JournalManager::open_or_create(db_path, initial_header, &mut main_file)?;
    let cursor = journal.begin_checkpoint_batch()?;
    let page = vec![CHECKPOINT_FILL; JournalPageSize::Large32k.bytes()];
    journal.append_checkpoint_frame(
        cursor.batch_id(),
        CheckpointPoolKind::Main,
        CHECKPOINT_PAGE,
        JournalPageSize::Large32k,
        &page,
    )?;
    let staged_header = staged_header(initial_header);
    let _boundary = journal.append_checkpoint_commit_boundary(&staged_header, cursor)?;
    journal.sync_journal()?;
    Ok(staged_header)
}

fn seed_stale_main_page(db_path: &std::path::Path) -> Result<()> {
    let mut main_file = open_main_file(db_path)?;
    main_file
        .seek(SeekFrom::Start(
            CHECKPOINT_PAGE as u64 * PAGE_SIZE_LEAF as u64,
        ))
        .map_err(Error::Io)?;
    main_file.write_all(&[STALE_MAIN_FILL]).map_err(Error::Io)
}

#[test]
fn F_test_eio_mid_emergency_checkpoint_resumes_on_reopen() -> Result<()> {
    let dir = tempfile::tempdir().map_err(Error::Io)?;
    let db_path = dir.path().join("phase7-us007-eio.mqlite");
    let initial_header = FileHeader::new_now();
    let mut main_file = open_main_file(&db_path)?;
    write_header(&mut main_file, &initial_header)?;
    append_durable_boundary_without_handle(&db_path, &initial_header)?;

    seed_stale_main_page(&db_path)?;
    drop(main_file);

    let mut reopen_file = open_main_file(&db_path)?;
    let recovered = JournalManager::open_or_create(&db_path, &initial_header, &mut reopen_file)?;
    assert!(
        recovered.did_recover_pages(),
        "recovery must report that the durable checkpoint boundary copied pages"
    );
    drop(recovered);

    assert_eq!(
        read_page_byte(&db_path, CHECKPOINT_PAGE)?,
        CHECKPOINT_FILL,
        "reopen must overwrite stale mid-copy page bytes from the journal batch"
    );
    assert_eq!(
        read_header(&mut reopen_file)?.total_page_count,
        CHECKPOINT_TOTAL_PAGE_COUNT,
        "reopen must copy the page-0 boundary staged header"
    );
    assert_eq!(
        journal_len(&db_path)?,
        crate::journal::log_file::JOURNAL_HEADER_SIZE as u64,
        "completed recovery must truncate the copied checkpoint journal"
    );
    Ok(())
}

#[test]
fn mid_step_10_crash_cut_replays_boundary_and_truncates_journal() -> Result<()> {
    let dir = tempfile::tempdir().map_err(Error::Io)?;
    let db_path = dir.path().join("phase7-us007-crash-cut.mqlite");
    let initial_header = FileHeader::new_now();
    let mut main_file = open_main_file(&db_path)?;
    write_header(&mut main_file, &initial_header)?;
    append_durable_boundary_without_handle(&db_path, &initial_header)?;
    drop(main_file);

    let mut reopen_file = open_main_file(&db_path)?;
    let recovered = JournalManager::open_or_create(&db_path, &initial_header, &mut reopen_file)?;
    assert!(
        recovered.did_recover_pages(),
        "post-boundary crash-cut recovery must finish the checkpoint copy"
    );
    drop(recovered);

    assert_eq!(read_page_byte(&db_path, CHECKPOINT_PAGE)?, CHECKPOINT_FILL);
    assert_eq!(
        read_header(&mut reopen_file)?.last_checkpoint_ts,
        Ts {
            physical_ms: 7,
            logical: 0
        }
    );
    assert_eq!(
        journal_len(&db_path)?,
        crate::journal::log_file::JOURNAL_HEADER_SIZE as u64
    );
    Ok(())
}

#[test]
fn emergency_checkpoint_after_boundary_checks_boundary_page_count() -> Result<()> {
    let fixture = fixture()?;
    let staged_header = append_durable_boundary(&fixture)?;
    let allocator_header_before = fixture
        .handle
        .allocator()
        .with_header(|header| header.clone())?;

    let err = fixture
        .handle
        .emergency_checkpoint_after_boundary(staged_header.total_page_count + 1)
        .expect_err("mismatched expected page count must reject before copy");
    assert!(
        matches!(err, Error::Internal(ref message) if message.contains("boundary page count")),
        "expected boundary page count mismatch, got {err:?}"
    );

    fixture
        .handle
        .emergency_checkpoint_after_boundary(staged_header.total_page_count)?;
    let allocator_header_after = fixture
        .handle
        .allocator()
        .with_header(|header| header.clone())?;
    assert_eq!(
        allocator_header_after, allocator_header_before,
        "post-boundary copy must not mutate allocator header state"
    );
    assert_eq!(
        read_page_byte(&fixture.db_path, CHECKPOINT_PAGE)?,
        CHECKPOINT_FILL
    );
    assert_eq!(
        journal_len(&fixture.db_path)?,
        crate::journal::log_file::JOURNAL_HEADER_SIZE as u64
    );
    Ok(())
}
