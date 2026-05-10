use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use crate::error::{Error, Result};
use crate::journal::log_file::JournalPageSize;
use crate::journal::{journal_path_for, CheckpointPoolKind, JournalManager};
use crate::mvcc::Ts;
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};
use crate::storage::page::PAGE_SIZE_LEAF;

const CHECKPOINT_PAGE: u32 = 9;
const CHECKPOINT_FILL: u8 = 0xA7;
const STALE_MAIN_FILL: u8 = 0x11;
const CHECKPOINT_TOTAL_PAGE_COUNT: u32 = 12;

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
fn eio_mid_emergency_checkpoint_resumes_on_reopen() -> Result<()> {
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

// `emergency_checkpoint_after_boundary_checks_boundary_page_count` deleted —
// the explicit `emergency_checkpoint_after_boundary` flow is gone now that
// the new recovery scan replays `CheckpointPageFrame` records into the main
// file natively when their matching `CheckpointBoundary` is encountered.
