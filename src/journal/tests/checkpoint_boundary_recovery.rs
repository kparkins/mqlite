#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use super::log_file::{JournalPageSize, JOURNAL_FRAME_HEADER_SIZE, JOURNAL_HEADER_SIZE};
use super::*;
use crate::mvcc::Ts;
use crate::storage::header::{FileHeader, HEADER_PAGE_SIZE};
use crate::storage::page::PAGE_SIZE_LEAF;

const CHECKPOINT_PAGE: u32 = 9;
const CHECKPOINT_FILL: u8 = 0xA7;
const STALE_MAIN_FILL: u8 = 0x11;
const CHECKPOINT_TOTAL_PAGE_COUNT: u32 = 12;
const CHECKPOINT_TS: Ts = Ts {
    physical_ms: 7,
    logical: 0,
};

struct JournalFixture {
    _dir: tempfile::TempDir,
    db_path: std::path::PathBuf,
    main_file: File,
    header: FileHeader,
}

fn make_header() -> FileHeader {
    FileHeader::new(1_700_000_000_000, 0xDEAD_BEEF, 0xCAFE_BABE)
}

fn open_main_file(db_path: &Path) -> File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(db_path)
        .expect("open main file")
}

fn write_header(file: &mut File, header: &FileHeader) {
    file.seek(SeekFrom::Start(0)).expect("seek header");
    file.write_all(&header.to_bytes()).expect("write header");
}

fn read_header(file: &mut File) -> FileHeader {
    let mut bytes = [0u8; HEADER_PAGE_SIZE];
    file.seek(SeekFrom::Start(0)).expect("seek header");
    file.read_exact(&mut bytes).expect("read header");
    FileHeader::from_bytes(&bytes).expect("decode header")
}

fn read_page_byte(db_path: &Path, page_number: u32) -> u8 {
    let mut file = open_main_file(db_path);
    file.seek(SeekFrom::Start(page_number as u64 * PAGE_SIZE_LEAF as u64))
        .expect("seek page");
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).expect("read page byte");
    byte[0]
}

fn write_page_fill(db_path: &Path, page_number: u32, fill: u8) {
    let mut file = open_main_file(db_path);
    file.seek(SeekFrom::Start(page_number as u64 * PAGE_SIZE_LEAF as u64))
        .expect("seek page");
    file.write_all(&[fill]).expect("write page fill");
    file.sync_data().expect("sync main page");
}

fn journal_len(db_path: &Path) -> u64 {
    std::fs::metadata(journal_path_for(db_path))
        .expect("journal metadata")
        .len()
}

fn fixture(name: &str) -> JournalFixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join(name);
    let mut main_file = open_main_file(&db_path);
    let header = make_header();
    write_header(&mut main_file, &header);
    main_file
        .set_len((CHECKPOINT_PAGE as u64 + 1) * PAGE_SIZE_LEAF as u64)
        .expect("preallocate main file");
    write_page_fill(&db_path, CHECKPOINT_PAGE, STALE_MAIN_FILL);
    JournalFixture {
        _dir: dir,
        db_path,
        main_file,
        header,
    }
}

fn staged_header(initial_header: &FileHeader) -> FileHeader {
    let mut header = initial_header.clone();
    header.total_page_count = CHECKPOINT_TOTAL_PAGE_COUNT;
    header.last_checkpoint_ts = CHECKPOINT_TS;
    header.catalog_root_page = 3;
    header.catalog_root_backup = 3;
    header.catalog_root_level = 1;
    header
}

fn append_checkpoint_page_frame(
    journal: &mut JournalManager,
    cursor: &CheckpointBatchCursor,
) -> u64 {
    let page = vec![CHECKPOINT_FILL; JournalPageSize::Large32k.bytes()];
    journal
        .append_checkpoint_frame(
            cursor.batch_id(),
            CheckpointPoolKind::Main,
            CHECKPOINT_PAGE,
            JournalPageSize::Large32k,
            &page,
        )
        .expect("append checkpoint frame")
}

fn corrupt_frame_checksum(db_path: &Path, frame_offset: u64) {
    let mut journal = OpenOptions::new()
        .read(true)
        .write(true)
        .open(journal_path_for(db_path))
        .expect("open journal");
    let checksum_offset = frame_offset + (JOURNAL_FRAME_HEADER_SIZE as u64 - 4);
    journal
        .seek(SeekFrom::Start(checksum_offset))
        .expect("seek checksum");
    let mut checksum = [0u8; 4];
    journal.read_exact(&mut checksum).expect("read checksum");
    checksum[0] ^= 0xFF;
    journal
        .seek(SeekFrom::Start(checksum_offset))
        .expect("seek checksum rewrite");
    journal.write_all(&checksum).expect("rewrite checksum");
    journal.sync_data().expect("sync corrupt checksum");
}

#[test]
fn post_step_8_checkpoint_batch_without_boundary_discards_batch() {
    let JournalFixture {
        _dir,
        db_path,
        mut main_file,
        header,
    } = fixture("phase7-us011-post-step8.mqlite");
    {
        let mut journal = JournalManager::open_or_create(&db_path, &header, &mut main_file)
            .expect("open journal");
        let cursor = journal.begin_checkpoint_batch().expect("begin batch");
        append_checkpoint_page_frame(&mut journal, &cursor);
        journal.sync_journal().expect("sync checkpoint batch");
    }

    let mut reopen_file = open_main_file(&db_path);
    let recovered = JournalManager::open_or_create(&db_path, &header, &mut reopen_file)
        .expect("recover post-step-8 batch");

    assert_eq!(
        read_page_byte(&db_path, CHECKPOINT_PAGE),
        STALE_MAIN_FILL,
        "checkpoint batch frames without a page-0 boundary must not copy to main"
    );
    assert_eq!(
        read_header(&mut reopen_file).last_checkpoint_ts,
        header.last_checkpoint_ts,
        "post-step-8 recovery must leave the durable header frontier unchanged"
    );
    assert!(
        recovered.index().lookup(CHECKPOINT_PAGE).is_none(),
        "discarded checkpoint batch must not be retained in the recovery index"
    );
    assert_eq!(
        recovered.write_cursor(),
        JOURNAL_HEADER_SIZE as u64,
        "discarded checkpoint batch must not remain as appendable journal tail"
    );
    assert_eq!(
        journal_len(&db_path),
        JOURNAL_HEADER_SIZE as u64,
        "discarded checkpoint batch bytes must be truncated from the journal"
    );
}

#[test]
fn torn_non_commit_frame_before_intact_commit_boundary_discards_batch() {
    let JournalFixture {
        _dir,
        db_path,
        mut main_file,
        header,
    } = fixture("phase7-us011-torn-before-boundary.mqlite");
    let checkpoint_frame_offset;
    {
        let mut journal = JournalManager::open_or_create(&db_path, &header, &mut main_file)
            .expect("open journal");
        let cursor = journal.begin_checkpoint_batch().expect("begin batch");
        checkpoint_frame_offset = append_checkpoint_page_frame(&mut journal, &cursor);
        let _ = journal
            .append_checkpoint_commit_boundary(&staged_header(&header), cursor)
            .expect("append boundary");
        journal.sync_journal().expect("sync checkpoint batch");
    }
    corrupt_frame_checksum(&db_path, checkpoint_frame_offset);

    let mut reopen_file = open_main_file(&db_path);
    let recovered = JournalManager::open_or_create(&db_path, &header, &mut reopen_file)
        .expect("recover with torn pre-boundary frame");

    assert_eq!(
        read_page_byte(&db_path, CHECKPOINT_PAGE),
        STALE_MAIN_FILL,
        "a torn pre-boundary non-commit frame must halt scan before the intact boundary"
    );
    assert_eq!(
        read_header(&mut reopen_file).last_checkpoint_ts,
        header.last_checkpoint_ts,
        "intact page-0 boundary after a torn non-commit frame must be ignored"
    );
    assert!(
        recovered.index().lookup(CHECKPOINT_PAGE).is_none(),
        "torn checkpoint batch must not be retained in the recovery index"
    );
    assert_eq!(
        recovered.write_cursor(),
        checkpoint_frame_offset,
        "recovery must resume appends at the torn frame offset"
    );
}

// `checkpoint_boundary_rejects_preexisting_pending_legacy_frame` deleted —
// the legacy 24-byte page-frame allocator (and its `legacy_pending_start_offset`
// validator) is gone now that every journal write goes through `LogManager`.
