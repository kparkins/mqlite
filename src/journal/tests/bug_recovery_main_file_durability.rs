//! BUG-8 regression test: recovery's `journal_truncatable` path durably
//! destroys the journal's only copy of replayed checkpoint pages — journal
//! header rewrite + `set_len(JOURNAL_HEADER_SIZE)` + `sync_data()` on the
//! JOURNAL (`src/journal/recovery.rs`, `recover_log_record_journal`) — and
//! the replayed pages were written to the MAIN file with buffered
//! `write_page_to_main` and at most `File::flush()` (`recovery.rs`,
//! `scan.applied_catalog_commit` branch). `std::io::Write::flush` on `File`
//! is NOT a durability fsync, and nothing later in `Client::open`
//! (`src/client/open.rs`) syncs the main file after
//! `JournalManager::open_or_create` returns. Without a main-file
//! `sync_data()` before the journal truncation, a power cut after `open()`
//! loses pages that were durable (in the journal) BEFORE recovery ran: the
//! journal copy is durably gone, the main-file copy was only in the OS page
//! cache.
//!
//! True OS-cache loss cannot be simulated faithfully from safe test code on
//! the concrete `File` handles recovery uses, so this test pins the
//! underlying ordering invariant via the codebase's own US-039
//! sync-ownership observation convention (`append_sync_observations`,
//! `src/journal/tests/append_sync_observations.rs`): every durable MAIN-file
//! sync boundary records `record_main_file_sync()` (the other such boundary
//! lives in `BufferPoolHandle`, `src/storage/handle.rs`). By the time
//! recovery has durably truncated the journal to its bare header, at least
//! one durable main-file sync must have been recorded.
//!
//! NOTE: the observation counters are process-global; run this test with a
//! targeted filter for a deterministic result.

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

use crate::journal::wire::{JournalPageSize, JOURNAL_HEADER_SIZE};
use crate::journal::{
    append_sync_observations, journal_path_for, CheckpointPoolKind, JournalManager,
};
use crate::mvcc::Ts;
use crate::storage::header::FileHeader;
use crate::storage::page::PAGE_SIZE_LEAF;

const CHECKPOINT_PAGE: u32 = 9;
const CHECKPOINT_FILL: u8 = 0xA7;
const STALE_MAIN_FILL: u8 = 0x11;
const CHECKPOINT_TOTAL_PAGE_COUNT: u32 = 12;
const CHECKPOINT_TS: Ts = Ts {
    physical_ms: 7,
    logical: 0,
};

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

fn staged_header(initial_header: &FileHeader) -> FileHeader {
    let mut header = initial_header.clone();
    header.total_page_count = CHECKPOINT_TOTAL_PAGE_COUNT;
    header.last_checkpoint_ts = CHECKPOINT_TS;
    header.catalog_root_page = 3;
    header.catalog_root_backup = 3;
    header.catalog_root_level = 1;
    header
}

/// BUG-8 invariant: before recovery durably truncates the journal to its
/// bare header (destroying the only durable copy of the replayed checkpoint
/// pages), the main database file holding those replayed pages must have been
/// durably synced (and the US-039 main-file-sync boundary recorded).
#[test]
fn bug8_truncatable_recovery_syncs_main_file_before_destroying_journal_copy() {
    let dir = tempfile::tempdir().expect("tempdir");
    let db_path = dir.path().join("bug8.mqlite");
    let header = make_header();
    let mut main_file = open_main_file(&db_path);
    main_file
        .seek(SeekFrom::Start(0))
        .and_then(|_| main_file.write_all(&header.to_bytes()))
        .expect("write header");
    main_file
        .set_len((CHECKPOINT_PAGE as u64 + 1) * PAGE_SIZE_LEAF as u64)
        .expect("preallocate main file");
    write_page_fill(&db_path, CHECKPOINT_PAGE, STALE_MAIN_FILL);

    // Session 1: append one checkpoint page frame plus its commit boundary
    // and make them durable in the journal (mirrors the engine's checkpoint
    // flow; see src/journal/tests/checkpoint_boundary_recovery.rs idioms).
    {
        let mut journal = JournalManager::open_or_create(&db_path, &header, &mut main_file)
            .expect("open journal");
        let cursor = journal.begin_checkpoint_batch().expect("begin batch");
        let page = vec![CHECKPOINT_FILL; JournalPageSize::Large32k.bytes()];
        journal
            .append_checkpoint_frame(
                cursor.batch_id(),
                CheckpointPoolKind::Main,
                CHECKPOINT_PAGE,
                JournalPageSize::Large32k,
                &page,
            )
            .expect("append checkpoint frame");
        let _ = journal
            .append_checkpoint_commit_boundary(&staged_header(&header), cursor)
            .expect("append boundary");
        journal.sync_journal().expect("sync checkpoint batch");
    }
    drop(main_file);

    append_sync_observations::reset();

    // Session 2: reopen. Recovery replays the boundary + page frame into the
    // main file and takes the journal_truncatable branch, durably resetting
    // the journal to its bare header.
    let mut reopen_file = open_main_file(&db_path);
    let recovered = JournalManager::open_or_create(&db_path, &header, &mut reopen_file)
        .expect("recover checkpoint journal");
    let observations = append_sync_observations::snapshot();
    drop(recovered);

    // Harness sanity preconditions (not the bug): the checkpointed page WAS
    // replayed into the main file, and the journal WAS durably reset to its
    // bare header — i.e. the journal's only durable copy of that page is gone.
    assert_eq!(
        read_page_byte(&db_path, CHECKPOINT_PAGE),
        CHECKPOINT_FILL,
        "harness: recovery must replay the checkpointed page into the main file"
    );
    assert_eq!(
        journal_len(&db_path),
        JOURNAL_HEADER_SIZE as u64,
        "harness: recovery must take the journal_truncatable branch"
    );

    assert!(
        observations.main_file_syncs >= 1,
        "BUG(BUG-8): recovery durably truncated the journal (header rewrite + set_len + \
         sync_data in recover_log_record_journal's journal_truncatable branch), destroying \
         the journal's only durable copy of the replayed checkpoint pages, but recorded \
         {} durable MAIN-file sync boundaries — the replayed pages exist only in the OS \
         page cache (write_page_to_main + File::flush are not fsyncs) and a power cut \
         after open() returns loses data that was durable before recovery ran. The main \
         file must be fsynced (recording the boundary via \
         append_sync_observations::record_main_file_sync per the US-039 convention) \
         BEFORE the journal header rewrite/set_len.",
        observations.main_file_syncs,
    );

    // R-bug8-order: pin the SEQUENCE, not just presence. A main-file sync
    // issued AFTER the journal-header rewrite would satisfy the count-only
    // assertion above while still losing the replayed pages on a power cut
    // between the destruction and the late sync. The truncatable branch
    // records its journal-truncate observation at the FIRST destructive
    // write — the journal-header rewrite that bumps checkpoint_seq, retiring
    // every following record, before set_len even runs (N26 strengthened it
    // from the set_len site) — so the first sync must be ordered strictly
    // before any destructive byte reaches the journal on the shared event
    // ticket.
    assert!(
        observations.journal_truncates >= 1,
        "harness: the journal_truncatable branch must record its journal-truncate \
         observation at its first destructive write (the header rewrite); recorded \
         {} truncate observations",
        observations.journal_truncates,
    );
    let sync_seq = observations
        .first_main_file_sync_seq
        .expect("main_file_syncs >= 1 implies a recorded first-sync ordinal");
    let truncate_seq = observations
        .first_journal_truncate_seq
        .expect("journal_truncates >= 1 implies a recorded first-truncate ordinal");
    assert!(
        sync_seq < truncate_seq,
        "BUG(BUG-8 order): the journal-truncate observation (event #{truncate_seq}) was \
         recorded before the first durable main-file sync (event #{sync_seq}); the \
         truncatable branch must fsync the main file BEFORE its first destructive \
         write (the journal-header rewrite, then set_len) destroys the journal's \
         only durable copy of the replayed checkpoint pages",
    );
}
