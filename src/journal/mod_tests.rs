mod tests {
    use super::super::*;
    use crate::storage::header::FileHeader;
    use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};
    use std::io::Read;
    use tempfile::TempDir;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_db_file() -> (TempDir, PathBuf, File) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.mqlite");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&db_path)
            .unwrap();
        (dir, db_path, file)
    }

    fn make_header() -> FileHeader {
        FileHeader::new(1_700_000_000_000, 0xDEAD_BEEF, 0xCAFE_BABE)
    }

    fn make_page_4k(fill: u8) -> Vec<u8> {
        vec![fill; PAGE_SIZE_INTERNAL as usize]
    }

    fn make_page_32k(fill: u8) -> Vec<u8> {
        vec![fill; PAGE_SIZE_LEAF as usize]
    }

    // -----------------------------------------------------------------------
    // Path helpers
    // -----------------------------------------------------------------------

    #[test]
    fn journal_path_derivation() {
        let db = Path::new("/tmp/foo.mqlite");
        assert_eq!(
            journal_path_for(db),
            PathBuf::from("/tmp/foo.mqlite-journal")
        );
    }

    // -----------------------------------------------------------------------
    // Open / create
    // -----------------------------------------------------------------------

    #[test]
    fn open_creates_journal_file() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let jp = journal_path_for(&db_path);
        assert!(jp.exists(), "journal file must be created");

        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    /// Regression: no `.mqlite-shm` sidecar must ever be created. The journal
    /// index is in-memory only.
    #[test]
    fn no_shm_file_created_in_any_phase() {
        let (dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let mut header = make_header();
        let shm_sidecar = {
            let mut p = db_path.as_os_str().to_owned();
            p.push("-shm");
            PathBuf::from(p)
        };

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert!(!shm_sidecar.exists(), "no -shm after open");

        mgr.append_non_commit(1, JournalPageSize::Small4k, &make_page_4k(0x01))
            .unwrap();
        assert!(!shm_sidecar.exists(), "no -shm after append");

        mgr.commit(2, JournalPageSize::Small4k, &make_page_4k(0x02), 5)
            .unwrap();
        assert!(!shm_sidecar.exists(), "no -shm after commit");

        mgr.checkpoint(&mut main_file, &mut header).unwrap();
        assert!(!shm_sidecar.exists(), "no -shm after checkpoint");

        mgr.close_and_cleanup(&mut main_file, &mut header).unwrap();
        assert!(!shm_sidecar.exists(), "no -shm after clean close");

        drop(main_file);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // Append and read back
    // -----------------------------------------------------------------------

    #[test]
    fn append_and_read_4k() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_data = make_page_4k(0xAB);
        mgr.append_non_commit(3, JournalPageSize::Small4k, &page_data)
            .unwrap();

        let result = mgr.read_page(3).unwrap();
        assert_eq!(result, Some(page_data));
        assert!(mgr.read_page(99).unwrap().is_none());
    }

    #[test]
    fn append_and_read_32k() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_data = make_page_32k(0xCC);
        mgr.append_non_commit(10, JournalPageSize::Large32k, &page_data)
            .unwrap();

        let result = mgr.read_page(10).unwrap();
        assert_eq!(result, Some(page_data));
    }

    #[test]
    fn latest_write_wins() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_v1 = make_page_4k(0x01);
        let page_v2 = make_page_4k(0x02);
        mgr.append_non_commit(5, JournalPageSize::Small4k, &page_v1)
            .unwrap();
        mgr.append_non_commit(5, JournalPageSize::Small4k, &page_v2)
            .unwrap();

        // Index lookup returns offset of latest (second) frame.
        let result = mgr.read_page(5).unwrap().unwrap();
        assert_eq!(result[0], 0x02);
    }

    // -----------------------------------------------------------------------
    // Commit
    // -----------------------------------------------------------------------

    #[test]
    fn commit_frame_marks_transaction_boundary() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_a = make_page_4k(0xAA);
        let page_b = make_page_4k(0xBB);
        mgr.append_non_commit(1, JournalPageSize::Small4k, &page_a)
            .unwrap();
        let emergency = mgr
            .commit(2, JournalPageSize::Small4k, &page_b, 10)
            .unwrap();
        assert!(!emergency);
        assert_eq!(mgr.last_committed_db_page_count, Some(10));
    }

    // -----------------------------------------------------------------------
    // Checkpoint
    // -----------------------------------------------------------------------

    #[test]
    fn checkpoint_writes_pages_to_main_file() {
        let (_dir, db_path, mut main_file) = make_db_file();
        // Pre-allocate main file large enough
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();

        let mut header = make_header();
        let mut mgr =
            JournalManager::open_or_create(&db_path, &mut header, &mut main_file).unwrap();

        let page_data = make_page_4k(0x42);
        mgr.append_non_commit(2, JournalPageSize::Small4k, &page_data)
            .unwrap();
        mgr.commit(2, JournalPageSize::Small4k, &page_data, 5)
            .unwrap();

        mgr.checkpoint(&mut main_file, &mut header).unwrap();

        // Verify: page 2 in main file at the uniform 32 KB slot offset.
        let offset = 2u64 * PAGE_SIZE_LEAF as u64;
        main_file.seek(SeekFrom::Start(offset)).unwrap();
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0x42);

        // Journal should be reset.
        assert_eq!(mgr.write_cursor, JOURNAL_HEADER_SIZE as u64);
        assert_eq!(mgr.index.occupied_count(), 0);
    }

    #[test]
    fn checkpoint_increments_sequence() {
        let (_dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let mut header = make_header();
        let mut mgr =
            JournalManager::open_or_create(&db_path, &mut header, &mut main_file).unwrap();
        assert_eq!(mgr.checkpoint_seq, 0);

        let page_data = make_page_4k(0x01);
        mgr.commit(1, JournalPageSize::Small4k, &page_data, 2)
            .unwrap();
        mgr.checkpoint(&mut main_file, &mut header).unwrap();

        assert_eq!(mgr.checkpoint_seq, 1);
    }

    // -----------------------------------------------------------------------
    // Recovery — crash simulation
    // -----------------------------------------------------------------------

    #[test]
    fn recovery_replays_committed_frames() {
        let (_dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let header = make_header();

        // Write two frames and commit.
        {
            let mut mgr =
                JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

            let page_a = make_page_4k(0xAA);
            let page_b = make_page_4k(0xBB);
            mgr.append_non_commit(1, JournalPageSize::Small4k, &page_a)
                .unwrap();
            mgr.commit(2, JournalPageSize::Small4k, &page_b, 5).unwrap();
            // Simulate crash: don't call close_and_cleanup.
            // Journal file left on disk.
        }

        // Reopen — recovery runs automatically.
        let mut main_file2 = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let _mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file2).unwrap();

        // Both pages should have been replayed into main file at 32 KB slots.
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file2
            .seek(SeekFrom::Start(1 * PAGE_SIZE_LEAF as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0xAA, "page 1 should be replayed");

        main_file2
            .seek(SeekFrom::Start(2 * PAGE_SIZE_LEAF as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0xBB, "page 2 should be replayed");
    }

    #[test]
    fn recovery_discards_uncommitted_frames() {
        let (_dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let header = make_header();

        // Write one committed frame, then one uncommitted (simulated crash mid-tx).
        {
            let mut mgr =
                JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

            let page_committed = make_page_4k(0xCC);
            let page_uncommitted = make_page_4k(0xDD);
            mgr.commit(1, JournalPageSize::Small4k, &page_committed, 3)
                .unwrap();
            // Append non-commit frame — transaction never completed.
            mgr.append_non_commit(2, JournalPageSize::Small4k, &page_uncommitted)
                .unwrap();
            // Crash: no commit for page 2.
        }

        let mut main_file2 = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file2).unwrap();

        // Page 1 (committed) should be in main file at the 32 KB slot offset.
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file2
            .seek(SeekFrom::Start(1 * PAGE_SIZE_LEAF as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0xCC, "committed page must be present");

        // Page 2 (uncommitted) — index should NOT have it after recovery.
        assert!(
            mgr2.index().lookup(2).is_none(),
            "uncommitted page must not be in journal index after recovery"
        );
    }

    #[test]
    fn stale_journal_is_deleted_on_open() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        // Create journal with original salts.
        {
            let _mgr =
                JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        }
        assert!(journal_path_for(&db_path).exists());

        // Reopen with different salts (simulates a different database open).
        let different_header =
            FileHeader::new(1_700_000_000_001, 0x1111_1111, 0x2222_2222);
        let mgr2 = JournalManager::open_or_create(
            &db_path,
            &different_header,
            &mut main_file,
        )
        .unwrap();
        // A fresh journal should have been created with the new salts.
        assert_eq!(mgr2.salt1, 0x1111_1111);
        assert_eq!(mgr2.salt2, 0x2222_2222);
    }

    // -----------------------------------------------------------------------
    // Clean close
    // -----------------------------------------------------------------------

    #[test]
    fn close_and_cleanup_removes_journal() {
        let (dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let mut header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &mut header, &mut main_file).unwrap();
        let page_data = make_page_4k(0xFF);
        mgr.commit(1, JournalPageSize::Small4k, &page_data, 2)
            .unwrap();

        let jp = journal_path_for(&db_path);

        mgr.close_and_cleanup(&mut main_file, &mut header).unwrap();

        assert!(!jp.exists(), "journal must be deleted after clean close");
        let _ = dir;
    }

    // -----------------------------------------------------------------------
    // Linear scan fallback
    // -----------------------------------------------------------------------

    #[test]
    fn linear_scan_finds_committed_pages() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_data = make_page_4k(0x77);
        mgr.append_non_commit(7, JournalPageSize::Small4k, &page_data)
            .unwrap();

        let result = mgr.read_page_linear(7).unwrap();
        assert_eq!(result, Some(page_data));
        assert!(mgr.read_page_linear(999).unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // Rollback (truncate_to)
    // -----------------------------------------------------------------------

    #[test]
    fn truncate_to_drops_frames_written_after_mark() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_non_commit(1, JournalPageSize::Small4k, &make_page_4k(0x11))
            .unwrap();
        let mark = mgr.write_cursor();
        mgr.append_non_commit(2, JournalPageSize::Small4k, &make_page_4k(0x22))
            .unwrap();
        mgr.append_non_commit(3, JournalPageSize::Small4k, &make_page_4k(0x33))
            .unwrap();

        mgr.truncate_to(mark).unwrap();

        assert_eq!(mgr.write_cursor(), mark);
        assert_eq!(
            mgr.read_page(1).unwrap(),
            Some(make_page_4k(0x11)),
            "frame before mark must survive"
        );
        assert!(
            mgr.read_page(2).unwrap().is_none(),
            "frame after mark must be dropped"
        );
        assert!(mgr.read_page(3).unwrap().is_none());
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn truncate_to_preserves_prior_commit_state() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_non_commit(1, JournalPageSize::Small4k, &make_page_4k(0x11))
            .unwrap();
        mgr.commit(2, JournalPageSize::Small4k, &make_page_4k(0x22), 50)
            .unwrap();
        let mark = mgr.write_cursor();
        mgr.append_non_commit(3, JournalPageSize::Small4k, &make_page_4k(0x33))
            .unwrap();

        mgr.truncate_to(mark).unwrap();

        assert_eq!(mgr.last_committed_db_page_count, Some(50));
        assert_eq!(mgr.read_page(1).unwrap(), Some(make_page_4k(0x11)));
        assert_eq!(mgr.read_page(2).unwrap(), Some(make_page_4k(0x22)));
        assert!(mgr.read_page(3).unwrap().is_none());
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn truncate_to_full_drops_all_non_header_frames() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_non_commit(1, JournalPageSize::Small4k, &make_page_4k(0xAA))
            .unwrap();
        mgr.append_non_commit(2, JournalPageSize::Small4k, &make_page_4k(0xBB))
            .unwrap();

        mgr.truncate_to(JOURNAL_HEADER_SIZE as u64).unwrap();

        assert_eq!(mgr.write_cursor(), JOURNAL_HEADER_SIZE as u64);
        assert!(mgr.read_page(1).unwrap().is_none());
        assert!(mgr.read_page(2).unwrap().is_none());
        assert_eq!(mgr.last_committed_db_page_count, None);
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // JournalLayeredSource
    // -----------------------------------------------------------------------

    struct StubFileSource {
        pages: Mutex<std::collections::HashMap<u32, Vec<u8>>>,
    }

    impl StubFileSource {
        fn new() -> Self {
            Self {
                pages: Mutex::new(std::collections::HashMap::new()),
            }
        }
    }

    impl PageSource for StubFileSource {
        fn read_page(&self, n: u32, size: PageSize, buf: &mut [u8]) -> Result<()> {
            let pages = self.pages.lock().unwrap();
            if let Some(v) = pages.get(&n) {
                buf.copy_from_slice(v);
            } else {
                buf.fill(0);
                let _ = size;
            }
            Ok(())
        }
        fn write_page(&self, n: u32, size: PageSize, buf: &[u8]) -> Result<()> {
            debug_assert_eq!(buf.len(), size.bytes());
            self.pages.lock().unwrap().insert(n, buf.to_vec());
            Ok(())
        }
    }

    #[test]
    fn layered_source_read_hits_journal_first() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let journal = Arc::new(Mutex::new(mgr));

        let file_src: Arc<dyn PageSource> = Arc::new(StubFileSource::new());
        let file_only = make_page_32k(0xFA);
        file_src
            .write_page(5, PageSize::Large32k, &file_only)
            .unwrap();

        journal
            .lock()
            .unwrap()
            .append_non_commit(5, JournalPageSize::Large32k, &make_page_32k(0xB1))
            .unwrap();

        let layered = JournalLayeredSource::new(Arc::clone(&file_src), Arc::clone(&journal));
        let mut buf = vec![0u8; PageSize::Large32k.bytes()];
        layered.read_page(5, PageSize::Large32k, &mut buf).unwrap();
        assert_eq!(buf, make_page_32k(0xB1), "journal version must win over file");
        drop(journal);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn layered_source_read_falls_back_to_file() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let journal = Arc::new(Mutex::new(mgr));

        let file_src: Arc<dyn PageSource> = Arc::new(StubFileSource::new());
        file_src
            .write_page(9, PageSize::Small4k, &make_page_4k(0xCC))
            .unwrap();

        let layered = JournalLayeredSource::new(Arc::clone(&file_src), Arc::clone(&journal));
        let mut buf = vec![0u8; PageSize::Small4k.bytes()];
        layered.read_page(9, PageSize::Small4k, &mut buf).unwrap();
        assert_eq!(buf, make_page_4k(0xCC));
        drop(journal);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn layered_source_write_appends_to_journal_not_file() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let journal = Arc::new(Mutex::new(mgr));

        let file_src = Arc::new(StubFileSource::new());
        let layered = JournalLayeredSource::new(
            Arc::clone(&file_src) as Arc<dyn PageSource>,
            Arc::clone(&journal),
        );

        let payload = make_page_4k(0x5A);
        layered.write_page(13, PageSize::Small4k, &payload).unwrap();

        let journal_bytes = journal.lock().unwrap().read_page(13).unwrap();
        assert_eq!(journal_bytes, Some(payload.clone()));

        let pages = file_src.pages.lock().unwrap();
        assert!(
            !pages.contains_key(&13),
            "write_page must not touch the backing file source"
        );
        drop(pages);
        drop(journal);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn truncate_to_rejects_out_of_range_cursor() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let cur = mgr.write_cursor();

        assert!(mgr.truncate_to(cur + 1).is_err());
        assert!(mgr.truncate_to(0).is_err());
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // T7 — HLC oracle recovery: ChainCommit frames fold into
    // `recovered_max_commit_ts` across reopen.
    // -----------------------------------------------------------------------

    #[test]
    fn recovered_max_commit_ts_none_on_fresh_journal() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert_eq!(mgr.recovered_max_commit_ts(), None);
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn recovered_max_commit_ts_folds_across_reopen() {
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        // Lifetime 1 — append three ChainCommit frames with non-monotonic ts;
        // `open_or_create` in the second lifetime must return the max.
        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_chain_commit(Ts { physical_ms: 50, logical: 0 }, vec![], vec![])
            .unwrap();
        mgr.append_chain_commit(Ts { physical_ms: 150, logical: 0 }, vec![], vec![])
            .unwrap();
        mgr.append_chain_commit(Ts { physical_ms: 100, logical: 7 }, vec![], vec![])
            .unwrap();
        drop(mgr);

        let mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert_eq!(
            mgr2.recovered_max_commit_ts(),
            Some(Ts { physical_ms: 150, logical: 0 }),
            "recovery must fold max(commit_ts) across ChainCommit frames"
        );
        drop(mgr2);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn recovered_max_commit_ts_compares_logical_component() {
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_chain_commit(Ts { physical_ms: 200, logical: 3 }, vec![], vec![])
            .unwrap();
        mgr.append_chain_commit(Ts { physical_ms: 200, logical: 9 }, vec![], vec![])
            .unwrap();
        mgr.append_chain_commit(Ts { physical_ms: 200, logical: 1 }, vec![], vec![])
            .unwrap();
        drop(mgr);

        let mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert_eq!(
            mgr2.recovered_max_commit_ts(),
            Some(Ts { physical_ms: 200, logical: 9 }),
            "tie-breaking on logical component required for HLC recovery"
        );
        drop(mgr2);
        drop(main_file);
        drop(dir);
    }
}

#[cfg(all(test, unix))]
mod crash_recovery_tests {
    //! Crash Recovery Testing — 500 cycles, 10 scenarios.
    //!
    //! Implements Jepsen-style crash injection against the mqlite journal layer.
    //! For each cycle the test:
    //!
    //!   1. Sets up a fresh database directory with pre-committed "epoch-1" data
    //!      in the journal (5 pages, fill byte derived from the cycle seed).
    //!   2. `fork()`s a child process that opens the journal (triggering recovery of
    //!      epoch-1) and then runs a scenario-specific "operation" — writing some
    //!      frames to the journal, or directly to the main file during a simulated
    //!      checkpoint.
    //!   3. The parent SIGKILLs the child at the scenario's injection point.
    //!   4. The parent re-opens the journal (triggering recovery again).
    //!   5. The parent validates all five correctness conditions:
    //!
    //!      (a) Database opens without error after crash.
    //!      (b) Journal replay does not fail (covered by (a) succeeding).
    //!      (c) Committed data is present in the main file.
    //!      (d) Uncommitted data does not appear (no phantom pages in the journal index).
    //!      (e) Index pages are absent when the index build was uncommitted.

    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;

    use crate::error::{Error, Result};
    use crate::storage::header::FileHeader;
    use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};
    use crate::journal::log_file::JournalPageSize;
    use crate::journal::{write_page_to_main, JournalManager};

    const CYCLES_PER_SCENARIO: u32 = 50;
    const EPOCH1_START: u32 = 1;
    const EPOCH1_END: u32 = 6;
    const EPOCH2_START: u32 = 6;
    const EPOCH2_END: u32 = 21;
    const INDEX_START: u32 = 100;
    const INDEX_END: u32 = 110;
    const CHECKPOINT_PAGES: u32 = 20;
    const SALT1: u32 = 0xDEAD_BEEF;
    const SALT2: u32 = 0xCAFE_BABE;

    fn epoch1_fill(seed: u32) -> u8 { ((seed % 200) + 1) as u8 }
    fn epoch2_fill(seed: u32) -> u8 { (((seed + 100) % 200) + 1) as u8 }
    fn uncommitted_fill(seed: u32) -> u8 { (((seed + 50) % 200) + 1) as u8 }
    const CHECKPOINT_GARBAGE_FILL: u8 = 0xDE;

    #[derive(Debug, Clone, Copy)]
    enum Scenario {
        InsertAtFrame0,
        InsertAtFrame10,
        InsertAtFrame100,
        InsertAtFinalFrame,
        CheckpointAt25Pct,
        CheckpointAt50Pct,
        CheckpointAt75Pct,
        IndexBuildAtStart,
        IndexBuildMidway,
        IndexBuildAtEnd,
    }

    const ALL_SCENARIOS: [Scenario; 10] = [
        Scenario::InsertAtFrame0,
        Scenario::InsertAtFrame10,
        Scenario::InsertAtFrame100,
        Scenario::InsertAtFinalFrame,
        Scenario::CheckpointAt25Pct,
        Scenario::CheckpointAt50Pct,
        Scenario::CheckpointAt75Pct,
        Scenario::IndexBuildAtStart,
        Scenario::IndexBuildMidway,
        Scenario::IndexBuildAtEnd,
    ];

    fn setup_epoch1(db_path: &Path, seed: u32) -> Result<()> {
        let mut main_file = OpenOptions::new()
            .read(true).write(true).create(true).open(db_path)
            .map_err(Error::Io)?;
        main_file.set_len(200 * PAGE_SIZE_LEAF as u64).map_err(Error::Io)?;
        let header = FileHeader::new(1_700_000_000_000, SALT1, SALT2);
        main_file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
        main_file.write_all(&header.to_bytes()).map_err(Error::Io)?;
        main_file.flush().map_err(Error::Io)?;
        let mut journal = JournalManager::open_or_create(db_path, &header, &mut main_file)?;
        let page_data = vec![epoch1_fill(seed); PAGE_SIZE_INTERNAL as usize];
        for page_no in EPOCH1_START..(EPOCH1_END - 1) {
            journal.append_non_commit(page_no, JournalPageSize::Small4k, &page_data)?;
        }
        journal.commit(EPOCH1_END - 1, JournalPageSize::Small4k, &page_data, EPOCH1_END - 1)?;
        drop(journal);
        drop(main_file);
        Ok(())
    }

    unsafe fn child_run_scenario(db_path: &Path, scenario: Scenario, seed: u32, write_fd: libc::c_int) {
        macro_rules! step {
            () => {{
                let b: u8 = 1;
                libc::write(write_fd, &b as *const u8 as *const libc::c_void, 1);
            }};
        }
        let mut main_file = match OpenOptions::new().read(true).write(true).open(db_path) {
            Ok(f) => f,
            Err(_) => libc::_exit(2),
        };
        let header = FileHeader::new(1_700_000_000_000, SALT1, SALT2);
        let mut journal = match JournalManager::open_or_create(db_path, &header, &mut main_file) {
            Ok(w) => w,
            Err(_) => libc::_exit(3),
        };
        let uc_fill = uncommitted_fill(seed);
        let e2_fill = epoch2_fill(seed);
        match scenario {
            Scenario::InsertAtFrame0 => {
                step!();
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::InsertAtFrame10 => {
                let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
                for i in 0u32..10 {
                    let _ = journal.append_non_commit(EPOCH2_START + i, JournalPageSize::Small4k, &page_data);
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::InsertAtFrame100 => {
                let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
                let span = EPOCH2_END - EPOCH2_START;
                for i in 0u32..100 {
                    let page_no = EPOCH2_START + (i % span);
                    let _ = journal.append_non_commit(page_no, JournalPageSize::Small4k, &page_data);
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::InsertAtFinalFrame => {
                let page_data = vec![e2_fill; PAGE_SIZE_INTERNAL as usize];
                for i in 0u32..5 {
                    let _ = journal.append_non_commit(EPOCH2_START + i, JournalPageSize::Small4k, &page_data);
                }
                let _ = journal.commit(EPOCH2_START + 5, JournalPageSize::Small4k, &page_data, EPOCH2_START + 5);
                step!();
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::CheckpointAt25Pct | Scenario::CheckpointAt50Pct | Scenario::CheckpointAt75Pct => {
                let epoch2_data = vec![e2_fill; PAGE_SIZE_INTERNAL as usize];
                let e2_span = EPOCH2_END - EPOCH2_START;
                for i in 0..(e2_span - 1) {
                    let _ = journal.append_non_commit(EPOCH2_START + i, JournalPageSize::Small4k, &epoch2_data);
                }
                let _ = journal.commit(EPOCH2_START + e2_span - 1, JournalPageSize::Small4k, &epoch2_data, EPOCH2_START + e2_span - 1);
                let garbage = vec![CHECKPOINT_GARBAGE_FILL; PAGE_SIZE_INTERNAL as usize];
                for page_no in 1..=CHECKPOINT_PAGES {
                    let _ = write_page_to_main(&mut main_file, page_no, PAGE_SIZE_INTERNAL as usize, &garbage);
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::IndexBuildAtStart => {
                step!();
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::IndexBuildMidway => {
                let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
                for i in 0u32..5 {
                    let _ = journal.append_non_commit(INDEX_START + i, JournalPageSize::Small4k, &page_data);
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::IndexBuildAtEnd => {
                let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
                for i in 0u32..(INDEX_END - INDEX_START) {
                    let _ = journal.append_non_commit(INDEX_START + i, JournalPageSize::Small4k, &page_data);
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
        }
        libc::_exit(0);
    }

    fn read_main_page(file: &mut std::fs::File, page_no: u32) -> Result<Vec<u8>> {
        let offset = page_no as u64 * PAGE_SIZE_LEAF as u64;
        file.seek(SeekFrom::Start(offset)).map_err(Error::Io)?;
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        file.read_exact(&mut buf).map_err(Error::Io)?;
        Ok(buf)
    }

    fn validate(journal: &JournalManager, main_file: &mut std::fs::File, scenario: Scenario, seed: u32) -> Result<()> {
        let e1_fill = epoch1_fill(seed);
        let e2_fill = epoch2_fill(seed);

        for page_no in EPOCH1_START..EPOCH1_END {
            let page = read_main_page(main_file, page_no)?;
            if page[0] != e1_fill {
                return Err(Error::Internal(format!(
                    "condition (c) FAIL: epoch-1 page {} fill={:#04x} want={:#04x} [scenario {:?} seed {}]",
                    page_no, page[0], e1_fill, scenario, seed
                )));
            }
        }

        if matches!(scenario, Scenario::InsertAtFinalFrame) {
            for page_no in EPOCH2_START..(EPOCH2_START + 6) {
                let page = read_main_page(main_file, page_no)?;
                if page[0] != e2_fill {
                    return Err(Error::Internal(format!(
                        "condition (c) FAIL: InsertAtFinalFrame page {} fill={:#04x} want={:#04x} [seed {}]",
                        page_no, page[0], e2_fill, seed
                    )));
                }
            }
        }

        if matches!(scenario, Scenario::CheckpointAt25Pct | Scenario::CheckpointAt50Pct | Scenario::CheckpointAt75Pct) {
            for page_no in EPOCH2_START..EPOCH2_END {
                let page = read_main_page(main_file, page_no)?;
                if page[0] != e2_fill {
                    return Err(Error::Internal(format!(
                        "condition (c) FAIL: checkpoint page {} fill={:#04x} want={:#04x} [scenario {:?} seed {}]",
                        page_no, page[0], e2_fill, scenario, seed
                    )));
                }
            }
            for page_no in 1..=CHECKPOINT_PAGES {
                let page = read_main_page(main_file, page_no)?;
                if page[0] == CHECKPOINT_GARBAGE_FILL {
                    return Err(Error::Internal(format!(
                        "condition (d) FAIL: checkpoint garbage fill {:#04x} found at page {} after journal recovery [scenario {:?} seed {}]",
                        CHECKPOINT_GARBAGE_FILL, page_no, scenario, seed
                    )));
                }
            }
        }

        if matches!(scenario, Scenario::InsertAtFrame10) {
            for i in 0u32..10 {
                let page_no = EPOCH2_START + i;
                if journal.index().lookup(page_no).is_some() {
                    return Err(Error::Internal(format!(
                        "condition (d) FAIL: uncommitted page {} in journal index after recovery [InsertAtFrame10 seed {}]",
                        page_no, seed
                    )));
                }
            }
        }

        if matches!(scenario, Scenario::InsertAtFrame100) {
            for page_no in EPOCH2_START..EPOCH2_END {
                if journal.index().lookup(page_no).is_some() {
                    return Err(Error::Internal(format!(
                        "condition (d) FAIL: uncommitted page {} in journal index after recovery [InsertAtFrame100 seed {}]",
                        page_no, seed
                    )));
                }
            }
        }

        if matches!(scenario, Scenario::IndexBuildAtStart | Scenario::IndexBuildMidway | Scenario::IndexBuildAtEnd) {
            for page_no in INDEX_START..INDEX_END {
                if journal.index().lookup(page_no).is_some() {
                    return Err(Error::Internal(format!(
                        "condition (e) FAIL: uncommitted index page {} in journal index after recovery [scenario {:?} seed {}]",
                        page_no, scenario, seed
                    )));
                }
            }
        }

        Ok(())
    }

    fn run_cycle(scenario: Scenario, seed: u32) -> Result<()> {
        let dir = tempfile::tempdir().map_err(Error::Io)?;
        let db_path = dir.path().join("crash.mqlite");
        setup_epoch1(&db_path, seed)?;

        let kill_after: u32 = match scenario {
            Scenario::InsertAtFrame0 => 1,
            Scenario::InsertAtFrame10 => 10,
            Scenario::InsertAtFrame100 => 100,
            Scenario::InsertAtFinalFrame => 1,
            Scenario::CheckpointAt25Pct => (CHECKPOINT_PAGES / 4).max(1),
            Scenario::CheckpointAt50Pct => CHECKPOINT_PAGES / 2,
            Scenario::CheckpointAt75Pct => (CHECKPOINT_PAGES * 3) / 4,
            Scenario::IndexBuildAtStart => 1,
            Scenario::IndexBuildMidway => 5,
            Scenario::IndexBuildAtEnd => INDEX_END - INDEX_START,
        };

        let mut pipe_fds = [0i32; 2];
        assert_eq!(unsafe { libc::pipe(pipe_fds.as_mut_ptr()) }, 0, "pipe() failed");
        let (read_fd, write_fd) = (pipe_fds[0], pipe_fds[1]);

        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork() failed");

        if pid == 0 {
            unsafe { libc::close(read_fd) };
            unsafe { child_run_scenario(&db_path, scenario, seed, write_fd) };
            unsafe { libc::_exit(1) };
        }

        unsafe { libc::close(write_fd) };
        let mut buf = 0u8;
        for signal_idx in 0..kill_after {
            let n = unsafe { libc::read(read_fd, &mut buf as *mut u8 as *mut libc::c_void, 1) };
            if n != 1 {
                unsafe { libc::kill(pid, libc::SIGKILL) };
                unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };
                unsafe { libc::close(read_fd) };
                return Err(Error::Internal(format!(
                    "child exited early: got {signal_idx}/{kill_after} signals [scenario {:?} seed {seed}]",
                    scenario
                )));
            }
        }
        unsafe { libc::close(read_fd) };
        unsafe { libc::kill(pid, libc::SIGKILL) };
        unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };

        let mut main_file = OpenOptions::new().read(true).write(true).open(&db_path)
            .map_err(|e| Error::Internal(format!(
                "condition (a) FAIL: cannot reopen main file after crash [scenario {:?} seed {seed}]: {e}",
                scenario
            )))?;
        let header = FileHeader::new(1_700_000_000_000, SALT1, SALT2);
        let journal = JournalManager::open_or_create(&db_path, &header, &mut main_file)
            .map_err(|e| Error::Internal(format!(
                "condition (a)+(b) FAIL: JournalManager::open_or_create failed after crash [scenario {:?} seed {seed}]: {e}",
                scenario
            )))?;
        validate(&journal, &mut main_file, scenario, seed)?;
        Ok(())
    }

    #[test]
    fn crash_recovery_500_cycles() {
        let mut failures: Vec<String> = Vec::new();
        let mut total: u32 = 0;
        for scenario in &ALL_SCENARIOS {
            for cycle in 0..CYCLES_PER_SCENARIO {
                total += 1;
                let seed = cycle;
                if let Err(e) = run_cycle(*scenario, seed) {
                    failures.push(format!(
                        "  [cycle {total}/500 | scenario {:?} | seed {seed}] {e}",
                        scenario
                    ));
                }
            }
        }
        if !failures.is_empty() {
            panic!(
                "CRASH RECOVERY FAILURES — {}/{} cycles failed:\n{}\n\
                 Hint: re-run with `RUST_BACKTRACE=1 cargo test crash_recovery` to reproduce.",
                failures.len(), total, failures.join("\n")
            );
        }
    }
}
