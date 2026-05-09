// Test code idiomatically uses .unwrap() / .expect() / panic! to fail
// loudly on setup-time errors. The five lints below are project-wide
// `warn` lints that become errors under `-D warnings`; the standard Rust
// pattern is to allow them only at the test-module scope. All other
// clippy warnings are fixed in the source, not suppressed.
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
mod tests {
    use super::super::*;
    use crate::journal::log_file::{PositionedLogFile, PositionedLogIo};
    use crate::storage::header::FileHeader;
    use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};
    use std::io::{Read, Seek, SeekFrom};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Mutex as StdMutex};
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
            .truncate(true)
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

    fn make_log_manager(initial_lsn: u64) -> (TempDir, LogManager) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("phase8-log.mqlite-log");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .unwrap();
        (dir, LogManager::new(file, initial_lsn))
    }

    #[derive(Clone)]
    struct TestPositionedLogIo {
        inner: Arc<TestPositionedLogIoInner>,
    }

    struct TestPositionedLogIoInner {
        data: StdMutex<Vec<u8>>,
        max_write: usize,
        fail_offset: Option<u64>,
        write_calls: AtomicUsize,
        sync_calls: AtomicUsize,
    }

    impl TestPositionedLogIo {
        fn new(max_write: usize, fail_offset: Option<u64>) -> Self {
            Self {
                inner: Arc::new(TestPositionedLogIoInner {
                    data: StdMutex::new(Vec::new()),
                    max_write,
                    fail_offset,
                    write_calls: AtomicUsize::new(0),
                    sync_calls: AtomicUsize::new(0),
                }),
            }
        }

        fn snapshot(&self) -> Vec<u8> {
            self.inner.data.lock().unwrap().clone()
        }

        fn write_calls(&self) -> usize {
            self.inner.write_calls.load(Ordering::Acquire)
        }

        fn sync_calls(&self) -> usize {
            self.inner.sync_calls.load(Ordering::Acquire)
        }
    }

    impl PositionedLogIo for TestPositionedLogIo {
        fn write_at(&self, offset: u64, data: &[u8]) -> std::io::Result<usize> {
            self.inner.write_calls.fetch_add(1, Ordering::AcqRel);
            if self.inner.fail_offset == Some(offset) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "injected positioned write failure",
                ));
            }
            let written = data.len().min(self.inner.max_write);
            if written == 0 {
                return Ok(0);
            }

            let mut buf = self.inner.data.lock().unwrap();
            let start = offset as usize;
            let end = start + written;
            if end > buf.len() {
                buf.resize(end, 0);
            }
            buf[start..end].copy_from_slice(&data[..written]);
            Ok(written)
        }

        fn sync_data(&self) -> std::io::Result<()> {
            self.inner.sync_calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    fn log_manager_from_io(io: TestPositionedLogIo, initial_lsn: u64) -> Arc<LogManager> {
        Arc::new(LogManager::from_positioned_file(
            PositionedLogFile::from_io(Box::new(io)),
            initial_lsn,
        ))
    }

    fn append_test_page0_boundary(
        mgr: &mut JournalManager,
        base_header: &FileHeader,
        checkpoint_ts: crate::mvcc::timestamp::Ts,
    ) -> BoundaryAppended {
        let cursor = mgr.begin_checkpoint_batch().unwrap();
        let mut staged_header = base_header.clone();
        staged_header.last_checkpoint_ts = checkpoint_ts;
        mgr.append_checkpoint_commit_boundary(&staged_header, cursor)
            .unwrap()
    }

    fn append_test_checkpoint_batch(
        mgr: &mut JournalManager,
        base_header: &FileHeader,
        pages: impl IntoIterator<Item = u32>,
        fill: u8,
        checkpoint_ts: crate::mvcc::timestamp::Ts,
    ) -> BoundaryAppended {
        let pages: Vec<u32> = pages.into_iter().collect();
        let cursor = mgr.begin_checkpoint_batch().unwrap();
        let batch_id = cursor.batch_id();
        let page_data = make_page_4k(fill);
        for page_number in &pages {
            mgr.append_checkpoint_frame(
                batch_id,
                CheckpointPoolKind::Main,
                *page_number,
                JournalPageSize::Small4k,
                &page_data,
            )
            .unwrap();
        }
        let mut staged_header = base_header.clone();
        if let Some(max_page) = pages.iter().copied().max() {
            staged_header.total_page_count = staged_header.total_page_count.max(max_page + 1);
        }
        staged_header.last_checkpoint_ts = checkpoint_ts;
        mgr.append_checkpoint_commit_boundary(&staged_header, cursor)
            .unwrap()
    }

    // -----------------------------------------------------------------------
    // Phase 8 LogManager slot reservation
    // -----------------------------------------------------------------------

    #[test]
    fn log_manager_concurrent_reservations_are_disjoint_monotonic() {
        let (_dir, manager) = make_log_manager(100);
        let manager = Arc::new(manager);
        let workers = 8usize;
        let barrier = Arc::new(Barrier::new(workers));
        let mut handles = Vec::new();

        for len in 1..=workers {
            let manager = Arc::clone(&manager);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                manager.reserve(len).unwrap()
            }));
        }

        let mut slots: Vec<LogSlot> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        slots.sort_by_key(LogSlot::start_lsn);

        assert_eq!(slots[0].start_lsn(), 100);
        for pair in slots.windows(2) {
            assert_eq!(
                pair[0].end_lsn(),
                pair[1].start_lsn(),
                "reservations must be contiguous and non-overlapping"
            );
        }
        let total_reserved: u64 = slots.iter().map(|slot| slot.bytes_len() as u64).sum();
        assert_eq!(slots.last().unwrap().end_lsn(), 100 + total_reserved);
    }

    #[test]
    fn log_manager_reserve_does_not_mutate_record_metadata() {
        use crate::journal::log_file::{LogRecord, LogRecordDraft};
        use crate::mvcc::timestamp::Ts;

        let (_dir, manager) = make_log_manager(4096);
        let commit_ts = Ts {
            physical_ms: 1_700_000_000_123,
            logical: 7,
        };
        let draft = LogRecordDraft::crud(99, 42, commit_ts, b"logical".to_vec(), b"chain".to_vec());
        let slot = manager.reserve(draft.encoded_len().unwrap()).unwrap();
        let record = draft.finalize(slot.start_lsn()).unwrap();
        let decoded = LogRecord::decode(record.bytes()).unwrap();

        assert_eq!(decoded.start_lsn, slot.start_lsn());
        assert_eq!(decoded.end_lsn, slot.end_lsn());
        assert_eq!(decoded.txn_id, 99);
        assert_eq!(decoded.publish_seq, 42);
        assert_eq!(decoded.commit_ts, commit_ts);
    }

    #[test]
    fn log_manager_ready_lsn_advances_only_contiguous_slots() {
        let io = TestPositionedLogIo::new(usize::MAX, None);
        let manager = log_manager_from_io(io, 0);
        let first = manager.reserve(4).unwrap();
        let second = manager.reserve(6).unwrap();

        manager.write_reserved(&second, b"second").unwrap();
        let receipt = manager.mark_written(&second).unwrap();
        assert_eq!(receipt.ready_lsn(), 0);
        assert_eq!(manager.ready_lsn(), 0);

        manager.write_reserved(&first, b"frst").unwrap();
        let receipt = manager.mark_written(&first).unwrap();
        assert_eq!(receipt.end_lsn(), first.end_lsn());
        assert_eq!(receipt.ready_lsn(), second.end_lsn());
        assert_eq!(manager.ready_lsn(), second.end_lsn());
    }

    #[test]
    fn log_manager_mark_before_write_poisons_gap_and_waiters() {
        let io = TestPositionedLogIo::new(usize::MAX, None);
        let manager = log_manager_from_io(io, 0);
        let first = manager.reserve(4).unwrap();
        let second = manager.reserve(6).unwrap();

        manager.write_reserved(&second, b"second").unwrap();
        manager.mark_written(&second).unwrap();
        assert_eq!(manager.ready_lsn(), 0);

        let waiter_manager = Arc::clone(&manager);
        let waiter = std::thread::spawn(move || waiter_manager.wait_ready(second.end_lsn()));

        let err = manager
            .mark_written(&first)
            .expect_err("marking an unwritten reservation must poison the gap");
        assert!(matches!(
            err,
            Error::EngineFatal {
                reason: EngineFatalReason::PostReservationLogWriteFailure
            }
        ));
        assert_eq!(manager.ready_lsn(), 0);

        let waiter_err = waiter.join().unwrap().expect_err("waiter must wake fatal");
        assert!(matches!(
            waiter_err,
            Error::EngineFatal {
                reason: EngineFatalReason::PostReservationLogWriteFailure
            }
        ));
    }

    #[test]
    fn log_manager_short_positioned_writes_retry_before_mark() {
        let io = TestPositionedLogIo::new(2, None);
        let manager = log_manager_from_io(io.clone(), 0);
        let slot = manager.reserve(5).unwrap();

        manager.write_reserved(&slot, b"abcde").unwrap();
        assert_eq!(
            manager.ready_lsn(),
            0,
            "write_reserved must not mark the slot ready"
        );
        assert!(
            io.write_calls() >= 3,
            "short positioned writes must be retried"
        );

        let receipt = manager.mark_written(&slot).unwrap();
        assert_eq!(receipt.ready_lsn(), slot.end_lsn());
        assert_eq!(
            &io.snapshot()[slot.start_lsn() as usize..slot.end_lsn() as usize],
            b"abcde"
        );
    }

    #[test]
    fn log_manager_write_failure_poisons_gap_and_waiters() {
        let io = TestPositionedLogIo::new(usize::MAX, Some(0));
        let manager = log_manager_from_io(io.clone(), 0);
        let first = manager.reserve(4).unwrap();
        let second = manager.reserve(5).unwrap();

        manager.write_reserved(&second, b"valid").unwrap();
        manager.mark_written(&second).unwrap();
        assert_eq!(manager.ready_lsn(), 0);

        let waiter_manager = Arc::clone(&manager);
        let waiter = std::thread::spawn(move || waiter_manager.wait_ready(second.end_lsn()));

        let err = manager
            .write_reserved(&first, b"fail")
            .expect_err("failed positioned write must poison the log manager");
        assert!(matches!(
            err,
            Error::EngineFatal {
                reason: EngineFatalReason::PostReservationLogWriteFailure
            }
        ));
        assert_eq!(manager.ready_lsn(), 0);

        let waiter_err = waiter.join().unwrap().expect_err("waiter must wake fatal");
        assert!(matches!(
            waiter_err,
            Error::EngineFatal {
                reason: EngineFatalReason::PostReservationLogWriteFailure
            }
        ));

        let snapshot = io.snapshot();
        assert_eq!(
            &snapshot[second.start_lsn() as usize..second.end_lsn() as usize],
            b"valid",
            "later valid bytes remain in place but cannot hide the poisoned gap"
        );
    }

    #[test]
    fn log_manager_partial_failure_preserves_neighbor_records() {
        let io = TestPositionedLogIo::new(2, Some(6));
        let manager = log_manager_from_io(io.clone(), 0);
        let first = manager.reserve(4).unwrap();
        let failed = manager.reserve(6).unwrap();
        let later = manager.reserve(5).unwrap();

        manager.write_reserved(&first, b"good").unwrap();
        manager.mark_written(&first).unwrap();
        assert_eq!(manager.ready_lsn(), first.end_lsn());

        manager.write_reserved(&later, b"later").unwrap();
        manager.mark_written(&later).unwrap();
        assert_eq!(
            manager.ready_lsn(),
            first.end_lsn(),
            "later record cannot advance ready_lsn across the unwritten gap"
        );

        let err = manager
            .write_reserved(&failed, b"broken")
            .expect_err("partial positioned write failure must poison the log manager");
        assert!(matches!(
            err,
            Error::EngineFatal {
                reason: EngineFatalReason::PostReservationLogWriteFailure
            }
        ));
        assert_eq!(manager.ready_lsn(), first.end_lsn());

        let snapshot = io.snapshot();
        assert_eq!(
            &snapshot[first.start_lsn() as usize..first.end_lsn() as usize],
            b"good",
            "partial failed writer must not overwrite the preceding ready record"
        );
        assert_eq!(
            &snapshot[later.start_lsn() as usize..later.end_lsn() as usize],
            b"later",
            "partial failed writer must not hide the later valid record bytes"
        );
        assert_eq!(
            &snapshot[failed.start_lsn() as usize..failed.start_lsn() as usize + 2],
            b"br",
            "test setup must cover a partial post-reservation write before failure"
        );
    }

    #[test]
    fn poisoned_log_manager_blocks_truncate_rollback() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let rollback_mark = mgr.write_cursor();
        let first = mgr.log_manager.reserve(4).unwrap();
        let second = mgr.log_manager.reserve(5).unwrap();

        mgr.log_manager.write_reserved(&second, b"valid").unwrap();
        mgr.log_manager.mark_written(&second).unwrap();
        let err = Error::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "injected post-reservation failure",
        ));
        let err = mgr.log_manager.poison_slot(&first, err);
        assert!(matches!(
            err,
            Error::EngineFatal {
                reason: EngineFatalReason::PostReservationLogWriteFailure
            }
        ));

        let truncate_err = mgr
            .truncate_to(rollback_mark)
            .expect_err("poisoned log manager must reject rollback truncation");
        assert!(matches!(
            truncate_err,
            Error::EngineFatal {
                reason: EngineFatalReason::PostReservationLogWriteFailure
            }
        ));
        assert!(
            mgr.journal_file.metadata().unwrap().len() >= second.end_lsn(),
            "rollback must not truncate away another writer's valid reserved bytes"
        );
    }

    #[test]
    fn log_manager_wait_durable_syncs_ready_prefix() {
        let io = TestPositionedLogIo::new(usize::MAX, None);
        let manager = log_manager_from_io(io.clone(), 0);
        let slot = manager.reserve(3).unwrap();
        manager.write_reserved(&slot, b"abc").unwrap();
        manager.mark_written(&slot).unwrap();

        manager.wait_durable(slot.end_lsn()).unwrap();

        assert_eq!(manager.ready_lsn(), slot.end_lsn());
        assert_eq!(manager.durable_lsn(), slot.end_lsn());
        assert_eq!(io.sync_calls(), 1);
    }

    #[test]
    fn log_manager_durable_lsn_stays_at_closed_sync_target() {
        crate::storage::paged_engine::group_commit_observations::reset();
        let io = TestPositionedLogIo::new(usize::MAX, None);
        let manager = log_manager_from_io(io.clone(), 0);
        let first = manager.reserve(3).unwrap();
        let second = manager.reserve(3).unwrap();

        manager.write_reserved(&first, b"abc").unwrap();
        manager.mark_written(&first).unwrap();
        manager.write_reserved(&second, b"def").unwrap();

        let mut pause =
            crate::storage::paged_engine::group_commit_observations::install_pause_after_close_for(
                manager.probe_id(),
            );
        let sync_manager = Arc::clone(&manager);
        let sync_target = first.end_lsn();
        let leader = std::thread::spawn(move || sync_manager.ensure_sync(sync_target));
        pause
            .wait_until_paused_timeout(std::time::Duration::from_secs(5))
            .expect("leader paused after closing sync target");

        manager.mark_written(&second).unwrap();
        assert_eq!(manager.ready_lsn(), second.end_lsn());

        pause.release().unwrap();
        leader.join().unwrap().unwrap();

        assert_eq!(manager.ready_lsn(), second.end_lsn());
        assert_eq!(
            manager.durable_lsn(),
            first.end_lsn(),
            "leader must not advance durability beyond its closed target"
        );
        assert!(manager.durable_lsn() <= manager.ready_lsn());
        assert_eq!(io.sync_calls(), 1);
        crate::storage::paged_engine::group_commit_observations::reset();
    }

    #[test]
    fn sync_journal_advances_log_manager_durable_lsn() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let durable_before = mgr.log_manager.durable_lsn();
        let commit_ts = crate::mvcc::timestamp::Ts {
            physical_ms: 1_700_000_000_321,
            logical: 2,
        };

        let frame_offset = mgr.append_chain_commit(commit_ts, vec![], vec![]).unwrap();
        let write_cursor = mgr.write_cursor();

        assert_eq!(frame_offset, durable_before);
        assert_eq!(mgr.log_manager.ready_lsn(), write_cursor);
        assert_eq!(
            mgr.log_manager.durable_lsn(),
            durable_before,
            "append must not claim durability before the sync boundary"
        );

        mgr.sync_journal().unwrap();

        assert_eq!(mgr.log_manager.durable_lsn(), write_cursor);
    }

    #[test]
    fn commit_append_uses_log_manager_not_seek_write_all() {
        let source = include_str!("../mod.rs");
        let chain_body = source
            .split("fn append_chain_commit_record")
            .nth(1)
            .unwrap()
            .split("/// Append a `LogicalTxnFrame`")
            .next()
            .unwrap();
        let logical_body = source
            .split("pub(crate) fn append_logical_txn")
            .nth(1)
            .unwrap()
            .split("/// Append a page-0 checkpoint commit boundary")
            .next()
            .unwrap();

        for body in [chain_body, logical_body] {
            assert!(body.contains("self.log_manager.reserve(bytes.len())"));
            assert!(body.contains("self.log_manager.write_reserved(&slot, &bytes)"));
            assert!(body.contains("self.log_manager.mark_written(&slot)"));
            assert!(!body.contains(".seek("));
            assert!(!body.contains(".write_all("));
        }
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

        let mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

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

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
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

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

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

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

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

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

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
    // append_logical_txn (§6.4)
    // -----------------------------------------------------------------------

    #[test]
    fn append_logical_txn_advances_cursor_and_is_durable() {
        use crate::journal::log_file::{LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION};
        use crate::mvcc::timestamp::Ts;

        let (_dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let header = make_header();

        let cursor_before_logical;
        let logical_frame_offset;
        let encoded_logical;
        {
            let mut mgr =
                JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

            cursor_before_logical = mgr.write_cursor();
            let index_occupied_before = mgr.index().occupied_count();

            let frame = LogicalTxnFrame {
                salt1: header.wal_salt1,
                salt2: header.wal_salt2,
                commit_ts: Ts {
                    physical_ms: 1_700_000_001_000,
                    logical: 3,
                },
                diagnostic_txn_id: 0xA1B2_C3D4_E5F6_0708,
                format_version: LOGICAL_TXN_FORMAT_VERSION,
                flags: 0,
                ops: vec![],
            };
            encoded_logical = frame.encode().unwrap();

            logical_frame_offset = mgr.append_logical_txn(frame).unwrap();

            assert_eq!(
                logical_frame_offset, cursor_before_logical,
                "returned offset must equal the pre-append write cursor"
            );
            assert_eq!(
                mgr.write_cursor(),
                cursor_before_logical + encoded_logical.len() as u64,
                "write cursor must advance by exactly encoded frame length"
            );
            assert_eq!(
                mgr.index().occupied_count(),
                index_occupied_before,
                "logical frame must not touch the in-memory JournalIndex"
            );

            // Round-trip the on-disk bytes via a fresh read-only handle so
            // the check bypasses any in-manager positioning state.
            let mut verify_file = OpenOptions::new()
                .read(true)
                .open(journal_path_for(&db_path))
                .unwrap();
            verify_file
                .seek(SeekFrom::Start(logical_frame_offset))
                .unwrap();
            let mut round_trip = vec![0u8; encoded_logical.len()];
            verify_file.read_exact(&mut round_trip).unwrap();
            assert_eq!(
                round_trip, encoded_logical,
                "logical frame bytes must be durable on disk at the returned offset"
            );

            // §8.2 / codex US-020 r2 blocker AC#2 — DO NOT call
            // `close_and_cleanup` here: that runs a checkpoint and
            // clears the journal, which would defeat the post-recovery
            // assertion below. The point of "advances cursor and is
            // durable" is that the LogicalTxn bytes survive a reopen
            // through `recover_existing`. Drop `mgr` to release file
            // handles; the journal file is preserved on disk.
            drop(mgr);
        }

        let mut main_file2 = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mut mgr2 = JournalManager::open_or_create(&db_path, &header, &mut main_file2).unwrap();

        assert!(
            mgr2.read_page_linear(99).unwrap().is_none(),
            "read_page_linear on an unrelated page must return None"
        );

        // §8.2 AC#2 — assert the LogicalTxn frame bytes survived the
        // reopen-via-recovery cycle by reading them back from the
        // journal file at the original offset and comparing to the
        // pre-reopen encoded bytes. This is the durability proof:
        // the LogicalTxn bytes are physically preserved on disk
        // across the recovery scan.
        //
        // Note: this LogicalTxn has no paired ChainCommit, so the
        // §3.8(b) orphan-logical sweep drops it from
        // `ParsedLogicalFrames`. That's expected; the durability
        // assertion is on the on-disk bytes themselves.
        let mut verify_file_post = OpenOptions::new()
            .read(true)
            .open(journal_path_for(&db_path))
            .unwrap();
        verify_file_post
            .seek(SeekFrom::Start(logical_frame_offset))
            .unwrap();
        let mut round_trip_post = vec![0u8; encoded_logical.len()];
        verify_file_post.read_exact(&mut round_trip_post).unwrap();
        assert_eq!(
            round_trip_post, encoded_logical,
            "post-recovery: LogicalTxnFrame bytes must still be on \
             disk at the original offset (durability across reopen)"
        );
        // Recovery also re-positioned the write_cursor past the
        // logical frame so subsequent appends would land after it.
        assert!(
            mgr2.write_cursor() >= logical_frame_offset + encoded_logical.len() as u64,
            "post-recovery write_cursor must be at or past the LogicalTxn end"
        );
    }

    #[test]
    fn append_logical_txn_rejects_oversize_without_writing() {
        use crate::error::Error;
        use crate::journal::log_file::{
            LogicalOp, LogicalOpKind, LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION,
            LOGICAL_TXN_MAX_FRAME_SIZE,
        };
        use crate::mvcc::timestamp::Ts;

        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let cursor_before = mgr.write_cursor();
        let index_before = mgr.index().occupied_count();

        // A single op whose value payload alone exceeds the frame cap forces
        // the encoder's oversize guard to trip before any I/O.
        let oversize_frame = LogicalTxnFrame {
            salt1: header.wal_salt1,
            salt2: header.wal_salt2,
            commit_ts: Ts {
                physical_ms: 1,
                logical: 0,
            },
            diagnostic_txn_id: 0,
            format_version: LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::PrimaryInsert {
                    ns_id: 1,
                    key: vec![0u8; 8],
                    value: vec![0u8; LOGICAL_TXN_MAX_FRAME_SIZE + 1],
                    overflow: None,
                },
            }],
        };

        let err = mgr
            .append_logical_txn(oversize_frame)
            .expect_err("oversize frame must return Err");
        assert!(
            matches!(err, Error::JournalFrameTooLarge { .. }),
            "expected JournalFrameTooLarge, got {err:?}"
        );

        assert_eq!(mgr.write_cursor(), cursor_before);
        assert_eq!(mgr.index().occupied_count(), index_before);
    }

    #[test]
    fn logical_txn_encode_rejects_oversize_inline_fields() {
        use crate::error::Error;
        use crate::journal::log_file::{
            LogicalOp, LogicalOpKind, LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION,
            LOGICAL_TXN_MAX_KEY_BYTES,
        };
        use crate::mvcc::timestamp::Ts;

        let frame = LogicalTxnFrame {
            salt1: 1,
            salt2: 2,
            commit_ts: Ts {
                physical_ms: 1,
                logical: 0,
            },
            diagnostic_txn_id: 0,
            format_version: LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::PrimaryDelete {
                    ns_id: 1,
                    key: vec![0u8; LOGICAL_TXN_MAX_KEY_BYTES + 1],
                },
            }],
        };

        let err = frame
            .encode()
            .expect_err("oversize key must be rejected before encoding");
        assert!(
            matches!(err, Error::JournalFrameTooLarge { .. }),
            "expected JournalFrameTooLarge, got {err:?}"
        );
    }

    #[test]
    fn chain_commit_decode_bounds_page_write_count_before_allocation() {
        use crate::journal::log_file::ChainCommitFrame;
        use crate::mvcc::timestamp::Ts;

        let frame = ChainCommitFrame {
            salt1: 1,
            salt2: 2,
            commit_ts: Ts {
                physical_ms: 1,
                logical: 0,
            },
            refcount_deltas: vec![],
            page_writes: vec![],
        };
        let mut bytes = frame.encode().unwrap();
        bytes[32..36].copy_from_slice(&u32::MAX.to_le_bytes());
        let checksum_at = bytes.len() - 4;
        let checksum = crc32c::crc32c(&bytes[..checksum_at]);
        bytes[checksum_at..].copy_from_slice(&checksum.to_le_bytes());

        let decoded = ChainCommitFrame::decode(&bytes, 1, 2).unwrap();
        assert!(
            decoded.is_none(),
            "untrusted page_write_count must be rejected before allocation"
        );
    }

    // -----------------------------------------------------------------------
    // Commit
    // -----------------------------------------------------------------------

    #[test]
    fn commit_frame_marks_transaction_boundary() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

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
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

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
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
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
    fn recovery_copies_checkpoint_batch_after_boundary() {
        use crate::mvcc::timestamp::Ts;

        let (_dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let header = make_header();

        // Write checkpoint-owned pages and commit them with a page-0 boundary.
        {
            let mut mgr =
                JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

            let _ = append_test_checkpoint_batch(
                &mut mgr,
                &header,
                [1, 2],
                0xAA,
                Ts {
                    physical_ms: 100,
                    logical: 0,
                },
            );
            // Simulate crash: don't call close_and_cleanup.
            // Journal file left on disk.
        }

        // Reopen — recovery runs automatically.
        let mut main_file2 = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let _mgr2 = JournalManager::open_or_create(&db_path, &header, &mut main_file2).unwrap();

        // Both checkpoint pages should have been copied into main file.
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file2
            .seek(SeekFrom::Start(PAGE_SIZE_LEAF as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0xAA, "page 1 should be copied");

        main_file2
            .seek(SeekFrom::Start(2 * PAGE_SIZE_LEAF as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0xAA, "page 2 should be copied");
    }

    #[test]
    fn recovery_discards_checkpoint_batch_without_boundary() {
        let (_dir, db_path, mut main_file) = make_db_file();
        main_file.set_len(100 * PAGE_SIZE_INTERNAL as u64).unwrap();
        let header = make_header();

        // Write a checkpoint-owned page without its page-0 boundary.
        {
            let mut mgr =
                JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

            let cursor = mgr.begin_checkpoint_batch().unwrap();
            mgr.append_checkpoint_frame(
                cursor.batch_id(),
                CheckpointPoolKind::Main,
                2,
                JournalPageSize::Small4k,
                &make_page_4k(0xDD),
            )
            .unwrap();
            // Crash: no boundary for page 2.
        }

        let mut main_file2 = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mut mgr2 = JournalManager::open_or_create(&db_path, &header, &mut main_file2).unwrap();

        // Page 2 should not be copied into the main file.
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file2
            .seek(SeekFrom::Start(2 * PAGE_SIZE_LEAF as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0x00, "incomplete checkpoint page is discarded");

        assert!(
            mgr2.read_page_linear(2).unwrap().is_none(),
            "incomplete checkpoint page must not remain readable from journal"
        );
    }

    #[test]
    fn stale_journal_is_deleted_on_open() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        // Create journal with original salts.
        {
            let _mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        }
        assert!(journal_path_for(&db_path).exists());

        // Reopen with different salts (simulates a different database open).
        let different_header = FileHeader::new(1_700_000_000_001, 0x1111_1111, 0x2222_2222);
        let mgr2 =
            JournalManager::open_or_create(&db_path, &different_header, &mut main_file).unwrap();
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

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
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
    fn linear_scan_ignores_untagged_page_frames() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let page_data = make_page_4k(0x77);
        mgr.append_non_commit(7, JournalPageSize::Small4k, &page_data)
            .unwrap();

        let result = mgr.read_page_linear(7).unwrap();
        assert_eq!(result, None);
        assert!(mgr.read_page_linear(999).unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // Rollback (truncate_to)
    // -----------------------------------------------------------------------

    #[test]
    fn truncate_to_does_not_rebuild_untagged_page_frames() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_non_commit(1, JournalPageSize::Small4k, &make_page_4k(0x11))
            .unwrap();
        let mark = mgr.write_cursor();
        mgr.append_non_commit(2, JournalPageSize::Small4k, &make_page_4k(0x22))
            .unwrap();
        mgr.append_non_commit(3, JournalPageSize::Small4k, &make_page_4k(0x33))
            .unwrap();

        mgr.truncate_to(mark).unwrap();

        assert_eq!(mgr.write_cursor(), mark);
        assert!(mgr.read_page(1).unwrap().is_none());
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
    fn truncate_to_does_not_rebuild_checkpoint_boundary_state() {
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let _ = append_test_page0_boundary(
            &mut mgr,
            &header,
            Ts {
                physical_ms: 100,
                logical: 0,
            },
        );
        let mark = mgr.write_cursor();
        let cursor = mgr.begin_checkpoint_batch().unwrap();
        mgr.append_checkpoint_frame(
            cursor.batch_id(),
            CheckpointPoolKind::Main,
            55,
            JournalPageSize::Small4k,
            &make_page_4k(0x55),
        )
        .unwrap();

        mgr.truncate_to(mark).unwrap();

        assert!(mgr.last_committed_db_page_count.is_none());
        assert!(mgr.read_page(0).unwrap().is_none());
        assert!(mgr.read_page(55).unwrap().is_none());
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn truncate_to_full_drops_all_non_header_frames() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
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
        let mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
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
        assert_eq!(
            buf,
            make_page_32k(0xB1),
            "journal version must win over file"
        );
        drop(journal);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn layered_source_read_falls_back_to_file() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
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
    fn layered_source_write_passes_through_to_file_not_journal() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let journal = Arc::new(Mutex::new(mgr));

        let file_src = Arc::new(StubFileSource::new());
        let layered = JournalLayeredSource::new(
            Arc::clone(&file_src) as Arc<dyn PageSource>,
            Arc::clone(&journal),
        );

        let payload = make_page_4k(0x5A);
        layered.write_page(13, PageSize::Small4k, &payload).unwrap();

        let journal_bytes = journal.lock().unwrap().read_page(13).unwrap();
        assert_eq!(journal_bytes, None);

        let pages = file_src.pages.lock().unwrap();
        assert_eq!(pages.get(&13), Some(&payload));
        drop(pages);
        drop(journal);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn truncate_to_rejects_out_of_range_cursor() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let cur = mgr.write_cursor();

        assert!(mgr.truncate_to(cur + 1).is_err());
        assert!(mgr.truncate_to(0).is_err());
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // HLC oracle recovery: ChainCommit frames fold into
    // `recovered_max_commit_ts` across reopen.
    // -----------------------------------------------------------------------

    #[test]
    fn recovered_max_commit_ts_none_on_fresh_journal() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
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
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_chain_commit(
            Ts {
                physical_ms: 50,
                logical: 0,
            },
            vec![],
            vec![],
        )
        .unwrap();
        mgr.append_chain_commit(
            Ts {
                physical_ms: 150,
                logical: 0,
            },
            vec![],
            vec![],
        )
        .unwrap();
        mgr.append_chain_commit(
            Ts {
                physical_ms: 100,
                logical: 7,
            },
            vec![],
            vec![],
        )
        .unwrap();
        drop(mgr);

        let mgr2 = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert_eq!(
            mgr2.recovered_max_commit_ts(),
            Some(Ts {
                physical_ms: 150,
                logical: 0
            }),
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

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.append_chain_commit(
            Ts {
                physical_ms: 200,
                logical: 3,
            },
            vec![],
            vec![],
        )
        .unwrap();
        mgr.append_chain_commit(
            Ts {
                physical_ms: 200,
                logical: 9,
            },
            vec![],
            vec![],
        )
        .unwrap();
        mgr.append_chain_commit(
            Ts {
                physical_ms: 200,
                logical: 1,
            },
            vec![],
            vec![],
        )
        .unwrap();
        drop(mgr);

        let mgr2 = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert_eq!(
            mgr2.recovered_max_commit_ts(),
            Some(Ts {
                physical_ms: 200,
                logical: 9
            }),
            "tie-breaking on logical component required for HLC recovery"
        );
        drop(mgr2);
        drop(main_file);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // Phase 2 US-012 / US-013 — ParsedLogicalFrames + take accessor
    // -----------------------------------------------------------------------

    /// `take_parsed_logical_frames` is take-once: first call returns the
    /// populated vec, second call returns an empty struct.
    #[test]
    fn take_parsed_logical_frames_returns_once() {
        use crate::journal::log_file::{LogicalOp, LogicalOpKind, LogicalTxnFrame};
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        // Synthesize one ParsedLogicalFrames entry by appending a logical
        // frame and re-reading via a forced truncate_to rebuild. Simpler:
        // populate directly through the pub(super) field since this is a
        // unit test in the journal module.
        let frame = LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: Ts {
                physical_ms: 42,
                logical: 1,
            },
            diagnostic_txn_id: 7,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::PrimaryInsert {
                    ns_id: 10,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    overflow: None,
                },
            }],
        };
        mgr.parsed_logical_frames
            .frames
            .push((crate::journal::log_file::JOURNAL_HEADER_SIZE as u64, frame));
        mgr.parsed_logical_frames.seen_commit_ts.insert(Ts {
            physical_ms: 42,
            logical: 1,
        });

        let first = mgr.take_parsed_logical_frames();
        assert_eq!(first.frames.len(), 1);
        assert_eq!(first.seen_commit_ts.len(), 1);

        let second = mgr.take_parsed_logical_frames();
        assert!(second.frames.is_empty());
        assert!(second.seen_commit_ts.is_empty());

        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn pass1_collects_logical_frames_into_parsed_struct() {
        use crate::journal::log_file::LogicalTxnFrame;
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        // Append two logical frames with distinct commit_ts, each preceded
        // by a ChainCommit so the Pass 1 walk sees the HLC floor advance
        // and the logical-frame dedup behave as documented.
        let frame_a = LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: Ts {
                physical_ms: 100,
                logical: 0,
            },
            diagnostic_txn_id: 1,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        };
        let frame_b = LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: Ts {
                physical_ms: 200,
                logical: 0,
            },
            diagnostic_txn_id: 2,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        };
        let off_a = mgr.append_logical_txn(frame_a.clone()).unwrap();
        mgr.append_chain_commit(
            Ts {
                physical_ms: 100,
                logical: 0,
            },
            vec![],
            vec![],
        )
        .unwrap();
        let off_b = mgr.append_logical_txn(frame_b.clone()).unwrap();
        mgr.append_chain_commit(
            Ts {
                physical_ms: 200,
                logical: 0,
            },
            vec![],
            vec![],
        )
        .unwrap();

        // Leak the journal file so the reopen sees the raw bytes; drop the
        // manager without running a checkpoint.
        std::mem::forget(mgr);
        drop(main_file);

        let mut main_file_reopen = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mut mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file_reopen).unwrap();
        let parsed = mgr2.take_parsed_logical_frames();
        // Per US-012 AC#6: assert the FULL (offset, LogicalTxnFrame) tuple
        // equality, in offset order — not just commit_ts / diagnostic_txn_id.
        let expected_frames = vec![(off_a, frame_a.clone()), (off_b, frame_b.clone())];
        assert_eq!(
            parsed.frames, expected_frames,
            "Pass 1 must collect (offset, LogicalTxnFrame) tuples in offset order"
        );
        assert!(parsed.seen_commit_ts.contains(&frame_a.commit_ts));
        assert!(parsed.seen_commit_ts.contains(&frame_b.commit_ts));

        drop(mgr2);
        drop(main_file_reopen);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // Phase 2 US-014 — HLC floor isolation (§3.10) and orphan sweep (§3.8(b))
    // -----------------------------------------------------------------------

    /// Pass 1 folds `recovered_max_commit_ts` solely from ChainCommit hits.
    /// Given [ChainCommit(T1), LogicalTxnFrame(T2)] with no ChainCommit at T2,
    /// the HLC floor must be T1 — the later logical-frame commit_ts MUST NOT
    /// leak into max_commit_ts (§3.10).
    #[test]
    fn recovered_max_commit_ts_folds_chain_commits_only() {
        use crate::journal::log_file::LogicalTxnFrame;
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let t1 = Ts {
            physical_ms: 100,
            logical: 0,
        };
        let t2 = Ts {
            physical_ms: 999,
            logical: 7,
        };

        mgr.append_chain_commit(t1, vec![], vec![]).unwrap();
        let frame = LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: t2,
            diagnostic_txn_id: 1,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        };
        mgr.append_logical_txn(frame).unwrap();

        std::mem::forget(mgr);
        drop(main_file);

        let mut main_file_reopen = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file_reopen).unwrap();
        assert_eq!(
            mgr2.recovered_max_commit_ts(),
            Some(t1),
            "HLC floor must come from ChainCommit only (§3.10); \
             logical-frame commit_ts must not leak into max_commit_ts"
        );
        drop(mgr2);
        drop(main_file_reopen);
        drop(dir);
    }

    /// Orphan logical-without-ChainCommit frames are swept post-scan per
    /// §3.8(b). Writes one logical frame without a matching ChainCommit and
    /// asserts `ParsedLogicalFrames.frames` is empty after the Pass 1 sweep
    /// AND the §3.8(b) sweep counter increments (observable proof of the
    /// `tracing::warn!` path, since `tracing` is an optional feature).
    #[test]
    fn recovery_discards_logical_without_matching_chain_commit() {
        use crate::journal::log_file::LogicalTxnFrame;
        use crate::mvcc::metrics::{
            logical_txn_pass1_orphan_logical_dropped_snapshot,
            reset_logical_txn_pass1_orphan_logical_dropped,
        };
        use crate::mvcc::timestamp::Ts;

        // Test-only mutex — these globals are shared across the crate.
        let _guard = orphan_metrics_guard();
        reset_logical_txn_pass1_orphan_logical_dropped();
        let before = logical_txn_pass1_orphan_logical_dropped_snapshot();

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let orphan_ts = Ts {
            physical_ms: 300,
            logical: 2,
        };
        let frame = LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: orphan_ts,
            diagnostic_txn_id: 99,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        };
        mgr.append_logical_txn(frame).unwrap();

        std::mem::forget(mgr);
        drop(main_file);

        let mut main_file_reopen = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mut mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file_reopen).unwrap();
        let parsed = mgr2.take_parsed_logical_frames();
        assert!(
            parsed.frames.is_empty(),
            "orphan logical frames (no matching ChainCommit) must be \
             swept by Pass 1 per §3.8(b); found {} frame(s)",
            parsed.frames.len()
        );
        // The §3.8(b) sweep MUST log a warning. Observable proof: counter
        // ticks at least once for the orphan frame.
        assert!(
            logical_txn_pass1_orphan_logical_dropped_snapshot() > before,
            "§3.8(b) sweep must record at least one orphan-logical drop \
             (warning observable via counter)"
        );
        drop(mgr2);
        drop(main_file_reopen);
        drop(dir);
    }

    /// Case (c) Phase 2 tolerance: ChainCommit present without a matching
    /// logical frame. Recovery must proceed without error (Phase 4 will
    /// upgrade this to a hard error per exit criterion §8.13.3) AND the
    /// §3.7-envelope-violation warning must be observable — verified
    /// here via the unmatched-ChainCommit counter.
    #[test]
    fn recovery_tolerates_chain_commit_without_matching_logical() {
        use crate::mvcc::metrics::{
            logical_txn_pass1_unmatched_chain_commit_snapshot,
            reset_logical_txn_pass1_unmatched_chain_commit,
        };
        use crate::mvcc::timestamp::Ts;

        let _guard = orphan_metrics_guard();
        reset_logical_txn_pass1_unmatched_chain_commit();
        let before = logical_txn_pass1_unmatched_chain_commit_snapshot();

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let ts = Ts {
            physical_ms: 444,
            logical: 0,
        };
        mgr.append_chain_commit(ts, vec![], vec![]).unwrap();

        std::mem::forget(mgr);
        drop(main_file);

        let mut main_file_reopen = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file_reopen).unwrap();
        assert_eq!(
            mgr2.recovered_max_commit_ts(),
            Some(ts),
            "Phase 2 must tolerate ChainCommit-without-logical; HLC floor \
             still advances from the ChainCommit alone"
        );
        // The §3.7-envelope-violation warning MUST be observable. Counter
        // ticks for the unmatched ChainCommit (at least 1, since a pristine
        // fresh-DB open may also contribute a no-op recovery pass).
        assert!(
            logical_txn_pass1_unmatched_chain_commit_snapshot() > before,
            "case (c) tolerance must record at least one unmatched-ChainCommit \
             (warning observable via counter)"
        );
        drop(mgr2);
        drop(main_file_reopen);
        drop(dir);
    }

    #[test]
    fn test_clean_page0_checkpoint_boundary_cut() {
        use crate::journal::log_file::LogicalTxnFrame;
        use crate::mvcc::metrics::{
            logical_txn_pass1_pre_boundary_dropped_snapshot,
            reset_logical_txn_pass1_pre_boundary_dropped,
        };
        use crate::mvcc::timestamp::Ts;

        let _guard = orphan_metrics_guard();
        reset_logical_txn_pass1_pre_boundary_dropped();
        let before = logical_txn_pass1_pre_boundary_dropped_snapshot();

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let pre_ts = Ts {
            physical_ms: 100,
            logical: 0,
        };
        let boundary_ts = Ts {
            physical_ms: 100,
            logical: 5,
        };
        let post_ts = Ts {
            physical_ms: 200,
            logical: 0,
        };

        mgr.append_logical_txn(LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: pre_ts,
            diagnostic_txn_id: 1,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        })
        .unwrap();
        mgr.append_chain_commit(pre_ts, vec![], vec![]).unwrap();

        let _ = append_test_page0_boundary(&mut mgr, &header, boundary_ts);

        mgr.append_logical_txn(LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: post_ts,
            diagnostic_txn_id: 2,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        })
        .unwrap();
        mgr.append_chain_commit(post_ts, vec![], vec![]).unwrap();

        std::mem::forget(mgr);
        drop(main_file);

        let mut main_file_reopen = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mut mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file_reopen).unwrap();
        let parsed = mgr2.take_parsed_logical_frames();

        assert_eq!(parsed.frames.len(), 1);
        assert_eq!(parsed.frames[0].1.commit_ts, post_ts);
        assert!(logical_txn_pass1_pre_boundary_dropped_snapshot() > before);
        drop(mgr2);
        drop(main_file_reopen);
        drop(dir);
    }

    #[test]
    fn test_page0_checkpoint_boundary_frontier_monotonicity_clean_pair() {
        use crate::journal::log_file::LogicalTxnFrame;
        use crate::mvcc::timestamp::Ts;

        let _guard = orphan_metrics_guard();
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let pre_ts = Ts {
            physical_ms: 10,
            logical: 0,
        };
        let hi1 = Ts {
            physical_ms: 10,
            logical: 5,
        };
        let mid_ts = Ts {
            physical_ms: 100,
            logical: 0,
        };
        let hi2 = Ts {
            physical_ms: 100,
            logical: 5,
        };
        let post_ts = Ts {
            physical_ms: 200,
            logical: 0,
        };

        for (ts, diagnostic_txn_id) in [(pre_ts, 1), (mid_ts, 2)] {
            mgr.append_logical_txn(LogicalTxnFrame {
                salt1: mgr.salt1,
                salt2: mgr.salt2,
                commit_ts: ts,
                diagnostic_txn_id,
                format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
                flags: 0,
                ops: vec![],
            })
            .unwrap();
            mgr.append_chain_commit(ts, vec![], vec![]).unwrap();
            let _ = append_test_page0_boundary(
                &mut mgr,
                &header,
                if diagnostic_txn_id == 1 { hi1 } else { hi2 },
            );
        }

        mgr.append_logical_txn(LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: post_ts,
            diagnostic_txn_id: 3,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        })
        .unwrap();
        mgr.append_chain_commit(post_ts, vec![], vec![]).unwrap();

        std::mem::forget(mgr);
        drop(main_file);

        let mut main_file_reopen = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mut mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file_reopen).unwrap();
        let parsed = mgr2.take_parsed_logical_frames();

        assert_eq!(parsed.frames.len(), 1);
        assert_eq!(parsed.frames[0].1.commit_ts, post_ts);
        drop(mgr2);
        drop(main_file_reopen);
        drop(dir);
    }

    #[test]
    fn test_page0_checkpoint_boundary_frontier_rejects_regression() {
        use crate::error::Error;
        use crate::mvcc::timestamp::Ts;

        let _guard = orphan_metrics_guard();
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let _ = append_test_page0_boundary(
            &mut mgr,
            &header,
            Ts {
                physical_ms: 100,
                logical: 9,
            },
        );
        let _ = append_test_page0_boundary(
            &mut mgr,
            &header,
            Ts {
                physical_ms: 50,
                logical: 9,
            },
        );

        std::mem::forget(mgr);
        drop(main_file);

        let mut main_file_reopen = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let res = JournalManager::open_or_create(&db_path, &header, &mut main_file_reopen);
        match res {
            Err(Error::CorruptDatabase { detail, .. }) => {
                assert!(detail.contains("last_checkpoint_ts regressed"));
            }
            Ok(_) => panic!("expected CorruptDatabase on regressed page-0 boundary frontier"),
            Err(other) => panic!("expected CorruptDatabase, got: {other:?}"),
        }
        drop(main_file_reopen);
        drop(dir);
    }

    #[test]
    fn read_page_linear_ignores_page0_checkpoint_boundary() {
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let _ = append_test_page0_boundary(
            &mut mgr,
            &header,
            Ts {
                physical_ms: 100,
                logical: 0,
            },
        );

        mgr.index.clear_index();
        assert!(
            mgr.read_page_linear(0).unwrap().is_none(),
            "generic linear lookup must not expose checkpoint page-0 boundaries"
        );
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    #[test]
    fn truncate_to_does_not_index_page0_checkpoint_boundary() {
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let _ = append_test_page0_boundary(
            &mut mgr,
            &header,
            Ts {
                physical_ms: 100,
                logical: 0,
            },
        );
        mgr.append_non_commit(44, JournalPageSize::Small4k, &make_page_4k(0x44))
            .unwrap();
        let mark = mgr.write_cursor();
        mgr.append_non_commit(55, JournalPageSize::Small4k, &make_page_4k(0x55))
            .unwrap();

        mgr.truncate_to(mark).unwrap();
        assert!(mgr.read_page(0).unwrap().is_none());
        assert!(mgr.read_page(44).unwrap().is_none());
        assert!(mgr.read_page(55).unwrap().is_none());
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    /// §8.2 / US-020 — Pass 1 must halt the scan at the offset of a torn
    /// `LogicalTxnFrame` (CRC mismatch / kind+salt match but body
    /// invalid). After recovery `write_cursor` must equal the torn
    /// frame's start offset, AND any bytes after the torn frame must
    /// NOT be scanned (would otherwise be replayed and corrupt state).
    #[test]
    fn pass1_torn_logical_frame_halts_scan_at_offset() {
        use crate::journal::log_file::{LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION};
        use crate::mvcc::timestamp::Ts;
        use std::io::{Seek, SeekFrom, Write};

        let _guard = orphan_metrics_guard();

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        // First valid logical + ChainCommit pair (so HLC floor advances
        // legitimately). Snapshot the cursor — this is where the torn
        // frame begins.
        let valid_ts = Ts {
            physical_ms: 100,
            logical: 0,
        };
        mgr.append_logical_txn(LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: valid_ts,
            diagnostic_txn_id: 1,
            format_version: LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        })
        .unwrap();
        mgr.append_chain_commit(valid_ts, vec![], vec![]).unwrap();

        let torn_start = mgr.write_cursor();

        // Append a SECOND logical frame, then corrupt its CRC tail so
        // the scanner cannot decode it. The bytes after this point
        // (none in this test) must NOT be parsed.
        let torn_frame_ts = Ts {
            physical_ms: 200,
            logical: 0,
        };
        mgr.append_logical_txn(LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: torn_frame_ts,
            diagnostic_txn_id: 2,
            format_version: LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        })
        .unwrap();
        let after_torn = mgr.write_cursor();

        // §8.2 / codex US-020 r1 blocker — append a VALID third
        // logical+chain-commit pair AFTER the torn frame so the test
        // can assert these bytes are NOT scanned/applied/collected.
        // Without these trailing valid bytes the "halts at the torn
        // offset" assertion is vacuous (nothing follows to be skipped).
        let post_torn_ts = Ts {
            physical_ms: 300,
            logical: 0,
        };
        mgr.append_logical_txn(LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: post_torn_ts,
            diagnostic_txn_id: 3,
            format_version: LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        })
        .unwrap();
        mgr.append_chain_commit(post_torn_ts, vec![], vec![])
            .unwrap();

        std::mem::forget(mgr);
        drop(main_file);

        // Corrupt the CRC tail (last 4 bytes) of the SECOND logical
        // frame to simulate a torn write at `after_torn - 4`.
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(crate::journal::journal_path_for(&db_path))
                .unwrap();
            let crc_tail_offset = after_torn - 4;
            f.seek(SeekFrom::Start(crc_tail_offset)).unwrap();
            f.write_all(&0xDEAD_BEEFu32.to_le_bytes()).unwrap();
            f.sync_all().unwrap();
        }

        let mut main_file_reopen = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mut mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file_reopen).unwrap();

        // The scan must halt at the torn frame's offset — write_cursor
        // is the byte position of the first un-parseable frame.
        assert_eq!(
            mgr2.write_cursor(),
            torn_start,
            "Pass 1 must halt at the torn LogicalTxnFrame's offset; \
             expected {torn_start}, got {}",
            mgr2.write_cursor()
        );
        // The post-torn ChainCommit's commit_ts (300) must NOT appear
        // in `recovered_max_commit_ts` — the scan halted before it.
        // Only the pre-torn `valid_ts` (100) was folded.
        let max = mgr2.recovered_max_commit_ts();
        assert_eq!(
            max,
            Some(valid_ts),
            "post-torn ChainCommit (commit_ts=300) must NOT have been \
             folded into the HLC floor; expected {valid_ts:?}, got {max:?}"
        );
        // The post-torn LogicalTxnFrame must NOT appear in the parsed
        // logical-frames hand-off either. The pre-torn `valid_ts`
        // logical frame survives because it has a matching ChainCommit;
        // the post-torn `post_torn_ts` is downstream of the scan's
        // halt offset and so was never observed.
        let parsed = mgr2.take_parsed_logical_frames();
        assert!(
            parsed
                .frames
                .iter()
                .all(|(_, f)| f.commit_ts != post_torn_ts),
            "post-torn LogicalTxnFrame (commit_ts=300) must NOT appear \
             in ParsedLogicalFrames after the scan halted at the torn offset"
        );
        drop(mgr2);
        drop(main_file_reopen);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // US-024 §7 — Phase 2 observability counter tests
    // -----------------------------------------------------------------------

    /// §7 / US-024 AC#4 — `logical_txn_append_bytes_total` grows by
    /// the encoded frame size after one `append_logical_txn` call.
    #[test]
    fn logical_txn_append_bytes_total_tracks_emit_bytes() {
        use crate::journal::log_file::{LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION};
        use crate::mvcc::metrics::{
            logical_txn_append_bytes_snapshot, reset_logical_txn_append_bytes,
        };
        use crate::mvcc::timestamp::Ts;

        let _guard = orphan_metrics_guard();
        reset_logical_txn_append_bytes();
        let before = logical_txn_append_bytes_snapshot();

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let frame = LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: Ts {
                physical_ms: 1,
                logical: 0,
            },
            diagnostic_txn_id: 0,
            format_version: LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        };
        let expected_bytes = frame.encode().expect("encode").len() as u64;
        mgr.append_logical_txn(frame).unwrap();

        let after = logical_txn_append_bytes_snapshot();
        // Counter is process-global; other concurrent tests may also
        // tick it. Assert "grew by at least the expected amount" rather
        // than strict equality so the test is robust under parallel
        // execution. The orphan_metrics_guard serializes the reset →
        // emit window for tests in this module.
        assert!(
            after >= before + expected_bytes,
            "append_bytes_total must grow by at least the encoded frame \
             size; before={before}, after={after}, expected={expected_bytes}"
        );
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    /// §7 / US-024 AC#4 — `parsed_logical_frames_len` matches the
    /// length of the Pass 1 → Pass 2 hand-off vector. After two valid
    /// logical+ChainCommit pairs, the gauge equals 2.
    #[test]
    fn parsed_logical_frames_len_matches_pass1_output() {
        use crate::journal::log_file::{LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION};
        use crate::mvcc::metrics::{
            parsed_logical_frames_len_snapshot, reset_parsed_logical_frames_len,
        };
        use crate::mvcc::timestamp::Ts;

        let _guard = orphan_metrics_guard();
        reset_parsed_logical_frames_len();

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        for ms in [100u64, 200] {
            let ts = Ts {
                physical_ms: ms,
                logical: 0,
            };
            mgr.append_logical_txn(LogicalTxnFrame {
                salt1: mgr.salt1,
                salt2: mgr.salt2,
                commit_ts: ts,
                diagnostic_txn_id: ms,
                format_version: LOGICAL_TXN_FORMAT_VERSION,
                flags: 0,
                ops: vec![],
            })
            .unwrap();
            mgr.append_chain_commit(ts, vec![], vec![]).unwrap();
        }
        std::mem::forget(mgr);
        drop(main_file);

        let mut main_file_reopen = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let _mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file_reopen).unwrap();
        assert_eq!(
            parsed_logical_frames_len_snapshot(),
            2,
            "parsed_logical_frames_len must equal Pass 1 vector length"
        );
        drop(_mgr2);
        drop(main_file_reopen);
        drop(dir);
    }

    const PASS2_LIVE_NS_ID: i64 = 1;
    const PASS2_ABSENT_NS_ID: i64 = 999;
    const PASS2_RESOLVED_TS: Ts = Ts {
        physical_ms: 1_000,
        logical: 0,
    };
    const PASS2_UNRESOLVED_TS: Ts = Ts {
        physical_ms: 2_000,
        logical: 0,
    };
    const MIN_SYNTHETIC_COMMIT_TS_OFFSET_MS: u64 = 1;

    fn synthetic_uncheckpointed_ts(header: &FileHeader, requested: Ts) -> Ts {
        if requested > header.last_checkpoint_ts {
            return requested;
        }
        Ts {
            physical_ms: header
                .last_checkpoint_ts
                .physical_ms
                .saturating_add(requested.physical_ms.max(MIN_SYNTHETIC_COMMIT_TS_OFFSET_MS)),
            logical: requested.logical,
        }
    }

    fn setup_pass2_live_catalog(db_path: &Path) {
        use crate::{Client, OpenOptions as DbOpts};

        let client = Client::open_with_options(db_path, DbOpts::new()).expect("open client");
        client
            .database("us024_db")
            .create_collection("c_resolved")
            .expect("create resolved");
        client.close().expect("checkpoint setup catalog");
    }

    fn append_pass2_logical_insert(db_path: &Path, ns_id: i64, commit_ts: Ts) {
        use crate::journal::log_file::{
            ChainCommitFrame, LogRecordDraft, LogicalOp, LogicalOpKind, LogicalTxnFrame,
            LOGICAL_TXN_FORMAT_VERSION,
        };
        use crate::storage::header::HEADER_PAGE_SIZE;

        let mut main_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(db_path)
            .expect("open main file");
        let header = {
            let mut buf = [0u8; HEADER_PAGE_SIZE];
            main_file.seek(SeekFrom::Start(0)).expect("seek header");
            main_file.read_exact(&mut buf).expect("read header");
            FileHeader::from_bytes(&buf).expect("decode header")
        };
        let mut mgr = JournalManager::open_or_create(db_path, &header, &mut main_file)
            .expect("open journal manager");
        let (salt1, salt2) = mgr.salts();
        let commit_ts = synthetic_uncheckpointed_ts(&header, commit_ts);
        let publish_seq = mgr.recovered_max_publish_seq().unwrap_or(0) + 1;
        let frame = LogicalTxnFrame {
            salt1,
            salt2,
            commit_ts,
            diagnostic_txn_id: ns_id as u64,
            format_version: LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::PrimaryInsert {
                    ns_id,
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    overflow: None,
                },
            }],
        };
        let logical = frame.encode().expect("encode logical");
        let chain = ChainCommitFrame {
            salt1,
            salt2,
            commit_ts,
            refcount_deltas: vec![],
            page_writes: vec![],
        }
        .encode()
        .expect("encode chain");
        let record = mgr
            .reserve_log_record(LogRecordDraft::crud(
                ns_id as u64,
                publish_seq,
                commit_ts,
                logical,
                chain,
            ))
            .expect("reserve phase8 crud");
        record.write_and_mark().expect("write phase8 crud");
        mgr.sync_journal().expect("sync journal");
    }

    /// §7 / US-024 AC#4 — `pass2_resolved_ops_total` and
    /// `pass2_unresolved_ops_total` split correctly on a known
    /// workload. Drives both paths through the actual Pass 2 code
    /// path (`SharedState::new` → catalog id resolution): a workload
    /// of one resolvable + one unresolvable id must produce one
    /// increment on each counter.
    ///
    /// codex US-024 r2 blocker AC#4: replaced the prior weak version
    /// (which only proved record/snapshot/reset helpers exist) with
    /// this one that actually drives Pass 2.
    #[test]
    #[serial_test::serial(logical_txn_pass2_metrics)]
    fn pass2_resolved_and_unresolved_counters_split_correctly() {
        use crate::mvcc::metrics::{
            logical_txn_pass2_resolved_ops_snapshot, logical_txn_pass2_unresolved_ops_snapshot,
            record_logical_txn_pass2_resolved_op, record_logical_txn_pass2_unresolved_op,
        };

        let _guard = orphan_metrics_guard();
        // Suppress dead-code warnings: this version of the test drives
        // Pass 2 through the real engine path rather than the direct
        // record_* calls (the prior weak version). The imports remain
        // for symmetry with the AC#1 named-helper inventory.
        let _ = (
            record_logical_txn_pass2_resolved_op,
            record_logical_txn_pass2_unresolved_op,
        );

        // Drive Pass 2 through the real engine open path. The setup checkpoints
        // a live catalog, then appends one durable uncheckpointed logical frame
        // whose ns_id is still live and one whose ns_id is absent.
        use crate::{Client, OpenOptions as DbOpts};
        let dir = tempfile::TempDir::new().expect("tempdir");
        let db_path = dir.path().join("us024.mqlite");
        setup_pass2_live_catalog(&db_path);
        append_pass2_logical_insert(&db_path, PASS2_LIVE_NS_ID, PASS2_RESOLVED_TS);
        append_pass2_logical_insert(&db_path, PASS2_ABSENT_NS_ID, PASS2_UNRESOLVED_TS);
        // Do NOT call reset_* here: other tests in the suite touch the
        // SAME crate-global counters concurrently, and a stale reset
        // from a parallel test could land between our pre snapshot and
        // post snapshot. The counters are monotonic between resets, so
        // taking pre/post snapshots without an intervening reset
        // guarantees `post >= pre + N` regardless of concurrent
        // workloads.
        let pre_resolved = logical_txn_pass2_resolved_ops_snapshot();
        let pre_unresolved = logical_txn_pass2_unresolved_ops_snapshot();
        let _client = Client::open_with_options(&db_path, DbOpts::new()).expect("reopen for pass2");
        let post_resolved = logical_txn_pass2_resolved_ops_snapshot();
        let post_unresolved = logical_txn_pass2_unresolved_ops_snapshot();
        assert!(
            post_resolved > pre_resolved,
            "resolved counter must increment for the LogicalTxn whose \
             ns_id IS in the catalog (pre={pre_resolved}, post={post_resolved})"
        );
        assert!(
            post_unresolved > pre_unresolved,
            "unresolved counter must increment for the LogicalTxn whose \
             ns_id is absent from the catalog (pre={pre_unresolved}, \
             post={post_unresolved})"
        );
    }

    /// Test-only mutex — Pass 1 metric counters are crate-globals and
    /// other tests in this module also touch them.
    fn orphan_metrics_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Phase 4 exit-criterion placeholder. Phase 2 tolerates two
    /// envelope violations that Phase 4 §8.13.3 promotes to hard errors:
    ///
    ///   (a) **case (c)** — ChainCommit without matching LogicalTxnFrame
    ///       (§3.7 envelope violation), promotion site:
    ///       `JournalManager::open_or_create`.
    ///
    ///   (b) **Pass-2 unresolved id** — LogicalTxnFrame references an
    ///       `ns_id` / `index_id` not present in the recovered catalog,
    ///       promotion site:
    ///       `SharedState::new` (`src/storage/paged_engine/state.rs`).
    ///
    /// Both promote together when Phase 4 lands. When that happens this
    /// test should be un-ignored and assert `Err` from BOTH:
    ///   - `JournalManager::open_or_create` for case (c)
    ///   - `Client::open_with_options` (driven through `SharedState::new`)
    ///     for Pass-2 unresolved id
    ///
    /// Gated with the exact ignore string required by US-014 AC#6 and
    /// extended for US-015 AC#6.
    #[test]
    #[ignore = "Phase 4 exit criterion §8.13.3"]
    fn test_unpaired_chain_commit_is_hard_error() {
        // Phase 4 implementation will populate this test body with two
        // assertions: (a) Err from JournalManager::open_or_create for a
        // ChainCommit-without-logical journal, and (b) Err from
        // Client::open / SharedState::new for a logical-frame-with-
        // unresolvable-ns_id journal. Both promotion sites land together.
        panic!("Phase 4 not yet implemented — see §8.13.3 / US-014 AC#6 / US-015 AC#6");
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
    use crate::journal::log_file::JournalPageSize;
    use crate::journal::{write_page_to_main, CheckpointPoolKind, JournalManager};
    use crate::storage::header::FileHeader;
    use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};

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

    fn epoch1_fill(seed: u32) -> u8 {
        ((seed % 200) + 1) as u8
    }
    fn epoch2_fill(seed: u32) -> u8 {
        (((seed + 100) % 200) + 1) as u8
    }
    fn uncommitted_fill(seed: u32) -> u8 {
        (((seed + 50) % 200) + 1) as u8
    }
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
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(db_path)
            .map_err(Error::Io)?;
        main_file
            .set_len(200 * PAGE_SIZE_LEAF as u64)
            .map_err(Error::Io)?;
        let header = FileHeader::new(1_700_000_000_000, SALT1, SALT2);
        main_file.seek(SeekFrom::Start(0)).map_err(Error::Io)?;
        main_file.write_all(&header.to_bytes()).map_err(Error::Io)?;
        main_file.flush().map_err(Error::Io)?;
        let page_data = vec![epoch1_fill(seed); PAGE_SIZE_INTERNAL as usize];
        for page_no in EPOCH1_START..EPOCH1_END {
            write_page_to_main(
                &mut main_file,
                page_no,
                PAGE_SIZE_INTERNAL as usize,
                &page_data,
            )?;
        }
        main_file.flush().map_err(Error::Io)?;
        drop(main_file);
        Ok(())
    }

    fn append_crash_checkpoint_batch(
        journal: &mut JournalManager,
        header: &FileHeader,
        pages: impl IntoIterator<Item = u32>,
        fill: u8,
        checkpoint_ts: crate::mvcc::timestamp::Ts,
    ) -> Result<()> {
        let pages: Vec<u32> = pages.into_iter().collect();
        let cursor = journal.begin_checkpoint_batch()?;
        let batch_id = cursor.batch_id();
        let page_data = vec![fill; PAGE_SIZE_INTERNAL as usize];
        for page_no in &pages {
            journal.append_checkpoint_frame(
                batch_id,
                CheckpointPoolKind::Main,
                *page_no,
                JournalPageSize::Small4k,
                &page_data,
            )?;
        }
        let mut staged_header = header.clone();
        if let Some(max_page) = pages.iter().copied().max() {
            staged_header.total_page_count = staged_header.total_page_count.max(max_page + 1);
        }
        staged_header.last_checkpoint_ts = checkpoint_ts;
        let _ = journal.append_checkpoint_commit_boundary(&staged_header, cursor)?;
        Ok(())
    }

    fn append_crash_checkpoint_image(
        journal: &mut JournalManager,
        header: &FileHeader,
        seed: u32,
        checkpoint_ts: crate::mvcc::timestamp::Ts,
    ) -> Result<()> {
        let cursor = journal.begin_checkpoint_batch()?;
        let batch_id = cursor.batch_id();
        let epoch1 = vec![epoch1_fill(seed); PAGE_SIZE_INTERNAL as usize];
        let epoch2 = vec![epoch2_fill(seed); PAGE_SIZE_INTERNAL as usize];
        for page_no in EPOCH1_START..EPOCH1_END {
            journal.append_checkpoint_frame(
                batch_id,
                CheckpointPoolKind::Main,
                page_no,
                JournalPageSize::Small4k,
                &epoch1,
            )?;
        }
        for page_no in EPOCH2_START..EPOCH2_END {
            journal.append_checkpoint_frame(
                batch_id,
                CheckpointPoolKind::Main,
                page_no,
                JournalPageSize::Small4k,
                &epoch2,
            )?;
        }
        let mut staged_header = header.clone();
        staged_header.total_page_count = staged_header.total_page_count.max(CHECKPOINT_PAGES + 1);
        staged_header.last_checkpoint_ts = checkpoint_ts;
        let _ = journal.append_checkpoint_commit_boundary(&staged_header, cursor)?;
        Ok(())
    }

    unsafe fn child_run_scenario(
        db_path: &Path,
        scenario: Scenario,
        seed: u32,
        write_fd: libc::c_int,
    ) {
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
                let cursor = match journal.begin_checkpoint_batch() {
                    Ok(cursor) => cursor,
                    Err(_) => libc::_exit(4),
                };
                let batch_id = cursor.batch_id();
                for i in 0u32..10 {
                    let _ = journal.append_checkpoint_frame(
                        batch_id,
                        CheckpointPoolKind::Main,
                        EPOCH2_START + i,
                        JournalPageSize::Small4k,
                        &page_data,
                    );
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::InsertAtFrame100 => {
                let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
                let span = EPOCH2_END - EPOCH2_START;
                let cursor = match journal.begin_checkpoint_batch() {
                    Ok(cursor) => cursor,
                    Err(_) => libc::_exit(4),
                };
                let batch_id = cursor.batch_id();
                for i in 0u32..100 {
                    let page_no = EPOCH2_START + (i % span);
                    let _ = journal.append_checkpoint_frame(
                        batch_id,
                        CheckpointPoolKind::Main,
                        page_no,
                        JournalPageSize::Small4k,
                        &page_data,
                    );
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::InsertAtFinalFrame => {
                let _ = append_crash_checkpoint_batch(
                    &mut journal,
                    &header,
                    EPOCH2_START..(EPOCH2_START + 6),
                    e2_fill,
                    crate::mvcc::timestamp::Ts {
                        physical_ms: 1_700_000_000_001 + seed as u64,
                        logical: 0,
                    },
                );
                step!();
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::CheckpointAt25Pct
            | Scenario::CheckpointAt50Pct
            | Scenario::CheckpointAt75Pct => {
                let _ = append_crash_checkpoint_image(
                    &mut journal,
                    &header,
                    seed,
                    crate::mvcc::timestamp::Ts {
                        physical_ms: 1_700_000_000_001 + seed as u64,
                        logical: 0,
                    },
                );
                let garbage = vec![CHECKPOINT_GARBAGE_FILL; PAGE_SIZE_INTERNAL as usize];
                for page_no in 1..=CHECKPOINT_PAGES {
                    let _ = write_page_to_main(
                        &mut main_file,
                        page_no,
                        PAGE_SIZE_INTERNAL as usize,
                        &garbage,
                    );
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
                let cursor = match journal.begin_checkpoint_batch() {
                    Ok(cursor) => cursor,
                    Err(_) => libc::_exit(4),
                };
                let batch_id = cursor.batch_id();
                for i in 0u32..5 {
                    let _ = journal.append_checkpoint_frame(
                        batch_id,
                        CheckpointPoolKind::Main,
                        INDEX_START + i,
                        JournalPageSize::Small4k,
                        &page_data,
                    );
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::IndexBuildAtEnd => {
                let page_data = vec![uc_fill; PAGE_SIZE_INTERNAL as usize];
                let cursor = match journal.begin_checkpoint_batch() {
                    Ok(cursor) => cursor,
                    Err(_) => libc::_exit(4),
                };
                let batch_id = cursor.batch_id();
                for i in 0u32..(INDEX_END - INDEX_START) {
                    let _ = journal.append_checkpoint_frame(
                        batch_id,
                        CheckpointPoolKind::Main,
                        INDEX_START + i,
                        JournalPageSize::Small4k,
                        &page_data,
                    );
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

    fn validate(
        journal: &JournalManager,
        main_file: &mut std::fs::File,
        scenario: Scenario,
        seed: u32,
    ) -> Result<()> {
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

        if matches!(
            scenario,
            Scenario::CheckpointAt25Pct | Scenario::CheckpointAt50Pct | Scenario::CheckpointAt75Pct
        ) {
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

        if matches!(
            scenario,
            Scenario::IndexBuildAtStart | Scenario::IndexBuildMidway | Scenario::IndexBuildAtEnd
        ) {
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
        assert_eq!(
            unsafe { libc::pipe(pipe_fds.as_mut_ptr()) },
            0,
            "pipe() failed"
        );
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
                failures.len(),
                total,
                failures.join("\n")
            );
        }
    }

    mod log_record_codec_tests {
        use crate::journal::log_file::{
            FinalizedLogRecord, LogRecord, LogRecordDraft, LogRecordFlags, LogRecordKind,
            LogRecordPayload, JOURNAL_FORMAT_VERSION, LOG_RECORD_COMMIT_TS_LOGICAL_OFFSET,
            LOG_RECORD_COMMIT_TS_PHYSICAL_OFFSET, LOG_RECORD_END_LSN_OFFSET,
            LOG_RECORD_FLAGS_OFFSET, LOG_RECORD_FORMAT_VERSION, LOG_RECORD_FORMAT_VERSION_OFFSET,
            LOG_RECORD_HEADER_CRC32C_OFFSET, LOG_RECORD_HEADER_LEN, LOG_RECORD_HEADER_LEN_OFFSET,
            LOG_RECORD_KIND_OFFSET, LOG_RECORD_MAGIC, LOG_RECORD_MAGIC_OFFSET,
            LOG_RECORD_PAYLOAD_CRC32C_OFFSET, LOG_RECORD_PAYLOAD_LEN_OFFSET,
            LOG_RECORD_PUBLISH_SEQ_OFFSET, LOG_RECORD_START_LSN_OFFSET,
            LOG_RECORD_TOTAL_LEN_OFFSET, LOG_RECORD_TXN_ID_OFFSET, MAX_LOG_RECORD_BYTES,
            RETIRED_PRE_RELEASE_JOURNAL_FORMAT_VERSIONS,
        };
        use crate::mvcc::timestamp::Ts;

        const START_LSN: u64 = 4096;

        fn sample_ts() -> Ts {
            Ts {
                physical_ms: 1_700_000_123_456,
                logical: 42,
            }
        }

        fn sample_crud_record() -> FinalizedLogRecord {
            let draft = LogRecordDraft::crud(
                0xAABB_CCDD_EEFF_0011,
                9,
                sample_ts(),
                b"logical-txn".to_vec(),
                b"chain-refcount-pages".to_vec(),
            );
            assert_eq!(
                draft.encoded_len().unwrap(),
                LOG_RECORD_HEADER_LEN + 8 + b"logical-txn".len() + b"chain-refcount-pages".len()
            );
            draft.finalize(START_LSN).unwrap()
        }

        fn decode(bytes: &[u8]) -> LogRecord {
            LogRecord::decode(bytes).expect("Phase 8 LogRecord must decode")
        }

        fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
            bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
        }

        fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
            bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        }

        fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
            bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        }

        fn get_u32(bytes: &[u8], offset: usize) -> u32 {
            u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap())
        }

        fn recompute_header_crc(bytes: &mut [u8]) {
            put_u32(bytes, LOG_RECORD_HEADER_CRC32C_OFFSET, 0);
            let crc = crc32c::crc32c(&bytes[..LOG_RECORD_HEADER_LEN]);
            put_u32(bytes, LOG_RECORD_HEADER_CRC32C_OFFSET, crc);
        }

        fn recompute_payload_crc_and_header_crc(bytes: &mut [u8]) {
            let payload_len = get_u32(bytes, LOG_RECORD_PAYLOAD_LEN_OFFSET) as usize;
            let payload_end = LOG_RECORD_HEADER_LEN + payload_len;
            let crc = crc32c::crc32c(&bytes[LOG_RECORD_HEADER_LEN..payload_end]);
            put_u32(bytes, LOG_RECORD_PAYLOAD_CRC32C_OFFSET, crc);
            recompute_header_crc(bytes);
        }

        #[test]
        fn journal_format_version_is_bumped() {
            assert_eq!(JOURNAL_FORMAT_VERSION, 3);
            assert_eq!(RETIRED_PRE_RELEASE_JOURNAL_FORMAT_VERSIONS, &[1, 2]);
        }

        #[test]
        fn log_record_constants_match_current_wire_contract() {
            assert_eq!(LOG_RECORD_MAGIC.to_le_bytes(), *b"MQL8");
            assert_eq!(LOG_RECORD_FORMAT_VERSION, 1);
            assert_eq!(LOG_RECORD_HEADER_LEN, 72);
            assert_eq!(MAX_LOG_RECORD_BYTES, 64 * 1024 * 1024);
            assert_eq!(LogRecordFlags::HAS_LOGICAL_PAYLOAD.bits(), 0x0001);
            assert_eq!(LogRecordFlags::HAS_CHAIN_PAYLOAD.bits(), 0x0002);
            assert_eq!(LogRecordFlags::HAS_CATALOG_PAYLOAD.bits(), 0x0004);
            assert_eq!(LogRecordFlags::CHECKPOINT_BOUNDARY.bits(), 0x0008);
        }

        #[test]
        fn log_record_crud_round_trip_finalizes_lsn_and_crc_fields() {
            let finalized = sample_crud_record();
            let bytes = finalized.bytes();
            let decoded = decode(bytes);

            assert_eq!(finalized.start_lsn(), START_LSN);
            assert_eq!(finalized.end_lsn(), START_LSN + bytes.len() as u64);
            assert_eq!(decoded.start_lsn, START_LSN);
            assert_eq!(decoded.end_lsn, finalized.end_lsn());
            assert_eq!(decoded.total_len, bytes.len());
            assert_eq!(decoded.payload_len, bytes.len() - LOG_RECORD_HEADER_LEN);
            assert_eq!(decoded.txn_id, 0xAABB_CCDD_EEFF_0011);
            assert_eq!(decoded.publish_seq, 9);
            assert_eq!(decoded.commit_ts, sample_ts());
            assert_eq!(decoded.kind, LogRecordKind::CrudCommit);
            assert_eq!(decoded.flags.bits(), 0x0003);
            assert_eq!(
                decoded.payload,
                LogRecordPayload::CrudCommit {
                    logical_payload: b"logical-txn".to_vec(),
                    chain_payload: b"chain-refcount-pages".to_vec(),
                }
            );

            let mut header = [0u8; LOG_RECORD_HEADER_LEN];
            header.copy_from_slice(&bytes[..LOG_RECORD_HEADER_LEN]);
            put_u32(&mut header, LOG_RECORD_HEADER_CRC32C_OFFSET, 0);
            assert_eq!(decoded.header_crc32c, crc32c::crc32c(&header));
            assert_eq!(
                decoded.payload_crc32c,
                crc32c::crc32c(&bytes[LOG_RECORD_HEADER_LEN..])
            );
        }

        #[test]
        fn log_record_catalog_and_checkpoint_round_trip() {
            let catalog = LogRecordDraft::catalog(11, 12, sample_ts(), b"catalog".to_vec())
                .finalize(100)
                .unwrap();
            let catalog = decode(catalog.bytes());
            assert_eq!(catalog.kind, LogRecordKind::CatalogCommit);
            assert_eq!(catalog.flags.bits(), 0x0004);
            assert_eq!(catalog.publish_seq, 12);
            assert_eq!(
                catalog.payload,
                LogRecordPayload::CatalogCommit(b"catalog".to_vec())
            );

            let checkpoint =
                LogRecordDraft::checkpoint_boundary(13, sample_ts(), b"frontier".to_vec())
                    .finalize(200)
                    .unwrap();
            let checkpoint = decode(checkpoint.bytes());
            assert_eq!(checkpoint.kind, LogRecordKind::CheckpointBoundary);
            assert_eq!(checkpoint.flags.bits(), 0x0008);
            assert_eq!(checkpoint.publish_seq, 0);
            assert_eq!(
                checkpoint.payload,
                LogRecordPayload::CheckpointBoundary(b"frontier".to_vec())
            );
        }

        #[test]
        fn log_record_rejects_bad_magic_and_version() {
            let mut bad_magic = sample_crud_record().bytes().to_vec();
            put_u32(&mut bad_magic, LOG_RECORD_MAGIC_OFFSET, 0xDEAD_BEEF);
            recompute_header_crc(&mut bad_magic);
            assert!(LogRecord::decode(&bad_magic).is_err());

            let mut bad_version = sample_crud_record().bytes().to_vec();
            put_u16(
                &mut bad_version,
                LOG_RECORD_FORMAT_VERSION_OFFSET,
                LOG_RECORD_FORMAT_VERSION + 1,
            );
            recompute_header_crc(&mut bad_version);
            assert!(LogRecord::decode(&bad_version).is_err());
        }

        #[test]
        fn log_record_rejects_unknown_kind_flag_and_invalid_combination() {
            let mut unknown_kind = sample_crud_record().bytes().to_vec();
            put_u16(&mut unknown_kind, LOG_RECORD_KIND_OFFSET, 99);
            recompute_header_crc(&mut unknown_kind);
            assert!(LogRecord::decode(&unknown_kind).is_err());

            let mut unknown_flag = sample_crud_record().bytes().to_vec();
            put_u16(&mut unknown_flag, LOG_RECORD_FLAGS_OFFSET, 0x8003);
            recompute_header_crc(&mut unknown_flag);
            assert!(LogRecord::decode(&unknown_flag).is_err());

            let mut invalid_combo = sample_crud_record().bytes().to_vec();
            put_u16(&mut invalid_combo, LOG_RECORD_FLAGS_OFFSET, 0x0004);
            recompute_header_crc(&mut invalid_combo);
            assert!(LogRecord::decode(&invalid_combo).is_err());
        }

        #[test]
        fn log_record_header_crc_covers_every_header_field() {
            let base = sample_crud_record().bytes().to_vec();
            let covered_offsets = [
                LOG_RECORD_MAGIC_OFFSET,
                LOG_RECORD_FORMAT_VERSION_OFFSET,
                LOG_RECORD_HEADER_LEN_OFFSET,
                LOG_RECORD_KIND_OFFSET,
                LOG_RECORD_FLAGS_OFFSET,
                LOG_RECORD_TOTAL_LEN_OFFSET,
                LOG_RECORD_START_LSN_OFFSET,
                LOG_RECORD_END_LSN_OFFSET,
                LOG_RECORD_TXN_ID_OFFSET,
                LOG_RECORD_PUBLISH_SEQ_OFFSET,
                LOG_RECORD_COMMIT_TS_PHYSICAL_OFFSET,
                LOG_RECORD_COMMIT_TS_LOGICAL_OFFSET,
                LOG_RECORD_PAYLOAD_LEN_OFFSET,
                LOG_RECORD_HEADER_CRC32C_OFFSET,
                LOG_RECORD_PAYLOAD_CRC32C_OFFSET,
            ];

            for offset in covered_offsets {
                let mut bytes = base.clone();
                bytes[offset] ^= 0x01;
                assert!(
                    LogRecord::decode(&bytes).is_err(),
                    "header mutation at offset {offset} must be rejected"
                );
            }
        }

        #[test]
        fn log_record_rejects_bad_payload_crc() {
            let mut bytes = sample_crud_record().bytes().to_vec();
            bytes[LOG_RECORD_HEADER_LEN] ^= 0x01;
            assert!(LogRecord::decode(&bytes).is_err());
        }

        #[test]
        fn log_record_rejects_truncated_header_and_payload() {
            let bytes = sample_crud_record().bytes().to_vec();
            assert!(LogRecord::decode(&bytes[..LOG_RECORD_HEADER_LEN - 1]).is_err());
            assert!(LogRecord::decode(&bytes[..bytes.len() - 1]).is_err());
        }

        #[test]
        fn log_record_rejects_mismatched_lengths_and_lsn() {
            let base = sample_crud_record().bytes().to_vec();

            let mut bad_total = base.clone();
            let total_len = get_u32(&bad_total, LOG_RECORD_TOTAL_LEN_OFFSET);
            put_u32(&mut bad_total, LOG_RECORD_TOTAL_LEN_OFFSET, total_len + 1);
            recompute_header_crc(&mut bad_total);
            assert!(LogRecord::decode(&bad_total).is_err());

            let mut bad_lsn = base;
            put_u64(&mut bad_lsn, LOG_RECORD_END_LSN_OFFSET, START_LSN + 1);
            recompute_header_crc(&mut bad_lsn);
            assert!(LogRecord::decode(&bad_lsn).is_err());
        }

        #[test]
        fn log_record_rejects_invalid_crud_payload_split() {
            let mut bytes = sample_crud_record().bytes().to_vec();
            let logical_len = get_u32(&bytes, LOG_RECORD_HEADER_LEN) + 1;
            put_u32(&mut bytes, LOG_RECORD_HEADER_LEN, logical_len);
            recompute_payload_crc_and_header_crc(&mut bytes);
            assert!(LogRecord::decode(&bytes).is_err());
        }

        #[test]
        fn log_record_rejects_publish_seq_control_record_violations() {
            assert!(LogRecordDraft::crud(
                1,
                0,
                sample_ts(),
                b"logical".to_vec(),
                b"chain".to_vec()
            )
            .encoded_len()
            .is_err());
            assert!(
                LogRecordDraft::catalog(1, 0, sample_ts(), b"catalog".to_vec())
                    .encoded_len()
                    .is_err()
            );

            let mut checkpoint =
                LogRecordDraft::checkpoint_boundary(1, sample_ts(), b"frontier".to_vec())
                    .finalize(START_LSN)
                    .unwrap()
                    .bytes()
                    .to_vec();
            put_u64(&mut checkpoint, LOG_RECORD_PUBLISH_SEQ_OFFSET, 7);
            recompute_header_crc(&mut checkpoint);
            assert!(LogRecord::decode(&checkpoint).is_err());
        }

        #[test]
        fn log_record_oversize_fails_from_encoded_len_before_finalize() {
            let payload = vec![0u8; MAX_LOG_RECORD_BYTES - LOG_RECORD_HEADER_LEN + 1];
            let draft = LogRecordDraft::catalog(1, 1, sample_ts(), payload);
            assert!(draft.encoded_len().is_err());
        }
    }
}
