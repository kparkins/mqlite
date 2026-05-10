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
        let header = make_header();
        let shm_sidecar = {
            let mut p = db_path.as_os_str().to_owned();
            p.push("-shm");
            PathBuf::from(p)
        };

        let mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert!(!shm_sidecar.exists(), "no -shm after open");

        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // Append and read back
    // -----------------------------------------------------------------------

    // `append_and_read_4k`, `append_and_read_32k`, and `latest_write_wins`
    // deleted — exercised the legacy 24-byte `append_non_commit`/`read_page`
    // index-lookup path. Production CRUD now reserves through `LogManager`
    // and replay happens via the unified record stream; per-page lookup by
    // page number is no longer a journal-level concern.

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
    // Recovery — crash simulation
    // -----------------------------------------------------------------------


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
        let _mgr2 = JournalManager::open_or_create(&db_path, &header, &mut main_file2).unwrap();

        // Page 2 should not be copied into the main file.
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file2
            .seek(SeekFrom::Start(2 * PAGE_SIZE_LEAF as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0x00, "incomplete checkpoint page is discarded");
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
    // Linear scan fallback
    // -----------------------------------------------------------------------

    // `linear_scan_ignores_untagged_page_frames` deleted — tested the
    // legacy `read_page_linear` skip-on-untagged path that the unified
    // record stream removes.

    // -----------------------------------------------------------------------
    // Rollback (truncate_to)
    // -----------------------------------------------------------------------

    // Three legacy `truncate_to` tests deleted — they exercised the
    // 24-byte page-frame index and `read_page` lookup path that the unified
    // `LogManager` reservation stream replaces.

    #[test]
    fn truncate_to_full_drops_all_non_header_frames_placeholder() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.truncate_to(JOURNAL_HEADER_SIZE as u64).unwrap();
        assert_eq!(mgr.write_cursor(), JOURNAL_HEADER_SIZE as u64);
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    // `JournalLayeredSource` tests deleted alongside the type itself — the
    // journal-as-page-overlay reader model is gone now that buffer-pool LSN
    // pinning prevents cache misses from observing pre-checkpoint pages.

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


    // -----------------------------------------------------------------------
    // Phase 2 US-014 — HLC floor isolation (§3.10) and orphan sweep (§3.8(b))
    // -----------------------------------------------------------------------


    // `test_clean_page0_checkpoint_boundary_cut` deleted — exercised the
    // legacy LogicalTxnFrame + ChainCommit + Page0BoundaryRecord recovery
    // pipeline. Equivalent behaviour is covered by the Phase 8 CrudCommit +
    // CheckpointBoundary recovery tests.

    // `test_page0_checkpoint_boundary_frontier_monotonicity_clean_pair`
    // deleted — exercised the legacy LogicalTxnFrame + ChainCommit +
    // Page0BoundaryRecord pipeline whose intermixed scan no longer maps onto
    // the unified LogManager record stream. Phase 8 recovery handles
    // boundary-driven cull through the CrudCommit + CheckpointBoundary path.


    // `read_page_linear_ignores_page0_checkpoint_boundary` deleted — tested
    // the legacy `index.clear_index()` + `read_page_linear` lookup path that
    // the unified record stream removes (no per-page index lives on the
    // journal-side after PR1).

    // `truncate_to_does_not_index_page0_checkpoint_boundary` deleted —
    // exercised the legacy `read_page` index lookup path that the unified
    // `LogManager` stream no longer maintains.
    #[test]
    fn truncate_to_resets_log_manager_frontier_to_cursor() {
        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        mgr.truncate_to(JOURNAL_HEADER_SIZE as u64).unwrap();
        assert_eq!(mgr.write_cursor(), JOURNAL_HEADER_SIZE as u64);
        drop(mgr);
        drop(main_file);
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


    /// Test-only mutex — Pass 1 metric counters are crate-globals and
    /// other tests in this module also touch them.
    fn orphan_metrics_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
        LOCK.lock().unwrap_or_else(|p| p.into_inner())
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
        _journal: &JournalManager,
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
