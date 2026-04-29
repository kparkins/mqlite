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
    use crate::storage::header::FileHeader;
    use crate::storage::page::{PAGE_SIZE_INTERNAL, PAGE_SIZE_LEAF};
    use std::io::{Read, Seek, SeekFrom};
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

        let page_data = make_page_4k(0x7E);
        let cursor_before_logical;
        let logical_frame_offset;
        let encoded_logical;
        {
            let mut mgr =
                JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

            mgr.commit(7, JournalPageSize::Small4k, &page_data, 8)
                .unwrap();

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

        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file2
            .seek(SeekFrom::Start(7 * PAGE_SIZE_LEAF as u64))
            .unwrap();
        main_file2.read_exact(&mut buf).unwrap();
        assert_eq!(buf[0], 0x7E, "committed legacy page must be durable");

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
        let _mgr2 = JournalManager::open_or_create(&db_path, &header, &mut main_file2).unwrap();

        // Both pages should have been replayed into main file at 32 KB slots.
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file2
            .seek(SeekFrom::Start(PAGE_SIZE_LEAF as u64))
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
        let mgr2 = JournalManager::open_or_create(&db_path, &header, &mut main_file2).unwrap();

        // Page 1 (committed) should be in main file at the 32 KB slot offset.
        let mut buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file2
            .seek(SeekFrom::Start(PAGE_SIZE_LEAF as u64))
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
    fn linear_scan_finds_committed_pages() {
        let (_dir, db_path, mut main_file) = make_db_file();
        let header = make_header();

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

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

        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
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
    fn layered_source_write_appends_to_journal_not_file() {
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
        assert_eq!(journal_bytes, Some(payload));

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

    /// §3.11 / US-017 AC#5 — clean checkpoint-boundary cut. One logical
    /// frame with `commit_ts <= covers_commit_ts_hi` is paired with a
    /// ChainCommit and followed by a clean CheckpointCommitBoundaryFrame.
    /// A second logical+ChainCommit pair at `commit_ts > covers_commit_ts_hi`
    /// comes AFTER the boundary. Reopen must:
    ///   1. Open cleanly (no recovery error).
    ///   2. Hand ONLY the post-boundary logical frame to Pass 2 (the
    ///      pre-boundary frame is already reconciled and must be dropped).
    ///   3. Tick the pre-boundary-dropped counter.
    #[test]
    fn test_clean_checkpoint_commit_boundary_cut() {
        use crate::journal::log_file::LogicalTxnFrame;
        use crate::journal::log_file::{CheckpointEpoch, PageId};
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
        let boundary_hi = Ts {
            physical_ms: 100,
            logical: 5,
        };
        let post_ts = Ts {
            physical_ms: 200,
            logical: 0,
        };

        // Pre-boundary logical + ChainCommit pair.
        let pre_frame = LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: pre_ts,
            diagnostic_txn_id: 1,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        };
        mgr.append_logical_txn(pre_frame).unwrap();
        mgr.append_chain_commit(pre_ts, vec![], vec![]).unwrap();

        // Clean boundary frame covering [pre_ts, boundary_hi].
        mgr.append_checkpoint_commit_boundary(CheckpointEpoch(1), pre_ts, boundary_hi, PageId(0))
            .unwrap();

        // Post-boundary logical + ChainCommit pair.
        let post_frame = LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: post_ts,
            diagnostic_txn_id: 2,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        };
        mgr.append_logical_txn(post_frame).unwrap();
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

        assert_eq!(
            parsed.frames.len(),
            1,
            "§3.11: pre-boundary logical frames must be dropped; Pass 2 \
             should see only post-boundary frames; got {} frame(s)",
            parsed.frames.len()
        );
        assert_eq!(
            parsed.frames[0].1.commit_ts, post_ts,
            "surviving frame must be the post-boundary one"
        );
        assert!(
            logical_txn_pass1_pre_boundary_dropped_snapshot() > before,
            "§3.11 pre-boundary cull must tick the counter at least once"
        );
        drop(mgr2);
        drop(main_file_reopen);
        drop(dir);
    }

    /// §3.11 / US-017 AC#4 — torn boundary frame, MID-WRITE partial prefix.
    /// Writes [pre + ChainCommit + valid-boundary(hi1) + mid + ChainCommit]
    /// then begins a SECOND boundary frame and truncates the journal
    /// part-way through it (kind byte present, CRC tail missing — exactly
    /// the failure mode an interrupted fsync produces). Reopen must:
    ///   1. NOT raise CorruptDatabase (torn boundary is treated as absent
    ///      per §3.11 point 4 — scan halts at the kind byte rather than
    ///      falling through to legacy parsing where the kind byte 0x04
    ///      would be misread as `page_number=4` and the bad page-size
    ///      field would surface a hard error).
    ///   2. Resume from the previous valid boundary's `hi1`.
    ///   3. Discard the pre-boundary logical frame; retain the post-hi1
    ///      logical frame even though the torn boundary claimed to cover it.
    ///   4. Tick the torn-boundary counter exactly once.
    #[test]
    fn test_torn_checkpoint_commit_boundary_cut() {
        use crate::journal::log_file::{
            CheckpointEpoch, LogicalTxnFrame, PageId, CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE,
        };
        use crate::mvcc::metrics::{
            recovery_torn_checkpoint_boundary_snapshot, reset_recovery_torn_checkpoint_boundary,
        };
        use crate::mvcc::timestamp::Ts;

        let _guard = orphan_metrics_guard();
        reset_recovery_torn_checkpoint_boundary();
        let before = recovery_torn_checkpoint_boundary_snapshot();

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let pre_ts = Ts {
            physical_ms: 50,
            logical: 0,
        };
        let hi1 = Ts {
            physical_ms: 50,
            logical: 9,
        };
        let mid_ts = Ts {
            physical_ms: 150,
            logical: 0,
        };
        let hi2 = Ts {
            physical_ms: 150,
            logical: 9,
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

        mgr.append_checkpoint_commit_boundary(CheckpointEpoch(1), pre_ts, hi1, PageId(0))
            .unwrap();

        mgr.append_logical_txn(LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: mid_ts,
            diagnostic_txn_id: 2,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        })
        .unwrap();
        mgr.append_chain_commit(mid_ts, vec![], vec![]).unwrap();

        let torn_start = mgr
            .append_checkpoint_commit_boundary(CheckpointEpoch(2), mid_ts, hi2, PageId(0))
            .unwrap();

        std::mem::forget(mgr);
        drop(main_file);

        // Mid-write torn frame: keep the kind byte + a partial prefix but
        // strip the CRC tail. Half the boundary frame survives the crash —
        // exactly the failure mode the scanner must detect and HALT on.
        let partial_len = torn_start + (CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE as u64 / 2);
        {
            let f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(crate::journal::journal_path_for(&db_path))
                .unwrap();
            f.set_len(partial_len).unwrap();
            f.sync_all().unwrap();
        }

        let mut main_file_reopen = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mut mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file_reopen).unwrap();
        let parsed = mgr2.take_parsed_logical_frames();

        assert_eq!(
            parsed.frames.len(),
            1,
            "§3.11 torn boundary (partial prefix): resume from prior hi1; \
             only mid_ts must survive. Got frames: {:?}",
            parsed
                .frames
                .iter()
                .map(|(_, f)| f.commit_ts)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            parsed.frames[0].1.commit_ts, mid_ts,
            "only the post-hi1 frame must survive when the second boundary is torn"
        );
        assert!(
            recovery_torn_checkpoint_boundary_snapshot() == before + 1,
            "§3.11 torn boundary must tick the torn counter (release-active \
             observability proof that the scan halted on the torn kind byte)"
        );
        drop(mgr2);
        drop(main_file_reopen);
        drop(dir);
    }

    /// §3.11 / US-017 AC#4 (CRC-mismatch variant). Writes a full-length
    /// second boundary frame, then corrupts ONLY the CRC tail. The frame
    /// kind byte and length field still parse, the salts still validate,
    /// but `CheckpointCommitBoundaryFrame::decode` rejects on CRC mismatch.
    /// The scanner must report `BoundaryScan::Torn` and HALT — NOT fall
    /// through to legacy parsing (which would misread byte 0 as a legacy
    /// `page_number=4` and surface a hard `CorruptDatabase`).
    #[test]
    fn test_torn_checkpoint_commit_boundary_cut_crc_mismatch() {
        use crate::journal::log_file::{
            CheckpointEpoch, LogicalTxnFrame, PageId, CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE,
        };
        use crate::mvcc::metrics::{
            recovery_torn_checkpoint_boundary_snapshot, reset_recovery_torn_checkpoint_boundary,
        };
        use crate::mvcc::timestamp::Ts;
        use std::io::{Seek, SeekFrom, Write};

        let _guard = orphan_metrics_guard();
        reset_recovery_torn_checkpoint_boundary();
        let before = recovery_torn_checkpoint_boundary_snapshot();

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        let pre_ts = Ts {
            physical_ms: 50,
            logical: 0,
        };
        let hi1 = Ts {
            physical_ms: 50,
            logical: 9,
        };
        let mid_ts = Ts {
            physical_ms: 150,
            logical: 0,
        };
        let hi2 = Ts {
            physical_ms: 150,
            logical: 9,
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

        mgr.append_checkpoint_commit_boundary(CheckpointEpoch(1), pre_ts, hi1, PageId(0))
            .unwrap();

        mgr.append_logical_txn(LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: mid_ts,
            diagnostic_txn_id: 2,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        })
        .unwrap();
        mgr.append_chain_commit(mid_ts, vec![], vec![]).unwrap();

        let torn_start = mgr
            .append_checkpoint_commit_boundary(CheckpointEpoch(2), mid_ts, hi2, PageId(0))
            .unwrap();

        std::mem::forget(mgr);
        drop(main_file);

        // Corrupt the CRC tail of the second boundary frame. Frame
        // length, kind byte, and salts are all unchanged — only the
        // last 4 bytes (CRC32C of the body) are flipped.
        {
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(crate::journal::journal_path_for(&db_path))
                .unwrap();
            let crc_offset = torn_start + (CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE as u64) - 4;
            f.seek(SeekFrom::Start(crc_offset)).unwrap();
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
        let parsed = mgr2.take_parsed_logical_frames();

        assert_eq!(
            parsed.frames.len(),
            1,
            "§3.11 torn boundary (CRC mismatch): scan must halt and resume \
             from prior hi1; only mid_ts survives. Got: {:?}",
            parsed
                .frames
                .iter()
                .map(|(_, f)| f.commit_ts)
                .collect::<Vec<_>>()
        );
        assert_eq!(parsed.frames[0].1.commit_ts, mid_ts);
        assert!(
            recovery_torn_checkpoint_boundary_snapshot() == before + 1,
            "§3.11 CRC-mismatch torn boundary must tick the torn counter \
             (no fallthrough to legacy parsing)"
        );
        drop(mgr2);
        drop(main_file_reopen);
        drop(dir);
    }

    /// §3.11 / US-017 AC#6 — covers_commit_ts_hi monotonicity. Two valid
    /// boundary frames with strictly increasing `covers_commit_ts_hi` must
    /// reopen cleanly and the highest observed hi must cull every
    /// pre-hi2 logical frame. The negative case (non-monotonic pair) is
    /// covered by `test_covers_commit_ts_hi_monotonicity_rejects_regression`
    /// which asserts a release-active hard error.
    #[test]
    fn test_covers_commit_ts_hi_monotonicity_clean_pair() {
        use crate::journal::log_file::LogicalTxnFrame;
        use crate::journal::log_file::{CheckpointEpoch, PageId};
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

        // pre + ChainCommit, boundary hi1, mid + ChainCommit, boundary hi2,
        // post + ChainCommit.
        {
            let ts = pre_ts;
            mgr.append_logical_txn(LogicalTxnFrame {
                salt1: mgr.salt1,
                salt2: mgr.salt2,
                commit_ts: ts,
                diagnostic_txn_id: ts.physical_ms,
                format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
                flags: 0,
                ops: vec![],
            })
            .unwrap();
            mgr.append_chain_commit(ts, vec![], vec![]).unwrap();
        }
        mgr.append_checkpoint_commit_boundary(CheckpointEpoch(1), pre_ts, hi1, PageId(0))
            .unwrap();

        {
            let ts = mid_ts;
            mgr.append_logical_txn(LogicalTxnFrame {
                salt1: mgr.salt1,
                salt2: mgr.salt2,
                commit_ts: ts,
                diagnostic_txn_id: ts.physical_ms,
                format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
                flags: 0,
                ops: vec![],
            })
            .unwrap();
            mgr.append_chain_commit(ts, vec![], vec![]).unwrap();
        }
        mgr.append_checkpoint_commit_boundary(CheckpointEpoch(2), mid_ts, hi2, PageId(0))
            .unwrap();

        mgr.append_logical_txn(LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: post_ts,
            diagnostic_txn_id: post_ts.physical_ms,
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

        // Highest hi wins (hi2); only post_ts > hi2 survives.
        assert_eq!(
            parsed.frames.len(),
            1,
            "monotonic boundary pair must cull every pre-hi2 logical \
             frame; got {:?}",
            parsed
                .frames
                .iter()
                .map(|(_, f)| f.commit_ts)
                .collect::<Vec<_>>()
        );
        assert_eq!(parsed.frames[0].1.commit_ts, post_ts);
        drop(mgr2);
        drop(main_file_reopen);
        drop(dir);
    }

    /// §3.11 / US-017 AC#6 — non-monotonic boundary pair MUST be rejected
    /// in release builds with a hard `CorruptDatabase` error. This proves
    /// the monotonicity check is release-active (not gated behind
    /// `debug_assert!`). Two valid boundary frames are appended with
    /// `hi2 < hi1` (regression) and `recover_existing` MUST return Err.
    #[test]
    fn test_covers_commit_ts_hi_monotonicity_rejects_regression() {
        use crate::error::Error;
        use crate::journal::log_file::{CheckpointEpoch, PageId};
        use crate::mvcc::timestamp::Ts;

        let _guard = orphan_metrics_guard();

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        // hi1 > hi2 — non-monotonic pair (regression).
        let lo1 = Ts {
            physical_ms: 100,
            logical: 0,
        };
        let hi1 = Ts {
            physical_ms: 100,
            logical: 9,
        };
        let lo2 = Ts {
            physical_ms: 50,
            logical: 0,
        };
        let hi2 = Ts {
            physical_ms: 50,
            logical: 9,
        };

        mgr.append_checkpoint_commit_boundary(CheckpointEpoch(1), lo1, hi1, PageId(0))
            .unwrap();
        mgr.append_checkpoint_commit_boundary(CheckpointEpoch(2), lo2, hi2, PageId(0))
            .unwrap();

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
                assert!(
                    detail.contains("monotonicity"),
                    "expected §3.11 monotonicity error; got: {detail}"
                );
            }
            Ok(_) => panic!(
                "expected CorruptDatabase on non-monotonic boundary pair (release-active \
                 §3.11 assertion); got Ok"
            ),
            Err(other) => panic!(
                "expected CorruptDatabase on non-monotonic boundary pair (release-active \
                 §3.11 assertion); got: {other:?}"
            ),
        }
        drop(main_file_reopen);
        drop(dir);
    }

    // -----------------------------------------------------------------------
    // US-018 §6.5 — mixed-format scanner updates (read_page_linear, truncate_to)
    // -----------------------------------------------------------------------

    /// §6.5 / US-018 AC#3 — `read_page_linear` skips a `LogicalTxnFrame`
    /// inserted between two legacy page frames and returns the second
    /// legacy page's data correctly. Without the logical-skip the linear
    /// scanner would halt at the logical frame and miss the trailing
    /// legacy frame entirely.
    #[test]
    fn read_page_linear_skips_logical_txn() {
        use crate::journal::log_file::LogicalTxnFrame;
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        // [legacy(page=10, data=0xAA), logical, legacy(page=20, data=0xBB)]
        mgr.append_non_commit(10, JournalPageSize::Small4k, &make_page_4k(0xAA))
            .unwrap();
        mgr.append_logical_txn(LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: Ts {
                physical_ms: 1,
                logical: 0,
            },
            diagnostic_txn_id: 1,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        })
        .unwrap();
        mgr.append_non_commit(20, JournalPageSize::Small4k, &make_page_4k(0xBB))
            .unwrap();

        // Force the linear-scan path by clearing the in-memory index — the
        // index would otherwise short-circuit `read_page` to O(1) lookup.
        mgr.index.clear_index();

        let p10 = mgr.read_page_linear(10).unwrap();
        let p20 = mgr.read_page_linear(20).unwrap();
        assert!(
            p10.is_some(),
            "page 10 must be found before the logical frame"
        );
        assert_eq!(p10.as_ref().unwrap()[0], 0xAA);
        assert!(
            p20.is_some(),
            "page 20 must be found AFTER the logical frame — read_page_linear \
             must skip past `LogicalTxnFrame` (§6.5/US-018 AC#3)"
        );
        assert_eq!(p20.as_ref().unwrap()[0], 0xBB);
        drop(mgr);

        // §8.2 / codex US-020 r2 blocker AC#2 — reopen via
        // `JournalManager::open_or_create` and assert the same
        // behavior post-recovery: page 20 is still found across
        // the LogicalTxnFrame after Pass 1 reconstructed the index.
        let mut mgr_reopen =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        let p20_post = mgr_reopen.read_page_linear(20).unwrap();
        assert!(
            p20_post.is_some(),
            "post-recovery: page 20 must still be findable past the \
             LogicalTxnFrame (the recovery scan must skip the logical \
             frame the same way the live read_page_linear does)"
        );
        assert_eq!(p20_post.unwrap()[0], 0xBB);
        drop(mgr_reopen);
        drop(main_file);
        drop(dir);
    }

    /// §6.5 / US-018 AC#4 — `truncate_to` rolls back to a cursor JUST BEFORE
    /// a `LogicalTxnFrame` and leaves no torn bytes. After truncation,
    /// the surviving journal must contain only the legacy frames before
    /// the logical-frame mark and the index must reflect those alone.
    #[test]
    fn truncate_to_handles_logical_txn_boundary() {
        use crate::journal::log_file::LogicalTxnFrame;
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        // Pre-mark legacy frames + commit, mark cursor, then write a
        // logical frame + a follow-on legacy frame. Roll back to the
        // mark and assert the post-mark frames are gone.
        mgr.append_non_commit(5, JournalPageSize::Small4k, &make_page_4k(0x05))
            .unwrap();
        mgr.commit(6, JournalPageSize::Small4k, &make_page_4k(0x06), 7)
            .unwrap();
        let mark = mgr.write_cursor();

        mgr.append_logical_txn(LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: Ts {
                physical_ms: 1,
                logical: 0,
            },
            diagnostic_txn_id: 1,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        })
        .unwrap();
        mgr.append_non_commit(99, JournalPageSize::Small4k, &make_page_4k(0x99))
            .unwrap();
        let after = mgr.write_cursor();
        assert!(
            after > mark,
            "logical+legacy must extend the cursor past the mark"
        );

        mgr.truncate_to(mark).unwrap();
        assert_eq!(
            mgr.write_cursor(),
            mark,
            "truncate_to must restore the cursor exactly to the mark"
        );
        assert!(
            mgr.read_page(99).unwrap().is_none(),
            "post-mark page 99 must be gone after rollback to a cursor \
             just before the logical frame"
        );
        // Pre-mark page 5 must still be present (its index entry is the
        // surviving non-commit frame).
        let p5 = mgr.read_page(5).unwrap();
        assert!(p5.is_some(), "pre-mark page 5 survives rollback");
        assert_eq!(p5.unwrap()[0], 0x05);
        drop(mgr);

        // §8.2 / codex US-020 r2 blocker AC#2 — reopen via
        // `JournalManager::open_or_create` and assert the truncation
        // is durable across a recovery cycle: page 99 (the post-mark
        // legacy write) is still gone after Pass 1, and page 5
        // (the pre-mark legacy non-commit) survives. Page 6 (the
        // legacy commit page) was already applied to the main file
        // during the initial commit; recovery's scan finds nothing
        // to apply for the truncated tail.
        let mut mgr_reopen =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert_eq!(
            mgr_reopen.write_cursor(),
            mark,
            "post-recovery write_cursor must equal the truncation mark"
        );
        assert!(
            mgr_reopen.read_page(99).unwrap().is_none(),
            "post-recovery: post-mark page 99 must still be absent \
             (truncation persisted through Pass 1)"
        );
        drop(mgr_reopen);
        drop(main_file);
        drop(dir);
    }

    /// §6.5 / US-018 AC#5 — `truncate_to` rolls back to a cursor AFTER a
    /// legacy frame but BEFORE a following logical frame. The legacy
    /// frame must survive; the logical frame and anything after must
    /// be dropped.
    #[test]
    fn truncate_to_handles_legacy_after_logical() {
        use crate::journal::log_file::LogicalTxnFrame;
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        // Commit page 11 (db_page_count=7) so Pass 1 recovery applies
        // it to the main file. A `append_non_commit`-only frame would
        // be discarded as `pending` without a follower commit; the
        // post-recovery assertion below requires page 11 to survive.
        mgr.commit(11, JournalPageSize::Small4k, &make_page_4k(0x11), 7)
            .unwrap();
        // Mark AFTER the legacy commit frame, BEFORE the logical frame.
        let mark = mgr.write_cursor();

        mgr.append_logical_txn(LogicalTxnFrame {
            salt1: mgr.salt1,
            salt2: mgr.salt2,
            commit_ts: Ts {
                physical_ms: 1,
                logical: 0,
            },
            diagnostic_txn_id: 1,
            format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![],
        })
        .unwrap();
        mgr.append_non_commit(22, JournalPageSize::Small4k, &make_page_4k(0x22))
            .unwrap();

        mgr.truncate_to(mark).unwrap();
        assert_eq!(mgr.write_cursor(), mark);

        // Pre-mark legacy frame (page 11) must still be readable.
        let p11 = mgr.read_page(11).unwrap();
        assert!(
            p11.is_some(),
            "pre-mark legacy frame must survive the rollback past the \
             logical frame (§6.5/US-018 AC#5)"
        );
        assert_eq!(p11.unwrap()[0], 0x11);
        // Post-mark legacy frame (page 22) must be gone.
        assert!(mgr.read_page(22).unwrap().is_none());
        drop(mgr);

        // §8.2 / codex US-020 r2 blocker AC#2 — reopen via
        // `JournalManager::open_or_create` and verify the truncation
        // is durable across recovery: write_cursor lands at the mark,
        // pre-mark page 11 is observable (committed before the mark
        // so Pass 1 applied it to the main file), page 22 (post-mark
        // legacy) is absent, and the LogicalTxnFrame (whose bytes
        // followed the mark) is NOT in `ParsedLogicalFrames`.
        let mut mgr_reopen =
            JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert_eq!(
            mgr_reopen.write_cursor(),
            mark,
            "post-recovery write_cursor must equal the truncation mark"
        );
        // Page 11 was committed before the mark (commit frame with
        // db_page_count=7), so Pass 1 applied it to the main file.
        // It is observable post-recovery via direct main-file read.
        let mut p11_buf = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        main_file
            .seek(SeekFrom::Start(11 * PAGE_SIZE_LEAF as u64))
            .unwrap();
        main_file.read_exact(&mut p11_buf).unwrap();
        assert_eq!(
            p11_buf[0], 0x11,
            "post-recovery: pre-mark legacy commit (page 11) must \
             be applied to the main file"
        );
        assert!(
            mgr_reopen.read_page(22).unwrap().is_none(),
            "post-recovery: post-mark legacy frame (page 22) must be absent"
        );
        let parsed = mgr_reopen.take_parsed_logical_frames();
        assert!(
            parsed.frames.is_empty(),
            "post-recovery: truncated LogicalTxnFrame must not appear in \
             ParsedLogicalFrames; got frames at {:?}",
            parsed.frames.iter().map(|(o, _)| *o).collect::<Vec<_>>()
        );
        drop(mgr_reopen);
        drop(main_file);
        drop(dir);
    }

    /// §6.5 / US-018 — `read_page_linear` skips a valid
    /// `CheckpointCommitBoundaryFrame` inserted between two legacy page
    /// frames and returns the second legacy page's data correctly. Same
    /// shape as the logical-skip test above but exercises the boundary
    /// dispatch added in this story.
    #[test]
    fn read_page_linear_skips_checkpoint_commit_boundary() {
        use crate::journal::log_file::{CheckpointEpoch, PageId};
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        mgr.append_non_commit(10, JournalPageSize::Small4k, &make_page_4k(0xCC))
            .unwrap();
        mgr.append_checkpoint_commit_boundary(
            CheckpointEpoch(1),
            Ts {
                physical_ms: 0,
                logical: 0,
            },
            Ts {
                physical_ms: 100,
                logical: 0,
            },
            PageId(0),
        )
        .unwrap();
        mgr.append_non_commit(20, JournalPageSize::Small4k, &make_page_4k(0xDD))
            .unwrap();

        mgr.index.clear_index();
        let p10 = mgr.read_page_linear(10).unwrap();
        let p20 = mgr.read_page_linear(20).unwrap();
        assert_eq!(p10.unwrap()[0], 0xCC);
        assert_eq!(
            p20.unwrap()[0],
            0xDD,
            "read_page_linear must skip past a valid CheckpointCommitBoundaryFrame"
        );
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    /// §6.5 / US-018 — `truncate_to` skips a `CheckpointCommitBoundaryFrame`
    /// when rebuilding the index across a rollback that crosses the
    /// boundary. Without the boundary-skip dispatch the index rebuild
    /// would halt at the boundary and lose any post-boundary legacy
    /// page frames.
    #[test]
    fn truncate_to_skips_checkpoint_commit_boundary() {
        use crate::journal::log_file::{CheckpointEpoch, PageId};
        use crate::mvcc::timestamp::Ts;

        let (dir, db_path, mut main_file) = make_db_file();
        let header = make_header();
        let mut mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        mgr.append_non_commit(33, JournalPageSize::Small4k, &make_page_4k(0x33))
            .unwrap();
        mgr.append_checkpoint_commit_boundary(
            CheckpointEpoch(1),
            Ts {
                physical_ms: 0,
                logical: 0,
            },
            Ts {
                physical_ms: 100,
                logical: 0,
            },
            PageId(0),
        )
        .unwrap();
        mgr.append_non_commit(44, JournalPageSize::Small4k, &make_page_4k(0x44))
            .unwrap();
        // Mark AFTER all three frames so truncate_to rebuilds the index
        // across the boundary.
        let mark = mgr.write_cursor();
        mgr.append_non_commit(55, JournalPageSize::Small4k, &make_page_4k(0x55))
            .unwrap();

        mgr.truncate_to(mark).unwrap();
        assert_eq!(mgr.write_cursor(), mark);
        // Both pre-boundary (33) and post-boundary (44) pages must
        // survive — the index rebuild crossed the boundary.
        let p33 = mgr.read_page(33).unwrap();
        let p44 = mgr.read_page(44).unwrap();
        assert!(p33.is_some() && p44.is_some());
        assert_eq!(p33.unwrap()[0], 0x33);
        assert_eq!(p44.unwrap()[0], 0x44);
        // Page 55 written after the mark must be gone.
        assert!(mgr.read_page(55).unwrap().is_none());
        drop(mgr);
        drop(main_file);
        drop(dir);
    }

    /// §3.11 / US-017 ROUND 3 regression — the boundary helper's structural
    /// signature (kind=0x04, reserved=[0,0,0], length_field=56, salts) ALIASES
    /// a valid legacy page-write frame with `page_number==4`,
    /// `db_page_count==56`, and matching salts. Without the legacy-CRC
    /// fallback probe in `try_skip_checkpoint_commit_boundary`, such a
    /// legacy frame gets misclassified as a torn boundary and recovery
    /// halts before replaying committed legacy data — silent durable-write
    /// loss after crash.
    ///
    /// This test fabricates exactly that legacy frame on disk and asserts
    /// reopen succeeds AND the legacy frame's index entry survives
    /// recovery (it must be visible via `read_page`).
    #[test]
    fn boundary_helper_does_not_alias_legacy_frame_at_page_4_count_56() {
        use crate::journal::log_file::JournalPageSize;
        use crate::storage::page::PAGE_SIZE_INTERNAL as PAGE_INT;
        use std::io::{Seek, SeekFrom};

        let _guard = orphan_metrics_guard();

        let (dir, db_path, mut main_file) = make_db_file();
        // Pre-allocate main file so checkpoint replay can write to it.
        main_file.set_len(100 * PAGE_INT as u64).unwrap();
        let header = make_header();
        let mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();

        // Fabricate a LEGACY commit frame with page_number=4 and
        // db_page_count=56 — the exact alias the codex critic flagged.
        // We cannot use `commit()` because it would set db_page_count
        // freely; instead we hand-craft the 24-byte header + 4096-byte
        // payload via `JournalFrameHeader::write`.
        let payload = make_page_4k(0xA5);
        let salts = mgr.salts();
        let header_frame = crate::journal::log_file::JournalFrameHeader {
            page_number: 4,
            db_page_count: 56, // ALIAS: matches CHECKPOINT_COMMIT_BOUNDARY_FRAME_SIZE
            salt1: salts.0,
            salt2: salts.1,
            page_size: JournalPageSize::Small4k,
        };
        // Append directly to the journal file, bypassing the typed
        // append helpers so we control every byte.
        let frame_offset = mgr.write_cursor();
        {
            let f_path = crate::journal::journal_path_for(&db_path);
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&f_path)
                .unwrap();
            f.seek(SeekFrom::Start(frame_offset)).unwrap();
            header_frame.write(&mut f, &payload).unwrap();
            f.sync_all().unwrap();
        }
        std::mem::forget(mgr);
        drop(main_file);

        // Reopen — the alias must NOT halt recovery; the legacy frame
        // must replay into the main file (commit frame replays via the
        // legacy path).
        let mut main_file_reopen = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .unwrap();
        let mut mgr2 =
            JournalManager::open_or_create(&db_path, &header, &mut main_file_reopen).unwrap();

        // The committed legacy frame must replay — readback page 4 from
        // the main file and confirm the payload byte survived.
        let observed = mgr2.read_page(4).unwrap();
        assert!(
            observed.is_some(),
            "page 4 must be readable after recovery — the legacy frame must \
             NOT have been misclassified as a torn boundary"
        );
        assert_eq!(
            observed.as_ref().unwrap()[0],
            0xA5,
            "legacy-frame payload must survive recovery; got {:?}",
            observed.as_ref().unwrap().first()
        );
        drop(mgr2);
        drop(main_file_reopen);
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
            LogicalOp, LogicalOpKind, LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION,
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

        mgr.append_logical_txn(frame).expect("append logical");
        mgr.append_chain_commit(commit_ts, vec![], vec![])
            .expect("append chain commit");
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
    fn test_phase4_case_c_is_hard_error() {
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
    use crate::journal::{write_page_to_main, JournalManager};
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
        let mut journal = JournalManager::open_or_create(db_path, &header, &mut main_file)?;
        let page_data = vec![epoch1_fill(seed); PAGE_SIZE_INTERNAL as usize];
        for page_no in EPOCH1_START..(EPOCH1_END - 1) {
            journal.append_non_commit(page_no, JournalPageSize::Small4k, &page_data)?;
        }
        journal.commit(
            EPOCH1_END - 1,
            JournalPageSize::Small4k,
            &page_data,
            EPOCH1_END - 1,
        )?;
        drop(journal);
        drop(main_file);
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
                for i in 0u32..10 {
                    let _ = journal.append_non_commit(
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
                for i in 0u32..100 {
                    let page_no = EPOCH2_START + (i % span);
                    let _ =
                        journal.append_non_commit(page_no, JournalPageSize::Small4k, &page_data);
                    step!();
                }
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::InsertAtFinalFrame => {
                let page_data = vec![e2_fill; PAGE_SIZE_INTERNAL as usize];
                for i in 0u32..5 {
                    let _ = journal.append_non_commit(
                        EPOCH2_START + i,
                        JournalPageSize::Small4k,
                        &page_data,
                    );
                }
                let _ = journal.commit(
                    EPOCH2_START + 5,
                    JournalPageSize::Small4k,
                    &page_data,
                    EPOCH2_START + 5,
                );
                step!();
                std::thread::sleep(std::time::Duration::from_secs(60));
            }
            Scenario::CheckpointAt25Pct
            | Scenario::CheckpointAt50Pct
            | Scenario::CheckpointAt75Pct => {
                let epoch2_data = vec![e2_fill; PAGE_SIZE_INTERNAL as usize];
                let e2_span = EPOCH2_END - EPOCH2_START;
                for i in 0..(e2_span - 1) {
                    let _ = journal.append_non_commit(
                        EPOCH2_START + i,
                        JournalPageSize::Small4k,
                        &epoch2_data,
                    );
                }
                let _ = journal.commit(
                    EPOCH2_START + e2_span - 1,
                    JournalPageSize::Small4k,
                    &epoch2_data,
                    EPOCH2_START + e2_span - 1,
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
                for i in 0u32..5 {
                    let _ = journal.append_non_commit(
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
                for i in 0u32..(INDEX_END - INDEX_START) {
                    let _ = journal.append_non_commit(
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
}
