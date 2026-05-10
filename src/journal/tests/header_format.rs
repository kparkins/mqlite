// Test code intentionally unwraps setup failures so regressions point at the
// broken invariant immediately.
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
mod tests {
    use crate::error::Error;
    use crate::journal::log_file::{
        ChainCommitFrame, JournalHeader, FRAME_KIND_CHAIN_COMMIT, JOURNAL_FORMAT_VERSION,
        JOURNAL_HEADER_SIZE, RETIRED_PRE_RELEASE_JOURNAL_FORMAT_VERSIONS,
    };
    use crate::journal::{journal_path_for, JournalManager};
    use crate::mvcc::timestamp::Ts;
    use crate::storage::header::FileHeader;
    use std::fs::OpenOptions;
    use std::io::{Read, Write};

    const TEST_SALT1: u32 = 0xDEAD_BEEF;
    const TEST_SALT2: u32 = 0xCAFE_BABE;

    fn main_file_fixture() -> (
        tempfile::TempDir,
        std::path::PathBuf,
        std::fs::File,
        FileHeader,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("phase7-us006.mqlite");
        let main_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&db_path)
            .unwrap();
        let header = FileHeader::new(1_700_000_000_000, TEST_SALT1, TEST_SALT2);
        (dir, db_path, main_file, header)
    }

    fn versioned_header_bytes(format_version: u32) -> [u8; JOURNAL_HEADER_SIZE] {
        let mut header = JournalHeader::new(TEST_SALT1, TEST_SALT2);
        header.format_version = format_version;
        header.to_bytes()
    }

    #[test]
    fn test_chain_commit_layout_matches_head() {
        let commit_ts = Ts {
            physical_ms: 1_700_000_001_234,
            logical: 42,
        };
        let frame = ChainCommitFrame {
            salt1: TEST_SALT1,
            salt2: TEST_SALT2,
            commit_ts,
            refcount_deltas: vec![],
            page_writes: vec![],
        };
        let bytes = frame.encode().unwrap();

        assert_eq!(bytes[0], FRAME_KIND_CHAIN_COMMIT);
        assert_eq!(&bytes[1..4], &[0, 0, 0]);
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize,
            bytes.len()
        );
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            TEST_SALT1
        );
        assert_eq!(
            u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            TEST_SALT2
        );
        assert_eq!(
            Ts::from_le_bytes(bytes[16..28].try_into().unwrap()),
            commit_ts
        );
        assert_eq!(u32::from_le_bytes(bytes[28..32].try_into().unwrap()), 0);
        assert_eq!(u32::from_le_bytes(bytes[32..36].try_into().unwrap()), 0);
        let body_end = bytes.len() - 4;
        assert_eq!(
            u32::from_le_bytes(bytes[body_end..].try_into().unwrap()),
            crc32c::crc32c(&bytes[..body_end])
        );
    }

    #[test]
    fn test_journal_format_version_is_journal_header_field() {
        assert_eq!(JOURNAL_FORMAT_VERSION, 3);
        assert_eq!(RETIRED_PRE_RELEASE_JOURNAL_FORMAT_VERSIONS, &[1, 2]);

        let header = JournalHeader::new(TEST_SALT1, TEST_SALT2);
        let bytes = header.to_bytes();
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            JOURNAL_FORMAT_VERSION
        );
        assert_eq!(JournalHeader::from_bytes(&bytes).unwrap(), header);
    }

    #[test]
    fn test_known_retired_journal_version_truncated_at_header_validation() {
        let (_dir, db_path, mut main_file, header) = main_file_fixture();
        let journal_path = journal_path_for(&db_path);
        {
            let mut journal = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&journal_path)
                .unwrap();
            journal.write_all(&versioned_header_bytes(1)).unwrap();
            journal.write_all(b"retired-version-body").unwrap();
        }

        let manager = JournalManager::open_or_create(&db_path, &header, &mut main_file).unwrap();
        assert_eq!(manager.write_cursor(), JOURNAL_HEADER_SIZE as u64);

        let mut recreated = [0u8; JOURNAL_HEADER_SIZE];
        let mut journal = OpenOptions::new().read(true).open(&journal_path).unwrap();
        journal.read_exact(&mut recreated).unwrap();
        assert_eq!(
            u32::from_le_bytes(recreated[4..8].try_into().unwrap()),
            JOURNAL_FORMAT_VERSION
        );
    }

    #[test]
    fn test_unknown_journal_version_errors_without_truncation() {
        let (_dir, db_path, mut main_file, header) = main_file_fixture();
        let journal_path = journal_path_for(&db_path);
        {
            let mut journal = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&journal_path)
                .unwrap();
            journal.write_all(&versioned_header_bytes(999)).unwrap();
            journal.write_all(b"unknown-version-body").unwrap();
        }
        let before_len = std::fs::metadata(&journal_path).unwrap().len();

        let err = match JournalManager::open_or_create(&db_path, &header, &mut main_file) {
            Ok(_) => panic!("unknown future journal version must not be truncated"),
            Err(err) => err,
        };
        assert!(matches!(err, Error::UnsupportedJournalFormat { .. }));
        assert_eq!(std::fs::metadata(&journal_path).unwrap().len(), before_len);
    }

    // Three legacy header-format tests deleted —
    // `test_checkpoint_boundary_does_not_claim_crud_chaincommit_coverage`,
    // `test_checkpoint_codecs_survive_legacy_grep_gate`, and
    // `page0_boundary_record_roundtrips_staged_header` — exercised the
    // 24-byte `Page0BoundaryRecord` codec that the unified `LogManager`
    // reservation stream replaced. The legacy boundary record is no longer
    // written; coverage moved to the `CheckpointBoundaryPayload` round-trip
    // tests in `log_file_codec.rs`.
}
