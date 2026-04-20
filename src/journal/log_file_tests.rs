    use super::*;

    fn sample_header() -> JournalHeader {
        JournalHeader::new(0xDEAD_BEEF, 0xCAFE_BABE)
    }

    #[test]
    fn journal_header_roundtrip() {
        let h = sample_header();
        let bytes = h.to_bytes();
        let decoded = JournalHeader::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.magic, JOURNAL_MAGIC);
        assert_eq!(decoded.format_version, JOURNAL_FORMAT_VERSION);
        assert_eq!(decoded.salt1, 0xDEAD_BEEF);
        assert_eq!(decoded.salt2, 0xCAFE_BABE);
        assert_eq!(decoded.checkpoint_seq, 0);
    }

    #[test]
    fn journal_header_bad_magic_rejected() {
        let mut bytes = sample_header().to_bytes();
        bytes[0] = b'X';
        // Recompute checksum so only magic is wrong
        let checksum = crc32c::crc32c(&bytes[..28]);
        bytes[28..32].copy_from_slice(&checksum.to_le_bytes());
        assert!(JournalHeader::from_bytes(&bytes).is_err());
    }

    #[test]
    fn journal_header_checksum_failure_rejected() {
        let mut bytes = sample_header().to_bytes();
        bytes[10] ^= 0xFF; // corrupt within checksum range
        assert!(JournalHeader::from_bytes(&bytes).is_err());
    }

    #[test]
    fn frame_header_size_constant() {
        assert_eq!(JOURNAL_FRAME_HEADER_SIZE, 24);
        assert_eq!(JOURNAL_HEADER_SIZE, 32);
    }

    #[test]
    fn frame_roundtrip_4k() {
        let frame = JournalFrameHeader {
            page_number: 42,
            db_page_count: 100,
            salt1: 0xDEAD,
            salt2: 0xBEEF,
            page_size: JournalPageSize::Small4k,
        };
        let page_data = vec![0xABu8; PAGE_SIZE_INTERNAL as usize];
        let mut buf = Vec::new();
        frame.write(&mut buf, &page_data).unwrap();
        assert_eq!(
            buf.len(),
            JOURNAL_FRAME_HEADER_SIZE + PAGE_SIZE_INTERNAL as usize
        );

        // Read back
        let mut cursor = std::io::Cursor::new(&buf);
        let decoded = JournalFrameHeader::read(&mut cursor, 0xDEAD, 0xBEEF)
            .unwrap()
            .expect("should parse");
        assert_eq!(decoded.page_number, 42);
        assert_eq!(decoded.db_page_count, 100);
        assert_eq!(decoded.page_size, JournalPageSize::Small4k);
    }

    #[test]
    fn frame_roundtrip_32k() {
        let frame = JournalFrameHeader {
            page_number: 7,
            db_page_count: 0,
            salt1: 1,
            salt2: 2,
            page_size: JournalPageSize::Large32k,
        };
        let page_data = vec![0x5Au8; PAGE_SIZE_LEAF as usize];
        let mut buf = Vec::new();
        frame.write(&mut buf, &page_data).unwrap();
        assert_eq!(buf.len(), JOURNAL_FRAME_HEADER_SIZE + PAGE_SIZE_LEAF as usize);

        let mut cursor = std::io::Cursor::new(&buf);
        let decoded = JournalFrameHeader::read(&mut cursor, 1, 2)
            .unwrap()
            .expect("should parse");
        assert_eq!(decoded.page_number, 7);
        assert_eq!(decoded.db_page_count, 0); // non-commit
    }

    #[test]
    fn frame_bad_checksum_returns_none() {
        let frame = JournalFrameHeader {
            page_number: 1,
            db_page_count: 10,
            salt1: 1,
            salt2: 2,
            page_size: JournalPageSize::Small4k,
        };
        let page_data = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        let mut buf = Vec::new();
        frame.write(&mut buf, &page_data).unwrap();
        // Corrupt a byte in the page data
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;

        let mut cursor = std::io::Cursor::new(&buf);
        let result = JournalFrameHeader::read(&mut cursor, 1, 2).unwrap();
        assert!(result.is_none(), "bad checksum must return None");
    }

    #[test]
    fn frame_salt_mismatch_returns_none() {
        let frame = JournalFrameHeader {
            page_number: 1,
            db_page_count: 10,
            salt1: 1,
            salt2: 2,
            page_size: JournalPageSize::Small4k,
        };
        let page_data = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        let mut buf = Vec::new();
        frame.write(&mut buf, &page_data).unwrap();

        // Read with wrong salts
        let mut cursor = std::io::Cursor::new(&buf);
        let result = JournalFrameHeader::read(&mut cursor, 99, 99).unwrap();
        assert!(result.is_none(), "salt mismatch must return None");
    }

    #[test]
    fn journal_page_size_roundtrip() {
        assert_eq!(JournalPageSize::from_u32(4096).unwrap(), JournalPageSize::Small4k);
        assert_eq!(JournalPageSize::from_u32(32768).unwrap(), JournalPageSize::Large32k);
        assert!(JournalPageSize::from_u32(9999).is_err());
    }

    // -----------------------------------------------------------------
    // ChainCommit frame tests
    // -----------------------------------------------------------------

    fn sample_chain_commit() -> ChainCommitFrame {
        ChainCommitFrame {
            salt1: 0xDEAD_BEEF,
            salt2: 0xCAFE_BABE,
            commit_ts: Ts {
                physical_ms: 0x0011_2233_4455_6677,
                logical: 0x89AB_CDEF,
            },
            refcount_deltas: vec![(10, 1), (20, -1), (u32::MAX - 1, 42)],
            page_writes: vec![
                ChainPageWrite {
                    page: 100,
                    page_size: JournalPageSize::Small4k,
                    data: vec![0xAAu8; PAGE_SIZE_INTERNAL as usize],
                },
                ChainPageWrite {
                    page: 200,
                    page_size: JournalPageSize::Large32k,
                    data: vec![0x5Au8; PAGE_SIZE_LEAF as usize],
                },
            ],
        }
    }

    #[test]
    fn chain_commit_roundtrip() {
        let frame = sample_chain_commit();
        let bytes = frame.encode().unwrap();
        assert_eq!(bytes.len(), frame.total_frame_bytes());
        let decoded = ChainCommitFrame::decode(&bytes, frame.salt1, frame.salt2)
            .unwrap()
            .expect("round-trip must decode");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn chain_commit_empty_payload_roundtrip() {
        let frame = ChainCommitFrame {
            salt1: 1,
            salt2: 2,
            commit_ts: Ts { physical_ms: 0, logical: 0 },
            refcount_deltas: vec![],
            page_writes: vec![],
        };
        let bytes = frame.encode().unwrap();
        // 32-byte fixed header + 4-byte page_write_count + 4-byte CRC = 40.
        assert_eq!(bytes.len(), 40);
        assert_eq!(frame.total_frame_bytes(), 40);
        let decoded = ChainCommitFrame::decode(&bytes, 1, 2).unwrap().expect("decode");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn chain_commit_total_frame_bytes_bound_min() {
        let frame = ChainCommitFrame {
            salt1: 1,
            salt2: 2,
            commit_ts: Ts::PENDING,
            refcount_deltas: vec![],
            page_writes: vec![],
        };
        // Minimum: 32-byte fixed header + 4-byte page_write_count + 4-byte CRC.
        assert_eq!(frame.total_frame_bytes(), 40);
    }

    #[test]
    fn chain_commit_truncation_returns_none() {
        let frame = sample_chain_commit();
        let bytes = frame.encode().unwrap();
        // Every prefix shorter than total must decode as None, not panic.
        for n in 0..bytes.len() {
            let res = ChainCommitFrame::decode(&bytes[..n], frame.salt1, frame.salt2).unwrap();
            assert!(res.is_none(), "prefix of length {n} should be truncated");
        }
        // Full-length must succeed.
        let full = ChainCommitFrame::decode(&bytes, frame.salt1, frame.salt2)
            .unwrap()
            .expect("full decode");
        assert_eq!(full, frame);
    }

    #[test]
    fn chain_commit_salt_mismatch_returns_none() {
        let frame = sample_chain_commit();
        let bytes = frame.encode().unwrap();
        assert!(ChainCommitFrame::decode(&bytes, 0, frame.salt2).unwrap().is_none());
        assert!(ChainCommitFrame::decode(&bytes, frame.salt1, 0).unwrap().is_none());
    }

    #[test]
    fn chain_commit_corrupt_checksum_returns_none() {
        let frame = sample_chain_commit();
        let mut bytes = frame.encode().unwrap();
        // Flip a byte in the body — CRC must now reject.
        bytes[40] ^= 0xFF;
        let res = ChainCommitFrame::decode(&bytes, frame.salt1, frame.salt2).unwrap();
        assert!(res.is_none(), "corrupt body must fail CRC and return None");
    }

    #[test]
    fn chain_commit_bad_frame_kind_returns_none() {
        let frame = sample_chain_commit();
        let mut bytes = frame.encode().unwrap();
        bytes[0] = 0xFF;
        assert!(
            ChainCommitFrame::decode(&bytes, frame.salt1, frame.salt2).unwrap().is_none(),
            "wrong frame_kind must return None, not parse as ChainCommit"
        );
    }

    #[test]
    fn chain_commit_bogus_length_prefix_returns_none() {
        let frame = sample_chain_commit();
        let mut bytes = frame.encode().unwrap();
        // total_frame_bytes field at offset 4..8 — set beyond MAX to hit the bound.
        let bogus = (CHAIN_COMMIT_MAX_FRAME_SIZE as u64 + 1) as u32;
        bytes[4..8].copy_from_slice(&bogus.to_le_bytes());
        assert!(
            ChainCommitFrame::decode(&bytes, frame.salt1, frame.salt2).unwrap().is_none(),
            "length prefix above MAX must reject before reading any count"
        );
    }

    // -----------------------------------------------------------------
    // try_skip_chain_commit
    // -----------------------------------------------------------------

    #[test]
    fn try_skip_chain_commit_advances_past_valid_frame() {
        let frame = sample_chain_commit();
        let expected_ts = frame.commit_ts;
        let bytes = frame.encode().unwrap();
        let mut cursor = std::io::Cursor::new(bytes.clone());
        let (n, ts) = try_skip_chain_commit(&mut cursor, frame.salt1, frame.salt2)
            .unwrap()
            .expect("valid frame must be skipped");
        assert_eq!(n as usize, bytes.len());
        assert_eq!(cursor.position() as usize, bytes.len());
        assert_eq!(ts, expected_ts, "commit_ts must be carried out of the scan");
    }

    #[test]
    fn try_skip_chain_commit_rewinds_on_legacy_frame() {
        // A legacy JournalFrameHeader should NOT look like a ChainCommit —
        // helper must restore position and return None.
        let legacy = JournalFrameHeader {
            page_number: 42,
            db_page_count: 100,
            salt1: 0xDEAD_BEEF,
            salt2: 0xCAFE_BABE,
            page_size: JournalPageSize::Small4k,
        };
        let page_data = vec![0u8; PAGE_SIZE_INTERNAL as usize];
        let mut buf = Vec::new();
        legacy.write(&mut buf, &page_data).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let result = try_skip_chain_commit(&mut cursor, 0xDEAD_BEEF, 0xCAFE_BABE).unwrap();
        assert!(result.is_none(), "legacy page_number=42 frame must not look like ChainCommit");
        assert_eq!(cursor.position(), 0, "cursor must be restored on non-ChainCommit");
    }

    #[test]
    fn try_skip_chain_commit_disambiguates_legacy_page_number_two() {
        // Legacy frame with page_number=2: first 4 bytes are [2,0,0,0], the
        // same as a ChainCommit header prefix. CRC check must reject.
        let legacy = JournalFrameHeader {
            page_number: 2,
            db_page_count: 1,
            salt1: 0xDEAD_BEEF,
            salt2: 0xCAFE_BABE,
            page_size: JournalPageSize::Small4k,
        };
        let page_data = vec![0xAAu8; PAGE_SIZE_INTERNAL as usize];
        let mut buf = Vec::new();
        legacy.write(&mut buf, &page_data).unwrap();

        let mut cursor = std::io::Cursor::new(buf);
        let result = try_skip_chain_commit(&mut cursor, 0xDEAD_BEEF, 0xCAFE_BABE).unwrap();
        assert!(
            result.is_none(),
            "page_number=2 legacy frame must be rejected via CRC disambiguation"
        );
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn try_skip_chain_commit_rewinds_on_salt_mismatch() {
        let frame = sample_chain_commit();
        let bytes = frame.encode().unwrap();
        let mut cursor = std::io::Cursor::new(bytes);
        let result = try_skip_chain_commit(&mut cursor, 0, 0).unwrap();
        assert!(result.is_none(), "wrong salts must return None");
        assert_eq!(cursor.position(), 0, "cursor restored on salt mismatch");
    }

    #[test]
    fn try_skip_chain_commit_rewinds_on_truncated_buffer() {
        let frame = sample_chain_commit();
        let bytes = frame.encode().unwrap();
        // Truncate inside the variable tail.
        let truncated = bytes[..bytes.len() - 10].to_vec();
        let mut cursor = std::io::Cursor::new(truncated);
        let result = try_skip_chain_commit(&mut cursor, frame.salt1, frame.salt2).unwrap();
        assert!(result.is_none(), "truncated frame must return None");
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn try_skip_chain_commit_handles_eof() {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        let result = try_skip_chain_commit(&mut cursor, 1, 2).unwrap();
        assert!(result.is_none(), "EOF returns None");
        assert_eq!(cursor.position(), 0);
    }

    #[test]
    fn chain_commit_inflated_delta_count_returns_none() {
        // An attacker-crafted frame whose refcount_delta_count claims more
        // deltas than the length prefix can accommodate must be rejected
        // before any out-of-bounds indexing.
        let frame = ChainCommitFrame {
            salt1: 1,
            salt2: 2,
            commit_ts: Ts::PENDING,
            refcount_deltas: vec![],
            page_writes: vec![],
        };
        let mut bytes = frame.encode().unwrap();
        // Poke refcount_delta_count = 1000 at offset 28..32 without resizing.
        bytes[28..32].copy_from_slice(&1000u32.to_le_bytes());
        let res = ChainCommitFrame::decode(&bytes, 1, 2).unwrap();
        assert!(res.is_none(), "count exceeding length prefix must return None");
    }
