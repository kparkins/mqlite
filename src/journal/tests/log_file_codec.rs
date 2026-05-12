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
fn journal_page_size_roundtrip() {
    assert_eq!(
        JournalPageSize::from_u32(4096).unwrap(),
        JournalPageSize::Small4k
    );
    assert_eq!(
        JournalPageSize::from_u32(32768).unwrap(),
        JournalPageSize::Large32k
    );
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
        commit_ts: Ts {
            physical_ms: 0,
            logical: 0,
        },
        refcount_deltas: vec![],
        page_writes: vec![],
    };
    let bytes = frame.encode().unwrap();
    // 32-byte fixed header + 4-byte page_write_count + 4-byte CRC = 40.
    assert_eq!(bytes.len(), 40);
    assert_eq!(frame.total_frame_bytes(), 40);
    let decoded = ChainCommitFrame::decode(&bytes, 1, 2)
        .unwrap()
        .expect("decode");
    assert_eq!(decoded, frame);
}

#[test]
fn chain_commit_total_frame_bytes_bound_min() {
    let frame = ChainCommitFrame {
        salt1: 1,
        salt2: 2,
        commit_ts: Ts::default(),
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
    assert!(ChainCommitFrame::decode(&bytes, 0, frame.salt2)
        .unwrap()
        .is_none());
    assert!(ChainCommitFrame::decode(&bytes, frame.salt1, 0)
        .unwrap()
        .is_none());
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
        ChainCommitFrame::decode(&bytes, frame.salt1, frame.salt2)
            .unwrap()
            .is_none(),
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
        ChainCommitFrame::decode(&bytes, frame.salt1, frame.salt2)
            .unwrap()
            .is_none(),
        "length prefix above MAX must reject before reading any count"
    );
}

// -----------------------------------------------------------------
// ChainCommit cursor reader
// -----------------------------------------------------------------

#[test]
fn chain_commit_cursor_reader_advances_past_valid_frame() {
    let frame = sample_chain_commit();
    let expected_ts = frame.commit_ts;
    let bytes = frame.encode().unwrap();
    let mut prefixed = vec![0xAA; 13];
    prefixed.extend_from_slice(&bytes);
    let mut cursor = std::io::Cursor::new(prefixed);
    cursor.set_position(13);
    let (n, ts, offset) = read_chain_commit_at_cursor(&mut cursor, frame.salt1, frame.salt2)
        .unwrap()
        .expect("valid frame must be skipped");
    assert_eq!(n as usize, bytes.len());
    assert_eq!(cursor.position() as usize, 13 + bytes.len());
    assert_eq!(ts, expected_ts, "commit_ts must be carried out of the scan");
    assert_eq!(offset, 13, "start offset must be carried out of the scan");
}

#[test]
fn chain_commit_cursor_reader_rewinds_on_salt_mismatch() {
    let frame = sample_chain_commit();
    let bytes = frame.encode().unwrap();
    let mut cursor = std::io::Cursor::new(bytes);
    let result = read_chain_commit_at_cursor(&mut cursor, 0, 0).unwrap();
    assert!(result.is_none(), "wrong salts must return None");
    assert_eq!(cursor.position(), 0, "cursor restored on salt mismatch");
}

#[test]
fn chain_commit_cursor_reader_rewinds_on_truncated_buffer() {
    let frame = sample_chain_commit();
    let bytes = frame.encode().unwrap();
    // Truncate inside the variable tail.
    let truncated = bytes[..bytes.len() - 10].to_vec();
    let mut cursor = std::io::Cursor::new(truncated);
    let result = read_chain_commit_at_cursor(&mut cursor, frame.salt1, frame.salt2).unwrap();
    assert!(result.is_none(), "truncated frame must return None");
    assert_eq!(cursor.position(), 0);
}

#[test]
fn chain_commit_cursor_reader_handles_eof() {
    let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
    let result = read_chain_commit_at_cursor(&mut cursor, 1, 2).unwrap();
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
        commit_ts: Ts::default(),
        refcount_deltas: vec![],
        page_writes: vec![],
    };
    let mut bytes = frame.encode().unwrap();
    // Poke refcount_delta_count = 1000 at offset 28..32 without resizing.
    bytes[28..32].copy_from_slice(&1000u32.to_le_bytes());
    let res = ChainCommitFrame::decode(&bytes, 1, 2).unwrap();
    assert!(
        res.is_none(),
        "count exceeding length prefix must return None"
    );
}

#[test]
fn logical_txn_constants_have_expected_values() {
    assert_eq!(FRAME_KIND_LOGICAL_TXN, 0x03_u8);
    assert_eq!(LOGICAL_TXN_FIXED_HEADER_LEN, 48_usize);
    assert_eq!(LOGICAL_TXN_MAX_FRAME_SIZE, 67_108_864_usize);
    assert_eq!(LOGICAL_TXN_MIN_FRAME_SIZE, 52_usize);
    assert_eq!(LOGICAL_TXN_MIN_FRAME_SIZE, LOGICAL_TXN_FIXED_HEADER_LEN + 4);
    assert_eq!(LOGICAL_TXN_MAX_OP_COUNT, 1_000_000_usize);
    assert_eq!(LOGICAL_TXN_MAX_KEY_BYTES, 16_384_usize);
    assert_eq!(LOGICAL_TXN_MAX_VALUE_BYTES, 16_777_216_usize);
    assert_eq!(LOGICAL_TXN_FORMAT_VERSION, 1_u16);
}

#[test]
fn logical_op_kind_variants_match_opcodes() {
    // Construct one LogicalOp of each of the five §4.2 variants and assert
    // structural equality via Clone + PartialEq. Pins the US-003 shape
    // contract: op_ordinal carrier, LogicalOpKind variant set with i64
    // ns_id/index_id, Option<OverflowRefWire> on primary writes, id_bytes
    // on secondary insert, and the absence of a refcount_deltas field on
    // LogicalTxnFrame.
    let primary_insert = LogicalOp {
        op_ordinal: 0,
        kind: LogicalOpKind::PrimaryInsert {
            ns_id: 1,
            key: vec![0x01, 0x02],
            value: vec![0xAA, 0xBB, 0xCC],
            overflow: None,
        },
    };
    let primary_update = LogicalOp {
        op_ordinal: 1,
        kind: LogicalOpKind::PrimaryUpdate {
            ns_id: i64::MIN,
            key: vec![0x03],
            value: Vec::new(),
            overflow: Some(OverflowRefWire {
                first_page: 42,
                total_len: 1u64 << 40,
            }),
        },
    };
    let primary_delete = LogicalOp {
        op_ordinal: 2,
        kind: LogicalOpKind::PrimaryDelete {
            ns_id: -7,
            key: vec![0xFF, 0xEE],
        },
    };
    let secondary_insert = LogicalOp {
        op_ordinal: 3,
        kind: LogicalOpKind::SecondaryInsert {
            index_id: 100,
            key: vec![0x10, 0x20],
            id_bytes: vec![0x30, 0x40, 0x50],
        },
    };
    let secondary_delete = LogicalOp {
        op_ordinal: 4,
        kind: LogicalOpKind::SecondaryDelete {
            index_id: i64::MAX,
            key: vec![0x99],
        },
    };

    for op in [
        &primary_insert,
        &primary_update,
        &primary_delete,
        &secondary_insert,
        &secondary_delete,
    ] {
        assert_eq!(op, &op.clone(), "Clone + PartialEq must round-trip");
    }

    // Variants are distinct from each other.
    assert_ne!(primary_insert.kind, primary_update.kind);
    assert_ne!(primary_insert.kind, primary_delete.kind);
    assert_ne!(primary_update.kind, primary_delete.kind);
    assert_ne!(secondary_insert.kind, secondary_delete.kind);
    assert_ne!(primary_insert.kind, secondary_insert.kind);

    // LogicalTxnFrame has the §4.2 field shape and clones structurally.
    let frame = LogicalTxnFrame {
        salt1: 0xDEAD_BEEF,
        salt2: 0xCAFE_BABE,
        commit_ts: Ts {
            physical_ms: 1_234,
            logical: 5,
        },
        diagnostic_txn_id: 42,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![
            secondary_insert,
            secondary_delete,
            primary_insert.clone(),
            primary_update,
            primary_delete,
        ],
    };
    assert_eq!(frame, frame.clone());
    assert_eq!(frame.ops.len(), 5);
    assert_eq!(frame.format_version, 1);
    assert_eq!(frame.flags, 0);

    // Exercise Debug so the #[derive(Debug)] bound is not vacuous.
    let _ = format!("{frame:?}");
    let _ = format!("{primary_insert:?}");

    // DecodeCtx has both §4.6 shapes and derives Clone + PartialEq + Eq.
    assert_eq!(DecodeCtx::Scanning, DecodeCtx::Scanning);
    assert_eq!(
        DecodeCtx::MidStream { follower: true },
        DecodeCtx::MidStream { follower: true }.clone()
    );
    assert_ne!(
        DecodeCtx::MidStream { follower: true },
        DecodeCtx::MidStream { follower: false }
    );
    assert_ne!(
        DecodeCtx::Scanning,
        DecodeCtx::MidStream { follower: false }
    );
}

#[test]
fn logical_txn_encode_fixed_header_offsets_are_stable() {
    // Encode a 0-op frame and assert every fixed-header byte by offset per
    // §4.1. Also asserts the CRC32C covers bytes [0 .. total_frame_bytes-4)
    // per the same spec section.
    let frame = LogicalTxnFrame {
        salt1: 0xDEAD_BEEF,
        salt2: 0xCAFE_BABE,
        commit_ts: Ts {
            physical_ms: 0x0011_2233_4455_6677,
            logical: 0x89AB_CDEF,
        },
        diagnostic_txn_id: 0x1122_3344_5566_7788,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![],
    };
    let bytes = frame.encode().unwrap();
    assert_eq!(
        bytes.len(),
        LOGICAL_TXN_MIN_FRAME_SIZE,
        "0-op frame must be exactly the minimum frame size (52 bytes)"
    );

    // frame_kind at byte 0.
    assert_eq!(bytes[0], FRAME_KIND_LOGICAL_TXN);
    // reserved_a at bytes 1..4 — MUST be [0, 0, 0].
    assert_eq!(&bytes[1..4], &[0u8, 0u8, 0u8]);
    // total_frame_bytes at bytes 4..8.
    assert_eq!(
        u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize,
        LOGICAL_TXN_MIN_FRAME_SIZE
    );
    // salt1 at bytes 8..12.
    assert_eq!(
        u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
        0xDEAD_BEEF
    );
    // salt2 at bytes 12..16.
    assert_eq!(
        u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
        0xCAFE_BABE
    );
    // commit_ts at bytes 16..28: physical_ms (u64) then logical (u32).
    assert_eq!(&bytes[16..24], &0x0011_2233_4455_6677u64.to_le_bytes()[..]);
    assert_eq!(&bytes[24..28], &0x89AB_CDEFu32.to_le_bytes()[..]);
    // diagnostic_txn_id at bytes 28..36.
    assert_eq!(
        u64::from_le_bytes(bytes[28..36].try_into().unwrap()),
        0x1122_3344_5566_7788
    );
    // format_version at bytes 36..38.
    assert_eq!(
        u16::from_le_bytes(bytes[36..38].try_into().unwrap()),
        LOGICAL_TXN_FORMAT_VERSION
    );
    // flags at bytes 38..40.
    assert_eq!(u16::from_le_bytes(bytes[38..40].try_into().unwrap()), 0);
    // op_count at bytes 40..44.
    assert_eq!(u32::from_le_bytes(bytes[40..44].try_into().unwrap()), 0);
    // reserved_b at bytes 44..48 — MUST be zero (§3.9).
    assert_eq!(u32::from_le_bytes(bytes[44..48].try_into().unwrap()), 0);
    // CRC32C at bytes 48..52 covers bytes [0..48).
    let expected_crc = crc32c::crc32c(&bytes[..48]);
    assert_eq!(
        u32::from_le_bytes(bytes[48..52].try_into().unwrap()),
        expected_crc,
        "trailing CRC32C must cover [0 .. total_frame_bytes - 4)"
    );
}

#[test]
fn logical_txn_encode_rejects_oversize() {
    // Build a frame whose computed total_frame_bytes strictly exceeds
    // LOGICAL_TXN_MAX_FRAME_SIZE. A single PrimaryInsert with a value
    // sized equal to LOGICAL_TXN_MAX_FRAME_SIZE forces the total past the
    // cap: fixed_header(48) + op_prefix(8) + ns_id(8) + key_len(4) +
    // value_len(4) + value(LOGICAL_TXN_MAX_FRAME_SIZE) + overflow_flag(1)
    // + crc(4) = LOGICAL_TXN_MAX_FRAME_SIZE + 77.
    let big_value = vec![0u8; LOGICAL_TXN_MAX_FRAME_SIZE];
    let frame = LogicalTxnFrame {
        salt1: 1,
        salt2: 2,
        commit_ts: Ts::default(),
        diagnostic_txn_id: 0,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![LogicalOp {
            op_ordinal: 0,
            kind: LogicalOpKind::PrimaryInsert {
                ns_id: 1,
                key: Vec::new(),
                value: big_value,
                overflow: None,
            },
        }],
    };

    let computed_total = frame.total_frame_bytes();
    assert!(
        computed_total > LOGICAL_TXN_MAX_FRAME_SIZE,
        "precondition: test frame must exceed the cap (was {computed_total})"
    );

    match frame.encode() {
        Err(crate::error::Error::JournalFrameTooLarge {
            logical_frame_bytes,
            max_bytes,
        }) => {
            assert_eq!(logical_frame_bytes, computed_total);
            assert_eq!(max_bytes, LOGICAL_TXN_MAX_FRAME_SIZE);
        }
        other => panic!(
            "expected Err(JournalFrameTooLarge), got {other:?}; encoder \
             must bail before appending any byte on oversize frames"
        ),
    }
}

// -----------------------------------------------------------------
// LogicalTxnFrame::decode — §4.6 disposition tests (US-005)
// -----------------------------------------------------------------

const TEST_SALT1: u32 = 0xDEAD_BEEF;
const TEST_SALT2: u32 = 0xCAFE_BABE;

fn sample_empty_logical_frame() -> LogicalTxnFrame {
    LogicalTxnFrame {
        salt1: TEST_SALT1,
        salt2: TEST_SALT2,
        commit_ts: Ts {
            physical_ms: 0x0011_2233_4455_6677,
            logical: 0x89AB_CDEF,
        },
        diagnostic_txn_id: 0x1122_3344_5566_7788,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![],
    }
}

fn sample_mixed_ops_logical_frame() -> LogicalTxnFrame {
    LogicalTxnFrame {
        salt1: TEST_SALT1,
        salt2: TEST_SALT2,
        commit_ts: Ts {
            physical_ms: 42,
            logical: 7,
        },
        diagnostic_txn_id: 0xAA55_AA55_AA55_AA55,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        // Staging order: secondary first, primary second per §3.6.
        // Ordinals are dense 0..=4 but deliberately not in-order to prove
        // the decoder accepts any dense permutation.
        ops: vec![
            LogicalOp {
                op_ordinal: 3,
                kind: LogicalOpKind::SecondaryInsert {
                    index_id: i64::MAX,
                    key: vec![0xAA, 0xBB],
                    id_bytes: vec![0x01, 0x02, 0x03],
                },
            },
            LogicalOp {
                op_ordinal: 4,
                kind: LogicalOpKind::SecondaryDelete {
                    index_id: -42,
                    key: vec![0xCC],
                },
            },
            LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::PrimaryInsert {
                    ns_id: 1,
                    key: vec![0x10, 0x20, 0x30],
                    value: vec![0xDE, 0xAD, 0xBE, 0xEF],
                    overflow: None,
                },
            },
            LogicalOp {
                op_ordinal: 1,
                kind: LogicalOpKind::PrimaryUpdate {
                    ns_id: i64::MIN,
                    key: vec![0x40],
                    value: vec![],
                    overflow: Some(OverflowRefWire {
                        first_page: 1_234,
                        total_len: 1u64 << 40,
                    }),
                },
            },
            LogicalOp {
                op_ordinal: 2,
                kind: LogicalOpKind::PrimaryDelete {
                    ns_id: -1,
                    key: vec![0x50, 0x60, 0x70, 0x80],
                },
            },
        ],
    }
}

/// Recompute CRC32C in-place over `[0 .. len - 4)` after a body mutation.
fn fix_crc(bytes: &mut [u8]) {
    let body_end = bytes.len() - 4;
    let cs = crc32c::crc32c(&bytes[..body_end]);
    bytes[body_end..body_end + 4].copy_from_slice(&cs.to_le_bytes());
}

/// Assert `decode(bytes, .., Scanning)` returns `Ok(None)`.
fn assert_scanning_none(bytes: &[u8]) {
    let got = LogicalTxnFrame::decode(bytes, TEST_SALT1, TEST_SALT2, DecodeCtx::Scanning)
        .expect("Scanning must not Err");
    assert!(
        got.is_none(),
        "Scanning must return Ok(None), got Ok(Some(_))"
    );
}

/// Assert `decode(bytes, .., MidStream { follower: true })` returns
/// `Err(CorruptDatabase { recoverable: expected_recoverable, .. })`.
fn assert_midstream_err(bytes: &[u8], expected_recoverable: bool) {
    let got = LogicalTxnFrame::decode(
        bytes,
        TEST_SALT1,
        TEST_SALT2,
        DecodeCtx::MidStream { follower: true },
    );
    match got {
        Err(crate::error::Error::CorruptDatabase { recoverable, .. }) => assert_eq!(
            recoverable, expected_recoverable,
            "recoverable flag must match §4.6 table"
        ),
        other => panic!("expected Err(CorruptDatabase), got {other:?}"),
    }
}

/// Assert `decode(bytes, .., MidStream { follower: true })` returns
/// `Ok(None)` — tail-like failure row per §4.6.
fn assert_midstream_none(bytes: &[u8]) {
    let got = LogicalTxnFrame::decode(
        bytes,
        TEST_SALT1,
        TEST_SALT2,
        DecodeCtx::MidStream { follower: true },
    )
    .expect("tail-like MidStream must not Err");
    assert!(got.is_none(), "tail-like MidStream must return Ok(None)");
}

#[test]
fn logical_txn_encode_decode_roundtrip_empty_ops() {
    let frame = sample_empty_logical_frame();
    let bytes = frame.encode().unwrap();
    let decoded = LogicalTxnFrame::decode(&bytes, TEST_SALT1, TEST_SALT2, DecodeCtx::Scanning)
        .unwrap()
        .expect("empty-ops frame must round-trip");
    assert_eq!(decoded, frame);

    // MidStream must also succeed on a valid frame.
    let decoded_mid = LogicalTxnFrame::decode(
        &bytes,
        TEST_SALT1,
        TEST_SALT2,
        DecodeCtx::MidStream { follower: true },
    )
    .unwrap()
    .expect("MidStream must succeed on a valid frame");
    assert_eq!(decoded_mid, frame);
}

#[test]
fn logical_txn_encode_decode_roundtrip_mixed_ops() {
    let frame = sample_mixed_ops_logical_frame();
    let bytes = frame.encode().unwrap();
    let decoded = LogicalTxnFrame::decode(&bytes, TEST_SALT1, TEST_SALT2, DecodeCtx::Scanning)
        .unwrap()
        .expect("mixed-ops frame must round-trip");
    assert_eq!(decoded, frame);

    // ns_id / index_id round-trip as i64 — cover negative and i64::MIN /
    // i64::MAX boundaries per US-019.
    let mut saw_min = false;
    let mut saw_max = false;
    let mut saw_negative = false;
    for op in &decoded.ops {
        match &op.kind {
            LogicalOpKind::PrimaryUpdate { ns_id, .. } if *ns_id == i64::MIN => {
                saw_min = true;
            }
            LogicalOpKind::SecondaryInsert { index_id, .. } if *index_id == i64::MAX => {
                saw_max = true;
            }
            LogicalOpKind::SecondaryDelete { index_id, .. } if *index_id < 0 => {
                saw_negative = true;
            }
            LogicalOpKind::PrimaryDelete { ns_id, .. } if *ns_id < 0 => {
                saw_negative = true;
            }
            _ => {}
        }
    }
    assert!(saw_min && saw_max && saw_negative);
}

// --- §4.6 row: truncated mid-header (both contexts → Ok(None)) ----------

#[test]
fn logical_txn_decode_midstream_rewinds_on_truncated_mid_header() {
    // buf.len() < LOGICAL_TXN_FIXED_HEADER_LEN — tail truncation.
    let short: [u8; LOGICAL_TXN_FIXED_HEADER_LEN - 1] = [0u8; LOGICAL_TXN_FIXED_HEADER_LEN - 1];
    assert_scanning_none(&short);
    assert_midstream_none(&short);
}

// --- §4.6 row: frame_kind != 0x03 (both → Ok(None)) ---------------------

#[test]
fn logical_txn_decode_scanning_rewinds_on_wrong_kind() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    bytes[0] = FRAME_KIND_CHAIN_COMMIT; // 0x02 — dispatch mismatch.
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_rewinds_on_wrong_kind() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    bytes[0] = FRAME_KIND_CHAIN_COMMIT;
    assert_midstream_none(&bytes);
}

// --- §4.6 row: non-zero reserved_a --------------------------------------

#[test]
fn logical_txn_decode_scanning_rewinds_on_reserved_nonzero() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    bytes[1] = 0x01;
    fix_crc(&mut bytes);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_reserved_nonzero() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    bytes[2] = 0xFF;
    fix_crc(&mut bytes);
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.6 row: total_frame_bytes out of range ---------------------------

#[test]
fn logical_txn_decode_scanning_rewinds_on_length_over_cap() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    let bogus = (LOGICAL_TXN_MAX_FRAME_SIZE as u32).wrapping_add(1);
    bytes[4..8].copy_from_slice(&bogus.to_le_bytes());
    // total-range check fires before CRC so CRC mismatch is irrelevant.
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_length_over_cap() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    let bogus = (LOGICAL_TXN_MAX_FRAME_SIZE as u32).wrapping_add(1);
    bytes[4..8].copy_from_slice(&bogus.to_le_bytes());
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

#[test]
fn logical_txn_decode_scanning_rewinds_on_length_below_min() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    let too_small = (LOGICAL_TXN_MIN_FRAME_SIZE as u32).saturating_sub(1);
    bytes[4..8].copy_from_slice(&too_small.to_le_bytes());
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_length_below_min() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    let too_small = (LOGICAL_TXN_MIN_FRAME_SIZE as u32).saturating_sub(1);
    bytes[4..8].copy_from_slice(&too_small.to_le_bytes());
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.6 row: salt mismatch (both → Ok(None) ALWAYS) -------------------

#[test]
fn logical_txn_decode_scanning_rewinds_on_salt_mismatch() {
    let frame = sample_empty_logical_frame();
    let bytes = frame.encode().unwrap();
    let got = LogicalTxnFrame::decode(&bytes, 0, TEST_SALT2, DecodeCtx::Scanning).unwrap();
    assert!(got.is_none());
    let got2 = LogicalTxnFrame::decode(&bytes, TEST_SALT1, 0, DecodeCtx::Scanning).unwrap();
    assert!(got2.is_none());
}

#[test]
fn logical_txn_decode_midstream_rewinds_on_salt_mismatch() {
    // Even in MidStream, salt mismatch is ALWAYS Ok(None) per §4.6
    // (different database lifetime, never corruption).
    let frame = sample_empty_logical_frame();
    let bytes = frame.encode().unwrap();
    let got = LogicalTxnFrame::decode(
        &bytes,
        0,
        TEST_SALT2,
        DecodeCtx::MidStream { follower: true },
    )
    .expect("salt mismatch must not Err in MidStream");
    assert!(got.is_none());
    let got2 = LogicalTxnFrame::decode(
        &bytes,
        TEST_SALT1,
        0,
        DecodeCtx::MidStream { follower: false },
    )
    .expect("salt mismatch must not Err in MidStream");
    assert!(got2.is_none());
}

// --- §4.6 row: CRC mismatch ---------------------------------------------

#[test]
fn logical_txn_decode_scanning_rewinds_on_checksum_mismatch() {
    let frame = sample_mixed_ops_logical_frame();
    let mut bytes = frame.encode().unwrap();
    // Flip a byte in the op body; do NOT recompute CRC — that's the
    // whole point of this test.
    let mid = LOGICAL_TXN_FIXED_HEADER_LEN + 4;
    bytes[mid] ^= 0x5A;
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_checksum_mismatch() {
    let frame = sample_mixed_ops_logical_frame();
    let mut bytes = frame.encode().unwrap();
    let mid = LOGICAL_TXN_FIXED_HEADER_LEN + 4;
    bytes[mid] ^= 0x5A;
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.6 row: unknown format_version -----------------------------------

#[test]
fn logical_txn_decode_scanning_rewinds_on_unknown_format_version() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    bytes[36..38].copy_from_slice(&2u16.to_le_bytes());
    fix_crc(&mut bytes);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_unknown_format_version() {
    // Unknown format_version is the ONE recoverable:false row per §4.6.
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    bytes[36..38].copy_from_slice(&2u16.to_le_bytes());
    fix_crc(&mut bytes);
    assert_midstream_err(&bytes, /*recoverable=*/ false);
}

// --- §4.6 row: non-zero flags -------------------------------------------

#[test]
fn logical_txn_decode_scanning_rewinds_on_nonzero_flags() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    bytes[38..40].copy_from_slice(&1u16.to_le_bytes());
    fix_crc(&mut bytes);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_nonzero_flags() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    bytes[38..40].copy_from_slice(&0xFFFFu16.to_le_bytes());
    fix_crc(&mut bytes);
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.6 row: op_count > LOGICAL_TXN_MAX_OP_COUNT ----------------------

#[test]
fn logical_txn_decode_scanning_rewinds_on_op_count_over_cap() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    let bogus = (LOGICAL_TXN_MAX_OP_COUNT as u32).wrapping_add(1);
    bytes[40..44].copy_from_slice(&bogus.to_le_bytes());
    fix_crc(&mut bytes);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_op_count_over_cap() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    let bogus = (LOGICAL_TXN_MAX_OP_COUNT as u32).wrapping_add(1);
    bytes[40..44].copy_from_slice(&bogus.to_le_bytes());
    fix_crc(&mut bytes);
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.6 row: non-zero reserved_b (bytes 44..48) -----------------------

#[test]
fn logical_txn_decode_scanning_rewinds_on_reserved_b_nonzero() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    bytes[44..48].copy_from_slice(&1u32.to_le_bytes());
    fix_crc(&mut bytes);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_reserved_b_nonzero() {
    let frame = sample_empty_logical_frame();
    let mut bytes = frame.encode().unwrap();
    bytes[44..48].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
    fix_crc(&mut bytes);
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.6 row: key_len > LOGICAL_TXN_MAX_KEY_BYTES ----------------------

/// Build a one-op PrimaryDelete frame with a custom `key_len` field
/// written into the encoded bytes. The actual key bytes are NOT extended
/// — the frame becomes malformed but the length-field check is the first
/// check that fires, which is the behavior under test.
fn build_primary_delete_frame_with_bogus_key_len(bogus_key_len: u32) -> Vec<u8> {
    let frame = LogicalTxnFrame {
        salt1: TEST_SALT1,
        salt2: TEST_SALT2,
        commit_ts: Ts::default(),
        diagnostic_txn_id: 0,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![LogicalOp {
            op_ordinal: 0,
            kind: LogicalOpKind::PrimaryDelete {
                ns_id: 7,
                key: vec![],
            },
        }],
    };
    let mut bytes = frame.encode().unwrap();
    // PrimaryDelete body: fixed_header(48) + op_prefix(8) = 56; ns_id at
    // [56..64]; key_len at [64..68]; trailing 4 CRC bytes at end.
    let key_len_off = LOGICAL_TXN_FIXED_HEADER_LEN + LOGICAL_OP_PREFIX_LEN + 8;
    bytes[key_len_off..key_len_off + 4].copy_from_slice(&bogus_key_len.to_le_bytes());
    fix_crc(&mut bytes);
    bytes
}

#[test]
fn logical_txn_decode_scanning_rewinds_on_key_len_over_cap() {
    let bytes = build_primary_delete_frame_with_bogus_key_len(LOGICAL_TXN_MAX_KEY_BYTES as u32 + 1);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_key_len_over_cap() {
    let bytes = build_primary_delete_frame_with_bogus_key_len(LOGICAL_TXN_MAX_KEY_BYTES as u32 + 1);
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.6 row: value_len > LOGICAL_TXN_MAX_VALUE_BYTES ------------------

/// Build a one-op PrimaryInsert frame with a custom `value_len` field.
fn build_primary_insert_frame_with_bogus_value_len(bogus_value_len: u32) -> Vec<u8> {
    let frame = LogicalTxnFrame {
        salt1: TEST_SALT1,
        salt2: TEST_SALT2,
        commit_ts: Ts::default(),
        diagnostic_txn_id: 0,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![LogicalOp {
            op_ordinal: 0,
            kind: LogicalOpKind::PrimaryInsert {
                ns_id: 7,
                key: vec![],
                value: vec![],
                overflow: None,
            },
        }],
    };
    let mut bytes = frame.encode().unwrap();
    // PrimaryInsert body: fixed_header(48) + op_prefix(8) + ns_id(8) +
    // key_len(4) + key(0) + value_len(4) ... value_len lives at offset
    // 48 + 8 + 8 + 4 = 68.
    let value_len_off = LOGICAL_TXN_FIXED_HEADER_LEN + LOGICAL_OP_PREFIX_LEN + 8 + 4;
    bytes[value_len_off..value_len_off + 4].copy_from_slice(&bogus_value_len.to_le_bytes());
    fix_crc(&mut bytes);
    bytes
}

#[test]
fn logical_txn_decode_scanning_rewinds_on_value_len_over_cap() {
    let bytes =
        build_primary_insert_frame_with_bogus_value_len(LOGICAL_TXN_MAX_VALUE_BYTES as u32 + 1);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_value_len_over_cap() {
    let bytes =
        build_primary_insert_frame_with_bogus_value_len(LOGICAL_TXN_MAX_VALUE_BYTES as u32 + 1);
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.6 row: unknown op_kind ------------------------------------------

fn build_one_op_primary_delete_frame() -> (Vec<u8>, LogicalTxnFrame) {
    let frame = LogicalTxnFrame {
        salt1: TEST_SALT1,
        salt2: TEST_SALT2,
        commit_ts: Ts::default(),
        diagnostic_txn_id: 0,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![LogicalOp {
            op_ordinal: 0,
            kind: LogicalOpKind::PrimaryDelete {
                ns_id: 7,
                key: vec![],
            },
        }],
    };
    let bytes = frame.encode().unwrap();
    (bytes, frame)
}

#[test]
fn logical_txn_decode_scanning_rewinds_on_unknown_op_kind() {
    let (mut bytes, _) = build_one_op_primary_delete_frame();
    bytes[LOGICAL_TXN_FIXED_HEADER_LEN] = 0x99; // not in {0x01,0x02,0x03,0x11,0x12}
    fix_crc(&mut bytes);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_unknown_op_kind() {
    let (mut bytes, _) = build_one_op_primary_delete_frame();
    bytes[LOGICAL_TXN_FIXED_HEADER_LEN] = 0x7F;
    fix_crc(&mut bytes);
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.6 row: op_ordinal >= op_count -----------------------------------

#[test]
fn logical_txn_decode_scanning_rewinds_on_ordinal_out_of_range() {
    let (mut bytes, _) = build_one_op_primary_delete_frame();
    // op_ordinal at offset LOGICAL_TXN_FIXED_HEADER_LEN + 4.
    let ord_off = LOGICAL_TXN_FIXED_HEADER_LEN + 4;
    bytes[ord_off..ord_off + 4].copy_from_slice(&5u32.to_le_bytes()); // op_count = 1
    fix_crc(&mut bytes);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_ordinal_out_of_range() {
    let (mut bytes, _) = build_one_op_primary_delete_frame();
    let ord_off = LOGICAL_TXN_FIXED_HEADER_LEN + 4;
    bytes[ord_off..ord_off + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    fix_crc(&mut bytes);
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.6 row: non-dense ordinals (duplicate / gap) ---------------------

/// Build a 3-op frame of PrimaryDeletes with initially-dense ordinals
/// 0, 1, 2. Returns (bytes, per-op start offsets within the body).
fn build_three_op_primary_delete_frame() -> (Vec<u8>, [usize; 3]) {
    let frame = LogicalTxnFrame {
        salt1: TEST_SALT1,
        salt2: TEST_SALT2,
        commit_ts: Ts::default(),
        diagnostic_txn_id: 0,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![
            LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::PrimaryDelete {
                    ns_id: 1,
                    key: vec![],
                },
            },
            LogicalOp {
                op_ordinal: 1,
                kind: LogicalOpKind::PrimaryDelete {
                    ns_id: 2,
                    key: vec![],
                },
            },
            LogicalOp {
                op_ordinal: 2,
                kind: LogicalOpKind::PrimaryDelete {
                    ns_id: 3,
                    key: vec![],
                },
            },
        ],
    };
    let bytes = frame.encode().unwrap();
    // Each PrimaryDelete op with empty key: 8 (prefix) + 8 (ns_id) +
    // 4 (key_len) + 0 (key) = 20 bytes.
    let op_len = 20usize;
    let off0 = LOGICAL_TXN_FIXED_HEADER_LEN;
    let off1 = off0 + op_len;
    let off2 = off1 + op_len;
    (bytes, [off0, off1, off2])
}

#[test]
fn logical_txn_decode_scanning_rewinds_on_non_dense_ordinals() {
    let (mut bytes, offs) = build_three_op_primary_delete_frame();
    // Duplicate op_ordinal=0 by overwriting op 1's ordinal → ops have
    // ordinals [0, 0, 2] which is non-dense (gap at 1).
    let ord_off1 = offs[1] + 4;
    bytes[ord_off1..ord_off1 + 4].copy_from_slice(&0u32.to_le_bytes());
    fix_crc(&mut bytes);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_non_dense_ordinals() {
    let (mut bytes, offs) = build_three_op_primary_delete_frame();
    let ord_off2 = offs[2] + 4;
    bytes[ord_off2..ord_off2 + 4].copy_from_slice(&1u32.to_le_bytes());
    fix_crc(&mut bytes);
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.6 row: overflow_present not in {0, 1} ---------------------------

/// Build a one-op PrimaryInsert with no overflow; returns bytes and the
/// offset of the `overflow_present` byte.
fn build_primary_insert_with_overflow_present() -> (Vec<u8>, usize) {
    let frame = LogicalTxnFrame {
        salt1: TEST_SALT1,
        salt2: TEST_SALT2,
        commit_ts: Ts::default(),
        diagnostic_txn_id: 0,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![LogicalOp {
            op_ordinal: 0,
            kind: LogicalOpKind::PrimaryInsert {
                ns_id: 7,
                key: vec![],
                value: vec![],
                overflow: None,
            },
        }],
    };
    let bytes = frame.encode().unwrap();
    // Body layout: fixed_header(48) + op_prefix(8) + ns_id(8) +
    // key_len(4) + 0 + value_len(4) + 0 + overflow_present(1).
    // overflow_present at offset 48 + 8 + 8 + 4 + 4 = 72.
    let overflow_off = LOGICAL_TXN_FIXED_HEADER_LEN + LOGICAL_OP_PREFIX_LEN + 8 + 4 + 4;
    (bytes, overflow_off)
}

#[test]
fn logical_txn_decode_scanning_rewinds_on_overflow_present_invalid() {
    let (mut bytes, off) = build_primary_insert_with_overflow_present();
    bytes[off] = 0x02; // not in {0, 1}
    fix_crc(&mut bytes);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_overflow_present_invalid() {
    let (mut bytes, off) = build_primary_insert_with_overflow_present();
    bytes[off] = 0xFF;
    fix_crc(&mut bytes);
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.6 row: EOF mid-body (both → Ok(None)) ---------------------------

#[test]
fn logical_txn_decode_scanning_rewinds_on_eof_mid_body() {
    let frame = sample_mixed_ops_logical_frame();
    let bytes = frame.encode().unwrap();
    // Truncate inside the ops region.
    let truncate_at = LOGICAL_TXN_FIXED_HEADER_LEN + 4;
    let truncated = &bytes[..truncate_at];
    assert_scanning_none(truncated);
}

#[test]
fn logical_txn_decode_midstream_rewinds_on_eof_mid_body() {
    // Even in MidStream, EOF mid-body is a tail signal → Ok(None).
    let frame = sample_mixed_ops_logical_frame();
    let bytes = frame.encode().unwrap();
    let truncate_at = LOGICAL_TXN_FIXED_HEADER_LEN + 4;
    let truncated = &bytes[..truncate_at];
    assert_midstream_none(truncated);
}

// --- Exhaustive truncation property: every prefix in Scanning returns
//     Ok(None) (never Ok(Some) or panic).

#[test]
fn logical_txn_decode_scanning_rewinds_on_every_truncation_offset() {
    let frame = sample_mixed_ops_logical_frame();
    let bytes = frame.encode().unwrap();
    for n in 0..bytes.len() {
        let res = LogicalTxnFrame::decode(&bytes[..n], TEST_SALT1, TEST_SALT2, DecodeCtx::Scanning)
            .unwrap_or_else(|e| {
                panic!("Scanning must not Err on prefix {n}: got {e:?}");
            });
        assert!(
            res.is_none(),
            "prefix of length {n} must decode as Ok(None)"
        );
    }
    // Full length must succeed.
    let res = LogicalTxnFrame::decode(&bytes, TEST_SALT1, TEST_SALT2, DecodeCtx::Scanning)
        .unwrap()
        .expect("full-length frame must decode");
    assert_eq!(res, frame);
}

// --- §4.4 / §4.6 row: op-prefix reserved bytes non-zero -----------------

#[test]
fn logical_txn_decode_scanning_rewinds_on_op_prefix_reserved_nonzero() {
    let (mut bytes, _) = build_one_op_primary_delete_frame();
    // Op prefix at LOGICAL_TXN_FIXED_HEADER_LEN: op_kind(1) + reserved(3)
    // + op_ordinal(4). Non-zero reserved byte at offset +1.
    bytes[LOGICAL_TXN_FIXED_HEADER_LEN + 1] = 0x01;
    fix_crc(&mut bytes);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_op_prefix_reserved_nonzero() {
    let (mut bytes, _) = build_one_op_primary_delete_frame();
    bytes[LOGICAL_TXN_FIXED_HEADER_LEN + 3] = 0xFF;
    fix_crc(&mut bytes);
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

// --- §4.4.3 row: SecondaryInsert id_len bounded by VALUE cap, not KEY cap

/// Build a one-op SecondaryInsert frame with an empty key and a custom
/// `id_len` field written into the encoded bytes.
fn build_secondary_insert_frame_with_bogus_id_len(bogus_id_len: u32) -> Vec<u8> {
    let frame = LogicalTxnFrame {
        salt1: TEST_SALT1,
        salt2: TEST_SALT2,
        commit_ts: Ts::default(),
        diagnostic_txn_id: 0,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![LogicalOp {
            op_ordinal: 0,
            kind: LogicalOpKind::SecondaryInsert {
                index_id: 11,
                key: vec![],
                id_bytes: vec![],
            },
        }],
    };
    let mut bytes = frame.encode().unwrap();
    // SecondaryInsert body: fixed_header(48) + op_prefix(8) + index_id(8)
    // + key_len(4) + key(0) + id_len(4). id_len at offset 48+8+8+4 = 68.
    let id_len_off = LOGICAL_TXN_FIXED_HEADER_LEN + LOGICAL_OP_PREFIX_LEN + 8 + 4;
    bytes[id_len_off..id_len_off + 4].copy_from_slice(&bogus_id_len.to_le_bytes());
    fix_crc(&mut bytes);
    bytes
}

#[test]
fn logical_txn_decode_scanning_rewinds_on_id_bytes_len_over_cap() {
    let bytes =
        build_secondary_insert_frame_with_bogus_id_len(LOGICAL_TXN_MAX_VALUE_BYTES as u32 + 1);
    assert_scanning_none(&bytes);
}

#[test]
fn logical_txn_decode_midstream_errors_on_id_bytes_len_over_cap() {
    let bytes =
        build_secondary_insert_frame_with_bogus_id_len(LOGICAL_TXN_MAX_VALUE_BYTES as u32 + 1);
    assert_midstream_err(&bytes, /*recoverable=*/ true);
}

#[test]
fn logical_txn_secondary_insert_id_bytes_above_key_cap_round_trips() {
    // Proves the decoder honors the §4.4.3 id_len cap (VALUE_BYTES, not
    // KEY_BYTES): an id_bytes just above LOGICAL_TXN_MAX_KEY_BYTES must
    // round-trip cleanly.
    let big_id = vec![0xA5u8; LOGICAL_TXN_MAX_KEY_BYTES + 1];
    let frame = LogicalTxnFrame {
        salt1: TEST_SALT1,
        salt2: TEST_SALT2,
        commit_ts: Ts::default(),
        diagnostic_txn_id: 0,
        format_version: LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops: vec![LogicalOp {
            op_ordinal: 0,
            kind: LogicalOpKind::SecondaryInsert {
                index_id: 99,
                key: vec![0xDE, 0xAD],
                id_bytes: big_id.clone(),
            },
        }],
    };
    let bytes = frame.encode().unwrap();
    let decoded = LogicalTxnFrame::decode(&bytes, TEST_SALT1, TEST_SALT2, DecodeCtx::Scanning)
        .unwrap()
        .expect("id_bytes above key cap but within value cap must round-trip");
    match &decoded.ops[0].kind {
        LogicalOpKind::SecondaryInsert { id_bytes, .. } => {
            assert_eq!(id_bytes.len(), LOGICAL_TXN_MAX_KEY_BYTES + 1);
            assert_eq!(id_bytes, &big_id);
        }
        other => panic!("expected SecondaryInsert, got {other:?}"),
    }
}

// -----------------------------------------------------------------
// try_skip_logical_txn (US-006)
// -----------------------------------------------------------------

#[test]
fn try_skip_logical_txn_advances_on_valid_frame() {
    let frame = sample_mixed_ops_logical_frame();
    let bytes = frame.encode().unwrap();
    let mut cursor = std::io::Cursor::new(bytes.clone());
    let (n, decoded) = try_skip_logical_txn(&mut cursor, frame.salt1, frame.salt2)
        .unwrap()
        .expect("valid logical-txn frame must be skipped");
    assert_eq!(n as usize, bytes.len(), "consumed length matches frame");
    assert_eq!(
        cursor.position() as usize,
        bytes.len(),
        "reader is positioned at start + n after a successful skip"
    );
    assert_eq!(decoded, frame, "helper carries out the decoded frame");
}

#[test]
fn try_skip_logical_txn_rewinds_on_checksum_mismatch() {
    let frame = sample_mixed_ops_logical_frame();
    let mut bytes = frame.encode().unwrap();
    // Flip a byte inside the CRC-covered body without repairing the CRC —
    // §4.6 Scanning disposition rewinds and returns Ok(None).
    bytes[LOGICAL_TXN_FIXED_HEADER_LEN] ^= 0xFF;
    let mut cursor = std::io::Cursor::new(bytes);
    let result = try_skip_logical_txn(&mut cursor, frame.salt1, frame.salt2).unwrap();
    assert!(result.is_none(), "CRC mismatch must rewind in Scanning");
    assert_eq!(cursor.position(), 0, "cursor restored on CRC mismatch");
}

#[test]
fn try_skip_logical_txn_rewinds_on_salt_mismatch() {
    let frame = sample_mixed_ops_logical_frame();
    let bytes = frame.encode().unwrap();
    let mut cursor = std::io::Cursor::new(bytes);
    // Salt mismatch is always Ok(None) per §4.6.
    let result = try_skip_logical_txn(&mut cursor, 0, 0).unwrap();
    assert!(result.is_none(), "wrong salts must return None");
    assert_eq!(cursor.position(), 0, "cursor restored on salt mismatch");
}

// ---------------------------------------------------------------------------
// Phase 6 US-009: unsupported legacy-shaped byte negatives
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Page-0 checkpoint boundary codec
// ---------------------------------------------------------------------------

// ===========================================================================
// US-022 §9.1 — Property tests
// ===========================================================================
//
// Seven proptest-backed invariants. Each generates arbitrary well-formed
// `LogicalTxnFrame` values (and arbitrary corruption overlays for the
// negative tests) and asserts the documented decoder semantics.

#[cfg(test)]
mod prop_tests {
    use super::*;
    use proptest::prelude::*;

    /// Strategy that produces an arbitrary, well-formed `LogicalOp` with
    /// no overflow. Sizes are bounded so encoded frames stay well under
    /// the §4 cap.
    fn arb_logical_op() -> impl Strategy<Value = LogicalOp> {
        let key = ::proptest::collection::vec(any::<u8>(), 1..64);
        let value = ::proptest::collection::vec(any::<u8>(), 0..96);
        let ns_id = any::<i64>();
        let index_id = any::<i64>();
        let id_bytes = ::proptest::collection::vec(any::<u8>(), 1..32);

        prop_oneof![
            (key.clone(), value.clone(), ns_id).prop_map(|(key, value, ns_id)| LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::PrimaryInsert {
                    ns_id,
                    key,
                    value,
                    overflow: None,
                },
            }),
            (key.clone(), value, ns_id).prop_map(|(key, value, ns_id)| LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::PrimaryUpdate {
                    ns_id,
                    key,
                    value,
                    overflow: None,
                },
            }),
            (key.clone(), ns_id).prop_map(|(key, ns_id)| LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::PrimaryDelete { ns_id, key },
            }),
            (id_bytes, key.clone(), index_id).prop_map(|(id_bytes, key, index_id)| LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::SecondaryInsert {
                    index_id,
                    key,
                    id_bytes,
                },
            }),
            (key, index_id).prop_map(|(key, index_id)| LogicalOp {
                op_ordinal: 0,
                kind: LogicalOpKind::SecondaryDelete { index_id, key },
            }),
        ]
    }

    /// Recompute the trailing CRC32C of an encoded `LogicalTxnFrame`
    /// after a body byte has been mutated, so the decoder reaches
    /// the SEMANTIC validation step rather than rejecting on the CRC.
    /// Used by `prop_midstream_errs_on_impossible_content` to model
    /// `bad_flags` / `bad_op_kind` / `gap_ordinal` as content errors
    /// (codex US-022 round-1 blocker AC#4).
    fn recompute_logical_txn_crc(bytes: &mut [u8]) {
        let n = bytes.len();
        debug_assert!(n >= 4, "frame must include the trailing CRC");
        let crc = ::crc32c::crc32c(&bytes[..n - 4]);
        bytes[n - 4..n].copy_from_slice(&crc.to_le_bytes());
    }

    /// Strategy that produces an arbitrary, well-formed `LogicalTxnFrame`.
    /// Op ordinals are densified to 0..n once the ops vector is built.
    fn arb_logical_txn_frame() -> impl Strategy<Value = LogicalTxnFrame> {
        let ops = ::proptest::collection::vec(arb_logical_op(), 0..8);
        let physical_ms = any::<u64>();
        let logical = any::<u32>();
        let txn_id = any::<u64>();
        (ops, physical_ms, logical, txn_id).prop_map(|(mut ops, physical_ms, logical, txn_id)| {
            for (i, op) in ops.iter_mut().enumerate() {
                op.op_ordinal = i as u32;
            }
            LogicalTxnFrame {
                salt1: TEST_SALT1,
                salt2: TEST_SALT2,
                commit_ts: Ts {
                    physical_ms,
                    logical,
                },
                diagnostic_txn_id: txn_id,
                format_version: LOGICAL_TXN_FORMAT_VERSION,
                flags: 0,
                ops,
            }
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// §9.1 invariant 1 — encode then decode (Scanning) returns the
        /// exact same frame for any well-formed input.
        #[test]
        fn prop_encode_decode_roundtrip(frame in arb_logical_txn_frame()) {
            let bytes = frame.encode().expect("encode well-formed frame");
            let decoded =
                LogicalTxnFrame::decode(&bytes, TEST_SALT1, TEST_SALT2, DecodeCtx::Scanning)
                    .expect("Scanning must not Err on a valid frame")
                    .expect("Scanning must decode to Some(_) on a valid frame");
            prop_assert_eq!(decoded, frame);
        }

        /// §9.1 invariant 2 — `prop_truncation_safety`: every prefix
        /// `0..encode(frame).len()` decoded under `Scanning` is `Ok(None)`,
        /// never `Ok(Some(_))` and never a panic / Err.
        #[test]
        fn prop_truncation_safety(frame in arb_logical_txn_frame()) {
            let bytes = frame.encode().expect("encode well-formed frame");
            for n in 0..bytes.len() {
                let res =
                    LogicalTxnFrame::decode(&bytes[..n], TEST_SALT1, TEST_SALT2, DecodeCtx::Scanning);
                let res = res.unwrap_or_else(|e| {
                    panic!("Scanning must not Err on prefix {n}: {e:?}")
                });
                prop_assert!(
                    res.is_none(),
                    "prefix {} must decode as Ok(None), got Some(_)",
                    n
                );
            }
            // Full length must succeed.
            let full = LogicalTxnFrame::decode(
                &bytes,
                TEST_SALT1,
                TEST_SALT2,
                DecodeCtx::Scanning,
            )
            .expect("Scanning Ok")
            .expect("Scanning Some on full length");
            prop_assert_eq!(full, frame);
        }

        /// §9.1 invariant 3 — `prop_unknown_frame_skip_safety`: a
        /// well-formed `LogicalTxnFrame` whose first byte (frame_kind
        /// discriminant) has been overwritten with an unknown value
        /// must always decode as `Ok(None)` under `Scanning`. Models
        /// the linear-scan dispatch contract: foreign bytes never
        /// panic and never produce a phantom frame.
        ///
        /// Per AC#2 the input is a CORRUPTION OVERLAY derived from an
        /// encoded well-formed `LogicalTxnFrame` (codex US-022 r2
        /// blocker AC#2): we encode `arb_logical_txn_frame()` first,
        /// then apply the unknown-frame-kind overlay.
        #[test]
        fn prop_unknown_frame_skip_safety(
            frame in arb_logical_txn_frame(),
            unknown_kind in any::<u8>(),
        ) {
            // Reject the three legitimate frame-kind discriminants so
            // we genuinely test the "unknown kind" disposition row.
            prop_assume!(
                unknown_kind != 0x02 && unknown_kind != 0x03 && unknown_kind != 0x04
            );
            let mut bytes = frame.encode().expect("encode well-formed frame");
            // Corruption overlay: replace the frame_kind discriminant.
            bytes[0] = unknown_kind;
            let res =
                LogicalTxnFrame::decode(&bytes, TEST_SALT1, TEST_SALT2, DecodeCtx::Scanning);
            let res = res.unwrap_or_else(|e| {
                panic!("Scanning must not Err on unknown frame_kind: {e:?}")
            });
            prop_assert!(
                res.is_none(),
                "unknown frame_kind must decode as Ok(None)"
            );
        }

        /// §9.1 invariant 4 — `prop_salt_discrimination`: a valid frame
        /// stamped with salts S1/S2 must decode as `Ok(None)` when
        /// presented to the decoder with any DIFFERENT salts. Models
        /// the cross-database stale-journal protection from §A.2.
        #[test]
        fn prop_salt_discrimination(
            frame in arb_logical_txn_frame(),
            other_salt in any::<u32>(),
        ) {
            prop_assume!(other_salt != TEST_SALT1 && other_salt != TEST_SALT2);
            let bytes = frame.encode().expect("encode");
            let res = LogicalTxnFrame::decode(
                &bytes,
                other_salt,
                other_salt,
                DecodeCtx::Scanning,
            )
            .expect("Scanning must not Err on salt mismatch");
            prop_assert!(
                res.is_none(),
                "salt mismatch must decode as Ok(None) (cross-db protection)"
            );
        }

        /// §9.1 invariant 5 — `prop_non_matching_frame_invariance`: a
        /// well-formed `LogicalTxnFrame` whose first byte (frame_kind
        /// discriminant) has been overwritten with a DIFFERENT but
        /// VALID frame-kind discriminant (`0x02` ChainCommit or `0x04`
        /// CheckpointCommitBoundary) under `Scanning` always decodes
        /// as `Ok(None)`. Models the linear-scan dispatch contract:
        /// the LogicalTxn decoder defers to the next dispatch level
        /// when the structural signature does not match.
        ///
        /// Per AC#2 the input is a CORRUPTION OVERLAY derived from an
        /// encoded well-formed `LogicalTxnFrame` (codex US-022 r2
        /// blocker AC#2): we encode `arb_logical_txn_frame()` first,
        /// then apply the wrong-kind overlay using either of the two
        /// known non-LogicalTxn discriminants.
        #[test]
        fn prop_non_matching_frame_invariance(
            frame in arb_logical_txn_frame(),
            wrong_kind_choice in 0u8..2,
        ) {
            let wrong_kind = if wrong_kind_choice == 0 { 0x02u8 } else { 0x04u8 };
            let mut bytes = frame.encode().expect("encode well-formed frame");
            // Corruption overlay: replace the LogicalTxn discriminant
            // with a different valid frame-kind discriminant. The
            // decoder sees a non-LogicalTxn structural signature and
            // must defer (Ok(None)) without panicking.
            bytes[0] = wrong_kind;
            let res =
                LogicalTxnFrame::decode(&bytes, TEST_SALT1, TEST_SALT2, DecodeCtx::Scanning);
            let res = res.unwrap_or_else(|e| {
                panic!(
                    "Scanning must not Err on non-matching frame_kind 0x{wrong_kind:02x}: {e:?}"
                )
            });
            prop_assert!(res.is_none());
        }

        /// §9.1 invariant 6 — `prop_scanning_never_errs_on_truncation`:
        /// decoding ANY prefix of an encoded frame in `Scanning`
        /// context must never return `Err`. Subset of invariant 2 but
        /// stated independently per the §9.1 named-test list.
        #[test]
        fn prop_scanning_never_errs_on_truncation(frame in arb_logical_txn_frame()) {
            let bytes = frame.encode().expect("encode");
            for n in 0..bytes.len() {
                let res = LogicalTxnFrame::decode(
                    &bytes[..n],
                    TEST_SALT1,
                    TEST_SALT2,
                    DecodeCtx::Scanning,
                );
                prop_assert!(
                    res.is_ok(),
                    "Scanning must never Err on a truncation; prefix {} returned {:?}",
                    n,
                    res
                );
            }
        }

        /// §9.1 invariant 7 — `prop_midstream_errs_on_impossible_content`:
        /// a single, content-impossible corruption from the
        /// {bad_crc, bad_flags, bad_op_kind, gap_ordinal} set must be
        /// rejected with `Err(CorruptDatabase)` under
        /// `MidStream { follower: true }`. Models the §4.6 disposition
        /// table's content-error rows.
        ///
        /// `gap_ordinal` and `bad_op_kind` both require `op_count >= 1` (the
        /// gap variant additionally needs `op_count >= 2`); on smaller
        /// frames the strategy falls back to `bad_crc` so the test still
        /// exercises a content-error row.
        ///
        /// Modes 1/2/3 (`bad_flags`, `bad_op_kind`, `gap_ordinal`)
        /// recompute the CRC32C tail after the mutation so the decoder
        /// reaches the SEMANTIC validation step rather than rejecting on
        /// the CRC. Without recomputation, every mode would degenerate
        /// to "bad CRC" — codex round-1 flagged this.
        #[test]
        fn prop_midstream_errs_on_impossible_content(
            frame in arb_logical_txn_frame(),
            mode in 0u32..4
        ) {
            let mut bytes = frame.encode().expect("encode");
            // bad_op_kind needs at least one op; gap_ordinal needs >=2.
            let mode = match mode {
                2 if frame.ops.is_empty() => 0,
                3 if frame.ops.len() < 2 => 0,
                m => m,
            };
            let label = match mode {
                0 => {
                    let n = bytes.len();
                    bytes[n - 1] ^= 0x01;
                    "bad_crc"
                }
                1 => {
                    bytes[38] = 0x01;
                    recompute_logical_txn_crc(&mut bytes);
                    "bad_flags"
                }
                2 => {
                    bytes[LOGICAL_TXN_FIXED_HEADER_LEN] = 0xFF;
                    recompute_logical_txn_crc(&mut bytes);
                    "bad_op_kind"
                }
                3 => {
                    let first_op_ordinal_offset = LOGICAL_TXN_FIXED_HEADER_LEN + 4;
                    bytes[first_op_ordinal_offset..first_op_ordinal_offset + 4]
                        .copy_from_slice(&7u32.to_le_bytes());
                    recompute_logical_txn_crc(&mut bytes);
                    "gap_ordinal"
                }
                _ => panic!("unexpected mutation case"),
            };
            let res = LogicalTxnFrame::decode(
                &bytes,
                TEST_SALT1,
                TEST_SALT2,
                DecodeCtx::MidStream { follower: true },
            );
            prop_assert!(
                matches!(res, Err(crate::error::Error::CorruptDatabase { .. })),
                "MidStream must Err on impossible content ({}); got {:?}",
                label,
                res
            );
        }
    }
}
