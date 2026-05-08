use super::*;

const TEST_TS: u64 = 1_700_000_000_000; // arbitrary fixed timestamp

fn fresh_header() -> FileHeader {
    FileHeader::new(TEST_TS, 0xDEAD_BEEF, 0xCAFE_BABE)
}

// -----------------------------------------------------------------------
// Roundtrip
// -----------------------------------------------------------------------

#[test]
fn roundtrip_preserves_all_fields() {
    let original = fresh_header();
    let bytes = original.to_bytes();
    let decoded = FileHeader::from_bytes(&bytes).unwrap();

    assert_eq!(decoded.magic, FILE_MAGIC);
    assert_eq!(decoded.format_version, FORMAT_VERSION);
    assert_eq!(decoded.page_size_internal, 4096);
    assert_eq!(decoded.page_size_leaf, 32768);
    assert_eq!(decoded.created_at, TEST_TS);
    assert_eq!(decoded.last_checkpoint_ts, Ts::default());
    assert_eq!(decoded.catalog_root_page, 0);
    assert_eq!(decoded.free_list_head_4k, 0);
    assert_eq!(decoded.free_list_head_32k, 0);
    assert_eq!(decoded.total_page_count, 1);
    assert_eq!(decoded.free_page_count_4k, 0);
    assert_eq!(decoded.free_page_count_32k, 0);
    assert_eq!(decoded.checksum_algo, CHECKSUM_ALGO_CRC32C);
    assert_eq!(decoded.wal_salt1, 0xDEAD_BEEF);
    assert_eq!(decoded.wal_salt2, 0xCAFE_BABE);
    assert_eq!(decoded.catalog_root_backup, 0);
    assert_eq!(decoded.checkpoint_applied_lsn, 0);
}

#[test]
fn roundtrip_nonzero_counters() {
    let mut h = fresh_header();
    h.catalog_root_page = 99;
    h.free_list_head_4k = 12;
    h.free_list_head_32k = 33;
    h.total_page_count = 500;
    h.free_page_count_4k = 10;
    h.free_page_count_32k = 5;
    h.catalog_root_backup = 99;
    h.checkpoint_applied_lsn = 123_456;
    h.last_checkpoint_ts = Ts {
        physical_ms: 1_700_000_050_000,
        logical: 42,
    };

    let bytes = h.to_bytes();
    let decoded = FileHeader::from_bytes(&bytes).unwrap();

    assert_eq!(decoded.catalog_root_page, 99);
    assert_eq!(decoded.free_list_head_4k, 12);
    assert_eq!(decoded.total_page_count, 500);
    assert_eq!(decoded.catalog_root_backup, 99);
    assert_eq!(decoded.checkpoint_applied_lsn, 123_456);
    assert_eq!(
        decoded.last_checkpoint_ts,
        Ts {
            physical_ms: 1_700_000_050_000,
            logical: 42
        }
    );
}

// -----------------------------------------------------------------------
// Size
// -----------------------------------------------------------------------

#[test]
fn serialized_size_is_4096_bytes() {
    let bytes = fresh_header().to_bytes();
    assert_eq!(bytes.len(), 4096);
    assert_eq!(HEADER_PAGE_SIZE, 4096);
}

#[test]
fn bytes_beyond_128_are_zero() {
    let bytes = fresh_header().to_bytes();
    assert!(
        bytes[128..].iter().all(|&b| b == 0),
        "padding bytes must be zero"
    );
}

#[test]
fn reserved_bytes_110_to_127_are_zero() {
    // Phase 8 US-007 — offsets 102..110 now carry checkpoint_applied_lsn.
    let bytes = fresh_header().to_bytes();
    assert!(
        bytes[110..128].iter().all(|&b| b == 0),
        "reserved region 110..128 must be zero-filled"
    );
}

#[test]
fn durable_id_counters_roundtrip() {
    // Phase 1 §10.7 — persist and recover `next_namespace_id`,
    // `next_index_id`, `history_store_root_page`, and
    // `history_store_root_level` via the header.
    let mut h = fresh_header();
    h.next_namespace_id = 42;
    h.next_index_id = 99;
    h.history_store_root_page = 314;
    h.history_store_root_level = 2;
    h.checkpoint_applied_lsn = 2718;

    let bytes = h.to_bytes();
    let decoded = FileHeader::from_bytes(&bytes).expect("parse");
    assert_eq!(decoded.next_namespace_id, 42);
    assert_eq!(decoded.next_index_id, 99);
    assert_eq!(decoded.history_store_root_page, 314);
    assert_eq!(decoded.history_store_root_level, 2);
    assert_eq!(decoded.checkpoint_applied_lsn, 2718);
}

#[test]
fn fresh_header_initializes_durable_counters_to_one() {
    // Phase 1 §10.7 — `0` is reserved, so the first allocation must
    // return 1. Both counters must start at 1 on fresh-DB creation.
    let h = fresh_header();
    assert_eq!(h.next_namespace_id, 1);
    assert_eq!(h.next_index_id, 1);
    assert_eq!(h.history_store_root_page, 0);
    assert_eq!(h.history_store_root_level, 0);
    assert_eq!(h.checkpoint_applied_lsn, 0);
}

// -----------------------------------------------------------------------
// Checksum
// -----------------------------------------------------------------------

#[test]
fn checksum_is_written_at_offset_64() {
    let h = fresh_header();
    let bytes = h.to_bytes();
    // Checksum at offset 64
    let stored = u32::from_le_bytes(bytes[64..68].try_into().unwrap());
    let computed = FileHeader::compute_checksum(&bytes);
    assert_eq!(stored, computed);
}

#[test]
fn checksum_verification_succeeds_for_valid_header() {
    let bytes = fresh_header().to_bytes();
    FileHeader::from_bytes(&bytes).expect("should parse without error");
}

#[test]
fn checksum_verification_fails_on_body_corruption() {
    let mut bytes = fresh_header().to_bytes();
    bytes[10] ^= 0xFF; // corrupt within checksum range [0,64)
    assert!(FileHeader::from_bytes(&bytes).is_err());
}

#[test]
fn checksum_does_not_cover_wal_salts() {
    // The checksum excludes WAL salts at 68..76. Corrupting them after
    // serialization should still parse.
    let original = fresh_header();
    let mut bytes = original.to_bytes();
    bytes[68] ^= 0xFF; // corrupt wal_salt1 LSB
    let decoded = FileHeader::from_bytes(&bytes).expect("should parse");
    // The decoded salt reflects the corruption
    assert_ne!(decoded.wal_salt1, original.wal_salt1);
}

// -----------------------------------------------------------------------
// Magic and version rejection
// -----------------------------------------------------------------------

#[test]
fn bad_magic_is_rejected() {
    let original = fresh_header();
    let mut bytes = original.to_bytes();
    // Overwrite magic + recompute checksum so only the magic is wrong
    bytes[0] = b'X';
    let new_checksum = FileHeader::compute_checksum(&bytes);
    bytes[64..68].copy_from_slice(&new_checksum.to_le_bytes());
    assert!(FileHeader::from_bytes(&bytes).is_err());
}

#[test]
fn unknown_format_version_is_rejected() {
    let original = fresh_header();
    let mut bytes = original.to_bytes();
    // Overwrite format_version with 99
    bytes[4..8].copy_from_slice(&99u32.to_le_bytes());
    let new_checksum = FileHeader::compute_checksum(&bytes);
    bytes[64..68].copy_from_slice(&new_checksum.to_le_bytes());
    assert!(FileHeader::from_bytes(&bytes).is_err());
}

// -----------------------------------------------------------------------
// validate()
// -----------------------------------------------------------------------

#[test]
fn validate_passes_for_well_formed_header() {
    fresh_header().validate().expect("should pass");
}

#[test]
fn validate_fails_for_zero_page_count() {
    let mut h = fresh_header();
    h.total_page_count = 0;
    assert!(h.validate().is_err());
}

#[test]
fn validate_fails_for_unknown_checksum_algo() {
    let mut h = fresh_header();
    h.checksum_algo = 99;
    assert!(h.validate().is_err());
}

// -----------------------------------------------------------------------
// new_now()
// -----------------------------------------------------------------------

#[test]
fn new_now_produces_parseable_header() {
    let h = FileHeader::new_now();
    let bytes = h.to_bytes();
    let decoded = FileHeader::from_bytes(&bytes).unwrap();
    decoded.validate().unwrap();
    assert!(decoded.created_at > 0);
}

// -----------------------------------------------------------------------
// derive_wal_salts
// -----------------------------------------------------------------------

#[test]
fn wal_salts_are_deterministic_for_same_input() {
    let (s1a, s2a) = derive_wal_salts(12345);
    let (s1b, s2b) = derive_wal_salts(12345);
    assert_eq!(s1a, s1b);
    assert_eq!(s2a, s2b);
}

#[test]
fn wal_salts_differ_for_different_timestamps() {
    let (s1a, _) = derive_wal_salts(10000);
    let (s1b, _) = derive_wal_salts(20000);
    // Different timestamps should (with very high probability) produce
    // different salts.
    assert_ne!(s1a, s1b);
}

// -----------------------------------------------------------------------
// Golden bytes — cross-platform endianness contract
// -----------------------------------------------------------------------

/// Verify exact byte layout of the serialized header.
///
/// This test proves that every multi-byte field is written in
/// **explicit little-endian** order.  A file created on x86_64
/// must be byte-identical and readable on aarch64 (and vice-versa).
///
/// If native-endian writes were ever introduced, this test would fail
/// on big-endian targets and the cross-platform CI job would catch the
/// regression on aarch64 hardware.
#[test]
fn golden_bytes_little_endian_layout() {
    let h = FileHeader::new(TEST_TS, 0xDEAD_BEEF, 0xCAFE_BABE);
    let bytes = h.to_bytes();

    // Magic "MQLT" is ASCII — byte order is irrelevant but must be exact.
    assert_eq!(&bytes[0..4], b"MQLT", "magic mismatch");

    // Format version = 1  →  LE bytes [0x01, 0x00, 0x00, 0x00]
    assert_eq!(bytes[4], 0x01, "version LSB at offset 4");
    assert_eq!(bytes[5], 0x00);
    assert_eq!(bytes[6], 0x00);
    assert_eq!(bytes[7], 0x00, "version MSB at offset 7");

    // Internal page size = 4096 = 0x0000_1000  →  LE [0x00, 0x10, 0x00, 0x00]
    assert_eq!(bytes[8], 0x00);
    assert_eq!(bytes[9], 0x10, "page_size_internal byte 1");
    assert_eq!(bytes[10], 0x00);
    assert_eq!(bytes[11], 0x00);

    // Leaf page size = 32768 = 0x0000_8000  →  LE [0x00, 0x80, 0x00, 0x00]
    assert_eq!(bytes[12], 0x00);
    assert_eq!(bytes[13], 0x80, "page_size_leaf byte 1");
    assert_eq!(bytes[14], 0x00);
    assert_eq!(bytes[15], 0x00);

    // created_at = TEST_TS = 1_700_000_000_000  →  exact LE bytes
    assert_eq!(
        &bytes[16..24],
        &TEST_TS.to_le_bytes(),
        "created_at LE bytes"
    );

    // last_checkpoint_ts = zero for a fresh header — 12 bytes
    assert_eq!(&bytes[24..36], &[0u8; 12], "last_checkpoint_ts Ts-LE bytes");

    // Checksum algorithm = 1 (CRC32C)  →  LE bytes [0x01, 0x00, 0x00, 0x00] at offset 60
    assert_eq!(bytes[60], 0x01, "checksum_algo LSB");
    assert_eq!(bytes[61], 0x00);
    assert_eq!(bytes[62], 0x00);
    assert_eq!(bytes[63], 0x00, "checksum_algo MSB");

    // WAL salt 1 = 0xDEAD_BEEF  →  LE bytes [0xEF, 0xBE, 0xAD, 0xDE] at offset 68
    assert_eq!(bytes[68], 0xEF, "wal_salt1 byte 0 (LSB)");
    assert_eq!(bytes[69], 0xBE);
    assert_eq!(bytes[70], 0xAD);
    assert_eq!(bytes[71], 0xDE, "wal_salt1 byte 3 (MSB)");

    // WAL salt 2 = 0xCAFE_BABE  →  LE bytes [0xBE, 0xBA, 0xFE, 0xCA] at offset 72
    assert_eq!(bytes[72], 0xBE, "wal_salt2 byte 0 (LSB)");
    assert_eq!(bytes[73], 0xBA);
    assert_eq!(bytes[74], 0xFE);
    assert_eq!(bytes[75], 0xCA, "wal_salt2 byte 3 (MSB)");
}
