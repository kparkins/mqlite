//! US-011 header layout regression tests.

use super::*;

const TEST_TS: u64 = 1_700_000_000_000;
const HISTORY_ROOT_PAGE_OFFSET: usize = 97;
const HISTORY_ROOT_LEVEL_OFFSET: usize = 101;
const RESERVED_START: usize = 102;
const RESERVED_END: usize = 128;

fn fresh_header() -> FileHeader {
    FileHeader::new(TEST_TS, 0xDEAD_BEEF, 0xCAFE_BABE)
}

#[test]
fn history_store_root_level_roundtrips_at_offset_101() {
    let mut header = fresh_header();
    header.history_store_root_page = 0x0102_0304;
    header.history_store_root_level = 3;

    let bytes = header.to_bytes();

    assert_eq!(
        &bytes[HISTORY_ROOT_PAGE_OFFSET..HISTORY_ROOT_LEVEL_OFFSET],
        &0x0102_0304u32.to_le_bytes()
    );
    assert_eq!(bytes[HISTORY_ROOT_LEVEL_OFFSET], 3);
    assert!(
        bytes[RESERVED_START..RESERVED_END].iter().all(|&b| b == 0),
        "reserved bytes 102..128 must stay zero-filled"
    );

    let decoded = FileHeader::from_bytes(&bytes).expect("valid US-011 header");
    assert_eq!(decoded.history_store_root_page, 0x0102_0304);
    assert_eq!(decoded.history_store_root_level, 3);
}

#[test]
fn checksum_covers_header_fields_after_wal_salts_but_not_reserved_tail() {
    let header = fresh_header();

    let mut covered_catalog_backup = header.to_bytes();
    covered_catalog_backup[76] ^= 0x01;
    assert!(
        FileHeader::from_bytes(&covered_catalog_backup).is_err(),
        "offset 76 must be covered by the US-011 header checksum"
    );

    let mut covered_history_level = header.to_bytes();
    covered_history_level[HISTORY_ROOT_LEVEL_OFFSET] ^= 0x01;
    assert!(
        FileHeader::from_bytes(&covered_history_level).is_err(),
        "offset 101 must be covered by the US-011 header checksum"
    );

    let mut wal_salt = header.to_bytes();
    wal_salt[68] ^= 0x01;
    assert!(
        FileHeader::from_bytes(&wal_salt).is_ok(),
        "WAL salts at 68..76 remain excluded from the checksum"
    );

    let mut reserved_tail = header.to_bytes();
    reserved_tail[RESERVED_START] ^= 0x01;
    assert!(
        FileHeader::from_bytes(&reserved_tail).is_ok(),
        "reserved bytes from 102 onward remain outside the checksum"
    );
}
