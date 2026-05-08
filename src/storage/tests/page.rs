use super::*;

// -----------------------------------------------------------------------
// Internal page tests
// -----------------------------------------------------------------------

fn make_internal_page() -> [u8; PAGE_SIZE_INTERNAL as usize] {
    let mut buf = [0u8; PAGE_SIZE_INTERNAL as usize];
    let mut hdr = InternalPageHeader {
        page_type: PAGE_TYPE_INTERNAL,
        level: 2,
        key_count: 3,
        checksum: 0,
        rightmost_child: 42,
    };
    hdr.write_to(&mut buf);
    let cs = internal_page_checksum(&buf);
    buf[4..8].copy_from_slice(&cs.to_le_bytes());
    // Re-parse and store the valid checksum in the header struct too
    hdr.checksum = cs;
    hdr.write_to(&mut buf);
    buf
}

#[test]
fn internal_page_roundtrip() {
    let page = make_internal_page();
    let hdr = InternalPageHeader::from_bytes(&page).unwrap();
    assert_eq!(hdr.page_type, PAGE_TYPE_INTERNAL);
    assert_eq!(hdr.level, 2);
    assert_eq!(hdr.key_count, 3);
    assert_eq!(hdr.rightmost_child, 42);
}

#[test]
fn internal_page_type_validation() {
    let page = make_internal_page();
    let hdr = InternalPageHeader::from_bytes(&page).unwrap();
    hdr.validate_type().expect("type should be valid");
}

#[test]
fn internal_page_bad_type_rejected() {
    let page = make_internal_page();
    let mut bad_hdr = InternalPageHeader::from_bytes(&page).unwrap();
    bad_hdr.page_type = PAGE_TYPE_LEAF; // wrong type
    assert!(bad_hdr.validate_type().is_err());
}

#[test]
fn internal_page_checksum_valid() {
    let page = make_internal_page();
    verify_internal_page_checksum(&page).expect("checksum should be valid");
}

#[test]
fn internal_page_checksum_detects_corruption() {
    let mut page = make_internal_page();
    page[100] ^= 0xFF; // flip bits in the key data area
    assert!(
        verify_internal_page_checksum(&page).is_err(),
        "should detect corruption"
    );
}

#[test]
fn internal_page_checksum_excludes_checksum_field() {
    // Corruption exactly at the checksum field (offset 4–7) should be
    // detected because the stored value no longer matches recomputed value.
    let mut page = make_internal_page();
    page[4] ^= 0xFF;
    assert!(verify_internal_page_checksum(&page).is_err());
}

// -----------------------------------------------------------------------
// Leaf page tests
// -----------------------------------------------------------------------

fn make_leaf_page() -> [u8; PAGE_SIZE_LEAF as usize] {
    let mut buf = [0u8; PAGE_SIZE_LEAF as usize];
    let mut hdr = LeafPageHeader {
        page_type: PAGE_TYPE_LEAF,
        flags: LEAF_FLAG_HAS_OVERFLOW,
        entry_count: 7,
        checksum: 0,
        next_leaf_page: 100,
        prev_leaf_page: 50,
        free_space_offset: LEAF_HEADER_SIZE as u16,
        cell_ptr_offset: LEAF_HEADER_SIZE as u16,
    };
    hdr.write_to(&mut buf);
    let cs = leaf_page_checksum(&buf);
    buf[4..8].copy_from_slice(&cs.to_le_bytes());
    hdr.checksum = cs;
    hdr.write_to(&mut buf);
    buf
}

#[test]
fn leaf_page_roundtrip() {
    let page = make_leaf_page();
    let hdr = LeafPageHeader::from_bytes(&page).unwrap();
    assert_eq!(hdr.page_type, PAGE_TYPE_LEAF);
    assert_eq!(hdr.flags, LEAF_FLAG_HAS_OVERFLOW);
    assert_eq!(hdr.entry_count, 7);
    assert_eq!(hdr.next_leaf_page, 100);
    assert_eq!(hdr.prev_leaf_page, 50);
    assert_eq!(hdr.free_space_offset, LEAF_HEADER_SIZE as u16);
    assert_eq!(hdr.cell_ptr_offset, LEAF_HEADER_SIZE as u16);
}

#[test]
fn leaf_page_has_overflow_flag() {
    let page = make_leaf_page();
    let hdr = LeafPageHeader::from_bytes(&page).unwrap();
    assert!(hdr.has_overflow());
}

#[test]
fn leaf_page_no_overflow_flag() {
    let mut buf = [0u8; PAGE_SIZE_LEAF as usize];
    let mut hdr = LeafPageHeader {
        page_type: PAGE_TYPE_LEAF,
        flags: 0,
        entry_count: 0,
        checksum: 0,
        next_leaf_page: 0,
        prev_leaf_page: 0,
        free_space_offset: LEAF_HEADER_SIZE as u16,
        cell_ptr_offset: LEAF_HEADER_SIZE as u16,
    };
    hdr.write_to(&mut buf);
    let cs = leaf_page_checksum(&buf);
    buf[4..8].copy_from_slice(&cs.to_le_bytes());
    hdr.checksum = cs;
    hdr.write_to(&mut buf);

    let parsed = LeafPageHeader::from_bytes(&buf).unwrap();
    assert!(!parsed.has_overflow());
}

#[test]
fn leaf_page_type_validation() {
    let page = make_leaf_page();
    let hdr = LeafPageHeader::from_bytes(&page).unwrap();
    hdr.validate_type().expect("type should be valid");
}

#[test]
fn leaf_page_checksum_valid() {
    let page = make_leaf_page();
    verify_leaf_page_checksum(&page).expect("checksum should be valid");
}

#[test]
fn leaf_page_checksum_detects_corruption() {
    let mut page = make_leaf_page();
    page[200] ^= 0xAB;
    assert!(verify_leaf_page_checksum(&page).is_err());
}

// -----------------------------------------------------------------------
// Overflow page tests
// -----------------------------------------------------------------------

fn make_overflow_page() -> [u8; PAGE_SIZE_LEAF as usize] {
    let mut buf = [0u8; PAGE_SIZE_LEAF as usize];
    let mut hdr = OverflowPageHeader {
        page_type: PAGE_TYPE_OVERFLOW,
        refcount: 1,
        checksum: 0,
        next_overflow_page: 77,
        data_length: 64,
    };
    hdr.write_to(&mut buf);
    // Write some payload bytes
    for i in 0..64usize {
        buf[OVERFLOW_HEADER_SIZE + i] = i as u8;
    }
    let cs = overflow_page_checksum(&buf);
    hdr.checksum = cs;
    hdr.write_to(&mut buf);
    buf
}

#[test]
fn overflow_page_roundtrip() {
    let page = make_overflow_page();
    let hdr = OverflowPageHeader::from_bytes(&page).unwrap();
    assert_eq!(hdr.page_type, PAGE_TYPE_OVERFLOW);
    assert_eq!(hdr.refcount, 1);
    assert_eq!(hdr.next_overflow_page, 77);
    assert_eq!(hdr.data_length, 64);
    // Reserved bytes read as zero
    assert_eq!(page[1], 0);
    assert_eq!(page[2], 0);
    assert_eq!(page[3], 0);
}

#[test]
fn overflow_page_type_validation() {
    let page = make_overflow_page();
    let hdr = OverflowPageHeader::from_bytes(&page).unwrap();
    hdr.validate_type().expect("type should be valid");
}

#[test]
fn overflow_page_checksum_valid() {
    let page = make_overflow_page();
    verify_overflow_page_checksum(&page).expect("checksum should be valid");
}

#[test]
fn overflow_page_checksum_detects_corruption() {
    let mut page = make_overflow_page();
    page[OVERFLOW_HEADER_SIZE + 10] ^= 0x55; // corrupt payload
    assert!(verify_overflow_page_checksum(&page).is_err());
}

/// Byte-exact gate for MVCC Format Lock §A.1 / MAJOR-3:
/// flipping any byte in the refcount field (4..8) must NOT alter the
/// stored checksum at bytes 8..12. This lets the allocator mutate the
/// atomic refcount without re-checksumming the page.
#[test]
fn overflow_page_checksum_excludes_refcount_bytes() {
    let page = make_overflow_page();
    let stored_checksum = u32::from_le_bytes([page[8], page[9], page[10], page[11]]);

    for offset in 4..8 {
        let mut mutated = page;
        mutated[offset] ^= 0xFF;
        let recomputed = overflow_page_checksum(&mutated);
        assert_eq!(
            recomputed, stored_checksum,
            "flipping refcount byte at offset {offset} must not change the checksum",
        );
        // And verification still succeeds — the stored checksum is unchanged
        // and the recomputed checksum matches.
        verify_overflow_page_checksum(&mutated)
            .expect("refcount flip must not invalidate page");
    }
}

/// The checksum field itself (bytes 8..12) is excluded from coverage, so
/// any bit flipped there is detected via the stored-vs-computed comparison
/// at verify time.
#[test]
fn overflow_page_checksum_detects_checksum_field_flip() {
    let mut page = make_overflow_page();
    page[8] ^= 0xFF;
    assert!(verify_overflow_page_checksum(&page).is_err());
}

#[test]
fn overflow_header_size_is_20() {
    assert_eq!(OVERFLOW_HEADER_SIZE, 20);
}

// -----------------------------------------------------------------------
// Constants sanity checks
// -----------------------------------------------------------------------

#[test]
fn page_type_constants_are_correct() {
    assert_eq!(PAGE_TYPE_INTERNAL, 0x01);
    assert_eq!(PAGE_TYPE_LEAF, 0x02);
    assert_eq!(PAGE_TYPE_OVERFLOW, 0x05);
}

#[test]
fn page_size_constants_are_correct() {
    assert_eq!(PAGE_SIZE_INTERNAL, 4096);
    assert_eq!(PAGE_SIZE_LEAF, 32768);
}

#[test]
fn value_type_constants_are_correct() {
    assert_eq!(VALUE_TYPE_INLINE, 0x01);
    assert_eq!(VALUE_TYPE_OVERFLOW, 0x02);
}
