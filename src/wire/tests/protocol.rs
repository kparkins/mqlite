use super::*;
use bson::doc;

// -----------------------------------------------------------------------
// MsgHeader
// -----------------------------------------------------------------------

#[test]
fn header_round_trip() {
    let h = MsgHeader {
        message_length: 100,
        request_id: 42,
        response_to: 0,
        op_code: OP_MSG,
    };
    let bytes = h.to_bytes();
    let h2 = MsgHeader::parse(&bytes).unwrap();
    assert_eq!(h, h2);
}

#[test]
fn header_parse_too_short() {
    let err = MsgHeader::parse(&[0u8; 8]).unwrap_err();
    match err {
        Error::InvalidWireMessage { detail } => {
            assert!(detail.contains("too short"), "got: {}", detail);
        }
        _ => panic!("wrong error type: {:?}", err),
    }
}

// -----------------------------------------------------------------------
// build_response / round-trip through parse
// -----------------------------------------------------------------------

fn build_simple_request(request_id: i32, body: &Document) -> Vec<u8> {
    // Build a minimal OP_MSG request (Kind-0, no checksum).
    let bson_bytes = bson::to_vec(body).unwrap();
    let total = MsgHeader::SIZE + 4 + 1 + bson_bytes.len();
    let header = MsgHeader {
        message_length: total as i32,
        request_id,
        response_to: 0,
        op_code: OP_MSG,
    };
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&header.to_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // flagBits
    buf.push(SECTION_KIND_BODY);
    buf.extend_from_slice(&bson_bytes);
    buf
}

#[test]
fn parse_simple_request() {
    let body = doc! { "ping": 1, "db": "admin" };
    let buf = build_simple_request(7, &body);
    let msg = OpMsg::parse(&buf).unwrap();

    assert_eq!(msg.header.op_code, OP_MSG);
    assert_eq!(msg.header.request_id, 7);
    assert_eq!(msg.flag_bits, 0);
    assert!(msg.checksum.is_none());

    let parsed_body = msg.body().expect("should have Kind-0 body");
    assert_eq!(parsed_body.get_i32("ping").unwrap(), 1);
}

#[test]
fn build_response_round_trip() {
    let resp_body = doc! { "ok": 1, "ismaster": true };
    let bytes = OpMsg::build_response(1, 7, &resp_body).unwrap();
    let msg = OpMsg::parse(&bytes).unwrap();

    assert_eq!(msg.header.op_code, OP_MSG);
    assert_eq!(msg.header.request_id, 1);
    assert_eq!(msg.header.response_to, 7);
    assert_eq!(msg.flag_bits, 0);
    assert!(msg.checksum.is_none());

    let body = msg.body().unwrap();
    assert_eq!(body.get_i32("ok").unwrap(), 1);
}

// -----------------------------------------------------------------------
// Checksum validation
// -----------------------------------------------------------------------

fn build_request_with_checksum(request_id: i32, body: &Document) -> Vec<u8> {
    let bson_bytes = bson::to_vec(body).unwrap();
    // Reserve 4 bytes for the checksum at the end.
    let total = MsgHeader::SIZE + 4 + 1 + bson_bytes.len() + 4;
    let header = MsgHeader {
        message_length: total as i32,
        request_id,
        response_to: 0,
        op_code: OP_MSG,
    };
    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&header.to_bytes());
    buf.extend_from_slice(&FLAG_CHECKSUM_PRESENT.to_le_bytes());
    buf.push(SECTION_KIND_BODY);
    buf.extend_from_slice(&bson_bytes);
    // Compute CRC-32C over everything before the checksum.
    let crc = crc32c::crc32c(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());
    buf
}

#[test]
fn valid_checksum_accepted() {
    let body = doc! { "hello": 1 };
    let buf = build_request_with_checksum(3, &body);
    let msg = OpMsg::parse(&buf).unwrap();
    assert!(msg.checksum.is_some());
}

#[test]
fn invalid_checksum_rejected() {
    let body = doc! { "hello": 1 };
    let mut buf = build_request_with_checksum(3, &body);
    // Corrupt the last byte of the checksum.
    let last = buf.len() - 1;
    buf[last] ^= 0xFF;
    let err = OpMsg::parse(&buf).unwrap_err();
    match err {
        Error::InvalidWireMessage { detail } => {
            assert!(detail.contains("checksum mismatch"), "got: {}", detail);
        }
        _ => panic!("wrong error type: {:?}", err),
    }
}

// -----------------------------------------------------------------------
// Size limit
// -----------------------------------------------------------------------

#[test]
fn oversized_message_rejected() {
    // Build a header claiming the message is 49 MiB.
    let claimed_size = 49 * 1024 * 1024;
    let header = MsgHeader {
        message_length: claimed_size,
        request_id: 1,
        response_to: 0,
        op_code: OP_MSG,
    };
    // We only need enough bytes to get past the header-length check.
    // The size check happens before reading sections, so we just need
    // a buffer whose length >= the claimed size to trigger the MAX_MESSAGE_SIZE path.
    // But 49 MiB would OOM; instead use a minimal buffer and check the
    // declared-vs-max path (declared > MAX_MESSAGE_SIZE is checked before
    // declared > buf.len()).
    let buf_prefix = header.to_bytes();
    let err = OpMsg::parse(&buf_prefix[..]).unwrap_err();
    match err {
        Error::InvalidWireMessage { detail } => {
            assert!(
                detail.contains("exceeds maximum") || detail.contains("48 MiB"),
                "got: {}",
                detail
            );
        }
        _ => panic!("wrong error type: {:?}", err),
    }
}

// -----------------------------------------------------------------------
// OP_COMPRESSED rejection
// -----------------------------------------------------------------------

#[test]
fn op_compressed_rejected() {
    let header = MsgHeader {
        message_length: MsgHeader::SIZE as i32,
        request_id: 1,
        response_to: 0,
        op_code: OP_COMPRESSED,
    };
    let buf = header.to_bytes();
    let err = OpMsg::parse(&buf).unwrap_err();
    match err {
        Error::InvalidWireMessage { detail } => {
            assert!(
                detail.contains("OP_COMPRESSED") || detail.contains("2012"),
                "got: {}",
                detail
            );
        }
        _ => panic!("wrong error type: {:?}", err),
    }
}

#[test]
fn unknown_opcode_rejected() {
    let header = MsgHeader {
        message_length: MsgHeader::SIZE as i32,
        request_id: 1,
        response_to: 0,
        op_code: 9999,
    };
    let buf = header.to_bytes();
    let err = OpMsg::parse(&buf).unwrap_err();
    match err {
        Error::InvalidWireMessage { ref detail } => {
            assert!(detail.contains("9999"), "got: {}", detail);
        }
        _ => panic!("wrong error type: {:?}", err),
    }
}

// -----------------------------------------------------------------------
// Kind-1 document sequence
// -----------------------------------------------------------------------

fn build_request_with_doc_seq(
    request_id: i32,
    body: &Document,
    identifier: &str,
    docs: &[Document],
) -> Vec<u8> {
    let body_bytes = bson::to_vec(body).unwrap();

    // Build Kind-1 section payload.
    // Layout: int32 size | cstring identifier | BSON docs...
    // (size includes the int32 size field itself)
    let id_bytes = {
        let mut v = identifier.as_bytes().to_vec();
        v.push(0); // null terminator
        v
    };
    let mut docs_bytes: Vec<u8> = Vec::with_capacity(docs.len() * 128);
    for d in docs.iter() {
        docs_bytes.extend_from_slice(
            &bson::to_vec(d).expect("BSON serialisation should not fail in test"),
        );
    }
    // size field (4) + identifier + docs
    let section_payload_size = 4 + id_bytes.len() + docs_bytes.len();

    let total = MsgHeader::SIZE
        + 4  // flagBits
        + 1 + body_bytes.len()  // Kind-0 section
        + 1 + section_payload_size; // Kind-1 section

    let header = MsgHeader {
        message_length: total as i32,
        request_id,
        response_to: 0,
        op_code: OP_MSG,
    };

    let mut buf = Vec::with_capacity(total);
    buf.extend_from_slice(&header.to_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // flagBits
                                                // Kind-0
    buf.push(SECTION_KIND_BODY);
    buf.extend_from_slice(&body_bytes);
    // Kind-1
    buf.push(SECTION_KIND_DOC_SEQ);
    buf.extend_from_slice(&(section_payload_size as i32).to_le_bytes());
    buf.extend_from_slice(&id_bytes);
    buf.extend_from_slice(&docs_bytes);

    buf
}

#[test]
fn kind1_doc_sequence_parsed() {
    let body = doc! { "insert": "users", "$db": "test" };
    let docs = vec![doc! { "name": "Alice" }, doc! { "name": "Bob" }];
    let buf = build_request_with_doc_seq(10, &body, "documents", &docs);
    let msg = OpMsg::parse(&buf).unwrap();

    // Should have two sections: one Kind-0 body and one Kind-1 doc sequence.
    assert_eq!(msg.sections.len(), 2);

    let seq = msg.sections.iter().find_map(|s| match s {
        Section::DocSequence {
            identifier,
            documents,
        } => Some((identifier.as_str(), documents)),
        _ => None,
    });
    let (id, docs_parsed) = seq.expect("should have a DocSequence section");
    assert_eq!(id, "documents");
    assert_eq!(docs_parsed.len(), 2);
    assert_eq!(docs_parsed[0].get_str("name").unwrap(), "Alice");
    assert_eq!(docs_parsed[1].get_str("name").unwrap(), "Bob");
}

// -----------------------------------------------------------------------
// Buffer too short / extra bytes ignored
// -----------------------------------------------------------------------

#[test]
fn buffer_shorter_than_declared_length_rejected() {
    let body = doc! { "ping": 1 };
    let mut buf = build_simple_request(1, &body);
    // Truncate the buffer by 2 bytes so it's shorter than messageLength.
    let len = buf.len();
    buf.truncate(len - 2);
    // Also fix the messageLength to the original (now larger than buf).
    // (build_simple_request already sets messageLength to exact buf size, so
    //  after truncation buf.len() < declared length.)
    let err = OpMsg::parse(&buf).unwrap_err();
    match err {
        Error::InvalidWireMessage { detail } => {
            assert!(
                detail.contains("too short") || detail.contains("buffer"),
                "got: {}",
                detail
            );
        }
        _ => panic!("wrong error type: {:?}", err),
    }
}
