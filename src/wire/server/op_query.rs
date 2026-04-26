//! OP_QUERY parsing and OP_REPLY building.
//!
//! These helpers exist solely to support the legacy two-opcode handshake used
//! by pymongo 4.x: the *initial* `isMaster`/`hello` arrives as OP_QUERY and the
//! reply must be OP_REPLY.  Subsequent commands use OP_MSG.

use bson::Document;

use crate::error::Result;
use crate::wire::protocol::MsgHeader;

/// OP_REPLY — legacy response opcode for OP_QUERY messages.
pub(crate) const OP_REPLY: i32 = 1;

/// Parse an OP_QUERY message and return the command document.
///
/// OP_QUERY layout (after the 16-byte header):
/// ```text
/// flags             : int32
/// fullCollectionName: cstring  (null-terminated)
/// numberToSkip      : int32
/// numberToReturn    : int32
/// query             : BSON document  (the command)
/// [returnFieldsSelector: BSON document]  (optional; ignored)
/// ```
pub(crate) fn parse_op_query_body(buf: &[u8]) -> Result<Document> {
    if buf.len() < 4 {
        return Err(crate::error::Error::InvalidWireMessage {
            detail: "OP_QUERY body too short for flags".into(),
        });
    }
    // Skip flags (4 bytes), then find the null terminator of fullCollectionName.
    let after_flags = &buf[4..];
    let null_pos = after_flags.iter().position(|&b| b == 0).ok_or_else(|| {
        crate::error::Error::InvalidWireMessage {
            detail: "OP_QUERY fullCollectionName not null-terminated".into(),
        }
    })?;
    // Skip the null terminator, then skip numberToSkip (4) and numberToReturn (4).
    let doc_offset = 4 + null_pos + 1 + 4 + 4;
    if doc_offset + 4 > buf.len() {
        return Err(crate::error::Error::InvalidWireMessage {
            detail: "OP_QUERY body too short for query document".into(),
        });
    }
    let doc_size =
        i32::from_le_bytes(buf[doc_offset..doc_offset + 4].try_into().expect("4 bytes")) as usize;
    if doc_offset + doc_size > buf.len() {
        return Err(crate::error::Error::InvalidWireMessage {
            detail: format!(
                "OP_QUERY document size {} exceeds remaining buffer",
                doc_size
            ),
        });
    }
    let raw = bson::RawDocumentBuf::from_bytes(buf[doc_offset..doc_offset + doc_size].to_vec())
        .map_err(|e| crate::error::Error::InvalidWireMessage {
            detail: format!("OP_QUERY BSON parse error: {}", e),
        })?;
    bson::from_slice::<Document>(raw.as_bytes()).map_err(|e| {
        crate::error::Error::InvalidWireMessage {
            detail: format!("OP_QUERY BSON deserialise error: {}", e),
        }
    })
}

/// Build an OP_REPLY response for an OP_QUERY request.
///
/// OP_REPLY layout:
/// ```text
/// MsgHeader      (16 bytes)
/// responseFlags  : int32   (0 = no flags)
/// cursorID       : int64   (0 = no cursor)
/// startingFrom   : int32   (0)
/// numberReturned : int32   (1)
/// document       : BSON
/// ```
pub(crate) fn build_op_reply(
    request_id: i32,
    response_to: i32,
    body: &Document,
) -> Result<Vec<u8>> {
    let bson_bytes = bson::to_vec(body)?;
    // header(16) + responseFlags(4) + cursorID(8) + startingFrom(4) + numberReturned(4) + doc
    let total = 16 + 4 + 8 + 4 + 4 + bson_bytes.len();
    let header = MsgHeader {
        message_length: total as i32,
        request_id,
        response_to,
        op_code: OP_REPLY,
    };
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&header.to_bytes());
    out.extend_from_slice(&0i32.to_le_bytes()); // responseFlags
    out.extend_from_slice(&0i64.to_le_bytes()); // cursorID
    out.extend_from_slice(&0i32.to_le_bytes()); // startingFrom
    out.extend_from_slice(&1i32.to_le_bytes()); // numberReturned
    out.extend_from_slice(&bson_bytes);
    Ok(out)
}

/// Extract the database name from an OP_QUERY body buffer.
///
/// OP_QUERY body layout (after the 16-byte `MsgHeader`):
/// ```text
/// flags             : int32
/// fullCollectionName: cstring  (e.g. "admin.$cmd")
/// numberToSkip      : int32
/// numberToReturn    : int32
/// query             : BSON document
/// ```
///
/// Returns the part of `fullCollectionName` before the first `'.'` (the
/// database name), or `None` if the buffer is too short or not valid UTF-8.
pub(crate) fn parse_op_query_db_name(buf: &[u8]) -> Option<String> {
    if buf.len() < 5 {
        return None;
    }
    // Skip 4-byte flags field.
    let after_flags = &buf[4..];
    // Locate the null terminator of fullCollectionName.
    let null_pos = after_flags.iter().position(|&b| b == 0)?;
    let coll_name = std::str::from_utf8(&after_flags[..null_pos]).ok()?;
    // Database name is the component before the first '.'.
    Some(coll_name.split('.').next().unwrap_or("").to_owned())
}

/// Validate the `$db` field in an OP_MSG command body.
///
/// Any non-empty `$db` value is accepted — the database is created on first
/// write ("use mydb" semantics).  Always returns `None` (no error).
#[allow(dead_code)]
pub(crate) fn check_db_field(_body: &Document, _server_db_name: &str) -> Option<Document> {
    None
}
