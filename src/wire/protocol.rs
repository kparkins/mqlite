//! OP_MSG framing parser and generator for the MongoDB wire protocol.
//!
//! This module handles the low-level framing of MongoDB wire protocol messages.
//! The OP_MSG format is used by MongoDB 3.6+ for all command communication.
//!
//! # Wire format
//!
//! ```text
//! MsgHeader (16 bytes):
//!   messageLength : int32   – total bytes including header
//!   requestID     : int32   – unique id for this request
//!   responseTo    : int32   – requestID of the message we are replying to (0 for requests)
//!   opCode        : int32   – 2013 for OP_MSG
//!
//! flagBits : uint32
//!   bit 0  – checksumPresent  (CRC-32C appended after sections)
//!   bit 1  – moreToCome       (sender will send more replies; not supported)
//!   bit 16 – exhaustAllowed   (client allows exhaust cursors; not supported)
//!
//! Sections (one or more):
//!   Kind 0 – body (exactly one per message, carries the command document)
//!     kind     : uint8
//!     document : BSON document
//!   Kind 1 – document sequence
//!     kind       : uint8
//!     size       : int32  (total bytes in this section including size field)
//!     identifier : cstring (null-terminated UTF-8)
//!     documents  : BSON document* (fills remaining bytes)
//!
//! Optional checksum (if flagChecksumPresent):
//!   checksum : uint32  – CRC-32C over all preceding bytes in the message
//! ```

use bson::Document;
use smallvec::SmallVec;

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Low-level read helpers
// ---------------------------------------------------------------------------

#[inline]
fn read_le_i32(buf: &[u8], off: usize) -> i32 {
    i32::from_le_bytes(
        buf[off..off + 4]
            .try_into()
            .expect("caller verified buf.len() >= off + 4"),
    )
}

#[inline]
fn read_le_u32(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(
        buf[off..off + 4]
            .try_into()
            .expect("caller verified buf.len() >= off + 4"),
    )
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// OP_MSG operation code.
pub const OP_MSG: i32 = 2013;

/// OP_COMPRESSED operation code – not supported; return a clean error.
pub const OP_COMPRESSED: i32 = 2012;

/// Maximum allowed wire message size: 48 MiB.
pub const MAX_MESSAGE_SIZE: usize = 48 * 1024 * 1024;

/// Section kind byte: body (one Kind-0 section per OP_MSG).
const SECTION_KIND_BODY: u8 = 0;

/// Section kind byte: document sequence.
const SECTION_KIND_DOC_SEQ: u8 = 1;

// ---------------------------------------------------------------------------
// flagBits masks
// ---------------------------------------------------------------------------

/// flagBits bit 0 – CRC-32C checksum is appended after sections.
pub const FLAG_CHECKSUM_PRESENT: u32 = 1 << 0;
/// flagBits bit 1 – sender will send more replies (streaming; not supported).
pub const FLAG_MORE_TO_COME: u32 = 1 << 1;
/// flagBits bit 16 – client allows exhaust cursors (not supported).
pub const FLAG_EXHAUST_ALLOWED: u32 = 1 << 16;

// ---------------------------------------------------------------------------
// MsgHeader
// ---------------------------------------------------------------------------

/// MongoDB wire protocol message header (16 bytes, all integers little-endian).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MsgHeader {
    /// Total message length in bytes, including the header itself.
    pub message_length: i32,
    /// Unique identifier for this request.
    pub request_id: i32,
    /// `request_id` from the message being responded to; 0 for client requests.
    pub response_to: i32,
    /// Operation code.
    pub op_code: i32,
}

impl MsgHeader {
    /// Size of a serialised message header in bytes.
    pub const SIZE: usize = 16;

    /// Parse a 16-byte header from `buf`.
    ///
    /// Returns `Err(InvalidWireMessage)` if the slice is shorter than 16 bytes.
    pub fn parse(buf: &[u8]) -> Result<MsgHeader> {
        if buf.len() < Self::SIZE {
            return Err(Error::InvalidWireMessage {
                detail: format!(
                    "header too short: expected {} bytes, got {}",
                    Self::SIZE,
                    buf.len()
                ),
            });
        }
        Ok(MsgHeader {
            message_length: read_le_i32(buf, 0),
            request_id: read_le_i32(buf, 4),
            response_to: read_le_i32(buf, 8),
            op_code: read_le_i32(buf, 12),
        })
    }

    /// Serialise the header to 16 bytes (little-endian).
    pub fn to_bytes(self) -> [u8; Self::SIZE] {
        let mut out = [0u8; Self::SIZE];
        out[0..4].copy_from_slice(&self.message_length.to_le_bytes());
        out[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.response_to.to_le_bytes());
        out[12..16].copy_from_slice(&self.op_code.to_le_bytes());
        out
    }
}

// ---------------------------------------------------------------------------
// Section
// ---------------------------------------------------------------------------

/// A section within an OP_MSG message.
#[derive(Debug, Clone)]
pub enum Section {
    /// Kind 0 – body: a single BSON document carrying the command.
    Body(Document),

    /// Kind 1 – document sequence: a named sequence of BSON documents.
    ///
    /// Used by some MongoDB drivers to send bulk write payloads without wrapping
    /// them in an array inside a Kind-0 document.
    DocSequence {
        /// Section identifier (e.g. `"documents"`, `"updates"`).
        identifier: String,
        /// The BSON documents in the sequence.
        documents: Vec<Document>,
    },
}

// ---------------------------------------------------------------------------
// OpMsg
// ---------------------------------------------------------------------------

/// A fully parsed OP_MSG message.
///
/// # Layout
///
/// `header` → `flag_bits` → `sections` → optional `checksum`
#[derive(Debug, Clone)]
pub struct OpMsg {
    /// The message header.
    pub header: MsgHeader,

    /// Raw flag bits from the wire.
    pub flag_bits: u32,

    /// Parsed sections (at least one Kind-0 body section).
    pub sections: SmallVec<[Section; 1]>,

    /// CRC-32C checksum if `FLAG_CHECKSUM_PRESENT` is set.
    pub checksum: Option<u32>,
}

impl OpMsg {
    /// Parse a complete OP_MSG message from `buf`.
    ///
    /// `buf` must contain exactly one complete message (including header).
    ///
    /// # Errors
    ///
    /// - `InvalidWireMessage` – header too short, wrong opcode, size mismatch,
    ///   unknown section kind, or invalid CRC-32C.
    pub fn parse(buf: &[u8]) -> Result<OpMsg> {
        // --- Header ---
        let header = MsgHeader::parse(buf)?;

        // Reject OP_COMPRESSED before any further processing.
        if header.op_code == OP_COMPRESSED {
            return Err(Error::InvalidWireMessage {
                detail: "OP_COMPRESSED (opcode 2012) is not supported; \
                         disable compression in your driver or client"
                    .into(),
            });
        }

        if header.op_code != OP_MSG {
            return Err(Error::InvalidWireMessage {
                detail: format!(
                    "unsupported opCode {}: only OP_MSG (2013) is supported",
                    header.op_code
                ),
            });
        }

        // Validate declared length against buffer.
        let declared = header.message_length as usize;
        if declared < MsgHeader::SIZE {
            return Err(Error::InvalidWireMessage {
                detail: format!(
                    "messageLength {} is smaller than header size {}",
                    declared,
                    MsgHeader::SIZE
                ),
            });
        }
        if declared > MAX_MESSAGE_SIZE {
            return Err(Error::InvalidWireMessage {
                detail: format!(
                    "message size {} exceeds maximum {} bytes (48 MiB)",
                    declared, MAX_MESSAGE_SIZE
                ),
            });
        }
        if buf.len() < declared {
            return Err(Error::InvalidWireMessage {
                detail: format!(
                    "buffer too short: messageLength={} but only {} bytes available",
                    declared,
                    buf.len()
                ),
            });
        }

        // Work only within the declared message bounds.
        let msg = &buf[..declared];

        // --- flagBits (4 bytes after header) ---
        const FLAGS_OFFSET: usize = MsgHeader::SIZE;
        const SECTIONS_OFFSET: usize = FLAGS_OFFSET + 4;

        if msg.len() < SECTIONS_OFFSET {
            return Err(Error::InvalidWireMessage {
                detail: "message too short to contain flagBits".into(),
            });
        }
        let flag_bits = read_le_u32(msg, FLAGS_OFFSET);

        // If checksum is present the last 4 bytes of the message are the CRC.
        let checksum_present = flag_bits & FLAG_CHECKSUM_PRESENT != 0;
        let sections_end = if checksum_present {
            if msg.len() < SECTIONS_OFFSET + 4 {
                return Err(Error::InvalidWireMessage {
                    detail: "checksumPresent flag set but message too short for checksum".into(),
                });
            }
            msg.len() - 4
        } else {
            msg.len()
        };

        // --- Validate checksum before decoding sections ---
        let checksum = if checksum_present {
            let stored = read_le_u32(msg, sections_end);
            let computed = crc32c::crc32c(&msg[..sections_end]);
            if stored != computed {
                return Err(Error::InvalidWireMessage {
                    detail: format!(
                        "CRC-32C checksum mismatch: stored=0x{:08x} computed=0x{:08x}",
                        stored, computed
                    ),
                });
            }
            Some(stored)
        } else {
            None
        };

        // --- Sections ---
        let sections = parse_sections(&msg[SECTIONS_OFFSET..sections_end])?;

        Ok(OpMsg {
            header,
            flag_bits,
            sections,
            checksum,
        })
    }

    /// Return the Kind-0 body document, if one is present.
    ///
    /// Every well-formed OP_MSG has exactly one Kind-0 section.
    pub fn body(&self) -> Option<&Document> {
        self.sections.iter().find_map(|s| match s {
            Section::Body(doc) => Some(doc),
            _ => None,
        })
    }

    /// Serialise an OP_MSG response.
    ///
    /// Outgoing responses always use `flagBits = 0` (no checksum, no moreToCome).
    ///
    /// `request_id` should be a fresh ID for this response.
    /// `response_to` should be the `request_id` of the incoming request.
    pub fn build_response(request_id: i32, response_to: i32, body: &Document) -> Result<Vec<u8>> {
        // Serialise the BSON document.
        let bson_bytes = bson::to_vec(body)?;

        // Section Kind 0: 1 byte kind + document bytes.
        let section_len = 1 + bson_bytes.len();

        // Total: header (16) + flagBits (4) + section.
        let total = MsgHeader::SIZE + 4 + section_len;

        let header = MsgHeader {
            message_length: total as i32,
            request_id,
            response_to,
            op_code: OP_MSG,
        };

        let mut out = Vec::with_capacity(total);
        out.extend_from_slice(&header.to_bytes());
        // flagBits = 0 (no checksum, no moreToCome)
        out.extend_from_slice(&0u32.to_le_bytes());
        // Kind-0 section
        out.push(SECTION_KIND_BODY);
        out.extend_from_slice(&bson_bytes);

        debug_assert_eq!(out.len(), total);
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Section parsing helper
// ---------------------------------------------------------------------------

/// Parse all sections from `buf` (the slice between flagBits and checksum).
fn parse_sections(mut buf: &[u8]) -> Result<SmallVec<[Section; 1]>> {
    let mut sections = SmallVec::new();

    while !buf.is_empty() {
        let kind = buf[0];
        buf = &buf[1..];

        match kind {
            SECTION_KIND_BODY => {
                // BSON document.
                let doc = read_bson_document(&mut buf)?;
                sections.push(Section::Body(doc));
            }
            SECTION_KIND_DOC_SEQ => {
                // int32 size field (includes itself), then cstring identifier, then BSON docs.
                if buf.len() < 4 {
                    return Err(Error::InvalidWireMessage {
                        detail: "Kind-1 section too short for size field".into(),
                    });
                }
                let size = read_le_i32(buf, 0) as usize;
                if size < 4 || size > buf.len() {
                    return Err(Error::InvalidWireMessage {
                        detail: format!(
                            "Kind-1 section size {} out of range (buf len={})",
                            size,
                            buf.len()
                        ),
                    });
                }

                // The section's payload starts at buf[4] and runs for (size - 4) bytes
                // (size includes the 4-byte size field itself).
                let section_buf = &buf[4..size];
                buf = &buf[size..];

                // Read the null-terminated identifier.
                let null_pos = section_buf.iter().position(|&b| b == 0).ok_or_else(|| {
                    Error::InvalidWireMessage {
                        detail: "Kind-1 section identifier not null-terminated".into(),
                    }
                })?;
                let identifier = std::str::from_utf8(&section_buf[..null_pos])
                    .map_err(|_| Error::InvalidWireMessage {
                        detail: "Kind-1 section identifier is not valid UTF-8".into(),
                    })?
                    .to_owned();

                // Remaining bytes are BSON documents.
                let mut doc_buf = &section_buf[null_pos + 1..];
                let mut documents = Vec::new();
                while !doc_buf.is_empty() {
                    let doc = read_bson_document(&mut doc_buf)?;
                    documents.push(doc);
                }

                sections.push(Section::DocSequence {
                    identifier,
                    documents,
                });
            }
            other => {
                return Err(Error::InvalidWireMessage {
                    detail: format!("unknown section kind byte: {}", other),
                });
            }
        }
    }

    Ok(sections)
}

/// Read a single BSON document from `buf`, advancing the slice past the document.
///
/// BSON documents are self-delimiting: the first 4 bytes encode the total document
/// size (including the size field itself).
fn read_bson_document(buf: &mut &[u8]) -> Result<Document> {
    if buf.len() < 4 {
        return Err(Error::InvalidWireMessage {
            detail: format!(
                "too few bytes for BSON document size field: need 4, have {}",
                buf.len()
            ),
        });
    }
    let size = read_le_i32(buf, 0) as usize;
    if size < 5 {
        // Minimum valid BSON document is 5 bytes ({} = int32 size + 0x00 terminator).
        return Err(Error::InvalidWireMessage {
            detail: format!("BSON document size {} is too small (minimum 5)", size),
        });
    }
    if size > buf.len() {
        return Err(Error::InvalidWireMessage {
            detail: format!(
                "BSON document size {} exceeds remaining buffer length {}",
                size,
                buf.len()
            ),
        });
    }
    let doc =
        bson::from_slice::<Document>(&buf[..size]).map_err(|e| Error::InvalidWireMessage {
            detail: format!("BSON deserialisation failed: {}", e),
        })?;
    *buf = &buf[size..];
    Ok(doc)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/protocol.rs"]
mod tests;
