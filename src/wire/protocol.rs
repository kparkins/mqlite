//! OP_MSG framing parser and generator for the MongoDB wire protocol.
//!
//! This module handles the low-level framing of MongoDB wire protocol messages.
//! The OP_MSG format is used by MongoDB 3.6+ for all command communication.
//!
//! Phase 1 implementation: tracked in hq-6d0 (Phase 1c: OP_MSG framing parser and generator).

/// MongoDB wire protocol message header.
/// All integers are little-endian.
#[derive(Debug, Clone, Copy)]
pub struct MsgHeader {
    /// Total message length in bytes, including the header.
    pub message_length: i32,
    /// Unique identifier for this request.
    pub request_id: i32,
    /// `request_id` from the message being responded to (0 for requests).
    pub response_to: i32,
    /// Operation code. Phase 1 uses OP_MSG (2013) exclusively.
    pub op_code: i32,
}

/// OP_MSG operation code constant.
pub const OP_MSG: i32 = 2013;

impl MsgHeader {
    /// Size of a serialized message header in bytes.
    pub const SIZE: usize = 16;
}
