//! Fuzz target: MongoDB wire protocol parser (OP_MSG).
//!
//! Exercises `MsgHeader::parse` and `OpMsg::parse` with arbitrary byte
//! sequences.
//!
//! Goal: no panics, no hangs; every malformed frame must produce a clean
//! `Err(...)` rather than a crash or an unbounded allocation.
//!
//! Run:
//! ```sh
//! cargo +nightly fuzz run wire_protocol -- -max_total_time=60
//! ```
#![no_main]

use libfuzzer_sys::fuzz_target;
use mqlite::wire::protocol::{MsgHeader, OpMsg};

fuzz_target!(|data: &[u8]| {
    // Parse just the header — exercises length / opcode validation.
    let _ = MsgHeader::parse(data);

    // Parse a full OP_MSG frame — exercises section parsing, BSON extraction,
    // checksum validation, and size-limit enforcement.
    let _ = OpMsg::parse(data);
});
