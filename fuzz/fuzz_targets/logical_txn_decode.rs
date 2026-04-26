//! Phase 2 §9.2 / US-023 — fuzz target: `LogicalTxnFrame::decode`.
//!
//! Calls the decoder under `Scanning` context with arbitrary fuzzed
//! bytes. The decoder must never panic / UB / loop on any input.

#![no_main]

use libfuzzer_sys::fuzz_target;

const SALT1_CONST: u32 = 0xDEAD_BEEF;
const SALT2_CONST: u32 = 0xCAFE_BABE;

fuzz_target!(|data: &[u8]| {
    let _ = mqlite::fuzz_logical_txn_decode_scanning(data, SALT1_CONST, SALT2_CONST);
});
