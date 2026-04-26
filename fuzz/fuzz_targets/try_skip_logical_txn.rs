//! Phase 2 §9.2 / US-023 — fuzz target: `try_skip_logical_txn`
//! cursor-rewind post-condition probe.

#![no_main]

use libfuzzer_sys::fuzz_target;

const SALT1_CONST: u32 = 0xDEAD_BEEF;
const SALT2_CONST: u32 = 0xCAFE_BABE;

fuzz_target!(|data: &[u8]| {
    let _ = mqlite::fuzz_try_skip_logical_txn(data, SALT1_CONST, SALT2_CONST);
});
