//! Phase 2 §9.2 / US-023 — fuzz target: `JournalManager::recover_existing`
//! over an arbitrary journal-file body.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = mqlite::fuzz_logical_txn_recover(data);
});
