//! Fuzz target: query filter evaluator.
//!
//! Exercises `eval_filter` with arbitrary BSON documents and filter
//! documents.
//!
//! Strategy: split the input buffer in half; parse each half as a BSON
//! document.  If both parse successfully, run the evaluator.
//!
//! Goal: no panics on any combination of valid-BSON doc + valid-BSON filter.
//! `eval_filter` is allowed to return `Err` for unsupported operators.
//!
//! Run:
//! ```sh
//! cargo +nightly fuzz run query_evaluator -- -max_total_time=60
//! ```
#![no_main]

use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // Split the fuzz input: first byte is the split point (as a percentage).
    let split_pct = data[0] as usize;
    let rest = &data[1..];
    if rest.is_empty() {
        return;
    }
    let split = (rest.len() * split_pct) / 256;
    let (doc_bytes, filter_bytes) = rest.split_at(split);

    // Try to parse both halves as BSON documents.
    let Ok(doc) = bson::Document::from_reader(Cursor::new(doc_bytes)) else {
        return;
    };
    let Ok(filter) = bson::Document::from_reader(Cursor::new(filter_bytes)) else {
        return;
    };

    // Run the evaluator — must not panic, may return an error.
    let _ = mqlite::fuzz_eval_filter(&doc, &filter);
});
