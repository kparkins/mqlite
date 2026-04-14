//! Fuzz target: BSON parser.
//!
//! Exercises `bson::Document::from_reader` with arbitrary byte sequences.
//!
//! Goal: no panics and no memory safety violations on any input.
//!
//! Run:
//! ```sh
//! cargo +nightly fuzz run bson_parser -- -max_total_time=60
//! ```
#![no_main]

use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    // --- Target 1: raw BSON document parse ---
    let _ = bson::Document::from_reader(Cursor::new(data));

    // --- Target 2: BSON deserialise into a generic Value ---
    // `bson::Bson` cannot be deserialized directly from raw bytes; we go
    // through a document and inspect the first value instead.
    if let Ok(doc) = bson::Document::from_reader(Cursor::new(data)) {
        for (_, val) in &doc {
            // Exercise Display / Debug paths to catch any format panics.
            let _ = format!("{val:?}");
        }
    }
});
