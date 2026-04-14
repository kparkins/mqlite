//! Fuzz target: BSON key encoder.
//!
//! Exercises `encode_key` and `encode_compound_key` with arbitrary BSON
//! values parsed from the fuzz input.
//!
//! Goals:
//! 1. No panics on any valid-BSON input.
//! 2. Encoding is deterministic — `encode_key(v) == encode_key(v)`.
//! 3. Two independently-encoded values preserve comparison ordering:
//!    if the fuzzer produces two valid BSON values `a` and `b` we check that
//!    the encoded comparison direction matches the direct BSON comparison.
//!
//! Run:
//! ```sh
//! cargo +nightly fuzz run key_encoder -- -max_total_time=60
//! ```
#![no_main]

use bson::Bson;
use libfuzzer_sys::fuzz_target;
use mqlite::key_encoding::{encode_compound_key, encode_key};
use std::io::Cursor;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }

    // Split input: first byte is split point.
    let split_pct = data[0] as usize;
    let rest = &data[1..];
    if rest.is_empty() {
        return;
    }
    let split = (rest.len() * split_pct) / 256;
    let (a_bytes, b_bytes) = rest.split_at(split);

    // --- Encode all values in the first document (no-panic check) ---
    if let Ok(doc_a) = bson::Document::from_reader(Cursor::new(a_bytes)) {
        for (_, val) in &doc_a {
            let enc1 = encode_key(val);
            // Determinism: encoding the same value twice must give the same result.
            let enc2 = encode_key(val);
            assert_eq!(enc1, enc2, "encode_key is not deterministic");

            // Compound key with a single ascending field must equal the plain key.
            let compound = encode_compound_key(&[(val, true)]);
            assert_eq!(enc1, compound, "single-field compound key must equal plain key");
        }
    }

    // --- Ordering property: encode(a) cmp encode(b) == bson_cmp(a, b) ---
    // We extract the *first* value from each half-document and verify the
    // ordering contract.  Only numeric types have a well-defined cross-type
    // ordering that is straightforward to verify here; for other types we
    // just confirm no panic.
    if let (Ok(doc_a), Ok(doc_b)) = (
        bson::Document::from_reader(Cursor::new(a_bytes)),
        bson::Document::from_reader(Cursor::new(b_bytes)),
    ) {
        if let (Some((_, val_a)), Some((_, val_b))) = (doc_a.iter().next(), doc_b.iter().next()) {
            let enc_a = encode_key(val_a);
            let enc_b = encode_key(val_b);

            // Ordering contract for same-type numeric values:
            // numeric encode should respect the numeric ordering.
            if let (Some(n_a), Some(n_b)) = (bson_to_f64(val_a), bson_to_f64(val_b)) {
                // Skip NaN — comparison is undefined for NaN.
                if !n_a.is_nan() && !n_b.is_nan() {
                    let enc_cmp = enc_a.cmp(&enc_b);
                    let num_cmp = n_a.partial_cmp(&n_b).unwrap();
                    assert_eq!(
                        enc_cmp, num_cmp,
                        "encode ordering mismatch for numerics {n_a} vs {n_b}"
                    );
                }
            }
        }
    }
});

/// Extract a finite f64 from numeric BSON types for ordering verification.
fn bson_to_f64(val: &Bson) -> Option<f64> {
    match val {
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        Bson::Double(d) => Some(*d),
        _ => None,
    }
}
