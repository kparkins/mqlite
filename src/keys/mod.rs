//! BSON key encoding for B+ tree index storage.
//!
//! Produces `memcmp`-sortable byte sequences that preserve MongoDB's full type
//! comparison ordering.
//!
//! # Type ordering
//!
//! ```text
//! MinKey(0x00) < Null(0x05) < Numbers(0x10) < Symbol(0x15) < String(0x20)
//!   < Object(0x30) < Array(0x40) < BinData(0x50) < ObjectId(0x60)
//!   < Boolean(0x70) < Date(0x80) < Timestamp(0x85) < RegExp(0x90) < MaxKey(0xFF)
//! ```
//!
//! # Numeric encoding
//!
//! All numeric types (`Int32`, `Int64`, `Double`, `Decimal128`) share the same type
//! discriminant and are encoded with a 17-byte sign-magnitude representation:
//!
//! ```text
//! [class_byte: 1] [integer_magnitude_be: 8] [fractional_bits_be: 8]
//! ```
//!
//! Values of any numeric type with the same mathematical value produce identical
//! byte sequences, satisfying `encode(a) == encode(b)` when `a == b` numerically.
//!
//! Limitations:
//! - Very large `Double` or `Decimal128` values whose integer part exceeds `u64::MAX`
//!   (roughly 1.8 × 10^19) are clamped to the maximum representable value.  This
//!   is not a problem for typical BSON documents; practical index keys stay well
//!   within `i64` range.
//!
//! # Compound key encoding
//!
//! Compound keys concatenate field encodings separated by `0x01`. Descending fields
//! have their encoding bytes bitwise-inverted so that `memcmp` naturally reverses
//! their sort order.
//!
//! # String encoding
//!
//! Strings are encoded as UTF-8 with null-byte escaping and a `0x00` terminator:
//! - `0x00` → `0x01 0x01`
//! - `0x01` → `0x01 0x02`

use bson::{Binary, Bson, Decimal128, Document};

use crate::error::{Error, Result};

// ---------------------------------------------------------------------------
// Type discriminant bytes (MongoDB canonical ordering)
// ---------------------------------------------------------------------------

const TYPE_MIN_KEY: u8 = 0x00;
const TYPE_NULL: u8 = 0x05;
const TYPE_NUMBER: u8 = 0x10;
const TYPE_SYMBOL: u8 = 0x15;
const TYPE_STRING: u8 = 0x20;
const TYPE_OBJECT: u8 = 0x30;
const TYPE_ARRAY: u8 = 0x40;
const TYPE_BIN_DATA: u8 = 0x50;
const TYPE_OBJECT_ID: u8 = 0x60;
const TYPE_BOOLEAN: u8 = 0x70;
const TYPE_DATE: u8 = 0x80;
const TYPE_TIMESTAMP: u8 = 0x85;
const TYPE_REGEXP: u8 = 0x90;
const TYPE_MAX_KEY: u8 = 0xFF;

// Numeric sub-class bytes (within `TYPE_NUMBER` block).
// Ordering: NAN < NEG < ZERO < POS.
const NUM_NAN: u8 = 0x00; // NaN sorts before all finite values
const NUM_NEG: u8 = 0x40; // negative finite values
const NUM_ZERO: u8 = 0x80; // ±0
const NUM_POS: u8 = 0xC0; // positive finite values

/// Separator byte used between fields in a compound key.
///
/// Fields end with either a fixed-length encoding (numbers, booleans, etc.) or
/// a null-terminated string, so `0x01` cannot appear ambiguously as a separator.
pub(crate) const COMPOUND_SEP: u8 = 0x01;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Encode a single BSON value into a `memcmp`-sortable byte sequence.
///
/// The encoded bytes preserve MongoDB's full type comparison ordering.  Two values
/// `a` and `b` satisfy `encode_key(a) < encode_key(b)` (lexicographically) if and
/// only if `a` compares less than `b` under MongoDB's ordering rules.
///
/// # Examples
///
/// ```rust
/// use mqlite::keys::encode_key;
/// use bson::Bson;
///
/// let k1 = encode_key(&Bson::Int32(-1));
/// let k2 = encode_key(&Bson::Double(0.0));
/// let k3 = encode_key(&Bson::Int64(1));
/// assert!(k1 < k2 && k2 < k3);
/// ```
#[must_use]
pub fn encode_key(value: &Bson) -> Vec<u8> {
    // Most scalar encodings fit in <= 32 bytes (numbers use 18, ObjectId uses 13).
    let mut buf = Vec::with_capacity(32);
    encode_into(&mut buf, value);
    buf
}

/// Encode a compound index key from a list of `(value, ascending)` pairs.
///
/// Fields are separated by `COMPOUND_SEP` (`0x01`).  Descending fields have
/// their encoding bytes bitwise-inverted so that `memcmp` reverses their order.
///
/// # Examples
///
/// ```rust
/// use mqlite::keys::encode_compound_key;
/// use bson::Bson;
///
/// // Compound key: (name ASC, age DESC)
/// let k = encode_compound_key(&[
///     (&Bson::String("Alice".into()), true),
///     (&Bson::Int32(30), false),
/// ]);
/// ```
#[must_use]
pub fn encode_compound_key(fields: &[(&Bson, bool)]) -> Vec<u8> {
    // Rough estimate: 32 bytes per field plus separators.
    let mut buf = Vec::with_capacity(fields.len() * 33);
    for (i, (value, ascending)) in fields.iter().enumerate() {
        if i > 0 {
            buf.push(COMPOUND_SEP);
        }
        if *ascending {
            encode_into(&mut buf, value);
        } else {
            let mut field_bytes = Vec::with_capacity(32);
            encode_into(&mut field_bytes, value);
            for b in &mut field_bytes {
                *b = !*b;
            }
            buf.extend_from_slice(&field_bytes);
        }
    }
    buf
}

/// Return the unique-index prefix range for a staged compound secondary key.
///
/// `field_directions` describes the indexed fields only; the trailing `_id`
/// field is always ascending and is deliberately excluded from the returned
/// range. The returned start key includes the separator before `_id`, matching
/// the historical unique-prefix scan shape.
pub(crate) fn compound_prefix_range_excluding_trailing_id(
    key: &[u8],
    field_directions: &[bool],
) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut offset = 0usize;
    for (idx, ascending) in field_directions.iter().enumerate() {
        if idx > 0 {
            require_byte(key, offset, COMPOUND_SEP, "compound field separator")?;
            offset += 1;
        }
        offset = skip_encoded_value(key, offset, *ascending)?;
    }
    require_byte(key, offset, COMPOUND_SEP, "trailing _id separator")?;
    let split = offset + 1;
    let start = key[..split].to_vec();
    let mut end = start.clone();
    if let Some(last) = end.last_mut() {
        *last = last.saturating_add(1);
    }
    Ok((start, end))
}

fn require_byte(bytes: &[u8], offset: usize, expected: u8, context: &str) -> Result<()> {
    match bytes.get(offset).copied() {
        Some(actual) if actual == expected => Ok(()),
        Some(actual) => Err(Error::Internal(format!(
            "compound key: expected {context} 0x{expected:02x} at {offset}, got 0x{actual:02x}"
        ))),
        None => Err(Error::Internal(format!(
            "compound key: missing {context} at {offset}"
        ))),
    }
}

fn raw_key_byte(bytes: &[u8], offset: usize, ascending: bool) -> Result<u8> {
    let byte = bytes
        .get(offset)
        .copied()
        .ok_or_else(|| Error::Internal(format!("compound key: truncated at {offset}")))?;
    Ok(if ascending { byte } else { !byte })
}

fn skip_encoded_value(bytes: &[u8], offset: usize, ascending: bool) -> Result<usize> {
    let ty = raw_key_byte(bytes, offset, ascending)?;
    let offset = offset + 1;
    match ty {
        TYPE_MIN_KEY | TYPE_NULL | TYPE_MAX_KEY => Ok(offset),
        TYPE_NUMBER => checked_skip(bytes, offset, 17, "number"),
        TYPE_SYMBOL | TYPE_STRING => skip_encoded_string(bytes, offset, ascending),
        TYPE_OBJECT => skip_encoded_document(bytes, offset, ascending),
        TYPE_ARRAY => skip_encoded_array(bytes, offset, ascending),
        TYPE_BIN_DATA => skip_encoded_binary(bytes, offset, ascending),
        TYPE_OBJECT_ID => checked_skip(bytes, offset, 12, "object id"),
        TYPE_BOOLEAN => checked_skip(bytes, offset, 1, "boolean"),
        TYPE_DATE => checked_skip(bytes, offset, 8, "date"),
        TYPE_TIMESTAMP => checked_skip(bytes, offset, 8, "timestamp"),
        TYPE_REGEXP => {
            let after_pattern = skip_encoded_string(bytes, offset, ascending)?;
            skip_encoded_string(bytes, after_pattern, ascending)
        }
        other => Err(Error::Internal(format!(
            "compound key: unknown encoded BSON type 0x{other:02x} at {}",
            offset.saturating_sub(1)
        ))),
    }
}

fn checked_skip(bytes: &[u8], offset: usize, len: usize, context: &str) -> Result<usize> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| Error::Internal(format!("compound key: {context} length overflow")))?;
    if end <= bytes.len() {
        Ok(end)
    } else {
        Err(Error::Internal(format!(
            "compound key: truncated {context} payload at {offset}"
        )))
    }
}

fn skip_encoded_string(bytes: &[u8], mut offset: usize, ascending: bool) -> Result<usize> {
    loop {
        let raw = raw_key_byte(bytes, offset, ascending)?;
        offset += 1;
        if raw == 0 {
            return Ok(offset);
        }
    }
}

fn skip_encoded_document(bytes: &[u8], mut offset: usize, ascending: bool) -> Result<usize> {
    loop {
        if raw_key_byte(bytes, offset, ascending)? == 0 {
            return Ok(offset + 1);
        }
        offset = skip_encoded_string(bytes, offset, ascending)?;
        offset = skip_encoded_value(bytes, offset, ascending)?;
    }
}

fn skip_encoded_array(bytes: &[u8], mut offset: usize, ascending: bool) -> Result<usize> {
    loop {
        if raw_key_byte(bytes, offset, ascending)? == 0 {
            return Ok(offset + 1);
        }
        offset = skip_encoded_value(bytes, offset, ascending)?;
        if raw_key_byte(bytes, offset, ascending)? != 0 {
            return Err(Error::Internal(format!(
                "compound key: array element separator missing at {offset}"
            )));
        }
        offset += 1;
    }
}

fn skip_encoded_binary(bytes: &[u8], offset: usize, ascending: bool) -> Result<usize> {
    let len_offset = checked_skip(bytes, offset, 1, "binary subtype")?;
    let len_end = checked_skip(bytes, len_offset, 4, "binary length")?;
    let mut len_bytes = [0u8; 4];
    for (idx, slot) in len_bytes.iter_mut().enumerate() {
        *slot = raw_key_byte(bytes, len_offset + idx, ascending)?;
    }
    checked_skip(
        bytes,
        len_end,
        u32::from_be_bytes(len_bytes) as usize,
        "binary bytes",
    )
}

// ---------------------------------------------------------------------------
// Core encoding dispatcher
// ---------------------------------------------------------------------------

fn encode_into(buf: &mut Vec<u8>, value: &Bson) {
    match value {
        Bson::MinKey => buf.push(TYPE_MIN_KEY),

        // Null and the deprecated Undefined both sort in the Null slot.
        Bson::Null | Bson::Undefined => {
            buf.push(TYPE_NULL);
        }

        // All numeric types share TYPE_NUMBER and use a unified sign-magnitude encoding
        // so that Int32(1) == Double(1.0) == Int64(1) in sort order.
        Bson::Double(v) => {
            buf.push(TYPE_NUMBER);
            encode_f64(buf, *v);
        }
        Bson::Int32(v) => {
            buf.push(TYPE_NUMBER);
            encode_i64(buf, i64::from(*v));
        }
        Bson::Int64(v) => {
            buf.push(TYPE_NUMBER);
            encode_i64(buf, *v);
        }
        Bson::Decimal128(v) => {
            buf.push(TYPE_NUMBER);
            encode_decimal128(buf, v);
        }

        Bson::Symbol(s) => {
            buf.push(TYPE_SYMBOL);
            encode_string_bytes(buf, s.as_bytes());
        }

        Bson::String(s) => {
            buf.push(TYPE_STRING);
            encode_string_bytes(buf, s.as_bytes());
        }

        Bson::Document(doc) => {
            buf.push(TYPE_OBJECT);
            encode_document(buf, doc);
        }

        Bson::Array(arr) => {
            buf.push(TYPE_ARRAY);
            encode_array(buf, arr);
        }

        Bson::Binary(bin) => {
            buf.push(TYPE_BIN_DATA);
            encode_binary(buf, bin);
        }

        Bson::ObjectId(oid) => {
            buf.push(TYPE_OBJECT_ID);
            // ObjectId's first 4 bytes are a big-endian timestamp, so raw bytes
            // already sort correctly (by time, then machine/pid, then counter).
            buf.extend_from_slice(&oid.bytes());
        }

        Bson::Boolean(b) => {
            buf.push(TYPE_BOOLEAN);
            buf.push(u8::from(*b)); // false=0x00, true=0x01
        }

        Bson::DateTime(dt) => {
            buf.push(TYPE_DATE);
            encode_signed_i64_be(buf, dt.timestamp_millis());
        }

        Bson::Timestamp(ts) => {
            buf.push(TYPE_TIMESTAMP);
            // MongoDB orders Timestamps by (time, increment) — both are u32.
            // Big-endian encoding gives correct unsigned ordering.
            buf.extend_from_slice(&ts.time.to_be_bytes());
            buf.extend_from_slice(&ts.increment.to_be_bytes());
        }

        Bson::RegularExpression(re) => {
            buf.push(TYPE_REGEXP);
            // Pattern first, then options — both null-terminated.
            encode_string_bytes(buf, re.pattern.as_bytes());
            encode_string_bytes(buf, re.options.as_bytes());
        }

        Bson::MaxKey => buf.push(TYPE_MAX_KEY),

        // Deprecated types that appear in older MongoDB data.
        // JavaScript code is treated as a string-category value.
        Bson::JavaScriptCode(s) => {
            buf.push(TYPE_STRING);
            encode_string_bytes(buf, s.as_bytes());
        }
        Bson::JavaScriptCodeWithScope(jswc) => {
            buf.push(TYPE_STRING);
            encode_string_bytes(buf, jswc.code.as_bytes());
        }
        // DbPointer is deprecated; give it its own slot (same as RegExp for ordering).
        // Its fields are crate-private in bson 2.x; use Debug representation for stable bytes.
        Bson::DbPointer(dp) => {
            buf.push(TYPE_REGEXP);
            let repr = format!("{dp:?}");
            encode_string_bytes(buf, repr.as_bytes());
        }
    }
}

// ---------------------------------------------------------------------------
// Numeric encoding helpers
// ---------------------------------------------------------------------------

/// Sign-magnitude encoding for `f64` values.
///
/// Produces 17 bytes: 1 sub-class byte + 8 integer-part bytes + 8 fractional-part bytes.
///
/// Ordering within `TYPE_NUMBER`:
///   `NaN < -∞ < negatives < -0 = +0 < positives < +∞`
///
/// Integer-valued doubles produce the **same** bytes as the equivalent `i64`/`i32`,
/// satisfying `encode(1_i64) == encode(1.0_f64)`.
///
/// Limitation: the integer part is clamped to `u64::MAX` (≈ 1.8 × 10^19).  Values
/// beyond this range are indistinguishable in sort order from the boundary.
fn encode_f64(buf: &mut Vec<u8>, v: f64) {
    if v.is_nan() {
        buf.push(NUM_NAN);
        buf.extend_from_slice(&[0u8; 16]);
        return;
    }

    // +0.0 and -0.0 are equal in MongoDB comparison.
    if v == 0.0 {
        buf.push(NUM_ZERO);
        buf.extend_from_slice(&[0u8; 16]);
        return;
    }

    // Encode ±infinity as boundary sentinels that sort beyond any finite value.
    if v == f64::INFINITY {
        buf.push(NUM_POS);
        buf.extend_from_slice(&[0xFF; 16]); // maximum positive sentinel
        return;
    }
    if v == f64::NEG_INFINITY {
        buf.push(NUM_NEG);
        buf.extend_from_slice(&[0x00; 16]); // minimum negative sentinel
        return;
    }

    let (class, abs_val) = if v < 0.0 { (NUM_NEG, -v) } else { (NUM_POS, v) };

    // Integer part: truncate to u64 (saturating for very large values).
    let int_part: u64 = abs_val.trunc() as u64;
    // Fractional part: scale [0, 1) to [0, u64::MAX] and truncate.
    // `abs_val.fract()` is always in [0, 1) for finite non-infinite doubles.
    let frac_part: u64 = (abs_val.fract() * (u64::MAX as f64)) as u64;

    buf.push(class);
    if class == NUM_NEG {
        // Invert both parts: larger absolute value → smaller encoded value,
        // so more-negative numbers sort before less-negative ones.
        buf.extend_from_slice(&(!int_part).to_be_bytes());
        buf.extend_from_slice(&(!frac_part).to_be_bytes());
    } else {
        buf.extend_from_slice(&int_part.to_be_bytes());
        buf.extend_from_slice(&frac_part.to_be_bytes());
    }
}

/// Sign-magnitude encoding for `i64` values.
///
/// Produces 17 bytes in the same format as [`encode_f64`], with the fractional
/// part always zero.  This ensures `encode_i64(n) == encode_f64(n as f64)` for
/// all values where the conversion is exact (|n| ≤ 2^53).
///
/// For large integers (|n| > 2^53) that cannot be exactly represented as `f64`,
/// the integer path preserves full precision while the float path would lose it.
fn encode_i64(buf: &mut Vec<u8>, v: i64) {
    if v == 0 {
        buf.push(NUM_ZERO);
        buf.extend_from_slice(&[0u8; 16]);
        return;
    }

    let (class, abs_val) = if v < 0 {
        (NUM_NEG, v.unsigned_abs())
    } else {
        (NUM_POS, v as u64)
    };

    buf.push(class);
    if class == NUM_NEG {
        buf.extend_from_slice(&(!abs_val).to_be_bytes()); // 8 bytes integer
        buf.extend_from_slice(&[0xFF; 8]); // inverted zero fractional part
    } else {
        buf.extend_from_slice(&abs_val.to_be_bytes()); // 8 bytes integer
        buf.extend_from_slice(&[0x00; 8]); // zero fractional part
    }
}

/// Best-effort `Decimal128` encoding via its string representation.
///
/// Parses the canonical decimal string produced by `Display` and converts to
/// `f64` for encoding.  This is accurate to ~15–16 significant digits and
/// correctly handles `NaN`, `Infinity`, and zero.
///
/// For values that cannot be represented in `f64` without loss (e.g., very high
/// precision Decimal128 values), the sort order may differ from MongoDB's.  This
/// is a known limitation for the initial implementation.
fn encode_decimal128(buf: &mut Vec<u8>, v: &Decimal128) {
    let s = v.to_string();
    // The bson Display renders: "-Inf", "Inf", "NaN", or decimal notation.
    let f: f64 = match s.as_str() {
        "Inf" | "Infinity" => f64::INFINITY,
        "-Inf" | "-Infinity" => f64::NEG_INFINITY,
        "NaN" => f64::NAN,
        other => other.parse().unwrap_or(f64::NAN),
    };
    encode_f64(buf, f);
}

// ---------------------------------------------------------------------------
// String encoding
// ---------------------------------------------------------------------------

/// Encode a byte string with null-byte escaping and a `0x00` null terminator.
///
/// Escape sequences:
/// - `0x00` → `0x01 0x01`  (embedded null)
/// - `0x01` → `0x01 0x02`  (escape the escape byte itself)
///
/// The escaping ensures that no field encoding contains a bare `0x00` before the
/// terminator, preserving `memcmp` correctness across variable-length fields.
fn encode_string_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.reserve(bytes.len() + 1);
    for &b in bytes {
        match b {
            0x00 => {
                buf.push(0x01);
                buf.push(0x01);
            }
            0x01 => {
                buf.push(0x01);
                buf.push(0x02);
            }
            other => buf.push(other),
        }
    }
    buf.push(0x00); // null terminator
}

// ---------------------------------------------------------------------------
// Composite type helpers
// ---------------------------------------------------------------------------

/// Encode a BSON document for comparison.
///
/// Fields are emitted in BSON key order.  Each field is encoded as:
/// `[null-terminated key] [value encoding]`.  The document ends with `0x00`.
fn encode_document(buf: &mut Vec<u8>, doc: &Document) {
    for (key, val) in doc.iter() {
        encode_string_bytes(buf, key.as_bytes());
        encode_into(buf, val);
    }
    buf.push(0x00); // document end sentinel
}

/// Encode a BSON array for comparison.
///
/// Each element encoding is followed by `0x00` as an element separator.
/// The array ends with an additional `0x00`.
fn encode_array(buf: &mut Vec<u8>, arr: &[Bson]) {
    for val in arr {
        encode_into(buf, val);
        buf.push(0x00); // element separator
    }
    buf.push(0x00); // array end sentinel
}

/// Encode a `Binary` value: subtype byte, big-endian length (4 bytes), then data.
///
/// MongoDB's `BinData` ordering is: subtype first, then length, then bytes.
fn encode_binary(buf: &mut Vec<u8>, bin: &Binary) {
    let subtype_byte: u8 = bin.subtype.into();
    buf.push(subtype_byte);
    let len = bin.bytes.len() as u32;
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&bin.bytes);
}

// ---------------------------------------------------------------------------
// Signed integer big-endian helper
// ---------------------------------------------------------------------------

/// Encode a signed `i64` in big-endian order with the sign bit flipped.
///
/// Mapping:
/// - `i64::MIN` → `0x0000_0000_0000_0000`  (smallest)
/// - `0`        → `0x8000_0000_0000_0000`  (middle)
/// - `i64::MAX` → `0xFFFF_FFFF_FFFF_FFFF`  (largest)
///
/// Used for `DateTime` (milliseconds since Unix epoch) to produce chronological
/// sort order.
fn encode_signed_i64_be(buf: &mut Vec<u8>, v: i64) {
    let encoded = (v as u64) ^ 0x8000_0000_0000_0000_u64;
    buf.extend_from_slice(&encoded.to_be_bytes());
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
