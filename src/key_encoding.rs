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

// ---------------------------------------------------------------------------
// Type discriminant bytes (MongoDB canonical ordering)
// ---------------------------------------------------------------------------

/// Type discriminant for `MinKey`.
pub(crate) const TYPE_MIN_KEY: u8 = 0x00;
/// Type discriminant for `Null` and `Undefined`.
pub(crate) const TYPE_NULL: u8 = 0x05;
/// Shared type discriminant for all numeric types (Int32, Int64, Double, Decimal128).
pub(crate) const TYPE_NUMBER: u8 = 0x10;
/// Type discriminant for `Symbol` (deprecated in MongoDB 4.0+).
pub(crate) const TYPE_SYMBOL: u8 = 0x15;
/// Type discriminant for UTF-8 `String`.
pub(crate) const TYPE_STRING: u8 = 0x20;
/// Type discriminant for embedded `Document` (Object).
pub(crate) const TYPE_OBJECT: u8 = 0x30;
/// Type discriminant for `Array`.
pub(crate) const TYPE_ARRAY: u8 = 0x40;
/// Type discriminant for `Binary` data.
pub(crate) const TYPE_BIN_DATA: u8 = 0x50;
/// Type discriminant for `ObjectId`.
pub(crate) const TYPE_OBJECT_ID: u8 = 0x60;
/// Type discriminant for `Boolean`.
pub(crate) const TYPE_BOOLEAN: u8 = 0x70;
/// Type discriminant for `DateTime`.
pub(crate) const TYPE_DATE: u8 = 0x80;
/// Type discriminant for `Timestamp`.
pub(crate) const TYPE_TIMESTAMP: u8 = 0x85;
/// Type discriminant for `RegularExpression`.
pub(crate) const TYPE_REGEXP: u8 = 0x90;
/// Type discriminant for `MaxKey`.
pub(crate) const TYPE_MAX_KEY: u8 = 0xFF;

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
/// use mqlite::key_encoding::encode_key;
/// use bson::Bson;
///
/// let k1 = encode_key(&Bson::Int32(-1));
/// let k2 = encode_key(&Bson::Double(0.0));
/// let k3 = encode_key(&Bson::Int64(1));
/// assert!(k1 < k2 && k2 < k3);
/// ```
pub fn encode_key(value: &Bson) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_into(&mut buf, value);
    buf
}

/// Encode a compound index key from a list of `(value, ascending)` pairs.
///
/// Fields are separated by [`COMPOUND_SEP`] (`0x01`).  Descending fields have
/// their encoding bytes bitwise-inverted so that `memcmp` reverses their order.
///
/// # Examples
///
/// ```rust
/// use mqlite::key_encoding::encode_compound_key;
/// use bson::Bson;
///
/// // Compound key: (name ASC, age DESC)
/// let k = encode_compound_key(&[
///     (&Bson::String("Alice".into()), true),
///     (&Bson::Int32(30), false),
/// ]);
/// ```
pub fn encode_compound_key(fields: &[(&Bson, bool)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (i, (value, ascending)) in fields.iter().enumerate() {
        if i > 0 {
            buf.push(COMPOUND_SEP);
        }
        let mut field_bytes = Vec::new();
        encode_into(&mut field_bytes, value);
        if *ascending {
            buf.extend_from_slice(&field_bytes);
        } else {
            buf.extend(field_bytes.iter().map(|b| !b));
        }
    }
    buf
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

    let (class, abs_val) = if v < 0.0 {
        (NUM_NEG, -v)
    } else {
        (NUM_POS, v)
    };

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
mod tests {
    use super::*;
    use bson::spec::BinarySubtype;
    use bson::{doc, oid::ObjectId, Bson, DateTime, Regex, Timestamp};

    // Helper: assert a < b in encoded order.
    fn assert_lt(a: &Bson, b: &Bson) {
        let ka = encode_key(a);
        let kb = encode_key(b);
        assert!(
            ka < kb,
            "expected encode({a:?}) < encode({b:?})\n  got: {ka:02x?}\n  vs:  {kb:02x?}"
        );
    }

    // Helper: assert a == b in encoded order (same sort key).
    fn assert_eq_key(a: &Bson, b: &Bson) {
        let ka = encode_key(a);
        let kb = encode_key(b);
        assert_eq!(
            ka, kb,
            "expected encode({a:?}) == encode({b:?})\n  got: {ka:02x?}\n  vs:  {kb:02x?}"
        );
    }

    // -----------------------------------------------------------------------
    // Type ordering
    // -----------------------------------------------------------------------

    #[test]
    fn type_ordering() {
        let ordered: Vec<Bson> = vec![
            Bson::MinKey,
            Bson::Null,
            Bson::Int32(0),
            Bson::Symbol("sym".into()),
            Bson::String("str".into()),
            Bson::Document(doc! {}),
            Bson::Array(vec![]),
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![],
            }),
            Bson::ObjectId(ObjectId::new()),
            Bson::Boolean(false),
            Bson::DateTime(DateTime::from_millis(0)),
            Bson::Timestamp(Timestamp {
                time: 0,
                increment: 0,
            }),
            Bson::RegularExpression(Regex {
                pattern: "".into(),
                options: "".into(),
            }),
            Bson::MaxKey,
        ];

        for i in 0..ordered.len() {
            for j in i + 1..ordered.len() {
                assert_lt(&ordered[i], &ordered[j]);
            }
        }
    }

    // -----------------------------------------------------------------------
    // Null and Undefined both map to TYPE_NULL
    // -----------------------------------------------------------------------

    #[test]
    fn null_and_undefined_equal() {
        assert_eq_key(&Bson::Null, &Bson::Undefined);
    }

    // -----------------------------------------------------------------------
    // Numeric ordering
    // -----------------------------------------------------------------------

    #[test]
    fn numbers_nan_sorts_first() {
        assert_lt(&Bson::Double(f64::NAN), &Bson::Double(f64::NEG_INFINITY));
        assert_lt(&Bson::Double(f64::NAN), &Bson::Int32(i32::MIN));
    }

    #[test]
    fn numbers_negative_zero_equals_positive_zero() {
        assert_eq_key(&Bson::Double(-0.0), &Bson::Double(0.0));
        assert_eq_key(&Bson::Double(0.0), &Bson::Int32(0));
        assert_eq_key(&Bson::Double(0.0), &Bson::Int64(0));
    }

    #[test]
    fn numbers_integer_types_equal_for_same_value() {
        assert_eq_key(&Bson::Int32(1), &Bson::Int64(1));
        assert_eq_key(&Bson::Int32(1), &Bson::Double(1.0));
        assert_eq_key(&Bson::Int64(-42), &Bson::Double(-42.0));
        assert_eq_key(&Bson::Int32(i32::MAX), &Bson::Int64(i32::MAX as i64));
        assert_eq_key(
            &Bson::Int32(i32::MAX),
            &Bson::Double(i32::MAX as f64),
        );
    }

    #[test]
    fn numbers_ordered_correctly() {
        let vals: Vec<Bson> = vec![
            Bson::Double(f64::NAN),
            Bson::Double(f64::NEG_INFINITY),
            Bson::Int64(i64::MIN),
            Bson::Int32(-1000),
            Bson::Double(-1.5),
            Bson::Int32(-1),
            Bson::Double(-0.5),
            Bson::Int32(0),
            Bson::Double(0.5),
            Bson::Int32(1),
            Bson::Double(1.5),
            Bson::Int64(1000),
            Bson::Int64(i64::MAX),
            Bson::Double(f64::INFINITY),
        ];
        for i in 0..vals.len() {
            for j in i + 1..vals.len() {
                assert_lt(&vals[i], &vals[j]);
            }
        }
    }

    #[test]
    fn numbers_large_i64_precision() {
        // These values are above the safe f64 integer range (2^53).
        // The i64 path must encode them with full precision.
        let a = i64::from(i32::MAX) + 1; // 2_147_483_648 — exactly representable in f64
        let b = a + 1;
        assert_lt(&Bson::Int64(a), &Bson::Int64(b));

        // Values beyond 2^53 that f64 cannot represent exactly.
        let big: i64 = 9_007_199_254_740_993; // 2^53 + 1
        let big_minus_one: i64 = 9_007_199_254_740_992; // 2^53
        assert_lt(&Bson::Int64(big_minus_one), &Bson::Int64(big));

        // The double nearest to big_minus_one (exact) vs. the i64 (also exact).
        assert_eq_key(
            &Bson::Double(9_007_199_254_740_992.0_f64),
            &Bson::Int64(9_007_199_254_740_992_i64),
        );
    }

    // -----------------------------------------------------------------------
    // String encoding
    // -----------------------------------------------------------------------

    #[test]
    fn strings_lexicographic_order() {
        assert_lt(
            &Bson::String("".into()),
            &Bson::String("a".into()),
        );
        assert_lt(
            &Bson::String("a".into()),
            &Bson::String("b".into()),
        );
        assert_lt(
            &Bson::String("abc".into()),
            &Bson::String("abd".into()),
        );
        assert_lt(
            &Bson::String("abc".into()),
            &Bson::String("abcd".into()),
        );
    }

    #[test]
    fn strings_embedded_nulls() {
        // A string with an embedded null should compare less than one without.
        assert_lt(
            &Bson::String("a\x00b".into()),
            &Bson::String("a\x01b".into()),
        );
    }

    // -----------------------------------------------------------------------
    // ObjectId
    // -----------------------------------------------------------------------

    #[test]
    fn object_id_ordering() {
        // ObjectIds generated sequentially should sort in order.
        let oid1 = ObjectId::from_bytes([0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]);
        let oid2 = ObjectId::from_bytes([0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_lt(&Bson::ObjectId(oid1), &Bson::ObjectId(oid2));
    }

    // -----------------------------------------------------------------------
    // Boolean
    // -----------------------------------------------------------------------

    #[test]
    fn boolean_false_before_true() {
        assert_lt(&Bson::Boolean(false), &Bson::Boolean(true));
    }

    // -----------------------------------------------------------------------
    // DateTime
    // -----------------------------------------------------------------------

    #[test]
    fn datetime_chronological_order() {
        let t0 = Bson::DateTime(DateTime::from_millis(0));
        let t1 = Bson::DateTime(DateTime::from_millis(1000));
        let t_neg = Bson::DateTime(DateTime::from_millis(-1000));
        assert_lt(&t_neg, &t0);
        assert_lt(&t0, &t1);
    }

    // -----------------------------------------------------------------------
    // Timestamp
    // -----------------------------------------------------------------------

    #[test]
    fn timestamp_ordering() {
        let ts0 = Bson::Timestamp(Timestamp {
            time: 1,
            increment: 0,
        });
        let ts1 = Bson::Timestamp(Timestamp {
            time: 1,
            increment: 1,
        });
        let ts2 = Bson::Timestamp(Timestamp {
            time: 2,
            increment: 0,
        });
        assert_lt(&ts0, &ts1);
        assert_lt(&ts1, &ts2);
    }

    // -----------------------------------------------------------------------
    // Binary
    // -----------------------------------------------------------------------

    #[test]
    fn binary_subtype_ordering() {
        // Generic (0x00) < Function (0x01)
        let b_generic = Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: vec![],
        });
        let b_function = Bson::Binary(Binary {
            subtype: BinarySubtype::Function,
            bytes: vec![],
        });
        assert_lt(&b_generic, &b_function);
    }

    #[test]
    fn binary_same_subtype_length_then_bytes() {
        let short = Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: vec![0x00],
        });
        let long = Bson::Binary(Binary {
            subtype: BinarySubtype::Generic,
            bytes: vec![0x00, 0x00],
        });
        assert_lt(&short, &long);
    }

    // -----------------------------------------------------------------------
    // RegExp
    // -----------------------------------------------------------------------

    #[test]
    fn regexp_ordering() {
        let re_a = Bson::RegularExpression(Regex {
            pattern: "a".into(),
            options: "".into(),
        });
        let re_b = Bson::RegularExpression(Regex {
            pattern: "b".into(),
            options: "".into(),
        });
        assert_lt(&re_a, &re_b);
    }

    // -----------------------------------------------------------------------
    // Compound key encoding
    // -----------------------------------------------------------------------

    #[test]
    fn compound_key_ascending_both() {
        let a = encode_compound_key(&[(&Bson::Int32(1), true), (&Bson::String("a".into()), true)]);
        let b = encode_compound_key(&[(&Bson::Int32(1), true), (&Bson::String("b".into()), true)]);
        let c = encode_compound_key(&[(&Bson::Int32(2), true), (&Bson::String("a".into()), true)]);
        assert!(a < b, "a < b (second field ascending)");
        assert!(b < c, "b < c (first field ascending)");
    }

    #[test]
    fn compound_key_descending_second_field() {
        // (1, "b") should sort BEFORE (1, "a") when second field is descending.
        let a = encode_compound_key(&[
            (&Bson::Int32(1), true),
            (&Bson::String("a".into()), false),
        ]);
        let b = encode_compound_key(&[
            (&Bson::Int32(1), true),
            (&Bson::String("b".into()), false),
        ]);
        // "b" > "a" ascending, so with descending flag: b_key < a_key
        assert!(b < a, "descending: 'b' sorts before 'a'");
    }

    #[test]
    fn compound_key_missing_field_encoded_as_null() {
        // Missing fields are encoded as Null in MongoDB indexes.
        let missing = encode_compound_key(&[(&Bson::Null, true), (&Bson::Int32(1), true)]);
        let present = encode_compound_key(&[
            (&Bson::Int32(0), true),
            (&Bson::Int32(1), true),
        ]);
        // Null (0x05) < Number (0x10), so missing < present.
        assert!(missing < present);
    }

    // -----------------------------------------------------------------------
    // Property-style test: encode(a) < encode(b) iff compare_bson(a,b) < 0
    // Tests a representative set of cross-type pairs.
    // -----------------------------------------------------------------------

    #[test]
    fn property_cross_type_order_preserved() {
        // A hand-constructed sequence that is monotonically increasing under MongoDB ordering.
        let ordered: Vec<Bson> = vec![
            Bson::MinKey,
            Bson::Null,
            Bson::Double(f64::NAN),
            Bson::Double(f64::NEG_INFINITY),
            Bson::Int64(i64::MIN),
            Bson::Int32(-100),
            Bson::Double(-1.5),
            Bson::Int32(-1),
            Bson::Double(-0.5),
            Bson::Int32(0),  // -0.0 == 0 == +0.0 in MongoDB ordering
            Bson::Double(0.5),
            Bson::Int64(1),
            Bson::Double(1.5),
            Bson::Int32(100),
            Bson::Int64(i64::MAX),
            Bson::Double(f64::INFINITY),
            Bson::Symbol("abc".into()),
            Bson::String("".into()),
            Bson::String("abc".into()),
            Bson::String("abd".into()),
            Bson::String("b".into()),
            Bson::Document(doc! {}),
            Bson::Document(doc! { "a": 1 }),
            Bson::Array(vec![]),
            Bson::Array(vec![Bson::Int32(1)]),
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![],
            }),
            Bson::Binary(Binary {
                subtype: BinarySubtype::Generic,
                bytes: vec![0x01],
            }),
            Bson::Boolean(false),
            Bson::Boolean(true),
            Bson::DateTime(DateTime::from_millis(-1)),
            Bson::DateTime(DateTime::from_millis(0)),
            Bson::DateTime(DateTime::from_millis(1)),
            Bson::Timestamp(Timestamp {
                time: 0,
                increment: 0,
            }),
            Bson::Timestamp(Timestamp {
                time: 1,
                increment: 0,
            }),
            Bson::MaxKey,
        ];

        for i in 0..ordered.len() {
            for j in i + 1..ordered.len() {
                assert_lt(&ordered[i], &ordered[j]);
            }
        }
    }
}
