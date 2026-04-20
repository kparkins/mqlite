//! Tests for `key_encoding.rs`. See [`super`] for the production code.

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
    assert_eq_key(&Bson::Int32(i32::MAX), &Bson::Double(i32::MAX as f64));
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
    assert_lt(&Bson::String("".into()), &Bson::String("a".into()));
    assert_lt(&Bson::String("a".into()), &Bson::String("b".into()));
    assert_lt(&Bson::String("abc".into()), &Bson::String("abd".into()));
    assert_lt(&Bson::String("abc".into()), &Bson::String("abcd".into()));
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
    let a = encode_compound_key(&[(&Bson::Int32(1), true), (&Bson::String("a".into()), false)]);
    let b = encode_compound_key(&[(&Bson::Int32(1), true), (&Bson::String("b".into()), false)]);
    // "b" > "a" ascending, so with descending flag: b_key < a_key
    assert!(b < a, "descending: 'b' sorts before 'a'");
}

#[test]
fn compound_key_missing_field_encoded_as_null() {
    // Missing fields are encoded as Null in MongoDB indexes.
    let missing = encode_compound_key(&[(&Bson::Null, true), (&Bson::Int32(1), true)]);
    let present = encode_compound_key(&[(&Bson::Int32(0), true), (&Bson::Int32(1), true)]);
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
        Bson::Int32(0), // -0.0 == 0 == +0.0 in MongoDB ordering
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
