//! Unit tests for the aggregation-expression evaluator ([`super`]).
//!
//! Covers every operator's happy path, null/missing propagation, arity and
//! type errors, system variables (`$$ROOT`/`$$NOW`/`$$CURRENT`), user
//! variable binding via `$map`/`$filter`, computed-document field omission,
//! nested operator composition, date extraction against known epochs, and
//! `$literal` escaping.

use super::*;
use bson::{doc, Bson, DateTime};

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

/// Evaluate `expr` against an empty document, expecting success.
fn ev(expr: Bson) -> Value {
    let doc = doc! {};
    let ctx = ExprContext::new(&doc);
    eval_expr(&expr, &ctx).expect("eval_expr should succeed")
}

/// Evaluate `expr` against `doc`, expecting success.
fn ev_doc(expr: Bson, doc: Document) -> Value {
    let ctx = ExprContext::new(&doc);
    eval_expr(&expr, &ctx).expect("eval_expr should succeed")
}

/// Evaluate `expr` against `doc`, expecting an error.
fn err_doc(expr: Bson, doc: Document) -> Error {
    let ctx = ExprContext::new(&doc);
    eval_expr(&expr, &ctx).expect_err("eval_expr should error")
}

/// Evaluate `expr` against an empty document, expecting an error.
fn err(expr: Bson) -> Error {
    err_doc(expr, doc! {})
}

/// Shorthand: an evaluated `Some(value)`.
fn some(v: impl Into<Bson>) -> Value {
    Some(v.into())
}

// -----------------------------------------------------------------------
// Literals, field paths, variables
// -----------------------------------------------------------------------

#[test]
fn self_evaluating_literals() {
    assert_eq!(ev(Bson::Int32(5)), some(5_i32));
    assert_eq!(ev(Bson::Boolean(true)), some(true));
    assert_eq!(ev(Bson::Null), Some(Bson::Null));
    assert_eq!(ev(Bson::Double(2.5)), some(2.5));
}

#[test]
fn plain_string_is_literal() {
    assert_eq!(ev(Bson::String("hello".into())), some("hello"));
}

#[test]
fn field_path_simple_and_dotted() {
    let doc = doc! { "a": { "b": 7 }, "x": 3 };
    assert_eq!(ev_doc(Bson::String("$x".into()), doc.clone()), some(3_i32));
    assert_eq!(ev_doc(Bson::String("$a.b".into()), doc), some(7_i32));
}

#[test]
fn field_path_missing_is_none() {
    assert_eq!(ev(Bson::String("$nope".into())), None);
    let doc = doc! { "a": { "b": 1 } };
    assert_eq!(ev_doc(Bson::String("$a.c".into()), doc), None);
}

#[test]
fn field_path_no_array_traversal_divergence() {
    // Path into an array of sub-docs yields MISSING (mqlite divergence; the
    // server would collect [1, 2] here).
    let doc = doc! { "items": [ { "v": 1 }, { "v": 2 } ] };
    assert_eq!(ev_doc(Bson::String("$items.v".into()), doc), None);
}

#[test]
fn var_root_and_current() {
    let doc = doc! { "a": 1, "b": 2 };
    let expected = Some(Bson::Document(doc.clone()));
    assert_eq!(ev_doc(Bson::String("$$ROOT".into()), doc.clone()), expected);
    assert_eq!(ev_doc(Bson::String("$$CURRENT".into()), doc), expected);
}

#[test]
fn var_root_with_dotted_suffix() {
    let doc = doc! { "a": { "b": 9 } };
    assert_eq!(ev_doc(Bson::String("$$ROOT.a.b".into()), doc), some(9_i32));
}

#[test]
fn var_now_is_frozen_datetime() {
    let doc = doc! {};
    let now = DateTime::from_millis(1_700_000_000_000);
    let ctx = ExprContext::with_now(&doc, now);
    let value = eval_expr(&Bson::String("$$NOW".into()), &ctx).unwrap();
    assert_eq!(value, Some(Bson::DateTime(now)));
}

#[test]
fn undefined_variable_errors() {
    let e = err(Bson::String("$$bogus".into()));
    assert!(format!("{e}").contains("Use of undefined variable: bogus"));
}

#[test]
fn user_var_binding_via_with_var() {
    let doc = doc! {};
    let ctx = ExprContext::new(&doc).with_var("this", Bson::Int32(42));
    let value = eval_expr(&Bson::String("$$this".into()), &ctx).unwrap();
    assert_eq!(value, some(42_i32));
}

// -----------------------------------------------------------------------
// $literal
// -----------------------------------------------------------------------

#[test]
fn literal_escapes_dollar_strings() {
    // {$literal: "$x"} returns the literal "$x", not a field path.
    let expr = doc! { "$literal": "$x" };
    assert_eq!(ev(Bson::Document(expr)), some("$x"));
}

#[test]
fn literal_escapes_operator_document() {
    let expr = doc! { "$literal": { "$add": [1, 2] } };
    let value = ev(Bson::Document(expr));
    assert_eq!(value, Some(Bson::Document(doc! { "$add": [1, 2] })));
}

// -----------------------------------------------------------------------
// Computed documents and arrays
// -----------------------------------------------------------------------

#[test]
fn computed_document_evaluates_fields() {
    let doc = doc! { "x": 2 };
    let expr = doc! { "sum": { "$add": ["$x", 3] }, "name": "fixed" };
    let value = ev_doc(Bson::Document(expr), doc);
    assert_eq!(
        value,
        Some(Bson::Document(doc! { "sum": 5_i32, "name": "fixed" }))
    );
}

#[test]
fn computed_document_omits_missing_fields() {
    let doc = doc! { "x": 1 };
    let expr = doc! { "present": "$x", "absent": "$missing" };
    let value = ev_doc(Bson::Document(expr), doc);
    assert_eq!(value, Some(Bson::Document(doc! { "present": 1_i32 })));
}

#[test]
fn array_expr_missing_elements_become_null() {
    let doc = doc! { "x": 1 };
    let expr = Bson::Array(vec![
        Bson::String("$x".into()),
        Bson::String("$missing".into()),
    ]);
    let value = ev_doc(expr, doc);
    assert_eq!(value, Some(Bson::Array(vec![Bson::Int32(1), Bson::Null])));
}

#[test]
fn mixed_dollar_and_plain_keys_errors() {
    let expr = doc! { "$add": [1, 2], "extra": 3 };
    let e = err(Bson::Document(expr));
    assert!(format!("{e}").contains("exactly one field"));
}

// -----------------------------------------------------------------------
// Comparison operators
// -----------------------------------------------------------------------

#[test]
fn comparison_operators() {
    assert_eq!(ev(Bson::Document(doc! { "$eq": [1, 1] })), some(true));
    assert_eq!(ev(Bson::Document(doc! { "$eq": [1, 2] })), some(false));
    assert_eq!(ev(Bson::Document(doc! { "$ne": [1, 2] })), some(true));
    assert_eq!(ev(Bson::Document(doc! { "$gt": [3, 2] })), some(true));
    assert_eq!(ev(Bson::Document(doc! { "$gte": [2, 2] })), some(true));
    assert_eq!(ev(Bson::Document(doc! { "$lt": [1, 2] })), some(true));
    assert_eq!(ev(Bson::Document(doc! { "$lte": [2, 2] })), some(true));
}

#[test]
fn cmp_returns_int() {
    assert_eq!(ev(Bson::Document(doc! { "$cmp": [1, 2] })), some(-1_i32));
    assert_eq!(ev(Bson::Document(doc! { "$cmp": [2, 2] })), some(0_i32));
    assert_eq!(ev(Bson::Document(doc! { "$cmp": [3, 2] })), some(1_i32));
}

#[test]
fn comparison_missing_treated_as_null() {
    // Missing field compares as null; null < 1 so $lt is true.
    let doc = doc! {};
    let expr = doc! { "$lt": ["$missing", 1] };
    assert_eq!(ev_doc(Bson::Document(expr), doc), some(true));
}

#[test]
fn comparison_arity_error() {
    let e = err(Bson::Document(doc! { "$eq": [1] }));
    assert!(format!("{e}").contains("$eq takes exactly 2 arguments. 1 were passed"));
}

// -----------------------------------------------------------------------
// Arithmetic operators
// -----------------------------------------------------------------------

#[test]
fn add_variadic_integers() {
    assert_eq!(ev(Bson::Document(doc! { "$add": [1, 2, 3] })), some(6_i32));
}

#[test]
fn add_promotes_to_double() {
    assert_eq!(ev(Bson::Document(doc! { "$add": [1, 2.5] })), some(3.5));
}

#[test]
fn add_date_plus_millis() {
    let base = DateTime::from_millis(1000);
    let expr = doc! { "$add": [Bson::DateTime(base), 500_i32] };
    assert_eq!(
        ev(Bson::Document(expr)),
        Some(Bson::DateTime(DateTime::from_millis(1500)))
    );
}

#[test]
fn add_null_propagates() {
    assert_eq!(
        ev(Bson::Document(doc! { "$add": [1, Bson::Null] })),
        Some(Bson::Null)
    );
    let doc = doc! {};
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$add": [1, "$missing"] }), doc),
        Some(Bson::Null)
    );
}

#[test]
fn subtract_numbers_and_dates() {
    assert_eq!(ev(Bson::Document(doc! { "$subtract": [5, 2] })), some(3_i32));
    let a = DateTime::from_millis(5000);
    let b = DateTime::from_millis(2000);
    let expr = doc! { "$subtract": [Bson::DateTime(a), Bson::DateTime(b)] };
    assert_eq!(ev(Bson::Document(expr)), some(3000_i64));
    let expr2 = doc! { "$subtract": [Bson::DateTime(a), 1000_i32] };
    assert_eq!(
        ev(Bson::Document(expr2)),
        Some(Bson::DateTime(DateTime::from_millis(4000)))
    );
}

#[test]
fn multiply_variadic() {
    assert_eq!(
        ev(Bson::Document(doc! { "$multiply": [2, 3, 4] })),
        some(24_i32)
    );
}

#[test]
fn divide_always_double_and_zero_errors() {
    assert_eq!(ev(Bson::Document(doc! { "$divide": [6, 2] })), some(3.0));
    let e = err(Bson::Document(doc! { "$divide": [1, 0] }));
    assert!(format!("{e}").contains("can't $divide by zero"));
}

#[test]
fn mod_preserves_int_and_zero_errors() {
    assert_eq!(ev(Bson::Document(doc! { "$mod": [7, 3] })), some(1_i32));
    assert_eq!(ev(Bson::Document(doc! { "$mod": [7.5, 2.0] })), some(1.5));
    let e = err(Bson::Document(doc! { "$mod": [1, 0] }));
    assert!(format!("{e}").contains("can't $mod by zero"));
}

#[test]
fn abs_ceil_floor_trunc() {
    assert_eq!(ev(Bson::Document(doc! { "$abs": -5_i32 })), some(5_i32));
    assert_eq!(ev(Bson::Document(doc! { "$ceil": 2.1 })), some(3.0));
    assert_eq!(ev(Bson::Document(doc! { "$floor": 2.9 })), some(2.0));
    assert_eq!(ev(Bson::Document(doc! { "$trunc": -2.7 })), some(-2.0));
}

#[test]
fn round_with_and_without_place() {
    // Round-half-to-even.
    assert_eq!(ev(Bson::Document(doc! { "$round": [2.5] })), some(2.0));
    assert_eq!(ev(Bson::Document(doc! { "$round": [3.5] })), some(4.0));
    assert_eq!(
        ev(Bson::Document(doc! { "$round": [1.23456, 2] })),
        some(1.23)
    );
}

#[test]
fn pow_sqrt_exp_logs() {
    assert_eq!(ev(Bson::Document(doc! { "$pow": [2, 10] })), some(1024_i32));
    assert_eq!(ev(Bson::Document(doc! { "$pow": [2.0, 3] })), some(8.0));
    assert_eq!(ev(Bson::Document(doc! { "$sqrt": 9 })), some(3.0));
    assert_eq!(ev(Bson::Document(doc! { "$ln": 1 })), some(0.0));
    assert_eq!(ev(Bson::Document(doc! { "$log10": 1000 })), some(3.0));
}

#[test]
fn sqrt_negative_and_log_nonpositive_error() {
    assert!(matches!(
        err(Bson::Document(doc! { "$sqrt": -1 })),
        Error::BsonDeserialization(_)
    ));
    assert!(matches!(
        err(Bson::Document(doc! { "$ln": 0 })),
        Error::BsonDeserialization(_)
    ));
}

#[test]
fn unary_math_null_in_null_out() {
    assert_eq!(
        ev(Bson::Document(doc! { "$abs": Bson::Null })),
        Some(Bson::Null)
    );
    let doc = doc! {};
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$ceil": "$missing" }), doc),
        Some(Bson::Null)
    );
}

#[test]
fn unary_math_non_numeric_errors() {
    let e = err(Bson::Document(doc! { "$abs": "text" }));
    assert!(format!("{e}").contains("$abs only supports numeric"));
}

// -----------------------------------------------------------------------
// Boolean operators
// -----------------------------------------------------------------------

#[test]
fn and_or_not_truthiness() {
    assert_eq!(ev(Bson::Document(doc! { "$and": [true, 1, "x"] })), some(true));
    assert_eq!(ev(Bson::Document(doc! { "$and": [true, 0] })), some(false));
    assert_eq!(ev(Bson::Document(doc! { "$or": [false, 0, "y"] })), some(true));
    assert_eq!(ev(Bson::Document(doc! { "$or": [false, 0] })), some(false));
    assert_eq!(ev(Bson::Document(doc! { "$not": [false] })), some(true));
    assert_eq!(ev(Bson::Document(doc! { "$not": [1] })), some(false));
}

#[test]
fn and_short_circuits_before_error() {
    // The second operand would error if evaluated, but $and stops at the
    // falsy first operand.
    let expr = doc! { "$and": [false, { "$divide": [1, 0] }] };
    assert_eq!(ev(Bson::Document(expr)), some(false));
}

// -----------------------------------------------------------------------
// Conditional operators
// -----------------------------------------------------------------------

#[test]
fn cond_array_and_doc_forms() {
    assert_eq!(
        ev(Bson::Document(doc! { "$cond": [true, "yes", "no"] })),
        some("yes")
    );
    let doc_form = doc! { "$cond": { "if": false, "then": 1, "else": 2 } };
    assert_eq!(ev(Bson::Document(doc_form)), some(2_i32));
}

#[test]
fn if_null_returns_first_non_null() {
    let doc = doc! { "a": Bson::Null, "b": 7 };
    let expr = doc! { "$ifNull": ["$a", "$b", "fallback"] };
    assert_eq!(ev_doc(Bson::Document(expr), doc), some(7_i32));
}

#[test]
fn if_null_returns_last_when_all_null() {
    let doc = doc! {};
    let expr = doc! { "$ifNull": ["$missing", "$alsoMissing", "default"] };
    assert_eq!(ev_doc(Bson::Document(expr), doc), some("default"));
}

#[test]
fn switch_matches_branch_and_default() {
    let doc = doc! { "score": 2 };
    let expr = doc! {
        "$switch": {
            "branches": [
                { "case": { "$eq": ["$score", 1] }, "then": "one" },
                { "case": { "$eq": ["$score", 2] }, "then": "two" },
            ],
            "default": "other"
        }
    };
    assert_eq!(ev_doc(Bson::Document(expr), doc), some("two"));
}

#[test]
fn switch_no_match_no_default_errors() {
    let expr = doc! {
        "$switch": { "branches": [ { "case": false, "then": 1 } ] }
    };
    let e = err(Bson::Document(expr));
    assert!(format!("{e}").contains("no default was specified"));
}

// -----------------------------------------------------------------------
// String operators
// -----------------------------------------------------------------------

#[test]
fn concat_and_null_propagation() {
    assert_eq!(
        ev(Bson::Document(doc! { "$concat": ["a", "b", "c"] })),
        some("abc")
    );
    assert_eq!(
        ev(Bson::Document(doc! { "$concat": ["a", Bson::Null] })),
        Some(Bson::Null)
    );
}

#[test]
fn concat_non_string_errors() {
    let e = err(Bson::Document(doc! { "$concat": ["a", 1] }));
    assert!(format!("{e}").contains("$concat only supports strings"));
}

#[test]
fn to_upper_lower() {
    assert_eq!(ev(Bson::Document(doc! { "$toUpper": "abc" })), some("ABC"));
    assert_eq!(ev(Bson::Document(doc! { "$toLower": "ABC" })), some("abc"));
    let doc = doc! {};
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$toUpper": "$missing" }), doc),
        Some(Bson::Null)
    );
}

#[test]
fn str_len_cp_counts_code_points() {
    // "héllo" — 'é' is one code point.
    assert_eq!(ev(Bson::Document(doc! { "$strLenCP": "héllo" })), some(5_i32));
}

#[test]
fn substr_cp_code_points() {
    assert_eq!(
        ev(Bson::Document(doc! { "$substrCP": ["hello", 1, 3] })),
        some("ell")
    );
    // Code-point based: skip past the multibyte 'é'.
    assert_eq!(
        ev(Bson::Document(doc! { "$substrCP": ["héllo", 2, 2] })),
        some("ll")
    );
}

#[test]
fn split_string() {
    let expr = doc! { "$split": ["a,b,c", ","] };
    assert_eq!(
        ev(Bson::Document(expr)),
        Some(Bson::Array(vec![
            Bson::String("a".into()),
            Bson::String("b".into()),
            Bson::String("c".into()),
        ]))
    );
    let doc = doc! {};
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$split": ["$missing", ","] }), doc),
        Some(Bson::Null)
    );
}

#[test]
fn trim_variants() {
    assert_eq!(
        ev(Bson::Document(doc! { "$trim": { "input": "  hi  " } })),
        some("hi")
    );
    assert_eq!(
        ev(Bson::Document(doc! { "$ltrim": { "input": "  hi  " } })),
        some("hi  ")
    );
    assert_eq!(
        ev(Bson::Document(doc! { "$rtrim": { "input": "  hi  " } })),
        some("  hi")
    );
    assert_eq!(
        ev(Bson::Document(
            doc! { "$trim": { "input": "xxhixx", "chars": "x" } }
        )),
        some("hi")
    );
}

#[test]
fn to_string_conversions() {
    assert_eq!(ev(Bson::Document(doc! { "$toString": 42 })), some("42"));
    assert_eq!(ev(Bson::Document(doc! { "$toString": true })), some("true"));
    let e = err(Bson::Document(doc! { "$toString": doc! { "a": 1 } }));
    assert!(format!("{e}").contains("$toString does not support object"));
}

// -----------------------------------------------------------------------
// Array operators
// -----------------------------------------------------------------------

#[test]
fn size_and_is_array() {
    // A bare array is an argument LIST (MongoDB convention): 3 args -> error.
    let e = err(Bson::Document(doc! { "$size": [1, 2, 3] }));
    assert!(format!("{e}").contains("takes exactly 1 arguments"));
    // A nested array is one argument evaluating to the array itself.
    assert_eq!(ev(Bson::Document(doc! { "$size": [[1, 2, 3]] })), some(3_i32));
    // Wrap in $literal so the array is a value, not an argument list:
    let lit = doc! { "$size": { "$literal": [9, 9] } };
    assert_eq!(ev(Bson::Document(lit)), some(2_i32));
    let e = err(Bson::Document(doc! { "$size": "notarray" }));
    assert!(format!("{e}").contains("must be an array"));
    assert_eq!(
        ev(Bson::Document(doc! { "$isArray": { "$literal": [1] } })),
        some(true)
    );
    assert_eq!(ev(Bson::Document(doc! { "$isArray": 5 })), some(false));
}

#[test]
fn in_membership() {
    let doc = doc! { "h": [1, 2, 3] };
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$in": [2, "$h"] }), doc.clone()),
        some(true)
    );
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$in": [9, "$h"] }), doc),
        some(false)
    );
    let e = err(Bson::Document(doc! { "$in": [1, 2] }));
    assert!(format!("{e}").contains("$in requires an array"));
}

#[test]
fn array_elem_at_negative_and_oob() {
    let doc = doc! { "a": [10, 20, 30] };
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$arrayElemAt": ["$a", 0] }), doc.clone()),
        some(10_i32)
    );
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$arrayElemAt": ["$a", -1] }), doc.clone()),
        some(30_i32)
    );
    // Out of range -> missing (None).
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$arrayElemAt": ["$a", 5] }), doc),
        None
    );
}

#[test]
fn first_last() {
    let doc = doc! { "a": [1, 2, 3] };
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$first": "$a" }), doc.clone()),
        some(1_i32)
    );
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$last": "$a" }), doc),
        some(3_i32)
    );
    // Empty array -> missing.
    let empty = doc! { "a": Bson::Array(vec![]) };
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$first": "$a" }), empty),
        None
    );
}

#[test]
fn concat_arrays() {
    let expr = doc! {
        "$concatArrays": [ { "$literal": [1, 2] }, { "$literal": [3] } ]
    };
    assert_eq!(
        ev(Bson::Document(expr)),
        Some(Bson::Array(vec![
            Bson::Int32(1),
            Bson::Int32(2),
            Bson::Int32(3),
        ]))
    );
    let null_expr = doc! { "$concatArrays": [ { "$literal": [1] }, Bson::Null ] };
    assert_eq!(ev(Bson::Document(null_expr)), Some(Bson::Null));
}

#[test]
fn slice_two_and_three_arg() {
    let doc = doc! { "a": [1, 2, 3, 4, 5] };
    // First 2.
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$slice": ["$a", 2] }), doc.clone()),
        Some(Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]))
    );
    // Last 2.
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$slice": ["$a", -2] }), doc.clone()),
        Some(Bson::Array(vec![Bson::Int32(4), Bson::Int32(5)]))
    );
    // Position 1, take 2.
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$slice": ["$a", 1, 2] }), doc),
        Some(Bson::Array(vec![Bson::Int32(2), Bson::Int32(3)]))
    );
}

#[test]
fn filter_with_default_this() {
    let doc = doc! { "nums": [1, 2, 3, 4] };
    let expr = doc! {
        "$filter": {
            "input": "$nums",
            "cond": { "$gt": ["$$this", 2] }
        }
    };
    assert_eq!(
        ev_doc(Bson::Document(expr), doc),
        Some(Bson::Array(vec![Bson::Int32(3), Bson::Int32(4)]))
    );
}

#[test]
fn filter_with_custom_as() {
    let doc = doc! { "nums": [1, 2, 3] };
    let expr = doc! {
        "$filter": {
            "input": "$nums",
            "as": "n",
            "cond": { "$lt": ["$$n", 3] }
        }
    };
    assert_eq!(
        ev_doc(Bson::Document(expr), doc),
        Some(Bson::Array(vec![Bson::Int32(1), Bson::Int32(2)]))
    );
}

#[test]
fn map_with_this_and_custom_as() {
    let doc = doc! { "nums": [1, 2, 3] };
    let expr = doc! {
        "$map": { "input": "$nums", "in": { "$multiply": ["$$this", 10] } }
    };
    assert_eq!(
        ev_doc(Bson::Document(expr), doc.clone()),
        Some(Bson::Array(vec![
            Bson::Int32(10),
            Bson::Int32(20),
            Bson::Int32(30),
        ]))
    );
    let expr2 = doc! {
        "$map": { "input": "$nums", "as": "v", "in": { "$add": ["$$v", 1] } }
    };
    assert_eq!(
        ev_doc(Bson::Document(expr2), doc),
        Some(Bson::Array(vec![
            Bson::Int32(2),
            Bson::Int32(3),
            Bson::Int32(4),
        ]))
    );
}

#[test]
fn filter_map_null_input() {
    let doc = doc! {};
    assert_eq!(
        ev_doc(
            Bson::Document(doc! {
                "$filter": { "input": "$missing", "cond": true }
            }),
            doc.clone()
        ),
        Some(Bson::Null)
    );
    assert_eq!(
        ev_doc(
            Bson::Document(doc! { "$map": { "input": "$missing", "in": 1 } }),
            doc
        ),
        Some(Bson::Null)
    );
}

#[test]
fn range_two_and_three_arg() {
    assert_eq!(
        ev(Bson::Document(doc! { "$range": [0, 4] })),
        Some(Bson::Array(vec![
            Bson::Int32(0),
            Bson::Int32(1),
            Bson::Int32(2),
            Bson::Int32(3),
        ]))
    );
    assert_eq!(
        ev(Bson::Document(doc! { "$range": [0, 10, 3] })),
        Some(Bson::Array(vec![
            Bson::Int32(0),
            Bson::Int32(3),
            Bson::Int32(6),
            Bson::Int32(9),
        ]))
    );
    assert_eq!(
        ev(Bson::Document(doc! { "$range": [5, 0, -2] })),
        Some(Bson::Array(vec![
            Bson::Int32(5),
            Bson::Int32(3),
            Bson::Int32(1),
        ]))
    );
    let e = err(Bson::Document(doc! { "$range": [0, 5, 0] }));
    assert!(format!("{e}").contains("non-zero step"));
}

// -----------------------------------------------------------------------
// Type / conversion operators
// -----------------------------------------------------------------------

#[test]
fn type_operator() {
    assert_eq!(ev(Bson::Document(doc! { "$type": 1 })), some("int"));
    assert_eq!(ev(Bson::Document(doc! { "$type": "s" })), some("string"));
    // Missing -> "missing".
    let doc = doc! {};
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$type": "$missing" }), doc),
        some("missing")
    );
}

#[test]
fn to_int_long_double() {
    assert_eq!(ev(Bson::Document(doc! { "$toInt": 3.9 })), some(3_i32));
    assert_eq!(ev(Bson::Document(doc! { "$toLong": 5 })), some(5_i64));
    assert_eq!(ev(Bson::Document(doc! { "$toDouble": 2 })), some(2.0));
    assert_eq!(ev(Bson::Document(doc! { "$toInt": "7" })), some(7_i32));
}

#[test]
fn to_bool_all_strings_true() {
    // MongoDB 8.0: every string (including "false" and "") converts to true.
    assert_eq!(ev(Bson::Document(doc! { "$toBool": "false" })), some(true));
    assert_eq!(ev(Bson::Document(doc! { "$toBool": "" })), some(true));
    assert_eq!(ev(Bson::Document(doc! { "$toBool": 0 })), some(false));
    assert_eq!(ev(Bson::Document(doc! { "$toBool": 100 })), some(true));
    assert_eq!(
        ev(Bson::Document(doc! { "$toBool": Bson::Null })),
        Some(Bson::Null)
    );
}

#[test]
fn to_date_from_millis() {
    let expr = doc! { "$toDate": 1000_i64 };
    assert_eq!(
        ev(Bson::Document(expr)),
        Some(Bson::DateTime(DateTime::from_millis(1000)))
    );
    // Strings unsupported (divergence).
    let e = err(Bson::Document(doc! { "$toDate": "2020-01-01" }));
    assert!(format!("{e}").contains("only supports numeric milliseconds"));
}

// -----------------------------------------------------------------------
// Date extraction (known epoch: 2021-01-01T00:00:00.000Z == 1609459200000)
// -----------------------------------------------------------------------

/// `2021-01-01T00:00:00.000Z` was a Friday (day-of-week 6).
const EPOCH_2021_01_01: i64 = 1_609_459_200_000;
/// `2021-03-15T13:45:30.250Z`.
const EPOCH_2021_03_15: i64 = 1_615_815_930_250;

#[test]
fn date_extraction_known_values() {
    let doc = doc! { "d": Bson::DateTime(DateTime::from_millis(EPOCH_2021_01_01)) };
    let part = |op: &str| {
        ev_doc(
            Bson::Document(doc! { op.to_string(): "$d" }),
            doc.clone(),
        )
    };
    assert_eq!(part("$year"), some(2021_i32));
    assert_eq!(part("$month"), some(1_i32));
    assert_eq!(part("$dayOfMonth"), some(1_i32));
    assert_eq!(part("$hour"), some(0_i32));
    assert_eq!(part("$minute"), some(0_i32));
    assert_eq!(part("$second"), some(0_i32));
    assert_eq!(part("$millisecond"), some(0_i32));
    // 2021-01-01 was a Friday: 1=Sun..7=Sat => Friday == 6.
    assert_eq!(part("$dayOfWeek"), some(6_i32));
    assert_eq!(part("$dayOfYear"), some(1_i32));
}

#[test]
fn date_extraction_with_time_components() {
    let doc = doc! { "d": Bson::DateTime(DateTime::from_millis(EPOCH_2021_03_15)) };
    let part = |op: &str| {
        ev_doc(
            Bson::Document(doc! { op.to_string(): "$d" }),
            doc.clone(),
        )
    };
    assert_eq!(part("$year"), some(2021_i32));
    assert_eq!(part("$month"), some(3_i32));
    assert_eq!(part("$dayOfMonth"), some(15_i32));
    assert_eq!(part("$hour"), some(13_i32));
    assert_eq!(part("$minute"), some(45_i32));
    assert_eq!(part("$second"), some(30_i32));
    assert_eq!(part("$millisecond"), some(250_i32));
    // 2021-03-15 was a Monday: 1=Sun => Monday == 2.
    assert_eq!(part("$dayOfWeek"), some(2_i32));
    // Jan(31) + Feb(28) + 15 = 74.
    assert_eq!(part("$dayOfYear"), some(74_i32));
}

#[test]
fn date_extraction_epoch_zero() {
    // 1970-01-01T00:00:00Z was a Thursday (dayOfWeek 5).
    let doc = doc! { "d": Bson::DateTime(DateTime::from_millis(0)) };
    let part = |op: &str| {
        ev_doc(
            Bson::Document(doc! { op.to_string(): "$d" }),
            doc.clone(),
        )
    };
    assert_eq!(part("$year"), some(1970_i32));
    assert_eq!(part("$month"), some(1_i32));
    assert_eq!(part("$dayOfMonth"), some(1_i32));
    assert_eq!(part("$dayOfWeek"), some(5_i32));
}

#[test]
fn date_extraction_non_date_errors() {
    let e = err(Bson::Document(doc! { "$year": 123 }));
    assert!(format!("{e}").contains("$year requires a Date"));
}

#[test]
fn date_extraction_null_in_null_out() {
    let doc = doc! {};
    assert_eq!(
        ev_doc(Bson::Document(doc! { "$year": "$missing" }), doc),
        Some(Bson::Null)
    );
}

// -----------------------------------------------------------------------
// $rand
// -----------------------------------------------------------------------

#[test]
fn rand_in_unit_interval() {
    let value = ev(Bson::Document(doc! { "$rand": {} }));
    match value {
        Some(Bson::Double(f)) => assert!((0.0..1.0).contains(&f)),
        other => panic!("expected double, got {other:?}"),
    }
    let e = err(Bson::Document(doc! { "$rand": 5 }));
    assert!(format!("{e}").contains("$rand requires an empty object"));
}

// -----------------------------------------------------------------------
// Truthiness helper and unknown operator
// -----------------------------------------------------------------------

#[test]
fn eval_expr_to_bool_truthiness() {
    let doc = doc! { "a": 0, "b": "x" };
    let ctx = ExprContext::new(&doc);
    assert!(!eval_expr_to_bool(&Bson::String("$a".into()), &ctx).unwrap());
    assert!(eval_expr_to_bool(&Bson::String("$b".into()), &ctx).unwrap());
    assert!(!eval_expr_to_bool(&Bson::String("$missing".into()), &ctx).unwrap());
    assert!(!eval_expr_to_bool(&Bson::Null, &ctx).unwrap());
    assert!(eval_expr_to_bool(&Bson::Boolean(true), &ctx).unwrap());
}

#[test]
fn unknown_operator_errors() {
    let e = err(Bson::Document(doc! { "$bogusOp": 1 }));
    match e {
        Error::UnsupportedOperator { operator } => {
            assert_eq!(operator, "Unrecognized expression '$bogusOp'");
        }
        other => panic!("expected UnsupportedOperator, got {other:?}"),
    }
}

// -----------------------------------------------------------------------
// Nested composition
// -----------------------------------------------------------------------

#[test]
fn nested_operator_composition() {
    let doc = doc! { "x": 4, "y": 2 };
    // ($x + $y) * 2 - 1 = 11
    let expr = doc! {
        "$subtract": [
            { "$multiply": [ { "$add": ["$x", "$y"] }, 2 ] },
            1
        ]
    };
    assert_eq!(ev_doc(Bson::Document(expr), doc), some(11_i32));
}

#[test]
fn nested_cond_with_comparison_and_map() {
    let doc = doc! { "vals": [1, 5, 10] };
    // Map each value to "big" if > 4 else "small".
    let expr = doc! {
        "$map": {
            "input": "$vals",
            "in": {
                "$cond": [ { "$gt": ["$$this", 4] }, "big", "small" ]
            }
        }
    };
    assert_eq!(
        ev_doc(Bson::Document(expr), doc),
        Some(Bson::Array(vec![
            Bson::String("small".into()),
            Bson::String("big".into()),
            Bson::String("big".into()),
        ]))
    );
}
