//! Per-operator implementations dispatched by [`super::eval_single_op`] and
//! [`super::eval_operator_document`].
//!
//! Each function receives the document field value (`None` if absent) and the
//! operator argument.  Operator semantics — including array-unwrap and missing-
//! field handling — are documented at each function.

use std::cmp::Ordering;

use bson::Bson;
use regex::RegexBuilder;

use crate::error::Result;

use super::util::{
    bad_value, bson_eq, bson_to_bool, bson_to_i64_strict, compare_bson, require_array,
    require_document,
};

// ---------------------------------------------------------------------------
// Per-query regex DFA size limit
// ---------------------------------------------------------------------------

/// Maximum DFA state bytes for compiled regex patterns (10 MB).
///
/// Prevents pathological patterns from consuming excessive memory during DFA
/// compilation.  The `regex` crate uses a linear-time matching algorithm
/// (DFA/NFA hybrid with lazy construction), so this limit covers compile-time
/// cost; catastrophic backtracking at match time is architecturally impossible.
///
/// If a pattern exceeds this limit, `build_regex` returns `Error::BsonDeserialization`
/// (MongoDB error code 2 / BadValue).
const REGEX_DFA_SIZE_LIMIT: usize = 10 * 1024 * 1024;

// ---------------------------------------------------------------------------
// $eq / $ne
// ---------------------------------------------------------------------------

/// Evaluate `$eq` with array-unwrap semantics.
///
/// For an array field value, returns true if **any** element equals `target`.
/// For a scalar field value, returns true if the value equals `target`.
/// For a missing field (`None`), returns true only if `target` is `Bson::Null`.
pub(super) fn eval_eq(field_value: Option<&Bson>, target: &Bson) -> Result<bool> {
    match field_value {
        None => {
            // Missing field: matches `null` (like MongoDB).
            Ok(matches!(target, Bson::Null))
        }
        Some(Bson::Array(arr)) => {
            // Match the whole array exactly, OR any element.
            if bson_eq(&Bson::Array(arr.clone()), target) {
                return Ok(true);
            }
            for elem in arr {
                if bson_eq(elem, target) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Some(val) => Ok(bson_eq(val, target)),
    }
}

// ---------------------------------------------------------------------------
// Comparison: $gt, $gte, $lt, $lte
// ---------------------------------------------------------------------------

/// Evaluate a comparison operator against `field_value`.
///
/// `direction` is the expected [`Ordering`] (e.g., `Greater` for `$gt`).
/// `allow_equal` is true for `$gte` / `$lte`.
///
/// Array fields: matches if any element satisfies the comparison.
/// Missing fields: never match.
pub(super) fn eval_cmp(
    field_value: Option<&Bson>,
    comparand: &Bson,
    direction: Ordering,
    allow_equal: bool,
) -> Result<bool> {
    match field_value {
        None => Ok(false),
        Some(Bson::Array(arr)) => {
            for elem in arr {
                if cmp_satisfies(elem, comparand, direction, allow_equal) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Some(val) => Ok(cmp_satisfies(val, comparand, direction, allow_equal)),
    }
}

fn cmp_satisfies(val: &Bson, comparand: &Bson, direction: Ordering, allow_equal: bool) -> bool {
    let ord = compare_bson(val, comparand);
    ord == direction || (allow_equal && ord == Ordering::Equal)
}

// ---------------------------------------------------------------------------
// $in / $nin
// ---------------------------------------------------------------------------

/// Evaluate `$in` with array-unwrap semantics.
///
/// Returns true if the field value (or any element of an array field) is
/// equal to any value in the `$in` list.
pub(super) fn eval_in(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let list = require_array("$in", arg)?;
    match field_value {
        None => {
            // Missing field: matches null in $in list.
            for item in list {
                if matches!(item, Bson::Null) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Some(Bson::Array(arr)) => {
            // Array field: match if the whole array OR any element is in the list.
            let field_arr = Bson::Array(arr.clone());
            for target in list {
                if bson_eq(&field_arr, target) {
                    return Ok(true);
                }
                for elem in arr {
                    if bson_eq(elem, target) {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
        Some(val) => {
            for target in list {
                if bson_eq(val, target) {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

// ---------------------------------------------------------------------------
// $not
// ---------------------------------------------------------------------------

/// Evaluate `$not` — negate an operator sub-document.
///
/// `arg` must be a document like `{$gt: 5}`.
pub(super) fn eval_not(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let ops = require_document("$not", arg)?;
    // $not requires at least one operator.
    if ops.is_empty() {
        return Err(bad_value("$not cannot have an empty sub-expression"));
    }
    // $not negates the result of evaluating the sub-expression.
    Ok(!super::eval_operator_document(field_value, ops)?)
}

// ---------------------------------------------------------------------------
// $exists
// ---------------------------------------------------------------------------

pub(super) fn eval_exists(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let want_exists = bson_to_bool("$exists", arg)?;
    let does_exist = field_value.is_some();
    Ok(does_exist == want_exists)
}

// ---------------------------------------------------------------------------
// $type
// ---------------------------------------------------------------------------

/// Evaluate `$type`.
///
/// `arg` can be:
/// - A string type alias (e.g., `"string"`, `"int"`)
/// - A numeric BSON type ID (e.g., `2` for string, `16` for int32)
/// - An array of either (matches if field type is in the list)
pub(super) fn eval_type(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let val = match field_value {
        None => return Ok(false),
        Some(v) => v,
    };

    match arg {
        Bson::Array(type_list) => {
            for t in type_list {
                if type_matches(val, t)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        _ => type_matches(val, arg),
    }
}

fn type_matches(val: &Bson, type_spec: &Bson) -> Result<bool> {
    let actual_type = bson_type_id(val);
    match type_spec {
        Bson::String(name) => {
            let expected = type_name_to_id(name.as_str())?;
            Ok(actual_type == expected)
        }
        Bson::Int32(id) => Ok(actual_type == *id as i64),
        Bson::Int64(id) => Ok(actual_type == *id),
        Bson::Double(id) => Ok(actual_type == *id as i64),
        _ => Err(bad_value(
            "$type argument must be a type string, number, or array",
        )),
    }
}

/// Returns the numeric BSON type ID for a value (MongoDB spec numbers).
fn bson_type_id(val: &Bson) -> i64 {
    match val {
        Bson::Double(_) => 1,
        Bson::String(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::Undefined => 6,
        Bson::ObjectId(_) => 7,
        Bson::Boolean(_) => 8,
        Bson::DateTime(_) => 9,
        Bson::Null => 10,
        Bson::RegularExpression(_) => 11,
        Bson::DbPointer(_) => 12,
        Bson::JavaScriptCode(_) => 13,
        Bson::Symbol(_) => 14,
        Bson::JavaScriptCodeWithScope(_) => 15,
        Bson::Int32(_) => 16,
        Bson::Timestamp(_) => 17,
        Bson::Int64(_) => 18,
        Bson::Decimal128(_) => 19,
        Bson::MinKey => -1,
        Bson::MaxKey => 127,
    }
}

/// Convert a MongoDB BSON type string alias to its numeric ID.
fn type_name_to_id(name: &str) -> Result<i64> {
    let id = match name {
        "double" => 1,
        "string" => 2,
        "object" => 3,
        "array" => 4,
        "binData" => 5,
        "undefined" => 6,
        "objectId" => 7,
        "bool" => 8,
        "date" => 9,
        "null" => 10,
        "regex" => 11,
        "dbPointer" => 12,
        "javascript" => 13,
        "symbol" => 14,
        "javascriptWithScope" => 15,
        "int" => 16,
        "timestamp" => 17,
        "long" => 18,
        "decimal" => 19,
        "minKey" => -1,
        "maxKey" => 127,
        // MongoDB also accepts "number" as an alias for any numeric type.
        "number" => {
            // Handled specially — not a single ID.
            // We use a sentinel; caller must check for it.
            return Err(bad_value(
                "type alias 'number' is not supported in $type; use an array of type IDs instead",
            ));
        }
        other => return Err(bad_value(&format!("unknown $type name: \"{other}\""))),
    };
    Ok(id)
}

// ---------------------------------------------------------------------------
// $elemMatch
// ---------------------------------------------------------------------------

/// Evaluate `$elemMatch` — a single array element must satisfy all conditions.
///
/// Only array-typed fields can match; scalars and missing fields never match.
///
/// If all top-level keys in `arg` start with `$`, the operators are applied
/// directly to each element (e.g., `{$gt: 5, $lt: 10}` tests each number).
/// Otherwise the element must be a sub-document matching `arg` as a filter.
pub(super) fn eval_elem_match(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let cond_doc = require_document("$elemMatch", arg)?;
    let arr = match field_value {
        Some(Bson::Array(a)) => a,
        _ => return Ok(false), // missing or non-array — no match
    };
    let is_operator_mode = cond_doc.keys().any(|k| k.starts_with('$'));
    for elem in arr {
        let matched = if is_operator_mode {
            // Apply operator conditions directly to the element value.
            super::eval_operator_document(Some(elem), cond_doc)?
        } else {
            // Element must be a document matching the sub-filter.
            match elem {
                Bson::Document(sub_doc) => super::eval_filter(sub_doc, cond_doc)?,
                _ => false,
            }
        };
        if matched {
            return Ok(true);
        }
    }
    Ok(false)
}

// ---------------------------------------------------------------------------
// $all
// ---------------------------------------------------------------------------

/// Evaluate `$all` — every value in the list must appear in the field array.
///
/// For a scalar field, the field is treated as a single-element array
/// (matching MongoDB 8.0 behaviour for `{a: {$all: [v]}}` vs `{a: v}`).
///
/// Returns `false` for an empty `$all` list or a missing/null field.
///
/// Each element in the `$all` list may itself be an `{$elemMatch: ...}` document;
/// in that case the sub-condition is evaluated against the whole field array.
pub(super) fn eval_all(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let required = require_array("$all", arg)?;
    if required.is_empty() {
        // An empty $all matches no documents.
        return Ok(false);
    }
    match field_value {
        None => Ok(false),
        Some(Bson::Array(arr)) => {
            for req_val in required {
                // Check for $all: [{$elemMatch: {...}}] syntax.
                let found = if let Bson::Document(cond) = req_val {
                    if let Some(em_arg) = cond.get("$elemMatch") {
                        eval_elem_match(Some(&Bson::Array(arr.clone())), em_arg)?
                    } else {
                        arr.iter().any(|elem| bson_eq(elem, req_val))
                    }
                } else {
                    arr.iter().any(|elem| bson_eq(elem, req_val))
                };
                if !found {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Some(scalar) => {
            // Treat a scalar as a single-element array.
            for req_val in required {
                if !bson_eq(scalar, req_val) {
                    return Ok(false);
                }
            }
            // Only matches if $all contains exactly one value (equal to scalar).
            Ok(true)
        }
    }
}

// ---------------------------------------------------------------------------
// $size
// ---------------------------------------------------------------------------

/// Evaluate `$size` — field array must have exactly N elements.
///
/// Only array-typed fields can match.  Missing fields and scalar fields never
/// match.  `N` must be a non-negative integer; fractional values are rejected.
pub(super) fn eval_size(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let n = bson_to_i64_strict("$size", arg)?;
    if n < 0 {
        return Err(bad_value("$size must be a non-negative integer"));
    }
    match field_value {
        Some(Bson::Array(arr)) => Ok(arr.len() as i64 == n),
        _ => Ok(false),
    }
}

// ---------------------------------------------------------------------------
// $regex
// ---------------------------------------------------------------------------

/// Evaluate `$regex` — field string must match the given pattern.
///
/// Only `String`-typed field values (or array elements that are strings) are
/// tested against the pattern.  Non-string values are skipped.
///
/// `options` is a string of regex flag characters:
/// - `i` — case-insensitive
/// - `m` — multiline (`^`/`$` match line boundaries)
/// - `s` — dotall (`.` matches `\n`)
/// - `x` — extended / verbose (whitespace and `#` comments are ignored)
///
/// **PCRE incompatibilities**: the Rust `regex` crate does not support
/// lookahead, lookbehind, atomic groups, possessive quantifiers, named
/// backreferences, conditional patterns, or recursive patterns.  Patterns
/// using these constructs will fail to compile.
pub(super) fn eval_regex(field_value: Option<&Bson>, pattern: &str, options: &str) -> Result<bool> {
    let re = build_regex(pattern, options)?;
    match field_value {
        None => Ok(false),
        Some(Bson::String(s)) => Ok(re.is_match(s)),
        Some(Bson::Array(arr)) => {
            // Array field: match if any string element matches.
            for elem in arr {
                if let Bson::String(s) = elem {
                    if re.is_match(s) {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        }
        Some(_) => Ok(false), // non-string, non-array — no match
    }
}

/// Compile a regex pattern with the given option flags.
///
/// Uses [`RegexBuilder`] with a DFA size cap ([`REGEX_DFA_SIZE_LIMIT`]) to
/// prevent compile-time memory explosion on pathological patterns.
fn build_regex(pattern: &str, options: &str) -> Result<regex::Regex> {
    let mut b = RegexBuilder::new(pattern);
    b.size_limit(REGEX_DFA_SIZE_LIMIT);
    b.dfa_size_limit(REGEX_DFA_SIZE_LIMIT);
    for flag in options.chars() {
        match flag {
            'i' => {
                b.case_insensitive(true);
            }
            'm' => {
                b.multi_line(true);
            }
            's' => {
                b.dot_matches_new_line(true);
            }
            'x' => {
                b.ignore_whitespace(true);
            }
            // 'l' (locale) and 'u' (unicode) are accepted but no-op:
            // the regex crate is Unicode-aware by default and has no
            // locale concept.
            'l' | 'u' => {}
            other => {
                return Err(bad_value(&format!("unknown $regex option '{other}'")));
            }
        }
    }
    b.build()
        .map_err(|e| bad_value(&format!("invalid $regex pattern: {e}")))
}

