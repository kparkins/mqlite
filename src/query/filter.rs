//! Filter evaluation engine for MongoDB-compatible query operators.
//!
//! Entry point: [`eval_filter`].
//!
// Phase 1b: functions are not yet wired to the storage engine; they are
// called only from tests.  Dead-code lint is suppressed until the query
// planner and storage engine integrate the filter evaluator.
#![allow(dead_code)]
//!
//! # Supported operators
//!
//! **Comparison:** `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`
//!
//! **Logical:** `$and` (implicit and explicit), `$or`, `$not`, `$nor`
//!
//! **Element:** `$exists`, `$type`
//!
//! **Array:** `$elemMatch`, `$all`, `$size`
//!
//! **Evaluation:** `$regex`
//!
//! # Cross-type comparison
//!
//! Ordering between BSON types follows MongoDB's canonical type ordering:
//!   MinKey < Null < Numbers < Symbol < String < Object < Array < BinData
//!   < ObjectId < Boolean < Date < Timestamp < RegExp < MaxKey
//!
//! All numeric types (`Int32`, `Int64`, `Double`, `Decimal128`) compare by value
//! (e.g., `Int32(1) == Double(1.0) == Int64(1)`).
//!
//! # Array semantics
//!
//! When the document field value is an `Array`, most operators apply an
//! *any-element* match: the filter matches if **any** element of the array
//! satisfies the condition.  This mirrors MongoDB 8.0 behaviour:
//!
//! ```text
//! // Matches {a: [1, 2, 3]}:
//! {a: {$gt: 2}}   // because 3 > 2
//! {a: 2}          // implicit $eq — because 2 is in the array
//! ```

use std::cmp::Ordering;

use bson::{Bson, Document};
use regex::RegexBuilder;

use serde::de::Error as SerdeDeError;

use crate::error::{Error, Result};
use crate::key_encoding::encode_key;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Evaluate a MongoDB filter document against a BSON document.
///
/// Returns `Ok(true)` if every condition in `filter` is satisfied by `doc`.
///
/// An empty filter `{}` matches all documents.
///
/// # Errors
///
/// Returns `Err(Error::UnsupportedOperator)` for unknown `$` operators.
/// Returns `Err(Error::BsonDeserialization)` (repurposed as `BadValue`) when
/// operators receive arguments of the wrong type (e.g., `$and` given a
/// non-array value).
pub(crate) fn eval_filter(doc: &Document, filter: &Document) -> Result<bool> {
    // Empty filter {} matches everything.
    if filter.is_empty() {
        return Ok(true);
    }

    // Reject collation (MongoDB error code 2 = BadValue).
    if filter.contains_key("collation") {
        return Err(bad_value("Collation is not supported by mqlite"));
    }

    // Each key at the top level is either a logical operator or a field path.
    for (key, condition) in filter.iter() {
        if !eval_top_level(doc, key.as_str(), condition)? {
            return Ok(false);
        }
    }
    Ok(true)
}

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
// Top-level condition dispatch
// ---------------------------------------------------------------------------

fn eval_top_level(doc: &Document, key: &str, condition: &Bson) -> Result<bool> {
    match key {
        "$and" => eval_logical_and(doc, condition),
        "$or" => eval_logical_or(doc, condition),
        "$nor" => eval_logical_nor(doc, condition),
        // $not at the top level is not a valid MongoDB operator.
        "$not" => Err(bad_value(
            "$not cannot be used at the top level; use $nor instead",
        )),
        // $expr is explicitly rejected — it uses aggregation expressions which
        // are not supported in mqlite.  It must never be silently passed through.
        "$expr" => {
            #[cfg(feature = "tracing")]
            tracing::warn!(target: "mqlite", operator = "$expr", "mqlite::unsupported_op");
            Err(Error::UnsupportedOperator {
                operator: "$expr".to_owned(),
            })
        }
        k if k.starts_with('$') => {
            #[cfg(feature = "tracing")]
            tracing::warn!(target: "mqlite", operator = k, "mqlite::unsupported_op");
            Err(Error::UnsupportedOperator {
                operator: k.to_owned(),
            })
        }
        // Field path (possibly dotted, e.g. "a.b.c").
        field_path => {
            let field_value = get_nested_field(doc, field_path);
            eval_field_condition(field_value, condition)
        }
    }
}

// ---------------------------------------------------------------------------
// Logical operators
// ---------------------------------------------------------------------------

fn eval_logical_and(doc: &Document, condition: &Bson) -> Result<bool> {
    let arr = require_array("$and", condition)?;
    for item in arr {
        let sub_filter = require_document("$and array element", item)?;
        if !eval_filter(doc, sub_filter)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn eval_logical_or(doc: &Document, condition: &Bson) -> Result<bool> {
    let arr = require_array("$or", condition)?;
    if arr.is_empty() {
        // MongoDB: {$or: []} is a bad value, not a no-match.
        return Err(bad_value("$or must have at least one element"));
    }
    for item in arr {
        let sub_filter = require_document("$or array element", item)?;
        if eval_filter(doc, sub_filter)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn eval_logical_nor(doc: &Document, condition: &Bson) -> Result<bool> {
    let arr = require_array("$nor", condition)?;
    if arr.is_empty() {
        return Err(bad_value("$nor must have at least one element"));
    }
    for item in arr {
        let sub_filter = require_document("$nor array element", item)?;
        if eval_filter(doc, sub_filter)? {
            return Ok(false);
        }
    }
    Ok(true)
}

// ---------------------------------------------------------------------------
// Field-level condition dispatch
// ---------------------------------------------------------------------------

/// Evaluate a condition against the current field value.
///
/// `field_value` is `None` when the field is absent from the document.
fn eval_field_condition(field_value: Option<&Bson>, condition: &Bson) -> Result<bool> {
    match condition {
        Bson::Document(ops) => {
            // Determine whether this document uses query operators or is a
            // plain sub-document value.
            let has_ops = ops.keys().any(|k| k.starts_with('$'));
            if has_ops {
                eval_operator_document(field_value, ops)
            } else {
                // Plain sub-document value: equality with array-unwrap semantics
                // (same as implicit $eq — {a: {x:1}} matches {a: [{x:1},...]})
                eval_eq(field_value, condition)
            }
        }
        // /pattern/flags shorthand — condition is a BSON RegularExpression.
        // Drivers (mongosh, pymongo) send {field: /pattern/flags} as this type.
        Bson::RegularExpression(re) => eval_regex(field_value, &re.pattern, &re.options),
        // Any other value is an implicit $eq with array-unwrap semantics.
        _ => eval_eq(field_value, condition),
    }
}

/// Evaluate an operator document like `{$gt: 5, $lt: 10, $not: {$eq: 7}}`.
///
/// All operators in the document must match (AND semantics).
///
/// `$regex` and `$options` are handled here as a compound pair because
/// `$options` is only meaningful alongside `$regex`.
fn eval_operator_document(field_value: Option<&Bson>, ops: &Document) -> Result<bool> {
    // Handle $regex/$options as a unit before iterating the rest.
    if let Some(pattern_bson) = ops.get("$regex") {
        let pattern: &str = match pattern_bson {
            Bson::String(s) => s.as_str(),
            Bson::RegularExpression(re) => re.pattern.as_str(),
            _ => return Err(bad_value("$regex must be a string or RegularExpression")),
        };
        let options: &str = match ops.get("$options") {
            Some(Bson::String(s)) => s.as_str(),
            Some(_) => return Err(bad_value("$options must be a string")),
            None => "",
        };
        if !eval_regex(field_value, pattern, options)? {
            return Ok(false);
        }
        // Process remaining operators, skipping $regex/$options.
        for (op, arg) in ops.iter() {
            if op == "$regex" || op == "$options" {
                continue;
            }
            if !eval_single_op(field_value, op.as_str(), arg)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }

    // $options without $regex is an error.
    if ops.contains_key("$options") {
        return Err(bad_value("$options is only valid when used with $regex"));
    }

    for (op, arg) in ops.iter() {
        if !eval_single_op(field_value, op.as_str(), arg)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Evaluate a single operator (e.g. `$gt`) against a field value.
///
/// `$regex` and `$options` are NOT dispatched here; they are handled as a
/// unit by [`eval_operator_document`] because `$options` depends on `$regex`.
fn eval_single_op(field_value: Option<&Bson>, op: &str, arg: &Bson) -> Result<bool> {
    match op {
        // ---- Comparison ----
        "$eq" => eval_eq(field_value, arg),
        "$ne" => Ok(!eval_eq(field_value, arg)?),
        "$gt" => eval_cmp(field_value, arg, Ordering::Greater, false),
        "$gte" => eval_cmp(field_value, arg, Ordering::Greater, true),
        "$lt" => eval_cmp(field_value, arg, Ordering::Less, false),
        "$lte" => eval_cmp(field_value, arg, Ordering::Less, true),
        "$in" => eval_in(field_value, arg),
        "$nin" => Ok(!eval_in(field_value, arg)?),
        // ---- Logical (field-level) ----
        "$not" => eval_not(field_value, arg),
        // ---- Element ----
        "$exists" => eval_exists(field_value, arg),
        "$type" => eval_type(field_value, arg),
        // ---- Array ----
        "$elemMatch" => eval_elem_match(field_value, arg),
        "$all" => eval_all(field_value, arg),
        "$size" => eval_size(field_value, arg),
        // ---- Evaluation operators ($regex/$options handled by eval_operator_document) ----
        "$regex" | "$options" => {
            #[cfg(feature = "tracing")]
            tracing::warn!(target: "mqlite", operator = op, "mqlite::unsupported_op");
            Err(Error::UnsupportedOperator {
                operator: op.to_owned(),
            })
        }
        // ---- Explicitly unsupported operators (error code 9) ----
        // These are named individually to ensure they are never silently ignored.
        "$expr"           // Aggregation-expression passthrough — explicitly forbidden.
        | "$jsonSchema"   // JSON Schema validation — Phase 2.
        | "$mod"          // Modulo arithmetic — not implemented.
        | "$text"         // Full-text search — not implemented.
        | "$where"        // JavaScript evaluation — intentionally unsupported.
        | "$geoWithin" | "$geoIntersects" | "$near" | "$nearSphere" // Geospatial.
        | "$slice"        // Projection operator — not valid in query filters.
        | "$meta"         // Projection meta — not valid in query filters.
        | "$comment"      // Query annotation — not implemented.
        | "$rand"         // Random sampling — not implemented.
        | "$natural"      // Natural sort hint — not valid in query filters.
        => {
            #[cfg(feature = "tracing")]
            tracing::warn!(target: "mqlite", operator = op, "mqlite::unsupported_op");
            Err(Error::UnsupportedOperator {
                operator: op.to_owned(),
            })
        }
        // ---- Catch-all for any other unknown operator ----
        other => {
            #[cfg(feature = "tracing")]
            tracing::warn!(target: "mqlite", operator = other, "mqlite::unsupported_op");
            Err(Error::UnsupportedOperator {
                operator: other.to_owned(),
            })
        }
    }
}

// ---------------------------------------------------------------------------
// $eq / $ne
// ---------------------------------------------------------------------------

/// Evaluate `$eq` with array-unwrap semantics.
///
/// For an array field value, returns true if **any** element equals `target`.
/// For a scalar field value, returns true if the value equals `target`.
/// For a missing field (`None`), returns true only if `target` is `Bson::Null`.
fn eval_eq(field_value: Option<&Bson>, target: &Bson) -> Result<bool> {
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
fn eval_cmp(
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
fn eval_in(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
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
fn eval_not(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let ops = require_document("$not", arg)?;
    // $not requires at least one operator.
    if ops.is_empty() {
        return Err(bad_value("$not cannot have an empty sub-expression"));
    }
    // $not negates the result of evaluating the sub-expression.
    Ok(!eval_operator_document(field_value, ops)?)
}

// ---------------------------------------------------------------------------
// $exists
// ---------------------------------------------------------------------------

fn eval_exists(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
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
fn eval_type(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
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
fn eval_elem_match(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let cond_doc = require_document("$elemMatch", arg)?;
    let arr = match field_value {
        Some(Bson::Array(a)) => a,
        _ => return Ok(false), // missing or non-array — no match
    };
    let is_operator_mode = cond_doc.keys().any(|k| k.starts_with('$'));
    for elem in arr {
        let matched = if is_operator_mode {
            // Apply operator conditions directly to the element value.
            eval_operator_document(Some(elem), cond_doc)?
        } else {
            // Element must be a document matching the sub-filter.
            match elem {
                Bson::Document(sub_doc) => eval_filter(sub_doc, cond_doc)?,
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
fn eval_all(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
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
fn eval_size(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
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
fn eval_regex(field_value: Option<&Bson>, pattern: &str, options: &str) -> Result<bool> {
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

// ---------------------------------------------------------------------------
// Nested field access (dot notation)
// ---------------------------------------------------------------------------

/// Retrieve the value at a dotted path in a document.
///
/// Supports:
/// - Simple field: `"name"` → `doc.get("name")`
/// - Nested: `"a.b.c"` → traverse embedded documents
/// - Array index: `"arr.0"` → first element of array `arr`
///
/// Returns `None` if the path does not exist.
pub(crate) fn get_nested_field<'a>(doc: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut parts = path.splitn(2, '.');
    let head = parts.next()?;
    let tail = parts.next();

    let current = doc.get(head)?;

    match tail {
        None => Some(current),
        Some(rest) => match current {
            Bson::Document(sub_doc) => get_nested_field(sub_doc, rest),
            Bson::Array(arr) => {
                // Try numeric index first.
                if let Ok(idx) = rest.parse::<usize>() {
                    let elem = arr.get(idx)?;
                    // If there are more path segments, recurse.
                    let after_idx = rest.split_once('.').map(|x| x.1);
                    if let Some(more) = after_idx {
                        if let Bson::Document(sub_doc) = elem {
                            get_nested_field(sub_doc, more)
                        } else {
                            None
                        }
                    } else {
                        Some(elem)
                    }
                } else {
                    // Non-numeric key in an array position — no match.
                    None
                }
            }
            _ => None,
        },
    }
}

// ---------------------------------------------------------------------------
// BSON comparison helpers
// ---------------------------------------------------------------------------

/// Compare two BSON values using MongoDB's canonical ordering.
///
/// Uses [`encode_key`] to produce `memcmp`-sortable byte sequences.
fn compare_bson(a: &Bson, b: &Bson) -> Ordering {
    let ka = encode_key(a);
    let kb = encode_key(b);
    ka.cmp(&kb)
}

/// Return true if two BSON values are equal under MongoDB's ordering.
fn bson_eq(a: &Bson, b: &Bson) -> bool {
    encode_key(a) == encode_key(b)
}

// ---------------------------------------------------------------------------
// Argument validation helpers
// ---------------------------------------------------------------------------

fn require_array<'a>(op: &str, val: &'a Bson) -> Result<&'a Vec<Bson>> {
    match val {
        Bson::Array(arr) => Ok(arr),
        _ => Err(bad_value(&format!(
            "{op} must be an array, got: {}",
            bson_type_name(val)
        ))),
    }
}

fn require_document<'a>(ctx: &str, val: &'a Bson) -> Result<&'a Document> {
    match val {
        Bson::Document(doc) => Ok(doc),
        _ => Err(bad_value(&format!(
            "{ctx} must be a document, got: {}",
            bson_type_name(val)
        ))),
    }
}

/// Convert a BSON value to `i64`, accepting only whole-number values.
///
/// `Double` values are accepted only if they are exact integers (e.g., `2.0`).
/// Used by `$size` which requires a non-negative integer argument.
fn bson_to_i64_strict(op: &str, val: &Bson) -> Result<i64> {
    match val {
        Bson::Int32(n) => Ok(*n as i64),
        Bson::Int64(n) => Ok(*n),
        Bson::Double(f) => {
            let i = *f as i64;
            if i as f64 == *f {
                Ok(i)
            } else {
                Err(bad_value(&format!(
                    "{op} requires a whole-number value, got {f}"
                )))
            }
        }
        _ => Err(bad_value(&format!(
            "{op} must be a number, got: {}",
            bson_type_name(val)
        ))),
    }
}

fn bson_to_bool(op: &str, val: &Bson) -> Result<bool> {
    match val {
        Bson::Boolean(b) => Ok(*b),
        Bson::Int32(n) => Ok(*n != 0),
        Bson::Int64(n) => Ok(*n != 0),
        Bson::Double(n) => Ok(*n != 0.0),
        _ => Err(bad_value(&format!(
            "{op} must be a boolean, got: {}",
            bson_type_name(val)
        ))),
    }
}

fn bson_type_name(val: &Bson) -> &'static str {
    match val {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Binary(_) => "binData",
        Bson::Undefined => "undefined",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::DbPointer(_) => "dbPointer",
        Bson::JavaScriptCode(_) => "javascript",
        Bson::Symbol(_) => "symbol",
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope",
        Bson::Int32(_) => "int",
        Bson::Timestamp(_) => "timestamp",
        Bson::Int64(_) => "long",
        Bson::Decimal128(_) => "decimal",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
    }
}

fn bad_value(msg: &str) -> Error {
    // Repurpose BsonDeserialization to carry BadValue messages.
    // MongoDB error code 2 (BadValue). Requires the serde::de::Error trait.
    Error::BsonDeserialization(bson::de::Error::custom(format!("BadValue: {msg}")))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "filter_tests.rs"]
mod tests;
