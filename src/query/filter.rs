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

mod operators;
mod util;

use std::cmp::Ordering;

use bson::{Bson, Document};

use crate::error::{Error, Result};

pub(crate) use util::get_nested_field;

use operators::{
    eval_all, eval_cmp, eval_elem_match, eval_eq, eval_exists, eval_in, eval_not, eval_regex,
    eval_size, eval_type,
};
use util::{bad_value, require_array, require_document};

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
pub(super) fn eval_operator_document(field_value: Option<&Bson>, ops: &Document) -> Result<bool> {
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "filter_tests.rs"]
mod tests_extracted;
