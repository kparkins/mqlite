//! MongoDB update operator implementation.
//!
//! Supported operators (Phase 1b):
//!
//! | Operator      | Description |
//! |---------------|-------------|
//! | `$set`        | Set field values |
//! | `$unset`      | Remove fields |
//! | `$inc`        | Increment numbers |
//! | `$mul`        | Multiply numbers |
//! | `$rename`     | Rename fields |
//! | `$min`        | Update if new value is less than current |
//! | `$max`        | Update if new value is greater than current |
//! | `$push`       | Append elements to arrays |
//! | `$pull`       | Remove matching elements from arrays |
//! | `$addToSet`   | Add elements to arrays (no duplicates) |
//! | `$pop`        | Remove first or last element from arrays |
//! | `$currentDate`| Set to current date/timestamp |
//! | `$setOnInsert`| Set fields only on upsert insert |
//!
//! All unknown `$` operators return [`Error::UnsupportedOperator`].
//! A top-level document with no `$` operators is treated as a replacement
//! and is **not** handled here; use the engine's replace path instead.

use bson::{Bson, DateTime, Document, Timestamp};

use crate::error::{Error, Result};
use crate::query::get_nested_field;

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Apply a MongoDB update document to `doc` in place.
///
/// `update` must contain only `$` operator keys at the top level.  A document
/// without any `$` keys is a **replacement** — call the engine's replace path
/// instead.
///
/// `is_insert` is `true` when this is the "insert" half of an upsert: the
/// `$setOnInsert` operator is applied only when `is_insert == true`.
///
/// # Errors
///
/// - [`Error::UnsupportedOperator`] for any unrecognised `$` operator.
/// - [`Error::Internal`] for type mismatches (e.g. `$inc` on a non-number).
pub(crate) fn apply_update(
    doc: &mut Document,
    update: &Document,
    is_insert: bool,
) -> Result<()> {
    for (op, args) in update {
        // Validate / reject the operator BEFORE attempting to parse args.
        // This ensures Error::UnsupportedOperator is returned regardless of
        // the argument type.
        match op.as_str() {
            "$set" | "$unset" | "$inc" | "$mul" | "$rename" | "$min" | "$max"
            | "$push" | "$pull" | "$addToSet" | "$pop" | "$currentDate"
            | "$setOnInsert" => {}  // supported — fall through to args parse

            "$bit" => {
                return Err(Error::UnsupportedOperator {
                    operator: "$bit".into(),
                });
            }
            other if other.starts_with('$') => {
                return Err(Error::UnsupportedOperator {
                    operator: other.to_owned(),
                });
            }
            _ => {
                return Err(Error::Internal(format!(
                    "invalid update operator '{op}': top-level keys must start with '$'"
                )));
            }
        }

        let args_doc = args.as_document().ok_or_else(|| {
            Error::Internal(format!(
                "update operator '{op}' requires a document argument, got {args:?}"
            ))
        })?;

        match op.as_str() {
            "$set" => apply_set(doc, args_doc)?,
            "$unset" => apply_unset(doc, args_doc)?,
            "$inc" => apply_inc(doc, args_doc)?,
            "$mul" => apply_mul(doc, args_doc)?,
            "$rename" => apply_rename(doc, args_doc)?,
            "$min" => apply_min(doc, args_doc)?,
            "$max" => apply_max(doc, args_doc)?,
            "$push" => apply_push(doc, args_doc)?,
            "$pull" => apply_pull(doc, args_doc)?,
            "$addToSet" => apply_add_to_set(doc, args_doc)?,
            "$pop" => apply_pop(doc, args_doc)?,
            "$currentDate" => apply_current_date(doc, args_doc)?,
            "$setOnInsert" => {
                if is_insert {
                    apply_set(doc, args_doc)?;
                }
            }
            _ => unreachable!("already validated operator above"),
        }
    }
    Ok(())
}

/// Returns `true` if `update` is an operator-based update document (has `$` keys).
///
/// A document with no `$` keys is treated as a replacement document.
pub(crate) fn is_operator_update(update: &Document) -> bool {
    update.keys().any(|k| k.starts_with('$'))
}

// ---------------------------------------------------------------------------
// Nested field path helpers
// ---------------------------------------------------------------------------

/// Set a (possibly dotted) field path to `value`, creating intermediate documents
/// as needed.  The `_id` field is protected — attempting to set it is a no-op.
pub(crate) fn set_nested(doc: &mut Document, path: &str, value: Bson) {
    if path == "_id" {
        // Protect _id: never allow it to be overwritten by $set.
        return;
    }

    let mut parts = path.splitn(2, '.');
    let head = parts.next().expect("non-empty path");

    match parts.next() {
        None => {
            doc.insert(head, value);
        }
        Some(rest) => {
            let nested = doc
                .entry(head.to_owned())
                .or_insert_with(|| Bson::Document(Document::new()));

            if let Bson::Document(nested_doc) = nested {
                set_nested(nested_doc, rest, value);
            } else {
                // Overwrite non-document intermediate with a new document.
                let mut new_doc = Document::new();
                set_nested(&mut new_doc, rest, value);
                doc.insert(head, Bson::Document(new_doc));
            }
        }
    }
}

/// Remove a (possibly dotted) field path.
fn remove_nested(doc: &mut Document, path: &str) {
    let mut parts = path.splitn(2, '.');
    let head = parts.next().expect("non-empty path");

    match parts.next() {
        None => {
            doc.remove(head);
        }
        Some(rest) => {
            if let Some(Bson::Document(nested)) = doc.get_mut(head) {
                remove_nested(nested, rest);
            }
        }
    }
}

/// Extract the numeric value from a BSON value as `f64`.  Returns `None` for
/// non-numeric types.
fn as_f64(v: &Bson) -> Option<f64> {
    match v {
        Bson::Int32(n) => Some(*n as f64),
        Bson::Int64(n) => Some(*n as f64),
        Bson::Double(n) => Some(*n),
        _ => None,
    }
}

/// Produce a numeric `Bson` from `f64`, preserving the type of an existing value
/// (`Int32`, `Int64`, or `Double`).  Falls back to `Double` if `existing` is not
/// a number or if the result doesn't fit in the integer type.
fn numeric_result(existing: Option<&Bson>, result: f64) -> Bson {
    match existing {
        Some(Bson::Int32(_)) => {
            if result >= i32::MIN as f64 && result <= i32::MAX as f64 && result.fract() == 0.0 {
                Bson::Int32(result as i32)
            } else {
                Bson::Double(result)
            }
        }
        Some(Bson::Int64(_)) => {
            if result >= i64::MIN as f64 && result <= i64::MAX as f64 && result.fract() == 0.0 {
                Bson::Int64(result as i64)
            } else {
                Bson::Double(result)
            }
        }
        _ => Bson::Double(result),
    }
}

// ---------------------------------------------------------------------------
// $set
// ---------------------------------------------------------------------------

fn apply_set(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, value) in args {
        set_nested(doc, path, value.clone());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $unset
// ---------------------------------------------------------------------------

fn apply_unset(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, _) in args {
        remove_nested(doc, path);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $inc
// ---------------------------------------------------------------------------

fn apply_inc(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, delta) in args {
        let d = as_f64(delta).ok_or_else(|| {
            Error::Internal(format!("$inc: value must be a number, got {delta:?}"))
        })?;

        let current = get_nested_field(doc, path).cloned();
        let current_n = current.as_ref().and_then(as_f64).unwrap_or(0.0);
        let result = numeric_result(current.as_ref(), current_n + d);
        set_nested(doc, path, result);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $mul
// ---------------------------------------------------------------------------

fn apply_mul(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, factor) in args {
        let f = as_f64(factor).ok_or_else(|| {
            Error::Internal(format!("$mul: value must be a number, got {factor:?}"))
        })?;

        let current = get_nested_field(doc, path).cloned();
        let current_n = current.as_ref().and_then(as_f64).unwrap_or(0.0);
        let result = numeric_result(current.as_ref(), current_n * f);
        set_nested(doc, path, result);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $rename
// ---------------------------------------------------------------------------

fn apply_rename(doc: &mut Document, args: &Document) -> Result<()> {
    for (old_path, new_name) in args {
        let new_str = new_name.as_str().ok_or_else(|| {
            Error::Internal(format!(
                "$rename: new name must be a string, got {new_name:?}"
            ))
        })?;

        // Read the old value (cloned to avoid borrow conflict).
        let old_val = get_nested_field(doc, old_path).cloned();

        if let Some(val) = old_val {
            remove_nested(doc, old_path);
            set_nested(doc, new_str, val);
        }
        // If old field doesn't exist, $rename is a no-op.
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $min / $max
// ---------------------------------------------------------------------------

/// Compare two BSON values using MongoDB's type ordering for `$min`/`$max`.
fn bson_cmp(a: &Bson, b: &Bson) -> std::cmp::Ordering {
    use crate::key_encoding::encode_key;
    encode_key(a).cmp(&encode_key(b))
}

fn apply_min(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, new_val) in args {
        let current = get_nested_field(doc, path).cloned();
        let should_set = match current {
            None => true,
            Some(ref cur) => bson_cmp(new_val, cur) == std::cmp::Ordering::Less,
        };
        if should_set {
            set_nested(doc, path, new_val.clone());
        }
    }
    Ok(())
}

fn apply_max(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, new_val) in args {
        let current = get_nested_field(doc, path).cloned();
        let should_set = match current {
            None => true,
            Some(ref cur) => bson_cmp(new_val, cur) == std::cmp::Ordering::Greater,
        };
        if should_set {
            set_nested(doc, path, new_val.clone());
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $push
// ---------------------------------------------------------------------------

fn apply_push(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, value) in args {
        let field = get_nested_field(doc, path).cloned();

        match field {
            None => {
                // Create a new array with the element.
                set_nested(doc, path, Bson::Array(vec![value.clone()]));
            }
            Some(Bson::Array(mut arr)) => {
                arr.push(value.clone());
                set_nested(doc, path, Bson::Array(arr));
            }
            Some(_) => {
                return Err(Error::Internal(format!(
                    "$push: field '{path}' is not an array"
                )));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $pull
// ---------------------------------------------------------------------------

fn apply_pull(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, condition) in args {
        let field = get_nested_field(doc, path).cloned();

        if let Some(Bson::Array(arr)) = field {
            let new_arr: Vec<Bson> = arr
                .into_iter()
                .filter(|elem| !matches_pull_condition(elem, condition))
                .collect();
            set_nested(doc, path, Bson::Array(new_arr));
        }
        // If field doesn't exist or isn't an array, $pull is a no-op.
    }
    Ok(())
}

/// Returns `true` if `elem` matches the `$pull` condition.
fn matches_pull_condition(elem: &Bson, condition: &Bson) -> bool {
    match condition {
        // Plain value: exact equality match.
        non_doc if !matches!(non_doc, Bson::Document(_)) => elem == non_doc,
        // Document: either a query filter or an embedded-doc match.
        Bson::Document(filter_doc) => {
            if let Bson::Document(elem_doc) = elem {
                crate::query::eval_filter(elem_doc, filter_doc).unwrap_or(false)
            } else {
                false
            }
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// $addToSet
// ---------------------------------------------------------------------------

fn apply_add_to_set(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, value) in args {
        let field = get_nested_field(doc, path).cloned();

        match field {
            None => {
                set_nested(doc, path, Bson::Array(vec![value.clone()]));
            }
            Some(Bson::Array(mut arr)) => {
                if !arr.contains(value) {
                    arr.push(value.clone());
                    set_nested(doc, path, Bson::Array(arr));
                }
            }
            Some(_) => {
                return Err(Error::Internal(format!(
                    "$addToSet: field '{path}' is not an array"
                )));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $pop
// ---------------------------------------------------------------------------

fn apply_pop(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, dir) in args {
        let dir_n = as_f64(dir).ok_or_else(|| {
            Error::Internal(format!("$pop: value must be 1 or -1, got {dir:?}"))
        })?;

        let field = get_nested_field(doc, path).cloned();
        if let Some(Bson::Array(mut arr)) = field {
            if !arr.is_empty() {
                if dir_n >= 1.0 {
                    arr.pop(); // remove last
                } else {
                    arr.remove(0); // remove first
                }
            }
            set_nested(doc, path, Bson::Array(arr));
        }
        // Non-existent or non-array fields are silently ignored.
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $currentDate
// ---------------------------------------------------------------------------

fn apply_current_date(doc: &mut Document, args: &Document) -> Result<()> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    for (path, spec) in args {
        let value = match spec {
            Bson::Boolean(true) => Bson::DateTime(DateTime::from_millis(now_ms)),
            Bson::Document(type_doc) => {
                let type_str = type_doc.get_str("$type").unwrap_or("date");
                match type_str {
                    "timestamp" => {
                        let secs = (now_ms / 1000) as u32;
                        let inc = (now_ms % 1000) as u32;
                        Bson::Timestamp(Timestamp { time: secs, increment: inc })
                    }
                    _ => Bson::DateTime(DateTime::from_millis(now_ms)),
                }
            }
            _ => Bson::DateTime(DateTime::from_millis(now_ms)),
        };
        set_nested(doc, path, value);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Upsert document construction
// ---------------------------------------------------------------------------

/// Build the base document for an upsert by extracting equality conditions
/// from `filter`.
///
/// Only `{ field: value }` equality conditions (including dotted-path forms)
/// contribute to the new document.  Operator conditions like `{age: {$gt: 5}}`
/// are ignored.
pub(crate) fn upsert_base_from_filter(filter: &Document) -> Document {
    let mut base = Document::new();

    for (key, value) in filter {
        // Skip top-level logical operators.
        if key.starts_with('$') {
            continue;
        }

        // Only plain equality values (not sub-documents starting with $).
        if let Bson::Document(sub) = value {
            if sub.keys().any(|k| k.starts_with('$')) {
                // Operator condition — skip.
                continue;
            }
        }

        set_nested(&mut base, key, value.clone());
    }

    base
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn apply(doc: &mut Document, update: &Document) -> Result<()> {
        apply_update(doc, update, false)
    }

    // ---- $set ---------------------------------------------------------------

    #[test]
    fn set_top_level_field() {
        let mut doc = doc! { "name": "Alice" };
        apply(&mut doc, &doc! { "$set": { "name": "Bob" } }).unwrap();
        assert_eq!(doc.get_str("name").unwrap(), "Bob");
    }

    #[test]
    fn set_nested_field() {
        let mut doc = doc! { "address": { "city": "NYC" } };
        apply(&mut doc, &doc! { "$set": { "address.city": "LA" } }).unwrap();
        let addr = doc.get_document("address").unwrap();
        assert_eq!(addr.get_str("city").unwrap(), "LA");
    }

    #[test]
    fn set_creates_new_field() {
        let mut doc = doc! {};
        apply(&mut doc, &doc! { "$set": { "x": 42i32 } }).unwrap();
        assert_eq!(doc.get_i32("x").unwrap(), 42);
    }

    #[test]
    fn set_cannot_overwrite_id() {
        let mut doc = doc! { "_id": "original" };
        apply(&mut doc, &doc! { "$set": { "_id": "changed" } }).unwrap();
        // _id must remain unchanged.
        assert_eq!(doc.get_str("_id").unwrap(), "original");
    }

    // ---- $unset -------------------------------------------------------------

    #[test]
    fn unset_removes_field() {
        let mut doc = doc! { "a": 1i32, "b": 2i32 };
        apply(&mut doc, &doc! { "$unset": { "a": "" } }).unwrap();
        assert!(!doc.contains_key("a"));
        assert!(doc.contains_key("b"));
    }

    #[test]
    fn unset_nonexistent_is_noop() {
        let mut doc = doc! { "a": 1i32 };
        apply(&mut doc, &doc! { "$unset": { "z": "" } }).unwrap();
        assert_eq!(doc.len(), 1);
    }

    // ---- $inc ---------------------------------------------------------------

    #[test]
    fn inc_existing_int() {
        let mut doc = doc! { "n": 5i32 };
        apply(&mut doc, &doc! { "$inc": { "n": 3i32 } }).unwrap();
        assert_eq!(doc.get_i32("n").unwrap(), 8);
    }

    #[test]
    fn inc_creates_field_if_missing() {
        let mut doc = doc! {};
        apply(&mut doc, &doc! { "$inc": { "cnt": 1i32 } }).unwrap();
        // Result is f64 when there's no existing field to guide the type.
        let v = doc.get("cnt").unwrap();
        assert_eq!(as_f64(v), Some(1.0));
    }

    // ---- $mul ---------------------------------------------------------------

    #[test]
    fn mul_doubles_field() {
        let mut doc = doc! { "price": 10i32 };
        apply(&mut doc, &doc! { "$mul": { "price": 3i32 } }).unwrap();
        let v = doc.get("price").unwrap();
        assert_eq!(as_f64(v), Some(30.0));
    }

    // ---- $rename ------------------------------------------------------------

    #[test]
    fn rename_moves_field() {
        let mut doc = doc! { "old": "value" };
        apply(&mut doc, &doc! { "$rename": { "old": "new_name" } }).unwrap();
        assert!(!doc.contains_key("old"));
        assert_eq!(doc.get_str("new_name").unwrap(), "value");
    }

    // ---- $push / $pull / $addToSet / $pop -----------------------------------

    #[test]
    fn push_appends_to_array() {
        let mut doc = doc! { "arr": [1i32, 2i32] };
        apply(&mut doc, &doc! { "$push": { "arr": 3i32 } }).unwrap();
        let arr = doc.get_array("arr").unwrap();
        assert_eq!(arr.len(), 3);
    }

    #[test]
    fn push_creates_array_if_missing() {
        let mut doc = doc! {};
        apply(&mut doc, &doc! { "$push": { "arr": 1i32 } }).unwrap();
        let arr = doc.get_array("arr").unwrap();
        assert_eq!(arr.len(), 1);
    }

    #[test]
    fn pull_removes_matching_element() {
        let mut doc = doc! { "arr": [1i32, 2i32, 3i32, 2i32] };
        apply(&mut doc, &doc! { "$pull": { "arr": 2i32 } }).unwrap();
        let arr = doc.get_array("arr").unwrap();
        assert_eq!(arr.len(), 2);
        assert!(!arr.contains(&Bson::Int32(2)));
    }

    #[test]
    fn add_to_set_does_not_duplicate() {
        let mut doc = doc! { "tags": ["a", "b"] };
        apply(&mut doc, &doc! { "$addToSet": { "tags": "b" } }).unwrap();
        let arr = doc.get_array("tags").unwrap();
        assert_eq!(arr.len(), 2); // still 2
    }

    #[test]
    fn add_to_set_inserts_new_value() {
        let mut doc = doc! { "tags": ["a"] };
        apply(&mut doc, &doc! { "$addToSet": { "tags": "c" } }).unwrap();
        let arr = doc.get_array("tags").unwrap();
        assert_eq!(arr.len(), 2);
    }

    #[test]
    fn pop_last_element() {
        let mut doc = doc! { "arr": [1i32, 2i32, 3i32] };
        apply(&mut doc, &doc! { "$pop": { "arr": 1i32 } }).unwrap();
        let arr = doc.get_array("arr").unwrap();
        // $pop with 1 removes the LAST element: [1, 2, 3] -> [1, 2]
        assert_eq!(arr.len(), 2);
        assert_eq!(arr.last(), Some(&Bson::Int32(2)));
    }

    #[test]
    fn pop_first_element() {
        let mut doc = doc! { "arr": [1i32, 2i32, 3i32] };
        apply(&mut doc, &doc! { "$pop": { "arr": -1i32 } }).unwrap();
        let arr = doc.get_array("arr").unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0], Bson::Int32(2));
    }

    // ---- unsupported operator -----------------------------------------------

    #[test]
    fn unsupported_operator_returns_error() {
        let mut doc = doc! {};
        // $where takes a string (not a doc) - ensures we check operator before args
        let result = apply_update(&mut doc, &doc! { "$where": "x > 5" }, false);
        assert!(
            matches!(result, Err(Error::UnsupportedOperator { .. })),
            "expected UnsupportedOperator, got: {:?}",
            result
        );
    }

    #[test]
    fn bit_operator_returns_unsupported() {
        let mut doc = doc! {};
        let result = apply_update(
            &mut doc,
            &doc! { "$bit": { "flags": { "or": 3i32 } } },
            false,
        );
        assert!(matches!(result, Err(Error::UnsupportedOperator { .. })));
    }

    // ---- upsert base from filter --------------------------------------------

    #[test]
    fn upsert_base_extracts_equality_fields() {
        let filter = doc! { "name": "Alice", "age": 30i32 };
        let base = upsert_base_from_filter(&filter);
        assert_eq!(base.get_str("name").unwrap(), "Alice");
        assert_eq!(base.get_i32("age").unwrap(), 30);
    }

    #[test]
    fn upsert_base_skips_operator_conditions() {
        let filter = doc! { "age": { "$gt": 18i32 }, "name": "Bob" };
        let base = upsert_base_from_filter(&filter);
        assert!(!base.contains_key("age"));
        assert_eq!(base.get_str("name").unwrap(), "Bob");
    }

    #[test]
    fn upsert_base_skips_logical_operators() {
        let filter = doc! { "$and": [{ "x": 1i32 }] };
        let base = upsert_base_from_filter(&filter);
        assert!(!base.contains_key("$and"));
        assert!(!base.contains_key("x"));
    }

    // ---- $setOnInsert -------------------------------------------------------

    #[test]
    fn set_on_insert_applied_when_is_insert() {
        let mut doc = doc! {};
        apply_update(&mut doc, &doc! { "$setOnInsert": { "created": 1i32 } }, true).unwrap();
        assert_eq!(doc.get_i32("created").unwrap(), 1);
    }

    #[test]
    fn set_on_insert_not_applied_when_not_insert() {
        let mut doc = doc! {};
        apply_update(&mut doc, &doc! { "$setOnInsert": { "created": 1i32 } }, false).unwrap();
        assert!(!doc.contains_key("created"));
    }
}
