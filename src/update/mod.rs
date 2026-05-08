//! MongoDB update operator implementation.
//!
//! Supported operators:
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
//! | `$push`       | Append elements to arrays; modifiers: `$each`, `$position`, `$sort`, `$slice` |
//! | `$pull`       | Remove matching elements from arrays |
//! | `$pullAll`    | Remove all occurrences of specified values |
//! | `$addToSet`   | Add elements to arrays (no duplicates); `$each` modifier supported |
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
pub(crate) fn apply_update(doc: &mut Document, update: &Document, is_insert: bool) -> Result<()> {
    for (op, args) in update {
        // Validate / reject the operator BEFORE attempting to parse args.
        // This ensures Error::UnsupportedOperator is returned regardless of
        // the argument type.
        match op.as_str() {
            "$set" | "$unset" | "$inc" | "$mul" | "$rename" | "$min" | "$max" | "$push"
            | "$pull" | "$pullAll" | "$addToSet" | "$pop" | "$currentDate" | "$setOnInsert" => {} // supported — fall through to args parse

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
            "$set" => apply_set(doc, args_doc),
            "$unset" => apply_unset(doc, args_doc),
            "$inc" => apply_inc(doc, args_doc)?,
            "$mul" => apply_mul(doc, args_doc)?,
            "$rename" => apply_rename(doc, args_doc)?,
            "$min" => apply_min(doc, args_doc),
            "$max" => apply_max(doc, args_doc),
            "$push" => apply_push(doc, args_doc)?,
            "$pull" => apply_pull(doc, args_doc),
            "$pullAll" => apply_pull_all(doc, args_doc)?,
            "$addToSet" => apply_add_to_set(doc, args_doc)?,
            "$pop" => apply_pop(doc, args_doc)?,
            "$currentDate" => apply_current_date(doc, args_doc),
            "$setOnInsert" => {
                if is_insert {
                    apply_set(doc, args_doc);
                }
            }
            _ => {
                return Err(Error::UnsupportedOperator {
                    operator: op.to_string(),
                })
            }
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
    let Some(head) = parts.next() else {
        return;
    };

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
    let Some(head) = parts.next() else {
        return;
    };

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

fn apply_set(doc: &mut Document, args: &Document) {
    for (path, value) in args {
        set_nested(doc, path, value.clone());
    }
}

// ---------------------------------------------------------------------------
// $unset
// ---------------------------------------------------------------------------

fn apply_unset(doc: &mut Document, args: &Document) {
    for (path, _) in args {
        remove_nested(doc, path);
    }
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
    use crate::keys::encode_key;
    encode_key(a).cmp(&encode_key(b))
}

fn apply_min(doc: &mut Document, args: &Document) {
    for (path, new_val) in args {
        let should_set = match get_nested_field(doc, path) {
            None => true,
            Some(cur) => bson_cmp(new_val, cur) == std::cmp::Ordering::Less,
        };
        if should_set {
            set_nested(doc, path, new_val.clone());
        }
    }
}

fn apply_max(doc: &mut Document, args: &Document) {
    for (path, new_val) in args {
        let should_set = match get_nested_field(doc, path) {
            None => true,
            Some(cur) => bson_cmp(new_val, cur) == std::cmp::Ordering::Greater,
        };
        if should_set {
            set_nested(doc, path, new_val.clone());
        }
    }
}

// ---------------------------------------------------------------------------
// $push (with optional modifiers: $each, $position, $sort, $slice)
// ---------------------------------------------------------------------------

fn apply_push(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, value) in args {
        // If value is a modifier document (has $each key), handle modifiers.
        if let Bson::Document(modifier) = value {
            if modifier.contains_key("$each") {
                apply_push_modifiers(doc, path, modifier)?;
                continue;
            }
        }

        // Simple single-element push.
        let field = get_nested_field(doc, path).cloned();
        match field {
            None => {
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

/// Handle `$push` with modifier sub-document (`$each` required; `$position`,
/// `$sort`, `$slice` optional).
fn apply_push_modifiers(doc: &mut Document, path: &str, modifiers: &Document) -> Result<()> {
    let each_vals = modifiers
        .get("$each")
        .and_then(|v| v.as_array())
        .ok_or_else(|| Error::Internal("$push: $each modifier must be an array".into()))?;

    let position: Option<i64> = modifiers
        .get("$position")
        .and_then(as_f64)
        .map(|f| f as i64);

    let sort_spec = modifiers.get("$sort");

    let slice: Option<i64> = modifiers.get("$slice").and_then(as_f64).map(|f| f as i64);

    let field = get_nested_field(doc, path).cloned();
    let mut arr = match field {
        None => vec![],
        Some(Bson::Array(a)) => a,
        Some(_) => {
            return Err(Error::Internal(format!(
                "$push: field '{path}' is not an array"
            )));
        }
    };

    // Step 1: Insert elements at the specified position or append at the end.
    match position {
        Some(pos) => {
            // Negative position: count from the end of the array.
            let insert_at = if pos < 0 {
                let from_end = arr.len() as i64 + pos;
                if from_end < 0 {
                    0
                } else {
                    from_end as usize
                }
            } else {
                (pos as usize).min(arr.len())
            };
            for (i, val) in each_vals.iter().enumerate() {
                arr.insert(insert_at + i, val.clone());
            }
        }
        None => {
            for val in each_vals {
                arr.push(val.clone());
            }
        }
    }

    // Step 2: Apply $sort (runs before $slice).
    if let Some(spec) = sort_spec {
        sort_bson_array(&mut arr, spec)?;
    }

    // Step 3: Apply $slice.
    if let Some(n) = slice {
        slice_bson_array(&mut arr, n);
    }

    set_nested(doc, path, Bson::Array(arr));
    Ok(())
}

/// Sort `arr` according to a MongoDB sort specification.
///
/// `spec` is either:
/// - `1` / `-1` (scalar ascending / descending)
/// - A document `{ field: 1|−1, ... }` for arrays of embedded documents
fn sort_bson_array(arr: &mut [Bson], spec: &Bson) -> Result<()> {
    // Scalar direction.
    if let Some(d) = as_f64(spec) {
        if d >= 0.0 {
            arr.sort_by(bson_cmp);
        } else {
            arr.sort_by(|a, b| bson_cmp(b, a));
        }
        return Ok(());
    }

    // Document sort spec: { field: direction, ... }
    if let Bson::Document(sort_doc) = spec {
        // Collect owned (String, f64) pairs so the closure doesn't borrow `spec`.
        let sort_fields: Vec<(String, f64)> = sort_doc
            .iter()
            .map(|(k, v)| (k.clone(), as_f64(v).unwrap_or(1.0)))
            .collect();

        arr.sort_by(|a, b| {
            for (field, dir) in &sort_fields {
                let va = if let Bson::Document(ad) = a {
                    get_nested_field(ad, field).cloned().unwrap_or(Bson::Null)
                } else {
                    Bson::Null
                };
                let vb = if let Bson::Document(bd) = b {
                    get_nested_field(bd, field).cloned().unwrap_or(Bson::Null)
                } else {
                    Bson::Null
                };
                let ord = bson_cmp(&va, &vb);
                if ord != std::cmp::Ordering::Equal {
                    return if *dir >= 0.0 { ord } else { ord.reverse() };
                }
            }
            std::cmp::Ordering::Equal
        });
        return Ok(());
    }

    Err(Error::Internal(format!(
        "$push $sort: invalid sort specification {spec:?}"
    )))
}

/// Trim `arr` to `n` elements using MongoDB `$slice` semantics.
/// - `n > 0`: keep the first `n` elements
/// - `n < 0`: keep the last `|n|` elements
/// - `n == 0`: clear the array
fn slice_bson_array(arr: &mut Vec<Bson>, n: i64) {
    if n == 0 {
        arr.clear();
    } else if n > 0 {
        arr.truncate(n as usize);
    } else {
        // Negative: keep last |n| elements.
        let keep = (-n) as usize;
        if keep < arr.len() {
            arr.drain(..arr.len() - keep);
        }
    }
}

// ---------------------------------------------------------------------------
// $pull
// ---------------------------------------------------------------------------

fn apply_pull(doc: &mut Document, args: &Document) {
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
}

/// Returns `true` if `elem` matches the `$pull` condition.
fn matches_pull_condition(elem: &Bson, condition: &Bson) -> bool {
    match condition {
        // Document: either a query filter or an embedded-doc match.
        Bson::Document(filter_doc) => match elem {
            Bson::Document(elem_doc) => {
                crate::query::eval_filter(elem_doc, filter_doc).unwrap_or(false)
            }
            _ => false,
        },
        // Plain value: exact equality match.
        non_doc => elem == non_doc,
    }
}

// ---------------------------------------------------------------------------
// $addToSet (with optional $each modifier)
// ---------------------------------------------------------------------------

fn apply_add_to_set(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, value) in args {
        // Collect the elements to (potentially) add.
        let elements: Vec<Bson> = if let Bson::Document(modifier) = value {
            if let Some(each_arr) = modifier.get("$each").and_then(|v| v.as_array()) {
                each_arr.clone()
            } else {
                vec![value.clone()]
            }
        } else {
            vec![value.clone()]
        };

        let field = get_nested_field(doc, path).cloned();
        let field_missing = field.is_none();
        let mut arr = match field {
            None => vec![],
            Some(Bson::Array(a)) => a,
            Some(_) => {
                return Err(Error::Internal(format!(
                    "$addToSet: field '{path}' is not an array"
                )));
            }
        };

        let mut any_added = false;
        for elem in elements {
            if !arr.contains(&elem) {
                arr.push(elem);
                any_added = true;
            }
        }

        // Write back whenever we added elements OR when the field didn't exist
        // (so the field is created even if $each was empty).
        if any_added || field_missing {
            set_nested(doc, path, Bson::Array(arr));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $pullAll
// ---------------------------------------------------------------------------

/// Remove all array elements that exactly match any value in the provided list.
///
/// Syntax: `{ $pullAll: { <field>: [ <value1>, <value2>, ... ] } }`
fn apply_pull_all(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, values_bson) in args {
        let values = values_bson.as_array().ok_or_else(|| {
            Error::Internal(format!(
                "$pullAll: argument for '{path}' must be an array, got {values_bson:?}"
            ))
        })?;

        if let Some(Bson::Array(arr)) = get_nested_field(doc, path).cloned() {
            let new_arr: Vec<Bson> = arr
                .into_iter()
                .filter(|elem| !values.contains(elem))
                .collect();
            set_nested(doc, path, Bson::Array(new_arr));
        }
        // If the field doesn't exist or isn't an array, $pullAll is a no-op.
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $pop
// ---------------------------------------------------------------------------

fn apply_pop(doc: &mut Document, args: &Document) -> Result<()> {
    for (path, dir) in args {
        let dir_n = as_f64(dir)
            .ok_or_else(|| Error::Internal(format!("$pop: value must be 1 or -1, got {dir:?}")))?;

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

fn apply_current_date(doc: &mut Document, args: &Document) {
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
                        Bson::Timestamp(Timestamp {
                            time: secs,
                            increment: inc,
                        })
                    }
                    _ => Bson::DateTime(DateTime::from_millis(now_ms)),
                }
            }
            _ => Bson::DateTime(DateTime::from_millis(now_ms)),
        };
        set_nested(doc, path, value);
    }
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
mod tests;
