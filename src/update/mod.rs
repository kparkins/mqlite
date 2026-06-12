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
//! | `$bit`        | Bitwise `and`/`or`/`xor` on integer fields |
//!
//! Path segments support the all-positional operator `$[]`, which applies the
//! remainder of the path and the operation to every element of the array at
//! that point (e.g. `grades.$[].score`); the filtered-positional
//! `$[<identifier>]` operator (paired with `arrayFilters`); and the positional
//! `$` operator, which targets the first array element matched by the query
//! filter.
//!
//! All unknown `$` operators return [`Error::UnsupportedOperator`].
//! A top-level document with no `$` operators is treated as a replacement
//! and is **not** handled here; use the engine's replace path instead.

use std::cmp::Ordering;
use std::collections::HashMap;

use bson::{Bson, DateTime, Document, Timestamp};

use crate::error::{Error, Result};
use crate::query::{eval_filter, get_nested_field};

pub use pipeline::UpdateModifications;
pub(crate) use pipeline::apply_update_pipeline;

mod pipeline;

// ---------------------------------------------------------------------------
// Array-filter / positional update context
// ---------------------------------------------------------------------------

/// Per-`apply_update` traversal context carrying the query filter (for the
/// positional `$` operator) and the parsed `arrayFilters` (for the
/// filtered-positional `$[<identifier>]` operator).
///
/// The context also records which array-filter identifiers were exercised by a
/// path so that [`UpdateContext::ensure_all_filters_used`] can flag an
/// `arrayFilter` that no update path referenced.
struct UpdateContext<'a> {
    /// The query filter that selected the document, used to resolve `$`.
    filter: &'a Document,
    /// Map of array-filter identifier -> its condition document.
    array_filters: HashMap<String, Document>,
    /// Identifiers actually consumed by a `$[<identifier>]` path segment.
    used_identifiers: std::cell::RefCell<Vec<String>>,
}

impl<'a> UpdateContext<'a> {
    /// Build a context from the query `filter` and the raw `array_filters`
    /// list, validating identifier uniqueness and shape.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] when a filter document is empty, names more
    /// than one top-level identifier, or duplicates an identifier.
    fn new(filter: &'a Document, array_filters: Option<&[Document]>) -> Result<Self> {
        let array_filters = parse_array_filters(array_filters)?;
        Ok(Self {
            filter,
            array_filters,
            used_identifiers: std::cell::RefCell::new(Vec::new()),
        })
    }

    /// Mark `ident` as used by an update path.
    fn mark_used(&self, ident: &str) {
        let mut used = self.used_identifiers.borrow_mut();
        if !used.iter().any(|existing| existing == ident) {
            used.push(ident.to_owned());
        }
    }

    /// Resolve the condition document for array-filter `ident`, recording its
    /// use.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] when no array filter defines `ident`.
    fn filter_for(&self, ident: &str, full_path: &str) -> Result<&Document> {
        self.mark_used(ident);
        self.array_filters.get(ident).ok_or_else(|| {
            Error::Internal(format!(
                "No array filter found for identifier '{ident}' in path '{full_path}'"
            ))
        })
    }

    /// Error if an array filter was declared but never referenced by a path.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] naming the first unused identifier.
    fn ensure_all_filters_used(&self) -> Result<()> {
        let used = self.used_identifiers.borrow();
        for ident in self.array_filters.keys() {
            if !used.iter().any(|existing| existing == ident) {
                return Err(Error::Internal(format!(
                    "The array filter for identifier '{ident}' was not used in \
                     the update."
                )));
            }
        }
        Ok(())
    }
}

/// Parse the raw `arrayFilters` list into a map of identifier -> condition.
///
/// Each filter document's top-level keys must all share a single first path
/// segment, which becomes the identifier. Two filter documents that declare the
/// same identifier are rejected, as is an empty filter document.
///
/// # Errors
///
/// Returns [`Error::Internal`] for an empty filter document, a filter naming
/// more than one identifier, or a duplicate identifier.
fn parse_array_filters(array_filters: Option<&[Document]>) -> Result<HashMap<String, Document>> {
    let mut map: HashMap<String, Document> = HashMap::new();
    let Some(filters) = array_filters else {
        return Ok(map);
    };

    for filter in filters {
        if filter.is_empty() {
            return Err(Error::Internal(
                "Cannot use an expression without a top-level field name in \
                 arrayFilters"
                    .to_owned(),
            ));
        }

        let mut identifier: Option<&str> = None;
        for key in filter.keys() {
            let first_segment = key.split('.').next().unwrap_or(key);
            match identifier {
                None => identifier = Some(first_segment),
                Some(existing) if existing == first_segment => {}
                Some(_) => {
                    return Err(Error::Internal(format!(
                        "Error parsing array filter: Expected a single top-level \
                         field name, found '{}' and '{first_segment}'",
                        identifier.unwrap_or_default()
                    )));
                }
            }
        }

        let identifier = identifier.unwrap_or_default().to_owned();
        if map.contains_key(&identifier) {
            return Err(Error::Internal(format!(
                "Found multiple array filters with the same top-level field name \
                 {identifier}"
            )));
        }
        map.insert(identifier, filter.clone());
    }

    Ok(map)
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Apply a MongoDB operator update document to `doc` in place.
///
/// `update` must contain only `$` operator keys at the top level.  A document
/// without any `$` keys is a **replacement** — call the engine's replace path
/// instead.
///
/// `filter` is the query that selected `doc`; it provides the matched position
/// for the positional `$` operator. `array_filters` supplies the conditions for
/// any filtered-positional `$[<identifier>]` path segment.
///
/// `is_insert` is `true` when this is the "insert" half of an upsert: the
/// `$setOnInsert` operator is applied only when `is_insert == true`.
///
/// # Errors
///
/// - [`Error::UnsupportedOperator`] for any unrecognised `$` operator.
/// - [`Error::Internal`] for type mismatches (e.g. `$inc` on a non-number),
///   array-filter parse errors, or unresolved/unused positional identifiers.
pub(crate) fn apply_update(
    doc: &mut Document,
    update: &Document,
    filter: &Document,
    array_filters: Option<&[Document]>,
    is_insert: bool,
) -> Result<()> {
    let ctx = UpdateContext::new(filter, array_filters)?;
    apply_update_with_ctx(doc, update, &ctx, is_insert)?;
    ctx.ensure_all_filters_used()
}

/// Apply `update` using an already-built [`UpdateContext`].
fn apply_update_with_ctx(
    doc: &mut Document,
    update: &Document,
    ctx: &UpdateContext<'_>,
    is_insert: bool,
) -> Result<()> {
    for (op, args) in update {
        // Validate / reject the operator BEFORE attempting to parse args.
        // This ensures Error::UnsupportedOperator is returned regardless of
        // the argument type.
        match op.as_str() {
            "$set" | "$unset" | "$inc" | "$mul" | "$rename" | "$min" | "$max" | "$push"
            | "$pull" | "$pullAll" | "$addToSet" | "$pop" | "$currentDate" | "$setOnInsert"
            | "$bit" => {} // supported — fall through to args parse

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
            "$set" => apply_set(doc, args_doc, ctx)?,
            "$unset" => apply_unset(doc, args_doc, ctx)?,
            "$inc" => apply_arithmetic_update(doc, args_doc, "$inc", ctx, |current, operand| {
                current + operand
            })?,
            "$mul" => apply_arithmetic_update(doc, args_doc, "$mul", ctx, |current, operand| {
                current * operand
            })?,
            "$rename" => apply_rename(doc, args_doc, ctx)?,
            "$min" => apply_min_max(doc, args_doc, Ordering::Less, ctx)?,
            "$max" => apply_min_max(doc, args_doc, Ordering::Greater, ctx)?,
            "$push" => apply_push(doc, args_doc, ctx)?,
            "$pull" => apply_pull(doc, args_doc, ctx)?,
            "$pullAll" => apply_pull_all(doc, args_doc, ctx)?,
            "$addToSet" => apply_add_to_set(doc, args_doc, ctx)?,
            "$pop" => apply_pop(doc, args_doc, ctx)?,
            "$bit" => apply_bit(doc, args_doc, ctx)?,
            "$currentDate" => apply_current_date(doc, args_doc, ctx)?,
            "$setOnInsert" => {
                if is_insert {
                    apply_set(doc, args_doc, ctx)?;
                }
            }
            _ => {
                return Err(Error::UnsupportedOperator {
                    operator: op.to_owned(),
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

/// The all-positional array update path segment.  Applies the remainder of the
/// path and the operation to every element of the array at that point.
const ALL_POSITIONAL: &str = "$[]";

/// The positional array update path segment `$`.
const POSITIONAL: &str = "$";

/// Classify a single path segment for update-path traversal.
enum Segment<'a> {
    /// An ordinary field name (may parse to a numeric array index).
    Field(&'a str),
    /// The all-positional operator `$[]`.
    AllPositional,
    /// The positional `$` operator: targets the query-matched array element.
    Positional,
    /// The filtered-positional operator `$[<identifier>]`.
    Filtered(&'a str),
    /// A malformed `$[...]` segment whose identifier is invalid.
    Unsupported(&'a str),
}

/// Classify `segment` for update traversal.
fn classify_segment(segment: &str) -> Segment<'_> {
    if segment == ALL_POSITIONAL {
        return Segment::AllPositional;
    }
    if segment == POSITIONAL {
        return Segment::Positional;
    }
    if segment.starts_with("$[") && segment.ends_with(']') {
        let ident = &segment[2..segment.len() - 1];
        if is_valid_array_filter_identifier(ident) {
            return Segment::Filtered(ident);
        }
        return Segment::Unsupported(segment);
    }
    Segment::Field(segment)
}

/// Returns `true` when `ident` is a valid array-filter identifier: it must
/// begin with a lowercase letter and contain only alphanumeric characters.
fn is_valid_array_filter_identifier(ident: &str) -> bool {
    let mut chars = ident.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric())
}

/// Walk `path` (dotted, with optional `$[]`/`$[<ident>]`/`$` array segments) to
/// every terminal location and apply `op` there.
///
/// `op` receives the current leaf value (cloned, `None` if absent) and returns
/// the replacement value, or `None` to remove the leaf.  Intermediate
/// documents are created on demand; a non-document intermediate is overwritten
/// with a fresh document, matching legacy `$set` behaviour.
///
/// `ctx` carries the query filter (for `$`) and the parsed `arrayFilters`
/// (for `$[<ident>]`).
///
/// # Errors
///
/// - [`Error::UnsupportedOperator`] when a `$[<ident>]` segment is malformed.
/// - [`Error::Internal`] for a missing/non-array array prefix, an unresolved
///   array-filter identifier, too many `$` segments, or a positional `$` with
///   no matching query condition.
fn modify_nested<F>(
    doc: &mut Document,
    path: &str,
    ctx: &UpdateContext<'_>,
    op: &mut F,
) -> Result<()>
where
    F: FnMut(Option<Bson>) -> Result<Option<Bson>>,
{
    if count_positional_segments(path) > 1 {
        return Err(Error::Internal(format!(
            "Too many positional (i.e. '$') elements found in path '{path}'"
        )));
    }
    modify_nested_inner(doc, "", path, path, ctx, op)
}

/// Count the number of bare positional `$` segments in `path`.
fn count_positional_segments(path: &str) -> usize {
    path.split('.')
        .filter(|seg| matches!(classify_segment(seg), Segment::Positional))
        .count()
}

#[allow(clippy::too_many_arguments)]
fn modify_nested_inner<F>(
    doc: &mut Document,
    prefix: &str,
    path: &str,
    full_path: &str,
    ctx: &UpdateContext<'_>,
    op: &mut F,
) -> Result<()>
where
    F: FnMut(Option<Bson>) -> Result<Option<Bson>>,
{
    let mut parts = path.splitn(2, '.');
    let Some(head) = parts.next() else {
        return Ok(());
    };
    let rest = parts.next();

    // A leading `$[]`/`$`/`$[ident]` cannot apply to a Document: arrays are
    // always reached through a preceding field, so these are handled during
    // field descent below.  Reaching one here means it was the very first
    // segment of the path.
    match classify_segment(head) {
        Segment::Unsupported(seg) => Err(Error::UnsupportedOperator {
            operator: seg.to_owned(),
        }),
        Segment::AllPositional => all_positional_missing(prefix),
        Segment::Positional | Segment::Filtered(_) => Err(Error::Internal(format!(
            "The path '{}' must exist in the document in order to apply array \
             updates.",
            join_path(prefix, head)
        ))),
        Segment::Field(field) => match rest {
            None => {
                // Protect top-level `_id` only: nested `_id` fields (e.g.
                // `a._id`) are writable, matching MongoDB.
                if prefix.is_empty() && field == "_id" {
                    return Ok(());
                }
                apply_leaf(doc, field, op)
            }
            Some(rest) => descend_field(doc, prefix, field, rest, full_path, ctx, op),
        },
    }
}

/// Apply `op` to the terminal `field` of `doc`, inserting/removing as directed.
/// Top-level `_id` protection is enforced by the caller.
fn apply_leaf<F>(doc: &mut Document, field: &str, op: &mut F) -> Result<()>
where
    F: FnMut(Option<Bson>) -> Result<Option<Bson>>,
{
    let current = doc.get(field).cloned();
    match op(current)? {
        Some(new_val) => {
            doc.insert(field, new_val);
        }
        None => {
            doc.remove(field);
        }
    }
    Ok(())
}

/// Descend into `field`, then continue traversal with `rest`.
///
/// When the next segment is an array operator (`$[]`, `$[<ident>]`, or `$`),
/// `field`'s value is the array to iterate.  Otherwise an intermediate document
/// is created on demand (overwriting a non-document intermediate, matching
/// legacy `$set`).
#[allow(clippy::too_many_arguments)]
fn descend_field<F>(
    doc: &mut Document,
    prefix: &str,
    field: &str,
    rest: &str,
    full_path: &str,
    ctx: &UpdateContext<'_>,
    op: &mut F,
) -> Result<()>
where
    F: FnMut(Option<Bson>) -> Result<Option<Bson>>,
{
    let new_prefix = join_path(prefix, field);

    // Peek the next segment: an array operator means `field` holds the array.
    let mut next_parts = rest.splitn(2, '.');
    let next_head = next_parts.next().unwrap_or("");
    let after = next_parts.next();

    match classify_segment(next_head) {
        Segment::AllPositional => {
            let Some(value) = doc.get_mut(field) else {
                return Err(missing_array_prefix(&join_path(&new_prefix, next_head)));
            };
            apply_all_positional(value, field, &new_prefix, after, full_path, ctx, op)
        }
        Segment::Filtered(ident) => {
            let condition = ctx.filter_for(ident, full_path)?.clone();
            let Some(value) = doc.get_mut(field) else {
                return Err(missing_array_prefix(&join_path(&new_prefix, next_head)));
            };
            apply_filtered_positional(
                value, field, &new_prefix, ident, &condition, after, full_path, ctx, op,
            )
        }
        Segment::Positional => {
            let index = resolve_positional_index(doc, field, &new_prefix, ctx)?;
            let Some(value) = doc.get_mut(field) else {
                return Err(missing_array_prefix(&join_path(&new_prefix, next_head)));
            };
            apply_positional(value, field, &new_prefix, index, after, full_path, ctx, op)
        }
        Segment::Unsupported(seg) => Err(Error::UnsupportedOperator {
            operator: seg.to_owned(),
        }),
        Segment::Field(_) => {
            let nested = doc
                .entry(field.to_owned())
                .or_insert_with(|| Bson::Document(Document::new()));

            if let Bson::Document(nested_doc) = nested {
                return modify_nested_inner(nested_doc, &new_prefix, rest, full_path, ctx, op);
            }

            // Overwrite a non-document intermediate with a fresh document.
            let mut new_doc = Document::new();
            modify_nested_inner(&mut new_doc, &new_prefix, rest, full_path, ctx, op)?;
            doc.insert(field, Bson::Document(new_doc));
            Ok(())
        }
    }
}

/// Apply the all-positional operation to every element of the array `value`.
///
/// `field` is the array's own field name (for the non-array error message);
/// `elem_prefix` is the full dotted path including `$[]` (for nested errors).
/// `after` is the remaining path after `$[]`, if any.
#[allow(clippy::too_many_arguments)]
fn apply_all_positional<F>(
    value: &mut Bson,
    field: &str,
    elem_prefix: &str,
    after: Option<&str>,
    full_path: &str,
    ctx: &UpdateContext<'_>,
    op: &mut F,
) -> Result<()>
where
    F: FnMut(Option<Bson>) -> Result<Option<Bson>>,
{
    let Bson::Array(arr) = value else {
        return Err(non_array_element(field, value));
    };

    match after {
        // `grades.$[]`: replace each element directly.
        None => replace_each_element(arr, op),
        // `grades.$[].field` (possibly with further `$[]`): each element must
        // be a document; recurse into it.
        Some(after) => {
            let elem_all_positional = join_path(elem_prefix, ALL_POSITIONAL);
            for elem in arr.iter_mut() {
                if let Bson::Document(elem_doc) = elem {
                    modify_nested_inner(elem_doc, &elem_all_positional, after, full_path, ctx, op)?;
                }
                // Non-document elements are skipped (no-op) rather than erroring.
            }
            Ok(())
        }
    }
}

/// Apply the operation to every array element that satisfies the
/// `$[<ident>]` array filter `condition`.
///
/// `condition` keys are matched against a synthetic `{ident: element}` document
/// so that scalar elements, embedded-document fields (`ident.field`), and
/// operator conditions all evaluate through the shared filter engine.
/// Non-matching elements are left untouched; zero matches is a successful
/// no-op.
#[allow(clippy::too_many_arguments)]
fn apply_filtered_positional<F>(
    value: &mut Bson,
    field: &str,
    elem_prefix: &str,
    ident: &str,
    condition: &Document,
    after: Option<&str>,
    full_path: &str,
    ctx: &UpdateContext<'_>,
    op: &mut F,
) -> Result<()>
where
    F: FnMut(Option<Bson>) -> Result<Option<Bson>>,
{
    let Bson::Array(arr) = value else {
        return Err(non_array_element(field, value));
    };

    let elem_prefix = join_path(elem_prefix, &format!("$[{ident}]"));
    let mut i = 0;
    while i < arr.len() {
        if !array_filter_matches(ident, condition, &arr[i])? {
            i += 1;
            continue;
        }
        match after {
            None => match op(Some(arr[i].clone()))? {
                Some(new_val) => {
                    arr[i] = new_val;
                    i += 1;
                }
                None => {
                    arr.remove(i);
                }
            },
            Some(after) => {
                if let Bson::Document(elem_doc) = &mut arr[i] {
                    modify_nested_inner(elem_doc, &elem_prefix, after, full_path, ctx, op)?;
                }
                i += 1;
            }
        }
    }
    Ok(())
}

/// Apply the operation to the single positional `$` element at `index`.
///
/// `index` is the query-matched array position resolved by
/// [`resolve_positional_index`].
#[allow(clippy::too_many_arguments)]
fn apply_positional<F>(
    value: &mut Bson,
    field: &str,
    elem_prefix: &str,
    index: usize,
    after: Option<&str>,
    full_path: &str,
    ctx: &UpdateContext<'_>,
    op: &mut F,
) -> Result<()>
where
    F: FnMut(Option<Bson>) -> Result<Option<Bson>>,
{
    let Bson::Array(arr) = value else {
        return Err(non_array_element(field, value));
    };
    if index >= arr.len() {
        return Err(positional_no_match());
    }

    let elem_prefix = join_path(elem_prefix, POSITIONAL);
    match after {
        None => match op(Some(arr[index].clone()))? {
            Some(new_val) => arr[index] = new_val,
            None => {
                arr.remove(index);
            }
        },
        Some(after) => {
            if let Bson::Document(elem_doc) = &mut arr[index] {
                modify_nested_inner(elem_doc, &elem_prefix, after, full_path, ctx, op)?;
            }
        }
    }
    Ok(())
}

/// Replace each element of `arr` in place, removing elements for which `op`
/// returns `None`. Propagates the first element-level error.
fn replace_each_element<F>(arr: &mut Vec<Bson>, op: &mut F) -> Result<()>
where
    F: FnMut(Option<Bson>) -> Result<Option<Bson>>,
{
    let mut i = 0;
    while i < arr.len() {
        match op(Some(arr[i].clone()))? {
            Some(new_val) => {
                arr[i] = new_val;
                i += 1;
            }
            None => {
                arr.remove(i);
            }
        }
    }
    Ok(())
}

/// Evaluate a `$[<ident>]` array filter `condition` against array element
/// `elem` by wrapping the element in a synthetic `{ident: elem}` document.
fn array_filter_matches(ident: &str, condition: &Document, elem: &Bson) -> Result<bool> {
    let mut synthetic = Document::new();
    synthetic.insert(ident, elem.clone());
    eval_filter(&synthetic, condition)
}

/// Resolve the positional `$` index for the array at `array_path` using the
/// query `ctx.filter`.
///
/// The restricted filter retains every top-level condition whose key equals the
/// array path or begins with `"<array_path>."`. The matched index is the first
/// array element that satisfies every restricted condition (see
/// [`positional_element_matches`]).
///
/// # Errors
///
/// Returns [`Error::Internal`] when no query condition references the array path
/// or no element matches.
fn resolve_positional_index(
    doc: &Document,
    field: &str,
    array_path: &str,
    ctx: &UpdateContext<'_>,
) -> Result<usize> {
    let restricted = restrict_filter_to_path(ctx.filter, array_path);
    if restricted.is_empty() {
        return Err(positional_no_match());
    }

    let arr = match doc.get(field) {
        Some(Bson::Array(arr)) => arr,
        _ => return Err(positional_no_match()),
    };

    let leaf = array_path.rsplit('.').next().unwrap_or(array_path);
    for (index, elem) in arr.iter().enumerate() {
        if positional_element_matches(elem, leaf, array_path, &restricted)? {
            return Ok(index);
        }
    }
    Err(positional_no_match())
}

/// Keep only the top-level filter conditions that reference `array_path`
/// (equal to it, or prefixed by `"<array_path>."`).
fn restrict_filter_to_path(filter: &Document, array_path: &str) -> Document {
    let dotted_prefix = format!("{array_path}.");
    let mut restricted = Document::new();
    for (key, value) in filter {
        if key == array_path || key.starts_with(&dotted_prefix) {
            restricted.insert(key.clone(), value.clone());
        }
    }
    restricted
}

/// Test whether array element `elem` satisfies every restricted positional
/// condition.
///
/// Conditions keyed exactly at `array_path` evaluate against a synthetic
/// `{<leaf>: [elem]}` document, so array-unwrap semantics cover equality,
/// comparison operators, and `$elemMatch`.  Dotted conditions
/// (`"<array_path>.sub"`) evaluate against the element document directly,
/// because the filter evaluator does not traverse arrays on dotted paths; a
/// non-document element cannot satisfy a dotted condition.
fn positional_element_matches(
    elem: &Bson,
    leaf: &str,
    array_path: &str,
    restricted: &Document,
) -> Result<bool> {
    let dotted_prefix = format!("{array_path}.");
    for (key, condition) in restricted {
        let matched = if key == array_path {
            let mut synthetic = Document::new();
            synthetic.insert(leaf, Bson::Array(vec![elem.clone()]));
            let mut single = Document::new();
            single.insert(leaf, condition.clone());
            eval_filter(&synthetic, &single)?
        } else if let Some(sub) = key.strip_prefix(&dotted_prefix) {
            match elem {
                Bson::Document(elem_doc) => {
                    let mut single = Document::new();
                    single.insert(sub, condition.clone());
                    eval_filter(elem_doc, &single)?
                }
                _ => false,
            }
        } else {
            false
        };
        if !matched {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Build the missing-array-prefix error for an array operator whose prefix is
/// absent.
fn missing_array_prefix(path: &str) -> Error {
    Error::Internal(format!(
        "The path '{path}' must exist in the document in order to apply array \
         updates."
    ))
}

/// Build the non-array-element error for an array operator applied to a
/// non-array value.
fn non_array_element(field: &str, value: &Bson) -> Error {
    Error::Internal(format!(
        "Cannot apply array updates to non-array element {field}: {value:?}"
    ))
}

/// Build the positional `$` no-match error.
fn positional_no_match() -> Error {
    Error::Internal(
        "The positional operator did not find the match needed from the query.".to_owned(),
    )
}

/// Build the missing-prefix error for an `$[]` segment whose array prefix is
/// absent.
fn all_positional_missing(prefix: &str) -> Result<()> {
    Err(Error::Internal(format!(
        "The path '{}' must exist in the document in order to apply array \
         updates.",
        join_path(prefix, ALL_POSITIONAL)
    )))
}

/// Join a dotted `prefix` with a `segment`, omitting the separator when the
/// prefix is empty.
fn join_path(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_owned()
    } else {
        format!("{prefix}.{segment}")
    }
}

/// Set a (possibly dotted) field path to `value`, creating intermediate
/// documents as needed.  Supports array segments.  The `_id` field is
/// protected — attempting to set it is a no-op.
///
/// # Errors
///
/// Propagates [`modify_nested`] errors (malformed `$[ident]` segments, a
/// missing or non-array array prefix, or unresolved positional segments).
fn set_nested(doc: &mut Document, path: &str, value: Bson, ctx: &UpdateContext<'_>) -> Result<()> {
    modify_nested(doc, path, ctx, &mut |_| Ok(Some(value.clone())))
}

/// Set a plain (non-array-operator) dotted path. Used for upsert-base
/// construction where the filter equality paths never contain array operators.
///
/// # Errors
///
/// Propagates [`modify_nested`] errors.
pub(crate) fn set_plain_path(doc: &mut Document, path: &str, value: Bson) -> Result<()> {
    let empty = Document::new();
    let ctx = UpdateContext::new(&empty, None)?;
    set_nested(doc, path, value, &ctx)
}

/// Remove a (possibly dotted) field path.  Supports array segments.
///
/// # Errors
///
/// Propagates [`modify_nested`] errors.
fn remove_nested(doc: &mut Document, path: &str, ctx: &UpdateContext<'_>) -> Result<()> {
    modify_nested(doc, path, ctx, &mut |_| Ok(None))
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

fn apply_set(doc: &mut Document, args: &Document, ctx: &UpdateContext<'_>) -> Result<()> {
    for (path, value) in args {
        set_nested(doc, path, value.clone(), ctx)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $unset
// ---------------------------------------------------------------------------

fn apply_unset(doc: &mut Document, args: &Document, ctx: &UpdateContext<'_>) -> Result<()> {
    for (path, _) in args {
        remove_nested(doc, path, ctx)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $inc / $mul
// ---------------------------------------------------------------------------

fn apply_arithmetic_update(
    doc: &mut Document,
    args: &Document,
    op: &str,
    ctx: &UpdateContext<'_>,
    combine: fn(f64, f64) -> f64,
) -> Result<()> {
    for (path, operand) in args {
        let operand_n = as_f64(operand).ok_or_else(|| {
            Error::Internal(format!("{op}: value must be a number, got {operand:?}"))
        })?;

        modify_nested(doc, path, ctx, &mut |current| {
            let current_n = current.as_ref().and_then(as_f64).unwrap_or(0.0);
            Ok(Some(numeric_result(
                current.as_ref(),
                combine(current_n, operand_n),
            )))
        })?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $rename
// ---------------------------------------------------------------------------

fn apply_rename(doc: &mut Document, args: &Document, ctx: &UpdateContext<'_>) -> Result<()> {
    for (old_path, new_name) in args {
        let new_str = new_name.as_str().ok_or_else(|| {
            Error::Internal(format!(
                "$rename: new name must be a string, got {new_name:?}"
            ))
        })?;

        // MongoDB rejects $rename whose source or target traverses an array
        // element (including the all-positional `$[]` and positional `$`).
        if path_has_positional(old_path) || path_has_positional(new_str) {
            return Err(Error::Internal(
                "$rename does not allow array elements".to_owned(),
            ));
        }

        // Read the old value (cloned to avoid borrow conflict).
        let old_val = get_nested_field(doc, old_path).cloned();

        if let Some(val) = old_val {
            remove_nested(doc, old_path, ctx)?;
            set_nested(doc, new_str, val, ctx)?;
        }
        // If old field doesn't exist, $rename is a no-op.
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $min / $max
// ---------------------------------------------------------------------------

/// Compare two BSON values using MongoDB's type ordering for `$min`/`$max`.
fn bson_cmp(a: &Bson, b: &Bson) -> Ordering {
    use crate::keys::encode_key;
    encode_key(a).cmp(&encode_key(b))
}

fn apply_min_max(
    doc: &mut Document,
    args: &Document,
    target: Ordering,
    ctx: &UpdateContext<'_>,
) -> Result<()> {
    for (path, new_val) in args {
        modify_nested(doc, path, ctx, &mut |current| {
            let keep = match current.as_ref() {
                None => Some(new_val.clone()),
                Some(cur) if bson_cmp(new_val, cur) == target => Some(new_val.clone()),
                Some(cur) => Some(cur.clone()),
            };
            Ok(keep)
        })?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $push (with optional modifiers: $each, $position, $sort, $slice)
// ---------------------------------------------------------------------------

fn apply_push(doc: &mut Document, args: &Document, ctx: &UpdateContext<'_>) -> Result<()> {
    for (path, value) in args {
        // If value is a modifier document (has $each key), handle modifiers.
        if let Bson::Document(modifier) = value {
            if modifier.contains_key("$each") {
                apply_push_modifiers(doc, path, modifier, ctx)?;
                continue;
            }
        }

        // Simple single-element push via modify_nested so array segments work.
        let push_val = value.clone();
        modify_nested(doc, path, ctx, &mut |current| match current {
            None => Ok(Some(Bson::Array(vec![push_val.clone()]))),
            Some(Bson::Array(mut arr)) => {
                arr.push(push_val.clone());
                Ok(Some(Bson::Array(arr)))
            }
            Some(_) => Err(Error::Internal(format!(
                "$push: field '{path}' is not an array"
            ))),
        })?;
    }
    Ok(())
}

/// Handle `$push` with modifier sub-document (`$each` required; `$position`,
/// `$sort`, `$slice` optional).
fn apply_push_modifiers(
    doc: &mut Document,
    path: &str,
    modifiers: &Document,
    ctx: &UpdateContext<'_>,
) -> Result<()> {
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

    // Capture owned copies of modifier values before the closure borrows `doc`.
    let each_vals: Vec<Bson> = each_vals.to_owned();
    let sort_spec: Option<Bson> = sort_spec.cloned();

    modify_nested(doc, path, ctx, &mut |current| {
        let mut arr = match current {
            None => vec![],
            Some(Bson::Array(a)) => a,
            Some(_) => {
                return Err(Error::Internal(format!(
                    "$push: field '{path}' is not an array"
                )));
            }
        };

        // Step 1: Insert elements at the specified position or append.
        match position {
            Some(pos) => {
                let insert_at = if pos < 0 {
                    let from_end = arr.len() as i64 + pos;
                    if from_end < 0 { 0 } else { from_end as usize }
                } else {
                    (pos as usize).min(arr.len())
                };
                for (i, val) in each_vals.iter().enumerate() {
                    arr.insert(insert_at + i, val.clone());
                }
            }
            None => {
                for val in &each_vals {
                    arr.push(val.clone());
                }
            }
        }

        // Step 2: Apply $sort (runs before $slice).
        if let Some(ref spec) = sort_spec {
            sort_bson_array(&mut arr, spec)?;
        }

        // Step 3: Apply $slice.
        if let Some(n) = slice {
            slice_bson_array(&mut arr, n);
        }

        Ok(Some(Bson::Array(arr)))
    })?;
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

fn apply_pull(doc: &mut Document, args: &Document, ctx: &UpdateContext<'_>) -> Result<()> {
    for (path, condition) in args {
        modify_nested(doc, path, ctx, &mut |current| {
            match current {
                Some(Bson::Array(arr)) => {
                    let new_arr: Vec<Bson> = arr
                        .into_iter()
                        .filter(|elem| !matches_pull_condition(elem, condition))
                        .collect();
                    Ok(Some(Bson::Array(new_arr)))
                }
                // Field missing or not an array: $pull is a no-op.
                other => Ok(other),
            }
        })?;
    }
    Ok(())
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

fn apply_add_to_set(doc: &mut Document, args: &Document, ctx: &UpdateContext<'_>) -> Result<()> {
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

        modify_nested(doc, path, ctx, &mut |current| {
            let mut arr = match current {
                None => vec![],
                Some(Bson::Array(a)) => a,
                Some(_) => {
                    return Err(Error::Internal(format!(
                        "$addToSet: field '{path}' is not an array"
                    )));
                }
            };
            for elem in &elements {
                if !arr.contains(elem) {
                    arr.push(elem.clone());
                }
            }
            Ok(Some(Bson::Array(arr)))
        })?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $pullAll
// ---------------------------------------------------------------------------

/// Remove all array elements that exactly match any value in the provided list.
///
/// Syntax: `{ $pullAll: { <field>: [ <value1>, <value2>, ... ] } }`
fn apply_pull_all(doc: &mut Document, args: &Document, ctx: &UpdateContext<'_>) -> Result<()> {
    for (path, values_bson) in args {
        let values = values_bson.as_array().ok_or_else(|| {
            Error::Internal(format!(
                "$pullAll: argument for '{path}' must be an array, got {values_bson:?}"
            ))
        })?;
        let values = values.clone();

        modify_nested(doc, path, ctx, &mut |current| match current {
            Some(Bson::Array(arr)) => {
                let new_arr: Vec<Bson> =
                    arr.into_iter().filter(|elem| !values.contains(elem)).collect();
                Ok(Some(Bson::Array(new_arr)))
            }
            // Field missing or not an array: $pullAll is a no-op.
            other => Ok(other),
        })?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $pop
// ---------------------------------------------------------------------------

fn apply_pop(doc: &mut Document, args: &Document, ctx: &UpdateContext<'_>) -> Result<()> {
    for (path, dir) in args {
        let dir_n = as_f64(dir)
            .ok_or_else(|| Error::Internal(format!("$pop: value must be 1 or -1, got {dir:?}")))?;

        modify_nested(doc, path, ctx, &mut |current| match current {
            Some(Bson::Array(mut arr)) => {
                if !arr.is_empty() {
                    if dir_n >= 1.0 {
                        arr.pop(); // remove last
                    } else {
                        arr.remove(0); // remove first
                    }
                }
                Ok(Some(Bson::Array(arr)))
            }
            // Non-existent or non-array fields are silently ignored.
            other => Ok(other),
        })?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// $bit
// ---------------------------------------------------------------------------

/// Kind of bitwise operation for `$bit`.
enum BitOpKind {
    And,
    Or,
    Xor,
}

/// A single step in a `$bit` operation sequence.
struct BitOp {
    kind: BitOpKind,
    /// Operand widened to `i64` for uniform arithmetic; see `is_i64`.
    operand: i64,
    /// `true` when the original BSON operand was `Int64` (forces `Int64` result).
    is_i64: bool,
}

/// Parse and validate the per-field `$bit` operation document.
///
/// Returns the ordered list of operations to apply.
///
/// # Errors
///
/// - [`Error::Internal`] when the document is empty, contains an unknown key,
///   or a value that is not `Int32`/`Int64`.
fn parse_bit_ops(ops_doc: &Document) -> Result<Vec<BitOp>> {
    if ops_doc.is_empty() {
        return Err(Error::Internal(
            "You must pass in at least one bit operation".to_owned(),
        ));
    }

    let mut ops = Vec::with_capacity(ops_doc.len());
    for (key, val) in ops_doc {
        let kind = match key.as_str() {
            "and" => BitOpKind::And,
            "or" => BitOpKind::Or,
            "xor" => BitOpKind::Xor,
            other => {
                return Err(Error::Internal(format!(
                    "The $bit modifier only supports 'and', 'or', and 'xor', \
                     not '{other}' which is an unknown operator: {{{other}: {val:?}}}"
                )));
            }
        };
        let (operand, is_i64) = match val {
            Bson::Int32(n) => (*n as i64, false),
            Bson::Int64(n) => (*n, true),
            _ => {
                return Err(Error::Internal(format!(
                    "The $bit modifier field must be an Integer(s): \
                     {{{key}: {val:?}}}"
                )));
            }
        };
        ops.push(BitOp { kind, operand, is_i64 });
    }
    Ok(ops)
}

/// Apply a sequence of `$bit` operations to `current`, returning the new value.
///
/// A missing field is treated as `0` (created as `Int32` unless any operand
/// is `Int64`).  An existing field that is neither `Int32` nor `Int64` is an
/// error.
///
/// Width rule: `Int32` op `Int32` → `Int32`; any `Int64` involved → `Int64`.
///
/// # Errors
///
/// [`Error::Internal`] when the existing field value is not an integer type.
fn apply_bit_ops(current: Option<&Bson>, ops: &[BitOp]) -> Result<Bson> {
    let (mut acc, mut result_is_i64) = match current {
        None => (0_i64, false),
        Some(Bson::Int32(n)) => (*n as i64, false),
        Some(Bson::Int64(n)) => (*n, true),
        Some(other) => {
            return Err(Error::Internal(format!(
                "Cannot apply $bit to a value of non-integral type. \
                 Current value: {other:?}"
            )));
        }
    };

    for op in ops {
        if op.is_i64 {
            result_is_i64 = true;
        }
        acc = match op.kind {
            BitOpKind::And => acc & op.operand,
            BitOpKind::Or => acc | op.operand,
            BitOpKind::Xor => acc ^ op.operand,
        };
    }

    if result_is_i64 {
        Ok(Bson::Int64(acc))
    } else {
        Ok(Bson::Int32(acc as i32))
    }
}

/// Apply the `$bit` update operator.
///
/// Syntax: `{ $bit: { <field>: { and|or|xor: <Int32|Int64> }, ... } }`
///
/// Multiple operations in the per-field document are applied in document order.
/// A missing target field is treated as `0`.  Supports dotted paths and `$[]`.
///
/// # Errors
///
/// - [`Error::Internal`] for unknown operation keys, non-integer operands,
///   non-integer existing field values, or an empty per-field document.
fn apply_bit(doc: &mut Document, args: &Document, ctx: &UpdateContext<'_>) -> Result<()> {
    for (path, spec) in args {
        let ops_doc = spec.as_document().ok_or_else(|| {
            Error::Internal(format!(
                "$bit: argument for '{path}' must be a document, got {spec:?}"
            ))
        })?;
        let ops = parse_bit_ops(ops_doc)?;

        modify_nested(doc, path, ctx, &mut |current| {
            Ok(Some(apply_bit_ops(current.as_ref(), &ops)?))
        })?;
    }
    Ok(())
}

/// Returns `true` if `path` contains the all-positional `$[]` segment or
/// the unsupported positional `$` / filtered-positional `$[<identifier>]`
/// segments.
///
/// Used by `$rename` to reject paths that traverse array elements.
fn path_has_positional(path: &str) -> bool {
    path.split('.').any(|seg| !matches!(classify_segment(seg), Segment::Field(_)))
}

// ---------------------------------------------------------------------------
// $currentDate
// ---------------------------------------------------------------------------

fn apply_current_date(doc: &mut Document, args: &Document, ctx: &UpdateContext<'_>) -> Result<()> {
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
        set_nested(doc, path, value, ctx)?;
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

        // `set_nested` protects top-level `_id`; adopt the filter's equality
        // `_id` directly so upserts insert at that `_id` (MongoDB semantics).
        if key == "_id" {
            base.insert("_id", value.clone());
            continue;
        }

        // Errors here are impossible (plain equality keys contain no `$`
        // segments) — ignore rather than propagate to keep this infallible.
        let _ = set_plain_path(&mut base, key, value.clone());
    }

    base
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
