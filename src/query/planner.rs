//! Query planner: index selection for query execution.
//!
//! The planner analyses a filter document and the collection's available indexes
//! to choose the best execution strategy.  The planner produces a [`ScanPlan`]
//! that tells the engine whether to do a full collection scan or an
//! index-accelerated scan.
//!
//! ## Index selection rules
//!
//! 1. Check whether the filter admits a direct lookup on the implicit primary
//!    `_id` key.
//! 2. Otherwise iterate available indexes in definition order.
//! 3. For each index, check whether the **leftmost prefix key** appears in the
//!    filter with an index-eligible operator.
//! 4. The first matching access path wins.
//! 5. If no access path matches, fall back to [`ScanPlan::CollScan`].
//!
//! ## Index-eligible operators (per field)
//!
//! | Operator | Notes |
//! |----------|-------|
//! | `{field: value}` / `$eq` | Equality point lookup |
//! | `$gt`, `$gte`, `$lt`, `$lte` | Range scan |
//! | `$in` | Multi-point lookup |
//! | `$all` | Multikey: any array element check |
//! | `$elemMatch` | Multikey: sub-document match |
//! | `$exists: true` | Field-presence lookup |
//! | `$regex` | Prefix-range (partial acceleration) |
//!
//! ## Non-eligible operators (force COLLSCAN)
//!
//! `$ne`, `$nin`, `$not` and `$exists: false` cannot be efficiently bounded
//! with a B+ tree range, so the planner falls back to a full collection scan.
//!
//! ## Index hints
//!
//! [`select_plan_with_hint`] accepts an optional [`crate::options::Hint`] that
//! OVERRIDES the heuristics above: a resolved hint forces its index (or the
//! primary `_id` path), `{ $natural: 1 }` forces a collection scan, and an
//! unresolvable hint is a `BadValue` error.

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::options::Hint;

/// The reserved hint key pattern field that forces a collection scan.
const NATURAL_KEY: &str = "$natural";

/// The implicit primary-index name used by `_id` hints.
const ID_INDEX_NAME: &str = "_id_";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Lightweight index descriptor passed to the planner.
pub(crate) struct IndexMeta<'a> {
    /// The index name (e.g. `"email_1"`).
    pub(crate) name: &'a str,
    /// The index key pattern (e.g. `doc! { "email": 1 }`).
    pub(crate) keys: &'a Document,
    /// Partial-index filter expression, if this is a partial index. The
    /// planner only selects a partial index when the query filter
    /// syntactically covers this expression. `None` for ordinary indexes.
    pub(crate) pfe: Option<&'a Document>,
}

/// An execution plan returned by [`select_plan`].
#[allow(clippy::large_enum_variant)]
pub(crate) enum ScanPlan {
    /// Full collection scan — no suitable index found.
    CollScan,
    /// Direct lookup on the implicit primary `_id` key.
    PrimaryKeyLookup {
        /// Extracted `_id` predicate for the lookup.
        condition: PrimaryKeyCondition,
    },
    /// Index-accelerated scan.
    IndexScan {
        /// Name of the selected index (used in [`ExplainResult`]).
        index_name: String,
        /// The leftmost key field of the selected index.
        primary_field: String,
        /// Pre-filter condition extracted from the query filter.
        condition: IndexCondition,
    },
}

/// A simplified index-level condition for pre-filtering documents during an
/// index scan.
///
/// This condition is evaluated on the indexed field before the full query
/// predicate is applied.  It is intentionally **permissive** — false negatives
/// are forbidden (every document matched by the full filter must pass the
/// `IndexCondition` check), but false positives are acceptable (the full
/// filter handles the final rejection).
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum IndexCondition {
    /// Point equality: `{field: val}` or `{field: {$eq: val}}`.
    Eq(Bson),
    /// Range: any combination of `$gt`, `$gte`, `$lt`, `$lte`.
    Range {
        gt: Option<Bson>,
        gte: Option<Bson>,
        lt: Option<Bson>,
        lte: Option<Bson>,
    },
    /// Multi-point: `{field: {$in: [...]}}`.
    In(Vec<Bson>),
    /// Presence/any-match: `$exists: true`, `$all`, `$elemMatch`, `$regex`.
    Any,
}

/// Primary-key predicates the engine can execute directly against the data tree.
#[derive(Debug, Clone)]
pub(crate) enum PrimaryKeyCondition {
    /// Point equality: `{_id: val}` or `{_id: {$eq: val}}`.
    Eq(Bson),
    /// Multi-point lookup: `{_id: {$in: [...]}}`.
    In(Vec<Bson>),
}

// ---------------------------------------------------------------------------
// Plan selection
// ---------------------------------------------------------------------------

/// Select the best scan plan for `filter` given the available `indexes`.
///
/// Returns the best available access path, preferring the implicit primary
/// `_id` key over secondary indexes, or
/// [`ScanPlan::CollScan`] if none qualifies.
pub(crate) fn select_plan(filter: &Document, indexes: &[IndexMeta<'_>]) -> ScanPlan {
    if let Some(condition) = extract_primary_key_condition(filter) {
        return ScanPlan::PrimaryKeyLookup { condition };
    }

    for idx in indexes {
        let Some((first_field, _)) = idx.keys.iter().next() else {
            continue;
        };

        // Partial index: only eligible when the query filter syntactically
        // covers the index's partial filter expression.
        if !filter_covers_pfe(filter, idx.pfe) {
            continue;
        }

        if let Some(condition) = extract_field_condition(filter, first_field) {
            return ScanPlan::IndexScan {
                index_name: idx.name.to_owned(),
                primary_field: first_field.clone(),
                condition,
            };
        }
    }

    ScanPlan::CollScan
}

/// Whether `filter` syntactically covers a partial index's filter expression.
///
/// Returns `true` unconditionally for a non-partial index (`pfe == None`). For
/// a partial index, every top-level `(key, condition)` pair in the PFE must
/// appear in `filter` with an identical key AND a `Bson`-equal condition value.
///
/// This is a deliberately CONSERVATIVE subsumption rule — strictly weaker than
/// MongoDB's logical-implication test. For example a PFE `{qty: {$gt: 10}}` is
/// NOT covered by a query `{qty: {$gt: 50}}` even though the query result set
/// is a subset, because the conditions are not syntactically identical.
fn filter_covers_pfe(filter: &Document, pfe: Option<&Document>) -> bool {
    let Some(pfe) = pfe else {
        return true;
    };
    pfe.iter().all(|(key, condition)| {
        filter.get(key).is_some_and(|q| q == condition)
    })
}

/// Select a scan plan honouring an optional index `hint`.
///
/// When `hint` is `None` this is exactly [`select_plan`]. When a hint is
/// supplied it OVERRIDES the normal cost-free heuristics:
///
/// - `Keys({$natural: 1})` / `Keys({$natural: -1})` forces a
///   [`ScanPlan::CollScan`], suppressing all index selection. The engine has
///   no reverse scan, so `-1` is treated as a FORWARD natural scan — a known
///   divergence from MongoDB, which would return documents in reverse.
/// - A hint that resolves to an existing index forces that index's
///   [`ScanPlan::IndexScan`] regardless of whether another index looks
///   "better". The leading key field's filter predicate supplies the scan
///   bounds; when the filter constrains nothing on that field the plan uses
///   [`IndexCondition::Any`], which the executor turns into an UNBOUNDED full
///   index scan (`index_bounds_free` yields `(None, None)` →
///   `Bound::Unbounded`). No collection-scan fallback is needed for that case.
/// - An `_id` hint (`{_id: 1}` or `"_id_"`) forces the primary `_id` path when
///   the filter carries an `_id` equality / `$in` predicate
///   ([`ScanPlan::PrimaryKeyLookup`]). The engine exposes no UNBOUNDED primary
///   scan, so an `_id` hint with no usable `_id` bound degrades to a
///   [`ScanPlan::CollScan`] — a divergence reported by the hint path.
///
/// # Errors
///
/// Returns [`Error::InvalidQuery`] (MongoDB `BadValue`) when the hint names an
/// index or key pattern that does not correspond to any existing index.
pub(crate) fn select_plan_with_hint(
    filter: &Document,
    indexes: &[IndexMeta<'_>],
    hint: Option<&Hint>,
) -> Result<ScanPlan> {
    let Some(hint) = hint else {
        return Ok(select_plan(filter, indexes));
    };

    if hint_is_natural(hint) {
        // $natural suppresses index selection entirely (forward only).
        return Ok(ScanPlan::CollScan);
    }

    if hint_targets_id(hint) {
        return Ok(plan_for_id_hint(filter));
    }

    let Some(idx) = resolve_hinted_index(hint, indexes) else {
        return Err(Error::InvalidQuery {
            detail: "planner returned error :: caused by :: hint provided \
                     does not correspond to an existing index"
                .to_owned(),
        });
    };

    Ok(plan_for_resolved_index(filter, idx))
}

/// Whether `hint` is the reserved `{ $natural: 1 }` / `{ $natural: -1 }`
/// key pattern.
fn hint_is_natural(hint: &Hint) -> bool {
    match hint {
        Hint::Keys(keys) => keys.len() == 1 && keys.contains_key(NATURAL_KEY),
        Hint::Name(_) => false,
    }
}

/// Whether `hint` targets the implicit primary `_id` index — either the name
/// `"_id_"` or the key pattern `{ _id: 1 }` / `{ _id: -1 }`.
fn hint_targets_id(hint: &Hint) -> bool {
    match hint {
        Hint::Name(name) => name == ID_INDEX_NAME,
        Hint::Keys(keys) => keys.len() == 1 && keys.contains_key("_id"),
    }
}

/// Build the plan for an `_id` hint: a primary-key lookup when the filter
/// carries an eligible `_id` predicate, else a collection scan (no unbounded
/// primary scan path exists).
fn plan_for_id_hint(filter: &Document) -> ScanPlan {
    match extract_primary_key_condition(filter) {
        Some(condition) => ScanPlan::PrimaryKeyLookup { condition },
        None => ScanPlan::CollScan,
    }
}

/// Resolve a non-`_id`, non-`$natural` hint against the available secondary
/// indexes. `Name` matches by index name; `Keys` matches by an exact,
/// order-sensitive key-pattern comparison (field names AND directions).
fn resolve_hinted_index<'a>(
    hint: &Hint,
    indexes: &'a [IndexMeta<'a>],
) -> Option<&'a IndexMeta<'a>> {
    match hint {
        Hint::Name(name) => indexes.iter().find(|idx| idx.name == name),
        Hint::Keys(keys) => indexes.iter().find(|idx| key_patterns_match(idx.keys, keys)),
    }
}

/// Exact, order-sensitive comparison of two index key patterns: the same
/// fields in the same order with the same directions.
fn key_patterns_match(a: &Document, b: &Document) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).all(|((af, ad), (bf, bd))| {
        af == bf && key_direction(ad) == key_direction(bd)
    })
}

/// Normalise an index-direction value to `+1` / `-1`. Any non-`-1` value is
/// treated as ascending (matching the executor's `ascending` derivation).
fn key_direction(v: &Bson) -> i32 {
    if matches!(v, Bson::Int32(-1) | Bson::Int64(-1)) {
        -1
    } else {
        1
    }
}

/// Build the forced [`ScanPlan::IndexScan`] for a resolved index. The leading
/// key field's filter predicate supplies the bounds; with no usable predicate
/// the scan is unbounded over the whole index ([`IndexCondition::Any`]).
fn plan_for_resolved_index(filter: &Document, idx: &IndexMeta<'_>) -> ScanPlan {
    let primary_field = idx
        .keys
        .iter()
        .next()
        .map(|(field, _)| field.clone())
        .unwrap_or_default();
    let condition = extract_field_condition(filter, &primary_field)
        .unwrap_or(IndexCondition::Any);
    ScanPlan::IndexScan {
        index_name: idx.name.to_owned(),
        primary_field,
        condition,
    }
}

// ---------------------------------------------------------------------------
// Index eligibility analysis
// ---------------------------------------------------------------------------

/// Try to extract a direct primary-key predicate from `filter`.
///
/// We currently accelerate only equality / `$in` shapes on `_id`. Range and
/// presence predicates still fall back to the general scan planner.
fn extract_primary_key_condition(filter: &Document) -> Option<PrimaryKeyCondition> {
    match extract_field_condition(filter, "_id")? {
        IndexCondition::Eq(val) => Some(PrimaryKeyCondition::Eq(val)),
        IndexCondition::In(vals) => Some(PrimaryKeyCondition::In(vals)),
        IndexCondition::Range { .. } | IndexCondition::Any => None,
    }
}

/// Try to extract an index-eligible condition for `field` from `filter`.
///
/// Returns `None` if the field is absent from the filter or the operator
/// cannot be efficiently evaluated via a B+ tree range.
fn extract_field_condition(filter: &Document, field: &str) -> Option<IndexCondition> {
    let value = filter.get(field)?;
    match value {
        Bson::Document(ops) if ops.keys().any(|k| k.starts_with('$')) => {
            extract_operator_condition(ops)
        }
        // Plain scalar / array / embedded doc → implicit equality.
        other => Some(IndexCondition::Eq(other.clone())),
    }
}

/// Convert a query-operator document into an `IndexCondition`, if eligible.
fn extract_operator_condition(ops: &Document) -> Option<IndexCondition> {
    // Negation operators are not index-eligible.
    if ops.contains_key("$ne") || ops.contains_key("$nin") || ops.contains_key("$not") {
        return None;
    }

    // $exists: false — index contains only present values, so we cannot use
    // it to find documents where the field is absent.
    if matches!(
        ops.get("$exists"),
        Some(Bson::Boolean(false)) | Some(Bson::Int32(0)) | Some(Bson::Int64(0))
    ) {
        return None;
    }

    // Point equality.
    if let Some(val) = ops.get("$eq") {
        return Some(IndexCondition::Eq(val.clone()));
    }

    // Multi-point lookup.
    if let Some(Bson::Array(vals)) = ops.get("$in") {
        return Some(IndexCondition::In(vals.clone()));
    }

    // Range: one or more of $gt/$gte/$lt/$lte.
    let gt = ops.get("$gt").cloned();
    let gte = ops.get("$gte").cloned();
    let lt = ops.get("$lt").cloned();
    let lte = ops.get("$lte").cloned();
    if gt.is_some() || gte.is_some() || lt.is_some() || lte.is_some() {
        return Some(IndexCondition::Range { gt, gte, lt, lte });
    }

    // $exists: true — field must be present.
    if matches!(
        ops.get("$exists"),
        Some(Bson::Boolean(true)) | Some(Bson::Int32(1)) | Some(Bson::Int64(1))
    ) {
        return Some(IndexCondition::Any);
    }

    // $all, $elemMatch, $regex — presence is a necessary (not sufficient) condition.
    if ops.contains_key("$all") || ops.contains_key("$elemMatch") || ops.contains_key("$regex") {
        return Some(IndexCondition::Any);
    }

    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/planner.rs"]
mod tests;
