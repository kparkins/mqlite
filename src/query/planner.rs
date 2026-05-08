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

use bson::{Bson, Document};

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Lightweight index descriptor passed to the planner.
pub(crate) struct IndexMeta<'a> {
    /// The index name (e.g. `"email_1"`).
    pub(crate) name: &'a str,
    /// The index key pattern (e.g. `doc! { "email": 1 }`).
    pub(crate) keys: &'a Document,
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
