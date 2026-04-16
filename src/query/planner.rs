//! Query planner: index selection for query execution.
//!
//! The planner analyses a filter document and the collection's available indexes
//! to choose the best execution strategy.  In Phase 1b (in-memory storage) the
//! planner produces a [`ScanPlan`] that tells the engine whether to do a full
//! collection scan or an index-accelerated scan.
//!
//! ## Index selection rules
//!
//! 1. Iterate available indexes in definition order.
//! 2. For each index, check whether the **leftmost prefix key** appears in the
//!    filter with an index-eligible operator.
//! 3. The first matching index wins (MongoDB-style "first usable index").
//! 4. If no index matches, fall back to [`ScanPlan::CollScan`].
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
pub(crate) enum ScanPlan {
    /// Full collection scan — no suitable index found.
    CollScan,
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

// ---------------------------------------------------------------------------
// Plan selection
// ---------------------------------------------------------------------------

/// Select the best scan plan for `filter` given the available `indexes`.
///
/// Returns the first index that can accelerate the query, or
/// [`ScanPlan::CollScan`] if none qualifies.
pub(crate) fn select_plan(filter: &Document, indexes: &[IndexMeta<'_>]) -> ScanPlan {
    for idx in indexes {
        if let Some((primary_field, condition)) = index_can_accelerate(filter, idx.keys) {
            return ScanPlan::IndexScan {
                index_name: idx.name.to_owned(),
                primary_field,
                condition,
            };
        }
    }
    ScanPlan::CollScan
}

// ---------------------------------------------------------------------------
// Index eligibility analysis
// ---------------------------------------------------------------------------

/// Check whether `index_keys` can accelerate `filter`.
///
/// Returns `Some((field, condition))` if the leftmost index key appears in
/// `filter` with an index-eligible operator; `None` otherwise.
fn index_can_accelerate(
    filter: &Document,
    index_keys: &Document,
) -> Option<(String, IndexCondition)> {
    let (first_field, _) = index_keys.iter().next()?;
    extract_field_condition(filter, first_field.as_str()).map(|cond| (first_field.clone(), cond))
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
    match ops.get("$exists") {
        Some(Bson::Boolean(false)) | Some(Bson::Int32(0)) | Some(Bson::Int64(0)) => {
            return None;
        }
        _ => {}
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
    let has_range = ops.contains_key("$gt")
        || ops.contains_key("$gte")
        || ops.contains_key("$lt")
        || ops.contains_key("$lte");
    if has_range {
        return Some(IndexCondition::Range {
            gt: ops.get("$gt").cloned(),
            gte: ops.get("$gte").cloned(),
            lt: ops.get("$lt").cloned(),
            lte: ops.get("$lte").cloned(),
        });
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
mod tests {
    use super::*;
    use bson::doc;

    fn make_indexes<'a>(specs: &'a [(&'a str, Document)]) -> Vec<IndexMeta<'a>> {
        specs
            .iter()
            .map(|(name, keys)| IndexMeta { name, keys })
            .collect()
    }

    // ---- select_plan -------------------------------------------------------

    #[test]
    fn collscan_when_no_indexes() {
        let plan = select_plan(&doc! { "age": 25i32 }, &[]);
        assert!(matches!(plan, ScanPlan::CollScan));
    }

    #[test]
    fn collscan_when_filter_field_not_indexed() {
        let specs = [("age_1", doc! { "age": 1i32 })];
        let indexes = make_indexes(&specs);
        // Filter on "name" but only "age" is indexed.
        let plan = select_plan(&doc! { "name": "Alice" }, &indexes);
        assert!(matches!(plan, ScanPlan::CollScan));
    }

    #[test]
    fn ixscan_for_equality_on_indexed_field() {
        let specs = [("email_1", doc! { "email": 1i32 })];
        let indexes = make_indexes(&specs);
        let plan = select_plan(&doc! { "email": "a@b.com" }, &indexes);
        match plan {
            ScanPlan::IndexScan { ref index_name, .. } => assert_eq!(index_name, "email_1"),
            ScanPlan::CollScan => panic!("expected IXSCAN"),
        }
    }

    #[test]
    fn ixscan_for_range_on_indexed_field() {
        let specs = [("age_1", doc! { "age": 1i32 })];
        let indexes = make_indexes(&specs);
        let plan = select_plan(&doc! { "age": { "$gt": 18i32 } }, &indexes);
        assert!(matches!(plan, ScanPlan::IndexScan { .. }));
    }

    #[test]
    fn ixscan_for_in_operator() {
        let specs = [("status_1", doc! { "status": 1i32 })];
        let indexes = make_indexes(&specs);
        let plan = select_plan(
            &doc! { "status": { "$in": ["pending", "active"] } },
            &indexes,
        );
        assert!(matches!(plan, ScanPlan::IndexScan { .. }));
    }

    #[test]
    fn collscan_for_ne_operator() {
        let specs = [("age_1", doc! { "age": 1i32 })];
        let indexes = make_indexes(&specs);
        let plan = select_plan(&doc! { "age": { "$ne": 0i32 } }, &indexes);
        assert!(matches!(plan, ScanPlan::CollScan));
    }

    #[test]
    fn collscan_for_nin_operator() {
        let specs = [("status_1", doc! { "status": 1i32 })];
        let indexes = make_indexes(&specs);
        let plan = select_plan(&doc! { "status": { "$nin": ["deleted"] } }, &indexes);
        assert!(matches!(plan, ScanPlan::CollScan));
    }

    #[test]
    fn compound_index_leftmost_prefix_used() {
        let specs = [("name_1_age_1", doc! { "name": 1i32, "age": 1i32 })];
        let indexes = make_indexes(&specs);
        // Filter on both fields — leftmost prefix "name" is present.
        let plan = select_plan(&doc! { "name": "Alice", "age": 30i32 }, &indexes);
        match plan {
            ScanPlan::IndexScan {
                ref index_name,
                ref primary_field,
                ..
            } => {
                assert_eq!(index_name, "name_1_age_1");
                assert_eq!(primary_field, "name");
            }
            ScanPlan::CollScan => panic!("expected IXSCAN"),
        }
    }

    #[test]
    fn compound_index_non_prefix_not_used() {
        let specs = [("name_1_age_1", doc! { "name": 1i32, "age": 1i32 })];
        let indexes = make_indexes(&specs);
        // Only filtering on "age" (second field) — leftmost "name" is absent.
        let plan = select_plan(&doc! { "age": 30i32 }, &indexes);
        assert!(matches!(plan, ScanPlan::CollScan));
    }

    #[test]
    fn ixscan_for_elematch_operator() {
        let specs = [("tags_1", doc! { "tags": 1i32 })];
        let indexes = make_indexes(&specs);
        let plan = select_plan(&doc! { "tags": { "$elemMatch": { "x": 1i32 } } }, &indexes);
        assert!(matches!(plan, ScanPlan::IndexScan { .. }));
    }

    #[test]
    fn ixscan_for_all_operator() {
        let specs = [("tags_1", doc! { "tags": 1i32 })];
        let indexes = make_indexes(&specs);
        let plan = select_plan(&doc! { "tags": { "$all": ["foo", "bar"] } }, &indexes);
        assert!(matches!(plan, ScanPlan::IndexScan { .. }));
    }

    #[test]
    fn ixscan_for_exists_true() {
        let specs = [("email_1", doc! { "email": 1i32 })];
        let indexes = make_indexes(&specs);
        let plan = select_plan(&doc! { "email": { "$exists": true } }, &indexes);
        assert!(matches!(plan, ScanPlan::IndexScan { .. }));
    }

    #[test]
    fn collscan_for_exists_false() {
        let specs = [("email_1", doc! { "email": 1i32 })];
        let indexes = make_indexes(&specs);
        let plan = select_plan(&doc! { "email": { "$exists": false } }, &indexes);
        assert!(matches!(plan, ScanPlan::CollScan));
    }

}
