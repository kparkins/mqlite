use super::*;
use bson::doc;

fn make_indexes<'a>(specs: &'a [(&'a str, Document)]) -> Vec<IndexMeta<'a>> {
    specs
        .iter()
        .map(|(name, keys)| IndexMeta {
            name,
            keys,
            pfe: None,
        })
        .collect()
}

/// Build index metas where each spec may carry a partial filter expression.
fn make_partial_indexes<'a>(
    specs: &'a [(&'a str, Document, Option<Document>)],
) -> Vec<IndexMeta<'a>> {
    specs
        .iter()
        .map(|(name, keys, pfe)| IndexMeta {
            name,
            keys,
            pfe: pfe.as_ref(),
        })
        .collect()
}

// ---- select_plan -------------------------------------------------------

#[test]
fn collscan_when_no_indexes() {
    let plan = select_plan(&doc! { "age": 25i32 }, &[]);
    assert!(matches!(plan, ScanPlan::CollScan));
}

#[test]
fn primary_lookup_for_id_equality_without_indexes() {
    let plan = select_plan(&doc! { "_id": 7i32 }, &[]);
    match plan {
        ScanPlan::PrimaryKeyLookup {
            condition: PrimaryKeyCondition::Eq(Bson::Int32(7)),
        } => {}
        _ => panic!("expected primary-key equality lookup"),
    }
}

#[test]
fn primary_lookup_for_id_in_without_indexes() {
    let plan = select_plan(&doc! { "_id": { "$in": [1i32, 2i32] } }, &[]);
    match plan {
        ScanPlan::PrimaryKeyLookup {
            condition: PrimaryKeyCondition::In(vals),
        } => {
            assert_eq!(vals, vec![Bson::Int32(1), Bson::Int32(2)]);
        }
        _ => panic!("expected primary-key $in lookup"),
    }
}

#[test]
fn collscan_for_id_range_without_indexes() {
    let plan = select_plan(&doc! { "_id": { "$gt": 7i32 } }, &[]);
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
fn primary_lookup_beats_secondary_index() {
    let specs = [("email_1", doc! { "email": 1i32 })];
    let indexes = make_indexes(&specs);
    let plan = select_plan(&doc! { "_id": 7i32, "email": "a@b.com" }, &indexes);
    assert!(matches!(plan, ScanPlan::PrimaryKeyLookup { .. }));
}

#[test]
fn ixscan_for_equality_on_indexed_field() {
    let specs = [("email_1", doc! { "email": 1i32 })];
    let indexes = make_indexes(&specs);
    let plan = select_plan(&doc! { "email": "a@b.com" }, &indexes);
    match plan {
        ScanPlan::IndexScan { ref index_name, .. } => assert_eq!(index_name, "email_1"),
        ScanPlan::PrimaryKeyLookup { .. } | ScanPlan::CollScan => panic!("expected IXSCAN"),
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
        ScanPlan::PrimaryKeyLookup { .. } | ScanPlan::CollScan => panic!("expected IXSCAN"),
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

// ---- select_plan_with_hint ----------------------------------------------

#[test]
fn hint_none_matches_select_plan() {
    let specs = [("email_1", doc! { "email": 1i32 })];
    let indexes = make_indexes(&specs);
    let plan =
        select_plan_with_hint(&doc! { "email": "a@b.com" }, &indexes, None).unwrap();
    match plan {
        ScanPlan::IndexScan { ref index_name, .. } => assert_eq!(index_name, "email_1"),
        _ => panic!("expected IXSCAN"),
    }
}

#[test]
fn hint_by_name_selects_index() {
    let specs = [("email_1", doc! { "email": 1i32 })];
    let indexes = make_indexes(&specs);
    let hint = Hint::Name("email_1".to_owned());
    let plan =
        select_plan_with_hint(&doc! { "email": "a@b.com" }, &indexes, Some(&hint)).unwrap();
    match plan {
        ScanPlan::IndexScan { ref index_name, .. } => assert_eq!(index_name, "email_1"),
        _ => panic!("expected IXSCAN via name hint"),
    }
}

#[test]
fn hint_by_keys_selects_index() {
    let specs = [("email_1", doc! { "email": 1i32 })];
    let indexes = make_indexes(&specs);
    let hint = Hint::Keys(doc! { "email": 1i32 });
    let plan =
        select_plan_with_hint(&doc! { "email": "a@b.com" }, &indexes, Some(&hint)).unwrap();
    match plan {
        ScanPlan::IndexScan { ref index_name, .. } => assert_eq!(index_name, "email_1"),
        _ => panic!("expected IXSCAN via keys hint"),
    }
}

#[test]
fn hint_keys_direction_must_match() {
    let specs = [("email_1", doc! { "email": 1i32 })];
    let indexes = make_indexes(&specs);
    // Asking for descending key pattern when only ascending exists.
    let hint = Hint::Keys(doc! { "email": -1i32 });
    let err = select_plan_with_hint(&doc! { "email": "a@b.com" }, &indexes, Some(&hint));
    assert!(err.is_err(), "descending key pattern must not match ascending index");
}

#[test]
fn hint_overrides_better_index() {
    // Filter has an exact equality on `email` (would normally pick email_1),
    // but the hint forces the unrelated `age_1` index. Override must win, and
    // because the filter has no bound on `age` the scan is unbounded (Any).
    let specs = [
        ("email_1", doc! { "email": 1i32 }),
        ("age_1", doc! { "age": 1i32 }),
    ];
    let indexes = make_indexes(&specs);
    let hint = Hint::Name("age_1".to_owned());
    let plan =
        select_plan_with_hint(&doc! { "email": "a@b.com" }, &indexes, Some(&hint)).unwrap();
    match plan {
        ScanPlan::IndexScan {
            ref index_name,
            ref primary_field,
            ref condition,
        } => {
            assert_eq!(index_name, "age_1");
            assert_eq!(primary_field, "age");
            assert!(matches!(condition, IndexCondition::Any));
        }
        _ => panic!("hint must override the heuristically-better index"),
    }
}

#[test]
fn hint_with_bounds_uses_filter_condition() {
    let specs = [("age_1", doc! { "age": 1i32 })];
    let indexes = make_indexes(&specs);
    let hint = Hint::Name("age_1".to_owned());
    let plan = select_plan_with_hint(
        &doc! { "age": { "$gt": 18i32 } },
        &indexes,
        Some(&hint),
    )
    .unwrap();
    match plan {
        ScanPlan::IndexScan { ref condition, .. } => {
            assert!(matches!(condition, IndexCondition::Range { .. }));
        }
        _ => panic!("expected bounded IXSCAN from hint + filter"),
    }
}

#[test]
fn bad_hint_name_errors() {
    let specs = [("email_1", doc! { "email": 1i32 })];
    let indexes = make_indexes(&specs);
    let hint = Hint::Name("nonexistent_1".to_owned());
    let result = select_plan_with_hint(&doc! { "email": "a@b.com" }, &indexes, Some(&hint));
    let msg = match result {
        Err(e) => e.to_string(),
        Ok(_) => panic!("bad hint name must error"),
    };
    assert!(
        msg.contains("hint provided does not correspond to an existing index"),
        "unexpected error message: {msg}"
    );
}

#[test]
fn bad_hint_keys_errors() {
    let specs = [("email_1", doc! { "email": 1i32 })];
    let indexes = make_indexes(&specs);
    let hint = Hint::Keys(doc! { "nope": 1i32 });
    let err = select_plan_with_hint(&doc! { "email": "a@b.com" }, &indexes, Some(&hint));
    assert!(err.is_err());
}

#[test]
fn natural_hint_forces_collscan() {
    let specs = [("email_1", doc! { "email": 1i32 })];
    let indexes = make_indexes(&specs);
    let hint = Hint::Keys(doc! { "$natural": 1i32 });
    // Even with an exact equality match on an indexed field, $natural wins.
    let plan =
        select_plan_with_hint(&doc! { "email": "a@b.com" }, &indexes, Some(&hint)).unwrap();
    assert!(matches!(plan, ScanPlan::CollScan));
}

#[test]
fn natural_hint_negative_one_forces_forward_collscan() {
    // Divergence: the engine has no reverse scan, so $natural: -1 is treated
    // as a FORWARD collection scan rather than reverse-order traversal.
    let specs = [("email_1", doc! { "email": 1i32 })];
    let indexes = make_indexes(&specs);
    let hint = Hint::Keys(doc! { "$natural": -1i32 });
    let plan =
        select_plan_with_hint(&doc! { "email": "a@b.com" }, &indexes, Some(&hint)).unwrap();
    assert!(matches!(plan, ScanPlan::CollScan));
}

#[test]
fn id_hint_by_name_with_eq_uses_primary_lookup() {
    let specs = [("email_1", doc! { "email": 1i32 })];
    let indexes = make_indexes(&specs);
    let hint = Hint::Name("_id_".to_owned());
    let plan = select_plan_with_hint(&doc! { "_id": 7i32 }, &indexes, Some(&hint)).unwrap();
    assert!(matches!(plan, ScanPlan::PrimaryKeyLookup { .. }));
}

#[test]
fn id_hint_by_keys_with_eq_uses_primary_lookup() {
    let indexes: Vec<IndexMeta<'_>> = Vec::new();
    let hint = Hint::Keys(doc! { "_id": 1i32 });
    let plan = select_plan_with_hint(&doc! { "_id": 7i32 }, &indexes, Some(&hint)).unwrap();
    assert!(matches!(plan, ScanPlan::PrimaryKeyLookup { .. }));
}

#[test]
fn id_hint_without_bound_degrades_to_collscan() {
    // Divergence: no unbounded primary-scan path exists, so an `_id` hint with
    // no usable `_id` predicate degrades to a collection scan.
    let specs = [("email_1", doc! { "email": 1i32 })];
    let indexes = make_indexes(&specs);
    let hint = Hint::Name("_id_".to_owned());
    let plan =
        select_plan_with_hint(&doc! { "email": "a@b.com" }, &indexes, Some(&hint)).unwrap();
    assert!(matches!(plan, ScanPlan::CollScan));
}

// ---- partial-index selection (conservative subsumption) ----------------

#[test]
fn partial_index_selected_when_filter_covers_pfe() {
    // PFE {qty: {$gt: 10}}; query carries the identical condition plus the
    // extra leading-field predicate, so the partial index is eligible.
    let specs = [(
        "qty_1",
        doc! { "qty": 1i32 },
        Some(doc! { "qty": { "$gt": 10i32 } }),
    )];
    let indexes = make_partial_indexes(&specs);
    let plan = select_plan(&doc! { "qty": { "$gt": 10i32 } }, &indexes);
    match plan {
        ScanPlan::IndexScan { ref index_name, .. } => assert_eq!(index_name, "qty_1"),
        _ => panic!("expected partial index to be selected"),
    }
}

#[test]
fn partial_index_skipped_when_filter_does_not_cover_pfe() {
    // PFE {status: "active"}; query is on a different value, so the index is
    // skipped and the planner falls back to a collection scan.
    let specs = [(
        "status_1",
        doc! { "status": 1i32 },
        Some(doc! { "status": "active" }),
    )];
    let indexes = make_partial_indexes(&specs);
    let plan = select_plan(&doc! { "status": "inactive" }, &indexes);
    assert!(matches!(plan, ScanPlan::CollScan));
}

#[test]
fn partial_index_requires_exact_syntactic_condition_match() {
    // Conservative rule: PFE {qty: {$gt: 10}} is NOT covered by a strictly
    // narrower query {qty: {$gt: 50}}, even though the result set is a subset.
    let specs = [(
        "qty_1",
        doc! { "qty": 1i32 },
        Some(doc! { "qty": { "$gt": 10i32 } }),
    )];
    let indexes = make_partial_indexes(&specs);
    let plan = select_plan(&doc! { "qty": { "$gt": 50i32 } }, &indexes);
    assert!(matches!(plan, ScanPlan::CollScan));
}

#[test]
fn uncovered_partial_index_falls_through_to_other_index() {
    // Two indexes on the same field: a partial one whose PFE is NOT covered and
    // a plain one. The planner skips the partial and selects the plain index.
    let specs = [
        (
            "qty_partial",
            doc! { "qty": 1i32 },
            Some(doc! { "qty": { "$gt": 10i32 } }),
        ),
        ("qty_plain", doc! { "qty": 1i32 }, None),
    ];
    let indexes = make_partial_indexes(&specs);
    let plan = select_plan(&doc! { "qty": { "$gt": 50i32 } }, &indexes);
    match plan {
        ScanPlan::IndexScan { ref index_name, .. } => assert_eq!(index_name, "qty_plain"),
        _ => panic!("expected fallback to the plain index"),
    }
}

#[test]
fn partial_index_covered_with_multikey_pfe_keys() {
    // A PFE with two top-level pairs is covered only when both appear
    // identically in the query filter.
    let pfe = doc! { "a": { "$exists": true }, "b": 1i32 };
    let specs = [("ab_partial", doc! { "a": 1i32 }, Some(pfe))];
    let indexes = make_partial_indexes(&specs);

    // Missing the `b` pair -> not covered.
    let plan = select_plan(&doc! { "a": { "$exists": true } }, &indexes);
    assert!(matches!(plan, ScanPlan::CollScan));

    // Both pairs present and identical -> selected.
    let plan = select_plan(
        &doc! { "a": { "$exists": true }, "b": 1i32 },
        &indexes,
    );
    assert!(matches!(plan, ScanPlan::IndexScan { .. }));
}

#[test]
fn hint_selects_partial_index_even_when_pfe_uncovered() {
    // Decision: an explicit hint naming a partial index honors the user's
    // intent and selects it regardless of PFE coverage (matches MongoDB's
    // ambiguous behavior toward selecting the hinted index).
    let specs = [(
        "qty_partial",
        doc! { "qty": 1i32 },
        Some(doc! { "qty": { "$gt": 10i32 } }),
    )];
    let indexes = make_partial_indexes(&specs);
    let hint = Hint::Name("qty_partial".to_owned());
    let plan =
        select_plan_with_hint(&doc! { "qty": { "$gt": 50i32 } }, &indexes, Some(&hint)).unwrap();
    match plan {
        ScanPlan::IndexScan { ref index_name, .. } => assert_eq!(index_name, "qty_partial"),
        _ => panic!("expected the hinted partial index to be selected"),
    }
}
