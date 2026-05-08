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
