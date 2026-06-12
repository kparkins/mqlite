use super::*;
use bson::spec::BinarySubtype;
use bson::{doc, Binary, Bson, DateTime};

/// Build a generic-subtype `BinData` BSON value from raw bytes (test helper).
fn bin_data(bytes: Vec<u8>) -> Bson {
    Bson::Binary(Binary {
        subtype: BinarySubtype::Generic,
        bytes,
    })
}

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

fn matches(filter: Document, doc: Document) -> bool {
    eval_filter(&doc, &filter).expect("eval_filter should not error")
}

fn no_match(filter: Document, doc: Document) -> bool {
    !matches(filter, doc)
}

fn errors(filter: Document, doc: Document) -> bool {
    eval_filter(&doc, &filter).is_err()
}

// -----------------------------------------------------------------------
// Empty filter
// -----------------------------------------------------------------------

#[test]
fn empty_filter_matches_all() {
    assert!(matches(doc! {}, doc! {}));
    assert!(matches(doc! {}, doc! { "a": 1 }));
    assert!(matches(doc! {}, doc! { "x": "hello", "y": [1, 2, 3] }));
}

// -----------------------------------------------------------------------
// Implicit $eq (field: value)
// -----------------------------------------------------------------------

#[test]
fn implicit_eq_scalar() {
    assert!(matches(doc! { "a": 1 }, doc! { "a": 1 }));
    assert!(no_match(doc! { "a": 1 }, doc! { "a": 2 }));
    assert!(no_match(doc! { "a": 1 }, doc! { "b": 1 }));
}

#[test]
fn implicit_eq_cross_type_numeric() {
    // Int32(1) == Double(1.0) == Int64(1) in MongoDB comparison ordering.
    assert!(matches(doc! { "a": 1_i32 }, doc! { "a": 1.0_f64 }));
    assert!(matches(doc! { "a": 1.0_f64 }, doc! { "a": Bson::Int64(1) }));
}

#[test]
fn implicit_eq_null_missing_field() {
    // Missing field matches null.
    assert!(matches(doc! { "a": Bson::Null }, doc! { "b": 1 }));
    assert!(no_match(doc! { "a": 1 }, doc! { "b": 1 }));
}

#[test]
fn implicit_eq_array_element_match() {
    // {a: 2} matches {a: [1, 2, 3]}
    assert!(matches(doc! { "a": 2 }, doc! { "a": [1, 2, 3] }));
    assert!(no_match(doc! { "a": 5 }, doc! { "a": [1, 2, 3] }));
}

#[test]
fn implicit_eq_string() {
    assert!(matches(doc! { "name": "Alice" }, doc! { "name": "Alice" }));
    assert!(no_match(doc! { "name": "Alice" }, doc! { "name": "Bob" }));
}

// -----------------------------------------------------------------------
// $eq
// -----------------------------------------------------------------------

#[test]
fn eq_operator() {
    assert!(matches(doc! { "a": { "$eq": 42 } }, doc! { "a": 42 }));
    assert!(no_match(doc! { "a": { "$eq": 42 } }, doc! { "a": 43 }));
}

#[test]
fn eq_operator_matches_whole_array() {
    assert!(matches(
        doc! { "a": { "$eq": [1, 2] } },
        doc! { "a": [1, 2] }
    ));
    assert!(no_match(
        doc! { "a": { "$eq": [1, 2] } },
        doc! { "a": [2, 1] }
    ));
}

#[test]
fn eq_null_matches_missing_and_null() {
    assert!(matches(doc! { "a": { "$eq": Bson::Null } }, doc! {}));
    assert!(matches(
        doc! { "a": { "$eq": Bson::Null } },
        doc! { "a": Bson::Null }
    ));
    assert!(no_match(
        doc! { "a": { "$eq": Bson::Null } },
        doc! { "a": 0 }
    ));
}

// -----------------------------------------------------------------------
// $ne
// -----------------------------------------------------------------------

#[test]
fn ne_operator() {
    assert!(matches(doc! { "a": { "$ne": 1 } }, doc! { "a": 2 }));
    assert!(no_match(doc! { "a": { "$ne": 1 } }, doc! { "a": 1 }));
    // Missing field: field is "null" equivalent, which equals null.
    // $ne: null → missing field does NOT match $ne: null.
    assert!(no_match(doc! { "a": { "$ne": Bson::Null } }, doc! {}));
}

// -----------------------------------------------------------------------
// $gt, $gte, $lt, $lte
// -----------------------------------------------------------------------

#[test]
fn comparison_operators() {
    assert!(matches(doc! { "age": { "$gt": 18 } }, doc! { "age": 20 }));
    assert!(no_match(doc! { "age": { "$gt": 18 } }, doc! { "age": 18 }));
    assert!(matches(doc! { "age": { "$gte": 18 } }, doc! { "age": 18 }));
    assert!(matches(doc! { "age": { "$lt": 100 } }, doc! { "age": 99 }));
    assert!(no_match(
        doc! { "age": { "$lt": 100 } },
        doc! { "age": 100 }
    ));
    assert!(matches(
        doc! { "age": { "$lte": 100 } },
        doc! { "age": 100 }
    ));
}

#[test]
fn comparison_cross_type_number_vs_string() {
    // Numbers sort before strings in MongoDB ordering.
    // So {a: {$lt: "hello"}} should match {a: 42}.
    assert!(matches(doc! { "a": { "$lt": "hello" } }, doc! { "a": 42 }));
    // And {a: {$gt: 42}} should NOT match {a: "hello"}
    // because string > number in BSON ordering.
    assert!(no_match(doc! { "a": { "$gt": "hello" } }, doc! { "a": 42 }));
}

#[test]
fn comparison_missing_field_no_match() {
    assert!(no_match(doc! { "x": { "$gt": 0 } }, doc! { "y": 5 }));
    assert!(no_match(doc! { "x": { "$lt": 100 } }, doc! { "y": 5 }));
}

#[test]
fn comparison_array_any_element() {
    // {a: {$gt: 3}} matches {a: [1, 2, 4]} because 4 > 3.
    assert!(matches(doc! { "a": { "$gt": 3 } }, doc! { "a": [1, 2, 4] }));
    assert!(no_match(
        doc! { "a": { "$gt": 5 } },
        doc! { "a": [1, 2, 3] }
    ));
}

#[test]
fn comparison_datetime() {
    let t0 = Bson::DateTime(DateTime::from_millis(1000));
    let t1 = Bson::DateTime(DateTime::from_millis(2000));
    let doc_val = doc! { "ts": t1 };
    let filter = doc! { "ts": { "$gt": t0 } };
    assert!(matches(filter, doc_val));
}

// -----------------------------------------------------------------------
// $in / $nin
// -----------------------------------------------------------------------

#[test]
fn in_operator() {
    assert!(matches(
        doc! { "status": { "$in": ["active", "pending"] } },
        doc! { "status": "active" }
    ));
    assert!(no_match(
        doc! { "status": { "$in": ["active", "pending"] } },
        doc! { "status": "closed" }
    ));
}

#[test]
fn in_null_matches_missing() {
    assert!(matches(
        doc! { "a": { "$in": [Bson::Null] } },
        doc! { "b": 1 }
    ));
}

#[test]
fn in_array_element_match() {
    // {tags: {$in: ["rust"]}} matches {tags: ["go", "rust"]}
    assert!(matches(
        doc! { "tags": { "$in": ["rust"] } },
        doc! { "tags": ["go", "rust"] }
    ));
}

#[test]
fn in_operator_matches_whole_array_candidate() {
    assert!(matches(
        doc! { "a": { "$in": [[1, 2]] } },
        doc! { "a": [1, 2] }
    ));
    assert!(no_match(
        doc! { "a": { "$in": [[1, 2]] } },
        doc! { "a": [2, 1] }
    ));
}

#[test]
fn nin_operator() {
    assert!(matches(
        doc! { "status": { "$nin": ["closed", "deleted"] } },
        doc! { "status": "active" }
    ));
    assert!(no_match(
        doc! { "status": { "$nin": ["closed", "deleted"] } },
        doc! { "status": "closed" }
    ));
}

// -----------------------------------------------------------------------
// Implicit $and (multiple conditions on same/different fields)
// -----------------------------------------------------------------------

#[test]
fn implicit_and_multiple_fields() {
    assert!(matches(
        doc! { "a": 1, "b": 2 },
        doc! { "a": 1, "b": 2, "c": 3 }
    ));
    assert!(no_match(doc! { "a": 1, "b": 2 }, doc! { "a": 1, "b": 3 }));
}

#[test]
fn implicit_and_multiple_operators_same_field() {
    // Range query: 5 < a < 10
    assert!(matches(
        doc! { "a": { "$gt": 5, "$lt": 10 } },
        doc! { "a": 7 }
    ));
    assert!(no_match(
        doc! { "a": { "$gt": 5, "$lt": 10 } },
        doc! { "a": 3 }
    ));
    assert!(no_match(
        doc! { "a": { "$gt": 5, "$lt": 10 } },
        doc! { "a": 15 }
    ));
}

// -----------------------------------------------------------------------
// $and
// -----------------------------------------------------------------------

#[test]
fn explicit_and() {
    assert!(matches(
        doc! { "$and": [{ "a": { "$gt": 0 } }, { "b": { "$lt": 100 } }] },
        doc! { "a": 5, "b": 50 }
    ));
    assert!(no_match(
        doc! { "$and": [{ "a": { "$gt": 0 } }, { "b": { "$lt": 100 } }] },
        doc! { "a": 5, "b": 200 }
    ));
}

#[test]
fn and_empty_array_matches_all() {
    assert!(matches(doc! { "$and": [] }, doc! { "a": 1 }));
}

// -----------------------------------------------------------------------
// $or
// -----------------------------------------------------------------------

#[test]
fn or_operator() {
    let filter = doc! {
        "$or": [{ "status": "active" }, { "priority": { "$gt": 5 } }]
    };
    assert!(matches(
        filter.clone(),
        doc! { "status": "active", "priority": 2 }
    ));
    assert!(matches(
        filter.clone(),
        doc! { "status": "closed", "priority": 8 }
    ));
    assert!(no_match(filter, doc! { "status": "closed", "priority": 2 }));
}

#[test]
fn or_empty_array_errors() {
    assert!(errors(doc! { "$or": [] }, doc! { "a": 1 }));
}

// -----------------------------------------------------------------------
// $nor
// -----------------------------------------------------------------------

#[test]
fn nor_operator() {
    let filter = doc! { "$nor": [{ "a": 1 }, { "b": 2 }] };
    assert!(matches(filter.clone(), doc! { "a": 3, "b": 5 }));
    assert!(no_match(filter.clone(), doc! { "a": 1, "b": 5 }));
    assert!(no_match(filter, doc! { "a": 3, "b": 2 }));
}

// -----------------------------------------------------------------------
// $not
// -----------------------------------------------------------------------

#[test]
fn not_operator() {
    // {a: {$not: {$gt: 5}}} matches if a <= 5 OR a is absent.
    let filter = doc! { "a": { "$not": { "$gt": 5 } } };
    assert!(matches(filter.clone(), doc! { "a": 3 }));
    assert!(no_match(filter.clone(), doc! { "a": 7 }));
    // Missing field: $gt would be false, $not makes it true.
    assert!(matches(filter, doc! { "b": 1 }));
}

#[test]
fn not_top_level_errors() {
    assert!(errors(doc! { "$not": { "a": 1 } }, doc! { "a": 1 }));
}

// -----------------------------------------------------------------------
// $exists
// -----------------------------------------------------------------------

#[test]
fn exists_true() {
    let filter = doc! { "a": { "$exists": true } };
    assert!(matches(filter.clone(), doc! { "a": 1 }));
    assert!(no_match(filter, doc! { "b": 1 }));
}

#[test]
fn exists_false() {
    let filter = doc! { "a": { "$exists": false } };
    assert!(matches(filter.clone(), doc! { "b": 1 }));
    assert!(no_match(filter, doc! { "a": 1 }));
}

#[test]
fn exists_null_field() {
    // A field with value null is considered to exist.
    assert!(matches(
        doc! { "a": { "$exists": true } },
        doc! { "a": Bson::Null }
    ));
    assert!(no_match(
        doc! { "a": { "$exists": false } },
        doc! { "a": Bson::Null }
    ));
}

// -----------------------------------------------------------------------
// $type
// -----------------------------------------------------------------------

#[test]
fn type_by_string() {
    assert!(matches(
        doc! { "a": { "$type": "string" } },
        doc! { "a": "hello" }
    ));
    assert!(no_match(
        doc! { "a": { "$type": "string" } },
        doc! { "a": 42 }
    ));
    assert!(matches(
        doc! { "a": { "$type": "int" } },
        doc! { "a": 42_i32 }
    ));
    assert!(matches(
        doc! { "a": { "$type": "long" } },
        doc! { "a": Bson::Int64(42) }
    ));
    assert!(matches(
        doc! { "a": { "$type": "double" } },
        doc! { "a": 1.5 }
    ));
    assert!(matches(
        doc! { "a": { "$type": "bool" } },
        doc! { "a": true }
    ));
    assert!(matches(
        doc! { "a": { "$type": "array" } },
        doc! { "a": [1, 2] }
    ));
    assert!(matches(
        doc! { "a": { "$type": "object" } },
        doc! { "a": { "x": 1 } }
    ));
    assert!(matches(
        doc! { "a": { "$type": "null" } },
        doc! { "a": Bson::Null }
    ));
}

#[test]
fn type_by_numeric_id() {
    // 2 = string
    assert!(matches(
        doc! { "a": { "$type": 2_i32 } },
        doc! { "a": "hello" }
    ));
    // 16 = int32
    assert!(matches(
        doc! { "a": { "$type": 16_i32 } },
        doc! { "a": 42_i32 }
    ));
}

#[test]
fn type_array_of_types() {
    // Match if field is int OR long.
    let filter = doc! { "a": { "$type": [16_i32, 18_i32] } };
    assert!(matches(filter.clone(), doc! { "a": 42_i32 }));
    assert!(matches(filter.clone(), doc! { "a": Bson::Int64(42) }));
    assert!(no_match(filter, doc! { "a": "hello" }));
}

#[test]
fn type_missing_field_no_match() {
    assert!(no_match(
        doc! { "a": { "$type": "string" } },
        doc! { "b": "hello" }
    ));
}

// -----------------------------------------------------------------------
// Nested field access (dot notation)
// -----------------------------------------------------------------------

#[test]
fn dot_notation_simple() {
    let doc = doc! { "user": { "name": "Alice", "age": 30 } };
    assert!(matches(doc! { "user.name": "Alice" }, doc.clone()));
    assert!(matches(doc! { "user.age": { "$gt": 18 } }, doc.clone()));
    assert!(no_match(doc! { "user.name": "Bob" }, doc));
}

#[test]
fn dot_notation_deep() {
    let doc = doc! { "a": { "b": { "c": 42 } } };
    assert!(matches(doc! { "a.b.c": 42 }, doc.clone()));
    assert!(no_match(doc! { "a.b.c": 43 }, doc));
}

#[test]
fn dot_notation_array_index() {
    let doc = doc! { "items": [10, 20, 30] };
    assert!(matches(doc! { "items.0": 10 }, doc.clone()));
    assert!(matches(doc! { "items.1": 20 }, doc.clone()));
    assert!(no_match(doc! { "items.0": 20 }, doc));
}

#[test]
fn dot_notation_missing_intermediate() {
    let doc = doc! { "a": 1 };
    // "a.b" — a is not a document, so no match.
    assert!(no_match(doc! { "a.b": 1 }, doc.clone()));
    // "x.y" — x doesn't exist.
    assert!(matches(doc! { "x.y": Bson::Null }, doc));
}

// -----------------------------------------------------------------------
// Collation rejection
// -----------------------------------------------------------------------

#[test]
fn collation_returns_error() {
    assert!(errors(
        doc! { "collation": { "locale": "en" } },
        doc! { "a": 1 }
    ));
}

// -----------------------------------------------------------------------
// Unsupported operator
// -----------------------------------------------------------------------

#[test]
fn unsupported_operator_errors() {
    assert!(errors(doc! { "a": { "$where": "true" } }, doc! { "a": 1 }));
    assert!(errors(doc! { "$where": "true" }, doc! { "a": 1 }));
}

// -----------------------------------------------------------------------
// Combined / real-world queries
// -----------------------------------------------------------------------

#[test]
fn combined_and_or() {
    // (status == "active" OR priority > 5) AND age >= 18
    let filter = doc! {
        "$or": [{ "status": "active" }, { "priority": { "$gt": 5 } }],
        "age": { "$gte": 18 }
    };
    assert!(matches(
        filter.clone(),
        doc! { "status": "active", "priority": 1, "age": 25 }
    ));
    assert!(matches(
        filter.clone(),
        doc! { "status": "closed", "priority": 7, "age": 20 }
    ));
    assert!(no_match(
        filter.clone(),
        doc! { "status": "active", "priority": 1, "age": 15 }
    ));
    assert!(no_match(
        filter,
        doc! { "status": "closed", "priority": 2, "age": 25 }
    ));
}

#[test]
fn combined_type_and_range() {
    let filter = doc! {
        "score": { "$type": "double", "$gte": 0.0, "$lte": 100.0 }
    };
    assert!(matches(filter.clone(), doc! { "score": 85.5_f64 }));
    assert!(no_match(filter.clone(), doc! { "score": 85_i32 }));
    assert!(no_match(filter, doc! { "score": 150.0_f64 }));
}

#[test]
fn booleans_in_filter() {
    assert!(matches(
        doc! { "active": true },
        doc! { "active": true, "name": "x" }
    ));
    assert!(no_match(
        doc! { "active": true },
        doc! { "active": false, "name": "x" }
    ));
}

// -----------------------------------------------------------------------
// Cross-type BSON ordering (type tower)
// -----------------------------------------------------------------------

#[test]
fn cross_type_null_lt_numbers() {
    // null < numbers in MongoDB ordering.
    assert!(no_match(
        doc! { "a": { "$gt": Bson::Null } },
        doc! { "a": Bson::Null }
    ));
    assert!(matches(
        doc! { "a": { "$gt": Bson::Null } },
        doc! { "a": 0 }
    ));
}

#[test]
fn cross_type_numbers_lt_strings() {
    assert!(matches(doc! { "a": { "$lt": "z" } }, doc! { "a": 9999 }));
}

// -----------------------------------------------------------------------
// $elemMatch
// -----------------------------------------------------------------------

#[test]
fn elem_match_operator_mode_single_condition() {
    // Single operator condition applied to each element.
    let doc = doc! { "scores": [5, 15, 95] };
    assert!(matches(
        doc! { "scores": { "$elemMatch": { "$gt": 80 } } },
        doc.clone()
    ));
    assert!(no_match(
        doc! { "scores": { "$elemMatch": { "$gt": 100 } } },
        doc
    ));
}

#[test]
fn elem_match_operator_mode_multi_condition() {
    // A SINGLE element must satisfy ALL conditions (not any element per condition).
    // Array [5, 15, 25]: only 15 satisfies both $gt:10 AND $lt:20.
    let doc = doc! { "scores": [5, 15, 25] };
    assert!(matches(
        doc! { "scores": { "$elemMatch": { "$gt": 10, "$lt": 20 } } },
        doc.clone()
    ));
    // No single element satisfies $gt:10 AND $lt:12.
    assert!(no_match(
        doc! { "scores": { "$elemMatch": { "$gt": 10, "$lt": 12 } } },
        doc
    ));
}

#[test]
fn elem_match_document_mode() {
    // Elements are documents; condition is a sub-filter.
    let doc = doc! {
        "items": [
            { "name": "a", "qty": 5 },
            { "name": "b", "qty": 15 },
        ]
    };
    assert!(matches(
        doc! { "items": { "$elemMatch": { "qty": { "$gt": 10 } } } },
        doc.clone()
    ));
    assert!(no_match(
        doc! { "items": { "$elemMatch": { "qty": { "$gt": 20 } } } },
        doc
    ));
}

#[test]
fn elem_match_document_mode_multi_field() {
    // The single-element multi-condition property: both qty > 10 AND
    // price < 5 must hold for the SAME element.
    let doc = doc! {
        "items": [
            { "qty": 15, "price": 3 },  // satisfies both
            { "qty": 5,  "price": 1 },  // qty fails
        ]
    };
    assert!(matches(
        doc! { "items": { "$elemMatch": { "qty": { "$gt": 10 }, "price": { "$lt": 5 } } } },
        doc.clone()
    ));
    // Neither element satisfies qty > 10 AND price > 4 simultaneously.
    assert!(no_match(
        doc! { "items": { "$elemMatch": { "qty": { "$gt": 10 }, "price": { "$gt": 4 } } } },
        doc
    ));
}

#[test]
fn elem_match_mixed_operator_document_errors() {
    assert!(errors(
        doc! { "scores": { "$elemMatch": { "$gt": 10, "ignored": 99 } } },
        doc! { "scores": [5, 15] }
    ));
}

#[test]
fn elem_match_non_array_no_match() {
    // Scalar field — $elemMatch never matches.
    assert!(no_match(
        doc! { "a": { "$elemMatch": { "$gt": 0 } } },
        doc! { "a": 5 }
    ));
    // Missing field — $elemMatch never matches.
    assert!(no_match(
        doc! { "a": { "$elemMatch": { "$gt": 0 } } },
        doc! { "b": 5 }
    ));
}

// -----------------------------------------------------------------------
// $all
// -----------------------------------------------------------------------

#[test]
fn all_basic() {
    let doc = doc! { "tags": ["rust", "go", "python"] };
    // All required tags present.
    assert!(matches(
        doc! { "tags": { "$all": ["rust", "python"] } },
        doc.clone()
    ));
    // One required tag absent.
    assert!(no_match(doc! { "tags": { "$all": ["rust", "java"] } }, doc));
}

#[test]
fn all_superset() {
    // $all list is a superset of the array — must fail.
    let doc = doc! { "nums": [1, 2] };
    assert!(no_match(doc! { "nums": { "$all": [1, 2, 3] } }, doc));
}

#[test]
fn all_single_value_matches_scalar() {
    // MongoDB treats scalar as single-element array for $all.
    assert!(matches(doc! { "a": { "$all": [42] } }, doc! { "a": 42 }));
    assert!(no_match(doc! { "a": { "$all": [42] } }, doc! { "a": 43 }));
}

#[test]
fn all_scalar_matches_duplicate_requirements_only_when_all_equal() {
    assert!(matches(
        doc! { "a": { "$all": [42, 42] } },
        doc! { "a": 42 }
    ));
    assert!(no_match(
        doc! { "a": { "$all": [42, 43] } },
        doc! { "a": 42 }
    ));
}

#[test]
fn all_null_scalar_matches_null_requirement() {
    assert!(matches(
        doc! { "a": { "$all": [Bson::Null] } },
        doc! { "a": Bson::Null }
    ));
    assert!(no_match(
        doc! { "a": { "$all": [Bson::Null, 1] } },
        doc! { "a": Bson::Null }
    ));
}

#[test]
fn all_empty_list_never_matches() {
    // $all: [] matches no documents.
    assert!(no_match(
        doc! { "a": { "$all": [] } },
        doc! { "a": [1, 2, 3] }
    ));
}

#[test]
fn all_missing_field_no_match() {
    assert!(no_match(doc! { "a": { "$all": [1] } }, doc! { "b": [1] }));
}

#[test]
fn all_with_elem_match() {
    // $all: [{$elemMatch: {...}}] — each $elemMatch sub-condition must be
    // satisfied by at least one element of the array.
    let doc = doc! {
        "results": [
            { "product": "abc", "score": 10 },
            { "product": "xyz", "score": 5 },
        ]
    };
    // Both $elemMatch conditions are satisfied by different elements.
    assert!(matches(
        doc! { "results": { "$all": [
            { "$elemMatch": { "product": "abc", "score": { "$gt": 8 } } },
            { "$elemMatch": { "product": "xyz", "score": { "$lt": 10 } } }
        ] } },
        doc.clone()
    ));
    // One $elemMatch condition is not satisfied.
    assert!(no_match(
        doc! { "results": { "$all": [
            { "$elemMatch": { "product": "abc", "score": { "$gt": 20 } } }
        ] } },
        doc
    ));
}

// -----------------------------------------------------------------------
// $size
// -----------------------------------------------------------------------

#[test]
fn size_exact_match() {
    let doc = doc! { "items": [1, 2, 3] };
    assert!(matches(doc! { "items": { "$size": 3 } }, doc.clone()));
    assert!(no_match(doc! { "items": { "$size": 2 } }, doc.clone()));
    assert!(no_match(doc! { "items": { "$size": 4 } }, doc));
}

#[test]
fn size_empty_array() {
    let doc = doc! { "items": [] };
    assert!(matches(doc! { "items": { "$size": 0 } }, doc.clone()));
    assert!(no_match(doc! { "items": { "$size": 1 } }, doc));
}

#[test]
fn size_non_array_no_match() {
    assert!(no_match(
        doc! { "a": { "$size": 1 } },
        doc! { "a": "hello" }
    ));
    assert!(no_match(doc! { "a": { "$size": 0 } }, doc! {}));
}

#[test]
fn size_float_whole_number() {
    // 3.0 is accepted as a whole number.
    let doc = doc! { "items": [1, 2, 3] };
    assert!(matches(doc! { "items": { "$size": 3.0_f64 } }, doc));
}

#[test]
fn size_float_fractional_errors() {
    // 2.5 is rejected.
    assert!(errors(
        doc! { "a": { "$size": 2.5_f64 } },
        doc! { "a": [1, 2] }
    ));
}

// -----------------------------------------------------------------------
// $regex
// -----------------------------------------------------------------------

#[test]
fn regex_basic_match() {
    let doc = doc! { "name": "Alice Smith" };
    assert!(matches(
        doc! { "name": { "$regex": "^Alice" } },
        doc.clone()
    ));
    assert!(no_match(doc! { "name": { "$regex": "^Bob" } }, doc));
}

#[test]
fn regex_case_insensitive() {
    let doc = doc! { "name": "Alice" };
    assert!(matches(
        doc! { "name": { "$regex": "alice", "$options": "i" } },
        doc.clone()
    ));
    assert!(no_match(doc! { "name": { "$regex": "alice" } }, doc));
}

#[test]
fn regex_multiline_flag() {
    // With 'm', ^ matches the start of each line.
    let doc = doc! { "text": "hello\nworld" };
    assert!(matches(
        doc! { "text": { "$regex": "^world", "$options": "m" } },
        doc.clone()
    ));
    assert!(no_match(doc! { "text": { "$regex": "^world" } }, doc));
}

#[test]
fn regex_dotall_flag() {
    // With 's', '.' matches newlines.
    let doc = doc! { "text": "hello\nworld" };
    assert!(matches(
        doc! { "text": { "$regex": "hello.world", "$options": "s" } },
        doc.clone()
    ));
    assert!(no_match(doc! { "text": { "$regex": "hello.world" } }, doc));
}

#[test]
fn regex_non_string_field_no_match() {
    // $regex does not match numeric or boolean fields.
    assert!(no_match(doc! { "a": { "$regex": "1" } }, doc! { "a": 1 }));
    assert!(no_match(
        doc! { "a": { "$regex": "true" } },
        doc! { "a": true }
    ));
}

#[test]
fn regex_missing_field_no_match() {
    assert!(no_match(
        doc! { "a": { "$regex": ".*" } },
        doc! { "b": "hello" }
    ));
}

#[test]
fn regex_array_field() {
    // $regex matches if any string element in the array matches.
    let doc = doc! { "tags": ["rust", "systems", "fast"] };
    assert!(matches(doc! { "tags": { "$regex": "^rust" } }, doc.clone()));
    assert!(no_match(doc! { "tags": { "$regex": "^python" } }, doc));
}

#[test]
fn regex_combined_with_other_ops() {
    // $regex + $exists: true in the same operator document.
    let doc = doc! { "name": "Alice" };
    assert!(matches(
        doc! { "name": { "$regex": "^A", "$exists": true } },
        doc.clone()
    ));
    assert!(no_match(
        doc! { "name": { "$regex": "^B", "$exists": true } },
        doc
    ));
}

#[test]
fn regex_bson_regular_expression_shorthand() {
    // /pattern/flags shorthand — condition is Bson::RegularExpression.
    let doc = doc! { "name": "Alice" };
    let filter = bson::doc! {
        "name": Bson::RegularExpression(bson::Regex {
            pattern: "^alice".to_string(),
            options: "i".to_string(),
        })
    };
    assert!(matches(filter, doc.clone()));

    let filter_no_match = bson::doc! {
        "name": Bson::RegularExpression(bson::Regex {
            pattern: "^bob".to_string(),
            options: "".to_string(),
        })
    };
    assert!(no_match(filter_no_match, doc));
}

#[test]
fn regex_options_without_regex_errors() {
    assert!(errors(
        doc! { "a": { "$options": "i" } },
        doc! { "a": "hello" }
    ));
}

#[test]
fn regex_invalid_pattern_errors() {
    // Unclosed group is an invalid pattern.
    assert!(errors(
        doc! { "a": { "$regex": "(unclosed" } },
        doc! { "a": "test" }
    ));
}

// -----------------------------------------------------------------------
// Explicitly unsupported operators (error code 9)
// -----------------------------------------------------------------------

#[test]
fn unsupported_operators_return_error() {
    // Top-level
    assert!(errors(
        doc! { "$text": { "$search": "foo" } },
        doc! { "a": 1 }
    ));
    assert!(errors(doc! { "$where": "this.a > 1" }, doc! { "a": 1 }));

    // Field-level
    assert!(errors(doc! { "a": { "$jsonSchema": {} } }, doc! { "a": 1 }));
}

#[test]
fn unsupported_operator_has_code_9() {
    use crate::error::codes;
    let result = eval_filter(&doc! { "a": 1 }, &doc! { "$where": "this.a > 1" });
    let err = result.unwrap_err();
    assert_eq!(
        err.code(),
        Some(codes::UNSUPPORTED_OPERATOR),
        "UnsupportedOperator must carry error code 9"
    );
    // Confirm it's actually code 9.
    assert_eq!(codes::UNSUPPORTED_OPERATOR, 9);
}

// -----------------------------------------------------------------------
// $mod
// -----------------------------------------------------------------------

#[test]
fn mod_basic_match() {
    assert!(matches(doc! { "a": { "$mod": [4, 0] } }, doc! { "a": 8 }));
    assert!(matches(doc! { "a": { "$mod": [4, 1] } }, doc! { "a": 9 }));
    assert!(no_match(doc! { "a": { "$mod": [4, 0] } }, doc! { "a": 9 }));
}

#[test]
fn mod_array_unwrap() {
    // Matches if any element matches.
    assert!(matches(
        doc! { "a": { "$mod": [4, 0] } },
        doc! { "a": [1, 2, 8] }
    ));
    assert!(no_match(
        doc! { "a": { "$mod": [4, 0] } },
        doc! { "a": [1, 2, 3] }
    ));
}

#[test]
fn mod_missing_field_no_match() {
    assert!(no_match(doc! { "a": { "$mod": [4, 0] } }, doc! { "b": 8 }));
}

#[test]
fn mod_non_numeric_field_no_match() {
    // Non-numeric field value never matches, never errors.
    assert!(no_match(
        doc! { "a": { "$mod": [4, 0] } },
        doc! { "a": "8" }
    ));
}

#[test]
fn mod_double_field_truncates_toward_zero() {
    // 8.9 truncates to 8; 8 % 4 == 0.
    assert!(matches(
        doc! { "a": { "$mod": [4, 0] } },
        doc! { "a": 8.9_f64 }
    ));
    // -8.9 truncates to -8; -8 % 4 == 0.
    assert!(matches(
        doc! { "a": { "$mod": [4, 0] } },
        doc! { "a": -8.9_f64 }
    ));
}

#[test]
fn mod_non_finite_double_field_no_match() {
    assert!(no_match(
        doc! { "a": { "$mod": [4, 0] } },
        doc! { "a": f64::NAN }
    ));
    assert!(no_match(
        doc! { "a": { "$mod": [4, 0] } },
        doc! { "a": f64::INFINITY }
    ));
}

#[test]
fn mod_negative_operands_c_style() {
    // Rust % on i64 is C-style truncated division (matches MongoDB).
    // -7 % 3 == -1.
    assert!(matches(doc! { "a": { "$mod": [3, -1] } }, doc! { "a": -7 }));
    // -7 % -3 == -1.
    assert!(matches(
        doc! { "a": { "$mod": [-3, -1] } },
        doc! { "a": -7 }
    ));
}

#[test]
fn mod_double_divisor_remainder_truncate() {
    // Divisor 4.5 -> 4, remainder 1.9 -> 1. 9 % 4 == 1.
    assert!(matches(
        doc! { "a": { "$mod": [4.5_f64, 1.9_f64] } },
        doc! { "a": 9 }
    ));
}

#[test]
fn mod_not_an_array_errors() {
    assert!(errors(doc! { "a": { "$mod": 4 } }, doc! { "a": 8 }));
}

#[test]
fn mod_not_enough_elements_errors() {
    assert!(errors(doc! { "a": { "$mod": [4] } }, doc! { "a": 8 }));
    assert!(errors(doc! { "a": { "$mod": [] } }, doc! { "a": 8 }));
}

#[test]
fn mod_too_many_elements_errors() {
    assert!(errors(doc! { "a": { "$mod": [4, 0, 1] } }, doc! { "a": 8 }));
}

#[test]
fn mod_non_numeric_operand_errors() {
    assert!(errors(doc! { "a": { "$mod": ["4", 0] } }, doc! { "a": 8 }));
    assert!(errors(doc! { "a": { "$mod": [4, "0"] } }, doc! { "a": 8 }));
}

#[test]
fn mod_non_finite_operand_errors() {
    assert!(errors(
        doc! { "a": { "$mod": [f64::NAN, 0] } },
        doc! { "a": 8 }
    ));
    assert!(errors(
        doc! { "a": { "$mod": [f64::INFINITY, 0] } },
        doc! { "a": 8 }
    ));
}

#[test]
fn mod_zero_divisor_errors() {
    assert!(errors(doc! { "a": { "$mod": [0, 0] } }, doc! { "a": 8 }));
    // 0.5 truncates to 0 -> still a zero divisor.
    assert!(errors(
        doc! { "a": { "$mod": [0.5_f64, 0] } },
        doc! { "a": 8 }
    ));
}

// -----------------------------------------------------------------------
// Bit-test operators: numeric masks
// -----------------------------------------------------------------------

#[test]
fn bits_all_set_numeric_mask() {
    // 54 = 0b110110; bits 4,5 set. Mask 0b110000 = 48.
    assert!(matches(
        doc! { "a": { "$bitsAllSet": 48_i32 } },
        doc! { "a": 54_i32 }
    ));
    // Mask bit 0 is not set in 54.
    assert!(no_match(
        doc! { "a": { "$bitsAllSet": 1_i32 } },
        doc! { "a": 54_i32 }
    ));
}

#[test]
fn bits_any_set_numeric_mask() {
    // 54 has bit 1 set; mask 0b10 = 2.
    assert!(matches(
        doc! { "a": { "$bitsAnySet": 2_i32 } },
        doc! { "a": 54_i32 }
    ));
    // 54 has neither bit 0 nor bit 3 set; mask 0b1001 = 9.
    assert!(no_match(
        doc! { "a": { "$bitsAnySet": 9_i32 } },
        doc! { "a": 54_i32 }
    ));
}

#[test]
fn bits_all_clear_numeric_mask() {
    // 54 = 0b110110; bits 0 and 3 are clear. Mask 0b1001 = 9.
    assert!(matches(
        doc! { "a": { "$bitsAllClear": 9_i32 } },
        doc! { "a": 54_i32 }
    ));
    // Bit 4 is set, so AllClear fails for a mask including it.
    assert!(no_match(
        doc! { "a": { "$bitsAllClear": 16_i32 } },
        doc! { "a": 54_i32 }
    ));
}

#[test]
fn bits_any_clear_numeric_mask() {
    // 54 has bit 0 clear; mask 0b1 = 1.
    assert!(matches(
        doc! { "a": { "$bitsAnyClear": 1_i32 } },
        doc! { "a": 54_i32 }
    ));
    // Every bit of mask 0b110110 = 54 is set in 54, so none is clear.
    assert!(no_match(
        doc! { "a": { "$bitsAnyClear": 54_i32 } },
        doc! { "a": 54_i32 }
    ));
}

#[test]
fn bits_double_integral_mask_and_value() {
    // Integral double mask and value pass the type gate.
    assert!(matches(
        doc! { "a": { "$bitsAllSet": 48.0_f64 } },
        doc! { "a": 54.0_f64 }
    ));
}

#[test]
fn bits_array_position_mask() {
    // 54 = 0b110110; bits 4 and 5 set.
    assert!(matches(
        doc! { "a": { "$bitsAllSet": [4_i32, 5_i32] } },
        doc! { "a": 54_i32 }
    ));
    assert!(no_match(
        doc! { "a": { "$bitsAllSet": [0_i32, 4_i32] } },
        doc! { "a": 54_i32 }
    ));
}

#[test]
fn bits_array_position_exceeds_63_positive() {
    // Non-negative value: bit 100 is clear.
    assert!(no_match(
        doc! { "a": { "$bitsAllSet": [100_i64] } },
        doc! { "a": 54_i32 }
    ));
    assert!(matches(
        doc! { "a": { "$bitsAllClear": [100_i64] } },
        doc! { "a": 54_i32 }
    ));
}

#[test]
fn bits_array_position_exceeds_63_negative() {
    // Negative value: infinite sign extension sets every bit >= 64.
    assert!(matches(
        doc! { "a": { "$bitsAllSet": [100_i64] } },
        doc! { "a": Bson::Int64(-1) }
    ));
    assert!(no_match(
        doc! { "a": { "$bitsAllClear": [100_i64] } },
        doc! { "a": Bson::Int64(-1) }
    ));
}

#[test]
fn bits_negative_value_low_bits() {
    // -1 has every low bit set.
    assert!(matches(
        doc! { "a": { "$bitsAllSet": [0_i32, 1_i32, 63_i32] } },
        doc! { "a": Bson::Int64(-1) }
    ));
    // -2 = 0b...1110; bit 0 is clear.
    assert!(matches(
        doc! { "a": { "$bitsAllClear": [0_i32] } },
        doc! { "a": Bson::Int64(-2) }
    ));
}

#[test]
fn bits_bindata_mask_docs_example() {
    // Docs example: BinData(0, "MA==") = one byte 0x30 = bits 4,5.
    // Value 54 = 0b110110 has bits 4,5 set.
    assert!(matches(
        doc! { "a": { "$bitsAllSet": bin_data(vec![0x30]) } },
        doc! { "a": 54_i32 }
    ));
    assert!(matches(
        doc! { "a": { "$bitsAnySet": bin_data(vec![0x30]) } },
        doc! { "a": 54_i32 }
    ));
    // 0b000001 has bit 0 only, which is clear in 54.
    assert!(no_match(
        doc! { "a": { "$bitsAllSet": bin_data(vec![0x01]) } },
        doc! { "a": 54_i32 }
    ));
}

#[test]
fn bits_bindata_multibyte_little_endian() {
    // Two bytes [0x00, 0x01] -> bit position 8 (LSB of second byte).
    assert!(matches(
        doc! { "a": { "$bitsAllSet": bin_data(vec![0x00, 0x01]) } },
        doc! { "a": 256_i32 }
    ));
    assert!(no_match(
        doc! { "a": { "$bitsAllSet": bin_data(vec![0x00, 0x01]) } },
        doc! { "a": 255_i32 }
    ));
}

#[test]
fn bits_bindata_field_value() {
    // BinData field value: byte 0x30 = bits 4,5 set.
    assert!(matches(
        doc! { "a": { "$bitsAllSet": [4_i32, 5_i32] } },
        doc! { "a": bin_data(vec![0x30]) }
    ));
    // Bits beyond the data length are clear.
    assert!(matches(
        doc! { "a": { "$bitsAllClear": [100_i32] } },
        doc! { "a": bin_data(vec![0x30]) }
    ));
    assert!(no_match(
        doc! { "a": { "$bitsAllSet": [100_i32] } },
        doc! { "a": bin_data(vec![0x30]) }
    ));
}

#[test]
fn bits_array_unwrap() {
    // Match if any element passes.
    assert!(matches(
        doc! { "a": { "$bitsAllSet": [4_i32, 5_i32] } },
        doc! { "a": [1_i32, 54_i32] }
    ));
    assert!(no_match(
        doc! { "a": { "$bitsAllSet": [4_i32, 5_i32] } },
        doc! { "a": [1_i32, 2_i32] }
    ));
}

#[test]
fn bits_missing_field_no_match() {
    assert!(no_match(
        doc! { "a": { "$bitsAllSet": 1_i32 } },
        doc! { "b": 1_i32 }
    ));
}

#[test]
fn bits_type_gate_no_match_no_error() {
    // Fractional double: no match, no error.
    assert!(no_match(
        doc! { "a": { "$bitsAllSet": 1_i32 } },
        doc! { "a": 1.5_f64 }
    ));
    // Non-finite double.
    assert!(no_match(
        doc! { "a": { "$bitsAnySet": 1_i32 } },
        doc! { "a": f64::NAN }
    ));
    // String.
    assert!(no_match(
        doc! { "a": { "$bitsAllSet": 1_i32 } },
        doc! { "a": "x" }
    ));
}

#[test]
fn bits_empty_mask_semantics() {
    // Numeric 0 mask: AllSet/AllClear vacuously match a type-gated field;
    // AnySet/AnyClear match nothing.
    assert!(matches(
        doc! { "a": { "$bitsAllSet": 0_i32 } },
        doc! { "a": 54_i32 }
    ));
    assert!(matches(
        doc! { "a": { "$bitsAllClear": 0_i32 } },
        doc! { "a": 54_i32 }
    ));
    assert!(no_match(
        doc! { "a": { "$bitsAnySet": 0_i32 } },
        doc! { "a": 54_i32 }
    ));
    assert!(no_match(
        doc! { "a": { "$bitsAnyClear": 0_i32 } },
        doc! { "a": 54_i32 }
    ));
    // Empty position array and all-zero BinData behave identically.
    assert!(matches(
        doc! { "a": { "$bitsAllSet": [] } },
        doc! { "a": 54_i32 }
    ));
    assert!(no_match(
        doc! { "a": { "$bitsAnySet": bin_data(vec![0x00]) } },
        doc! { "a": 54_i32 }
    ));
    // Empty mask still requires the field to pass the type gate.
    assert!(no_match(
        doc! { "a": { "$bitsAllSet": 0_i32 } },
        doc! { "a": "x" }
    ));
}

#[test]
fn bits_negative_numeric_mask_errors() {
    assert!(errors(
        doc! { "a": { "$bitsAllSet": -1_i32 } },
        doc! { "a": 1_i32 }
    ));
}

#[test]
fn bits_fractional_numeric_mask_errors() {
    assert!(errors(
        doc! { "a": { "$bitsAllSet": 1.5_f64 } },
        doc! { "a": 1_i32 }
    ));
}

#[test]
fn bits_invalid_array_element_errors() {
    // Negative position.
    assert!(errors(
        doc! { "a": { "$bitsAllSet": [-1_i32] } },
        doc! { "a": 1_i32 }
    ));
    // Fractional position.
    assert!(errors(
        doc! { "a": { "$bitsAllSet": [1.5_f64] } },
        doc! { "a": 1_i32 }
    ));
    // Non-numeric position.
    assert!(errors(
        doc! { "a": { "$bitsAllSet": ["1"] } },
        doc! { "a": 1_i32 }
    ));
}

#[test]
fn bits_invalid_mask_type_errors() {
    assert!(errors(
        doc! { "a": { "$bitsAllSet": "mask" } },
        doc! { "a": 1_i32 }
    ));
}

// -----------------------------------------------------------------------
// $comment (top-level no-op)
// -----------------------------------------------------------------------

#[test]
fn comment_top_level_no_op_alone() {
    // A lone $comment matches every document and never errors.
    assert!(matches(doc! { "$comment": "find active" }, doc! { "a": 1 }));
    assert!(matches(doc! { "$comment": "x" }, doc! {}));
}

#[test]
fn comment_top_level_with_other_predicates() {
    // $comment is ignored; the sibling predicate still applies.
    assert!(matches(
        doc! { "$comment": "note", "a": 1 },
        doc! { "a": 1 }
    ));
    assert!(no_match(
        doc! { "$comment": "note", "a": 1 },
        doc! { "a": 2 }
    ));
}

#[test]
fn comment_top_level_non_string_accepted() {
    // Non-string $comment values are accepted silently (no error).
    assert!(matches(
        doc! { "$comment": 42_i32, "a": 1 },
        doc! { "a": 1 }
    ));
    assert!(matches(
        doc! { "$comment": { "nested": true }, "a": 1 },
        doc! { "a": 1 }
    ));
}

#[test]
fn comment_field_level_still_errors() {
    // A field-level $comment remains an unknown-operator error.
    assert!(errors(doc! { "a": { "$comment": "x" } }, doc! { "a": 1 }));
}

// -----------------------------------------------------------------------
// $expr (top-level aggregation-expression filter)
// -----------------------------------------------------------------------

#[test]
fn expr_field_to_field_comparison() {
    // {$expr: {$gt: ["$a", "$b"]}} compares two fields of the same document.
    let filter = doc! { "$expr": { "$gt": ["$a", "$b"] } };
    assert!(matches(filter.clone(), doc! { "a": 5, "b": 3 }));
    assert!(no_match(filter, doc! { "a": 2, "b": 3 }));
}

#[test]
fn expr_arithmetic_comparison() {
    // {$expr: {$gt: [{$add: ["$a", 5]}, "$b"]}} — arithmetic inside $expr.
    let filter = doc! { "$expr": { "$gt": [{ "$add": ["$a", 5] }, "$b"] } };
    // 1 + 5 = 6 > 4 -> match.
    assert!(matches(filter.clone(), doc! { "a": 1, "b": 4 }));
    // 1 + 5 = 6 > 10 is false -> no match.
    assert!(no_match(filter, doc! { "a": 1, "b": 10 }));
}

#[test]
fn expr_and_composed_with_normal_predicate() {
    // $expr is a sibling of a normal field predicate (implicit AND).
    let filter = doc! { "$expr": { "$gt": ["$a", "$b"] }, "c": 1 };
    assert!(matches(filter.clone(), doc! { "a": 5, "b": 3, "c": 1 }));
    // $expr matches but the normal predicate fails.
    assert!(no_match(filter.clone(), doc! { "a": 5, "b": 3, "c": 2 }));
    // Normal predicate matches but $expr fails.
    assert!(no_match(filter, doc! { "a": 1, "b": 3, "c": 1 }));
}

#[test]
fn expr_inside_or() {
    // $expr nested inside $or composes via top-level recursion.
    let filter = doc! {
        "$or": [
            { "$expr": { "$gt": ["$a", "$b"] } },
            { "c": 1 },
        ]
    };
    // First branch ($expr) matches.
    assert!(matches(filter.clone(), doc! { "a": 5, "b": 3, "c": 0 }));
    // Second branch matches even though $expr does not.
    assert!(matches(filter.clone(), doc! { "a": 1, "b": 3, "c": 1 }));
    // Neither branch matches.
    assert!(no_match(filter, doc! { "a": 1, "b": 3, "c": 0 }));
}

#[test]
fn expr_boolean_literals() {
    // {$expr: true} matches every document; {$expr: false} matches none.
    assert!(matches(doc! { "$expr": true }, doc! { "a": 1 }));
    assert!(no_match(doc! { "$expr": false }, doc! { "a": 1 }));
}

#[test]
fn expr_truthiness() {
    // Numeric zero is falsy; a non-empty value (string) is truthy.
    assert!(no_match(doc! { "$expr": "$a" }, doc! { "a": 0_i32 }));
    assert!(matches(doc! { "$expr": "$a" }, doc! { "a": "x" }));
}

#[test]
fn expr_root_based_comparison() {
    // $$ROOT-based comparison: compare a field reached through $$ROOT.
    let filter = doc! { "$expr": { "$gt": ["$$ROOT.a", "$b"] } };
    assert!(matches(filter.clone(), doc! { "a": 5, "b": 3 }));
    assert!(no_match(filter, doc! { "a": 1, "b": 3 }));
}

#[test]
fn expr_unknown_operator_errors() {
    // An unknown expression operator inside $expr propagates as an error.
    assert!(errors(
        doc! { "$expr": { "$bogusOp": ["$a", 1] } },
        doc! { "a": 1 }
    ));
}

#[test]
fn expr_field_level_still_errors() {
    // Field-level {field: {$expr: ...}} is invalid (unknown-operator path).
    assert!(errors(
        doc! { "a": { "$expr": { "$gt": ["$a", 1] } } },
        doc! { "a": 1 }
    ));
}
