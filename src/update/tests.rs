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

// ---- $min / $max --------------------------------------------------------

#[test]
fn min_sets_missing_and_smaller_values() {
    let mut doc = doc! { "score": 10i32 };
    apply(&mut doc, &doc! { "$min": { "score": 7i32, "floor": 1i32 } }).unwrap();
    assert_eq!(doc.get_i32("score").unwrap(), 7);
    assert_eq!(doc.get_i32("floor").unwrap(), 1);
}

#[test]
fn min_keeps_equal_or_greater_current_value() {
    let mut doc = doc! { "score": 10i32, "same": 5i32 };
    apply(&mut doc, &doc! { "$min": { "score": 11i32, "same": 5i32 } }).unwrap();
    assert_eq!(doc.get_i32("score").unwrap(), 10);
    assert_eq!(doc.get_i32("same").unwrap(), 5);
}

#[test]
fn max_sets_missing_and_greater_values() {
    let mut doc = doc! { "score": 10i32 };
    apply(
        &mut doc,
        &doc! { "$max": { "score": 12i32, "ceiling": 20i32 } },
    )
    .unwrap();
    assert_eq!(doc.get_i32("score").unwrap(), 12);
    assert_eq!(doc.get_i32("ceiling").unwrap(), 20);
}

#[test]
fn max_keeps_equal_or_smaller_current_value() {
    let mut doc = doc! { "score": 10i32, "same": 5i32 };
    apply(&mut doc, &doc! { "$max": { "score": 9i32, "same": 5i32 } }).unwrap();
    assert_eq!(doc.get_i32("score").unwrap(), 10);
    assert_eq!(doc.get_i32("same").unwrap(), 5);
}

#[test]
fn min_max_use_bson_type_ordering() {
    let mut doc = doc! { "min_field": "z", "max_field": 1i32 };
    apply(
        &mut doc,
        &doc! { "$min": { "min_field": 1i32 }, "$max": { "max_field": "z" } },
    )
    .unwrap();
    assert_eq!(doc.get_i32("min_field").unwrap(), 1);
    assert_eq!(doc.get_str("max_field").unwrap(), "z");
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

// ---- $push with modifiers -----------------------------------------------

#[test]
fn push_each_appends_multiple() {
    let mut doc = doc! { "arr": [1i32] };
    apply(
        &mut doc,
        &doc! { "$push": { "arr": { "$each": [2i32, 3i32] } } },
    )
    .unwrap();
    let arr = doc.get_array("arr").unwrap();
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[1], Bson::Int32(2));
    assert_eq!(arr[2], Bson::Int32(3));
}

#[test]
fn push_each_empty_is_noop() {
    let mut doc = doc! { "arr": [1i32, 2i32] };
    apply(&mut doc, &doc! { "$push": { "arr": { "$each": [] } } }).unwrap();
    let arr = doc.get_array("arr").unwrap();
    assert_eq!(arr.len(), 2);
}

#[test]
fn push_each_position_at_start() {
    let mut doc = doc! { "arr": [2i32, 3i32] };
    apply(
        &mut doc,
        &doc! { "$push": { "arr": { "$each": [0i32, 1i32], "$position": 0i32 } } },
    )
    .unwrap();
    let arr = doc.get_array("arr").unwrap();
    assert_eq!(
        arr,
        &[
            Bson::Int32(0),
            Bson::Int32(1),
            Bson::Int32(2),
            Bson::Int32(3)
        ]
    );
}

#[test]
fn push_each_position_in_middle() {
    let mut doc = doc! { "arr": [1i32, 4i32] };
    apply(
        &mut doc,
        &doc! { "$push": { "arr": { "$each": [2i32, 3i32], "$position": 1i32 } } },
    )
    .unwrap();
    let arr = doc.get_array("arr").unwrap();
    assert_eq!(
        arr,
        &[
            Bson::Int32(1),
            Bson::Int32(2),
            Bson::Int32(3),
            Bson::Int32(4)
        ]
    );
}

#[test]
fn push_each_negative_position() {
    // Negative $position counts from end.
    // [10, 20, 30] + $each:[99] $position:-1 → insert before last → [10, 20, 99, 30]
    let mut doc = doc! { "arr": [10i32, 20i32, 30i32] };
    apply(
        &mut doc,
        &doc! { "$push": { "arr": { "$each": [99i32], "$position": -1i32 } } },
    )
    .unwrap();
    let arr = doc.get_array("arr").unwrap();
    assert_eq!(
        arr,
        &[
            Bson::Int32(10),
            Bson::Int32(20),
            Bson::Int32(99),
            Bson::Int32(30)
        ]
    );
}

#[test]
fn push_each_slice_keeps_first_n() {
    let mut doc = doc! { "arr": [1i32, 2i32, 3i32] };
    apply(
        &mut doc,
        &doc! { "$push": { "arr": { "$each": [4i32, 5i32], "$slice": 3i32 } } },
    )
    .unwrap();
    let arr = doc.get_array("arr").unwrap();
    // After push: [1,2,3,4,5]; after $slice:3 → [1,2,3]
    assert_eq!(arr, &[Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]);
}

#[test]
fn push_each_slice_negative_keeps_last_n() {
    let mut doc = doc! { "arr": [1i32, 2i32, 3i32] };
    apply(
        &mut doc,
        &doc! { "$push": { "arr": { "$each": [4i32, 5i32], "$slice": -3i32 } } },
    )
    .unwrap();
    let arr = doc.get_array("arr").unwrap();
    // After push: [1,2,3,4,5]; after $slice:-3 → [3,4,5]
    assert_eq!(arr, &[Bson::Int32(3), Bson::Int32(4), Bson::Int32(5)]);
}

#[test]
fn push_each_slice_zero_clears_array() {
    let mut doc = doc! { "arr": [1i32, 2i32] };
    apply(
        &mut doc,
        &doc! { "$push": { "arr": { "$each": [3i32], "$slice": 0i32 } } },
    )
    .unwrap();
    let arr = doc.get_array("arr").unwrap();
    assert!(arr.is_empty());
}

#[test]
fn push_each_sort_ascending() {
    let mut doc = doc! { "arr": [3i32, 1i32] };
    apply(
        &mut doc,
        &doc! { "$push": { "arr": { "$each": [2i32], "$sort": 1i32 } } },
    )
    .unwrap();
    let arr = doc.get_array("arr").unwrap();
    assert_eq!(arr, &[Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]);
}

#[test]
fn push_each_sort_descending() {
    let mut doc = doc! { "arr": [1i32, 3i32] };
    apply(
        &mut doc,
        &doc! { "$push": { "arr": { "$each": [2i32], "$sort": -1i32 } } },
    )
    .unwrap();
    let arr = doc.get_array("arr").unwrap();
    assert_eq!(arr, &[Bson::Int32(3), Bson::Int32(2), Bson::Int32(1)]);
}

#[test]
fn push_each_sort_by_field() {
    // Array of embedded documents sorted by "score" descending.
    let mut doc = doc! {
        "scores": [
            { "name": "b", "score": 2i32 },
            { "name": "c", "score": 3i32 },
        ]
    };
    apply(
        &mut doc,
        &doc! {
            "$push": {
                "scores": {
                    "$each": [{ "name": "a", "score": 1i32 }],
                    "$sort": { "score": -1i32 }
                }
            }
        },
    )
    .unwrap();
    let arr = doc.get_array("scores").unwrap();
    assert_eq!(arr.len(), 3);
    // Descending order: score 3, 2, 1
    let first = arr[0].as_document().unwrap();
    let last = arr[2].as_document().unwrap();
    assert_eq!(first.get_i32("score").unwrap(), 3);
    assert_eq!(last.get_i32("score").unwrap(), 1);
}

#[test]
fn push_each_sort_then_slice() {
    // Sort ascending then slice to keep first 2.
    let mut doc = doc! { "arr": [3i32, 1i32, 5i32] };
    apply(
        &mut doc,
        &doc! { "$push": { "arr": { "$each": [4i32, 2i32], "$sort": 1i32, "$slice": 3i32 } } },
    )
    .unwrap();
    let arr = doc.get_array("arr").unwrap();
    // After push: [3,1,5,4,2]; after sort: [1,2,3,4,5]; after slice:3 → [1,2,3]
    assert_eq!(arr, &[Bson::Int32(1), Bson::Int32(2), Bson::Int32(3)]);
}

// ---- $addToSet with $each -----------------------------------------------

#[test]
fn add_to_set_each_adds_missing_elements() {
    let mut doc = doc! { "tags": ["a", "b"] };
    apply(
        &mut doc,
        &doc! { "$addToSet": { "tags": { "$each": ["b", "c", "d"] } } },
    )
    .unwrap();
    let arr = doc.get_array("tags").unwrap();
    // "b" already present, "c" and "d" added → ["a", "b", "c", "d"]
    assert_eq!(arr.len(), 4);
}

#[test]
fn add_to_set_each_no_duplicates_added() {
    let mut doc = doc! { "tags": ["x", "y"] };
    apply(
        &mut doc,
        &doc! { "$addToSet": { "tags": { "$each": ["x", "y"] } } },
    )
    .unwrap();
    let arr = doc.get_array("tags").unwrap();
    assert_eq!(arr.len(), 2); // nothing added
}

#[test]
fn add_to_set_each_creates_array_when_missing() {
    let mut doc = doc! {};
    apply(
        &mut doc,
        &doc! { "$addToSet": { "new_field": { "$each": [1i32, 2i32] } } },
    )
    .unwrap();
    let arr = doc.get_array("new_field").unwrap();
    assert_eq!(arr.len(), 2);
}

// ---- $pullAll -----------------------------------------------------------

#[test]
fn pull_all_removes_listed_values() {
    let mut doc = doc! { "scores": [0i32, 2i32, 5i32, 0i32, 3i32] };
    apply(&mut doc, &doc! { "$pullAll": { "scores": [0i32, 5i32] } }).unwrap();
    let arr = doc.get_array("scores").unwrap();
    // 0 and 5 should be removed; 2 and 3 remain
    assert_eq!(arr.len(), 2);
    assert!(arr.contains(&Bson::Int32(2)));
    assert!(arr.contains(&Bson::Int32(3)));
}

#[test]
fn pull_all_removes_all_occurrences() {
    let mut doc = doc! { "arr": [1i32, 2i32, 1i32, 3i32, 1i32] };
    apply(&mut doc, &doc! { "$pullAll": { "arr": [1i32] } }).unwrap();
    let arr = doc.get_array("arr").unwrap();
    assert_eq!(arr.len(), 2);
    assert!(!arr.contains(&Bson::Int32(1)));
}

#[test]
fn pull_all_noop_when_field_missing() {
    let mut doc = doc! {};
    apply(&mut doc, &doc! { "$pullAll": { "arr": [1i32, 2i32] } }).unwrap();
    assert!(!doc.contains_key("arr"));
}

#[test]
fn pull_all_empty_list_removes_nothing() {
    let mut doc = doc! { "arr": [1i32, 2i32, 3i32] };
    apply(&mut doc, &doc! { "$pullAll": { "arr": [] } }).unwrap();
    let arr = doc.get_array("arr").unwrap();
    assert_eq!(arr.len(), 3);
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
    apply_update(
        &mut doc,
        &doc! { "$setOnInsert": { "created": 1i32 } },
        true,
    )
    .unwrap();
    assert_eq!(doc.get_i32("created").unwrap(), 1);
}

#[test]
fn set_on_insert_not_applied_when_not_insert() {
    let mut doc = doc! {};
    apply_update(
        &mut doc,
        &doc! { "$setOnInsert": { "created": 1i32 } },
        false,
    )
    .unwrap();
    assert!(!doc.contains_key("created"));
}
