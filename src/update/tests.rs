use super::*;
use bson::doc;

fn apply(doc: &mut Document, update: &Document) -> Result<()> {
    apply_update(doc, update, &Document::new(), None, false)
}

/// Apply an update with a query `filter` (for the positional `$` operator) and
/// no `arrayFilters`.
fn apply_with_filter(doc: &mut Document, update: &Document, filter: &Document) -> Result<()> {
    apply_update(doc, update, filter, None, false)
}

/// Apply an update with `arrayFilters` and an empty query filter.
fn apply_with_array_filters(
    doc: &mut Document,
    update: &Document,
    array_filters: &[Document],
) -> Result<()> {
    apply_update(doc, update, &Document::new(), Some(array_filters), false)
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
    let result = apply_update(&mut doc, &doc! { "$where": "x > 5" }, &Document::new(), None, false);
    assert!(
        matches!(result, Err(Error::UnsupportedOperator { .. })),
        "expected UnsupportedOperator, got: {:?}",
        result
    );
}

// ---- $bit ---------------------------------------------------------------

#[test]
fn bit_and_i32() {
    let mut doc = doc! { "flags": 0b1111i32 };
    apply(&mut doc, &doc! { "$bit": { "flags": { "and": 0b1010i32 } } }).unwrap();
    assert_eq!(doc.get_i32("flags").unwrap(), 0b1010);
}

#[test]
fn bit_or_i32() {
    let mut doc = doc! { "flags": 0b0101i32 };
    apply(&mut doc, &doc! { "$bit": { "flags": { "or": 0b1010i32 } } }).unwrap();
    assert_eq!(doc.get_i32("flags").unwrap(), 0b1111);
}

#[test]
fn bit_xor_i32() {
    let mut doc = doc! { "flags": 0b1100i32 };
    apply(&mut doc, &doc! { "$bit": { "flags": { "xor": 0b1010i32 } } }).unwrap();
    assert_eq!(doc.get_i32("flags").unwrap(), 0b0110);
}

#[test]
fn bit_and_i64() {
    let mut doc = doc! { "flags": 0b1111i64 };
    apply(&mut doc, &doc! { "$bit": { "flags": { "and": 0b1010i64 } } }).unwrap();
    assert_eq!(doc.get_i64("flags").unwrap(), 0b1010);
}

#[test]
fn bit_or_i64() {
    let mut doc = doc! { "flags": 0b0101i64 };
    apply(&mut doc, &doc! { "$bit": { "flags": { "or": 0b1010i64 } } }).unwrap();
    assert_eq!(doc.get_i64("flags").unwrap(), 0b1111);
}

#[test]
fn bit_xor_i64() {
    let mut doc = doc! { "flags": 0b1100i64 };
    apply(&mut doc, &doc! { "$bit": { "flags": { "xor": 0b1010i64 } } }).unwrap();
    assert_eq!(doc.get_i64("flags").unwrap(), 0b0110);
}

#[test]
fn bit_width_promotion_i32_op_i64_gives_i64() {
    // Int32 field, Int64 operand → result is Int64.
    let mut doc = doc! { "v": 0b1111i32 };
    apply(&mut doc, &doc! { "$bit": { "v": { "and": 0b1010i64 } } }).unwrap();
    let val = doc.get("v").unwrap();
    assert!(matches!(val, Bson::Int64(0b1010)), "expected Int64(10), got {val:?}");
}

#[test]
fn bit_sequential_multi_op() {
    // Multiple ops in one doc applied in document order.
    // Start 0b1111 (15): OR 0b0001 → 15, AND 0b1010 → 10, XOR 0b0011 → 9.
    let mut doc = doc! { "v": 0b1111i32 };
    apply(
        &mut doc,
        &doc! { "$bit": { "v": { "or": 0b0001i32, "and": 0b1010i32, "xor": 0b0011i32 } } },
    )
    .unwrap();
    assert_eq!(doc.get_i32("v").unwrap(), (0b1111 | 0b0001) & 0b1010 ^ 0b0011);
}

#[test]
fn bit_missing_field_created_as_zero() {
    let mut doc = doc! {};
    apply(&mut doc, &doc! { "$bit": { "flags": { "or": 5i32 } } }).unwrap();
    assert_eq!(doc.get_i32("flags").unwrap(), 5);
}

#[test]
fn bit_dotted_path() {
    let mut doc = doc! { "a": { "b": 0b1111i32 } };
    apply(&mut doc, &doc! { "$bit": { "a.b": { "and": 0b1010i32 } } }).unwrap();
    let inner = doc.get_document("a").unwrap();
    assert_eq!(inner.get_i32("b").unwrap(), 0b1010);
}

#[test]
fn bit_double_operand_error() {
    let mut doc = doc! { "v": 1i32 };
    let result = apply(&mut doc, &doc! { "$bit": { "v": { "and": 1.0 } } });
    assert!(
        matches!(result, Err(Error::Internal(_))),
        "expected Internal error for Double operand, got {result:?}"
    );
}

#[test]
fn bit_unknown_op_key_error() {
    let mut doc = doc! { "v": 1i32 };
    let result = apply(&mut doc, &doc! { "$bit": { "v": { "nand": 1i32 } } });
    assert!(
        matches!(result, Err(Error::Internal(_))),
        "expected Internal error for unknown op key, got {result:?}"
    );
}

#[test]
fn bit_non_integer_target_error() {
    let mut doc = doc! { "v": "hello" };
    let result = apply(&mut doc, &doc! { "$bit": { "v": { "or": 1i32 } } });
    assert!(
        matches!(result, Err(Error::Internal(_))),
        "expected Internal error for non-integer target, got {result:?}"
    );
}

#[test]
fn bit_empty_doc_error() {
    let mut doc = doc! { "v": 1i32 };
    // The per-field arg is an empty document.
    let result = apply_update(
        &mut doc,
        &bson::doc! { "$bit": { "v": {} } },
        &Document::new(),
        None,
        false,
    );
    assert!(
        matches!(result, Err(Error::Internal(_))),
        "expected Internal error for empty per-field doc, got {result:?}"
    );
}

// ---- $[] all-positional -------------------------------------------------

#[test]
fn all_positional_set_trailing() {
    // { $set: { "arr.$[]": 99 } } sets every element of `arr`.
    let mut doc = doc! { "arr": [1i32, 2i32, 3i32] };
    apply(&mut doc, &doc! { "$set": { "arr.$[]": 99i32 } }).unwrap();
    let arr = doc.get_array("arr").unwrap();
    assert_eq!(arr, &[Bson::Int32(99), Bson::Int32(99), Bson::Int32(99)]);
}

#[test]
fn all_positional_nested_field() {
    // { $set: { "docs.$[].score": 0 } } sets the `score` field in every element.
    let mut doc = doc! {
        "docs": [
            { "score": 10i32 },
            { "score": 20i32 },
        ]
    };
    apply(&mut doc, &doc! { "$set": { "docs.$[].score": 0i32 } }).unwrap();
    let arr = doc.get_array("docs").unwrap();
    for elem in arr {
        assert_eq!(elem.as_document().unwrap().get_i32("score").unwrap(), 0);
    }
}

#[test]
fn all_positional_doubly_nested() {
    // `outer.$[].inner.$[]` — two levels of array expansion.
    let mut doc = doc! {
        "outer": [
            { "inner": [1i32, 2i32] },
            { "inner": [3i32, 4i32] },
        ]
    };
    apply(&mut doc, &doc! { "$set": { "outer.$[].inner.$[]": 0i32 } }).unwrap();
    let outer = doc.get_array("outer").unwrap();
    for elem in outer {
        let inner = elem.as_document().unwrap().get_array("inner").unwrap();
        assert_eq!(inner, &[Bson::Int32(0), Bson::Int32(0)]);
    }
}

#[test]
fn all_positional_empty_array_noop() {
    // `$[]` on an empty array succeeds and leaves it empty.
    let mut doc = doc! { "arr": [] };
    apply(&mut doc, &doc! { "$set": { "arr.$[]": 99i32 } }).unwrap();
    let arr = doc.get_array("arr").unwrap();
    assert!(arr.is_empty());
}

#[test]
fn all_positional_missing_path_error() {
    // Array prefix does not exist → Internal error.
    let mut doc = doc! {};
    let result = apply(&mut doc, &doc! { "$set": { "arr.$[]": 1i32 } });
    assert!(
        matches!(result, Err(Error::Internal(_))),
        "expected Internal error for missing prefix, got {result:?}"
    );
}

#[test]
fn all_positional_non_array_error() {
    // Prefix exists but is not an array → Internal error.
    let mut doc = doc! { "arr": "not-an-array" };
    let result = apply(&mut doc, &doc! { "$set": { "arr.$[]": 1i32 } });
    assert!(
        matches!(result, Err(Error::Internal(_))),
        "expected Internal error for non-array prefix, got {result:?}"
    );
}

#[test]
fn all_positional_inc_through_array() {
    // $inc works through $[] as well.
    let mut doc = doc! { "scores": [10i32, 20i32, 30i32] };
    apply(&mut doc, &doc! { "$inc": { "scores.$[]": 5i32 } }).unwrap();
    let arr = doc.get_array("scores").unwrap();
    assert_eq!(
        arr,
        &[Bson::Int32(15), Bson::Int32(25), Bson::Int32(35)]
    );
}

#[test]
fn positional_dollar_without_query_condition_errors() {
    // `$` requires a query condition on the array; an empty filter has none.
    let mut doc = doc! { "arr": [1i32] };
    let result = apply(&mut doc, &doc! { "$set": { "arr.$": 99i32 } });
    let msg = format!("{:?}", result.unwrap_err());
    assert!(
        msg.contains("did not find the match needed from the query"),
        "{msg}"
    );
}

#[test]
fn filtered_positional_without_array_filter_errors() {
    // `$[identifier]` requires a matching entry in `arrayFilters`.
    let mut doc = doc! { "arr": [1i32] };
    let result = apply(&mut doc, &doc! { "$set": { "arr.$[elem]": 99i32 } });
    let msg = format!("{:?}", result.unwrap_err());
    assert!(
        msg.contains("No array filter found for identifier 'elem'"),
        "{msg}"
    );
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

#[test]
fn upsert_base_adopts_equality_id() {
    let filter = doc! { "_id": 42i32, "name": "Ann" };
    let base = upsert_base_from_filter(&filter);
    assert_eq!(base.get_i32("_id").unwrap(), 42);
    assert_eq!(base.get_str("name").unwrap(), "Ann");
}

#[test]
fn set_allows_nested_id_field() {
    let mut doc = doc! { "a": { "_id": 1i32 } };
    apply(&mut doc, &doc! { "$set": { "a._id": 2i32 } }).unwrap();
    assert_eq!(
        doc.get_document("a").unwrap().get_i32("_id").unwrap(),
        2
    );
}

// ---- $setOnInsert -------------------------------------------------------

#[test]
fn set_on_insert_applied_when_is_insert() {
    let mut doc = doc! {};
    apply_update(
        &mut doc,
        &doc! { "$setOnInsert": { "created": 1i32 } },
        &Document::new(),
        None,
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
        &Document::new(),
        None,
        false,
    )
    .unwrap();
    assert!(!doc.contains_key("created"));
}

// ---- arrayFilters / $[<identifier>] -------------------------------------

#[test]
fn array_filter_basic_scalar_condition() {
    // Set every grade >= 100 to 100.
    let mut doc = doc! { "grades": [95i32, 102i32, 110i32, 80i32] };
    apply_with_array_filters(
        &mut doc,
        &doc! { "$set": { "grades.$[elem]": 100i32 } },
        &[doc! { "elem": { "$gte": 100i32 } }],
    )
    .unwrap();
    let grades = doc.get_array("grades").unwrap();
    assert_eq!(grades[0].as_i32().unwrap(), 95);
    assert_eq!(grades[1].as_i32().unwrap(), 100);
    assert_eq!(grades[2].as_i32().unwrap(), 100);
    assert_eq!(grades[3].as_i32().unwrap(), 80);
}

#[test]
fn array_filter_dotted_condition_on_embedded_docs() {
    // Set mean=100 for embedded docs whose grade >= 85.
    let mut doc = doc! {
        "grades": [
            { "grade": 80i32, "mean": 75i32 },
            { "grade": 85i32, "mean": 90i32 },
            { "grade": 90i32, "mean": 85i32 },
        ]
    };
    apply_with_array_filters(
        &mut doc,
        &doc! { "$set": { "grades.$[elem].mean": 100i32 } },
        &[doc! { "elem.grade": { "$gte": 85i32 } }],
    )
    .unwrap();
    let grades = doc.get_array("grades").unwrap();
    assert_eq!(grades[0].as_document().unwrap().get_i32("mean").unwrap(), 75);
    assert_eq!(grades[1].as_document().unwrap().get_i32("mean").unwrap(), 100);
    assert_eq!(grades[2].as_document().unwrap().get_i32("mean").unwrap(), 100);
}

#[test]
fn array_filter_multiple_identifiers_in_one_path() {
    // Nested arrays addressed by two identifiers in one path.
    let mut doc = doc! {
        "items": [
            { "tags": [ { "k": "a", "v": 1i32 }, { "k": "b", "v": 2i32 } ] },
        ]
    };
    apply_with_array_filters(
        &mut doc,
        &doc! { "$set": { "items.$[i].tags.$[t].v": 99i32 } },
        &[doc! { "i.tags": { "$exists": true } }, doc! { "t.k": "b" }],
    )
    .unwrap();
    let tags = doc.get_array("items").unwrap()[0]
        .as_document()
        .unwrap()
        .get_array("tags")
        .unwrap();
    assert_eq!(tags[0].as_document().unwrap().get_i32("v").unwrap(), 1);
    assert_eq!(tags[1].as_document().unwrap().get_i32("v").unwrap(), 99);
}

#[test]
fn array_filter_no_match_is_noop() {
    let mut doc = doc! { "grades": [95i32, 80i32] };
    apply_with_array_filters(
        &mut doc,
        &doc! { "$set": { "grades.$[elem]": 100i32 } },
        &[doc! { "elem": { "$gte": 200i32 } }],
    )
    .unwrap();
    let grades = doc.get_array("grades").unwrap();
    assert_eq!(grades[0].as_i32().unwrap(), 95);
    assert_eq!(grades[1].as_i32().unwrap(), 80);
}

#[test]
fn array_filter_unused_is_error() {
    let mut doc = doc! { "grades": [95i32] };
    let result = apply_with_array_filters(
        &mut doc,
        &doc! { "$set": { "grades.$[a]": 1i32 } },
        &[doc! { "a": { "$gte": 0i32 } }, doc! { "b": { "$gte": 0i32 } }],
    );
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("was not used in the update"), "{msg}");
}

#[test]
fn array_filter_missing_identifier_is_error() {
    let mut doc = doc! { "grades": [95i32] };
    let result = apply_with_array_filters(
        &mut doc,
        &doc! { "$set": { "grades.$[missing]": 1i32 } },
        &[doc! { "elem": { "$gte": 0i32 } }],
    );
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("No array filter found for identifier 'missing'"), "{msg}");
}

#[test]
fn array_filter_duplicate_identifier_is_error() {
    let mut doc = doc! { "grades": [95i32] };
    let result = apply_with_array_filters(
        &mut doc,
        &doc! { "$set": { "grades.$[a]": 1i32 } },
        &[doc! { "a": { "$gte": 0i32 } }, doc! { "a": { "$lte": 9i32 } }],
    );
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("Found multiple array filters"), "{msg}");
}

#[test]
fn array_filter_bad_identifier_is_error() {
    // `$[1bad]` begins with a digit: not a valid identifier.
    let mut doc = doc! { "grades": [95i32] };
    let result = apply_with_array_filters(
        &mut doc,
        &doc! { "$set": { "grades.$[1bad]": 1i32 } },
        &[doc! { "elem": { "$gte": 0i32 } }],
    );
    assert!(
        matches!(result, Err(Error::UnsupportedOperator { .. })),
        "expected UnsupportedOperator, got {result:?}"
    );
}

// ---- positional $ -------------------------------------------------------

#[test]
fn positional_basic_via_filter_equality() {
    let mut doc = doc! { "grades": [80i32, 85i32, 90i32] };
    let filter = doc! { "grades": 85i32 };
    apply_with_filter(&mut doc, &doc! { "$set": { "grades.$": 100i32 } }, &filter).unwrap();
    let grades = doc.get_array("grades").unwrap();
    assert_eq!(grades[0].as_i32().unwrap(), 80);
    assert_eq!(grades[1].as_i32().unwrap(), 100);
    assert_eq!(grades[2].as_i32().unwrap(), 90);
}

#[test]
fn positional_via_elem_match() {
    let mut doc = doc! {
        "items": [
            { "k": "a", "v": 1i32 },
            { "k": "b", "v": 2i32 },
        ]
    };
    let filter = doc! { "items": { "$elemMatch": { "k": "b" } } };
    apply_with_filter(&mut doc, &doc! { "$set": { "items.$.v": 99i32 } }, &filter).unwrap();
    let items = doc.get_array("items").unwrap();
    assert_eq!(items[0].as_document().unwrap().get_i32("v").unwrap(), 1);
    assert_eq!(items[1].as_document().unwrap().get_i32("v").unwrap(), 99);
}

#[test]
fn positional_via_dotted_condition() {
    let mut doc = doc! {
        "items": [
            { "k": "a", "v": 1i32 },
            { "k": "b", "v": 2i32 },
        ]
    };
    let filter = doc! { "items.k": "b" };
    apply_with_filter(&mut doc, &doc! { "$set": { "items.$.v": 7i32 } }, &filter).unwrap();
    let items = doc.get_array("items").unwrap();
    assert_eq!(items[1].as_document().unwrap().get_i32("v").unwrap(), 7);
}

#[test]
fn positional_no_condition_is_error() {
    // Filter references a different field than the positional array path.
    let mut doc = doc! { "grades": [1i32, 2i32] };
    let filter = doc! { "other": 1i32 };
    let result = apply_with_filter(&mut doc, &doc! { "$set": { "grades.$": 9i32 } }, &filter);
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("did not find the match needed from the query"), "{msg}");
}

#[test]
fn positional_no_match_is_error() {
    let mut doc = doc! { "grades": [1i32, 2i32] };
    let filter = doc! { "grades": 99i32 };
    let result = apply_with_filter(&mut doc, &doc! { "$set": { "grades.$": 9i32 } }, &filter);
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("did not find the match needed from the query"), "{msg}");
}

#[test]
fn positional_too_many_is_error() {
    let mut doc = doc! { "a": [ { "b": [1i32] } ] };
    let filter = doc! { "a.b": 1i32 };
    let result = apply_with_filter(&mut doc, &doc! { "$set": { "a.$.b.$": 9i32 } }, &filter);
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("Too many positional"), "{msg}");
}

// ---- pipeline-form updates ----------------------------------------------

#[test]
fn pipeline_set_computed_from_existing_fields() {
    let original = doc! { "_id": 1i32, "price": 4i32, "qty": 3i32 };
    let result = apply_update_pipeline(
        &original,
        &[doc! { "$set": { "total": { "$multiply": ["$price", "$qty"] } } }],
    )
    .unwrap();
    assert_eq!(result.get_i32("total").unwrap(), 12);
    assert_eq!(result.get_i32("_id").unwrap(), 1);
}

#[test]
fn pipeline_disallowed_stage_is_error() {
    let original = doc! { "_id": 1i32 };
    let result = apply_update_pipeline(&original, &[doc! { "$match": { "_id": 1i32 } }]);
    let msg = format!("{:?}", result.unwrap_err());
    assert!(msg.contains("is not allowed to be used within an update"), "{msg}");
}

#[test]
fn pipeline_id_mutation_is_error() {
    let original = doc! { "_id": 1i32, "v": 0i32 };
    let result = apply_update_pipeline(&original, &[doc! { "$set": { "_id": 2i32 } }]);
    assert!(result.is_err(), "expected immutable _id error");
}

#[test]
fn pipeline_restores_dropped_id() {
    let original = doc! { "_id": 7i32, "v": 1i32 };
    let result =
        apply_update_pipeline(&original, &[doc! { "$replaceWith": { "v": 2i32 } }]).unwrap();
    assert_eq!(result.get_i32("_id").unwrap(), 7);
    assert_eq!(result.get_i32("v").unwrap(), 2);
}

#[test]
fn update_modifications_from_impls() {
    let from_doc: UpdateModifications = doc! { "$set": { "a": 1i32 } }.into();
    assert!(matches!(from_doc, UpdateModifications::Document(_)));
    let from_vec: UpdateModifications = vec![doc! { "$set": { "a": 1i32 } }].into();
    assert!(matches!(from_vec, UpdateModifications::Pipeline(_)));
}
