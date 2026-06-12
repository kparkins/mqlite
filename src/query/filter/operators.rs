//! Per-operator implementations dispatched by [`super::eval_single_op`] and
//! [`super::eval_operator_document`].
//!
//! Each function receives the document field value (`None` if absent) and the
//! operator argument.  Operator semantics — including array-unwrap and missing-
//! field handling — are documented at each function.

use std::{cmp::Ordering, slice};

use bson::Bson;
use regex::RegexBuilder;

use crate::error::Result;

use super::util::{
    bad_value, bson_eq, bson_to_bool, bson_to_i64_strict, compare_bson, require_array,
    require_document,
};

// ---------------------------------------------------------------------------
// Per-query regex DFA size limit
// ---------------------------------------------------------------------------

/// Maximum DFA state bytes for compiled regex patterns (10 MB).
///
/// Prevents pathological patterns from consuming excessive memory during DFA
/// compilation.  The `regex` crate uses a linear-time matching algorithm
/// (DFA/NFA hybrid with lazy construction), so this limit covers compile-time
/// cost; catastrophic backtracking at match time is architecturally impossible.
///
/// If a pattern exceeds this limit, `build_regex` returns `Error::BsonDeserialization`
/// (MongoDB error code 2 / BadValue).
const REGEX_DFA_SIZE_LIMIT: usize = 10 * 1024 * 1024;

// ---------------------------------------------------------------------------
// $eq / $ne
// ---------------------------------------------------------------------------

/// Evaluate `$eq` with array-unwrap semantics.
///
/// For an array field value, returns true if **any** element equals `target`.
/// For a scalar field value, returns true if the value equals `target`.
/// For a missing field (`None`), returns true only if `target` is `Bson::Null`.
pub(super) fn eval_eq(field_value: Option<&Bson>, target: &Bson) -> Result<bool> {
    match field_value {
        None => {
            // Missing field: matches `null` (like MongoDB).
            Ok(matches!(target, Bson::Null))
        }
        Some(val) => Ok(bson_eq(val, target)
            || matches!(val, Bson::Array(arr) if arr.iter().any(|elem| bson_eq(elem, target)))),
    }
}

// ---------------------------------------------------------------------------
// Comparison: $gt, $gte, $lt, $lte
// ---------------------------------------------------------------------------

/// Evaluate a comparison operator against `field_value`.
///
/// `direction` is the expected [`Ordering`] (e.g., `Greater` for `$gt`).
/// `allow_equal` is true for `$gte` / `$lte`.
///
/// Array fields: matches if any element satisfies the comparison.
/// Missing fields: never match.
pub(super) fn eval_cmp(
    field_value: Option<&Bson>,
    comparand: &Bson,
    direction: Ordering,
    allow_equal: bool,
) -> Result<bool> {
    let Some(val) = field_value else {
        return Ok(false);
    };
    let elems = if let Bson::Array(arr) = val {
        arr.as_slice()
    } else {
        slice::from_ref(val)
    };
    Ok(elems.iter().any(|elem| {
        let ord = compare_bson(elem, comparand);
        ord == direction || (allow_equal && ord == Ordering::Equal)
    }))
}

// ---------------------------------------------------------------------------
// $in / $nin
// ---------------------------------------------------------------------------

/// Evaluate `$in` with array-unwrap semantics.
///
/// Returns true if the field value (or any element of an array field) is
/// equal to any value in the `$in` list.
pub(super) fn eval_in(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let list = require_array("$in", arg)?;
    match field_value {
        None => {
            // Missing field: matches null in $in list.
            Ok(list.iter().any(|item| matches!(item, Bson::Null)))
        }
        Some(val) => Ok(list.iter().any(|target| {
            bson_eq(val, target)
                || matches!(val, Bson::Array(arr) if arr.iter().any(|elem| bson_eq(elem, target)))
        })),
    }
}

// ---------------------------------------------------------------------------
// $not
// ---------------------------------------------------------------------------

/// Evaluate `$not` — negate an operator sub-document.
///
/// `arg` must be a document like `{$gt: 5}`.
pub(super) fn eval_not(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let ops = require_document("$not", arg)?;
    // $not requires at least one operator.
    if ops.is_empty() {
        return Err(bad_value("$not cannot have an empty sub-expression"));
    }
    // $not negates the result of evaluating the sub-expression.
    Ok(!super::eval_operator_document(field_value, ops)?)
}

// ---------------------------------------------------------------------------
// $exists
// ---------------------------------------------------------------------------

pub(super) fn eval_exists(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    Ok(field_value.is_some() == bson_to_bool("$exists", arg)?)
}

// ---------------------------------------------------------------------------
// $type
// ---------------------------------------------------------------------------

/// Evaluate `$type`.
///
/// `arg` can be:
/// - A string type alias (e.g., `"string"`, `"int"`)
/// - A numeric BSON type ID (e.g., `2` for string, `16` for int32)
/// - An array of either (matches if field type is in the list)
pub(super) fn eval_type(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let Some(val) = field_value else {
        return Ok(false);
    };
    let specs = if let Bson::Array(type_list) = arg {
        type_list.as_slice()
    } else {
        slice::from_ref(arg)
    };
    for type_spec in specs {
        if type_matches(val, type_spec)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn type_matches(val: &Bson, type_spec: &Bson) -> Result<bool> {
    let actual_type = bson_type_id(val);
    match type_spec {
        Bson::String(name) => {
            let expected = type_name_to_id(name.as_str())?;
            Ok(actual_type == expected)
        }
        Bson::Int32(id) => Ok(actual_type == *id as i64),
        Bson::Int64(id) => Ok(actual_type == *id),
        Bson::Double(id) => Ok(actual_type == *id as i64),
        _ => Err(bad_value(
            "$type argument must be a type string, number, or array",
        )),
    }
}

/// Returns the numeric BSON type ID for a value (MongoDB spec numbers).
fn bson_type_id(val: &Bson) -> i64 {
    match val {
        Bson::Double(_) => 1,
        Bson::String(_) => 2,
        Bson::Document(_) => 3,
        Bson::Array(_) => 4,
        Bson::Binary(_) => 5,
        Bson::Undefined => 6,
        Bson::ObjectId(_) => 7,
        Bson::Boolean(_) => 8,
        Bson::DateTime(_) => 9,
        Bson::Null => 10,
        Bson::RegularExpression(_) => 11,
        Bson::DbPointer(_) => 12,
        Bson::JavaScriptCode(_) => 13,
        Bson::Symbol(_) => 14,
        Bson::JavaScriptCodeWithScope(_) => 15,
        Bson::Int32(_) => 16,
        Bson::Timestamp(_) => 17,
        Bson::Int64(_) => 18,
        Bson::Decimal128(_) => 19,
        Bson::MinKey => -1,
        Bson::MaxKey => 127,
    }
}

/// Convert a MongoDB BSON type string alias to its numeric ID.
fn type_name_to_id(name: &str) -> Result<i64> {
    let id = match name {
        "double" => 1,
        "string" => 2,
        "object" => 3,
        "array" => 4,
        "binData" => 5,
        "undefined" => 6,
        "objectId" => 7,
        "bool" => 8,
        "date" => 9,
        "null" => 10,
        "regex" => 11,
        "dbPointer" => 12,
        "javascript" => 13,
        "symbol" => 14,
        "javascriptWithScope" => 15,
        "int" => 16,
        "timestamp" => 17,
        "long" => 18,
        "decimal" => 19,
        "minKey" => -1,
        "maxKey" => 127,
        "number" => {
            return Err(bad_value(
                "type alias 'number' is not supported in $type; use an array of type IDs instead",
            ));
        }
        other => return Err(bad_value(&format!("unknown $type name: \"{other}\""))),
    };
    Ok(id)
}

// ---------------------------------------------------------------------------
// $elemMatch
// ---------------------------------------------------------------------------

/// Evaluate `$elemMatch` — a single array element must satisfy all conditions.
///
/// Only array-typed fields can match; scalars and missing fields never match.
///
/// If any top-level key in `arg` starts with `$`, the operators are applied
/// directly to each element (e.g., `{$gt: 5, $lt: 10}` tests each number).
/// Otherwise the element must be a sub-document matching `arg` as a filter.
pub(super) fn eval_elem_match(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let cond_doc = require_document("$elemMatch", arg)?;
    let arr = match field_value {
        Some(Bson::Array(a)) => a,
        _ => return Ok(false), // missing or non-array — no match
    };
    let is_operator_mode = cond_doc.keys().any(|k| k.starts_with('$'));
    for elem in arr {
        let matched = if is_operator_mode {
            // Apply operator conditions directly to the element value.
            super::eval_operator_document(Some(elem), cond_doc)?
        } else {
            // Element must be a document matching the sub-filter.
            match elem {
                Bson::Document(sub_doc) => super::eval_filter(sub_doc, cond_doc)?,
                _ => false,
            }
        };
        if matched {
            return Ok(true);
        }
    }
    Ok(false)
}

// ---------------------------------------------------------------------------
// $all
// ---------------------------------------------------------------------------

/// Evaluate `$all` — every value in the list must appear in the field array.
///
/// For a scalar field, the field is treated as a single-element array
/// (matching MongoDB 8.0 behaviour for `{a: {$all: [v]}}` vs `{a: v}`).
///
/// Returns `false` for an empty `$all` list or a missing field.
///
/// Each element in the `$all` list may itself be an `{$elemMatch: ...}` document;
/// in that case the sub-condition is evaluated against the whole field array.
pub(super) fn eval_all(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let required = require_array("$all", arg)?;
    if required.is_empty() {
        // An empty $all matches no documents.
        return Ok(false);
    }
    match field_value {
        None => Ok(false),
        Some(arr_bson @ Bson::Array(arr)) => {
            for req_val in required {
                // Check for $all: [{$elemMatch: {...}}] syntax.
                let elem_match_arg = match req_val {
                    Bson::Document(cond) => cond.get("$elemMatch"),
                    _ => None,
                };
                let found = if let Some(em_arg) = elem_match_arg {
                    eval_elem_match(Some(arr_bson), em_arg)?
                } else {
                    arr.iter().any(|elem| bson_eq(elem, req_val))
                };
                if !found {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Some(scalar) => Ok(required.iter().all(|req_val| bson_eq(scalar, req_val))),
    }
}

// ---------------------------------------------------------------------------
// $size
// ---------------------------------------------------------------------------

/// Evaluate `$size` — field array must have exactly N elements.
///
/// Only array-typed fields can match.  Missing fields and scalar fields never
/// match.  `N` must be a non-negative integer; fractional values are rejected.
pub(super) fn eval_size(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let n = bson_to_i64_strict("$size", arg)?;
    if n < 0 {
        return Err(bad_value("$size must be a non-negative integer"));
    }
    match field_value {
        Some(Bson::Array(arr)) => Ok(arr.len() as i64 == n),
        _ => Ok(false),
    }
}

// ---------------------------------------------------------------------------
// $mod
// ---------------------------------------------------------------------------

/// Number of elements MongoDB requires in a `$mod` argument array.
const MOD_ARG_LEN: usize = 2;

/// Evaluate `$mod` — `{field: {$mod: [divisor, remainder]}}`.
///
/// The argument must be a two-element array of numbers. `Double` divisor and
/// remainder are truncated toward zero (e.g. `4.5` becomes `4`). Matching uses
/// Rust's `%` on `i64`, which is C-style truncated division and therefore
/// agrees with MongoDB for negative operands.
///
/// Only numeric field values can match. `Double` field values are truncated
/// toward zero before the modulo; non-finite doubles never match. Array fields
/// unwrap: the operator matches if any element matches. A missing field or a
/// non-numeric value yields no match and no error.
///
/// # Errors
///
/// Returns `BadValue` when the argument is not an array
/// (`"malformed mod, needs to be an array"`), has fewer than two elements
/// (`"malformed mod, not enough elements"`), has more than two elements
/// (`"malformed mod, too many elements"`), when the divisor or remainder is
/// non-numeric or non-finite, or when the divisor truncates to zero
/// (`"divisor cannot be 0"`).
pub(super) fn eval_mod(field_value: Option<&Bson>, arg: &Bson) -> Result<bool> {
    let Bson::Array(arr) = arg else {
        return Err(bad_value("malformed mod, needs to be an array"));
    };
    if arr.len() < MOD_ARG_LEN {
        return Err(bad_value("malformed mod, not enough elements"));
    }
    if arr.len() > MOD_ARG_LEN {
        return Err(bad_value("malformed mod, too many elements"));
    }
    let divisor = mod_operand_to_i64(&arr[0])?;
    let remainder = mod_operand_to_i64(&arr[1])?;
    if divisor == 0 {
        return Err(bad_value("divisor cannot be 0"));
    }

    let val = match field_value {
        Some(v) => v,
        None => return Ok(false),
    };
    let elems = if let Bson::Array(elements) = val {
        elements.as_slice()
    } else {
        slice::from_ref(val)
    };
    Ok(elems
        .iter()
        .filter_map(mod_field_value_to_i64)
        .any(|v| v % divisor == remainder))
}

/// Convert a `$mod` divisor or remainder argument to `i64`.
///
/// Accepts `Int32`, `Int64`, and finite `Double` (truncated toward zero).
///
/// # Errors
///
/// Returns `BadValue` for non-numeric values and for `NaN`/`Infinity` doubles.
fn mod_operand_to_i64(val: &Bson) -> Result<i64> {
    match val {
        Bson::Int32(n) => Ok(*n as i64),
        Bson::Int64(n) => Ok(*n),
        Bson::Double(f) if f.is_finite() => Ok(*f as i64),
        _ => Err(bad_value(
            "malformed mod, divisor and remainder must be numbers",
        )),
    }
}

/// Convert a `$mod` field value to `i64`, or `None` if it cannot match.
///
/// Accepts `Int32`, `Int64`, and finite `Double` (truncated toward zero).
/// Non-numeric values and non-finite doubles return `None` (no match).
fn mod_field_value_to_i64(val: &Bson) -> Option<i64> {
    match val {
        Bson::Int32(n) => Some(*n as i64),
        Bson::Int64(n) => Some(*n),
        Bson::Double(f) if f.is_finite() => Some(*f as i64),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Bit-test operators: $bitsAllSet, $bitsAnySet, $bitsAllClear, $bitsAnyClear
// ---------------------------------------------------------------------------

/// The bit-test mode requested by a `$bits*` operator.
#[derive(Clone, Copy)]
pub(super) enum BitTest {
    /// `$bitsAllSet` — every mask bit must be set in the value.
    AllSet,
    /// `$bitsAnySet` — at least one mask bit must be set in the value.
    AnySet,
    /// `$bitsAllClear` — every mask bit must be clear in the value.
    AllClear,
    /// `$bitsAnyClear` — at least one mask bit must be clear in the value.
    AnyClear,
}

impl BitTest {
    /// The operator name, used in error messages.
    fn op_name(self) -> &'static str {
        match self {
            BitTest::AllSet => "$bitsAllSet",
            BitTest::AnySet => "$bitsAnySet",
            BitTest::AllClear => "$bitsAllClear",
            BitTest::AnyClear => "$bitsAnyClear",
        }
    }
}

/// Number of bits in a byte.
const BITS_PER_BYTE: usize = 8;

/// Evaluate a bit-test operator (`$bitsAllSet`, `$bitsAnySet`,
/// `$bitsAllClear`, `$bitsAnyClear`) against `field_value`.
///
/// The mask argument is one of: a non-negative numeric value (`Int32`,
/// `Int64`, or `Double` with a non-negative integral value), an array of
/// non-negative integer bit positions, or a `BinData` little-endian bit
/// string (byte `i` holds bit positions `[8*i, 8*i+8)`, least-significant-bit
/// first within each byte).
///
/// Only `Int32`, `Int64`, integral `Double` representable in `i64`, and
/// `BinData` field values pass the type gate; every other value yields no
/// match and no error. Numeric field values are treated as 64-bit two's
/// complement with infinite sign extension (negative values have every bit at
/// position `>= 64` set); `BinData` field values have every bit beyond their
/// length clear. Array fields unwrap: the operator matches if any element
/// matches.
///
/// # Errors
///
/// Returns `BadValue` for a negative or fractional numeric mask, a bit-position
/// array containing a negative, fractional, or non-numeric element, or a mask
/// argument of any other type.
pub(super) fn eval_bits(field_value: Option<&Bson>, arg: &Bson, test: BitTest) -> Result<bool> {
    let mask = BitMask::from_arg(arg, test)?;
    let val = match field_value {
        Some(v) => v,
        None => return Ok(false),
    };
    let elems = if let Bson::Array(elements) = val {
        elements.as_slice()
    } else {
        slice::from_ref(val)
    };
    Ok(elems.iter().any(|elem| mask.matches(elem, test)))
}

/// A normalised bit mask: the sorted, de-duplicated set of bit positions to
/// test. Positions may exceed 63 (only meaningful against negative numeric
/// values via sign extension).
struct BitMask {
    positions: Vec<u64>,
}

impl BitMask {
    /// Build a [`BitMask`] from a `$bits*` argument.
    ///
    /// # Errors
    ///
    /// Returns `BadValue` for invalid numeric, array, or unsupported mask
    /// argument types (see [`eval_bits`]).
    fn from_arg(arg: &Bson, test: BitTest) -> Result<Self> {
        let op = test.op_name();
        let positions = match arg {
            Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) => {
                bit_positions_from_numeric_mask(arg, op)?
            }
            Bson::Array(arr) => bit_positions_from_array_mask(arr, op)?,
            Bson::Binary(bin) => bit_positions_from_bytes(&bin.bytes),
            _ => {
                return Err(bad_value(&format!(
                    "{op} takes an integer, array, or BinData mask"
                )))
            }
        };
        Ok(BitMask { positions })
    }

    /// Test the mask against a single field value under `test` semantics.
    ///
    /// Returns `false` for any value that fails the type gate.
    fn matches(&self, val: &Bson, test: BitTest) -> bool {
        let Some(is_set) = bit_reader(val) else {
            return false;
        };
        match test {
            BitTest::AllSet => self.positions.iter().all(|&p| is_set(p)),
            BitTest::AnySet => self.positions.iter().any(|&p| is_set(p)),
            BitTest::AllClear => self.positions.iter().all(|&p| !is_set(p)),
            BitTest::AnyClear => self.positions.iter().any(|&p| !is_set(p)),
        }
    }
}

/// Extract bit positions from a numeric mask argument.
///
/// # Errors
///
/// Returns `BadValue` for a negative value or a fractional `Double`.
fn bit_positions_from_numeric_mask(arg: &Bson, op: &str) -> Result<Vec<u64>> {
    let bits = numeric_mask_to_u64(arg, op)?;
    Ok((0..u64::BITS as u64)
        .filter(|&p| bits & (1 << p) != 0)
        .collect())
}

/// Convert a numeric `$bits*` mask to its `u64` bit pattern.
///
/// # Errors
///
/// Returns `BadValue` for a negative value or a non-integral `Double`.
fn numeric_mask_to_u64(arg: &Bson, op: &str) -> Result<u64> {
    match arg {
        Bson::Int32(n) if *n >= 0 => Ok(*n as u64),
        Bson::Int64(n) if *n >= 0 => Ok(*n as u64),
        Bson::Double(f) if f.is_finite() && *f >= 0.0 && f.fract() == 0.0 => Ok(*f as u64),
        _ => Err(bad_value(&format!(
            "{op} numeric mask must be a non-negative integer"
        ))),
    }
}

/// Extract bit positions from a bit-position array mask argument.
///
/// # Errors
///
/// Returns `BadValue` for any negative, fractional, or non-numeric element.
fn bit_positions_from_array_mask(arr: &[Bson], op: &str) -> Result<Vec<u64>> {
    let mut positions = Vec::with_capacity(arr.len());
    for elem in arr {
        positions.push(bit_position_value(elem, op)?);
    }
    Ok(positions)
}

/// Convert a single bit-position array element to a `u64` position.
///
/// # Errors
///
/// Returns `BadValue` for a negative, fractional, or non-numeric element.
fn bit_position_value(elem: &Bson, op: &str) -> Result<u64> {
    let position = match elem {
        Bson::Int32(n) if *n >= 0 => *n as u64,
        Bson::Int64(n) if *n >= 0 => *n as u64,
        Bson::Double(f) if f.is_finite() && *f >= 0.0 && f.fract() == 0.0 => *f as u64,
        _ => {
            return Err(bad_value(&format!(
                "{op} bit positions must be non-negative integers"
            )))
        }
    };
    Ok(position)
}

/// Extract the set bit positions from a little-endian `BinData` byte slice.
///
/// Byte `i` holds positions `[8*i, 8*i+8)`, least-significant-bit first.
fn bit_positions_from_bytes(bytes: &[u8]) -> Vec<u64> {
    let mut positions = Vec::new();
    for (i, byte) in bytes.iter().enumerate() {
        for bit in 0..BITS_PER_BYTE {
            if byte & (1 << bit) != 0 {
                positions.push((i * BITS_PER_BYTE + bit) as u64);
            }
        }
    }
    positions
}

/// Build a "is bit at position set?" reader for a bit-testable field value.
///
/// Returns `None` for values that fail the type gate (fractional doubles,
/// non-finite doubles, doubles outside `i64`, and all non-numeric, non-BinData
/// types).
///
/// Numeric values are read as 64-bit two's complement with infinite sign
/// extension. `BinData` values are read little-endian, with every bit beyond
/// the data length treated as clear.
fn bit_reader(val: &Bson) -> Option<Box<dyn Fn(u64) -> bool + '_>> {
    match val {
        Bson::Int32(n) => {
            let bits = *n as i64;
            Some(Box::new(move |p| signed_bit_set(bits, p)))
        }
        Bson::Int64(n) => {
            let bits = *n;
            Some(Box::new(move |p| signed_bit_set(bits, p)))
        }
        Bson::Double(f) if f.is_finite() && f.fract() == 0.0 && double_fits_i64(*f) => {
            let bits = *f as i64;
            Some(Box::new(move |p| signed_bit_set(bits, p)))
        }
        Bson::Binary(bin) => Some(Box::new(move |p| binary_bit_set(&bin.bytes, p))),
        _ => None,
    }
}

/// Return whether `f` is exactly representable as an `i64`.
fn double_fits_i64(f: f64) -> bool {
    f >= i64::MIN as f64 && f <= i64::MAX as f64
}

/// Return whether bit `position` is set in a 64-bit two's-complement integer
/// with infinite sign extension.
///
/// For `position >= 64` the result is the sign bit: negative values report
/// every high bit as set, non-negative values as clear.
fn signed_bit_set(bits: i64, position: u64) -> bool {
    if position >= u64::BITS as u64 {
        return bits < 0;
    }
    bits & (1_i64 << position) != 0
}

/// Return whether bit `position` is set in a little-endian `BinData` byte
/// slice. Positions beyond the data length are clear.
fn binary_bit_set(bytes: &[u8], position: u64) -> bool {
    let byte_index = (position / BITS_PER_BYTE as u64) as usize;
    let Some(byte) = bytes.get(byte_index) else {
        return false;
    };
    let bit_in_byte = position % BITS_PER_BYTE as u64;
    byte & (1 << bit_in_byte) != 0
}

// ---------------------------------------------------------------------------
// $regex
// ---------------------------------------------------------------------------

/// Evaluate `$regex` — field string must match the given pattern.
///
/// Only `String`-typed field values (or array elements that are strings) are
/// tested against the pattern.  Non-string values are skipped.
///
/// `options` is a string of regex flag characters:
/// - `i` — case-insensitive
/// - `m` — multiline (`^`/`$` match line boundaries)
/// - `s` — dotall (`.` matches `\n`)
/// - `x` — extended / verbose (whitespace and `#` comments are ignored)
///
/// **PCRE incompatibilities**: the Rust `regex` crate does not support
/// lookahead, lookbehind, atomic groups, possessive quantifiers, named
/// backreferences, conditional patterns, or recursive patterns.  Patterns
/// using these constructs will fail to compile.
pub(super) fn eval_regex(field_value: Option<&Bson>, pattern: &str, options: &str) -> Result<bool> {
    let re = build_regex(pattern, options)?;
    match field_value {
        None => Ok(false),
        Some(Bson::String(s)) => Ok(re.is_match(s)),
        Some(Bson::Array(arr)) => {
            // Array field: match if any string element matches.
            Ok(arr
                .iter()
                .any(|elem| matches!(elem, Bson::String(s) if re.is_match(s))))
        }
        Some(_) => Ok(false), // non-string, non-array — no match
    }
}

/// Compile a regex pattern with the given option flags.
///
/// Uses [`RegexBuilder`] with a DFA size cap ([`REGEX_DFA_SIZE_LIMIT`]) to
/// prevent compile-time memory explosion on pathological patterns.
fn build_regex(pattern: &str, options: &str) -> Result<regex::Regex> {
    let mut b = RegexBuilder::new(pattern);
    b.size_limit(REGEX_DFA_SIZE_LIMIT);
    b.dfa_size_limit(REGEX_DFA_SIZE_LIMIT);
    for flag in options.chars() {
        match flag {
            'i' => {
                b.case_insensitive(true);
            }
            'm' => {
                b.multi_line(true);
            }
            's' => {
                b.dot_matches_new_line(true);
            }
            'x' => {
                b.ignore_whitespace(true);
            }
            // 'l' (locale) and 'u' (unicode) are accepted but no-op:
            // the regex crate is Unicode-aware by default and has no
            // locale concept.
            'l' | 'u' => {}
            other => {
                return Err(bad_value(&format!("unknown $regex option '{other}'")));
            }
        }
    }
    b.build()
        .map_err(|e| bad_value(&format!("invalid $regex pattern: {e}")))
}
