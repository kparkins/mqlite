//! MongoDB 8.0 aggregation-expression evaluator.
//!
//! Evaluates aggregation expressions (the `$`-prefixed mini-language used by
//! `$project`, `$addFields`, `$group`, `$expr`, and pipeline-form updates)
//! against a current document plus a small variable scope. This module is the
//! shared foundation for those consumers; it is intentionally standalone and
//! is not wired into any pipeline stage here.
//!
//! # Value model
//!
//! Evaluation yields [`Value`] = `Option<Bson>` where `None` means *missing*
//! (the field/expression produced no value) and `Some(Bson::Null)` means an
//! explicit null. Distinguishing the two matters for computed documents
//! (missing fields are omitted) and for `$ifNull` / truthiness.
//!
//! # Field paths
//!
//! A `$`-prefixed string is a dotted field path on the current document. Like
//! the existing `$group` expression engine, mqlite does **not** perform the
//! server's implicit array traversal: a path that descends into an array of
//! sub-documents yields *missing* rather than collecting matches into an
//! array. This is a deliberate divergence from MongoDB server semantics.
//!
//! # Divergences from MongoDB 8.0 server
//!
//! - Field paths do not traverse arrays (see above).
//! - `$trunc` accepts only the 1-argument form (no place argument).
//! - `$convert` is not implemented.
//! - Date operators are UTC only; no `timezone` option is accepted.
//! - `$toDate` accepts numeric milliseconds only; strings are not supported.
//!
//! # Integration status
//!
//! The public surface (`eval_expr`, `eval_expr_to_bool`, `ExprContext`) is
//! consumed by the aggregation pipeline (`$group`, `$project`, `$addFields`/
//! `$set`, `$replaceRoot`/`$replaceWith`, `$sortByCount`). The unit tests
//! fully exercise every operator.

use std::cmp::Ordering;

use bson::{Bson, DateTime, Document};

use crate::error::{Error, Result};
use crate::keys::encode_key;
use crate::query::get_nested_field;

/// Result of evaluating an aggregation expression.
///
/// `None` represents *missing* (no value produced); `Some(Bson::Null)`
/// represents an explicit null. Keeping them distinct is required for
/// computed-document field omission and `$ifNull` semantics.
pub(crate) type Value = Option<Bson>;

/// Prefix marking a variable reference (e.g. `$$ROOT`, `$$this.field`).
const VAR_PREFIX: &str = "$$";

/// Prefix marking a field-path reference (e.g. `$a.b`).
const FIELD_PREFIX: char = '$';

/// Number of milliseconds in one second (date arithmetic / extraction).
const MILLIS_PER_SEC: i64 = 1000;
/// Number of seconds in one minute.
const SECS_PER_MIN: i64 = 60;
/// Number of minutes in one hour.
const MINS_PER_HOUR: i64 = 60;
/// Number of hours in one day.
const HOURS_PER_DAY: i64 = 24;
/// Number of seconds in one day.
const SECS_PER_DAY: i64 = SECS_PER_MIN * MINS_PER_HOUR * HOURS_PER_DAY;
/// Number of milliseconds in one day.
const MILLIS_PER_DAY: i64 = SECS_PER_DAY * MILLIS_PER_SEC;

// ---------------------------------------------------------------------------
// Evaluation context
// ---------------------------------------------------------------------------

/// A single `name -> value` variable binding in the evaluation scope.
type VarBinding = (String, Bson);

/// Evaluation context for [`eval_expr`].
///
/// Holds the current document (`$$ROOT` / `$$CURRENT` and the target of
/// `$`-field paths), a frozen `$$NOW` timestamp captured at construction, and
/// a stack of user variable bindings (`$$this`, custom `as` names, etc.).
pub(crate) struct ExprContext<'a> {
    /// The current document — target of field paths and `$$ROOT`/`$$CURRENT`.
    root: &'a Document,
    /// The `$$NOW` value, frozen for the whole evaluation.
    now: DateTime,
    /// User variable bindings, innermost-last (later entries shadow earlier).
    vars: Vec<VarBinding>,
}

impl<'a> ExprContext<'a> {
    /// Create a context rooted at `doc` with `$$NOW` frozen to the wall clock.
    pub(crate) fn new(doc: &'a Document) -> Self {
        Self {
            root: doc,
            now: DateTime::now(),
            vars: Vec::new(),
        }
    }

    /// Create a context rooted at `doc` with an explicit `$$NOW` value.
    ///
    /// Used by callers that must share one `$$NOW` across many evaluations
    /// (e.g. a whole pipeline) or by tests needing a deterministic clock.
    pub(crate) fn with_now(doc: &'a Document, now: DateTime) -> Self {
        Self {
            root: doc,
            now,
            vars: Vec::new(),
        }
    }

    /// Return a child context that additionally binds `name` to `value`.
    ///
    /// The current document and frozen `$$NOW` are shared with the parent; the
    /// variable frame is cloned and extended so the parent scope is unchanged.
    /// Lookups consult the innermost binding first, so re-binding an existing
    /// name shadows it. Used by `$map`/`$filter` (which bind `$$this` or the
    /// `as` name) and available to external consumers binding their own
    /// `let`-style variables.
    pub(crate) fn with_var(&self, name: &str, value: Bson) -> ExprContext<'a> {
        let mut vars = self.vars.clone();
        vars.push((name.to_owned(), value));
        ExprContext {
            root: self.root,
            now: self.now,
            vars,
        }
    }

    /// Look up a user variable binding by name (innermost wins).
    fn lookup_var(&self, name: &str) -> Option<&Bson> {
        self.vars
            .iter()
            .rev()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v)
    }
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

/// Build a `BadValue` (MongoDB code 2) error carrying `msg`.
///
/// Mirrors the filter/aggregate layers, which thread `BadValue` text through
/// [`Error::BsonDeserialization`] so the wire layer maps them to code 2.
fn bad_value(msg: impl Into<String>) -> Error {
    use serde::de::Error as _;
    Error::BsonDeserialization(bson::de::Error::custom(format!("BadValue: {}", msg.into())))
}

/// The MongoDB BSON type name for `val`, as reported by `$type`.
fn bson_type_name(val: &Bson) -> &'static str {
    match val {
        Bson::Double(_) => "double",
        Bson::String(_) => "string",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::Binary(_) => "binData",
        Bson::Undefined => "undefined",
        Bson::ObjectId(_) => "objectId",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Null => "null",
        Bson::RegularExpression(_) => "regex",
        Bson::DbPointer(_) => "dbPointer",
        Bson::JavaScriptCode(_) => "javascript",
        Bson::Symbol(_) => "symbol",
        Bson::JavaScriptCodeWithScope(_) => "javascriptWithScope",
        Bson::Int32(_) => "int",
        Bson::Timestamp(_) => "timestamp",
        Bson::Int64(_) => "long",
        Bson::Decimal128(_) => "decimal",
        Bson::MinKey => "minKey",
        Bson::MaxKey => "maxKey",
    }
}

// ---------------------------------------------------------------------------
// BSON comparison / equality (total order via key encoding)
// ---------------------------------------------------------------------------

/// Compare two BSON values using MongoDB's canonical total order.
fn compare_bson(a: &Bson, b: &Bson) -> Ordering {
    encode_key(a).cmp(&encode_key(b))
}

/// Return true if two BSON values are equal under MongoDB's ordering.
fn bson_eq(a: &Bson, b: &Bson) -> bool {
    encode_key(a) == encode_key(b)
}

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

/// Evaluate an aggregation `expr` against `ctx`.
///
/// Returns [`Value`] (`None` = missing, `Some(Bson::Null)` = explicit null).
///
/// # Errors
///
/// Returns [`Error::BsonDeserialization`] (a `BadValue`) for malformed
/// expressions, arity violations, and type errors, and
/// [`Error::UnsupportedOperator`] for unrecognized `$`-operators (the message
/// uses MongoDB's `Unrecognized expression '<op>'` text).
pub(crate) fn eval_expr(expr: &Bson, ctx: &ExprContext) -> Result<Value> {
    match expr {
        Bson::String(s) => eval_string(s, ctx),
        Bson::Document(doc) => eval_document(doc, ctx),
        Bson::Array(items) => eval_array(items, ctx),
        // Numbers, bools, null, dates, ObjectId, etc. are self-evaluating.
        other => Ok(Some(other.clone())),
    }
}

/// Evaluate `expr` and apply MongoDB truthiness.
///
/// `false`, numeric zero (`Int32`/`Int64`/`Double`), `null`, and *missing*
/// are falsy; every other value is truthy. Used by `$expr`, `$cond`,
/// `$filter`, and the boolean operators.
///
/// # Errors
///
/// Propagates any error from [`eval_expr`].
pub(crate) fn eval_expr_to_bool(expr: &Bson, ctx: &ExprContext) -> Result<bool> {
    Ok(is_truthy(&eval_expr(expr, ctx)?))
}

/// MongoDB truthiness of an evaluated [`Value`].
fn is_truthy(value: &Value) -> bool {
    match value {
        None => false,
        Some(Bson::Null) => false,
        Some(Bson::Boolean(b)) => *b,
        Some(Bson::Int32(n)) => *n != 0,
        Some(Bson::Int64(n)) => *n != 0,
        Some(Bson::Double(n)) => *n != 0.0,
        Some(_) => true,
    }
}

// ---------------------------------------------------------------------------
// String expressions: variables and field paths
// ---------------------------------------------------------------------------

/// Evaluate a BSON string expression: a variable, a field path, or a literal.
fn eval_string(s: &str, ctx: &ExprContext) -> Result<Value> {
    if let Some(rest) = s.strip_prefix(VAR_PREFIX) {
        return eval_variable(rest, ctx);
    }
    if let Some(path) = s.strip_prefix(FIELD_PREFIX) {
        return Ok(get_nested_field(ctx.root, path).cloned());
    }
    Ok(Some(Bson::String(s.to_owned())))
}

/// Evaluate a `$$`-variable reference (`name` is everything after `$$`).
///
/// Supports the system variables `ROOT`, `CURRENT` (alias of `ROOT`), and
/// `NOW`, plus user-bound variables with an optional dotted suffix
/// (`this.field`). Unknown variables raise the server's
/// `Use of undefined variable` error.
fn eval_variable(name: &str, ctx: &ExprContext) -> Result<Value> {
    let mut parts = name.splitn(2, '.');
    let head = parts.next().unwrap_or("");
    let suffix = parts.next();

    let base: Bson = match head {
        "ROOT" | "CURRENT" => Bson::Document(ctx.root.clone()),
        "NOW" => Bson::DateTime(ctx.now),
        other => match ctx.lookup_var(other) {
            Some(value) => value.clone(),
            None => {
                return Err(bad_value(format!("Use of undefined variable: {other}")));
            }
        },
    };

    match suffix {
        None => Ok(Some(base)),
        Some(path) => match &base {
            Bson::Document(doc) => Ok(get_nested_field(doc, path).cloned()),
            // A dotted suffix on a non-document base yields missing.
            _ => Ok(None),
        },
    }
}

// ---------------------------------------------------------------------------
// Array and document expressions
// ---------------------------------------------------------------------------

/// Evaluate an array expression: each element is evaluated; *missing*
/// elements become explicit `null` inside the resulting array.
fn eval_array(items: &[Bson], ctx: &ExprContext) -> Result<Value> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        out.push(eval_expr(item, ctx)?.unwrap_or(Bson::Null));
    }
    Ok(Some(Bson::Array(out)))
}

/// Evaluate a document expression: an operator call, a `$literal`, or a
/// computed document.
fn eval_document(doc: &Document, ctx: &ExprContext) -> Result<Value> {
    let dollar_keys = doc.keys().filter(|k| k.starts_with('$')).count();

    if dollar_keys == 0 {
        return eval_computed_document(doc, ctx);
    }

    if doc.len() != 1 {
        return Err(bad_value(
            "an expression specification must contain exactly one field",
        ));
    }

    // Exactly one field and it is `$`-prefixed: an operator expression.
    let Some((op, arg)) = doc.iter().next() else {
        return Err(bad_value(
            "an expression specification must contain exactly one field",
        ));
    };
    if op == "$literal" {
        return Ok(Some(arg.clone()));
    }
    eval_operator(op, arg, ctx)
}

/// Evaluate a computed document `{k: <expr>, ...}` with no `$`-prefixed keys.
///
/// Each value is evaluated; fields whose value is *missing* are omitted.
fn eval_computed_document(doc: &Document, ctx: &ExprContext) -> Result<Value> {
    let mut out = Document::new();
    for (key, value_expr) in doc.iter() {
        if let Some(value) = eval_expr(value_expr, ctx)? {
            out.insert(key.clone(), value);
        }
    }
    Ok(Some(Bson::Document(out)))
}

// ---------------------------------------------------------------------------
// Operator dispatch
// ---------------------------------------------------------------------------

/// Dispatch a single `$`-operator to its implementation.
fn eval_operator(op: &str, arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    match op {
        // ---- Comparison ----
        "$eq" => cmp_op(op, arg, ctx, |o| o == Ordering::Equal),
        "$ne" => cmp_op(op, arg, ctx, |o| o != Ordering::Equal),
        "$gt" => cmp_op(op, arg, ctx, |o| o == Ordering::Greater),
        "$gte" => cmp_op(op, arg, ctx, |o| o != Ordering::Less),
        "$lt" => cmp_op(op, arg, ctx, |o| o == Ordering::Less),
        "$lte" => cmp_op(op, arg, ctx, |o| o != Ordering::Greater),
        "$cmp" => eval_cmp(arg, ctx),

        // ---- Arithmetic ----
        "$add" => eval_add(arg, ctx),
        "$subtract" => eval_subtract(arg, ctx),
        "$multiply" => eval_multiply(arg, ctx),
        "$divide" => eval_divide(arg, ctx),
        "$mod" => eval_mod(arg, ctx),
        "$abs" => unary_num(op, arg, ctx, math_abs),
        "$ceil" => unary_num(op, arg, ctx, |v| Ok(num_ceil(v))),
        "$floor" => unary_num(op, arg, ctx, |v| Ok(num_floor(v))),
        "$trunc" => unary_num(op, arg, ctx, |v| Ok(num_trunc(v))),
        "$round" => eval_round(arg, ctx),
        "$sqrt" => unary_num(op, arg, ctx, math_sqrt),
        "$exp" => unary_num(op, arg, ctx, |v| Ok(Bson::Double(num_as_f64(v).exp()))),
        "$ln" => unary_num(op, arg, ctx, |v| math_log(v, f64::ln, "$ln")),
        "$log10" => unary_num(op, arg, ctx, |v| math_log(v, f64::log10, "$log10")),
        "$pow" => eval_pow(arg, ctx),

        // ---- Boolean ----
        "$and" => eval_and(arg, ctx),
        "$or" => eval_or(arg, ctx),
        "$not" => eval_not(arg, ctx),

        // ---- Conditional ----
        "$cond" => eval_cond(arg, ctx),
        "$ifNull" => eval_if_null(arg, ctx),
        "$switch" => eval_switch(arg, ctx),

        // ---- String ----
        "$concat" => eval_concat(arg, ctx),
        "$toUpper" => str_case(op, arg, ctx, true),
        "$toLower" => str_case(op, arg, ctx, false),
        "$strLenCP" => eval_str_len_cp(arg, ctx),
        "$substrCP" => eval_substr_cp(arg, ctx),
        "$split" => eval_split(arg, ctx),
        "$trim" => eval_trim(arg, ctx, true, true),
        "$ltrim" => eval_trim(arg, ctx, true, false),
        "$rtrim" => eval_trim(arg, ctx, false, true),
        "$toString" => eval_to_string(arg, ctx),

        // ---- Array ----
        "$size" => eval_size(arg, ctx),
        "$isArray" => eval_is_array(arg, ctx),
        "$in" => eval_in(arg, ctx),
        "$arrayElemAt" => eval_array_elem_at(arg, ctx),
        "$first" => eval_first_last(op, arg, ctx, false),
        "$last" => eval_first_last(op, arg, ctx, true),
        "$concatArrays" => eval_concat_arrays(arg, ctx),
        "$slice" => eval_slice(arg, ctx),
        "$filter" => eval_filter_expr(arg, ctx),
        "$map" => eval_map(arg, ctx),
        "$range" => eval_range(arg, ctx),

        // ---- Type / conversion ----
        "$type" => eval_type(arg, ctx),
        "$toInt" => to_number(op, arg, ctx, NumericTarget::Int),
        "$toLong" => to_number(op, arg, ctx, NumericTarget::Long),
        "$toDouble" => to_number(op, arg, ctx, NumericTarget::Double),
        "$toBool" => eval_to_bool(arg, ctx),
        "$toDate" => eval_to_date(arg, ctx),

        // ---- Date extraction ----
        "$year" => date_part(op, arg, ctx, DatePart::Year),
        "$month" => date_part(op, arg, ctx, DatePart::Month),
        "$dayOfMonth" => date_part(op, arg, ctx, DatePart::DayOfMonth),
        "$hour" => date_part(op, arg, ctx, DatePart::Hour),
        "$minute" => date_part(op, arg, ctx, DatePart::Minute),
        "$second" => date_part(op, arg, ctx, DatePart::Second),
        "$millisecond" => date_part(op, arg, ctx, DatePart::Millisecond),
        "$dayOfWeek" => date_part(op, arg, ctx, DatePart::DayOfWeek),
        "$dayOfYear" => date_part(op, arg, ctx, DatePart::DayOfYear),

        // ---- Misc ----
        "$rand" => eval_rand(arg),

        other => Err(Error::UnsupportedOperator {
            operator: format!("Unrecognized expression '{other}'"),
        }),
    }
}

// ---------------------------------------------------------------------------
// Argument helpers
// ---------------------------------------------------------------------------

/// Collect operator arguments into a slice view.
///
/// An array argument is the argument list; any other single value is treated
/// as a one-element argument list (matching server convenience syntax such as
/// `{$toUpper: "$x"}`).
fn arg_list<'b>(arg: &'b Bson) -> std::borrow::Cow<'b, [Bson]> {
    match arg {
        Bson::Array(items) => std::borrow::Cow::Borrowed(items),
        single => std::borrow::Cow::Owned(vec![single.clone()]),
    }
}

/// Evaluate every element of an argument list.
fn eval_all(items: &[Bson], ctx: &ExprContext) -> Result<Vec<Value>> {
    items.iter().map(|e| eval_expr(e, ctx)).collect()
}

/// Validate exact arity, returning a `BadValue` with the server's message.
fn require_arity(op: &str, items: &[Bson], n: usize) -> Result<()> {
    if items.len() == n {
        return Ok(());
    }
    Err(bad_value(format!(
        "Expression {op} takes exactly {n} arguments. {} were passed in.",
        items.len()
    )))
}

/// Validate a minimum arity, returning a `BadValue` on shortfall.
fn require_min_arity(op: &str, items: &[Bson], n: usize) -> Result<()> {
    if items.len() >= n {
        return Ok(());
    }
    Err(bad_value(format!(
        "Expression {op} takes at least {n} arguments. {} were passed in.",
        items.len()
    )))
}

/// True when a [`Value`] is missing or explicit null.
fn is_null_or_missing(v: &Value) -> bool {
    matches!(v, None | Some(Bson::Null))
}

// ---------------------------------------------------------------------------
// Numeric helpers
// ---------------------------------------------------------------------------

/// A numeric BSON value normalized for arithmetic.
#[derive(Clone, Copy)]
enum Num {
    /// An integral value that originated from `Int32` or `Int64`.
    Int(i64),
    /// A floating-point value (or any value promoted to floating point).
    Float(f64),
}

/// Interpret a BSON value as a [`Num`], if it is numeric.
fn as_num(val: &Bson) -> Option<Num> {
    match val {
        Bson::Int32(n) => Some(Num::Int(*n as i64)),
        Bson::Int64(n) => Some(Num::Int(*n)),
        Bson::Double(f) => Some(Num::Float(*f)),
        _ => None,
    }
}

/// The `f64` magnitude of a [`Num`].
fn num_as_f64(n: Num) -> f64 {
    match n {
        Num::Int(i) => i as f64,
        Num::Float(f) => f,
    }
}

/// Convert a [`Num`] back to its narrowest BSON representation.
///
/// Integral values that fit in `i32` become `Int32`; wider integral values
/// become `Int64`; floating values become `Double`.
fn num_to_bson(n: Num) -> Bson {
    match n {
        Num::Int(i) => {
            if let Ok(narrow) = i32::try_from(i) {
                Bson::Int32(narrow)
            } else {
                Bson::Int64(i)
            }
        }
        Num::Float(f) => Bson::Double(f),
    }
}

// ---------------------------------------------------------------------------
// Comparison operators
// ---------------------------------------------------------------------------

/// Shared body for the binary comparison operators (`$eq` .. `$lte`).
///
/// Missing operands are treated as `null` for comparison, matching the
/// server. `keep` maps the [`Ordering`] of `a` vs `b` to the boolean result.
fn cmp_op(
    op: &str,
    arg: &Bson,
    ctx: &ExprContext,
    keep: impl Fn(Ordering) -> bool,
) -> Result<Value> {
    let items = arg_list(arg);
    require_arity(op, &items, 2)?;
    let values = eval_all(&items, ctx)?;
    let a = values[0].clone().unwrap_or(Bson::Null);
    let b = values[1].clone().unwrap_or(Bson::Null);
    Ok(Some(Bson::Boolean(keep(compare_bson(&a, &b)))))
}

/// `$cmp` — `-1`/`0`/`1` for the ordering of the two operands.
fn eval_cmp(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$cmp", &items, 2)?;
    let values = eval_all(&items, ctx)?;
    let a = values[0].clone().unwrap_or(Bson::Null);
    let b = values[1].clone().unwrap_or(Bson::Null);
    let result = match compare_bson(&a, &b) {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    };
    Ok(Some(Bson::Int32(result)))
}

// ---------------------------------------------------------------------------
// Arithmetic operators
// ---------------------------------------------------------------------------

/// `$add` — variadic numeric sum; one `DateTime` operand shifts a date by the
/// summed milliseconds. Any null/missing operand yields `null`.
fn eval_add(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    let values = eval_all(&items, ctx)?;

    let mut date_base: Option<i64> = None;
    let mut int_acc: i64 = 0;
    let mut float_acc: f64 = 0.0;
    let mut any_float = false;

    for value in &values {
        match value {
            None | Some(Bson::Null) => return Ok(Some(Bson::Null)),
            Some(Bson::DateTime(dt)) => {
                if date_base.is_some() {
                    return Err(bad_value("only one date allowed in an $add expression"));
                }
                date_base = Some(dt.timestamp_millis());
            }
            Some(other) => match as_num(other) {
                Some(Num::Int(i)) => int_acc += i,
                Some(Num::Float(f)) => {
                    any_float = true;
                    float_acc += f;
                }
                None => {
                    return Err(bad_value(format!(
                        "$add only supports numeric or date types, not {}",
                        bson_type_name(other)
                    )));
                }
            },
        }
    }

    if let Some(base) = date_base {
        let offset = if any_float {
            (float_acc + int_acc as f64) as i64
        } else {
            int_acc
        };
        return Ok(Some(Bson::DateTime(DateTime::from_millis(base + offset))));
    }

    if any_float {
        Ok(Some(Bson::Double(float_acc + int_acc as f64)))
    } else {
        Ok(Some(num_to_bson(Num::Int(int_acc))))
    }
}

/// `$subtract` — `num-num`, `date-date` (-> `Int64` ms), or `date-num`
/// (-> date). Any null/missing operand yields `null`.
fn eval_subtract(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$subtract", &items, 2)?;
    let values = eval_all(&items, ctx)?;
    if values.iter().any(is_null_or_missing) {
        return Ok(Some(Bson::Null));
    }
    let a = values[0].as_ref().unwrap_or(&Bson::Null);
    let b = values[1].as_ref().unwrap_or(&Bson::Null);

    match (a, b) {
        (Bson::DateTime(x), Bson::DateTime(y)) => Ok(Some(Bson::Int64(
            x.timestamp_millis() - y.timestamp_millis(),
        ))),
        (Bson::DateTime(x), num) => {
            let n = as_num(num).ok_or_else(|| {
                bad_value(format!(
                    "cannot $subtract {} from a Date",
                    bson_type_name(num)
                ))
            })?;
            Ok(Some(Bson::DateTime(DateTime::from_millis(
                x.timestamp_millis() - num_as_f64(n) as i64,
            ))))
        }
        (left, right) => {
            let (x, y) = two_nums("$subtract", left, right)?;
            Ok(Some(combine(x, y, |a, b| a - b, |a, b| a - b)))
        }
    }
}

/// `$multiply` — variadic numeric product. Any null/missing operand yields
/// `null`.
fn eval_multiply(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    let values = eval_all(&items, ctx)?;
    if values.iter().any(is_null_or_missing) {
        return Ok(Some(Bson::Null));
    }
    let mut int_acc: i64 = 1;
    let mut float_acc: f64 = 1.0;
    let mut any_float = false;
    for value in &values {
        let v = value.as_ref().unwrap_or(&Bson::Null);
        match as_num(v) {
            Some(Num::Int(i)) => int_acc *= i,
            Some(Num::Float(f)) => {
                any_float = true;
                float_acc *= f;
            }
            None => {
                return Err(bad_value(format!(
                    "$multiply only supports numeric types, not {}",
                    bson_type_name(v)
                )));
            }
        }
    }
    if any_float {
        Ok(Some(Bson::Double(float_acc * int_acc as f64)))
    } else {
        Ok(Some(num_to_bson(Num::Int(int_acc))))
    }
}

/// `$divide` — 2-arg division, always `Double`. Divisor zero errors.
fn eval_divide(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$divide", &items, 2)?;
    let values = eval_all(&items, ctx)?;
    if values.iter().any(is_null_or_missing) {
        return Ok(Some(Bson::Null));
    }
    let (a, b) = two_nums(
        "$divide",
        values[0].as_ref().unwrap_or(&Bson::Null),
        values[1].as_ref().unwrap_or(&Bson::Null),
    )?;
    let divisor = num_as_f64(b);
    if divisor == 0.0 {
        return Err(bad_value("can't $divide by zero"));
    }
    Ok(Some(Bson::Double(num_as_f64(a) / divisor)))
}

/// `$mod` — 2-arg remainder; preserves integer type when both operands are
/// integral, else `Double`. Zero divisor errors.
fn eval_mod(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$mod", &items, 2)?;
    let values = eval_all(&items, ctx)?;
    if values.iter().any(is_null_or_missing) {
        return Ok(Some(Bson::Null));
    }
    let (a, b) = two_nums(
        "$mod",
        values[0].as_ref().unwrap_or(&Bson::Null),
        values[1].as_ref().unwrap_or(&Bson::Null),
    )?;
    match (a, b) {
        (Num::Int(x), Num::Int(y)) => {
            if y == 0 {
                return Err(bad_value("can't $mod by zero"));
            }
            Ok(Some(num_to_bson(Num::Int(x % y))))
        }
        _ => {
            let y = num_as_f64(b);
            if y == 0.0 {
                return Err(bad_value("can't $mod by zero"));
            }
            Ok(Some(Bson::Double(num_as_f64(a) % y)))
        }
    }
}

/// `$pow` — `base ^ exponent`. Stays integral when both operands are integers
/// and the exponent is non-negative and the result fits `i64`; else `Double`.
fn eval_pow(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$pow", &items, 2)?;
    let values = eval_all(&items, ctx)?;
    if values.iter().any(is_null_or_missing) {
        return Ok(Some(Bson::Null));
    }
    let (base, exp) = two_nums(
        "$pow",
        values[0].as_ref().unwrap_or(&Bson::Null),
        values[1].as_ref().unwrap_or(&Bson::Null),
    )?;
    if let (Num::Int(b), Num::Int(e)) = (base, exp) {
        if e >= 0 {
            if let Ok(exp_u32) = u32::try_from(e) {
                if let Some(result) = b.checked_pow(exp_u32) {
                    return Ok(Some(num_to_bson(Num::Int(result))));
                }
            }
        }
    }
    Ok(Some(Bson::Double(num_as_f64(base).powf(num_as_f64(exp)))))
}

/// Validate two numeric operands for a binary arithmetic operator.
fn two_nums(op: &str, a: &Bson, b: &Bson) -> Result<(Num, Num)> {
    let na = as_num(a).ok_or_else(|| {
        bad_value(format!(
            "{op} only supports numeric types, not {}",
            bson_type_name(a)
        ))
    })?;
    let nb = as_num(b).ok_or_else(|| {
        bad_value(format!(
            "{op} only supports numeric types, not {}",
            bson_type_name(b)
        ))
    })?;
    Ok((na, nb))
}

/// Combine two [`Num`]s, staying integral when both are integral.
fn combine(
    a: Num,
    b: Num,
    int_op: impl Fn(i64, i64) -> i64,
    float_op: impl Fn(f64, f64) -> f64,
) -> Bson {
    match (a, b) {
        (Num::Int(x), Num::Int(y)) => num_to_bson(Num::Int(int_op(x, y))),
        _ => Bson::Double(float_op(num_as_f64(a), num_as_f64(b))),
    }
}

// ---------------------------------------------------------------------------
// Unary math operators
// ---------------------------------------------------------------------------

/// Shared body for unary numeric operators (`$abs`, `$sqrt`, ...).
///
/// Null/missing input yields `null`; non-numeric input errors.
fn unary_num(
    op: &str,
    arg: &Bson,
    ctx: &ExprContext,
    f: impl Fn(Num) -> Result<Bson>,
) -> Result<Value> {
    let items = arg_list(arg);
    require_arity(op, &items, 1)?;
    let value = eval_expr(&items[0], ctx)?;
    if is_null_or_missing(&value) {
        return Ok(Some(Bson::Null));
    }
    let v = value.unwrap_or(Bson::Null);
    let n = as_num(&v).ok_or_else(|| {
        bad_value(format!(
            "{op} only supports numeric types, not {}",
            bson_type_name(&v)
        ))
    })?;
    Ok(Some(f(n)?))
}

/// `$abs` — preserves integer type; `i64::MIN` overflow falls back to f64.
fn math_abs(n: Num) -> Result<Bson> {
    match n {
        Num::Int(i) => match i.checked_abs() {
            Some(a) => Ok(num_to_bson(Num::Int(a))),
            None => Ok(Bson::Double((i as f64).abs())),
        },
        Num::Float(f) => Ok(Bson::Double(f.abs())),
    }
}

/// `$sqrt` — negative input errors.
fn math_sqrt(n: Num) -> Result<Bson> {
    let f = num_as_f64(n);
    if f < 0.0 {
        return Err(bad_value("$sqrt's argument must be greater than or equal to 0"));
    }
    Ok(Bson::Double(f.sqrt()))
}

/// `$ln`/`$log10` — non-positive input errors.
fn math_log(n: Num, f: impl Fn(f64) -> f64, op: &str) -> Result<Bson> {
    let x = num_as_f64(n);
    if x <= 0.0 {
        return Err(bad_value(format!("{op}'s argument must be a positive number")));
    }
    Ok(Bson::Double(f(x)))
}

/// `$ceil` — integral input passes through; floating input rounds up.
fn num_ceil(n: Num) -> Bson {
    match n {
        Num::Int(_) => num_to_bson(n),
        Num::Float(f) => Bson::Double(f.ceil()),
    }
}

/// `$floor` — integral input passes through; floating input rounds down.
fn num_floor(n: Num) -> Bson {
    match n {
        Num::Int(_) => num_to_bson(n),
        Num::Float(f) => Bson::Double(f.floor()),
    }
}

/// `$trunc` (1-arg) — integral input passes through; floating input truncates
/// toward zero.
fn num_trunc(n: Num) -> Bson {
    match n {
        Num::Int(_) => num_to_bson(n),
        Num::Float(f) => Bson::Double(f.trunc()),
    }
}

/// `$round` — `[value]` or `[value, place]`. `place` is an integral constant;
/// rounding uses round-half-to-even at the given decimal place.
fn eval_round(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_min_arity("$round", &items, 1)?;
    if items.len() > 2 {
        return Err(bad_value(format!(
            "Expression $round takes at most 2 arguments. {} were passed in.",
            items.len()
        )));
    }
    let value = eval_expr(&items[0], ctx)?;
    if is_null_or_missing(&value) {
        return Ok(Some(Bson::Null));
    }
    let v = value.unwrap_or(Bson::Null);
    let n = as_num(&v).ok_or_else(|| {
        bad_value(format!(
            "$round only supports numeric types, not {}",
            bson_type_name(&v)
        ))
    })?;

    let place = match items.get(1) {
        None => 0,
        Some(expr) => {
            let pv = eval_expr(expr, ctx)?.unwrap_or(Bson::Null);
            match as_num(&pv) {
                Some(Num::Int(p)) => p,
                _ => return Err(bad_value("$round requires an integral place argument")),
            }
        }
    };

    // Integral value with place >= 0 is already exact.
    if let Num::Int(_) = n {
        if place >= 0 {
            return Ok(Some(num_to_bson(n)));
        }
    }

    let x = num_as_f64(n);
    let factor = 10f64.powi(place as i32);
    let rounded = round_half_even(x * factor) / factor;
    match n {
        Num::Int(_) => Ok(Some(num_to_bson(Num::Int(rounded as i64)))),
        Num::Float(_) => Ok(Some(Bson::Double(rounded))),
    }
}

/// Round to the nearest integer, ties to even (banker's rounding).
fn round_half_even(x: f64) -> f64 {
    let floor = x.floor();
    let diff = x - floor;
    if diff < 0.5 {
        floor
    } else if diff > 0.5 {
        floor + 1.0
    } else if (floor as i64) % 2 == 0 {
        floor
    } else {
        floor + 1.0
    }
}

// ---------------------------------------------------------------------------
// Boolean operators
// ---------------------------------------------------------------------------

/// `$and` — variadic truthiness AND, short-circuiting on the first falsy arg.
fn eval_and(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    for item in items.iter() {
        if !eval_expr_to_bool(item, ctx)? {
            return Ok(Some(Bson::Boolean(false)));
        }
    }
    Ok(Some(Bson::Boolean(true)))
}

/// `$or` — variadic truthiness OR, short-circuiting on the first truthy arg.
fn eval_or(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    for item in items.iter() {
        if eval_expr_to_bool(item, ctx)? {
            return Ok(Some(Bson::Boolean(true)));
        }
    }
    Ok(Some(Bson::Boolean(false)))
}

/// `$not` — 1-arg truthiness negation.
fn eval_not(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$not", &items, 1)?;
    Ok(Some(Bson::Boolean(!eval_expr_to_bool(&items[0], ctx)?)))
}

// ---------------------------------------------------------------------------
// Conditional operators
// ---------------------------------------------------------------------------

/// `$cond` — `[if, then, else]` array form or `{if, then, else}` doc form.
fn eval_cond(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let (cond_e, then_e, else_e) = match arg {
        Bson::Array(items) => {
            require_arity("$cond", items, 3)?;
            (items[0].clone(), items[1].clone(), items[2].clone())
        }
        Bson::Document(doc) => {
            let get = |k: &str| {
                doc.get(k)
                    .cloned()
                    .ok_or_else(|| bad_value(format!("$cond requires the '{k}' argument")))
            };
            (get("if")?, get("then")?, get("else")?)
        }
        _ => return Err(bad_value("$cond requires an array or document argument")),
    };
    if eval_expr_to_bool(&cond_e, ctx)? {
        eval_expr(&then_e, ctx)
    } else {
        eval_expr(&else_e, ctx)
    }
}

/// `$ifNull` — first non-null non-missing argument; the final argument is the
/// fallback returned when every preceding argument is null/missing.
fn eval_if_null(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_min_arity("$ifNull", &items, 2)?;
    let last = items.len() - 1;
    for item in &items[..last] {
        let value = eval_expr(item, ctx)?;
        if !is_null_or_missing(&value) {
            return Ok(value);
        }
    }
    eval_expr(&items[last], ctx)
}

/// `$switch` — `{branches: [{case, then}...], default?}`. With no matching
/// branch and no default, errors.
fn eval_switch(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let doc = match arg {
        Bson::Document(d) => d,
        _ => return Err(bad_value("$switch requires an object as its argument")),
    };
    let branches = match doc.get("branches") {
        Some(Bson::Array(b)) => b,
        _ => return Err(bad_value("$switch requires an array for 'branches'")),
    };
    for branch in branches {
        let bdoc = match branch {
            Bson::Document(d) => d,
            _ => return Err(bad_value("$switch branch must be an object")),
        };
        let case_e = bdoc
            .get("case")
            .ok_or_else(|| bad_value("$switch branch requires a 'case' expression"))?;
        if eval_expr_to_bool(case_e, ctx)? {
            let then_e = bdoc
                .get("then")
                .ok_or_else(|| bad_value("$switch branch requires a 'then' expression"))?;
            return eval_expr(then_e, ctx);
        }
    }
    match doc.get("default") {
        Some(default_e) => eval_expr(default_e, ctx),
        None => Err(bad_value(
            "$switch could not find a matching branch for an input, \
             and no default was specified.",
        )),
    }
}

// ---------------------------------------------------------------------------
// String operators
// ---------------------------------------------------------------------------

/// `$concat` — variadic string concatenation. Any null/missing argument yields
/// `null`; any non-string non-null argument errors.
fn eval_concat(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    let values = eval_all(&items, ctx)?;
    let mut out = String::new();
    for value in &values {
        match value {
            None | Some(Bson::Null) => return Ok(Some(Bson::Null)),
            Some(Bson::String(s)) => out.push_str(s),
            Some(other) => {
                return Err(bad_value(format!(
                    "$concat only supports strings, not {}",
                    bson_type_name(other)
                )));
            }
        }
    }
    Ok(Some(Bson::String(out)))
}

/// `$toUpper`/`$toLower` — null/missing input yields `null`; a non-string
/// non-null input errors.
fn str_case(op: &str, arg: &Bson, ctx: &ExprContext, upper: bool) -> Result<Value> {
    let items = arg_list(arg);
    require_arity(op, &items, 1)?;
    let value = eval_expr(&items[0], ctx)?;
    match value {
        None | Some(Bson::Null) => Ok(Some(Bson::Null)),
        Some(Bson::String(s)) => {
            let mapped = if upper {
                s.to_uppercase()
            } else {
                s.to_lowercase()
            };
            Ok(Some(Bson::String(mapped)))
        }
        Some(other) => Err(bad_value(format!(
            "{op} only supports strings, not {}",
            bson_type_name(&other)
        ))),
    }
}

/// `$strLenCP` — number of UTF-8 code points. Null/missing input yields `null`.
fn eval_str_len_cp(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$strLenCP", &items, 1)?;
    let value = eval_expr(&items[0], ctx)?;
    match value {
        None | Some(Bson::Null) => Ok(Some(Bson::Null)),
        Some(Bson::String(s)) => Ok(Some(Bson::Int32(s.chars().count() as i32))),
        Some(other) => Err(bad_value(format!(
            "$strLenCP requires a string argument, found: {}",
            bson_type_name(&other)
        ))),
    }
}

/// `$substrCP` — `[str, start, len]`, code-point based. Out-of-range indices
/// clamp to the string bounds.
fn eval_substr_cp(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$substrCP", &items, 3)?;
    let values = eval_all(&items, ctx)?;
    let s = match &values[0] {
        None | Some(Bson::Null) => return Ok(Some(Bson::String(String::new()))),
        Some(Bson::String(s)) => s.clone(),
        Some(other) => {
            return Err(bad_value(format!(
                "$substrCP requires a string as its first argument, found: {}",
                bson_type_name(other)
            )))
        }
    };
    let start = nonneg_index("$substrCP", values[1].as_ref())?;
    let len = nonneg_index("$substrCP", values[2].as_ref())?;
    let result: String = s.chars().skip(start).take(len).collect();
    Ok(Some(Bson::String(result)))
}

/// Validate a non-negative code-point index/length argument for `$substrCP`.
fn nonneg_index(op: &str, value: Option<&Bson>) -> Result<usize> {
    let v = value.cloned().unwrap_or(Bson::Null);
    match as_num(&v) {
        Some(Num::Int(i)) if i >= 0 => Ok(i as usize),
        Some(Num::Float(f)) if f >= 0.0 && f.fract() == 0.0 => Ok(f as usize),
        _ => Err(bad_value(format!(
            "{op} expects a non-negative integer for index and length"
        ))),
    }
}

/// `$split` — `[string, delimiter]`. Both must be strings; null/missing in
/// either yields `null`.
fn eval_split(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$split", &items, 2)?;
    let values = eval_all(&items, ctx)?;
    if values.iter().any(is_null_or_missing) {
        return Ok(Some(Bson::Null));
    }
    let s = require_str("$split", values[0].as_ref())?;
    let delim = require_str("$split", values[1].as_ref())?;
    if delim.is_empty() {
        return Err(bad_value("$split requires a non-empty separator"));
    }
    let parts: Vec<Bson> = s
        .split(delim.as_str())
        .map(|p| Bson::String(p.to_owned()))
        .collect();
    Ok(Some(Bson::Array(parts)))
}

/// `$trim`/`$ltrim`/`$rtrim` — `{input, chars?}`. `chars` defaults to
/// whitespace. Null/missing `input` yields `null`.
fn eval_trim(arg: &Bson, ctx: &ExprContext, left: bool, right: bool) -> Result<Value> {
    let doc = match arg {
        Bson::Document(d) => d,
        _ => return Err(bad_value("$trim requires a document argument")),
    };
    let input_e = doc
        .get("input")
        .ok_or_else(|| bad_value("$trim requires an 'input' field"))?;
    let input_v = eval_expr(input_e, ctx)?;
    if is_null_or_missing(&input_v) {
        return Ok(Some(Bson::Null));
    }
    let input = require_str("$trim", input_v.as_ref())?;

    let chars: Option<Vec<char>> = match doc.get("chars") {
        None => None,
        Some(chars_e) => {
            let cv = eval_expr(chars_e, ctx)?;
            if is_null_or_missing(&cv) {
                None
            } else {
                Some(require_str("$trim", cv.as_ref())?.chars().collect())
            }
        }
    };

    let trimmed: &str = match &chars {
        None => {
            let mut s = input.as_str();
            if left {
                s = s.trim_start();
            }
            if right {
                s = s.trim_end();
            }
            s
        }
        Some(set) => {
            let pred = |c: char| set.contains(&c);
            let mut s = input.as_str();
            if left {
                s = s.trim_start_matches(pred);
            }
            if right {
                s = s.trim_end_matches(pred);
            }
            s
        }
    };
    Ok(Some(Bson::String(trimmed.to_owned())))
}

/// Validate that a [`Value`] holds a `String`, returning a clone.
fn require_str(op: &str, value: Option<&Bson>) -> Result<String> {
    match value {
        Some(Bson::String(s)) => Ok(s.clone()),
        other => Err(bad_value(format!(
            "{op} requires string arguments, found: {}",
            other.map_or("missing", bson_type_name)
        ))),
    }
}

/// `$toString` — scalar-to-string conversion. Document/array error;
/// null/missing yields `null`.
fn eval_to_string(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$toString", &items, 1)?;
    let value = eval_expr(&items[0], ctx)?;
    let v = match value {
        None | Some(Bson::Null) => return Ok(Some(Bson::Null)),
        Some(v) => v,
    };
    let s = match &v {
        Bson::String(s) => s.clone(),
        Bson::Boolean(b) => b.to_string(),
        Bson::Int32(n) => n.to_string(),
        Bson::Int64(n) => n.to_string(),
        Bson::Double(f) => f.to_string(),
        Bson::ObjectId(oid) => oid.to_hex(),
        Bson::DateTime(dt) => dt
            .try_to_rfc3339_string()
            .unwrap_or_else(|_| dt.timestamp_millis().to_string()),
        other => {
            return Err(bad_value(format!(
                "$toString does not support {}",
                bson_type_name(other)
            )));
        }
    };
    Ok(Some(Bson::String(s)))
}

// ---------------------------------------------------------------------------
// Array operators
// ---------------------------------------------------------------------------

/// `$size` — element count of an array argument (`Int32`); non-array errors.
fn eval_size(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$size", &items, 1)?;
    match eval_expr(&items[0], ctx)? {
        Some(Bson::Array(a)) => Ok(Some(Bson::Int32(a.len() as i32))),
        _ => Err(bad_value("The argument to $size must be an array")),
    }
}

/// `$isArray` — true when the single argument evaluates to an array
/// (missing -> false).
fn eval_is_array(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$isArray", &items, 1)?;
    let is_arr = matches!(eval_expr(&items[0], ctx)?, Some(Bson::Array(_)));
    Ok(Some(Bson::Boolean(is_arr)))
}

/// `$in` — `[needle, haystack]`; true when `needle` equals any haystack
/// element (`bson_eq`). Non-array haystack errors.
fn eval_in(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$in", &items, 2)?;
    let values = eval_all(&items, ctx)?;
    let needle = values[0].clone().unwrap_or(Bson::Null);
    match &values[1] {
        Some(Bson::Array(haystack)) => {
            Ok(Some(Bson::Boolean(haystack.iter().any(|e| bson_eq(e, &needle)))))
        }
        _ => Err(bad_value(
            "$in requires an array as a second argument",
        )),
    }
}

/// `$arrayElemAt` — `[array, index]`; negative index counts from the end;
/// out-of-range yields *missing*. Null/missing array yields `null`.
fn eval_array_elem_at(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$arrayElemAt", &items, 2)?;
    let values = eval_all(&items, ctx)?;
    let array = match &values[0] {
        None | Some(Bson::Null) => return Ok(Some(Bson::Null)),
        Some(Bson::Array(a)) => a,
        Some(other) => {
            return Err(bad_value(format!(
                "$arrayElemAt's first argument must be an array, but is {}",
                bson_type_name(other)
            )))
        }
    };
    let idx = match values[1].as_ref().and_then(as_num) {
        Some(Num::Int(i)) => i,
        Some(Num::Float(f)) if f.fract() == 0.0 => f as i64,
        _ => return Err(bad_value("$arrayElemAt's second argument must be a numeric value")),
    };
    let resolved = if idx < 0 {
        array.len() as i64 + idx
    } else {
        idx
    };
    if resolved < 0 || resolved as usize >= array.len() {
        return Ok(None);
    }
    Ok(Some(array[resolved as usize].clone()))
}

/// `$first`/`$last` — first/last element of an array. Empty array yields
/// *missing*; null/missing argument yields `null`.
fn eval_first_last(op: &str, arg: &Bson, ctx: &ExprContext, last: bool) -> Result<Value> {
    let items = arg_list(arg);
    require_arity(op, &items, 1)?;
    match eval_expr(&items[0], ctx)? {
        None | Some(Bson::Null) => Ok(Some(Bson::Null)),
        Some(Bson::Array(a)) => {
            let elem = if last { a.last() } else { a.first() };
            Ok(elem.cloned())
        }
        Some(other) => Err(bad_value(format!(
            "{op} requires an array argument, found: {}",
            bson_type_name(&other)
        ))),
    }
}

/// `$concatArrays` — variadic array concatenation. Any null/missing argument
/// yields `null`; any non-array argument errors.
fn eval_concat_arrays(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    let values = eval_all(&items, ctx)?;
    let mut out = Vec::new();
    for value in &values {
        match value {
            None | Some(Bson::Null) => return Ok(Some(Bson::Null)),
            Some(Bson::Array(a)) => out.extend(a.iter().cloned()),
            Some(other) => {
                return Err(bad_value(format!(
                    "$concatArrays only supports arrays, not {}",
                    bson_type_name(other)
                )));
            }
        }
    }
    Ok(Some(Bson::Array(out)))
}

/// `$slice` — `[array, n]` or `[array, position, n]`. Matches server
/// semantics: a 2-arg positive `n` takes the first `n`, negative the last
/// `|n|`; a 3-arg form starts at `position` (negative from the end) and takes
/// `n` elements.
fn eval_slice(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    if items.len() != 2 && items.len() != 3 {
        return Err(bad_value(
            "Expression $slice takes 2 or 3 arguments.",
        ));
    }
    let values = eval_all(&items, ctx)?;
    let array = match &values[0] {
        None | Some(Bson::Null) => return Ok(Some(Bson::Null)),
        Some(Bson::Array(a)) => a,
        Some(other) => {
            return Err(bad_value(format!(
                "$slice's first argument must be an array, but is {}",
                bson_type_name(other)
            )))
        }
    };
    let len = array.len() as i64;

    let (start, count): (i64, i64) = if values.len() == 2 {
        let n = slice_int("$slice", values[1].as_ref())?;
        if n >= 0 {
            (0, n)
        } else {
            ((len + n).max(0), -n)
        }
    } else {
        let position = slice_int("$slice", values[1].as_ref())?;
        let n = slice_int("$slice", values[2].as_ref())?;
        if n < 0 {
            return Err(bad_value(
                "$slice third argument (count) must be positive when a position is given",
            ));
        }
        let start = if position < 0 {
            (len + position).max(0)
        } else {
            position
        };
        (start, n)
    };

    if start >= len {
        return Ok(Some(Bson::Array(Vec::new())));
    }
    let end = (start + count).min(len);
    let slice: Vec<Bson> = array[start as usize..end as usize].to_vec();
    Ok(Some(Bson::Array(slice)))
}

/// Validate an integral `$slice` index argument.
fn slice_int(op: &str, value: Option<&Bson>) -> Result<i64> {
    match value.and_then(as_num) {
        Some(Num::Int(i)) => Ok(i),
        Some(Num::Float(f)) if f.fract() == 0.0 => Ok(f as i64),
        _ => Err(bad_value(format!("{op} requires integer arguments"))),
    }
}

/// `$filter` — `{input, as?, cond}`. Keeps elements of `input` for which
/// `cond` is truthy, binding each to `$$<as>` (default `this`). Null/missing
/// `input` yields `null`.
fn eval_filter_expr(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let doc = require_spec_doc("$filter", arg)?;
    let input_e = doc
        .get("input")
        .ok_or_else(|| bad_value("$filter requires an 'input' field"))?;
    let cond_e = doc
        .get("cond")
        .ok_or_else(|| bad_value("$filter requires a 'cond' field"))?;
    let var_name = as_name(doc.get("as"), "this")?;

    let array = match input_array("$filter", eval_expr(input_e, ctx)?)? {
        Some(a) => a,
        None => return Ok(Some(Bson::Null)),
    };

    let mut out = Vec::new();
    for element in array {
        let scope = ctx.with_var(&var_name, element.clone());
        if eval_expr_to_bool(cond_e, &scope)? {
            out.push(element);
        }
    }
    Ok(Some(Bson::Array(out)))
}

/// `$map` — `{input, as?, in}`. Applies `in` to each element of `input`,
/// binding each to `$$<as>` (default `this`). Null/missing `input` yields
/// `null`; per-element missing results become `null`.
fn eval_map(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let doc = require_spec_doc("$map", arg)?;
    let input_e = doc
        .get("input")
        .ok_or_else(|| bad_value("$map requires an 'input' field"))?;
    let in_e = doc
        .get("in")
        .ok_or_else(|| bad_value("$map requires an 'in' field"))?;
    let var_name = as_name(doc.get("as"), "this")?;

    let array = match input_array("$map", eval_expr(input_e, ctx)?)? {
        Some(a) => a,
        None => return Ok(Some(Bson::Null)),
    };

    let mut out = Vec::with_capacity(array.len());
    for element in array {
        let scope = ctx.with_var(&var_name, element);
        let mapped = eval_expr(in_e, &scope)?.unwrap_or(Bson::Null);
        out.push(mapped);
    }
    Ok(Some(Bson::Array(out)))
}

/// `$range` — `[start, end, step?]`. Generates `[start, start+step, ...)` up to
/// (excluding) `end`. All arguments integral; `step` defaults to 1 and must be
/// nonzero.
fn eval_range(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    if items.len() != 2 && items.len() != 3 {
        return Err(bad_value("Expression $range takes 2 or 3 arguments."));
    }
    let values = eval_all(&items, ctx)?;
    let start = range_int(values[0].as_ref())?;
    let end = range_int(values[1].as_ref())?;
    let step = match values.get(2) {
        None => 1,
        Some(v) => range_int(v.as_ref())?,
    };
    if step == 0 {
        return Err(bad_value("$range requires a non-zero step value"));
    }
    let mut out = Vec::new();
    let mut current = start;
    if step > 0 {
        while current < end {
            out.push(Bson::Int32(current as i32));
            current += step;
        }
    } else {
        while current > end {
            out.push(Bson::Int32(current as i32));
            current += step;
        }
    }
    Ok(Some(Bson::Array(out)))
}

/// Validate an integral `$range` argument.
fn range_int(value: Option<&Bson>) -> Result<i64> {
    match value.and_then(as_num) {
        Some(Num::Int(i)) => Ok(i),
        Some(Num::Float(f)) if f.fract() == 0.0 => Ok(f as i64),
        _ => Err(bad_value("$range requires integer arguments")),
    }
}

/// Interpret an evaluated `input` value for `$filter`/`$map`.
///
/// Returns `Ok(None)` for null/missing input (the caller returns `null`),
/// `Ok(Some(array))` for an array, and an error for any other type.
fn input_array(op: &str, value: Value) -> Result<Option<Vec<Bson>>> {
    match value {
        None | Some(Bson::Null) => Ok(None),
        Some(Bson::Array(a)) => Ok(Some(a)),
        Some(other) => Err(bad_value(format!(
            "input to {op} must be an array not {}",
            bson_type_name(&other)
        ))),
    }
}

/// Require a document-form operator specification (`$filter`/`$map`).
fn require_spec_doc<'b>(op: &str, arg: &'b Bson) -> Result<&'b Document> {
    match arg {
        Bson::Document(d) => Ok(d),
        _ => Err(bad_value(format!("{op} requires a document argument"))),
    }
}

/// Resolve the `as` variable name (a string constant), or the default.
fn as_name(value: Option<&Bson>, default: &str) -> Result<String> {
    match value {
        None => Ok(default.to_owned()),
        Some(Bson::String(s)) => Ok(s.clone()),
        Some(_) => Err(bad_value("the 'as' field must be a string")),
    }
}

// ---------------------------------------------------------------------------
// Type / conversion operators
// ---------------------------------------------------------------------------

/// `$type` — the BSON type name of the argument; *missing* -> `"missing"`.
fn eval_type(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$type", &items, 1)?;
    let name = match eval_expr(&items[0], ctx)? {
        None => "missing",
        Some(v) => bson_type_name(&v),
    };
    Ok(Some(Bson::String(name.to_owned())))
}

/// Numeric conversion target for `$toInt`/`$toLong`/`$toDouble`.
#[derive(Clone, Copy)]
enum NumericTarget {
    Int,
    Long,
    Double,
}

/// `$toInt`/`$toLong`/`$toDouble` — numeric/bool/string conversion to the
/// given target. Null/missing yields `null`.
fn to_number(
    op: &str,
    arg: &Bson,
    ctx: &ExprContext,
    target: NumericTarget,
) -> Result<Value> {
    let items = arg_list(arg);
    require_arity(op, &items, 1)?;
    let value = eval_expr(&items[0], ctx)?;
    let v = match value {
        None | Some(Bson::Null) => return Ok(Some(Bson::Null)),
        Some(v) => v,
    };

    let as_f64: f64 = match &v {
        Bson::Int32(n) => *n as f64,
        Bson::Int64(n) => *n as f64,
        Bson::Double(f) => *f,
        Bson::Boolean(b) => {
            if *b {
                1.0
            } else {
                0.0
            }
        }
        Bson::String(s) => s.trim().parse::<f64>().map_err(|_| {
            bad_value(format!("Failed to parse number '{s}' in {op}"))
        })?,
        other => {
            return Err(bad_value(format!(
                "{op} does not support {}",
                bson_type_name(other)
            )));
        }
    };

    let result = match target {
        NumericTarget::Int => {
            let i = as_f64 as i64;
            if i < i32::MIN as i64 || i > i32::MAX as i64 {
                return Err(bad_value(format!(
                    "Conversion would overflow target type in {op}"
                )));
            }
            Bson::Int32(i as i32)
        }
        NumericTarget::Long => Bson::Int64(as_f64 as i64),
        NumericTarget::Double => Bson::Double(as_f64),
    };
    Ok(Some(result))
}

/// `$toBool` — MongoDB 8.0 conversion: non-zero numbers, *all* strings
/// (including `"false"` and `""`), dates, and ObjectIds convert to `true`;
/// numeric zero converts to `false`; null/missing yields `null`.
fn eval_to_bool(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$toBool", &items, 1)?;
    let value = eval_expr(&items[0], ctx)?;
    let result = match value {
        None | Some(Bson::Null) => return Ok(Some(Bson::Null)),
        Some(Bson::Boolean(b)) => b,
        Some(Bson::Int32(n)) => n != 0,
        Some(Bson::Int64(n)) => n != 0,
        Some(Bson::Double(f)) => f != 0.0,
        // Every other supported type (strings, dates, ObjectId, ...) is true.
        Some(_) => true,
    };
    Ok(Some(Bson::Boolean(result)))
}

/// `$toDate` — `Int64`/`Double` milliseconds since the epoch -> `DateTime`.
/// Strings are not supported (divergence). Null/missing yields `null`.
fn eval_to_date(arg: &Bson, ctx: &ExprContext) -> Result<Value> {
    let items = arg_list(arg);
    require_arity("$toDate", &items, 1)?;
    let value = eval_expr(&items[0], ctx)?;
    match value {
        None | Some(Bson::Null) => Ok(Some(Bson::Null)),
        Some(Bson::DateTime(dt)) => Ok(Some(Bson::DateTime(dt))),
        Some(Bson::Int64(ms)) => Ok(Some(Bson::DateTime(DateTime::from_millis(ms)))),
        Some(Bson::Int32(ms)) => Ok(Some(Bson::DateTime(DateTime::from_millis(ms as i64)))),
        Some(Bson::Double(ms)) => Ok(Some(Bson::DateTime(DateTime::from_millis(ms as i64)))),
        Some(other) => Err(bad_value(format!(
            "$toDate only supports numeric milliseconds in mqlite, not {}",
            bson_type_name(&other)
        ))),
    }
}

// ---------------------------------------------------------------------------
// Date extraction operators
// ---------------------------------------------------------------------------

/// Which calendar field a date-extraction operator returns.
#[derive(Clone, Copy)]
enum DatePart {
    Year,
    Month,
    DayOfMonth,
    Hour,
    Minute,
    Second,
    Millisecond,
    /// 1 = Sunday .. 7 = Saturday.
    DayOfWeek,
    /// 1-based day index within the year.
    DayOfYear,
}

/// Broken-down UTC calendar fields derived from epoch milliseconds.
struct CivilDate {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
    millisecond: u32,
    /// 1 = Sunday .. 7 = Saturday.
    day_of_week: u32,
    /// 1-based day-of-year.
    day_of_year: u32,
}

/// Shared body for the date-extraction operators. The argument must evaluate
/// to a `DateTime`; null/missing yields `null`.
fn date_part(op: &str, arg: &Bson, ctx: &ExprContext, part: DatePart) -> Result<Value> {
    let items = arg_list(arg);
    require_arity(op, &items, 1)?;
    let value = eval_expr(&items[0], ctx)?;
    let dt = match value {
        None | Some(Bson::Null) => return Ok(Some(Bson::Null)),
        Some(Bson::DateTime(dt)) => dt,
        Some(other) => {
            return Err(bad_value(format!(
                "{op} requires a Date, found: {}",
                bson_type_name(&other)
            )))
        }
    };
    let civil = civil_from_millis(dt.timestamp_millis());
    let value = match part {
        DatePart::Year => civil.year as i32,
        DatePart::Month => civil.month as i32,
        DatePart::DayOfMonth => civil.day as i32,
        DatePart::Hour => civil.hour as i32,
        DatePart::Minute => civil.minute as i32,
        DatePart::Second => civil.second as i32,
        DatePart::Millisecond => civil.millisecond as i32,
        DatePart::DayOfWeek => civil.day_of_week as i32,
        DatePart::DayOfYear => civil.day_of_year as i32,
    };
    Ok(Some(Bson::Int32(value)))
}

/// Convert epoch milliseconds (UTC) to broken-down calendar fields.
///
/// Uses Howard Hinnant's branchless civil-from-days algorithm (public
/// domain), avoiding any date-library dependency.
fn civil_from_millis(millis: i64) -> CivilDate {
    // Split into whole days and the within-day millisecond remainder, using
    // floor semantics so negative (pre-epoch) timestamps stay correct.
    let days = millis.div_euclid(MILLIS_PER_DAY);
    let ms_of_day = millis.rem_euclid(MILLIS_PER_DAY);

    let millisecond = (ms_of_day % MILLIS_PER_SEC) as u32;
    let secs_of_day = ms_of_day / MILLIS_PER_SEC;
    let second = (secs_of_day % SECS_PER_MIN) as u32;
    let minute = ((secs_of_day / SECS_PER_MIN) % MINS_PER_HOUR) as u32;
    let hour = (secs_of_day / (SECS_PER_MIN * MINS_PER_HOUR)) as u32;

    // Howard Hinnant civil_from_days: days since 1970-01-01 -> (y, m, d).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1, 12]
    let year = if month <= 2 { y + 1 } else { y };

    // Day of week: 1970-01-01 was a Thursday. weekday_from_days gives
    // 0 = Sunday; MongoDB wants 1 = Sunday .. 7 = Saturday.
    let dow0 = days.rem_euclid(7); // 0 == 1970-01-01 (Thursday)
    let sunday_based = (dow0 + 4).rem_euclid(7); // 0 = Sunday
    let day_of_week = (sunday_based + 1) as u32;

    // Day of year: days since Jan 1 of `year`, 1-based.
    let jan1 = days_from_civil(year, 1, 1);
    let day_of_year = (days - jan1 + 1) as u32;

    CivilDate {
        year,
        month,
        day,
        hour,
        minute,
        second,
        millisecond,
        day_of_week,
        day_of_year,
    }
}

/// Howard Hinnant's `days_from_civil`: (year, month, day) -> days since the
/// Unix epoch (1970-01-01). Used to derive day-of-year.
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let m = month as i64;
    let d = day as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    era * 146_097 + doe - 719_468
}

// ---------------------------------------------------------------------------
// Misc operators
// ---------------------------------------------------------------------------

/// `$rand` — a `Double` in `[0, 1)`. Takes `{}` (no arguments).
///
/// Reuses the crate's lightweight, non-cryptographic entropy approach (the
/// same style as ObjectId random seeding): mixes a high-resolution clock
/// reading and a stack address through the default hasher. Sufficient for
/// `$rand`; no new dependency is introduced.
fn eval_rand(arg: &Bson) -> Result<Value> {
    match arg {
        Bson::Document(d) if d.is_empty() => {}
        _ => return Err(bad_value("$rand requires an empty object: {}")),
    }
    Ok(Some(Bson::Double(next_unit_float())))
}

/// Produce a pseudo-random `f64` in `[0, 1)` from process-local entropy.
fn next_unit_float() -> f64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};

    let mut h = DefaultHasher::new();
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .hash(&mut h);
    let stack_probe: usize = &h as *const _ as usize;
    stack_probe.hash(&mut h);
    let bits = h.finish();
    // Map the top 53 bits into [0, 1) for a uniform double.
    (bits >> 11) as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
#[path = "tests/expr_eval.rs"]
mod tests;
