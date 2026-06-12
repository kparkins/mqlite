//! In-memory aggregation pipeline engine.
//!
//! Parses a `pipeline` array into a sequence of validated [`Stage`]s and
//! executes them over an in-flight `Vec<Document>`. The seed documents are
//! supplied by the engine: when the first stage is `$match` the engine routes
//! its filter through the index-aware find path for acceleration, and the
//! remaining stages run here. Stages that need the filter evaluator delegate
//! to [`crate::query::eval_filter`]; sort ordering reuses the find-path
//! comparator and `$project`/`$unset` reuse the find-path projection so BSON
//! ordering and projection semantics are never duplicated. Computed
//! expressions (`_id`, accumulator arguments, `$project`/`$addFields` values,
//! `$replaceRoot`/`$replaceWith`) evaluate through the shared
//! [`crate::query::expr`] evaluator.
//!
//! # Supported stages
//!
//! | Stage | Notes |
//! |-------|-------|
//! | `$match` | filter via [`eval_filter`]; leading `$match` index-accelerated |
//! | `$sort` | `field -> 1 | -1`, find-path comparator |
//! | `$skip` / `$limit` | integral counts |
//! | `$count` | replaces the stream with `{<name>: <count>}` |
//! | `$project` | include/exclude *and* computed expression fields |
//! | `$addFields` / `$set` | add/overwrite fields via expressions |
//! | `$unset` | remove field paths (delegates to exclusion projection) |
//! | `$group` | full-expression `_id` and accumulator arguments |
//! | `$sortByCount` | `$group` by an expression, then `$sort` by `count` desc |
//! | `$replaceRoot` / `$replaceWith` | promote an expression document to root |
//! | `$unwind` | one output document per array element |
//! | `$lookup` | equality-form left-outer join against a foreign collection |
//!
//! # Memory
//!
//! mqlite is an embedded, in-memory store; there is no 100 MB
//! spill-to-disk limit emulation. Every stage materializes its full output in
//! memory.

use bson::{Bson, DateTime, Document};

use crate::error::{Error, Result};
use crate::keys::encode_key;
use crate::query::expr::{eval_expr, ExprContext};
use crate::query::{
    apply_projection_to_doc, compare_docs, eval_filter, get_nested_field,
};

/// Resolves a foreign collection (by bare name) to its documents for `$lookup`.
///
/// The argument is the *unqualified* collection name as written in the
/// `$lookup` `from` field; the engine-side implementation prepends the active
/// database prefix and reads the same snapshot as the main scan. A nonexistent
/// collection yields an empty `Vec` (not an error).
pub(crate) type LookupResolver<'a> = dyn Fn(&str) -> Result<Vec<Document>> + 'a;

/// Field name under which `$group` writes the group key.
const GROUP_ID_FIELD: &str = "_id";

/// Output field name `$sortByCount` writes the per-group document count to.
const SORT_BY_COUNT_FIELD: &str = "count";

/// Build a `BadValue` (MongoDB code 2) error carrying `msg`.
///
/// Mirrors the query/filter layer, which threads `BadValue` messages through
/// [`Error::BsonDeserialization`] so the wire layer maps them to code 2.
fn bad_value(msg: impl Into<String>) -> Error {
    use serde::de::Error as _;
    Error::BsonDeserialization(bson::de::Error::custom(format!("BadValue: {}", msg.into())))
}

/// Build an [`Error::UnsupportedOperator`] naming `operator`.
fn unsupported(operator: impl Into<String>) -> Error {
    Error::UnsupportedOperator {
        operator: operator.into(),
    }
}

/// A single accumulator inside a `$group` specification.
struct Accumulator {
    /// Output field name.
    out_field: String,
    /// Accumulator operator (e.g. `$sum`).
    op: AccumulatorOp,
    /// The expression the accumulator is applied to.
    arg: Expr,
}

/// The set of supported `$group` accumulator operators.
enum AccumulatorOp {
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    Push,
    AddToSet,
    /// `{$count: {}}` — document-count, equivalent to `{$sum: 1}`.
    Count,
    /// `$mergeObjects` — merge per-document documents in encounter order.
    MergeObjects,
    /// `$stdDevPop` — population standard deviation of numeric inputs.
    StdDevPop,
    /// `$stdDevSamp` — sample standard deviation of numeric inputs.
    StdDevSamp,
    /// `$firstN` / `$lastN` — first/last `n` values in encounter order.
    FirstN { n: usize, keep_last: bool },
    /// `$minN` / `$maxN` — `n` smallest/largest values by BSON ordering.
    MinMaxN { n: usize, want_max: bool },
}

/// A `$group`/`_id`/accumulator expression.
///
/// The stored `Bson` is a raw aggregation expression evaluated through the
/// shared [`crate::query::expr`] evaluator: a constant, a `$`-field path, an
/// operator expression (`$toUpper`, `$multiply`, `$cond`, ...), or a computed
/// document of nested expressions. This is a thin wrapper that preserves the
/// `$group` call sites while delegating all evaluation to `eval_expr`.
struct Expr(Bson);

/// A validated pipeline stage.
enum Stage {
    Match(Document),
    Sort(Document),
    Skip(usize),
    Limit(usize),
    Count(String),
    Project(Document),
    Group {
        id: Expr,
        accumulators: Vec<Accumulator>,
    },
    /// `$sortByCount <expr>` — desugars to `$group` by `<expr>` with a document
    /// count, then `$sort` by `count` descending.
    SortByCount(Expr),
    /// `$addFields` / `$set` — per-field expressions assigned in spec order,
    /// each evaluated against the original incoming document.
    AddFields(Vec<(String, Bson)>),
    /// `$replaceRoot` / `$replaceWith` — the expression must evaluate to a
    /// document, which becomes the new root.
    ReplaceRoot(Bson),
    /// `$unwind` — flatten an array field into one document per element.
    Unwind(Unwind),
    /// `$lookup` — equality-form left-outer join against a foreign collection.
    Lookup(Lookup),
}

/// A validated `$unwind` specification.
struct Unwind {
    /// The dotted field path to unwind (stored without the leading `$`).
    path: String,
    /// Optional output field receiving the array index (`Int64`); never `$`.
    include_array_index: Option<String>,
    /// When true, null/missing/empty-array inputs are preserved (one output).
    preserve_null_and_empty: bool,
}

/// A validated equality-form `$lookup` specification.
struct Lookup {
    /// The foreign collection's unqualified name.
    from: String,
    /// Dotted local field path (stored without the leading `$`).
    local_field: String,
    /// Dotted foreign field path (stored without the leading `$`).
    foreign_field: String,
    /// Output array field receiving the matched foreign documents; never `$`.
    as_field: String,
}

/// A fully parsed and validated pipeline.
pub(crate) struct Pipeline {
    stages: Vec<Stage>,
}

impl Pipeline {
    /// Parse and validate a `pipeline` array into a [`Pipeline`].
    ///
    /// Each element must be a single-key document naming a supported stage.
    ///
    /// # Errors
    ///
    /// Returns [`Error::BsonDeserialization`] (a `BadValue`) when an element is
    /// not a document or a stage argument is malformed,
    /// [`Error::UnsupportedOperator`] for an unknown stage, accumulator, or
    /// expression form, and propagates any error raised while validating a
    /// `$match` filter shape.
    pub(crate) fn parse(pipeline: &[Bson]) -> Result<Self> {
        let mut stages = Vec::with_capacity(pipeline.len());
        for element in pipeline {
            let stage_doc = element.as_document().ok_or_else(|| {
                bad_value("Each element of the 'pipeline' array must be an object")
            })?;
            stages.push(parse_stage(stage_doc)?);
        }
        Ok(Self { stages })
    }

    /// Return `true` when the first stage is `$match`.
    ///
    /// The engine uses this to route the leading `$match` filter through the
    /// index-aware find path before executing the remainder of the pipeline.
    pub(crate) fn first_stage_is_match(&self) -> bool {
        matches!(self.stages.first(), Some(Stage::Match(_)))
    }

    /// Return the leading `$match` filter, if the first stage is `$match`.
    pub(crate) fn leading_match_filter(&self) -> Option<&Document> {
        match self.stages.first() {
            Some(Stage::Match(filter)) => Some(filter),
            _ => None,
        }
    }

    /// Execute the pipeline over `docs`.
    ///
    /// When `skip_first_match` is `true` the leading `$match` stage is assumed
    /// to have already been applied by the seed query (index-accelerated find)
    /// and is not re-evaluated. `resolve_foreign` resolves a `$lookup` `from`
    /// collection (by unqualified name) to its documents on the same read
    /// snapshot as the main scan; pipelines without `$lookup` never call it.
    ///
    /// A single `$$NOW` timestamp is frozen for the whole pipeline so every
    /// expression sees a consistent clock.
    ///
    /// # Errors
    ///
    /// Propagates filter-evaluation errors from [`eval_filter`], any
    /// accumulator/expression error surfaced while running a stage, and any
    /// error raised by `resolve_foreign` for a `$lookup`.
    pub(crate) fn execute(
        &self,
        mut docs: Vec<Document>,
        skip_first_match: bool,
        resolve_foreign: &LookupResolver<'_>,
    ) -> Result<Vec<Document>> {
        let now = DateTime::now();
        for (index, stage) in self.stages.iter().enumerate() {
            if index == 0 && skip_first_match && matches!(stage, Stage::Match(_)) {
                continue;
            }
            docs = run_stage(stage, docs, now, resolve_foreign)?;
        }
        Ok(docs)
    }
}

/// Run a restricted aggregation `pipeline` over a single document, returning
/// the single resulting document.
///
/// This is the execution backend for pipeline-form updates
/// ([`crate::update`]). The caller is responsible for validating that the
/// pipeline contains only update-safe stages (`$set`, `$unset`, `$project`,
/// `$addFields`, `$replaceRoot`, `$replaceWith`), all of which are 1:1 over a
/// single document. `$lookup` is excluded by that validation, so the foreign
/// resolver is a never-firing stub.
///
/// # Errors
///
/// Returns [`Error::Internal`] if the pipeline does not yield exactly one
/// output document, plus any parse or stage-execution error. A `$lookup`
/// reaching the stub resolver is also an [`Error::Internal`] (the caller's
/// validation must prevent this).
pub(crate) fn run_single_doc_pipeline(
    doc: &Document,
    pipeline: &[Document],
) -> Result<Document> {
    let stages: Vec<Bson> = pipeline.iter().cloned().map(Bson::Document).collect();
    let parsed = Pipeline::parse(&stages)?;

    let resolve_foreign = |_from: &str| -> Result<Vec<Document>> {
        Err(Error::Internal(
            "$lookup is not allowed within an update pipeline".to_owned(),
        ))
    };

    let mut out = parsed.execute(vec![doc.clone()], false, &resolve_foreign)?;
    match out.len() {
        1 => Ok(out.remove(0)),
        n => Err(Error::Internal(format!(
            "update pipeline produced {n} documents; expected exactly one"
        ))),
    }
}

/// Parse a single-key stage document into a [`Stage`].
fn parse_stage(stage_doc: &Document) -> Result<Stage> {
    let mut iter = stage_doc.iter();
    let (name, value) = iter
        .next()
        .ok_or_else(|| bad_value("Each element of the 'pipeline' array must be an object"))?;
    if iter.next().is_some() {
        return Err(bad_value(
            "A pipeline stage specification object must contain exactly one field.",
        ));
    }

    match name.as_str() {
        "$match" => Ok(Stage::Match(require_document("$match", value)?.clone())),
        "$sort" => parse_sort(value),
        "$skip" => parse_skip(value),
        "$limit" => parse_limit(value),
        "$count" => parse_count(value),
        "$project" => Ok(Stage::Project(require_document("$project", value)?.clone())),
        "$group" => parse_group(value),
        "$sortByCount" => parse_sort_by_count(value),
        // `$addFields` and `$set` are byte-identical; the stage name is passed
        // through only to disambiguate `$set` (the stage) from `$set` (the
        // update operator) in error messages.
        "$addFields" => parse_add_fields("$addFields", value),
        "$set" => parse_add_fields("$set", value),
        "$unset" => parse_unset(value),
        "$replaceRoot" => parse_replace_root(value),
        "$replaceWith" => Ok(Stage::ReplaceRoot(value.clone())),
        "$unwind" => parse_unwind(value),
        "$lookup" => parse_lookup(value),
        other => Err(unsupported(format!(
            "Unrecognized pipeline stage name: '{other}'"
        ))),
    }
}

/// Require `value` to be a document, else a `BadValue` naming `ctx`.
fn require_document<'a>(ctx: &str, value: &'a Bson) -> Result<&'a Document> {
    value
        .as_document()
        .ok_or_else(|| bad_value(format!("{ctx} stage specification must be an object")))
}

/// Parse a `$sort` value: a non-empty document of `field -> 1 | -1`.
fn parse_sort(value: &Bson) -> Result<Stage> {
    let spec = require_document("$sort", value)?;
    if spec.is_empty() {
        return Err(bad_value("$sort stage must have at least one sort key"));
    }
    for (_field, dir) in spec {
        if !is_sort_direction(dir) {
            return Err(bad_value(
                "$sort key ordering must be 1 (for ascending) or -1 (for descending)",
            ));
        }
    }
    Ok(Stage::Sort(spec.clone()))
}

/// Return `true` when `dir` is `1` or `-1` (as `Int32`/`Int64`/`Double`).
fn is_sort_direction(dir: &Bson) -> bool {
    matches!(
        dir,
        Bson::Int32(1 | -1) | Bson::Int64(1 | -1) | Bson::Double(1.0) | Bson::Double(-1.0)
    )
}

/// Parse a `$skip` value: a non-negative integral number.
fn parse_skip(value: &Bson) -> Result<Stage> {
    let n = integral_i64("$skip", value)?;
    if n < 0 {
        return Err(bad_value("the $skip value must be non-negative"));
    }
    Ok(Stage::Skip(n as usize))
}

/// Parse a `$limit` value: a positive integral number.
fn parse_limit(value: &Bson) -> Result<Stage> {
    let n = integral_i64("$limit", value)?;
    if n <= 0 {
        return Err(bad_value("the limit must be positive"));
    }
    Ok(Stage::Limit(n as usize))
}

/// Coerce a numeric BSON value to `i64`, accepting only whole numbers.
fn integral_i64(stage: &str, value: &Bson) -> Result<i64> {
    match value {
        Bson::Int32(n) => Ok(*n as i64),
        Bson::Int64(n) => Ok(*n),
        Bson::Double(f) if f.fract() == 0.0 && f.is_finite() => Ok(*f as i64),
        _ => Err(bad_value(format!("invalid argument to {stage} stage"))),
    }
}

/// Parse a `$count` value: a non-empty string with no `$` prefix and no `.`.
fn parse_count(value: &Bson) -> Result<Stage> {
    let name = match value {
        Bson::String(s) => s,
        _ => return Err(bad_value("the count field must be a non-empty string")),
    };
    if name.is_empty() {
        return Err(bad_value("the count field must be a non-empty string"));
    }
    if name.starts_with('$') {
        return Err(bad_value("the count field cannot be a $-prefixed path"));
    }
    if name.contains('.') {
        return Err(bad_value("the count field cannot contain '.'"));
    }
    Ok(Stage::Count(name.clone()))
}

/// Parse a `$group` document into its `_id` expression and accumulators.
fn parse_group(value: &Bson) -> Result<Stage> {
    let spec = require_document("$group", value)?;
    let id_value = spec
        .get(GROUP_ID_FIELD)
        .ok_or_else(|| bad_value("a group specification must include an _id"))?;
    let id = Expr(id_value.clone());

    let mut accumulators = Vec::with_capacity(spec.len().saturating_sub(1));
    for (field, acc_value) in spec {
        if field == GROUP_ID_FIELD {
            continue;
        }
        accumulators.push(parse_accumulator(field, acc_value)?);
    }
    Ok(Stage::Group { id, accumulators })
}

/// Parse a `$sortByCount` value into a [`Stage::SortByCount`].
///
/// The expression accepts any aggregation expression (field path, constant, or
/// operator expression such as `$toUpper`/`$mergeObjects`), evaluated through
/// the shared expression evaluator.
fn parse_sort_by_count(value: &Bson) -> Result<Stage> {
    Ok(Stage::SortByCount(Expr(value.clone())))
}

/// Parse an `$addFields`/`$set` argument into ordered `(field, expr)` pairs.
///
/// `stage` is the originating stage name (`$addFields` or `$set`) and is woven
/// into error messages so a malformed `$set` stage is not confused with the
/// `$set` update operator. The argument must be a non-empty document; each
/// value is a raw aggregation expression evaluated at run time.
fn parse_add_fields(stage: &str, value: &Bson) -> Result<Stage> {
    let spec = value.as_document().ok_or_else(|| {
        bad_value(format!("{stage} stage requires a non-empty object as its argument"))
    })?;
    if spec.is_empty() {
        return Err(bad_value(format!(
            "{stage} stage requires a non-empty object as its argument"
        )));
    }
    let fields = spec
        .iter()
        .map(|(field, expr)| (field.clone(), expr.clone()))
        .collect();
    Ok(Stage::AddFields(fields))
}

/// Parse a `$unset` argument (a non-empty field path string or an array of
/// such strings) into the equivalent exclusion `$project`.
///
/// `{$unset: "a.b"}` is exactly `{$project: {"a.b": 0}}` minus the `_id`
/// special case, so it is implemented by delegating to the exclusion
/// projection. Each path must be non-empty and must not begin with `$`.
fn parse_unset(value: &Bson) -> Result<Stage> {
    let paths: Vec<String> = match value {
        Bson::String(s) => vec![s.clone()],
        Bson::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Bson::String(s) => out.push(s.clone()),
                    _ => {
                        return Err(bad_value(
                            "$unset specification must be a string or an array of strings",
                        ))
                    }
                }
            }
            out
        }
        _ => {
            return Err(bad_value(
                "$unset specification must be a string or an array of strings",
            ))
        }
    };
    if paths.is_empty() {
        return Err(bad_value("$unset specification must not be empty"));
    }
    let mut proj = Document::new();
    for path in paths {
        if path.is_empty() {
            return Err(bad_value("$unset field path must be a non-empty string"));
        }
        if path.starts_with('$') {
            return Err(bad_value("$unset field path must not start with '$'"));
        }
        proj.insert(path, Bson::Int32(0));
    }
    // Delegate to the exclusion projection (item 4). The all-exclusion spec
    // never triggers the projection's `_id`-inclusion override.
    Ok(Stage::Project(proj))
}

/// Parse a `$replaceRoot` argument: `{newRoot: <expr>}` with no other keys.
fn parse_replace_root(value: &Bson) -> Result<Stage> {
    let spec = value.as_document().ok_or_else(|| {
        bad_value("$replaceRoot requires an object of the form {newRoot: <expression>}")
    })?;
    let new_root = spec
        .get("newRoot")
        .ok_or_else(|| bad_value("$replaceRoot requires a 'newRoot' field"))?;
    for key in spec.keys() {
        if key != "newRoot" {
            return Err(bad_value(format!(
                "$replaceRoot found an unknown argument: {key}"
            )));
        }
    }
    Ok(Stage::ReplaceRoot(new_root.clone()))
}

/// Parse a `$unwind` argument: a `"$path"` string or a `{path, ...}` document.
fn parse_unwind(value: &Bson) -> Result<Stage> {
    match value {
        Bson::String(s) => {
            let path = strip_field_path("$unwind", s)?;
            Ok(Stage::Unwind(Unwind {
                path,
                include_array_index: None,
                preserve_null_and_empty: false,
            }))
        }
        Bson::Document(spec) => parse_unwind_doc(spec),
        _ => Err(bad_value(
            "$unwind requires a field path string or a document with a 'path'",
        )),
    }
}

/// Parse the document form of `$unwind`.
fn parse_unwind_doc(spec: &Document) -> Result<Stage> {
    let path_value = spec
        .get("path")
        .ok_or_else(|| bad_value("$unwind requires a 'path' field"))?;
    let path = match path_value {
        Bson::String(s) => strip_field_path("$unwind", s)?,
        _ => return Err(bad_value("$unwind 'path' must be a field path string")),
    };

    let mut include_array_index: Option<String> = None;
    let mut preserve_null_and_empty = false;
    for (key, value) in spec {
        match key.as_str() {
            "path" => {}
            "includeArrayIndex" => match value {
                Bson::String(s) if !s.is_empty() && !s.starts_with('$') => {
                    include_array_index = Some(s.clone());
                }
                _ => {
                    return Err(bad_value(
                        "$unwind 'includeArrayIndex' must be a non-empty, non-$ string",
                    ))
                }
            },
            "preserveNullAndEmptyArrays" => match value {
                Bson::Boolean(b) => preserve_null_and_empty = *b,
                _ => {
                    return Err(bad_value(
                        "$unwind 'preserveNullAndEmptyArrays' must be a boolean",
                    ))
                }
            },
            other => {
                return Err(bad_value(format!(
                    "$unwind found an unknown argument: {other}"
                )))
            }
        }
    }

    Ok(Stage::Unwind(Unwind {
        path,
        include_array_index,
        preserve_null_and_empty,
    }))
}

/// Parse an equality-form `$lookup` argument.
///
/// All four of `from`, `localField`, `foreignField`, and `as` are required.
/// The pipeline form (`let`/`pipeline`) is rejected with an unsupported error.
fn parse_lookup(value: &Bson) -> Result<Stage> {
    let spec = value
        .as_document()
        .ok_or_else(|| bad_value("$lookup requires an object as its argument"))?;

    let mut from: Option<String> = None;
    let mut local_field: Option<String> = None;
    let mut foreign_field: Option<String> = None;
    let mut as_field: Option<String> = None;
    for (key, val) in spec {
        match key.as_str() {
            "from" => from = Some(lookup_string("from", val)?),
            "localField" => local_field = Some(lookup_path("localField", val)?),
            "foreignField" => foreign_field = Some(lookup_path("foreignField", val)?),
            "as" => as_field = Some(lookup_as(val)?),
            "let" | "pipeline" => {
                return Err(unsupported(
                    "the $lookup 'let'/'pipeline' (sub-pipeline) form is not supported",
                ))
            }
            other => {
                return Err(bad_value(format!(
                    "$lookup found an unknown argument: {other}"
                )))
            }
        }
    }

    let lookup = Lookup {
        from: from.ok_or_else(|| bad_value("$lookup requires a 'from' field"))?,
        local_field: local_field
            .ok_or_else(|| bad_value("$lookup requires a 'localField' field"))?,
        foreign_field: foreign_field
            .ok_or_else(|| bad_value("$lookup requires a 'foreignField' field"))?,
        as_field: as_field.ok_or_else(|| bad_value("$lookup requires an 'as' field"))?,
    };
    Ok(Stage::Lookup(lookup))
}

/// Require a `$lookup` string field (`from`).
fn lookup_string(field: &str, value: &Bson) -> Result<String> {
    match value {
        Bson::String(s) if !s.is_empty() => Ok(s.clone()),
        _ => Err(bad_value(format!("$lookup '{field}' must be a non-empty string"))),
    }
}

/// Require a `$lookup` field-path string (`localField`/`foreignField`), stored
/// without a leading `$` (a `$`-prefix here is illegal).
fn lookup_path(field: &str, value: &Bson) -> Result<String> {
    match value {
        Bson::String(s) if !s.is_empty() && !s.starts_with('$') => Ok(s.clone()),
        _ => Err(bad_value(format!(
            "$lookup '{field}' must be a non-empty, non-$ field path"
        ))),
    }
}

/// Require the `$lookup` `as` output field: a non-empty, non-`$` string.
fn lookup_as(value: &Bson) -> Result<String> {
    match value {
        Bson::String(s) if !s.is_empty() && !s.starts_with('$') => Ok(s.clone()),
        _ => Err(bad_value("$lookup 'as' must be a non-empty, non-$ string")),
    }
}

/// Strip the leading `$` from a `$unwind` field path, erroring if absent.
fn strip_field_path(stage: &str, raw: &str) -> Result<String> {
    match raw.strip_prefix('$') {
        Some(rest) if !rest.is_empty() => Ok(rest.to_owned()),
        _ => Err(bad_value(format!(
            "{stage} field path must be a '$'-prefixed string"
        ))),
    }
}

/// Parse one accumulator entry `{<op>: <expr>}` for output field `out_field`.
fn parse_accumulator(out_field: &str, value: &Bson) -> Result<Accumulator> {
    let acc_doc = value
        .as_document()
        .ok_or_else(|| bad_value(format!("the field '{out_field}' must be an accumulator object")))?;
    let mut iter = acc_doc.iter();
    let (op_name, arg_value) = iter.next().ok_or_else(|| {
        bad_value(format!("the field '{out_field}' must specify one accumulator"))
    })?;
    if iter.next().is_some() {
        return Err(bad_value(format!(
            "the field '{out_field}' must specify exactly one accumulator"
        )));
    }

    // The `$count`, `$firstN`/`$lastN`, and `$minN`/`$maxN` accumulators take a
    // document argument rather than a plain expression, so they parse their own
    // `(op, arg)` pair.
    match op_name.as_str() {
        "$count" => return parse_count_accumulator(out_field, arg_value),
        "$firstN" | "$lastN" | "$minN" | "$maxN" => {
            return parse_n_accumulator(out_field, op_name, arg_value);
        }
        _ => {}
    }

    let op = match op_name.as_str() {
        "$sum" => AccumulatorOp::Sum,
        "$avg" => AccumulatorOp::Avg,
        "$min" => AccumulatorOp::Min,
        "$max" => AccumulatorOp::Max,
        "$first" => AccumulatorOp::First,
        "$last" => AccumulatorOp::Last,
        "$push" => AccumulatorOp::Push,
        "$addToSet" => AccumulatorOp::AddToSet,
        "$mergeObjects" => AccumulatorOp::MergeObjects,
        "$stdDevPop" => AccumulatorOp::StdDevPop,
        "$stdDevSamp" => AccumulatorOp::StdDevSamp,
        other => return Err(unsupported(format!("unknown group operator '{other}'"))),
    };
    let arg = Expr(arg_value.clone());
    Ok(Accumulator {
        out_field: out_field.to_owned(),
        op,
        arg,
    })
}

/// Parse a `{$count: {}}` accumulator: the argument must be an empty document.
///
/// `$count` behaves exactly like `{$sum: 1}`; the stored argument is the
/// constant `1` so the [`AccumulatorOp::Count`] state can reuse the numeric
/// folding path.
fn parse_count_accumulator(out_field: &str, arg_value: &Bson) -> Result<Accumulator> {
    let arg_doc = arg_value.as_document().ok_or_else(|| {
        bad_value("$count requires an empty document as its argument")
    })?;
    if !arg_doc.is_empty() {
        return Err(bad_value(
            "$count requires an empty document as its argument",
        ));
    }
    Ok(Accumulator {
        out_field: out_field.to_owned(),
        op: AccumulatorOp::Count,
        arg: Expr(Bson::Int32(1)),
    })
}

/// Parse an `{input: <expr>, n: <int>}` accumulator (`$firstN`/`$lastN`/
/// `$minN`/`$maxN`).
///
/// Both `input` and `n` are required and no other keys are allowed. `n` must be
/// a positive integral constant. Unlike MongoDB — which allows an expression for
/// `n` — mqlite requires a constant; this divergence is intentional.
fn parse_n_accumulator(out_field: &str, op_name: &str, arg_value: &Bson) -> Result<Accumulator> {
    let spec = arg_value.as_document().ok_or_else(|| {
        bad_value(format!("{op_name} requires an object with 'input' and 'n'"))
    })?;
    let mut input_value: Option<&Bson> = None;
    let mut n_value: Option<&Bson> = None;
    for (key, value) in spec {
        match key.as_str() {
            "input" => input_value = Some(value),
            "n" => n_value = Some(value),
            other => {
                return Err(bad_value(format!(
                    "{op_name} found an unknown argument: {other}"
                )))
            }
        }
    }
    let input = input_value
        .ok_or_else(|| bad_value(format!("{op_name} requires an 'input' expression")))?;
    let n_value =
        n_value.ok_or_else(|| bad_value(format!("{op_name} requires an 'n' value")))?;
    let n = positive_n(n_value)?;
    let arg = Expr(input.clone());
    let op = match op_name {
        "$firstN" => AccumulatorOp::FirstN {
            n,
            keep_last: false,
        },
        "$lastN" => AccumulatorOp::FirstN { n, keep_last: true },
        "$minN" => AccumulatorOp::MinMaxN {
            n,
            want_max: false,
        },
        // "$maxN"
        _ => AccumulatorOp::MinMaxN { n, want_max: true },
    };
    Ok(Accumulator {
        out_field: out_field.to_owned(),
        op,
        arg,
    })
}

/// Coerce `value` to a positive integral count, rejecting non-positive or
/// non-integral numbers and all non-numeric values.
fn positive_n(value: &Bson) -> Result<usize> {
    let n = match value {
        Bson::Int32(n) => *n as i64,
        Bson::Int64(n) => *n,
        Bson::Double(f) if f.fract() == 0.0 && f.is_finite() => *f as i64,
        _ => return Err(bad_value("n must be a positive integer")),
    };
    if n <= 0 {
        return Err(bad_value("n must be a positive integer"));
    }
    Ok(n as usize)
}

impl Expr {
    /// Evaluate this expression against `doc` with the shared `$$NOW`.
    ///
    /// Returns `None` for a *missing* result (an unresolved field path or a
    /// computed document/operator that produced no value) and `Some(value)`
    /// otherwise, delegating to [`eval_expr`].
    ///
    /// # Errors
    ///
    /// Propagates any error from [`eval_expr`] (malformed operator, arity, or
    /// type error).
    fn resolve(&self, doc: &Document, now: DateTime) -> Result<Option<Bson>> {
        let ctx = ExprContext::with_now(doc, now);
        eval_expr(&self.0, &ctx)
    }
}

/// Build an evaluation context rooted at `doc` with the shared `$$NOW`.
fn expr_ctx(doc: &Document, now: DateTime) -> ExprContext<'_> {
    ExprContext::with_now(doc, now)
}

/// Execute a single stage over `docs`, returning the transformed stream.
fn run_stage(
    stage: &Stage,
    docs: Vec<Document>,
    now: DateTime,
    resolve_foreign: &LookupResolver<'_>,
) -> Result<Vec<Document>> {
    match stage {
        Stage::Match(filter) => run_match(filter, docs),
        Stage::Sort(spec) => Ok(run_sort(spec, docs)),
        Stage::Skip(n) => Ok(run_skip(*n, docs)),
        Stage::Limit(n) => Ok(run_limit(*n, docs)),
        Stage::Count(name) => Ok(run_count(name, docs)),
        Stage::Project(proj) => run_project(proj, docs, now),
        Stage::Group { id, accumulators } => run_group(id, accumulators, docs, now),
        Stage::SortByCount(expr) => run_sort_by_count(expr, docs, now),
        Stage::AddFields(fields) => run_add_fields(fields, docs, now),
        Stage::ReplaceRoot(expr) => run_replace_root(expr, docs, now),
        Stage::Unwind(unwind) => Ok(run_unwind(unwind, docs)),
        Stage::Lookup(lookup) => run_lookup(lookup, docs, resolve_foreign),
    }
}

/// `$sortByCount`: group by `expr` counting documents, then sort by `count`
/// descending. Equivalent to `$group: {_id: <expr>, count: {$sum: 1}}` followed
/// by `$sort: {count: -1}`.
fn run_sort_by_count(expr: &Expr, docs: Vec<Document>, now: DateTime) -> Result<Vec<Document>> {
    let accumulators = vec![Accumulator {
        out_field: SORT_BY_COUNT_FIELD.to_owned(),
        op: AccumulatorOp::Count,
        arg: Expr(Bson::Int32(1)),
    }];
    let grouped = run_group(expr, &accumulators, docs, now)?;
    let sort_spec = doc_count_descending();
    Ok(run_sort(&sort_spec, grouped))
}

/// Build the `{count: -1}` sort spec used by `$sortByCount`.
fn doc_count_descending() -> Document {
    let mut spec = Document::new();
    spec.insert(SORT_BY_COUNT_FIELD, Bson::Int32(-1));
    spec
}

/// `$match`: keep documents passing `filter` via the shared filter evaluator.
fn run_match(filter: &Document, docs: Vec<Document>) -> Result<Vec<Document>> {
    let mut out = Vec::with_capacity(docs.len());
    for doc in docs {
        if eval_filter(&doc, filter)? {
            out.push(doc);
        }
    }
    Ok(out)
}

/// `$sort`: stable sort reusing the find-path comparator.
fn run_sort(spec: &Document, mut docs: Vec<Document>) -> Vec<Document> {
    docs.sort_by(|a, b| compare_docs(a, b, spec));
    docs
}

/// `$skip`: drop the first `n` documents.
fn run_skip(n: usize, mut docs: Vec<Document>) -> Vec<Document> {
    if n >= docs.len() {
        docs.clear();
    } else {
        docs.drain(..n);
    }
    docs
}

/// `$limit`: keep at most `n` documents.
fn run_limit(n: usize, mut docs: Vec<Document>) -> Vec<Document> {
    docs.truncate(n);
    docs
}

/// `$count`: replace the stream with a single `{<name>: <count>}` document.
///
/// The count is emitted as `Int32` when it fits, else `Int64`.
fn run_count(name: &str, docs: Vec<Document>) -> Vec<Document> {
    let count = docs.len();
    let value = i32::try_from(count)
        .map(Bson::Int32)
        .unwrap_or(Bson::Int64(count as i64));
    let mut out = Document::new();
    out.insert(name, value);
    vec![out]
}

/// `$project`: include/exclude fields and evaluate computed-expression fields.
///
/// A spec value of `1`/`0`/`true`/`false` (or any numeric truthy/falsy, per
/// find-path behavior) is a plain include/exclude flag handled by the find-path
/// projection. Any other value is a *computed field* expression: it is
/// evaluated per document and, when its result is *missing*, the field is
/// omitted. The presence of any computed field forces inclusion mode (computed
/// targets are written onto the include-projected document); dotted computed
/// targets create nested documents.
fn run_project(proj: &Document, docs: Vec<Document>, now: DateTime) -> Result<Vec<Document>> {
    // Partition the spec into the plain include/exclude flags (delegated to the
    // find-path projection) and the computed-field expressions.
    let mut include_exclude = Document::new();
    let mut computed: Vec<(String, Bson)> = Vec::new();
    for (field, value) in proj {
        if is_projection_flag(value) {
            include_exclude.insert(field.clone(), value.clone());
        } else {
            computed.push((field.clone(), value.clone()));
        }
    }

    // A computed field forces inclusion mode: mark inclusion so the find-path
    // projection keeps only flagged fields (and `_id` unless excluded), then
    // the computed values are layered on top.
    let force_include = !computed.is_empty();

    let mut out = Vec::with_capacity(docs.len());
    for doc in docs {
        let ctx = expr_ctx(&doc, now);
        let mut evaluated: Vec<(String, Option<Bson>)> = Vec::with_capacity(computed.len());
        for (field, expr) in &computed {
            evaluated.push((field.clone(), eval_expr(expr, &ctx)?));
        }

        let mut projected = if force_include && include_exclude_is_empty_inclusion(&include_exclude)
        {
            // No explicit include flags: start from `_id` only (computed fields
            // supply the rest), honoring an explicit `_id: 0` exclusion.
            project_id_only(&doc, &include_exclude)
        } else {
            apply_projection_to_doc(doc, &include_exclude)
        };

        for (field, value) in evaluated {
            match value {
                // A missing computed result omits the field entirely.
                None => {}
                Some(v) => set_nested_overwrite(&mut projected, &field, v),
            }
        }
        out.push(projected);
    }
    Ok(out)
}

/// True when a `$project` spec value is a plain include/exclude flag (`1`/`0`/
/// `true`/`false`, or any numeric truthy/falsy), rather than a computed
/// expression.
fn is_projection_flag(value: &Bson) -> bool {
    matches!(
        value,
        Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_) | Bson::Boolean(_)
    )
}

/// True when the include/exclude spec carries no inclusion flag (only an
/// optional `_id` directive), so computed fields alone determine the output.
fn include_exclude_is_empty_inclusion(spec: &Document) -> bool {
    spec.iter().all(|(k, _)| k == "_id")
}

/// Project only `_id` (unless explicitly excluded) for a computed-only
/// `$project` whose include/exclude spec names no fields.
fn project_id_only(doc: &Document, spec: &Document) -> Document {
    let id_excluded = spec
        .get("_id")
        .is_some_and(|v| matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)));
    let mut out = Document::new();
    if !id_excluded {
        if let Some(id) = doc.get("_id") {
            out.insert("_id", id.clone());
        }
    }
    out
}

/// Per-group running accumulator state, indexed parallel to the spec order.
struct GroupState {
    /// The group key (already folded so missing == null).
    id: Bson,
    /// One accumulator state per spec entry.
    accs: Vec<AccState>,
}

/// Running state for a single accumulator within a group.
enum AccState {
    /// `$sum`: integer-while-possible, promoting to `Double` on any double or
    /// integer overflow (mirrors `$inc`/`$mul` width rules).
    Sum(NumericAcc),
    /// `$avg`: running `Double` total and count of numeric values.
    Avg { total: f64, count: u64 },
    /// `$min`: smallest non-missing/non-null value seen so far.
    Min(Option<Bson>),
    /// `$max`: largest non-missing/non-null value seen so far.
    Max(Option<Bson>),
    /// `$first`: value from the first document (missing stays `None`).
    First(Option<Bson>),
    /// `$last`: value from the last document (missing stays `None`).
    Last(Option<Bson>),
    /// `$push`: every non-missing value, in order.
    Push(Vec<Bson>),
    /// `$addToSet`: deduplicated values in discovery order, with their keys.
    AddToSet { values: Vec<Bson>, keys: Vec<Vec<u8>> },
    /// `$mergeObjects`: accumulated document, merged in encounter order.
    MergeObjects(Document),
    /// `$stdDevPop` / `$stdDevSamp`: Welford running mean/M2 over numeric inputs.
    StdDev {
        count: u64,
        mean: f64,
        m2: f64,
        sample: bool,
    },
    /// `$firstN` / `$lastN`: values in encounter order, capped to `n`.
    FirstN {
        values: Vec<Bson>,
        n: usize,
        keep_last: bool,
    },
    /// `$minN` / `$maxN`: candidate `(key, value)` pairs, reduced at finish.
    MinMaxN {
        values: Vec<(Vec<u8>, Bson)>,
        n: usize,
        want_max: bool,
    },
}

/// `$sum` numeric accumulator preserving integer width while possible.
enum NumericAcc {
    /// Still an integer; promotes to `Int64` on `Int32` overflow.
    Int(i64),
    /// A `Double` value has been seen (or an integer overflowed `i64`).
    Float(f64),
}

impl NumericAcc {
    /// Fold `value` into the accumulator, ignoring non-numeric values.
    ///
    /// Width rule (mirrors `$inc` in `crate::update`): the accumulator stays an
    /// integer while every contribution is `Int32`/`Int64`; any `Double` makes
    /// it a `Double`, and an `i64` overflow likewise falls back to `Double`.
    fn add(&mut self, value: &Bson) {
        match value {
            Bson::Int32(n) => self.add_int(*n as i64),
            Bson::Int64(n) => self.add_int(*n),
            Bson::Double(f) => self.add_float(*f),
            _ => {}
        }
    }

    fn add_int(&mut self, n: i64) {
        match self {
            NumericAcc::Int(acc) => match acc.checked_add(n) {
                Some(sum) => *acc = sum,
                None => *self = NumericAcc::Float(*acc as f64 + n as f64),
            },
            NumericAcc::Float(acc) => *acc += n as f64,
        }
    }

    fn add_float(&mut self, f: f64) {
        match self {
            NumericAcc::Int(acc) => *self = NumericAcc::Float(*acc as f64 + f),
            NumericAcc::Float(acc) => *acc += f,
        }
    }

    /// Materialize the accumulated sum as a BSON value.
    fn into_bson(self) -> Bson {
        match self {
            NumericAcc::Int(n) => i32::try_from(n)
                .map(Bson::Int32)
                .unwrap_or(Bson::Int64(n)),
            NumericAcc::Float(f) => Bson::Double(f),
        }
    }
}

impl AccState {
    /// Create the initial state for `op`.
    fn new(op: &AccumulatorOp) -> Self {
        match op {
            AccumulatorOp::Sum => AccState::Sum(NumericAcc::Int(0)),
            AccumulatorOp::Avg => AccState::Avg {
                total: 0.0,
                count: 0,
            },
            AccumulatorOp::Min => AccState::Min(None),
            AccumulatorOp::Max => AccState::Max(None),
            AccumulatorOp::First => AccState::First(None),
            AccumulatorOp::Last => AccState::Last(None),
            AccumulatorOp::Push => AccState::Push(Vec::new()),
            AccumulatorOp::AddToSet => AccState::AddToSet {
                values: Vec::new(),
                keys: Vec::new(),
            },
            // `$count` reuses the `$sum` integer accumulator (its stored
            // argument is the constant 1).
            AccumulatorOp::Count => AccState::Sum(NumericAcc::Int(0)),
            AccumulatorOp::MergeObjects => AccState::MergeObjects(Document::new()),
            AccumulatorOp::StdDevPop => AccState::StdDev {
                count: 0,
                mean: 0.0,
                m2: 0.0,
                sample: false,
            },
            AccumulatorOp::StdDevSamp => AccState::StdDev {
                count: 0,
                mean: 0.0,
                m2: 0.0,
                sample: true,
            },
            AccumulatorOp::FirstN { n, keep_last } => AccState::FirstN {
                values: Vec::new(),
                n: *n,
                keep_last: *keep_last,
            },
            AccumulatorOp::MinMaxN { n, want_max } => AccState::MinMaxN {
                values: Vec::new(),
                n: *n,
                want_max: *want_max,
            },
        }
    }

    /// Fold the resolved expression value (`None` == missing) into this state.
    ///
    /// # Errors
    ///
    /// Returns a `BadValue` when `$mergeObjects` receives a non-document,
    /// non-null input.
    fn accumulate(&mut self, value: Option<&Bson>) -> Result<()> {
        match self {
            AccState::Sum(acc) => {
                if let Some(v) = value {
                    acc.add(v);
                }
            }
            AccState::Avg { total, count } => {
                if let Some(f) = value.and_then(numeric_as_f64) {
                    *total += f;
                    *count += 1;
                }
            }
            AccState::Min(current) => fold_extreme(current, value, std::cmp::Ordering::Less),
            AccState::Max(current) => fold_extreme(current, value, std::cmp::Ordering::Greater),
            AccState::First(slot) => {
                if slot.is_none() {
                    *slot = value.cloned();
                }
            }
            AccState::Last(slot) => {
                *slot = value.cloned();
            }
            AccState::Push(items) => {
                if let Some(v) = value {
                    items.push(v.clone());
                }
            }
            AccState::AddToSet { values, keys } => {
                if let Some(v) = value {
                    let key = encode_key(v);
                    if !keys.iter().any(|existing| existing == &key) {
                        keys.push(key);
                        values.push(v.clone());
                    }
                }
            }
            AccState::MergeObjects(merged) => match value {
                // Missing and explicit null are ignored.
                None | Some(Bson::Null) => {}
                Some(Bson::Document(doc)) => {
                    for (key, sub) in doc {
                        merged.insert(key.clone(), sub.clone());
                    }
                }
                Some(_) => {
                    return Err(bad_value("$mergeObjects requires object inputs"));
                }
            },
            AccState::StdDev {
                count, mean, m2, ..
            } => {
                if let Some(x) = value.and_then(numeric_as_f64) {
                    // Welford's online algorithm for numerical stability.
                    *count += 1;
                    let delta = x - *mean;
                    *mean += delta / *count as f64;
                    let delta2 = x - *mean;
                    *m2 += delta * delta2;
                }
            }
            AccState::FirstN {
                values,
                n,
                keep_last,
            } => {
                // null is included; only MISSING is skipped.
                if let Some(v) = value {
                    values.push(v.clone());
                    // $firstN caps eagerly; $lastN keeps the trailing window.
                    if !*keep_last && values.len() > *n {
                        values.truncate(*n);
                    } else if *keep_last && values.len() > *n {
                        values.remove(0);
                    }
                }
            }
            AccState::MinMaxN { values, .. } => {
                // null AND missing are skipped (matches $min/$max).
                if let Some(v) = value {
                    if !matches!(v, Bson::Null) {
                        values.push((encode_key(v), v.clone()));
                    }
                }
            }
        }
        Ok(())
    }

    /// Materialize this accumulator's result value.
    ///
    /// Every accumulator emits a value: `$first`/`$last` yield `null` for a
    /// missing value (per the MongoDB aggregation spec), `$avg` yields `null`
    /// for an empty group, and `$min`/`$max` yield `null` when nothing was
    /// seen.
    fn finish(self) -> Bson {
        match self {
            AccState::Sum(acc) => acc.into_bson(),
            AccState::Avg { total, count } => {
                if count == 0 {
                    Bson::Null
                } else {
                    Bson::Double(total / count as f64)
                }
            }
            AccState::Min(value) | AccState::Max(value) => value.unwrap_or(Bson::Null),
            // $first/$last yield null when the value was missing (MongoDB spec).
            AccState::First(value) | AccState::Last(value) => value.unwrap_or(Bson::Null),
            AccState::Push(items) => Bson::Array(items),
            AccState::AddToSet { values, .. } => Bson::Array(values),
            AccState::MergeObjects(merged) => Bson::Document(merged),
            AccState::StdDev {
                count, m2, sample, ..
            } => finish_std_dev(count, m2, sample),
            AccState::FirstN { values, .. } => Bson::Array(values),
            AccState::MinMaxN {
                values, n, want_max,
            } => finish_min_max_n(values, n, want_max),
        }
    }
}

/// Materialize a standard-deviation accumulator.
///
/// Population (`sample == false`): 0 values yield `null`, otherwise the
/// population stddev (`sqrt(M2 / n)`), which is `0.0` for a single value.
/// Sample (`sample == true`): fewer than 2 values yield `null`, otherwise the
/// sample stddev (`sqrt(M2 / (n - 1))`).
fn finish_std_dev(count: u64, m2: f64, sample: bool) -> Bson {
    if sample {
        if count < 2 {
            return Bson::Null;
        }
        return Bson::Double((m2 / (count - 1) as f64).sqrt());
    }
    if count == 0 {
        return Bson::Null;
    }
    Bson::Double((m2 / count as f64).sqrt())
}

/// Materialize a `$minN`/`$maxN` accumulator: take the `n` smallest (or largest)
/// candidates by BSON ordering and emit them sorted ascending (`$minN`) or
/// descending (`$maxN`).
fn finish_min_max_n(mut values: Vec<(Vec<u8>, Bson)>, n: usize, want_max: bool) -> Bson {
    values.sort_by(|a, b| a.0.cmp(&b.0));
    let selected: Vec<Bson> = if want_max {
        // Largest n, then reverse so output is descending.
        values
            .into_iter()
            .rev()
            .take(n)
            .map(|(_, v)| v)
            .collect()
    } else {
        values.into_iter().take(n).map(|(_, v)| v).collect()
    };
    Bson::Array(selected)
}

/// Fold `value` into a `$min`/`$max` slot, ignoring missing AND null values.
///
/// `keep` is [`std::cmp::Ordering::Less`] for `$min` and `Greater` for `$max`:
/// the candidate replaces the current extreme when `candidate.cmp(current)`
/// equals `keep`.
fn fold_extreme(current: &mut Option<Bson>, value: Option<&Bson>, keep: std::cmp::Ordering) {
    let candidate = match value {
        Some(v) if !matches!(v, Bson::Null) => v,
        _ => return,
    };
    match current {
        None => *current = Some(candidate.clone()),
        Some(existing) => {
            if encode_key(candidate).cmp(&encode_key(existing)) == keep {
                *existing = candidate.clone();
            }
        }
    }
}

/// Extract a numeric BSON value as `f64`, or `None` for non-numeric values.
fn numeric_as_f64(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(n) => Some(*n as f64),
        Bson::Int64(n) => Some(*n as f64),
        Bson::Double(f) => Some(*f),
        _ => None,
    }
}

/// `$group`: combine documents by `id`, preserving first-seen group order.
///
/// Group-key equality uses the crate's canonical BSON encoding so that, e.g.,
/// `Int32(1)` and `Double(1.0)` group together; a missing field-path key folds
/// to `null` and groups with explicit `null`.
fn run_group(
    id: &Expr,
    accumulators: &[Accumulator],
    docs: Vec<Document>,
    now: DateTime,
) -> Result<Vec<Document>> {
    let mut order: Vec<Vec<u8>> = Vec::new();
    let mut groups: Vec<(Vec<u8>, GroupState)> = Vec::new();

    for doc in &docs {
        // A missing group-key path folds to null (MongoDB groups missing with
        // null in $group).
        let key_value = id.resolve(doc, now)?.unwrap_or(Bson::Null);
        let key_bytes = encode_key(&key_value);

        let position = match order.iter().position(|existing| existing == &key_bytes) {
            Some(pos) => pos,
            None => {
                order.push(key_bytes.clone());
                groups.push((
                    key_bytes,
                    GroupState {
                        id: key_value,
                        accs: accumulators.iter().map(|a| AccState::new(&a.op)).collect(),
                    },
                ));
                groups.len() - 1
            }
        };

        let state = &mut groups[position].1;
        for (acc, acc_state) in accumulators.iter().zip(state.accs.iter_mut()) {
            let value = acc.arg.resolve(doc, now)?;
            acc_state.accumulate(value.as_ref())?;
        }
    }

    Ok(groups
        .into_iter()
        .map(|(_, state)| build_group_doc(state, accumulators))
        .collect())
}

/// Assemble a group's output document: `_id` first, then accumulators in spec
/// order. Every accumulator emits a value (`$first`/`$last` use `null` for a
/// missing value, per the MongoDB spec).
fn build_group_doc(state: GroupState, accumulators: &[Accumulator]) -> Document {
    let mut out = Document::new();
    out.insert(GROUP_ID_FIELD, state.id);
    for (acc, acc_state) in accumulators.iter().zip(state.accs.into_iter()) {
        out.insert(&acc.out_field, acc_state.finish());
    }
    out
}

// ---------------------------------------------------------------------------
// $addFields / $set
// ---------------------------------------------------------------------------

/// `$addFields`/`$set`: evaluate each `(field, expr)` against the *original*
/// incoming document and assign in spec order.
///
/// Every expression sees the document as it entered the stage (snapshot
/// semantics), so a field referencing another field set earlier in the same
/// stage observes the original value, not the freshly-assigned one. A *missing*
/// result leaves the target field unchanged; dotted targets create nested
/// documents, overwriting non-document intermediates.
fn run_add_fields(
    fields: &[(String, Bson)],
    docs: Vec<Document>,
    now: DateTime,
) -> Result<Vec<Document>> {
    let mut out = Vec::with_capacity(docs.len());
    for doc in docs {
        // Evaluate every expression against the original document first so
        // intra-stage references see pre-assignment values.
        let ctx = expr_ctx(&doc, now);
        let mut evaluated: Vec<(&str, Option<Bson>)> = Vec::with_capacity(fields.len());
        for (field, expr) in fields {
            evaluated.push((field.as_str(), eval_expr(expr, &ctx)?));
        }
        drop(ctx);

        let mut result = doc;
        for (field, value) in evaluated {
            // A missing result leaves the field unchanged (MongoDB spec).
            if let Some(v) = value {
                set_nested_overwrite(&mut result, field, v);
            }
        }
        out.push(result);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// $replaceRoot / $replaceWith
// ---------------------------------------------------------------------------

/// `$replaceRoot`/`$replaceWith`: evaluate `expr` per document; the result must
/// be a document, which becomes the new root. A null/missing/non-document
/// result is an error.
fn run_replace_root(expr: &Bson, docs: Vec<Document>, now: DateTime) -> Result<Vec<Document>> {
    let mut out = Vec::with_capacity(docs.len());
    for doc in docs {
        let ctx = expr_ctx(&doc, now);
        let value = eval_expr(expr, &ctx)?;
        match value {
            Some(Bson::Document(new_root)) => out.push(new_root),
            other => {
                return Err(bad_value(format!(
                    "'newRoot' expression must evaluate to an object, but resulting \
                     value was: {}",
                    display_value(&other)
                )))
            }
        }
    }
    Ok(out)
}

/// Render an evaluated [`Value`] for the `$replaceRoot` error message.
fn display_value(value: &Option<Bson>) -> String {
    match value {
        None => "MISSING".to_owned(),
        Some(v) => format!("{v:?}"),
    }
}

// ---------------------------------------------------------------------------
// $unwind
// ---------------------------------------------------------------------------

/// `$unwind`: emit one output document per element of the array at `path`.
///
/// Semantics (item 6): an array value yields one document per element with the
/// path replaced by the element; a present non-array, non-null value passes
/// through unchanged as a single document. A null, missing, or empty-array
/// input is dropped unless `preserveNullAndEmptyArrays` keeps a single document
/// (null stays null; missing stays missing; an empty array has the path field
/// removed). When `includeArrayIndex` is set, the index field is the element's
/// `Int64` index, or `null` for the non-array / preserved cases.
fn run_unwind(unwind: &Unwind, docs: Vec<Document>) -> Vec<Document> {
    let mut out = Vec::new();
    for doc in docs {
        unwind_one(unwind, doc, &mut out);
    }
    out
}

/// Expand a single document into `out` according to `unwind`.
fn unwind_one(unwind: &Unwind, doc: Document, out: &mut Vec<Document>) {
    let value = get_nested_field(&doc, &unwind.path).cloned();
    match value {
        Some(Bson::Array(elements)) => {
            if elements.is_empty() {
                if unwind.preserve_null_and_empty {
                    // Empty array: remove the path field, index null.
                    let mut kept = doc;
                    remove_nested_path(&mut kept, &unwind.path);
                    set_index_field(unwind, &mut kept, None);
                    out.push(kept);
                }
                return;
            }
            for (index, element) in elements.into_iter().enumerate() {
                let mut emitted = doc.clone();
                set_nested_overwrite(&mut emitted, &unwind.path, element);
                set_index_field(unwind, &mut emitted, Some(index as i64));
                out.push(emitted);
            }
        }
        // A present, non-array, non-null value passes through as one document.
        Some(Bson::Null) | None => {
            if unwind.preserve_null_and_empty {
                let mut kept = doc;
                set_index_field(unwind, &mut kept, None);
                out.push(kept);
            }
        }
        Some(_) => {
            let mut kept = doc;
            set_index_field(unwind, &mut kept, None);
            out.push(kept);
        }
    }
}

/// Write the `includeArrayIndex` output field, if configured.
///
/// `index` is the `Int64` element index, or `None` to write an explicit null
/// (non-array passthrough and preserved null/missing/empty cases).
fn set_index_field(unwind: &Unwind, doc: &mut Document, index: Option<i64>) {
    let Some(field) = &unwind.include_array_index else {
        return;
    };
    let value = index.map_or(Bson::Null, Bson::Int64);
    set_nested_overwrite(doc, field, value);
}

// ---------------------------------------------------------------------------
// $lookup
// ---------------------------------------------------------------------------

/// `$lookup` (equality form): left-outer join each input document against the
/// foreign collection resolved by `resolve_foreign`.
///
/// The `as` field receives an array of every foreign document whose
/// `foreignField` value equals the input's `localField` value under the crate's
/// BSON equality. Either side being an array matches per element (the field's
/// array elements are unwrapped on both sides); a missing field is treated as
/// null and matches foreign nulls and missing fields. A nonexistent `from`
/// collection yields an empty match array.
fn run_lookup(
    lookup: &Lookup,
    docs: Vec<Document>,
    resolve_foreign: &LookupResolver<'_>,
) -> Result<Vec<Document>> {
    let foreign_docs = resolve_foreign(&lookup.from)?;
    // Precompute each foreign document's comparison keys once.
    let foreign_keys: Vec<Vec<Vec<u8>>> = foreign_docs
        .iter()
        .map(|fdoc| match_keys(fdoc, &lookup.foreign_field))
        .collect();

    let mut out = Vec::with_capacity(docs.len());
    for mut doc in docs {
        let local_keys = match_keys(&doc, &lookup.local_field);
        let mut matched = Vec::new();
        for (fdoc, fkeys) in foreign_docs.iter().zip(foreign_keys.iter()) {
            if keys_intersect(&local_keys, fkeys) {
                matched.push(Bson::Document(fdoc.clone()));
            }
        }
        set_nested_overwrite(&mut doc, &lookup.as_field, Bson::Array(matched));
        out.push(doc);
    }
    Ok(out)
}

/// Compute the equality-comparison keys for `path` in `doc`.
///
/// A missing field is treated as a single `null` key (so it matches foreign
/// nulls and missing fields). An array value contributes one key per element
/// (per-element matching, like find equality); every other value contributes a
/// single key.
fn match_keys(doc: &Document, path: &str) -> Vec<Vec<u8>> {
    match get_nested_field(doc, path) {
        None => vec![encode_key(&Bson::Null)],
        Some(Bson::Array(elements)) if !elements.is_empty() => {
            elements.iter().map(encode_key).collect()
        }
        Some(value) => vec![encode_key(value)],
    }
}

/// True when any key in `local` equals any key in `foreign`.
fn keys_intersect(local: &[Vec<u8>], foreign: &[Vec<u8>]) -> bool {
    local
        .iter()
        .any(|lk| foreign.iter().any(|fk| lk == fk))
}

// ---------------------------------------------------------------------------
// Nested-path assignment
// ---------------------------------------------------------------------------

/// Assign `value` at the dotted `path` in `doc`, creating intermediate
/// documents and overwriting any non-document value encountered along the way.
///
/// Used by computed `$project`, `$addFields`/`$set`, `$unwind`, and `$lookup`
/// to write (possibly nested) output fields. A single (non-dotted) path is a
/// direct insert.
fn set_nested_overwrite(doc: &mut Document, path: &str, value: Bson) {
    let mut parts = path.splitn(2, '.');
    let head = parts.next().unwrap_or(path);
    match parts.next() {
        None => {
            doc.insert(head, value);
        }
        Some(rest) => {
            // Ensure an intermediate document exists, overwriting non-documents.
            let needs_doc = !matches!(doc.get(head), Some(Bson::Document(_)));
            if needs_doc {
                doc.insert(head, Bson::Document(Document::new()));
            }
            if let Some(Bson::Document(child)) = doc.get_mut(head) {
                set_nested_overwrite(child, rest, value);
            }
        }
    }
}

/// Remove the value at the dotted `path` in `doc` (used by `$unwind`'s
/// empty-array preservation). Intermediate non-documents are left untouched.
fn remove_nested_path(doc: &mut Document, path: &str) {
    let mut parts = path.splitn(2, '.');
    let head = parts.next().unwrap_or(path);
    match parts.next() {
        None => {
            doc.remove(head);
        }
        Some(rest) => {
            if let Some(Bson::Document(child)) = doc.get_mut(head) {
                remove_nested_path(child, rest);
            }
        }
    }
}
