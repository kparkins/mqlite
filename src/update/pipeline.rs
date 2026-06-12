//! Pipeline-form updates: `update`/`findAndModify` with an aggregation
//! pipeline instead of an operator document.
//!
//! MongoDB 5.0+ accepts an array of aggregation stages in place of an update
//! operator document. Only a restricted set of stages is permitted; any other
//! stage is rejected before execution. The restricted pipeline is run over the
//! single target document through the additive helper
//! [`crate::query::aggregate::run_single_doc_pipeline`].

use bson::{Bson, Document};

use crate::error::{Error, Result};

/// The pipeline stages permitted inside an update.
///
/// MongoDB restricts update pipelines to these document-shaping stages; every
/// other stage (e.g. `$match`, `$group`, `$lookup`) is rejected.
const ALLOWED_STAGES: [&str; 6] = [
    "$addFields",
    "$set",
    "$project",
    "$unset",
    "$replaceRoot",
    "$replaceWith",
];

/// An update modification: either a classic operator/replacement [`Document`]
/// or an aggregation [`Pipeline`](UpdateModifications::Pipeline).
///
/// Mirrors the official MongoDB Rust driver's `UpdateModifications`, letting
/// `update_one`/`update_many`/`find_one_and_update` accept either form via
/// `impl Into<UpdateModifications>`.
#[derive(Debug, Clone, PartialEq)]
pub enum UpdateModifications {
    /// A classic update operator document (or replacement document).
    Document(Document),
    /// An aggregation pipeline (array of stage documents).
    Pipeline(Vec<Document>),
}

impl From<Document> for UpdateModifications {
    fn from(doc: Document) -> Self {
        UpdateModifications::Document(doc)
    }
}

impl From<Vec<Document>> for UpdateModifications {
    fn from(pipeline: Vec<Document>) -> Self {
        UpdateModifications::Pipeline(pipeline)
    }
}

/// Validate that every stage in `pipeline` is permitted inside an update.
///
/// Each element must be a single-key document naming an allowed stage. Runs
/// before execution so a disallowed stage is reported without mutating any
/// document.
///
/// # Errors
///
/// Returns [`Error::Internal`] naming the first disallowed stage, or when a
/// pipeline element is not a single-key stage document.
fn validate_update_pipeline(pipeline: &[Document]) -> Result<()> {
    for stage in pipeline {
        let mut keys = stage.keys();
        let Some(name) = keys.next() else {
            return Err(Error::Internal(
                "Each element of an update pipeline must be a stage document".to_owned(),
            ));
        };
        if keys.next().is_some() {
            return Err(Error::Internal(
                "An update pipeline stage must contain exactly one field".to_owned(),
            ));
        }
        if !ALLOWED_STAGES.contains(&name.as_str()) {
            return Err(Error::Internal(format!(
                "{name} is not allowed to be used within an update"
            )));
        }
    }
    Ok(())
}

/// Apply an update `pipeline` to `doc`, returning the transformed document.
///
/// Validates the stage allow-list, runs the pipeline over the single document
/// through the aggregation engine, and enforces `_id` immutability: a result
/// `_id` differing from the original (when the original had one) is an error,
/// and a result missing `_id` restores the original.
///
/// # Errors
///
/// Returns [`Error::Internal`] for a disallowed stage, a result that changes
/// the original `_id`, or any error raised while executing the pipeline.
pub(crate) fn apply_update_pipeline(doc: &Document, pipeline: &[Document]) -> Result<Document> {
    validate_update_pipeline(pipeline)?;

    let original_id = doc.get("_id").cloned();
    let mut result = crate::query::aggregate::run_single_doc_pipeline(doc, pipeline)?;

    match (&original_id, result.get("_id").cloned()) {
        (Some(original), Some(new_id)) if !ids_equal(original, &new_id) => {
            return Err(immutable_id_error());
        }
        (Some(original), None) => {
            // A pipeline that drops `_id` restores the original (MongoDB keeps
            // `_id` immutable across pipeline updates).
            insert_id_first(&mut result, original.clone());
        }
        _ => {}
    }

    Ok(result)
}

/// Reinsert `_id` as the first field of `doc`, preserving the rest in order.
fn insert_id_first(doc: &mut Document, id: Bson) {
    let mut rebuilt = Document::new();
    rebuilt.insert("_id", id);
    for (key, value) in std::mem::take(doc) {
        if key == "_id" {
            continue;
        }
        rebuilt.insert(key, value);
    }
    *doc = rebuilt;
}

/// Compare two `_id` values under MongoDB's canonical equality
/// (`Int32(1)` equals `Double(1.0)`).
fn ids_equal(a: &Bson, b: &Bson) -> bool {
    crate::keys::encode_key(a) == crate::keys::encode_key(b)
}

/// Build the immutable-`_id` error, matching the shape `replace_one` reports.
fn immutable_id_error() -> Error {
    Error::DocumentValidationFailure {
        detail: "the (immutable) field '_id' was found to have been altered \
                 by the update pipeline"
            .to_owned(),
    }
}
