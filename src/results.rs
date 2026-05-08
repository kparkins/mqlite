use std::collections::HashMap;

use bson::{oid::ObjectId, Bson};

/// Result returned by `Collection::insert_one`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct InsertOneResult {
    /// The `_id` of the inserted document.
    pub inserted_id: ObjectId,
}

/// An error that occurred during a bulk write operation.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct BulkWriteError {
    /// The zero-based index of the document that failed.
    pub index: usize,
    /// The error that occurred.
    pub code: i32,
    /// A human-readable description of the error.
    pub message: String,
}

/// Result returned by `Collection::insert_many`.
///
/// For ordered inserts (`InsertManyOptions::ordered = true`, the default),
/// execution stops at the first error. For unordered inserts, all documents
/// are attempted and all errors are collected in `errors`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct InsertManyResult {
    /// Map of insertion index → `_id` for each successfully inserted document.
    pub inserted_ids: HashMap<usize, Bson>,
    /// Errors that occurred during insertion (empty on full success).
    pub errors: Vec<BulkWriteError>,
}

/// Result returned by `Collection::update_one` and `Collection::update_many`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Number of documents that matched the filter.
    pub matched_count: u64,
    /// Number of documents actually modified.
    pub modified_count: u64,
    /// The `_id` of the upserted document, if an upsert occurred.
    /// The `_id` can be any BSON type, not just `ObjectId`.
    pub upserted_id: Option<Bson>,
}

/// Result returned by `Collection::delete_one` and `Collection::delete_many`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// Number of documents deleted.
    pub deleted_count: u64,
}
