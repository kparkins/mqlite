use bson::oid::ObjectId;

/// Result returned by `Collection::insert_one`.
#[derive(Debug, Clone)]
pub struct InsertOneResult {
    /// The `_id` of the inserted document.
    pub inserted_id: ObjectId,
}

/// Result returned by `Collection::insert_many`.
#[derive(Debug, Clone)]
pub struct InsertManyResult {
    /// The `_id` values of the inserted documents, in insertion order.
    pub inserted_ids: Vec<ObjectId>,
}

/// Result returned by `Collection::update_one` and `Collection::update_many`.
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Number of documents that matched the filter.
    pub matched_count: u64,
    /// Number of documents actually modified.
    pub modified_count: u64,
    /// The `_id` of the upserted document, if an upsert occurred.
    pub upserted_id: Option<ObjectId>,
}

/// Result returned by `Collection::delete_one` and `Collection::delete_many`.
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// Number of documents deleted.
    pub deleted_count: u64,
}
