use bson::Document;

use crate::options::IndexOptions;

/// Specifies an index to create on a collection.
///
/// # Example
/// ```no_run
/// use mqlite::{IndexModel, IndexOptions};
/// use bson::doc;
///
/// let model = IndexModel::builder()
///     .keys(doc! { "email": 1 })
///     .options(IndexOptions::new().unique(true))
///     .build();
/// ```
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct IndexModel {
    /// Key pattern for the index. Values are 1 (ascending) or -1 (descending).
    pub(crate) keys: Document,
    /// Optional index options.
    pub(crate) options: IndexOptions,
    /// Optional partial-index filter. When set, only documents matching this
    /// filter are referenced by the index (MongoDB `partialFilterExpression`).
    pub(crate) partial_filter_expression: Option<Document>,
    /// Optional TTL: documents expire this many seconds after the indexed
    /// field's BSON date value (MongoDB `expireAfterSeconds`). `None` for a
    /// non-TTL index. Only valid on single-field, non-`_id` indexes.
    pub(crate) expire_after_seconds: Option<i64>,
}

/// Typestate marker: builder has no keys set yet.
pub struct NoKeys;
/// Typestate marker: builder has keys set.
pub struct HasKeys(Document);

/// Builder for [`IndexModel`].
///
/// Call `.keys(doc)` first to transition from `NoKeys` to `HasKeys`, then `.build()`.
pub struct IndexModelBuilder<S = NoKeys> {
    options: IndexOptions,
    partial_filter_expression: Option<Document>,
    expire_after_seconds: Option<i64>,
    state: S,
}

impl IndexModel {
    /// Create a builder for `IndexModel`.
    #[must_use]
    pub fn builder() -> IndexModelBuilder<NoKeys> {
        IndexModelBuilder {
            options: IndexOptions::default(),
            partial_filter_expression: None,
            expire_after_seconds: None,
            state: NoKeys,
        }
    }
}

impl<S> IndexModelBuilder<S> {
    /// Set the index options.
    #[must_use]
    pub fn options(self, options: IndexOptions) -> Self {
        Self { options, ..self }
    }

    /// Set the partial-index filter expression (MongoDB
    /// `partialFilterExpression`). Only documents matching `filter` are
    /// referenced by the index.
    #[must_use]
    pub fn partial_filter_expression(self, filter: Document) -> Self {
        Self {
            partial_filter_expression: Some(filter),
            ..self
        }
    }

    /// Set the TTL expiry in seconds (MongoDB `expireAfterSeconds`). Documents
    /// expire `seconds` after the indexed field's BSON date value. Only valid
    /// on single-field, non-`_id` indexes; validated at create time.
    #[must_use]
    pub fn expire_after_seconds(self, seconds: i64) -> Self {
        Self {
            expire_after_seconds: Some(seconds),
            ..self
        }
    }
}

impl IndexModelBuilder<NoKeys> {
    /// Set the key pattern for the index. Transitions the builder to `HasKeys`.
    #[must_use]
    pub fn keys(self, keys: Document) -> IndexModelBuilder<HasKeys> {
        IndexModelBuilder {
            options: self.options,
            partial_filter_expression: self.partial_filter_expression,
            expire_after_seconds: self.expire_after_seconds,
            state: HasKeys(keys),
        }
    }
}

impl IndexModelBuilder<HasKeys> {
    /// Build the [`IndexModel`]. Infallible: the typestate guarantees keys are present.
    #[must_use]
    pub fn build(self) -> IndexModel {
        let IndexModelBuilder {
            options,
            partial_filter_expression,
            expire_after_seconds,
            state: HasKeys(keys),
        } = self;
        IndexModel {
            keys,
            options,
            partial_filter_expression,
            expire_after_seconds,
        }
    }
}

/// Information about an existing index returned by `Collection::list_indexes`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct IndexInfo {
    /// The index name (auto-generated or user-specified).
    pub name: String,
    /// The key pattern of the index.
    pub keys: Document,
    /// Whether the index enforces uniqueness.
    pub unique: bool,
    /// Whether this is a sparse index.
    pub sparse: bool,
    /// Partial-index filter expression, if this is a partial index
    /// (MongoDB `partialFilterExpression`). `None` for ordinary indexes.
    pub partial_filter_expression: Option<Document>,
    /// TTL expiry in seconds (MongoDB `expireAfterSeconds`), if this is a TTL
    /// index. `None` for non-TTL indexes.
    pub expire_after_seconds: Option<i64>,
}
