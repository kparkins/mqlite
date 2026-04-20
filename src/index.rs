use std::marker::PhantomData;

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
}

/// Typestate marker: builder has no keys set yet.
pub struct NoKeys;
/// Typestate marker: builder has keys set.
pub struct HasKeys;

/// Builder for [`IndexModel`].
///
/// Call `.keys(doc)` first to transition from `NoKeys` to `HasKeys`, then `.build()`.
pub struct IndexModelBuilder<S = NoKeys> {
    keys: Option<Document>,
    options: IndexOptions,
    _state: PhantomData<S>,
}

impl IndexModel {
    /// Create a builder for `IndexModel`.
    #[must_use]
    pub fn builder() -> IndexModelBuilder<NoKeys> {
        IndexModelBuilder {
            keys: None,
            options: IndexOptions::default(),
            _state: PhantomData,
        }
    }
}

impl<S> IndexModelBuilder<S> {
    /// Set the index options.
    #[must_use]
    pub fn options(self, options: IndexOptions) -> IndexModelBuilder<S> {
        IndexModelBuilder {
            keys: self.keys,
            options,
            _state: PhantomData,
        }
    }
}

impl IndexModelBuilder<NoKeys> {
    /// Set the key pattern for the index. Transitions the builder to `HasKeys`.
    #[must_use]
    pub fn keys(self, keys: Document) -> IndexModelBuilder<HasKeys> {
        IndexModelBuilder {
            keys: Some(keys),
            options: self.options,
            _state: PhantomData,
        }
    }
}

impl IndexModelBuilder<HasKeys> {
    /// Build the [`IndexModel`]. Infallible — the typestate guarantees keys are present.
    #[must_use]
    pub fn build(self) -> IndexModel {
        IndexModel {
            // SAFETY: HasKeys typestate guarantees keys is Some.
            keys: self.keys.expect("HasKeys typestate guarantees keys is Some"),
            options: self.options,
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
}
