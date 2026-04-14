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
#[derive(Debug, Clone)]
pub struct IndexModel {
    /// Key pattern for the index. Values are 1 (ascending) or -1 (descending).
    pub keys: Document,
    /// Optional index options.
    pub options: IndexOptions,
}

/// Builder for `IndexModel`.
#[derive(Default)]
pub struct IndexModelBuilder {
    keys: Option<Document>,
    options: IndexOptions,
}

impl IndexModel {
    /// Create a builder for `IndexModel`.
    pub fn builder() -> IndexModelBuilder {
        IndexModelBuilder::default()
    }
}

impl IndexModelBuilder {
    /// Set the key pattern for the index.
    pub fn keys(mut self, keys: Document) -> Self {
        self.keys = Some(keys);
        self
    }

    /// Set the index options.
    pub fn options(mut self, options: IndexOptions) -> Self {
        self.options = options;
        self
    }

    /// Build the `IndexModel`.
    ///
    /// # Panics
    /// Panics if `keys` was not set.
    pub fn build(self) -> IndexModel {
        IndexModel {
            keys: self.keys.expect("IndexModel::builder().keys(...) is required"),
            options: self.options,
        }
    }
}

/// Information about an existing index returned by `Collection::list_indexes`.
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
