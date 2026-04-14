use bson::Document;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

use crate::{
    cursor::Cursor,
    error::Result,
    index::{IndexInfo, IndexModel},
    options::{FindOptions, InsertManyOptions, UpdateOptions},
    results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
};

/// A handle to a named collection within a [`Database`].
///
/// `Collection<T>` is cheap to clone — all clones share the same underlying storage.
///
/// # Type Parameter
/// `T` must implement `Serialize + DeserializeOwned`. Use `Collection<bson::Document>`
/// for untyped access.
///
/// # Example
/// ```no_run
/// use mqlite::{Database, doc};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct User { name: String, email: String }
///
/// # fn main() -> mqlite::Result<()> {
/// let db = Database::open_in_memory()?;
/// let users = db.collection::<User>("users");
/// users.insert_one(&User { name: "Alice".into(), email: "alice@example.com".into() })?;
/// # Ok(())
/// # }
/// ```
pub struct Collection<T> {
    pub(crate) name: String,
    pub(crate) inner: Arc<crate::database::DatabaseInner>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

impl<T> Clone for Collection<T> {
    fn clone(&self) -> Self {
        Collection {
            name: self.name.clone(),
            inner: Arc::clone(&self.inner),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: Serialize + DeserializeOwned> Collection<T> {
    // -------------------------------------------------------------------------
    // Insert
    // -------------------------------------------------------------------------

    /// Insert a single document. Returns the `_id` of the inserted document.
    pub fn insert_one(&self, doc: &T) -> Result<InsertOneResult> {
        self.inner.insert_one(&self.name, doc)
    }

    /// Insert multiple documents. Returns the `_id` values of all inserted documents.
    pub fn insert_many(&self, docs: &[T]) -> Result<InsertManyResult> {
        self.inner
            .insert_many(&self.name, docs, InsertManyOptions::new())
    }

    /// Insert multiple documents with options.
    pub fn insert_many_with_options(
        &self,
        docs: &[T],
        opts: InsertManyOptions,
    ) -> Result<InsertManyResult> {
        self.inner.insert_many(&self.name, docs, opts)
    }

    // -------------------------------------------------------------------------
    // Find
    // -------------------------------------------------------------------------

    /// Find the first document matching `filter`. Returns `None` if no document matches.
    pub fn find_one(&self, filter: Document) -> Result<Option<T>> {
        self.inner.find_one(&self.name, filter)
    }

    /// Find all documents matching `filter`. Returns a [`Cursor`] over the results.
    pub fn find(&self, filter: Document) -> Result<Cursor<T>> {
        self.inner.find(&self.name, filter, FindOptions::new())
    }

    /// Find all documents matching `filter` with options (sort, limit, skip, projection).
    pub fn find_with_options(&self, filter: Document, opts: FindOptions) -> Result<Cursor<T>> {
        self.inner.find(&self.name, filter, opts)
    }

    // -------------------------------------------------------------------------
    // Update
    // -------------------------------------------------------------------------

    /// Update the first document matching `filter`.
    pub fn update_one(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        self.inner
            .update_one(&self.name, filter, update, UpdateOptions::new())
    }

    /// Update the first document matching `filter` with options (e.g., upsert).
    pub fn update_one_with_options(
        &self,
        filter: Document,
        update: Document,
        opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        self.inner.update_one(&self.name, filter, update, opts)
    }

    /// Update all documents matching `filter`.
    pub fn update_many(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        self.inner
            .update_many(&self.name, filter, update, UpdateOptions::new())
    }

    /// Update all documents matching `filter` with options.
    pub fn update_many_with_options(
        &self,
        filter: Document,
        update: Document,
        opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        self.inner.update_many(&self.name, filter, update, opts)
    }

    // -------------------------------------------------------------------------
    // Delete
    // -------------------------------------------------------------------------

    /// Delete the first document matching `filter`.
    pub fn delete_one(&self, filter: Document) -> Result<DeleteResult> {
        self.inner.delete_one(&self.name, filter)
    }

    /// Delete all documents matching `filter`.
    pub fn delete_many(&self, filter: Document) -> Result<DeleteResult> {
        self.inner.delete_many(&self.name, filter)
    }

    // -------------------------------------------------------------------------
    // Atomic read-modify-write (findAndModify equivalents)
    // -------------------------------------------------------------------------

    /// Atomically find the first document matching `filter`, apply `update`, and return the
    /// original document (before the update). Returns `None` if no document matched.
    pub fn find_one_and_update(&self, filter: Document, update: Document) -> Result<Option<T>> {
        self.inner.find_one_and_update(&self.name, filter, update)
    }

    /// Atomically find the first document matching `filter`, delete it, and return it.
    /// Returns `None` if no document matched.
    pub fn find_one_and_delete(&self, filter: Document) -> Result<Option<T>> {
        self.inner.find_one_and_delete(&self.name, filter)
    }

    /// Atomically find the first document matching `filter`, replace it with `replacement`,
    /// and return the original document. Returns `None` if no document matched.
    pub fn find_one_and_replace(&self, filter: Document, replacement: &T) -> Result<Option<T>> {
        self.inner
            .find_one_and_replace(&self.name, filter, replacement)
    }

    // -------------------------------------------------------------------------
    // Count
    // -------------------------------------------------------------------------

    /// Return an approximate count of all documents in the collection.
    /// This is a fast estimate, not a precise count.
    pub fn estimated_document_count(&self) -> Result<u64> {
        self.inner.estimated_document_count(&self.name)
    }

    /// Return the exact count of documents matching `filter`.
    pub fn count_documents(&self, filter: Document) -> Result<u64> {
        self.inner.count_documents(&self.name, filter)
    }

    // -------------------------------------------------------------------------
    // Indexes
    // -------------------------------------------------------------------------

    /// Create an index on the collection.
    pub fn create_index(&self, model: IndexModel) -> Result<String> {
        self.inner.create_index(&self.name, model)
    }

    /// Drop an index by name.
    pub fn drop_index(&self, index_name: &str) -> Result<()> {
        self.inner.drop_index(&self.name, index_name)
    }

    /// List all indexes on the collection.
    pub fn list_indexes(&self) -> Result<Vec<IndexInfo>> {
        self.inner.list_indexes(&self.name)
    }
}
