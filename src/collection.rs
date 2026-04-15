use bson::Document;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

use crate::{
    cursor::Cursor,
    error::Result,
    index::{IndexInfo, IndexModel},
    options::{
        FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
        InsertManyOptions, UpdateOptions,
    },
    results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
};

/// A typed handle to a named collection within a [`crate::Database`].
///
/// `Collection<T>` is cheap to clone — all clones share the same underlying storage.
/// Collections are addressed by their **qualified name** (`<db_name>.<collection_name>`)
/// in the storage engine, providing logical namespace isolation.
///
/// # Type Parameter
/// `T` must implement `Serialize + DeserializeOwned`. Use `Collection<bson::Document>`
/// for untyped access.
///
/// # Example
/// ```no_run
/// use mqlite::{Client, doc};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct User { name: String, email: String }
///
/// # fn main() -> mqlite::Result<()> {
/// let client = Client::open_in_memory()?;
/// let db = client.database("myapp");
/// let users = db.collection::<User>("users");
/// users.insert_one(&User { name: "Alice".into(), email: "alice@example.com".into() })?;
/// # Ok(())
/// # }
/// ```
pub struct Collection<T> {
    /// The database name component of the qualified namespace.
    pub(crate) db_name: String,
    /// The unqualified collection name.
    pub(crate) name: String,
    pub(crate) inner: Arc<crate::client::ClientInner>,
    pub(crate) _phantom: std::marker::PhantomData<T>,
}

impl<T> Clone for Collection<T> {
    fn clone(&self) -> Self {
        Collection {
            db_name: self.db_name.clone(),
            name: self.name.clone(),
            inner: Arc::clone(&self.inner),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> Collection<T> {
    /// The fully-qualified namespace: `<db_name>.<collection_name>`.
    ///
    /// This is the key used in the storage engine, matching MongoDB's namespace format.
    pub fn namespace(&self) -> String {
        format!("{}.{}", self.db_name, self.name)
    }

    /// The unqualified collection name (without the database prefix).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The database name this collection belongs to.
    pub fn db_name(&self) -> &str {
        &self.db_name
    }
}

impl<T: Serialize + DeserializeOwned> Collection<T> {
    // -------------------------------------------------------------------------
    // Insert
    // -------------------------------------------------------------------------

    /// Insert a single document. Returns the `_id` of the inserted document.
    pub fn insert_one(&self, doc: &T) -> Result<InsertOneResult> {
        self.inner.insert_one(&self.namespace(), doc)
    }

    /// Insert multiple documents. Returns the `_id` values of all inserted documents.
    pub fn insert_many(&self, docs: &[T]) -> Result<InsertManyResult> {
        self.inner
            .insert_many(&self.namespace(), docs, InsertManyOptions::new())
    }

    /// Insert multiple documents with options.
    pub fn insert_many_with_options(
        &self,
        docs: &[T],
        opts: InsertManyOptions,
    ) -> Result<InsertManyResult> {
        self.inner.insert_many(&self.namespace(), docs, opts)
    }

    // -------------------------------------------------------------------------
    // Find
    // -------------------------------------------------------------------------

    /// Find the first document matching `filter`. Returns `None` if no document matches.
    pub fn find_one(&self, filter: Document) -> Result<Option<T>> {
        self.inner.find_one(&self.namespace(), filter)
    }

    /// Find all documents matching `filter`. Returns a [`Cursor`] over the results.
    pub fn find(&self, filter: Document) -> Result<Cursor<T>> {
        self.inner.find(&self.namespace(), filter, FindOptions::new())
    }

    /// Find all documents matching `filter` with options (sort, limit, skip, projection).
    pub fn find_with_options(&self, filter: Document, opts: FindOptions) -> Result<Cursor<T>> {
        self.inner.find(&self.namespace(), filter, opts)
    }

    // -------------------------------------------------------------------------
    // Update
    // -------------------------------------------------------------------------

    /// Update the first document matching `filter`.
    pub fn update_one(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        self.inner
            .update_one(&self.namespace(), filter, update, UpdateOptions::new())
    }

    /// Update the first document matching `filter` with options (e.g., upsert).
    pub fn update_one_with_options(
        &self,
        filter: Document,
        update: Document,
        opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        self.inner.update_one(&self.namespace(), filter, update, opts)
    }

    /// Update all documents matching `filter`.
    pub fn update_many(&self, filter: Document, update: Document) -> Result<UpdateResult> {
        self.inner
            .update_many(&self.namespace(), filter, update, UpdateOptions::new())
    }

    /// Update all documents matching `filter` with options.
    pub fn update_many_with_options(
        &self,
        filter: Document,
        update: Document,
        opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        self.inner.update_many(&self.namespace(), filter, update, opts)
    }

    // -------------------------------------------------------------------------
    // Delete
    // -------------------------------------------------------------------------

    /// Delete the first document matching `filter`.
    pub fn delete_one(&self, filter: Document) -> Result<DeleteResult> {
        self.inner.delete_one(&self.namespace(), filter)
    }

    /// Delete all documents matching `filter`.
    pub fn delete_many(&self, filter: Document) -> Result<DeleteResult> {
        self.inner.delete_many(&self.namespace(), filter)
    }

    // -------------------------------------------------------------------------
    // Atomic read-modify-write (findAndModify equivalents)
    // -------------------------------------------------------------------------

    /// Atomically find the first document matching `filter`, apply `update`, and return the
    /// original document (before the update). Returns `None` if no document matched.
    pub fn find_one_and_update(&self, filter: Document, update: Document) -> Result<Option<T>> {
        self.inner.find_one_and_update(&self.namespace(), filter, update)
    }

    /// Atomically find the first document matching `filter`, apply `update` with options.
    /// Returns `None` if no document matched (and no upsert was performed).
    pub fn find_one_and_update_with_options(
        &self,
        filter: Document,
        update: Document,
        opts: FindOneAndUpdateOptions,
    ) -> Result<Option<T>> {
        self.inner
            .find_one_and_update_with_options(&self.namespace(), filter, update, opts)
    }

    /// Atomically find the first document matching `filter`, delete it, and return it.
    /// Returns `None` if no document matched.
    pub fn find_one_and_delete(&self, filter: Document) -> Result<Option<T>> {
        self.inner.find_one_and_delete(&self.namespace(), filter)
    }

    /// Atomically find the first document matching `filter`, delete it, and return it with options.
    pub fn find_one_and_delete_with_options(
        &self,
        filter: Document,
        opts: FindOneAndDeleteOptions,
    ) -> Result<Option<T>> {
        self.inner
            .find_one_and_delete_with_options(&self.namespace(), filter, opts)
    }

    /// Atomically find the first document matching `filter`, replace it with `replacement`,
    /// and return the original document. Returns `None` if no document matched.
    pub fn find_one_and_replace(&self, filter: Document, replacement: &T) -> Result<Option<T>> {
        self.inner
            .find_one_and_replace(&self.namespace(), filter, replacement)
    }

    /// Atomically find the first document matching `filter`, replace it with `replacement`
    /// with options.  Returns `None` if no document matched (and no upsert was performed).
    pub fn find_one_and_replace_with_options(
        &self,
        filter: Document,
        replacement: &T,
        opts: FindOneAndReplaceOptions,
    ) -> Result<Option<T>> {
        self.inner
            .find_one_and_replace_with_options(&self.namespace(), filter, replacement, opts)
    }

    // -------------------------------------------------------------------------
    // Count
    // -------------------------------------------------------------------------

    /// Return an approximate count of all documents in the collection.
    /// This is a fast estimate, not a precise count.
    pub fn estimated_document_count(&self) -> Result<u64> {
        self.inner.estimated_document_count(&self.namespace())
    }

    /// Return the exact count of documents matching `filter`.
    pub fn count_documents(&self, filter: Document) -> Result<u64> {
        self.inner.count_documents(&self.namespace(), filter)
    }

    // -------------------------------------------------------------------------
    // Indexes
    // -------------------------------------------------------------------------

    /// Create an index on the collection.
    pub fn create_index(&self, model: IndexModel) -> Result<String> {
        self.inner.create_index(&self.namespace(), model)
    }

    /// Drop an index by name.
    pub fn drop_index(&self, index_name: &str) -> Result<()> {
        self.inner.drop_index(&self.namespace(), index_name)
    }

    /// List all indexes on the collection.
    pub fn list_indexes(&self) -> Result<Vec<IndexInfo>> {
        self.inner.list_indexes(&self.namespace())
    }
}
