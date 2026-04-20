use bson::Document;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

use crate::{
    cursor::Cursor,
    error::Result,
    index::{IndexInfo, IndexModel},
    options::{
        FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
        InsertManyOptions, ReturnDocument, UpdateOptions,
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
/// # use tempfile::TempDir;
/// # let dir = TempDir::new()?;
/// # let client = Client::open(dir.path().join("db.mqlite"))?;
/// let db = client.database("myapp");
/// let users = db.collection::<User>("users");
/// users.insert_one(&User { name: "Alice".into(), email: "alice@example.com".into() })?;
/// # Ok(())
/// # }
/// ```
pub struct Collection<T> {
    pub(crate) db_name: String,
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
    #[must_use]
    pub fn namespace(&self) -> String {
        format!("{}.{}", self.db_name, self.name)
    }

    /// The unqualified collection name (without the database prefix).
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The database name this collection belongs to.
    #[must_use]
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

    /// Insert multiple documents.
    ///
    /// Returns an [`InsertMany`] action. Chain option methods before calling `.run()`:
    /// ```no_run
    /// # use mqlite::{Client, doc};
    /// # use bson::Document;
    /// # fn main() -> mqlite::error::Result<()> {
    /// # let client = Client::open("/tmp/db.mqlite")?;
    /// # let col = client.database("test").collection::<Document>("items");
    /// let docs = vec![doc! { "x": 1 }, doc! { "x": 2 }];
    /// col.insert_many(&docs).ordered(false).run()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_many<'a>(&'a self, docs: &'a [T]) -> InsertMany<'a, T> {
        InsertMany {
            coll: self,
            docs,
            options: InsertManyOptions::default(),
        }
    }

    // -------------------------------------------------------------------------
    // Find
    // -------------------------------------------------------------------------

    /// Find the first document matching `filter`. Returns `None` if no document matches.
    pub fn find_one(&self, filter: Document) -> Result<Option<T>> {
        self.inner.find_one(&self.namespace(), filter)
    }

    /// Find all documents matching `filter`.
    ///
    /// Returns a [`Find`] action. Chain option methods before calling `.run()`:
    /// ```no_run
    /// # use mqlite::{Client, doc};
    /// # use bson::Document;
    /// # fn main() -> mqlite::error::Result<()> {
    /// # let client = Client::open("/tmp/db.mqlite")?;
    /// # let col = client.database("test").collection::<Document>("items");
    /// let cursor = col.find(doc! { "active": true })
    ///     .sort(doc! { "name": 1 })
    ///     .limit(20)
    ///     .run()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn find(&self, filter: Document) -> Find<'_, T> {
        Find {
            coll: self,
            filter,
            options: FindOptions::default(),
        }
    }

    // -------------------------------------------------------------------------
    // Update
    // -------------------------------------------------------------------------

    /// Update the first document matching `filter`.
    ///
    /// Returns an [`Update`] action. Chain option methods before calling `.run()`:
    /// ```no_run
    /// # use mqlite::{Client, doc};
    /// # use bson::Document;
    /// # fn main() -> mqlite::error::Result<()> {
    /// # let client = Client::open("/tmp/db.mqlite")?;
    /// # let col = client.database("test").collection::<Document>("users");
    /// col.update_one(doc! { "email": "a@b.com" }, doc! { "$set": { "active": true } })
    ///     .upsert(true)
    ///     .run()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn update_one(&self, filter: Document, update: Document) -> Update<'_, T> {
        Update {
            coll: self,
            filter,
            update,
            options: UpdateOptions::default(),
            multi: false,
        }
    }

    /// Update all documents matching `filter`.
    ///
    /// Returns an [`Update`] action. Chain option methods before calling `.run()`.
    pub fn update_many(&self, filter: Document, update: Document) -> Update<'_, T> {
        Update {
            coll: self,
            filter,
            update,
            options: UpdateOptions::default(),
            multi: true,
        }
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
    // Atomic read-modify-write
    // -------------------------------------------------------------------------

    /// Atomically find the first document matching `filter`, apply `update`, and return a
    /// document. By default returns the document **before** the update.
    ///
    /// Returns a [`FindOneAndUpdate`] action. Chain option methods before calling `.run()`:
    /// ```no_run
    /// # use mqlite::{Client, doc, options::ReturnDocument};
    /// # use bson::Document;
    /// # fn main() -> mqlite::error::Result<()> {
    /// # let client = Client::open("/tmp/db.mqlite")?;
    /// # let col = client.database("test").collection::<Document>("items");
    /// let updated = col
    ///     .find_one_and_update(doc! { "x": 1 }, doc! { "$set": { "x": 2 } })
    ///     .return_document(ReturnDocument::After)
    ///     .run()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn find_one_and_update(
        &self,
        filter: Document,
        update: Document,
    ) -> FindOneAndUpdate<'_, T> {
        FindOneAndUpdate {
            coll: self,
            filter,
            update,
            options: FindOneAndUpdateOptions::default(),
        }
    }

    /// Atomically find the first document matching `filter`, delete it, and return it.
    ///
    /// Returns a [`FindOneAndDelete`] action. Chain option methods before calling `.run()`.
    pub fn find_one_and_delete(&self, filter: Document) -> FindOneAndDelete<'_, T> {
        FindOneAndDelete {
            coll: self,
            filter,
            options: FindOneAndDeleteOptions::default(),
        }
    }

    /// Atomically find the first document matching `filter`, replace it with `replacement`,
    /// and return a document. By default returns the document **before** the replacement.
    ///
    /// Returns a [`FindOneAndReplace`] action. Chain option methods before calling `.run()`.
    pub fn find_one_and_replace<'a>(
        &'a self,
        filter: Document,
        replacement: &'a T,
    ) -> FindOneAndReplace<'a, T> {
        FindOneAndReplace {
            coll: self,
            filter,
            replacement,
            options: FindOneAndReplaceOptions::default(),
        }
    }

    // -------------------------------------------------------------------------
    // Count
    // -------------------------------------------------------------------------

    /// Return an approximate count of all documents in the collection.
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

// =============================================================================
// Action types
// =============================================================================

/// Action returned by [`Collection::find`]. Chain option methods, then call `.run()`.
pub struct Find<'a, T> {
    coll: &'a Collection<T>,
    filter: Document,
    options: FindOptions,
}

impl<'a, T: DeserializeOwned> Find<'a, T> {
    /// Sort order for results.
    #[must_use]
    pub fn sort(mut self, sort: Document) -> Self {
        self.options.sort = Some(sort);
        self
    }

    /// Maximum number of documents to return.
    #[must_use]
    pub fn limit(mut self, limit: i64) -> Self {
        self.options.limit = Some(limit);
        self
    }

    /// Number of documents to skip before returning results.
    #[must_use]
    pub fn skip(mut self, skip: u64) -> Self {
        self.options.skip = Some(skip);
        self
    }

    /// Projection document specifying fields to include or exclude.
    #[must_use]
    pub fn projection(mut self, projection: Document) -> Self {
        self.options.projection = Some(projection);
        self
    }

    /// Number of documents per internal batch.
    #[must_use]
    pub fn batch_size(mut self, batch_size: u32) -> Self {
        self.options.batch_size = Some(batch_size);
        self
    }

    /// Execute the find and return a [`Cursor`].
    pub fn run(self) -> Result<Cursor<T>> {
        self.coll
            .inner
            .find(&self.coll.namespace(), self.filter, self.options)
    }
}

/// Action returned by [`Collection::insert_many`]. Chain option methods, then call `.run()`.
pub struct InsertMany<'a, T> {
    coll: &'a Collection<T>,
    docs: &'a [T],
    options: InsertManyOptions,
}

impl<'a, T: Serialize + DeserializeOwned> InsertMany<'a, T> {
    /// If `true` (default), stop at the first error.
    /// If `false`, attempt all documents and collect errors.
    #[must_use]
    pub fn ordered(mut self, ordered: bool) -> Self {
        self.options.ordered = ordered;
        self
    }

    /// Execute the insert and return an [`InsertManyResult`].
    pub fn run(self) -> Result<InsertManyResult> {
        self.coll
            .inner
            .insert_many(&self.coll.namespace(), self.docs, self.options)
    }
}

/// Action returned by [`Collection::update_one`] and [`Collection::update_many`].
/// Chain option methods, then call `.run()`.
pub struct Update<'a, T> {
    coll: &'a Collection<T>,
    filter: Document,
    update: Document,
    options: UpdateOptions,
    multi: bool,
}

impl<'a, T> Update<'a, T> {
    /// Insert a new document when no document matches the filter.
    #[must_use]
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.options.upsert = upsert;
        self
    }

    /// Execute the update and return an [`UpdateResult`].
    pub fn run(self) -> Result<UpdateResult> {
        if self.multi {
            self.coll.inner.update_many(
                &self.coll.namespace(),
                self.filter,
                self.update,
                self.options,
            )
        } else {
            self.coll.inner.update_one(
                &self.coll.namespace(),
                self.filter,
                self.update,
                self.options,
            )
        }
    }
}

/// Action returned by [`Collection::find_one_and_update`]. Chain option methods, then call `.run()`.
pub struct FindOneAndUpdate<'a, T> {
    coll: &'a Collection<T>,
    filter: Document,
    update: Document,
    options: FindOneAndUpdateOptions,
}

impl<'a, T: Serialize + DeserializeOwned> FindOneAndUpdate<'a, T> {
    /// Which version of the document to return. Default: [`ReturnDocument::Before`].
    #[must_use]
    pub fn return_document(mut self, rd: ReturnDocument) -> Self {
        self.options.return_document = rd;
        self
    }

    /// Insert a new document when no document matches the filter.
    #[must_use]
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.options.upsert = upsert;
        self
    }

    /// Sort order used to pick a document when multiple match.
    #[must_use]
    pub fn sort(mut self, sort: Document) -> Self {
        self.options.sort = Some(sort);
        self
    }

    /// Execute and return the document (before or after update, per [`return_document`]).
    pub fn run(self) -> Result<Option<T>> {
        self.coll.inner.find_one_and_update_with_options(
            &self.coll.namespace(),
            self.filter,
            self.update,
            self.options,
        )
    }
}

/// Action returned by [`Collection::find_one_and_delete`]. Chain option methods, then call `.run()`.
pub struct FindOneAndDelete<'a, T> {
    coll: &'a Collection<T>,
    filter: Document,
    options: FindOneAndDeleteOptions,
}

impl<'a, T: DeserializeOwned> FindOneAndDelete<'a, T> {
    /// Sort order used to pick a document when multiple match.
    #[must_use]
    pub fn sort(mut self, sort: Document) -> Self {
        self.options.sort = Some(sort);
        self
    }

    /// Execute and return the deleted document, or `None` if no document matched.
    pub fn run(self) -> Result<Option<T>> {
        self.coll.inner.find_one_and_delete_with_options(
            &self.coll.namespace(),
            self.filter,
            self.options,
        )
    }
}

/// Action returned by [`Collection::find_one_and_replace`]. Chain option methods, then call `.run()`.
pub struct FindOneAndReplace<'a, T> {
    coll: &'a Collection<T>,
    filter: Document,
    replacement: &'a T,
    options: FindOneAndReplaceOptions,
}

impl<'a, T: Serialize + DeserializeOwned> FindOneAndReplace<'a, T> {
    /// Which version of the document to return. Default: [`ReturnDocument::Before`].
    #[must_use]
    pub fn return_document(mut self, rd: ReturnDocument) -> Self {
        self.options.return_document = rd;
        self
    }

    /// Insert a new document when no document matches the filter.
    #[must_use]
    pub fn upsert(mut self, upsert: bool) -> Self {
        self.options.upsert = upsert;
        self
    }

    /// Sort order used to pick a document when multiple match.
    #[must_use]
    pub fn sort(mut self, sort: Document) -> Self {
        self.options.sort = Some(sort);
        self
    }

    /// Execute and return the document (before or after replacement, per [`return_document`]).
    pub fn run(self) -> Result<Option<T>> {
        self.coll.inner.find_one_and_replace_with_options(
            &self.coll.namespace(),
            self.filter,
            self.replacement,
            self.options,
        )
    }
}
