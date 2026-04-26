mod actions;

pub use actions::{
    Find, FindOneAndDelete, FindOneAndReplace, FindOneAndUpdate, InsertMany, Update,
};

use bson::Document;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;

use crate::{
    error::Result,
    index::{IndexInfo, IndexModel},
    options::{
        FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
        InsertManyOptions, UpdateOptions,
    },
    results::{DeleteResult, InsertOneResult},
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
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the storage engine rejects
    /// the insert.
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
    #[must_use]
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
    ///
    /// # Errors
    ///
    /// Returns an error if the query cannot be evaluated or deserialization
    /// into `T` fails.
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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
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
    ///
    /// # Errors
    ///
    /// Returns an error if the delete cannot be applied by the storage engine.
    pub fn delete_one(&self, filter: Document) -> Result<DeleteResult> {
        self.inner.delete_one(&self.namespace(), filter)
    }

    /// Delete all documents matching `filter`.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete cannot be applied by the storage engine.
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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
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
    ///
    /// # Errors
    ///
    /// Returns an error if the storage engine cannot read collection metadata.
    pub fn estimated_document_count(&self) -> Result<u64> {
        self.inner.estimated_document_count(&self.namespace())
    }

    /// Return the exact count of documents matching `filter`.
    ///
    /// # Errors
    ///
    /// Returns an error if the query cannot be evaluated.
    pub fn count_documents(&self, filter: Document) -> Result<u64> {
        self.inner.count_documents(&self.namespace(), filter)
    }

    // -------------------------------------------------------------------------
    // Indexes
    // -------------------------------------------------------------------------

    /// Create an index on the collection.
    ///
    /// # Errors
    ///
    /// Returns an error if the index definition is unsupported or index build
    /// fails.
    pub fn create_index(&self, model: IndexModel) -> Result<String> {
        self.inner.create_index(&self.namespace(), model)
    }

    /// Drop an index by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the index cannot be found or dropped.
    pub fn drop_index(&self, index_name: &str) -> Result<()> {
        self.inner.drop_index(&self.namespace(), index_name)
    }

    /// List all indexes on the collection.
    ///
    /// # Errors
    ///
    /// Returns an error if index metadata cannot be read.
    pub fn list_indexes(&self) -> Result<Vec<IndexInfo>> {
        self.inner.list_indexes(&self.namespace())
    }
}
